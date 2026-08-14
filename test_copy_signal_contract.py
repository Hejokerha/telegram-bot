"""Pure contract tests for the Copy signal boundary in ``main.py``.

The production module owns Telegram/Firebase/FastAPI startup state, so importing it
would make these unit tests depend on credentials and network services.  The small
loader below extracts only the requested pure helpers (and their pure, named
dependencies) from the module AST.
"""

from __future__ import annotations

import ast
import hashlib
import hmac
import json
import logging
import re
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


UTC = timezone.utc
MAIN_PATH = Path(__file__).with_name("main.py")

CONTRACT_ROOTS = {
    "build_copy_trading_payload",
    "_copy_duration_seconds",
    "_copy_signal_contract_kind",
    "_copy_server_duration_seconds",
    "_copy_server_sanitize_signal",
    "_copy_signal_fingerprint",
    "_copy_register_signal_id",
    "_copy_append_signal_history",
    "_copy_store_signal_if_allowed",
    "_copy_is_mobile_executable_signal",
    "_copy_signal_delivery_allowed",
    "_copy_filter_mobile_recent_signals",
    "_copy_broadcast_global_control",
}


def _normalize_telegram_user_id(value):
    try:
        parsed = int(value)
        return parsed if parsed > 0 else None
    except (TypeError, ValueError):
        return None


def _load_contract_namespace() -> dict:
    source = MAIN_PATH.read_text(encoding="utf-8-sig")
    tree = ast.parse(source, filename=str(MAIN_PATH))
    definitions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    missing = CONTRACT_ROOTS.difference(definitions)
    if missing:
        raise AssertionError(
            "main.py is missing Copy contract helpers: " + ", ".join(sorted(missing))
        )

    # Follow direct calls to other module functions.  This normally selects only
    # parsing/normalization helpers and never executes application startup code.
    selected = set(CONTRACT_ROOTS)
    pending = list(CONTRACT_ROOTS)
    while pending:
        node = definitions[pending.pop()]
        for child in ast.walk(node):
            if not isinstance(child, ast.Call) or not isinstance(child.func, ast.Name):
                continue
            dependency = child.func.id
            if dependency in definitions and dependency not in selected:
                selected.add(dependency)
                pending.append(dependency)

    selected_nodes = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in selected
    ]
    module = ast.Module(body=selected_nodes, type_ignores=[])
    ast.fix_missing_locations(module)

    namespace = {
        "__builtins__": __builtins__,
        "datetime": datetime,
        "timedelta": timedelta,
        "timezone": timezone,
        "UTC": UTC,
        "hashlib": hashlib,
        "hmac": hmac,
        "json": json,
        "re": re,
        "logger": logging.getLogger("copy-contract-test"),
        "COPY_SIGNAL_VALIDITY_SECONDS": 25,
        "COPY_SIGNAL_HISTORY_LIMIT": 200,
        # Accept either descriptive constant spelling used by the implementation.
        "COPY_SIGNAL_MIN_DURATION_SECONDS": 5,
        "COPY_SIGNAL_MAX_DURATION_SECONDS": 600,
        "COPY_SIGNAL_DURATION_MIN_SECONDS": 5,
        "COPY_SIGNAL_DURATION_MAX_SECONDS": 600,
        "COPY_SIGNAL_MIN_ENTRY_DELAY_SECONDS": 1,
        "COPY_SIGNAL_MAX_ENTRY_DELAY_SECONDS": 15,
        "COPY_SIGNAL_ENTRY_DELAY_MIN_SECONDS": 1,
        "COPY_SIGNAL_ENTRY_DELAY_MAX_SECONDS": 15,
        "COPY_SIGNAL_MAX_ENTRY_DELAY_MIN_SECONDS": 1,
        "COPY_SIGNAL_MAX_ENTRY_DELAY_MAX_SECONDS": 15,
        "COPY_ALLOWED_SIGNAL_SOURCES": frozenset(
            {
                "three_candle",
                "timed_list",
                "otc_live",
                "real_market",
                "trading_room",
                "otc_edge",
                "admin_manual",
            }
        ),
        "COPY_EXECUTABLE_SOURCES": frozenset(
            {
                "three_candle",
                "timed_list",
                "otc_live",
                "real_market",
                "trading_room",
                "otc_edge",
            }
        ),
        "COPY_SOURCE_MAX_ENTRY_DELAY_SECONDS": {
            "three_candle": 10,
            "timed_list": 10,
            "otc_live": 10,
            "real_market": 10,
            "trading_room": 10,
            "otc_edge": 10,
        },
        "normalize_copy_telegram_user_id": _normalize_telegram_user_id,
        "_copy_clients": {},
        "_mobile_signal_clients": {},
    }
    exec(compile(module, str(MAIN_PATH), "exec"), namespace)
    return namespace


NS = _load_contract_namespace()


def _execute_payload(source: str, **updates) -> dict:
    entry = datetime(2026, 8, 13, 12, 0, 0, tzinfo=UTC)
    is_otc = source != "real_market"
    payload = {
        "id": f"test-{source}",
        "source": source,
        "pair": "EUR/USD (OTC)" if is_otc else "EUR/USD",
        "platform_symbol": "EURUSD_otc" if is_otc else "EURUSD",
        "direction": "CALL",
        "duration_seconds": 60,
        "entry_time": entry.isoformat(),
        "created_at": (entry - timedelta(seconds=1)).isoformat(),
        "expires_at": (entry + timedelta(seconds=10)).isoformat(),
        "expiry_timestamp": int(entry.timestamp()) + 60,
        "max_entry_delay_seconds": 10,
        "trade_expiry_mode": "timer",
    }
    payload.update(updates)
    return payload


class CopySourceContractTests(unittest.TestCase):
    def setUp(self):
        self.sanitize = NS["_copy_server_sanitize_signal"]
        self.kind = NS["_copy_signal_contract_kind"]
        self.mobile_executable = NS["_copy_is_mobile_executable_signal"]

    def test_all_six_canonical_sources_are_executable(self):
        for source in (
            "three_candle",
            "timed_list",
            "otc_live",
            "real_market",
            "trading_room",
            "otc_edge",
        ):
            with self.subTest(source=source):
                normalized = self.sanitize(_execute_payload(source))
                self.assertEqual(normalized["source"], source)
                self.assertEqual(self.kind(normalized), "execute")
                self.assertFalse(normalized["prepare_only"])
                self.assertTrue(self.mobile_executable(normalized))

    def test_only_otc_edge_can_be_a_prepare_signal(self):
        prepare = _execute_payload(
            "otc_edge",
            direction=None,
            prepare_only=True,
            signal_kind="prepare",
            max_entry_delay_seconds=None,
        )
        normalized = self.sanitize(prepare)
        self.assertEqual(self.kind(normalized), "prepare")
        self.assertTrue(normalized["prepare_only"])
        self.assertFalse(self.mobile_executable(normalized))

        for source in (
            "three_candle",
            "timed_list",
            "otc_live",
            "real_market",
            "trading_room",
        ):
            with self.subTest(source=source), self.assertRaises(ValueError):
                self.sanitize(
                    _execute_payload(
                        source,
                        prepare_only=True,
                        signal_kind="prepare",
                        max_entry_delay_seconds=None,
                    )
                )

    def test_admin_manual_is_the_only_noncanonical_execute_source(self):
        normalized = self.sanitize(_execute_payload("admin_manual"))
        self.assertEqual(normalized["source"], "admin_manual")
        self.assertEqual(self.kind(normalized), "execute")
        self.assertTrue(self.mobile_executable(normalized))

    def test_unknown_source_is_rejected(self):
        for source in ("untrusted_source", "untrusted_list", "global_fake", "direct_attack"):
            with self.subTest(source=source), self.assertRaises(ValueError):
                self.sanitize(_execute_payload(source))

    def test_pair_and_platform_symbol_must_describe_the_same_market(self):
        normalized = self.sanitize(_execute_payload("otc_live"))
        self.assertEqual(normalized["pair"], "EUR/USD (OTC)")
        self.assertEqual(normalized["platform_symbol"], "EURUSD_otc")
        self.assertTrue(normalized["otc"])

        for mutation in (
            {"pair": "EUR/USD", "platform_symbol": "EURUSD_otc"},
            {"pair": "EUR/USD (OTC)", "platform_symbol": "EURUSD"},
            {"pair": "EUR/USD (OTC)", "platform_symbol": "USDJPY_otc"},
        ):
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.sanitize(_execute_payload("otc_live", **mutation))

        with self.assertRaises(ValueError):
            self.sanitize(
                _execute_payload(
                    "real_market",
                    pair="EUR/USD (OTC)",
                    platform_symbol="EURUSD_otc",
                )
            )

    def test_source_duration_matrix(self):
        cases = {
            "three_candle": 60,
            "timed_list": 60,
            "otc_live": 60,
            "real_market": 600,
            "trading_room": 60,
            "otc_edge": 5,
        }
        entry = datetime(2026, 8, 13, 12, 0, 0, tzinfo=UTC)
        for source, duration in cases.items():
            payload = _execute_payload(
                source,
                duration_seconds=duration,
                timeframe="M10" if duration == 600 else "M1",
                expiry_timestamp=int(entry.timestamp()) + duration,
            )
            with self.subTest(source=source, duration=duration):
                normalized = self.sanitize(payload)
                self.assertEqual(normalized["duration_seconds"], duration)

    def test_producer_builder_emits_complete_execute_contract_for_every_source(self):
        build = NS["build_copy_trading_payload"]
        entry = datetime(2026, 8, 13, 12, 0, 0, tzinfo=UTC)
        fixtures = {
            "three_candle": {"duration_seconds": 60},
            "timed_list": {"duration_seconds": 60},
            "otc_live": {"timeframe": "M1", "duration_seconds": 60},
            "real_market": {"timeframe": 5, "duration_minutes": 5},
            "trading_room": {"duration_seconds": 60},
            "otc_edge": {
                "duration_seconds": 5,
                "max_entry_delay_seconds": 10,
                "expiry_timestamp": int(entry.timestamp()) + 5,
            },
        }
        for source, fields in fixtures.items():
            payload = build(
                {
                    "pair": "EUR/USD",
                    "direction": "CALL",
                    "entry_time": entry.isoformat(),
                    **fields,
                },
                source=source,
            )
            with self.subTest(source=source):
                self.assertEqual(payload["signal_kind"], "execute")
                self.assertIsInstance(payload["duration_seconds"], int)
                self.assertGreaterEqual(payload["duration_seconds"], 5)
                self.assertLessEqual(payload["duration_seconds"], 600)
                self.assertIsInstance(payload["max_entry_delay_seconds"], int)
                self.assertTrue(payload["expiry_time"] or payload["expiry_timestamp"])
                self.sanitize(payload)

    def test_producer_builder_never_defaults_a_missing_duration(self):
        with self.assertRaises(ValueError):
            NS["build_copy_trading_payload"](
                {
                    "pair": "EUR/USD",
                    "direction": "CALL",
                    "entry_time": "2026-08-13T12:00:00+00:00",
                },
                source="otc_live",
            )

    def test_otc_edge_short_durations_remain_exact(self):
        entry = datetime(2026, 8, 13, 12, 0, 0, tzinfo=UTC)
        for duration in (5, 9, 10, 59):
            with self.subTest(source="otc_edge", duration=duration):
                normalized = self.sanitize(
                    _execute_payload(
                        "otc_edge",
                        duration_seconds=duration,
                        expiry_timestamp=int(entry.timestamp()) + duration,
                    )
                )
                self.assertEqual(normalized["duration_seconds"], duration)


class CopyTimingContractTests(unittest.TestCase):
    def setUp(self):
        self.duration = NS["_copy_server_duration_seconds"]
        self.sanitize = NS["_copy_server_sanitize_signal"]

    def test_duration_bounds_are_inclusive(self):
        self.assertEqual(self.duration({"duration_seconds": 5}), 5)
        self.assertEqual(self.duration({"duration_seconds": 600}), 600)

    def test_duration_outside_bounds_or_invalid_is_rejected(self):
        for value in (None, "", 0, 4, 601, -1, "not-a-number", True, 5.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.duration({"duration_seconds": value})

    def test_execute_requires_integer_max_entry_delay_in_range(self):
        for value in (1, 15):
            with self.subTest(value=value):
                normalized = self.sanitize(
                    _execute_payload(
                        "three_candle",
                        max_entry_delay_seconds=value,
                        expires_at=(
                            datetime(2026, 8, 13, 12, 0, 0, tzinfo=UTC)
                            + timedelta(seconds=value)
                        ).isoformat(),
                    )
                )
                self.assertEqual(normalized["max_entry_delay_seconds"], value)

        for value in (None, "", 0, 16, -1, True, 1.5, "2.5"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.sanitize(
                    _execute_payload("three_candle", max_entry_delay_seconds=value)
                )

    def test_expiry_is_derived_from_entry_plus_duration(self):
        payload = _execute_payload("otc_live")
        payload.pop("expiry_timestamp")
        normalized = self.sanitize(payload)
        entry = datetime.fromisoformat(normalized["entry_time"])
        self.assertEqual(
            normalized["expiry_timestamp"], int(entry.timestamp()) + 60
        )

    def test_explicit_expiry_tolerates_at_most_two_seconds(self):
        base = _execute_payload("otc_edge")
        expected = base["expiry_timestamp"]
        for delta in (-2, -1, 0, 1, 2):
            with self.subTest(delta=delta):
                normalized = self.sanitize(
                    {**base, "expiry_timestamp": expected + delta}
                )
                self.assertEqual(normalized["expiry_timestamp"], expected + delta)

        for delta in (-3, 3):
            with self.subTest(delta=delta), self.assertRaises(ValueError):
                self.sanitize({**base, "expiry_timestamp": expected + delta})

    def test_expiry_mode_is_canonical_and_absolute_time_is_otc_minute_aligned(self):
        timer = self.sanitize(
            _execute_payload("otc_edge", duration_seconds=5, expiry_timestamp=1786622405)
        )
        self.assertEqual(timer["trade_expiry_mode"], "timer")

        absolute = self.sanitize(
            _execute_payload(
                "otc_edge",
                trade_expiry_mode="m1_candle_close",
                expiry_timestamp=1786622460,
            )
        )
        self.assertEqual(absolute["trade_expiry_mode"], "absolute_time")

        with self.assertRaises(ValueError):
            self.sanitize(
                _execute_payload(
                    "real_market",
                    trade_expiry_mode="absolute_time",
                )
            )
        with self.assertRaises(ValueError):
            self.sanitize(
                _execute_payload(
                    "otc_edge",
                    trade_expiry_mode="absolute_time",
                    duration_seconds=59,
                    expiry_timestamp=1786622459,
                )
            )
        with self.assertRaises(ValueError):
            self.sanitize(_execute_payload("otc_edge", trade_expiry_mode="mystery"))

    def test_entry_window_must_be_positive_and_not_exceed_max_delay(self):
        entry = datetime(2026, 8, 13, 12, 0, 0, tzinfo=UTC)
        for seconds in (0, -1, 16):
            with self.subTest(seconds=seconds), self.assertRaises(ValueError):
                self.sanitize(
                    _execute_payload(
                        "trading_room",
                        max_entry_delay_seconds=15,
                        expires_at=(entry + timedelta(seconds=seconds)).isoformat(),
                    )
                )


class CopyIdAndHistoryContractTests(unittest.TestCase):
    def setUp(self):
        self.sanitize = NS["_copy_server_sanitize_signal"]
        self.fingerprint = NS["_copy_signal_fingerprint"]
        self.register = NS["_copy_register_signal_id"]
        self.append = NS["_copy_append_signal_history"]
        self.store = NS["_copy_store_signal_if_allowed"]

    def test_fingerprint_is_order_independent_but_payload_sensitive(self):
        signal = self.sanitize(_execute_payload("timed_list", id="same-id"))
        reordered = dict(reversed(list(signal.items())))
        changed = {**signal, "direction": "PUT"}
        self.assertEqual(self.fingerprint(signal), self.fingerprint(reordered))
        self.assertNotEqual(self.fingerprint(signal), self.fingerprint(changed))

    def test_register_distinguishes_duplicate_from_collision_without_growth(self):
        registry = {}
        signal = self.sanitize(_execute_payload("timed_list", id="same-id"))
        changed = {**signal, "direction": "PUT"}

        first = self.register(registry, signal, 10)
        duplicate = self.register(registry, dict(signal), 10)
        self.assertEqual(len(registry), 1)
        self.assertEqual(first["status"], "accepted")
        self.assertTrue(first["accepted"])
        self.assertEqual(duplicate["status"], "duplicate")
        self.assertFalse(duplicate["accepted"])

        try:
            collision = self.register(registry, changed, 10)
        except ValueError:
            collision = "collision"
        self.assertEqual(len(registry), 1)
        if isinstance(collision, dict):
            self.assertEqual(collision["status"], "collision")
            self.assertFalse(collision["accepted"])

    def test_history_drops_duplicates_and_rejects_id_collisions(self):
        history = []
        registry = {}
        signal = self.sanitize(_execute_payload("timed_list", id="same-id"))
        for candidate in (signal, dict(signal)):
            registration = self.register(registry, candidate, 10)
            if registration["accepted"]:
                self.append(history, candidate, 10)
        self.assertEqual(len(history), 1)

        changed = {**signal, "direction": "PUT"}
        try:
            registration = self.register(registry, changed, 10)
            if registration["accepted"]:
                self.append(history, changed, 10)
        except ValueError:
            pass
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["direction"], "CALL")

    def test_registry_and_history_are_bounded(self):
        registry = {}
        history = []
        for index in range(8):
            signal = self.sanitize(
                _execute_payload("real_market", id=f"bounded-{index}")
            )
            self.register(registry, signal, 3)
            self.append(history, signal, 3)
        self.assertLessEqual(len(registry), 3)
        self.assertEqual(len(history), 3)
        self.assertEqual(
            [row["id"] for row in history],
            ["bounded-5", "bounded-6", "bounded-7"],
        )

    def test_global_stop_never_registers_or_appends(self):
        registry = {}
        history = []
        signal = self.sanitize(_execute_payload("three_candle", id="stopped"))
        result = self.store(history, registry, signal, False, 10, 10)
        self.assertEqual(result["status"], "global_stop")
        self.assertFalse(result["accepted"])
        self.assertEqual(history, [])
        self.assertEqual(registry, {})


class CopyGlobalStopContractTests(unittest.TestCase):
    def setUp(self):
        self.sanitize = NS["_copy_server_sanitize_signal"]
        self.allowed = NS["_copy_signal_delivery_allowed"]
        self.recent = NS["_copy_filter_mobile_recent_signals"]

    def test_global_stop_blocks_extension_mobile_and_recent_visibility(self):
        signal = self.sanitize(_execute_payload("three_candle"))
        self.assertFalse(self.allowed(False, signal))
        self.assertFalse(self.allowed(False, signal, mobile=True))
        self.assertEqual(self.recent([signal], False, 123), [])

    def test_running_state_allows_execute_but_mobile_never_executes_prepare(self):
        execute = self.sanitize(_execute_payload("otc_edge"))
        prepare = self.sanitize(
            _execute_payload(
                "otc_edge",
                direction=None,
                prepare_only=True,
                signal_kind="prepare",
                max_entry_delay_seconds=None,
            )
        )
        self.assertTrue(self.allowed(True, execute))
        self.assertTrue(self.allowed(True, execute, mobile=True))
        self.assertFalse(self.allowed(True, prepare, mobile=True))
        self.assertEqual(self.recent([execute, prepare], True, None), [execute])

    def test_recent_filters_target_scope_and_is_bounded(self):
        broadcast = self.sanitize(_execute_payload("three_candle", id="broadcast"))
        own = self.sanitize(
            _execute_payload("timed_list", id="own", target_user_id=123)
        )
        other = self.sanitize(
            _execute_payload("otc_live", id="other", target_user_id=456)
        )
        visible = self.recent([broadcast, own, other], True, 123, limit=2)
        self.assertEqual([row["id"] for row in visible], ["broadcast", "own"])


class _FakeSocket:
    def __init__(self):
        self.messages = []

    async def send_json(self, payload):
        self.messages.append(dict(payload))


class CopyGlobalControlTests(unittest.IsolatedAsyncioTestCase):
    async def test_stop_control_reaches_extension_and_mobile(self):
        extension = _FakeSocket()
        mobile = _FakeSocket()
        NS["_copy_clients"].clear()
        NS["_mobile_signal_clients"].clear()
        NS["_copy_clients"]["ext"] = {"ws": extension}
        NS["_mobile_signal_clients"]["mobile"] = {"ws": mobile}

        result = await NS["_copy_broadcast_global_control"](False, 123)

        self.assertEqual(result["action"], "global_stop")
        self.assertEqual(result["extension_delivered"], 1)
        self.assertEqual(result["mobile_delivered"], 1)
        for message in (extension.messages[0], mobile.messages[0]):
            self.assertEqual(message["type"], "control")
            self.assertEqual(message["action"], "global_stop")
            self.assertFalse(message["global_enabled"])
            self.assertTrue(message["discard_pending_signals"])
        self.assertEqual(mobile.messages[0]["transport"], "mobile_push_v1")


class CopyEndpointWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = MAIN_PATH.read_text(encoding="utf-8-sig")

    def test_mobile_push_and_recent_use_fail_closed_helpers(self):
        self.assertIn(
            "_copy_signal_delivery_allowed(global_enabled, signal, mobile=True)",
            self.source,
        )
        self.assertIn("_copy_filter_mobile_recent_signals(", self.source)
        self.assertIn('"global_enabled": global_enabled', self.source)

    def test_producers_use_the_same_bounded_entry_window_as_mobile(self):
        self.assertIn('COPY_SIGNAL_MAX_ENTRY_DELAY_MAX_SECONDS = 15', self.source)
        for source in ("timed_list", "three_candle", "trading_room", "otc_edge"):
            self.assertIn(
                f'COPY_SOURCE_MAX_ENTRY_DELAY_SECONDS["{source}"]',
                self.source,
            )

    def test_ingress_uses_atomic_global_gate_and_bounded_dedupe(self):
        self.assertGreaterEqual(self.source.count("_copy_store_signal_if_allowed("), 3)
        self.assertIn('status"] == "collision"', self.source)
        self.assertIn("COPY_SIGNAL_DEDUPE_LIMIT", self.source)

    def test_mobile_request_endpoint_documents_501(self):
        self.assertIn(
            'responses={501: {"description": "On-demand mobile analysis is not implemented"}}',
            self.source,
        )

    def test_admin_stop_broadcasts_control(self):
        self.assertIn("await _copy_broadcast_global_control(False, user.id)", self.source)


if __name__ == "__main__":
    unittest.main()
