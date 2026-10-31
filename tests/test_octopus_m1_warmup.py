"""Offline checks for Octopus historical M1 startup readiness."""
import ast
import asyncio
from collections import deque
import math
from pathlib import Path
from threading import Lock
from types import SimpleNamespace
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "main.py"


def load_named(name, namespace, *, from_class=None):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    nodes = tree.body
    if from_class:
        nodes = next(node.body for node in tree.body if isinstance(node, ast.ClassDef) and node.name == from_class)
    selected = next(node for node in nodes if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name)
    exec(compile(ast.Module(body=[selected], type_ignores=[]), str(SOURCE), "exec"), namespace)
    return namespace[name]


class OctopusM1WarmupTests(unittest.TestCase):
    def test_history_request_opens_chart_stream_before_loading(self):
        events = []
        now = 1_792_000_000.0
        ns = {
            "asyncio": asyncio,
            "time_module": SimpleNamespace(time=lambda: now, monotonic=lambda: now),
            "logger": SimpleNamespace(info=lambda *args: None),
        }
        send_history = load_named("_send_m1_history_hydration", ns, from_class="QuotexOTCLiveFeed")

        class Feed:
            lock = Lock()
            history_hydration_requested = {"EURUSD_otc"}
            history_hydration_requested_at = {}
            history_hydration_attempts = {}
            history_hydration_sent = 0

            async def _send_event_async(self, ws, name, payload):
                events.append((name, payload))

        feed = Feed()
        asyncio.run(send_history(feed, object(), "EURUSD_otc"))
        self.assertEqual([name for name, _ in events], ["depth/follow", "history/subscribe_all", "history/load"])
        self.assertEqual(events[0][1], "EURUSD_otc")
        self.assertEqual(feed.history_hydration_attempts["EURUSD_otc"], 1)

    def test_octopus_warmup_counts_scanner_ready_bars_separately_from_continuity(self):
        now = 1_792_000_000.0
        current = int(now // 60) * 60
        candles = {
            "A_otc": {current - 60 * i: {} for i in range(1, 19)},
            "B_otc": {current - 60 * i: {} for i in list(range(1, 14)) + list(range(25, 30))},
            "C_otc": {current - 60 * i: {} for i in list(range(1, 7)) + list(range(10, 22))},
        }
        feed = SimpleNamespace(
            lock=Lock(), connected=True, authorized=True, candles=candles,
            request_m1_history=lambda symbol, count: False,
        )
        state = {"ready": 0, "recent_twelve": 0, "requested": 0, "last_error": None,
                 "startup_at": now - 600}
        ns = {
            "time_module": SimpleNamespace(time=lambda: now),
            "quotex_otc_feed": feed,
            "_octopus_history_state": state,
            "_octopus_history_restore_cache": lambda force=False: None,
            "_octopus_history_save_cache": lambda symbols: None,
            "get_otc_analysis_pair_map": lambda: {symbol: symbol for symbol in candles},
            "OCTOPUS_SR_EXEC_MIN_CLOSED_M1": 18,
        }
        maintenance = load_named("_octopus_history_maintenance", ns)
        maintenance()
        self.assertEqual((state["ready"], state["recent_twelve"], state["requested"]), (3, 2, 3))

    def test_overlapping_deployments_keep_both_sets_of_closed_candles(self):
        now = 1_792_000_000.0
        current = int(now // 60) * 60
        old = {"saved_at_epoch": now - 30, "symbols": {
            "EURUSD_otc": [{"time": current - 180, "open": 1, "high": 2, "low": 1, "close": 2}],
        }}
        incoming = {"EURUSD_otc": [
            {"time": current - 120, "open": 2, "high": 3, "low": 2, "close": 3},
            {"time": current, "open": 3, "high": 4, "low": 3, "close": 4},
        ]}
        merge = load_named("_octopus_history_merge_cache", {})
        result = merge(old, incoming, now)
        self.assertEqual(
            [row["time"] for row in result["symbols"]["EURUSD_otc"]],
            [current - 180, current - 120],
        )
        self.assertEqual(len(old["symbols"]["EURUSD_otc"]), 1)

    def test_recent_cache_can_be_merged_again_after_startup(self):
        now = 1_792_000_000.0
        cache = {"saved_at_epoch": now - 10, "symbols": {"EURUSD_otc": [
            {"time": now - 60, "open": 1, "high": 2, "low": 1, "close": 2},
        ]}}
        merged = []

        class Ref:
            def get(self):
                return cache

        state = {"restore_attempted": True, "restored_bars": 20,
                 "last_sync_at": now - 31, "last_error": None}
        ns = {
            "time_module": SimpleNamespace(time=lambda: now),
            "_octopus_history_state": state,
            "_octopus_history_cache_ref": lambda: Ref(),
            "quotex_otc_feed": SimpleNamespace(merge_extension_m1_candles=lambda symbol, rows, source: merged.append(symbol) or 1),
            "OCTOPUS_HISTORY_CACHE_MAX_AGE_SECONDS": 600,
            "OCTOPUS_HISTORY_CACHE_WRITE_SECONDS": 30,
            "logger": SimpleNamespace(info=lambda *args: None, warning=lambda *args: None),
        }
        restore = load_named("_octopus_history_restore_cache", ns)
        self.assertEqual(restore(force=True), 20)
        self.assertEqual(merged, ["EURUSD_otc"])

    def test_checkpoint_refresh_keeps_candles_already_built_from_live_ticks(self):
        now = 1_792_000_000.0
        bucket = int(now // 60) * 60 - 60
        live = {"open": 1.0, "high": 1.4, "low": 0.9, "close": 1.3, "source": "quotes/stream"}
        ns = {"time_module": SimpleNamespace(time=lambda: now), "math": math, "now_iso": lambda: "now"}
        merge = load_named("merge_extension_m1_candles", ns, from_class="QuotexOTCLiveFeed")

        class Feed:
            lock = Lock()
            candles = {"EURUSD_otc": {bucket: live}}
            extension_m1_history_received_at = {}
            history_hydration_received_at = {}
            history_hydration_requested = set()

            def _ensure_symbol_locked(self, symbol):
                self.candles.setdefault(symbol, {})

        feed = Feed()
        rows = [{"time": bucket, "open": 1.0, "high": 1.2, "low": 1.0, "close": 1.1},
                {"time": bucket - 60, "open": 0.8, "high": 1.0, "low": 0.8, "close": 1.0}]
        merge(feed, "EURUSD_otc", rows, source="octopus_checkpoint")
        self.assertIs(feed.candles["EURUSD_otc"][bucket], live)
        self.assertEqual(feed.candles["EURUSD_otc"][bucket - 60]["source"], "octopus_checkpoint")

    def test_tick_history_rebuild_preserves_completed_checkpoint_bar(self):
        now = 1_792_000_000.0
        bucket = int(now // 60) * 60 - 60
        ns = {"time_module": SimpleNamespace(time=lambda: now)}
        rebuild = load_named("_rebuild_candles_from_prices_locked", ns, from_class="QuotexOTCLiveFeed")
        complete = {"open": 1.0, "high": 1.2, "low": 0.9, "close": 1.1, "source": "octopus_checkpoint"}
        feed = SimpleNamespace(prices={"EURUSD_otc": deque([(bucket + 30, 1.05, 0)])}, candles={"EURUSD_otc": {bucket: complete}})
        rebuild(feed, "EURUSD_otc")
        self.assertEqual(feed.candles["EURUSD_otc"][bucket], complete)

    def test_historical_bars_cannot_replace_a_live_quote(self):
        now = 1_792_000_000.0
        ns = {"time_module": SimpleNamespace(time=lambda: now)}
        fresh = load_named("_octopus_has_fresh_tick", ns)
        self.assertTrue(fresh({"time": now - 2}))
        self.assertFalse(fresh({"time": now - 120}))
        self.assertFalse(fresh({}))

    def test_history_request_requires_recent_consecutive_closed_candles(self):
        now = 1_792_000_000.0
        current = int(now // 60) * 60
        ns = {"time_module": SimpleNamespace(time=lambda: now)}
        check = load_named("_has_recent_closed_m1_locked", ns, from_class="QuotexOTCLiveFeed")
        request = load_named("request_m1_history", ns, from_class="QuotexOTCLiveFeed")

        class Feed:
            lock = Lock()
            candles = {"EURUSD_otc": {current - 60 * i: {} for i in range(1, 19)}}
            history_hydration_requested = set()
            history_hydration_attempts = {}
            history_hydration_sent = 0
            history_payload_seen = 0
            _has_recent_closed_m1_locked = check

            def add_symbol(self, symbol):
                self.candles.setdefault(symbol, {})

        feed = Feed()
        self.assertTrue(request(feed, "EURUSD_otc", 18))
        feed.candles["EURUSD_otc"].pop(current - 60 * 8)
        self.assertFalse(request(feed, "EURUSD_otc", 18))
        self.assertIn("EURUSD_otc", feed.history_hydration_requested)
        # Plenty of older bars must not hide a gap near the current candle.
        feed.candles["EURUSD_otc"].update({current - 60 * i: {} for i in range(25, 55)})
        self.assertFalse(request(feed, "EURUSD_otc", 18))
        # An unresponsive history endpoint must not be retried forever.
        feed.history_hydration_attempts["EURUSD_otc"] = 2
        self.assertFalse(request(feed, "EURUSD_otc", 18))
        self.assertNotIn("EURUSD_otc", feed.history_hydration_requested)
        feed.history_hydration_attempts.clear()
        feed.history_hydration_sent = 16
        self.assertFalse(request(feed, "EURUSD_otc", 18))
        self.assertNotIn("EURUSD_otc", feed.history_hydration_requested)

    def test_fresh_checkpoint_restores_bars_but_old_checkpoint_is_ignored(self):
        now = 1_792_000_000.0
        restored = []
        raw = {"saved_at_epoch": now - 120, "symbols": {"EURUSD_otc": [{"time": now - 60, "open": 1, "high": 2, "low": 1, "close": 2}]}}

        class Ref:
            def get(self):
                return raw

        ns = {
            "time_module": SimpleNamespace(time=lambda: now),
            "_octopus_history_state": {"restore_attempted": False, "restored_bars": 0, "last_error": None},
            "_octopus_history_cache_ref": lambda: Ref(),
            "quotex_otc_feed": SimpleNamespace(merge_extension_m1_candles=lambda symbol, rows, source: restored.append((symbol, source)) or len(rows)),
            "OCTOPUS_HISTORY_CACHE_MAX_AGE_SECONDS": 600,
            "logger": SimpleNamespace(info=lambda *args: None, warning=lambda *args: None),
        }
        restore = load_named("_octopus_history_restore_cache", ns)
        self.assertEqual(restore(), 1)
        self.assertEqual(restored, [("EURUSD_otc", "octopus_checkpoint")])
        raw["saved_at_epoch"] = now - 900
        ns["_octopus_history_state"].update(restore_attempted=False, restored_bars=0)
        self.assertEqual(restore(), 0)
        self.assertEqual(len(restored), 1)


if __name__ == "__main__":
    unittest.main()
