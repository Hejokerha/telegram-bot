"""Offline VIP mobile routing checks; importing the live bot is intentionally avoided."""
import ast
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import secrets
import time
import threading
from types import SimpleNamespace
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "main.py"
UTC = timezone.utc
NAMES = {
    "_copy_is_mobile_executable_signal",
    "_copy_filter_mobile_recent_signals",
    "_copy_vip_leader_identity_ok",
    "_copy_publish_vip_manual_trade",
    "_copy_broadcast_mobile_signal",
}


def load_functions(namespace):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    selected = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in NAMES]
    assert len(selected) == len(NAMES)
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(SOURCE), "exec"), namespace)


class VipMobileContractTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.sent = []
        self.now = datetime(2026, 10, 13, 12, 0, tzinfo=UTC)

        async def send_safe(ws, payload):
            self.sent.append((ws, payload))
            return True

        async def broadcast_extension(signal, exclude_client_id=None):
            self.sent.append(("extension", signal))
            return {"delivered": 1, "global_enabled": True}

        async def publish_channel(event, leader_id, signal):
            return {"posted": False, "reason": "not_prepared"}

        self.ns = {
            "ADMIN_TELEGRAM_ID": 123,
            "VIP_TEAM_LEADER_TELEGRAM_IDS": {123, 6656949901},
            "normalize_copy_telegram_user_id": lambda value: str(value or ""),
            "normalize_copy_source": lambda value: str(value or "").lower(),
            "_copy_signal_contract_kind": lambda signal: signal.get("signal_kind", "execute"),
            "is_copy_global_enabled": lambda: True,
            "is_vip_team_copy_enabled": lambda: True,
            "now_utc": lambda: self.now,
            "COPY_SIGNAL_DURATION_MIN_SECONDS": 5,
            "COPY_SIGNAL_DURATION_MAX_SECONDS": 600,
            "COPY_SOURCE_MAX_ENTRY_DELAY_SECONDS": {"vip_copy": 3},
            "COPY_SIGNAL_HISTORY_LIMIT": 100,
            "COPY_SIGNAL_DEDUPE_LIMIT": 100,
            "COPY_CLIENT_AUTH_RECHECK_SECONDS": 10,
            "_copy_signal_history": [],
            "_copy_signal_id_registry": {},
            "_copy_signal_store_lock": None,
            "_copy_server_sanitize_signal": lambda signal: dict(signal),
            "_copy_store_signal_if_allowed": lambda *args: {"accepted": True},
            "_copy_broadcast_signal": broadcast_extension,
            "_vip_channel_publish_entry": publish_channel,
            "_copy_send_json_safe": send_safe,
            "_mobile_signal_clients": {},
            "normalize_copy_license_token": lambda token: str(token or ""),
            "copy_validate_license_for_device": lambda *args, **kwargs: (True, "ok", {}),
            "copy_validate_telegram_subscription_device": lambda *args, **kwargs: (True, "ok", {}),
            "now_iso": lambda: self.now.isoformat(),
            "time_module": SimpleNamespace(time=lambda: self.now.timestamp(), monotonic=time.monotonic),
            "_copy_signal_delivery_allowed": lambda enabled, signal, mobile=False: enabled and signal.get("source") == "vip_copy",
            "re": re,
            "secrets": secrets,
            "asyncio": asyncio,
            "datetime": datetime,
            "timedelta": timedelta,
            "UTC": UTC,
            "logger": SimpleNamespace(info=lambda *args: None, warning=lambda *args: None, exception=lambda *args: None),
        }
        load_functions(self.ns)

    async def test_vip_replay_reaches_followers_only(self):
        executable = self.ns["_copy_is_mobile_executable_signal"]
        self.assertTrue(executable({"source": "vip_copy", "signal_kind": "execute"}))
        self.assertFalse(executable({"source": "otc_edge", "signal_kind": "execute"}))
        self.assertFalse(executable({"source": "vip_copy", "signal_kind": "prepare"}))
        rows = [{"id": "vip:a", "source": "vip_copy"}]
        visible = self.ns["_copy_filter_mobile_recent_signals"]
        self.assertEqual(visible(rows, True, "456"), rows)
        self.assertEqual(visible(rows, True, "123"), [])
        self.assertEqual(visible(rows, True, "6656949901"), [])
        self.assertEqual(visible(rows, False, "456"), [])

    async def test_owner_manual_trade_broadcasts_without_amount_or_account(self):
        self.ns["_mobile_signal_clients"] = {
            "owner": {"telegram_user_id": "123", "token": "owner-token", "device_id": "one", "ws": "owner-ws"},
            "second_leader": {"telegram_user_id": "6656949901", "token": "leader-token", "device_id": "three", "ws": "leader-ws"},
            "follower": {"telegram_user_id": "456", "token": "follower-token", "device_id": "two", "ws": "follower-ws"},
        }
        self.ns["_copy_signal_delivery_allowed"] = lambda enabled, signal, mobile=False: enabled
        self.ns["_copy_broadcast_mobile_signal"] = self.ns["_copy_broadcast_mobile_signal"]
        event = {
            "event_id": "mobile_12345678",
            "trade": {"asset": "AUDNZD_otc", "action": "call", "time": (self.now + timedelta(seconds=60)).timestamp(),
                      "amount": 99, "isDemo": 0},
        }
        result = await self.ns["_copy_publish_vip_manual_trade"](
            event, "", {"telegram_user_id": "123"}
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["signal_id"], "vip:mobile_12345678")
        self.assertEqual(result["mobile_delivery"]["delivered"], 1)
        self.assertEqual([ws for ws, _ in self.sent if ws.endswith("-ws")], ["follower-ws"])
        extension_signal = next(payload for ws, payload in self.sent if ws == "extension")
        self.assertNotIn("amount", extension_signal)
        self.assertNotIn("isDemo", extension_signal)
        self.assertEqual(extension_signal["creator_user_id"], 123)
        second_leader = await self.ns["_copy_publish_vip_manual_trade"](
            {**event, "event_id": "mobile_second_leader"}, "", {"telegram_user_id": "6656949901"}
        )
        self.assertTrue(second_leader["ok"])
        second_signal = [payload for ws, payload in self.sent if ws == "extension"][-1]
        self.assertEqual(second_signal["creator_user_id"], 6656949901)
        denied = await self.ns["_copy_publish_vip_manual_trade"](
            event, "", {"telegram_user_id": "456"}
        )
        self.assertEqual(denied["reason"], "leader_only")

    async def test_manual_trade_is_not_retimed_after_backend_delay(self):
        clock = [self.now.timestamp()]
        self.ns["time_module"] = SimpleNamespace(time=lambda: clock[0], monotonic=time.monotonic)

        def delayed_settings_read():
            clock[0] += 4
            return True

        self.ns["is_copy_global_enabled"] = delayed_settings_read
        result = await self.ns["_copy_publish_vip_manual_trade"](
            {"event_id": "mobile_delayed", "trade": {
                "asset": "AUDNZD_otc", "action": "call",
                "time": (self.now + timedelta(seconds=60)).timestamp(),
            }},
            "", {"telegram_user_id": "123"},
        )
        self.assertEqual(result["reason"], "entry_window_elapsed")
        self.assertEqual(self.sent, [])

    async def test_channel_post_starts_while_mobile_delivery_is_busy(self):
        mobile_started = asyncio.Event()
        release_mobile = asyncio.Event()
        channel_started = asyncio.Event()

        async def slow_mobile(_signal):
            mobile_started.set()
            await release_mobile.wait()
            return {"delivered": 1}

        async def post_channel(_event, _leader_id, _signal):
            channel_started.set()
            return {"posted": True}

        self.ns["_copy_broadcast_mobile_signal"] = slow_mobile
        self.ns["_vip_channel_publish_entry"] = post_channel
        event = {"event_id": "mobile_parallel", "trade": {
            "asset": "AUDNZD_otc", "action": "call",
            "time": (self.now + timedelta(seconds=60)).timestamp(),
        }}
        publishing = asyncio.create_task(self.ns["_copy_publish_vip_manual_trade"](
            event, "", {"telegram_user_id": "123"},
        ))
        try:
            await asyncio.wait_for(mobile_started.wait(), 1)
            await asyncio.wait_for(channel_started.wait(), 1)
        finally:
            release_mobile.set()
        result = await publishing
        self.assertTrue(result["channel_delivery"]["posted"])

    async def test_slow_mobile_auth_does_not_hold_back_another_device(self):
        slow_started = threading.Event()
        release_slow = threading.Event()

        def validate(token, *_args, **_kwargs):
            if token == "slow":
                slow_started.set()
                import time as real_time
                while not release_slow.is_set():
                    real_time.sleep(0.01)
            return True, "ok", {}

        self.ns["copy_validate_license_for_device"] = validate
        self.ns["_mobile_signal_clients"] = {
            "slow": {"telegram_user_id": "456", "token": "slow", "device_id": "one", "ws": "slow-ws"},
            "fast": {"telegram_user_id": "789", "token": "fast", "device_id": "two", "ws": "fast-ws"},
        }
        publishing = asyncio.create_task(self.ns["_copy_broadcast_mobile_signal"]({"source": "vip_copy"}))
        try:
            self.assertTrue(await asyncio.to_thread(slow_started.wait, 1))
            for _ in range(20):
                if any(ws == "fast-ws" for ws, _ in self.sent):
                    break
                await asyncio.sleep(0.01)
            self.assertTrue(any(ws == "fast-ws" for ws, _ in self.sent))
        finally:
            release_slow.set()
        result = await publishing
        self.assertEqual(result["delivered"], 2)

    async def test_extension_vip_delivery_excludes_both_leaders(self):
        tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
        selected = [node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "_copy_broadcast_signal"]
        self.assertEqual(len(selected), 1)
        clients = {
            "owner": {"telegram_user_id": "123", "auth_mode": "license", "ws": "owner-ws"},
            "second": {"telegram_user_id": "6656949901", "auth_mode": "license", "ws": "leader-ws"},
            "follower": {"telegram_user_id": "456", "auth_mode": "license", "ws": "follower-ws"},
        }
        namespace = {
            "_copy_clients": clients,
            "asyncio": asyncio,
            "is_copy_global_enabled": lambda: True,
            "_copy_signal_delivery_allowed": lambda enabled, signal, mobile=False: enabled,
            "normalize_copy_telegram_user_id": self.ns["normalize_copy_telegram_user_id"],
            "normalize_copy_source": self.ns["normalize_copy_source"],
            "_copy_vip_leader_identity_ok": self.ns["_copy_vip_leader_identity_ok"],
            "_copy_send_json_safe": self.ns["_copy_send_json_safe"],
            "now_iso": self.ns["now_iso"],
        }
        exec(compile(ast.Module(body=selected, type_ignores=[]), str(SOURCE), "exec"), namespace)
        result = await namespace["_copy_broadcast_signal"]({"source": "vip_copy", "id": "vip:one"})
        self.assertEqual(result["delivered"], 1)
        self.assertEqual([ws for ws, _ in self.sent], ["follower-ws"])


if __name__ == "__main__":
    unittest.main()
