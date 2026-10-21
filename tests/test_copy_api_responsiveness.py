"""Offline checks for the Firebase latency boundary in Copy/Mobile API."""
import ast
import asyncio
import copy
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import time
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "main.py"
TREE = ast.parse(SOURCE.read_text(encoding="utf-8"))


def load_function(name, namespace, *, parent=None):
    nodes = TREE.body if parent is None else next(
        node.body for node in TREE.body if isinstance(node, ast.FunctionDef) and node.name == parent
    )
    selected = next(node for node in nodes if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name)
    selected = copy.deepcopy(selected)
    selected.decorator_list = []
    exec(compile(ast.Module(body=[selected], type_ignores=[]), str(SOURCE), "exec"), namespace)
    return namespace[name]


class ApiError(Exception):
    def __init__(self, status_code, detail):
        self.status_code = status_code
        self.detail = detail


class MobileAuthCacheTests(unittest.TestCase):
    def setUp(self):
        self.now = 1000.0
        self.sessions = {"session": {
            "token": "", "device_id": "phone", "telegram_user_id": "456",
            "device_proof_key": "proof", "expires_at": 99999,
        }}
        self.calls = 0

        def validate(*args, **kwargs):
            self.calls += 1
            return False, "bot subscription inactive", {}

        self.ns = {
            "_copy_ws_sessions": self.sessions,
            "COPY_CLIENT_AUTH_RECHECK_SECONDS": 10,
            "COPY_VIP_LEADER_AUTH_CACHE_SECONDS": 35,
            "VIP_TEAM_LEADER_TELEGRAM_IDS": {123, 6656949901},
            "time_module": SimpleNamespace(time=lambda: self.now),
            "parse_iso": datetime.fromisoformat,
            "timezone": timezone,
            "HTTPException": ApiError,
            "_copy_session_token_verify": lambda token: (True, "ok", dict(self.sessions[token])),
            "normalize_copy_license_token": lambda value: str(value or ""),
            "normalize_copy_telegram_user_id": lambda value: str(value or ""),
            "copy_validate_telegram_subscription_device": validate,
            "COPY_SESSION_TOKEN_TTL_SECONDS": 43200,
        }
        self.ns["_copy_mobile_auth_cache_store"] = load_function("_copy_mobile_auth_cache_store", self.ns)
        self.auth = load_function("_mobile_bearer_session", self.ns, parent="create_embedded_copy_api")

    def test_reuses_recent_validation_then_rechecks_revocation(self):
        self.ns["_copy_mobile_auth_cache_store"]("session", {"expires_at": "forever", "plan": "active"})
        self.assertEqual(self.auth("Bearer session")["license_record"]["plan"], "active")
        self.assertEqual(self.calls, 0)
        self.now += 11
        with self.assertRaises(ApiError) as raised:
            self.auth("Bearer session")
        self.assertEqual(raised.exception.status_code, 401)
        self.assertEqual(self.calls, 1)
        self.assertNotIn("mobile_auth_record", self.sessions["session"])

    def test_cache_never_outlives_subscription_expiry(self):
        expiry = datetime.fromtimestamp(self.now + 2, timezone.utc).isoformat()
        self.ns["_copy_mobile_auth_cache_store"]("session", {"expires_at": expiry})
        self.assertLessEqual(self.sessions["session"]["mobile_auth_valid_until"], self.now + 2)
        self.now += 3
        with self.assertRaises(ApiError):
            self.auth("Bearer session")
        self.assertEqual(self.calls, 1)

    def test_vip_leader_cache_covers_mobile_account_heartbeat(self):
        self.sessions["session"]["telegram_user_id"] = "123"
        self.ns["_copy_mobile_auth_cache_store"]("session", {"expires_at": "forever", "plan": "admin"})
        def validate(*args, **kwargs):
            self.calls += 1
            return (True, "ok", {"expires_at": "forever", "plan": "admin"}) if self.calls == 1 else (False, "revoked", {})
        self.ns["copy_validate_telegram_subscription_device"] = validate
        self.now += 30
        self.assertEqual(self.auth("Bearer session", refresh_before_seconds=15)["license_record"]["plan"], "admin")
        self.assertEqual(self.calls, 1)
        self.now += 14
        self.assertEqual(self.auth("Bearer session")["license_record"]["plan"], "admin")
        self.assertEqual(self.calls, 1)
        self.now += 22
        with self.assertRaises(ApiError):
            self.auth("Bearer session")
        self.assertEqual(self.calls, 2)


class WebSocketDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_subscription_read_does_not_block_event_loop(self):
        clients = {"follower": {"telegram_user_id": "456", "auth_mode": "telegram", "ws": object()}}

        def slow_validate(user_id):
            time.sleep(0.15)
            return True, "ok", {}

        async def send(ws, payload):
            return True

        ns = {
            "asyncio": asyncio,
            "time_module": time,
            "COPY_CLIENT_AUTH_RECHECK_SECONDS": 10,
            "_copy_clients": clients,
            "is_copy_global_enabled": lambda: True,
            "_copy_signal_delivery_allowed": lambda *args, **kwargs: True,
            "normalize_copy_telegram_user_id": lambda value: str(value or ""),
            "normalize_copy_source": str,
            "_copy_vip_leader_identity_ok": lambda value: False,
            "copy_bot_subscription_record": slow_validate,
            "_copy_send_json_safe": send,
            "now_iso": lambda: "now",
        }
        broadcast = load_function("_copy_broadcast_signal", ns)
        pending = asyncio.create_task(broadcast({"source": "vip_copy", "id": "vip:test"}))
        await asyncio.sleep(0.02)
        self.assertFalse(pending.done(), "Firebase read should be pending in a worker")
        result = await pending
        self.assertEqual(result["delivered"], 1)
        self.assertGreater(clients["follower"]["last_auth_ok_at"], 0)

    async def test_admin_overview_returns_cached_result_after_slow_refresh(self):
        def slow_stats():
            time.sleep(0.15)
            return {"copy_enabled": True, "bot_enabled": True, "total_users": 8}

        short_wait = SimpleNamespace(
            to_thread=asyncio.to_thread, gather=asyncio.gather,
            create_task=asyncio.create_task, shield=asyncio.shield,
            wait_for=lambda task, timeout: asyncio.wait_for(task, timeout=0.03),
            TimeoutError=asyncio.TimeoutError,
        )
        ns = {
            "asyncio": short_wait,
            "Header": lambda default=None: default,
            "HTTPException": ApiError,
            "time_module": time,
            "_mobile_admin_overview_state": {"payload": None, "updated_at": 0.0, "task": None, "generation": 0},
            "_mobile_bearer_session": lambda authorization: {"telegram_user_id": "owner"},
            "_mobile_require_admin": lambda session: None,
            "_mobile_bot_stats_payload": slow_stats,
            "_three_candle_get_settings": lambda: {"enabled": True, "daily_limit": 4},
            "_three_candle_public_get_settings": lambda: {"enabled": False, "session_limit": 0},
            "_three_candle_channel_id": lambda: -1001,
            "_three_candle_public_channel_id": lambda: -1002,
            "_copy_clients": {},
            "logger": SimpleNamespace(warning=lambda *args: None),
        }
        overview = load_function("mobile_admin_overview", ns, parent="create_embedded_copy_api")
        with self.assertRaises(ApiError) as raised:
            await overview("Bearer test")
        self.assertEqual(raised.exception.status_code, 503)
        await asyncio.sleep(0.2)
        cached = await overview("Bearer test")
        self.assertEqual(cached["stats"]["total_users"], 8)
        self.assertTrue(cached["sections"]["copy"]["enabled"])


if __name__ == "__main__":
    unittest.main()
