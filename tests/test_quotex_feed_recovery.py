"""Offline feed regression tests; never import/start the bot or use a network."""
import ast
import asyncio
from collections import deque
from enum import IntFlag
import json
import math
from pathlib import Path
from types import SimpleNamespace
import threading
import unittest
from unittest.mock import Mock
from urllib.parse import urlparse


SOURCE = Path(__file__).resolve().parents[1] / "main.py"


class StopLoop(BaseException):
    pass


class FakeTime:
    def __init__(self):
        self.now = 100.0
        self.sleeps = []
        self.stop_after = 1

    def monotonic(self):
        return self.now

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        if len(self.sleeps) >= self.stop_after:
            raise StopLoop()
        self.now += seconds


class Flags(IntFlag):
    TEXT = 1
    BINARY = 2


class FakeWebSocket:
    def __init__(self, clock, frames):
        self.clock = clock
        self.frames = iter(frames)
        self.sent = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def recv(self, timeout):
        self.clock.now += 1.0
        frame = next(self.frames, asyncio.TimeoutError())
        if isinstance(frame, BaseException):
            raise frame
        return frame, Flags.TEXT

    async def send_str(self, packet):
        self.sent.append(packet)


class FakeSession:
    def __init__(self, ws):
        self.ws = ws

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def ws_connect(self, *args, **kwargs):
        return self.ws


def load_feed():
    """Compile just the class AST, with explicit synthetic dependencies."""
    source = ast.parse(SOURCE.read_text(encoding="utf-8-sig"))
    feed_class = next(node for node in source.body if isinstance(node, ast.ClassDef) and node.name == "QuotexOTCLiveFeed")
    clock = FakeTime()

    async def sleep(seconds):
        clock.now += seconds

    runtime_asyncio = SimpleNamespace(
        sleep=sleep, run=asyncio.run, Lock=asyncio.Lock,
        create_task=asyncio.create_task, CancelledError=asyncio.CancelledError,
        TimeoutError=asyncio.TimeoutError,
    )
    env = {
        "deque": deque, "threading": threading, "asyncio": runtime_asyncio,
        "time_module": clock, "json": json, "math": math,
        "logger": Mock(), "now_iso": lambda: f"offline-{clock.now}",
        "urlparse": urlparse, "random": SimpleNamespace(uniform=lambda *args: 0.0),
        "should_log_quiet": lambda *args: False,
        "CurlAsyncSession": object, "CurlWsFlag": Flags, "CurlWebSocketTimeout": None,
        "QUOTEX_WS_URL": "wss://offline.invalid/socket.io/?EIO=3&transport=websocket",
        "QUOTEX_ORIGIN": "https://offline.invalid", "QUOTEX_REFERER": "https://offline.invalid/trade",
        "QUOTEX_IMPERSONATE": "chrome", "QUOTEX_CONNECT_TIMEOUT_SECONDS": 15.0,
        "QUOTEX_AUTH_TIMEOUT_SECONDS": 15.0, "QUOTEX_RECONNECT_INITIAL_SECONDS": 5.0,
        "QUOTEX_RECONNECT_MAX_SECONDS": 300.0, "QUOTEX_RECONNECT_JITTER_SECONDS": 0.0,
        "QUOTEX_BOOTSTRAP_SYMBOL": "SEED_otc", "QUOTEX_LEGACY_LIST_DELAY_SECONDS": 2.0,
        "QUOTEX_LEGACY_PROBE_TIMEOUT_SECONDS": 5.0, "QUOTEX_STREAM_STALE_SECONDS": 3.0,
    }
    exec(compile(ast.Module(body=[feed_class], type_ignores=[]), str(SOURCE), "exec"), env)
    feed = env["QuotexOTCLiveFeed"](["SEED_otc", "OTHER_otc"])
    feed._load_cookies = lambda: "offline=fixture"

    async def parked_worker(*args):
        await asyncio.Future()

    feed._legacy_keepalive_worker = parked_worker
    feed._legacy_follow_worker = parked_worker
    feed._history_hydration_worker = parked_worker
    return feed, env, clock


def quote(symbol, timestamp):
    return ('42' + json.dumps(["quotes/stream", [[symbol, timestamp, 1.2345, 0]]])).encode()


class FeedRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.feed, self.env, self.clock = load_feed()

    def connect(self, frames):
        ws = FakeWebSocket(self.clock, [b"0{}", b"40", *frames])
        self.env["CurlAsyncSession"] = lambda: FakeSession(ws)
        return ws

    def run_lifecycle(self, connection):
        self.feed._legacy_cookie_only_connection_main = connection
        with self.assertRaises(StopLoop):
            self.feed._run_forever()

    def test_server_rejection_retains_specific_failure(self):
        async def connection(_cookies):
            await self.feed._handle_frame(None, b'42["authorization/reject",{}]', True, False, True)
        self.run_lifecycle(connection)
        self.assertEqual(self.feed.last_failure_category, "authorization_rejected")
        self.assertNotIn("QUOTEX_SESSION", self.feed.last_failure_detail)

    def test_namespace_disconnect_retains_specific_failure(self):
        async def connection(_cookies):
            await self.feed._handle_frame(None, b"41", True, False, False)
        self.run_lifecycle(connection)
        self.assertEqual(self.feed.last_failure_category, "session_disconnected")

    def test_previous_attempt_does_not_hide_new_transport_error(self):
        attempts = []
        self.clock.stop_after = 2
        async def connection(_cookies):
            attempts.append(True)
            if len(attempts) == 1:
                self.feed._set_failure("socketio_namespace_timeout", "first attempt")
            raise ConnectionError("synthetic private diagnostic text")
        self.run_lifecycle(connection)
        self.assertEqual(self.feed.last_failure_category, "transport_error")
        self.assertIn("ConnectionError", self.feed.last_failure_detail)
        self.assertNotIn("private", self.feed.last_failure_detail)
        self.assertNotIn("first attempt", self.feed.last_failure_detail)

    def test_false_bootstrap_replaces_a_previous_failure(self):
        self.feed._set_failure("cloudflare_rejection", "previous")
        async def connection(_cookies):
            return False
        self.run_lifecycle(connection)
        self.assertEqual(self.feed.last_failure_category, "cookie_only_no_quotes")

    def test_response_status_and_curl_code_are_reported_safely(self):
        exc = RuntimeError("secret cookie value must never be logged")
        exc.response = SimpleNamespace(status_code=403, text="secret body")
        exc.code = 35
        async def connection(_cookies):
            raise exc
        self.run_lifecycle(connection)
        self.assertEqual(self.feed.last_failure_category, "cloudflare_rejection")
        self.assertEqual(self.feed.last_failure_detail, "RuntimeError http_status=403 curl_code=35 stage=websocket_handshake")

    def test_unstructured_exception_metadata_is_ignored(self):
        exc = RuntimeError("secret message")
        exc.status_code = "secret header"
        exc.status = True
        exc.response = SimpleNamespace(status_code="secret body")
        exc.code = "secret URL"
        self.assertEqual(self.feed._safe_exception_detail(exc, stage="secret stage"), "RuntimeError")

    def test_curl_host_resolution_failure_has_specific_diagnosis(self):
        exc = ConnectionError("private URL and cookie details")
        exc.code = 6
        async def connection(_cookies):
            raise exc
        self.run_lifecycle(connection)
        self.assertEqual(self.feed.last_failure_category, "dns_resolution_failed")
        self.assertEqual(self.feed.last_failure_detail, "ConnectionError curl_code=6 stage=websocket_handshake")

    def test_cached_seed_plus_another_symbol_cannot_pass_bootstrap(self):
        self.feed._parse_quotes_payload([["SEED_otc", 10.0, 1.2, 0]])
        ws = self.connect([quote("OTHER_otc", 20.0)])
        ready = asyncio.run(self.feed._legacy_cookie_only_connection_main("offline=fixture"))
        self.assertFalse(ready)
        self.assertFalse(self.feed.authorized)
        self.assertEqual(self.feed.last_failure_category, "cookie_only_no_quotes")
        self.assertEqual(ws.sent, ['40', '42["instruments/list",[]]', '42["instruments/follow","SEED_otc"]'])

    def test_fresh_seed_quote_passes_bootstrap(self):
        self.feed._parse_quotes_payload([["SEED_otc", 10.0, 1.2, 0]])
        self.connect([quote("OTHER_otc", 20.0), quote("SEED_otc", 21.0), RuntimeError("fixture complete")])
        with self.assertRaisesRegex(RuntimeError, "fixture complete"):
            asyncio.run(self.feed._legacy_cookie_only_connection_main("offline=fixture"))
        self.assertTrue(self.feed.connection_was_ready)
        self.assertTrue(self.feed.legacy_probe_success)
        self.assertEqual(self.feed.connection_stage, "stream")

    def test_heartbeat_frames_do_not_hide_stale_quotes(self):
        ws = self.connect([quote("SEED_otc", 20.0), b"2", b"2", b"2"])
        with self.assertRaisesRegex(RuntimeError, "quotex_quote_stream_stale"):
            asyncio.run(self.feed._legacy_cookie_only_connection_main("offline=fixture"))
        self.assertTrue(self.feed.connection_was_ready)
        self.assertEqual(self.feed.last_failure_category, "stream_stale")
        self.assertIn("no live quotes/stream", self.feed.last_failure_detail)
        self.assertEqual(ws.sent.count("3"), 3)

    def test_idle_socket_still_reconnects(self):
        self.connect([quote("SEED_otc", 20.0)])
        with self.assertRaisesRegex(RuntimeError, "quotex_stream_stale"):
            asyncio.run(self.feed._legacy_cookie_only_connection_main("offline=fixture"))
        self.assertIn("no inbound frames", self.feed.last_failure_detail)

    def test_live_quotes_keep_stream_healthy(self):
        self.connect([quote("SEED_otc", timestamp) for timestamp in range(20, 26)] + [RuntimeError("fixture complete")])
        with self.assertRaisesRegex(RuntimeError, "fixture complete"):
            asyncio.run(self.feed._legacy_cookie_only_connection_main("offline=fixture"))
        self.assertIsNone(self.feed.last_failure_category)
        self.assertEqual(self.feed._quote_generation_by_symbol["SEED_otc"], 6)

    def test_history_cannot_satisfy_live_seed_freshness(self):
        self.feed._parse_history_payload({"asset": "SEED_otc", "history": [[10.0, 1.2, 0]]})
        self.assertNotIn("SEED_otc", self.feed._quote_generation_by_symbol)
        self.assertNotIn("SEED_otc", self.feed.last_tick)
        self.assertEqual(self.feed.last_quote_monotonic, 0.0)


class EndpointConfigurationTests(unittest.TestCase):
    defaults = {
        "QUOTEX_WS_URL": "wss://ws2.quotex.com/socket.io/?EIO=3&transport=websocket",
        "QUOTEX_ORIGIN": "https://quotex.com",
        "QUOTEX_REFERER": "https://quotex.com/en/trade",
    }

    def config(self, overrides):
        source = ast.parse(SOURCE.read_text(encoding="utf-8-sig"))
        assignments = [node for node in source.body if isinstance(node, ast.Assign)
                       and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
                       and node.targets[0].id in self.defaults]
        self.assertEqual(len(assignments), 3)
        env = {"os": SimpleNamespace(getenv=lambda key, default: overrides.get(key, default))}
        exec(compile(ast.Module(body=assignments, type_ignores=[]), str(SOURCE), "exec"), env)
        return {key: env[key] for key in self.defaults}

    def test_defaults_use_verified_quotex_domain(self):
        self.assertEqual(self.config({}), self.defaults)

    def test_explicit_overrides_are_preserved(self):
        overrides = {
            "QUOTEX_WS_URL": "wss://custom.invalid/socket.io/?EIO=3&transport=websocket",
            "QUOTEX_ORIGIN": "https://custom.invalid",
            "QUOTEX_REFERER": "https://custom.invalid/trade",
        }
        self.assertEqual(self.config(overrides), overrides)

    def test_blank_overrides_fall_back_to_verified_defaults(self):
        self.assertEqual(self.config({key: "  " for key in self.defaults}), self.defaults)


if __name__ == "__main__":
    unittest.main()
