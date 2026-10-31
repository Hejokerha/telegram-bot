"""Offline checks for the Octopus Telegram channel; no bot or Firebase import."""
import ast
import asyncio
from datetime import datetime, timedelta, timezone
import math
from pathlib import Path
from types import SimpleNamespace
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "main.py"
NAMES = {
    "_octopus_auto_channel_day_key", "_octopus_auto_channel_grade",
    "_octopus_auto_channel_signal_text", "_octopus_auto_channel_result_text",
    "_octopus_auto_channel_finish", "_octopus_auto_channel_resolve",
    "octopus_auto_channel_job",
}


class Ref:
    def __init__(self, root, path=()):
        self.root, self.path = root, path

    def child(self, key):
        return Ref(self.root, self.path + (str(key),))

    def _parent(self):
        node = self.root
        for key in self.path[:-1]:
            node = node.setdefault(key, {})
        return node

    def get(self):
        return self._parent().get(self.path[-1]) if self.path else self.root

    def set(self, value):
        self._parent()[self.path[-1]] = value

    def update(self, value):
        self._parent().setdefault(self.path[-1], {}).update(value)

    def delete(self):
        self._parent().pop(self.path[-1], None)


class OctopusAutoChannelTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.root = {}
        self.sent = []
        self.candles = {}
        self.now = 1_792_000_000.0
        self.context = SimpleNamespace(bot=object())

        async def send(bot, chat_id, text):
            self.sent.append((chat_id, text))
            return True

        self.ns = {
            "asyncio": asyncio, "datetime": datetime, "timedelta": timedelta,
            "UTC": timezone.utc, "UTC_PLUS_3": timezone(timedelta(hours=3)),
            "math": math, "ContextTypes": SimpleNamespace(DEFAULT_TYPE=object),
            "time_module": SimpleNamespace(time=lambda: self.now),
            "_octopus_auto_channel_state": {"pending_trade": None, "loaded": True, "last_error": None},
            "_octopus_mode_detector_state": {},
            "_octopus_auto_channel_ref": lambda: Ref(self.root),
            "_octopus_auto_channel_restore": lambda: True,
            "_octopus_auto_channel_settings": lambda: {"enabled": True, "daily_limit": 0, "martingale_enabled": False},
            "_octopus_snapshot_ready": lambda: True,
            "_octopus_apply_execution_direction_mode": lambda candidate: candidate,
            "_structure_edge_target_chat_id": lambda: -100123,
            "STRUCTURE_EDGE_CHANNEL_ID_RAW": "-100123",
            "_three_candle_get_candle": lambda symbol, bucket: self.candles.get((symbol, bucket), {}),
            "safe_send_message": send,
            "now_iso": lambda: "2026-10-31T00:00:00+00:00",
            "OCTOPUS_AUTO_CHANNEL_RESULT_DELAY_SECONDS": 5,
            "OCTOPUS_AUTO_CHANNEL_CANDLE_GRACE_SECONDS": 180,
            "OCTOPUS_AUTO_CHANNEL_SCAN_FIRST_SECOND": 38.0,
            "OCTOPUS_AUTO_CHANNEL_SCAN_LAST_SECOND": 44.0,
            "OCTOPUS_AUTO_CHANNEL_MIN_LEAD_SECONDS": 15.0,
            "logger": SimpleNamespace(info=lambda *args: None, exception=lambda *args: None),
        }
        tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
        selected = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in NAMES]
        self.assertEqual(len(selected), len(NAMES))
        exec(compile(ast.Module(body=selected, type_ignores=[]), str(SOURCE), "exec"), self.ns)

    def trade(self, martingale=False):
        return {"pair": "EURUSD_otc", "symbol": "EURUSD_otc", "direction": "CALL",
                "entry_bucket": 1_792_000_000 // 60 * 60 - 120, "step": 0,
                "martingale_enabled": martingale}

    async def test_direct_result_uses_exact_entry_candle(self):
        trade = self.trade()
        self.ns["_octopus_auto_channel_state"]["pending_trade"] = trade
        self.candles[(trade["symbol"], trade["entry_bucket"])] = {"open": 1.0, "close": 1.1}
        pending = await self.ns["_octopus_auto_channel_resolve"](self.context, self.now)
        self.assertFalse(pending)
        self.assertIn("Win✅.", self.sent[0][1])
        self.assertIsNone(self.ns["_octopus_auto_channel_state"]["pending_trade"])
        self.assertEqual(self.root["results"][str(trade["entry_bucket"])]["result"], "win")

    async def test_martingale_waits_for_second_candle_then_posts_one_result(self):
        trade = self.trade(martingale=True)
        self.ns["_octopus_auto_channel_state"]["pending_trade"] = trade
        self.candles[(trade["symbol"], trade["entry_bucket"])] = {"open": 1.1, "close": 1.0}
        self.assertTrue(await self.ns["_octopus_auto_channel_resolve"](self.context, trade["entry_bucket"] + 66))
        self.assertEqual(self.sent, [])
        self.assertEqual(self.root["pending_trade"]["step"], 1)
        self.candles[(trade["symbol"], trade["entry_bucket"] + 60)] = {"open": 1.0, "close": 1.1}
        self.assertFalse(await self.ns["_octopus_auto_channel_resolve"](self.context, trade["entry_bucket"] + 126))
        self.assertEqual(len(self.sent), 1)
        self.assertIn("Win✅¹", self.sent[0][1])

    async def test_loss_without_martingale_is_final_after_first_candle(self):
        trade = self.trade(martingale=False)
        self.ns["_octopus_auto_channel_state"]["pending_trade"] = trade
        self.candles[(trade["symbol"], trade["entry_bucket"])] = {"open": 1.1, "close": 1.0}
        self.assertFalse(await self.ns["_octopus_auto_channel_resolve"](self.context, trade["entry_bucket"] + 66))
        self.assertEqual(len(self.sent), 1)
        self.assertIn("Lose💔", self.sent[0][1])

    async def test_missing_candle_is_reported_unconfirmed_after_grace(self):
        trade = self.trade()
        self.ns["_octopus_auto_channel_state"]["pending_trade"] = trade
        self.assertTrue(await self.ns["_octopus_auto_channel_resolve"](self.context, trade["entry_bucket"] + 70))
        self.assertEqual(self.sent, [])
        self.assertFalse(await self.ns["_octopus_auto_channel_resolve"](self.context, trade["entry_bucket"] + 241))
        self.assertIn("تعذر تأكيد النتيجة", self.sent[0][1])

    async def test_channel_posts_early_without_execution_client(self):
        target = int(self.now // 60) * 60 + 60
        self.ns["_octopus_sr_prearm_watch_scan"] = lambda bucket: {
            "ready": 22, "ranked": [{"pair": "EURUSD_otc", "symbol": "EURUSD_otc", "direction": "PUT", "payout": 91}],
        }
        self.ns["_octopus_auto_channel_resolve"] = lambda *args: asyncio.sleep(0, result=False)
        await self.ns["octopus_auto_channel_job"](self.context)
        self.assertEqual(len(self.sent), 1)
        self.assertIn("الشمعة القادمة", self.sent[0][1])
        self.assertEqual(self.root["pending_trade"]["entry_bucket"], target)
        self.assertGreaterEqual(self.ns["_octopus_auto_channel_state"]["last_sent_lead_seconds"], 15)

    async def test_channel_skips_signal_if_early_window_is_missed(self):
        self.now += 10  # second 50: fewer than ten seconds before entry
        calls = []
        self.ns["_octopus_sr_prearm_watch_scan"] = lambda bucket: calls.append(bucket)
        self.ns["_octopus_auto_channel_resolve"] = lambda *args: asyncio.sleep(0, result=False)
        await self.ns["octopus_auto_channel_job"](self.context)
        self.assertEqual(calls, [])
        self.assertEqual(self.sent, [])

    def test_invalid_candle_never_becomes_a_loss(self):
        grade = self.ns["_octopus_auto_channel_grade"]
        self.assertEqual(grade("CALL", {}), "unknown")
        self.assertEqual(grade("CALL", {"open": 1.0, "close": 1.0}), "draw")


if __name__ == "__main__":
    unittest.main()
