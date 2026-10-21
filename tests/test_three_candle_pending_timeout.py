"""Offline regression checks for a missing Quotex candle blocking the channel."""
import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "main.py"
NAMES = {
    "_three_candle_pending_deadline",
    "_three_candle_process_pending_trade",
    "three_candle_channel_job",
}


def load_functions(namespace):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    selected = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in NAMES]
    assert len(selected) == len(NAMES)
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(SOURCE), "exec"), namespace)


class ThreeCandlePendingTimeoutTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.now = 1_200
        self.result_calls = []
        self.scans = 0
        self.state = {"pending_trade": None, "pending_timeouts": 0}

        def candle(_symbol, bucket):
            return self.candles.get(bucket, {})

        async def record_result(*args, **kwargs):
            self.result_calls.append((args, kwargs))

        self.candles = {}
        async def scheduler_targets():
            return True, False, []

        self.ns = {
            "asyncio": asyncio,
            "ContextTypes": SimpleNamespace(DEFAULT_TYPE=object),
            "time_module": SimpleNamespace(time=lambda: self.now),
            "THREE_CANDLE_RESULT_DELAY_SECONDS": 5,
            "THREE_CANDLE_PENDING_CANDLE_GRACE_SECONDS": 180,
            "_three_candle_channel_state": self.state,
            "_three_candle_channel_id": lambda: "owner-channel",
            "_three_candle_get_candle": candle,
            "_three_candle_result_for_direction": lambda *args: "win",
            "_three_candle_result_message": lambda *args, **kwargs: "result",
            "_three_candle_publish_result_to_partners": record_result,
            "_three_candle_register_final_result": lambda *args: self.result_calls.append(args),
            "_three_candle_record_result": lambda *args, **kwargs: self.result_calls.append((args, kwargs)),
            "_three_candle_public_channel_id": lambda: None,
            "safe_send_message": record_result,
            "now_iso": lambda: "test-time",
            "logger": SimpleNamespace(warning=lambda *args: None, exception=lambda *args: None),
            "_three_candle_any_target_enabled": lambda: True,
            "_three_candle_scheduler_targets": scheduler_targets,
            "_three_candle_is_enabled": lambda: True,
            "_three_candle_daily_limit_reached": lambda: False,
            "eligible_partner_channels": lambda: [],
            "_three_candle_collect_candidates": self._collect_candidates,
        }
        load_functions(self.ns)

    def _collect_candidates(self):
        self.scans += 1
        return []

    def _trade(self, **changes):
        return {
            "pair": "USD/PHP",
            "symbol": "USDPHP_otc",
            "direction": "CALL",
            "current_bucket": 900,
            "entry_bucket": 960,
            "step": 0,
            "owner_three_candle_published": True,
            **changes,
        }

    async def test_missing_confirmation_waits_then_releases_without_false_result(self):
        self.state["pending_trade"] = self._trade()
        self.assertTrue(await self.ns["_three_candle_process_pending_trade"](SimpleNamespace(bot=object())))
        self.assertEqual(self.scans, 0)
        self.now = self.ns["_three_candle_pending_deadline"](self.state["pending_trade"])
        await self.ns["three_candle_channel_job"](SimpleNamespace(bot=object()))
        self.assertIsNone(self.state["pending_trade"])
        self.assertEqual(self.state["pending_timeouts"], 1)
        self.assertEqual(self.scans, 1)
        self.assertEqual(self.result_calls, [])

    async def test_missing_martingale_candle_releases_without_recording_loss(self):
        self.state["pending_trade"] = self._trade(third_candle_confirmed=True, step=1, martingale_bucket=1020)
        self.now = 1_210
        self.assertTrue(await self.ns["_three_candle_process_pending_trade"](SimpleNamespace(bot=object())))
        self.now = self.ns["_three_candle_pending_deadline"](self.state["pending_trade"])
        self.assertFalse(await self.ns["_three_candle_process_pending_trade"](SimpleNamespace(bot=object())))
        self.assertIsNone(self.state["pending_trade"])
        self.assertEqual(self.result_calls, [])

    async def test_present_candle_still_produces_normal_result(self):
        self.state["pending_trade"] = self._trade(third_candle_confirmed=True)
        self.candles[960] = {"open": 1.0, "close": 1.1}
        self.now = 1_210
        self.assertFalse(await self.ns["_three_candle_process_pending_trade"](SimpleNamespace(bot=object())))
        self.assertIsNone(self.state["pending_trade"])
        self.assertEqual(self.state["pending_timeouts"], 0)
        self.assertTrue(self.result_calls)

    async def test_invalid_pending_bucket_cannot_block_scanning(self):
        self.state["pending_trade"] = self._trade(current_bucket=0, entry_bucket=0)
        self.assertFalse(await self.ns["_three_candle_process_pending_trade"](SimpleNamespace(bot=object())))
        self.assertIsNone(self.state["pending_trade"])


if __name__ == "__main__":
    unittest.main()
