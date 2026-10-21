"""The two-second channel scheduler must never wait for partner Firebase reads."""
import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
import time
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "main.py"


def load_targets(namespace):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    node = next(
        node for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_three_candle_scheduler_targets"
    )
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(SOURCE), "exec"), namespace)
    return namespace[node.name]


class ThreeCandleSchedulerResponsivenessTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_partner_read_keeps_tick_responsive_and_cache_bounded(self):
        clock = [1000.0]
        calls = []
        state = {
            "owner_enabled": False, "owner_limit_reached": False,
            "owner_updated_at": 0.0, "owner_task": None,
            "partners": [], "partners_updated_at": 0.0, "partners_task": None,
        }
        settings = {"settings_cache": {"enabled": True}}

        def partners():
            calls.append(1)
            time.sleep(0.15)
            return [{"partner_code": "one", "exploit_channel_id": -1001}]

        namespace = {
            "asyncio": asyncio,
            "time_module": SimpleNamespace(time=lambda: clock[0]),
            "_three_candle_scheduler_target_cache": state,
            "_three_candle_channel_state": settings,
            "_three_candle_is_enabled": lambda: True,
            "_three_candle_daily_limit_reached": lambda: False,
            "eligible_partner_channels": partners,
            "logger": SimpleNamespace(warning=lambda *args: None),
        }
        targets = load_targets(namespace)
        started = time.monotonic()
        self.assertEqual(await targets(), (False, False, []))
        self.assertLess(time.monotonic() - started, 0.08)
        await state["owner_task"]
        self.assertFalse(state["partners_task"].done())
        self.assertEqual(await targets(), (True, False, []))
        await asyncio.sleep(0.2)
        self.assertEqual(len((await targets())[2]), 1)
        self.assertTrue((await targets())[0])
        self.assertEqual(len(calls), 1)
        settings["settings_cache"]["enabled"] = False
        self.assertFalse((await targets())[0])
        clock[0] += 16
        self.assertEqual(await targets(), (False, False, []))
        await asyncio.sleep(0.2)
        self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main()
