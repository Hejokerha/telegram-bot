"""Offline checks for Octopus historical M1 startup readiness."""
import ast
from collections import deque
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
