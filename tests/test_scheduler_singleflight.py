"""Fast scheduler ticks must not start overlapping strategy scans."""
import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
import time
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "main.py"


def load_singleflight(namespace):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    node = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                and node.name == "_scheduler_singleflight")
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(SOURCE), "exec"), namespace)
    return namespace[node.name]


class SchedulerSingleflightTests(unittest.IsolatedAsyncioTestCase):
    async def test_coalesces_busy_ticks_then_runs_fresh_scan(self):
        started = asyncio.Event()
        release = asyncio.Event()
        counts = {"running": 0, "max_running": 0, "calls": 0}
        logs = []

        async def scan(_context):
            counts["running"] += 1
            counts["calls"] += 1
            counts["max_running"] = max(counts["max_running"], counts["running"])
            started.set()
            await release.wait()
            counts["running"] -= 1

        namespace = {
            "asyncio": asyncio, "ContextTypes": SimpleNamespace(DEFAULT_TYPE=object),
            "time_module": SimpleNamespace(monotonic=time.monotonic),
            "logger": SimpleNamespace(info=lambda *args: logs.append(args)),
            "_scheduler_background_task_done": lambda task, label: task.result(),
        }
        tick = load_singleflight(namespace)(scan, "test-scan")
        await tick(None)
        await started.wait()
        for _ in range(20):
            await tick(None)
        self.assertEqual(counts["calls"], 1)
        self.assertEqual(counts["max_running"], 1)
        release.set()
        await asyncio.sleep(0.02)
        await tick(None)
        await asyncio.sleep(0)
        self.assertEqual(counts["calls"], 2)
        self.assertEqual(counts["max_running"], 1)


if __name__ == "__main__":
    unittest.main()
