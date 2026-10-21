"""Slow Firebase work must not hold the half-second DS scan open."""
import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
import time
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "main.py"


def load_function(name, namespace):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    node = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(SOURCE), "exec"), namespace)
    return namespace[name]


class DsSchedulerMaintenanceTests(unittest.IsolatedAsyncioTestCase):
    async def test_octopus_startup_snapshot_does_not_hold_scan(self):
        namespace = {"asyncio": asyncio, "_octopus_restore_task": None,
                     "_scheduler_background_task_done": lambda task, label: task.result()}
        calls = []

        def restore():
            calls.append(1)
            time.sleep(0.15)

        namespace["_octopus_restore_snapshot_once"] = restore
        ready = load_function("_octopus_snapshot_ready", namespace)
        started = time.monotonic()
        self.assertFalse(ready())
        await asyncio.sleep(0)
        self.assertFalse(ready())
        self.assertLess(time.monotonic() - started, 0.08)
        await namespace["_octopus_restore_task"]
        self.assertTrue(ready())
        self.assertEqual(len(calls), 1)

    async def test_slow_settings_and_checkpoint_do_not_block_scans(self):
        clock = [1000.0]
        calls = {"settings": 0, "history": 0}
        state = {"loaded": False, "enabled": False, "last_settings_at": 0.0,
                 "history_last_prime_monotonic": 0.0}
        tasks = {"settings": None, "history": None}

        def settings(_force):
            calls["settings"] += 1
            time.sleep(0.15)
            state.update(loaded=True, enabled=True, last_settings_at=clock[0])

        def history():
            calls["history"] += 1
            time.sleep(0.15)
            state["history_last_prime_monotonic"] = clock[0]

        namespace = {
            "asyncio": asyncio, "time_module": SimpleNamespace(time=lambda: clock[0]),
            "_ds_state": state, "_ds_maintenance_tasks": tasks,
            "DS_SETTINGS_CACHE_SECONDS": 30, "DS_HISTORY_PRIME_SECONDS": 4,
            "_ds_get_settings": settings, "_ds_prime_m1_history": history,
            "_scheduler_background_task_done": lambda task, label: task.result(),
        }
        schedule = load_function("_ds_schedule_maintenance", namespace)
        started = time.monotonic()
        schedule()
        await asyncio.sleep(0)
        schedule()
        self.assertLess(time.monotonic() - started, 0.08)
        self.assertEqual(calls["settings"], 1)
        await tasks["settings"]
        schedule()
        await asyncio.sleep(0)
        schedule()
        self.assertEqual(calls["history"], 1)
        await tasks["history"]
        self.assertEqual(calls, {"settings": 1, "history": 1})

    async def test_pending_read_cannot_undo_manual_toggle(self):
        state = {"loaded": True, "enabled": True, "last_settings_at": 1000.0}

        class Ref:
            def get(self):
                time.sleep(0.1)
                return {"enabled": False}

        namespace = {
            "time_module": SimpleNamespace(time=lambda: 1031.0),
            "_ds_state": state, "DS_SETTINGS_CACHE_SECONDS": 30,
            "DS_DEFAULT_ENABLED": False, "_ds_settings_ref": lambda: Ref(),
        }
        get_settings = load_function("_ds_get_settings", namespace)
        read = asyncio.create_task(asyncio.to_thread(get_settings, False))
        await asyncio.sleep(0.03)
        state.update(enabled=True, last_settings_at=1032.0)
        self.assertEqual(await read, {"enabled": True})
        self.assertEqual(state["last_settings_at"], 1032.0)


if __name__ == "__main__":
    unittest.main()
