"""Exercise the Render hooks without importing the trading backend."""
import ast
import asyncio
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch


class RenderSupportTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        source = Path(__file__).resolve().parents[1] / "main.py"
        names = {"_start_support_bot_background_once", "_render_support_shutdown"}
        nodes = [node for node in ast.parse(source.read_text(encoding="utf-8")).body
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
        assert len(nodes) == len(names)
        for node in nodes:
            node.decorator_list = []
        cls.code = compile(ast.Module(body=nodes, type_ignores=[]), str(source), "exec")

    async def asyncSetUp(self):
        self.ns = {"asyncio": asyncio, "os": os, "__file__": str(Path(__file__).resolve().parents[1] / "main.py"),
                   "load_dotenv": Mock(), "logger": SimpleNamespace(error=Mock()),
                   "ADMIN_TELEGRAM_ID": 999, "_support_bot_task": None, "_support_bot_service": None}
        exec(self.code, self.ns)

    async def test_disabled_support_has_no_task_or_client(self):
        with patch.dict(os.environ, {"SUPPORT_BOT_ENABLED": "false"}):
            self.ns["_start_support_bot_background_once"]()
        self.assertIsNone(self.ns["_support_bot_task"])
        self.assertIsNone(self.ns["_support_bot_service"])

    async def test_support_failure_is_contained_without_logging_secret(self):
        service = SimpleNamespace(launch=AsyncMock(side_effect=RuntimeError("secret-credential")))
        with patch.dict(os.environ, {"SUPPORT_BOT_ENABLED": "true"}), \
             patch("support_bot.SupportConfig.from_env", return_value=SimpleNamespace(store_backend="sqlite")), \
             patch("support_bot.SupportService", return_value=service):
            self.ns["_start_support_bot_background_once"]()
            await self.ns["_support_bot_task"]
        self.assertIsNone(self.ns["_support_bot_service"])
        self.assertNotIn("secret", str(self.ns["logger"].error.call_args))

    async def test_start_once_and_shutdown_stop_independent_service(self):
        service = SimpleNamespace(launch=AsyncMock(), stop=AsyncMock())
        with patch.dict(os.environ, {"SUPPORT_BOT_ENABLED": "true"}), \
             patch("support_bot.SupportConfig.from_env", return_value=SimpleNamespace(store_backend="sqlite")), \
             patch("support_bot.SupportService", return_value=service) as factory:
            self.ns["_start_support_bot_background_once"]()
            self.ns["_start_support_bot_background_once"]()
            await self.ns["_support_bot_task"]
            factory.assert_called_once()
        await self.ns["_render_support_shutdown"]()
        service.stop.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
