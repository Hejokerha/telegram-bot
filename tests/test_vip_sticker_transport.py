"""VIP sticker delivery must not wait for the Telegram polling loop."""
import ast
import asyncio
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "main.py"


class VipStickerTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_vip_sticker_uses_independent_bot_api_request(self):
        tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
        selected = [node for node in tree.body if isinstance(node, ast.AsyncFunctionDef)
                    and node.name == "safe_send_sticker"]
        self.assertEqual(len(selected), 1)
        sent = []

        def post(url, *, data, files, timeout):
            sent.append((url, data, files["sticker"][0], timeout))
            return SimpleNamespace(status_code=200, json=lambda: {"ok": True, "result": {"message_id": 7}})

        namespace = {
            "asyncio": asyncio,
            "os": os,
            "requests": SimpleNamespace(post=post),
            "VIP_CHANNEL_CHAT_ID": -1003588701661,
            "logger": SimpleNamespace(warning=lambda *args: None),
        }
        exec(compile(ast.Module(body=selected, type_ignores=[]), str(SOURCE), "exec"), namespace)
        with tempfile.TemporaryDirectory() as folder:
            sticker = Path(folder) / "call.webp"
            sticker.write_bytes(b"fake-sticker")
            result = await namespace["safe_send_sticker"](
                SimpleNamespace(token="test-token"),
                chat_id=-1003588701661, sticker_path=str(sticker),
            )
        self.assertEqual(result["message_id"], 7)
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][1], {"chat_id": -1003588701661})
        self.assertEqual(sent[0][2], "call.webp")


if __name__ == "__main__":
    unittest.main()
