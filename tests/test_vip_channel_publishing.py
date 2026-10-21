"""Offline channel tests; no Telegram post or Firebase connection is made."""
import ast
import asyncio
import hashlib
from pathlib import Path
import re
from types import SimpleNamespace
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "main.py"
NAMES = {
    "_vip_channel_pair", "_vip_channel_pair_matches",
    "_vip_channel_trade_ref", "_vip_channel_publish_entry",
}


class Ref:
    def __init__(self, rows, path):
        self.rows, self.path = rows, path

    def get(self):
        return self.rows.get(self.path)

    def set(self, value):
        self.rows[self.path] = value


class VipChannelPublishingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
        selected = [node for node in tree.body
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and node.name in NAMES]
        assert len(selected) == len(NAMES)
        self.rows = {}
        self.sent = []

        async def send_sticker(bot, *, chat_id, sticker_path):
            self.sent.append((chat_id, sticker_path))
            return object()

        self.ns = {
            "re": re, "hashlib": hashlib, "asyncio": asyncio,
            "db": SimpleNamespace(reference=lambda path: Ref(self.rows, path)),
            "safe_send_sticker": send_sticker,
            "VIP_CHANNEL_CHAT_ID": -1003588701661,
            "VIP_CHANNEL_STICKER_DIR": "stickers",
            "os": __import__("os"),
            "time_module": SimpleNamespace(time=lambda: 1000),
            "now_iso": lambda: "2026-10-14T12:00:00+00:00",
            "TRADING_TIME_TELEGRAM_APP": SimpleNamespace(bot=object()),
        }
        exec(compile(ast.Module(body=selected, type_ignores=[]),
                     str(SOURCE), "exec"), self.ns)

    async def test_current_pair_matches_quotex_asset_and_reverse_alias(self):
        match = self.ns["_vip_channel_pair_matches"]
        self.assertTrue(match("USD/BRL", "USDBRL_otc"))
        self.assertTrue(match("USD/BRL", "BRLUSD_otc"))
        self.assertFalse(match("USD/BRL", "USDJPY_otc"))

    async def test_two_distinct_same_pair_trades_each_get_direction_sticker(self):
        self.rows["vip_channel/prepared/123"] = {
            "pair": "USD/BRL", "expires_epoch": 1200,
        }
        publish = self.ns["_vip_channel_publish_entry"]
        signal = {"platform_symbol": "USDBRL_otc", "direction": "CALL",
                  "expiry_timestamp": 1060, "id": "vip:one"}
        first = await publish({"event_id": "mobile_one"}, "123", signal)
        second = await publish({"event_id": "mobile_two"}, "123",
                               {**signal, "id": "vip:two", "direction": "PUT"})
        duplicate = await publish({"event_id": "mobile_one"}, "123", signal)
        self.assertEqual([first["posted"], second["posted"], duplicate["reason"]],
                         [True, True, "duplicate"])
        self.assertEqual(len(self.sent), 2)
        self.assertTrue(self.sent[0][1].endswith("call.webp"))
        self.assertTrue(self.sent[1][1].endswith("put.webp"))
