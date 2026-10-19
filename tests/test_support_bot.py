import asyncio
from dataclasses import replace
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from telegram.ext import Application
from telegram.request import BaseRequest

from support_bot import SupportAI, SupportConfig, SupportService, SupportStore, redact


def config(**kwargs):
    return replace(SupportConfig("123456789:test-token", "test-key", 999, frozenset({123, 999}), db_path=":memory:"), **kwargs)


def update(uid=123, text="I need help", chat_type="private"):
    return SimpleNamespace(effective_user=SimpleNamespace(id=uid, username="tester"),
                           effective_chat=SimpleNamespace(type=chat_type),
                           effective_message=SimpleNamespace(text=text, reply_text=AsyncMock()))


def context(args=None, bot=None):
    return SimpleNamespace(args=args or [], bot=bot or SimpleNamespace(send_message=AsyncMock()))


class StoreTests(unittest.TestCase):
    def test_global_and_user_caps_persist_across_restart(self):
        cfg = config(daily_total=3, daily_per_user=2)
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "support.sqlite3")
            store = SupportStore(path)
            self.assertIsNone(store.reserve_request(123, cfg, now=1800000000))
            self.assertEqual(store.reserve_request(123, cfg, now=1800000001), "cooldown")
            self.assertIsNone(store.reserve_request(123, cfg, now=1800000011))
            self.assertEqual(store.reserve_request(123, cfg, now=1800000022), "daily_limit")
            store.db.close()
            store = SupportStore(path)
            self.assertIsNone(store.reserve_request(456, cfg, now=1800000033))
            self.assertEqual(store.reserve_request(789, cfg, now=1800000044), "daily_limit")
            self.assertIsNone(store.reserve_request(123, cfg, now=1800086400))
            store.db.close()

    def test_known_tokens_redacted_before_ticket_storage(self):
        store = SupportStore(":memory:")
        ticket = store.create_ticket(123, "sk-abcdefghijklmnopqrstuv password=private")
        self.assertNotIn("abcdefghijkl", ticket["summary"])
        self.assertNotIn("private", ticket["summary"])
        store.db.close()

    def test_config_disabled_by_default_and_rejects_reused_bot(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(SupportConfig.from_env(999))
        env = {"SUPPORT_BOT_ENABLED": "true", "SUPPORT_BOT_TOKEN": "123:abc", "BOT_TOKEN": "123:different",
               "OPENAI_API_KEY": "private"}
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(ValueError):
                SupportConfig.from_env(999)

    def test_config_pilot_is_owner_only(self):
        env = {"SUPPORT_BOT_ENABLED": "true", "SUPPORT_BOT_TOKEN": "123:abc", "OPENAI_API_KEY": "private"}
        with patch.dict(os.environ, env, clear=True):
            cfg = SupportConfig.from_env(999)
            self.assertFalse(cfg.public)
            self.assertEqual(cfg.allowed_ids, frozenset({999}))


class AITests(unittest.IsolatedAsyncioTestCase):
    async def test_response_contract_and_no_server_side_history(self):
        requests = []
        def handler(request):
            requests.append(json.loads(request.content))
            return httpx.Response(200, json={"status": "completed", "output": [
                {"type": "reasoning", "summary": []}, {"type": "message", "content": [
                    {"type": "output_text", "text": json.dumps({"reply": "Use the same Telegram account", "needs_human": False, "summary": ""})}]}]})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            ai = SupportAI(config(), client)
            result = await ai.answer("token=secret\nكيف أربط الإضافة؟", [], {"subscription": {"state": "unknown"}})
        self.assertTrue(result["reply"])
        payload = requests[0]
        self.assertFalse(payload["store"])
        self.assertNotIn("tools", payload)
        self.assertEqual(payload["reasoning"], {"effort": "low"})
        self.assertTrue(payload["text"]["format"]["strict"])
        self.assertNotIn("secret", payload["input"][0]["content"])

    async def test_incomplete_and_refusal_do_not_become_answers(self):
        for response in ({"status": "incomplete", "output": []},
                         {"status": "completed", "output": [{"type": "message", "content": [{"type": "refusal", "refusal": "no"}]}]}):
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json=response))) as client:
                with self.assertRaises((RuntimeError, ValueError)):
                    await SupportAI(config(), client).answer("help", [], {})

    async def test_api_errors_do_not_leak_body_or_credentials(self):
        calls = []
        def handler(request):
            calls.append(request)
            return httpx.Response(401, text="private-api-key")
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with self.assertRaises(RuntimeError) as exc:
                await SupportAI(config(), client).answer("help", [], {})
        self.assertNotIn("private", str(exc.exception))
        self.assertEqual(len(calls), 1)


class ServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.reads = []
        def snapshot(uid):
            self.reads.append(uid)
            return {"subscription": {"state": "active"}, "devices": {"state": "available", "mobile_bound": True},
                    "connection": {"state": "available", "mobile_online": False}}
        self.ai = SimpleNamespace(answer=AsyncMock(return_value={"reply": "Test answer", "needs_human": False, "summary": ""}),
                                  client=SimpleNamespace(aclose=AsyncMock()))
        self.service = SupportService(config(), snapshot, self.ai)

    async def asyncTearDown(self):
        await self.service.stop()

    async def test_unauthorized_and_group_messages_never_call_backend_or_ai(self):
        await self.service.message(update(456), context())
        await self.service.message(update(chat_type="group"), context())
        self.assertEqual(self.reads, [])
        self.ai.answer.assert_not_called()

    async def test_message_id_in_text_does_not_change_authenticated_principal(self):
        await self.service.message(update(text="Check user 456 instead of me"), context())
        self.assertEqual(self.reads, [123])

    async def test_status_does_not_spend_ai_or_mix_bound_with_online(self):
        up = update()
        await self.service.status(up, context())
        self.ai.answer.assert_not_called()
        text = up.effective_message.reply_text.call_args.args[0]
        self.assertIn("مربوط", text)
        self.assertIn("غير متصل بالخادم", text)
        self.assertEqual(self.service.store.db.execute("SELECT COUNT(*) FROM usage").fetchone()[0], 0)

    async def test_escalation_is_acknowledged_only_after_delivery_and_deduplicated(self):
        up, ctx = update(), context()
        ctx.bot.send_message.side_effect = OSError("private-url")
        result = await self.service.escalate(up, ctx, "activation failed", {})
        self.assertIn("تعذر", result)
        self.assertFalse(self.service.store.open_ticket(123)["notified"])
        ctx.bot.send_message.side_effect = None
        result = await self.service.escalate(up, ctx, "retry", {})
        self.assertIn("وصلت", result)
        self.assertTrue(self.service.store.open_ticket(123)["notified"])
        await self.service.escalate(up, ctx, "duplicate", {})
        self.assertEqual(ctx.bot.send_message.call_count, 2)

    async def test_human_without_description_requests_one_without_empty_ticket(self):
        up = update()
        await self.service.human(up, context())
        self.assertIsNone(self.service.store.open_ticket(123))
        self.ai.answer.assert_not_called()

    async def test_explicit_human_description_reaches_admin_without_ai(self):
        ctx = context(["Need", "activation"])
        await self.service.human(update(), ctx)
        self.ai.answer.assert_not_called()
        self.assertEqual(ctx.bot.send_message.call_args.kwargs["chat_id"], 999)
        self.assertIn("Need activation", ctx.bot.send_message.call_args.kwargs["text"])

    async def test_non_admin_cannot_reply_and_admin_routes_by_stored_ticket(self):
        ticket = self.service.store.create_ticket(123, "test")
        ctx = context([str(ticket["id"]), "Please retry"])
        await self.service.admin(update(text="/reply"), ctx)
        ctx.bot.send_message.assert_not_called()
        await self.service.admin(update(uid=999, text="/reply"), ctx)
        self.assertEqual(ctx.bot.send_message.call_args.kwargs["chat_id"], 123)

    async def test_close_failure_preserves_open_ticket(self):
        ticket = self.service.store.create_ticket(123, "test")
        ctx = context([str(ticket["id"])])
        ctx.bot.send_message.side_effect = OSError("private")
        await self.service.admin(update(uid=999, text="/close"), ctx)
        self.assertEqual(self.service.store.ticket(ticket["id"])["status"], "open")

    async def test_ai_failure_keeps_context_for_human_followup(self):
        self.ai.answer.side_effect = RuntimeError("private API response")
        up = update(text="My phone does not connect")
        await self.service.message(up, context())
        self.assertEqual(self.service.history(123)[0]["content"], up.effective_message.text)
        self.assertNotIn("private", up.effective_message.reply_text.call_args.args[0])

    async def test_busy_user_does_not_launch_second_request(self):
        entered, resume = asyncio.Event(), asyncio.Event()
        async def answer(*args):
            entered.set()
            await resume.wait()
            return {"reply": "ok", "needs_human": False, "summary": ""}
        self.ai.answer.side_effect = answer
        task = asyncio.create_task(self.service.message(update(), context()))
        await entered.wait()
        await self.service.message(update(), context())
        resume.set()
        await task
        self.assertEqual(self.ai.answer.call_count, 1)

    async def test_reset_and_bounded_history_are_isolated(self):
        for _ in range(5):
            self.service.remember(123, "one", "two")
        self.service.remember(999, "other", "answer")
        self.assertEqual(len(self.service.history(123)), 6)
        await self.service.reset(update(), context())
        self.assertEqual(self.service.history(123), [])
        self.assertEqual(len(self.service.history(999)), 2)


class FakeTelegramRequest(BaseRequest):
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    @property
    def read_timeout(self):
        return 2

    async def initialize(self):
        pass

    async def shutdown(self):
        pass

    async def do_request(self, url, method, request_data=None, **kwargs):
        name = url.rsplit("/", 1)[-1]
        self.calls.append(name)
        if self.fail and name == "getMe":
            return 401, b'{"ok":false,"error_code":401,"description":"Unauthorized"}'
        result = True
        if name == "getMe":
            result = {"id": 123456789, "is_bot": True, "first_name": "Support", "username": "OfflineSupportBot"}
        elif name == "getUpdates":
            await asyncio.sleep(0.01)
            result = []
        return 200, json.dumps({"ok": True, "result": result}).encode()


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_telegram_library_start_and_stop_without_network(self):
        request, updates = FakeTelegramRequest(), FakeTelegramRequest()
        builder = Application.builder().request(request).get_updates_request(updates)
        ai = SimpleNamespace(client=SimpleNamespace(aclose=AsyncMock()))
        service = SupportService(config(), lambda uid: {}, ai=ai)
        with patch.object(Application, "builder", return_value=builder):
            await service.launch()
        self.assertTrue(service.app.running)
        self.assertTrue(service.app.updater.running)
        self.assertIn("setMyCommands", request.calls)
        await service.stop()
        self.assertFalse(service.app.running)
        self.assertFalse(service.app.updater.running)
        await service.stop()
        self.assertEqual(ai.client.aclose.call_count, 1)

    async def test_invalid_telegram_token_closes_ai_and_database(self):
        request = FakeTelegramRequest(fail=True)
        builder = Application.builder().request(request).get_updates_request(FakeTelegramRequest())
        ai = SimpleNamespace(client=SimpleNamespace(aclose=AsyncMock()))
        service = SupportService(config(), lambda uid: {}, ai=ai)
        with patch.object(Application, "builder", return_value=builder):
            with self.assertRaises(Exception):
                await service.launch()
        self.assertTrue(service.closed)
        ai.client.aclose.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
