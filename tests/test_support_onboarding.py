import asyncio
from dataclasses import replace
from decimal import Decimal
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from support_bot import SupportConfig, SupportService, SupportStore
from support_onboarding import parse_partner_report
from support_state import FirebaseSupportStore
from test_support_state import MemoryRef


def report(trader="12345678", link="569153", count=1, amount="50.00", balance="0.00"):
    return f"Trader # {trader}\nCountry: Test\nLink Id: {link}\nBalance: $ {balance}\nDeposits Count: {count}\nDeposits Sum: $ {amount}\nBonuses Sum: $ 100.00"


def update(uid=123, text="بدي سجل معك"):
    message = SimpleNamespace(text=text, reply_text=AsyncMock())
    return SimpleNamespace(effective_user=SimpleNamespace(id=uid), effective_chat=SimpleNamespace(type="private"), effective_message=message)


class ParserTests(unittest.TestCase):
    def test_deposits_not_balance_bonus_or_profit(self):
        self.assertEqual(parse_partner_report(report(balance="0.00"))["result"], "eligible")
        self.assertEqual(parse_partner_report(report(count=0, amount="0.00", balance="1000"))["result"], "no_deposit")
        self.assertEqual(parse_partner_report(report(amount="49.99"))["result"], "no_deposit")
        self.assertEqual(parse_partner_report(report(amount="1,050.25"))["deposits_sum"], "1050.25")

    def test_both_accepted_links_and_wrong_agency(self):
        self.assertEqual(parse_partner_report(report(link="1726475"))["result"], "eligible")
        self.assertEqual(parse_partner_report(report(link="99999"))["result"], "wrong_agency")

    def test_not_found_preserves_exact_id(self):
        self.assertEqual(parse_partner_report("Trader with ID = '12345678' was not found."), {"trader_id":"12345678", "result":"not_found"})

    def test_missing_ambiguous_or_inconsistent_fields_are_not_approval(self):
        for text in ["timeout", report()+"\nLink Id: 569153", report(count=0), report(amount="-50"), report(amount="50.001"), report().replace("Deposits Sum", "Balance")]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                parse_partner_report(text)


class OnboardingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        cfg = SupportConfig("123456789:test", "test", 999, frozenset({123,999}), db_path=":memory:")
        self.ai=SimpleNamespace(answer=AsyncMock(return_value={"reply":"ok", "needs_human":False,"summary":""}),client=SimpleNamespace(aclose=AsyncMock()))
        self.service=SupportService(cfg,lambda uid:{},ai=self.ai)
        self.ctx=SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock()),args=[])
        self.flow=self.service.onboarding

    async def asyncTearDown(self):
        await self.service.stop()

    async def enroll(self):
        await self.service.message(update(),self.ctx)
        await self.service.message(update(text="12345678"),self.ctx)

    def callback(self, row, uid=999):
        up=update(uid)
        up.callback_query=SimpleNamespace(data=f"ob:approve:123:{row['revision']}",answer=AsyncMock(),message=SimpleNamespace(reply_text=AsyncMock()),edit_message_reply_markup=AsyncMock())
        return up

    async def verify(self, text=None):
        await self.flow.verify(update(999,"/verify 123\n"+(text or report())),self.ctx)
        return await self.flow.get(123)

    async def test_full_round_trip_with_restart_and_fresh_review(self):
        await self.enroll()
        self.ai.answer.assert_not_called()
        self.assertEqual((await self.flow.get(123))["stage"],"registration_review")
        row=await self.verify(report(count=0,amount="0"))
        await self.flow.callback(self.callback(row),self.ctx)
        self.assertEqual((await self.flow.get(123))["stage"],"awaiting_deposit")
        await self.service.message(update(text="تم الإيداع"),self.ctx)
        self.assertEqual((await self.flow.get(123))["stage"],"deposit_review")
        row=await self.verify()
        # Private links are a setting, never knowledge passed to the AI.
        self.service.store.db.execute("INSERT INTO support_settings VALUES ('onboarding_links',?)",(json.dumps([{"title":"private","url":"https://t.me/+test-only"}]),))
        self.service.store.db.commit()
        callback=self.callback(row)
        await self.flow.callback(callback,self.ctx)
        self.assertEqual((await self.flow.get(123))["stage"],"approved")
        delivered=self.ctx.bot.send_message.call_args.kwargs
        self.assertEqual(delivered["chat_id"],123)
        self.assertIn("https://t.me/+test-only",delivered["text"])
        calls=self.ctx.bot.send_message.call_count
        await self.flow.callback(callback,self.ctx)
        self.assertEqual(self.ctx.bot.send_message.call_count,calls)

    async def test_customer_report_never_approves_and_wrong_id_admin_report_rejected(self):
        await self.enroll()
        await self.service.message(update(text=report()),self.ctx)
        self.assertEqual((await self.flow.get(123))["stage"],"registration_review")
        row=await self.verify(report(trader="87654321"))
        self.assertIsNone(row.get("candidate"))

    async def test_other_user_cannot_review_or_press_admin_callback(self):
        await self.enroll()
        await self.flow.verify(update(123,"/verify 123\n"+report()),self.ctx)
        self.assertIsNone((await self.flow.get(123)).get("candidate"))
        row=await self.verify()
        await self.flow.callback(self.callback(row,123),self.ctx)
        self.assertEqual((await self.flow.get(123))["stage"],"registration_review")

    async def test_expired_review_and_old_revision_rejected(self):
        await self.enroll()
        old=await self.verify()
        row=await self.flow.change(123,old,checked_at=0)
        await self.flow.callback(self.callback(old),self.ctx)
        await self.flow.callback(self.callback(row),self.ctx)
        self.assertEqual((await self.flow.get(123))["stage"],"registration_review")

    async def test_not_found_requests_correction_without_deposit_or_deletion(self):
        await self.enroll()
        row=await self.verify("Trader with ID = '12345678' was not found.")
        await self.flow.callback(self.callback(row),self.ctx)
        self.assertEqual((await self.flow.get(123))["stage"],"id_recheck")
        self.assertNotIn("إيداع",self.ctx.bot.send_message.call_args.kwargs["text"])
        await self.service.message(update(text="87654321"),self.ctx)
        self.assertEqual((await self.flow.get(123))["trader_id"],"87654321")

    async def test_delivery_failure_keeps_approved_for_retry(self):
        await self.enroll()
        row=await self.verify()
        self.ctx.bot.send_message.side_effect=OSError("network")
        with self.assertRaises(OSError):
            await self.flow.callback(self.callback(row),self.ctx)
        self.assertEqual((await self.flow.get(123))["stage"],"approved")
        self.ctx.bot.send_message.side_effect=None
        await self.flow.register(update(text="/register"),self.ctx)
        self.assertEqual(self.ctx.bot.send_message.call_args.kwargs["chat_id"],123)

    async def test_firebase_atomic_revision_and_persistence(self):
        data={"users":{"123":{"subscription":"untouched"}}}
        ref=MemoryRef(data).child("system").child("support_bot")
        store=FirebaseSupportStore(lambda:ref)
        first={"revision":"aaa", "stage":"awaiting_id"}
        self.assertTrue(store.onboarding_cas(123,None,first))
        other=FirebaseSupportStore(lambda:ref)
        self.assertEqual(other.onboarding_get(123),first)
        self.assertFalse(other.onboarding_cas(123,None,{"revision":"bad"}))
        self.assertTrue(other.onboarding_cas(123,"aaa",{"revision":"bbb"}))
        self.assertEqual(data["users"],{"123":{"subscription":"untouched"}})


if __name__=="__main__": unittest.main()
