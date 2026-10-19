"""Owner-reviewed enrollment. Never logs into Quotex or changes trading access."""
from decimal import Decimal, InvalidOperation
import re
import secrets
import time

LINK_IDS = frozenset({"569153", "1726475"})
REGISTER_URL = "https://broker-qx.pro/sign-up/?lid=569153"
REGISTER_TEXT = (
    "أهلًا فيك 👋 منقدم بوت إشارات، تطبيق Android وإضافة للكمبيوتر، مع قنوات الفريق والدعم.\n"
    "للانضمام: سجّل من الرابط، وبعدين ابعت ID حساب Quotex هون للتشييك قبل الإيداع:\n"
    + REGISTER_URL + "\nإذا عندك حساب بالفعل، ابعت الـID لنراجعه أولًا. ما بتحذف حسابك."
)
STAGES = {
    "awaiting_id": "بانتظار ID حساب Quotex",
    "registration_review": "بانتظار مراجعة التسجيل من المسؤول",
    "id_recheck": "تأكد من ID حساب Quotex وأرسله مجددًا",
    "agency_review": "الحساب بحاجة مراجعة ارتباطه بالوكالة",
    "awaiting_deposit": "تم تأكيد التسجيل؛ بانتظار إتمام شرط الإيداع",
    "deposit_review": "بانتظار مراجعة الإيداع من المسؤول",
    "approved": "اعتمد المسؤول التسجيل والإيداع؛ روابطك جاهزة عبر /register",
}


def normalized(text):
    return str(text).translate(str.maketrans("أإآى٠١٢٣٤٥٦٧٨٩", "اااي0123456789"))


def registration_intent(text):
    text = normalized(text).lower()
    return bool(re.search(r"(?:سجل|تسجيل|اشترك|اشتراك مع|انضم|شو (?:الخدمات|بتقدم|منقدم)|خدماتكم|كيف.*اشتراك|how.*(?:join|register)|sign up)", text))


def deposit_intent(text):
    return bool(re.search(r"(?:ايداع|اودعت|شحنت|تم الشحن|deposited)", normalized(text).lower()))


def parse_partner_report(text):
    """Parse only minimal fields. Caller must establish trusted admin provenance."""
    text = text.replace("**", "").replace("\u00a0", " ").strip()
    not_found = re.fullmatch(r"Trader with ID\s*=\s*['\"]?(\d{5,12})['\"]?\s+was not found\.", text, re.I)
    if not_found:
        return {"trader_id": not_found[1], "result": "not_found"}
    patterns = {
        "trader_id": r"^Trader\s*#\s*(\d{5,12})\s*$",
        "link_id": r"^Link Id:\s*(\d+)\s*$",
        "deposits_count": r"^Deposits Count:\s*(\d+)\s*$",
        "deposits_sum": r"^Deposits Sum:\s*\$\s*((?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d{1,2})?)\s*$",
    }
    fields = {}
    for key, pattern in patterns.items():
        matches = re.findall(pattern, text, re.M | re.I)
        if len(matches) != 1:
            raise ValueError("Incomplete or ambiguous partner report")
        fields[key] = matches[0]
    try:
        amount = Decimal(fields["deposits_sum"].replace(",", ""))
    except InvalidOperation:
        raise ValueError("Invalid deposit total") from None
    count = int(fields["deposits_count"])
    if (count == 0) != (amount == 0):
        raise ValueError("Inconsistent deposit fields")
    fields["deposits_sum"] = str(amount)
    fields["deposits_count"] = count
    fields["result"] = "wrong_agency" if fields["link_id"] not in LINK_IDS else ("eligible" if amount >= 50 and count > 0 else "no_deposit")
    return fields


class Onboarding:
    def __init__(self, service):
        self.service = service

    async def get(self, uid):
        return await self.service.storage("onboarding_get", uid) or {}

    async def change(self, uid, old, **changes):
        record = dict(old, **changes, updated=time.time(), revision=secrets.token_hex(4))
        if not await self.service.storage("onboarding_cas", uid, old.get("revision"), record):
            raise ValueError("Enrollment changed; reopen request")
        return record

    async def register(self, update, context):
        if not await self.service.authorized(update):
            return
        uid = update.effective_user.id
        if self.service.free_throttled(uid):
            return
        row = await self.get(uid)
        if not row:
            row = await self.change(uid, row, stage="awaiting_id")
            await update.effective_message.reply_text(REGISTER_TEXT, link_preview_options={"is_disabled": True})
        elif row.get("stage") == "approved":
            await self.deliver(uid, context)
        else:
            if row.get("stage") in {"registration_review", "deposit_review"}:
                await self.notify(uid, row, context)
            await update.effective_message.reply_text(self.status(row))

    def status(self, row):
        message = STAGES.get(row.get("stage"), "ابدأ بـ /register")
        if row.get("trader_id"):
            message += "\nQuotex ID: " + row["trader_id"]
        if row.get("stage") == "awaiting_deposit":
            message += "\nشرط الانضمام مجموع إيداعات 50$ على الأقل. بعد إتمامه اكتب «تم الإيداع» لنراجعه."
        return message

    async def notify(self, uid, row, context):
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("نسخ أمر التحقق: /verify", callback_data=f"ob:help:{uid}:{row['revision']}")]])
        await context.bot.send_message(
            chat_id=self.service.config.admin_id,
            text=f"مراجعة انضمام\nTelegram ID: {uid}\nQuotex ID: {row['trader_id']}\n"
                 + self.status(row) + "\nافحص الـID من حسابك المعتمد لدى @QuotexPartnerBot، ثم أرسل:\n"
                 + f"/verify {uid}\nوبعده رد المنصة كاملًا بنفس الرسالة.\nتحقق من ملكية الحساب قبل الاعتماد.",
            reply_markup=keyboard)

    async def message(self, update, context):
        uid = update.effective_user.id
        text = update.effective_message.text or ""
        row = await self.get(uid)
        number = re.fullmatch(r"(?:ID\s*[:#]?\s*)?(\d{5,12})", normalized(text).strip(), re.I)
        if number and (not row or row.get("stage") in {"awaiting_id", "id_recheck", "agency_review"}):
            row = await self.change(uid, row, trader_id=number[1], stage="registration_review", candidate=None)
            await self.notify(uid, row, context)
            await update.effective_message.reply_text("وصل الـID للمراجعة. انتظر تأكيد التسجيل قبل الإيداع. تابع حالتك بـ /register.")
            return True
        if row.get("stage") == "awaiting_deposit" and deposit_intent(text):
            row = await self.change(uid, row, stage="deposit_review", candidate=None)
            await self.notify(uid, row, context)
            await update.effective_message.reply_text("وصل طلب التحقق من الإيداع للمسؤول. منبعتلك الروابط بعد اعتماده.")
            return True
        if registration_intent(text) or (row and (number or deposit_intent(text))):
            if not row:
                row = await self.change(uid, row, stage="awaiting_id")
                await update.effective_message.reply_text(REGISTER_TEXT, link_preview_options={"is_disabled": True})
            else:
                await update.effective_message.reply_text(self.status(row))
            return True
        return False

    async def verify(self, update, context):
        if not await self.service.authorized(update) or update.effective_user.id != self.service.config.admin_id:
            return
        text = update.effective_message.text or ""
        parts = text.split(maxsplit=2)
        if len(parts) != 3 or not parts[1].isdigit():
            await update.effective_message.reply_text("استخدم /verify Telegram_ID ثم رد بوت المنصة كاملًا بنفس الرسالة.")
            return
        uid = int(parts[1])
        row = await self.get(uid)
        if row.get("stage") not in {"registration_review", "deposit_review"}:
            await update.effective_message.reply_text("ما في طلب تحقق معلّق لهالشخص.")
            return
        try:
            report = parse_partner_report(parts[2])
        except ValueError:
            await update.effective_message.reply_text("رد المنصة ناقص أو غير واضح. الطلب بقي بانتظار التحقق.")
            return
        if report["trader_id"] != row.get("trader_id"):
            await update.effective_message.reply_text("الـID بالنتيجة مختلف عن الطلب. ما تغيّرت الحالة.")
            return
        row = await self.change(uid, row, candidate=report, checked_at=time.time())
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        labels = {"eligible": "تأكيد الملكية واعتماد التسجيل والإيداع", "no_deposit": "تأكيد الملكية واعتماد التسجيل فقط",
                  "not_found": "اطلب تصحيح الـID", "wrong_agency": "حوّل لمراجعة الوكالة"}
        await update.effective_message.reply_text(
            f"نتيجة الفحص: {report['result']}\nQuotex ID: {report['trader_id']}\n"
            f"مجموع الإيداعات: {report.get('deposits_sum', 'غير متاح')}\n"
            "الاعتماد يؤكد أنك راجعت النتيجة الحديثة وملكية الحساب. مهلة التأكيد 15 دقيقة.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(labels[report['result']], callback_data=f"ob:approve:{uid}:{row['revision']}")]]))

    async def callback(self, update, context):
        query = update.callback_query
        if not query or update.effective_user.id != self.service.config.admin_id or not update.effective_chat or update.effective_chat.type != "private":
            if query:
                await query.answer("للمسؤول فقط", show_alert=True)
            return
        match = re.fullmatch(r"ob:(help|approve):(\d{1,15}):([a-f0-9]{8})", query.data or "")
        if not match:
            return
        await query.answer()
        action, uid, revision = match[1], int(match[2]), match[3]
        row = await self.get(uid)
        if row.get("revision") != revision:
            await query.message.reply_text("الطلب تغيّر أو تمت معالجته. افتح أحدث طلب.")
            return
        if action == "help":
            await query.message.reply_text(f"/verify {uid}\nالصق رد بوت المنصة بعد الأمر بنفس الرسالة.")
            return
        candidate = row.get("candidate") or {}
        if row.get("stage") not in {"registration_review", "deposit_review"} or time.time() - row.get("checked_at", 0) > 900:
            await query.message.reply_text("الفحص قديم. أرسل نتيجة حديثة عبر /verify.")
            return
        stage = {"eligible": "approved", "no_deposit": "awaiting_deposit", "not_found": "id_recheck", "wrong_agency": "agency_review"}.get(candidate.get("result"))
        if not stage:
            return
        # Atomic compare-and-swap rejects stale double clicks and concurrent reviews.
        row = await self.change(uid, row, stage=stage, reviewed_by=self.service.config.admin_id, candidate=None)
        if stage == "approved":
            await self.deliver(uid, context)
        else:
            messages = {
                "awaiting_deposit": "تأكد تسجيل حسابك معنا ✅\nشرط الانضمام مجموع إيداعات 50$ على الأقل. بعد إتمامه اكتب «تم الإيداع» لنراجعه. توثيق الحساب بيكون من منصة Quotex نفسها.",
                "id_recheck": "ما ظهر حسابك بالتشييك. تأكد من ID حساب Quotex وأرسله مجددًا. إذا سجلت هلأ خبرنا لنعيد التحقق.",
                "agency_review": "حسابك بحاجة مراجعة ارتباطه بوكالتنا. المسؤول بيتابع معك طريقة التسجيل المناسبة؛ لا تحذف حسابك. /human للتواصل.",
            }
            await context.bot.send_message(chat_id=uid, text=messages[stage])
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text("تم حفظ المراجعة. إذا تعذر التسليم، يستطيع المستخدم طلب حالته وروابطه عبر /register.")

    async def deliver(self, uid, context):
        if (await self.get(uid)).get("stage") != "approved":
            return
        links = await self.service.storage("onboarding_links") or []
        if not links:
            await context.bot.send_message(chat_id=uid, text="تم اعتماد التسجيل والإيداع ✅ المسؤول عم يجهّز روابط الانضمام. /register للمحاولة لاحقًا.")
            return
        text = "تم اعتماد التسجيل والإيداع ✅\nروابط البداية:\n" + "\n\n".join(f"{link['title']}\n{link['url']}" for link in links)
        text += "\n\nافتح بوت التداول واتبع خطواته؛ تفعيل الاشتراك بيتم من الإدارة. لا ترسل كلمة مرورك أو وثائقك هون."
        await context.bot.send_message(chat_id=uid, text=text, link_preview_options={"is_disabled": True})
