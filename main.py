import os
import json
import hashlib
import asyncio
from datetime import datetime, timedelta, timezone

import firebase_admin
from firebase_admin import credentials, db

from dotenv import load_dotenv
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

UTC_PLUS_3 = timezone(timedelta(hours=3))

ADMIN_USERNAME = "@coach_WAEL_trading"
ADMIN_TELEGRAM_ID = 1582593617

DATABASE_URL = "https://telegram-bot-f0229-default-rtdb.firebaseio.com"

PAIRS = [
    "USD/BRL (OTC)",
    "USD/ARS (OTC)",
    "USD/BDT (OTC)",
    "USD/NGN (OTC)",
    "USD/PKR (OTC)",
    "USD/DZD (OTC)",
    "USD/MXN (OTC)",
    "USD/INR (OTC)",
    "USD/IDR (OTC)",
    "USD/EGP (OTC)",
    "USD/TRY (OTC)",
    "USD/COP (OTC)",
    "EUR/JPY (OTC)",
    "EUR/USD (OTC)",
    "CAD/CHF (OTC)",
    "CAD/JPY (OTC)",
    "AUD/CHF (OTC)",
    "AUD/CAD (OTC)",
]

TRADE_COUNTS = [3, 5, 10, 15, 20]
INTERVALS = [1, 3, 5]

ONLINE_MINUTES_WINDOW = 15

# ===== Firebase init =====
firebase_json = os.getenv("FIREBASE_CREDENTIALS_JSON")

if firebase_json:
    cred_dict = json.loads(firebase_json)
    cred = credentials.Certificate(cred_dict)
else:
    cred = credentials.Certificate("serviceAccountKey.json")

firebase_admin.initialize_app(cred, {
    "databaseURL": DATABASE_URL
})

# ===== Keyboards =====
main_keyboard = ReplyKeyboardMarkup(
    [
        ["📊 توليد إشارات", "👤 حسابي"],
        ["📞 تواصل مع المسؤول"],
    ],
    resize_keyboard=True
)

admin_main_keyboard = ReplyKeyboardMarkup(
    [
        ["📥 الطلبات المعلقة", "📋 كافة المستخدمين"],
        ["🟢 المستخدمون النشطون", "🔍 تفاصيل مستخدم"],
        ["🟢 تشغيل البوت", "🔴 إيقاف البوت"],
        ["⬅️ رجوع"],
    ],
    resize_keyboard=True
)

welcome_keyboard = ReplyKeyboardMarkup(
    [
        ["✅ نعم، أنا منضم", "❌ لا، لست مشتركًا"],
    ],
    resize_keyboard=True
)

pairs_keyboard = ReplyKeyboardMarkup(
    [
        ["USD/BRL (OTC)", "USD/ARS (OTC)"],
        ["USD/BDT (OTC)", "USD/NGN (OTC)"],
        ["USD/PKR (OTC)", "USD/DZD (OTC)"],
        ["USD/MXN (OTC)", "USD/INR (OTC)"],
        ["USD/IDR (OTC)", "USD/EGP (OTC)"],
        ["USD/TRY (OTC)", "USD/COP (OTC)"],
        ["EUR/JPY (OTC)", "EUR/USD (OTC)"],
        ["CAD/CHF (OTC)", "CAD/JPY (OTC)"],
        ["AUD/CHF (OTC)", "AUD/CAD (OTC)"],
        ["🔙 رجوع"],
    ],
    resize_keyboard=True
)

count_keyboard = ReplyKeyboardMarkup(
    [
        ["3", "5", "10"],
        ["15", "20"],
        ["🔙 رجوع"],
    ],
    resize_keyboard=True
)

interval_keyboard = ReplyKeyboardMarkup(
    [
        [f"{INTERVALS[0]} دقيقة", f"{INTERVALS[1]} دقائق", f"{INTERVALS[2]} دقائق"],
        ["🔙 رجوع"],
    ],
    resize_keyboard=True
)

admin_duration_keyboard = ReplyKeyboardMarkup(
    [
        ["🗓 أسبوع", "🗓 شهر"],
        ["♾ دائم", "⛔ إلغاء التفعيل"],
        ["⬅️ رجوع"],
    ],
    resize_keyboard=True
)


# ===== Firebase refs =====
def users_ref():
    return db.reference("users")


def pending_ref():
    return db.reference("pending_users")


def approved_ref():
    return db.reference("approved_users")


def system_ref():
    return db.reference("system")


# ===== Helpers =====
def now_utc():
    return datetime.now(timezone.utc)


def now_iso():
    return now_utc().isoformat()


def parse_iso(value: str):
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def format_dt_ar(iso_value: str):
    dt = parse_iso(iso_value)
    if not dt:
        return "غير متوفر"
    return dt.astimezone(UTC_PLUS_3).strftime("%Y-%m-%d %H:%M")


def get_user_record(user_id: int):
    return users_ref().child(str(user_id)).get()


def save_user_record(user_id: int, data: dict):
    users_ref().child(str(user_id)).update(data)


def save_pending_user(user_id: int, data: dict):
    pending_ref().child(str(user_id)).set(data)


def remove_pending_user(user_id: int):
    pending_ref().child(str(user_id)).delete()


def set_approved_user(user_id: int, data: dict):
    approved_ref().child(str(user_id)).set(data)


def get_approved_user(user_id: int):
    return approved_ref().child(str(user_id)).get()


def get_all_pending_users():
    return pending_ref().get() or {}


def get_all_users():
    return users_ref().get() or {}


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_TELEGRAM_ID


def get_bot_enabled() -> bool:
    data = system_ref().get() or {}
    return bool(data.get("bot_enabled", True))


def set_bot_enabled(value: bool):
    system_ref().update({
        "bot_enabled": value,
        "updated_at": now_iso(),
    })


def is_approved(user_id: int) -> bool:
    if is_admin(user_id):
        return True

    data = get_approved_user(user_id)
    if not data:
        return False

    status = data.get("status", "approved")
    if status != "approved":
        return False

    expires_at = data.get("expires_at")
    if not expires_at:
        return True

    if expires_at == "forever":
        return True

    exp_dt = parse_iso(expires_at)
    if not exp_dt:
        return False

    return exp_dt > now_utc()


def get_user_status(user_id: int) -> str:
    if is_admin(user_id):
        return "admin"

    approved_data = get_approved_user(user_id)
    if approved_data:
        status = approved_data.get("status", "approved")
        if status == "blocked":
            return "blocked"
        if is_approved(user_id):
            return "approved"
        return "expired"

    pending_data = pending_ref().child(str(user_id)).get()
    if pending_data:
        return "pending"

    return "new"


def set_user_expiry(user_id: int, mode: str):
    approved_data = get_approved_user(user_id) or {"telegram_id": user_id}

    if mode == "week":
        expires_at = (now_utc() + timedelta(days=7)).isoformat()
    elif mode == "month":
        expires_at = (now_utc() + timedelta(days=30)).isoformat()
    elif mode == "forever":
        expires_at = "forever"
    else:
        return

    approved_data.update({
        "telegram_id": user_id,
        "status": "approved",
        "approved_at": now_iso(),
        "expires_at": expires_at,
    })

    set_approved_user(user_id, approved_data)
    save_user_record(user_id, {
        "status": "approved",
        "expires_at": expires_at,
    })
    remove_pending_user(user_id)


def block_user(user_id: int):
    approved_data = get_approved_user(user_id) or {"telegram_id": user_id}
    approved_data.update({
        "telegram_id": user_id,
        "status": "blocked",
        "blocked_at": now_iso(),
    })
    set_approved_user(user_id, approved_data)
    save_user_record(user_id, {
        "status": "blocked",
    })


def get_recent_active_users(minutes: int = ONLINE_MINUTES_WINDOW):
    users = get_all_users()
    result = []
    cutoff = now_utc() - timedelta(minutes=minutes)

    for user_id, data in users.items():
        last_seen = parse_iso(data.get("last_seen", ""))
        if last_seen and last_seen >= cutoff:
            result.append((user_id, data))

    result.sort(key=lambda x: x[1].get("last_seen", ""), reverse=True)
    return result


def format_utc_plus_3(dt: datetime) -> str:
    dt_plus_3 = dt.astimezone(UTC_PLUS_3)
    return dt_plus_3.strftime("%H:%M")


def get_stable_direction(pair: str, dt: datetime) -> str:
    dt_plus_3 = dt.astimezone(UTC_PLUS_3)

    key = (
        f"{pair}|"
        f"{dt_plus_3.year}-"
        f"{dt_plus_3.month:02d}-"
        f"{dt_plus_3.day:02d}|"
        f"H{dt_plus_3.hour:02d}|"
        f"M{dt_plus_3.minute:02d}"
    )

    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    value = int(digest[:8], 16)

    return "CALL 📈" if value % 2 == 0 else "PUT 📉"


def generate_signals(pair: str, count: int, interval_minutes: int, start_dt: datetime):
    signals = []

    for i in range(count):
        entry_time = start_dt + timedelta(minutes=i * interval_minutes)
        direction = get_stable_direction(pair, entry_time)
        formatted_time = format_utc_plus_3(entry_time)
        signals.append(f"{pair} — {formatted_time} — {direction}")

    return signals


def build_signals_message(pair: str, count: int, interval_minutes: int, signals: list[str]) -> str:
    header = (
        "╔══════════════╗\n"
        "   📊 Quotex Signals\n"
        "╚══════════════╝\n\n"
        "⏰ توقيت المنصة\n"
        "UTC / GMT +3.00\n\n"
        f"💱 الزوج: {pair}\n"
        f"📈 عدد الصفقات: {count}\n"
        f"⏳ الفاصل: {interval_minutes} دقيقة\n\n"
        "⚠️ ملاحظات مهمة:\n"
        "• مدة كل صفقة: 1M\n"
        "• تجنب صفقة عكس مومنتم\n"
        "• تجنب دخول الصفقة بعد شمعة دوجي\n"
        "• تجنب صفقة عكس تريند قوي\n"
        "• استخدم مضاعفة واحدة عند الخسارة\n\n"
        "📍 الإشارات:\n"
    )

    formatted_signals = []
    for index, signal in enumerate(signals, start=1):
        parts = signal.split(" — ")
        if len(parts) == 3:
            _, signal_time, direction = parts
            formatted_signals.append(f"{index}) {signal_time} → {direction}")
        else:
            formatted_signals.append(f"{index}) {signal}")

    return header + "\n".join(formatted_signals)


def reset_signal_state(context: ContextTypes.DEFAULT_TYPE):
    context.user_data["step"] = None
    context.user_data["pair"] = None
    context.user_data["count"] = None
    context.user_data["interval"] = None
    context.user_data["admin_target_id"] = None


async def send_welcome_flow(update: Update):
    await update.message.reply_text(
        "👋 أهلًا بك في بوت TRADING TIME\n\n"
        "هل أنت منضم لفريق TRADING TIME؟",
        reply_markup=welcome_keyboard
    )


def build_main_menu_for_user(user_id: int):
    if is_admin(user_id):
        return ReplyKeyboardMarkup(
            [
                ["📊 توليد إشارات", "👤 حسابي"],
                ["📞 تواصل مع المسؤول", "🛠 لوحة الأدمن"],
            ],
            resize_keyboard=True
        )
    return main_keyboard


async def send_user_details(update: Update, target_id: int, show_admin_actions: bool = False):
    data = get_user_record(target_id)
    if not data:
        await update.message.reply_text("❌ هذا المستخدم غير موجود.")
        return

    username = f"@{data.get('username')}" if data.get("username") else "بدون username"
    status = get_user_status(target_id)
    quotex_id = data.get("quotex_id", "غير موجود")
    expires_at = data.get("expires_at", "غير محدد")

    if expires_at != "forever":
        expires_at = format_dt_ar(expires_at) if expires_at != "غير محدد" else expires_at

    msg = (
        "👤 تفاصيل المستخدم\n\n"
        f"• الاسم: {data.get('name', 'غير معروف')}\n"
        f"• اليوزر: {username}\n"
        f"• Telegram ID: {target_id}\n"
        f"• Quotex ID: {quotex_id}\n"
        f"• الحالة: {status}\n"
        f"• آخر نشاط: {format_dt_ar(data.get('last_seen', ''))}\n"
        f"• انتهاء الصلاحية: {expires_at}"
    )

    if show_admin_actions:
        await update.message.reply_text(msg, reply_markup=admin_duration_keyboard)
    else:
        await update.message.reply_text(msg)


async def send_maintenance_message(update: Update):
    await update.message.reply_text(
        "🛠 البوت تحت الصيانة حاليًا\n\n"
        "نقوم حاليًا ببعض التحديثات والتحسينات.\n"
        "يرجى المحاولة لاحقًا.\n\n"
        "شكرًا لتفهمك 🤍"
    )


# ===== Main Handlers =====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    reset_signal_state(context)

    save_user_record(user.id, {
        "telegram_id": user.id,
        "name": user.full_name,
        "username": user.username or "",
        "last_seen": now_iso(),
    })

    if is_admin(user.id):
        await update.message.reply_text(
            f"👋 أهلًا {user.first_name}\n"
            "مرحبًا بك في وضع الأدمن.",
            reply_markup=build_main_menu_for_user(user.id)
        )
        return

    if not get_bot_enabled():
        await send_maintenance_message(update)
        return

    if is_approved(user.id):
        await update.message.reply_text(
            f"✅ أهلًا {user.first_name}\n"
            "مرحبًا بك من جديد في بوت TRADING TIME.",
            reply_markup=build_main_menu_for_user(user.id)
        )
    else:
        await send_welcome_flow(update)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.message.text
    step = context.user_data.get("step")

    save_user_record(user.id, {
        "telegram_id": user.id,
        "name": user.full_name,
        "username": user.username or "",
        "last_seen": now_iso(),
    })

    # ===== Maintenance mode =====
    if not is_admin(user.id) and not get_bot_enabled():
        await send_maintenance_message(update)
        return

    # ===== Non-approved users =====
    if not is_admin(user.id) and not is_approved(user.id):
        if text == "✅ نعم، أنا منضم":
            context.user_data["step"] = "waiting_quotex_id"
            await update.message.reply_text(
                "📩 أرسل ID الخاص بحسابك على QUOTEX ليتم فحص حسابك.\n"
                "بعد التأكد سيتم إتاحة البوت لك بشكل مجاني."
            )
            return

        if text == "❌ لا، لست مشتركًا":
            await update.message.reply_text(WELCOME_MESSAGE = """
📌 طريقة التسجيل والاشتراك في بوت TRADING TIME 👇

1️⃣ إنشاء حساب جديد على منصة Quotex عبر الرابط التالي:
🔗 Broker-qx.pro/?lid=569153

2️⃣ بعد إنشاء الحساب (قبل التوثيق أو الإيداع)
📩 أرسل ID حسابك إلى الأدمن:
👉 @MOHAMMED_trading

3️⃣ بعد المراجعة من قبل الأدمن
✔ قم بتوثيق حسابك (البريد الإلكتروني + الهوية)

4️⃣ 💰 قم بالإيداع بمبلغ لا يقل عن 50$ لبدء التداول بشكل صحيح

5️⃣ بعد إتمام جميع الخطوات
🤖 أرسل ID حسابك للبوت هنا ليتم:
✔ فحص الحساب
✔ تفعيلك داخل البوت

━━━━━━━━━━━━━━━
🚀 بعدها ستحصل على إشارات التداول مجانًا
مع فريق TRADING TIME

بالتوفيق للجميع 💚
"""
            )
            return

        if step == "waiting_quotex_id":
            quotex_id = text.strip()

            pending_data = {
                "telegram_id": user.id,
                "name": user.full_name,
                "username": user.username or "",
                "quotex_id": quotex_id,
                "status": "pending",
                "created_at": now_iso(),
            }

            save_pending_user(user.id, pending_data)
            save_user_record(user.id, {
                "quotex_id": quotex_id,
                "status": "pending",
            })

            await update.message.reply_text(
                "📩 تم استلام طلبك بنجاح\n\n"
                "تم حفظ Quotex ID الخاص بك وإرساله للإدارة للمراجعة.\n"
                "بعد التأكد، سيتم تفعيل البوت لك مجانًا.\n\n"
                "يرجى انتظار موافقة الأدمن ✅"
            )

            username_text = f"@{user.username}" if user.username else "بدون username"
            admin_message = (
                "📥 طلب تفعيل جديد\n\n"
                f"👤 الاسم: {user.full_name}\n"
                f"🔗 اليوزر: {username_text}\n"
                f"🆔 Telegram ID: {user.id}\n"
                f"💱 Quotex ID: {quotex_id}\n\n"
                "──────────────"
            )
            try:
                await context.bot.send_message(chat_id=ADMIN_TELEGRAM_ID, text=admin_message)
            except Exception:
                pass

            context.user_data["step"] = "pending_review"
            return

        await send_welcome_flow(update)
        return

    # ===== Common buttons =====
    if text == "🔙 رجوع":
        if step == "choose_count":
            context.user_data["step"] = "choose_pair"
            await update.message.reply_text("💱 اختر الزوج 👇", reply_markup=pairs_keyboard)
            return

        if step == "choose_interval":
            context.user_data["step"] = "choose_count"
            await update.message.reply_text("📈 اختر عدد الصفقات 👇", reply_markup=count_keyboard)
            return

        reset_signal_state(context)
        await update.message.reply_text(
            "↩️ رجعت للقائمة الرئيسية",
            reply_markup=build_main_menu_for_user(user.id)
        )
        return

    if text == "⬅️ رجوع":
        reset_signal_state(context)
        await update.message.reply_text(
            "↩️ رجعت للقائمة الرئيسية",
            reply_markup=build_main_menu_for_user(user.id)
        )
        return

    if text == "📊 توليد إشارات":
        reset_signal_state(context)
        context.user_data["step"] = "choose_pair"
        await update.message.reply_text("💱 اختر الزوج 👇", reply_markup=pairs_keyboard)
        return

    if text == "👤 حسابي":
        username = f"@{user.username}" if user.username else "ما عنده username"
        await update.message.reply_text(
            "👤 معلومات حسابك\n\n"
            f"• الاسم: {user.full_name}\n"
            f"• اليوزر: {username}\n"
            f"• الآيدي: {user.id}",
            reply_markup=build_main_menu_for_user(user.id)
        )
        return

    if text == "📞 تواصل مع المسؤول":
        await update.message.reply_text(
            f"📞 راسل المسؤول من هنا:\n{ADMIN_USERNAME}",
            reply_markup=build_main_menu_for_user(user.id)
        )
        return

    # ===== Admin panel =====
    if is_admin(user.id):
        if text == "🛠 لوحة الأدمن":
            reset_signal_state(context)
            await update.message.reply_text(
                "🛠 مرحبًا بك في لوحة الأدمن",
                reply_markup=admin_main_keyboard
            )
            return

        if text == "🟢 تشغيل البوت":
            set_bot_enabled(True)
            await update.message.reply_text("✅ تم تشغيل البوت للعامة", reply_markup=admin_main_keyboard)
            return

        if text == "🔴 إيقاف البوت":
            set_bot_enabled(False)
            await update.message.reply_text("🛠 تم إيقاف البوت للعامة", reply_markup=admin_main_keyboard)
            return

        if text == "📥 الطلبات المعلقة":
            data = get_all_pending_users()
            if not data:
                await update.message.reply_text("📭 لا يوجد طلبات معلقة حاليًا.", reply_markup=admin_main_keyboard)
                return

            lines = []
            for _, item in data.items():
                username = f"@{item.get('username')}" if item.get("username") else "بدون username"
                lines.append(
                    "📥 طلب تفعيل\n\n"
                    f"👤 الاسم: {item.get('name')}\n"
                    f"🔗 اليوزر: {username}\n"
                    f"🆔 Telegram ID: {item.get('telegram_id')}\n"
                    f"💱 Quotex ID: {item.get('quotex_id')}\n\n"
                    "──────────────"
                )

            await update.message.reply_text("\n\n".join(lines), reply_markup=admin_main_keyboard)
            return

        if text == "📋 كافة المستخدمين":
            users = get_all_users()
            if not users:
                await update.message.reply_text("📭 لا يوجد مستخدمون.", reply_markup=admin_main_keyboard)
                return

            lines = []
            for user_id, data in users.items():
                status = get_user_status(int(user_id))
                name = data.get("name", "غير معروف")
                username = f"@{data.get('username')}" if data.get("username") else "بدون username"
                lines.append(
                    f"👤 {name} | {username}\n"
                    f"🆔 ID: {user_id}\n"
                    f"📌 الحالة: {status}\n"
                    "──────────────"
                )

            msg = "\n".join(lines[:50])
            await update.message.reply_text(msg, reply_markup=admin_main_keyboard)
            return

        if text == "🟢 المستخدمون النشطون":
            active_users = get_recent_active_users()
            if not active_users:
                await update.message.reply_text(
                    "🕒 لا يوجد مستخدمون نشطون خلال آخر 15 دقيقة.",
                    reply_markup=admin_main_keyboard
                )
                return

            lines = []
            for user_id, data in active_users:
                name = data.get("name", "غير معروف")
                username = f"@{data.get('username')}" if data.get("username") else "بدون username"
                lines.append(
                    f"🟢 {name} | {username}\n"
                    f"🆔 ID: {user_id}\n"
                    f"⏰ آخر نشاط: {format_dt_ar(data.get('last_seen', ''))}\n"
                    "──────────────"
                )

            await update.message.reply_text("\n".join(lines[:50]), reply_markup=admin_main_keyboard)
            return

        if text == "🔍 تفاصيل مستخدم":
            context.user_data["step"] = "admin_waiting_user_id"
            await update.message.reply_text(
                "🔍 أرسل Telegram ID الخاص بالمستخدم لعرض تفاصيله.",
                reply_markup=admin_main_keyboard
            )
            return

        if step == "admin_waiting_user_id":
            try:
                target_id = int(text.strip())
            except ValueError:
                await update.message.reply_text("❌ أرسل Telegram ID صحيحًا.")
                return

            context.user_data["admin_target_id"] = target_id
            context.user_data["step"] = "admin_user_actions"
            await send_user_details(update, target_id, show_admin_actions=True)
            return

        if step == "admin_user_actions":
            target_id = context.user_data.get("admin_target_id")
            if not target_id:
                await update.message.reply_text("❌ لا يوجد مستخدم محدد.", reply_markup=admin_main_keyboard)
                context.user_data["step"] = None
                return

            if text == "🗓 أسبوع":
                set_user_expiry(target_id, "week")
                await update.message.reply_text("✅ تم تفعيل المستخدم لمدة أسبوع", reply_markup=admin_main_keyboard)
                try:
                    await context.bot.send_message(
                        chat_id=target_id,
                        text="✅ تم تفعيل حسابك بنجاح لمدة أسبوع\n\nأصبح بإمكانك الآن استخدام بوت TRADING TIME.\nاضغط /start للدخول إلى القائمة الرئيسية."
                    )
                except Exception:
                    pass
                context.user_data["step"] = None
                return

            if text == "🗓 شهر":
                set_user_expiry(target_id, "month")
                await update.message.reply_text("✅ تم تفعيل المستخدم لمدة شهر", reply_markup=admin_main_keyboard)
                try:
                    await context.bot.send_message(
                        chat_id=target_id,
                        text="✅ تم تفعيل حسابك بنجاح لمدة شهر\n\nأصبح بإمكانك الآن استخدام بوت TRADING TIME.\nاضغط /start للدخول إلى القائمة الرئيسية."
                    )
                except Exception:
                    pass
                context.user_data["step"] = None
                return

            if text == "♾ دائم":
                set_user_expiry(target_id, "forever")
                await update.message.reply_text("✅ تم تفعيل المستخدم بشكل دائم", reply_markup=admin_main_keyboard)
                try:
                    await context.bot.send_message(
                        chat_id=target_id,
                        text="✅ تم تفعيل حسابك بنجاح بشكل دائم\n\nأصبح بإمكانك الآن استخدام بوت TRADING TIME.\nاضغط /start للدخول إلى القائمة الرئيسية."
                    )
                except Exception:
                    pass
                context.user_data["step"] = None
                return

            if text == "⛔ إلغاء التفعيل":
                block_user(target_id)
                await update.message.reply_text("⛔ تم إلغاء تفعيل المستخدم", reply_markup=admin_main_keyboard)
                try:
                    await context.bot.send_message(
                        chat_id=target_id,
                        text="⛔ تم إلغاء تفعيل حسابك\n\nإذا كنت ترى أن هذا بالخطأ، تواصل مع الأدمن."
                    )
                except Exception:
                    pass
                context.user_data["step"] = None
                return

    # ===== Signal flow =====
    if step == "choose_pair":
        if text not in PAIRS:
            await update.message.reply_text("💱 اختر زوجًا من الأزرار 👇", reply_markup=pairs_keyboard)
            return

        context.user_data["pair"] = text
        context.user_data["step"] = "choose_count"
        await update.message.reply_text("📈 اختر عدد الصفقات 👇", reply_markup=count_keyboard)
        return

    if step == "choose_count":
        if text not in [str(x) for x in TRADE_COUNTS]:
            await update.message.reply_text("📈 اختر عدد الصفقات من الأزرار 👇", reply_markup=count_keyboard)
            return

        context.user_data["count"] = int(text)
        context.user_data["step"] = "choose_interval"
        await update.message.reply_text("⏳ اختر الفاصل الزمني 👇", reply_markup=interval_keyboard)
        return

    if step == "choose_interval":
        interval_map = {
            "1 دقيقة": 1,
            "3 دقائق": 3,
            "5 دقائق": 5,
        }

        if text not in interval_map:
            await update.message.reply_text("⏳ اختر الفاصل من الأزرار 👇", reply_markup=interval_keyboard)
            return

        context.user_data["interval"] = interval_map[text]

        pair = context.user_data["pair"]
        count = context.user_data["count"]
        interval_minutes = context.user_data["interval"]
        start_dt = now_utc()

        signals = generate_signals(pair, count, interval_minutes, start_dt)
        message_text = build_signals_message(pair, count, interval_minutes, signals)

        await update.message.reply_text(
            message_text,
            reply_markup=build_main_menu_for_user(user.id)
        )

        reset_signal_state(context)
        return

    await update.message.reply_text(
        "📌 اختر خيارًا من القائمة.",
        reply_markup=build_main_menu_for_user(user.id)
    )


# ===== App Runner =====
def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN غير موجود داخل ملف .env")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Bot is running...")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()