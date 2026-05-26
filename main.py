import os
import json
import hashlib
import asyncio
import random
import re
import requests
import threading
import time as time_module
from collections import deque
import logging
import traceback
try:
    import websocket
except Exception:
    websocket = None
from datetime import time
from datetime import datetime, timedelta, timezone
from statistics import median

import firebase_admin
from firebase_admin import credentials, db

from dotenv import load_dotenv
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

UTC = timezone.utc
UTC_PLUS_3 = timezone(timedelta(hours=3))

CHANNEL_ID = "@quotexsignals_tt"
GLOBAL_CHANNEL_ID = -1003918647685

# قناة الصفقات المباشرة الجديدة الخاصة بـ OTC Live
OTC_LIVE_CHANNEL_ID = int(os.getenv("OTC_LIVE_CHANNEL_ID", "-1003880574173"))
OTC_LIVE_CHANNEL_ENABLED = os.getenv("OTC_LIVE_CHANNEL_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
OTC_LIVE_MIN_QUALITY = int(os.getenv("OTC_LIVE_MIN_QUALITY", "65"))
OTC_LIVE_MIN_PAYOUT = int(os.getenv("OTC_LIVE_MIN_PAYOUT", "80"))
OTC_LIVE_REVERSE_AUTOPUBLISH = os.getenv("OTC_LIVE_REVERSE_AUTOPUBLISH", "true").lower() == "true"
OTC_LIVE_RESULT_EXTRA_DELAY_SECONDS = int(os.getenv("OTC_LIVE_RESULT_EXTRA_DELAY_SECONDS", "3"))
OTC_LIVE_MIN_ENTRY_LEAD_SECONDS = int(os.getenv("OTC_LIVE_MIN_ENTRY_LEAD_SECONDS", "10"))
OTC_LIVE_TIE_EPSILON = float(os.getenv("OTC_LIVE_TIE_EPSILON", "0.0000001"))
OTC_LIVE_SCAN_INTERVAL_SECONDS = int(os.getenv("OTC_LIVE_SCAN_INTERVAL_SECONDS", "5"))
OTC_LIVE_TRADE_DURATION_SECONDS = int(os.getenv("OTC_LIVE_TRADE_DURATION_SECONDS", "65"))
OTC_LIVE_COOLDOWN_SECONDS = int(os.getenv("OTC_LIVE_COOLDOWN_SECONDS", "60"))
OTC_LIVE_ACTIVE_TIMEOUT_SECONDS = int(os.getenv("OTC_LIVE_ACTIVE_TIMEOUT_SECONDS", "300"))
OTC_LIVE_ENTRY_SCAN_WINDOW_SECONDS = int(os.getenv("OTC_LIVE_ENTRY_SCAN_WINDOW_SECONDS", "20"))
OTC_LIVE_ENTRY_MIN_REMAINING_SECONDS = float(os.getenv("OTC_LIVE_ENTRY_MIN_REMAINING_SECONDS", "15"))
OTC_LIVE_ENTRY_MAX_REMAINING_SECONDS = float(os.getenv("OTC_LIVE_ENTRY_MAX_REMAINING_SECONDS", "20"))
OTC_LIVE_LEARNING_ENABLED = os.getenv("OTC_LIVE_LEARNING_ENABLED", "true").lower() == "true"
OTC_LIVE_PAIR_LOSS_LOOKBACK = int(os.getenv("OTC_LIVE_PAIR_LOSS_LOOKBACK", "10"))
OTC_LIVE_PAIR_LOSS_LIMIT = int(os.getenv("OTC_LIVE_PAIR_LOSS_LIMIT", "2"))
OTC_LIVE_PAIR_COOLDOWN_MINUTES = int(os.getenv("OTC_LIVE_PAIR_COOLDOWN_MINUTES", "30"))
OTC_LIVE_CAUTION_LOOKBACK = int(os.getenv("OTC_LIVE_CAUTION_LOOKBACK", "15"))
OTC_LIVE_CAUTION_MIN_QUALITY_BOOST = int(os.getenv("OTC_LIVE_CAUTION_MIN_QUALITY_BOOST", "8"))
OTC_LIST_NO_DATA_RETRY_SECONDS = int(os.getenv("OTC_LIST_NO_DATA_RETRY_SECONDS", "15"))
OTC_LIST_NO_DATA_MAX_RETRIES = int(os.getenv("OTC_LIST_NO_DATA_MAX_RETRIES", "6"))
OTC_LIVE_SMART_MARTINGALE_ENABLED = os.getenv("OTC_LIVE_SMART_MARTINGALE_ENABLED", "true").lower() == "true"
OTC_LIVE_MARTINGALE_DECISION_SECONDS_BEFORE_CLOSE = int(os.getenv("OTC_LIVE_MARTINGALE_DECISION_SECONDS_BEFORE_CLOSE", "8"))
OTC_LIVE_MARTINGALE_ADVICE_CHECK_SECONDS = [12, 8, 5, 3]
OTC_LIVE_ADAPTIVE_FILTER_ENABLED = os.getenv("OTC_LIVE_ADAPTIVE_FILTER_ENABLED", "true").lower() == "true"
OTC_LIVE_PAIR_RECENT_LIMIT = int(os.getenv("OTC_LIVE_PAIR_RECENT_LIMIT", "20"))
OTC_LIVE_PAIR_MAX_RECENT_LOSSES = int(os.getenv("OTC_LIVE_PAIR_MAX_RECENT_LOSSES", "5"))
OTC_LIVE_DIRECTION_RECENT_LIMIT = int(os.getenv("OTC_LIVE_DIRECTION_RECENT_LIMIT", "15"))
OTC_LIVE_DIRECTION_MAX_RECENT_LOSSES = int(os.getenv("OTC_LIVE_DIRECTION_MAX_RECENT_LOSSES", "4"))
OTC_LIVE_PAIR_MAX_RECENT_NEGATIVE_UNITS = float(os.getenv("OTC_LIVE_PAIR_MAX_RECENT_NEGATIVE_UNITS", "-8.0"))
OTC_LIVE_RESULT_DELAY_SECONDS = int(os.getenv("OTC_LIVE_RESULT_DELAY_SECONDS", "8"))

ADMIN_USERNAME = "@coach_WAEL_trading"
ADMIN_TELEGRAM_ID = 1582593617

DATABASE_URL = "https://telegram-bot-f0229-default-rtdb.firebaseio.com"

WELCOME_MESSAGE = """
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

OTC_PAIRS = [
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

CHANNEL_OTC_PAIRS = [
    "USD/BRL (OTC)",
    "USD/ARS (OTC)",
    "USD/BDT (OTC)",
]
CHANNEL_DAILY_SIGNAL_COUNT = 35
CHANNEL_SIGNAL_INTERVAL_MINUTES = 3


# ===== Quotex OTC live websocket settings =====
# ضع ملف cookies.txt بجانب main.py. الملف يجب أن يحتوي cookies جلسة Quotex بسطر واحد.
QUOTEX_COOKIE_FILE = os.getenv("QUOTEX_COOKIE_FILE", "cookies.txt")
QUOTEX_WS_URL = "wss://ws2.qxbroker.com/socket.io/?EIO=4&transport=websocket"
QUOTEX_USER_AGENT = os.getenv(
    "QUOTEX_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/148.0.0.0 Safari/537.36"
)

OTC_PAIR_TO_QUOTEX_SYMBOL = {
    "USD/BRL (OTC)": "BRLUSD_otc",
    "USD/ARS (OTC)": "ARSUSD_otc",
    "USD/BDT (OTC)": "BDTUSD_otc",
    "USD/NGN (OTC)": "NGNUSD_otc",
    "USD/PKR (OTC)": "PKRUSD_otc",
    "USD/DZD (OTC)": "DZDUSD_otc",
    "USD/MXN (OTC)": "MXNUSD_otc",
    "USD/INR (OTC)": "INRUSD_otc",
    "USD/IDR (OTC)": "IDRUSD_otc",
    "USD/EGP (OTC)": "EGPUSD_otc",
    "USD/TRY (OTC)": "TRYUSD_otc",
    "USD/COP (OTC)": "COPUSD_otc",
    "EUR/JPY (OTC)": "EURJPY_otc",
    "EUR/USD (OTC)": "EURUSD_otc",
    "CAD/CHF (OTC)": "CADCHF_otc",
    "CAD/JPY (OTC)": "CADJPY_otc",
    "AUD/CHF (OTC)": "AUDCHF_otc",
    "AUD/CAD (OTC)": "AUDCAD_otc",
    "AUD/NZD (OTC)": "AUDNZD_otc",
    "NZD/CAD (OTC)": "NZDCAD_otc",
    "USD/ZAR (OTC)": "ZARUSD_otc",
    "USD/PHP (OTC)": "PHPUSD_otc",
}


REAL_PAIRS = [
    "EUR/USD",
    "GBP/USD",
    "USD/JPY",
    "USD/CHF",
    "USD/CAD",
    "AUD/USD",
    "NZD/USD",
    "EUR/JPY",
    "AUD/JPY",
    "EUR/GBP",
    "CAD/JPY",
    "EUR/CAD",
    "AUD/CHF",
    "CHF/CAD",
    "AUD/CAD",
    "GBP/AUD",
]

REAL_PAIR_TO_YAHOO_SYMBOL = {
    "EUR/USD": "EURUSD=X",
    "GBP/USD": "GBPUSD=X",
    "USD/JPY": "USDJPY=X",
    "USD/CHF": "USDCHF=X",
    "USD/CAD": "USDCAD=X",
    "AUD/USD": "AUDUSD=X",
    "NZD/USD": "NZDUSD=X",
    "EUR/JPY": "EURJPY=X",
    "AUD/JPY": "AUDJPY=X",
    "EUR/GBP": "EURGBP=X",
    "CAD/JPY": "CADJPY=X",
    "EUR/CAD": "EURCAD=X",
    "AUD/CHF": "AUDCHF=X",
    "CHF/CAD": "CHFCAD=X",
    "AUD/CAD": "AUDCAD=X",
    "GBP/AUD": "GBPAUD=X",
}

REAL_PAIR_TO_TV_SYMBOL = {
    "EUR/USD": "FX_IDC:EURUSD",
    "GBP/USD": "FX_IDC:GBPUSD",
    "USD/JPY": "FX_IDC:USDJPY",
    "USD/CHF": "FX_IDC:USDCHF",
    "USD/CAD": "FX_IDC:USDCAD",
    "AUD/USD": "FX_IDC:AUDUSD",
    "NZD/USD": "FX_IDC:NZDUSD",
    "EUR/JPY": "FX_IDC:EURJPY",
    "AUD/JPY": "FX_IDC:AUDJPY",
    "EUR/GBP": "FX_IDC:EURGBP",
    "CAD/JPY": "FX_IDC:CADJPY",
    "EUR/CAD": "FX_IDC:EURCAD",
    "AUD/CHF": "FX_IDC:AUDCHF",
    "CHF/CAD": "FX_IDC:CHFCAD",
    "AUD/CAD": "FX_IDC:AUDCAD",
    "GBP/AUD": "FX_IDC:GBPAUD",
}

TRADINGVIEW_WS_URL = "wss://data.tradingview.com/socket.io/websocket"
TRADINGVIEW_RESULT_RETRY_SECONDS = 30
GLOBAL_MIN_CONFIDENCE = int(os.getenv("GLOBAL_MIN_CONFIDENCE", "78"))
GLOBAL_MIN_QUALITY = int(os.getenv("GLOBAL_MIN_QUALITY", "85"))
GLOBAL_PAIR_COOLDOWN_MINUTES = int(os.getenv("GLOBAL_PAIR_COOLDOWN_MINUTES", "20"))
GLOBAL_MAX_RESULT_RETRIES = int(os.getenv("GLOBAL_MAX_RESULT_RETRIES", "40"))
HTTP_RETRY_ATTEMPTS = int(os.getenv("HTTP_RETRY_ATTEMPTS", "3"))
HTTP_RETRY_BACKOFF_SECONDS = float(os.getenv("HTTP_RETRY_BACKOFF_SECONDS", "0.8"))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("trading_time_bot")

# نسب تقريبية حتى لا تكون مسافات الدعم/المقاومة والـ Round Numbers ثابتة لكل الأزواج.
# السبب: الأزواج مثل USD/JPY تتحرك بعدد خانات مختلف عن EUR/USD.
PAIR_CONTEXT = {
    "EUR/USD": {"round_step": 0.0010, "near_factor": 0.18, "touch_factor": 0.07},
    "GBP/USD": {"round_step": 0.0010, "near_factor": 0.20, "touch_factor": 0.08},
    "USD/JPY": {"round_step": 0.10, "near_factor": 0.18, "touch_factor": 0.07},
    "USD/CHF": {"round_step": 0.0010, "near_factor": 0.18, "touch_factor": 0.07},
    "USD/CAD": {"round_step": 0.0010, "near_factor": 0.18, "touch_factor": 0.07},
    "AUD/USD": {"round_step": 0.0010, "near_factor": 0.20, "touch_factor": 0.08},
    "NZD/USD": {"round_step": 0.0010, "near_factor": 0.20, "touch_factor": 0.08},
    "EUR/JPY": {"round_step": 0.10, "near_factor": 0.18, "touch_factor": 0.07},
    "AUD/JPY": {"round_step": 0.10, "near_factor": 0.20, "touch_factor": 0.08},
    "CAD/JPY": {"round_step": 0.10, "near_factor": 0.18, "touch_factor": 0.07},
    "EUR/GBP": {"round_step": 0.0010, "near_factor": 0.18, "touch_factor": 0.07},
    "EUR/CAD": {"round_step": 0.0010, "near_factor": 0.18, "touch_factor": 0.07},
    "AUD/CHF": {"round_step": 0.0010, "near_factor": 0.20, "touch_factor": 0.08},
    "CHF/CAD": {"round_step": 0.0010, "near_factor": 0.18, "touch_factor": 0.07},
    "AUD/CAD": {"round_step": 0.0010, "near_factor": 0.20, "touch_factor": 0.08},
    "GBP/AUD": {"round_step": 0.0010, "near_factor": 0.20, "touch_factor": 0.08},
}

TRADE_COUNTS = [3, 5, 10, 15, 20]
INTERVALS = [1, 3, 5]
REAL_INTERVALS = [1, 5, 10]
# للنشر التلقائي في قناة السوق العالمي:
# 1M له الأولوية دائمًا. أما 5M/10M فلا يتم نشرها إلا عندما يقترب إغلاق الشمعة الحالية،
# حتى لا يكون التحليل قديمًا قبل وقت الدخول.
GLOBAL_AUTOPUBLISH_PRIMARY_TIMEFRAMES = [1]
GLOBAL_AUTOPUBLISH_SECONDARY_TIMEFRAMES = [5, 10]
GLOBAL_SECONDARY_TIMEFRAME_MAX_LEAD_SECONDS = 70
GLOBAL_MARKET_AUTOPUBLISH_START_HOUR_UTC_PLUS_3 = 10
GLOBAL_MARKET_AUTOPUBLISH_END_HOUR_UTC_PLUS_3 = 21

# قناة السوق العالمي لا تتحول إلى OTC إطلاقًا.
# حسب سلوك Quotex الذي ظهر عندك: عند توقف السوق العالمي تصبح الأزواج OTC،
# لذلك نوقف النشر العالمي من الجمعة 17:00 UTC+3 حتى الاثنين 00:00 UTC+3.
# إذا تغيّر وقت إغلاق/فتح Quotex لاحقًا، عدّل هذه القيم فقط.
QUOTEX_GLOBAL_FRIDAY_CLOSE_HOUR_UTC_PLUS_3 = 17
QUOTEX_GLOBAL_MONDAY_OPEN_HOUR_UTC_PLUS_3 = 0
GLOBAL_MARKET_CLOSED_MESSAGE_ENABLED = True
GLOBAL_MARKET_CLOSED_MESSAGE = (
    "🌍 السوق العالمي مغلق الآن\n\n"
    "تم إيقاف نشر صفقات السوق العالمي مؤقتًا لأن الأزواج على منصة Quotex تحولت إلى OTC.\n"
    "سيعود النشر تلقائيًا عند فتح السوق العالمي من جديد."
)

ONLINE_MINUTES_WINDOW = 15

# ===== Firebase init =====
firebase_json = os.getenv("FIREBASE_CREDENTIALS_JSON")

if firebase_json:
    cred_dict = json.loads(firebase_json)
    cred = credentials.Certificate(cred_dict)
else:
    cred = credentials.Certificate("serviceAccountKey.json")

if not firebase_admin._apps:
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
        ["📡 قنوات البوت", "📢 رسالة جماعية"],
        ["⬅️ رجوع"],
    ],
    resize_keyboard=True
)

admin_channels_keyboard = ReplyKeyboardMarkup(
    [
        ["🌍 تشغيل نشر العالمي", "🌍 إيقاف نشر العالمي"],
        ["⚡ تشغيل نشر OTC", "⚡ إيقاف نشر OTC"],
        ["🔥 تشغيل OTC مباشر", "🔥 إيقاف OTC مباشر"],
        ["📊 حالة النشر", "📊 إحصائيات قناة OTC"],
        ["⬅️ رجوع"],
    ],
    resize_keyboard=True
)

admin_otc_stats_keyboard = ReplyKeyboardMarkup(
    [
        ["📈 إحصائيات OTC مباشر", "🔢 إحصائيات آخر عدد صفقات"],
        ["🤖 تحليل ونصائح البوت", "🧹 تصفير إحصائيات OTC"],
        ["🧠 حالة تعلم OTC Live", "🔎 فحص بيانات زوج OTC"],
        ["🕯️ فحص شمعة OTC"],
        ["🧾 فحص ليستة OTC", "📋 عرض نتائج الليستة"],
        ["⬅️ رجوع"],
    ],
    resize_keyboard=True
)

admin_otc_list_ready_keyboard = ReplyKeyboardMarkup(
    [
        ["📋 عرض نتائج الليستة"],
        ["📊 إحصائيات قناة OTC"],
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

market_mode_keyboard = ReplyKeyboardMarkup(
    [
        ["⚡ OTC", "🌍 سوق عالمي"],
        ["🔙 رجوع"],
    ],
    resize_keyboard=True
)

otc_mode_keyboard = ReplyKeyboardMarkup(
    [
        ["🕒 زمني", "⚡ صفقة مباشرة"],
        ["🔙 رجوع"],
    ],
    resize_keyboard=True
)

otc_live_search_keyboard = ReplyKeyboardMarkup(
    [
        ["🔎 ابحث عن صفقة الآن"],
        ["🔙 رجوع"],
    ],
    resize_keyboard=True
)

otc_pairs_keyboard = ReplyKeyboardMarkup(
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

real_pairs_keyboard = ReplyKeyboardMarkup(
    [
        ["EUR/USD", "GBP/USD"],
        ["USD/JPY", "USD/CHF"],
        ["USD/CAD", "AUD/USD"],
        ["NZD/USD", "EUR/JPY"],
        ["AUD/JPY", "EUR/GBP"],
        ["CAD/JPY", "EUR/CAD"],
        ["AUD/CHF", "CHF/CAD"],
        ["AUD/CAD", "GBP/AUD"],
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

real_interval_keyboard = ReplyKeyboardMarkup(
    [
        ["1 دقيقة", "5 دقائق"],
        ["10 دقائق", "🔥 أفضل فرصة"],
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


def channel_publish_ref():
    return system_ref().child("channel_publish")


def get_channel_publish_settings():
    data = channel_publish_ref().get() or {}
    return {
        "global": bool(data.get("global", True)),
        "otc": bool(data.get("otc", True)),
        "otc_live": bool(data.get("otc_live", True)),
    }


def is_channel_publish_enabled(channel_key: str) -> bool:
    return bool(get_channel_publish_settings().get(channel_key, True))


def set_channel_publish_enabled(channel_key: str, enabled: bool):
    channel_publish_ref().update({
        channel_key: bool(enabled),
        f"{channel_key}_updated_at": now_iso(),
    })


def format_channel_publish_status() -> str:
    settings = get_channel_publish_settings()
    global_status = "شغال ✅" if settings.get("global", True) else "متوقف ⛔"
    otc_status = "شغال ✅" if settings.get("otc", True) else "متوقف ⛔"
    otc_live_status = "شغال ✅" if settings.get("otc_live", True) else "متوقف ⛔"
    return (
        "📊 حالة نشر القنوات\n\n"
        f"🌍 قناة السوق العالمي: {global_status}\n"
        f"⚡ قناة OTC الزمني: {otc_status}\n"
        f"🔥 قناة OTC المباشر: {otc_live_status}"
    )


# ===== Helpers =====
def now_utc():
    return datetime.now(UTC)


def now_iso():
    return now_utc().isoformat()


def parse_iso(value: str):
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None



def safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def request_json_with_retries(url: str, *, params=None, headers=None, timeout: int = 10, attempts: int | None = None):
    attempts = attempts or HTTP_RETRY_ATTEMPTS
    last_error = None

    for attempt in range(1, attempts + 1):
        try:
            res = requests.get(url, params=params, headers=headers, timeout=timeout)
            if res.status_code == 200:
                return res.json(), None

            last_error = f"API status={res.status_code}"
            if res.status_code < 500 and res.status_code not in {408, 429}:
                break

        except Exception as e:
            last_error = str(e)

        if attempt < attempts:
            delay = HTTP_RETRY_BACKOFF_SECONDS * attempt
            logger.warning("HTTP retry %s/%s for %s after error: %s", attempt, attempts, url, last_error)
            try:
                import time as _time
                _time.sleep(delay)
            except Exception:
                pass

    return None, last_error or "تعذر جلب البيانات"


async def safe_send_message(bot, *, chat_id, text: str, parse_mode: str | None = None, reply_markup=None):
    try:
        return await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
        )
    except Exception as e:
        logger.exception("Telegram send_message failed | chat_id=%s | error=%s", chat_id, e)
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


def get_all_approved_users():
    return approved_ref().get() or {}


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
    remove_pending_user(user_id)


def get_recent_active_approved_users(minutes: int = ONLINE_MINUTES_WINDOW):
    approved_users = get_all_approved_users()
    all_users = get_all_users()

    result = []
    cutoff = now_utc() - timedelta(minutes=minutes)

    for user_id_str in approved_users.keys():
        try:
            user_id = int(user_id_str)
        except ValueError:
            continue

        if not is_approved(user_id):
            continue

        user_data = all_users.get(user_id_str)
        if not user_data:
            continue

        last_seen = parse_iso(user_data.get("last_seen", ""))
        if last_seen and last_seen >= cutoff:
            result.append((user_id_str, user_data))

    result.sort(key=lambda x: x[1].get("last_seen", ""), reverse=True)
    return result


def format_utc_plus_3(dt: datetime) -> str:
    return dt.astimezone(UTC_PLUS_3).strftime("%H:%M")


def next_full_minute(dt: datetime) -> datetime:
    dt = dt.astimezone(UTC_PLUS_3)
    dt = dt.replace(second=0, microsecond=0)
    return (dt + timedelta(minutes=1)).astimezone(UTC)


def next_timeframe_boundary(dt: datetime, timeframe_minutes: int) -> datetime:
    """يرجع بداية الشمعة القادمة للفريم المطلوب بدل now + timeframe.
    مثال: لو الآن 19:47 والفريم 10M → يرجع 19:50 أو 20:00 حسب تجاوز الحد.
    نعتمد UTC+3 لأن العرض للمستخدم وكل التوقيتات في البوت مبنية عليه.
    """
    dt_local = dt.astimezone(UTC_PLUS_3).replace(second=0, microsecond=0)
    floored_minute = dt_local.minute - (dt_local.minute % timeframe_minutes)
    current_boundary = dt_local.replace(minute=floored_minute)
    if current_boundary <= dt_local:
        current_boundary += timedelta(minutes=timeframe_minutes)
    return current_boundary.astimezone(UTC)


def seconds_until_timeframe_boundary(dt: datetime, timeframe_minutes: int) -> float:
    """عدد الثواني المتبقية لبداية شمعة الفريم القادمة."""
    boundary = next_timeframe_boundary(dt, timeframe_minutes)
    return max(0.0, (boundary - dt.astimezone(UTC)).total_seconds())


def can_autopublish_timeframe(timeframe_minutes: int, check_dt: datetime | None = None) -> bool:
    """
    1M مسموح دائمًا لأنه أصلًا قريب من الدخول.
    5M/10M مسموحة فقط قبل إغلاق الشمعة الحالية بحوالي دقيقة،
    حتى لا يتم نشر صفقة 5M/10M مبكرًا والتحليل يتغير قبل وقت الدخول.
    """
    if timeframe_minutes in GLOBAL_AUTOPUBLISH_PRIMARY_TIMEFRAMES:
        return True

    if timeframe_minutes not in GLOBAL_AUTOPUBLISH_SECONDARY_TIMEFRAMES:
        return False

    remaining_seconds = seconds_until_timeframe_boundary(check_dt or now_utc(), timeframe_minutes)
    return 0 < remaining_seconds <= GLOBAL_SECONDARY_TIMEFRAME_MAX_LEAD_SECONDS



# ===== Quotex OTC Live Feed =====
class QuotexOTCLiveFeed:
    """اتصال WebSocket خام مع Quotex للحصول على quotes/stream.
    الأمر الصحيح للاشتراك الذي تم اختباره: 42["instruments/follow", "BRLUSD_otc"]
    """

    def __init__(self, symbols: list[str]):
        self.symbols = list(dict.fromkeys([s for s in symbols if s]))
        self.ws = None
        self.connected = False
        self.started = False
        self.lock = threading.Lock()
        self.prices = {symbol: deque(maxlen=3000) for symbol in self.symbols}
        self.candles = {symbol: {} for symbol in self.symbols}  # bucket_ts -> OHLC from live Quotex ticks
        self.last_tick = {}
        self.instruments = {}
        self.thread = None

    def start(self):
        if self.started:
            return
        self.started = True
        self.thread = threading.Thread(target=self._run_forever, daemon=True, name="quotex_otc_live_feed")
        self.thread.start()

    def _load_cookies(self) -> str | None:
        try:
            env_cookies = os.getenv("QUOTEX_COOKIES", "").strip()
            if env_cookies:
                return env_cookies

            cookie_path = os.path.abspath(QUOTEX_COOKIE_FILE)
            if not os.path.exists(cookie_path):
                logger.warning("Quotex cookies not found. Set QUOTEX_COOKIES env var or add file: %s", cookie_path)
                return None

            cookies = open(cookie_path, "r", encoding="utf-8").read().strip()
            if not cookies:
                logger.warning("Quotex cookies file is empty: %s", cookie_path)
                return None

            return cookies
        except Exception as e:
            logger.exception("Could not read Quotex cookies: %s", e)
            return None

    def _run_forever(self):
        if websocket is None:
            logger.warning("websocket-client غير مثبت، لن يعمل بث OTC المباشر")
            return

        while True:
            cookies = self._load_cookies()
            if not cookies:
                time_module.sleep(30)
                continue

            headers = [
                f"Cookie: {cookies}",
                f"User-Agent: {QUOTEX_USER_AGENT}",
                "Origin: https://qxbroker.com",
                "Referer: https://qxbroker.com/en/trade",
                "Accept-Language: ar-TR,ar;q=0.9,en-TR;q=0.8,en;q=0.7,tr-TR;q=0.6,tr;q=0.5,en-US;q=0.4",
                "Cache-Control: no-cache",
                "Pragma: no-cache",
            ]

            try:
                logger.info("Starting Quotex OTC live websocket for symbols: %s", ", ".join(self.symbols))
                ws_app = websocket.WebSocketApp(
                    QUOTEX_WS_URL,
                    header=headers,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                ws_app.run_forever(ping_interval=0, ping_timeout=None)
            except Exception as e:
                logger.exception("Quotex OTC websocket crashed: %s", e)

            self.connected = False
            time_module.sleep(5)

    def _send_raw(self, packet: str):
        try:
            if self.ws:
                self.ws.send(packet)
        except Exception as e:
            logger.warning("Quotex raw send failed: %s", e)

    def _send_event(self, event_name: str, payload):
        packet = "42" + json.dumps([event_name, payload], separators=(",", ":"))
        self._send_raw(packet)

    def _keepalive_loop(self):
        while self.connected:
            time_module.sleep(10)
            if self.connected:
                self._send_raw("3")  # Engine.IO pong

    def _subscribe_loop(self):
        time_module.sleep(2)
        # نطلب قائمة الأدوات حتى نعرف payout وحالة OTC قبل النشر
        self._send_event("instruments/list", [])
        time_module.sleep(1)

        for symbol in self.symbols:
            if not self.connected:
                break
            logger.info("Following Quotex OTC symbol: %s", symbol)
            self._send_event("instruments/follow", symbol)
            time_module.sleep(0.4)

    def _on_open(self, ws):
        self.ws = ws
        self.connected = True
        logger.info("Quotex OTC websocket opened")
        self._send_raw("40")  # Socket.IO default namespace
        threading.Thread(target=self._keepalive_loop, daemon=True).start()

    def _on_message(self, ws, message):
        try:
            if isinstance(message, bytes):
                self._parse_quote_binary(message)
                return

            if message == "2":
                ws.send("3")
                return

            if isinstance(message, str) and message.startswith("40"):
                logger.info("Quotex OTC Socket.IO namespace connected")
                threading.Thread(target=self._subscribe_loop, daemon=True).start()
                return
        except Exception as e:
            logger.exception("Quotex message handling error: %s", e)

    def _parse_quote_binary(self, message: bytes):
        text = message.decode("utf-8", errors="ignore")
        start = text.find("[[")
        if start == -1:
            return

        try:
            data = json.loads(text[start:])
        except Exception:
            return

        if not isinstance(data, list):
            return

        # quotes/stream rows شكلها:
        # ["BRLUSD_otc", timestamp, price, flag]
        # instruments/list rows شكلها:
        # [id, "BRLUSD_otc", "USD/BRL (OTC)", ..., payout, ..., is_otc, ...]
        with self.lock:
            for row in data:
                if not isinstance(row, list) or len(row) < 3:
                    continue

                # instruments/list
                if len(row) >= 15 and isinstance(row[1], str) and row[1].endswith("_otc"):
                    try:
                        symbol = str(row[1])
                        name = str(row[2])
                        payout = int(float(row[5]))
                        is_otc = bool(row[14])
                    except Exception:
                        continue

                    self.instruments[symbol] = {
                        "symbol": symbol,
                        "name": name,
                        "payout": payout,
                        "is_otc": is_otc,
                        "updated_at": now_iso(),
                    }
                    continue

                # quotes/stream
                if len(row) < 4:
                    continue

                symbol = row[0]
                if symbol not in self.prices:
                    continue

                try:
                    ts = float(row[1])
                    price = float(row[2])
                    flag = int(row[3])
                except Exception:
                    continue

                self.prices[symbol].append((ts, price, flag))

                bucket_ts = int(ts // 60) * 60
                symbol_candles = self.candles.setdefault(symbol, {})
                candle = symbol_candles.get(bucket_ts)

                if candle is None:
                    candle = {
                        "symbol": symbol,
                        "bucket_ts": bucket_ts,
                        "open": price,
                        "high": price,
                        "low": price,
                        "close": price,
                        "open_tick_ts": ts,
                        "close_tick_ts": ts,
                        "ticks": 1,
                    }
                    symbol_candles[bucket_ts] = candle
                else:
                    candle["high"] = max(float(candle.get("high", price)), price)
                    candle["low"] = min(float(candle.get("low", price)), price)
                    candle["close"] = price
                    candle["close_tick_ts"] = ts
                    candle["ticks"] = int(candle.get("ticks", 0)) + 1

                if len(symbol_candles) > 240:
                    for old_bucket in sorted(symbol_candles.keys())[:-200]:
                        symbol_candles.pop(old_bucket, None)

                self.last_tick[symbol] = {
                    "symbol": symbol,
                    "time": ts,
                    "price": price,
                    "flag": flag,
                    "received_at": now_iso(),
                }

    def instrument(self, symbol: str):
        with self.lock:
            return dict(self.instruments.get(symbol) or {})

    def _on_error(self, ws, error):
        logger.warning("Quotex OTC websocket error: %s", error)

    def _on_close(self, ws, close_status_code, close_msg):
        self.connected = False
        logger.warning("Quotex OTC websocket closed | code=%s | msg=%s", close_status_code, close_msg)

    def snapshot(self, symbol: str):
        with self.lock:
            return dict(self.last_tick.get(symbol) or {})

    def candle(self, symbol: str, entry_ts: float):
        """يرجع شمعة M1 التي تبدأ عند entry_ts من الكاش المبني لحظيًا."""
        bucket_ts = int(float(entry_ts) // 60) * 60
        with self.lock:
            candle = (self.candles.get(symbol) or {}).get(bucket_ts)
            return dict(candle) if candle else {}

    def direction(self, symbol: str) -> str | None:
        """اتجاه بسيط من آخر الأسعار: CALL إذا آخر سعر أعلى من بداية العينة، PUT إذا أقل."""
        with self.lock:
            rows = list(self.prices.get(symbol, []))

        if len(rows) < 6:
            return None

        # نستخدم آخر عدة ticks، ونتجاهل التذبذب الضعيف جدًا.
        sample = rows[-8:] if len(rows) >= 8 else rows
        first_price = sample[0][1]
        last_price = sample[-1][1]
        up_moves = sum(1 for a, b in zip(sample, sample[1:]) if b[1] > a[1])
        down_moves = sum(1 for a, b in zip(sample, sample[1:]) if b[1] < a[1])

        if last_price > first_price and up_moves >= down_moves:
            return "CALL"
        if last_price < first_price and down_moves >= up_moves:
            return "PUT"
        return None


OTC_ALL_POSSIBLE_QUOTEX_SYMBOLS = []
for _pair_key, _mapped_symbol in OTC_PAIR_TO_QUOTEX_SYMBOL.items():
    if _mapped_symbol and _mapped_symbol not in OTC_ALL_POSSIBLE_QUOTEX_SYMBOLS:
        OTC_ALL_POSSIBLE_QUOTEX_SYMBOLS.append(_mapped_symbol)
    try:
        _base_symbol = _pair_key.replace(" (OTC)", "").replace("/", "").upper()
        if len(_base_symbol) == 6:
            for _symbol_candidate in (
                f"{_base_symbol}_otc",
                f"{_base_symbol[3:]}{_base_symbol[:3]}_otc",
            ):
                if _symbol_candidate not in OTC_ALL_POSSIBLE_QUOTEX_SYMBOLS:
                    OTC_ALL_POSSIBLE_QUOTEX_SYMBOLS.append(_symbol_candidate)
    except Exception:
        pass

quotex_otc_feed = QuotexOTCLiveFeed(OTC_ALL_POSSIBLE_QUOTEX_SYMBOLS)


def start_quotex_otc_feed():
    try:
        quotex_otc_feed.start()
    except Exception as e:
        logger.exception("Could not start Quotex OTC live feed: %s", e)


def get_live_otc_direction(pair: str, fallback_dt: datetime | None = None) -> str:
    symbol = OTC_PAIR_TO_QUOTEX_SYMBOL.get(pair)
    if symbol:
        live_direction = quotex_otc_feed.direction(symbol)
        if live_direction in {"CALL", "PUT"}:
            return live_direction
    return get_stable_direction(pair, fallback_dt or now_utc())


def get_live_otc_snapshot(pair: str) -> dict:
    symbol = OTC_PAIR_TO_QUOTEX_SYMBOL.get(pair)
    if not symbol:
        return {}
    return quotex_otc_feed.snapshot(symbol)


def analyze_best_live_otc_now() -> dict:
    """يفحص كل أزواج OTC من بث Quotex live ويختار أفضل فرصة M1 حالية.
    لا يغيّر نظام الليستات الزمني، ويُستخدم فقط في خيار: صفقة مباشرة.
    """
    candidates = []
    now_ts = time_module.time()

    for pair, symbol in OTC_PAIR_TO_QUOTEX_SYMBOL.items():
        with quotex_otc_feed.lock:
            rows = list(quotex_otc_feed.prices.get(symbol, []))
            tick = dict(quotex_otc_feed.last_tick.get(symbol) or {})

        if len(rows) < 10 or not tick:
            continue

        try:
            last_exchange_ts = float(tick.get("time", 0))
            current_price = float(tick.get("price"))
        except Exception:
            continue

        instrument = quotex_otc_feed.instrument(symbol)
        payout = int(instrument.get("payout", 0) or 0)
        is_otc = bool(instrument.get("is_otc", True))

        if instrument and (not is_otc or payout < OTC_LIVE_MIN_PAYOUT):
            continue

        # آخر 12 تيك تقريبًا تعطي قراءة سريعة لفريم الدقيقة بدون انتظار طويل.
        sample = rows[-12:] if len(rows) >= 12 else rows
        prices = [float(r[1]) for r in sample]
        first_price = prices[0]
        last_price = prices[-1]
        price_range = max(prices) - min(prices)
        change = last_price - first_price

        up_moves = sum(1 for a, b in zip(prices, prices[1:]) if b > a)
        down_moves = sum(1 for a, b in zip(prices, prices[1:]) if b < a)
        flat_moves = max(0, (len(prices) - 1) - up_moves - down_moves)

        if change > 0 and up_moves >= down_moves:
            direction = "CALL"
            consistency = up_moves / max(1, up_moves + down_moves + flat_moves)
        elif change < 0 and down_moves >= up_moves:
            direction = "PUT"
            consistency = down_moves / max(1, up_moves + down_moves + flat_moves)
        else:
            continue

        if price_range <= 0:
            continue

        momentum = min(abs(change) / price_range, 1.0)
        movement_density = (up_moves + down_moves) / max(1, len(prices) - 1)

        # فلترة بسيطة: نتجنب زوجًا ثابتًا جدًا أو متذبذبًا بلا اتجاه.
        score = int(round((momentum * 45) + (consistency * 40) + (movement_density * 15)))

        if score < 55:
            continue

        candidates.append({
            "pair": pair,
            "symbol": symbol,
            "direction": direction,
            "score": score,
            "price": current_price,
            "exchange_time": last_exchange_ts,
            "change": change,
            "moves": {"up": up_moves, "down": down_moves, "flat": flat_moves},
            "sample_size": len(sample),
            "payout": payout,
        })

    if not candidates:
        return {
            "ok": False,
            "message": (
                "⚡ صفقة مباشرة OTC\n\n"
                "❌ لا توجد فرصة واضحة الآن على فريم الدقيقة.\n\n"
                "انتظر 30-60 ثانية ثم اضغط:\n"
                "🔎 ابحث عن صفقة الآن"
            )
        }

    ranked_candidates = sorted(candidates, key=lambda x: (x["score"], abs(x["change"])), reverse=True)

    best = None
    for candidate in ranked_candidates:
        effective_direction = candidate.get("direction")
        if OTC_LIVE_REVERSE_AUTOPUBLISH:
            if effective_direction == "CALL":
                effective_direction = "PUT"
            elif effective_direction == "PUT":
                effective_direction = "CALL"

        blocked, reason = is_otc_live_candidate_blocked(candidate.get("pair"), effective_direction)
        if blocked:
            logger.info(
                "OTC LIVE adaptive filter skipped candidate | pair=%s | direction=%s | reason=%s",
                candidate.get("pair"),
                effective_direction,
                reason,
            )
            continue

        best = candidate
        break

    if best is None:
        # Soft fallback:
        # إذا الفلتر منع كل المرشحين، لا نوقف البوت كليًا.
        # نأخذ أفضل فرصة خام حتى يبقى النشر شغال، لكن نسجل ذلك في اللوج.
        best = ranked_candidates[0]
        logger.info(
            "OTC LIVE adaptive soft fallback used | pair=%s | score=%s",
            best.get("pair"),
            best.get("score"),
        )

    entry_dt = next_full_minute(now_utc())
    direction_line = "🟢 CALL" if best["direction"] == "CALL" else "🔴 PUT"
    price_text = f"{best['price']:.5f}" if "JPY" not in best["pair"] else f"{best['price']:.3f}"

    msg = (
        "⚡ صفقة مباشرة OTC\n\n"
        f"💱 الزوج: {best['pair']}\n"
        "🧭 الفريم: M1\n"
        f"⏰ وقت الدخول: {format_utc_plus_3(entry_dt)}\n"
        f"📌 الاتجاه: {direction_line}\n"
        f"💵 السعر الحالي: {price_text}\n"
        f"📊 قوة الفرصة: {best['score']}%\n\n"
        "📌 سبب الاختيار:\n"
        f"• تم فحص {len(candidates)} فرصة من أزواج OTC الحية.\n"
        f"• هذا الزوج كان الأقوى حسب آخر {best['sample_size']} تحديثات سعرية مباشرة من Quotex.\n\n"
        "⚠️ التزم بإدارة رأس المال، وادخل فقط إذا بقي الاتجاه بنفس الشكل عند وقت الدخول."
    )

    return {
        "ok": True,
        "pair": best["pair"],
        "symbol": best["symbol"],
        "direction": best["direction"],
        "quality": best["score"],
        "entry_price": best["price"],
        "payout": best.get("payout", 80),
        "entry_time": entry_dt.isoformat(),
        "message": msg,
    }


# ===== OTC LIVE CHANNEL AUTOPUBLISH =====
otc_live_channel_state = {
    "active": False,
    "active_since": None,
    "martingale_direction": None,
    "martingale_decision_type": None,
    "martingale_for_message_id": None,
    "martingale_advice_message_id": None,
    "last_published_at": None,
}


def format_otc_live_price(pair: str, value: float) -> str:
    return f"{value:.3f}" if "JPY" in pair else f"{value:.5f}"


def seconds_until_dt(target_dt: datetime) -> float:
    return max(1.0, (target_dt.astimezone(UTC) - now_utc()).total_seconds())


def next_otc_m1_entry_time(check_dt: datetime | None = None) -> datetime:
    """بداية الشمعة القادمة M1.
    بما أن النشر مسموح فقط بآخر 15 ثانية، الدخول يكون على بداية الشمعة القادمة مباشرة.
    """
    base = (check_dt or now_utc()).astimezone(UTC)
    return next_full_minute(base).astimezone(UTC)


def display_otc_live_quality(raw_quality: int) -> int:
    """إعادة معايرة شكل قوة الفرصة فقط للعرض في القناة.
    لا تغيّر التحليل ولا قرار الدخول.
    """
    try:
        q = int(raw_quality or 0)
    except Exception:
        q = 0

    if q <= 0:
        return 0

    shown = int(round(q + 15))
    return max(70, min(95, shown))


def build_otc_live_channel_signal_message(signal: dict) -> str:
    pair = str(signal.get("pair", ""))
    direction = str(signal.get("direction", ""))
    quality = int(signal.get("quality", 0) or 0)
    display_quality = display_otc_live_quality(quality)
    entry_dt = parse_iso(str(signal.get("entry_time", ""))) or next_full_minute(now_utc())
    direction_line = "🟢 CALL" if direction == "CALL" else "🔴 PUT"

    return (
        "╔══════════════╗\n"
        "   🔥 Quotex OTC Signal Boot 🔥\n"
        "╚══════════════╝\n\n"
        f"💎 {pair}\n"
        "🔥 M1\n"
        f"⌛️ {format_utc_plus_3(entry_dt)}\n"
        f"{direction_line}\n"
        f"📊 قوة الفرصة: {display_quality}%\n"
        "⚠️ التزم بإدارة رأس المال\n\n"
        "@coach_WAEL_trading\n"
        "@sttrade_helper_bot"
    )


def build_otc_live_channel_result_message(signal: dict, exit_price: float | None, result: str) -> str:
    step = int(signal.get("martingale_step", 0) or 0)

    if result == "win":
        if step == 1:
            return "WIN ✅¹"
        return "WIN✅"

    if result == "loss":
        return "Loss💔"

    return "Doji⚖️"

def get_otc_live_recent_price_rows(symbol: str, limit: int = 14) -> list[tuple[float, float, int]]:
    try:
        with quotex_otc_feed.lock:
            rows = list(quotex_otc_feed.prices.get(symbol, []))[-limit:]
        return rows
    except Exception:
        return []


def build_smart_martingale_decision(signal: dict, open_price: float | None, current_price: float | None) -> dict:
    """يقرر اتجاه المضاعفة قبل نهاية الشمعة بثواني.
    المنطق:
    - إذا الصفقة ليست خاسرة الآن: لا نرسل مضاعفة.
    - إذا الخسارة تتقلص آخر اللحظات: مضاعفة بنفس الاتجاه.
    - إذا الخسارة تتوسع آخر اللحظات: مضاعفة بعكس الاتجاه.
    """
    pair = signal.get("pair")
    symbol = signal.get("symbol") or OTC_PAIR_TO_QUOTEX_SYMBOL.get(pair)
    direction = signal.get("direction")

    if open_price is None or current_price is None or direction not in {"CALL", "PUT"}:
        return {"needed": False, "reason": "missing_data"}

    current_result = resolve_candle_direction_result(direction, open_price, current_price)

    # لا ننتظر الخسارة الأكيدة فقط، لأن آخر الثواني ممكن تقلب بسرعة.
    # إذا الصفقة رابحة بفرق واضح لا نرسل تنبيه.
    # أما إذا خاسرة أو قريبة من التعادل نرسل تنبيه مبكر حتى تلحق تدخل المضاعفة من بداية الشمعة.
    diff_now = float(current_price) - float(open_price)
    if direction == "PUT":
        diff_now = -diff_now

    safe_margin = 0.00003
    if pair and "JPY" in str(pair):
        safe_margin = 0.003

    if current_result == "win" and diff_now > safe_margin:
        return {
            "needed": False,
            "reason": "trade_safely_winning",
            "current_result": current_result,
            "diff_now": diff_now,
            "safe_margin": safe_margin,
        }

    rows = get_otc_live_recent_price_rows(symbol, limit=12) if symbol else []
    prices = [float(r[1]) for r in rows if len(r) >= 2]

    if len(prices) >= 4:
        recent_change = prices[-1] - prices[0]
    else:
        recent_change = float(current_price) - float(open_price)

    loss_amount = abs(float(current_price) - float(open_price))

    # هل آخر اللحظات تتحرك لصالح الصفقة الأصلية أم ضدها؟
    if direction == "CALL":
        recovery = recent_change > 0
    else:
        recovery = recent_change < 0

    if recovery:
        martingale_direction = direction
        decision_type = "same"
    else:
        martingale_direction = opposite_otc_direction(direction)
        decision_type = "opposite"

    return {
        "needed": True,
        "decision_type": decision_type,
        "martingale_direction": martingale_direction,
        "current_result": current_result,
        "open_price": float(open_price),
        "current_price": float(current_price),
        "recent_change": float(recent_change),
        "loss_amount": float(loss_amount),
    }


def build_smart_martingale_message(decision: dict) -> str:
    martingale_direction = decision.get("martingale_direction")
    decision_type = decision.get("decision_type")

    direction_line = "🟢 CALL" if martingale_direction == "CALL" else "🔴 PUT"

    if decision_type == "same":
        title = "ضاعف بنفس اتجاه الصفقة"
    else:
        title = "ضاعف بعكس اتجاه الصفقة"

    return (
        "⚠️ تنبيه مضاعفة\n\n"
        f"{title}\n"
        f"{direction_line}"
    )

async def send_smart_martingale_advice(context: ContextTypes.DEFAULT_TYPE):
    signal = dict(context.job.data or {})

    try:
        if not OTC_LIVE_SMART_MARTINGALE_ENABLED:
            return

        if int(signal.get("martingale_advice_sent", 0) or 0) == 1:
            return

        if otc_live_channel_state.get("martingale_for_message_id") == signal.get("message_id"):
            return

        pair = signal.get("pair")
        symbol = signal.get("symbol") or OTC_PAIR_TO_QUOTEX_SYMBOL.get(pair)
        entry_ts = float(signal.get("entry_ts") or 0)

        candle = quotex_otc_feed.candle(symbol, entry_ts) if symbol else {}
        open_price = candle.get("open") if candle else None

        snapshot = get_live_otc_snapshot(pair)
        current_price = snapshot.get("price") if snapshot else None

        if open_price is None or current_price is None:
            return

        decision = build_smart_martingale_decision(signal, float(open_price), float(current_price))

        if not decision.get("needed"):
            logger.info(
                "Smart martingale advice skipped | pair=%s | reason=%s | current_result=%s",
                pair,
                decision.get("reason"),
                decision.get("current_result"),
            )
            return

        signal["martingale_direction"] = decision.get("martingale_direction")
        signal["martingale_decision_type"] = decision.get("decision_type")
        signal["martingale_advice_sent"] = 1

        # نخزن القرار بحالة الصفقة الحالية حتى يستخدمه resolve عند الخسارة
        otc_live_channel_state["martingale_direction"] = decision.get("martingale_direction")
        otc_live_channel_state["martingale_decision_type"] = decision.get("decision_type")
        otc_live_channel_state["martingale_for_message_id"] = signal.get("message_id")

        sent_advice = await context.bot.send_message(
            chat_id=OTC_LIVE_CHANNEL_ID,
            text=build_smart_martingale_message(decision)
        )
        otc_live_channel_state["martingale_advice_message_id"] = sent_advice.message_id

        logger.info(
            "Smart martingale advice sent | pair=%s | original=%s | martingale=%s | type=%s | open=%s | current=%s",
            pair,
            signal.get("direction"),
            decision.get("martingale_direction"),
            decision.get("decision_type"),
            decision.get("open_price"),
            decision.get("current_price"),
        )

    except Exception as e:
        logger.exception("Smart martingale advice error: %s", e)



async def delete_martingale_advice_if_direct_win(context: ContextTypes.DEFAULT_TYPE, signal: dict, result: str, martingale_step: int):
    """إذا تم إرسال تنبيه مضاعفة مبكرًا ثم ربحت الصفقة مباشر، نحذف التنبيه لتنظيف القناة.
    مهم: هذه الدالة لا يجب أن توقف إرسال النتيجة حتى لو فشل الحذف.
    """
    try:
        if result != "win" or int(martingale_step or 0) != 0:
            return

        advice_message_id = otc_live_channel_state.get("martingale_advice_message_id")
        advice_for_message_id = otc_live_channel_state.get("martingale_for_message_id")
        signal_message_id = signal.get("message_id")

        if not advice_message_id:
            return

        if advice_for_message_id and signal_message_id and advice_for_message_id != signal_message_id:
            return

        try:
            await context.bot.delete_message(
                chat_id=OTC_LIVE_CHANNEL_ID,
                message_id=int(advice_message_id)
            )
            logger.info(
                "Deleted early martingale advice because trade won directly | pair=%s | advice_message_id=%s",
                signal.get("pair"),
                advice_message_id,
            )
        except Exception as e:
            logger.warning("Could not delete martingale advice message: %s", e)

        otc_live_channel_state["martingale_advice_message_id"] = None

    except Exception as e:
        logger.warning("delete_martingale_advice_if_direct_win failed safely: %s", e)


async def resolve_otc_live_channel_trade(context: ContextTypes.DEFAULT_TYPE):
    signal = dict(context.job.data or {})
    pair = signal.get("pair")
    symbol = signal.get("symbol") or OTC_PAIR_TO_QUOTEX_SYMBOL.get(pair)
    direction = signal.get("direction")
    message_id = signal.get("message_id")
    martingale_step = int(signal.get("martingale_step", 0) or 0)

    result = "unknown"
    exit_price = None
    actual_entry_price = None

    try:
        entry_ts = float(signal.get("entry_ts") or 0)
        close_ts = float(signal.get("close_ts") or 0)

        candle = quotex_otc_feed.candle(symbol, entry_ts) if symbol else {}

        if candle:
            actual_entry_price = float(candle.get("open"))
            exit_price = float(candle.get("close"))
            signal["actual_entry_price"] = actual_entry_price
            signal["actual_entry_tick_ts"] = candle.get("open_tick_ts")
            signal["close_tick_ts"] = candle.get("close_tick_ts")
            signal["candle_ticks"] = candle.get("ticks")
        else:
            logger.warning(
                "No cached candle found for result | pair=%s | symbol=%s | entry_ts=%s | step=%s",
                pair, symbol, entry_ts, martingale_step
            )

        if actual_entry_price is None:
            actual_entry_price = float(signal.get("entry_price", 0) or 0)
            signal["actual_entry_price"] = actual_entry_price

        if exit_price is None:
            snapshot = get_live_otc_snapshot(pair)
            if snapshot and snapshot.get("price") is not None:
                exit_price = float(snapshot.get("price"))

        result = resolve_candle_direction_result(direction, actual_entry_price, exit_price)

        logger.info(
            "OTC candle-cache result | pair=%s | direction=%s | open=%s | close=%s | result=%s | step=%s | ticks=%s",
            pair,
            direction,
            actual_entry_price,
            exit_price,
            result,
            martingale_step,
            signal.get("candle_ticks"),
        )

        await delete_martingale_advice_if_direct_win(context, signal, result, martingale_step)

        if martingale_step == 0:
            signal["first_candle_result"] = result
            signal["first_candle_open"] = actual_entry_price
            signal["first_candle_close"] = exit_price

        # مضاعفة واحدة ذكية:
        # إذا خسرت الصفقة الأساسية، نستخدم قرار آخر ثانيتين إن وجد:
        # نفس اتجاه الصفقة أو عكس اتجاه الصفقة.
        if result == "loss" and martingale_step == 0:
            next_entry_dt = datetime.fromtimestamp(close_ts, tz=UTC)
            next_close_dt = next_entry_dt + timedelta(seconds=OTC_LIVE_TRADE_DURATION_SECONDS)

            chosen_martingale_direction = otc_live_channel_state.get("martingale_direction")
            decision_msg_id = otc_live_channel_state.get("martingale_for_message_id")

            if decision_msg_id != message_id or chosen_martingale_direction not in {"CALL", "PUT"}:
                # fallback إذا لم يصل قرار التنبيه المبكر لأي سبب.
                # نرسل تنبيه فوري، لكن هذا احتياطي فقط وقد يكون متأخرًا عن بداية شمعة المضاعفة.
                chosen_martingale_direction = direction
                try:
                    await context.bot.send_message(
                        chat_id=OTC_LIVE_CHANNEL_ID,
                        text=(
                            "⚠️ تنبيه مضاعفة\n\n"
                            "ضاعف بنفس اتجاه الصفقة\n"
                            f"{'🟢 CALL' if chosen_martingale_direction == 'CALL' else '🔴 PUT'}"
                        )
                    )
                except Exception as e:
                    logger.warning("Could not send fallback martingale advice: %s", e)

            signal["martingale_step"] = 1
            signal["martingale_base_direction"] = direction
            signal["direction"] = chosen_martingale_direction
            signal["martingale_direction"] = chosen_martingale_direction
            signal["martingale_decision_type"] = otc_live_channel_state.get("martingale_decision_type", "same")
            signal["entry_ts"] = next_entry_dt.timestamp()
            signal["close_ts"] = next_close_dt.timestamp()
            signal["entry_time"] = next_entry_dt.isoformat()

            delay = seconds_until_dt(next_close_dt) + OTC_LIVE_RESULT_EXTRA_DELAY_SECONDS

            context.job_queue.run_once(
                resolve_otc_live_channel_trade,
                when=delay,
                data=signal,
                name=f"otc_live_martingale_result_{message_id}",
            )

            logger.info(
                "OTC live trade lost first candle, waiting smart martingale candle | pair=%s | original=%s | martingale=%s | result_in=%.1fs",
                pair, direction, chosen_martingale_direction, delay
            )
            return

        # نسجل اختبار NORMAL vs REVERSE بعد النتيجة النهائية.
        record_otc_live_shadow_direction_test(
            signal=signal,
            first_result=str(signal.get("first_candle_result", result)),
            final_result=result,
            candle_open=signal.get("first_candle_open", actual_entry_price),
            candle_close=signal.get("first_candle_close", exit_price),
        )

        # نسجل النتيجة النهائية فقط:
        record_otc_live_channel_result(signal, result)
        update_otc_live_learning_after_result(signal, result)

        text = build_otc_live_channel_result_message(signal, exit_price, result)
        await context.bot.send_message(
            chat_id=OTC_LIVE_CHANNEL_ID,
            text=text
        )

    except Exception as e:
        logger.exception("OTC live channel result error: %s", e)

    finally:
        # إذا دخلنا مضاعفة، لا نفتح صفقة جديدة قبل انتهاء المضاعفة.
        if not (result == "loss" and martingale_step == 0):
            otc_live_channel_state["active"] = False
            otc_live_channel_state["active_since"] = None
            otc_live_channel_state["martingale_direction"] = None
            otc_live_channel_state["martingale_decision_type"] = None
            otc_live_channel_state["martingale_for_message_id"] = None
            otc_live_channel_state["martingale_advice_message_id"] = None
            otc_live_channel_state["last_published_at"] = time_module.time()



def is_inside_otc_entry_scan_window(check_dt: datetime | None = None) -> tuple[bool, float]:
    """Strict final decision window.
    لا يتم تحليل أو اختيار الصفقة إلا داخل نافذة النشر نفسها.
    الافتراضي:
    - باقي 20 إلى 15 ثانية على بداية شمعة M1 القادمة => مسموح التحليل والنشر.
    - خارج هذه النافذة => لا تحليل ولا قرار.
    """
    check_dt = check_dt or now_utc()
    entry_dt = next_otc_m1_entry_time(check_dt)
    remaining = (entry_dt - check_dt.astimezone(UTC)).total_seconds()

    inside = (
        OTC_LIVE_ENTRY_MIN_REMAINING_SECONDS
        <= remaining
        <= OTC_LIVE_ENTRY_MAX_REMAINING_SECONDS
    )

    return inside, remaining

async def auto_publish_otc_live_channel(context: ContextTypes.DEFAULT_TYPE):
    logger.info("OTC LIVE CHANNEL SCAN started | enabled=%s | active=%s | min_quality=%s",
                OTC_LIVE_CHANNEL_ENABLED, otc_live_channel_state.get("active"), OTC_LIVE_MIN_QUALITY)

    if not OTC_LIVE_CHANNEL_ENABLED or not is_channel_publish_enabled("otc_live"):
        logger.info("OTC LIVE CHANNEL SCAN skipped: disabled")
        return

    if otc_live_channel_state.get("active"):
        if reset_stuck_otc_live_trade_if_needed():
            logger.info("OTC LIVE CHANNEL SCAN recovered from stuck active trade")
        else:
            logger.info("OTC LIVE CHANNEL SCAN skipped: active trade is still running")
            return

    last_published_at = otc_live_channel_state.get("last_published_at")
    if last_published_at and time_module.time() - float(last_published_at) < OTC_LIVE_COOLDOWN_SECONDS:
        remaining = OTC_LIVE_COOLDOWN_SECONDS - (time_module.time() - float(last_published_at))
        logger.info("OTC LIVE CHANNEL SCAN skipped: cooldown %.1fs remaining", remaining)
        return

    try:
        inside_window, remaining_to_entry = is_inside_otc_entry_scan_window(now_utc())
        if not inside_window:
            logger.info(
                "OTC LIVE CHANNEL SCAN skipped: outside strict final decision window | allowed=%.1f-%.1fs remaining | current_remaining=%.1fs",
                OTC_LIVE_ENTRY_MIN_REMAINING_SECONDS,
                OTC_LIVE_ENTRY_MAX_REMAINING_SECONDS,
                remaining_to_entry,
            )
            return

        # مهم: لا يتم تحليل واختيار الصفقة إلا الآن، بعد التأكد أننا داخل نافذة 20–15 ثانية.
        signal = analyze_best_live_otc_now()
        if not signal.get("ok"):
            logger.info("OTC LIVE CHANNEL SCAN result: no clear opportunity")
            return

        quality = int(signal.get("quality", 0) or 0)
        effective_min_quality, quality_reason = get_otc_live_effective_min_quality()

        logger.info("OTC LIVE CHANNEL SCAN best | pair=%s | direction=%s | quality=%s | min=%s | learning=%s",
                    signal.get("pair"), signal.get("direction"), quality, effective_min_quality, quality_reason)

        pair_blocked, block_reason = is_otc_live_pair_blocked_by_learning(signal.get("pair"))
        if pair_blocked:
            logger.info("OTC LIVE CHANNEL SCAN skipped by learning | pair=%s | reason=%s",
                        signal.get("pair"), block_reason)
            return

        if quality < effective_min_quality:
            logger.info("OTC LIVE CHANNEL SCAN skipped: quality below effective minimum | %s", quality_reason)
            return

        if OTC_LIVE_REVERSE_AUTOPUBLISH:
            original_direction = signal.get("direction")
            if original_direction == "CALL":
                signal["direction"] = "PUT"
            elif original_direction == "PUT":
                signal["direction"] = "CALL"

            signal["original_direction"] = original_direction
            logger.info(
                "OTC LIVE CHANNEL direction reversed for next-candle entry | pair=%s | original=%s | published=%s",
                signal.get("pair"), original_direction, signal.get("direction")
            )

        # الدخول مع بداية شمعة M1 القادمة، والإغلاق مع نهاية نفس الشمعة.
        # النتيجة لاحقًا تُحسب من candle cache: open/close للشمعة نفسها.
        entry_dt = next_otc_m1_entry_time(now_utc())
        close_dt = entry_dt + timedelta(seconds=OTC_LIVE_TRADE_DURATION_SECONDS)

        signal["entry_time"] = entry_dt.isoformat()
        signal["entry_ts"] = entry_dt.timestamp()
        signal["close_ts"] = close_dt.timestamp()

        text = build_otc_live_channel_signal_message(signal)

        sent = await context.bot.send_message(
            chat_id=OTC_LIVE_CHANNEL_ID,
            text=text,
        )

        signal["message_id"] = sent.message_id
        signal["published_at"] = now_iso()

        otc_live_channel_state["active"] = True
        otc_live_channel_state["active_since"] = time_module.time()

        resolve_delay = seconds_until_dt(close_dt) + OTC_LIVE_RESULT_EXTRA_DELAY_SECONDS
        if OTC_LIVE_SMART_MARTINGALE_ENABLED:
            for check_seconds in OTC_LIVE_MARTINGALE_ADVICE_CHECK_SECONDS:
                advice_dt = close_dt - timedelta(seconds=check_seconds)
                advice_delay = max(0.1, seconds_until_dt(advice_dt))

                context.job_queue.run_once(
                    send_smart_martingale_advice,
                    when=advice_delay,
                    data=signal,
                    name=f"otc_live_martingale_advice_{sent.message_id}_{check_seconds}s",
                )

        context.job_queue.run_once(
            resolve_otc_live_channel_trade,
            when=resolve_delay,
            data=signal,
            name=f"otc_live_result_{sent.message_id}",
        )

        logger.info(
            "Published OTC live channel signal | pair=%s | direction=%s | quality=%s | result_in=%.1fs",
            signal.get("pair"),
            signal.get("direction"),
            signal.get("quality"),
            resolve_delay,
        )

    except Exception as e:
        otc_live_channel_state["active"] = False
        logger.exception("OTC live channel publish error: %s", e)



# ===== OTC LIVE ADAPTIVE FILTER =====
def otc_live_trade_units(result: str, martingale_step: int = 0, payout: float | int | None = None) -> float:
    """حساب النتيجة المالية بالوحدات.
    WIN مباشر = payout
    WIN بالمضاعفة = -1 + 2*payout
    Loss بعد المضاعفة = -3
    """
    try:
        payout_rate = float(payout or 80) / 100.0
    except Exception:
        payout_rate = 0.80

    if result == "win":
        if int(martingale_step or 0) == 1:
            return round(-1.0 + (2.0 * payout_rate), 4)
        return round(payout_rate, 4)

    if result == "loss":
        return -3.0

    return 0.0


def get_otc_live_recent_trades(day_key: str | None = None, limit: int = 300) -> list[dict]:
    try:
        day_key = day_key or get_otc_live_day_key()
        raw = otc_live_stats_ref().child(day_key).child("trades").get() or {}

        rows = []
        if isinstance(raw, dict):
            for trade_id, trade in raw.items():
                if isinstance(trade, dict):
                    item = dict(trade)
                    item["_id"] = trade_id
                    rows.append(item)

        rows.sort(key=lambda x: str(x.get("created_at", "")))
        return rows[-limit:]
    except Exception as e:
        logger.exception("Could not read OTC live recent trades: %s", e)
        return []


def is_otc_live_candidate_blocked(pair: str, direction: str) -> tuple[bool, str]:
    """فلتر تعلم ناعم:
    لا يمنع بسرعة. يحتاج بيانات كافية وخسائر واضحة قبل أن يتجنب النمط.
    """
    if not OTC_LIVE_ADAPTIVE_FILTER_ENABLED:
        return False, ""

    trades = get_otc_live_recent_trades(limit=500)
    if not trades:
        return False, ""

    pair_trades = [t for t in trades if t.get("pair") == pair]
    pair_recent = pair_trades[-OTC_LIVE_PAIR_RECENT_LIMIT:]
    pair_losses = sum(1 for t in pair_recent if t.get("result") == "loss")
    pair_units = sum(float(t.get("units", 0) or 0) for t in pair_recent)

    # لا نحكم على الزوج إلا بعد عدد صفقات كافي
    if len(pair_recent) >= OTC_LIVE_PAIR_RECENT_LIMIT:
        if pair_losses >= OTC_LIVE_PAIR_MAX_RECENT_LOSSES:
            return True, f"soft_pair_losses={pair_losses}/{len(pair_recent)}"

        if pair_units <= OTC_LIVE_PAIR_MAX_RECENT_NEGATIVE_UNITS:
            return True, f"soft_pair_units={pair_units:.2f}"

    dir_trades = [t for t in trades if t.get("pair") == pair and t.get("direction") == direction]
    dir_recent = dir_trades[-OTC_LIVE_DIRECTION_RECENT_LIMIT:]
    dir_losses = sum(1 for t in dir_recent if t.get("result") == "loss")
    dir_units = sum(float(t.get("units", 0) or 0) for t in dir_recent)

    # لا نحكم على الزوج + الاتجاه إلا بعد عدد صفقات كافي
    if len(dir_recent) >= OTC_LIVE_DIRECTION_RECENT_LIMIT:
        if dir_losses >= OTC_LIVE_DIRECTION_MAX_RECENT_LOSSES:
            return True, f"soft_direction_losses={dir_losses}/{len(dir_recent)}"

        if dir_units <= -6.0:
            return True, f"soft_direction_units={dir_units:.2f}"

    return False, ""


# ===== OTC LIVE SHADOW DIRECTION TEST =====
def opposite_otc_direction(direction: str) -> str:
    if direction == "CALL":
        return "PUT"
    if direction == "PUT":
        return "CALL"
    return direction


def resolve_candle_direction_result(direction: str, open_price: float | None, close_price: float | None) -> str:
    try:
        if open_price is None or close_price is None:
            return "unknown"
        open_price = float(open_price)
        close_price = float(close_price)
        diff = close_price - open_price

        if abs(diff) <= OTC_LIVE_TIE_EPSILON:
            return "unknown"
        if direction == "CALL":
            return "win" if diff > 0 else "loss"
        if direction == "PUT":
            return "win" if diff < 0 else "loss"
    except Exception:
        return "unknown"

    return "unknown"


def record_otc_live_shadow_direction_test(signal: dict, first_result: str, final_result: str, candle_open: float | None, candle_close: float | None):
    """يسجل اختبار NORMAL vs REVERSE بالخلفية.
    normal_direction = الاتجاه الأصلي قبل العكس
    reverse_direction = الاتجاه المنشور بعد العكس
    """
    try:
        day_key = get_otc_live_day_key()
        ref = otc_live_stats_ref().child(day_key).child("shadow_direction_test")

        published_direction = signal.get("direction")
        original_direction = signal.get("original_direction") or opposite_otc_direction(published_direction)

        # نتيجة أول شمعة للاتجاه المنشور
        reverse_first = resolve_candle_direction_result(published_direction, candle_open, candle_close)

        # نتيجة أول شمعة للاتجاه الأصلي
        normal_first = resolve_candle_direction_result(original_direction, candle_open, candle_close)

        # لأن NORMAL عكس REVERSE في نفس الشمعة، لو المنشور خسر الأولى فالأصلي ربح مباشر غالبًا.
        # لو المنشور ربح مباشر، الأصلي خسر الأولى. لا نعرف مضاعفته المستقبلية كاملة هنا،
        # لذلك نسجل اختبار أول شمعة بدقة، والنتيجة المالية النهائية للمنشور فقط.
        current = ref.get() or {}

        normal_first_wins = safe_int(current.get("normal_first_wins"), 0)
        normal_first_losses = safe_int(current.get("normal_first_losses"), 0)
        reverse_first_wins = safe_int(current.get("reverse_first_wins"), 0)
        reverse_first_losses = safe_int(current.get("reverse_first_losses"), 0)
        total = safe_int(current.get("total"), 0) + 1

        if normal_first == "win":
            normal_first_wins += 1
        elif normal_first == "loss":
            normal_first_losses += 1

        if reverse_first == "win":
            reverse_first_wins += 1
        elif reverse_first == "loss":
            reverse_first_losses += 1

        ref.update({
            "total": total,
            "normal_first_wins": normal_first_wins,
            "normal_first_losses": normal_first_losses,
            "reverse_first_wins": reverse_first_wins,
            "reverse_first_losses": reverse_first_losses,
            "updated_at": now_iso(),
        })

        ref.child("trades").push({
            "pair": signal.get("pair"),
            "symbol": signal.get("symbol"),
            "quality": signal.get("quality"),
            "payout": signal.get("payout"),
            "published_direction": published_direction,
            "original_direction": original_direction,
            "normal_first_result": normal_first,
            "reverse_first_result": reverse_first,
            "published_final_result": final_result,
            "published_first_result": first_result,
            "open": candle_open,
            "close": candle_close,
            "created_at": now_iso(),
        })

    except Exception as e:
        logger.exception("Could not record shadow direction test: %s", e)




# ===== OTC LIST WATCHER / RESULTS CHECKER =====
OTC_PAIR_TIME_RE = re.compile(
    r"(?P<pair>[A-Z]{3}/[A-Z]{3})\s*(?:\(OTC\))?\s+"
    r"(?P<hour>\d{1,2}):(?P<minute>\d{2})",
    re.IGNORECASE
)

OTC_LIST_TRADE_RE = re.compile(
    r"(?P<pair>[A-Z]{3}/[A-Z]{3})\s*\(OTC\)\s+"
    r"(?P<hour>\d{1,2}):(?P<minute>\d{2})\s+"
    r"(?P<direction>CALL|PUT)",
    re.IGNORECASE
)




def get_otc_candle_debug_for_pair_time(pair: str, entry_ts: float) -> str:
    try:
        possible_symbols = get_otc_possible_symbols_for_pair(pair)
        target_bucket = int(float(entry_ts) // 60) * 60
        target_dt = datetime.fromtimestamp(target_bucket, tz=UTC).astimezone(UTC_PLUS_3)

        lines = [
            f"target={target_dt.strftime('%Y-%m-%d %H:%M:%S')}",
            f"target_bucket={target_bucket}",
        ]

        with quotex_otc_feed.lock:
            for symbol in possible_symbols:
                candles = dict(quotex_otc_feed.candles.get(symbol, {}) or {})
                prices = list(quotex_otc_feed.prices.get(symbol, []) or [])

                candle_keys = sorted(candles.keys())
                nearest = []
                for key in candle_keys:
                    try:
                        diff = int(key) - int(target_bucket)
                        if abs(diff) <= 600:
                            dt = datetime.fromtimestamp(float(key), tz=UTC).astimezone(UTC_PLUS_3)
                            c = candles.get(key) or {}
                            nearest.append(
                                f"{dt.strftime('%H:%M')} diff={diff}s O={c.get('open')} C={c.get('close')}"
                            )
                    except Exception:
                        pass

                tick_in_target = 0
                for row in prices:
                    try:
                        ts = float(row[0])
                        if int(ts // 60) * 60 == target_bucket:
                            tick_in_target += 1
                    except Exception:
                        pass

                last_tick = "none"
                if prices:
                    try:
                        last_ts = float(prices[-1][0])
                        last_dt = datetime.fromtimestamp(last_ts, tz=UTC).astimezone(UTC_PLUS_3)
                        last_tick = f"{last_dt.strftime('%H:%M:%S')} price={prices[-1][1]}"
                    except Exception:
                        pass

                lines.extend([
                    f"symbol={symbol}",
                    f"candles_count={len(candle_keys)} ticks_count={len(prices)} ticks_in_target_minute={tick_in_target}",
                    f"last_tick={last_tick}",
                    "near_candles=" + (" | ".join(nearest[-10:]) if nearest else "none"),
                ])

        return " ; ".join(lines)
    except Exception as e:
        logger.exception("Could not build candle debug: %s", e)
        return "debug_error"


def get_otc_possible_symbols_for_pair(pair: str) -> list[str]:
    """يرجع كل الرموز المحتملة للزوج لأن بعض أزواج OTC تظهر معكوسة في Quotex."""
    symbols = []
    mapped = OTC_PAIR_TO_QUOTEX_SYMBOL.get(pair)
    if mapped:
        symbols.append(mapped)

    try:
        base = pair.replace(" (OTC)", "").replace("/", "").upper()
        if len(base) == 6:
            symbols.append(f"{base}_otc")
            symbols.append(f"{base[3:]}{base[:3]}_otc")
    except Exception:
        pass

    unique = []
    for s in symbols:
        if s and s not in unique:
            unique.append(s)
    return unique


def get_otc_cached_candle_for_pair(pair: str, entry_ts: float) -> tuple[dict, str | None]:
    for symbol in get_otc_possible_symbols_for_pair(pair):
        candle = quotex_otc_feed.candle(symbol, entry_ts)
        if candle:
            return candle, symbol
    return {}, None


def parse_otc_list_trades(raw_text: str) -> list[dict]:
    trades = []
    for line in (raw_text or "").splitlines():
        clean = line.strip()
        if not clean:
            continue

        match = OTC_LIST_TRADE_RE.search(clean)
        if not match:
            continue

        pair = f"{match.group('pair').upper()} (OTC)"
        hour = int(match.group("hour"))
        minute = int(match.group("minute"))
        direction = match.group("direction").upper()
        up_down = "Up" if direction == "CALL" else "Down"

        trades.append({
            "raw": clean,
            "pair": pair,
            "hour": hour,
            "minute": minute,
            "direction": direction,
            "up_down": up_down,
        })

    return trades


def otc_list_entry_datetime(hour: int, minute: int) -> datetime:
    """تحديد تاريخ وقت الصفقة بذكاء.
    القاعدة:
    - إذا وقت الصفقة صار اليوم خلال آخر 12 ساعة، نختار اليوم.
    - إذا وقت الصفقة قادم خلال 12 ساعة، نختار الوقت القادم.
    - هذا يحل مشكلة 00:00 قبل منتصف الليل بدون أن يحول صفقات اليوم إلى بكرا.
    """
    now_local = now_utc().astimezone(UTC_PLUS_3)

    candidates = []
    for day_shift in (-1, 0, 1):
        candidate = (
            now_local
            .replace(hour=hour, minute=minute, second=0, microsecond=0)
            + timedelta(days=day_shift)
        )
        candidates.append(candidate)

    # 1) الأفضل: أقرب وقت ماضي حديث، لأن أغلب فحص النتائج يتم بعد الصفقة بدقائق أو ساعات.
    recent_past = [
        dt for dt in candidates
        if dt <= now_local and (now_local - dt).total_seconds() <= 12 * 3600
    ]
    if recent_past:
        chosen = max(recent_past)
        return chosen.astimezone(UTC)

    # 2) إذا الصفقة لسه قادمة قريبًا، مثل ليستة 00:00 قبل منتصف الليل.
    near_future = [
        dt for dt in candidates
        if dt > now_local and (dt - now_local).total_seconds() <= 12 * 3600
    ]
    if near_future:
        chosen = min(near_future)
        return chosen.astimezone(UTC)

    # 3) fallback: أقرب وقت مطلقًا.
    chosen = min(candidates, key=lambda dt: abs((dt - now_local).total_seconds()))
    return chosen.astimezone(UTC)


def candle_color_from_prices(open_price, close_price) -> str:
    try:
        open_price = float(open_price)
        close_price = float(close_price)
        diff = close_price - open_price
        if abs(diff) <= OTC_LIVE_TIE_EPSILON:
            return "DOJI"
        return "CALL" if diff > 0 else "PUT"
    except Exception:
        return "UNKNOWN"


def detect_otc_list_momentum_violation(trade: dict) -> tuple[bool, str]:
    """المخالفة = الصفقة عكس مومنتم 4 شموع متتالية قبل وقت الدخول."""
    pair = trade.get("pair")
    direction = trade.get("direction")

    if direction not in {"CALL", "PUT"}:
        return False, ""

    entry_dt = otc_list_entry_datetime(int(trade["hour"]), int(trade["minute"]))
    entry_ts = entry_dt.timestamp()

    colors = []
    for i in range(4, 0, -1):
        candle, _used_symbol = get_otc_cached_candle_for_pair(pair, entry_ts - (60 * i))
        if not candle:
            return False, ""

        color = candle_color_from_prices(candle.get("open"), candle.get("close"))
        if color in {"UNKNOWN", "DOJI"}:
            return False, ""

        colors.append(color)

    if len(colors) == 4 and len(set(colors)) == 1:
        momentum = colors[0]
        if momentum != direction:
            label = "4 شموع صاعدة" if momentum == "CALL" else "4 شموع هابطة"
            return True, f"عكس مومنتم: {label}"

    return False, ""


def evaluate_otc_list_trade(trade: dict) -> dict:
    pair = trade.get("pair")
    direction = trade.get("direction")
    entry_dt = otc_list_entry_datetime(int(trade["hour"]), int(trade["minute"]))
    entry_ts = entry_dt.timestamp()

    violation, violation_reason = detect_otc_list_momentum_violation(trade)

    first_candle, used_symbol = get_otc_cached_candle_for_pair(pair, entry_ts)
    if not first_candle:
        possible_symbols = get_otc_possible_symbols_for_pair(pair)
        recent_status = []
        try:
            now_ts = time_module.time()
            with quotex_otc_feed.lock:
                for _sym in possible_symbols:
                    _prices = list(quotex_otc_feed.prices.get(_sym, []))
                    if _prices:
                        _age = round(now_ts - float(_prices[-1][0]), 1)
                        recent_status.append(f"{_sym}: last_tick_age={_age}s ticks={len(_prices)}")
                    else:
                        recent_status.append(f"{_sym}: no_ticks")
        except Exception:
            pass

        candle_debug = get_otc_candle_debug_for_pair_time(pair, entry_ts)

        logger.warning(
            "OTC list No Data | pair=%s | time=%02d:%02d | symbols=%s | status=%s | candle_debug=%s",
            pair,
            int(trade.get("hour", 0)),
            int(trade.get("minute", 0)),
            possible_symbols,
            "; ".join(recent_status),
            candle_debug,
        )

        return {
            **trade,
            "result": "no_data",
            "mark": "No Data⚠️",
            "note": "no_first_candle",
            "debug": "; ".join(recent_status),
            "candle_debug": candle_debug,
            "violation": violation,
            "violation_reason": violation_reason,
        }

    first_result = resolve_candle_direction_result(
        direction,
        first_candle.get("open"),
        first_candle.get("close"),
    )

    if first_result == "win":
        result = "win"
        mark = "✅"
    elif first_result == "loss":
        second_candle, _used_symbol2 = get_otc_cached_candle_for_pair(pair, entry_ts + 60)
        if not second_candle:
            result = "no_data"
            mark = "No Data⚠️"
        else:
            second_result = resolve_candle_direction_result(
                direction,
                second_candle.get("open"),
                second_candle.get("close"),
            )

            if second_result == "win":
                result = "martingale_win"
                mark = "✅¹"
            elif second_result == "loss":
                result = "loss"
                mark = "↘️"
            else:
                result = "unknown"
                mark = "Doji⚖️"
    else:
        result = "unknown"
        mark = "Doji⚖️"

    return {
        **trade,
        "result": result,
        "mark": mark,
        "note": "",
        "entry_iso": entry_dt.isoformat(),
        "violation": violation,
        "violation_reason": violation_reason,
    }


def format_otc_list_trade_line(item: dict) -> str:
    return (
        f"{item['pair']} {item['hour']:02d}:{item['minute']:02d} "
        f"{item['direction']} ({item['up_down']}) {item['mark']}"
    )


def build_otc_list_results_message(raw_text: str) -> tuple[str, dict]:
    parsed = parse_otc_list_trades(raw_text)
    if not parsed:
        return (
            "❌ لم أستطع قراءة أي صفقة من الليستة.\n\n"
            "تأكد أن الصيغة مثل:\n"
            "USD/BRL (OTC) 12:04 PUT (Down)",
            {"total": 0}
        )

    evaluated = [evaluate_otc_list_trade(trade) for trade in parsed]

    regular_items = [item for item in evaluated if not item.get("violation")]
    violation_items = [item for item in evaluated if item.get("violation")]

    win_count = sum(1 for item in evaluated if item["result"] in {"win", "martingale_win"})
    direct_win_count = sum(1 for item in evaluated if item["result"] == "win")
    martingale_win_count = sum(1 for item in evaluated if item["result"] == "martingale_win")
    loss_count = sum(1 for item in evaluated if item["result"] == "loss")
    doji_count = sum(1 for item in evaluated if item["result"] == "unknown")
    no_data_count = sum(1 for item in evaluated if item["result"] == "no_data")

    lines = [
        "╔══════════════╗",
        "   🟢 Quotex Results 🟢",
        "╚══════════════╝",
        "",
    ]

    for item in regular_items:
        lines.append(format_otc_list_trade_line(item))

    if violation_items:
        lines.extend([
            "",
            "——————————————",
            "⚠️ الصفقات المخالفة",
            "——————————————",
        ])
        for item in violation_items:
            lines.append(format_otc_list_trade_line(item) + " ❗")

    lines.extend([
        "",
        "——————————————",
        f"{win_count} win✅",
        f"{loss_count} loss↘️",
    ])

    if martingale_win_count:
        lines.append(f"{martingale_win_count} win¹✅")

    if doji_count:
        lines.append(f"{doji_count} Doji⚖️")

    if no_data_count:
        lines.append(f"{no_data_count} No Data⚠️")

    lines.append("——————————————")

    meta = {
        "total": len(evaluated),
        "win": win_count,
        "direct_win": direct_win_count,
        "martingale_win": martingale_win_count,
        "loss": loss_count,
        "doji": doji_count,
        "no_data": no_data_count,
        "violations": len(violation_items),
    }

    return "\n".join(lines), meta


def get_otc_list_ready_delay_seconds(raw_text: str) -> tuple[float, int]:
    trades = parse_otc_list_trades(raw_text)
    if not trades:
        return 0.0, 0

    latest_entry = max(
        otc_list_entry_datetime(int(trade["hour"]), int(trade["minute"]))
        for trade in trades
    )

    # آخر صفقة: شمعة أولى 60 ثانية + شمعة مضاعفة 60 ثانية + تأخير بسيط لجمع الإغلاق.
    ready_dt = latest_entry + timedelta(seconds=130)
    delay = max(1.0, (ready_dt - now_utc()).total_seconds())
    return delay, len(trades)



def otc_list_results_ref(admin_id: int):
    return system_ref().child("otc_list_results").child(str(admin_id))


def save_ready_otc_list_result(admin_id: int, raw_text: str, result_text: str, meta: dict):
    try:
        otc_list_results_ref(admin_id).set({
            "raw_text": raw_text,
            "result_text": result_text,
            "meta": meta or {},
            "ready_at": now_iso(),
        })
    except Exception as e:
        logger.exception("Could not save ready OTC list result: %s", e)


def get_ready_otc_list_result(admin_id: int) -> dict:
    try:
        return otc_list_results_ref(admin_id).get() or {}
    except Exception as e:
        logger.exception("Could not read ready OTC list result: %s", e)
        return {}


def get_otc_list_job_ref(admin_id: int, list_id: str):
    return system_ref().child("otc_list_jobs").child(str(admin_id)).child(str(list_id))


def save_otc_list_job(admin_id: int, list_id: str, raw_text: str, trades: list[dict]):
    try:
        get_otc_list_job_ref(admin_id, list_id).set({
            "raw_text": raw_text,
            "trades": trades,
            "created_at": now_iso(),
            "status": "watching",
        })
    except Exception as e:
        logger.exception("Could not save OTC list job: %s", e)


def save_otc_list_trade_result(admin_id: int, list_id: str, index: int, item: dict):
    try:
        get_otc_list_job_ref(admin_id, list_id).child("results").child(str(index)).set(item)
    except Exception as e:
        logger.exception("Could not save OTC list trade result: %s", e)


def get_otc_list_job(admin_id: int, list_id: str) -> dict:
    try:
        return get_otc_list_job_ref(admin_id, list_id).get() or {}
    except Exception as e:
        logger.exception("Could not read OTC list job: %s", e)
        return {}


def build_otc_list_results_message_from_items(items: list[dict]) -> tuple[str, dict]:
    if not items:
        return "❌ لا توجد نتائج محفوظة لهذه الليستة.", {"total": 0}

    regular_items = [item for item in items if not item.get("violation")]
    violation_items = [item for item in items if item.get("violation")]

    win_count = sum(1 for item in items if item.get("result") in {"win", "martingale_win"})
    direct_win_count = sum(1 for item in items if item.get("result") == "win")
    martingale_win_count = sum(1 for item in items if item.get("result") == "martingale_win")
    loss_count = sum(1 for item in items if item.get("result") == "loss")
    doji_count = sum(1 for item in items if item.get("result") == "unknown")
    no_data_count = sum(1 for item in items if item.get("result") == "no_data")

    lines = [
        "╔══════════════╗",
        "   🟢 Quotex Results 🟢",
        "╚══════════════╝",
        "",
    ]

    for item in regular_items:
        lines.append(format_otc_list_trade_line(item))

    if violation_items:
        lines.extend([
            "",
            "——————————————",
            "⚠️ الصفقات المخالفة",
            "——————————————",
        ])
        for item in violation_items:
            lines.append(format_otc_list_trade_line(item) + " ❗")

    lines.extend([
        "",
        "——————————————",
        f"{win_count} win✅",
        f"{loss_count} loss↘️",
    ])

    if martingale_win_count:
        lines.append(f"{martingale_win_count} win¹✅")

    if doji_count:
        lines.append(f"{doji_count} Doji⚖️")

    if no_data_count:
        lines.append(f"{no_data_count} No Data⚠️")

    lines.append("——————————————")

    meta = {
        "total": len(items),
        "win": win_count,
        "direct_win": direct_win_count,
        "martingale_win": martingale_win_count,
        "loss": loss_count,
        "doji": doji_count,
        "no_data": no_data_count,
        "violations": len(violation_items),
    }

    return "\n".join(lines), meta


async def evaluate_single_otc_list_trade_job(context: ContextTypes.DEFAULT_TYPE):
    data = dict(context.job.data or {})
    admin_id = int(data.get("admin_id"))
    list_id = str(data.get("list_id"))
    index = int(data.get("index"))
    retry = int(data.get("retry", 0) or 0)
    trade = dict(data.get("trade") or {})

    try:
        item = evaluate_otc_list_trade(trade)
        item["index"] = index
        item["evaluated_at"] = now_iso()
        item["retry"] = retry

        if item.get("result") == "no_data" and retry < OTC_LIST_NO_DATA_MAX_RETRIES:
            logger.warning(
                "OTC list trade No Data, retry scheduled | admin=%s | list=%s | index=%s | pair=%s | time=%02d:%02d | retry=%s/%s",
                admin_id,
                list_id,
                index,
                trade.get("pair"),
                int(trade.get("hour", 0)),
                int(trade.get("minute", 0)),
                retry + 1,
                OTC_LIST_NO_DATA_MAX_RETRIES,
            )

            context.job_queue.run_once(
                evaluate_single_otc_list_trade_job,
                when=OTC_LIST_NO_DATA_RETRY_SECONDS,
                data={
                    "admin_id": admin_id,
                    "list_id": list_id,
                    "index": index,
                    "trade": trade,
                    "retry": retry + 1,
                },
                name=f"otc_list_trade_retry_{admin_id}_{list_id}_{index}_{retry + 1}",
            )
            return

        save_otc_list_trade_result(admin_id, list_id, index, item)

        logger.info(
            "OTC list trade evaluated | admin=%s | list=%s | index=%s | pair=%s | time=%02d:%02d | result=%s | retry=%s",
            admin_id,
            list_id,
            index,
            item.get("pair"),
            int(item.get("hour", 0)),
            int(item.get("minute", 0)),
            item.get("result"),
            retry,
        )
    except Exception as e:
        logger.exception("Could not evaluate OTC list trade job: %s", e)

async def finalize_otc_list_results_job(context: ContextTypes.DEFAULT_TYPE):
    data = dict(context.job.data or {})
    admin_id = int(data.get("admin_id"))
    list_id = str(data.get("list_id"))
    finalize_retry = int(data.get("finalize_retry", 0) or 0)

    try:
        job = get_otc_list_job(admin_id, list_id)
        trades = job.get("trades") or []
        results_raw = job.get("results") or {}

        # إذا لسه في صفقات لم تُحفظ بسبب retries، انتظر قليلًا قبل التجميع النهائي.
        saved_count = len(results_raw) if isinstance(results_raw, dict) else 0
        if saved_count < len(trades) and finalize_retry < OTC_LIST_NO_DATA_MAX_RETRIES:
            logger.warning(
                "OTC list finalizer waiting for pending results | admin=%s | list=%s | saved=%s/%s | retry=%s",
                admin_id,
                list_id,
                saved_count,
                len(trades),
                finalize_retry + 1,
            )

            context.job_queue.run_once(
                finalize_otc_list_results_job,
                when=OTC_LIST_NO_DATA_RETRY_SECONDS,
                data={
                    "admin_id": admin_id,
                    "list_id": list_id,
                    "finalize_retry": finalize_retry + 1,
                },
                name=f"otc_list_finalize_retry_{admin_id}_{list_id}_{finalize_retry + 1}",
            )
            return

        items = []
        for idx, trade in enumerate(trades):
            saved = results_raw.get(str(idx)) if isinstance(results_raw, dict) else None

            if isinstance(saved, dict):
                # إذا محفوظة No Data، نجرب مرة أخيرة عند العرض النهائي لأن البيانات قد تكون وصلت بعد الحفظ.
                if saved.get("result") == "no_data":
                    fresh = evaluate_otc_list_trade(trade)
                    fresh["index"] = idx
                    fresh["evaluated_at"] = now_iso()
                    fresh["retry"] = safe_int(saved.get("retry"), 0) + 1
                    if fresh.get("result") != "no_data":
                        saved = fresh
                        save_otc_list_trade_result(admin_id, list_id, idx, saved)
                items.append(saved)
            else:
                item = evaluate_otc_list_trade(trade)
                item["index"] = idx
                item["evaluated_at"] = now_iso()
                items.append(item)
                save_otc_list_trade_result(admin_id, list_id, idx, item)

        items.sort(key=lambda x: int(x.get("index", 0)))
        result_text, meta = build_otc_list_results_message_from_items(items)

        raw_text = job.get("raw_text") or ""
        save_ready_otc_list_result(admin_id, raw_text, result_text, meta)

        get_otc_list_job_ref(admin_id, list_id).update({
            "status": "ready",
            "ready_at": now_iso(),
            "meta": meta,
        })

        await context.bot.send_message(
            chat_id=admin_id,
            text=(
                "✅ نتائج ليستة OTC جاهزة ومحفوظة.\n\n"
                "اضغط الزر بالأسفل لعرض النتائج ونسخها بأي وقت."
            ),
            reply_markup=admin_otc_list_ready_keyboard
        )

    except Exception as e:
        logger.exception("Could not finalize OTC list results: %s", e)

# ===== OTC LIVE LOSS LEARNING SYSTEM =====
def otc_live_learning_ref():
    return system_ref().child("otc_live_learning")


def get_otc_live_recent_trades_for_learning(day_key: str | None = None, limit: int = 30) -> list[dict]:
    try:
        day_key = day_key or get_otc_live_day_key()
        raw = otc_live_stats_ref().child(day_key).child("trades").get() or {}
        rows = []
        if isinstance(raw, dict):
            for trade_id, trade in raw.items():
                if isinstance(trade, dict):
                    item = dict(trade)
                    item["_id"] = trade_id
                    rows.append(item)
        rows.sort(key=lambda x: str(x.get("created_at", "")))
        return rows[-int(limit):] if limit and int(limit) > 0 else rows
    except Exception as e:
        logger.exception("Could not read recent OTC trades for learning: %s", e)
        return []


def get_otc_live_trade_units_for_learning(trade: dict) -> float:
    try:
        units = trade.get("units")
        if units is not None:
            return float(units)
    except Exception:
        pass
    return float(otc_live_trade_units(
        str(trade.get("result", "unknown")),
        safe_int(trade.get("martingale_step"), 0),
        safe_int(trade.get("payout"), 80),
    ))


def get_otc_live_pair_cooldown_until(pair: str) -> float:
    try:
        data = otc_live_learning_ref().child("pair_cooldowns").child(safe_key(pair)).get() or {}
        return float(data.get("until_ts", 0) or 0)
    except Exception:
        return 0.0


def set_otc_live_pair_cooldown(pair: str, minutes: int, reason: str):
    try:
        until_ts = time_module.time() + (int(minutes) * 60)
        otc_live_learning_ref().child("pair_cooldowns").child(safe_key(pair)).set({
            "pair": pair,
            "until_ts": until_ts,
            "until_iso": datetime.fromtimestamp(until_ts, tz=UTC).isoformat(),
            "reason": reason,
            "updated_at": now_iso(),
        })
        logger.warning("OTC learning pair cooldown set | pair=%s | minutes=%s | reason=%s", pair, minutes, reason)
    except Exception as e:
        logger.exception("Could not set OTC pair cooldown: %s", e)


def clear_expired_otc_live_pair_cooldowns():
    try:
        raw = otc_live_learning_ref().child("pair_cooldowns").get() or {}
        now_ts = time_module.time()
        if isinstance(raw, dict):
            for key, data in raw.items():
                if isinstance(data, dict) and float(data.get("until_ts", 0) or 0) <= now_ts:
                    otc_live_learning_ref().child("pair_cooldowns").child(key).delete()
    except Exception as e:
        logger.exception("Could not clear expired OTC pair cooldowns: %s", e)


def is_otc_live_pair_blocked_by_learning(pair: str) -> tuple[bool, str]:
    if not OTC_LIVE_LEARNING_ENABLED:
        return False, ""
    until_ts = get_otc_live_pair_cooldown_until(pair)
    if until_ts > time_module.time():
        remaining_min = round((until_ts - time_module.time()) / 60, 1)
        return True, f"pair cooldown {remaining_min}m remaining"
    return False, ""


def is_otc_live_caution_mode() -> tuple[bool, float, int]:
    if not OTC_LIVE_LEARNING_ENABLED:
        return False, 0.0, 0
    trades = get_otc_live_recent_trades_for_learning(limit=OTC_LIVE_CAUTION_LOOKBACK)
    decided = [t for t in trades if str(t.get("result")) in {"win", "loss"}]
    if len(decided) < max(5, min(OTC_LIVE_CAUTION_LOOKBACK, 10)):
        return False, 0.0, len(decided)
    net_units = round(sum(get_otc_live_trade_units_for_learning(t) for t in decided), 2)
    return net_units < 0, net_units, len(decided)


def get_otc_live_effective_min_quality() -> tuple[int, str]:
    base_quality = int(OTC_LIVE_MIN_QUALITY)
    caution, net_units, count = is_otc_live_caution_mode()
    if caution:
        boosted = base_quality + int(OTC_LIVE_CAUTION_MIN_QUALITY_BOOST)
        return boosted, f"caution mode: last {count} trades net={net_units}, min_quality {base_quality}->{boosted}"
    return base_quality, "normal"


def update_otc_live_learning_after_result(signal: dict, result: str):
    try:
        if not OTC_LIVE_LEARNING_ENABLED:
            return
        pair = signal.get("pair")
        if not pair or result not in {"win", "loss"}:
            return
        clear_expired_otc_live_pair_cooldowns()
        recent = get_otc_live_recent_trades_for_learning(limit=max(OTC_LIVE_PAIR_LOSS_LOOKBACK, 10))
        recent_pair = [t for t in recent if str(t.get("pair")) == str(pair)]
        recent_pair = recent_pair[-OTC_LIVE_PAIR_LOSS_LOOKBACK:]
        pair_losses = sum(1 for t in recent_pair if str(t.get("result")) == "loss")
        if result == "loss" and pair_losses >= OTC_LIVE_PAIR_LOSS_LIMIT:
            set_otc_live_pair_cooldown(
                pair,
                OTC_LIVE_PAIR_COOLDOWN_MINUTES,
                f"{pair_losses} losses in last {len(recent_pair)} pair trades",
            )
    except Exception as e:
        logger.exception("Could not update OTC live learning after result: %s", e)


def format_otc_live_learning_status() -> str:
    try:
        clear_expired_otc_live_pair_cooldowns()
        caution, net_units, count = is_otc_live_caution_mode()
        min_quality, quality_reason = get_otc_live_effective_min_quality()
        raw_cd = otc_live_learning_ref().child("pair_cooldowns").get() or {}
        lines = []
        now_ts = time_module.time()
        if isinstance(raw_cd, dict):
            for _, data in raw_cd.items():
                if isinstance(data, dict):
                    until_ts = float(data.get("until_ts", 0) or 0)
                    if until_ts > now_ts:
                        remaining = round((until_ts - now_ts) / 60, 1)
                        lines.append(f"• {data.get('pair')}: {remaining} دقيقة")
        cooldown_text = "\n".join(lines) if lines else "لا يوجد أزواج موقوفة حاليًا."
        return (
            "╔══════════════╗\n"
            "   🧠 تعلم OTC Live\n"
            "╚══════════════╝\n\n"
            f"الحالة: {'وضع حذر ⚠️' if caution else 'طبيعي ✅'}\n"
            f"آخر {count} صفقات صافيها: {net_units}\n"
            f"حد الجودة الحالي: {min_quality}\n"
            f"السبب: {quality_reason}\n\n"
            "الأزواج الموقوفة مؤقتًا:\n"
            f"{cooldown_text}"
        )
    except Exception as e:
        logger.exception("Could not build learning status: %s", e)
        return "تعذر عرض حالة التعلم."


# ===== OTC LIVE CHANNEL STATS =====
def otc_live_stats_ref():
    return system_ref().child("otc_live_channel_stats")


def get_otc_live_day_key(check_dt: datetime | None = None) -> str:
    return (check_dt or now_utc()).astimezone(UTC_PLUS_3).strftime("%Y-%m-%d")


def record_otc_live_channel_result(signal: dict, result: str):
    try:
        day_key = get_otc_live_day_key()
        ref = otc_live_stats_ref().child(day_key)

        current = ref.get() or {}
        total = safe_int(current.get("total"), 0) + 1
        wins = safe_int(current.get("wins"), 0)
        losses = safe_int(current.get("losses"), 0)
        unknown = safe_int(current.get("unknown"), 0)
        martingale_wins = safe_int(current.get("martingale_wins"), 0)
        direct_wins = safe_int(current.get("direct_wins"), 0)

        martingale_step = int(signal.get("martingale_step", 0) or 0)
        payout = int(float(signal.get("payout", 80) or 80))
        units = otc_live_trade_units(result, martingale_step, payout)

        if result == "win":
            wins += 1
            if martingale_step == 1:
                martingale_wins += 1
            else:
                direct_wins += 1
        elif result == "loss":
            losses += 1
        else:
            unknown += 1

        net_units = round(float(current.get("net_units", 0) or 0) + units, 4)
        gross_win_units = round(float(current.get("gross_win_units", 0) or 0) + max(units, 0), 4)
        gross_loss_units = round(float(current.get("gross_loss_units", 0) or 0) + min(units, 0), 4)

        ref.update({
            "date": day_key,
            "total": total,
            "wins": wins,
            "direct_wins": direct_wins,
            "losses": losses,
            "unknown": unknown,
            "martingale_wins": martingale_wins,
            "net_units": net_units,
            "gross_win_units": gross_win_units,
            "gross_loss_units": gross_loss_units,
            "updated_at": now_iso(),
        })

        ref.child("trades").push({
            "pair": signal.get("pair"),
            "symbol": signal.get("symbol"),
            "direction": signal.get("direction"),
            "original_direction": signal.get("original_direction"),
            "quality": signal.get("quality"),
            "payout": payout,
            "entry_time": signal.get("entry_time"),
            "result": result,
            "martingale_step": martingale_step,
            "units": units,
            "created_at": now_iso(),
        })
    except Exception as e:
        logger.exception("Could not record OTC live channel result: %s", e)


def build_otc_live_stats_message(day_key: str | None = None) -> str:
    day_key = day_key or get_otc_live_day_key()
    trades = get_otc_live_trades(day_key=day_key, after_reset=True)
    base_message = build_otc_live_stats_from_trades(
        trades=trades,
        title="📊 إحصائيات OTC Live",
        day_key=day_key,
        include_advice=False,
    )

    data = otc_live_stats_ref().child(day_key).get() or {}
    shadow = data.get("shadow_direction_test") or {}
    shadow_total = safe_int(shadow.get("total"), 0)
    normal_w = safe_int(shadow.get("normal_first_wins"), 0)
    normal_l = safe_int(shadow.get("normal_first_losses"), 0)
    reverse_w = safe_int(shadow.get("reverse_first_wins"), 0)
    reverse_l = safe_int(shadow.get("reverse_first_losses"), 0)

    normal_rate = round((normal_w / max(1, normal_w + normal_l)) * 100, 1) if shadow_total else 0
    reverse_rate = round((reverse_w / max(1, reverse_w + reverse_l)) * 100, 1) if shadow_total else 0

    if normal_rate > reverse_rate:
        best_mode = "NORMAL ➡️"
    elif reverse_rate > normal_rate:
        best_mode = "REVERSE 🔁"
    else:
        best_mode = "متعادل"

    extra = (
        "\n🧪 اختبار الاتجاه أول شمعة:\n"
        f"➡️ NORMAL: {normal_w}W / {normal_l}L | {normal_rate}%\n"
        f"🔁 REVERSE: {reverse_w}W / {reverse_l}L | {reverse_rate}%\n"
        f"🏆 الأفضل حاليًا: {best_mode}\n\n"
        "@coach_WAEL_trading\n"
        "@sttrade_helper_bot"
    )

    return base_message + extra


async def publish_daily_otc_live_stats(context: ContextTypes.DEFAULT_TYPE):
    try:
        yesterday = (now_utc().astimezone(UTC_PLUS_3) - timedelta(days=1)).strftime("%Y-%m-%d")
        trades = get_otc_live_trades(day_key=yesterday, after_reset=False)
        stats = calculate_otc_live_trade_stats(trades)

        if stats["wins"] >= stats["losses"]:
            title = "today totally win ✅"
        else:
            title = "today totally loss 💔"

        text = (
            "╔══════════════╗\n"
            f"    {title}\n"
            "╚══════════════╝\n"
            "—————————\n"
            f"{stats['wins']} win\n"
            "—————————\n"
            f"{stats['losses']} loss\n"
            "—————————"
        )

        await context.bot.send_message(
            chat_id=OTC_LIVE_CHANNEL_ID,
            text=text
        )
    except Exception as e:
        logger.exception("Daily OTC live stats publish error: %s", e)


# ===== OTC ENGINE =====
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

    return "CALL" if value % 2 == 0 else "PUT"


async def publish_otc_list(context: ContextTypes.DEFAULT_TYPE):
    try:
        if not is_channel_publish_enabled("otc"):
            return

        count = CHANNEL_DAILY_SIGNAL_COUNT
        interval_minutes = CHANNEL_SIGNAL_INTERVAL_MINUTES

        # النشر اليومي للقناة: ليستة واحدة فقط، 35 صفقة، بأزواج عشوائية من BRL/ARS/BDT.
        start_dt = next_full_minute(now_utc())

        signals = generate_channel_signals_random_pairs(
            CHANNEL_OTC_PAIRS,
            count,
            interval_minutes,
            start_dt
        )
        message_text = build_channel_otc_signals_message("MIXED OTC", count, interval_minutes, signals)

        await context.bot.send_message(
            chat_id=CHANNEL_ID,
            text=message_text,
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.exception("Publish OTC Error: %s", e)


async def schedule_random_daily_otc_list(context: ContextTypes.DEFAULT_TYPE):
    """يحدد وقت نشر عشوائي كل يوم بين 12:00 و 20:00 بتوقيت سوريا."""
    try:
        now_local = now_utc().astimezone(UTC_PLUS_3)

        start_window = now_local.replace(hour=12, minute=0, second=0, microsecond=0)
        end_window = now_local.replace(hour=20, minute=0, second=0, microsecond=0)

        if now_local >= end_window:
            start_window += timedelta(days=1)
            end_window += timedelta(days=1)
        elif now_local > start_window:
            start_window = now_local + timedelta(minutes=1)

        window_seconds = max(60, int((end_window - start_window).total_seconds()))
        delay_seconds = random.randint(0, window_seconds)

        run_at = start_window + timedelta(seconds=delay_seconds)

        context.job_queue.run_once(
            publish_otc_list,
            when=run_at.astimezone(UTC),
            name=f"daily_random_otc_publish_{run_at.strftime('%Y%m%d')}"
        )

        logger.info("Next random OTC channel list scheduled at %s", run_at.isoformat())

    except Exception as e:
        logger.exception("Schedule random OTC Error: %s", e)


def generate_signals(pair: str, count: int, interval_minutes: int, start_dt: datetime):
    signals = []

    for i in range(count):
        entry_time = start_dt + timedelta(minutes=i * interval_minutes)
        direction = get_stable_direction(pair, entry_time)
        formatted_time = format_utc_plus_3(entry_time)
        signals.append(f"{pair} — {formatted_time} — {direction}")

    return signals


def generate_channel_signals_random_pairs(pairs: list[str], count: int, interval_minutes: int, start_dt: datetime):
    signals = []

    for i in range(count):
        pair = random.choice(pairs)
        entry_time = start_dt + timedelta(minutes=i * interval_minutes)
        direction = get_stable_direction(pair, entry_time)
        formatted_time = format_utc_plus_3(entry_time)
        signals.append(f"{pair} — {formatted_time} — {direction}")

    return signals




def generate_live_otc_signals(pair: str, count: int, interval_minutes: int, start_dt: datetime):
    """خيار منفصل يعتمد على بث Quotex live فقط ولا يؤثر على ليستات OTC العادية."""
    signals = []

    for i in range(count):
        entry_time = start_dt + timedelta(minutes=i * interval_minutes)
        direction = get_live_otc_direction(pair, entry_time)
        formatted_time = format_utc_plus_3(entry_time)
        signals.append(f"{pair} — {formatted_time} — {direction}")

    return signals


def build_live_otc_signals_message(pair: str, count: int, interval_minutes: int, signals: list[str]) -> str:
    snapshot = get_live_otc_snapshot(pair)
    price = snapshot.get("price")
    tick_time = snapshot.get("time")

    price_line = f"💵 آخر سعر مباشر: {price}" if price is not None else "💵 آخر سعر مباشر: غير متوفر بعد"
    tick_line = f"🛰 آخر tick: {tick_time}" if tick_time is not None else "🛰 آخر tick: بانتظار البيانات"

    header = (
        "╔══════════════╗\n"
        "   📡 Quotex OTC LIVE\n"
        "╚══════════════╝\n\n"
        "هذا خيار إضافي منفصل عن ليستات OTC العادية.\n"
        "يعتمد على حركة السعر المباشرة من Quotex الآن.\n\n"
        f"{price_line}\n"
        f"{tick_line}\n\n"
        "⏰ توقيت المنصة\n"
        "UTC / GMT +3.00\n\n"
        "⚠️ ملاحظة:\n"
        "• هذا الخيار تجريبي مباشر ولا يغيّر نظام الليستات القديم.\n"
        "• مدة كل صفقة: 1M\n\n"
        "📍 الإشارات المباشرة:\n\n"
    )

    formatted_signals = []
    for signal in signals:
        parts = signal.split(" — ")
        if len(parts) == 3:
            pair_name, signal_time, direction = parts
            formatted_signals.append(f"M1 {pair_name} {signal_time} {direction}")
        else:
            formatted_signals.append(signal)

    return header + "\n".join(formatted_signals)


def build_signals_message(pair: str, count: int, interval_minutes: int, signals: list[str]) -> str:
    
    header = (
        "╔══════════════╗\n"
        "   📊 Quotex Signals - OTC\n"
        "╚══════════════╝\n\n"

        "⏰ توقيت المنصة\n"
        "UTC / GMT +3.00\n\n"

        "⚠️ ملاحظات مهمة:\n"
        "• مدة كل صفقة: 1M\n"
        "• تجنب صفقة عكس مومنتم\n"
        "• تجنب دخول الصفقة بعد شمعة دوجي\n"
        "• تجنب صفقة عكس تريند قوي\n"
        "• استخدم مضاعفة واحدة عند الخسارة\n\n"

        "📍 الإشارات:\n\n"
    )

    formatted_signals = []

    for signal in signals:
        # signal شكله: "USD/BRL-OTC — 21:06 — CALL"
        parts = signal.split(" — ")

        if len(parts) == 3:
            pair_name, signal_time, direction = parts
            formatted_signals.append(f"M1 {pair_name} {signal_time} {direction}")
        else:
            formatted_signals.append(signal)

    return header + "\n".join(formatted_signals)


def build_channel_otc_signals_message(pair: str, count: int, interval_minutes: int, signals: list[str]) -> str:
    """رسالة النشر التلقائي لقناة OTC فقط، بتفاصيل أخف بدون تحذيرات الدوجي/المومنتم/التريند."""
    header = (
        "╔══════════════╗\n"
        "   📊 Quotex Signals - OTC\n"
        "╚══════════════╝\n\n"

        "⏰ توقيت المنصة\n"
        "UTC / GMT +3.00\n\n"

        "⚠️ ملاحظات مهمة:\n"
        "• مدة كل صفقة: 1M\n"
        "• استخدم مضاعفة واحدة عند الخسارة\n"
        "• التزم بإدارة رأس المال ولا تدخل بأكثر من صفقة بنفس الوقت\n\n"

        "📍 الإشارات:\n\n"
    )

    formatted_signals = []
    for signal in signals:
        parts = signal.split(" — ")
        if len(parts) == 3:
            pair_name, signal_time, direction = parts
            formatted_signals.append(f"M1 {pair_name} {signal_time} {direction}")
        else:
            formatted_signals.append(signal)

    return header + "\n".join(formatted_signals)


# ===== REAL MARKET ENGINE =====

def is_real_pair_available(pair: str, check_dt: datetime | None = None) -> bool:
    if pair not in REAL_PAIRS:
        return False

    dt = (check_dt or now_utc()).astimezone(UTC)

    # سوق الفوركس يعمل تقريبًا 24/5:
    # يفتح مساء الأحد UTC ويغلق مساء الجمعة UTC.
    weekday = dt.weekday()  # Mon=0 ... Sun=6

    if weekday == 5:  # Saturday
        return False
    if weekday == 6 and dt.hour < 21:  # Sunday before open
        return False
    if weekday == 4 and dt.hour >= 21:  # Friday after close
        return False

    return True




def is_quotex_global_market_open(check_dt: datetime | None = None) -> bool:
    """
    يتحقق من حالة السوق العالمي كما نريدها لقناة Quotex Global.
    عند إغلاق السوق العالمي على Quotex تتحول الأزواج إلى OTC، لذلك لا ننشر في قناة العالمي.
    التوقيت هنا مبني على UTC+3 مثل باقي البوت.
    """
    dt = (check_dt or now_utc()).astimezone(UTC_PLUS_3)
    weekday = dt.weekday()  # Mon=0 ... Sun=6

    # السبت والأحد: لا يوجد سوق عالمي لقناة Quotex Global.
    if weekday in (5, 6):
        return False

    # الجمعة بعد وقت إغلاق Quotex العالمي: توقف كامل حتى لا ننشر على OTC.
    if weekday == 4 and dt.hour >= QUOTEX_GLOBAL_FRIDAY_CLOSE_HOUR_UTC_PLUS_3:
        return False

    # الاثنين قبل وقت الفتح المحدد، إن تم رفع قيمة الفتح لاحقًا.
    if weekday == 0 and dt.hour < QUOTEX_GLOBAL_MONDAY_OPEN_HOUR_UTC_PLUS_3:
        return False

    return True



def is_global_autopublish_allowed(check_dt: datetime | None = None) -> bool:
    """يسمح بالنشر التلقائي لقناة السوق العالمي فقط بين 10:00 و 21:00 بتوقيت سوريا/UTC+3،
    بشرط أن يكون سوق Quotex العالمي مفتوحًا أيضًا.
    """
    dt = (check_dt or now_utc()).astimezone(UTC_PLUS_3)

    if not is_quotex_global_market_open(dt):
        return False

    return GLOBAL_MARKET_AUTOPUBLISH_START_HOUR_UTC_PLUS_3 <= dt.hour < GLOBAL_MARKET_AUTOPUBLISH_END_HOUR_UTC_PLUS_3


def format_global_channel_pair(pair: str) -> str:
    return pair.replace("/", "")


def build_global_channel_signal_message(signal: dict) -> str:
    """رسالة مختصرة خاصة بالنشر التلقائي لقناة السوق العالمي فقط.
    لا تُستخدم في التوليد اليدوي حتى تبقى تفاصيل التحليل للمستخدم كما هي.
    """
    pair = format_global_channel_pair(str(signal.get("pair", "")))
    direction = str(signal.get("direction", ""))
    timeframe = int(signal.get("timeframe", signal.get("duration_minutes", 1)) or 1)

    entry_dt = parse_iso(str(signal.get("entry_time", "")))
    entry_text = entry_dt.astimezone(UTC_PLUS_3).strftime("%H:%M:%S") if entry_dt else "--:--:--"

    direction_line = "🟢 CALL" if direction == "CALL" else "🔴 PUT"

    return (
        "╔══════════════╗\n"
        "   🌍 TRADING TIME BOT\n"
        "╚══════════════╝\n\n"
        f"💎 {pair}\n"
        f"🔥 M{timeframe}\n"
        f"⌛️ {entry_text}\n"
        f"{direction_line}\n\n\n"
        "@coach_WAEL_trading\n"
        "@sttrade_helper_bot"
    )


def global_market_channel_state_ref():
    return system_ref().child("global_market_channel_state")


def get_global_market_channel_state():
    return global_market_channel_state_ref().get() or {}


def set_global_market_channel_state(data: dict):
    global_market_channel_state_ref().update(data)


async def notify_global_market_closed_once(context: ContextTypes.DEFAULT_TYPE):
    """يرسل رسالة إغلاق مرة واحدة فقط حتى لا يزعج القناة كل دقيقة."""
    if not GLOBAL_MARKET_CLOSED_MESSAGE_ENABLED:
        return

    state = get_global_market_channel_state()
    if state.get("status") == "closed" and state.get("closed_notified"):
        return

    try:
        await context.bot.send_message(
            chat_id=GLOBAL_CHANNEL_ID,
            text=GLOBAL_MARKET_CLOSED_MESSAGE,
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.exception("Global Market Closed Notify Error: %s", e)

    set_global_market_channel_state({
        "status": "closed",
        "closed_notified": True,
        "closed_at": now_iso(),
    })


def mark_global_market_open():
    """يعيد تهيئة حالة الإغلاق عند فتح السوق حتى يمكن إرسال تنبيه جديد بالإغلاق القادم."""
    state = get_global_market_channel_state()
    if state.get("status") != "open":
        set_global_market_channel_state({
            "status": "open",
            "closed_notified": False,
            "opened_at": now_iso(),
        })

def get_session_name(check_dt: datetime | None = None) -> str:
    dt = (check_dt or now_utc()).astimezone(UTC_PLUS_3)
    hour = dt.hour

    if 0 <= hour < 8:
        return "الآسيوية"
    if 8 <= hour < 16:
        return "الأوروبية"
    return "الأمريكية"


def get_price_decimals(pair: str) -> int:
    return 3 if pair == "USD/JPY" else 5


def format_price(pair: str, value: float) -> str:
    return f"{value:.{get_price_decimals(pair)}f}"


def get_pair_context(pair: str) -> dict:
    return PAIR_CONTEXT.get(pair, {"round_step": 0.0010, "near_factor": 0.18, "touch_factor": 0.07})


def aggregate_candles(candles, timeframe_minutes: int):
    if timeframe_minutes <= 1:
        return candles

    aggregated = []
    bucket = []
    bucket_start = None

    for candle in candles:
        candle_time = candle["time"]
        floored_minute = candle_time.minute - (candle_time.minute % timeframe_minutes)
        current_bucket_start = candle_time.replace(minute=floored_minute, second=0, microsecond=0)

        if bucket_start is None:
            bucket_start = current_bucket_start

        if current_bucket_start != bucket_start:
            if bucket:
                aggregated.append({
                    "time": bucket_start,
                    "open": bucket[0]["open"],
                    "high": max(x["high"] for x in bucket),
                    "low": min(x["low"] for x in bucket),
                    "close": bucket[-1]["close"],
                })
            bucket = []
            bucket_start = current_bucket_start

        bucket.append(candle)

    if bucket:
        aggregated.append({
            "time": bucket_start,
            "open": bucket[0]["open"],
            "high": max(x["high"] for x in bucket),
            "low": min(x["low"] for x in bucket),
            "close": bucket[-1]["close"],
        })

    return aggregated


def get_candles(pair: str, timeframe_minutes: int = 1, limit: int = 180):
    symbol = REAL_PAIR_TO_YAHOO_SYMBOL.get(pair)
    if not symbol:
        return None, f"الزوج {pair} غير مربوط برمز بيانات"

    # نجلب 1m دائمًا ثم نعيد تجميعها إلى 5m/10m.
    # السبب: حتى خيار الفاصل في السوق العالمي يصبح فريم تحليل حقيقي، وليس مجرد وقت دخول شكلي.
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

    try:
        data, request_error = request_json_with_retries(
            url,
            params={
                "range": "5d",
                "interval": "1m",
                "includePrePost": "false",
                "events": "div,splits",
            },
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            },
            timeout=10,
        )

        if request_error:
            return None, request_error

        chart = (data or {}).get("chart", {})
        result = (chart.get("result") or [None])[0]
        if not result:
            error_obj = chart.get("error") or {}
            return None, error_obj.get("description", "تعذر قراءة بيانات Yahoo")

        timestamps = result.get("timestamp") or []
        quote = (((result.get("indicators") or {}).get("quote") or [None])[0]) or {}

        opens = quote.get("open") or []
        highs = quote.get("high") or []
        lows = quote.get("low") or []
        closes = quote.get("close") or []

        candles_1m = []
        for ts, o, h, l, c in zip(timestamps, opens, highs, lows, closes):
            if None in (ts, o, h, l, c):
                continue
            candles_1m.append({
                "time": datetime.fromtimestamp(ts, tz=UTC),
                "open": float(o),
                "high": float(h),
                "low": float(l),
                "close": float(c),
            })

        if len(candles_1m) < 60:
            return None, "عدد شموع 1m غير كافٍ"

        candles = aggregate_candles(candles_1m, timeframe_minutes)
        if len(candles) < 35:
            return None, f"عدد الشموع غير كافٍ بعد تجميع فريم {timeframe_minutes}m"

        return candles[-limit:], None

    except Exception as e:
        return None, str(e)


def calculate_ema(candles, period: int):
    closes = [c["close"] for c in candles]
    ema = []
    k = 2 / (period + 1)

    for i, price in enumerate(closes):
        if i == 0:
            ema.append(price)
        else:
            ema.append(price * k + ema[i - 1] * (1 - k))

    return ema


def calculate_atr(candles, period: int = 14) -> float:
    if len(candles) < period + 1:
        return 0.0

    trs = []
    for i in range(1, len(candles)):
        cur = candles[i]
        prev = candles[i - 1]
        tr = max(
            cur["high"] - cur["low"],
            abs(cur["high"] - prev["close"]),
            abs(cur["low"] - prev["close"]),
        )
        trs.append(tr)

    recent = trs[-period:]
    return sum(recent) / len(recent) if recent else 0.0


def analyze_candle(candle: dict):
    body = abs(candle["close"] - candle["open"])
    full = candle["high"] - candle["low"]

    if full <= 0:
        return {
            "doji": True,
            "strong": False,
            "body_ratio": 0.0,
            "bullish": False,
            "bearish": False,
            "upper_wick": 0.0,
            "lower_wick": 0.0,
        }

    upper = candle["high"] - max(candle["close"], candle["open"])
    lower = min(candle["close"], candle["open"]) - candle["low"]
    body_ratio = body / full

    return {
        "doji": body_ratio <= 0.16,
        "strong": body_ratio >= 0.55,
        "body_ratio": body_ratio,
        "bullish": candle["close"] > candle["open"],
        "bearish": candle["close"] < candle["open"],
        "upper_wick": upper / full,
        "lower_wick": lower / full,
    }


def is_rejection_candle(candle: dict):
    body = abs(candle["close"] - candle["open"])
    upper = candle["high"] - max(candle["close"], candle["open"])
    lower = min(candle["close"], candle["open"]) - candle["low"]

    min_body = max(body, 1e-9)

    if lower >= min_body * 2 and upper <= min_body * 1.2:
        return "bullish"
    if upper >= min_body * 2 and lower <= min_body * 1.2:
        return "bearish"
    return None


def cluster_levels(values, tolerance: float):
    if not values:
        return []

    sorted_values = sorted(values)
    clusters = [[sorted_values[0]]]

    for value in sorted_values[1:]:
        if abs(value - median(clusters[-1])) <= tolerance:
            clusters[-1].append(value)
        else:
            clusters.append([value])

    levels = []
    for cluster in clusters:
        if len(cluster) >= 2:
            levels.append(median(cluster))

    return levels


def find_levels(candles, atr: float, lookback: int = 120):
    recent = candles[-lookback:]
    tolerance = max(atr * 0.35, 1e-6)

    swing_highs = []
    swing_lows = []

    for i in range(2, len(recent) - 2):
        h = recent[i]["high"]
        l = recent[i]["low"]

        if h >= recent[i - 1]["high"] and h >= recent[i - 2]["high"] and h >= recent[i + 1]["high"] and h >= recent[i + 2]["high"]:
            swing_highs.append(h)

        if l <= recent[i - 1]["low"] and l <= recent[i - 2]["low"] and l <= recent[i + 1]["low"] and l <= recent[i + 2]["low"]:
            swing_lows.append(l)

    supports = cluster_levels(swing_lows, tolerance)
    resistances = cluster_levels(swing_highs, tolerance)

    return supports, resistances


def nearest_level(price: float, levels: list[float], side: str):
    if not levels:
        return None

    if side == "support":
        candidates = [lvl for lvl in levels if lvl <= price]
        return max(candidates) if candidates else max(levels)

    candidates = [lvl for lvl in levels if lvl >= price]
    return min(candidates) if candidates else min(levels)


def classify_distance(price: float, level: float | None, atr: float, pair: str):
    if level is None:
        return "none", None

    dist = abs(price - level)
    ctx = get_pair_context(pair)
    touch_limit = max(atr * ctx["touch_factor"], 1e-6)
    near_limit = max(atr * ctx["near_factor"], touch_limit * 2.2)
    approach_limit = max(atr * 0.45, near_limit * 1.6)

    if dist <= touch_limit:
        return "touch", dist
    if dist <= near_limit:
        return "near", dist
    if dist <= approach_limit:
        return "approaching", dist
    return "far", dist


def round_number(price: float, pair: str):
    step = get_pair_context(pair)["round_step"]
    return round(price / step) * step


def get_round_levels(price: float, pair: str):
    step = get_pair_context(pair)["round_step"]
    center = round_number(price, pair)
    return [center - step, center, center + step]


def build_nearby_setup_lines(pair: str, price: float, atr: float, support: float | None, resistance: float | None, trend_bias: str):
    lines = []

    support_state, _ = classify_distance(price, support, atr, pair)
    resistance_state, _ = classify_distance(price, resistance, atr, pair)

    if support is not None and support_state in {"touch", "near", "approaching"}:
        if support_state == "touch":
            lines.append(f"📍 السعر يلامس دعم {format_price(pair, support)} — لا تبيع مباشرة، راقب ارتداد CALL أو كسرًا واضحًا ثم PUT")
        else:
            lines.append(f"📍 دعم قريب عند {format_price(pair, support)} — راقب CALL عند ظهور رفض سعري أو PUT فقط بعد كسر واضح")

    if resistance is not None and resistance_state in {"touch", "near", "approaching"}:
        if resistance_state == "touch":
            lines.append(f"📍 السعر يلامس مقاومة {format_price(pair, resistance)} — لا تشتري مباشرة، راقب رفضًا هابطًا ثم PUT أو كسرًا واضحًا ثم CALL")
        else:
            lines.append(f"📍 مقاومة قريبة عند {format_price(pair, resistance)} — راقب PUT عند ظهور رفض سعري أو CALL فقط بعد كسر واضح")

    for lvl in get_round_levels(price, pair):
        state, _ = classify_distance(price, lvl, atr, pair)
        if state in {"touch", "near"}:
            lines.append(f"🔢 Round Number قريب عند {format_price(pair, lvl)} — انتظر رد فعل واضح قبل الدخول")
            break

    uniq = []
    for line in lines:
        if line not in uniq:
            uniq.append(line)
    return uniq


def detect_market_structure(candles, lookback: int = 18) -> str:
    """تصنيف بسيط لبنية السوق: صاعد / هابط / رينج.
    السبب: الاعتماد على EMA والزخم فقط كان يعطي صفقات عند مناطق سيئة.
    """
    recent = candles[-lookback:] if len(candles) >= lookback else candles
    if len(recent) < 6:
        return "range"

    half = len(recent) // 2
    first_high = max(c["high"] for c in recent[:half])
    first_low = min(c["low"] for c in recent[:half])
    last_high = max(c["high"] for c in recent[half:])
    last_low = min(c["low"] for c in recent[half:])

    if last_high > first_high and last_low > first_low:
        return "uptrend"
    if last_high < first_high and last_low < first_low:
        return "downtrend"
    return "range"


def candle_body_ratio(candle: dict) -> float:
    full = candle["high"] - candle["low"]
    if full <= 0:
        return 0.0
    return abs(candle["close"] - candle["open"]) / full


def is_strong_breakout(candle: dict, level: float | None, direction: str, atr: float = 0.0) -> bool:
    """كسر حقيقي: إغلاق خارج المستوى + جسم شمعة واضح."""
    if level is None:
        return False

    full = candle["high"] - candle["low"]
    body = abs(candle["close"] - candle["open"])
    if full <= 0:
        return False

    body_ok = body / full >= 0.48
    buffer = max(atr * 0.03, 1e-9)

    if direction == "down":
        return candle["close"] < (level - buffer) and body_ok and candle["close"] < candle["open"]
    if direction == "up":
        return candle["close"] > (level + buffer) and body_ok and candle["close"] > candle["open"]
    return False


def is_strong_rejection_from_level(candle: dict, level: float | None, side: str, atr: float = 0.0):
    """رفض سعري من مستوى دعم/مقاومة."""
    if level is None:
        return None

    full = candle["high"] - candle["low"]
    if full <= 0:
        return None

    body = max(abs(candle["close"] - candle["open"]), 1e-9)
    upper = candle["high"] - max(candle["close"], candle["open"])
    lower = min(candle["close"], candle["open"]) - candle["low"]
    buffer = max(atr * 0.08, full * 0.08, 1e-9)

    if side == "support":
        touched = candle["low"] <= level + buffer
        closed_above = candle["close"] > level
        if touched and closed_above and lower >= body * 1.6:
            return "bullish"

    if side == "resistance":
        touched = candle["high"] >= level - buffer
        closed_below = candle["close"] < level
        if touched and closed_below and upper >= body * 1.6:
            return "bearish"

    return None


def is_exhausted_move(candles, direction: str, atr: float) -> bool:
    """يمنع الدخول المتأخر بعد حركة مستهلكة قرب مستوى."""
    if len(candles) < 5 or atr <= 0:
        return False

    recent = candles[-4:-1]
    if direction == "down":
        bearish_count = sum(1 for c in recent if c["close"] < c["open"])
        move_size = recent[0]["open"] - recent[-1]["close"]
        return bearish_count >= 3 and move_size >= atr * 1.15

    if direction == "up":
        bullish_count = sum(1 for c in recent if c["close"] > c["open"])
        move_size = recent[-1]["close"] - recent[0]["open"]
        return bullish_count >= 3 and move_size >= atr * 1.15

    return False


def build_conditional_message(header: str, reason: str, price_text: str, watch_time: str, plan_lines: list[str], notes: list[str] | None = None):
    msg = (
        header
        + "⚠️ فرصة مشروطة — لا تدخل مباشرة الآن\n\n"
        + f"السبب: {reason}\n"
        + f"💵 السعر الحالي: {price_text}\n"
        + f"⏰ وقت المراقبة: {watch_time}\n\n"
        + "📋 الخطة المقترحة:\n"
        + "\n".join(plan_lines)
    )
    if notes:
        msg += "\n\n📌 ملاحظات التحليل:\n" + "\n".join(notes[:5])
    return msg


def analyze_real_market(pair: str, timeframe_minutes: int):
    if not is_real_pair_available(pair):
        return {
            "ok": False,
            "setup_type": "closed",
            "quality": 0,
            "timeframe": timeframe_minutes,
            "message": f"❌ الزوج {pair} غير متاح الآن\n⏰ السوق مغلق"
        }

    candles, error_msg = get_candles(pair, timeframe_minutes=timeframe_minutes, limit=220)
    if not candles or len(candles) < 40:
        return {
            "ok": False,
            "setup_type": "error",
            "quality": 0,
            "timeframe": timeframe_minutes,
            "message": f"❌ فشل جلب بيانات السوق\n\nالسبب: {error_msg}"
        }

    ema9 = calculate_ema(candles, 9)
    ema21 = calculate_ema(candles, 21)
    atr = calculate_atr(candles, 14)

    last_closed = candles[-2]
    prev_closed = candles[-3]

    candle = analyze_candle(last_closed)
    structure = detect_market_structure(candles)

    supports, resistances = find_levels(candles[:-1], atr)
    price = last_closed["close"]

    support = nearest_level(price, supports, "support")
    resistance = nearest_level(price, resistances, "resistance")

    sup_state, _ = classify_distance(price, support, atr, pair)
    res_state, _ = classify_distance(price, resistance, atr, pair)

    nearest_round = min(get_round_levels(price, pair), key=lambda lvl: abs(lvl - price))
    round_state, _ = classify_distance(price, nearest_round, atr, pair)

    reject_support = is_strong_rejection_from_level(last_closed, support, "support", atr)
    reject_resistance = is_strong_rejection_from_level(last_closed, resistance, "resistance", atr)
    break_support = is_strong_breakout(last_closed, support, "down", atr)
    break_resistance = is_strong_breakout(last_closed, resistance, "up", atr)

    score_call = 0
    score_put = 0
    notes = []
    warnings = []

    trend_bias = "neutral"

    # 1) الاتجاه العام EMA
    if ema9[-2] > ema21[-2]:
        score_call += 2
        trend_bias = "bullish"
        notes.append("📈 الترند العام صاعد (EMA 9 فوق EMA 21)")
    elif ema9[-2] < ema21[-2]:
        score_put += 2
        trend_bias = "bearish"
        notes.append("📉 الترند العام هابط (EMA 9 تحت EMA 21)")

    # 2) بنية السوق Market Structure
    if structure == "uptrend":
        score_call += 3
        notes.append("🏗 بنية السوق صاعدة (قمم وقيعان أعلى)")
    elif structure == "downtrend":
        score_put += 3
        notes.append("🏗 بنية السوق هابطة (قمم وقيعان أدنى)")
    else:
        warnings.append("⚠️ بنية السوق متذبذبة — لا نعتمد على الترند وحده")

    # 3) الزخم آخر شمعة
    if last_closed["close"] > prev_closed["close"]:
        score_call += 2
        notes.append("⚡ الزخم الأخير لصالح الصعود")
    elif last_closed["close"] < prev_closed["close"]:
        score_put += 2
        notes.append("⚡ الزخم الأخير لصالح الهبوط")

    # 4) جودة الشمعة الأخيرة
    if candle["strong"]:
        if candle["bullish"]:
            score_call += 2
            notes.append("🟢 آخر شمعة مغلقة قوية صاعدة")
        elif candle["bearish"]:
            score_put += 2
            notes.append("🔴 آخر شمعة مغلقة قوية هابطة")

    if candle["doji"]:
        warnings.append("⚠️ آخر شمعة مغلقة قريبة من الدوجي — يلزم تأكيد إضافي")
        score_call -= 1
        score_put -= 1

    # 5) دعم ومقاومة: كسر / رفض / قرب
    if support is not None:
        if reject_support == "bullish":
            score_call += 4
            notes.append(f"🔥 رفض صاعد واضح من دعم {format_price(pair, support)}")
        elif break_support:
            score_put += 4
            notes.append(f"💥 كسر دعم بإغلاق واضح عند {format_price(pair, support)}")
        elif sup_state in {"touch", "near"}:
            warnings.append(f"📍 السعر قريب من دعم {format_price(pair, support)} — لا تطارد PUT بدون كسر واضح")

    if resistance is not None:
        if reject_resistance == "bearish":
            score_put += 4
            notes.append(f"🔥 رفض هابط واضح من مقاومة {format_price(pair, resistance)}")
        elif break_resistance:
            score_call += 4
            notes.append(f"💥 كسر مقاومة بإغلاق واضح عند {format_price(pair, resistance)}")
        elif res_state in {"touch", "near"}:
            warnings.append(f"📍 السعر قريب من مقاومة {format_price(pair, resistance)} — لا تطارد CALL بدون كسر واضح")

    # 6) Round Number
    if round_state == "touch":
        warnings.append(f"🔢 السعر يلامس Round Number {format_price(pair, nearest_round)} — انتظر تأكيد")
    elif round_state == "near":
        warnings.append(f"🔢 Round Number قريب عند {format_price(pair, nearest_round)}")

    # 7) منع مطاردة حركة مستهلكة عند منطقة
    exhausted_down = is_exhausted_move(candles, "down", atr)
    exhausted_up = is_exhausted_move(candles, "up", atr)
    if exhausted_down and support is not None and sup_state in {"touch", "near", "approaching"}:
        warnings.append("⚠️ هبوط مستهلك قرب دعم — البيع المتأخر خطر")
        score_put -= 2
    if exhausted_up and resistance is not None and res_state in {"touch", "near", "approaching"}:
        warnings.append("⚠️ صعود مستهلك قرب مقاومة — الشراء المتأخر خطر")
        score_call -= 2

    score_call = max(score_call, 0)
    score_put = max(score_put, 0)

    best_score = max(score_call, score_put)
    score_gap = abs(score_call - score_put)
    direction = None
    confidence = 0

    # القرار النهائي: لا نعطي صفقة مباشرة إلا عند توافق واضح.
    if score_call >= 8 and score_call >= score_put + 2:
        direction = "CALL"
        confidence = min(58 + score_call * 4 + min(score_gap, 3) * 3, 95)
    elif score_put >= 8 and score_put >= score_call + 2:
        direction = "PUT"
        confidence = min(58 + score_put * 4 + min(score_gap, 3) * 3, 95)
    elif best_score >= 7 and score_gap >= 3 and structure != "range":
        if score_call > score_put:
            direction = "CALL"
            confidence = min(54 + score_call * 4, 88)
        else:
            direction = "PUT"
            confidence = min(54 + score_put * 4, 88)

    entry_time = next_timeframe_boundary(now_utc(), timeframe_minutes)
    session_name = get_session_name()

    nearby_lines = build_nearby_setup_lines(pair, price, atr, support, resistance, trend_bias)
    for line in warnings:
        if line not in nearby_lines:
            nearby_lines.append(line)

    quality = max(best_score * 10 + min(score_gap, 3) * 5, 0)
    if direction:
        quality += 15

    header = (
        "🌍 السوق العالمي\n\n"
        f"💱 الزوج: {pair}\n"
        f"🕒 الجلسة: {session_name}\n"
        f"🧭 الفريم: {timeframe_minutes}M\n"
    )

    # فلتر المناطق الذكي: يمنع الدخول المباشر لكنه يعطي سيناريو بديل.
    if direction == "PUT" and support is not None and sup_state in {"touch", "near", "approaching"} and not break_support:
        reason = f"السعر قريب من دعم {format_price(pair, support)}"
        if reject_support == "bullish":
            reason = f"ظهر رفض صاعد من دعم {format_price(pair, support)}"
        return {
            "ok": False,
            "setup_type": "conditional",
            "quality": max(quality, 65),
            "timeframe": timeframe_minutes,
            "message": build_conditional_message(
                header,
                reason,
                format_price(pair, price),
                format_utc_plus_3(entry_time),
                [
                    "• إذا كسر الدعم بإغلاق واضح → خذ PUT",
                    "• إذا ظهر رفض صاعد من الدعم → خذ CALL",
                    "• لا تدخل PUT مباشرة قبل تأكيد الكسر",
                ],
                notes,
            )
        }

    if direction == "CALL" and resistance is not None and res_state in {"touch", "near", "approaching"} and not break_resistance:
        reason = f"السعر قريب من مقاومة {format_price(pair, resistance)}"
        if reject_resistance == "bearish":
            reason = f"ظهر رفض هابط من مقاومة {format_price(pair, resistance)}"
        return {
            "ok": False,
            "setup_type": "conditional",
            "quality": max(quality, 65),
            "timeframe": timeframe_minutes,
            "message": build_conditional_message(
                header,
                reason,
                format_price(pair, price),
                format_utc_plus_3(entry_time),
                [
                    "• إذا كسر المقاومة بإغلاق واضح → خذ CALL",
                    "• إذا ظهر رفض هابط من المقاومة → خذ PUT",
                    "• لا تدخل CALL مباشرة قبل تأكيد الكسر",
                ],
                notes,
            )
        }

    # إذا السوق رينج وفيه منطقة واضحة، نعطي مراقبة بدل رفض صامت.
    if direction is None and structure == "range" and nearby_lines:
        return {
            "ok": False,
            "setup_type": "conditional",
            "quality": max(quality, 55),
            "timeframe": timeframe_minutes,
            "message": (
                header
                + "⚠️ السوق متذبذب — الأفضل انتظار رد فعل من المنطقة\n\n"
                + "📋 سيناريوهات المراقبة:\n"
                + "\n".join(nearby_lines[:4])
            )
        }

    if direction:
        return {
            "ok": True,
            "setup_type": "direct",
            "quality": quality,
            "timeframe": timeframe_minutes,
            "pair": pair,
            "direction": direction,
            "confidence": confidence,
            "entry_price": price,
            "entry_time": entry_time.isoformat(),
            "duration_minutes": timeframe_minutes,
            "message": (
                header
                + f"📊 الثقة: {confidence}%\n\n"
                + f"📌 الإشارة: {direction}\n"
                + f"⏰ وقت الدخول: {format_utc_plus_3(entry_time)}\n"
                + f"💵 السعر الحالي: {format_price(pair, price)}\n"
                + f"⏳ مدة الصفقة المقترحة: {timeframe_minutes}M\n\n"
                + "\n".join(notes[:7])
                + (
                    "\n\n⚠️ تنبيهات إضافية:\n" + "\n".join(nearby_lines[:3])
                    if nearby_lines else ""
                )
            )
        }

    if nearby_lines:
        return {
            "ok": False,
            "setup_type": "watch",
            "quality": quality,
            "timeframe": timeframe_minutes,
            "message": (
                header
                + "❌ لا توجد صفقة مباشرة الآن\n\n"
                + "⚠️ لكن توجد فرص قريبة للمراقبة:\n"
                + "\n".join(nearby_lines[:4])
            )
        }

    reason = "السبب: شروط الدخول غير مكتملة بعد"
    if candle["doji"]:
        reason = "السبب: آخر شمعة مغلقة حيادية وتحتاج تأكيد"
    elif structure == "range":
        reason = "السبب: السوق متذبذب ولا توجد منطقة تأكيد واضحة"

    return {
        "ok": False,
        "setup_type": "none",
        "quality": quality,
        "timeframe": timeframe_minutes,
        "message": (
            header
            + "❌ لا توجد فرصة واضحة الآن\n\n"
            + reason
        )
    }





def global_active_trade_ref():
    return system_ref().child("global_active_trade")


def get_global_active_trade():
    return global_active_trade_ref().get()


def set_global_active_trade(data: dict):
    global_active_trade_ref().set(data)


def clear_global_active_trade():
    global_active_trade_ref().delete()





def signal_history_ref():
    return system_ref().child("signal_history")


def record_signal_history(signal: dict, source: str = "global_channel") -> str | None:
    try:
        payload = {
            "source": source,
            "pair": signal.get("pair"),
            "direction": signal.get("direction"),
            "timeframe": signal.get("timeframe"),
            "duration_minutes": signal.get("duration_minutes", signal.get("timeframe", 1)),
            "confidence": signal.get("confidence"),
            "quality": signal.get("quality"),
            "adjusted_quality": signal.get("adjusted_quality", signal.get("quality")),
            "entry_price": signal.get("entry_price"),
            "entry_time": signal.get("entry_time"),
            "published_at": now_iso(),
            "status": "published",
        }
        new_ref = signal_history_ref().push(payload)
        return new_ref.key
    except Exception as e:
        logger.exception("Could not record signal history: %s", e)
        return None


def update_signal_history_result(history_id: str | None, result: dict | None = None, *, status: str = "resolved"):
    if not history_id:
        return
    try:
        payload = {
            "status": status,
            "resolved_at": now_iso(),
        }
        if result:
            payload.update({
                "is_win": bool(result.get("is_win")),
                "martingale_step": int(result.get("martingale_step", 0)),
                "result_source": result.get("source"),
                "close_price": result.get("close_price"),
                "martingale_close_price": result.get("martingale_close_price"),
            })
        signal_history_ref().child(history_id).update(payload)
    except Exception as e:
        logger.exception("Could not update signal history %s: %s", history_id, e)


def get_recent_signal_history(hours: int = 24) -> list[dict]:
    try:
        data = signal_history_ref().get() or {}
    except Exception as e:
        logger.exception("Could not read signal history: %s", e)
        return []

    cutoff = now_utc() - timedelta(hours=hours)
    rows = []
    for signal_id, row in data.items():
        if not isinstance(row, dict):
            continue
        published_at = parse_iso(str(row.get("published_at", "")))
        if published_at and published_at >= cutoff:
            row = dict(row)
            row["id"] = signal_id
            rows.append(row)

    rows.sort(key=lambda x: x.get("published_at", ""), reverse=True)
    return rows


def is_pair_on_global_cooldown(pair: str, timeframe: int, minutes: int | None = None) -> bool:
    minutes = minutes or GLOBAL_PAIR_COOLDOWN_MINUTES
    recent = get_recent_signal_history(hours=max(1, (minutes // 60) + 2))
    cutoff = now_utc() - timedelta(minutes=minutes)

    for row in recent:
        if row.get("source") != "global_channel":
            continue
        if row.get("pair") != pair:
            continue
        if safe_int(row.get("timeframe"), 0) != safe_int(timeframe, 0):
            continue
        published_at = parse_iso(str(row.get("published_at", "")))
        if published_at and published_at >= cutoff:
            return True
    return False


def get_pair_recent_performance(pair: str, timeframe: int | None = None, lookback: int = 20):
    rows = get_recent_signal_history(hours=24 * 14)
    filtered = []
    for row in rows:
        if row.get("pair") != pair:
            continue
        if timeframe is not None and safe_int(row.get("timeframe"), 0) != safe_int(timeframe, 0):
            continue
        if row.get("status") != "resolved" or "is_win" not in row:
            continue
        filtered.append(row)
        if len(filtered) >= lookback:
            break

    if not filtered:
        return {"count": 0, "wins": 0, "losses": 0, "win_rate": None}

    wins = sum(1 for row in filtered if row.get("is_win"))
    losses = len(filtered) - wins
    return {
        "count": len(filtered),
        "wins": wins,
        "losses": losses,
        "win_rate": round((wins / len(filtered)) * 100, 1),
    }


def enrich_global_candidate(result: dict) -> dict:
    result = dict(result)
    perf = get_pair_recent_performance(result.get("pair"), result.get("timeframe"))
    adjusted_quality = safe_int(result.get("quality"), 0)

    if perf.get("count", 0) >= 5 and perf.get("win_rate") is not None:
        win_rate = float(perf["win_rate"])
        if win_rate < 45:
            adjusted_quality -= 18
        elif win_rate < 55:
            adjusted_quality -= 8
        elif win_rate >= 70:
            adjusted_quality += 8

    result["recent_performance"] = perf
    result["adjusted_quality"] = max(adjusted_quality, 0)
    return result


def is_publishable_global_candidate(result: dict) -> bool:
    if not result.get("ok"):
        return False

    if safe_int(result.get("confidence"), 0) < GLOBAL_MIN_CONFIDENCE:
        return False

    if safe_int(result.get("quality"), 0) < GLOBAL_MIN_QUALITY:
        return False

    if is_pair_on_global_cooldown(result.get("pair"), result.get("timeframe")):
        return False

    enriched = enrich_global_candidate(result)
    if safe_int(enriched.get("adjusted_quality"), 0) < GLOBAL_MIN_QUALITY:
        return False

    result.update(enriched)
    return True

def tv_make_session(prefix: str = "qs") -> str:
    return prefix + "_" + hashlib.sha256(f"{prefix}|{now_iso()}|{random.random()}".encode("utf-8")).hexdigest()[:12]


def tv_prepend_header(message: str) -> str:
    return f"~m~{len(message)}~m~{message}"


def tv_send(ws, func: str, params: list):
    ws.send(tv_prepend_header(json.dumps({"m": func, "p": params}, separators=(",", ":"))))


def parse_tradingview_series(raw_message: str):
    candles = []

    try:
        # TradingView messages are wrapped like ~m~LEN~m~JSON
        messages = re.split(r"~m~\d+~m~", raw_message)
        for msg in messages:
            if not msg or "timescale_update" not in msg:
                continue

            try:
                data = json.loads(msg)
            except Exception:
                continue

            payload = data.get("p", [])
            if len(payload) < 2:
                continue

            series_data = payload[1]
            for _, series_obj in series_data.items():
                points = series_obj.get("s", [])
                for point in points:
                    values = point.get("v", [])
                    if len(values) < 5:
                        continue

                    ts = int(float(values[0]))
                    candles.append({
                        "time": datetime.fromtimestamp(ts, tz=UTC),
                        "open": float(values[1]),
                        "high": float(values[2]),
                        "low": float(values[3]),
                        "close": float(values[4]),
                    })

    except Exception:
        pass

    candles.sort(key=lambda c: c["time"])
    return candles


def get_tradingview_candles(pair: str, interval: str = "1", bars: int = 80):
    """يجلب شموع 1m من TradingView عبر websocket غير رسمي.
    إذا فشل الاتصال أو لم تتوفر المكتبة يرجع None مع سبب واضح.
    """
    if websocket is None:
        return None, "websocket-client غير مثبت"

    symbol = REAL_PAIR_TO_TV_SYMBOL.get(pair)
    if not symbol:
        return None, f"لا يوجد رمز TradingView للزوج {pair}"

    qs = tv_make_session("qs")
    cs = tv_make_session("cs")

    try:
        ws = websocket.create_connection(
            TRADINGVIEW_WS_URL,
            timeout=12,
            header=[
                "Origin: https://www.tradingview.com",
                "User-Agent: Mozilla/5.0",
            ],
        )

        tv_send(ws, "quote_create_session", [qs])
        tv_send(ws, "quote_set_fields", [qs, "lp", "ch", "chp", "bid", "ask", "open_price", "high_price", "low_price", "prev_close_price"])
        tv_send(ws, "quote_add_symbols", [qs, symbol, {"flags": ["force_permission"]}])
        tv_send(ws, "chart_create_session", [cs, ""])
        tv_send(ws, "resolve_symbol", [cs, "symbol_1", json.dumps({"symbol": symbol, "adjustment": "splits", "session": "regular"})])
        tv_send(ws, "create_series", [cs, "s1", "s1", "symbol_1", interval, bars])

        raw = ""
        for _ in range(40):
            msg = ws.recv()
            raw += msg
            if '"timescale_update"' in msg and '"s":[' in msg:
                candles = parse_tradingview_series(raw)
                if candles:
                    ws.close()
                    return candles[-bars:], None

        ws.close()
        candles = parse_tradingview_series(raw)
        if candles:
            return candles[-bars:], None

        return None, "لم يتم استقبال شموع من TradingView"

    except Exception as e:
        try:
            ws.close()
        except Exception:
            pass
        return None, str(e)


def get_result_candles(pair: str, limit: int = 80):
    """مصدر التحقق الأساسي TradingView، والاحتياطي Yahoo إذا فشل."""
    candles, error_msg = get_tradingview_candles(pair, interval="1", bars=limit)
    if candles:
        return candles, "TradingView", None

    fallback_candles, fallback_error = get_candles(pair, timeframe_minutes=1, limit=limit)
    if fallback_candles:
        return fallback_candles, "YahooFallback", error_msg

    return None, None, f"TradingView: {error_msg} | Yahoo: {fallback_error}"

def floor_to_minute(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(second=0, microsecond=0)


def find_candle_by_minute(candles: list[dict], target_dt: datetime, allow_nearest: bool = True):
    target = floor_to_minute(target_dt)
    exact = [c for c in candles if floor_to_minute(c["time"]) == target]
    if exact:
        return exact[0]

    # في نتائج الصفقات لا نستخدم أقرب شمعة، لأن هذا قد يحسب شمعة غير شمعة الدخول/المضاعفة.
    # نسمح بالـ fallback فقط للأماكن القديمة التي قد تحتاجه، أما التحقق فيستدعي allow_nearest=False.
    if not allow_nearest:
        return None

    candidates = []
    for c in candles:
        diff = abs((floor_to_minute(c["time"]) - target).total_seconds())
        if diff <= 120:
            candidates.append((diff, c))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def get_real_trade_result_from_candles(pair: str, direction: str, entry_time: datetime, duration_minutes: int, fallback_entry_price=None):
    """تحقق النتيجة مع مضاعفة واحدة:
    - إذا ربحت الصفقة من أول شمعة: WIN عادي.
    - إذا خسرت الصفقة من أول شمعة: ننتظر الصفقة التالية بنفس الاتجاه ونفس المدة.
    - إذا ربحت المضاعفة: WIN ✅¹.
    - إذا خسرت المضاعفة: Loss.

    مهم جدًا:
    المضاعفة لا تُقارن بسعر دخول الصفقة الأصلية.
    المضاعفة تُحسب من شمعة المضاعفة نفسها: Open شمعة المضاعفة مقابل Close نفس الشمعة.
    مثال PUT: إذا الشمعة الأصلية خضرا ثم الشمعة التالية حمرا => WIN ✅¹.
    """
    try:
        candles, source_name, error_msg = get_result_candles(pair, limit=140)
    except Exception as e:
        return None, f"خطأ في مصدر التحقق: {e}"

    if isinstance(candles, tuple):
        candles = candles[0]

    if not candles or not isinstance(candles, list):
        return None, f"تعذر جلب بيانات التحقق: {error_msg}"

    candles = [c for c in candles if isinstance(c, dict) and "time" in c and "open" in c and "close" in c]

    # مهم جدًا: لا نحسب على شمعة ما زالت تتشكل.
    # TradingView/Yahoo قد يرجعان شمعة الدقيقة الحالية قبل إغلاقها، وهذا كان يسبب
    # تسجيل Loss للمضاعفة بينما الشمعة لاحقًا تغلق باتجاه الصفقة.
    current_open_minute = floor_to_minute(now_utc())
    candles = [c for c in candles if floor_to_minute(c["time"]) < current_open_minute]

    if not candles:
        return None, "بيانات الشموع المغلقة غير صالحة أو غير متاحة بعد"

    entry_time = floor_to_minute(entry_time)
    duration_minutes = max(1, int(duration_minutes))
    expiry_time = entry_time + timedelta(minutes=duration_minutes)

    def judge_trade(open_price: float, close_price: float):
        if direction == "CALL":
            return close_price > open_price
        if direction == "PUT":
            return close_price < open_price
        return None

    if direction not in {"CALL", "PUT"}:
        return None, "اتجاه الصفقة غير معروف"

    # نتيجة الدخول الأساسي
    entry_candle = find_candle_by_minute(candles, entry_time, allow_nearest=False)
    close_candle = find_candle_by_minute(candles, expiry_time - timedelta(minutes=1), allow_nearest=False)

    if not close_candle:
        return None, "شمعة الإغلاق الأساسية غير متاحة بعد"

    if entry_candle:
        entry_price = float(entry_candle["open"])
    elif fallback_entry_price is not None:
        entry_price = float(fallback_entry_price)
    else:
        return None, "شمعة الدخول الأساسية غير متاحة"

    close_price = float(close_candle["close"])
    direct_is_win = judge_trade(entry_price, close_price)

    if direct_is_win is True:
        return {
            "is_win": True,
            "martingale_step": 0,
            "entry_price": entry_price,
            "close_price": close_price,
            "entry_time": entry_time,
            "expiry_time": expiry_time,
            "source": source_name,
        }, None

    # إذا خسرت مباشرة، لا ننشر Loss قبل فحص مضاعفة واحدة بعد انتهاء الصفقة الأساسية.
    martingale_entry_time = expiry_time
    martingale_expiry_time = martingale_entry_time + timedelta(minutes=duration_minutes)

    # لا نفحص المضاعفة قبل إغلاق شمعتها نهائيًا + هامش انتظار بسيط لمصدر البيانات.
    # مثال 1M: دخول 22:45، الأصلية تغلق 22:46، المضاعفة 22:46 وتغلق 22:47.
    # لا يجوز الحكم على المضاعفة عند 22:46:30 لأنها ما زالت مفتوحة.
    if now_utc() < martingale_expiry_time + timedelta(seconds=TRADINGVIEW_RESULT_RETRY_SECONDS):
        return None, "ننتظر إغلاق شمعة المضاعفة"

    martingale_entry_candle = find_candle_by_minute(candles, martingale_entry_time, allow_nearest=False)
    martingale_close_candle = find_candle_by_minute(candles, martingale_expiry_time - timedelta(minutes=1), allow_nearest=False)

    if not martingale_entry_candle:
        return None, "شمعة دخول المضاعفة غير متاحة بعد"

    if not martingale_close_candle:
        return None, "شمعة إغلاق المضاعفة غير متاحة بعد"

    martingale_entry_price = float(martingale_entry_candle["open"])
    martingale_close_price = float(martingale_close_candle["close"])
    martingale_is_win = judge_trade(martingale_entry_price, martingale_close_price)

    return {
        "is_win": bool(martingale_is_win),
        "martingale_step": 1,
        "entry_price": entry_price,
        "close_price": close_price,
        "martingale_entry_price": martingale_entry_price,
        "martingale_close_price": martingale_close_price,
        "entry_time": entry_time,
        "expiry_time": expiry_time,
        "martingale_entry_time": martingale_entry_time,
        "martingale_expiry_time": martingale_expiry_time,
        "source": source_name,
    }, None

async def resolve_global_active_trade_if_due(context: ContextTypes.DEFAULT_TYPE) -> bool:
    """يرجع True إذا في صفقة عالمية نشطة ولسا ما انتهت.
    إذا انتهت، ينتظر توفر شمعة الإغلاق، ثم يتحقق من النتيجة بدقة أعلى.
    """
    trade = get_global_active_trade()
    if not trade:
        return False

    expires_at = parse_iso(trade.get("expires_at", ""))
    if not expires_at:
        clear_global_active_trade()
        return False

    # ننتظر بعد انتهاء الصفقة حتى تتوفر شمعة الإغلاق من مصدر البيانات.
    result_check_at = parse_iso(trade.get("result_check_at", ""))
    if not result_check_at:
        result_check_at = expires_at + timedelta(seconds=TRADINGVIEW_RESULT_RETRY_SECONDS)

    if result_check_at > now_utc():
        return True

    pair = trade.get("pair")
    direction = trade.get("direction")
    entry_time = parse_iso(trade.get("entry_time", ""))
    duration_minutes = int(trade.get("duration_minutes", trade.get("timeframe", 1)))
    fallback_entry_price = trade.get("entry_price")

    if not pair or not direction or not entry_time:
        clear_global_active_trade()
        return False

    try:
        result, error_msg = get_real_trade_result_from_candles(
            pair=pair,
            direction=direction,
            entry_time=entry_time,
            duration_minutes=duration_minutes,
            fallback_entry_price=fallback_entry_price,
        )

        if not result:
            if int(trade.get("result_retry_count", 0)) % 5 == 0:
                logger.info("Global result check waiting: %s", error_msg)

            # إذا البيانات تأخرت، لا نسجل Loss غلط؛ نعيد المحاولة لاحقًا
            # ولا ننشر أي شيء غير WIN أو Loss.
            retry_count = int(trade.get("result_retry_count", 0)) + 1
            if retry_count >= GLOBAL_MAX_RESULT_RETRIES:
                logger.error("Global result unresolved after %s retries: %s", retry_count, error_msg)
                update_signal_history_result(trade.get("history_id"), status="unresolved")
                clear_global_active_trade()
                return False

            trade["result_retry_count"] = retry_count
            trade["result_check_at"] = (now_utc() + timedelta(seconds=TRADINGVIEW_RESULT_RETRY_SECONDS)).isoformat()
            set_global_active_trade(trade)
            return True

        if result["is_win"] and int(result.get("martingale_step", 0)) == 1:
            result_text = "WIN ✅¹"
        elif result["is_win"]:
            result_text = "WIN ✅"
        else:
            result_text = "💔 Loss"

        await safe_send_message(
            context.bot,
            chat_id=GLOBAL_CHANNEL_ID,
            text=result_text,
            parse_mode="Markdown"
        )

        update_signal_history_result(trade.get("history_id"), result, status="resolved")
        clear_global_active_trade()
        return False

    except Exception as e:
        logger.exception("Resolve Global Trade Error: %s", e)
        return True


async def auto_publish_real_market(context: ContextTypes.DEFAULT_TYPE):
    try:
        # أولًا: إذا في صفقة منشورة سابقًا، لا ننشر صفقة جديدة قبل انتهائها.
        active_still_running = await resolve_global_active_trade_if_due(context)
        if active_still_running:
            return

        if not is_channel_publish_enabled("global"):
            return

        # قناة السوق العالمي تبقى Global فقط.
        # إذا Quotex حوّل الأزواج إلى OTC، نوقف النشر ولا نرسل أي صفقة OTC هنا.
        if not is_global_autopublish_allowed():
            await notify_global_market_closed_once(context)
            return

        mark_global_market_open()

        shuffled_pairs = REAL_PAIRS[:]
        random.shuffle(shuffled_pairs)

        # 1) الأولوية دائمًا لصفقات 1M.
        # إذا وجدنا فرصة 1M مباشرة، ننشرها حتى لو كانت هناك فرصة 5M/10M بجودة أعلى.
        primary_candidates = []
        for pair in shuffled_pairs:
            for timeframe in GLOBAL_AUTOPUBLISH_PRIMARY_TIMEFRAMES:
                result = analyze_real_market(pair, timeframe)
                if is_publishable_global_candidate(result):
                    primary_candidates.append(result)

        if primary_candidates:
            candidates = primary_candidates
        else:
            # 2) إذا لم توجد 1M، نفحص 5M/10M فقط عندما يكون وقت الدخول قريبًا جدًا
            # قبل بداية الشمعة القادمة بحوالي دقيقة.
            # مثال: لفريم 5M لا ننشر عند 22:35:25 لصفقة دخولها 22:40، بل ننتظر قرب 22:39.
            allowed_secondary_timeframes = [
                tf for tf in GLOBAL_AUTOPUBLISH_SECONDARY_TIMEFRAMES
                if can_autopublish_timeframe(tf)
            ]

            if not allowed_secondary_timeframes:
                return

            candidates = []
            for pair in shuffled_pairs:
                for timeframe in allowed_secondary_timeframes:
                    result = analyze_real_market(pair, timeframe)
                    if is_publishable_global_candidate(result):
                        candidates.append(result)

        if not candidates:
            return

        # الأفضلية داخل نفس مجموعة الأولوية: أعلى جودة ثم أعلى ثقة.
        best_result = max(
            candidates,
            key=lambda r: (
                r.get("adjusted_quality", r.get("quality", 0)),
                r.get("confidence", 0),
            )
        )

        sent_message = await safe_send_message(
            context.bot,
            chat_id=GLOBAL_CHANNEL_ID,
            text=build_global_channel_signal_message(best_result),
            parse_mode="HTML",
        )
        if not sent_message:
            return

        history_id = record_signal_history(best_result, source="global_channel")

        entry_time = parse_iso(best_result.get("entry_time", ""))
        duration_minutes = int(best_result.get("duration_minutes", best_result.get("timeframe", 1)))
        if not entry_time:
            entry_time = now_utc()

        expires_at = entry_time + timedelta(minutes=duration_minutes)

        set_global_active_trade({
            "pair": best_result.get("pair"),
            "direction": best_result.get("direction"),
            "entry_price": best_result.get("entry_price"),
            "timeframe": best_result.get("timeframe"),
            "duration_minutes": duration_minutes,
            "entry_time": entry_time.isoformat(),
            "expires_at": expires_at.isoformat(),
            "result_check_at": (expires_at + timedelta(seconds=TRADINGVIEW_RESULT_RETRY_SECONDS)).isoformat(),
            "published_at": now_iso(),
            "history_id": history_id,
            "result_retry_count": 0,
        })

    except Exception as e:
        logger.exception("Auto Global Market Error: %s", e)


def analyze_real_market_best(pair: str):
    results = [analyze_real_market(pair, tf) for tf in REAL_INTERVALS]
    successful = [r for r in results if r.get("ok")]

    if successful:
        best = max(successful, key=lambda x: (x.get("quality", 0), x.get("confidence", 0)))
        best["message"] += "\n\n🔥 تم اختيار هذه الصفقة لأنها الأقوى بين 1M / 5M / 10M"
        return best

    conditional = [r for r in results if r.get("setup_type") == "conditional"]
    if conditional:
        best = max(conditional, key=lambda x: x.get("quality", 0))
        best["message"] += "\n\n🔥 لا توجد صفقة مباشرة قوية، لذلك تم عرض أفضل فرصة مشروطة بين 1M / 5M / 10M"
        return best

    best_watch = max(results, key=lambda x: x.get("quality", 0))
    frames_summary = "\n".join(
        [f"• {r.get('timeframe')}M: {'جاهزة' if r.get('ok') else 'لا'}" for r in results]
    )
    best_watch["message"] += "\n\n📋 ملخص الفريمات المفحوصة:\n" + frames_summary
    return best_watch


def reset_signal_state(context: ContextTypes.DEFAULT_TYPE):
    context.user_data["step"] = None
    context.user_data["mode"] = None
    context.user_data["pair"] = None
    context.user_data["count"] = None
    context.user_data["interval"] = None
    context.user_data["otc_submode"] = None
    context.user_data["admin_target_id"] = None


def build_pending_request_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🗓 أسبوع", callback_data=f"approve_week:{user_id}"),
            InlineKeyboardButton("🗓 شهر", callback_data=f"approve_month:{user_id}"),
        ],
        [
            InlineKeyboardButton("♾ دائم", callback_data=f"approve_forever:{user_id}")
        ],
        [
            InlineKeyboardButton("❌ رفض", callback_data=f"reject:{user_id}")
        ]
    ])


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
        f"• Telegram ID: <code>{target_id}</code>\n"
        f"• Quotex ID: <code>{quotex_id}</code>\n"
        f"• الحالة: {status}\n"
        f"• آخر نشاط: {format_dt_ar(data.get('last_seen', ''))}\n"
        f"• انتهاء الصلاحية: {expires_at}"
    )

    if show_admin_actions:
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=admin_duration_keyboard)
    else:
        await update.message.reply_text(msg, parse_mode="HTML")


async def send_maintenance_message(update: Update):
    await update.message.reply_text(
        "🛠 البوت تحت الصيانة حاليًا\n\n"
        "نقوم حاليًا ببعض التحديثات والتحسينات.\n"
        "يرجى المحاولة لاحقًا.\n\n"
        "شكرًا لتفهمك 🤍"
    )


async def handle_admin_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user

    if not is_admin(user.id):
        await query.answer("هذا الزر للأدمن فقط", show_alert=True)
        return

    data = query.data or ""
    await query.answer()

    try:
        action, target_id_str = data.split(":")
        target_id = int(target_id_str)
    except Exception:
        await query.answer("بيانات غير صالحة", show_alert=True)
        return

    user_data = get_user_record(target_id)
    target_name = user_data.get("name", "المستخدم") if user_data else "المستخدم"

    if action == "approve_week":
        set_user_expiry(target_id, "week")
        await query.edit_message_text(
            f"✅ تم تفعيل {target_name} لمدة أسبوع\n"
            f"🆔 Telegram ID: <code>{target_id}</code>",
            parse_mode="HTML"
        )
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text="✅ تم تفعيل حسابك بنجاح لمدة أسبوع\n\nأصبح بإمكانك الآن استخدام بوت TRADING TIME.\nاضغط /start للدخول إلى القائمة الرئيسية."
            )
        except Exception:
            pass
        return

    if action == "approve_month":
        set_user_expiry(target_id, "month")
        await query.edit_message_text(
            f"✅ تم تفعيل {target_name} لمدة شهر\n"
            f"🆔 Telegram ID: <code>{target_id}</code>",
            parse_mode="HTML"
        )
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text="✅ تم تفعيل حسابك بنجاح لمدة شهر\n\nأصبح بإمكانك الآن استخدام بوت TRADING TIME.\nاضغط /start للدخول إلى القائمة الرئيسية."
            )
        except Exception:
            pass
        return

    if action == "approve_forever":
        set_user_expiry(target_id, "forever")
        await query.edit_message_text(
            f"✅ تم تفعيل {target_name} بشكل دائم\n"
            f"🆔 Telegram ID: <code>{target_id}</code>",
            parse_mode="HTML"
        )
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text="✅ تم تفعيل حسابك بنجاح بشكل دائم\n\nأصبح بإمكانك الآن استخدام بوت TRADING TIME.\nاضغط /start للدخول إلى القائمة الرئيسية."
            )
        except Exception:
            pass
        return

    if action == "reject":
        block_user(target_id)
        await query.edit_message_text(
            f"❌ تم رفض/حظر {target_name}\n"
            f"🆔 Telegram ID: <code>{target_id}</code>",
            parse_mode="HTML"
        )
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text="❌ تم رفض طلبك أو إيقافه\n\nإذا كنت ترى أن هذا بالخطأ، تواصل مع الأدمن."
            )
        except Exception:
            pass
        return


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
    if not update.message or not update.message.text:
        return

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
            await update.message.reply_text(WELCOME_MESSAGE)
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
                f"🆔 Telegram ID: <code>{user.id}</code>\n"
                f"💱 Quotex ID: <code>{quotex_id}</code>\n\n"
                "──────────────"
            )
            try:
                await context.bot.send_message(
                    chat_id=ADMIN_TELEGRAM_ID,
                    text=admin_message,
                    parse_mode="HTML",
                    reply_markup=build_pending_request_keyboard(user.id)
                )
            except Exception:
                pass

            context.user_data["step"] = "pending_review"
            return

        await send_welcome_flow(update)
        return

    # ===== Admin waiting inputs =====
    if is_admin(user.id) and step == "otc_stats_waiting_count":
        clean_text = text.strip()
        if clean_text.isdigit():
            count = int(clean_text)
            if count <= 0:
                await update.message.reply_text("اكتب رقم أكبر من صفر.", reply_markup=admin_otc_stats_keyboard)
                return

            context.user_data["step"] = None
            trades = get_otc_live_trades(limit=count)
            await update.message.reply_text(
                build_otc_live_stats_from_trades(
                    trades=trades,
                    title=f"📊 إحصائيات آخر {count} صفقة",
                    include_advice=True,
                ),
                reply_markup=admin_otc_stats_keyboard
            )
            return

        context.user_data["step"] = None
        await update.message.reply_text("تم إلغاء الطلب لأنك لم ترسل رقمًا.", reply_markup=admin_otc_stats_keyboard)
        return

    if is_admin(user.id) and step == "admin_broadcast_waiting_message":
        message = text.strip()
        if not message:
            await update.message.reply_text("الرسالة فارغة، تم الإلغاء.", reply_markup=admin_main_keyboard)
            context.user_data["step"] = None
            return

        context.user_data["step"] = None
        all_users = get_all_users() or {}
        sent_count = 0
        failed_count = 0
        broadcast_text = "📢 رسالة من الأدمن\n\n" + message

        for uid in list(all_users.keys()):
            try:
                await context.bot.send_message(chat_id=int(uid), text=broadcast_text)
                sent_count += 1
            except Exception:
                failed_count += 1

        await update.message.reply_text(
            f"📢 تم إرسال الرسالة الجماعية.\n\n✅ وصل: {sent_count}\n❌ فشل: {failed_count}",
            reply_markup=admin_main_keyboard
        )
        return

    if is_admin(user.id) and step == "otc_list_waiting_text":
        raw_list = text
        parsed = parse_otc_list_trades(raw_list)
        context.user_data["step"] = None

        if not parsed:
            await update.message.reply_text(
                "❌ لم أستطع قراءة الليستة.\n"
                "أرسلها بنفس صيغة: USD/BRL (OTC) 12:04 PUT (Down)",
                reply_markup=admin_otc_stats_keyboard
            )
            return

        context.user_data["last_otc_list_raw_text"] = raw_list
        context.user_data["last_otc_list_result_text"] = None
        try:
            otc_list_results_ref(user.id).delete()
        except Exception:
            pass

        list_id = str(int(time_module.time()))
        parsed_trades = parse_otc_list_trades(raw_list)
        save_otc_list_job(user.id, list_id, raw_list, parsed_trades)

        # نحسب كل صفقة لوحدها بعد انتهاء شمعتها والمضاعفة مباشرة، حتى لا تضيع بيانات الليستات الطويلة.
        latest_delay = 1.0
        for idx, trade in enumerate(parsed_trades):
            entry_dt = otc_list_entry_datetime(int(trade["hour"]), int(trade["minute"]))
            ready_dt = entry_dt + timedelta(seconds=130)
            trade_delay = max(1.0, (ready_dt - now_utc()).total_seconds())
            latest_delay = max(latest_delay, trade_delay)

            context.job_queue.run_once(
                evaluate_single_otc_list_trade_job,
                when=trade_delay,
                data={"admin_id": user.id, "list_id": list_id, "index": idx, "trade": trade},
                name=f"otc_list_trade_{user.id}_{list_id}_{idx}",
            )

        context.job_queue.run_once(
            finalize_otc_list_results_job,
            when=latest_delay + 2,
            data={"admin_id": user.id, "list_id": list_id},
            name=f"otc_list_finalize_{user.id}_{list_id}",
        )

        context.user_data["last_otc_list_id"] = list_id

        ready_minutes = round((latest_delay + 2) / 60, 1)
        await update.message.reply_text(
            f"✅ تم استلام الليستة وعدد صفقاتها: {len(parsed_trades)}\n"
            f"⏳ سأحسب كل صفقة فور انتهائها، وأخبرك عندما تصبح النتيجة النهائية جاهزة تقريبًا بعد {ready_minutes} دقيقة.",
            reply_markup=admin_otc_stats_keyboard
        )
        return

    if is_admin(user.id) and step == "otc_pair_diagnostics_waiting":
        context.user_data["step"] = None
        pair = normalize_otc_pair_input(text)
        await update.message.reply_text(
            get_otc_feed_diagnostics_for_pair(pair),
            reply_markup=admin_otc_stats_keyboard
        )
        return

    if is_admin(user.id) and step == "otc_candle_diagnostics_waiting":
        context.user_data["step"] = None
        raw = text.strip()
        try:
            match = OTC_PAIR_TIME_RE.search(raw)
            if not match:
                raise ValueError("pair_time_not_found")

            pair = f"{match.group('pair').upper()} (OTC)"
            hour = int(match.group("hour"))
            minute = int(match.group("minute"))

            entry_dt = otc_list_entry_datetime(hour, minute)
            await update.message.reply_text(
                "🕯️ فحص شمعة OTC\n\n" + get_otc_candle_debug_for_pair_time(pair, entry_dt.timestamp()),
                reply_markup=admin_otc_stats_keyboard
            )
        except Exception:
            await update.message.reply_text(
                "اكتب الزوج والوقت بهذا الشكل:\n"
                "USD/BDT (OTC) 15:53\n\n"
                "أو:\n"
                "USD/BDT 15:53",
                reply_markup=admin_otc_stats_keyboard
            )
        return


    # ===== Common buttons =====
    if text == "🔙 رجوع":
        if is_admin(user.id) and step in {"otc_stats_waiting_count", "admin_broadcast_waiting_message", "otc_list_waiting_text", "otc_pair_diagnostics_waiting", "otc_candle_diagnostics_waiting"}:
            context.user_data["step"] = None
            await update.message.reply_text("تم الرجوع.", reply_markup=admin_main_keyboard)
            return

        if step == "choose_count":
            context.user_data["step"] = "choose_pair"
            await update.message.reply_text("💱 اختر الزوج 👇", reply_markup=otc_pairs_keyboard)
            return

        if step == "choose_interval" and context.user_data.get("mode") == "otc":
            context.user_data["step"] = "choose_count"
            await update.message.reply_text("📈 اختر عدد الصفقات 👇", reply_markup=count_keyboard)
            return

        if step == "choose_interval_real" and context.user_data.get("mode") == "real":
            context.user_data["step"] = "choose_real_pair"
            await update.message.reply_text("🌍 اختر الزوج العالمي 👇", reply_markup=real_pairs_keyboard)
            return

        if step in {"choose_market_mode", "choose_real_pair", "choose_pair", "choose_live_otc_pair"}:
            reset_signal_state(context)
            await update.message.reply_text(
                "↩️ رجعت للقائمة الرئيسية",
                reply_markup=build_main_menu_for_user(user.id)
            )
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
        context.user_data["step"] = "choose_market_mode"
        await update.message.reply_text("📊 اختر نوع السوق 👇", reply_markup=market_mode_keyboard)
        return

    if text == "👤 حسابي":
        username = f"@{user.username}" if user.username else "ما عنده username"
        await update.message.reply_text(
            "👤 معلومات حسابك\n\n"
            f"• الاسم: {user.full_name}\n"
            f"• اليوزر: {username}\n"
            f"• الآيدي: <code>{user.id}</code>\n"
            f"• الحالة: {get_user_status(user.id)}",
            parse_mode="HTML",
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

        if text == "📡 قنوات البوت":
            context.user_data["step"] = "admin_channel_controls"
            await update.message.reply_text(
                format_channel_publish_status() + "\n\nاختر القناة التي تريد تشغيل أو إيقاف النشر فيها 👇",
                reply_markup=admin_channels_keyboard
            )
            return

        if step == "admin_channel_controls":
            if text == "🌍 تشغيل نشر العالمي":
                set_channel_publish_enabled("global", True)
                await update.message.reply_text("✅ تم تشغيل النشر في قناة السوق العالمي", reply_markup=admin_channels_keyboard)
                return

            if text == "🌍 إيقاف نشر العالمي":
                set_channel_publish_enabled("global", False)
                await update.message.reply_text("⛔ تم إيقاف النشر في قناة السوق العالمي", reply_markup=admin_channels_keyboard)
                return

            if text == "⚡ تشغيل نشر OTC":
                set_channel_publish_enabled("otc", True)
                await update.message.reply_text("✅ تم تشغيل النشر في قناة OTC", reply_markup=admin_channels_keyboard)
                return

            if text == "⚡ إيقاف نشر OTC":
                set_channel_publish_enabled("otc", False)
                await update.message.reply_text("⛔ تم إيقاف النشر في قناة OTC", reply_markup=admin_channels_keyboard)
                return

            if text == "🔥 تشغيل OTC مباشر":
                set_channel_publish_enabled("otc_live", True)
                await update.message.reply_text("🔥 تم تشغيل نشر قناة OTC المباشر ✅", reply_markup=admin_channels_keyboard)
                return

            if text == "🔥 إيقاف OTC مباشر":
                set_channel_publish_enabled("otc_live", False)
                await update.message.reply_text("🔥 تم إيقاف نشر قناة OTC المباشر ⛔", reply_markup=admin_channels_keyboard)
                return

            if text == "📊 إحصائيات قناة OTC":
                await update.message.reply_text("📊 قسم إحصائيات قناة OTC 👇", reply_markup=admin_otc_stats_keyboard)
                return

            if text == "📊 حالة النشر":
                await update.message.reply_text(format_channel_publish_status(), reply_markup=admin_channels_keyboard)
                return

        if is_admin(user.id):
            if text == "📈 إحصائيات OTC مباشر":
                await update.message.reply_text(build_otc_live_stats_message(), reply_markup=admin_otc_stats_keyboard)
                return

            if text == "🔢 إحصائيات آخر عدد صفقات":
                context.user_data["step"] = "otc_stats_waiting_count"
                await update.message.reply_text(
                    "🔢 اكتب عدد الصفقات التي تريد فحصها، مثال: 50 أو 100",
                    reply_markup=admin_otc_stats_keyboard
                )
                return

            if text == "🤖 تحليل ونصائح البوت":
                trades = get_otc_live_trades()
                await update.message.reply_text(
                    build_otc_live_stats_from_trades(
                        trades=trades,
                        title="🤖 تحليل ونصائح OTC",
                        include_advice=True,
                    ),
                    reply_markup=admin_otc_stats_keyboard
                )
                return

            if text == "🧠 حالة تعلم OTC Live":
                await update.message.reply_text(
                    format_otc_live_learning_status(),
                    reply_markup=admin_otc_stats_keyboard
                )
                return

            if text == "🔎 فحص بيانات زوج OTC":
                context.user_data["step"] = "otc_pair_diagnostics_waiting"
                await update.message.reply_text(
                    "🔎 أرسل اسم الزوج لفحص بياناته، مثال:\nUSD/BRL (OTC)",
                    reply_markup=admin_otc_stats_keyboard
                )
                return

            if text == "🕯️ فحص شمعة OTC":
                context.user_data["step"] = "otc_candle_diagnostics_waiting"
                await update.message.reply_text(
                    "🕯️ أرسل الزوج والوقت لفحص الشمعة، مثال:\nUSD/BDT (OTC) 15:53",
                    reply_markup=admin_otc_stats_keyboard
                )
                return


            if text == "🧹 تصفير إحصائيات OTC":
                reset_otc_live_stats_marker()
                await update.message.reply_text(
                    "🧹 تم تصفير إحصائيات OTC من هذه اللحظة.\n"
                    "أي إحصائية لاحقة ستُحسب من بعد هذا التصفير.",
                    reply_markup=admin_otc_stats_keyboard
                )
                return

            if text == "🧾 فحص ليستة OTC":
                context.user_data["step"] = "otc_list_waiting_text"
                await update.message.reply_text(
                    "🧾 أرسل الآن ليستة OTC كاملة كما نشرتها بالقناة.\n"
                    "سأراقبها وأخبرك عندما تنتهي آخر صفقة وتصبح النتائج جاهزة.",
                    reply_markup=admin_otc_stats_keyboard
                )
                return

            if text == "📋 عرض نتائج الليستة":
                saved_result = get_ready_otc_list_result(user.id)
                result_text = saved_result.get("result_text")

                if not result_text:
                    raw_list = context.user_data.get("last_otc_list_raw_text")
                    if not raw_list:
                        await update.message.reply_text(
                            "لا توجد ليستة محفوظة بعد. أرسل ليستة أولًا من زر: 🧾 فحص ليستة OTC",
                            reply_markup=admin_otc_stats_keyboard
                        )
                        return

                    await update.message.reply_text(
                        "⏳ النتيجة لم تُحفظ بعد. انتظر رسالة: نتائج الليستة جاهزة.",
                        reply_markup=admin_otc_stats_keyboard
                    )
                    return

                context.user_data["last_otc_list_result_text"] = result_text

                await update.message.reply_text(
                    result_text,
                    reply_markup=admin_otc_stats_keyboard
                )
                return


        if is_admin(user.id) and text == "📋 عرض نتائج الليستة":
            raw_list = context.user_data.get("last_otc_list_raw_text")
            if not raw_list:
                await update.message.reply_text(
                    "لا توجد ليستة محفوظة بعد. أرسل ليستة أولًا من زر: 🧾 فحص ليستة OTC",
                    reply_markup=admin_otc_stats_keyboard
                )
                return

            result_text, meta = build_otc_list_results_message(raw_list)
            context.user_data["last_otc_list_result_text"] = result_text
            await update.message.reply_text(
                result_text,
                reply_markup=admin_otc_stats_keyboard
            )
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
                    f"🆔 Telegram ID: <code>{item.get('telegram_id')}</code>\n"
                    f"💱 Quotex ID: <code>{item.get('quotex_id')}</code>\n\n"
                    "──────────────"
                )

            await update.message.reply_text(
                "\n\n".join(lines),
                parse_mode="HTML",
                reply_markup=admin_main_keyboard
            )
            return

        if text == "📋 كافة المستخدمين":
            approved_users = get_all_approved_users()
            all_users = get_all_users()

            approved_active_list = []
            for user_id_str in approved_users.keys():
                try:
                    user_id = int(user_id_str)
                except ValueError:
                    continue

                if not is_approved(user_id):
                    continue

                user_data = all_users.get(user_id_str)
                if not user_data:
                    continue

                approved_active_list.append((user_id_str, user_data))

            if not approved_active_list:
                await update.message.reply_text("📭 لا يوجد مستخدمون مفعّلون حاليًا.", reply_markup=admin_main_keyboard)
                return

            lines = []
            for user_id_str, data in approved_active_list:
                name = data.get("name", "غير معروف")
                username = f"@{data.get('username')}" if data.get("username") else "بدون username"
                lines.append(
                    f"👤 {name} | {username}\n"
                    f"🆔 ID: <code>{user_id_str}</code>\n"
                    f"📌 الحالة: approved\n"
                    "──────────────"
                )

            msg = "\n".join(lines[:50])
            await update.message.reply_text(
                msg,
                parse_mode="HTML",
                reply_markup=admin_main_keyboard
            )
            return

        if text == "🟢 المستخدمون النشطون":
            active_users = get_recent_active_approved_users()
            if not active_users:
                await update.message.reply_text(
                    "🕒 لا يوجد مستخدمون نشطون من المفعّلين خلال آخر 15 دقيقة.",
                    reply_markup=admin_main_keyboard
                )
                return

            lines = []
            for user_id_str, data in active_users:
                name = data.get("name", "غير معروف")
                username = f"@{data.get('username')}" if data.get("username") else "بدون username"
                lines.append(
                    f"🟢 {name} | {username}\n"
                    f"🆔 ID: <code>{user_id_str}</code>\n"
                    f"⏰ آخر نشاط: {format_dt_ar(data.get('last_seen', ''))}\n"
                    "──────────────"
                )

            await update.message.reply_text(
                "\n".join(lines[:50]),
                parse_mode="HTML",
                reply_markup=admin_main_keyboard
            )
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

    # ===== Market mode flow =====
    if step == "choose_market_mode":
        if text in {"⚡ OTC", "OTC"}:
            context.user_data["mode"] = "otc"
            context.user_data["step"] = "choose_otc_mode"
            await update.message.reply_text("⚡ اختر نوع إشارات OTC 👇", reply_markup=otc_mode_keyboard)
            return

        if text == "🌍 سوق عالمي":
            context.user_data["mode"] = "real"
            context.user_data["step"] = "choose_real_pair"
            await update.message.reply_text("🌍 اختر الزوج العالمي 👇", reply_markup=real_pairs_keyboard)
            return

        await update.message.reply_text("📌 اختر نوع السوق من الأزرار 👇", reply_markup=market_mode_keyboard)
        return

    # ===== OTC sub-mode flow =====
    if step == "choose_otc_mode" and context.user_data.get("mode") == "otc":
        if text == "🕒 زمني":
            context.user_data["otc_submode"] = "timed"
            context.user_data["step"] = "choose_pair"
            await update.message.reply_text("💱 اختر زوج OTC للّيستة الزمنية 👇", reply_markup=otc_pairs_keyboard)
            return

        if text == "⚡ صفقة مباشرة":
            context.user_data["otc_submode"] = "live_now"
            context.user_data["step"] = "choose_live_otc_action"
            await update.message.reply_text(
                "⚡ صفقة مباشرة OTC\n\nاضغط الزر ليبحث البوت الآن عن أفضل فرصة بين كل أزواج OTC على فريم الدقيقة.",
                reply_markup=otc_live_search_keyboard
            )
            return

        await update.message.reply_text("⚡ اختر نوع إشارات OTC من الأزرار 👇", reply_markup=otc_mode_keyboard)
        return

    # ===== OTC timed list flow - النظام القديم كما هو =====
    if step == "choose_pair" and context.user_data.get("mode") == "otc":
        if text not in OTC_PAIRS:
            await update.message.reply_text("💱 اختر زوجًا من الأزرار 👇", reply_markup=otc_pairs_keyboard)
            return

        context.user_data["pair"] = text
        context.user_data["step"] = "choose_count"
        await update.message.reply_text("📈 اختر عدد الصفقات 👇", reply_markup=count_keyboard)
        return

    if step == "choose_count" and context.user_data.get("mode") == "otc":
        if text not in [str(x) for x in TRADE_COUNTS]:
            await update.message.reply_text("📈 اختر عدد الصفقات من الأزرار 👇", reply_markup=count_keyboard)
            return

        context.user_data["count"] = int(text)

        # النظام الزمني القديم: فاصل 3 دقائق واتجاه ثابت زمني، بدون استخدام بث live.
        interval_minutes = 3
        pair = context.user_data["pair"]
        count = context.user_data["count"]
        start_dt = next_full_minute(now_utc())

        signals = generate_signals(pair, count, interval_minutes, start_dt)
        message_text = build_signals_message(pair, count, interval_minutes, signals)

        await update.message.reply_text(
            message_text,
            reply_markup=build_main_menu_for_user(user.id),
            parse_mode="Markdown"
        )

        reset_signal_state(context)
        return

    # ===== OTC direct trade flow - خيار جديد منفصل =====
    if step == "choose_live_otc_action" and context.user_data.get("mode") == "otc":
        if text != "🔎 ابحث عن صفقة الآن":
            await update.message.reply_text(
                "اضغط الزر ليبحث البوت عن أفضل فرصة مباشرة الآن 👇",
                reply_markup=otc_live_search_keyboard
            )
            return

        await update.message.reply_text("🔎 جاري فحص أزواج OTC الحية على فريم الدقيقة...")
        result = analyze_best_live_otc_now()

        await update.message.reply_text(
            result["message"],
            reply_markup=build_main_menu_for_user(user.id),
            parse_mode="Markdown"
        )

        reset_signal_state(context)
        return

    # ===== Real market flow =====
    if step == "choose_real_pair":
        if text not in REAL_PAIRS:
            await update.message.reply_text("🌍 اختر زوجًا من الأزرار 👇", reply_markup=real_pairs_keyboard)
            return

        context.user_data["pair"] = text
        context.user_data["step"] = "choose_interval_real"
        await update.message.reply_text("⏳ اختر الفريم أو دع البوت يحدد أفضل فرصة 👇", reply_markup=real_interval_keyboard)
        return

    if step == "choose_interval_real":
        interval_map = {
            "1 دقيقة": 1,
            "5 دقائق": 5,
            "10 دقائق": 10,
        }

        if text not in interval_map and text != "🔥 أفضل فرصة":
            await update.message.reply_text("⏳ اختر الفريم من الأزرار 👇", reply_markup=real_interval_keyboard)
            return

        pair = context.user_data["pair"]

        if text == "🔥 أفضل فرصة":
            result = analyze_real_market_best(pair)
        else:
            interval_minutes = interval_map[text]
            result = analyze_real_market(pair, interval_minutes)

        await update.message.reply_text(
            result["message"],
            reply_markup=build_main_menu_for_user(user.id)
        )

        reset_signal_state(context)
        return

    await update.message.reply_text(
        "📌 اختر خيارًا من القائمة.",
        reply_markup=build_main_menu_for_user(user.id)
    )


# ===== App Runner =====

async def telegram_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error(
        "Telegram handler error | update=%s | error=%s\n%s",
        update,
        context.error,
        "".join(traceback.format_exception(None, context.error, context.error.__traceback__)) if context.error else "",
    )

def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN غير موجود داخل ملف .env")

    app = Application.builder().token(BOT_TOKEN).build()

    # تشغيل بث Quotex OTC الحقيقي بالخلفية
    start_quotex_otc_feed()

    # Auto publish global market
    app.job_queue.run_repeating(
        auto_publish_real_market,
        interval=120,
        first=15
    )

    # Auto publish OTC live direct trades to the new private channel
    app.job_queue.run_repeating(
        auto_publish_otc_live_channel,
        interval=OTC_LIVE_SCAN_INTERVAL_SECONDS,
        first=25
    )

    job_queue = app.job_queue

    # نشر تلقائي مرة واحدة يوميًا:
    # يتم اختيار وقت عشوائي للنشر بين 12:00 و 20:00 بتوقيت سوريا.
    job_queue.run_daily(
        schedule_random_daily_otc_list,
        time=time(hour=0, minute=5, tzinfo=UTC_PLUS_3)
    )

    # جدولة أول ليستة عند تشغيل البوت، حتى لو تم إعادة التشغيل بعد منتصف الليل.
    job_queue.run_once(
        schedule_random_daily_otc_list,
        when=5
    )


    # نشر إحصائيات OTC Live اليومية بنهاية اليوم بتوقيت UTC+3
    job_queue.run_daily(
        publish_daily_otc_live_stats,
        time=time(hour=23, minute=59, tzinfo=UTC_PLUS_3)
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_admin_buttons))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(telegram_error_handler)

    logger.info("Bot is running...")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    app.run_polling(drop_pending_updates=True, close_loop=False)


if __name__ == "__main__":
    main()
