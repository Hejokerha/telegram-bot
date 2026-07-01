from urllib.parse import urlparse
import os
import html
import json
import csv
import io
import hashlib
import hmac
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
# قناة النشر التلقائي OTC Live تم إلغاؤها نهائيًا بناءً على طلب الأدمن.
# يبقى بث Quotex والوظائف اليدوية كما هي، لكن لا يوجد نشر تلقائي على قناة OTC Live.
OTC_AUTO_PUBLISH_CHANNEL_REMOVED = True
OTC_LIVE_CHANNEL_ENABLED = False
OTC_LIVE_MIN_QUALITY = int(os.getenv("OTC_LIVE_MIN_QUALITY", "65"))
OTC_LIVE_MIN_PAYOUT = int(os.getenv("OTC_LIVE_MIN_PAYOUT", "80"))
OTC_LIVE_DYNAMIC_PAIRS_ENABLED = os.getenv("OTC_LIVE_DYNAMIC_PAIRS_ENABLED", "true").lower() == "true"
OTC_LIVE_DYNAMIC_MIN_PAYOUT = int(os.getenv("OTC_LIVE_DYNAMIC_MIN_PAYOUT", str(OTC_LIVE_MIN_PAYOUT)))
OTC_LIVE_MAX_DYNAMIC_PAIRS = int(os.getenv("OTC_LIVE_MAX_DYNAMIC_PAIRS", "80"))
OTC_LIVE_TOP_CANDIDATES_POOL = int(os.getenv("OTC_LIVE_TOP_CANDIDATES_POOL", "5"))
OTC_LIVE_ALLOWED_FALLBACK_ENABLED = os.getenv("OTC_LIVE_ALLOWED_FALLBACK_ENABLED", "true").lower() == "true"
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
OTC_LIVE_SMART_MARTINGALE_ENABLED = os.getenv("OTC_LIVE_SMART_MARTINGALE_ENABLED", "false").lower() == "true"
OTC_LIVE_MARTINGALE_DECISION_SECONDS_BEFORE_CLOSE = int(os.getenv("OTC_LIVE_MARTINGALE_DECISION_SECONDS_BEFORE_CLOSE", "8"))
OTC_LIVE_MARTINGALE_ADVICE_CHECK_SECONDS = [12, 8, 5, 3]
OTC_LIVE_ADAPTIVE_FILTER_ENABLED = os.getenv("OTC_LIVE_ADAPTIVE_FILTER_ENABLED", "true").lower() == "true"
OTC_LIVE_PAIR_RECENT_LIMIT = int(os.getenv("OTC_LIVE_PAIR_RECENT_LIMIT", "20"))
OTC_LIVE_PAIR_MAX_RECENT_LOSSES = int(os.getenv("OTC_LIVE_PAIR_MAX_RECENT_LOSSES", "5"))
OTC_LIVE_DIRECTION_RECENT_LIMIT = int(os.getenv("OTC_LIVE_DIRECTION_RECENT_LIMIT", "15"))
OTC_LIVE_DIRECTION_MAX_RECENT_LOSSES = int(os.getenv("OTC_LIVE_DIRECTION_MAX_RECENT_LOSSES", "4"))
OTC_LIVE_PAIR_MAX_RECENT_NEGATIVE_UNITS = float(os.getenv("OTC_LIVE_PAIR_MAX_RECENT_NEGATIVE_UNITS", "-8.0"))
OTC_LIVE_RESULT_DELAY_SECONDS = int(os.getenv("OTC_LIVE_RESULT_DELAY_SECONDS", "8"))
OTC_LIVE_HEALTH_CHECK_ENABLED = os.getenv("OTC_LIVE_HEALTH_CHECK_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
OTC_LIVE_NO_TICKS_ALERT_SECONDS = int(os.getenv("OTC_LIVE_NO_TICKS_ALERT_SECONDS", "180"))
OTC_LIVE_HEALTH_CHECK_INTERVAL_SECONDS = int(os.getenv("OTC_LIVE_HEALTH_CHECK_INTERVAL_SECONDS", "60"))
OTC_LIVE_HEALTH_ALERT_COOLDOWN_SECONDS = int(os.getenv("OTC_LIVE_HEALTH_ALERT_COOLDOWN_SECONDS", "600"))


ADMIN_USERNAME = "@coach_WAEL_trading"
YOUTUBE_TUTORIAL_URL = "https://www.youtube.com/watch?v=YPqgJcgvyFw"
VIDEO_TRIAL_DELAY_SECONDS = int(os.getenv("VIDEO_TRIAL_DELAY_SECONDS", "90"))
VIDEO_TRIAL_DURATION_SECONDS = int(os.getenv("VIDEO_TRIAL_DURATION_SECONDS", "3600"))

ADMIN_TELEGRAM_ID = 1582593617
def parse_id_set_from_env(name: str) -> set[int]:
    ids = set()
    raw = os.getenv(name, "") or ""
    raw = raw.replace(";", ",")
    raw = raw.replace("\n", ",")
    for part in raw.split(","):
        part = part.strip()
        if part and part.lstrip("-").isdigit():
            ids.add(int(part))
    return ids


OTC_LIST_MANAGER_IDS = parse_id_set_from_env("OTC_LIST_MANAGER_IDS")

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

# ===== Auto publishing hard switch =====
# بناءً على طلب الأدمن: لا يوجد أي نشر تلقائي على أي قناة.
# التوليد اليدوي داخل البوت يبقى كما هو.
AUTO_PUBLISHING_DISABLED = True


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


OTC_CURRENCIES_ALLOWED_PAIRS = {
    "CAD/CHF (OTC)",
    "NZD/JPY (OTC)",
    "USD/IDR (OTC)",
    "USD/DZD (OTC)",
    "EUR/NZD (OTC)",
    "USD/MXN (OTC)",
    "AUD/NZD (OTC)",
    "USD/PKR (OTC)",
    "USD/BRL (OTC)",
    "USD/EGP (OTC)",
    "USD/COP (OTC)",
    "NZD/USD (OTC)",
    "NZD/CAD (OTC)",
    "USD/ARS (OTC)",
    "USD/BDT (OTC)",
    "USD/INR (OTC)",
    "USD/NGN (OTC)",
    "USD/PHP (OTC)",
    "USD/ZAR (OTC)",
    "GBP/NZD (OTC)",
    "EUR/CHF (OTC)",
    "AUD/USD (OTC)",
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
logging.getLogger("apscheduler").setLevel(logging.WARNING)
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
def _load_firebase_credential():
    """Load Firebase credentials safely on Render/local.

    Priority:
    1) FIREBASE_CREDENTIALS_JSON when it is valid JSON.
    2) FIREBASE_CREDENTIALS_FILE or GOOGLE_APPLICATION_CREDENTIALS when set.
    3) serviceAccountKey.json in the project folder.

    If FIREBASE_CREDENTIALS_JSON is present but malformed, we do NOT crash
    immediately; we fall back to the file path if available and print a clear log.
    """
    firebase_json_raw = os.getenv("FIREBASE_CREDENTIALS_JSON")

    if firebase_json_raw:
        firebase_json_raw = str(firebase_json_raw).strip()
        try:
            cred_dict = json.loads(firebase_json_raw)

            # Some dashboards store the whole JSON as a quoted JSON string.
            if isinstance(cred_dict, str):
                cred_dict = json.loads(cred_dict)

            if not isinstance(cred_dict, dict):
                raise ValueError("FIREBASE_CREDENTIALS_JSON did not decode to a JSON object")

            # Normalize private key newlines for Firebase Admin SDK.
            private_key = cred_dict.get("private_key")
            if isinstance(private_key, str):
                cred_dict["private_key"] = private_key.replace("\\n", "\n")

            return credentials.Certificate(cred_dict)
        except Exception as e:
            logger.warning(
                "FIREBASE_CREDENTIALS_JSON is present but invalid; trying credential file fallback. Error: %s",
                e,
            )

    credential_file = (
        os.getenv("FIREBASE_CREDENTIALS_FILE")
        or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        or "serviceAccountKey.json"
    )

    if credential_file and os.path.exists(credential_file):
        logger.warning("Using Firebase credential file: %s", credential_file)
        return credentials.Certificate(credential_file)

    # Render secret files are often mounted under /etc/secrets.
    render_secret_file = "/etc/secrets/serviceAccountKey.json"
    if os.path.exists(render_secret_file):
        logger.warning("Using Firebase Render secret file: %s", render_secret_file)
        return credentials.Certificate(render_secret_file)

    raise RuntimeError(
        "Firebase credentials not found. Fix FIREBASE_CREDENTIALS_JSON, "
        "or set FIREBASE_CREDENTIALS_FILE / GOOGLE_APPLICATION_CREDENTIALS, "
        "or provide serviceAccountKey.json."
    )


cred = _load_firebase_credential()

if not firebase_admin._apps:
    firebase_admin.initialize_app(cred, {
        "databaseURL": DATABASE_URL
    })

# ===== Keyboards =====
main_keyboard = ReplyKeyboardMarkup(
    [
        ["📊 توليد إشارات"],
        ["🧠 غرفة جلسة تداول"],
        ["👤 حالة حسابي", "🎥 مشاهدة فيديو شرح البوت"],
        ["📞 تواصل مع المسؤول", "🌐 تغيير اللغة"],
    ],
    resize_keyboard=True
)

admin_main_keyboard = ReplyKeyboardMarkup(
    [
        ["📥 الطلبات المعلقة", "📋 كافة المستخدمين"],
        ["🟢 المستخدمون النشطون", "🔍 تفاصيل مستخدم"],
        ["📊 إحصائيات البوت", "📤 تصدير المستخدمين"],
        ["🔐 Copy Trading", "📡 حالة Copy"],
        ["🧠 غرفة جلسة تداول", "🧠 OTC Edge Engine"],
        ["📡 قناة 3 شموع", "🧾 فحص ليستة OTC"],
        ["📋 عرض نتائج الليستة"],
        ["🟢 تشغيل البوت", "🔴 إيقاف البوت"],
        ["📢 رسالة جماعية"],
        ["⬅️ رجوع"],
    ],
    resize_keyboard=True
)

# بقي هذا الكيبورد فقط كمرجع داخلي لقسم فحص الليستات، وليس لقنوات نشر تلقائي.
admin_channels_keyboard = ReplyKeyboardMarkup(
    [
        ["🧾 فحص ليستة OTC", "📋 عرض نتائج الليستة"],
        ["⬅️ رجوع"],
    ],
    resize_keyboard=True
)

admin_otc_stats_keyboard = ReplyKeyboardMarkup(
    [
        ["🧾 فحص ليستة OTC", "📋 عرض نتائج الليستة"],
        ["⬅️ رجوع"],
    ],
    resize_keyboard=True
)

admin_otc_edge_keyboard = ReplyKeyboardMarkup(
    [
        ["🔎 فحص السوق الآن", "🚀 مراقبة كل السوق"],
        ["🎯 مراقبة زوج محدد", "🛑 إيقاف مراقبة Edge"],
        ["📋 حالة مراقبة Edge", "📊 تقرير الأنماط"],
        ["🧪 فحص زوج محدد"],
        ["⬅️ رجوع"],
    ],
    resize_keyboard=True
)

three_candle_admin_keyboard = ReplyKeyboardMarkup(
    [
        ["🟢 تشغيل نشر القناة", "🔴 إيقاف نشر القناة"],
        ["🎯 حد صفقات اليوم", "♾ نشر مفتوح"],
        ["📊 ملخص القناة", "📋 حالة القناة"],
        ["⬅️ رجوع"],
    ],
    resize_keyboard=True
)

copy_admin_keyboard = ReplyKeyboardMarkup(
    [
        ["🟢 تشغيل Copy", "🔴 إيقاف Copy"],
        ["🔑 كود أسبوع", "🔑 كود شهر"],
        ["🔑 كود دائم", "📋 أكواد Copy"],
        ["⛔ إيقاف كود", "♻️ تصفير جهاز كود"],
        ["♻️ تصفير كل الأجهزة", "🗑 حذف كود"],
        ["🧹 تنظيف الأكواد", "📌 رسالة تحديث"],
        ["📡 حالة Copy", "⬅️ رجوع"],
    ],
    resize_keyboard=True
)

otc_list_manager_keyboard = ReplyKeyboardMarkup(
    [
        ["📊 توليد إشارات"],
        ["👤 حالة حسابي", "🎥 مشاهدة فيديو شرح البوت"],
        ["📞 تواصل مع المسؤول", "🌐 تغيير اللغة"],
        ["🧾 فحص ليستة OTC", "📋 عرض نتائج الليستة"],
    ],
    resize_keyboard=True
)

admin_otc_list_ready_keyboard = ReplyKeyboardMarkup(
    [
        ["📋 عرض نتائج الليستة", "🧾 فحص ليستة OTC"],
        ["⬅️ رجوع"],
    ],
    resize_keyboard=True
)


# ===== Trading Room Keyboards =====
# رجعنا أزرار غرفة الجلسة إلى ReplyKeyboard مثل قبل حتى لا تظهر كأزرار تحت كل رسالة.
# مع دعم كامل للغة الإنجليزية داخل الغرفة.

def _tr_lang(user_id: int) -> str:
    try:
        return get_user_language(user_id)
    except Exception:
        return "ar"


def _tr_room_text(user_id: int, ar: str, en: str) -> str:
    return en if _tr_lang(user_id) == "en" else ar


def get_trading_room_menu_keyboard(user_id: int):
    lang = _tr_lang(user_id)
    if lang == "en":
        rows = [["🚀 Start Trading Session"]]
        if is_admin(user_id):
            rows.append(["📊 Session Status", "🩺 OTC Live Check"])
        rows.append(["🛑 Stop Session"])
        rows.append(["🔙 Back"])
        return ReplyKeyboardMarkup(rows, resize_keyboard=True)
    rows = [["🚀 بدء جلسة تداول"]]
    if is_admin(user_id):
        rows.append(["📊 حالة الجلسة", "🩺 فحص بيانات OTC Live"])
    rows.append(["🛑 إيقاف الجلسة"])
    rows.append(["⬅️ رجوع"])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def get_trading_room_active_keyboard(user_id: int):
    if _tr_lang(user_id) == "en":
        return ReplyKeyboardMarkup([["🛑 Stop Session"]], resize_keyboard=True)
    return ReplyKeyboardMarkup([["🛑 إيقاف الجلسة"]], resize_keyboard=True)


def get_trading_room_smart_exit_keyboard(user_id: int):
    if _tr_lang(user_id) == "en":
        return ReplyKeyboardMarkup([["🛑 Stop and secure result"], ["▶️ Continue session"]], resize_keyboard=True)
    return ReplyKeyboardMarkup([["🛑 إيقاف وحفظ النتيجة"], ["▶️ متابعة الجلسة"]], resize_keyboard=True)


def get_trading_room_ready_keyboard(user_id: int):
    if _tr_lang(user_id) == "en":
        return ReplyKeyboardMarkup([["✅ Yes, I am ready"], ["❌ Cancel Session"]], resize_keyboard=True)
    return ReplyKeyboardMarkup([["✅ نعم، أنا مستعد"], ["❌ إلغاء الجلسة"]], resize_keyboard=True)


def get_trading_room_after_win_keyboard(user_id: int):
    if _tr_lang(user_id) == "en":
        return ReplyKeyboardMarkup([["🚀 New Session"], ["🛑 End Today"], ["🔙 Back"]], resize_keyboard=True)
    return ReplyKeyboardMarkup([["🚀 جلسة جديدة"], ["🛑 إنهاء اليوم"], ["⬅️ رجوع"]], resize_keyboard=True)


def get_trading_room_after_loss_keyboard(user_id: int):
    if _tr_lang(user_id) == "en":
        return ReplyKeyboardMarkup([["🚀 Start New Session"], ["⏰ Remind me in 30 minutes"], ["🧊 Lock room for 30 minutes"], ["🔙 Back"]], resize_keyboard=True)
    return ReplyKeyboardMarkup([["🚀 بدء جلسة جديدة"], ["⏰ ذكرني بعد نصف ساعة"], ["🧊 تعطيل غرفة التداول نصف ساعة"], ["⬅️ رجوع"]], resize_keyboard=True)


def get_trading_room_retreat_keyboard(user_id: int):
    if _tr_lang(user_id) == "en":
        return ReplyKeyboardMarkup([["🧊 Lock room for 30 minutes"], ["🔙 Back"]], resize_keyboard=True)
    return ReplyKeyboardMarkup([["🧊 تعطيل غرفة التداول نصف ساعة"], ["⬅️ رجوع"]], resize_keyboard=True)


def get_trading_room_loss_confirm_keyboard(user_id: int, stage: int):
    lang = _tr_lang(user_id)
    if lang == "en":
        labels = {
            1: [["Yes, I am sure"], ["No, let me step back"]],
            2: [["I do not care, continue"], ["Thanks for reminding me"]],
            3: [["I have a clear plan"], ["Probably anger, stop me"]],
            4: [["I accept responsibility"], ["Stop me for 30 minutes"]],
            5: [["I agree, start a new session"], ["Step back and lock 30 minutes"]],
        }
    else:
        labels = {
            1: [["نعم متأكد"], ["لا، خليني أتراجع"]],
            2: [["لا يهمني دعنا نكمل"], ["حسنا شكرا لتذكيري"]],
            3: [["عندي خطة واضحة"], ["غالبًا غضب، أوقفني"]],
            4: [["أتحمل القرار"], ["أوقفني نصف ساعة"]],
            5: [["أوافق، ابدأ جلسة جديدة"], ["تراجع وتعطيل نصف ساعة"]],
        }
    return ReplyKeyboardMarkup(labels.get(int(stage), labels[1]), resize_keyboard=True)

# متغيرات قديمة للتوافق مع أي موضع يستخدمها مباشرة.
trading_room_public_keyboard = ReplyKeyboardMarkup([["🚀 بدء جلسة تداول"], ["🛑 إيقاف الجلسة"], ["⬅️ رجوع"]], resize_keyboard=True)
trading_room_public_keyboard_en = ReplyKeyboardMarkup([["🚀 Start Trading Session"], ["🛑 Stop Session"], ["🔙 Back"]], resize_keyboard=True)
trading_room_ready_keyboard = ReplyKeyboardMarkup([["✅ نعم، أنا مستعد"], ["❌ إلغاء الجلسة"]], resize_keyboard=True)
trading_room_after_win_keyboard = ReplyKeyboardMarkup([["🚀 جلسة جديدة"], ["🛑 إنهاء اليوم"]], resize_keyboard=True)
trading_room_after_loss_keyboard = ReplyKeyboardMarkup([["🚀 بدء جلسة جديدة"], ["⏰ ذكرني بعد نصف ساعة"], ["🧊 تعطيل غرفة التداول نصف ساعة"]], resize_keyboard=True)
trading_room_retreat_keyboard = ReplyKeyboardMarkup([["🧊 تعطيل غرفة التداول نصف ساعة"], ["⬅️ رجوع"]], resize_keyboard=True)
trading_room_loss_confirm_keyboards = {
    1: ReplyKeyboardMarkup([["نعم متأكد"], ["لا، خليني أتراجع"]], resize_keyboard=True),
    2: ReplyKeyboardMarkup([["لا يهمني دعنا نكمل"], ["حسنا شكرا لتذكيري"]], resize_keyboard=True),
    3: ReplyKeyboardMarkup([["عندي خطة واضحة"], ["غالبًا غضب، أوقفني"]], resize_keyboard=True),
    4: ReplyKeyboardMarkup([["أتحمل القرار"], ["أوقفني نصف ساعة"]], resize_keyboard=True),
    5: ReplyKeyboardMarkup([["أوافق، ابدأ جلسة جديدة"], ["تراجع وتعطيل نصف ساعة"]], resize_keyboard=True),
}


def build_trading_room_warning_message(lang: str = "ar") -> str:
    if lang == "en":
        return (
            "🧠 Trading Session Room\n\n"
            "⚠️ This feature is still new and experimental.\n"
            "Use it carefully, follow money management, and do not enter trades outside the plan.\n\n"
            "The bot will prepare one OTC trading session, choose a pair, monitor the market, and suggest entries only when it finds a suitable pattern."
        )
    return (
        "🧠 غرفة جلسة تداول\n\n"
        "⚠️ هذه الميزة لا تزال جديدة وقيد التجربة.\n"
        "استخدمها بحذر، والتزم بإدارة رأس المال، ولا تدخل أي صفقة خارج الخطة.\n\n"
        "البوت سيجهز جلسة تداول OTC، يختار زوجًا مناسبًا، يراقب السوق، ثم يعطيك الدخول عند ظهور نمط مناسب."
    )

welcome_keyboard = ReplyKeyboardMarkup(
    [
        ["🎁 الحصول على تجربة مجانية"],
        ["✅ نعم، أنا منضم", "❌ لا، لست مشتركًا"],
        ["🎥 مشاهدة فيديو شرح البوت"],
        ["📞 تواصل مع المسؤول", "🌐 تغيير اللغة"],
    ],
    resize_keyboard=True
)

video_watched_keyboard = ReplyKeyboardMarkup(
    [
        ["✅ شاهدت الفيديو"],
        ["🔙 رجوع"],
    ],
    resize_keyboard=True
)


# ===== Language / English UI keyboards =====
language_keyboard = ReplyKeyboardMarkup(
    [
        ["🇸🇦 العربية", "🇬🇧 English"],
    ],
    resize_keyboard=True,
    one_time_keyboard=True,
)

main_keyboard_en = ReplyKeyboardMarkup(
    [
        ["📊 Generate Signals"],
        ["🧠 Trading Session Room"],
        ["👤 My Account", "🎥 Watch Bot Tutorial"],
        ["📞 Contact Support", "🌐 Change Language"],
    ],
    resize_keyboard=True
)

welcome_keyboard_en = ReplyKeyboardMarkup(
    [
        ["🎁 Get Free Trial"],
        ["✅ Yes, I Joined", "❌ No, I Haven't Joined"],
        ["🎥 Watch Bot Tutorial"],
        ["📞 Contact Support", "🌐 Change Language"],
    ],
    resize_keyboard=True
)

video_watched_keyboard_en = ReplyKeyboardMarkup(
    [
        ["✅ I Watched the Video"],
        ["🔙 Back"],
    ],
    resize_keyboard=True
)

market_mode_keyboard_en = ReplyKeyboardMarkup(
    [
        ["⚡ OTC", "🌍 Global Market"],
        ["🔙 Back"],
    ],
    resize_keyboard=True
)

otc_mode_keyboard_en = ReplyKeyboardMarkup(
    [
        ["🕒 Timed List", "⚡ Live Trade"],
        ["🔙 Back"],
    ],
    resize_keyboard=True
)

otc_live_search_keyboard_en = ReplyKeyboardMarkup(
    [
        ["🔎 Find a Trade Now"],
        ["🔙 Back"],
    ],
    resize_keyboard=True
)

real_interval_keyboard_en = ReplyKeyboardMarkup(
    [
        ["1 minute", "5 minutes"],
        ["10 minutes", "🔥 Best Opportunity"],
        ["🔙 Back"],
    ],
    resize_keyboard=True
)


otc_pairs_keyboard_en = ReplyKeyboardMarkup(
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
        ["🔙 Back"],
    ],
    resize_keyboard=True
)

real_pairs_keyboard_en = ReplyKeyboardMarkup(
    [
        ["EUR/USD", "GBP/USD"],
        ["USD/JPY", "USD/CHF"],
        ["USD/CAD", "AUD/USD"],
        ["NZD/USD", "EUR/JPY"],
        ["AUD/JPY", "EUR/GBP"],
        ["CAD/JPY", "EUR/CAD"],
        ["AUD/CHF", "CHF/CAD"],
        ["AUD/CAD", "GBP/AUD"],
        ["🔙 Back"],
    ],
    resize_keyboard=True
)

count_keyboard_en = ReplyKeyboardMarkup(
    [
        ["3", "5", "10"],
        ["15", "20"],
        ["🔙 Back"],
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
        ["💬 إرسال رسالة"],
        ["⬅️ رجوع"],
    ],
    resize_keyboard=True
)

# ===== Firebase refs =====



# ===== OTC Live hard stop when disabled =====
OTC_LIVE_DISABLED_CHECK_SECONDS = int(os.getenv("OTC_LIVE_DISABLED_CHECK_SECONDS", "120"))
_OTC_LIVE_DISABLED_LAST_CHECK = 0.0
_OTC_LIVE_DISABLED_LAST_ENABLED = None


def should_check_otc_live_enabled_when_disabled() -> bool:
    """عندما تكون قناة OTC Live متوقفة، لا نفحص Firebase كل 5 ثواني.
    نفحص فقط كل OTC_LIVE_DISABLED_CHECK_SECONDS حتى نعرف إذا تم تشغيلها من الأدمن.
    """
    global _OTC_LIVE_DISABLED_LAST_CHECK

    try:
        now_ts = time_module.time()
        if now_ts - float(_OTC_LIVE_DISABLED_LAST_CHECK or 0) >= int(OTC_LIVE_DISABLED_CHECK_SECONDS):
            _OTC_LIVE_DISABLED_LAST_CHECK = now_ts
            return True
        return False
    except Exception:
        return True


def remember_otc_live_enabled_state(enabled: bool):
    global _OTC_LIVE_DISABLED_LAST_ENABLED
    _OTC_LIVE_DISABLED_LAST_ENABLED = bool(enabled)


def get_remembered_otc_live_enabled_state():
    return _OTC_LIVE_DISABLED_LAST_ENABLED


# ===== Quiet skipped job logging =====
_QUIET_LOG_TIMERS = {}


def should_log_quiet(key: str, every_seconds: int = 300) -> bool:
    """يمنع تكرار نفس اللوج كل 5 ثواني، ويطبعه مرة كل عدة دقائق فقط."""
    try:
        now_ts = time_module.time()
        last = float(_QUIET_LOG_TIMERS.get(key, 0) or 0)
        if now_ts - last >= int(every_seconds):
            _QUIET_LOG_TIMERS[key] = now_ts
            return True
        return False
    except Exception:
        return True


# ===== Firebase read saver cache =====
FIREBASE_CACHE_TTL_SECONDS = int(os.getenv("FIREBASE_CACHE_TTL_SECONDS", "60"))
FIREBASE_CHANNEL_SETTINGS_TTL_SECONDS = int(os.getenv("FIREBASE_CHANNEL_SETTINGS_TTL_SECONDS", "60"))
FIREBASE_USER_CACHE_TTL_SECONDS = int(os.getenv("FIREBASE_USER_CACHE_TTL_SECONDS", "300"))
FIREBASE_APPROVED_CACHE_TTL_SECONDS = int(os.getenv("FIREBASE_APPROVED_CACHE_TTL_SECONDS", "300"))
FIREBASE_PENDING_CACHE_TTL_SECONDS = int(os.getenv("FIREBASE_PENDING_CACHE_TTL_SECONDS", "60"))
FIREBASE_FULL_LIST_CACHE_TTL_SECONDS = int(os.getenv("FIREBASE_FULL_LIST_CACHE_TTL_SECONDS", "30"))
FIREBASE_BOT_SETTINGS_TTL_SECONDS = int(os.getenv("FIREBASE_BOT_SETTINGS_TTL_SECONDS", "60"))
SAVE_USER_LAST_SEEN_THROTTLE_SECONDS = int(os.getenv("SAVE_USER_LAST_SEEN_THROTTLE_SECONDS", "300"))

# ===== Signal usage limits =====
# هذه الحدود تطبق على توليد الإشارات اليدوي فقط، ولا علاقة لها بأي نشر تلقائي.
FREE_TRIAL_SIGNAL_TOTAL_LIMIT = int(os.getenv("FREE_TRIAL_SIGNAL_TOTAL_LIMIT", "10"))
WEEKLY_SIGNAL_DAILY_LIMIT = int(os.getenv("WEEKLY_SIGNAL_DAILY_LIMIT", "30"))
MONTHLY_SIGNAL_DAILY_LIMIT = int(os.getenv("MONTHLY_SIGNAL_DAILY_LIMIT", "50"))
SIGNAL_USAGE_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_USAGE_COOLDOWN_SECONDS", "3"))
ADMIN_ERROR_ALERT_COOLDOWN_SECONDS = int(os.getenv("ADMIN_ERROR_ALERT_COOLDOWN_SECONDS", "300"))

# ===== TRADING TIME COPY =====
# يربط البوت مع Copy Server حتى تصل الصفقات مباشرة إلى إضافة Chrome.
# اتركه false حتى تكون جاهزًا للتجربة، ثم فعّله من .env.
COPY_TRADING_ENABLED = os.getenv("COPY_TRADING_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
COPY_SERVER_URL = os.getenv("COPY_SERVER_URL", f"http://127.0.0.1:{os.getenv('PORT', '8080')}").rstrip("/")
COPY_SERVER_SECRET = os.getenv("COPY_SERVER_SECRET", "change-me-now")
COPY_SIGNAL_VALIDITY_SECONDS = int(os.getenv("COPY_SIGNAL_VALIDITY_SECONDS", "25"))
COPY_REQUEST_TIMEOUT_SECONDS = int(os.getenv("COPY_REQUEST_TIMEOUT_SECONDS", "6"))
COPY_SEND_OTC_LIVE_NOW = os.getenv("COPY_SEND_OTC_LIVE_NOW", "true").lower() in {"1", "true", "yes", "on"}
COPY_SEND_REAL_MARKET = os.getenv("COPY_SEND_REAL_MARKET", "true").lower() in {"1", "true", "yes", "on"}
COPY_SEND_TIMED_LISTS = os.getenv("COPY_SEND_TIMED_LISTS", "true").lower() in {"1", "true", "yes", "on"}
COPY_SEND_THREE_CANDLE = os.getenv("COPY_SEND_THREE_CANDLE", "true").lower() in {"1", "true", "yes", "on"}
COPY_SEND_TRADING_ROOM = os.getenv("COPY_SEND_TRADING_ROOM", "true").lower() in {"1", "true", "yes", "on"}

# يمنع صفقات المستخدمين العاديين داخل البوت من الوصول إلى إضافة النسخ.
# الافتراضي: فقط الأدمن أو الأرقام الموجودة في COPY_SIGNAL_ALLOWED_TELEGRAM_IDS يستطيعون بث الصفقة إلى Copy Trading.
# v0.26: user signal routing makes every user signal private to that user's own extension.
# When this is enabled, user-generated signals are allowed into Copy Server, but delivered only
# to extensions linked with the same Telegram user id. Broadcast/system signals without target_user_id
# still go to all connected extensions that enabled their source.
COPY_USER_SIGNAL_ROUTING_ENABLED = os.getenv("COPY_USER_SIGNAL_ROUTING_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
COPY_USER_SIGNALS_ADMIN_ONLY = os.getenv("COPY_USER_SIGNALS_ADMIN_ONLY", "false").lower() in {"1", "true", "yes", "on"}
COPY_SIGNAL_ALLOWED_TELEGRAM_IDS = parse_id_set_from_env("COPY_SIGNAL_ALLOWED_TELEGRAM_IDS")
COPY_SIGNAL_ALLOWED_TELEGRAM_IDS.add(int(ADMIN_TELEGRAM_ID))

# ===== Embedded TRADING TIME COPY SERVER =====
# يشغل Copy Server داخل نفس خدمة Render الخاصة بالبوت حتى لا تحتاج Web Service ثاني.
COPY_EMBEDDED_SERVER_ENABLED = os.getenv("COPY_EMBEDDED_SERVER_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
COPY_ALLOWED_ORIGINS = [x.strip() for x in os.getenv("COPY_ALLOWED_ORIGINS", "*").split(",") if x.strip()]
COPY_LICENSES = os.getenv("COPY_LICENSES", "DEMO-111:active")
COPY_LICENSE_DEFAULT_MAX_DEVICES = int(os.getenv("COPY_LICENSE_DEFAULT_MAX_DEVICES", "1"))
COPY_LICENSE_DEVICE_TOUCH_SECONDS = int(os.getenv("COPY_LICENSE_DEVICE_TOUCH_SECONDS", "600"))
COPY_SETTINGS_CACHE_TTL_SECONDS = int(os.getenv("COPY_SETTINGS_CACHE_TTL_SECONDS", "60"))
COPY_SIGNAL_HISTORY_LIMIT = int(os.getenv("COPY_SIGNAL_HISTORY_LIMIT", "200"))
COPY_UVICORN_LOG_LEVEL = os.getenv("COPY_UVICORN_LOG_LEVEL", "info")


_firebase_cache = {}


def _cache_get(key: str, ttl: int | None = None):
    try:
        ttl = int(ttl or FIREBASE_CACHE_TTL_SECONDS)
        item = _firebase_cache.get(key)
        if not item:
            return None
        ts, value = item
        if time_module.time() - float(ts) > ttl:
            _firebase_cache.pop(key, None)
            return None
        return value
    except Exception:
        return None


def _cache_set(key: str, value):
    try:
        _firebase_cache[key] = (time_module.time(), value)
    except Exception:
        pass
    return value


def _cache_delete_prefix(prefix: str):
    try:
        for key in list(_firebase_cache.keys()):
            if str(key).startswith(prefix):
                _firebase_cache.pop(key, None)
    except Exception:
        pass


def clear_user_cache(user_id: int):
    try:
        uid = int(user_id)
        _cache_delete_prefix(f"user_status:{uid}")
        _cache_delete_prefix(f"approved:{uid}")
        _cache_delete_prefix(f"approved_data:{uid}")
        _cache_delete_prefix(f"user_record:{uid}")
        _cache_delete_prefix(f"pending:{uid}")
        _cache_delete_prefix(f"video_trial:{uid}")
        _cache_delete_prefix(f"last_seen_write:{uid}")
    except Exception:
        pass


def clear_users_list_cache():
    _cache_delete_prefix("all_users")
    _cache_delete_prefix("recent_active_approved")


def clear_approved_list_cache():
    _cache_delete_prefix("all_approved_users")
    _cache_delete_prefix("recent_active_approved")


def clear_pending_list_cache():
    _cache_delete_prefix("all_pending_users")



def clear_channel_publish_cache():
    _cache_delete_prefix("channel_publish")



# ===== Firebase read diagnostics =====
FIREBASE_READ_DIAGNOSTICS_ENABLED = os.getenv("FIREBASE_READ_DIAGNOSTICS_ENABLED", "false").lower() == "true"
FIREBASE_READ_REPORT_SECONDS = int(os.getenv("FIREBASE_READ_REPORT_SECONDS", "300"))
FIREBASE_READ_TOP_N = int(os.getenv("FIREBASE_READ_TOP_N", "25"))

_firebase_read_stats = {}
_firebase_write_stats = {}
_firebase_diag_installed = False


def _diag_ref_path(ref) -> str:
    try:
        return str(getattr(ref, "_path", None) or getattr(ref, "path", None) or ref)
    except Exception:
        return "unknown"


def _diag_size(value) -> int:
    try:
        return len(json.dumps(value, ensure_ascii=False, default=str).encode("utf-8"))
    except Exception:
        try:
            return len(str(value).encode("utf-8"))
        except Exception:
            return 0


def _diag_add(store: dict, op: str, path: str, size: int = 0):
    try:
        key = f"{op} {path}"
        item = store.setdefault(key, {"count": 0, "bytes": 0})
        item["count"] += 1
        item["bytes"] += int(size or 0)
    except Exception:
        pass


def install_firebase_diagnostics():
    global _firebase_diag_installed
    if _firebase_diag_installed or not FIREBASE_READ_DIAGNOSTICS_ENABLED:
        return
    try:
        ref_cls = type(db.reference("/"))

        original_get = ref_cls.get
        original_set = ref_cls.set
        original_update = ref_cls.update
        original_delete = ref_cls.delete

        def patched_get(self, *args, **kwargs):
            result = original_get(self, *args, **kwargs)
            _diag_add(_firebase_read_stats, "GET", _diag_ref_path(self), _diag_size(result))
            return result

        def patched_set(self, value, *args, **kwargs):
            _diag_add(_firebase_write_stats, "SET", _diag_ref_path(self), _diag_size(value))
            return original_set(self, value, *args, **kwargs)

        def patched_update(self, value, *args, **kwargs):
            _diag_add(_firebase_write_stats, "UPDATE", _diag_ref_path(self), _diag_size(value))
            return original_update(self, value, *args, **kwargs)

        def patched_delete(self, *args, **kwargs):
            _diag_add(_firebase_write_stats, "DELETE", _diag_ref_path(self), 0)
            return original_delete(self, *args, **kwargs)

        ref_cls.get = patched_get
        ref_cls.set = patched_set
        ref_cls.update = patched_update
        ref_cls.delete = patched_delete
        _firebase_diag_installed = True
        logger.warning("Firebase diagnostics installed successfully")
    except Exception as e:
        logger.exception("Could not install Firebase diagnostics: %s", e)



# ===== Firebase HTTP diagnostics =====
_firebase_http_stats = {}
_firebase_http_diag_installed = False


def _is_firebase_url(url: str) -> bool:
    try:
        parsed = urlparse(str(url))
        host = parsed.netloc.lower()
        return (
            "firebaseio.com" in host
            or "firebasedatabase.app" in host
            or "/firebaseio.com/" in str(url)
            or "/firebasedatabase.app/" in str(url)
        )
    except Exception:
        return False


def _clean_firebase_url_path(url: str) -> str:
    try:
        parsed = urlparse(str(url))
        path = parsed.path or "/"
        # اخفِ query/auth
        return f"{parsed.netloc}{path}"
    except Exception:
        return str(url).split("?")[0]


def _http_stat_add(method: str, url: str, response_size: int = 0):
    try:
        key = f"{str(method).upper()} {_clean_firebase_url_path(url)}"
        item = _firebase_http_stats.setdefault(key, {"count": 0, "bytes": 0})
        item["count"] += 1
        item["bytes"] += int(response_size or 0)
    except Exception:
        pass


def install_firebase_http_diagnostics():
    global _firebase_http_diag_installed
    if _firebase_http_diag_installed or not FIREBASE_READ_DIAGNOSTICS_ENABLED:
        return

    try:
        import requests

        original_request = requests.sessions.Session.request

        def patched_request(self, method, url, *args, **kwargs):
            response = original_request(self, method, url, *args, **kwargs)

            try:
                if _is_firebase_url(url):
                    size = 0
                    try:
                        content = getattr(response, "content", b"") or b""
                        size = len(content)
                    except Exception:
                        pass
                    _http_stat_add(method, url, size)
            except Exception:
                pass

            return response

        requests.sessions.Session.request = patched_request
        _firebase_http_diag_installed = True
        logger.warning("Firebase HTTP diagnostics installed successfully")
    except Exception as e:
        logger.exception("Could not install Firebase HTTP diagnostics: %s", e)



# ===== Firebase urllib3 diagnostics =====
_firebase_urllib3_stats = {}
_firebase_urllib3_diag_installed = False


def _urllib3_build_url(pool, url: str) -> str:
    try:
        host = getattr(pool, "host", "") or ""
        scheme = getattr(pool, "scheme", "https") or "https"
        if str(url).startswith("http://") or str(url).startswith("https://"):
            return str(url)
        return f"{scheme}://{host}{url}"
    except Exception:
        return str(url)


def _urllib3_stat_add(method: str, url: str, response_size: int = 0):
    try:
        key = f"{str(method).upper()} {_clean_firebase_url_path(url)}"
        item = _firebase_urllib3_stats.setdefault(key, {"count": 0, "bytes": 0})
        item["count"] += 1
        item["bytes"] += int(response_size or 0)
    except Exception:
        pass


def install_firebase_urllib3_diagnostics():
    global _firebase_urllib3_diag_installed

    if _firebase_urllib3_diag_installed or not FIREBASE_READ_DIAGNOSTICS_ENABLED:
        return

    try:
        import urllib3

        original_urlopen = urllib3.connectionpool.HTTPConnectionPool.urlopen

        def patched_urlopen(self, method, url, *args, **kwargs):
            full_url = _urllib3_build_url(self, url)
            response = original_urlopen(self, method, url, *args, **kwargs)

            try:
                if _is_firebase_url(full_url):
                    size = 0

                    try:
                        data = getattr(response, "data", None)
                        if data is not None:
                            size = len(data)
                    except Exception:
                        pass

                    if not size:
                        try:
                            cl = response.headers.get("Content-Length")
                            if cl:
                                size = int(cl)
                        except Exception:
                            pass

                    _urllib3_stat_add(method, full_url, size)
            except Exception:
                pass

            return response

        urllib3.connectionpool.HTTPConnectionPool.urlopen = patched_urlopen
        _firebase_urllib3_diag_installed = True
        logger.warning("Firebase urllib3 diagnostics installed successfully")
    except Exception as e:
        logger.exception("Could not install Firebase urllib3 diagnostics: %s", e)


def format_firebase_urllib3_diagnostics_report() -> str:
    try:
        items = sorted(
            _firebase_urllib3_stats.items(),
            key=lambda kv: (kv[1].get("bytes", 0), kv[1].get("count", 0)),
            reverse=True
        )[:FIREBASE_READ_TOP_N]

        total_bytes = sum(v.get("bytes", 0) for v in _firebase_urllib3_stats.values())
        total_count = sum(v.get("count", 0) for v in _firebase_urllib3_stats.values())

        lines = [
            "========== FIREBASE URLLIB3 DIAGNOSTICS ==========",
            f"URLLIB3 FIREBASE CALLS: {total_count} calls | approx {total_bytes / 1024 / 1024:.2f} MB response",
            "",
            "TOP FIREBASE URLLIB3 PATHS:",
        ]

        if not items:
            lines.append("- no firebase urllib3 calls recorded")
        else:
            for key, stat in items:
                lines.append(
                    f"- {key} | calls={stat.get('count', 0)} | approx={stat.get('bytes', 0) / 1024 / 1024:.2f} MB"
                )

        lines.append("===================================================")
        return "\n".join(lines)
    except Exception as e:
        return f"Firebase urllib3 diagnostics report error: {e}"



def format_firebase_http_diagnostics_report() -> str:
    try:
        items = sorted(
            _firebase_http_stats.items(),
            key=lambda kv: (kv[1].get("bytes", 0), kv[1].get("count", 0)),
            reverse=True
        )[:FIREBASE_READ_TOP_N]

        total_bytes = sum(v.get("bytes", 0) for v in _firebase_http_stats.values())
        total_count = sum(v.get("count", 0) for v in _firebase_http_stats.values())

        lines = [
            "========== FIREBASE HTTP DIAGNOSTICS ==========",
            f"HTTP FIREBASE CALLS: {total_count} calls | approx {total_bytes / 1024 / 1024:.2f} MB response",
            "",
            "TOP FIREBASE HTTP PATHS:",
        ]

        if not items:
            lines.append("- no firebase http calls recorded")
        else:
            for key, stat in items:
                lines.append(
                    f"- {key} | calls={stat.get('count', 0)} | approx={stat.get('bytes', 0) / 1024 / 1024:.2f} MB"
                )

        lines.append("===============================================")
        return "\n".join(lines)
    except Exception as e:
        return f"Firebase HTTP diagnostics report error: {e}"



def format_firebase_diagnostics_report() -> str:
    read_items = sorted(
        _firebase_read_stats.items(),
        key=lambda kv: (kv[1].get("bytes", 0), kv[1].get("count", 0)),
        reverse=True
    )[:FIREBASE_READ_TOP_N]
    write_items = sorted(
        _firebase_write_stats.items(),
        key=lambda kv: (kv[1].get("bytes", 0), kv[1].get("count", 0)),
        reverse=True
    )[:FIREBASE_READ_TOP_N]

    total_read_bytes = sum(v.get("bytes", 0) for v in _firebase_read_stats.values())
    total_read_count = sum(v.get("count", 0) for v in _firebase_read_stats.values())
    total_write_bytes = sum(v.get("bytes", 0) for v in _firebase_write_stats.values())
    total_write_count = sum(v.get("count", 0) for v in _firebase_write_stats.values())

    lines = [
        "========== FIREBASE DIAGNOSTICS REPORT ==========",
        f"READS: {total_read_count} calls | approx {total_read_bytes / 1024 / 1024:.2f} MB returned",
        f"WRITES: {total_write_count} calls | approx {total_write_bytes / 1024 / 1024:.2f} MB payload",
        "",
        "TOP READ PATHS:",
    ]

    if not read_items:
        lines.append("- no reads recorded")
    else:
        for key, stat in read_items:
            lines.append(f"- {key} | calls={stat.get('count', 0)} | approx={stat.get('bytes', 0) / 1024 / 1024:.2f} MB")

    lines.append("")
    lines.append("TOP WRITE PATHS:")
    if not write_items:
        lines.append("- no writes recorded")
    else:
        for key, stat in write_items:
            lines.append(f"- {key} | calls={stat.get('count', 0)} | approx={stat.get('bytes', 0) / 1024 / 1024:.2f} MB")

    lines.append("=================================================")
    return "\n".join(lines)


async def firebase_diagnostics_report_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        logger.warning("\n%s", format_firebase_diagnostics_report())
        logger.warning("\n%s", format_firebase_http_diagnostics_report())
        logger.warning("\n%s", format_firebase_urllib3_diagnostics_report())
    except Exception as e:
        logger.exception("Firebase diagnostics job error: %s", e)




def safe_key(value) -> str:
    """Firebase-safe key."""
    try:
        s = str(value or "").strip()
        s = s.replace(".", "_").replace("#", "_").replace("$", "_").replace("[", "_").replace("]", "_").replace("/", "_")
        s = re.sub(r"[^A-Za-z0-9_\-]+", "_", s)
        s = re.sub(r"_+", "_", s).strip("_")
        return s or "unknown"
    except Exception:
        return "unknown"


def normalize_otc_pair_input(pair_text: str) -> str:
    """Normalize display OTC pair text to internal symbol style."""
    raw = str(pair_text or "").strip()
    raw = raw.replace("(OTC)", "").replace("OTC", "").strip()
    raw = raw.replace(" ", "")
    if "/" in raw:
        a, b = raw.split("/", 1)
        return f"{a.upper()}{b.upper()}_otc"
    if raw.lower().endswith("_otc"):
        return raw
    return f"{raw.upper()}_otc"


def otc_list_jobs_ref(user_id: int):
    """Reference for saved OTC list jobs per user/admin."""
    return system_ref().child("otc_list_jobs").child(str(int(user_id)))


def get_otc_feed_diagnostics_for_pair(pair_text: str) -> str:
    """Safe diagnostics message for OTC pair feed status."""
    try:
        symbol = normalize_otc_pair_input(pair_text)
        ticks = OTC_TICKS_CACHE.get(symbol, []) if "OTC_TICKS_CACHE" in globals() else []
        candles = OTC_CANDLES_CACHE.get(symbol, []) if "OTC_CANDLES_CACHE" in globals() else []

        last_tick = ticks[-1] if ticks else None
        last_candle = candles[-1] if candles else None

        lines = [
            "فحص بيانات زوج OTC",
            "",
            f"الزوج: {pair_text}",
            f"الرمز: {symbol}",
            "",
            f"عدد ticks بالكاش: {len(ticks)}",
            f"عدد الشموع بالكاش: {len(candles)}",
        ]

        if isinstance(last_tick, dict):
            lines.append(f"آخر tick: {last_tick}")
        elif last_tick is not None:
            lines.append(f"آخر tick: {last_tick}")
        else:
            lines.append("آخر tick: لا يوجد")

        if isinstance(last_candle, dict):
            lines.append(f"آخر شمعة: {last_candle}")
        elif last_candle is not None:
            lines.append(f"آخر شمعة: {last_candle}")
        else:
            lines.append("آخر شمعة: لا يوجد")

        return "\n".join(lines)
    except Exception as e:
        return f"تعذر فحص بيانات الزوج: {e}"




# ===== 24h Firebase usage monitor =====
FIREBASE_24H_MONITOR_ENABLED = os.getenv("FIREBASE_24H_MONITOR_ENABLED", "true").lower() == "true"
FIREBASE_24H_REPORT_SECONDS = int(os.getenv("FIREBASE_24H_REPORT_SECONDS", "86400"))
FIREBASE_24H_TOP_N = int(os.getenv("FIREBASE_24H_TOP_N", "25"))

_firebase_24h_read_stats = {}
_firebase_24h_write_stats = {}
_firebase_24h_monitor_installed = False
_firebase_24h_started_at = None


def _fb24_ref_path(ref) -> str:
    try:
        return str(getattr(ref, "_path", None) or getattr(ref, "path", None) or ref)
    except Exception:
        return "unknown"


def _fb24_size(value) -> int:
    try:
        return len(json.dumps(value, ensure_ascii=False, default=str).encode("utf-8"))
    except Exception:
        try:
            return len(str(value).encode("utf-8"))
        except Exception:
            return 0


def _fb24_add(store: dict, op: str, path: str, size: int = 0):
    try:
        key = f"{op} {path}"
        item = store.setdefault(key, {"count": 0, "bytes": 0})
        item["count"] += 1
        item["bytes"] += int(size or 0)
    except Exception:
        pass


def install_firebase_24h_monitor():
    """يراقب Firebase Admin SDK reads/writes ويرسل ملخص كل 24 ساعة للأدمن."""
    global _firebase_24h_monitor_installed, _firebase_24h_started_at

    if _firebase_24h_monitor_installed or not FIREBASE_24H_MONITOR_ENABLED:
        return

    try:
        ref_cls = type(db.reference("/"))

        # إذا كان في تشخيص سابق مركب، لا نركب فوقه مرتين.
        if getattr(ref_cls, "_trading_time_fb24_wrapped", False):
            _firebase_24h_monitor_installed = True
            return

        original_get = ref_cls.get
        original_set = ref_cls.set
        original_update = ref_cls.update
        original_delete = ref_cls.delete

        def patched_get(self, *args, **kwargs):
            result = original_get(self, *args, **kwargs)
            _fb24_add(_firebase_24h_read_stats, "GET", _fb24_ref_path(self), _fb24_size(result))
            return result

        def patched_set(self, value, *args, **kwargs):
            _fb24_add(_firebase_24h_write_stats, "SET", _fb24_ref_path(self), _fb24_size(value))
            return original_set(self, value, *args, **kwargs)

        def patched_update(self, value, *args, **kwargs):
            _fb24_add(_firebase_24h_write_stats, "UPDATE", _fb24_ref_path(self), _fb24_size(value))
            return original_update(self, value, *args, **kwargs)

        def patched_delete(self, *args, **kwargs):
            _fb24_add(_firebase_24h_write_stats, "DELETE", _fb24_ref_path(self), 0)
            return original_delete(self, *args, **kwargs)

        ref_cls.get = patched_get
        ref_cls.set = patched_set
        ref_cls.update = patched_update
        ref_cls.delete = patched_delete
        ref_cls._trading_time_fb24_wrapped = True

        _firebase_24h_started_at = now_iso()
        _firebase_24h_monitor_installed = True
        logger.warning("Firebase 24h monitor installed successfully")

    except Exception as e:
        logger.exception("Could not install Firebase 24h monitor: %s", e)


def build_firebase_24h_report_text() -> str:
    read_items = sorted(
        _firebase_24h_read_stats.items(),
        key=lambda kv: (kv[1].get("bytes", 0), kv[1].get("count", 0)),
        reverse=True
    )[:FIREBASE_24H_TOP_N]

    write_items = sorted(
        _firebase_24h_write_stats.items(),
        key=lambda kv: (kv[1].get("bytes", 0), kv[1].get("count", 0)),
        reverse=True
    )[:FIREBASE_24H_TOP_N]

    total_read_bytes = sum(v.get("bytes", 0) for v in _firebase_24h_read_stats.values())
    total_read_count = sum(v.get("count", 0) for v in _firebase_24h_read_stats.values())
    total_write_bytes = sum(v.get("bytes", 0) for v in _firebase_24h_write_stats.values())
    total_write_count = sum(v.get("count", 0) for v in _firebase_24h_write_stats.values())

    lines = []
    lines.append("📊 Firebase 24h Usage Monitor")
    lines.append("━━━━━━━━━━━━━━")
    lines.append(f"بدأت المراقبة: {_firebase_24h_started_at or 'unknown'}")
    lines.append(f"مدة التقرير: {int(FIREBASE_24H_REPORT_SECONDS / 3600)} ساعة")
    lines.append("")
    lines.append(f"📥 READS: {total_read_count} calls")
    lines.append(f"📦 Approx read size: {total_read_bytes / 1024 / 1024:.2f} MB")
    lines.append("")
    lines.append(f"📤 WRITES: {total_write_count} calls")
    lines.append(f"📦 Approx write payload: {total_write_bytes / 1024 / 1024:.2f} MB")
    lines.append("")
    lines.append("TOP READ PATHS:")

    if not read_items:
        lines.append("- لا يوجد قراءات مسجلة")
    else:
        for key, stat in read_items:
            lines.append(
                f"- {key} | {stat.get('count', 0)} calls | {stat.get('bytes', 0) / 1024 / 1024:.2f} MB"
            )

    lines.append("")
    lines.append("TOP WRITE PATHS:")
    if not write_items:
        lines.append("- لا يوجد كتابات مسجلة")
    else:
        for key, stat in write_items:
            lines.append(
                f"- {key} | {stat.get('count', 0)} calls | {stat.get('bytes', 0) / 1024 / 1024:.2f} MB"
            )

    lines.append("━━━━━━━━━━━━━━")
    lines.append("ملاحظة: الحجم تقريبي من داخل البوت، وقيمة Firebase الرسمية قد تتأخر أو تختلف قليلًا.")
    return "\n".join(lines)


def reset_firebase_24h_stats():
    global _firebase_24h_started_at
    try:
        _firebase_24h_read_stats.clear()
        _firebase_24h_write_stats.clear()
        _firebase_24h_started_at = now_iso()
    except Exception:
        pass


async def send_firebase_24h_report_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        if not FIREBASE_24H_MONITOR_ENABLED:
            return

        report = build_firebase_24h_report_text()

        # إرسال التقرير للأدمن على الخاص
        await safe_send_message(context.bot,
            chat_id=ADMIN_TELEGRAM_ID,
            text=report[:3900]
        )

        # إذا التقرير طويل، أرسل الباقي برسالة ثانية
        if len(report) > 3900:
            await safe_send_message(context.bot,
                chat_id=ADMIN_TELEGRAM_ID,
                text=report[3900:7800]
            )

        logger.warning("\n%s", report)

        # نبدأ عدّاد جديد لليوم التالي
        reset_firebase_24h_stats()

    except Exception as e:
        logger.exception("Firebase 24h report job error: %s", e)



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



def force_channel_publish_setting(channel_key: str, enabled: bool):
    try:
        system_ref().child("channel_publish").child(str(channel_key)).set(bool(enabled))
        clear_channel_publish_cache()
        if str(channel_key) == "otc_live":
            remember_otc_live_enabled_state(bool(enabled))
        return True
    except Exception as e:
        logger.exception("Could not force channel publish setting %s=%s: %s", channel_key, enabled, e)
        return False


def get_channel_publish_settings() -> dict:
    cached = _cache_get("channel_publish:settings", FIREBASE_CHANNEL_SETTINGS_TTL_SECONDS)
    if cached is not None:
        return cached

    default = {"real": True, "otc": True, "otc_live": False}
    try:
        data = system_ref().child("channel_publish").get() or {}
        if not isinstance(data, dict):
            data = {}

        result = {
            "real": bool(data.get("real", default["real"])),
            "otc": bool(data.get("otc", default["otc"])),
            "otc_live": False,
        }
        return _cache_set("channel_publish:settings", result)
    except Exception as e:
        logger.exception("Could not read channel publish settings: %s", e)
        return _cache_set("channel_publish:settings", default)



def is_channel_publish_enabled(channel_key: str) -> bool:
    return bool(get_channel_publish_settings().get(channel_key, True))


def set_channel_publish_enabled(channel_key: str, enabled: bool):
    force_channel_publish_setting(channel_key, enabled)




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



# ===== TRADING TIME COPY LIGHT ADMIN CONTROL =====
def copy_settings_ref():
    return system_ref().child("copy_trading").child("settings")


def get_copy_settings() -> dict:
    cached = _cache_get("copy_trading:settings", COPY_SETTINGS_CACHE_TTL_SECONDS)
    if cached is not None:
        return cached

    default = {
        "global_enabled": True,
        "latest_version": "v0.42",
        "update_notice": "",
        "updated_at": None,
    }
    try:
        data = copy_settings_ref().get() or {}
        if not isinstance(data, dict):
            data = {}
        result = {**default, **data}
        result["global_enabled"] = bool(result.get("global_enabled", True))
        return _cache_set("copy_trading:settings", result)
    except Exception as e:
        logger.warning("Could not read copy settings: %s", e)
        return _cache_set("copy_trading:settings", default)


def clear_copy_settings_cache():
    _cache_delete_prefix("copy_trading:settings")


def is_copy_global_enabled() -> bool:
    return bool(get_copy_settings().get("global_enabled", True))


def set_copy_global_enabled(enabled: bool, admin_id: int | None = None) -> bool:
    try:
        copy_settings_ref().update({
            "global_enabled": bool(enabled),
            "updated_at": now_iso(),
            "updated_by": int(admin_id) if admin_id else None,
        })
        clear_copy_settings_cache()
        return True
    except Exception as e:
        logger.warning("Could not update copy global enabled: %s", e)
        return False


def set_copy_update_notice(message: str, admin_id: int | None = None) -> bool:
    try:
        text = str(message or "").strip()[:600]
        copy_settings_ref().update({
            "update_notice": text,
            "latest_version": "v0.42",
            "updated_at": now_iso(),
            "updated_by": int(admin_id) if admin_id else None,
        })
        clear_copy_settings_cache()
        return True
    except Exception as e:
        logger.warning("Could not update copy notice: %s", e)
        return False


def copy_public_settings_payload() -> dict:
    settings = get_copy_settings()
    return {
        "global_enabled": bool(settings.get("global_enabled", True)),
        "latest_version": settings.get("latest_version") or "v0.42",
        "update_notice": settings.get("update_notice") or "",
        "updated_at": settings.get("updated_at"),
    }


# ===== TRADING TIME COPY LICENSE MANAGER =====
_copy_license_touch_cache = {}


def copy_licenses_ref():
    return system_ref().child("copy_trading").child("licenses")


def copy_license_key(token: str) -> str:
    return safe_key(str(token or "").strip())


def normalize_copy_license_token(token: str) -> str:
    return str(token or "").strip().upper().replace(" ", "")


def generate_copy_license_token(plan: str = "month") -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    chunks = []
    for _ in range(3):
        chunks.append("".join(random.choice(alphabet) for _ in range(4)))
    prefix = {"week": "TTW", "month": "TTM", "forever": "TTV"}.get(str(plan), "TTC")
    return f"{prefix}-" + "-".join(chunks)


def copy_license_expiry_for_plan(plan: str):
    plan = str(plan or "month").lower()
    if plan in {"forever", "vip", "lifetime", "permanent"}:
        return "forever"
    if plan in {"week", "weekly", "7d"}:
        return (now_utc() + timedelta(days=7)).isoformat()
    if plan in {"month", "monthly", "30d"}:
        return (now_utc() + timedelta(days=30)).isoformat()
    return (now_utc() + timedelta(days=30)).isoformat()


def copy_license_is_expired(expires_at) -> bool:
    try:
        if not expires_at or str(expires_at).lower() == "forever":
            return False
        dt = parse_iso(str(expires_at))
        if dt is None:
            # Accept ISO strings with Z suffix.
            raw = str(expires_at).strip()
            if raw.endswith("Z"):
                dt = datetime.fromisoformat(raw[:-1] + "+00:00")
        if dt is None:
            return False
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return now_utc() > dt.astimezone(UTC)
    except Exception:
        return False


def copy_license_env_records() -> dict:
    # Backward-compatible static licenses from Render env.
    result = {}
    raw = str(COPY_LICENSES or "DEMO-111:active")
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        bits = part.split(":")
        token = normalize_copy_license_token(bits[0] if bits else "")
        status = (bits[1].strip().lower() if len(bits) > 1 and bits[1].strip() else "active")
        if token:
            result[token] = {
                "token": token,
                "status": status,
                "plan": "env",
                "source": "env",
                "expires_at": "forever",
                "max_devices": 999,
                "devices": {},
                "created_at": None,
            }
    return result


def get_copy_license_record(token: str):
    token = normalize_copy_license_token(token)
    if not token:
        return None
    try:
        data = copy_licenses_ref().child(copy_license_key(token)).get()
        if isinstance(data, dict):
            data["token"] = normalize_copy_license_token(data.get("token") or token)
            data["source"] = data.get("source") or "firebase"
            return data
    except Exception as e:
        logger.warning("Could not read copy license from Firebase: %s", e)
    return copy_license_env_records().get(token)


def create_copy_license(plan: str, created_by: int | None = None, max_devices: int | None = None) -> dict:
    plan = str(plan or "month").lower()
    max_devices = max(1, int(max_devices or COPY_LICENSE_DEFAULT_MAX_DEVICES or 1))

    # Avoid collisions, even though they are very unlikely.
    token = generate_copy_license_token(plan)
    for _ in range(10):
        if not get_copy_license_record(token):
            break
        token = generate_copy_license_token(plan)

    record = {
        "token": token,
        "status": "active",
        "plan": plan,
        "source": "firebase",
        "created_by": int(created_by) if created_by else None,
        "created_at": now_iso(),
        "expires_at": copy_license_expiry_for_plan(plan),
        "max_devices": max_devices,
        "devices": {},
        "telegram_user_id": None,
        "telegram_linked_at": None,
        "disabled_at": None,
        "last_seen_at": None,
    }
    copy_licenses_ref().child(copy_license_key(token)).set(record)
    return record


def disable_copy_license(token: str) -> bool:
    token = normalize_copy_license_token(token)
    if not token:
        return False
    record = get_copy_license_record(token)
    if not record or record.get("source") == "env":
        return False
    copy_licenses_ref().child(copy_license_key(token)).update({
        "status": "disabled",
        "disabled_at": now_iso(),
    })
    return True



def reset_copy_license_devices(token: str) -> bool:
    token = normalize_copy_license_token(token)
    if not token:
        return False
    record = get_copy_license_record(token)
    if not record or record.get("source") == "env":
        return False
    copy_licenses_ref().child(copy_license_key(token)).update({
        "devices": {},
        "telegram_user_id": None,
        "telegram_linked_at": None,
        "last_seen_at": None,
        "devices_reset_at": now_iso(),
        "telegram_unlinked_at": now_iso(),
    })
    try:
        prefix = f"{token}:"
        for key in list(_copy_license_touch_cache.keys()):
            if str(key).startswith(prefix):
                _copy_license_touch_cache.pop(key, None)
    except Exception:
        pass
    return True




def delete_copy_license(token: str) -> bool:
    token = normalize_copy_license_token(token)
    if not token:
        return False
    record = get_copy_license_record(token)
    if not record or record.get("source") == "env":
        return False
    try:
        copy_licenses_ref().child(copy_license_key(token)).delete()
        try:
            prefix = f"{token}:"
            for key in list(_copy_license_touch_cache.keys()):
                if str(key).startswith(prefix):
                    _copy_license_touch_cache.pop(key, None)
        except Exception:
            pass
        return True
    except Exception as e:
        logger.warning("Could not delete copy license %s: %s", token, e)
        return False


def reset_all_copy_license_devices() -> dict:
    """Reset device and Telegram bindings for every Firebase Copy license. Env licenses are not affected."""
    result = {"reset": 0, "skipped": 0, "errors": 0}
    try:
        data = copy_licenses_ref().get() or {}
        if not isinstance(data, dict):
            return result
        now_value = now_iso()
        for key, rec in data.items():
            if not isinstance(rec, dict):
                result["skipped"] += 1
                continue
            try:
                token = normalize_copy_license_token(rec.get("token") or key)
                copy_licenses_ref().child(str(key)).update({
                    "devices": {},
                    "telegram_user_id": None,
                    "telegram_linked_at": None,
                    "last_seen_at": None,
                    "devices_reset_at": now_value,
                    "telegram_unlinked_at": now_value,
                })
                if token:
                    prefix = f"{token}:"
                    for cache_key in list(_copy_license_touch_cache.keys()):
                        if str(cache_key).startswith(prefix):
                            _copy_license_touch_cache.pop(cache_key, None)
                result["reset"] += 1
            except Exception:
                result["errors"] += 1
    except Exception as e:
        logger.warning("Could not reset all copy license devices: %s", e)
        result["errors"] += 1
    return result


def cleanup_copy_licenses(delete_disabled: bool = True, delete_expired: bool = True) -> dict:
    """Delete useless Firebase licenses: disabled and/or expired. Active valid licenses are kept."""
    result = {"deleted": 0, "disabled": 0, "expired": 0, "kept": 0, "errors": 0}
    try:
        data = copy_licenses_ref().get() or {}
        if not isinstance(data, dict):
            return result
        for key, rec in data.items():
            if not isinstance(rec, dict):
                result["kept"] += 1
                continue
            status = str(rec.get("status") or "").lower()
            is_disabled = status == "disabled"
            is_expired = status == "expired" or copy_license_is_expired(rec.get("expires_at"))
            should_delete = (delete_disabled and is_disabled) or (delete_expired and is_expired)
            if not should_delete:
                result["kept"] += 1
                continue
            try:
                copy_licenses_ref().child(str(key)).delete()
                result["deleted"] += 1
                if is_disabled:
                    result["disabled"] += 1
                if is_expired:
                    result["expired"] += 1
            except Exception:
                result["errors"] += 1
    except Exception as e:
        logger.warning("Could not cleanup copy licenses: %s", e)
        result["errors"] += 1
    return result


def build_copy_cleanup_result_message(result: dict) -> str:
    return (
        "🧹 نتيجة تنظيف أكواد Copy\n"
        "━━━━━━━━━━━━━━\n"
        f"🗑 المحذوفة: {int(result.get('deleted', 0) or 0)}\n"
        f"⛔ منها معطلة: {int(result.get('disabled', 0) or 0)}\n"
        f"⏳ منها منتهية: {int(result.get('expired', 0) or 0)}\n"
        f"✅ بقيت كما هي: {int(result.get('kept', 0) or 0)}\n"
        f"⚠️ أخطاء: {int(result.get('errors', 0) or 0)}"
    )


def build_copy_reset_all_result_message(result: dict) -> str:
    return (
        "♻️ نتيجة تصفير أجهزة أكواد Copy\n"
        "━━━━━━━━━━━━━━\n"
        f"✅ تم تصفير: {int(result.get('reset', 0) or 0)} كود\n"
        f"⏭ تم تخطي: {int(result.get('skipped', 0) or 0)}\n"
        f"⚠️ أخطاء: {int(result.get('errors', 0) or 0)}\n\n"
        "بعدها أول جهاز و Telegram ID يستخدم الكود سيرتبط من جديد."
    )

def list_copy_licenses(limit: int = 20) -> list[dict]:
    items = []
    try:
        data = copy_licenses_ref().get() or {}
        if isinstance(data, dict):
            for _key, rec in data.items():
                if isinstance(rec, dict):
                    rec = dict(rec)
                    rec["token"] = normalize_copy_license_token(rec.get("token") or _key)
                    rec["source"] = rec.get("source") or "firebase"
                    items.append(rec)
    except Exception as e:
        logger.warning("Could not list copy licenses from Firebase: %s", e)

    for token, rec in copy_license_env_records().items():
        items.append(dict(rec))

    def sort_key(rec):
        return str(rec.get("created_at") or rec.get("token") or "")

    items.sort(key=sort_key, reverse=True)
    return items[: int(limit or 20)]


def format_copy_license_expiry(expires_at) -> str:
    if not expires_at or str(expires_at).lower() == "forever":
        return "دائم"
    try:
        return format_dt_ar(str(expires_at))
    except Exception:
        return str(expires_at)


def build_copy_license_message(record: dict) -> str:
    token = record.get("token") or "-"
    plan = record.get("plan") or "-"
    expires = format_copy_license_expiry(record.get("expires_at"))
    max_devices = record.get("max_devices", 1)
    return (
        "🔑 كود تفعيل Copy Trading جاهز\n\n"
        f"<code>{html.escape(str(token))}</code>\n\n"
        f"📦 النوع: {html.escape(str(plan))}\n"
        f"⏳ الصلاحية: {html.escape(str(expires))}\n"
        f"📱 عدد الأجهزة: {html.escape(str(max_devices))}\n"
        "👤 الربط: أول Telegram ID يوضع داخل الإضافة سيثبت على هذا الكود\n\n"
        "انسخ الكود وضعه داخل إضافة TRADING TIME COPY مع Telegram ID الخاص بالمستخدم ثم اضغط حفظ وربط."
    )


def build_copy_licenses_list_message(limit: int = 20) -> str:
    items = list_copy_licenses(limit)
    if not items:
        return "📭 لا يوجد أكواد Copy محفوظة."
    lines = [f"📋 آخر {len(items)} كود Copy", "━━━━━━━━━━━━━━"]
    for rec in items:
        token = rec.get("token") or "-"
        status = rec.get("status") or "unknown"
        plan = rec.get("plan") or "-"
        expires = format_copy_license_expiry(rec.get("expires_at"))
        devices = rec.get("devices") if isinstance(rec.get("devices"), dict) else {}
        max_devices = rec.get("max_devices", 1)
        source = rec.get("source") or "firebase"
        expired = " | منتهي" if copy_license_is_expired(rec.get("expires_at")) else ""
        lines.append(
            f"🔑 <code>{html.escape(str(token))}</code>\n"
            f"الحالة: {html.escape(str(status))}{expired}\n"
            f"النوع: {html.escape(str(plan))} | الصلاحية: {html.escape(str(expires))}\n"
            f"الأجهزة: {len(devices)}/{max_devices} | Telegram ID: {html.escape(str(rec.get('telegram_user_id') or '-'))}\n"
            f"المصدر: {html.escape(str(source))}\n"
            "──────────────"
        )
    return "\n".join(lines)[:3900]


def normalize_copy_telegram_user_id(value) -> str:
    try:
        text = str(value or "").strip()
        if text.startswith("@"):  # usernames are not stable enough for routing; numeric ID is required.
            return ""
        if text.lstrip("-").isdigit():
            return str(int(text))
        return ""
    except Exception:
        return ""


def copy_validate_license_for_device(token: str, device_id: str = "unknown", telegram_user_id=None, touch: bool = True) -> tuple[bool, str, dict | None]:
    token = normalize_copy_license_token(token)
    device_id = str(device_id or "unknown").strip()[:120] or "unknown"
    telegram_user_id = normalize_copy_telegram_user_id(telegram_user_id)
    record = get_copy_license_record(token)
    if not record:
        return False, "invalid license", None
    if str(record.get("status") or "").lower() != "active":
        return False, "inactive license", record
    if copy_license_is_expired(record.get("expires_at")):
        # Mark Firebase records as expired for easier admin visibility.
        try:
            if record.get("source") != "env":
                copy_licenses_ref().child(copy_license_key(token)).update({"status": "expired", "expired_at": now_iso()})
        except Exception:
            pass
        return False, "expired license", record

    if record.get("source") == "env":
        if telegram_user_id:
            try:
                record["telegram_user_id"] = telegram_user_id
            except Exception:
                pass
        return True, "ok", record

    existing_telegram_id = normalize_copy_telegram_user_id(record.get("telegram_user_id") or record.get("owner_telegram_id"))
    if existing_telegram_id and telegram_user_id and existing_telegram_id != telegram_user_id:
        return False, "license linked to another Telegram ID", record

    devices = record.get("devices") if isinstance(record.get("devices"), dict) else {}
    max_devices = max(1, int(record.get("max_devices") or COPY_LICENSE_DEFAULT_MAX_DEVICES or 1))
    device_key = safe_key(device_id)

    if device_key not in devices and len(devices) >= max_devices:
        return False, "device limit reached", record

    try:
        updates = {}
        now_ts = time_module.time()
        now_value = now_iso()
        touch_key = f"{token}:{device_key}"
        last_touch = float(_copy_license_touch_cache.get(touch_key, 0) or 0)
        if telegram_user_id and not existing_telegram_id:
            updates["telegram_user_id"] = telegram_user_id
            updates["telegram_linked_at"] = now_value
        if device_key not in devices:
            updates["last_seen_at"] = now_value
            updates[f"devices/{device_key}"] = {
                "device_id": device_id,
                "bound_at": now_value,
                "last_seen_at": now_value,
            }
            _copy_license_touch_cache[touch_key] = now_ts
        elif touch and now_ts - last_touch >= int(COPY_LICENSE_DEVICE_TOUCH_SECONDS):
            updates["last_seen_at"] = now_value
            updates[f"devices/{device_key}/last_seen_at"] = now_value
            _copy_license_touch_cache[touch_key] = now_ts
        if updates:
            copy_licenses_ref().child(copy_license_key(token)).update(updates)
    except Exception as e:
        logger.warning("Could not update copy license device binding: %s", e)

    return True, "ok", record


def build_copy_status_message() -> str:
    licenses = list_copy_licenses(1000)
    active = 0
    expired = 0
    disabled = 0
    total_devices = 0
    linked_telegram = 0
    for rec in licenses:
        status = str(rec.get("status") or "").lower()
        devices = rec.get("devices") if isinstance(rec.get("devices"), dict) else {}
        total_devices += len(devices)
        if normalize_copy_telegram_user_id(rec.get("telegram_user_id")):
            linked_telegram += 1
        if status == "disabled":
            disabled += 1
        elif copy_license_is_expired(rec.get("expires_at")) or status == "expired":
            expired += 1
        elif status == "active":
            active += 1

    settings = get_copy_settings()
    enabled = bool(settings.get("global_enabled", True))
    update_notice = str(settings.get("update_notice") or "").strip()
    latest_version = settings.get("latest_version") or "v0.42"

    lines = [
        "📡 حالة Copy Trading",
        "━━━━━━━━━━━━━━",
        f"الحالة العامة: {'🟢 شغال' if enabled else '🔴 موقوف للجميع'}",
        f"آخر نسخة: {html.escape(str(latest_version))}",
        "",
        f"🔑 الأكواد النشطة: {active}",
        f"⏳ الأكواد المنتهية: {expired}",
        f"⛔ الأكواد المعطلة: {disabled}",
        f"📱 الأجهزة المربوطة: {total_devices}",
        f"👤 الأكواد المربوطة بـ Telegram ID: {linked_telegram}",
        "",
        "ملاحظة: صفقات كل مستخدم تصل فقط للإضافة المربوطة بنفس Telegram ID. يمكنك تصفير الأجهزة بعد تحديث الإضافة إذا تغيّر جهاز المستخدم.",
    ]
    if update_notice:
        lines.extend(["", "📌 رسالة التحديث:", html.escape(update_notice)])
    return "\n".join(lines)[:3900]

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
    """Send Telegram message safely.
    Telegram can occasionally timeout without the bot being broken.
    We retry once, then log a warning without crashing job callbacks.
    """
    last_error = None
    for attempt in range(2):
        try:
            return await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
            )
        except Exception as e:
            last_error = e
            err_name = e.__class__.__name__
            err_text = str(e)
            if err_name == "TimedOut" or "Timed out" in err_text:
                logger.warning(
                    "Telegram send_message timeout | chat_id=%s | attempt=%s/2",
                    chat_id,
                    attempt + 1,
                )
                if attempt == 0:
                    try:
                        await asyncio.sleep(1.5)
                    except Exception:
                        pass
                    continue
                return None

            logger.exception("Telegram send_message failed | chat_id=%s | error=%s", chat_id, e)
            return None

    if last_error:
        logger.warning("Telegram send_message skipped after retries | chat_id=%s | error=%s", chat_id, last_error)
    return None

# ===== TRADING TIME COPY helpers =====
def _copy_parse_iso(value: str | None):
    try:
        if not value:
            return None
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return None


def _copy_timeframe(signal: dict) -> str:
    try:
        tf = signal.get("timeframe") or signal.get("duration_minutes") or signal.get("timeframe_minutes") or 1
        text = str(tf).strip().upper()
        if text.isdigit():
            return f"M{text}"
        if text.startswith("M") and text[1:].isdigit():
            return text
        return text or "M1"
    except Exception:
        return "M1"


def _copy_duration_seconds(signal: dict) -> int:
    try:
        if signal.get("duration_seconds"):
            return max(5, int(float(signal.get("duration_seconds"))))
        if signal.get("duration_minutes"):
            return max(5, int(float(signal.get("duration_minutes")) * 60))
        if signal.get("timeframe_minutes"):
            return max(5, int(float(signal.get("timeframe_minutes")) * 60))
        tf = _copy_timeframe(signal)
        if tf.startswith("M") and tf[1:].isdigit():
            return max(5, int(tf[1:]) * 60)
    except Exception:
        pass
    return 60



def normalize_copy_source(source: str | None) -> str:
    """Normalize all bot sections to canonical extension source keys."""
    raw = str(source or "bot").strip().lower()
    compact = raw.replace("-", "_").replace(" ", "_")

    if any(x in compact for x in ["three_candle", "3_candle", "threecandle"]) or ("3" in compact and "candle" in compact):
        return "three_candle"
    if any(x in compact for x in ["trading_room", "session_room", "room_session"]):
        return "trading_room"
    if any(x in compact for x in ["timed_list", "otc_timed", "schedule", "scheduled_list", "list"]):
        return "timed_list"
    if ("otc" in compact and "live" in compact) or "live_now" in compact or "direct" in compact:
        return "otc_live"
    if any(x in compact for x in ["real_market", "global_market", "global", "real"]):
        return "real_market"
    return raw or "bot"

def _copy_display_pair(signal: dict) -> str:
    try:
        pair = str(signal.get("pair_display") or signal.get("pair") or signal.get("symbol") or "").strip()
        # إذا كانت صفقة OTC وكان اسم الزوج لا يحتوي OTC، أضفها للعرض حتى تعرف الإضافة أنه OTC.
        src = str(signal.get("source") or "").lower()
        symbol = str(signal.get("symbol") or signal.get("platform_symbol") or "")
        if pair and "otc" not in pair.lower() and ("otc" in src or symbol.lower().endswith("_otc")):
            if "/" in pair:
                return f"{pair} (OTC)"
        return pair
    except Exception:
        return str(signal.get("pair") or "")


def is_copy_signal_creator_allowed(creator_user_id=None) -> bool:
    """يسمح ببث Copy Trading فقط من الأدمن/الأرقام المسموحة عند وجود مستخدم مولّد للصفقة."""
    try:
        if COPY_USER_SIGNAL_ROUTING_ENABLED:
            return True
        if not COPY_USER_SIGNALS_ADMIN_ONLY:
            return True
        if creator_user_id is None:
            # مصادر النظام/القنوات لا تحمل user_id، وتبقى مسموحة.
            return True
        uid = int(creator_user_id)
        return uid in set(COPY_SIGNAL_ALLOWED_TELEGRAM_IDS or {int(ADMIN_TELEGRAM_ID)})
    except Exception:
        return False


def copy_signal_guard_skip_payload(creator_user_id=None) -> dict:
    return {
        "ok": False,
        "skipped": True,
        "reason": "creator_user_not_allowed_for_copy",
        "creator_user_id": creator_user_id,
    }


def build_copy_trading_payload(signal: dict, source: str = "bot") -> dict:
    """يبني Payload موحد لإرساله إلى TRADING TIME COPY SERVER."""
    source = normalize_copy_source(source)
    signal = dict(signal or {})
    entry_dt = _copy_parse_iso(signal.get("entry_time")) or now_utc()
    expires_dt = _copy_parse_iso(signal.get("expires_at")) or (entry_dt + timedelta(seconds=int(COPY_SIGNAL_VALIDITY_SECONDS)))

    pair_display = _copy_display_pair({**signal, "source": source})
    platform_symbol = signal.get("platform_symbol") or signal.get("symbol") or signal.get("pair") or pair_display or ""
    direction = str(signal.get("direction") or "").strip().upper()

    payload = {
        "source": source,
        "pair": pair_display,
        "pair_display": pair_display,
        "platform_symbol": platform_symbol,
        "direction": direction,
        "timeframe": _copy_timeframe(signal),
        "duration_seconds": _copy_duration_seconds(signal),
        "entry_time": entry_dt.isoformat(),
        "expires_at": expires_dt.isoformat(),
        "created_at": now_iso(),
        "quality": signal.get("quality"),
        "confidence": signal.get("confidence"),
        "entry_price": signal.get("entry_price"),
        "payout": signal.get("payout"),
        "note": str(signal.get("note") or "")[:500],
        "creator_user_id": normalize_copy_telegram_user_id(signal.get("creator_user_id") or signal.get("user_id")),
        "target_user_id": normalize_copy_telegram_user_id(signal.get("target_user_id") or signal.get("telegram_user_id")),
        "batch_id": signal.get("batch_id") or signal.get("list_batch_id") or signal.get("timed_list_batch_id"),
        "timed_list_batch_id": signal.get("timed_list_batch_id") or signal.get("list_batch_id") or signal.get("batch_id"),
        "list_index": signal.get("list_index"),
        "list_total": signal.get("list_total"),
    }

    base = "|".join([
        str(payload.get("source")),
        str(payload.get("pair")),
        str(payload.get("platform_symbol")),
        str(payload.get("direction")),
        str(payload.get("entry_time")),
    ])
    payload["id"] = str(signal.get("id") or hashlib.sha256(base.encode("utf-8")).hexdigest()[:18])
    return payload


def send_copy_trading_signal_sync(signal: dict, source: str = "bot") -> dict:
    """يرسل الإشارة إلى Copy Server بدون كسر البوت إذا فشل الاتصال."""
    if not COPY_TRADING_ENABLED:
        return {"ok": False, "skipped": True, "reason": "COPY_TRADING_ENABLED=false"}

    try:
        payload = build_copy_trading_payload(signal, source=source)
        if not payload.get("pair") or not payload.get("direction"):
            return {"ok": False, "skipped": True, "reason": "missing pair/direction", "payload": payload}
        res = requests.post(
            f"{COPY_SERVER_URL}/api/bot/signal",
            json=payload,
            headers={"X-TTCOPY-SECRET": COPY_SERVER_SECRET},
            timeout=int(COPY_REQUEST_TIMEOUT_SECONDS),
        )
        if res.status_code != 200:
            logger.warning("Copy Trading signal rejected | status=%s | text=%s", res.status_code, res.text[:300])
            return {"ok": False, "status_code": res.status_code, "text": res.text[:300]}
        return res.json()
    except Exception as e:
        logger.warning("Copy Trading send failed: %s", e)
        return {"ok": False, "error": str(e)}


async def publish_copy_trading_signal(signal: dict, source: str = "bot") -> dict:
    try:
        return await asyncio.to_thread(send_copy_trading_signal_sync, signal, source)
    except Exception as e:
        logger.warning("Copy Trading async publish failed: %s", e)
        return {"ok": False, "error": str(e)}


async def maybe_publish_copy_signal(result: dict, source: str, enabled: bool = True, creator_user_id=None) -> dict:
    """يرسل فقط الإشارات المباشرة الناجحة إلى TRADING TIME COPY."""
    try:
        if not enabled:
            return {"ok": False, "skipped": True, "reason": "source disabled"}
        if not is_copy_signal_creator_allowed(creator_user_id):
            logger.info("Copy Trading blocked user-generated signal | user_id=%s | source=%s", creator_user_id, source)
            return copy_signal_guard_skip_payload(creator_user_id)
        if not result or not result.get("ok"):
            return {"ok": False, "skipped": True, "reason": "result not ok"}
        if COPY_USER_SIGNAL_ROUTING_ENABLED and creator_user_id is not None:
            result = dict(result)
            result["creator_user_id"] = int(creator_user_id)
            result["target_user_id"] = int(creator_user_id)
        copy_result = await publish_copy_trading_signal(result, source=source)
        if copy_result.get("ok"):
            logger.info("Copy Trading signal sent | source=%s | delivery=%s", source, copy_result.get("delivery"))
        else:
            logger.info("Copy Trading signal skipped/failed | source=%s | result=%s", source, copy_result)
        return copy_result
    except Exception as e:
        logger.warning("Copy Trading hook error | source=%s | error=%s", source, e)
        return {"ok": False, "error": str(e)}


def _copy_direction_from_signal_line(line: str) -> str:
    try:
        raw = str(line or "").upper()
        if "CALL" in raw or "صاعد" in raw or "BUY" in raw:
            return "CALL"
        if "PUT" in raw or "هابط" in raw or "SELL" in raw:
            return "PUT"
    except Exception:
        pass
    return ""


def _copy_pair_from_signal_line(line: str, fallback_pair: str) -> str:
    try:
        parts = str(line or "").split(" — ")
        if parts and str(parts[0]).strip():
            return str(parts[0]).strip()
    except Exception:
        pass
    return str(fallback_pair or "").strip()


async def publish_copy_timed_list_signals(pair: str, signals: list[str], interval_minutes: int, start_dt: datetime, source: str = "timed_list", creator_user_id=None) -> dict:
    """Send each timed-list item to the extension with its real entry time.

    مهم: الإضافة في v0.22 تدعم Queue، لذلك ترسل الليستة كاملة الآن،
    وكل صفقة تنتظر وقتها داخل الإضافة.
    """
    if not COPY_SEND_TIMED_LISTS:
        return {"ok": False, "skipped": True, "reason": "COPY_SEND_TIMED_LISTS=false"}
    if not is_copy_signal_creator_allowed(creator_user_id):
        logger.info("Copy Trading blocked timed-list user signal | user_id=%s | pair=%s", creator_user_id, pair)
        return copy_signal_guard_skip_payload(creator_user_id)
    if not signals:
        return {"ok": False, "skipped": True, "reason": "empty list"}

    list_batch_id = f"timed_batch_{safe_key(pair)}_{int(now_utc().timestamp())}_{safe_key(str(creator_user_id or 'broadcast'))}_{random.randint(1000, 9999)}"
    sent = 0
    failed = 0
    details = []
    for index, line in enumerate(list(signals)):
        try:
            entry_dt = start_dt + timedelta(minutes=int(index) * int(interval_minutes or 1))
            direction = _copy_direction_from_signal_line(line) or get_stable_direction(pair, entry_dt)
            pair_name = _copy_pair_from_signal_line(line, pair)
            if not pair_name or not direction:
                failed += 1
                continue
            payload = {
                "ok": True,
                "id": f"timed_{safe_key(pair_name)}_{int(entry_dt.timestamp())}_{direction}_{index}",
                "pair": pair_name,
                "pair_display": pair_name,
                "direction": direction,
                "timeframe": "M1",
                "duration_seconds": 60,
                "duration_minutes": 1,
                "entry_time": entry_dt.isoformat(),
                "expires_at": (entry_dt + timedelta(seconds=max(30, int(COPY_SIGNAL_VALIDITY_SECONDS)))).isoformat(),
                "note": f"timed_list {index + 1}/{len(signals)} | interval={interval_minutes}m",
                "batch_id": list_batch_id,
                "timed_list_batch_id": list_batch_id,
                "list_index": index + 1,
                "list_total": len(signals),
                "creator_user_id": int(creator_user_id) if creator_user_id is not None else None,
                "target_user_id": int(creator_user_id) if (COPY_USER_SIGNAL_ROUTING_ENABLED and creator_user_id is not None) else None,
            }
            result = await publish_copy_trading_signal(payload, source=source)
            details.append(result)
            if result.get("ok"):
                sent += 1
            else:
                failed += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            logger.warning("Timed-list Copy signal failed | pair=%s | index=%s | error=%s", pair, index, e)
    logger.info("Copy Trading timed list sent | pair=%s | sent=%s | failed=%s", pair, sent, failed)
    return {"ok": sent > 0, "sent": sent, "failed": failed, "details": details[-3:]}


async def publish_copy_three_candle_signal(trade: dict) -> dict:
    """Send the 3-candle channel signal to the extension as source=three_candle."""
    if not COPY_SEND_THREE_CANDLE:
        return {"ok": False, "skipped": True, "reason": "COPY_SEND_THREE_CANDLE=false"}
    try:
        entry_bucket = int(float((trade or {}).get("entry_bucket") or 0))
        if entry_bucket <= 0:
            return {"ok": False, "skipped": True, "reason": "missing entry_bucket"}
        entry_dt = datetime.fromtimestamp(entry_bucket, tz=UTC)
        pair = str((trade or {}).get("pair") or "").strip()
        direction = str((trade or {}).get("direction") or "").strip().upper()
        if not pair or direction not in {"CALL", "PUT"}:
            return {"ok": False, "skipped": True, "reason": "missing pair/direction"}
        payload = {
            "ok": True,
            "id": f"three_{safe_key(pair)}_{entry_bucket}_{direction}",
            "pair": pair,
            "pair_display": pair,
            "symbol": (trade or {}).get("symbol"),
            "platform_symbol": (trade or {}).get("symbol"),
            "direction": direction,
            "timeframe": "M1",
            "duration_seconds": 60,
            "duration_minutes": 1,
            "entry_time": entry_dt.isoformat(),
            "expires_at": (entry_dt + timedelta(seconds=max(30, int(COPY_SIGNAL_VALIDITY_SECONDS)))).isoformat(),
            "quality": int(round(float((trade or {}).get("body_score", 0) or 0) * 100)) if (trade or {}).get("body_score") is not None else None,
            "payout": (trade or {}).get("payout"),
            "note": "three_candle_channel",
        }
        result = await publish_copy_trading_signal(payload, source="three_candle")
        logger.info("Copy Trading three-candle sent | pair=%s | result=%s", pair, result)
        return result
    except Exception as e:
        logger.warning("Three-candle Copy signal failed: %s", e)
        return {"ok": False, "error": str(e)}


async def publish_copy_trading_room_signal(trade: dict, state: dict | None = None, creator_user_id=None) -> dict:
    """Send Trading Room entries to the extension as source=trading_room."""
    if not COPY_SEND_TRADING_ROOM:
        return {"ok": False, "skipped": True, "reason": "COPY_SEND_TRADING_ROOM=false"}
    if not is_copy_signal_creator_allowed(creator_user_id):
        logger.info("Copy Trading blocked trading-room user signal | user_id=%s", creator_user_id)
        return copy_signal_guard_skip_payload(creator_user_id)
    try:
        entry_ts = float((trade or {}).get("entry_ts") or 0)
        if entry_ts <= 0:
            return {"ok": False, "skipped": True, "reason": "missing entry_ts"}
        entry_dt = datetime.fromtimestamp(entry_ts, tz=UTC)
        pair = str((trade or {}).get("pair") or (state or {}).get("pair") or "").strip()
        direction = str((trade or {}).get("direction") or "").strip().upper()
        if not pair or direction not in {"CALL", "PUT"}:
            return {"ok": False, "skipped": True, "reason": "missing pair/direction"}
        payload = {
            "ok": True,
            "id": f"room_{safe_key(pair)}_{int(entry_ts)}_{direction}_{safe_key((trade or {}).get('setup_kind'))}",
            "pair": pair,
            "pair_display": pair,
            "symbol": (trade or {}).get("symbol") or (state or {}).get("symbol"),
            "platform_symbol": (trade or {}).get("symbol") or (state or {}).get("symbol"),
            "direction": direction,
            "timeframe": "M1",
            "duration_seconds": 60,
            "duration_minutes": 1,
            "entry_time": entry_dt.isoformat(),
            "expires_at": (entry_dt + timedelta(seconds=max(30, int(COPY_SIGNAL_VALIDITY_SECONDS)))).isoformat(),
            "quality": (trade or {}).get("score"),
            "confidence": (trade or {}).get("score"),
            "entry_price": (trade or {}).get("price"),
            "payout": (trade or {}).get("payout"),
            "note": str((trade or {}).get("setup") or (trade or {}).get("setup_kind") or "trading_room")[:500],
            "creator_user_id": int(creator_user_id) if creator_user_id is not None else None,
            "target_user_id": int(creator_user_id) if (COPY_USER_SIGNAL_ROUTING_ENABLED and creator_user_id is not None) else None,
        }
        result = await publish_copy_trading_signal(payload, source="trading_room")
        logger.info("Copy Trading trading-room sent | pair=%s | result=%s", pair, result)
        return result
    except Exception as e:
        logger.warning("Trading Room Copy signal failed: %s", e)
        return {"ok": False, "error": str(e)}


def format_dt_ar(iso_value: str):
    dt = parse_iso(iso_value)
    if not dt:
        return "غير متوفر"
    return dt.astimezone(UTC_PLUS_3).strftime("%Y-%m-%d %H:%M")


def get_user_record(user_id: int):
    uid = int(user_id)
    cached = _cache_get(f"user_record:{uid}", FIREBASE_USER_CACHE_TTL_SECONDS)
    if cached is not None:
        return cached
    data = users_ref().child(str(uid)).get()
    return _cache_set(f"user_record:{uid}", data)



def save_user_record(user_id: int, data: dict):
    uid = int(user_id)
    data = dict(data or {})

    # لا نكتب last_seen على Firebase مع كل رسالة؛ هذا كان يسبب writes كثيرة بلا فائدة.
    critical_keys = {
        "status", "quotex_id", "expires_at", "trial_used", "trial_source",
        "approved", "pending", "role", "rejected_at", "revoked_at", "blocked_at",
        "updated_at",
    }
    has_critical_update = any(k in data for k in critical_keys)

    if not has_critical_update and set(data.keys()).issubset({"telegram_id", "name", "username", "last_seen"}):
        last_key = f"last_seen_write:{uid}"
        last_write = _cache_get(last_key, SAVE_USER_LAST_SEEN_THROTTLE_SECONDS)
        if last_write is not None:
            # حدّث الكاش فقط، لا تكتب Firebase.
            cached = _cache_get(f"user_record:{uid}", FIREBASE_USER_CACHE_TTL_SECONDS) or {}
            if isinstance(cached, dict):
                cached.update(data)
                _cache_set(f"user_record:{uid}", cached)
            return
        _cache_set(last_key, True)

    users_ref().child(str(uid)).update(data)

    cached = _cache_get(f"user_record:{uid}", FIREBASE_USER_CACHE_TTL_SECONDS) or {}
    if isinstance(cached, dict):
        cached.update(data)
        _cache_set(f"user_record:{uid}", cached)
    else:
        _cache_set(f"user_record:{uid}", data)

    clear_users_list_cache()



def save_pending_user(user_id: int, data: dict):
    uid = int(user_id)
    data = dict(data or {})

    # لا نحذف سجل التجربة ولا نمسح المستخدم بالكامل عند تقديم طلب جديد.
    # فقط نلغي أي تفعيل قديم ونحفظ الطلب المعلق.
    try:
        approved_ref().child(str(uid)).delete()
    except Exception:
        pass

    pending_ref().child(str(uid)).set(data)
    users_ref().child(str(uid)).update({
        "telegram_id": uid,
        "name": data.get("name", ""),
        "username": data.get("username", ""),
        "quotex_id": data.get("quotex_id", ""),
        "status": "pending",
        "pending": True,
        "updated_at": now_iso(),
    })

    clear_user_cache(uid)
    clear_pending_list_cache()
    clear_approved_list_cache()
    clear_users_list_cache()
    _cache_set(f"user_status:{uid}", "pending")
    _cache_set(f"approved:{uid}", False)



def remove_pending_user(user_id: int):
    uid = int(user_id)
    try:
        pending_ref().child(str(uid)).delete()
    except Exception:
        pass
    clear_pending_list_cache()
    clear_user_cache(uid)



def set_approved_user(user_id: int, data: dict):
    uid = int(user_id)
    data = dict(data or {})
    approved_ref().child(str(uid)).set(data)
    clear_user_cache(uid)
    clear_approved_list_cache()
    _cache_set(f"approved_data:{uid}", data)
    _cache_set(f"approved:{uid}", str(data.get("status", "approved")) == "approved")
    if str(data.get("status", "approved")) == "approved":
        _cache_set(f"user_status:{uid}", "approved")



def get_approved_user(user_id: int):
    uid = int(user_id)
    cached = _cache_get(f"approved_data:{uid}", FIREBASE_APPROVED_CACHE_TTL_SECONDS)
    if cached is not None:
        return cached
    try:
        data = approved_ref().child(str(uid)).get()
        return _cache_set(f"approved_data:{uid}", data)
    except Exception as e:
        logger.exception("Could not get approved user: %s", e)
        return None



def get_all_pending_users():
    cached = _cache_get("all_pending_users", FIREBASE_FULL_LIST_CACHE_TTL_SECONDS)
    if cached is not None:
        return cached
    data = pending_ref().get() or {}
    return _cache_set("all_pending_users", data)



def get_all_users():
    cached = _cache_get("all_users", FIREBASE_FULL_LIST_CACHE_TTL_SECONDS)
    if cached is not None:
        return cached
    data = users_ref().get() or {}
    return _cache_set("all_users", data)



def get_all_approved_users():
    cached = _cache_get("all_approved_users", FIREBASE_FULL_LIST_CACHE_TTL_SECONDS)
    if cached is not None:
        return cached
    data = approved_ref().get() or {}
    return _cache_set("all_approved_users", data)



def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_TELEGRAM_ID


def is_otc_list_manager(user_id: int) -> bool:
    try:
        return is_admin(int(user_id)) or int(user_id) in OTC_LIST_MANAGER_IDS
    except Exception:
        return False


def get_bot_enabled() -> bool:
    cached = _cache_get("system:bot_enabled", FIREBASE_BOT_SETTINGS_TTL_SECONDS)
    if cached is not None:
        return bool(cached)
    try:
        value = system_ref().child("bot_enabled").get()
        if value is None:
            value = True
        return bool(_cache_set("system:bot_enabled", bool(value)))
    except Exception as e:
        logger.exception("Could not read bot_enabled: %s", e)
        return True



def set_bot_enabled(value: bool):
    system_ref().update({
        "bot_enabled": bool(value),
        "updated_at": now_iso(),
    })
    _cache_set("system:bot_enabled", bool(value))


# ===== Maintenance waiters =====
def maintenance_waiters_ref():
    return system_ref().child("maintenance_waiters")


def remember_maintenance_waiter(user_id: int, lang: str = "ar", name: str = "", username: str = ""):
    """يحفظ فقط المستخدمين الذين حاولوا استخدام البوت أثناء الصيانة حتى نبلغهم عند عودة التشغيل."""
    try:
        uid = int(user_id)
        lang = "en" if str(lang).lower() == "en" else "ar"
        ref = maintenance_waiters_ref().child(str(uid))
        old_data = ref.get() or {}
        data = {
            "telegram_id": uid,
            "language": lang,
            "name": name or old_data.get("name", ""),
            "username": username or old_data.get("username", ""),
            "first_seen": old_data.get("first_seen") or now_iso(),
            "last_seen": now_iso(),
        }
        ref.set(data)
        return True
    except Exception as e:
        logger.warning("Could not remember maintenance waiter | user=%s | error=%s", user_id, e)
        return False


def get_maintenance_waiters() -> dict:
    try:
        data = maintenance_waiters_ref().get() or {}
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning("Could not read maintenance waiters: %s", e)
        return {}


def clear_maintenance_waiters():
    try:
        maintenance_waiters_ref().delete()
    except Exception as e:
        logger.warning("Could not clear maintenance waiters: %s", e)


def build_maintenance_finished_text(lang: str = "ar") -> str:
    if str(lang).lower() == "en":
        return (
            "✅ Maintenance is complete.\n\n"
            "TRADING TIME Bot is back online now.\n"
            "You can use the bot again."
        )
    return (
        "✅ انتهت التحديثات.\n\n"
        "بوت TRADING TIME عاد للعمل الآن.\n"
        "يمكنك استخدام البوت من جديد."
    )


async def notify_maintenance_waiters(context: ContextTypes.DEFAULT_TYPE) -> tuple[int, int]:
    """يرسل رسالة العودة فقط لمن حاول استخدام البوت أثناء الصيانة ثم يمسح القائمة."""
    waiters = get_maintenance_waiters()
    if not waiters:
        return 0, 0

    sent = 0
    failed = 0

    for uid_str, info in list(waiters.items()):
        try:
            uid = int(uid_str)
            lang = "en" if isinstance(info, dict) and str(info.get("language") or "").lower() == "en" else "ar"
            if is_approved(uid) or is_otc_list_manager(uid):
                markup = build_main_menu_for_user(uid, lang)
            else:
                markup = welcome_keyboard_en if lang == "en" else welcome_keyboard

            await safe_send_message(context.bot,
                chat_id=uid,
                text=build_maintenance_finished_text(lang),
                reply_markup=markup,
            )
            sent += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            logger.warning("Could not notify maintenance waiter %s: %s", uid_str, e)

    clear_maintenance_waiters()
    return sent, failed


def force_reject_pending_user(user_id: int):
    """رفض الطلب وتنظيف pending/cache فورًا حتى لا يبقى المستخدم عالقًا."""
    uid = int(user_id)

    try:
        pending_ref().child(str(uid)).delete()
    except Exception:
        pass

    try:
        users_ref().child(str(uid)).update({
            "status": "new",
            "pending": False,
            "rejected_at": now_iso(),
            "updated_at": now_iso(),
        })
    except Exception:
        pass

    clear_user_cache(uid)
    clear_pending_list_cache()
    clear_users_list_cache()
    _cache_set(f"user_status:{uid}", "new")
    _cache_set(f"approved:{uid}", False)
    _cache_set(f"approved_data:{uid}", None)
    return True



def force_revoke_user_access(user_id: int, status: str = "new"):
    """إلغاء تفعيل المستخدم وإرجاعه كأنه شخص جديد تمامًا."""
    uid = int(user_id)

    preserve_video_trial_used = False
    try:
        old_user_data = users_ref().child(str(uid)).get() or {}
        if isinstance(old_user_data, dict):
            old_trial = old_user_data.get("video_trial") or {}
            if isinstance(old_trial, dict) and old_trial.get("used"):
                preserve_video_trial_used = True
    except Exception:
        pass

    if preserve_video_trial_used or has_used_video_trial_permanent(uid):
        mark_video_trial_used_permanent(uid)

    try:
        approved_ref().child(str(uid)).delete()
    except Exception:
        pass

    try:
        force_reject_pending_user(uid)
    except Exception:
        pass

    try:
        users_ref().child(str(uid)).delete()
    except Exception:
        pass

    try:
        clear_user_cache(uid)
        _cache_set(f"approved:{uid}", False)
        _cache_set(f"user_status:{uid}", "new")
        _cache_set(f"approved_data:{uid}", None)
        _cache_set(f"video_trial:{uid}", has_used_video_trial_permanent(uid))
    except Exception:
        pass

    return True



async def send_revoked_welcome_keyboard(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    try:
        await safe_send_message(context.bot,
            chat_id=int(user_id),
            text=(
                "⛔ تم إلغاء تفعيل حسابك.\n\n"
                "إذا كنت تعتقد أن هذا حدث عن طريق الخطأ، تواصل مع الأدمن."
            ),
            reply_markup=welcome_keyboard
        )
    except Exception as e:
        logger.warning("Could not send revoked welcome keyboard | user=%s | error=%s", user_id, e)



def is_approved(user_id: int) -> bool:
    uid = int(user_id)
    cached = _cache_get(f"approved:{uid}", FIREBASE_APPROVED_CACHE_TTL_SECONDS)
    if cached is not None:
        return bool(cached)

    try:
        data = get_approved_user(uid)
        if not data:
            return _cache_set(f"approved:{uid}", False)

        status = data.get("status", "approved") if isinstance(data, dict) else "approved"
        if status != "approved":
            return _cache_set(f"approved:{uid}", False)

        expires_at = data.get("expires_at") if isinstance(data, dict) else None
        if expires_at and expires_at != "forever":
            try:
                exp = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=timezone.utc)
                if now_utc() > exp:
                    return _cache_set(f"approved:{uid}", False)
            except Exception:
                pass

        return _cache_set(f"approved:{uid}", True)
    except Exception as e:
        logger.exception("Could not check approved user: %s", e)
        return _cache_set(f"approved:{uid}", False)



def get_user_status(user_id: int) -> str:
    uid = int(user_id)
    cached = _cache_get(f"user_status:{uid}", FIREBASE_USER_CACHE_TTL_SECONDS)
    if cached is not None:
        return str(cached)

    try:
        if is_approved(uid):
            return _cache_set(f"user_status:{uid}", "approved")

        pending_cached = _cache_get(f"pending:{uid}", FIREBASE_PENDING_CACHE_TTL_SECONDS)
        if pending_cached is not None:
            return _cache_set(f"user_status:{uid}", "pending" if pending_cached else "new")

        pending = pending_ref().child(str(uid)).get()
        _cache_set(f"pending:{uid}", bool(pending))
        if pending:
            return _cache_set(f"user_status:{uid}", "pending")

        approved_data = get_approved_user(uid)
        if isinstance(approved_data, dict):
            status = str(approved_data.get("status") or "")
            if status:
                return _cache_set(f"user_status:{uid}", status)

        return _cache_set(f"user_status:{uid}", "new")
    except Exception as e:
        logger.exception("Could not get user status: %s", e)
        return _cache_set(f"user_status:{uid}", "new")



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
        "mode": mode,
        "plan": mode,
        "approved_at": now_iso(),
        "expires_at": expires_at,
    })

    set_approved_user(user_id, approved_data)
    save_user_record(user_id, {
        "status": "approved",
        "plan": mode,
        "expires_at": expires_at,
    })
    remove_pending_user(user_id)
    try:
        clear_user_cache(user_id)
    except Exception:
        pass


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
    cache_key = f"recent_active_approved:{int(minutes)}"
    cached = _cache_get(cache_key, 60)
    if cached is not None:
        return cached

    approved_users = get_all_approved_users()
    all_users = get_all_users()

    result = []
    cutoff = now_utc() - timedelta(minutes=minutes)

    for user_id_str, approved_data in (approved_users or {}).items():
        try:
            if isinstance(approved_data, dict):
                if approved_data.get("status", "approved") != "approved":
                    continue
                expires_at = approved_data.get("expires_at")
                if expires_at and expires_at != "forever":
                    exp = parse_iso(str(expires_at).replace("Z", "+00:00"))
                    if exp:
                        if exp.tzinfo is None:
                            exp = exp.replace(tzinfo=timezone.utc)
                        if now_utc() > exp:
                            continue
            user_data = all_users.get(user_id_str)
            if not user_data:
                continue
            last_seen = parse_iso(user_data.get("last_seen", ""))
            if last_seen:
                if last_seen.tzinfo is None:
                    last_seen = last_seen.replace(tzinfo=timezone.utc)
                if last_seen >= cutoff:
                    result.append((user_id_str, user_data))
        except Exception:
            continue

    result.sort(key=lambda x: x[1].get("last_seen", ""), reverse=True)
    return _cache_set(cache_key, result)



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

    def add_symbol(self, symbol: str):
        """إضافة رمز OTC جديد أثناء التشغيل والاشتراك به مباشرة إذا الاتصال مفتوح."""
        try:
            if not symbol:
                return

            with self.lock:
                if symbol in self.symbols:
                    return

                self.symbols.append(symbol)
                self.prices.setdefault(symbol, deque(maxlen=3000))
                self.candles.setdefault(symbol, {})

            logger.info("Dynamic OTC symbol added: %s", symbol)

            if self.connected:
                try:
                    self._send_event("instruments/follow", symbol)
                    logger.info("Dynamic OTC symbol followed: %s", symbol)
                except Exception as e:
                    logger.warning("Could not follow dynamic OTC symbol %s: %s", symbol, e)

        except Exception as e:
            logger.exception("Could not add dynamic OTC symbol: %s", e)

    def get_dynamic_otc_pairs(self, min_payout: int | None = None) -> dict:
        """يرجع خريطة name -> symbol للأزواج OTC المتاحة من instruments/list."""
        min_payout = int(min_payout if min_payout is not None else OTC_LIVE_DYNAMIC_MIN_PAYOUT)

        result = {}
        try:
            with self.lock:
                instruments = dict(self.instruments or {})

            for symbol, info in instruments.items():
                if not isinstance(info, dict):
                    continue

                name = str(info.get("name") or "").strip()
                payout = int(info.get("payout") or 0)
                is_otc = bool(info.get("is_otc", False))

                if not symbol or not symbol.endswith("_otc"):
                    continue
                if not is_otc:
                    continue
                if payout < min_payout:
                    continue
                normalized_name = normalize_otc_currency_pair_name(name, symbol)
                if not normalized_name:
                    continue

                # فلتر أزواج العملات فقط، ونستبعد المعادن/الأسهم/الكريبتو.
                if not is_valid_otc_currency_pair_name(normalized_name):
                    continue

                result[normalized_name] = symbol

            return result

        except Exception as e:
            logger.exception("Could not build dynamic OTC pairs: %s", e)
            return result


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


for _allowed_pair in OTC_CURRENCIES_ALLOWED_PAIRS:
    try:
        for _symbol_candidate in possible_symbols_for_currency_pair(_allowed_pair):
            if _symbol_candidate not in OTC_ALL_POSSIBLE_QUOTEX_SYMBOLS:
                OTC_ALL_POSSIBLE_QUOTEX_SYMBOLS.append(_symbol_candidate)
    except Exception:
        pass

quotex_otc_feed = QuotexOTCLiveFeed(OTC_ALL_POSSIBLE_QUOTEX_SYMBOLS)



FIAT_CURRENCY_CODES = {
    # Majors
    "USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD",
    # Platform/common OTC fiat currencies
    "BRL", "ARS", "BDT", "NGN", "PKR", "DZD", "EGP", "IDR", "MXN", "PHP",
    "INR", "ZAR", "COP", "CAD", "CHF",
    # Extra ISO fiat currencies for safety
    "AED", "AFN", "ALL", "AMD", "ANG", "AOA", "AWG", "AZN",
    "BAM", "BBD", "BGN", "BHD", "BIF", "BMD", "BND", "BOB", "BSD", "BTN", "BWP", "BYN", "BZD",
    "CDF", "CLP", "CNY", "CRC", "CUP", "CVE", "CZK",
    "DJF", "DKK", "DOP",
    "ERN", "ETB",
    "FJD", "FKP",
    "GEL", "GHS", "GIP", "GMD", "GNF", "GTQ", "GYD",
    "HKD", "HNL", "HTG", "HUF",
    "ILS", "IQD", "IRR", "ISK",
    "JMD", "JOD",
    "KES", "KGS", "KHR", "KMF", "KRW", "KWD", "KYD", "KZT",
    "LAK", "LBP", "LKR", "LRD", "LSL", "LYD",
    "MAD", "MDL", "MGA", "MKD", "MMK", "MNT", "MOP", "MRU", "MUR", "MVR", "MWK", "MYR", "MZN",
    "NAD", "NIO", "NOK", "NPR",
    "OMR",
    "PAB", "PEN", "PGK", "PLN", "PYG",
    "QAR",
    "RON", "RSD", "RUB", "RWF",
    "SAR", "SBD", "SCR", "SDG", "SEK", "SGD", "SHP", "SLE", "SOS", "SRD", "SSP", "STN", "SYP", "SZL",
    "THB", "TJS", "TMT", "TND", "TOP", "TRY", "TTD", "TWD", "TZS",
    "UAH", "UGX", "UYU", "UZS",
    "VES", "VND", "VUV",
    "WST",
    "XAF", "XCD", "XOF", "XPF",
    "YER",
    "ZMW", "ZWL",
}



def normalize_pair_name_basic(pair: str) -> str:
    raw = str(pair or "").strip().upper().replace("  ", " ")
    raw = raw.replace(" OTC", " (OTC)")
    if "(OTC)" not in raw and "/" in raw:
        raw = raw + " (OTC)"
    return raw


def is_allowed_otc_currency_pair(pair: str) -> bool:
    return normalize_pair_name_basic(pair) in OTC_CURRENCIES_ALLOWED_PAIRS


def possible_symbols_for_currency_pair(pair: str) -> list[str]:
    """رموز محتملة للزوج داخل Quotex، لأن بعض أزواج USD تأتي معكوسة مثل BRLUSD_otc."""
    pair = normalize_pair_name_basic(pair).replace(" (OTC)", "")
    if "/" not in pair:
        return []

    base, quote = pair.split("/", 1)
    symbols = []

    # الشكل الطبيعي
    symbols.append(f"{base}{quote}_otc")

    # الشكل المعكوس، مهم جدًا لأزواج مثل USD/BRL التي تظهر داخليًا BRLUSD_otc
    symbols.append(f"{quote}{base}_otc")

    # إزالة التكرار
    return list(dict.fromkeys(symbols))


def allowed_otc_currency_fallback_map() -> dict:
    """Fallback مؤقت عند بداية التشغيل قبل وصول instruments/list."""
    result = {}
    for pair in OTC_CURRENCIES_ALLOWED_PAIRS:
        # نعطي الأولوية للماب اليدوي إذا موجود، وإلا أول رمز محتمل.
        symbol = OTC_PAIR_TO_QUOTEX_SYMBOL.get(pair)
        if not symbol:
            symbols = possible_symbols_for_currency_pair(pair)
            symbol = symbols[0] if symbols else None
        if symbol:
            result[pair] = symbol
    return result


def is_valid_otc_currency_pair_name(name: str) -> bool:
    """نقبل فقط أزواج قسم Currencies التي حددناها من واجهة Quotex."""
    try:
        return is_allowed_otc_currency_pair(name)
    except Exception:
        return False

        base = quote = None

        m = re.fullmatch(r"([A-Z]{3})/([A-Z]{3})( \(OTC\))?", value)
        if m:
            base, quote = m.group(1), m.group(2)

        if base is None:
            m = re.fullmatch(r"([A-Z]{3})([A-Z]{3})_OTC", value)
            if m:
                base, quote = m.group(1), m.group(2)

        if not base or not quote:
            return False

        return base in FIAT_CURRENCY_CODES and quote in FIAT_CURRENCY_CODES

    except Exception:
        return False


def normalize_otc_currency_pair_name(name: str, symbol: str | None = None) -> str | None:
    """توحيد اسم الزوج بشرط أن يكون من Currencies المسموحة."""
    try:
        raw = normalize_pair_name_basic(name)
        if raw in OTC_CURRENCIES_ALLOWED_PAIRS:
            return raw

        sym = str(symbol or "").strip().upper().replace("_OTC", "")
        if re.fullmatch(r"[A-Z]{6}", sym):
            a, b = sym[:3], sym[3:]

            candidates = [
                f"{a}/{b} (OTC)",
                f"{b}/{a} (OTC)",
            ]

            for candidate in candidates:
                if candidate in OTC_CURRENCIES_ALLOWED_PAIRS:
                    return candidate

        return None
    except Exception:
        return None

        m = re.fullmatch(r"([A-Z]{3})/([A-Z]{3})( \\(OTC\\))?", raw)
        if m:
            base, quote = m.group(1), m.group(2)
            candidate = f"{base}/{quote} (OTC)"
            return candidate if is_valid_otc_currency_pair_name(candidate) else None

        sym = str(symbol or "").strip().upper().replace("_OTC", "")
        if re.fullmatch(r"[A-Z]{6}", sym):
            a = sym[:3]
            b = sym[3:]

            # إذا كان أحد الطرفين USD، نعرض USD أولًا لتطابق أسماء المنصة مثل USD/BRL.
            if b == "USD":
                candidate = f"{b}/{a} (OTC)"
            else:
                candidate = f"{a}/{b} (OTC)"

            return candidate if is_valid_otc_currency_pair_name(candidate) else None

        return None
    except Exception:
        return None



def get_otc_analysis_pair_map() -> dict:
    """أزواج OTC Live للتحليل.
    يعتمد على:
    1) قائمة Currencies المسموحة من الواجهة.
    2) توفر الزوج الآن في instruments/list كـ OTC وبـ payout مناسب.
    3) fallback مؤقت للقائمة المسموحة عند بداية التشغيل إذا instruments/list لم يصل بعد.
    """
    live_map = {}

    try:
        if OTC_LIVE_DYNAMIC_PAIRS_ENABLED:
            with quotex_otc_feed.lock:
                instruments = dict(quotex_otc_feed.instruments or {})

            for symbol, info in instruments.items():
                if not isinstance(info, dict):
                    continue

                name = str(info.get("name") or "").strip()
                payout = int(info.get("payout") or 0)
                is_otc = bool(info.get("is_otc", False))

                if not symbol or not str(symbol).endswith("_otc"):
                    continue
                if not is_otc:
                    continue
                if payout < OTC_LIVE_DYNAMIC_MIN_PAYOUT:
                    continue

                normalized = normalize_otc_currency_pair_name(name, symbol)
                if not normalized:
                    continue

                live_map[normalized] = symbol
                quotex_otc_feed.add_symbol(symbol)

                if len(live_map) >= OTC_LIVE_MAX_DYNAMIC_PAIRS:
                    break

    except Exception as e:
        logger.exception("Dynamic OTC currencies availability error: %s", e)

    # إذا وصلت بيانات instruments/list، نستخدم المتاح الآن فقط.
    if live_map:
        return live_map

    # عند بداية التشغيل فقط، قبل وصول instruments/list، نستخدم fallback حتى لا يتوقف البوت.
    if OTC_LIVE_ALLOWED_FALLBACK_ENABLED:
        fallback = allowed_otc_currency_fallback_map()
        try:
            for symbol in fallback.values():
                quotex_otc_feed.add_symbol(symbol)
        except Exception:
            pass
        return fallback

    return {}



def get_otc_symbol_for_pair(pair: str) -> str | None:
    """يرجع الرمز الداخلي فقط إذا الزوج من Currencies المسموحة ومتاح بالخريطة الحالية."""
    try:
        pair = normalize_pair_name_basic(pair)
        if not is_allowed_otc_currency_pair(pair):
            return None

        pair_map = get_otc_analysis_pair_map()
        if pair in pair_map:
            return pair_map[pair]

    except Exception:
        pass

    return None



def start_quotex_otc_feed():
    try:
        quotex_otc_feed.start()
    except Exception as e:
        logger.exception("Could not start Quotex OTC live feed: %s", e)


def get_live_otc_direction(pair: str, fallback_dt: datetime | None = None) -> str:
    symbol = get_otc_symbol_for_pair(pair)
    if symbol:
        live_direction = quotex_otc_feed.direction(symbol)
        if live_direction in {"CALL", "PUT"}:
            return live_direction
    return get_stable_direction(pair, fallback_dt or now_utc())


def get_live_otc_snapshot(pair: str) -> dict:
    symbol = get_otc_symbol_for_pair(pair)
    if not symbol:
        return {}
    return quotex_otc_feed.snapshot(symbol)




def get_stable_direction(pair_or_candles=None, dt=None, lookback: int = 5, min_majority: int = 3, *args, **kwargs):
    """اتجاه ثابت لقسم OTC الزمني.

    الاستخدام الأصلي:
        get_stable_direction(pair: str, dt: datetime) -> "CALL"/"PUT"

    دعم احتياطي:
        get_stable_direction(candles, lookback=5) -> "CALL"/"PUT"/None
    """
    try:
        # المسار الأصلي لقسم OTC الزمني: اتجاه ثابت حسب الزوج والوقت.
        if isinstance(pair_or_candles, str) and isinstance(dt, datetime):
            pair = pair_or_candles
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

        # دعم احتياطي إذا استدعيت الدالة على شموع.
        candles = pair_or_candles
        if candles is None:
            return None

        # إذا ثاني باراميتر رقم، اعتبره lookback.
        if isinstance(dt, int):
            lookback = dt

        recent = list(candles)[-int(lookback):]
        up = 0
        down = 0

        for c in recent:
            if not isinstance(c, dict):
                continue

            o = c.get("open", c.get("o"))
            close = c.get("close", c.get("c"))

            if o is None or close is None:
                continue

            o = float(o)
            close = float(close)

            if close > o:
                up += 1
            elif close < o:
                down += 1

        if up >= int(min_majority) and up > down:
            return "CALL"

        if down >= int(min_majority) and down > up:
            return "PUT"

        return None

    except Exception as e:
        try:
            logger.warning("get_stable_direction failed: %s", e)
        except Exception:
            pass
        return None



def stable_direction(*args, **kwargs):
    return get_stable_direction(*args, **kwargs)


def get_candles_stable_direction(*args, **kwargs):
    return get_stable_direction(*args, **kwargs)






def analyze_best_live_otc_now(lang: str = "ar") -> dict:
    """يفحص كل أزواج OTC من بث Quotex live ويختار أفضل فرصة M1 حالية.
    لا يغيّر نظام الليستات الزمني، ويُستخدم فقط في خيار: صفقة مباشرة.
    """
    candidates = []
    now_ts = time_module.time()

    pair_map = get_otc_analysis_pair_map()

    for pair, symbol in pair_map.items():
        normalized_pair = normalize_otc_currency_pair_name(pair, symbol)
        if not normalized_pair or not is_valid_otc_currency_pair_name(normalized_pair):
            continue
        pair = normalized_pair

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
        try:
            logger.info("OTC LIVE no candidates | pair_map_count=%s | dynamic_enabled=%s | min_payout=%s",
                        len(pair_map), OTC_LIVE_DYNAMIC_PAIRS_ENABLED, OTC_LIVE_DYNAMIC_MIN_PAYOUT)
        except Exception:
            pass

        if str(lang).lower() == "en":
            no_msg = (
                "⚡ OTC Live Trade\n\n"
                "❌ No clear M1 opportunity right now.\n\n"
                "Wait 30-60 seconds, then press:\n"
                "🔎 Find a Trade Now"
            )
        else:
            no_msg = (
                "⚡ صفقة مباشرة OTC\n\n"
                "❌ لا توجد فرصة واضحة الآن على فريم الدقيقة.\n\n"
                "انتظر 30-60 ثانية ثم اضغط:\n"
                "🔎 ابحث عن صفقة الآن"
            )
        return {"ok": False, "message": no_msg}

    ranked_candidates = sorted(candidates, key=lambda x: (x["score"], abs(x["change"]), int(x.get("payout", 0) or 0)), reverse=True)

    # بدل اختيار Top 1 دائمًا، نأخذ من أفضل عدة فرص لتقليل احتكار نفس الزوج للنشر.
    top_pool = ranked_candidates[:max(1, int(OTC_LIVE_TOP_CANDIDATES_POOL))]
    last_pair = otc_live_channel_state.get("last_pair") if "otc_live_channel_state" in globals() else None

    best = None
    for candidate in top_pool:
        if last_pair and candidate.get("pair") == last_pair and len(top_pool) > 1:
            continue

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

    if str(lang).lower() == "en":
        direction_line = "🟢 CALL" if best["direction"] == "CALL" else "🔴 PUT"
        msg = (
            "⚡ OTC Live Trade\n\n"
            f"💱 Pair: {best['pair']}\n"
            "🧭 Timeframe: M1\n"
            f"⏰ Entry Time: {format_utc_plus_3(entry_dt)}\n"
            f"📌 Direction: {direction_line}\n"
            f"💵 Current Price: {price_text}\n"
            f"📊 Opportunity Strength: {best['score']}%\n\n"
            "⚠️ Follow proper risk management. Enter only if the direction still looks the same at entry time."
        )
    else:
        msg = (
            "⚡ صفقة مباشرة OTC\n\n"
            f"💱 الزوج: {best['pair']}\n"
            "🧭 الفريم: M1\n"
            f"⏰ وقت الدخول: {format_utc_plus_3(entry_dt)}\n"
            f"📌 الاتجاه: {direction_line}\n"
            f"💵 السعر الحالي: {price_text}\n"
            f"📊 قوة الفرصة: {best['score']}%\n\n"
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


# ===== Admin Experimental Trading Session Room =====
TRADING_ROOM_ADMIN_ONLY = False
TRADING_ROOM_SCAN_SECONDS = int(os.getenv("TRADING_ROOM_SCAN_SECONDS", "300"))
TRADING_ROOM_SCAN_INTERVAL_SECONDS = int(os.getenv("TRADING_ROOM_SCAN_INTERVAL_SECONDS", "5"))
TRADING_ROOM_RESULT_DELAY_SECONDS = int(os.getenv("TRADING_ROOM_RESULT_DELAY_SECONDS", "70"))
TRADING_ROOM_RESULT_EXTRA_DELAY_SECONDS = int(os.getenv("TRADING_ROOM_RESULT_EXTRA_DELAY_SECONDS", "4"))
TRADING_ROOM_MIN_PAIR_SCORE = int(os.getenv("TRADING_ROOM_MIN_PAIR_SCORE", "62"))
TRADING_ROOM_MIN_ENTRY_SCORE = int(os.getenv("TRADING_ROOM_MIN_ENTRY_SCORE", "68"))
TRADING_ROOM_MAX_EXTRA_RECOVERY = int(os.getenv("TRADING_ROOM_MAX_EXTRA_RECOVERY", "1"))
TRADING_ROOM_MIN_TICKS = int(os.getenv("TRADING_ROOM_MIN_TICKS", "35"))
TRADING_ROOM_MIN_CANDLES = int(os.getenv("TRADING_ROOM_MIN_CANDLES", "4"))
TRADING_ROOM_DIRECT_ENTRY_MAX_SECOND = int(os.getenv("TRADING_ROOM_DIRECT_ENTRY_MAX_SECOND", "30"))
TRADING_ROOM_DATA_RETRY_SECONDS = int(os.getenv("TRADING_ROOM_DATA_RETRY_SECONDS", "30"))
TRADING_ROOM_DATA_MAX_RETRIES = int(os.getenv("TRADING_ROOM_DATA_MAX_RETRIES", "6"))
# بعد أي خسارة لا ننهي الجلسة بسرعة؛ نمدد المراقبة بحثًا عن فرصة تعويض آمنة.
TRADING_ROOM_RECOVERY_SEARCH_SECONDS = int(os.getenv("TRADING_ROOM_RECOVERY_SEARCH_SECONDS", "900"))
TRADING_ROOM_RECOVERY_MESSAGE_COOLDOWN_SECONDS = int(os.getenv("TRADING_ROOM_RECOVERY_MESSAGE_COOLDOWN_SECONDS", "180"))
# شروط إنهاء غرفة جلسة التداول: لا تنتهي الجلسة إلا عند أحد هذه الأهداف.
TRADING_ROOM_TARGET_WINS = int(os.getenv("TRADING_ROOM_TARGET_WINS", "3"))
TRADING_ROOM_MAX_LOSSES = int(os.getenv("TRADING_ROOM_MAX_LOSSES", "3"))
TRADING_ROOM_MAX_RECOVERY_LOSSES = int(os.getenv("TRADING_ROOM_MAX_RECOVERY_LOSSES", "2"))
TRADING_ROOM_RECOVERY_MULTIPLIER = float(os.getenv("TRADING_ROOM_RECOVERY_MULTIPLIER", "2.0"))
TRADING_ROOM_TRADE_AMOUNT_PERCENT = float(os.getenv("TRADING_ROOM_TRADE_AMOUNT_PERCENT", "1"))
# أهداف غرفة جلسة التداول أصبحت مبنية على نسبة من رأس المال، لا على عدد صفقات.
TRADING_ROOM_TARGET_PROFIT_PERCENT = float(os.getenv("TRADING_ROOM_TARGET_PROFIT_PERCENT", "3"))
TRADING_ROOM_MAX_LOSS_PERCENT = float(os.getenv("TRADING_ROOM_MAX_LOSS_PERCENT", "4"))

# Smart exit suggestion: if a session takes too long and reaches a decent partial profit
# or a controlled partial loss, pause and ask the user whether to stop or continue.
TRADING_ROOM_SMART_EXIT_MIN_SECONDS = int(os.getenv("TRADING_ROOM_SMART_EXIT_MIN_SECONDS", "1800"))
TRADING_ROOM_SMART_EXIT_PROFIT_PART = float(os.getenv("TRADING_ROOM_SMART_EXIT_PROFIT_PART", "0.50"))
TRADING_ROOM_SMART_EXIT_LOSS_PART = float(os.getenv("TRADING_ROOM_SMART_EXIT_LOSS_PART", "0.50"))
TRADING_ROOM_SMART_EXIT_FLAT_SECONDS = int(os.getenv("TRADING_ROOM_SMART_EXIT_FLAT_SECONDS", "2700"))
TRADING_ROOM_SMART_EXIT_COOLDOWN_SECONDS = int(os.getenv("TRADING_ROOM_SMART_EXIT_COOLDOWN_SECONDS", "900"))

# ===== Trading Room Session Brain =====
# طبقة وعي الجلسة: تراقب سلوك الزوج، تمنع تكرار نفس النمط الخاسر،
# وتغيّر الزوج تلقائيًا إذا صار ضعيفًا أو متذبذبًا.
TRADING_ROOM_PAIR_MAX_SWITCHES = int(os.getenv("TRADING_ROOM_PAIR_MAX_SWITCHES", "2"))
TRADING_ROOM_PAIR_BAD_HEALTH = int(os.getenv("TRADING_ROOM_PAIR_BAD_HEALTH", "42"))
TRADING_ROOM_PAIR_WEAK_HEALTH = int(os.getenv("TRADING_ROOM_PAIR_WEAK_HEALTH", "55"))
TRADING_ROOM_PAIR_STALE_SECONDS = int(os.getenv("TRADING_ROOM_PAIR_STALE_SECONDS", "12"))
TRADING_ROOM_PAIR_NO_ENTRY_SWITCH_SCANS = int(os.getenv("TRADING_ROOM_PAIR_NO_ENTRY_SWITCH_SCANS", "24"))
TRADING_ROOM_BRAIN_NOTICE_COOLDOWN_SECONDS = int(os.getenv("TRADING_ROOM_BRAIN_NOTICE_COOLDOWN_SECONDS", "90"))
# تهدئة وعي الجلسة: لا نغيّر الزوج فورًا بعد اختياره، ولا بسبب قراءة واحدة ضعيفة.
TRADING_ROOM_PAIR_MIN_OBSERVE_SECONDS = int(os.getenv("TRADING_ROOM_PAIR_MIN_OBSERVE_SECONDS", "120"))
TRADING_ROOM_PAIR_SWITCH_COOLDOWN_SECONDS = int(os.getenv("TRADING_ROOM_PAIR_SWITCH_COOLDOWN_SECONDS", "180"))
TRADING_ROOM_PAIR_BAD_CONFIRM_SCANS = int(os.getenv("TRADING_ROOM_PAIR_BAD_CONFIRM_SCANS", "3"))
TRADING_ROOM_PAIR_TICK_MAX_AGE_SECONDS = int(os.getenv("TRADING_ROOM_PAIR_TICK_MAX_AGE_SECONDS", "20"))



def trading_room_key(admin_id: int) -> str:
    return f"trading_room:{int(admin_id)}"


def get_trading_room_state(context: ContextTypes.DEFAULT_TYPE, admin_id: int) -> dict:
    return context.bot_data.setdefault(trading_room_key(admin_id), {})


def clear_trading_room_state(context: ContextTypes.DEFAULT_TYPE, admin_id: int):
    try:
        context.bot_data.pop(trading_room_key(admin_id), None)
    except Exception:
        pass


def parse_balance_amount(text: str) -> float | None:
    try:
        raw = str(text or "").strip()
        if not raw:
            return None
        # دعم الأرقام العربية والفاصلة العربية/العشرية، واستخراج آخر رقم من الرسالة
        # حتى لو كان المستخدم يرد على رسالة بوت وفي النص اقتباس أو كلمات إضافية.
        trans = str.maketrans({
            "٠": "0", "١": "1", "٢": "2", "٣": "3", "٤": "4",
            "٥": "5", "٦": "6", "٧": "7", "٨": "8", "٩": "9",
            "۰": "0", "۱": "1", "۲": "2", "۳": "3", "۴": "4",
            "۵": "5", "۶": "6", "۷": "7", "۸": "8", "۹": "9",
            "٫": ".", "،": ".", ",": ".", "$": " ",
        })
        cleaned = raw.translate(trans)
        matches = re.findall(r"\d+(?:\.\d+)?", cleaned)
        if not matches:
            return None
        value = float(matches[-1])
        if value <= 0:
            return None
        return value
    except Exception:
        return None


def _round_platform_amount(value) -> int:
    """Quotex amount fields do not support decimals: 6.50+ -> 7, below 6.50 -> 6."""
    try:
        v = float(value or 0)
        if v <= 0:
            return 1
        return max(1, int(v + 0.5))
    except Exception:
        return 1


def _money_whole(value) -> str:
    try:
        return f"{_round_platform_amount(value)}$"
    except Exception:
        return "1$"


def trading_room_user_cooldown_key(user_id: int) -> str:
    return f"trading_room_cooldown_until:{int(user_id)}"


def get_trading_room_cooldown_remaining(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> int:
    try:
        until_ts = float(context.bot_data.get(trading_room_user_cooldown_key(user_id), 0) or 0)
        return max(0, int(until_ts - time_module.time()))
    except Exception:
        return 0


def set_trading_room_cooldown(context: ContextTypes.DEFAULT_TYPE, user_id: int, seconds: int = 1800):
    try:
        context.bot_data[trading_room_user_cooldown_key(user_id)] = time_module.time() + int(seconds)
    except Exception:
        pass


def clear_trading_room_cooldown(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    try:
        context.bot_data.pop(trading_room_user_cooldown_key(user_id), None)
    except Exception:
        pass


def _cooldown_text(seconds: int) -> str:
    try:
        minutes = max(1, int((int(seconds) + 59) // 60))
        return f"{minutes} دقيقة"
    except Exception:
        return "نصف ساعة"


def build_session_money_plan(balance: float) -> dict:
    balance = float(balance)
    # المنصة لا تقبل فواصل في مبلغ الدخول؛ لذلك نقرّب المبلغ لرقم صحيح.
    risk_percent = float(TRADING_ROOM_TRADE_AMOUNT_PERCENT) / 100.0
    trade_amount_raw = balance * risk_percent
    trade_amount = float(_round_platform_amount(trade_amount_raw))
    recovery_amount = float(_round_platform_amount(trade_amount * TRADING_ROOM_RECOVERY_MULTIPLIER))
    # أهداف الجلسة كنسبة من رأس المال، وتُعرض كأرقام صحيحة حتى تكون واضحة للمستخدم.
    target_profit_amount = float(_round_platform_amount(balance * (TRADING_ROOM_TARGET_PROFIT_PERCENT / 100.0)))
    max_loss_amount = float(_round_platform_amount(balance * (TRADING_ROOM_MAX_LOSS_PERCENT / 100.0)))
    return {
        "balance": balance,
        "risk_percent": risk_percent,
        "trade_amount": trade_amount,
        "recovery_amount": recovery_amount,
        "max_trades": 0,
        "target_profit_amount": target_profit_amount,
        "max_loss_amount": max_loss_amount,
        "max_planned_risk": max_loss_amount,
    }

def _safe_price_text(pair: str, price: float) -> str:
    try:
        return f"{float(price):.5f}" if "JPY" not in str(pair) else f"{float(price):.3f}"
    except Exception:
        return str(price)


def _money_signed(value) -> str:
    try:
        v = float(value or 0)
        sign = "+" if v > 0 else ""
        return f"{sign}{v:.2f}$"
    except Exception:
        return "0.00$"


def _normalize_payout_percent(value, default: float = 80.0) -> float:
    """يرجع نسبة payout كما كانت عند لحظة دخول الصفقة.
    مثال: 83 يعني الربح الصافي = مبلغ الدخول * 0.83.
    """
    try:
        payout = float(value or default)
        if payout <= 0:
            payout = default
        if payout > 100:
            payout = 100.0
        return round(payout, 2)
    except Exception:
        return float(default)


def _trading_room_win_profit(trade_amount: float, payout_percent: float | int | None) -> float:
    try:
        return round(float(trade_amount or 0) * (_normalize_payout_percent(payout_percent) / 100.0), 2)
    except Exception:
        return 0.0


def _append_trading_room_ledger(state: dict, trade: dict, win: bool, pnl: float, effective_win_units: int = 0, effective_loss_units: int = 0):
    """يسجل نتيجة الصفقة كدفتر حسابات فعلي للجلسة.

    الربح في Quotex = مبلغ الدخول × payout المثبت عند لحظة الدخول.
    الخسارة = مبلغ الدخول كامل.
    صفقة التعويض الرابحة تُسجل كوحدتي ربح منطقيًا: تعويض الصفقة السابقة + ربح الصفقة الحالية،
    لكن صافي المال يبقى محسوبًا فعليًا حسب مبلغ الدخول والـ payout.
    """
    try:
        history = list(state.get("trade_ledger") or [])
        history.append({
            "time": time_module.time(),
            "pair": trade.get("pair"),
            "symbol": trade.get("symbol"),
            "direction": trade.get("direction"),
            "recovery_trade": bool(trade.get("recovery_trade")),
            "amount": float(trade.get("trade_amount") or 0),
            "payout": _normalize_payout_percent(trade.get("payout", 80), 80.0),
            "result": "win" if win else "loss",
            "pnl": round(float(pnl or 0), 2),
            "effective_win_units": int(effective_win_units or 0),
            "effective_loss_units": int(effective_loss_units or 0),
            "net_after": round(float(state.get("net_profit", 0.0) or 0.0), 2),
        })
        state["trade_ledger"] = history[-100:]
    except Exception:
        pass


def _percent_text(value) -> str:
    try:
        v = float(value)
        return f"{v:g}%"
    except Exception:
        return str(value)


def build_trading_room_selected_pair_message(pair: str, lang: str = "ar") -> str:
    if lang == "en":
        return (
            "🎯 Session pair selected\n\n"
            f"💱 Pair: {pair}\n\n"
            "Open this pair and get ready. I will monitor it and send an entry only when a suitable pattern appears."
        )
    return (
        "🎯 تم اختيار زوج الجلسة\n\n"
        f"💱 الزوج: {pair}\n\n"
        "افتح هذا الزوج وجهّزه. سأراقبه وأرسل لك الدخول عند ظهور نمط مناسب."
    )



def _trading_room_admin_setup_deep_text(setup_kind: str, direction: str, setup: str, lang: str = "ar") -> tuple[str, str, str]:
    """تحليل نصي أعمق للأدمن: لماذا دخلنا؟ وما الذي كان يفترض أن يحدث؟ ومتى يفشل السيناريو؟"""
    kind = str(setup_kind or "UNKNOWN").upper()
    direction = str(direction or "").upper()
    up = direction == "CALL"
    if lang == "en":
        data = {
            "STRUCTURE_RETEST": (
                "The engine saw a price area that had been respected before, then price returned to it and showed rejection.",
                "The expected behavior was that the retest holds and the next candle continues away from the area in the trade direction.",
                "The setup fails if price accepts back inside/through the zone instead of respecting the retest."
            ),
            "TREND_RETEST_CONTINUATION": (
                "The market had an active trend/momentum leg and price pulled back into a retest area instead of breaking structure.",
                "The expected behavior was continuation with the trend after the pullback finishes.",
                "The setup fails if the pullback turns into structure break or momentum flips before the entry candle closes."
            ),
            "LIQUIDITY_SWEEP": (
                "Price swept a nearby high/low liquidity point and returned back from it.",
                "The expected behavior was a reversal after the stop-hunt/liquidity grab.",
                "The setup fails if the sweep becomes a real breakout and price keeps accepting beyond that level."
            ),
            "FAILED_BREAKOUT": (
                "Price tried to break a nearby micro high/low but failed to hold the break.",
                "The expected behavior was a move back in the opposite direction after the failed break.",
                "The setup fails if the breakout attempt becomes valid and the market continues beyond the broken level."
            ),
            "ROUND_NUMBER_REJECTION": (
                "Price reacted near a psychological/round number where many traders usually place orders.",
                "The expected behavior was rejection from that number, not acceptance beyond it.",
                "The setup fails if price starts accepting above/below the round number with no rejection."
            ),
            "ORDER_BLOCK_RETEST": (
                "Price returned to the last opposite candle before an impulse move, which can act as an order block.",
                "The expected behavior was a reaction from that block and continuation in the chosen direction.",
                "The setup fails if the block is absorbed and price closes through it."
            ),
            "BOS_CHOCH_RETEST": (
                "The engine detected a small structure shift/break, then a retest of the broken area.",
                "The expected behavior was that the retest confirms the new structure direction.",
                "The setup fails if the supposed structure shift is invalidated quickly."
            ),
            "EQUAL_LIQUIDITY_SWEEP": (
                "Price attacked equal highs/lows and then showed a rejection attempt.",
                "The expected behavior was reversal after liquidity collection.",
                "The setup fails if liquidity collection turns into continuation instead of rejection."
            ),
            "TRENDLINE_PULLBACK": (
                "Price pulled back toward a micro trendline/channel area and showed reaction.",
                "The expected behavior was continuation from that pullback area.",
                "The setup fails if the trendline/channel stops being respected."
            ),
            "WICK_REJECTION": (
                "A wick by itself is not enough for a trade. This old label should only appear as support, not as a standalone setup.",
                "The expected behavior needs confirmation from zone, liquidity, structure, or round-number context.",
                "The setup fails if the wick was only noise and price accepts against it."
            ),
            "WICK_REJECTION_CONFLUENCE": (
                "Price rejected with a wick at a meaningful context area, not randomly in the middle of movement. The wick was supported by zone/liquidity/structure confirmation.",
                "The expected behavior was that the wick represents real rejection, so the next candle should move away from that area in the trade direction.",
                "The setup fails if the level gets absorbed, the liquidity sweep turns into a real breakout, or price accepts back through the rejected area."
            ),
            "MOMENTUM_CONTINUATION": (
                "The last ticks and recent candles showed clean pressure in one direction.",
                "The expected behavior was short continuation before momentum exhausts.",
                "The setup fails if the move was already exhausted or the last seconds flip against it."
            ),
            "STRONG_TREND_CONTINUATION": (
                "The engine detected a strong directional trend/momentum and avoided counter-trend logic.",
                "The expected behavior was continuation with the strong direction.",
                "The setup fails if momentum suddenly stalls and forms a pullback/reversal candle."
            ),
            "OVEREXTENSION_REVERSAL": (
                "The move looked stretched and the engine expected a short pullback/reversal.",
                "The expected behavior was that the exhausted move slows down and reverses.",
                "The setup fails if the move was not exhaustion but real trend acceleration."
            ),
            "COMPRESSION_BREAKOUT": (
                "The market compressed in a narrow range, then started to break out.",
                "The expected behavior was clean continuation after the compression release.",
                "The setup fails if the breakout becomes a fake breakout and returns inside the range."
            ),
            "MOOD_SHIFT": (
                "Recent behavior changed from one-sided pressure to the opposite side.",
                "The expected behavior was that the new pressure continues for the entry candle.",
                "The setup fails if the change was only a temporary correction."
            ),
        }
        fallback = (
            f"The engine selected this trade because the internal pattern was {setup or kind}.",
            "The expected behavior was that the next closed candle confirms the selected direction.",
            "The setup fails if price behavior changes before or during the entry candle."
        )
        return data.get(kind, fallback)
    data = {
        "STRUCTURE_RETEST": (
            "البوت رأى منطقة سعرية تم احترامها سابقًا، ثم عاد السعر لاختبارها وظهر رفض سعري منها.",
            "المفروض أن المنطقة تمسك السعر وتدفع الشمعة التالية بعيدًا عنها باتجاه الصفقة.",
            "يفشل السيناريو إذا السعر قبل الدخول داخل المنطقة أو اخترقها بدل ما يحترم إعادة الاختبار."
        ),
        "TREND_RETEST_CONTINUATION": (
            "كان في اتجاه/زخم فعّال، والسعر رجع لاختبار منطقة داخل الاتجاه بدل ما يكسر البنية.",
            "المفروض أن الرجوع يكون مجرد تصحيح، وبعده تكمل الشمعة مع الاتجاه.",
            "يفشل السيناريو إذا التصحيح تحول لكسر بنية أو انعكس الزخم قبل إغلاق شمعة الصفقة."
        ),
        "LIQUIDITY_SWEEP": (
            "السعر سحب سيولة من قمة/قاع قريب ثم رجع منها، وهذا غالبًا يعطي ارتداد قصير.",
            "المفروض بعد سحب السيولة يصير رفض ويرجع السعر بعكس جهة السحب.",
            "يفشل السيناريو إذا السحب تحول لاختراق حقيقي والسعر قبل البقاء بعد المستوى."
        ),
        "FAILED_BREAKOUT": (
            "السعر حاول يكسر قمة/قاع صغير لكنه فشل يثبت بعد الكسر.",
            "المفروض بعد فشل الاختراق يرجع السعر للجهة المعاكسة.",
            "يفشل السيناريو إذا محاولة الاختراق صارت اختراق حقيقي واستمر السعر بعدها."
        ),
        "ROUND_NUMBER_REJECTION": (
            "السعر اقترب من رقم دائري/نفسي، وهي مناطق كثير أوامر تتجمع حولها.",
            "المفروض يظهر رفض من الرقم الدائري بدل قبول السعر فوقه أو تحته.",
            "يفشل السيناريو إذا السعر قبل التداول بعد الرقم بدون رفض واضح."
        ),
        "ORDER_BLOCK_RETEST": (
            "السعر رجع لآخر شمعة عكسية قبل اندفاع، وهي ممكن تتصرف كمنطقة أوامر.",
            "المفروض تظهر ردة فعل من هذه المنطقة ويكمل السعر باتجاه الصفقة.",
            "يفشل السيناريو إذا منطقة الأوامر انامتصت وأغلقت الشمعة داخلها أو بعدها."
        ),
        "BOS_CHOCH_RETEST": (
            "البوت قرأ تبدل/كسر بنية صغير ثم إعادة اختبار لمنطقة الكسر.",
            "المفروض إعادة الاختبار تؤكد اتجاه البنية الجديدة.",
            "يفشل السيناريو إذا تبدل البنية طلع وهمي وانلغى بسرعة."
        ),
        "EQUAL_LIQUIDITY_SWEEP": (
            "السعر هاجم قمم/قيعان متساوية ثم ظهر رفض بعد أخذ السيولة.",
            "المفروض بعد جمع السيولة يرجع السعر بعكس جهة السحب.",
            "يفشل السيناريو إذا جمع السيولة تحول لاستمرار وليس انعكاس."
        ),
        "TRENDLINE_PULLBACK": (
            "السعر رجع على ترند/قناة مصغّرة وظهر تفاعل من نفس الاتجاه.",
            "المفروض أن الارتداد من خط الحركة يعطي استمرار للاتجاه.",
            "يفشل السيناريو إذا الترند المصغر ما عاد محترم وانكسر."
        ),
        "WICK_REJECTION": (
            "الذيل وحده لا يكفي للدخول. هذا التصنيف القديم يجب أن يكون داعمًا فقط وليس سببًا مستقلًا للصفقة.",
            "المفروض وجود تأكيد من منطقة أو سيولة أو بنية أو رقم دائري قبل الاعتماد عليه.",
            "يفشل السيناريو إذا الذيل كان مجرد ضوضاء والسعر قبل الاتجاه المعاكس."
        ),
        "WICK_REJECTION_CONFLUENCE": (
            "الدخول لم يكن بسبب ذيل فقط؛ الذيل ظهر عند سياق مهم مثل منطقة/سيولة/بنية/رقم دائري، لذلك اعتبره البوت رفضًا سعريًا له معنى.",
            "المفروض أن الذيل يمثل رفضًا حقيقيًا، فتتحرك شمعة الدخول بعيدًا عن المنطقة باتجاه الصفقة.",
            "يفشل السيناريو إذا تم امتصاص المنطقة، أو تحول سحب السيولة إلى اختراق حقيقي، أو قبل السعر الرجوع داخل المنطقة المرفوضة."
        ),
        "MOMENTUM_CONTINUATION": (
            "آخر الحركة والشموع كانت تعطي ضغط نظيف باتجاه واحد.",
            "المفروض يكون في استمرار قصير قبل ما يستهلك الزخم.",
            "يفشل السيناريو إذا الحركة كانت مستهلكة أو آخر الثواني قلبت ضد الصفقة."
        ),
        "STRONG_TREND_CONTINUATION": (
            "البوت قرأ ترند/مومنتم قوي وفضّل الدخول مع الاتجاه بدل عكسه.",
            "المفروض يستمر الدفع مع الاتجاه خلال شمعة الصفقة.",
            "يفشل السيناريو إذا الزخم وقف فجأة وتحوّل لتصحيح أو انعكاس."
        ),
        "OVEREXTENSION_REVERSAL": (
            "الحركة ظهرت متمددة زيادة، والبوت توقع تصحيح/ارتداد قصير.",
            "المفروض الحركة المستهلكة تهدأ ثم تعكس مؤقتًا.",
            "يفشل السيناريو إذا الحركة لم تكن استهلاكًا بل تسارع ترند حقيقي."
        ),
        "COMPRESSION_BREAKOUT": (
            "السوق كان مضغوطًا داخل نطاق ضيق ثم بدأ خروج واضح.",
            "المفروض خروج الضغط يعطي استمرار قصير باتجاه الكسر.",
            "يفشل السيناريو إذا الكسر كان وهمي ورجع السعر داخل النطاق."
        ),
        "MOOD_SHIFT": (
            "سلوك السعر تبدل من ضغط باتجاه إلى بداية ضغط بالاتجاه الآخر.",
            "المفروض المزاج الجديد يستمر خلال شمعة الدخول.",
            "يفشل السيناريو إذا التبدل كان مجرد تصحيح مؤقت."
        ),
    }
    fallback = (
        f"البوت اختار الدخول لأن القراءة الداخلية كانت {setup or kind}.",
        "المفروض أن شمعة الدخول تؤكد الاتجاه المختار.",
        "يفشل السيناريو إذا تغير سلوك السعر قبل أو أثناء شمعة الصفقة."
    )
    return data.get(kind, fallback)


def build_trading_room_admin_entry_reason(trade: dict, lang: str = "ar") -> str:
    """سبب دخول الصفقة يظهر لحساب الأدمن فقط داخل غرفة جلسة التداول، بصياغة تحليلية مفيدة."""
    try:
        setup = str(trade.get("setup") or trade.get("setup_kind") or "غير محدد")
        setup_kind = str(trade.get("setup_kind") or "UNKNOWN")
        score = trade.get("score")
        direction = str(trade.get("direction") or "")
        payout = _normalize_payout_percent(trade.get("payout", 80), 80.0)
        price = trade.get("price")
        score_line = f"{int(score)}%" if score is not None else ("غير متوفر" if lang != "en" else "not available")
        premise, expectation, risk = _trading_room_admin_setup_deep_text(setup_kind, direction, setup, lang)
        price_line = ""
        try:
            price_line = f"\nEntry price snapshot: {float(price):.5f}" if lang == "en" else f"\nلقطة السعر وقت القرار: {float(price):.5f}"
        except Exception:
            pass
        if lang == "en":
            return (
                "🧠 Admin entry analysis\n"
                f"Setup: {setup_kind}\n"
                f"Direction: {direction}\n"
                f"Why entry: {premise}\n"
                f"Expected reaction: {expectation}\n"
                f"Failure condition: {risk}\n"
                f"Entry strength: {score_line}\n"
                f"Payout fixed at entry: {payout:g}%"
                f"{price_line}"
            )
        return (
            "🧠 تحليل سبب الدخول للأدمن\n"
            f"نوع القراءة: {setup_kind}\n"
            f"الاتجاه المختار: {direction}\n"
            f"لماذا دخلنا؟ {premise}\n"
            f"ماذا كان المتوقع؟ {expectation}\n"
            f"متى يفشل السيناريو؟ {risk}\n"
            f"قوة الدخول: {score_line}\n"
            f"نسبة الزوج المثبتة وقت الدخول: {payout:g}%"
            f"{price_line}"
        )
    except Exception:
        return "🧠 سبب الدخول غير متوفر حاليًا." if lang != "en" else "🧠 Entry reason is not available right now."


def _trading_room_admin_result_deep_text(trade: dict, win: bool | None, candle_open: float, candle_close: float, candle_high=None, candle_low=None, lang: str = "ar") -> str:
    kind = str(trade.get("setup_kind") or "UNKNOWN").upper()
    direction = str(trade.get("direction") or "").upper()
    setup = str(trade.get("setup") or kind)
    try:
        o = float(candle_open); c = float(candle_close)
        h = float(candle_high) if candle_high is not None else max(o, c)
        l = float(candle_low) if candle_low is not None else min(o, c)
        rng = max(h-l, 1e-12)
        body_ratio = abs(c-o)/rng
        upper_wick = (h-max(o,c))/rng
        lower_wick = (min(o,c)-l)/rng
    except Exception:
        body_ratio = upper_wick = lower_wick = 0.0
    expected_side = "bullish" if direction == "CALL" else "bearish"
    if lang == "en":
        if win is None:
            return (
                "The entry candle finished almost neutral. This means the setup did not get real follow-through, "
                "but it was not invalidated strongly enough to count as a loss. The correct lesson is to treat this as no confirmation, not as a valid win."
            )
        if win:
            base = {
                "STRUCTURE_RETEST": "The retest zone held. Price did not accept through the zone and the candle closed in the expected direction.",
                "TREND_RETEST_CONTINUATION": "The pullback stayed corrective and the trend/momentum continued during the entry candle.",
                "LIQUIDITY_SWEEP": "The liquidity sweep was followed by rejection, so the stop-hunt behaved as a reversal trigger.",
                "FAILED_BREAKOUT": "The breakout attempt failed and price returned away from the broken micro level.",
                "ROUND_NUMBER_REJECTION": "The psychological number held as a reaction area and price rejected it.",
                "ORDER_BLOCK_RETEST": "The order block reacted correctly; price respected the block instead of absorbing it.",
                "BOS_CHOCH_RETEST": "The structure shift remained valid and the retest confirmed the new direction.",
                "EQUAL_LIQUIDITY_SWEEP": "Equal high/low liquidity was collected and price rejected after the sweep.",
                "TRENDLINE_PULLBACK": "The micro trendline/channel pullback held and continuation followed.",
                "WICK_REJECTION": "The rejection wick got follow-through; price moved away from the rejected side.",
                "WICK_REJECTION_CONFLUENCE": "The wick rejection got confirmation: price moved away from the confluence area instead of accepting through it.",
                "MOMENTUM_CONTINUATION": "Momentum continued long enough and did not flip during the entry candle.",
                "STRONG_TREND_CONTINUATION": "The strong trend kept control during the trade candle.",
                "OVEREXTENSION_REVERSAL": "The stretched move cooled down and gave the expected pullback/reversal.",
                "COMPRESSION_BREAKOUT": "The compression breakout produced follow-through instead of returning inside the range.",
                "MOOD_SHIFT": "The new mood/pressure continued during the entry candle."
            }.get(kind, "The market confirmed the original entry idea during the trade candle.")
            return base
        # loss
        base = {
            "STRUCTURE_RETEST": "The retest idea failed: the area did not hold after entry. What looked like rejection was likely only a temporary bounce/absorption, then price accepted back through the zone.",
            "TREND_RETEST_CONTINUATION": "The trend pullback turned into a deeper counter-move. Momentum was not strong enough to continue from the retest.",
            "LIQUIDITY_SWEEP": "The supposed liquidity sweep did not reverse. It behaved more like a real breakout/continuation after liquidity was taken.",
            "FAILED_BREAKOUT": "The breakout was not truly failed. Price continued validating the break instead of rejecting it.",
            "ROUND_NUMBER_REJECTION": "The round number did not reject price strongly enough. The market accepted around/beyond the number, so the psychological level was weak.",
            "ORDER_BLOCK_RETEST": "The order block was absorbed. Instead of reacting from the block, price traded through it and invalidated the area.",
            "BOS_CHOCH_RETEST": "The structure shift was not confirmed. The retest failed and the market invalidated the assumed new structure.",
            "EQUAL_LIQUIDITY_SWEEP": "Liquidity collection did not create reversal. Price kept moving after taking the equal highs/lows.",
            "TRENDLINE_PULLBACK": "The trendline/channel pullback failed; price stopped respecting the micro trend structure.",
            "WICK_REJECTION": "The wick rejection did not get follow-through. The next candle absorbed the rejection and accepted against it.",
            "WICK_REJECTION_CONFLUENCE": "The confluence failed: the zone/liquidity rejection was absorbed and price accepted back through the area, so the wick was not real rejection.",
            "MOMENTUM_CONTINUATION": "The momentum was already weakening or exhausted. The entry followed pressure that did not continue into the trade candle.",
            "STRONG_TREND_CONTINUATION": "The strong trend paused or pulled back during the exact entry candle, so continuation did not materialize.",
            "OVEREXTENSION_REVERSAL": "The move was not exhaustion; it was still active acceleration, so the counter move was too early.",
            "COMPRESSION_BREAKOUT": "The breakout after compression was fake. Price returned against the breakout instead of continuing.",
            "MOOD_SHIFT": "The mood shift was not stable; it was only a temporary correction and the previous pressure came back."
        }.get(kind, "The market invalidated the original entry idea during the trade candle.")
        if body_ratio < 0.22 and max(upper_wick, lower_wick) > 0.45:
            base += " The candle also had a weak body/large wick, which suggests indecision and absorption rather than clean confirmation."
        return base
    # Arabic
    if win is None:
        return (
            "شمعة الدخول انتهت شبه محايدة. هذا يعني أن الفكرة لم تحصل على متابعة حقيقية، "
            "لكنها أيضًا لم تُكسر بقوة كافية لنحسبها خسارة. الدرس هنا: هذا كان غياب تأكيد، وليس صفقة ناجحة."
        )
    if win:
        base = {
            "STRUCTURE_RETEST": "منطقة إعادة الاختبار صمدت؛ السعر لم يقبل الكسر داخلها/بعدها وأغلقت الشمعة مع الاتجاه المتوقع.",
            "TREND_RETEST_CONTINUATION": "التصحيح بقي صحيًا، والترند/المومنتم كمل خلال شمعة الدخول.",
            "LIQUIDITY_SWEEP": "سحب السيولة تبعه رفض فعلي، لذلك تصرف كفخ ثم ارتداد.",
            "FAILED_BREAKOUT": "محاولة الاختراق فشلت فعلًا والسعر رجع بعيدًا عن مستوى الكسر الصغير.",
            "ROUND_NUMBER_REJECTION": "الرقم الدائري صمد كمنطقة تفاعل وظهر منه رفض سعري.",
            "ORDER_BLOCK_RETEST": "منطقة الأوردر بلوك أعطت ردة فعل صحيحة ولم يتم امتصاصها.",
            "BOS_CHOCH_RETEST": "تبدل/كسر البنية بقي صالحًا وإعادة الاختبار أكدت الاتجاه الجديد.",
            "EQUAL_LIQUIDITY_SWEEP": "تم أخذ سيولة القمم/القيعان المتساوية ثم ظهر رفض بعدها.",
            "TRENDLINE_PULLBACK": "الترند/القناة المصغرة صمدت، وبعد الرجوع كمل السعر بالاتجاه.",
            "WICK_REJECTION": "ذيل الرفض حصل على متابعة، والسعر ابتعد عن جهة الرفض.",
            "WICK_REJECTION_CONFLUENCE": "رفض الذيل حصل على تأكيد؛ السعر ابتعد عن منطقة الالتقاء بدل ما يقبل التداول داخلها.",
            "MOMENTUM_CONTINUATION": "الزخم كمل كفاية ولم ينقلب خلال شمعة الصفقة.",
            "STRONG_TREND_CONTINUATION": "الترند القوي بقي مسيطرًا خلال شمعة الدخول.",
            "OVEREXTENSION_REVERSAL": "الحركة المتمددة هدأت وأعطت التصحيح/الانعكاس المتوقع.",
            "COMPRESSION_BREAKOUT": "الخروج من الضغط أعطى متابعة ولم يرجع داخل النطاق.",
            "MOOD_SHIFT": "المزاج الجديد للسوق استمر خلال شمعة الدخول."
        }.get(kind, "السوق أكد فكرة الدخول الأصلية خلال شمعة الصفقة.")
        return base
    base = {
        "STRUCTURE_RETEST": "فكرة إعادة الاختبار فشلت: المنطقة لم تصمد بعد الدخول. الرفض اللي ظهر قبل الصفقة كان غالبًا ارتداد مؤقت أو امتصاص، وبعدها السعر قبل الرجوع داخل/عكس المنطقة.",
        "TREND_RETEST_CONTINUATION": "رجوع السعر لم يعد تصحيحًا صحيًا؛ تحول لحركة أعمق عكس الاتجاه، والزخم لم يكن كافيًا ليكمل من إعادة الاختبار.",
        "LIQUIDITY_SWEEP": "سحب السيولة لم ينتج عنه انعكاس. الحركة تصرفت كاختراق/استمرار حقيقي بعد أخذ السيولة.",
        "FAILED_BREAKOUT": "الاختراق لم يكن فاشلًا فعليًا؛ السعر أكّد الكسر بدل ما يرفضه.",
        "ROUND_NUMBER_REJECTION": "الرقم الدائري لم يرفض السعر بقوة. السوق قبل التداول حول/بعد الرقم، لذلك المستوى النفسي كان ضعيفًا.",
        "ORDER_BLOCK_RETEST": "منطقة الأوردر بلوك تم امتصاصها. بدل ما تعطي ردة فعل، السعر تداول خلالها وأبطلها.",
        "BOS_CHOCH_RETEST": "تبدل البنية لم يتأكد. إعادة الاختبار فشلت والسوق ألغى البنية المفترضة.",
        "EQUAL_LIQUIDITY_SWEEP": "جمع السيولة من القمم/القيعان المتساوية لم يعطِ انعكاس؛ السعر كمل بعد أخذ السيولة.",
        "TRENDLINE_PULLBACK": "رجوع الترند/القناة فشل؛ السعر لم يعد يحترم البنية المصغرة.",
        "WICK_REJECTION": "ذيل الرفض لم يحصل على متابعة. الشمعة التالية ابتلعت الرفض وقبلت بعكسه.",
        "WICK_REJECTION_CONFLUENCE": "فشل الالتقاء: المنطقة/السيولة التي ظهر عندها الذيل تم امتصاصها، والسعر قبل الرجوع داخلها، لذلك لم يكن الذيل رفضًا حقيقيًا.",
        "MOMENTUM_CONTINUATION": "الزخم كان يضعف أو مستهلكًا. الدخول تبع ضغط لم يستمر داخل شمعة الصفقة.",
        "STRONG_TREND_CONTINUATION": "الترند القوي توقف أو دخل بتصحيح أثناء شمعة الدخول، لذلك لم تظهر المتابعة المتوقعة.",
        "OVEREXTENSION_REVERSAL": "الحركة لم تكن استهلاكًا؛ كانت تسارعًا حقيقيًا، لذلك الدخول العكسي كان مبكرًا.",
        "COMPRESSION_BREAKOUT": "الخروج بعد الهدوء كان كسرًا وهميًا، والسعر رجع عكس الاختراق بدل المتابعة.",
        "MOOD_SHIFT": "تبدل المزاج لم يكن ثابتًا؛ كان تصحيحًا مؤقتًا ثم عاد الضغط السابق."
    }.get(kind, "السوق أبطل فكرة الدخول الأصلية خلال شمعة الصفقة.")
    if body_ratio < 0.22 and max(upper_wick, lower_wick) > 0.45:
        base += " كذلك جسم الشمعة كان ضعيفًا مع ذيل واضح، وهذا يدل على تردد/امتصاص وليس تأكيد نظيف."
    return base


def build_trading_room_admin_result_reason(trade: dict, win: bool | None, candle_open: float, candle_close: float, lang: str = "ar", candle_high=None, candle_low=None) -> str:
    """سبب الربح/الخسارة يظهر للأدمن فقط بصياغة تحليلية مفيدة وليس فقط لون الشمعة."""
    try:
        direction = str(trade.get("direction") or "")
        setup = str(trade.get("setup") or trade.get("setup_kind") or "غير محدد")
        setup_kind = str(trade.get("setup_kind") or "UNKNOWN")
        o = float(candle_open)
        c = float(candle_close)
        try:
            h = float(candle_high) if candle_high is not None else max(o, c)
            l = float(candle_low) if candle_low is not None else min(o, c)
        except Exception:
            h, l = max(o, c), min(o, c)
        candle_dir_ar = "خضراء صاعدة" if c > o else "حمراء هابطة" if c < o else "دوجي / تعادل"
        candle_dir_en = "green bullish" if c > o else "red bearish" if c < o else "doji / draw"
        deep = _trading_room_admin_result_deep_text(trade, win, o, c, h, l, lang)
        if lang == "en":
            status = "draw" if win is None else "won" if win else "lost"
            return (
                "🧠 Admin result analysis\n"
                f"Result: {status}\n"
                f"Original setup: {setup} ({setup_kind})\n"
                f"Market explanation: {deep}\n"
                f"Trade candle: {candle_dir_en}\n"
                f"O/H/L/C: {o:.5f} / {h:.5f} / {l:.5f} / {c:.5f}"
            )
        status = "تعادل" if win is None else "ربحت" if win else "خسرت"
        return (
            "🧠 تحليل سبب النتيجة للأدمن\n"
            f"النتيجة: {status}\n"
            f"منطق الدخول الأصلي: {setup} ({setup_kind})\n"
            f"التفسير السوقي: {deep}\n"
            f"شمعة الصفقة: {candle_dir_ar}\n"
            f"O/H/L/C: {o:.5f} / {h:.5f} / {l:.5f} / {c:.5f}"
        )
    except Exception:
        return "🧠 سبب النتيجة غير متوفر حاليًا." if lang != "en" else "🧠 Result reason is not available right now."

def _trading_room_loss_units_for_trade(trade: dict) -> int:
    try:
        if bool((trade or {}).get("recovery_trade")):
            return max(1, int(round(float(TRADING_ROOM_RECOVERY_MULTIPLIER))))
    except Exception:
        pass
    return 1


def _get_otc_rows_and_candles(symbol: str):
    try:
        with quotex_otc_feed.lock:
            rows = list(quotex_otc_feed.prices.get(symbol, []))
            last_tick = dict(quotex_otc_feed.last_tick.get(symbol) or {})
            candles_map = dict((quotex_otc_feed.candles.get(symbol) or {}))
        candles = [dict(c) for _, c in sorted(candles_map.items())]
        return rows, last_tick, candles
    except Exception:
        return [], {}, []


def analyze_pair_for_trading_room(pair: str, symbol: str) -> dict | None:
    """قراءة خفيفة لاختيار زوج مناسب لجلسة واحدة، وليس لتوليد إشارات المستخدمين."""
    rows, last_tick, candles = _get_otc_rows_and_candles(symbol)
    if len(rows) < TRADING_ROOM_MIN_TICKS or not last_tick or len(candles) < TRADING_ROOM_MIN_CANDLES:
        return None

    try:
        current_price = float(last_tick.get("price"))
    except Exception:
        return None

    # لا نختار زوج للجلسة إذا آخر tick قديم.
    # هذا كان سبب التخبيص: يختار زوجًا ثم يغيّره فورًا لأن البيانات قديمة.
    try:
        tick_ts = float(last_tick.get("time") or last_tick.get("ts") or last_tick.get("timestamp") or 0)
        if tick_ts > 1e12:
            tick_ts = tick_ts / 1000.0
        if not tick_ts or time_module.time() - tick_ts > TRADING_ROOM_PAIR_TICK_MAX_AGE_SECONDS:
            return None
    except Exception:
        return None

    instrument = quotex_otc_feed.instrument(symbol)
    payout = int(instrument.get("payout", 0) or 0) if instrument else 0
    if instrument and payout and payout < OTC_LIVE_MIN_PAYOUT:
        return None

    recent_ticks = rows[-45:] if len(rows) >= 45 else rows
    prices = [float(r[1]) for r in recent_ticks]
    if len(prices) < 12:
        return None

    tick_range = max(prices) - min(prices)
    if tick_range <= 0:
        return None
    tick_change = prices[-1] - prices[0]
    up_moves = sum(1 for a, b in zip(prices, prices[1:]) if b > a)
    down_moves = sum(1 for a, b in zip(prices, prices[1:]) if b < a)
    total_moves = max(1, up_moves + down_moves)
    pressure = (up_moves - down_moves) / total_moves
    direction = "CALL" if tick_change > 0 and pressure > 0.08 else "PUT" if tick_change < 0 and pressure < -0.08 else None

    recent_closed = candles[-7:-1] if len(candles) >= 7 else candles[:-1]
    if len(recent_closed) < 3:
        return None

    bodies = []
    ranges = []
    candle_dirs = []
    for c in recent_closed[-6:]:
        try:
            o = float(c.get("open")); h = float(c.get("high")); l = float(c.get("low")); cl = float(c.get("close"))
        except Exception:
            continue
        rng = max(h - l, 0.0)
        body = abs(cl - o)
        if rng > 0:
            ranges.append(rng)
            bodies.append(body / rng)
        candle_dirs.append(1 if cl > o else -1 if cl < o else 0)

    if not ranges:
        return None

    avg_body = sum(bodies) / max(1, len(bodies))
    rhythm = abs(sum(candle_dirs[-4:])) / max(1, len(candle_dirs[-4:]))
    noise = 1.0 - min(abs(pressure), 1.0)
    momentum = min(abs(tick_change) / tick_range, 1.0)

    # استراتيجية الجلسة: إذا السوق ناعم وواضح نختار استمرار، وإذا فيه تبادل شموع نراقب رد الفعل.
    if direction and rhythm >= 0.50 and avg_body >= 0.38:
        strategy = "استمرار زخم قصير"
        strategy_type = "continuation"
    elif rhythm < 0.50 and avg_body >= 0.28 and direction:
        strategy = "اقتناص ارتداد قصير"
        strategy_type = "reaction"
    else:
        strategy = "مراقبة فقط"
        strategy_type = "watch"

    score = int(round(momentum * 35 + abs(pressure) * 30 + avg_body * 20 + rhythm * 10 + min(payout or 80, 95) / 95 * 5))
    if strategy_type == "watch":
        score -= 8
    score = max(0, min(100, score))

    if score < TRADING_ROOM_MIN_PAIR_SCORE or not direction:
        return None

    return {
        "pair": pair,
        "symbol": symbol,
        "score": score,
        "price": current_price,
        "direction_hint": direction,
        "payout": payout,
        "strategy": strategy,
        "strategy_type": strategy_type,
        "momentum": round(momentum, 2),
        "pressure": round(pressure, 2),
        "avg_body": round(avg_body, 2),
        "rhythm": round(rhythm, 2),
        "noise": round(noise, 2),
    }


def select_trading_room_pair() -> dict | None:
    pair_map = get_otc_analysis_pair_map()
    candidates = []
    for pair, symbol in pair_map.items():
        normalized = normalize_otc_currency_pair_name(pair, symbol)
        if not normalized or not is_valid_otc_currency_pair_name(normalized):
            continue
        result = analyze_pair_for_trading_room(normalized, symbol)
        if result:
            candidates.append(result)
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x.get("score", 0), int(x.get("payout", 0) or 0)), reverse=True)
    return candidates[0]




def _trading_room_market_mood_from_metrics(momentum: float, pressure: float, rhythm: float, avg_body: float, noise: float) -> str:
    try:
        if noise >= 0.82 or abs(pressure) < 0.08:
            return "متذبذب"
        if rhythm >= 0.62 and avg_body >= 0.34 and momentum >= 0.42:
            return "ترندي"
        if rhythm < 0.48 and avg_body >= 0.24 and momentum >= 0.30:
            return "ارتدادي"
        if momentum < 0.20 or avg_body < 0.18:
            return "ميت"
        return "متوسط"
    except Exception:
        return "غير واضح"


def assess_trading_room_pair_health(state: dict) -> dict:
    """يقيّم هل الزوج الحالي ما زال مناسبًا للجلسة.
    هذه الدالة لا تعطي صفقات، فقط تعطي قرار وعي للجلسة.
    """
    pair = state.get("pair")
    symbol = state.get("symbol")
    if not pair or not symbol:
        return {"health": 0, "label": "bad", "mood": "غير محدد", "reason": "لا يوجد زوج محدد"}

    rows, last_tick, candles = _get_otc_rows_and_candles(symbol)
    now_ts = time_module.time()
    if len(rows) < 20 or not last_tick or len(candles) < 3:
        return {"health": 20, "label": "bad", "mood": "بيانات ضعيفة", "reason": "بيانات الزوج غير كافية"}

    try:
        tick_ts = float(last_tick.get("time") or last_tick.get("ts") or last_tick.get("timestamp") or 0)
        if tick_ts > 1e12:
            tick_ts = tick_ts / 1000.0
        age = now_ts - tick_ts if tick_ts else 999
    except Exception:
        age = 999
    if age > TRADING_ROOM_PAIR_STALE_SECONDS:
        return {"health": 25, "label": "bad", "mood": "متوقف", "reason": f"آخر tick قديم منذ {int(age)} ثانية"}

    try:
        sample = rows[-35:]
        prices = [float(r[1]) for r in sample]
        rng = max(prices) - min(prices)
        if rng <= 0:
            return {"health": 25, "label": "bad", "mood": "ميت", "reason": "الحركة شبه ثابتة"}
        change = prices[-1] - prices[0]
        up = sum(1 for a, b in zip(prices, prices[1:]) if b > a)
        down = sum(1 for a, b in zip(prices, prices[1:]) if b < a)
        total = max(1, up + down)
        pressure = (up - down) / total
        momentum = min(abs(change) / rng, 1.0)

        recent_closed = candles[-7:-1] if len(candles) >= 7 else candles[:-1]
        bodies = []
        dirs = []
        wick_noise = 0
        for c in recent_closed[-6:]:
            o = float(c.get("open")); h = float(c.get("high")); l = float(c.get("low")); cl = float(c.get("close"))
            cr = max(h - l, 0.0)
            if cr <= 0:
                continue
            body = abs(cl - o) / cr
            upper = (h - max(o, cl)) / cr
            lower = (min(o, cl) - l) / cr
            bodies.append(body)
            dirs.append(1 if cl > o else -1 if cl < o else 0)
            if upper > 0.45 or lower > 0.45 or body < 0.16:
                wick_noise += 1
        avg_body = sum(bodies) / max(1, len(bodies))
        rhythm = abs(sum(dirs[-4:])) / max(1, len(dirs[-4:])) if dirs else 0
        noise = 1.0 - min(abs(pressure), 1.0)
        mood = _trading_room_market_mood_from_metrics(momentum, pressure, rhythm, avg_body, noise)

        health = int(round(
            momentum * 28 + abs(pressure) * 26 + avg_body * 20 + rhythm * 16 + max(0, 10 - wick_noise * 2)
        ))

        reason_parts = []
        if wick_noise >= 4:
            health -= 14
            reason_parts.append("ذيول/دوجي كثيرة")
        if noise >= 0.86:
            health -= 12
            reason_parts.append("ضغط الحركة غير واضح")
        if momentum < 0.18:
            health -= 10
            reason_parts.append("حركة ضعيفة")

        # لا نكرر نفس النمط الخاسر فورًا إذا السوق لم يتغير.
        last_loss_setup = state.get("last_loss_setup")
        current_strategy = state.get("strategy_type")
        if last_loss_setup and current_strategy and str(last_loss_setup) == str(current_strategy):
            health -= 10
            reason_parts.append("نفس النمط السابق الخاسر")

        # طول الانتظار بدون دخول يقلل صحة الزوج.
        no_entry_scans = int(state.get("no_entry_scans", 0) or 0)
        if no_entry_scans >= TRADING_ROOM_PAIR_NO_ENTRY_SWITCH_SCANS:
            health -= 18
            reason_parts.append("انتظار طويل بدون فرصة")

        health = max(0, min(100, health))
        label = "good" if health >= TRADING_ROOM_PAIR_WEAK_HEALTH else "weak" if health >= TRADING_ROOM_PAIR_BAD_HEALTH else "bad"
        reason = "، ".join(reason_parts) if reason_parts else f"سلوك الزوج {mood} وصالح للمراقبة"
        return {
            "health": health,
            "label": label,
            "mood": mood,
            "reason": reason,
            "momentum": round(momentum, 2),
            "pressure": round(pressure, 2),
            "avg_body": round(avg_body, 2),
            "rhythm": round(rhythm, 2),
        }
    except Exception as e:
        return {"health": 20, "label": "bad", "mood": "خطأ", "reason": f"تعذر تقييم الزوج: {e}"}


def select_trading_room_pair_for_brain(state: dict) -> dict | None:
    """اختيار زوج جديد مع استبعاد الزوج الحالي والأزواج التي فشلت داخل نفس الجلسة."""
    pair_map = get_otc_analysis_pair_map()
    current_symbol = state.get("symbol")
    bad_symbols = set(state.get("bad_symbols") or [])
    candidates = []
    for pair, symbol in pair_map.items():
        if symbol == current_symbol or symbol in bad_symbols:
            continue
        normalized = normalize_otc_currency_pair_name(pair, symbol)
        if not normalized or not is_valid_otc_currency_pair_name(normalized):
            continue
        result = analyze_pair_for_trading_room(normalized, symbol)
        if result:
            candidates.append(result)
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x.get("score", 0), int(x.get("payout", 0) or 0)), reverse=True)
    return candidates[0]


def trading_room_should_switch_pair(state: dict, health_info: dict) -> tuple[bool, str]:
    """قرار تغيير الزوج يجب أن يكون هادئًا وليس عصبيًا.
    لا نغيّر الزوج بسبب قراءة واحدة، ولا فورًا بعد اختيار الزوج، ولا كل عدة ثواني.
    """
    if state.get("waiting_pair_selection") or state.get("waiting_result"):
        return False, ""

    now_ts = time_module.time()
    selected_at = float(state.get("pair_selected_at", 0) or 0)
    if selected_at and now_ts - selected_at < TRADING_ROOM_PAIR_MIN_OBSERVE_SECONDS:
        return False, ""

    last_switch_at = float(state.get("last_pair_switch_at", 0) or 0)
    if last_switch_at and now_ts - last_switch_at < TRADING_ROOM_PAIR_SWITCH_COOLDOWN_SECONDS:
        return False, ""

    if int(state.get("pair_switches", 0) or 0) >= TRADING_ROOM_PAIR_MAX_SWITCHES:
        return False, "تم الوصول لحد تغيير الأزواج داخل الجلسة"

    label = health_info.get("label")
    reason = str(health_info.get("reason") or "")

    bad_condition = False
    if label == "bad":
        bad_condition = True
    elif state.get("recovery_mode") and label == "weak":
        # في التعويض نكون أصرم، لكن أيضًا نطلب تكرار الضعف أكثر من مرة.
        bad_condition = True

    if not bad_condition:
        state["pair_bad_scans"] = 0
        return False, ""

    bad_scans = int(state.get("pair_bad_scans", 0) or 0) + 1
    state["pair_bad_scans"] = bad_scans
    if bad_scans < TRADING_ROOM_PAIR_BAD_CONFIRM_SCANS:
        return False, ""

    return True, reason or "سلوك الزوج أصبح ضعيفًا أكثر من مرة"


async def trading_room_switch_pair_if_needed(context: ContextTypes.DEFAULT_TYPE, admin_id: int, state: dict) -> bool:
    health = assess_trading_room_pair_health(state)
    state["pair_health"] = int(health.get("health", 0) or 0)
    state["pair_health_label"] = health.get("label")
    state["market_mood"] = health.get("mood")
    state["pair_health_reason"] = health.get("reason")

    should_switch, reason = trading_room_should_switch_pair(state, health)
    if not should_switch:
        return False

    old_pair = state.get("pair")
    old_symbol = state.get("symbol")
    if old_symbol:
        bad_symbols = set(state.get("bad_symbols") or [])
        bad_symbols.add(old_symbol)
        state["bad_symbols"] = list(bad_symbols)

    new_pair = select_trading_room_pair_for_brain(state)
    if not new_pair:
        state["last_reason"] = (f"Current pair is weak: {reason}. I could not find a cleaner replacement right now." if get_user_language(admin_id) == "en" else f"الزوج الحالي ضعيف: {reason}. لم أجد بديلًا أنظف الآن.")
        last_notice = float(state.get("last_brain_notice_at", 0) or 0)
        if time_module.time() - last_notice >= TRADING_ROOM_BRAIN_NOTICE_COOLDOWN_SECONDS:
            state["last_brain_notice_at"] = time_module.time()
            await safe_send_message(context.bot,
                chat_id=admin_id,
                text=(
                    f"The current pair has become weak: {old_pair}\n"
                    "I will keep monitoring without random entries"
                    if get_user_language(admin_id) == "en" else
                    f"الزوج الحالي أصبح ضعيفًا: {old_pair}\n"
                    "سأستمر بالمراقبة بدون دخول عشوائي"
                ),
                reply_markup=get_trading_room_active_keyboard(admin_id)
            )
        return False

    state.update(new_pair)
    state["pair_switches"] = int(state.get("pair_switches", 0) or 0) + 1
    state["last_pair_switch_at"] = time_module.time()
    state["pair_selected_at"] = time_module.time()
    state["pair_bad_scans"] = 0
    state["no_entry_scans"] = 0
    state["last_reason"] = (f"Pair changed by trading room decision: {reason}" if get_user_language(admin_id) == "en" else f"تم تغيير الزوج بقرار غرفة التداول: {reason}")
    state["pair_health"] = None
    state["pair_health_label"] = None
    state["market_mood"] = None
    await safe_send_message(context.bot,
        chat_id=admin_id,
        text=("🔄 I will switch the session pair." if get_user_language(admin_id) == "en" else "🔄 سأغيّر الزوج للجلسة."),
        reply_markup=get_trading_room_active_keyboard(admin_id)
    )
    await safe_send_message(context.bot,
        chat_id=admin_id,
        text=build_trading_room_selected_pair_message(new_pair['pair'], get_user_language(admin_id)),
        reply_markup=get_trading_room_active_keyboard(admin_id)
    )
    return True

def get_trading_room_market_data_status() -> dict:
    """يعطي حالة بيانات OTC Live التي تحتاجها غرفة الجلسة قبل اختيار زوج."""
    try:
        pair_map = get_otc_analysis_pair_map()
        symbols = list(dict.fromkeys(pair_map.values()))
        connected = bool(getattr(quotex_otc_feed, "connected", False)) if "quotex_otc_feed" in globals() else False
        started = bool(getattr(quotex_otc_feed, "started", False)) if "quotex_otc_feed" in globals() else False

        tick_ready = 0
        candle_ready = 0
        candidate_ready = 0
        latest_age = None
        latest_symbol = None
        now_ts = time_module.time()

        for pair, symbol in pair_map.items():
            rows, last_tick, candles = _get_otc_rows_and_candles(symbol)
            if len(rows) >= TRADING_ROOM_MIN_TICKS:
                tick_ready += 1
            if len(candles) >= TRADING_ROOM_MIN_CANDLES:
                candle_ready += 1
            if len(rows) >= TRADING_ROOM_MIN_TICKS and len(candles) >= TRADING_ROOM_MIN_CANDLES and last_tick:
                candidate_ready += 1
            try:
                ts = float(last_tick.get("time") or last_tick.get("ts") or last_tick.get("timestamp") or 0)
                if ts > 1e12:
                    ts = ts / 1000.0
                if ts > 0:
                    age = max(0.0, now_ts - ts)
                    if latest_age is None or age < latest_age:
                        latest_age = age
                        latest_symbol = symbol
            except Exception:
                pass

        try:
            best = select_trading_room_pair()
        except Exception:
            best = None

        return {
            "connected": connected,
            "started": started,
            "total_symbols": len(symbols),
            "tick_ready": tick_ready,
            "candle_ready": candle_ready,
            "candidate_ready": candidate_ready,
            "latest_age": latest_age,
            "latest_symbol": latest_symbol,
            "best": best,
        }
    except Exception as e:
        return {"error": str(e)}


def build_trading_room_market_data_status_message() -> str:
    status = get_trading_room_market_data_status()
    if status.get("error"):
        return f"🩺 فحص بيانات OTC Live\n\nتعذر قراءة الحالة: {status.get('error')}"

    latest_age = status.get("latest_age")
    latest_text = "لا يوجد tick حديث" if latest_age is None else f"منذ {int(latest_age)} ثانية"
    best = status.get("best")
    best_text = f"{best.get('pair')} | قراءة {best.get('score')}%" if best else "لا يوجد زوج جاهز حاليًا"

    return (
        "🩺 فحص بيانات OTC Live\n"
        "━━━━━━━━━━━━━━\n"
        f"WebSocket: {'متصل ✅' if status.get('connected') else 'غير متصل ❌'}\n"
        f"Live Feed Started: {'نعم ✅' if status.get('started') else 'لا ❌'}\n"
        f"آخر tick: {latest_text}\n"
        f"آخر رمز نشط: {status.get('latest_symbol') or 'غير متوفر'}\n"
        f"الأزواج المتابعة: {status.get('total_symbols', 0)}\n"
        f"أزواج عندها ticks كافية: {status.get('tick_ready', 0)}\n"
        f"أزواج عندها شموع كافية: {status.get('candle_ready', 0)}\n"
        f"أزواج جاهزة مبدئيًا للجلسة: {status.get('candidate_ready', 0)}\n"
        f"أفضل زوج الآن: {best_text}\n\n"
        "إذا الأرقام قليلة بعد Restart، انتظر دقيقة أو دقيقتين ثم أعد بدء الجلسة."
    )


async def trading_room_pair_select_retry_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    admin_id = int(data.get("admin_id") or 0)
    lang = get_user_language(admin_id)
    en = lang == "en"
    state = get_trading_room_state(context, admin_id)
    if not state or not state.get("active") or not state.get("waiting_pair_selection"):
        return

    retries = int(state.get("pair_select_retries", 0) or 0) + 1
    state["pair_select_retries"] = retries
    selected = select_trading_room_pair()

    if selected:
        state.update(selected)
        state["waiting_pair_selection"] = False
        state["pair_selected_at"] = time_module.time()
        state["pair_bad_scans"] = 0
        state["last_reason"] = "Session pair selected after market data became ready" if en else "تم اختيار زوج الجلسة بعد تجهيز البيانات"
        await safe_send_message(context.bot,
            chat_id=admin_id,
            text=(
                ("✅ Market data is ready\n\n" if en else "✅ أصبحت بيانات السوق جاهزة\n\n")
                + build_trading_room_selected_pair_message(selected['pair'], lang)
            ),
            reply_markup=get_trading_room_active_keyboard(admin_id)
        )
        try:
            context.job_queue.run_repeating(
                trading_room_scan_job,
                interval=TRADING_ROOM_SCAN_INTERVAL_SECONDS,
                first=2,
                data={"admin_id": admin_id},
                name=f"trading_room_scan_{admin_id}_{int(time_module.time())}",
            )
        except Exception as e:
            logger.exception("Could not start trading room scan job after retry: %s", e)
        return

    if retries >= TRADING_ROOM_DATA_MAX_RETRIES:
        state["active"] = False
        state["waiting_pair_selection"] = False
        await safe_send_message(context.bot,
            chat_id=admin_id,
            text=(
                "❌ I could not find a suitable pair for this session after checking again.\n\n"
                "Live data is not enough or the market is unclear right now. Try again later."
                if en else
                "❌ لم أجد زوجًا مناسبًا للجلسة بعد إعادة الفحص.\n\n"
                "البيانات الحية غير كافية أو السوق غير واضح حاليًا. جرّب بعد قليل."
            ),
            reply_markup=get_trading_room_menu_keyboard(admin_id)
        )
        return

    await safe_send_message(context.bot,
        chat_id=admin_id,
        text=("⏳ Preparing market data..." if en else "⏳ جاري تجهيز بيانات السوق..."),
        reply_markup=get_trading_room_active_keyboard(admin_id)
    )
    try:
        context.job_queue.run_once(
            trading_room_pair_select_retry_job,
            when=TRADING_ROOM_DATA_RETRY_SECONDS,
            data={"admin_id": admin_id},
            name=f"trading_room_pair_retry_{admin_id}_{int(time_module.time())}",
        )
    except Exception as e:
        logger.exception("Could not schedule trading room pair retry job: %s", e)


async def trading_room_start_market_flow(context: ContextTypes.DEFAULT_TYPE, admin_id: int):
    lang = get_user_language(admin_id)
    en = lang == "en"
    state = get_trading_room_state(context, admin_id)
    if not state or not state.get("active") or not state.get("ready_confirmed"):
        return

    selected = select_trading_room_pair()
    if not selected:
        state["waiting_pair_selection"] = True
        state["pair_select_retries"] = 0
        state["last_reason"] = "Waiting for OTC Live market data" if en else "بانتظار تجهيز بيانات OTC Live"
        await safe_send_message(
            context.bot,
            chat_id=admin_id,
            text=("⏳ Preparing market data..." if en else "⏳ جاري تجهيز بيانات السوق..."),
            reply_markup=get_trading_room_active_keyboard(admin_id),
        )
        try:
            context.job_queue.run_once(
                trading_room_pair_select_retry_job,
                when=TRADING_ROOM_DATA_RETRY_SECONDS,
                data={"admin_id": admin_id},
                name=f"trading_room_pair_retry_{admin_id}_{int(time_module.time())}",
            )
        except Exception as e:
            logger.exception("Could not schedule trading room pair retry job: %s", e)
        return

    state.update(selected)
    state["pair_selected_at"] = time_module.time()
    state["pair_bad_scans"] = 0
    state["last_reason"] = "Session pair selected" if en else "تم اختيار زوج الجلسة"
    await safe_send_message(
        context.bot,
        chat_id=admin_id,
        text=("✅ Market data is ready\n\n" if en else "✅ أصبحت بيانات السوق جاهزة\n\n") + build_trading_room_selected_pair_message(selected["pair"], lang),
        reply_markup=get_trading_room_active_keyboard(admin_id),
    )
    try:
        context.job_queue.run_repeating(
            trading_room_scan_job,
            interval=TRADING_ROOM_SCAN_INTERVAL_SECONDS,
            first=2,
            data={"admin_id": admin_id},
            name=f"trading_room_scan_{admin_id}_{int(time_module.time())}",
        )
    except Exception as e:
        logger.exception("Could not start trading room scan job: %s", e)


async def trading_room_begin_market_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    admin_id = int(data.get("admin_id") or 0)
    await trading_room_start_market_flow(context, admin_id)


async def trading_room_half_hour_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    admin_id = int(data.get("admin_id") or 0)
    if not admin_id:
        return
    lang = get_user_language(admin_id)
    await safe_send_message(
        context.bot,
        chat_id=admin_id,
        text=(
            "⏰ 30 minutes have passed. If you are calm and ready, you can start a new session. Stay committed to the plan and avoid rushing."
            if lang == "en" else
            "⏰ مرّت نصف ساعة. إذا كنت هادئًا ومستعدًا تقدر تبدأ جلسة جديدة، والأفضل تلتزم بالخطة بدون استعجال."
        ),
        reply_markup=get_trading_room_menu_keyboard(admin_id),
    )


def _trading_room_market_structure_context(closed_parts: list[dict], current_parts: dict, price: float, m10: dict, m20: dict, m35: dict) -> dict:
    """Smart-structure context for Trading Room entries.

    This is intentionally lightweight and works only from cached OTC ticks/candles:
    support/resistance zones, round numbers, retest, fake breakout, wick rejection, trend bias, BOS/CHOCH,
    simple order-block/FVG approximations, equal-high/low liquidity, and pullback continuation.
    """
    ctx = {
        "support": None,
        "resistance": None,
        "atr": 0.0,
        "trend_bias": None,
        "near_support": False,
        "near_resistance": False,
        "support_rejection": False,
        "resistance_rejection": False,
        "fake_breakout_up": False,
        "fake_breakout_down": False,
        "retest_bullish": False,
        "retest_bearish": False,
        "fvg_bullish_retest": False,
        "fvg_bearish_retest": False,
        "round_near": False,
        "round_level": None,
        "round_bullish_rejection": False,
        "round_bearish_rejection": False,
        "bos_bullish_retest": False,
        "bos_bearish_retest": False,
        "choch_bullish": False,
        "choch_bearish": False,
        "order_block_bullish_retest": False,
        "order_block_bearish_retest": False,
        "equal_low_sweep": False,
        "equal_high_sweep": False,
        "trendline_pullback_bullish": False,
        "trendline_pullback_bearish": False,
        "micro_double_bottom": False,
        "micro_double_top": False,
        "liquidity_sweep_low": False,
        "liquidity_sweep_high": False,
        "quality": 0,
    }
    try:
        parts = [c for c in (closed_parts or []) if c and c.get("range", 0) > 0]
        if len(parts) < 5:
            return ctx

        recent = parts[-12:]
        ranges = [float(c.get("range", 0) or 0) for c in recent if float(c.get("range", 0) or 0) > 0]
        atr = sum(ranges) / max(1, len(ranges))
        ctx["atr"] = atr
        if atr <= 0:
            return ctx

        highs = [float(c["high"]) for c in recent]
        lows = [float(c["low"]) for c in recent]
        closes = [float(c["close"]) for c in recent]
        opens = [float(c["open"]) for c in recent]
        cp = current_parts or {}
        cur_high = float(cp.get("high", price) or price)
        cur_low = float(cp.get("low", price) or price)
        cur_close = float(cp.get("close", price) or price)

        # nearest active levels from recent candle structure, excluding the current forming candle
        below_levels = [x for x in lows[-10:] + closes[-10:] if x <= price]
        above_levels = [x for x in highs[-10:] + closes[-10:] if x >= price]
        support = max(below_levels) if below_levels else min(lows)
        resistance = min(above_levels) if above_levels else max(highs)
        ctx["support"] = support
        ctx["resistance"] = resistance

        zone = max(atr * 0.38, abs(price) * 0.000015)
        ctx["near_support"] = abs(price - support) <= zone or (cur_low <= support + zone and cur_close >= support - zone)
        ctx["near_resistance"] = abs(price - resistance) <= zone or (cur_high >= resistance - zone and cur_close <= resistance + zone)

        # Trend structure from highs/lows and closes, not only ticks.
        old_mid = (sum(closes[-10:-5]) / max(1, len(closes[-10:-5]))) if len(closes) >= 10 else closes[0]
        new_mid = sum(closes[-5:]) / max(1, len(closes[-5:]))
        higher_lows = lows[-1] > min(lows[-5:-1]) if len(lows) >= 5 else False
        lower_highs = highs[-1] < max(highs[-5:-1]) if len(highs) >= 5 else False
        if new_mid > old_mid and m20.get("pressure", 0) > -0.08 and (m35.get("change", 0) > 0 or higher_lows):
            ctx["trend_bias"] = "CALL"
        elif new_mid < old_mid and m20.get("pressure", 0) < 0.08 and (m35.get("change", 0) < 0 or lower_highs):
            ctx["trend_bias"] = "PUT"

        # Rejection/sweep at support/resistance from current candle + last closed candle.
        last = parts[-1]
        lower_reject = (cp.get("lower_wick", 0) >= 0.34 or last.get("lower_wick", 0) >= 0.40) and cur_close >= support
        upper_reject = (cp.get("upper_wick", 0) >= 0.34 or last.get("upper_wick", 0) >= 0.40) and cur_close <= resistance
        ctx["support_rejection"] = bool(ctx["near_support"] and lower_reject and m10.get("pressure", 0) > -0.16)
        ctx["resistance_rejection"] = bool(ctx["near_resistance"] and upper_reject and m10.get("pressure", 0) < 0.16)

        prev_low = min(lows[-8:-1]) if len(lows) >= 8 else min(lows[:-1])
        prev_high = max(highs[-8:-1]) if len(highs) >= 8 else max(highs[:-1])
        ctx["liquidity_sweep_low"] = bool(cur_low < prev_low and cur_close > prev_low and m10.get("pressure", 0) > -0.05)
        ctx["liquidity_sweep_high"] = bool(cur_high > prev_high and cur_close < prev_high and m10.get("pressure", 0) < 0.05)
        ctx["fake_breakout_down"] = bool(ctx["liquidity_sweep_low"] and cp.get("lower_wick", 0) >= 0.28)
        ctx["fake_breakout_up"] = bool(ctx["liquidity_sweep_high"] and cp.get("upper_wick", 0) >= 0.28)

        # Retest of broken level: price returns to old resistance/support and rejects.
        broken_resistance = None
        broken_support = None
        if len(parts) >= 7:
            old_high = max(float(c["high"]) for c in parts[-7:-2])
            old_low = min(float(c["low"]) for c in parts[-7:-2])
            last_close = float(parts[-1]["close"])
            prev_close = float(parts[-2]["close"])
            if prev_close > old_high - zone and last_close >= old_high - zone:
                broken_resistance = old_high
            if prev_close < old_low + zone and last_close <= old_low + zone:
                broken_support = old_low
        if broken_resistance is not None:
            ctx["retest_bullish"] = bool(abs(price - broken_resistance) <= zone * 1.25 and (ctx["support_rejection"] or m10.get("pressure", 0) >= 0.08))
        if broken_support is not None:
            ctx["retest_bearish"] = bool(abs(price - broken_support) <= zone * 1.25 and (ctx["resistance_rejection"] or m10.get("pressure", 0) <= -0.08))

        # Simple FVG/imbalance approximation: 3-candle impulse leaves a zone; later price retests it.
        # We do not draw it, only use it as an extra confirmation.
        for i in range(max(2, len(parts) - 8), len(parts)):
            if i < 2:
                continue
            a, b, c = parts[i-2], parts[i-1], parts[i]
            bullish_impulse = a["high"] < c["low"] and b.get("body_ratio", 0) >= 0.45 and c["dir"] >= 0
            bearish_impulse = a["low"] > c["high"] and b.get("body_ratio", 0) >= 0.45 and c["dir"] <= 0
            if bullish_impulse:
                lo, hi = float(a["high"]), float(c["low"])
                if lo <= price <= hi + zone and m10.get("pressure", 0) > -0.10:
                    ctx["fvg_bullish_retest"] = True
            if bearish_impulse:
                lo, hi = float(c["high"]), float(a["low"])
                if lo - zone <= price <= hi and m10.get("pressure", 0) < 0.10:
                    ctx["fvg_bearish_retest"] = True

        # Round-number reaction (psychological levels). Works generically for OTC prices too.
        try:
            abs_price = abs(float(price))
            if abs_price >= 1000:
                round_step = 10.0
            elif abs_price >= 100:
                round_step = 1.0
            elif abs_price >= 10:
                round_step = 0.10
            elif abs_price >= 1:
                round_step = 0.01
            elif abs_price >= 0.10:
                round_step = 0.001
            elif abs_price >= 0.01:
                round_step = 0.0001
            else:
                round_step = max(abs_price * 0.001, 0.00001)
            nearest_round = round(price / round_step) * round_step if round_step > 0 else price
            round_zone = max(atr * 0.30, round_step * 0.08)
            ctx["round_level"] = nearest_round
            ctx["round_near"] = bool(abs(price - nearest_round) <= round_zone or cur_low <= nearest_round <= cur_high)
            ctx["round_bullish_rejection"] = bool(ctx["round_near"] and cur_low <= nearest_round + round_zone and cur_close > nearest_round and (cp.get("lower_wick", 0) >= 0.26 or m10.get("pressure", 0) > 0.08))
            ctx["round_bearish_rejection"] = bool(ctx["round_near"] and cur_high >= nearest_round - round_zone and cur_close < nearest_round and (cp.get("upper_wick", 0) >= 0.26 or m10.get("pressure", 0) < -0.08))
        except Exception:
            pass

        # BOS/CHOCH approximation from recent swing highs/lows.
        try:
            if len(parts) >= 10:
                swing_high = max(float(c["high"]) for c in parts[-10:-3])
                swing_low = min(float(c["low"]) for c in parts[-10:-3])
                prev_swing_high = max(float(c["high"]) for c in parts[-14:-7]) if len(parts) >= 14 else swing_high
                prev_swing_low = min(float(c["low"]) for c in parts[-14:-7]) if len(parts) >= 14 else swing_low
                last_close = float(parts[-1]["close"])
                prior_close = float(parts[-2]["close"])
                broke_high = last_close > swing_high - zone and prior_close <= swing_high + zone
                broke_low = last_close < swing_low + zone and prior_close >= swing_low - zone
                ctx["bos_bullish_retest"] = bool(broke_high and abs(price - swing_high) <= zone * 1.45 and m10.get("pressure", 0) > -0.12)
                ctx["bos_bearish_retest"] = bool(broke_low and abs(price - swing_low) <= zone * 1.45 and m10.get("pressure", 0) < 0.12)
                ctx["choch_bullish"] = bool(last_close > prev_swing_high and m20.get("change", 0) > 0 and m10.get("pressure", 0) > 0.05)
                ctx["choch_bearish"] = bool(last_close < prev_swing_low and m20.get("change", 0) < 0 and m10.get("pressure", 0) < -0.05)
        except Exception:
            pass

        # Order-block approximation: last opposite candle before impulse, then retest of its body.
        try:
            impulse = [c for c in parts[-8:] if c.get("body_ratio", 0) >= 0.48]
            if impulse and avg_range > 0:
                for j in range(len(parts)-2, max(1, len(parts)-10), -1):
                    c = parts[j]
                    prev = parts[j-1]
                    # bullish OB: last red candle before strong bullish displacement
                    if c.get("dir") > 0 and c.get("body_ratio", 0) >= 0.45 and prev.get("dir") < 0:
                        ob_low = min(float(prev["open"]), float(prev["close"]))
                        ob_high = max(float(prev["open"]), float(prev["close"]))
                        if ob_low - zone <= price <= ob_high + zone and (ctx.get("support_rejection") or cp.get("lower_wick", 0) >= 0.24 or m10.get("pressure", 0) > 0.08):
                            ctx["order_block_bullish_retest"] = True
                            break
                    # bearish OB: last green candle before strong bearish displacement
                    if c.get("dir") < 0 and c.get("body_ratio", 0) >= 0.45 and prev.get("dir") > 0:
                        ob_low = min(float(prev["open"]), float(prev["close"]))
                        ob_high = max(float(prev["open"]), float(prev["close"]))
                        if ob_low - zone <= price <= ob_high + zone and (ctx.get("resistance_rejection") or cp.get("upper_wick", 0) >= 0.24 or m10.get("pressure", 0) < -0.08):
                            ctx["order_block_bearish_retest"] = True
                            break
        except Exception:
            pass

        # Equal highs/lows liquidity sweep + micro double top/bottom.
        try:
            if len(parts) >= 8:
                lows7 = [float(c["low"]) for c in parts[-8:-1]]
                highs7 = [float(c["high"]) for c in parts[-8:-1]]
                low_cluster = min(lows7)
                high_cluster = max(highs7)
                equal_lows = sum(1 for x in lows7 if abs(x - low_cluster) <= zone * 0.85) >= 2
                equal_highs = sum(1 for x in highs7 if abs(x - high_cluster) <= zone * 0.85) >= 2
                ctx["equal_low_sweep"] = bool(equal_lows and cur_low < low_cluster - zone * 0.15 and cur_close > low_cluster and m10.get("pressure", 0) > -0.05)
                ctx["equal_high_sweep"] = bool(equal_highs and cur_high > high_cluster + zone * 0.15 and cur_close < high_cluster and m10.get("pressure", 0) < 0.05)
                ctx["micro_double_bottom"] = bool(equal_lows and ctx.get("near_support") and cur_close > low_cluster and m10.get("pressure", 0) > 0.08)
                ctx["micro_double_top"] = bool(equal_highs and ctx.get("near_resistance") and cur_close < high_cluster and m10.get("pressure", 0) < -0.08)
        except Exception:
            pass

        # Trendline / channel pullback approximation using recent closes.
        try:
            if len(closes) >= 8:
                first_half = sum(closes[-8:-4]) / 4
                second_half = sum(closes[-4:]) / 4
                slope = second_half - first_half
                pullback_zone = max(atr * 0.55, abs(price) * 0.00002)
                rising_structure = slope > 0 and lows[-1] >= min(lows[-5:]) and m20.get("pressure", 0) > -0.10
                falling_structure = slope < 0 and highs[-1] <= max(highs[-5:]) and m20.get("pressure", 0) < 0.10
                ctx["trendline_pullback_bullish"] = bool(rising_structure and (ctx.get("near_support") or ctx.get("round_bullish_rejection") or abs(price - new_mid) <= pullback_zone) and m10.get("pressure", 0) > -0.05)
                ctx["trendline_pullback_bearish"] = bool(falling_structure and (ctx.get("near_resistance") or ctx.get("round_bearish_rejection") or abs(price - new_mid) <= pullback_zone) and m10.get("pressure", 0) < 0.05)
        except Exception:
            pass

        q = 0
        for key in (
            "support_rejection", "resistance_rejection", "fake_breakout_down", "fake_breakout_up",
            "retest_bullish", "retest_bearish", "fvg_bullish_retest", "fvg_bearish_retest",
            "round_bullish_rejection", "round_bearish_rejection", "bos_bullish_retest", "bos_bearish_retest",
            "choch_bullish", "choch_bearish", "order_block_bullish_retest", "order_block_bearish_retest",
            "equal_low_sweep", "equal_high_sweep", "trendline_pullback_bullish", "trendline_pullback_bearish",
            "micro_double_bottom", "micro_double_top"
        ):
            if ctx.get(key):
                q += 10
        if ctx.get("trend_bias"):
            q += 8
        if ctx.get("round_near"):
            q += 4
        ctx["quality"] = min(100, q)
        return ctx
    except Exception:
        return ctx


def analyze_trading_room_entry(state: dict) -> dict:
    """Entry Confirmation Engine لغرفة جلسة التداول فقط.

    الفكرة: لا نقرر CALL/PUT من الزخم فقط. نبني عدة فرص مرشحة، نفلتر الخطير منها،
    ثم نختار أنظف فرصة ونعيد تأكيد توقيت دخول الشمعة القادمة قرب بداية الشمعة فقط.
    """
    pair = state.get("pair")
    symbol = state.get("symbol")
    if not pair or not symbol:
        return {"ok": False, "reason": "لم يتم اختيار زوج للجلسة"}

    rows, last_tick, candles = _get_otc_rows_and_candles(symbol)
    if len(rows) < max(45, TRADING_ROOM_MIN_TICKS) or not last_tick or len(candles) < max(5, TRADING_ROOM_MIN_CANDLES):
        return {"ok": False, "reason": "بيانات الزوج غير كافية الآن"}

    try:
        price = float(last_tick.get("price"))
    except Exception:
        return {"ok": False, "reason": "تعذر قراءة السعر الحالي"}

    try:
        tick_ts = float(last_tick.get("time") or last_tick.get("ts") or last_tick.get("timestamp") or 0)
        if tick_ts > 1e12:
            tick_ts = tick_ts / 1000.0
        if not tick_ts or time_module.time() - tick_ts > TRADING_ROOM_PAIR_TICK_MAX_AGE_SECONDS:
            return {"ok": False, "reason": "آخر tick للزوج قديم"}
    except Exception:
        return {"ok": False, "reason": "تعذر قراءة وقت آخر tick"}

    try:
        instrument = quotex_otc_feed.instrument(symbol) or {}
        payout_at_entry = _normalize_payout_percent(instrument.get("payout", 80), 80.0)
    except Exception:
        payout_at_entry = 80.0

    def _prices_from(n: int):
        sample = rows[-n:] if len(rows) >= n else rows
        return [float(r[1]) for r in sample]

    def _movement_metrics(prices: list[float]) -> dict:
        if len(prices) < 3:
            return {"range": 0.0, "change": 0.0, "pressure": 0.0, "momentum": 0.0, "up": 0, "down": 0, "density": 0.0}
        rng = max(prices) - min(prices)
        change = prices[-1] - prices[0]
        up = sum(1 for a, b in zip(prices, prices[1:]) if b > a)
        down = sum(1 for a, b in zip(prices, prices[1:]) if b < a)
        total = max(1, up + down)
        pressure = (up - down) / total
        momentum = min(abs(change) / rng, 1.0) if rng > 0 else 0.0
        density = (up + down) / max(1, len(prices) - 1)
        return {"range": rng, "change": change, "pressure": pressure, "momentum": momentum, "up": up, "down": down, "density": density}

    prices_60 = _prices_from(60)
    prices_35 = _prices_from(35)
    prices_20 = _prices_from(20)
    prices_10 = _prices_from(10)
    m60 = _movement_metrics(prices_60)
    m35 = _movement_metrics(prices_35)
    m20 = _movement_metrics(prices_20)
    m10 = _movement_metrics(prices_10)
    if m20["range"] <= 0 or m35["range"] <= 0:
        return {"ok": False, "reason": "الحركة ثابتة جدًا"}

    # قراءة الشموع الأخيرة: دوجي/ذيول/اتجاه/استهلاك الحركة.
    closed = candles[-8:-1] if len(candles) >= 8 else candles[:-1]
    current_candle = candles[-1] if candles else {}

    def _candle_parts(c: dict) -> dict:
        try:
            o = float(c.get("open")); h = float(c.get("high")); l = float(c.get("low")); cl = float(c.get("close"))
            rng = max(h - l, 0.0)
            body = abs(cl - o)
            return {
                "open": o, "high": h, "low": l, "close": cl, "range": rng, "body": body,
                "body_ratio": (body / rng if rng > 0 else 0.0),
                "upper_wick": ((h - max(o, cl)) / rng if rng > 0 else 0.0),
                "lower_wick": ((min(o, cl) - l) / rng if rng > 0 else 0.0),
                "dir": 1 if cl > o else -1 if cl < o else 0,
            }
        except Exception:
            return {"open": 0.0, "high": 0.0, "low": 0.0, "close": 0.0, "range": 0.0, "body": 0.0, "body_ratio": 0.0, "upper_wick": 0.0, "lower_wick": 0.0, "dir": 0}

    cp = _candle_parts(current_candle)
    closed_parts = [_candle_parts(c) for c in closed if c]
    if len(closed_parts) < 3:
        return {"ok": False, "reason": "شموع الزوج غير كافية الآن"}

    avg_body = sum(c["body_ratio"] for c in closed_parts[-6:]) / max(1, len(closed_parts[-6:]))
    avg_range = sum(c["range"] for c in closed_parts[-6:]) / max(1, len(closed_parts[-6:]))
    recent_dirs = [c["dir"] for c in closed_parts[-5:]]
    rhythm = abs(sum(recent_dirs)) / max(1, len(recent_dirs))
    doji_count = sum(1 for c in closed_parts[-5:] if c["body_ratio"] <= 0.16)
    wick_heavy_count = sum(1 for c in closed_parts[-5:] if max(c["upper_wick"], c["lower_wick"]) >= 0.62)
    noisy_market = (doji_count >= 3 and rhythm < 0.55) or (wick_heavy_count >= 4 and abs(m20["pressure"]) < 0.28)

    # فلتر اتجاه قوي: إذا السوق صار ترند/مومنتم واضح، ممنوع نعاكسه إلا بعد كسر حقيقي للزخم.
    # هذا يمنع المشكلة التي ظهرت بالتجربة: البوت كان يعتبر الاندفاع فرصة انعكاس ويدخل عكس الترند.
    strong_trend_up = (
        m60["change"] > 0 and m35["change"] > 0 and m20["change"] > 0
        and m60["pressure"] >= 0.16 and m35["pressure"] >= 0.22 and m20["pressure"] >= 0.22
        and m35["momentum"] >= 0.34 and rhythm >= 0.42
        and cp["upper_wick"] < 0.58
    )
    strong_trend_down = (
        m60["change"] < 0 and m35["change"] < 0 and m20["change"] < 0
        and m60["pressure"] <= -0.16 and m35["pressure"] <= -0.22 and m20["pressure"] <= -0.22
        and m35["momentum"] >= 0.34 and rhythm >= 0.42
        and cp["lower_wick"] < 0.58
    )
    market_trend_bias = "CALL" if strong_trend_up else "PUT" if strong_trend_down else None

    seconds = now_utc().astimezone(UTC_PLUS_3).second
    direct_allowed = False  # direct moving-minute entries disabled by request
    # دخول الشمعة القادمة لا يرسل مبكرًا. ننتظر آخر ثواني ونفحص مرة ثانية عمليًا لأن الجوب يكرر كل عدة ثوان.
    next_candle_allowed = seconds >= int(os.getenv("TRADING_ROOM_NEXT_CONFIRM_MIN_SECOND", "54"))

    candidates = []

    def add_candidate(kind: str, direction: str, score: float, preferred_mode: str, setup_ar: str, setup_en: str, extra: dict | None = None):
        if not direction or score <= 0:
            return
        candidates.append({
            "kind": kind,
            "direction": direction,
            "score": int(round(max(0, min(100, score)))),
            "preferred_mode": preferred_mode,
            "setup_ar": setup_ar,
            "setup_en": setup_en,
            "extra": extra or {},
        })

    structure_ctx = _trading_room_market_structure_context(closed_parts, cp, price, m10, m20, m35)
    structure_bias = structure_ctx.get("trend_bias")

    # Smart Money / Market Structure layer:
    # support-resistance retest, liquidity sweep, fake breakout, FVG-like retest.
    # These are not shown to the user; they only improve the internal entry decision.
    if not noisy_market:
        if structure_ctx.get("retest_bullish") or structure_ctx.get("support_rejection") or structure_ctx.get("fvg_bullish_retest"):
            if not strong_trend_down and m10["pressure"] > -0.14:
                score = 66 + min(14, structure_ctx.get("quality", 0) * 0.22) + max(0, m10["pressure"]) * 14 + cp["lower_wick"] * 10
                add_candidate("STRUCTURE_RETEST", "CALL", score, "next_candle", "إعادة اختبار منطقة ورفض سعري صاعد", "Bullish zone retest with price rejection")
        if structure_ctx.get("retest_bearish") or structure_ctx.get("resistance_rejection") or structure_ctx.get("fvg_bearish_retest"):
            if not strong_trend_up and m10["pressure"] < 0.14:
                score = 66 + min(14, structure_ctx.get("quality", 0) * 0.22) + max(0, -m10["pressure"]) * 14 + cp["upper_wick"] * 10
                add_candidate("STRUCTURE_RETEST", "PUT", score, "next_candle", "إعادة اختبار منطقة ورفض سعري هابط", "Bearish zone retest with price rejection")
        if structure_ctx.get("fake_breakout_down") and not strong_trend_down:
            score = 70 + min(12, structure_ctx.get("quality", 0) * 0.18) + max(0, m10["pressure"]) * 16 + cp["lower_wick"] * 10
            add_candidate("LIQUIDITY_SWEEP", "CALL", score, "next_candle", "سحب سيولة أسفل المنطقة ثم رجوع", "Liquidity sweep below support then reclaim")
        if structure_ctx.get("fake_breakout_up") and not strong_trend_up:
            score = 70 + min(12, structure_ctx.get("quality", 0) * 0.18) + max(0, -m10["pressure"]) * 16 + cp["upper_wick"] * 10
            add_candidate("LIQUIDITY_SWEEP", "PUT", score, "next_candle", "سحب سيولة أعلى المنطقة ثم رجوع", "Liquidity sweep above resistance then reject")

    # Round-number rejection: psychological level + rejection/tick pressure.
    if not noisy_market:
        if structure_ctx.get("round_bullish_rejection") and not strong_trend_down:
            score = 67 + min(14, structure_ctx.get("quality", 0) * 0.16) + cp["lower_wick"] * 12 + max(0, m10["pressure"]) * 14
            add_candidate("ROUND_NUMBER_REJECTION", "CALL", score, "next_candle", "رفض سعري من رقم دائري", "Bullish rejection from a round number")
        if structure_ctx.get("round_bearish_rejection") and not strong_trend_up:
            score = 67 + min(14, structure_ctx.get("quality", 0) * 0.16) + cp["upper_wick"] * 12 + max(0, -m10["pressure"]) * 14
            add_candidate("ROUND_NUMBER_REJECTION", "PUT", score, "next_candle", "رفض سعري من رقم دائري", "Bearish rejection from a round number")

    # Broader technical-analysis layer: BOS/CHOCH, order block retest, equal liquidity, trendline pullback, double top/bottom.
    if not noisy_market:
        if (structure_ctx.get("bos_bullish_retest") or structure_ctx.get("choch_bullish")) and not strong_trend_down and m10["pressure"] > -0.12:
            score = 69 + min(13, structure_ctx.get("quality", 0) * 0.15) + max(0, m20["pressure"]) * 12
            add_candidate("BOS_CHOCH_RETEST", "CALL", score, "next_candle", "كسر بنية صاعد مع إعادة اختبار", "Bullish structure break / CHOCH retest")
        if (structure_ctx.get("bos_bearish_retest") or structure_ctx.get("choch_bearish")) and not strong_trend_up and m10["pressure"] < 0.12:
            score = 69 + min(13, structure_ctx.get("quality", 0) * 0.15) + max(0, -m20["pressure"]) * 12
            add_candidate("BOS_CHOCH_RETEST", "PUT", score, "next_candle", "كسر بنية هابط مع إعادة اختبار", "Bearish structure break / CHOCH retest")
        if structure_ctx.get("order_block_bullish_retest") and not strong_trend_down:
            score = 70 + min(14, structure_ctx.get("quality", 0) * 0.17) + cp["lower_wick"] * 10 + max(0, m10["pressure"]) * 12
            add_candidate("ORDER_BLOCK_RETEST", "CALL", score, "next_candle", "إعادة اختبار أوردر بلوك صاعد", "Bullish order-block retest")
        if structure_ctx.get("order_block_bearish_retest") and not strong_trend_up:
            score = 70 + min(14, structure_ctx.get("quality", 0) * 0.17) + cp["upper_wick"] * 10 + max(0, -m10["pressure"]) * 12
            add_candidate("ORDER_BLOCK_RETEST", "PUT", score, "next_candle", "إعادة اختبار أوردر بلوك هابط", "Bearish order-block retest")
        if (structure_ctx.get("equal_low_sweep") or structure_ctx.get("micro_double_bottom")) and not strong_trend_down:
            score = 68 + min(12, structure_ctx.get("quality", 0) * 0.14) + cp["lower_wick"] * 14 + max(0, m10["pressure"]) * 14
            add_candidate("EQUAL_LIQUIDITY_SWEEP", "CALL", score, "next_candle", "سحب سيولة من قيعان متساوية", "Equal lows liquidity sweep / double bottom")
        if (structure_ctx.get("equal_high_sweep") or structure_ctx.get("micro_double_top")) and not strong_trend_up:
            score = 68 + min(12, structure_ctx.get("quality", 0) * 0.14) + cp["upper_wick"] * 14 + max(0, -m10["pressure"]) * 14
            add_candidate("EQUAL_LIQUIDITY_SWEEP", "PUT", score, "next_candle", "سحب سيولة من قمم متساوية", "Equal highs liquidity sweep / double top")
        if structure_ctx.get("trendline_pullback_bullish") and not strong_trend_down:
            score = 66 + min(12, structure_ctx.get("quality", 0) * 0.13) + max(0, m20["pressure"]) * 11 + rhythm * 8
            add_candidate("TRENDLINE_PULLBACK", "CALL", score, "next_candle", "ارتداد مع ترند صاعد", "Bullish trendline/channel pullback")
        if structure_ctx.get("trendline_pullback_bearish") and not strong_trend_up:
            score = 66 + min(12, structure_ctx.get("quality", 0) * 0.13) + max(0, -m20["pressure"]) * 11 + rhythm * 8
            add_candidate("TRENDLINE_PULLBACK", "PUT", score, "next_candle", "ارتداد مع ترند هابط", "Bearish trendline/channel pullback")

    # إذا في ترند قوي ومعه إعادة اختبار، نعزز الدخول مع الاتجاه بدل البحث عن عكسه.
    if not noisy_market and strong_trend_up and (structure_ctx.get("retest_bullish") or structure_ctx.get("support_rejection") or structure_bias == "CALL"):
        score = 76 + min(10, structure_ctx.get("quality", 0) * 0.16) + max(0, m20["pressure"]) * 10
        add_candidate("TREND_RETEST_CONTINUATION", "CALL", score, "next_candle", "ترند صاعد مع إعادة اختبار منطقة", "Bullish trend retest continuation")
    if not noisy_market and strong_trend_down and (structure_ctx.get("retest_bearish") or structure_ctx.get("resistance_rejection") or structure_bias == "PUT"):
        score = 76 + min(10, structure_ctx.get("quality", 0) * 0.16) + max(0, -m20["pressure"]) * 10
        add_candidate("TREND_RETEST_CONTINUATION", "PUT", score, "next_candle", "ترند هابط مع إعادة اختبار منطقة", "Bearish trend retest continuation")

    # 1) استمرار زخم نظيف: مسموح فقط إذا الحركة ليست مستهلكة بشكل مبالغ.
    overextended_up = m35["change"] > 0 and m35["momentum"] >= 0.88 and m20["momentum"] >= 0.82 and cp["upper_wick"] < 0.18
    overextended_down = m35["change"] < 0 and m35["momentum"] >= 0.88 and m20["momentum"] >= 0.82 and cp["lower_wick"] < 0.18

    # ترند/مومنتم قوي: الأولوية تكون مع الاتجاه فقط، وليس عكسه.
    # إذا ظهرت حركة قوية جدًا، لا نعتبرها تلقائيًا انعكاس؛ ننتظر أو ندخل مع الاتجاه عند تأكيد مناسب.
    if not noisy_market and strong_trend_up and m10["pressure"] >= -0.12 and cp["upper_wick"] < 0.52:
        score = 70 + min(12, abs(m35["pressure"]) * 18) + min(10, m35["momentum"] * 12) + min(6, rhythm * 8)
        add_candidate("STRONG_TREND_CONTINUATION", "CALL", score, "next_candle", "استمرار ترند صاعد قوي", "Strong bullish trend continuation")
    if not noisy_market and strong_trend_down and m10["pressure"] <= 0.12 and cp["lower_wick"] < 0.52:
        score = 70 + min(12, abs(m35["pressure"]) * 18) + min(10, m35["momentum"] * 12) + min(6, rhythm * 8)
        add_candidate("STRONG_TREND_CONTINUATION", "PUT", score, "next_candle", "استمرار ترند هابط قوي", "Strong bearish trend continuation")

    if not noisy_market:
        if m35["change"] > 0 and m20["pressure"] >= 0.24 and m20["momentum"] >= 0.42 and rhythm >= 0.44 and not overextended_up and cp["upper_wick"] < 0.48:
            score = 42 + abs(m20["pressure"]) * 22 + m20["momentum"] * 20 + avg_body * 10 + rhythm * 8
            add_candidate("MOMENTUM_CONTINUATION", "CALL", score, "next_candle", "استمرار زخم صاعد نظيف", "Clean bullish momentum continuation")
        if m35["change"] < 0 and m20["pressure"] <= -0.24 and m20["momentum"] >= 0.42 and rhythm >= 0.44 and not overextended_down and cp["lower_wick"] < 0.48:
            score = 42 + abs(m20["pressure"]) * 22 + m20["momentum"] * 20 + avg_body * 10 + rhythm * 8
            add_candidate("MOMENTUM_CONTINUATION", "PUT", score, "next_candle", "استمرار زخم هابط نظيف", "Clean bearish momentum continuation")

    # 2) انعكاس بعد اندفاع زائد: لا نعاكس ترند قوي.
    # الانعكاس مسموح فقط إذا السوق ليس بترند قوي واضح، أو ظهر كسر قوي جدًا للزخم الحالي.
    hard_bullish_exhaustion = cp["upper_wick"] >= 0.62 and m10["pressure"] <= -0.35 and m20["pressure"] <= 0.05
    hard_bearish_exhaustion = cp["lower_wick"] >= 0.62 and m10["pressure"] >= 0.35 and m20["pressure"] >= -0.05
    allow_reversal_against_uptrend = (not strong_trend_up) or hard_bullish_exhaustion
    allow_reversal_against_downtrend = (not strong_trend_down) or hard_bearish_exhaustion
    if allow_reversal_against_uptrend and m35["change"] > 0 and m35["momentum"] >= 0.70 and (m10["pressure"] <= -0.08 or cp["upper_wick"] >= 0.34):
        score = 45 + m35["momentum"] * 18 + max(0, -m10["pressure"]) * 18 + cp["upper_wick"] * 18 + min(0.20, avg_body) * 20
        if strong_trend_up:
            score -= 18
        add_candidate("OVEREXTENSION_REVERSAL", "PUT", score, "next_candle", "انعكاس بعد اندفاع صاعد زائد", "Reversal after overextended bullish push")
    if allow_reversal_against_downtrend and m35["change"] < 0 and m35["momentum"] >= 0.70 and (m10["pressure"] >= 0.08 or cp["lower_wick"] >= 0.34):
        score = 45 + m35["momentum"] * 18 + max(0, m10["pressure"]) * 18 + cp["lower_wick"] * 18 + min(0.20, avg_body) * 20
        if strong_trend_down:
            score -= 18
        add_candidate("OVEREXTENSION_REVERSAL", "CALL", score, "next_candle", "انعكاس بعد اندفاع هابط زائد", "Reversal after overextended bearish push")

    # 3) فشل اختراق صغير: السعر يكسر قمة/قاع مصغر ثم يرجع بسرعة.
    if len(prices_60) >= 30:
        prev_high = max(prices_60[-30:-5])
        prev_low = min(prices_60[-30:-5])
        last_five = prices_60[-5:]
        broke_up_then_failed = max(last_five) > prev_high and prices_60[-1] < prev_high and m10["pressure"] < -0.02
        broke_down_then_failed = min(last_five) < prev_low and prices_60[-1] > prev_low and m10["pressure"] > 0.02
        if broke_up_then_failed:
            score = 60 + min(20, abs(prices_60[-1] - prev_high) / max(m60["range"], 1e-12) * 80) + max(0, -m10["pressure"]) * 15
            add_candidate("FAILED_BREAKOUT", "PUT", score, "next_candle", "فشل اختراق علوي صغير", "Failed small upside breakout")
        if broke_down_then_failed:
            score = 60 + min(20, abs(prices_60[-1] - prev_low) / max(m60["range"], 1e-12) * 80) + max(0, m10["pressure"]) * 15
            add_candidate("FAILED_BREAKOUT", "CALL", score, "next_candle", "فشل كسر سفلي صغير", "Failed small downside breakout")

    # 4) الهدوء ثم الانفجار: ضغط جديد بعد ضغط سابق ضيق.
    if len(prices_60) >= 50 and avg_range > 0:
        old_range = max(prices_60[-50:-18]) - min(prices_60[-50:-18])
        new_range = max(prices_60[-18:]) - min(prices_60[-18:])
        compression = old_range < (avg_range * 0.75) if avg_range > 0 else False
        breakout_clean = new_range > old_range * 1.35 if old_range > 0 else False
        if compression and breakout_clean and not noisy_market:
            if m20["change"] > 0 and m20["pressure"] > 0.22 and not overextended_up:
                score = 55 + abs(m20["pressure"]) * 18 + min(20, (new_range / max(old_range, 1e-12)))
                add_candidate("COMPRESSION_BREAKOUT", "CALL", score, "next_candle", "خروج صاعد بعد هدوء", "Bullish breakout after compression")
            elif m20["change"] < 0 and m20["pressure"] < -0.22 and not overextended_down:
                score = 55 + abs(m20["pressure"]) * 18 + min(20, (new_range / max(old_range, 1e-12)))
                add_candidate("COMPRESSION_BREAKOUT", "PUT", score, "next_candle", "خروج هابط بعد هدوء", "Bearish breakout after compression")

    # 5) رفض الذيل: ممنوع يدخل على ذيل فقط.
    # الذيل لا يصبح فرصة إلا إذا كان داخل قصة سوق واضحة: منطقة/سيولة/راوند نمبر/بنية/أوردر بلوك/ترند لاين.
    bullish_wick_confluence_keys = (
        "support_rejection", "retest_bullish", "fvg_bullish_retest", "fake_breakout_down",
        "round_bullish_rejection", "bos_bullish_retest", "choch_bullish",
        "order_block_bullish_retest", "equal_low_sweep", "trendline_pullback_bullish",
        "micro_double_bottom", "liquidity_sweep_low",
    )
    bearish_wick_confluence_keys = (
        "resistance_rejection", "retest_bearish", "fvg_bearish_retest", "fake_breakout_up",
        "round_bearish_rejection", "bos_bearish_retest", "choch_bearish",
        "order_block_bearish_retest", "equal_high_sweep", "trendline_pullback_bearish",
        "micro_double_top", "liquidity_sweep_high",
    )
    bullish_wick_confluence = [k for k in bullish_wick_confluence_keys if structure_ctx.get(k)]
    bearish_wick_confluence = [k for k in bearish_wick_confluence_keys if structure_ctx.get(k)]
    if cp["lower_wick"] >= 0.38 and m10["pressure"] > 0.10 and cp["body_ratio"] >= 0.12 and bullish_wick_confluence:
        confluence_bonus = min(18, len(bullish_wick_confluence) * 5)
        score = 54 + cp["lower_wick"] * 20 + max(0, m10["pressure"]) * 14 + min(8, m10["momentum"] * 8) + confluence_bonus
        add_candidate(
            "WICK_REJECTION_CONFLUENCE", "CALL", score, "next_candle",
            "رفض ذيل سفلي مع تأكيد منطقة/سيولة", "Lower wick rejection with zone/liquidity confluence",
            {"confluence": bullish_wick_confluence},
        )
    if cp["upper_wick"] >= 0.38 and m10["pressure"] < -0.10 and cp["body_ratio"] >= 0.12 and bearish_wick_confluence:
        confluence_bonus = min(18, len(bearish_wick_confluence) * 5)
        score = 54 + cp["upper_wick"] * 20 + max(0, -m10["pressure"]) * 14 + min(8, m10["momentum"] * 8) + confluence_bonus
        add_candidate(
            "WICK_REJECTION_CONFLUENCE", "PUT", score, "next_candle",
            "رفض ذيل علوي مع تأكيد منطقة/سيولة", "Upper wick rejection with zone/liquidity confluence",
            {"confluence": bearish_wick_confluence},
        )

    # 6) تبدل المزاج: اتجاه كان واضحًا ثم بدأ ينعكس تدريجيًا، وليس عشوائيًا.
    if len(closed_parts) >= 4 and not noisy_market:
        old_bias = sum(c["dir"] for c in closed_parts[-5:-2])
        last_bias = sum(c["dir"] for c in closed_parts[-2:])
        if old_bias <= -2 and last_bias >= 1 and m20["pressure"] > 0.18 and m20["change"] > 0:
            score = 54 + max(0, m20["pressure"]) * 20 + m20["momentum"] * 14 + cp["lower_wick"] * 10
            add_candidate("MOOD_SHIFT", "CALL", score, "next_candle", "تبدل مزاج لصعود", "Market mood shift to bullish")
        elif old_bias >= 2 and last_bias <= -1 and m20["pressure"] < -0.18 and m20["change"] < 0:
            score = 54 + max(0, -m20["pressure"]) * 20 + m20["momentum"] * 14 + cp["upper_wick"] * 10
            add_candidate("MOOD_SHIFT", "PUT", score, "next_candle", "تبدل مزاج لهبوط", "Market mood shift to bearish")

    if not candidates:
        if noisy_market:
            return {"ok": False, "reason": "السوق متذبذب وكثرة ذيول/دوجي تمنع الدخول الآن"}
        return {"ok": False, "reason": "لا يوجد نمط دخول منطقي الآن"}

    # فلتر الخطر العام: لا نسمح باستمرار زخم بعد استهلاك قوي، ولا نسمح بعكس ترند قوي واضح.
    filtered = []
    for c in candidates:
        kind = c["kind"]
        direction = c["direction"]
        if market_trend_bias and direction != market_trend_bias:
            # إذا السوق ترند/مومنتم قوي، أي فرصة عكس الاتجاه تُرفض.
            # الاستثناء الوحيد: انعكاس قاسٍ جدًا ومؤكد، لكنه يحتاج Score أعلى بكثير.
            hard_reversal = (
                (market_trend_bias == "CALL" and direction == "PUT" and hard_bullish_exhaustion)
                or (market_trend_bias == "PUT" and direction == "CALL" and hard_bearish_exhaustion)
            )
            if not hard_reversal or c.get("score", 0) < 92:
                continue
        if kind in ("MOMENTUM_CONTINUATION", "COMPRESSION_BREAKOUT", "MOOD_SHIFT", "STRONG_TREND_CONTINUATION"):
            if direction == "CALL" and overextended_up and kind != "STRONG_TREND_CONTINUATION":
                continue
            if direction == "PUT" and overextended_down and kind != "STRONG_TREND_CONTINUATION":
                continue
        # إذا آخر 10 ticks عكس اتجاه المرشح بقوة، نرفضه إلا لو هو انعكاس مؤكد.
        if kind not in ("OVEREXTENSION_REVERSAL", "FAILED_BREAKOUT", "WICK_REJECTION_CONFLUENCE", "WICK_REJECTION", "STRUCTURE_RETEST", "LIQUIDITY_SWEEP", "ROUND_NUMBER_REJECTION", "BOS_CHOCH_RETEST", "ORDER_BLOCK_RETEST", "EQUAL_LIQUIDITY_SWEEP", "TRENDLINE_PULLBACK"):
            if direction == "CALL" and m10["pressure"] < -0.22:
                continue
            if direction == "PUT" and m10["pressure"] > 0.22:
                continue
        filtered.append(c)

    if not filtered:
        return {"ok": False, "reason": "النمط ظهر لكن فلتر الخطر رفضه قبل الدخول"}

    # لا نكرر نفس النمط الخاسر مباشرة إلا إذا النتيجة عالية جدًا أو نوع فرصة مختلف.
    last_loss_setup = state.get("last_loss_setup")
    last_loss_direction = state.get("last_loss_direction")
    safe_candidates = []
    for c in filtered:
        same_setup = last_loss_setup and str(last_loss_setup) == str(c["kind"])
        same_direction = last_loss_direction and str(last_loss_direction) == str(c["direction"])
        if same_setup and same_direction and c["score"] < (TRADING_ROOM_MIN_ENTRY_SCORE + 14):
            continue
        safe_candidates.append(c)
    if not safe_candidates:
        return {"ok": False, "reason": "رفضت تكرار نفس النمط الخاسر قبل ظهور تأكيد أقوى"}

    required_score = TRADING_ROOM_MIN_ENTRY_SCORE
    if state.get("recovery_mode"):
        required_score += 7
    if state.get("brain_mode") == "protect_profit":
        required_score += 5
    if state.get("brain_mode") == "danger":
        required_score += 9

    def _candidate_priority(x: dict) -> tuple:
        direction = x.get("direction")
        kind = x.get("kind")
        trend_bonus = 0
        if market_trend_bias and direction == market_trend_bias:
            trend_bonus = 20 if kind == "STRONG_TREND_CONTINUATION" else 12
        structure_bonus = 12 if kind in ("TREND_RETEST_CONTINUATION", "STRUCTURE_RETEST", "LIQUIDITY_SWEEP", "ROUND_NUMBER_REJECTION", "BOS_CHOCH_RETEST", "ORDER_BLOCK_RETEST", "EQUAL_LIQUIDITY_SWEEP", "TRENDLINE_PULLBACK") else 0
        reversal_bonus = 0 if market_trend_bias else (1 if kind in ("FAILED_BREAKOUT", "OVEREXTENSION_REVERSAL", "WICK_REJECTION_CONFLUENCE", "WICK_REJECTION", "STRUCTURE_RETEST", "LIQUIDITY_SWEEP", "ROUND_NUMBER_REJECTION", "BOS_CHOCH_RETEST", "ORDER_BLOCK_RETEST", "EQUAL_LIQUIDITY_SWEEP", "TRENDLINE_PULLBACK") else 0)
        return (int(x.get("score", 0)) + trend_bonus + structure_bonus, trend_bonus, structure_bonus, reversal_bonus)

    safe_candidates.sort(key=_candidate_priority, reverse=True)
    best = safe_candidates[0]
    if best["score"] < required_score:
        return {"ok": False, "reason": f"قوة النمط غير كافية حسب وضع الجلسة ({best['score']}% / المطلوب {required_score}%)"}

    entry_mode = "next_candle"  # direct moving-minute entries are disabled
    if entry_mode == "next_candle" and not next_candle_allowed:
        # هذه هي إعادة التأكيد: لا نرسل الآن. سنعيد الفحص تلقائيًا عند قرب بداية الشمعة.
        return {
            "ok": False,
            "watch": True,
            "reason": "يوجد مرشح دخول، لكن سأعيد تأكيده قبل وقت الدخول بثوانٍ",
            "score": best["score"],
            "direction": best["direction"],
            "candidate_kind": best["kind"],
        }

    # إعادة فحص أخيرة داخلية مباشرة قبل الإرسال: لو آخر ticks انقلبت فجأة نلغي.
    latest = _movement_metrics(_prices_from(8))
    if best["direction"] == "CALL" and latest["pressure"] < -0.20 and best["kind"] not in ("FAILED_BREAKOUT", "WICK_REJECTION", "STRUCTURE_RETEST", "LIQUIDITY_SWEEP", "ROUND_NUMBER_REJECTION", "BOS_CHOCH_RETEST", "ORDER_BLOCK_RETEST", "EQUAL_LIQUIDITY_SWEEP", "TRENDLINE_PULLBACK"):
        return {"ok": False, "reason": "تم إلغاء الدخول لأن آخر ثواني ضعفت قبل الإرسال"}
    if best["direction"] == "PUT" and latest["pressure"] > 0.20 and best["kind"] not in ("FAILED_BREAKOUT", "WICK_REJECTION", "STRUCTURE_RETEST", "LIQUIDITY_SWEEP", "ROUND_NUMBER_REJECTION", "BOS_CHOCH_RETEST", "ORDER_BLOCK_RETEST", "EQUAL_LIQUIDITY_SWEEP", "TRENDLINE_PULLBACK"):
        return {"ok": False, "reason": "تم إلغاء الدخول لأن آخر ثواني ضعفت قبل الإرسال"}
    if market_trend_bias and best["direction"] != market_trend_bias:
        return {"ok": False, "reason": "تم إلغاء الدخول لأن الاتجاه العام قوي عكس الصفقة"}

    now_dt = now_utc()
    if entry_mode == "direct":
        entry_dt = now_dt
        bucket_start = int(entry_dt.timestamp() // 60) * 60
        expiry_ts = bucket_start + 60
    else:
        entry_dt = next_full_minute(now_dt)
        expiry_ts = entry_dt.timestamp() + 60

    setup = best["setup_en"] if get_user_language(int(state.get("admin_id") or 0)) == "en" else best["setup_ar"]
    return {
        "ok": True,
        "pair": pair,
        "symbol": symbol,
        "direction": best["direction"],
        "entry_mode": entry_mode,
        "setup": setup,
        "setup_kind": best["kind"],
        "structure_bias": structure_bias,
        "score": min(int(best["score"]), 96),
        "price": price,
        "payout": payout_at_entry,
        "entry_ts": entry_dt.timestamp(),
        "expiry_ts": float(expiry_ts),
        "entry_time_text": format_utc_plus_3(entry_dt),
    }

def build_trading_room_intro(plan: dict, lang: str = "ar") -> str:
    if lang == "en":
        return (
            "🧠 Trading Session Room\n\n"
            f"💰 Balance: {plan['balance']:.2f}$\n"
            f"📌 Suggested trade amount: {int(plan['trade_amount'])}$ ({_percent_text(TRADING_ROOM_TRADE_AMOUNT_PERCENT)})\n"
            f"🔁 Recovery trade amount: {int(plan['recovery_amount'])}$\n"
            f"🎯 Profit target: {_percent_text(TRADING_ROOM_TARGET_PROFIT_PERCENT)} ({int(plan['target_profit_amount'])}$)\n"
            f"⛔ Loss limit: {_percent_text(TRADING_ROOM_MAX_LOSS_PERCENT)} ({int(plan['max_loss_amount'])}$)\n\n"
            "I will check live OTC pairs, choose a suitable pair for the session, then monitor it only during the session.\n"
            "The session stops based on net profit or loss, not by a fixed number of trades.\n\n"
            "Read the plan calmly, then confirm when you are ready."
        )
    return (
        "🧠 غرفة جلسة تداول\n\n"
        f"💰 الرصيد المسجل: {plan['balance']:.2f}$\n"
        f"📌 دخول الصفقة المقترح: {int(plan['trade_amount'])}$ ({_percent_text(TRADING_ROOM_TRADE_AMOUNT_PERCENT)})\n"
        f"🔁 دخول التعويض: {int(plan['recovery_amount'])}$\n"
        f"🎯 تارجت الربح: {_percent_text(TRADING_ROOM_TARGET_PROFIT_PERCENT)} ({int(plan['target_profit_amount'])}$)\n"
        f"⛔ حد الخسارة: {_percent_text(TRADING_ROOM_MAX_LOSS_PERCENT)} ({int(plan['max_loss_amount'])}$)\n\n"
        "سأفحص أزواج OTC الحية، أختار زوجًا مناسبًا للجلسة، ثم أراقبه فقط خلال الجلسة.\n"
        "الجلسة تتوقف حسب صافي الربح أو الخسارة، وليس حسب عدد الصفقات.\n\n"
        "اقرأ الخطة بهدوء، وبعدها أكد إذا كنت مستعدًا."
    )


def build_trading_room_state_message(state: dict) -> str:
    uid = int((state or {}).get("admin_id") or 0)
    lang = get_user_language(uid) if uid else "ar"
    if not state or not state.get("active"):
        return "📊 No active trading session right now." if lang == "en" else "📊 لا توجد جلسة تداول نشطة الآن."
    if lang == "en":
        lines = [
            "📊 Trading Session Status",
            "━━━━━━━━━━━━━━",
            f"Status: {'Waiting for trade result' if state.get('waiting_result') else 'Searching for entry'}",
            f"Pair: {state.get('pair') or 'Not selected yet'}",
            f"Current strategy: {state.get('strategy') or 'Reading market'}",
            f"Balance: {float(state.get('balance', 0) or 0):.2f}$",
            f"Base trade amount: {int(_round_platform_amount(state.get('trade_amount', 0)))}$",
            f"Recovery trade amount: {int(_round_platform_amount(state.get('recovery_amount', state.get('trade_amount', 0))))}$",
            f"Trades taken: {int(state.get('trades_done', 0) or 0)}",
            f"Results: {int(state.get('wins', 0) or 0)} win / {int(state.get('losses', 0) or 0)} loss",
            f"Session net: {_money_signed(state.get('net_profit', 0.0))}",
            f"Profit target: {int(_round_platform_amount(state.get('target_profit_amount', 0.0)))}$",
            f"Loss limit: -{int(_round_platform_amount(state.get('max_loss_amount', 0.0)))}$",
            f"Recovery losses: {int(state.get('recovery_losses', 0) or 0)}",
            f"Session mode: {state.get('brain_mode') or state.get('session_mode') or 'normal'}",
            f"Market mood: {state.get('market_mood') or 'reading'}",
            f"Pair health: {state.get('pair_health') if state.get('pair_health') is not None else 'reading'}",
            f"Pair switches: {int(state.get('pair_switches', 0) or 0)} / {TRADING_ROOM_PAIR_MAX_SWITCHES}",
        ]
        if state.get("last_reason"):
            lines.append(f"Last reading: {state.get('last_reason')}")
        return "\n".join(lines)
    lines = [
        "📊 حالة غرفة جلسة التداول",
        "━━━━━━━━━━━━━━",
        f"الحالة: {'بانتظار نتيجة صفقة' if state.get('waiting_result') else 'تبحث عن دخول'}",
        f"الزوج: {state.get('pair') or 'لم يحدد بعد'}",
        f"الاستراتيجية الحالية: {state.get('strategy') or 'قيد القراءة'}",
        f"الرصيد: {float(state.get('balance', 0) or 0):.2f}$",
        f"دخول الصفقة الأساسي: {int(_round_platform_amount(state.get('trade_amount', 0)))}$",
        f"دخول التعويض: {int(_round_platform_amount(state.get('recovery_amount', state.get('trade_amount', 0))))}$",
        f"الصفقات المنفذة: {int(state.get('trades_done', 0) or 0)}",
        f"النتائج: {int(state.get('wins', 0) or 0)} ربح / {int(state.get('losses', 0) or 0)} خسارة",
        f"صافي الجلسة: {_money_signed(state.get('net_profit', 0.0))}",
        f"تارجت الربح: {int(_round_platform_amount(state.get('target_profit_amount', 0.0)))}$",
        f"حد الخسارة: -{int(_round_platform_amount(state.get('max_loss_amount', 0.0)))}$",
        f"خسائر التعويض: {int(state.get('recovery_losses', 0) or 0)}",
        f"وضع الجلسة: {state.get('brain_mode') or state.get('session_mode') or 'normal'}",
        f"مزاج السوق: {state.get('market_mood') or 'قيد القراءة'}",
        f"صحة الزوج: {state.get('pair_health') if state.get('pair_health') is not None else 'قيد القراءة'}",
        f"تغييرات الزوج: {int(state.get('pair_switches', 0) or 0)} / {TRADING_ROOM_PAIR_MAX_SWITCHES}",
    ]
    if state.get("last_reason"):
        lines.append(f"آخر قراءة: {state.get('last_reason')}")
    return "\n".join(lines)


async def start_trading_room_session(update: Update, context: ContextTypes.DEFAULT_TYPE, balance: float):
    admin_id = int(update.effective_user.id)
    plan = build_session_money_plan(balance)
    state = {
        "active": True,
        "pending_ready": True,
        "ready_confirmed": False,
        "admin_id": admin_id,
        "balance": plan["balance"],
        "trade_amount": plan["trade_amount"],
        "recovery_amount": plan["recovery_amount"],
        "target_profit_amount": plan["target_profit_amount"],
        "max_loss_amount": plan["max_loss_amount"],
        "max_trades": plan["max_trades"],
        "wins": 0,
        "losses": 0,
        "net_profit": 0.0,
        "trade_ledger": [],
        "pending_loss_units": 0,
        "pending_loss_amount": 0.0,
        "pending_loss_payout": 0.0,
        "unrecovered_loss": False,
        "session_mode": "normal",
        "trades_done": 0,
        "recovery_losses": 0,
        "extra_recovery_used": 0,
        "recovery_mode": False,
        "recovery_notified_at": 0.0,
        "waiting_result": False,
        "started_at": time_module.time(),
        "expires_at": time_module.time() + TRADING_ROOM_SCAN_SECONDS,
        # Session Brain
        "brain_mode": "normal",
        "market_mood": None,
        "pair_health": None,
        "pair_health_label": None,
        "pair_switches": 0,
        "bad_symbols": [],
        "last_loss_setup": None,
        "last_loss_direction": None,
        "last_trade_setup": None,
        "last_trade_direction": None,
        "no_entry_scans": 0,
        "last_brain_notice_at": 0.0,
        "pair_selected_at": 0.0,
        "last_pair_switch_at": 0.0,
        "pair_bad_scans": 0,
        "smart_exit_waiting": False,
        "smart_exit_reason": None,
        "smart_exit_last_suggested_at": 0.0,
    }
    context.bot_data[trading_room_key(admin_id)] = state
    await safe_send_message(
        context.bot,
        chat_id=admin_id,
        text=build_trading_room_intro(plan, get_user_language(admin_id)),
        reply_markup=get_trading_room_ready_keyboard(admin_id),
    )
    await safe_send_message(
        context.bot,
        chat_id=admin_id,
        text=("Are you ready to start the session?" if get_user_language(admin_id) == "en" else "هل أنت مستعد نبدأ الجلسة؟"),
        reply_markup=get_trading_room_ready_keyboard(admin_id),
    )



def build_trading_room_smart_exit_message(state: dict, reason_type: str, lang: str = "ar") -> str:
    net_profit = float(state.get("net_profit", 0.0) or 0.0)
    elapsed_min = int(max(0, time_module.time() - float(state.get("started_at", time_module.time()) or time_module.time())) // 60)
    trades_done = int(state.get("trades_done", 0) or 0)
    pair_switches = int(state.get("pair_switches", 0) or 0)
    if lang == "en":
        if reason_type == "profit":
            intro = "The session has been running for a while and you are currently in profit."
            advice = "A smart exit now can be better than forcing the full target and giving the market a chance to take it back."
        elif reason_type == "loss":
            intro = "The session has been running for a while and the loss is still controlled."
            advice = "Stopping here may protect the account from emotional continuation or revenge trading."
        else:
            intro = "The session has been running for a long time without clear progress."
            advice = "It may be better to pause and try again when the market is cleaner."
        return (
            "🧠 Smart session suggestion\n\n"
            f"{intro}\n\n"
            f"⏱ Session time: {elapsed_min} minutes\n"
            f"📊 Trades so far: {trades_done}\n"
            f"🔄 Pair switches: {pair_switches}\n"
            f"💰 Current net: {_money_signed(net_profit)}\n\n"
            f"{advice}\n\n"
            "Choose whether to stop and secure the current result, or continue the session."
        )
    if reason_type == "profit":
        intro = "الجلسة طولت، وحاليًا نحن على ربح جيد."
        advice = "أحيانًا الخروج الذكي بربح جزئي أفضل من الإصرار على التارجت الكامل وترك السوق يرجع يسحب الربح."
    elif reason_type == "loss":
        intro = "الجلسة طولت، والخسارة الحالية ما زالت تحت السيطرة."
        advice = "الإيقاف هنا ممكن يحمي رأس المال من الاستمرار تحت ضغط نفسي أو محاولة تعويض عشوائية."
    else:
        intro = "الجلسة طولت بدون تقدم واضح."
        advice = "ممكن الأفضل نرتاح ونرجع بوقت يكون السوق أنظف."
    return (
        "🧠 اقتراح ذكي للجلسة\n\n"
        f"{intro}\n\n"
        f"⏱ مدة الجلسة: {elapsed_min} دقيقة\n"
        f"📊 عدد الصفقات: {trades_done}\n"
        f"🔄 تغييرات الزوج: {pair_switches}\n"
        f"💰 صافي الجلسة الحالي: {_money_signed(net_profit)}\n\n"
        f"{advice}\n\n"
        "اختر إذا بدك نوقف ونحفظ النتيجة الحالية، أو نكمل الجلسة."
    )


def trading_room_smart_exit_reason(state: dict) -> str | None:
    try:
        if not state or not state.get("active") or state.get("waiting_result") or state.get("pending_ready"):
            return None
        if state.get("smart_exit_waiting"):
            return None
        now = time_module.time()
        last = float(state.get("smart_exit_last_suggested_at", 0) or 0)
        if last and now - last < TRADING_ROOM_SMART_EXIT_COOLDOWN_SECONDS:
            return None
        elapsed = now - float(state.get("started_at", now) or now)
        trades_done = int(state.get("trades_done", 0) or 0)
        if elapsed < TRADING_ROOM_SMART_EXIT_MIN_SECONDS or trades_done < 2:
            return None
        net_profit = float(state.get("net_profit", 0.0) or 0.0)
        target_profit = abs(float(state.get("target_profit_amount", 0.0) or 0.0))
        max_loss = abs(float(state.get("max_loss_amount", 0.0) or 0.0))
        trade_amount = abs(float(state.get("trade_amount", 1.0) or 1.0))
        # Protect decent partial profit after a long, back-and-forth session.
        profit_threshold = max(trade_amount, target_profit * TRADING_ROOM_SMART_EXIT_PROFIT_PART) if target_profit > 0 else trade_amount
        if net_profit >= profit_threshold:
            return "profit"
        # Stop controlled loss before it becomes the full loss limit.
        loss_threshold = max(trade_amount, max_loss * TRADING_ROOM_SMART_EXIT_LOSS_PART) if max_loss > 0 else trade_amount
        if net_profit <= -loss_threshold:
            return "loss"
        # If the session is flat for too long with several trades/switches, suggest a pause.
        if elapsed >= TRADING_ROOM_SMART_EXIT_FLAT_SECONDS and trades_done >= 4 and abs(net_profit) < max(trade_amount, 1.0):
            return "flat"
    except Exception:
        logger.exception("trading_room_smart_exit_reason failed")
    return None


async def trading_room_maybe_suggest_smart_exit(context: ContextTypes.DEFAULT_TYPE, admin_id: int, state: dict) -> bool:
    reason = trading_room_smart_exit_reason(state)
    if not reason:
        return False
    lang = get_user_language(admin_id)
    state["smart_exit_waiting"] = True
    state["smart_exit_reason"] = reason
    state["smart_exit_last_suggested_at"] = time_module.time()
    await safe_send_message(
        context.bot,
        chat_id=admin_id,
        text=build_trading_room_smart_exit_message(state, reason, lang),
        reply_markup=get_trading_room_smart_exit_keyboard(admin_id),
    )
    return True


async def trading_room_scan_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    admin_id = int(data.get("admin_id") or 0)
    lang = get_user_language(admin_id)
    en = lang == "en"
    state = get_trading_room_state(context, admin_id)
    if not state or not state.get("active"):
        try:
            context.job.schedule_removal()
        except Exception:
            pass
        return

    if state.get("waiting_result"):
        return

    if state.get("smart_exit_waiting"):
        return

    if time_module.time() > float(state.get("expires_at", 0) or 0):
        # لا ننهي غرفة الجلسة بسبب انتهاء وقت المراقبة فقط.
        # الجلسة تنتهي فقط عند تحقق هدف الربح أو حد الخسارة كنسبة من رأس المال.
        state["expires_at"] = time_module.time() + (
            TRADING_ROOM_RECOVERY_SEARCH_SECONDS if state.get("recovery_mode") else TRADING_ROOM_SCAN_SECONDS
        )
        last_notice = float(state.get("monitor_extend_notified_at", 0) or 0)
        if time_module.time() - last_notice >= TRADING_ROOM_RECOVERY_MESSAGE_COOLDOWN_SECONDS:
            state["monitor_extend_notified_at"] = time_module.time()
            await safe_send_message(context.bot,
                chat_id=admin_id,
                text=("⏳ No suitable entry right now. I will keep monitoring without random entries." if en else "⏳ لا يوجد دخول مناسب الآن. سأستمر بالمراقبة بدون دخول عشوائي."),
                reply_markup=get_trading_room_active_keyboard(admin_id)
            )

    switched = await trading_room_switch_pair_if_needed(context, admin_id, state)
    if switched:
        return

    analysis = analyze_trading_room_entry(state)
    if not analysis.get("ok"):
        state["last_reason"] = analysis.get("reason", "لا يوجد دخول الآن")
        state["no_entry_scans"] = int(state.get("no_entry_scans", 0) or 0) + 1
        return

    state["no_entry_scans"] = 0

    direction = analysis["direction"]
    entry_mode = analysis["entry_mode"]
    entry_text = ("⚡ Enter now for one moving minute" if entry_mode == "direct" else f"🕯 Next candle entry at {analysis['entry_time_text']}") if en else ("⚡ دخول مباشر الآن لمدة دقيقة متحركة" if entry_mode == "direct" else f"🕯 دخول الشمعة القادمة عند {analysis['entry_time_text']}")
    dir_line = "🟢 CALL" if direction == "CALL" else "🔴 PUT"

    base_amount = float(state.get('trade_amount', 0) or 0)
    # الصفقة التعويضية تكون فقط عند وجود خسارة غير معوضة.
    # لا نكرر تعويض وراء تعويض؛ بعد تعويض خاسر ترجع الصفقة التالية عادية إذا لم تنتهِ الجلسة.
    is_recovery_trade = bool(state.get("recovery_mode") and state.get("unrecovered_loss"))
    trade_amount = float(_round_platform_amount(state.get('recovery_amount') if is_recovery_trade else base_amount))
    analysis["trade_amount"] = trade_amount
    analysis["recovery_trade"] = is_recovery_trade

    state.update({
        "waiting_result": True,
        "current_trade": analysis,
        "trades_done": int(state.get("trades_done", 0) or 0) + 1,
        "last_trade_setup": analysis.get("setup_kind") or analysis.get("setup") or state.get("strategy_type"),
        "last_trade_direction": analysis.get("direction"),
    })

    amount_line = (f"💰 Suggested trade amount: {int(trade_amount)}$" if en else f"💰 دخول الصفقة المقترح: {int(trade_amount)}$")
    if is_recovery_trade:
        amount_line += ("  🔁 Recovery" if en else "  🔁 تعويض")

    trade_message = (
        f"{entry_text}\n\n"
        + ("🧭 Duration: M1\n" if en else "🧭 المدة: M1\n")
        + (f"📌 Direction: {dir_line}\n\n\n" if en else f"📌 الاتجاه: {dir_line}\n\n\n")
        + f"{amount_line}"
    )
    if is_admin(admin_id):
        trade_message += "\n\n" + build_trading_room_admin_entry_reason(analysis, lang)

    await safe_send_message(context.bot,
        chat_id=admin_id,
        text=trade_message,
        reply_markup=get_trading_room_active_keyboard(admin_id)
    )

    await publish_copy_trading_room_signal(analysis, state, creator_user_id=admin_id)

    try:
        result_when = max(
            5,
            float(analysis.get("expiry_ts", time_module.time() + TRADING_ROOM_RESULT_DELAY_SECONDS))
            + TRADING_ROOM_RESULT_EXTRA_DELAY_SECONDS
            - time_module.time()
        )
        context.job_queue.run_once(
            trading_room_result_job,
            when=result_when,
            data={"admin_id": admin_id, "trade": analysis},
            name=f"trading_room_result_{admin_id}_{int(time_module.time())}",
        )
    except Exception as e:
        logger.exception("Could not schedule trading room result job: %s", e)


async def trading_room_result_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    admin_id = int(data.get("admin_id") or 0)
    lang = get_user_language(admin_id)
    en = lang == "en"
    trade = data.get("trade") or {}
    state = get_trading_room_state(context, admin_id)
    if not state or not state.get("active"):
        return

    symbol = trade.get("symbol")
    pair = trade.get("pair")
    direction = trade.get("direction")
    # لا نحسب النتيجة قبل وقت انتهاء الصفقة الفعلي.
    expiry_ts = float(trade.get("expiry_ts") or (time_module.time() + TRADING_ROOM_RESULT_DELAY_SECONDS))
    due_ts = expiry_ts + TRADING_ROOM_RESULT_EXTRA_DELAY_SECONDS
    if time_module.time() < due_ts:
        try:
            context.job_queue.run_once(
                trading_room_result_job,
                when=max(3, due_ts - time_module.time()),
                data={"admin_id": admin_id, "trade": trade},
                name=f"trading_room_result_wait_{admin_id}_{int(time_module.time())}",
            )
        except Exception as e:
            logger.exception("Could not reschedule early trading room result job: %s", e)
        return

    entry_ts = float(trade.get("entry_ts") or 0)
    entry_bucket_ts = int(entry_ts // 60) * 60 if entry_ts else 0

    # النتيجة هنا تُحسب حصراً من شكل شمعة الصفقة المغلقة.
    # لا نستخدم السعر اللحظي snapshot إطلاقاً حتى لا تظهر نتيجة قبل إغلاق الشمعة أو من tick متأخر/مبكر.
    try:
        candle = quotex_otc_feed.candle(symbol, entry_ts)
    except Exception:
        candle = {}

    candle_open = None
    candle_close = None
    candle_high = None
    candle_low = None
    try:
        candle_bucket = int(float(candle.get("bucket_ts", 0) or 0)) if candle else 0
        if not candle or candle_bucket != entry_bucket_ts:
            raise ValueError("closed trade candle not available yet")
        candle_open = float(candle.get("open"))
        candle_close = float(candle.get("close"))
        candle_high = float(candle.get("high", max(candle_open, candle_close)))
        candle_low = float(candle.get("low", min(candle_open, candle_close)))
    except Exception:
        state["waiting_result"] = False
        await safe_send_message(context.bot,
            chat_id=admin_id,
            text=(
                "⚠️ I could not calculate the trade result because the closed trade candle is not available yet.\n"
                "I will not rely on the live price. We will continue monitoring the session."
                if en else
                "⚠️ تعذر حساب نتيجة الصفقة لأن شمعة الصفقة المغلقة غير متوفرة بعد.\n"
                "لن أعتمد على السعر اللحظي. سنكمل مراقبة الجلسة."
            ),
            reply_markup=get_trading_room_active_keyboard(admin_id)
        )
        return

    eps = OTC_LIVE_TIE_EPSILON
    candle_is_green = candle_close > candle_open + eps
    candle_is_red = candle_close < candle_open - eps
    state["waiting_result"] = False

    was_recovery_trade = bool(trade.get("recovery_trade"))
    trade_amount = float(trade.get("trade_amount") or state.get("trade_amount", 0) or 0)
    payout_at_entry = _normalize_payout_percent(trade.get("payout", 80), 80.0)
    win_profit = _trading_room_win_profit(trade_amount, payout_at_entry)

    # إذا أغلقت الشمعة دوجي / تعادل (الفتح والإغلاق نفس السعر تقريبًا)
    # لا تُحسب الصفقة ربح ولا خسارة، وفي Quotex عادةً يرجع مبلغ الدخول بدون ربح.
    # مهم جدًا خصوصًا بصفقة التعويض: لا نعتبرها خسارة تعويضية ولا نلغي الخسارة السابقة.
    if not candle_is_green and not candle_is_red:
        net_profit = float(state.get("net_profit", 0.0) or 0.0)
        wins = int(state.get("wins", 0) or 0)
        losses = int(state.get("losses", 0) or 0)
        if was_recovery_trade and bool(state.get("unrecovered_loss")):
            state["recovery_mode"] = True
            state["session_mode"] = "recovery"
            state["brain_mode"] = "recovery"
            state["expires_at"] = time_module.time() + TRADING_ROOM_RECOVERY_SEARCH_SECONDS
            state["last_reason"] = "Recovery trade ended as a draw; searching for another safe recovery" if en else "صفقة التعويض انتهت تعادل؛ سأبحث عن تعويض آمن من جديد"
            result_line = "⚖️ Recovery trade ended as a draw" if en else "⚖️ صفقة التعويض انتهت تعادل"
            follow_line = "The previous loss was not recovered yet. I will look for another safe recovery opportunity without counting this as a loss." if en else "الخسارة السابقة لم تُحسب كتعويض، وسأبحث عن فرصة تعويضية آمنة بدون اعتبارها خسارة."
        else:
            state["recovery_mode"] = False
            state["session_mode"] = "normal"
            state["brain_mode"] = "normal"
            state["last_reason"] = "The trade ended as a draw and was not counted as win or loss" if en else "الصفقة انتهت تعادل ولم تُحسب ربح أو خسارة"
            result_line = "⚖️ The trade ended as a draw" if en else "⚖️ الصفقة انتهت تعادل"
            follow_line = "It will not count as a win or a loss. I will continue monitoring normally." if en else "لن تُحسب ربح ولا خسارة، وسأكمل مراقبة الجلسة بشكل طبيعي."
        try:
            history = list(state.get("trade_ledger") or [])
            history.append({
                "time": time_module.time(),
                "pair": trade.get("pair"),
                "symbol": trade.get("symbol"),
                "direction": trade.get("direction"),
                "recovery_trade": was_recovery_trade,
                "amount": float(trade_amount or 0),
                "payout": payout_at_entry,
                "result": "draw",
                "pnl": 0.0,
                "effective_win_units": 0,
                "effective_loss_units": 0,
                "net_after": round(net_profit, 2),
            })
            state["trade_ledger"] = history[-100:]
        except Exception:
            pass
        result_message = (
            f"{result_line}\n\n"
            f"{follow_line}\n\n"
        )
        if is_admin(admin_id):
            result_message += build_trading_room_admin_result_reason(trade, None, candle_open, candle_close, lang, candle_high, candle_low) + "\n\n"
        result_message += (f"📊 Session result now: {wins} win / {losses} loss\n" if en else f"📊 نتيجة الجلسة الآن: {wins} ربح / {losses} خسارة\n")
        result_message += (f"💰 Session net now: {_money_signed(net_profit)}" if en else f"💰 صافي الجلسة الآن: {_money_signed(net_profit)}")
        await safe_send_message(context.bot,
            chat_id=admin_id,
            text=result_message,
            reply_markup=get_trading_room_active_keyboard(admin_id)
        )
        try:
            save_trading_room_state(context, admin_id, state)
        except Exception:
            pass
        if state.get("active") and not state.get("waiting_result"):
            try:
                context.job_queue.run_once(
                    trading_room_scan_job,
                    when=8,
                    data={"admin_id": admin_id},
                    name=f"trading_room_scan_after_draw_{admin_id}_{int(time_module.time())}",
                )
            except Exception as e:
                logger.exception("Could not schedule scan after draw: %s", e)
        return

    if direction == "CALL":
        win = candle_is_green
    else:
        win = candle_is_red

    if win:
        # الربح الصافي في Quotex ليس كامل مبلغ الدخول؛ بل مبلغ الدخول × payout لحظة الدخول.
        state["net_profit"] = round(float(state.get("net_profit", 0.0) or 0.0) + win_profit, 2)
        if was_recovery_trade:
            # صفقة التعويض الرابحة تغطي الصفقة السابقة + تعتبر الصفقة الحالية رابحة.
            # مثال: خسارة 7$ ثم تعويض 14$ رابح على payout 83% => الصافي = -7 + 11.62 = +4.62$
            # لكن عدّاد النتيجة يتحول إلى ربحين منطقيًا: الصفقة القديمة تعوضت + الصفقة الجديدة ربحت.
            recovered_units = max(1, int(state.get("pending_loss_units", 0) or 1))
            effective_win_units = recovered_units + 1
            state["losses"] = max(0, int(state.get("losses", 0) or 0) - recovered_units)
            state["wins"] = int(state.get("wins", 0) or 0) + effective_win_units
            state["pending_loss_units"] = 0
            state["pending_loss_amount"] = 0.0
            state["pending_loss_payout"] = 0.0
            state["unrecovered_loss"] = False
            state["recovery_mode"] = False
            state["session_mode"] = "normal"
            state["last_reason"] = "Previous loss recovered and recovery trade won" if en else "تم تعويض الخسارة السابقة وربحت صفقة التعويض"
            state["brain_mode"] = "normal"
            state["last_loss_setup"] = None
            state["last_loss_direction"] = None
            _append_trading_room_ledger(state, trade, True, win_profit, effective_win_units=effective_win_units, effective_loss_units=0)
            result_line = ("✅ Great, the recovery trade won and covered the previous loss" if en else "✅ مبروك، صفقة التعويض ربحت وغطّت الخسارة السابقة")
        else:
            state["wins"] = int(state.get("wins", 0) or 0) + 1
            state["pending_loss_units"] = 0
            state["pending_loss_amount"] = 0.0
            state["pending_loss_payout"] = 0.0
            state["unrecovered_loss"] = False
            state["recovery_mode"] = False
            state["session_mode"] = "normal"
            state["brain_mode"] = "protect_profit" if int(state.get("wins", 0) or 0) >= 2 else "normal"
            state["last_loss_setup"] = None
            state["last_loss_direction"] = None
            _append_trading_room_ledger(state, trade, True, win_profit, effective_win_units=1, effective_loss_units=0)
            result_line = ("✅ Great, the trade won" if en else "✅ مبروك، الصفقة ربحت")
    else:
        loss_amount = round(float(trade_amount or 0), 2)
        state["net_profit"] = round(float(state.get("net_profit", 0.0) or 0.0) - loss_amount, 2)
        if was_recovery_trade:
            # تعويض خاسر = خسارتين بوحدة الصفقة الأساسية، بالإضافة للخسارة الأصلية الموجودة أصلًا بالعدّاد.
            # لذلك لو خسرنا 7$ ثم تعويض 14$ خسر، تصبح النتيجة 3 خسارات فعلية من ناحية رأس المال.
            effective_loss_units = _trading_room_loss_units_for_trade(trade)
            state["losses"] = int(state.get("losses", 0) or 0) + effective_loss_units
            state["recovery_losses"] = int(state.get("recovery_losses", 0) or 0) + 1
            state["pending_loss_units"] = 0
            state["pending_loss_amount"] = 0.0
            state["pending_loss_payout"] = 0.0
            state["unrecovered_loss"] = False
            state["recovery_mode"] = False
            state["session_mode"] = "normal"
            state["last_reason"] = "Recovery trade lost; next trade is normal if the session continues" if en else "صفقة التعويض خسرت؛ الصفقة التالية عادية إذا لم تنتهِ الجلسة"
            state["brain_mode"] = "danger"
            state["last_loss_setup"] = trade.get("setup_kind") or trade.get("setup") or state.get("strategy_type")
            state["last_loss_direction"] = trade.get("direction")
            try:
                bs = set(state.get("bad_symbols") or [])
                if trade.get("symbol"):
                    bs.add(trade.get("symbol"))
                state["bad_symbols"] = list(bs)
            except Exception:
                pass
            _append_trading_room_ledger(state, trade, False, -loss_amount, effective_win_units=0, effective_loss_units=effective_loss_units)
            result_line = ("❌ Recovery trade lost" if en else "❌ معوضة، صفقة التعويض خسرت")
        else:
            state["losses"] = int(state.get("losses", 0) or 0) + 1
            state["pending_loss_units"] = 1
            state["pending_loss_amount"] = loss_amount
            state["pending_loss_payout"] = payout_at_entry
            state["unrecovered_loss"] = True
            state["recovery_mode"] = True
            state["session_mode"] = "recovery"
            state["expires_at"] = time_module.time() + TRADING_ROOM_RECOVERY_SEARCH_SECONDS
            state["last_reason"] = "Searching for a safe recovery trade" if en else "جاري البحث عن صفقة تعويضية آمنة"
            state["brain_mode"] = "recovery"
            state["last_loss_setup"] = trade.get("setup_kind") or trade.get("setup") or state.get("strategy_type")
            state["last_loss_direction"] = trade.get("direction")
            _append_trading_room_ledger(state, trade, False, -loss_amount, effective_win_units=0, effective_loss_units=1)
            result_line = ("❌ The trade lost" if en else "❌ معوضة، الصفقة خسرت")

    wins = int(state.get("wins", 0) or 0)
    losses = int(state.get("losses", 0) or 0)
    recovery_losses = int(state.get("recovery_losses", 0) or 0)
    net_profit = float(state.get("net_profit", 0.0) or 0.0)

    result_message = f"{result_line}\n\n"
    if is_admin(admin_id):
        result_message += build_trading_room_admin_result_reason(trade, win, candle_open, candle_close, lang, candle_high, candle_low) + "\n\n"
    result_message += (f"📊 Session result now: {wins} win / {losses} loss\n" if en else f"📊 نتيجة الجلسة الآن: {wins} ربح / {losses} خسارة\n")
    result_message += (f"💰 Session net now: {_money_signed(net_profit)}" if en else f"💰 صافي الجلسة الآن: {_money_signed(net_profit)}")
    await safe_send_message(context.bot,
        chat_id=admin_id,
        text=result_message,
        reply_markup=get_trading_room_active_keyboard(admin_id)
    )

    should_finish = False
    finish_reason = ""
    target_profit_amount = float(state.get("target_profit_amount", 0.0) or 0.0)
    max_loss_amount = float(state.get("max_loss_amount", 0.0) or 0.0)
    if target_profit_amount > 0 and net_profit >= target_profit_amount:
        should_finish = True
        finish_reason = f"Profit target reached: {_money_signed(net_profit)}." if en else f"تحقق تارجت الربح: {_money_signed(net_profit)}."
    elif max_loss_amount > 0 and net_profit <= -max_loss_amount:
        should_finish = True
        finish_reason = f"Loss limit reached: {_money_signed(net_profit)}." if en else f"وصلنا لحد الخسارة: {_money_signed(net_profit)}."

    if should_finish:
        state["active"] = False
        if net_profit >= 0:
            if en:
                end_text = (
                    "🏁 Trading session ended in profit ✅\n\n"
                    f"Reason: {finish_reason}\n"
                    f"Final result: {wins} win / {losses} loss\n"
                    f"Session net: {_money_signed(net_profit)}\n\n"
                    "🔥 Excellent work: clear target, discipline, and exiting while profitable.\n"
                    "The best profit is the one you know when to protect."
                )
            else:
                end_text = (
                    "🏁 انتهت جلسة التداول على ربح ✅\n\n"
                    f"السبب: {finish_reason}\n"
                    f"النتيجة النهائية: {wins} ربح / {losses} خسارة\n"
                    f"صافي الجلسة: {_money_signed(net_profit)}\n\n"
                    "🔥 ممتاز، هيك الشغل الصح: هدف واضح، التزام بالخطة، وخروج وأنت رابح.\n"
                    "أجمل ربح هو الربح اللي بتعرف توقف عنده."
                )
            keyboard = get_trading_room_after_win_keyboard(admin_id)
        else:
            if en:
                end_text = (
                    "🏁 Trading session ended\n\n"
                    f"Reason: {finish_reason}\n"
                    f"Final result: {wins} win / {losses} loss\n"
                    f"Session net: {_money_signed(net_profit)}\n\n"
                    "It is better to respect the plan and money management. We can try again in 30 minutes if you want."
                )
            else:
                end_text = (
                    "🏁 انتهت جلسة التداول\n\n"
                    f"السبب: {finish_reason}\n"
                    f"النتيجة النهائية: {wins} ربح / {losses} خسارة\n"
                    f"صافي الجلسة: {_money_signed(net_profit)}\n\n"
                    "الأفضل الالتزام بالخطة وإدارة رأس المال. دعنا نحاول مجددًا بعد نصف ساعة لو أحببت."
                )
            keyboard = get_trading_room_after_loss_keyboard(admin_id)
        await safe_send_message(context.bot, chat_id=admin_id, text=end_text, reply_markup=keyboard)
        return

    if await trading_room_maybe_suggest_smart_exit(context, admin_id, state):
        return

    if state.get("recovery_mode") and state.get("unrecovered_loss"):
        await safe_send_message(context.bot,
            chat_id=admin_id,
            text=("🔁 Searching for a safe recovery trade..." if en else "🔁 جاري البحث عن صفقة تعويضية آمنة..."),
            reply_markup=get_trading_room_active_keyboard(admin_id)
        )



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


otc_live_health_state = {
    "last_alert_at": 0.0,
    "last_ok": None,
    "last_reason": "unknown",
}


def get_otc_live_feed_health() -> dict:
    """حالة بث Quotex OTC Live من داخل البوت للأدمن والتنبيهات."""
    try:
        enabled = bool(OTC_LIVE_CHANNEL_ENABLED and is_channel_publish_enabled("otc_live"))
        connected = bool(getattr(quotex_otc_feed, "connected", False)) if "quotex_otc_feed" in globals() else False

        latest_tick = None
        latest_symbol = None
        tick_count = 0
        instruments_count = 0
        followed_count = 0

        if "quotex_otc_feed" in globals():
            try:
                followed_count = len(getattr(quotex_otc_feed, "symbols", []) or [])
                with quotex_otc_feed.lock:
                    ticks_map = dict(getattr(quotex_otc_feed, "last_tick", {}) or {})
                    instruments_count = len(getattr(quotex_otc_feed, "instruments", {}) or {})
                tick_count = len(ticks_map)

                for symbol, tick in ticks_map.items():
                    if not isinstance(tick, dict):
                        continue
                    received = parse_iso(str(tick.get("received_at") or ""))
                    if received and (latest_tick is None or received > latest_tick):
                        latest_tick = received
                        latest_symbol = symbol
            except Exception:
                pass

        age_seconds = None
        if latest_tick:
            if latest_tick.tzinfo is None:
                latest_tick = latest_tick.replace(tzinfo=UTC)
            age_seconds = max(0, int((now_utc() - latest_tick.astimezone(UTC)).total_seconds()))

        if not enabled:
            ok = True
            reason = "disabled"
            title = "متوقف من الإعدادات"
        elif not connected:
            ok = False
            reason = "disconnected"
            title = "WebSocket مفصول"
        elif latest_tick is None:
            ok = False
            reason = "no_ticks"
            title = "لا يوجد ticks"
        elif age_seconds is not None and age_seconds > OTC_LIVE_NO_TICKS_ALERT_SECONDS:
            ok = False
            reason = "stale_ticks"
            title = f"آخر tick قديم منذ {age_seconds} ثانية"
        else:
            ok = True
            reason = "ok"
            title = "شغال"

        return {
            "ok": ok,
            "reason": reason,
            "title": title,
            "enabled": enabled,
            "connected": connected,
            "age_seconds": age_seconds,
            "latest_symbol": latest_symbol,
            "tick_count": tick_count,
            "instruments_count": instruments_count,
            "followed_count": followed_count,
        }
    except Exception as e:
        logger.exception("OTC live health check error: %s", e)
        return {
            "ok": False,
            "reason": "health_error",
            "title": f"خطأ فحص الحالة: {e}",
            "enabled": False,
            "connected": False,
            "age_seconds": None,
            "latest_symbol": None,
            "tick_count": 0,
            "instruments_count": 0,
            "followed_count": 0,
        }


def build_otc_live_health_message() -> str:
    health = get_otc_live_feed_health()
    ok_text = "شغال ✅" if health.get("ok") else "في مشكلة ⚠️"
    connected_text = "نعم ✅" if health.get("connected") else "لا ❌"
    enabled_text = "نعم ✅" if health.get("enabled") else "لا ⛔"

    age = health.get("age_seconds")
    if age is None:
        last_tick_text = "لا يوجد"
    elif age < 60:
        last_tick_text = f"منذ {age} ثانية"
    else:
        last_tick_text = f"منذ {age // 60} دقيقة و {age % 60} ثانية"

    return (
        "🩺 حالة OTC Live\n"
        "━━━━━━━━━━━━━━\n"
        f"الحالة: {ok_text}\n"
        f"السبب: {health.get('title')}\n"
        f"النشر مفعّل: {enabled_text}\n"
        f"WebSocket متصل: {connected_text}\n"
        f"آخر tick: {last_tick_text}\n"
        f"آخر زوج وصل منه tick: {health.get('latest_symbol') or 'لا يوجد'}\n"
        f"أزواج لديها ticks: {health.get('tick_count', 0)}\n"
        f"الأزواج المتابعة: {health.get('followed_count', 0)}\n"
        f"الأدوات/الـ payout بالكاش: {health.get('instruments_count', 0)}\n"
        "━━━━━━━━━━━━━━\n"
        f"حد التنبيه: لا ticks لأكثر من {OTC_LIVE_NO_TICKS_ALERT_SECONDS} ثانية"
    )


async def otc_live_feed_health_check_job(context: ContextTypes.DEFAULT_TYPE):
    """يرسل تنبيه للأدمن إذا قناة OTC Live مفعلة لكن WebSocket أو ticks متوقفة."""
    # تنبيهات قناة OTC Live التلقائية ملغاة مع إلغاء القناة.
    return
    try:
        if not OTC_LIVE_HEALTH_CHECK_ENABLED:
            return

        health = get_otc_live_feed_health()
        previous_ok = otc_live_health_state.get("last_ok")
        otc_live_health_state["last_ok"] = bool(health.get("ok"))
        otc_live_health_state["last_reason"] = str(health.get("reason") or "unknown")

        # لا نرسل إنذار إذا القناة متوقفة يدويًا من الإعدادات.
        if str(health.get("reason")) == "disabled":
            return

        if not health.get("ok"):
            now_ts = time_module.time()
            last_alert_at = float(otc_live_health_state.get("last_alert_at") or 0)
            if now_ts - last_alert_at >= OTC_LIVE_HEALTH_ALERT_COOLDOWN_SECONDS:
                otc_live_health_state["last_alert_at"] = now_ts
                await safe_send_message(context.bot,
                    chat_id=ADMIN_TELEGRAM_ID,
                    text="⚠️ تنبيه OTC Live\n\n" + build_otc_live_health_message(),
                )
            return

        # عند رجوع البث بعد مشكلة، نرسل رسالة رجوع مرة واحدة.
        if previous_ok is False and health.get("ok"):
            await safe_send_message(context.bot,
                chat_id=ADMIN_TELEGRAM_ID,
                text="✅ عاد بث OTC Live للعمل\n\n" + build_otc_live_health_message(),
            )
    except Exception as e:
        logger.exception("OTC live feed health alert job error: %s", e)


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
    symbol = signal.get("symbol") or get_otc_symbol_for_pair(pair)
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
    """المضاعفة الذكية ملغاة عمدًا.

    لم نعد نرسل أي رسالة مضاعفة في القناة قبل نهاية الصفقة.
    إذا خسرت الصفقة الأساسية، يتم حساب شمعة مضاعفة واحدة تلقائيًا
    بنفس اتجاه الصفقة الأصلية فقط، ثم تُنشر النتيجة النهائية WIN¹ أو Loss.
    """
    try:
        logger.info("Smart martingale advice disabled: no advice message will be sent")
    except Exception:
        pass
    return



async def delete_martingale_advice_if_direct_win(context: ContextTypes.DEFAULT_TYPE, signal: dict, result: str, martingale_step: int):
    # ===== FULL STOP OTC LIVE BEFORE RESULT/STORAGE =====
    if should_skip_otc_live_work("result_or_storage_disabled"):
        return

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
    symbol = signal.get("symbol") or get_otc_symbol_for_pair(pair)
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

        # مضاعفة واحدة فقط بنفس اتجاه الصفقة الأصلية:
        # لا يوجد مضاعفة ذكية ولا عكس اتجاه ولا رسالة تنبيه مضاعفة بالقناة.
        # إذا خسرت الشمعة الأولى، نحسب نتيجة الشمعة التالية بنفس CALL/PUT الأصلي فقط.
        if result == "loss" and martingale_step == 0:
            next_entry_dt = datetime.fromtimestamp(close_ts, tz=UTC)
            next_close_dt = next_entry_dt + timedelta(seconds=OTC_LIVE_TRADE_DURATION_SECONDS)

            chosen_martingale_direction = direction

            signal["martingale_step"] = 1
            signal["martingale_base_direction"] = direction
            signal["direction"] = chosen_martingale_direction
            signal["martingale_direction"] = chosen_martingale_direction
            signal["martingale_decision_type"] = "same"
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
                "OTC live trade lost first candle, waiting same-direction martingale candle | pair=%s | original=%s | martingale=%s | result_in=%.1fs",
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
        if not is_otc_live_publish_allowed_now():
            logger.info("OTC LIVE absolute guard blocked send_message")
            return
        await safe_send_message(context.bot,
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


def reset_stuck_otc_live_trade_if_needed() -> bool:
    """يفك تعليق حالة active إذا بقيت صفقة OTC معلقة أكثر من الحد المسموح.
    هذا يمنع توقف نشر OTC Live إذا ضاعت job النتيجة أو صار restart جزئي.
    """
    try:
        if not otc_live_channel_state.get("active"):
            return False

        active_since = otc_live_channel_state.get("active_since")
        if not active_since:
            otc_live_channel_state["active_since"] = time_module.time()
            return False

        elapsed = time_module.time() - float(active_since)
        if elapsed >= OTC_LIVE_ACTIVE_TIMEOUT_SECONDS:
            logger.warning(
                "OTC LIVE active trade watchdog reset | active_for=%.1fs | timeout=%ss",
                elapsed,
                OTC_LIVE_ACTIVE_TIMEOUT_SECONDS,
            )

            otc_live_channel_state["active"] = False
            otc_live_channel_state["active_since"] = None
            otc_live_channel_state["martingale_direction"] = None
            otc_live_channel_state["martingale_decision_type"] = None
            otc_live_channel_state["martingale_for_message_id"] = None
            otc_live_channel_state["martingale_advice_message_id"] = None
            otc_live_channel_state["last_published_at"] = time_module.time()
            return True

    except Exception as e:
        logger.exception("OTC LIVE active watchdog error: %s", e)

    return False



# ===== Absolute channel publish safety guards =====
def is_otc_live_publish_allowed_now() -> bool:
    # قناة OTC Live التلقائية ملغاة نهائيًا.
    return False


# ===== OTC Live disabled means full stop =====
def is_otc_live_fully_enabled_now() -> bool:
    """حارس نهائي لقناة OTC Live التلقائية.
    القناة أُلغيت بالكامل من النشر التلقائي، لذلك ترجع False دائمًا.
    هذا لا يوقف بث Quotex المستخدم للأقسام اليدوية.
    """
    return False



def stop_otc_live_runtime_state(reason: str = "disabled"):
    """إيقاف حالة OTC Live الداخلية حتى لا يبقى البوت يحسب نتائج لصفقات غير منشورة."""
    try:
        otc_live_channel_state["active"] = False
        otc_live_channel_state["current_trade"] = None
        otc_live_channel_state["martingale_sent"] = False
        otc_live_channel_state["last_disabled_reason"] = reason
        otc_live_channel_state["disabled_at"] = now_iso()
    except Exception:
        pass


def should_skip_otc_live_work(reason: str = "disabled") -> bool:
    if not is_otc_live_fully_enabled_now():
        stop_otc_live_runtime_state(reason)
        if should_log_quiet("otc_live_full_stop", 300):
            logger.info("OTC LIVE full stop: disabled; skip analysis/results/storage")
        return True
    return False


async def auto_publish_otc_live_channel(context: ContextTypes.DEFAULT_TYPE):
    # قناة النشر التلقائي OTC Live ملغاة بالكامل.
    # لا تنشر أي صفقة ولا تسجل أي نتيجة، مع إبقاء التوليد اليدوي كما هو.
    return
    # ===== FULL STOP OTC LIVE BEFORE ANY WORK =====
    if should_skip_otc_live_work("auto_publish_disabled"):
        return

    # ===== ABSOLUTE OTC LIVE PUBLISH GUARD =====
    if not is_otc_live_publish_allowed_now():
        if should_log_quiet("otc_live_absolute_guard_blocked", 300):
            logger.info("OTC LIVE absolute guard: publish disabled, job stopped before scan")
        return

    live_publish_enabled = bool(OTC_LIVE_CHANNEL_ENABLED and is_channel_publish_enabled("otc_live"))
    remember_otc_live_enabled_state(live_publish_enabled)

    if not live_publish_enabled:
        if should_log_quiet("otc_live_disabled", 300):
            logger.info("OTC LIVE CHANNEL disabled: hard stop active")
        return

    if should_log_quiet("otc_live_scan_started", 300):
        logger.info(
            "OTC LIVE CHANNEL SCAN started | enabled=%s | active=%s | min_quality=%s",
            live_publish_enabled,
            otc_live_channel_state.get("active"),
            OTC_LIVE_MIN_QUALITY
        )

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
        # ===== OTC LIVE HARD STOP disabled gate =====
        remembered_enabled = get_remembered_otc_live_enabled_state()
        if remembered_enabled is False and not should_check_otc_live_enabled_when_disabled():
            return

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

        if not is_otc_live_publish_allowed_now():
            logger.info("OTC LIVE absolute guard blocked send_message")
            return
        sent = await safe_send_message(context.bot,
            chat_id=OTC_LIVE_CHANNEL_ID,
            text=text,
        )

        signal["message_id"] = sent.message_id
        signal["published_at"] = now_iso()
        otc_live_channel_state["last_pair"] = signal.get("pair")

        otc_live_channel_state["active"] = True
        otc_live_channel_state["active_since"] = time_module.time()

        resolve_delay = seconds_until_dt(close_dt) + OTC_LIVE_RESULT_EXTRA_DELAY_SECONDS
        # المضاعفة الذكية ورسائل تنبيه المضاعفة ملغاة.
        # إذا خسرت الصفقة الأساسية، سيتم حساب مضاعفة واحدة تلقائيًا بنفس الاتجاه فقط
        # داخل resolve_otc_live_channel_trade بدون نشر رسالة مضاعفة منفصلة.

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


def build_otc_list_results_message_legacy(raw_text: str) -> tuple[str, dict]:
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


def build_otc_list_results_message(*args, **kwargs):
    legacy_result = build_otc_list_results_message_legacy(*args, **kwargs)
    try:
        if isinstance(legacy_result, tuple):
            result_text, meta = legacy_result
            return prettify_existing_otc_result_text(result_text), meta
        return prettify_existing_otc_result_text(legacy_result)
    except Exception:
        return legacy_result


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




def looks_like_otc_list_text(text: str) -> bool:
    """فحص سريع هل النص يبدو مثل ليستة OTC.
    لا يعتمد على دقة كاملة، فقط يمنع التعامل مع أزرار عادية كليستة.
    """
    try:
        raw = str(text or "")
        if not raw.strip():
            return False

        # لازم يحتوي على OTC وعلى وقت HH:MM وعلى CALL/PUT.
        if "OTC" not in raw.upper():
            return False
        if not re.search(r"\b\d{1,2}:\d{2}\b", raw):
            return False
        if not re.search(r"\b(CALL|PUT|UP|DOWN)\b", raw, re.IGNORECASE):
            return False

        return True
    except Exception:
        return False


async def start_otc_list_watch_for_user(update: Update, context: ContextTypes.DEFAULT_TYPE, raw_list: str, reply_markup):
    user = update.effective_user

    context.user_data["last_otc_list_raw_text"] = raw_list
    context.user_data["last_otc_list_result_text"] = None
    try:
        otc_list_results_ref(user.id).delete()
    except Exception:
        pass

    list_id = str(int(time_module.time()))
    parsed_trades = parse_otc_list_trades(raw_list)

    if not parsed_trades:
        await update.message.reply_text(
            "❌ لم أستطع قراءة أي صفقة من الليستة. تأكد من صيغة الوقت والزوج والاتجاه.",
            reply_markup=reply_markup
        )
        return

    save_otc_list_job(user.id, list_id, raw_list, parsed_trades)

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
        reply_markup=reply_markup
    )


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


def otc_list_result_icon_from_line(line: str) -> str:
    s = str(line or "").lower()
    raw = str(line or "")

    if "no data" in s or "nodata" in s:
        return "⚠️"
    if "doji" in s or "دوجي" in s:
        return "⚖️"
    if "loss" in s or "❌" in raw or "↘️" in raw:
        return "❌"
    if "win¹" in s or "win1" in s or "✅¹" in raw or "✅¹" in raw or "✅¹" in raw:
        return "✅¹"
    if "win" in s or "✅" in raw:
        return "✅"
    return "⚠️"


def otc_list_is_violation_from_line(line: str) -> bool:
    """المخالفة المقصودة: صفقة عكس مومنتم، وليست ربح مضاعفة."""
    raw = str(line or "")
    s = raw.lower()

    # ✅¹ كان تنسيق قديم للربح بالمضاعفة، وليس مخالفة.
    if "✅¹" in raw or "✅¹" in raw:
        return False

    return any(token in s or token in raw for token in [
        "مخالفة",
        "عكس مومنتم",
        "عكس momentum",
        "against momentum",
        "violation",
        "⚠️",
    ])


def format_otc_result_row_fixed(pair: str, time_str: str, direction: str, icon: str) -> str:
    pair = str(pair or "").strip().upper()
    time_str = str(time_str or "").strip()
    direction = str(direction or "").strip().upper()

    if direction == "CALL":
        dir_text = "CALL (Up)"
    elif direction == "PUT":
        dir_text = "PUT  (Down)"
    else:
        dir_text = direction

    return f"{pair:<15} {time_str:<5} {dir_text:<11} {icon}"


def prettify_existing_otc_result_text(result_text: str) -> str:
    """إعادة تنسيق نتائج ليستة OTC:
    ✅ ربح مباشر
    ✅¹ ربح مضاعفة
    ❌ خسارة
    ⚖️ دوجي
    ⚠️ للصفقات المخالفة أو No Data فقط
    """
    try:
        raw = str(result_text or "")
        normal_rows = []
        violation_rows = []

        wins = 0
        losses = 0

        for line in raw.splitlines():
            m = re.search(
                r"([A-Z]{3}/[A-Z]{3}\s*\(OTC\))\s+(\d{1,2}:\d{2})\s+(CALL|PUT)",
                line,
                re.IGNORECASE,
            )
            if not m:
                continue

            icon = otc_list_result_icon_from_line(line)
            is_violation = otc_list_is_violation_from_line(line)

            # الخلاصة النهائية: win/loss فقط.
            if icon in {"✅", "✅¹"}:
                wins += 1
            elif icon == "❌":
                losses += 1

            row = format_otc_result_row_fixed(
                pair=m.group(1),
                time_str=m.group(2),
                direction=m.group(3),
                icon=icon,
            )

            if is_violation:
                violation_rows.append(row)
            else:
                normal_rows.append(row)

        if not normal_rows and not violation_rows:
            return raw

        lines = [
            "🟢 Quotex Results 🟢",
            "━━━━━━━━━━━━━━━━",
            "",
        ]

        lines.extend(normal_rows)

        if violation_rows:
            lines += [
                "",
                "⚠️ الصفقات المخالفة",
                "━━━━━━━━━━━━━━━━",
                *violation_rows,
            ]

        lines += [
            "",
            "━━━━━━━━━━━━━━━━",
            f"{wins} win ✅",
            f"{losses} loss ❌",
            "━━━━━━━━━━━━━━━━",
        ]

        return "<pre>" + html.escape("\n".join(lines)) + "</pre>"

    except Exception as e:
        logger.warning("Could not prettify OTC list result text: %s", e)
        return str(result_text or "")


def normalize_pretty_otc_result_for_telegram(result_text: str) -> str:
    raw = str(result_text or "")
    stripped = raw.strip()

    if stripped.startswith("<pre>") and stripped.endswith("</pre>"):
        inner = html.unescape(stripped.replace("<pre>", "").replace("</pre>", ""))
        return prettify_existing_otc_result_text(inner)

    if stripped.startswith("```") and stripped.endswith("```"):
        inner = stripped.strip("`").strip()
        return prettify_existing_otc_result_text(inner)

    return prettify_existing_otc_result_text(raw)



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
        result_text = prettify_existing_otc_result_text(result_text)
        save_ready_otc_list_result(admin_id, raw_text, result_text, meta)

        get_otc_list_job_ref(admin_id, list_id).update({
            "status": "ready",
            "ready_at": now_iso(),
            "meta": meta,
        })

        await safe_send_message(context.bot,
            chat_id=admin_id,
            text=(
                "✅ نتائج ليستة OTC جاهزة ومحفوظة.\n\n"
                "اضغط الزر بالأسفل لعرض النتائج ونسخها بأي وقت."
            ),
            reply_markup=admin_otc_list_ready_keyboard if is_admin(admin_id) else otc_list_manager_keyboard
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



# ===== Admin-only OTC Edge Engine =====
# هذا القسم لا يظهر للمستخدمين. وظيفته قراءة سلوك سوق OTC المباشر من كاش Quotex
# واستخراج فرص إحصائية/أنماط متكررة بدون أي محاولة اختراق أو تجاوز للمنصة.
OTC_EDGE_MIN_TICKS = int(os.getenv("OTC_EDGE_MIN_TICKS", "60"))
OTC_EDGE_MIN_CANDLES = int(os.getenv("OTC_EDGE_MIN_CANDLES", "6"))
OTC_EDGE_MIN_SCORE = int(os.getenv("OTC_EDGE_MIN_SCORE", "70"))
OTC_EDGE_WATCH_SCORE = int(os.getenv("OTC_EDGE_WATCH_SCORE", "60"))
OTC_EDGE_TOP_LIMIT = int(os.getenv("OTC_EDGE_TOP_LIMIT", "6"))
OTC_EDGE_MIN_PAYOUT = int(os.getenv("OTC_EDGE_MIN_PAYOUT", str(OTC_LIVE_MIN_PAYOUT)))
OTC_EDGE_TICK_MAX_AGE_SECONDS = int(os.getenv("OTC_EDGE_TICK_MAX_AGE_SECONDS", "20"))
OTC_EDGE_HISTORY_LIMIT = int(os.getenv("OTC_EDGE_HISTORY_LIMIT", "80"))
# مراقبة خاصة للأدمن: البوت يفحص بشكل دوري ويرسل تنبيه فقط عند ظهور فرصة قوية.
OTC_EDGE_WATCHER_SCAN_SECONDS = int(os.getenv("OTC_EDGE_WATCHER_SCAN_SECONDS", "7"))
OTC_EDGE_WATCHER_MIN_SCORE = int(os.getenv("OTC_EDGE_WATCHER_MIN_SCORE", "78"))
OTC_EDGE_WATCHER_MIN_PAYOUT = int(os.getenv("OTC_EDGE_WATCHER_MIN_PAYOUT", "85"))
OTC_EDGE_WATCHER_COOLDOWN_SECONDS = int(os.getenv("OTC_EDGE_WATCHER_COOLDOWN_SECONDS", "75"))
OTC_EDGE_WATCHER_SIGNAL_VALID_SECONDS = int(os.getenv("OTC_EDGE_WATCHER_SIGNAL_VALID_SECONDS", "5"))
OTC_EDGE_WATCHER_MAX_ALERTS_PER_HOUR = int(os.getenv("OTC_EDGE_WATCHER_MAX_ALERTS_PER_HOUR", "25"))
OTC_EDGE_WATCHER_TOP_ALERT_POOL = int(os.getenv("OTC_EDGE_WATCHER_TOP_ALERT_POOL", "8"))
# ===== OTC Edge Timing Gate =====
# لا نغيّر عقل التحليل ولا Edge Score. هذه طبقة توقيت فقط حتى يكون الدخول والانتهاء مطابقين لطريقة الصفقة.
# الوضع الافتراضي: الصفقة تنتهي عند إغلاق شمعة M1 الحالية، لذلك لا نرسل التنبيه إلا ببداية الشمعة.
OTC_EDGE_TIMING_MODE = os.getenv("OTC_EDGE_TIMING_MODE", "m1_candle_close").strip().lower()
OTC_EDGE_CANDLE_SECONDS = int(os.getenv("OTC_EDGE_CANDLE_SECONDS", "60"))
OTC_EDGE_WATCHER_TRADE_DURATION_SECONDS = int(os.getenv("OTC_EDGE_WATCHER_TRADE_DURATION_SECONDS", "60"))
OTC_EDGE_WATCHER_TRADE_LOCK_SECONDS = int(os.getenv("OTC_EDGE_WATCHER_TRADE_LOCK_SECONDS", str(OTC_EDGE_WATCHER_TRADE_DURATION_SECONDS + 10)))
OTC_EDGE_TRADE_CLOSE_BUFFER_SECONDS = int(os.getenv("OTC_EDGE_TRADE_CLOSE_BUFFER_SECONDS", "3"))
OTC_EDGE_ENTRY_WINDOW_ENABLED = os.getenv("OTC_EDGE_ENTRY_WINDOW_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
OTC_EDGE_ENTRY_MIN_SECOND = int(os.getenv("OTC_EDGE_ENTRY_MIN_SECOND", "3"))
OTC_EDGE_ENTRY_WINDOW_SECONDS = int(os.getenv("OTC_EDGE_ENTRY_WINDOW_SECONDS", "30"))
OTC_EDGE_ENTRY_LAST_ALERT_SECOND = int(os.getenv(
    "OTC_EDGE_ENTRY_LAST_ALERT_SECOND",
    str(max(1, OTC_EDGE_ENTRY_WINDOW_SECONDS - OTC_EDGE_WATCHER_SIGNAL_VALID_SECONDS))
))

_otc_edge_watcher_state = {
    "enabled": False,
    "mode": "all",  # all | pairs
    "pairs": [],
    "chat_id": ADMIN_TELEGRAM_ID,
    "started_at": None,
    "started_by": None,
    "last_scan_at": None,
    "last_alert_at": None,
    "last_candidate": None,
    "last_error": None,
    "last_alerts": {},
    "alert_times": [],
    "alerts_sent": 0,
    "active_trade_until_ts": 0.0,
    "active_trade": None,
    "entry_window_skips": 0,
}


def _otc_edge_direction_icon(direction: str) -> str:
    direction = str(direction or "").upper()
    if direction == "CALL":
        return "🟢 CALL"
    if direction == "PUT":
        return "🔴 PUT"
    return "⚪ غير محدد"


def _otc_edge_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _otc_edge_movement_metrics(prices: list[float]) -> dict:
    try:
        values = [float(x) for x in prices if x is not None]
        if len(values) < 3:
            return {"range": 0.0, "change": 0.0, "pressure": 0.0, "momentum": 0.0, "up": 0, "down": 0, "density": 0.0}
        rng = max(values) - min(values)
        change = values[-1] - values[0]
        up = sum(1 for a, b in zip(values, values[1:]) if b > a)
        down = sum(1 for a, b in zip(values, values[1:]) if b < a)
        total_directional = max(1, up + down)
        pressure = (up - down) / total_directional
        momentum = min(abs(change) / rng, 1.0) if rng > 0 else 0.0
        density = (up + down) / max(1, len(values) - 1)
        return {
            "range": rng,
            "change": change,
            "pressure": pressure,
            "momentum": momentum,
            "up": up,
            "down": down,
            "density": density,
        }
    except Exception:
        return {"range": 0.0, "change": 0.0, "pressure": 0.0, "momentum": 0.0, "up": 0, "down": 0, "density": 0.0}


def _otc_edge_candle_parts(candle: dict) -> dict:
    try:
        o = float(candle.get("open"))
        h = float(candle.get("high"))
        l = float(candle.get("low"))
        cl = float(candle.get("close"))
        rng = max(h - l, 0.0)
        body = abs(cl - o)
        return {
            "open": o,
            "high": h,
            "low": l,
            "close": cl,
            "range": rng,
            "body": body,
            "body_ratio": body / rng if rng > 0 else 0.0,
            "upper_wick": (h - max(o, cl)) / rng if rng > 0 else 0.0,
            "lower_wick": (min(o, cl) - l) / rng if rng > 0 else 0.0,
            "dir": 1 if cl > o else -1 if cl < o else 0,
        }
    except Exception:
        return {
            "open": 0.0,
            "high": 0.0,
            "low": 0.0,
            "close": 0.0,
            "range": 0.0,
            "body": 0.0,
            "body_ratio": 0.0,
            "upper_wick": 0.0,
            "lower_wick": 0.0,
            "dir": 0,
        }


def _otc_edge_price_text(pair: str, price) -> str:
    try:
        return _safe_price_text(pair, float(price))
    except Exception:
        try:
            return f"{float(price):.5f}"
        except Exception:
            return str(price)


def _otc_edge_history_bias(pair: str, direction: str, limit: int | None = None) -> dict:
    """قراءة سريعة من نتائج OTC Live المسجلة سابقًا إذا كانت موجودة.
    ليست شرطًا للدخول؛ فقط تعطي سياق للأدمن.
    """
    try:
        limit = int(limit or OTC_EDGE_HISTORY_LIMIT)
        trades = get_otc_live_trades(limit=limit) if "get_otc_live_trades" in globals() else []
        decided = [t for t in trades if str(t.get("result")) in {"win", "loss"}]
        same_pair = [t for t in decided if str(t.get("pair")) == str(pair)]
        same_direction = [t for t in same_pair if str(t.get("direction", "")).upper() == str(direction).upper()]

        def _wr(rows):
            if not rows:
                return None
            wins = sum(1 for t in rows if str(t.get("result")) == "win")
            return round((wins / max(1, len(rows))) * 100, 1)

        return {
            "total": len(decided),
            "pair_count": len(same_pair),
            "pair_wr": _wr(same_pair),
            "direction_count": len(same_direction),
            "direction_wr": _wr(same_direction),
        }
    except Exception as e:
        logger.debug("OTC Edge history bias unavailable: %s", e)
        return {"total": 0, "pair_count": 0, "pair_wr": None, "direction_count": 0, "direction_wr": None}


def analyze_otc_edge_pair(pair: str, symbol: str | None = None, include_history: bool = True) -> dict:
    """يحلل زوج OTC واحد ويُرجع أفضل Edge سلوكي حالي.
    لا ينفذ صفقات ولا يغيّر منطق المستخدمين، فقط قراءة أدمن.
    """
    try:
        normalized = normalize_otc_currency_pair_name(pair, symbol) if symbol else normalize_pair_name_basic(pair)
        if not normalized or not is_valid_otc_currency_pair_name(normalized):
            return {"ok": False, "pair": pair, "reason": "اسم الزوج غير صالح أو ليس من أزواج Currencies المسموحة."}

        symbol = symbol or get_otc_symbol_for_pair(normalized)
        if not symbol:
            return {"ok": False, "pair": normalized, "reason": "الزوج غير متاح الآن في خريطة OTC Live أو لم يصل instruments/list بعد."}

        rows, last_tick, candles = _get_otc_rows_and_candles(symbol)
        if len(rows) < OTC_EDGE_MIN_TICKS or len(candles) < OTC_EDGE_MIN_CANDLES or not last_tick:
            return {
                "ok": False,
                "pair": normalized,
                "symbol": symbol,
                "reason": f"بيانات غير كافية للـ Edge: ticks={len(rows)} / candles={len(candles)}.",
            }

        try:
            tick_ts = float(last_tick.get("time") or last_tick.get("ts") or last_tick.get("timestamp") or 0)
            if tick_ts > 1e12:
                tick_ts = tick_ts / 1000.0
            tick_age = time_module.time() - tick_ts if tick_ts else 9999
        except Exception:
            tick_age = 9999

        if tick_age > OTC_EDGE_TICK_MAX_AGE_SECONDS:
            return {
                "ok": False,
                "pair": normalized,
                "symbol": symbol,
                "reason": f"آخر tick قديم منذ {int(tick_age)} ثانية؛ لا نعطي قراءة Edge على بيانات قديمة.",
            }

        instrument = quotex_otc_feed.instrument(symbol) if "quotex_otc_feed" in globals() else {}
        payout = int(float((instrument or {}).get("payout", 0) or 0))
        is_otc = bool((instrument or {}).get("is_otc", True))
        if instrument and (not is_otc or payout < OTC_EDGE_MIN_PAYOUT):
            return {
                "ok": False,
                "pair": normalized,
                "symbol": symbol,
                "reason": f"payout غير مناسب أو الزوج ليس OTC الآن. payout={payout}%",
            }

        current_price = float(last_tick.get("price"))
        prices = [float(r[1]) for r in rows if len(r) >= 2]
        p90 = prices[-90:] if len(prices) >= 90 else prices
        p60 = prices[-60:] if len(prices) >= 60 else prices
        p35 = prices[-35:] if len(prices) >= 35 else prices
        p20 = prices[-20:] if len(prices) >= 20 else prices
        p12 = prices[-12:] if len(prices) >= 12 else prices
        m90 = _otc_edge_movement_metrics(p90)
        m60 = _otc_edge_movement_metrics(p60)
        m35 = _otc_edge_movement_metrics(p35)
        m20 = _otc_edge_movement_metrics(p20)
        m12 = _otc_edge_movement_metrics(p12)

        closed = candles[-9:-1] if len(candles) >= 9 else candles[:-1]
        current_candle = candles[-1] if candles else {}
        cp = _otc_edge_candle_parts(current_candle)
        closed_parts = [_otc_edge_candle_parts(c) for c in closed if c]
        if len(closed_parts) < 4:
            return {"ok": False, "pair": normalized, "symbol": symbol, "reason": "شموع مغلقة غير كافية للقراءة."}

        recent_parts = closed_parts[-6:]
        avg_body = sum(c["body_ratio"] for c in recent_parts) / max(1, len(recent_parts))
        avg_range = sum(c["range"] for c in recent_parts) / max(1, len(recent_parts))
        dirs = [c["dir"] for c in recent_parts]
        rhythm = abs(sum(dirs[-5:])) / max(1, len(dirs[-5:])) if dirs else 0.0
        doji_count = sum(1 for c in recent_parts if c["body_ratio"] <= 0.16)
        wick_heavy = sum(1 for c in recent_parts if max(c["upper_wick"], c["lower_wick"]) >= 0.62)
        recent_high = max(c["high"] for c in recent_parts)
        recent_low = min(c["low"] for c in recent_parts)
        same_dir_4 = sum(dirs[-4:]) if len(dirs) >= 4 else 0
        noisy = (doji_count >= 3 and rhythm < 0.50) or (wick_heavy >= 4 and abs(m20["pressure"]) < 0.25)

        candidates = []

        def add_candidate(kind: str, direction: str, score: float, reason: str, detail: str, risk: str):
            direction = str(direction or "").upper()
            if direction not in {"CALL", "PUT"}:
                return
            score = int(round(max(0, min(100, score))))
            candidates.append({
                "kind": kind,
                "direction": direction,
                "score": score,
                "reason": reason,
                "detail": detail,
                "risk": risk,
            })

        # 1) استمرار زخم: ضغط تيكات واضح + شموع لها جسم محترم.
        trend_call = m60["change"] > 0 and m35["change"] > 0 and m20["change"] > 0 and m35["pressure"] >= 0.22
        trend_put = m60["change"] < 0 and m35["change"] < 0 and m20["change"] < 0 and m35["pressure"] <= -0.22
        if not noisy and (trend_call or trend_put) and m35["momentum"] >= 0.30 and avg_body >= 0.24:
            direction = "CALL" if trend_call else "PUT"
            score = 54 + abs(m35["pressure"]) * 18 + m35["momentum"] * 14 + avg_body * 9 + rhythm * 5
            if cp["body_ratio"] < 0.12:
                score -= 5
            add_candidate(
                "MOMENTUM_CONTINUATION",
                direction,
                score,
                "استمرار زخم قصير",
                "آخر التيكات والشموع تتحرك بضغط واضح في نفس الاتجاه، والسوق ليس عشوائيًا جدًا.",
                "يفشل إذا آخر ثواني قبل الدخول قلبت عكس الضغط أو ظهرت شمعة رفض قوية.",
            )

        # 2) سحب سيولة/رفض من قمة أو قاع قريب.
        if cp["high"] > recent_high and current_price < recent_high and cp["upper_wick"] >= 0.42:
            score = 60 + cp["upper_wick"] * 16 + max(0, -m12["pressure"]) * 14 + min(cp["body_ratio"], 0.6) * 8
            add_candidate(
                "LIQUIDITY_SWEEP_REVERSAL",
                "PUT",
                score,
                "سحب سيولة من قمة قريبة",
                "السعر ضرب قمة قريبة ثم رجع تحتها مع ذيل علوي، وهذا يوحي بفشل الاختراق مؤقتًا.",
                "يفشل إذا عاد السعر وقبل التداول فوق القمة بدل الرفض.",
            )
        if cp["low"] < recent_low and current_price > recent_low and cp["lower_wick"] >= 0.42:
            score = 60 + cp["lower_wick"] * 16 + max(0, m12["pressure"]) * 14 + min(cp["body_ratio"], 0.6) * 8
            add_candidate(
                "LIQUIDITY_SWEEP_REVERSAL",
                "CALL",
                score,
                "سحب سيولة من قاع قريب",
                "السعر ضرب قاع قريب ثم رجع فوقه مع ذيل سفلي، وهذا يوحي بفشل الهبوط مؤقتًا.",
                "يفشل إذا عاد السعر وقبل التداول تحت القاع بدل الرفض.",
            )

        # 3) تمدد زائد ثم بداية ضعف: ليس عكس ترند عشوائي، لازم تظهر علامة استهلاك.
        over_up = same_dir_4 >= 3 and m90["change"] > 0 and m60["momentum"] >= 0.45 and (cp["upper_wick"] >= 0.36 or m12["pressure"] < -0.25)
        over_down = same_dir_4 <= -3 and m90["change"] < 0 and m60["momentum"] >= 0.45 and (cp["lower_wick"] >= 0.36 or m12["pressure"] > 0.25)
        if over_up:
            score = 56 + m60["momentum"] * 17 + cp["upper_wick"] * 12 + max(0, -m12["pressure"]) * 12
            add_candidate(
                "OVEREXTENSION_PULLBACK",
                "PUT",
                score,
                "تمدد صعودي زائد مع بداية ضعف",
                "الصعود ظهر مستهلكًا، ومعه ذيل/ضغط معاكس يوحي بتصحيح قصير.",
                "يفشل إذا كان التمدد تسارع ترند حقيقي وليس استهلاكًا.",
            )
        if over_down:
            score = 56 + m60["momentum"] * 17 + cp["lower_wick"] * 12 + max(0, m12["pressure"]) * 12
            add_candidate(
                "OVEREXTENSION_PULLBACK",
                "CALL",
                score,
                "تمدد هبوطي زائد مع بداية ضعف",
                "الهبوط ظهر مستهلكًا، ومعه ذيل/ضغط معاكس يوحي بتصحيح قصير.",
                "يفشل إذا كان التمدد تسارع ترند حقيقي وليس استهلاكًا.",
            )

        # 4) ضغط/تجميع ثم كسر صغير: قراءة اختراق ضغط ضيق.
        recent_ranges = [c["range"] for c in recent_parts if c["range"] > 0]
        median_range = median(recent_ranges) if recent_ranges else avg_range
        compressed = avg_range > 0 and median_range > 0 and (max(c["high"] for c in recent_parts[-4:]) - min(c["low"] for c in recent_parts[-4:])) <= median_range * 2.4
        if not noisy and compressed and m12["momentum"] >= 0.42 and abs(m12["pressure"]) >= 0.35:
            direction = "CALL" if m12["pressure"] > 0 else "PUT"
            score = 55 + m12["momentum"] * 18 + abs(m12["pressure"]) * 16 + m12["density"] * 6
            add_candidate(
                "COMPRESSION_RELEASE",
                direction,
                score,
                "خروج من ضغط قصير",
                "السعر كان مضغوطًا ثم بدأت التيكات تطلع من النطاق باتجاه واضح.",
                "يفشل إذا كان الخروج كسرًا وهميًا ورجع السعر داخل النطاق.",
            )

        # 5) تبدل مزاج: ضغط كبير باتجاه ثم آخر تيكات انقلبت.
        if m60["pressure"] >= 0.30 and m12["pressure"] <= -0.42 and cp["upper_wick"] >= 0.25:
            score = 58 + abs(m12["pressure"]) * 16 + cp["upper_wick"] * 10 + m12["momentum"] * 10
            add_candidate(
                "MOOD_SHIFT",
                "PUT",
                score,
                "تبدل مزاج من شراء إلى بيع",
                "الضغط الأكبر كان صاعدًا لكن آخر الحركة انقلبت بوضوح مع رفض علوي.",
                "يفشل إذا كان الانقلاب مجرد نفس قصير ضمن الترند الصاعد.",
            )
        if m60["pressure"] <= -0.30 and m12["pressure"] >= 0.42 and cp["lower_wick"] >= 0.25:
            score = 58 + abs(m12["pressure"]) * 16 + cp["lower_wick"] * 10 + m12["momentum"] * 10
            add_candidate(
                "MOOD_SHIFT",
                "CALL",
                score,
                "تبدل مزاج من بيع إلى شراء",
                "الضغط الأكبر كان هابطًا لكن آخر الحركة انقلبت بوضوح مع رفض سفلي.",
                "يفشل إذا كان الانقلاب مجرد نفس قصير ضمن الترند الهابط.",
            )

        if noisy:
            for c in candidates:
                c["score"] = max(0, int(c["score"]) - 10)
                c["risk"] += " السوق فيه ضوضاء/ذيول كثيرة لذلك تم تخفيض التقييم."

        if not candidates:
            return {
                "ok": False,
                "pair": normalized,
                "symbol": symbol,
                "price": current_price,
                "payout": payout,
                "reason": "لا يوجد Edge واضح الآن. الحركة إما عشوائية أو بدون نمط قوي.",
                "metrics": {
                    "pressure": round(m35["pressure"], 2),
                    "momentum": round(m35["momentum"], 2),
                    "avg_body": round(avg_body, 2),
                    "rhythm": round(rhythm, 2),
                    "noise": noisy,
                },
            }

        candidates.sort(key=lambda x: (int(x.get("score", 0)), x.get("kind", "")), reverse=True)
        best = candidates[0]
        history = _otc_edge_history_bias(normalized, best["direction"]) if include_history else {}

        status = "edge" if best["score"] >= OTC_EDGE_MIN_SCORE else "watch" if best["score"] >= OTC_EDGE_WATCH_SCORE else "weak"
        return {
            "ok": True,
            "status": status,
            "pair": normalized,
            "symbol": symbol,
            "direction": best["direction"],
            "score": int(best["score"]),
            "pattern": best["kind"],
            "reason": best["reason"],
            "detail": best["detail"],
            "risk": best["risk"],
            "price": current_price,
            "payout": payout,
            "tick_age": round(tick_age, 1),
            "metrics": {
                "pressure_35": round(m35["pressure"], 2),
                "momentum_35": round(m35["momentum"], 2),
                "pressure_12": round(m12["pressure"], 2),
                "momentum_12": round(m12["momentum"], 2),
                "avg_body": round(avg_body, 2),
                "rhythm": round(rhythm, 2),
                "doji_count": doji_count,
                "wick_heavy": wick_heavy,
                "noisy": noisy,
            },
            "history": history,
            "candidates": candidates[:3],
        }
    except Exception as e:
        logger.exception("OTC Edge pair analysis failed | pair=%s | symbol=%s | error=%s", pair, symbol, e)
        return {"ok": False, "pair": pair, "symbol": symbol, "reason": f"تعذر تحليل الزوج: {e}"}


def scan_otc_edge_market(limit: int | None = None, include_weak: bool = False) -> list[dict]:
    results = []
    try:
        pair_map = get_otc_analysis_pair_map()
        for pair, symbol in pair_map.items():
            normalized = normalize_otc_currency_pair_name(pair, symbol)
            if not normalized or not is_valid_otc_currency_pair_name(normalized):
                continue
            item = analyze_otc_edge_pair(normalized, symbol, include_history=False)
            if not item.get("ok"):
                continue
            if not include_weak and item.get("status") == "weak":
                continue
            results.append(item)
        results.sort(key=lambda x: (int(x.get("score", 0)), int(x.get("payout", 0) or 0)), reverse=True)
        if limit and int(limit) > 0:
            return results[:int(limit)]
        return results
    except Exception as e:
        logger.exception("OTC Edge market scan failed: %s", e)
        return results


def build_otc_edge_menu_message() -> str:
    watcher_status = "شغالة ✅" if _otc_edge_watcher_state.get("enabled") else "متوقفة ⏸"
    return (
        "🧠 OTC Edge Engine - Admin Only\n"
        "━━━━━━━━━━━━━━\n\n"
        "هذا القسم خاص بالأدمن فقط.\n"
        "وظيفته كشف الأنماط السلوكية المتكررة في OTC من بث Quotex Live، بدون نشر للمستخدمين وبدون تغيير نظام الإشارات.\n\n"
        f"👁 حالة المراقبة التلقائية: {watcher_status}\n\n"
        "الأزرار:\n"
        "🔎 فحص السوق الآن: يعرض أفضل الفرص الحالية فورًا.\n"
        "🚀 مراقبة كل السوق: البوت يراقب كل الأزواج ويرسل لك فرصة مباشرة عند ظهور Edge قوي.\n"
        "🎯 مراقبة زوج محدد: تختار زوجًا أو عدة أزواج، والبوت ينبهك فقط عليها.\n"
        "📋 حالة مراقبة Edge: يعرض وضع المراقبة والآخر فرصة رآها البوت.\n"
        "📊 تقرير الأنماط: يلخص نتائج الأنماط السابقة المسجلة إن وجدت.\n"
        "🧪 فحص زوج محدد: يعطيك قراءة تفصيلية مرة واحدة لزوج واحد.\n\n"
        "قاعدة الدخول من تنبيه المراقبة:\n"
        f"ادخل فقط خلال صلاحية التنبيه، والصفقة تكون على إغلاق شمعة M1 الحالية حتى يطابق الدخول تحليل البوت.\n\n"
        "⚠️ هذه قراءة احتمالية وليست ضمان ربح."
    )

def format_otc_edge_item(item: dict, rank: int | None = None, detailed: bool = False) -> str:
    try:
        prefix = f"#{rank} " if rank else ""
        score = int(item.get("score", 0) or 0)
        status_icon = "✅" if score >= OTC_EDGE_MIN_SCORE else "👀" if score >= OTC_EDGE_WATCH_SCORE else "⚠️"
        metrics = item.get("metrics") or {}
        history = item.get("history") or {}
        history_line = ""
        if history.get("direction_wr") is not None and int(history.get("direction_count", 0) or 0) >= 3:
            history_line = f"\n📚 تاريخ قريب لنفس الزوج/الاتجاه: {history.get('direction_wr')}% من {history.get('direction_count')} صفقات"
        elif history.get("pair_wr") is not None and int(history.get("pair_count", 0) or 0) >= 3:
            history_line = f"\n📚 تاريخ قريب لنفس الزوج: {history.get('pair_wr')}% من {history.get('pair_count')} صفقات"

        text = (
            f"{prefix}{status_icon} {item.get('pair')}\n"
            f"📌 الاتجاه: {_otc_edge_direction_icon(item.get('direction'))}\n"
            f"📊 Edge Score: {score}% | payout: {item.get('payout', 0)}%\n"
            f"🧩 النمط: {item.get('reason')}\n"
            f"💵 السعر: {_otc_edge_price_text(item.get('pair', ''), item.get('price'))}\n"
        )
        if detailed:
            text += (
                f"⏱ عمر آخر tick: {item.get('tick_age')} ثانية\n"
                f"🔍 القراءة: {item.get('detail')}\n"
                f"⚠️ متى يفشل؟ {item.get('risk')}\n"
                f"📐 metrics: pressure35={metrics.get('pressure_35')} | momentum35={metrics.get('momentum_35')} | "
                f"pressure12={metrics.get('pressure_12')} | rhythm={metrics.get('rhythm')} | avg_body={metrics.get('avg_body')}"
                f"{history_line}\n"
            )
        return text.strip()
    except Exception as e:
        return f"تعذر تنسيق نتيجة Edge: {e}"


def build_otc_edge_scan_message() -> str:
    try:
        connected = bool(getattr(quotex_otc_feed, "connected", False)) if "quotex_otc_feed" in globals() else False
        started = bool(getattr(quotex_otc_feed, "started", False)) if "quotex_otc_feed" in globals() else False
        results = scan_otc_edge_market(limit=OTC_EDGE_TOP_LIMIT, include_weak=False)
        now_text = now_utc().astimezone(UTC_PLUS_3).strftime("%H:%M:%S")
        header = (
            "🧠 OTC Edge Engine - فحص السوق\n"
            "━━━━━━━━━━━━━━\n"
            f"⏰ وقت الفحص: {now_text} UTC+3\n"
            f"📡 بث Quotex: {'متصل ✅' if connected else 'غير متصل ⚠️'} | started={started}\n"
            f"🎯 حد Edge: {OTC_EDGE_MIN_SCORE}% | حد المراقبة: {OTC_EDGE_WATCH_SCORE}%\n\n"
        )
        if not results:
            return header + (
                "❌ لا يوجد Edge واضح الآن.\n\n"
                "المعنى: السوق إما عشوائي، أو البيانات غير كافية، أو payout أقل من الحد، أو لا يوجد نمط يستحق الدخول.\n"
                "الأفضل تنتظر 30-60 ثانية وتعيد الفحص."
            )
        lines = [header, "أفضل الفرص الحالية:\n"]
        for idx, item in enumerate(results, start=1):
            lines.append(format_otc_edge_item(item, rank=idx, detailed=(idx == 1)))
            lines.append("────────────")
        lines.append("⚠️ خاص بالأدمن: هذه قراءة احتمالية من سلوك السوق وليست ضمانًا أو اختراقًا للمنصة.")
        return "\n".join(lines)[:3900]
    except Exception as e:
        logger.exception("Could not build OTC Edge scan message: %s", e)
        return "تعذر إنشاء تقرير OTC Edge. راجع اللوج."


def build_otc_edge_single_pair_message(pair_text: str) -> str:
    try:
        pair_text = str(pair_text or "").strip()
        symbol = get_otc_symbol_for_pair(pair_text)
        if not symbol:
            # محاولة ثانية لو كتب الرمز الداخلي بدل اسم الزوج.
            normalized_symbol = normalize_otc_pair_input(pair_text)
            pair_map = get_otc_analysis_pair_map()
            reverse = {v: k for k, v in pair_map.items()}
            if normalized_symbol in reverse:
                pair_text = reverse[normalized_symbol]
                symbol = normalized_symbol
        result = analyze_otc_edge_pair(pair_text, symbol=symbol, include_history=True)
        if not result.get("ok"):
            return (
                "🧪 فحص زوج OTC Edge\n"
                "━━━━━━━━━━━━━━\n"
                f"الزوج: {pair_text}\n\n"
                f"❌ لا توجد قراءة صالحة الآن.\nالسبب: {result.get('reason')}"
            )
        return (
            "🧪 فحص زوج OTC Edge\n"
            "━━━━━━━━━━━━━━\n"
            + format_otc_edge_item(result, detailed=True)
            + "\n\n⚠️ خاص بالأدمن فقط. لا تعتمد عليه كضمان ربح؛ استخدمه كفلتر جودة."
        )[:3900]
    except Exception as e:
        logger.exception("Could not build OTC Edge single pair message: %s", e)
        return f"تعذر فحص الزوج: {e}"




def _otc_edge_watch_mode_text() -> str:
    try:
        mode = str(_otc_edge_watcher_state.get("mode") or "all")
        if mode == "pairs":
            pairs = list(_otc_edge_watcher_state.get("pairs") or [])
            if not pairs:
                return "أزواج محددة: لا يوجد"
            if len(pairs) == 1:
                return f"زوج محدد: {pairs[0]}"
            return "أزواج محددة: " + "، ".join(pairs[:8]) + (f" +{len(pairs)-8}" if len(pairs) > 8 else "")
        return "كل السوق"
    except Exception:
        return "غير معروف"


def _otc_edge_resolve_pair_text(pair_text: str) -> tuple[str | None, str | None]:
    """يرجع (pair, symbol) لاسم زوج عادي أو رمز داخلي مثل BRLUSD_otc."""
    try:
        raw = str(pair_text or "").strip()
        if not raw:
            return None, None

        symbol = get_otc_symbol_for_pair(raw)
        if symbol:
            normalized = normalize_otc_currency_pair_name(raw, symbol) or normalize_pair_name_basic(raw)
            return normalized, symbol

        normalized_symbol = normalize_otc_pair_input(raw)
        pair_map = get_otc_analysis_pair_map()
        reverse = {str(v): str(k) for k, v in pair_map.items()}
        if normalized_symbol in reverse:
            return reverse[normalized_symbol], normalized_symbol

        normalized = normalize_pair_name_basic(raw)
        symbol = get_otc_symbol_for_pair(normalized)
        if symbol:
            return normalized, symbol

        if is_valid_otc_currency_pair_name(normalized):
            possible = possible_symbols_for_currency_pair(normalized)
            if possible:
                return normalized, possible[0]
    except Exception:
        pass
    return None, None


def parse_otc_edge_watch_pairs(text: str) -> tuple[list[str], list[str]]:
    """يدعم زوج واحد أو عدة أزواج بأسطر/فواصل."""
    pairs = []
    errors = []
    try:
        raw = str(text or "").strip()
        parts = []
        for chunk in re.split(r"[\n,;]+", raw):
            chunk = chunk.strip()
            if chunk:
                parts.append(chunk)
        for part in parts:
            pair, symbol = _otc_edge_resolve_pair_text(part)
            if pair and symbol:
                if pair not in pairs:
                    pairs.append(pair)
            else:
                errors.append(part)
    except Exception as e:
        errors.append(f"تعذر قراءة الأزواج: {e}")
    return pairs, errors


def start_otc_edge_watcher(chat_id: int, started_by: int, mode: str = "all", pairs: list[str] | None = None) -> str:
    try:
        pairs = list(dict.fromkeys(pairs or []))
        if mode == "pairs" and not pairs:
            return "❌ لم يتم تشغيل المراقبة لأن قائمة الأزواج فارغة."

        _otc_edge_watcher_state.update({
            "enabled": True,
            "mode": "pairs" if mode == "pairs" else "all",
            "pairs": pairs,
            "chat_id": int(chat_id),
            "started_by": int(started_by),
            "started_at": now_iso(),
            "last_scan_at": None,
            "last_alert_at": None,
            "last_candidate": None,
            "last_error": None,
            "last_alerts": {},
            "alert_times": [],
            "alerts_sent": 0,
            "active_trade_until_ts": 0.0,
            "active_trade": None,
            "entry_window_skips": 0,
        })
        return build_otc_edge_watcher_status_message(prefix="✅ تم تشغيل مراقبة Edge.")
    except Exception as e:
        logger.exception("Could not start OTC Edge watcher: %s", e)
        return f"❌ تعذر تشغيل مراقبة Edge: {e}"


def stop_otc_edge_watcher() -> str:
    try:
        was_enabled = bool(_otc_edge_watcher_state.get("enabled"))
        _otc_edge_watcher_state["enabled"] = False
        _otc_edge_watcher_state["last_error"] = None
        return "🛑 تم إيقاف مراقبة OTC Edge." if was_enabled else "مراقبة OTC Edge متوقفة أصلًا."
    except Exception as e:
        return f"تعذر إيقاف المراقبة: {e}"


def _otc_edge_alert_rate_limited(now_ts: float) -> bool:
    try:
        times = [float(t) for t in (_otc_edge_watcher_state.get("alert_times") or []) if now_ts - float(t) < 3600]
        _otc_edge_watcher_state["alert_times"] = times
        return len(times) >= int(OTC_EDGE_WATCHER_MAX_ALERTS_PER_HOUR)
    except Exception:
        return False


def _otc_edge_timing_mode() -> str:
    """وضع توقيت الإشارة. الافتراضي يطابق إغلاق شمعة M1 الحالية."""
    try:
        mode = str(OTC_EDGE_TIMING_MODE or "m1_candle_close").strip().lower()
        if mode in {"fixed", "fixed_60", "fixed_60s", "60s"}:
            return "fixed_60s"
        return "m1_candle_close"
    except Exception:
        return "m1_candle_close"


def _otc_edge_current_candle_timing() -> dict:
    """تفاصيل توقيت شمعة M1 الحالية حسب UTC+3.
    نعتمدها فقط للبوابة الزمنية، بدون أي تغيير على حساب Edge نفسه.
    """
    try:
        dt = now_utc().astimezone(UTC_PLUS_3)
        second = float(dt.second) + (float(dt.microsecond) / 1_000_000.0)
        candle_seconds = max(30, int(OTC_EDGE_CANDLE_SECONDS))
        minute_start = dt.replace(second=0, microsecond=0)
        close_dt = minute_start + timedelta(seconds=candle_seconds)
        remaining = max(0.0, (close_dt - dt).total_seconds())
        return {
            "now": dt,
            "second": second,
            "remaining": remaining,
            "close_dt": close_dt,
            "close_text": close_dt.strftime("%H:%M:%S"),
        }
    except Exception:
        dt = now_utc().astimezone(UTC_PLUS_3)
        return {"now": dt, "second": 999.0, "remaining": 0.0, "close_dt": dt, "close_text": dt.strftime("%H:%M:%S")}


def _otc_edge_entry_window_status() -> tuple[bool, str, dict]:
    """بوابة توقيت للدخول. لا تغيّر اتجاه الصفقة ولا النمط، فقط تمنع التنبيه المتأخر."""
    try:
        if not OTC_EDGE_ENTRY_WINDOW_ENABLED:
            return True, "فلتر توقيت الدخول متوقف", _otc_edge_current_candle_timing()

        timing = _otc_edge_current_candle_timing()
        if _otc_edge_timing_mode() != "m1_candle_close":
            # في وضع 60 ثانية ثابتة نستخدم الفلتر كحماية اختيارية فقط.
            return True, "وضع 60 ثانية ثابتة", timing

        sec = float(timing.get("second", 999.0) or 999.0)
        remaining = float(timing.get("remaining", 0.0) or 0.0)
        min_second = max(0, int(OTC_EDGE_ENTRY_MIN_SECOND))
        # آخر ثانية نسمح فيها بإرسال التنبيه، حتى تبقى صلاحية التنبيه داخل أول 30 ثانية.
        latest_alert_second = min(
            max(min_second, int(OTC_EDGE_ENTRY_LAST_ALERT_SECOND)),
            max(min_second, int(OTC_EDGE_ENTRY_WINDOW_SECONDS) - int(OTC_EDGE_WATCHER_SIGNAL_VALID_SECONDS)),
        )
        if sec < min_second:
            return False, f"بداية الشمعة مبكرة جدًا: الثانية {sec:.1f} أقل من {min_second}", timing
        if sec > latest_alert_second:
            return False, f"توقيت متأخر: الثانية {sec:.1f} بعد حد التنبيه {latest_alert_second}", timing
        if remaining <= int(OTC_EDGE_WATCHER_SIGNAL_VALID_SECONDS):
            return False, f"الوقت المتبقي للإغلاق قليل جدًا: {remaining:.1f} ثانية", timing
        return True, "داخل نافذة الدخول المطابقة لإغلاق شمعة M1", timing
    except Exception as e:
        return True, f"تعذر فحص بوابة التوقيت: {e}", _otc_edge_current_candle_timing()


def _otc_edge_entry_window_ok() -> bool:
    ok, reason, timing = _otc_edge_entry_window_status()
    if not ok:
        try:
            _otc_edge_watcher_state["last_timing_skip"] = reason
            _otc_edge_watcher_state["last_timing_skip_at"] = now_iso()
        except Exception:
            pass
    return bool(ok)


def _otc_edge_active_trade_remaining(now_ts: float | None = None) -> int:
    try:
        now_ts = float(now_ts or time_module.time())
        until_ts = float(_otc_edge_watcher_state.get("active_trade_until_ts") or 0)
        remain = int(round(until_ts - now_ts))
        if remain <= 0:
            _otc_edge_watcher_state["active_trade_until_ts"] = 0.0
            _otc_edge_watcher_state["active_trade"] = None
            return 0
        return remain
    except Exception:
        return 0


def _otc_edge_set_active_trade_lock(item: dict, now_ts: float | None = None):
    try:
        now_ts = float(now_ts or time_module.time())
        timing = _otc_edge_current_candle_timing()
        if _otc_edge_timing_mode() == "m1_candle_close":
            # القفل يطابق الصفقة: حتى إغلاق الشمعة الحالية + هامش صغير.
            lock_seconds = max(1, int(float(timing.get("remaining", 0) or 0)) + int(OTC_EDGE_TRADE_CLOSE_BUFFER_SECONDS))
            close_at = timing.get("close_dt").isoformat() if timing.get("close_dt") else None
        else:
            lock_seconds = max(int(OTC_EDGE_WATCHER_TRADE_DURATION_SECONDS), int(OTC_EDGE_WATCHER_TRADE_LOCK_SECONDS))
            close_at = (now_utc().astimezone(UTC_PLUS_3) + timedelta(seconds=lock_seconds)).isoformat()

        _otc_edge_watcher_state["active_trade_until_ts"] = now_ts + lock_seconds
        _otc_edge_watcher_state["active_trade"] = {
            "pair": item.get("pair"),
            "direction": item.get("direction"),
            "score": item.get("score"),
            "payout": item.get("payout"),
            "pattern": item.get("pattern"),
            "started_at": now_iso(),
            "timing_mode": _otc_edge_timing_mode(),
            "close_at": close_at,
            "close_text": timing.get("close_text"),
            "entry_second": round(float(timing.get("second", 0) or 0), 1),
            "lock_seconds": lock_seconds,
        }
    except Exception:
        pass


def _otc_edge_can_alert(item: dict, now_ts: float) -> bool:
    try:
        if not item or not item.get("ok"):
            return False
        if _otc_edge_active_trade_remaining(now_ts) > 0:
            return False
        if not _otc_edge_entry_window_ok():
            _otc_edge_watcher_state["entry_window_skips"] = int(_otc_edge_watcher_state.get("entry_window_skips", 0) or 0) + 1
            return False
        if int(item.get("score", 0) or 0) < int(OTC_EDGE_WATCHER_MIN_SCORE):
            return False
        if int(item.get("payout", 0) or 0) < int(OTC_EDGE_WATCHER_MIN_PAYOUT):
            return False
        if float(item.get("tick_age", 999) or 999) > max(3, int(OTC_EDGE_TICK_MAX_AGE_SECONDS)):
            return False
        if _otc_edge_alert_rate_limited(now_ts):
            return False
        key = f"{item.get('pair')}|{item.get('direction')}|{item.get('pattern')}"
        last_alerts = _otc_edge_watcher_state.setdefault("last_alerts", {})
        last_ts = float(last_alerts.get(key, 0) or 0)
        if now_ts - last_ts < int(OTC_EDGE_WATCHER_COOLDOWN_SECONDS):
            return False
        last_alerts[key] = now_ts
        return True
    except Exception:
        return False


def _otc_edge_collect_watcher_candidates() -> list[dict]:
    try:
        mode = str(_otc_edge_watcher_state.get("mode") or "all")
        candidates = []
        if mode == "pairs":
            for pair in list(_otc_edge_watcher_state.get("pairs") or []):
                symbol = get_otc_symbol_for_pair(pair)
                if not symbol:
                    resolved_pair, resolved_symbol = _otc_edge_resolve_pair_text(pair)
                    pair = resolved_pair or pair
                    symbol = resolved_symbol
                if not symbol:
                    continue
                item = analyze_otc_edge_pair(pair, symbol=symbol, include_history=True)
                if item.get("ok"):
                    candidates.append(item)
        else:
            candidates = scan_otc_edge_market(limit=OTC_EDGE_WATCHER_TOP_ALERT_POOL, include_weak=False)

        candidates = [c for c in candidates if c.get("ok")]
        candidates.sort(key=lambda x: (int(x.get("score", 0) or 0), int(x.get("payout", 0) or 0)), reverse=True)
        return candidates
    except Exception as e:
        _otc_edge_watcher_state["last_error"] = str(e)
        logger.exception("OTC Edge watcher collect failed: %s", e)
        return []


def build_otc_edge_entry_alert_message(item: dict) -> str:
    try:
        timing = _otc_edge_current_candle_timing()
        now_text = timing.get("now", now_utc().astimezone(UTC_PLUS_3)).strftime("%H:%M:%S")
        mode_text = _otc_edge_watch_mode_text()
        score = int(item.get("score", 0) or 0)
        tick_age = item.get("tick_age")
        metrics = item.get("metrics") or {}
        timing_mode = _otc_edge_timing_mode()

        if timing_mode == "m1_candle_close":
            remaining = int(float(timing.get("remaining", 0) or 0))
            sec = float(timing.get("second", 0) or 0)
            timing_lines = (
                "🕯 نظام الصفقة: M1 Candle Close\n"
                f"🟢 الدخول: الآن خلال {OTC_EDGE_WATCHER_SIGNAL_VALID_SECONDS} ثواني فقط\n"
                f"🏁 انتهاء الصفقة: {timing.get('close_text')} UTC+3\n"
                f"⏱ المتبقي لإغلاق الشمعة: {remaining} ثانية | الثانية الحالية: {sec:.1f}\n"
                f"🚫 لا تدخل إذا وصلت بعد الثانية {OTC_EDGE_ENTRY_WINDOW_SECONDS} من الشمعة.\n"
            )
        else:
            timing_lines = (
                "🕐 نظام الصفقة: Fixed 60s\n"
                f"🟢 الدخول: الآن خلال {OTC_EDGE_WATCHER_SIGNAL_VALID_SECONDS} ثواني فقط\n"
                f"🏁 انتهاء الصفقة: بعد {OTC_EDGE_WATCHER_TRADE_DURATION_SECONDS} ثانية من لحظة دخولك\n"
            )

        return (
            "🚨 OTC Edge Alert - فرصة مباشرة\n"
            "━━━━━━━━━━━━━━\n"
            f"⏰ وقت التنبيه: {now_text} UTC+3\n"
            f"👁 المراقبة: {mode_text}\n\n"
            "✅ قرار الدخول: ادخل الآن فقط إذا كنت جاهزًا\n"
            f"⏳ صلاحية التنبيه: {OTC_EDGE_WATCHER_SIGNAL_VALID_SECONDS} ثواني\n"
            f"{timing_lines}"
            f"🔒 لن يتم إرسال فرصة جديدة قبل انتهاء هذه الصفقة.\n\n"
            f"💱 الزوج: {item.get('pair')}\n"
            f"📌 الاتجاه: {_otc_edge_direction_icon(item.get('direction'))}\n"
            f"📊 Edge Score: {score}% | payout: {item.get('payout', 0)}%\n"
            f"🧩 النمط: {item.get('reason')}\n"
            f"💵 السعر الآن: {_otc_edge_price_text(item.get('pair', ''), item.get('price'))}\n"
            f"⏱ عمر آخر tick: {tick_age} ثانية\n\n"
            f"🔍 القراءة: {item.get('detail')}\n"
            f"⚠️ إلغاء الدخول إذا تأخرت، أو السعر عكس بقوة، أو ظهرت شمعة رفض ضد الاتجاه.\n"
            f"📐 p35={metrics.get('pressure_35')} | m35={metrics.get('momentum_35')} | p12={metrics.get('pressure_12')}"
        )[:3900]
    except Exception as e:
        return f"🚨 OTC Edge Alert\nتعذر تنسيق التنبيه: {e}"


def build_otc_edge_watcher_status_message(prefix: str | None = None) -> str:
    try:
        enabled = bool(_otc_edge_watcher_state.get("enabled"))
        connected = bool(getattr(quotex_otc_feed, "connected", False)) if "quotex_otc_feed" in globals() else False
        started = bool(getattr(quotex_otc_feed, "started", False)) if "quotex_otc_feed" in globals() else False
        started_at = _otc_edge_watcher_state.get("started_at")
        last_scan_at = _otc_edge_watcher_state.get("last_scan_at")
        last_alert_at = _otc_edge_watcher_state.get("last_alert_at")
        last_candidate = _otc_edge_watcher_state.get("last_candidate") or {}
        lines = []
        if prefix:
            lines.append(str(prefix))
            lines.append("")
        lines.extend([
            "📋 حالة مراقبة OTC Edge",
            "━━━━━━━━━━━━━━",
            f"الحالة: {'شغالة ✅' if enabled else 'متوقفة ⏸'}",
            f"الوضع: {_otc_edge_watch_mode_text()}",
            f"📡 بث Quotex: {'متصل ✅' if connected else 'غير متصل ⚠️'} | started={started}",
            f"🎯 حد التنبيه: Edge {OTC_EDGE_WATCHER_MIN_SCORE}% | payout {OTC_EDGE_WATCHER_MIN_PAYOUT}%",
            f"⏱ الفحص كل: {OTC_EDGE_WATCHER_SCAN_SECONDS} ثواني",
            f"🧊 منع تكرار نفس النمط: {OTC_EDGE_WATCHER_COOLDOWN_SECONDS} ثانية",
            f"🧭 وضع التوقيت: {'إغلاق شمعة M1 ✅' if _otc_edge_timing_mode() == 'm1_candle_close' else 'Fixed 60s'}",
            f"🪟 نافذة التنبيه: من الثانية {OTC_EDGE_ENTRY_MIN_SECOND} إلى {min(OTC_EDGE_ENTRY_LAST_ALERT_SECOND, OTC_EDGE_ENTRY_WINDOW_SECONDS - OTC_EDGE_WATCHER_SIGNAL_VALID_SECONDS)} | الدخول مسموح حتى الثانية {OTC_EDGE_ENTRY_WINDOW_SECONDS}",
            f"🔒 القفل بعد التنبيه: حتى نهاية الصفقة الحالية",
            f"📨 تنبيهات مرسلة منذ التشغيل: {_otc_edge_watcher_state.get('alerts_sent', 0)}",
        ])
        remain = _otc_edge_active_trade_remaining(time_module.time())
        if remain > 0:
            active_trade = _otc_edge_watcher_state.get("active_trade") or {}
            close_extra = ""
            if active_trade.get("close_text"):
                close_extra = f" | الإغلاق: {active_trade.get('close_text')} UTC+3"
            lines.extend([
                "",
                "🔒 صفقة حالية تحت المتابعة:",
                f"• {active_trade.get('pair')} {_otc_edge_direction_icon(active_trade.get('direction'))} | باقي تقريبًا {remain} ثانية{close_extra}",
            ])
        if _otc_edge_watcher_state.get("last_timing_skip"):
            lines.extend([
                "",
                f"⏳ آخر منع بسبب التوقيت: {_otc_edge_watcher_state.get('last_timing_skip')}",
            ])
        if started_at:
            lines.append(f"بدأت: {format_dt_ar(started_at)}")
        if last_scan_at:
            lines.append(f"آخر فحص: {format_dt_ar(last_scan_at)}")
        if last_alert_at:
            lines.append(f"آخر تنبيه: {format_dt_ar(last_alert_at)}")
        if last_candidate:
            lines.extend([
                "",
                "آخر فرصة رآها البوت:",
                f"• {last_candidate.get('pair')} {_otc_edge_direction_icon(last_candidate.get('direction'))} | {last_candidate.get('score')}% | payout {last_candidate.get('payout')}%",
            ])
        if _otc_edge_watcher_state.get("last_error"):
            lines.append(f"⚠️ آخر خطأ: {_otc_edge_watcher_state.get('last_error')}")
        return "\n".join(lines)[:3900]
    except Exception as e:
        return f"تعذر عرض حالة المراقبة: {e}"


async def otc_edge_watcher_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        if not bool(_otc_edge_watcher_state.get("enabled")):
            return
        _otc_edge_watcher_state["last_scan_at"] = now_iso()

        candidates = _otc_edge_collect_watcher_candidates()
        if candidates:
            _otc_edge_watcher_state["last_candidate"] = {
                "pair": candidates[0].get("pair"),
                "direction": candidates[0].get("direction"),
                "score": candidates[0].get("score"),
                "payout": candidates[0].get("payout"),
                "pattern": candidates[0].get("pattern"),
            }

        now_ts = time_module.time()
        for item in candidates:
            if not _otc_edge_can_alert(item, now_ts):
                continue
            chat_id = int(_otc_edge_watcher_state.get("chat_id") or ADMIN_TELEGRAM_ID)
            sent = await safe_send_message(
                context.bot,
                chat_id=chat_id,
                text=build_otc_edge_entry_alert_message(item),
            )
            if sent:
                _otc_edge_set_active_trade_lock(item, now_ts)
                _otc_edge_watcher_state["last_alert_at"] = now_iso()
                _otc_edge_watcher_state["alerts_sent"] = int(_otc_edge_watcher_state.get("alerts_sent", 0) or 0) + 1
                times = list(_otc_edge_watcher_state.get("alert_times") or [])
                times.append(now_ts)
                _otc_edge_watcher_state["alert_times"] = [float(t) for t in times if now_ts - float(t) < 3600]
            break
    except Exception as e:
        _otc_edge_watcher_state["last_error"] = str(e)
        logger.exception("OTC Edge watcher job error: %s", e)


def build_otc_edge_patterns_report() -> str:
    try:
        trades = get_otc_live_trades(limit=OTC_EDGE_HISTORY_LIMIT) if "get_otc_live_trades" in globals() else []
        decided = [t for t in trades if str(t.get("result")) in {"win", "loss"}]
        if not decided:
            return (
                "📊 تقرير الأنماط - OTC Edge\n"
                "━━━━━━━━━━━━━━\n\n"
                "لا توجد نتائج OTC Live كافية محفوظة بعد لبناء تقرير تاريخي.\n"
                "يمكنك استخدام زر 🔎 فحص السوق الآن للحصول على قراءة مباشرة من الكاش الحي."
            )

        pair_stats = {}
        direction_stats = {}
        for t in decided:
            pair = str(t.get("pair") or "unknown")
            direction = str(t.get("direction") or "unknown").upper()
            result = str(t.get("result"))
            p = pair_stats.setdefault(pair, {"total": 0, "wins": 0, "losses": 0})
            d = direction_stats.setdefault(f"{pair} {direction}", {"pair": pair, "direction": direction, "total": 0, "wins": 0, "losses": 0})
            for bucket in (p, d):
                bucket["total"] += 1
                if result == "win":
                    bucket["wins"] += 1
                elif result == "loss":
                    bucket["losses"] += 1

        def _rate(row):
            return round(row["wins"] / max(1, row["total"]) * 100, 1)

        best_pairs = sorted(pair_stats.items(), key=lambda kv: (_rate(kv[1]), kv[1]["total"]), reverse=True)[:8]
        best_dirs = sorted(direction_stats.values(), key=lambda v: (_rate(v), v["total"]), reverse=True)[:8]

        lines = [
            "📊 تقرير الأنماط - OTC Edge",
            "━━━━━━━━━━━━━━",
            f"آخر نتائج محسوبة: {len(decided)} صفقة",
            "",
            "أفضل الأزواج تاريخيًا في آخر النتائج:",
        ]
        for pair, stat in best_pairs:
            lines.append(f"• {pair}: {_rate(stat)}% | {stat['wins']}W/{stat['losses']}L من {stat['total']}")

        lines.append("")
        lines.append("أفضل زوج + اتجاه:")
        for stat in best_dirs:
            if stat["total"] < 2:
                continue
            lines.append(f"• {stat['pair']} {_otc_edge_direction_icon(stat['direction'])}: {_rate(stat)}% من {stat['total']}")

        lines.append("")
        lines.append("ملاحظة: هذا التقرير يعتمد على النتائج المحفوظة فقط، وليس كشفًا لخوارزمية المنصة.")
        return "\n".join(lines)[:3900]
    except Exception as e:
        logger.exception("Could not build OTC Edge patterns report: %s", e)
        return "تعذر إنشاء تقرير الأنماط. راجع اللوج."



# ===== Beta channel: 3 same-color candles continuation strategy =====
# إضافة اختبارية مستقلة عن OTC Edge Engine وOTC SNIPER.
# تنشر على قناة Telegram فقط إذا تم ضبط THREE_CANDLE_CHANNEL_ID وتفعيل THREE_CANDLE_CHANNEL_ENABLED.
THREE_CANDLE_CHANNEL_ID_RAW = os.getenv("THREE_CANDLE_CHANNEL_ID", "").strip()
THREE_CANDLE_CHANNEL_ENABLED = os.getenv("THREE_CANDLE_CHANNEL_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
THREE_CANDLE_MIN_PAYOUT = int(os.getenv("THREE_CANDLE_MIN_PAYOUT", "80"))
THREE_CANDLE_SCAN_SECONDS = int(os.getenv("THREE_CANDLE_SCAN_SECONDS", "2"))
THREE_CANDLE_ALERT_REMAINING_SECONDS = float(os.getenv("THREE_CANDLE_ALERT_REMAINING_SECONDS", "10"))
THREE_CANDLE_MIN_REMAINING_SECONDS = float(os.getenv("THREE_CANDLE_MIN_REMAINING_SECONDS", "2"))
THREE_CANDLE_RESULT_DELAY_SECONDS = int(os.getenv("THREE_CANDLE_RESULT_DELAY_SECONDS", "5"))
THREE_CANDLE_PAIR_LOSS_LIMIT = int(os.getenv("THREE_CANDLE_PAIR_LOSS_LIMIT", "2"))
THREE_CANDLE_PAIR_COOLDOWN_SECONDS = int(os.getenv("THREE_CANDLE_PAIR_COOLDOWN_SECONDS", "1800"))
THREE_CANDLE_TIE_EPSILON = float(os.getenv("THREE_CANDLE_TIE_EPSILON", str(OTC_LIVE_TIE_EPSILON)))
THREE_CANDLE_DAILY_LIMIT_DEFAULT = int(os.getenv("THREE_CANDLE_DAILY_LIMIT", "0"))  # 0 = مفتوح

_three_candle_channel_state = {
    "pending_trade": None,
    "last_signal_buckets": {},
    "pair_loss_streaks": {},
    "pair_cooldowns": {},
    "signals_sent": 0,
    "results_sent": 0,
    "cancelled_sent": 0,
    "today_signals": 0,
    "last_scan_at": None,
    "last_error": None,
}


def _three_candle_channel_id():
    raw = str(THREE_CANDLE_CHANNEL_ID_RAW or "").strip()
    if not raw:
        return None
    try:
        if raw.lstrip("-").isdigit():
            return int(raw)
    except Exception:
        pass
    return raw


def _three_candle_settings_ref():
    return system_ref().child("three_candle_channel").child("settings")


def _three_candle_results_ref():
    return system_ref().child("three_candle_channel").child("results")


def _three_candle_daily_ref(day_key: str | None = None):
    return system_ref().child("three_candle_channel").child("daily").child(day_key or get_utc3_day_key())


def _three_candle_get_settings() -> dict:
    default = {
        "enabled": bool(THREE_CANDLE_CHANNEL_ENABLED),
        "daily_limit": int(THREE_CANDLE_DAILY_LIMIT_DEFAULT),
    }
    try:
        data = _three_candle_settings_ref().get() or {}
        if not isinstance(data, dict):
            data = {}
        return {
            "enabled": bool(data.get("enabled", default["enabled"])),
            "daily_limit": int(data.get("daily_limit", default["daily_limit"]) or 0),
        }
    except Exception as e:
        logger.exception("Could not read three-candle channel settings: %s", e)
        return default


def _three_candle_set_enabled(enabled: bool) -> bool:
    try:
        _three_candle_settings_ref().update({"enabled": bool(enabled), "updated_at": now_iso()})
        return True
    except Exception as e:
        logger.exception("Could not update three-candle enabled: %s", e)
        return False


def _three_candle_set_daily_limit(limit: int) -> bool:
    try:
        _three_candle_settings_ref().update({"daily_limit": max(0, int(limit)), "updated_at": now_iso()})
        return True
    except Exception as e:
        logger.exception("Could not update three-candle daily limit: %s", e)
        return False


def _three_candle_today_signal_count() -> int:
    try:
        return int((_three_candle_daily_ref().get() or {}).get("signals", 0) or 0)
    except Exception:
        return int(_three_candle_channel_state.get("today_signals", 0) or 0)


def _three_candle_increment_today_signal_count():
    try:
        ref = _three_candle_daily_ref()
        data = ref.get() or {}
        if not isinstance(data, dict):
            data = {}
        ref.update({
            "signals": int(data.get("signals", 0) or 0) + 1,
            "day": get_utc3_day_key(),
            "updated_at": now_iso(),
        })
    except Exception as e:
        logger.debug("Could not increment three-candle daily count: %s", e)
    try:
        _three_candle_channel_state["today_signals"] = int(_three_candle_channel_state.get("today_signals", 0) or 0) + 1
    except Exception:
        pass


def _three_candle_daily_limit_reached() -> bool:
    try:
        settings = _three_candle_get_settings()
        limit = int(settings.get("daily_limit", 0) or 0)
        if limit <= 0:
            return False
        return _three_candle_today_signal_count() >= limit
    except Exception:
        return False


def _three_candle_is_enabled() -> bool:
    settings = _three_candle_get_settings()
    return bool(settings.get("enabled") and _three_candle_channel_id())


def _three_candle_direction_icon(direction: str) -> str:
    direction = str(direction or "").upper()
    if direction == "CALL":
        return "🟢 CALL"
    if direction == "PUT":
        return "🔴 PUT"
    return "⚪"


def _three_candle_result_for_direction(direction: str, open_price, close_price) -> str:
    try:
        o = float(open_price)
        c = float(close_price)
        if abs(c - o) <= float(THREE_CANDLE_TIE_EPSILON):
            return "draw"
        direction = str(direction or "").upper()
        if direction == "CALL":
            return "win" if c > o else "loss"
        if direction == "PUT":
            return "win" if c < o else "loss"
        return "unknown"
    except Exception:
        return "unknown"


def _three_candle_bucket_now() -> tuple[int, float]:
    now_ts = time_module.time()
    bucket_ts = int(now_ts // 60) * 60
    remaining = float(bucket_ts + 60 - now_ts)
    return bucket_ts, remaining


def _three_candle_get_sorted_candles(symbol: str) -> list[dict]:
    try:
        with quotex_otc_feed.lock:
            candles_map = dict((quotex_otc_feed.candles.get(symbol) or {}))
        return [dict(c) for _, c in sorted(candles_map.items())]
    except Exception:
        return []


def _three_candle_pair_on_cooldown(pair: str) -> tuple[bool, int]:
    try:
        now_ts = time_module.time()
        until_ts = float((_three_candle_channel_state.get("pair_cooldowns") or {}).get(pair, 0) or 0)
        if until_ts > now_ts:
            return True, int(round(until_ts - now_ts))
        if until_ts:
            (_three_candle_channel_state.get("pair_cooldowns") or {}).pop(pair, None)
        return False, 0
    except Exception:
        return False, 0


def _three_candle_register_final_result(pair: str, result: str):
    try:
        streaks = _three_candle_channel_state.setdefault("pair_loss_streaks", {})
        cooldowns = _three_candle_channel_state.setdefault("pair_cooldowns", {})
        if result == "loss":
            streaks[pair] = int(streaks.get(pair, 0) or 0) + 1
            if int(streaks[pair]) >= int(THREE_CANDLE_PAIR_LOSS_LIMIT):
                cooldowns[pair] = time_module.time() + int(THREE_CANDLE_PAIR_COOLDOWN_SECONDS)
                streaks[pair] = 0
        elif result in {"win", "draw"}:
            streaks[pair] = 0
    except Exception:
        pass


def _three_candle_record_result(trade: dict, result_type: str, result_text: str):
    try:
        record = {
            "created_at": now_iso(),
            "day": get_utc3_day_key(),
            "pair": str((trade or {}).get("pair", "")),
            "symbol": str((trade or {}).get("symbol", "")),
            "direction": str((trade or {}).get("direction", "")),
            "result_type": str(result_type),
            "result_text": str(result_text),
            "payout": safe_int((trade or {}).get("payout", 0), 0),
        }
        key = f"{int(time_module.time() * 1000)}_{safe_key(record.get('pair'))}"
        _three_candle_results_ref().child(key).set(record)
    except Exception as e:
        logger.debug("Could not record three-candle result: %s", e)


def _three_candle_fetch_result_records() -> list[dict]:
    try:
        data = _three_candle_results_ref().get() or {}
        if not isinstance(data, dict):
            return []
        items = []
        for k, v in data.items():
            if isinstance(v, dict):
                item = dict(v)
                item["_key"] = k
                items.append(item)
        items.sort(key=lambda x: str(x.get("created_at", "")))
        return items
    except Exception as e:
        logger.exception("Could not fetch three-candle result records: %s", e)
        return []


def build_three_candle_channel_summary(limit: int | None = None) -> str:
    records = _three_candle_fetch_result_records()
    if limit and int(limit) > 0:
        records = records[-int(limit):]
        title = f"📊 ملخص قناة 3 شموع - آخر {int(limit)} نتيجة"
    else:
        title = "📊 ملخص قناة 3 شموع - من بداية القناة"

    direct_win = sum(1 for r in records if r.get("result_type") == "direct_win")
    mg_win = sum(1 for r in records if r.get("result_type") == "mg_win")
    loss = sum(1 for r in records if r.get("result_type") == "loss")
    draw = sum(1 for r in records if r.get("result_type") == "draw")
    cancelled = sum(1 for r in records if r.get("result_type") == "cancelled")
    total_closed = direct_win + mg_win + loss + draw
    wins = direct_win + mg_win
    win_rate = round((wins / total_closed) * 100, 1) if total_closed else 0

    return (
        f"{title}\n"
        "━━━━━━━━━━━━━━\n"
        f"📌 النتائج المحسوبة: {total_closed}\n"
        f"Win✅ مباشر: {direct_win}\n"
        f"Win✅. مضاعفة: {mg_win}\n"
        f"Lose💔: {loss}\n"
        f"🟰doji: {draw}\n"
        f"🚫 إلغاء: {cancelled}\n"
        f"📈 نسبة الربح: {win_rate}%"
    )[:3900]


def build_three_candle_channel_status() -> str:
    settings = _three_candle_get_settings()
    channel_id = _three_candle_channel_id()
    limit = int(settings.get("daily_limit", 0) or 0)
    today_count = _three_candle_today_signal_count()
    pending = _three_candle_channel_state.get("pending_trade")
    return (
        "📋 حالة قناة 3 شموع\n"
        "━━━━━━━━━━━━━━\n"
        f"النشر: {'شغال ✅' if _three_candle_is_enabled() else 'متوقف ⛔'}\n"
        f"القناة: {channel_id or 'غير مضبوطة'}\n"
        f"حد الصفقات اليومي: {'مفتوح ♾' if limit <= 0 else limit}\n"
        f"منشور اليوم: {today_count}\n"
        f"آخر فحص: {_three_candle_channel_state.get('last_scan_at') or 'لا يوجد'}\n"
        f"صفقة قيد المتابعة: {'نعم' if isinstance(pending, dict) else 'لا'}\n"
        f"آخر خطأ: {_three_candle_channel_state.get('last_error') or 'لا يوجد'}"
    )[:3900]


def _three_candle_candidate_for_pair(pair: str, symbol: str) -> dict | None:
    try:
        on_cd, _remain = _three_candle_pair_on_cooldown(pair)
        if on_cd:
            return None

        instrument = quotex_otc_feed.instrument(symbol) if "quotex_otc_feed" in globals() else {}
        payout = int(float((instrument or {}).get("payout", 0) or 0))
        is_otc = bool((instrument or {}).get("is_otc", True))
        if instrument and (not is_otc or payout < int(THREE_CANDLE_MIN_PAYOUT)):
            return None
        if payout and payout < int(THREE_CANDLE_MIN_PAYOUT):
            return None

        current_bucket, remaining = _three_candle_bucket_now()
        if remaining > float(THREE_CANDLE_ALERT_REMAINING_SECONDS):
            return None
        if remaining < float(THREE_CANDLE_MIN_REMAINING_SECONDS):
            return None

        candles = _three_candle_get_sorted_candles(symbol)
        if len(candles) < 3:
            return None
        by_bucket = {int(float(c.get("bucket_ts", 0) or 0)): c for c in candles}
        current = by_bucket.get(current_bucket)
        prev1 = by_bucket.get(current_bucket - 60)
        prev2 = by_bucket.get(current_bucket - 120)
        if not current or not prev1 or not prev2:
            return None

        p1 = _otc_edge_candle_parts(prev2)
        p2 = _otc_edge_candle_parts(prev1)
        pc = _otc_edge_candle_parts(current)
        if p1["dir"] == 0 or p2["dir"] == 0 or pc["dir"] == 0:
            return None
        if not (p1["dir"] == p2["dir"] == pc["dir"]):
            return None

        direction = "CALL" if pc["dir"] > 0 else "PUT"
        last_signals = _three_candle_channel_state.setdefault("last_signal_buckets", {})
        if int(last_signals.get(symbol, 0) or 0) == int(current_bucket):
            return None

        # نعطي أولوية للشمعات الواضحة والباي أوت الأعلى.
        body_score = round((p1.get("body_ratio", 0) + p2.get("body_ratio", 0) + pc.get("body_ratio", 0)) / 3, 4)
        return {
            "pair": pair,
            "symbol": symbol,
            "direction": direction,
            "payout": payout,
            "current_bucket": int(current_bucket),
            "entry_bucket": int(current_bucket + 60),
            "entry_close_bucket": int(current_bucket + 120),
            "remaining": round(float(remaining), 1),
            "body_score": body_score,
            "third_color": "خضراء" if direction == "CALL" else "حمراء",
        }
    except Exception as e:
        logger.debug("three candle candidate failed for %s/%s: %s", pair, symbol, e)
        return None


def _three_candle_collect_candidates() -> list[dict]:
    results = []
    try:
        pair_map = get_otc_analysis_pair_map()
        for pair, symbol in pair_map.items():
            candidate = _three_candle_candidate_for_pair(pair, symbol)
            if candidate:
                results.append(candidate)
        results.sort(key=lambda x: (int(x.get("payout", 0) or 0), float(x.get("body_score", 0) or 0)), reverse=True)
    except Exception as e:
        _three_candle_channel_state["last_error"] = str(e)
        logger.exception("Three-candle channel candidate scan failed: %s", e)
    return results


def _three_candle_signal_message(trade: dict) -> str:
    try:
        entry_dt = datetime.fromtimestamp(int(trade.get("entry_bucket")), tz=UTC).astimezone(UTC_PLUS_3)
        close_dt = datetime.fromtimestamp(int(trade.get("entry_close_bucket")), tz=UTC).astimezone(UTC_PLUS_3)
        direction_icon = _three_candle_direction_icon(trade.get('direction'))
        pair = trade.get('pair')
        payout = trade.get('payout', 0)
        warning_color = trade.get('third_color')
        return (
            "⚡ OTC SIGNAL\n"
            "━━━━━━━━━━━━━━\n"
            f"💱 {pair}\n"
            f"📌 {direction_icon}\n"
            f"⏳ الدخول: {entry_dt.strftime('%H:%M:%S')} UTC+3\n"
            f"🏁 الانتهاء: {close_dt.strftime('%H:%M:%S')} UTC+3\n"
            f"💰 payout: {payout}%\n"
            f"⚠️ ادخل فقط إذا أغلقت الشمعة الحالية {warning_color}."
        )[:3900]
    except Exception as e:
        return f"⚡ OTC SIGNAL\nتعذر تنسيق الرسالة: {e}"


def _three_candle_result_message(trade: dict, result: str, *, martingale: bool = False, candle: dict | None = None) -> str:
    try:
        if result == "win":
            # Win✅. تعني أن الربح جاء بعد المضاعفة MG1.
            return "Win✅." if martingale else "Win✅"
        if result == "loss":
            return "Lose💔"
        if result == "draw":
            return "🟰doji"
        return "🟰doji"
    except Exception:
        return "🟰doji"


def _three_candle_get_candle(symbol: str, bucket_ts: int) -> dict:
    try:
        with quotex_otc_feed.lock:
            candle = (quotex_otc_feed.candles.get(symbol) or {}).get(int(bucket_ts))
        return dict(candle) if candle else {}
    except Exception:
        return {}


async def _three_candle_process_pending_trade(context: ContextTypes.DEFAULT_TYPE) -> bool:
    """يعالج الصفقة الحالية إن وجدت. يرجع True إذا توجد صفقة قيد المتابعة."""
    trade = _three_candle_channel_state.get("pending_trade")
    if not isinstance(trade, dict):
        return False

    try:
        now_ts = time_module.time()
        channel_id = _three_candle_channel_id()
        if not channel_id:
            return True

        symbol = trade.get("symbol")
        direction = str(trade.get("direction") or "").upper()

        # أولًا: نتأكد أن الشمعة الثالثة أغلقت بنفس اللون المتوقع.
        # إذا عكست آخر الثواني، نلغي الصفقة قبل حساب الدخول.
        if not bool(trade.get("third_candle_confirmed", False)):
            current_bucket = int(trade.get("current_bucket", 0) or 0)
            if now_ts < current_bucket + 60 + int(THREE_CANDLE_RESULT_DELAY_SECONDS):
                return True
            confirmation_candle = _three_candle_get_candle(symbol, current_bucket)
            if not confirmation_candle:
                return True
            parts = _otc_edge_candle_parts(confirmation_candle)
            expected_dir = 1 if direction == "CALL" else -1
            if int(parts.get("dir", 0) or 0) != expected_dir:
                await safe_send_message(context.bot, chat_id=channel_id, text="تم الغاء الصفقة")
                _three_candle_channel_state["cancelled_sent"] = int(_three_candle_channel_state.get("cancelled_sent", 0) or 0) + 1
                _three_candle_record_result(trade, "cancelled", "تم الغاء الصفقة")
                _three_candle_channel_state["pending_trade"] = None
                return False
            trade["third_candle_confirmed"] = True
            _three_candle_channel_state["pending_trade"] = trade

        step = int(trade.get("step", 0) or 0)
        bucket = int(trade.get("entry_bucket") if step == 0 else trade.get("martingale_bucket"))
        # لا نحكم قبل إغلاق الشمعة مع تأخير صغير حتى يكتمل الكاش.
        if now_ts < bucket + 60 + int(THREE_CANDLE_RESULT_DELAY_SECONDS):
            return True

        candle = _three_candle_get_candle(symbol, bucket)
        if not candle:
            # إذا لم تصل الشمعة بعد ننتظر، ولا ننشر نتيجة خاطئة.
            return True

        result = _three_candle_result_for_direction(direction, candle.get("open"), candle.get("close"))
        if step == 0 and result == "loss":
            # مضاعفة: لا ننشر الخسارة المباشرة، ننتظر الشمعة التالية بنفس الاتجاه.
            trade["step"] = 1
            trade["first_result"] = "loss"
            trade["martingale_bucket"] = int(bucket + 60)
            _three_candle_channel_state["pending_trade"] = trade
            return True

        final_result = result
        martingale = (step == 1)
        result_text = _three_candle_result_message(trade, final_result, martingale=martingale, candle=candle)
        await safe_send_message(context.bot, chat_id=channel_id, text=result_text)
        _three_candle_channel_state["results_sent"] = int(_three_candle_channel_state.get("results_sent", 0) or 0) + 1
        _three_candle_register_final_result(str(trade.get("pair")), final_result)

        if final_result == "win" and martingale:
            result_type = "mg_win"
        elif final_result == "win":
            result_type = "direct_win"
        elif final_result == "loss":
            result_type = "loss"
        else:
            result_type = "draw"
        _three_candle_record_result(trade, result_type, result_text)

        _three_candle_channel_state["pending_trade"] = None
        return False
    except Exception as e:
        _three_candle_channel_state["last_error"] = str(e)
        logger.exception("Three-candle pending trade processing failed: %s", e)
        return True


async def three_candle_channel_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        if not _three_candle_is_enabled():
            return
        _three_candle_channel_state["last_scan_at"] = now_iso()

        # إذا في صفقة قائمة، ننتظر نتيجتها ولا نرسل صفقة جديدة.
        has_pending = await _three_candle_process_pending_trade(context)
        if has_pending:
            return

        if _three_candle_daily_limit_reached():
            return

        candidates = _three_candle_collect_candidates()
        if not candidates:
            return

        item = candidates[0]
        channel_id = _three_candle_channel_id()
        if not channel_id:
            return

        # نحجز الصفقة قبل الإرسال حتى لا تتكرر بنفس الدورة.
        trade = dict(item)
        trade.update({
            "created_at": now_iso(),
            "step": 0,
        })
        _three_candle_channel_state.setdefault("last_signal_buckets", {})[trade.get("symbol")] = int(trade.get("current_bucket"))
        _three_candle_channel_state["pending_trade"] = trade

        sent = await safe_send_message(context.bot, chat_id=channel_id, text=_three_candle_signal_message(trade))
        if sent:
            _three_candle_channel_state["signals_sent"] = int(_three_candle_channel_state.get("signals_sent", 0) or 0) + 1
            _three_candle_increment_today_signal_count()
            await publish_copy_three_candle_signal(trade)
        else:
            _three_candle_channel_state["pending_trade"] = None
    except Exception as e:
        _three_candle_channel_state["last_error"] = str(e)
        logger.exception("Three-candle channel job error: %s", e)



def format_otc_dynamic_universe_status() -> str:
    try:
        live_map = get_otc_analysis_pair_map()
        sample_pairs = list(live_map.keys())[:30]
        sample_text = "\n".join(f"• {p}" for p in sample_pairs)
        if len(live_map) > len(sample_pairs):
            sample_text += f"\n... و {len(live_map) - len(sample_pairs)} زوج إضافي"

        return (
            "╔══════════════╗\n"
            "   🌐 Currencies OTC المتاحة\n"
            "╚══════════════╝\n\n"
            f"الأزواج المسموحة من Currencies: {len(OTC_CURRENCIES_ALLOWED_PAIRS)}\n"
            f"الأزواج المتاحة الآن للتحليل: {len(live_map)}\n"
            f"حد payout: {OTC_LIVE_DYNAMIC_MIN_PAYOUT}%\n\n"
            "المتاح الآن:\n"
            f"{sample_text if sample_text else 'لا يوجد بعد'}"
        )
    except Exception as e:
        logger.exception("Could not build dynamic universe status: %s", e)
        return "تعذر عرض حالة أزواج OTC."



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



# ===== OTC LIVE ADMIN STATS TOOLS =====
def otc_live_stats_control_ref():
    return system_ref().child("otc_live_channel_stats_control")


def get_otc_live_stats_reset_at() -> str:
    try:
        data = otc_live_stats_control_ref().get() or {}
        return str(data.get("reset_at", "") or "")
    except Exception:
        return ""


def reset_otc_live_stats_marker():
    try:
        otc_live_stats_control_ref().update({
            "reset_at": now_iso(),
            "reset_by_admin": True,
        })
    except Exception as e:
        logger.exception("Could not reset OTC live stats marker: %s", e)


def get_otc_live_trades(day_key: str | None = None, after_reset: bool = True, limit: int | None = None) -> list[dict]:
    try:
        day_key = day_key or get_otc_live_day_key()
        reset_at = get_otc_live_stats_reset_at() if after_reset else ""

        raw = otc_live_stats_ref().child(day_key).child("trades").get() or {}
        rows = []
        if isinstance(raw, dict):
            for trade_id, trade in raw.items():
                if isinstance(trade, dict):
                    item = dict(trade)
                    item["_id"] = trade_id
                    created_at = str(item.get("created_at", "") or "")
                    if reset_at and created_at and created_at < reset_at:
                        continue
                    rows.append(item)

        rows.sort(key=lambda x: str(x.get("created_at", "")))
        if limit and int(limit) > 0:
            rows = rows[-int(limit):]
        return rows
    except Exception as e:
        logger.exception("Could not read OTC live trades: %s", e)
        return []


def calculate_otc_live_trade_stats(trades: list[dict]) -> dict:
    total = len(trades)
    wins = 0
    direct_wins = 0
    losses = 0
    unknown = 0
    martingale_wins = 0
    net_units = 0.0
    gross_win_units = 0.0
    gross_loss_units = 0.0
    pair_stats = {}
    mg_same = {"total": 0, "wins": 0, "losses": 0}
    mg_opposite = {"total": 0, "wins": 0, "losses": 0}
    mg_same = {"total": 0, "wins": 0, "losses": 0}
    mg_opposite = {"total": 0, "wins": 0, "losses": 0}

    for trade in trades:
        result = str(trade.get("result", "unknown"))
        step = safe_int(trade.get("martingale_step"), 0)
        payout = safe_int(trade.get("payout"), 80)

        units = trade.get("units")
        if units is None:
            units = otc_live_trade_units(result, step, payout)
        else:
            try:
                units = float(units)
            except Exception:
                units = otc_live_trade_units(result, step, payout)

        pair = str(trade.get("pair", "غير معروف"))
        if is_valid_otc_currency_pair_name(pair):
            pair_info = pair_stats.setdefault(pair, {"total": 0, "wins": 0, "losses": 0, "unknown": 0, "units": 0.0})
            pair_info["total"] += 1
            pair_info["units"] += float(units)
        else:
            pair_info = None

        if step == 1:
            decision_type = str(trade.get("martingale_decision_type") or "").lower()
            if decision_type == "same":
                mg_same["total"] += 1
                if result == "win":
                    mg_same["wins"] += 1
                elif result == "loss":
                    mg_same["losses"] += 1
            elif decision_type in {"opposite", "reverse"}:
                mg_opposite["total"] += 1
                if result == "win":
                    mg_opposite["wins"] += 1
                elif result == "loss":
                    mg_opposite["losses"] += 1

        if step == 1:
            decision_type = str(trade.get("martingale_decision_type") or "").lower()
            if decision_type == "same":
                mg_same["total"] += 1
                if result == "win":
                    mg_same["wins"] += 1
                elif result == "loss":
                    mg_same["losses"] += 1
            elif decision_type in {"opposite", "reverse"}:
                mg_opposite["total"] += 1
                if result == "win":
                    mg_opposite["wins"] += 1
                elif result == "loss":
                    mg_opposite["losses"] += 1

        if result == "win":
            wins += 1
            if pair_info is not None:
                pair_info["wins"] += 1
            if step == 1:
                martingale_wins += 1
            else:
                direct_wins += 1
        elif result == "loss":
            losses += 1
            if pair_info is not None:
                pair_info["losses"] += 1
        else:
            unknown += 1
            if pair_info is not None:
                pair_info["unknown"] += 1

        net_units += float(units)
        if units > 0:
            gross_win_units += float(units)
        elif units < 0:
            gross_loss_units += float(units)

    decided = wins + losses
    win_rate = round((wins / decided) * 100, 1) if decided > 0 else 0
    loss_rate = round((losses / decided) * 100, 1) if decided > 0 else 0
    avg_units = round(net_units / decided, 3) if decided > 0 else 0

    return {
        "total": total,
        "wins": wins,
        "direct_wins": direct_wins,
        "martingale_wins": martingale_wins,
        "losses": losses,
        "unknown": unknown,
        "decided": decided,
        "win_rate": win_rate,
        "loss_rate": loss_rate,
        "net_units": round(net_units, 2),
        "gross_win_units": round(gross_win_units, 2),
        "gross_loss_units": round(gross_loss_units, 2),
        "avg_units": avg_units,
        "pair_stats": pair_stats,
        "mg_same": mg_same,
        "mg_opposite": mg_opposite,
    }


def build_otc_live_bot_advice(stats: dict) -> str:
    advice_lines = ["\n🤖 نصائح البوت بناءً على الإحصائيات:"]

    if stats["total"] < 20:
        advice_lines.append("• العينة قليلة، انتظر 20 صفقة على الأقل قبل الحكم.")
    else:
        if stats["net_units"] > 0:
            advice_lines.append("• الأداء المالي موجب، لا تغيّر الإعدادات بسرعة.")
        else:
            advice_lines.append("• الأداء المالي سلبي، راقب الخسائر النهائية قبل زيادة الصفقات.")

        if stats["loss_rate"] >= 25:
            advice_lines.append("• الخسارة النهائية مرتفعة، الأفضل تفعيل Safety Pause أو رفع الجودة.")
        elif stats["loss_rate"] <= 15:
            advice_lines.append("• الخسارة النهائية منخفضة نسبيًا، الوضع جيد حاليًا.")

        if stats["martingale_wins"] > stats["direct_wins"]:
            advice_lines.append("• الربح يعتمد كثيرًا على المضاعفة، هذا إنذار خطر.")
        else:
            advice_lines.append("• الربح المباشر جيد مقارنة بالمضاعفة.")

    pair_stats = stats.get("pair_stats") or {}
    if pair_stats:
        sorted_pairs = sorted(pair_stats.items(), key=lambda kv: kv[1]["units"])
        worst_pair, worst_data = sorted_pairs[0]
        if worst_data["units"] < 0:
            advice_lines.append(f"• أضعف زوج حاليًا: {worst_pair}، راقبه أو أوقفه مؤقتًا إذا تكررت خسائره.")

    return "\n".join(advice_lines) + "\n"



def get_otc_live_current_direction_mode_label() -> str:
    try:
        return "REVERSE 🔁" if OTC_LIVE_REVERSE_AUTOPUBLISH else "NORMAL ➡️"
    except Exception:
        return "غير معروف"


def build_otc_live_stats_from_trades(trades: list[dict], title: str, day_key: str | None = None, include_advice: bool = False) -> str:
    stats = calculate_otc_live_trade_stats(trades)

    if stats["net_units"] > 0:
        money_status = "🟢 رابح"
    elif stats["net_units"] < 0:
        money_status = "🔴 خاسر"
    else:
        money_status = "⚪ تعادل"

    reset_at = get_otc_live_stats_reset_at()
    reset_line = f"🧹 من بعد التصفير: {reset_at}\n" if reset_at else ""

    best_pair_line = ""
    worst_pair_line = ""
    if stats["pair_stats"]:
        sorted_pairs = sorted(stats["pair_stats"].items(), key=lambda kv: kv[1]["units"], reverse=True)
        best_pair, best_data = sorted_pairs[0]
        worst_pair, worst_data = sorted_pairs[-1]
        best_pair_line = f"🏆 أفضل زوج: {best_pair} | {round(best_data['units'], 2)} وحدة\n"
        worst_pair_line = f"⚠️ أضعف زوج: {worst_pair} | {round(worst_data['units'], 2)} وحدة\n"

    mg_same = stats.get("mg_same", {"total": 0, "wins": 0, "losses": 0})
    mg_opposite = stats.get("mg_opposite", {"total": 0, "wins": 0, "losses": 0})

    def _rate(w, total):
        return round((int(w) / max(1, int(total))) * 100, 1) if int(total) > 0 else 0

    smart_mg_line = (
        "🧠 إحصائيات المضاعفة الذكية:\n"
        f"↔️ بنفس اتجاه الصفقة: {mg_same.get('wins', 0)}W / {mg_same.get('losses', 0)}L | {_rate(mg_same.get('wins', 0), mg_same.get('total', 0))}%\n"
        f"🔁 بعكس اتجاه الصفقة: {mg_opposite.get('wins', 0)}W / {mg_opposite.get('losses', 0)}L | {_rate(mg_opposite.get('wins', 0), mg_opposite.get('total', 0))}%\n\n"
    )

    advice = build_otc_live_bot_advice(stats) if include_advice else ""

    return (
        "╔══════════════╗\n"
        f"   {title}\n"
        "╚══════════════╝\n\n"
        f"📅 التاريخ: {day_key or get_otc_live_day_key()}\n"
        f"{reset_line}"
        f"📌 عدد الصفقات: {stats['total']}\n"
        f"✅ ربح مباشر: {stats['direct_wins']}\n"
        f"✅¹ ربح بالمضاعفة: {stats['martingale_wins']}\n"
        f"💔 خسارة نهائية: {stats['losses']}\n"
        f"⚖️ دوجي: {stats['unknown']}\n\n"
        f"📈 نسبة نجاح الإشارات: {stats['win_rate']}%\n"
        f"📉 نسبة الخسارة النهائية: {stats['loss_rate']}%\n\n"
        f"💰 صافي الوحدات: {stats['net_units']}\n"
        f"➕ وحدات رابحة: {stats['gross_win_units']}\n"
        f"➖ وحدات خاسرة: {stats['gross_loss_units']}\n"
        f"📊 متوسط الوحدة/صفقة: {stats['avg_units']}\n"
        f"💵 الأداء المالي: {money_status}\n\n"
        f"{best_pair_line}"
        f"{worst_pair_line}"
        f"{smart_mg_line}"
        f"{advice}"
    )


# ===== OTC LIVE CHANNEL STATS =====
def otc_live_stats_ref():
    return system_ref().child("otc_live_channel_stats")


def get_otc_live_day_key(check_dt: datetime | None = None) -> str:
    return (check_dt or now_utc()).astimezone(UTC_PLUS_3).strftime("%Y-%m-%d")


def record_otc_live_channel_result(signal: dict, result: str):
    # ===== FULL STOP OTC LIVE BEFORE RESULT/STORAGE =====
    if should_skip_otc_live_work("result_or_storage_disabled"):
        return

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
            "martingale_decision_type": signal.get("martingale_decision_type"),
            "martingale_direction": signal.get("martingale_direction"),
            "martingale_base_direction": signal.get("martingale_base_direction"),
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
        f"🏆 الأفضل حاليًا: {best_mode}\n"
        f"🧠 أسلوب البوت الحالي: {get_otc_live_current_direction_mode_label()}\n\n"
        "@coach_WAEL_trading\n"
        "@sttrade_helper_bot"
    )

    return base_message + extra



def get_utc3_day_key(dt=None) -> str:
    try:
        if dt is None:
            dt = now_utc()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        local_dt = dt.astimezone(UTC_PLUS_3)
        return local_dt.strftime("%Y-%m-%d")
    except Exception:
        return datetime.now(UTC_PLUS_3).strftime("%Y-%m-%d")


def _extract_trade_day_key(trade: dict) -> str:
    """يحاول استخراج يوم الصفقة UTC+3 من بيانات الصفقة المخزنة."""
    if not isinstance(trade, dict):
        return ""

    for key in ("day_key", "date", "trade_date", "entry_date"):
        value = trade.get(key)
        if value:
            s = str(value)
            m = re.search(r"\d{4}-\d{2}-\d{2}", s)
            if m:
                return m.group(0)

    for key in ("entry_time", "entry_at", "created_at", "published_at", "result_at", "closed_at", "timestamp"):
        value = trade.get(key)
        if not value:
            continue
        try:
            if isinstance(value, (int, float)):
                dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
            else:
                s = str(value).replace("Z", "+00:00")
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            return get_utc3_day_key(dt)
        except Exception:
            continue

    return ""


def is_trade_from_today_utc3(trade: dict) -> bool:
    return _extract_trade_day_key(trade) == get_utc3_day_key()


def get_today_otc_live_trade_items() -> list:
    """يرجع صفقات OTC Live الخاصة باليوم فقط، حتى لا تُحسب الإحصائيات القديمة بنهاية اليوم."""
    try:
        raw = otc_live_stats_ref().child("trades").get() or {}
        if isinstance(raw, dict):
            items = list(raw.values())
        elif isinstance(raw, list):
            items = [x for x in raw if x]
        else:
            items = []

        return [x for x in items if isinstance(x, dict) and is_trade_from_today_utc3(x)]
    except Exception as e:
        logger.exception("Could not load today OTC live trade items: %s", e)
        return []


def build_today_otc_live_daily_summary_message() -> str | None:
    """ملخص نهاية اليوم من صفقات اليوم فقط.
    إذا لا توجد صفقات اليوم، لا يرجع رسالة حتى لا ينشر أرقام قديمة.
    """
    trades = get_today_otc_live_trade_items()

    if not trades:
        return None

    wins = 0
    losses = 0
    doji = 0

    for trade in trades:
        result = str(trade.get("result") or trade.get("final_result") or trade.get("status") or "").lower()
        if "win" in result or "✅" in result:
            wins += 1
        elif "loss" in result or "lose" in result or "❌" in result:
            losses += 1
        elif "doji" in result or "draw" in result or "⚖️" in result:
            doji += 1

    total = wins + losses + doji
    if total <= 0:
        return None

    lines = [
        "╔════════════════╗",
        " today totally win ✅",
        "╚════════════════╝",
        "",
        "━━━━━━━━━━━━",
        f"{wins} win",
        "━━━━━━━━━━━━",
        f"{losses} loss",
        "━━━━━━━━━━━━",
    ]

    if doji:
        lines.append(f"{doji} doji")
        lines.append("━━━━━━━━━━━━")

    return "\n".join(lines)


async def publish_daily_otc_live_stats(context: ContextTypes.DEFAULT_TYPE):
    # الملخص اليومي لقناة OTC Live التلقائية ملغى مع إلغاء القناة.
    return
    if should_skip_otc_live_work("daily_summary_disabled"):
        logger.info("Daily OTC Live stats skipped: otc_live disabled full stop")
        return

    # ===== FULL STOP OTC LIVE BEFORE RESULT/STORAGE =====
    if should_skip_otc_live_work("result_or_storage_disabled"):
        return

    """نشر ملخص نهاية اليوم لقناة OTC Live من صفقات اليوم فقط."""
    try:
        # لا تنشر ملخص إذا القناة متوقفة من الأدمن.
        if not is_otc_live_publish_allowed_now():
            logger.info("Daily OTC Live stats skipped: otc_live publish disabled")
            return

        message = build_today_otc_live_daily_summary_message()

        # إذا لا توجد صفقات اليوم، لا تنشر شيئًا حتى لا تظهر أرقام قديمة.
        if not message:
            logger.info("Daily OTC Live stats skipped: no trades for today")
            return

        await safe_send_message(context.bot,
            chat_id=OTC_LIVE_CHANNEL_ID,
            text=message
        )

    except Exception as e:
        logger.exception("Daily OTC Live stats publish error: %s", e)



async def publish_otc_list(context: ContextTypes.DEFAULT_TYPE):
    # النشر التلقائي للـ OTC الزمني ملغى بالكامل بناءً على طلب الأدمن.
    # لا يؤثر هذا على توليد الإشارات اليدوي داخل البوت.
    return
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

        await safe_send_message(context.bot,
            chat_id=CHANNEL_ID,
            text=message_text,
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.exception("Publish OTC Error: %s", e)


async def schedule_random_daily_otc_list(context: ContextTypes.DEFAULT_TYPE):
    """النشر التلقائي ملغى بالكامل. الدالة باقية فقط لمنع كسر أي مراجع قديمة."""
    return
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


def build_signals_message(pair: str, count: int, interval_minutes: int, signals: list[str], lang: str = "ar") -> str:
    
    if str(lang).lower() == "en":
        header = (
            "╔══════════════╗\n"
            "   📊 Quotex Signals - OTC\n"
            "╚══════════════╝\n\n"
            "⏰ Platform Time\n"
            "UTC / GMT +3.00\n\n"
            "⚠️ Important Notes:\n"
            "• Each trade duration: 1M\n"
            "• Avoid trading against strong momentum\n"
            "• Avoid entering after a doji candle\n"
            "• Avoid trading against a strong trend\n"
            "• Use only one martingale after a loss\n\n"
            "📍 Signals:\n\n"
        )
    else:
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
        await safe_send_message(context.bot,
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
    # النشر التلقائي للسوق العالمي ملغى بالكامل بناءً على طلب الأدمن.
    # تبقى الدالة فقط حتى لا تنكسر أي مراجع قديمة.
    return
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
            InlineKeyboardButton("❌ رفض", callback_data=f"reject:{user_id}"),
            InlineKeyboardButton("💬 إرسال رسالة", callback_data=f"message_user:{user_id}"),
        ],
    ])


def build_user_admin_inline_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⛔ إلغاء التفعيل", callback_data=f"reject:{user_id}"),
            InlineKeyboardButton("💬 إرسال رسالة", callback_data=f"message_user:{user_id}"),
        ]
    ])


def get_user_language(user_id: int, context: ContextTypes.DEFAULT_TYPE | None = None, default: str = "ar") -> str:
    try:
        if context is not None:
            cached = context.user_data.get("language")
            if cached in {"ar", "en"}:
                return cached
        data = get_user_record(int(user_id)) or {}
        lang = str(data.get("language") or "").lower()
        if lang in {"ar", "en"}:
            if context is not None:
                context.user_data["language"] = lang
            return lang
    except Exception:
        pass
    return default


def has_selected_language(user_id: int, context: ContextTypes.DEFAULT_TYPE | None = None) -> bool:
    try:
        if context is not None and context.user_data.get("language") in {"ar", "en"}:
            return True
        data = get_user_record(int(user_id)) or {}
        return str(data.get("language") or "").lower() in {"ar", "en"}
    except Exception:
        return False


def set_user_language(user_id: int, lang: str, context: ContextTypes.DEFAULT_TYPE | None = None):
    lang = "en" if str(lang).lower().startswith("en") else "ar"
    if context is not None:
        context.user_data["language"] = lang
    try:
        save_user_record(int(user_id), {"language": lang, "updated_at": now_iso()})
    except Exception:
        pass
    return lang


def is_english_user(user_id: int, context: ContextTypes.DEFAULT_TYPE | None = None) -> bool:
    return get_user_language(user_id, context) == "en"


def build_user_video_keyboard() -> ReplyKeyboardMarkup:
    return main_keyboard


async def ask_language(update: Update):
    await update.message.reply_text(
        "🌐 اختر اللغة / Choose your language",
        reply_markup=language_keyboard
    )


async def send_welcome_flow(update: Update, lang: str | None = None):
    if lang is None:
        lang = get_user_language(update.effective_user.id)
    if lang == "en":
        await update.message.reply_text(
            "👋 Welcome to TRADING TIME Bot\n\n"
            "Are you a member of TRADING TIME?",
            reply_markup=welcome_keyboard_en
        )
        return

    await update.message.reply_text(
        "👋 أهلًا بك في بوت TRADING TIME\n\n"
        "هل أنت منضم لفريق TRADING TIME؟",
        reply_markup=welcome_keyboard
    )


def build_main_menu_for_user(user_id: int, lang: str | None = None):
    if lang is None:
        lang = get_user_language(user_id)

    if is_otc_list_manager(user_id) and not is_admin(user_id):
        return otc_list_manager_keyboard if lang != "en" else main_keyboard_en
    if is_admin(user_id):
        if lang == "en":
            return ReplyKeyboardMarkup(
                [
                    ["📊 Generate Signals"],
                    ["🧠 Trading Session Room"],
                    ["👤 My Account", "📞 Contact Support"],
                    ["🌐 Change Language", "🛠 Admin Panel"],
                ],
                resize_keyboard=True
            )
        return ReplyKeyboardMarkup(
            [
                ["📊 توليد إشارات"],
                ["🧠 غرفة جلسة تداول"],
                ["👤 حالة حسابي", "📞 تواصل مع المسؤول"],
                ["🌐 تغيير اللغة", "🛠 لوحة الأدمن"],
            ],
            resize_keyboard=True
        )
    return main_keyboard_en if lang == "en" else main_keyboard


# ===== Signal usage / account status helpers =====
def signal_usage_ref(user_id: int):
    return system_ref().child("signal_usage").child(str(int(user_id)))


def get_signal_usage_data(user_id: int) -> dict:
    try:
        data = signal_usage_ref(user_id).get() or {}
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.exception("Could not read signal usage for %s: %s", user_id, e)
        return {}


def get_signal_day_key(dt: datetime | None = None) -> str:
    return get_utc3_day_key(dt or now_utc())


def get_signal_plan_info(user_id: int) -> dict:
    uid = int(user_id)

    if is_admin(uid):
        return {"key": "admin", "label": "أدمن", "limit_type": "unlimited", "limit": None}

    approved_data = get_approved_user(uid) or {}
    user_data = get_user_record(uid) or {}

    mode = str(approved_data.get("mode") or approved_data.get("plan") or user_data.get("plan") or "").lower()
    expires_at = approved_data.get("expires_at") or user_data.get("expires_at")

    source = str(approved_data.get("source") or user_data.get("trial_source") or "").lower()
    if mode == "video_trial" or source == "youtube_video_trial":
        return {
            "key": "trial",
            "label": "تجربة مجانية",
            "limit_type": "total",
            "limit": FREE_TRIAL_SIGNAL_TOTAL_LIMIT,
        }

    if expires_at == "forever" or mode == "forever":
        return {"key": "forever", "label": "VIP دائم", "limit_type": "unlimited", "limit": None}

    if mode == "week":
        return {"key": "week", "label": "اشتراك أسبوع", "limit_type": "daily", "limit": WEEKLY_SIGNAL_DAILY_LIMIT}

    if mode == "month":
        return {"key": "month", "label": "اشتراك شهر", "limit_type": "daily", "limit": MONTHLY_SIGNAL_DAILY_LIMIT}

    # دعم المستخدمين القدامى الذين لا يملكون plan مخزن.
    if expires_at and expires_at != "forever":
        exp = parse_iso(str(expires_at).replace("Z", "+00:00"))
        if exp:
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            remaining_days = (exp - now_utc()).total_seconds() / 86400
            if remaining_days <= 10:
                return {"key": "week", "label": "اشتراك أسبوع", "limit_type": "daily", "limit": WEEKLY_SIGNAL_DAILY_LIMIT}
            return {"key": "month", "label": "اشتراك شهر", "limit_type": "daily", "limit": MONTHLY_SIGNAL_DAILY_LIMIT}

    return {"key": "week", "label": "اشتراك أسبوع", "limit_type": "daily", "limit": WEEKLY_SIGNAL_DAILY_LIMIT}


def get_signal_usage_summary(user_id: int) -> dict:
    data = get_signal_usage_data(user_id)
    day_key = get_signal_day_key()
    daily = data.get("daily") or {}
    today = daily.get(day_key) or {}
    return {
        "day_key": day_key,
        "total": safe_int(data.get("total"), 0),
        "today": safe_int(today.get("count"), 0),
        "last_at": float(data.get("last_at") or 0),
    }


def check_signal_usage_allowed(user_id: int, amount: int = 1) -> tuple[bool, str]:
    # Usage limits disabled by request: all approved users can generate signals without limits.
    uid = int(user_id)
    amount = max(1, int(amount or 1))
    return True, ""

    if is_admin(uid):
        return True, ""

    plan = get_signal_plan_info(uid)
    usage = get_signal_usage_summary(uid)

    last_at = float(usage.get("last_at") or 0)
    now_ts = time_module.time()
    if SIGNAL_USAGE_COOLDOWN_SECONDS > 0 and last_at and now_ts - last_at < SIGNAL_USAGE_COOLDOWN_SECONDS:
        wait = int(max(1, SIGNAL_USAGE_COOLDOWN_SECONDS - (now_ts - last_at)))
        return False, f"⏳ انتظر {wait} ثانية قبل طلب إشارة جديدة."

    if plan["limit_type"] == "unlimited":
        return True, ""

    limit = int(plan.get("limit") or 0)
    used = int(usage["total"] if plan["limit_type"] == "total" else usage["today"])
    remaining = max(0, limit - used)

    if amount > remaining:
        if remaining <= 0:
            if plan["limit_type"] == "total":
                return False, (
                    "⛔ انتهى حد التجربة المجانية.\n\n"
                    f"استخدمت {used}/{limit} إشارات.\n"
                    "للاستمرار تواصل مع الأدمن لتفعيل اشتراكك."
                )
            return False, (
                "⛔ وصلت للحد اليومي لتوليد الإشارات.\n\n"
                f"الخطة: {plan['label']}\n"
                f"استخدمت اليوم: {used}/{limit}\n"
                "يرجع العداد من جديد مع بداية اليوم بتوقيت UTC+3."
            )

        return False, (
            f"⚠️ المتبقي لك حاليًا {remaining} إشارات فقط.\n"
            f"طلبك الحالي يحتاج {amount} إشارات.\n"
            "اختر عددًا أقل أو انتظر تجدد الحد اليومي إذا كان اشتراكك يوميًا."
        )

    return True, ""


def record_signal_usage(user_id: int, amount: int = 1, source: str = "manual_signal"):
    # Usage counting disabled by request. Keep the function as no-op for compatibility.
    uid = int(user_id)
    amount = max(1, int(amount or 1))
    return

    if is_admin(uid):
        return

    try:
        ref = signal_usage_ref(uid)
        data = ref.get() or {}
        if not isinstance(data, dict):
            data = {}

        day_key = get_signal_day_key()
        total = safe_int(data.get("total"), 0) + amount
        daily = data.get("daily") or {}
        today = daily.get(day_key) or {}
        today_count = safe_int(today.get("count"), 0) + amount

        ref.update({
            "total": total,
            "last_at": time_module.time(),
            "last_source": source,
            "updated_at": now_iso(),
        })
        ref.child("daily").child(day_key).update({
            "count": today_count,
            "updated_at": now_iso(),
        })
    except Exception as e:
        logger.exception("Could not record signal usage for %s: %s", uid, e)


def format_expiry_for_account(expires_at, lang: str = "ar") -> str:
    if not expires_at:
        return "Not specified" if lang == "en" else "غير محدد"
    if expires_at == "forever":
        return "Forever" if lang == "en" else "دائم"
    try:
        return format_dt_ar(str(expires_at))
    except Exception:
        return str(expires_at)


def build_account_status_message(user, lang: str = "ar") -> str:
    uid = int(user.id)
    data = get_user_record(uid) or {}
    approved_data = get_approved_user(uid) or {}
    status = get_user_status(uid)
    plan = get_signal_plan_info(uid) if is_approved(uid) or is_admin(uid) else {"label": "غير مفعل", "limit_type": "none", "limit": 0}
    usage = get_signal_usage_summary(uid)

    if lang == "en":
        plan_labels = {
            "admin": "Admin",
            "trial": "Free Trial",
            "forever": "VIP Forever",
            "week": "Weekly Subscription",
            "month": "Monthly Subscription",
        }
        username = f"@{user.username}" if getattr(user, "username", None) else "None"
        quotex_id = data.get("quotex_id") or approved_data.get("quotex_id") or "Not registered"
        expires_at = approved_data.get("expires_at") or data.get("expires_at")
        trial_used = "Yes" if has_used_video_trial(uid) else "No"
        plan_label = plan_labels.get(str(plan.get("key") or ""), "Not active" if not is_approved(uid) else str(plan.get("label", "Not specified")))

        usage_line = "Usage: Unlimited ♾"

        return (
            "👤 My Account\n\n"
            f"• Name: {html.escape(user.full_name or '')}\n"
            f"• Username: {html.escape(username)}\n"
            f"• Telegram ID: <code>{uid}</code>\n"
            f"• Quotex ID: <code>{html.escape(str(quotex_id))}</code>\n"
            f"• Status: {html.escape(str(status))}\n"
            f"• Plan: {html.escape(str(plan_label))}\n"
            f"• Expiration: {html.escape(format_expiry_for_account(expires_at, lang='en'))}\n"
            f"• Free trial used: {trial_used}\n"
            f"• {html.escape(usage_line)}"
        )

    username = f"@{user.username}" if getattr(user, "username", None) else "لا يوجد"
    quotex_id = data.get("quotex_id") or approved_data.get("quotex_id") or "غير مسجل"
    expires_at = approved_data.get("expires_at") or data.get("expires_at")
    trial_used = "نعم" if has_used_video_trial(uid) else "لا"

    usage_line = "الاستخدام: غير محدود ♾"

    return (
        "👤 حالة حسابي\n\n"
        f"• الاسم: {html.escape(user.full_name or '')}\n"
        f"• اليوزر: {html.escape(username)}\n"
        f"• Telegram ID: <code>{uid}</code>\n"
        f"• Quotex ID: <code>{html.escape(str(quotex_id))}</code>\n"
        f"• الحالة: {html.escape(str(status))}\n"
        f"• نوع الاشتراك: {html.escape(str(plan.get('label', 'غير محدد')))}\n"
        f"• انتهاء الصلاحية: {html.escape(format_expiry_for_account(expires_at))}\n"
        f"• التجربة المجانية مستخدمة: {trial_used}\n"
        f"• {html.escape(usage_line)}"
    )


def build_bot_stats_message() -> str:
    all_users = get_all_users() or {}
    approved_users = get_all_approved_users() or {}
    pending_users = get_all_pending_users() or {}

    active_approved = 0
    expired_or_blocked = 0
    for uid in list(approved_users.keys()):
        try:
            if is_approved(int(uid)):
                active_approved += 1
            else:
                expired_or_blocked += 1
        except Exception:
            pass

    day_key = get_signal_day_key()
    try:
        usage_data = system_ref().child("signal_usage").get() or {}
    except Exception:
        usage_data = {}

    today_signals = 0
    total_signals = 0
    top_user = None
    top_today = -1
    if isinstance(usage_data, dict):
        for uid, item in usage_data.items():
            if not isinstance(item, dict):
                continue
            total_signals += safe_int(item.get("total"), 0)
            today_count = safe_int(((item.get("daily") or {}).get(day_key) or {}).get("count"), 0)
            today_signals += today_count
            if today_count > top_today:
                top_today = today_count
                top_user = str(uid)

    try:
        video_trials = db.reference("video_trials").get() or {}
        trial_used_count = sum(1 for x in (video_trials or {}).values() if isinstance(x, dict) and x.get("used"))
    except Exception:
        trial_used_count = 0

    active_today = 0
    cutoff = now_utc() - timedelta(hours=24)
    for item in all_users.values():
        if not isinstance(item, dict):
            continue
        last_seen = parse_iso(str(item.get("last_seen", "")))
        if last_seen:
            if last_seen.tzinfo is None:
                last_seen = last_seen.replace(tzinfo=timezone.utc)
            if last_seen >= cutoff:
                active_today += 1

    top_user_line = "لا يوجد"
    if top_user and top_today > 0:
        user_data = all_users.get(top_user, {}) if isinstance(all_users, dict) else {}
        top_name = user_data.get("name", "غير معروف") if isinstance(user_data, dict) else "غير معروف"
        top_user_line = f"{top_name} | <code>{top_user}</code> | {top_today} إشارات اليوم"

    return (
        "📊 إحصائيات البوت\n"
        "━━━━━━━━━━━━━━\n"
        f"👥 إجمالي المستخدمين: {len(all_users)}\n"
        f"✅ المفعّلون حاليًا: {active_approved}\n"
        f"📥 الطلبات المعلقة: {len(pending_users)}\n"
        f"⛔ منتهون/محظورون: {expired_or_blocked}\n"
        f"🟢 نشطوا آخر 24 ساعة: {active_today}\n"
        f"🎁 مستخدمو التجربة: {trial_used_count}\n\n"
        f"📌 إشارات اليوم: {today_signals}\n"
        f"📌 إجمالي الإشارات المسجلة: {total_signals}\n"
        f"🏆 أعلى مستخدم اليوم: {top_user_line}\n"
        "━━━━━━━━━━━━━━"
    )


def build_users_export_csv_bytes() -> bytes:
    all_users = get_all_users() or {}
    approved_users = get_all_approved_users() or {}
    pending_users = get_all_pending_users() or {}

    try:
        usage_data = system_ref().child("signal_usage").get() or {}
    except Exception:
        usage_data = {}

    day_key = get_signal_day_key()
    ids = set(str(x) for x in all_users.keys()) | set(str(x) for x in approved_users.keys()) | set(str(x) for x in pending_users.keys())

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "telegram_id", "name", "username", "quotex_id", "status", "plan",
        "expires_at", "approved_at", "pending", "trial_used",
        "signals_today", "signals_total", "last_seen"
    ])

    for uid in sorted(ids, key=lambda x: int(x) if str(x).lstrip('-').isdigit() else 0):
        user_data = all_users.get(uid, {}) if isinstance(all_users, dict) else {}
        approved_data = approved_users.get(uid, {}) if isinstance(approved_users, dict) else {}
        pending_data = pending_users.get(uid, {}) if isinstance(pending_users, dict) else {}
        merged = {}
        for source in (user_data, approved_data, pending_data):
            if isinstance(source, dict):
                merged.update({k: v for k, v in source.items() if v not in {None, ""}})

        usage = usage_data.get(uid, {}) if isinstance(usage_data, dict) else {}
        signals_today = safe_int(((usage.get("daily") or {}).get(day_key) or {}).get("count"), 0) if isinstance(usage, dict) else 0
        signals_total = safe_int(usage.get("total"), 0) if isinstance(usage, dict) else 0

        writer.writerow([
            uid,
            merged.get("name", ""),
            merged.get("username", ""),
            merged.get("quotex_id", ""),
            get_user_status(int(uid)) if str(uid).lstrip('-').isdigit() else merged.get("status", ""),
            merged.get("plan") or merged.get("mode", ""),
            merged.get("expires_at", ""),
            merged.get("approved_at") or merged.get("activated_at", ""),
            "yes" if uid in pending_users else "no",
            "yes" if (str(uid).lstrip('-').isdigit() and has_used_video_trial(int(uid))) else "",
            signals_today,
            signals_total,
            merged.get("last_seen", ""),
        ])

    return output.getvalue().encode("utf-8-sig")


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


async def send_maintenance_message(update: Update, context: ContextTypes.DEFAULT_TYPE | None = None, lang: str | None = None):
    user = update.effective_user
    if lang is None and user is not None:
        lang = get_user_language(user.id, context)
    lang = "en" if str(lang).lower() == "en" else "ar"

    if user is not None and not is_admin(user.id):
        remember_maintenance_waiter(
            user.id,
            lang=lang,
            name=user.full_name or "",
            username=user.username or "",
        )

    if lang == "en":
        await update.message.reply_text(
            "🛠 The bot is currently under maintenance.\n\n"
            "We are making some updates and improvements.\n"
            "Please try again later.\n\n"
            "Thank you for understanding 🤍"
        )
        return

    await update.message.reply_text(
        "🛠 البوت تحت الصيانة حاليًا\n\n"
        "نقوم حاليًا ببعض التحديثات والتحسينات.\n"
        "يرجى المحاولة لاحقًا.\n\n"
        "شكرًا لتفهمك 🤍"
    )



async def handle_trading_room_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Inline-button handler for Trading Session Room. Keeps button presses silent (no quoted user messages)."""
    query = update.callback_query
    if not query:
        return False
    user = query.from_user
    data = query.data or ""
    if not data.startswith("tr_"):
        return False
    await query.answer()

    uid = int(user.id)
    lang = get_user_language(uid)
    allowed = is_admin(uid) or is_approved(uid)
    if not allowed:
        await safe_send_message(context.bot, chat_id=uid, text=("⛔ Your account is not active." if lang == "en" else "⛔ حسابك غير مفعّل حاليًا."))
        return True

    def t(ar: str, en: str) -> str:
        return en if lang == "en" else ar

    if data == "tr_room":
        reset_signal_state(context)
        await safe_send_message(context.bot, chat_id=uid, text=build_trading_room_warning_message(lang), reply_markup=get_trading_room_menu_keyboard(uid))
        return True

    if data == "tr_back":
        reset_signal_state(context)
        context.user_data["trading_room_loss_confirm_stage"] = None
        await safe_send_message(
            context.bot,
            chat_id=uid,
            text=t("تم الرجوع للقائمة.", "Back to menu."),
            reply_markup=build_main_menu_for_user(uid, lang) if lang == "en" else build_main_menu_for_user(uid)
        )
        return True

    if data == "tr_start":
        remaining = get_trading_room_cooldown_remaining(context, uid)
        if remaining > 0:
            await safe_send_message(
                context.bot,
                chat_id=uid,
                text=t(
                    f"🧊 غرفة التداول متوقفة احتياطيًا عندك لمدة {_cooldown_text(remaining)} تقريبًا.\n\nهذا لحماية رأس المال ومنع الدخول تحت تأثير الانفعال.",
                    f"🧊 Trading room is locked for about {_cooldown_text(remaining)}.\n\nThis protects your capital and prevents emotional entries."
                ),
                reply_markup=get_trading_room_menu_keyboard(uid)
            )
            return True
        context.user_data["step"] = "trading_room_waiting_balance"
        await safe_send_message(
            context.bot,
            chat_id=uid,
            text=t("💰 اكتب رصيد الحساب الحالي بالدولار.\n\nمثال: 50 أو 120.5", "💰 Send your current account balance in dollars.\n\nExample: 50 or 120.5"),
            reply_markup=get_trading_room_menu_keyboard(uid),
        )
        return True

    if data == "tr_stop":
        context.user_data["trading_room_loss_confirm_stage"] = None
        clear_trading_room_state(context, uid)
        await safe_send_message(context.bot, chat_id=uid, text=t("🛑 تم إيقاف جلسة التداول التجريبية.", "🛑 Trading session stopped."), reply_markup=get_trading_room_menu_keyboard(uid))
        return True

    if data == "tr_ready_yes":
        state = get_trading_room_state(context, uid)
        if not state or not state.get("active") or not state.get("pending_ready"):
            await safe_send_message(context.bot, chat_id=uid, text=t("لا توجد جلسة بانتظار التأكيد الآن.", "There is no session waiting for confirmation right now."), reply_markup=get_trading_room_menu_keyboard(uid))
            return True
        state["pending_ready"] = False
        state["ready_confirmed"] = True
        await safe_send_message(context.bot, chat_id=uid, text=t("بسم الله، جاري البحث عن زوج مناسب...", "Starting now. Searching for a suitable pair..."), reply_markup=get_trading_room_active_keyboard(uid))
        try:
            context.job_queue.run_once(
                trading_room_begin_market_job,
                when=10,
                data={"admin_id": uid},
                name=f"trading_room_begin_{uid}_{int(time_module.time())}",
            )
        except Exception as e:
            logger.exception("Could not schedule trading room begin job: %s", e)
            await trading_room_start_market_flow(context, uid)
        return True

    if data == "tr_ready_cancel":
        clear_trading_room_state(context, uid)
        context.user_data["trading_room_loss_confirm_stage"] = None
        await safe_send_message(context.bot, chat_id=uid, text=t("تم إلغاء الجلسة.", "Session cancelled."), reply_markup=get_trading_room_menu_keyboard(uid))
        return True

    if data == "tr_status":
        await safe_send_message(context.bot, chat_id=uid, text=build_trading_room_state_message(get_trading_room_state(context, uid)), reply_markup=get_trading_room_menu_keyboard(uid))
        return True

    if data == "tr_diag":
        if not is_admin(uid):
            await safe_send_message(context.bot, chat_id=uid, text=t("هذا الفحص متاح للأدمن فقط.", "This check is for admin only."), reply_markup=get_trading_room_menu_keyboard(uid))
            return True
        await safe_send_message(context.bot, chat_id=uid, text=build_trading_room_market_data_status_message(), reply_markup=get_trading_room_menu_keyboard(uid))
        return True

    if data == "tr_end_day":
        context.user_data["trading_room_loss_confirm_stage"] = None
        await safe_send_message(context.bot, chat_id=uid, text=t(_tr_room_text(user.id, "قرار ممتاز. الحفاظ على الربح والهدوء أهم من كثرة الصفقات.", "Excellent decision. Protecting profit and staying calm matters more than taking more trades."), "Excellent decision. Protecting profit and staying calm matters more than taking more trades."), reply_markup=get_trading_room_menu_keyboard(uid))
        return True

    if data == "tr_new_win":
        context.user_data["step"] = "trading_room_waiting_balance"
        await safe_send_message(context.bot, chat_id=uid, text=t("💰 اكتب رصيد الحساب الحالي بالدولار.\n\nمثال: 50 أو 120.5", "💰 Send your current account balance in dollars.\n\nExample: 50 or 120.5"), reply_markup=get_trading_room_menu_keyboard(uid))
        return True

    if data == "tr_new_loss":
        context.user_data["trading_room_loss_confirm_stage"] = 1
        await safe_send_message(
            context.bot,
            chat_id=uid,
            text=t("لا يبدو هذا خيارًا صائبًا الآن. هل أنت متأكد أنك تريد جلسة جديدة مباشرة بعد الخسارة؟", "This does not look like a wise choice right now. Are you sure you want a new session immediately after a loss?"),
            reply_markup=get_trading_room_loss_confirm_keyboard(uid, 1),
        )
        return True

    if data == "tr_remind_30":
        try:
            context.job_queue.run_once(
                trading_room_half_hour_reminder_job,
                when=1800,
                data={"admin_id": uid},
                name=f"trading_room_reminder_{uid}_{int(time_module.time())}",
            )
        except Exception as e:
            logger.exception("Could not schedule trading room reminder: %s", e)
        await safe_send_message(context.bot, chat_id=uid, text=t("تمام، سأذكّرك بعد نصف ساعة. الأفضل الآن تبعد شوي عن الشاشة.", "Done. I will remind you in 30 minutes. It is better to step away from the screen now."), reply_markup=get_trading_room_menu_keyboard(uid))
        return True

    if data in {"tr_cooldown_30", "tr_loss_cooldown"}:
        clear_trading_room_state(context, uid)
        set_trading_room_cooldown(context, uid, 1800)
        context.user_data["trading_room_loss_confirm_stage"] = None
        await safe_send_message(context.bot, chat_id=uid, text=t(_tr_room_text(user.id, "🧊 تم تعطيل غرفة التداول عندك لمدة نصف ساعة.\n\nهذا القرار لحماية رأس المال ومنع التهور بعد الخسارة.", "🧊 Trading room has been locked for 30 minutes.\n\nThis protects your capital and prevents revenge trading after a loss."), "🧊 Trading room has been locked for 30 minutes.\n\nThis protects your capital and prevents revenge trading after a loss."), reply_markup=get_trading_room_menu_keyboard(uid))
        return True

    if data == "tr_loss_retreat":
        context.user_data["trading_room_loss_confirm_stage"] = None
        await safe_send_message(context.bot, chat_id=uid, text=t(_tr_room_text(user.id, "قرار ممتاز. إذا بتحب، فيك توقف غرفة التداول عندك نصف ساعة احتياطيًا حتى ما ترجع بتهور.", "Good decision. You can lock the trading room for 30 minutes if you want extra protection from emotional trading."), "Good decision. You can lock the trading room for 30 minutes if you want extra protection from emotional trading."), reply_markup=get_trading_room_retreat_keyboard(uid))
        return True

    if data == "tr_loss_yes":
        stage = int(context.user_data.get("trading_room_loss_confirm_stage") or 0)
        if stage <= 0:
            await safe_send_message(context.bot, chat_id=uid, text=t("لا يوجد تأكيد خسارة نشط الآن.", "There is no active loss confirmation right now."), reply_markup=get_trading_room_menu_keyboard(uid))
            return True
        if stage == 1:
            context.user_data["trading_room_loss_confirm_stage"] = 2
            await safe_send_message(context.bot, chat_id=uid, text=t("تذكر أن السوق لا يرحم. لا تنجر نحو الغضب أو محاولة الانتقام من السوق.", "Remember: the market does not care. Do not trade out of anger or revenge."), reply_markup=get_trading_room_loss_confirm_keyboard(uid, 2))
            return True
        if stage == 2:
            context.user_data["trading_room_loss_confirm_stage"] = 3
            await safe_send_message(context.bot, chat_id=uid, text=t("قبل ما نكمل: هل قرارك مبني على خطة واضحة أم مجرد غضب من الخسارة؟", "Before we continue: is this decision based on a clear plan or just anger after the loss?"), reply_markup=get_trading_room_loss_confirm_keyboard(uid, 3))
            return True
        if stage == 3:
            context.user_data["trading_room_loss_confirm_stage"] = 4
            await safe_send_message(context.bot, chat_id=uid, text=t("آخر تنبيه جدي: جلسة ثانية بعد الخسارة تزيد خطر التهور. هل تتحمل القرار؟", "Serious warning: a second session after a loss increases the risk of emotional trading. Do you accept the decision?"), reply_markup=get_trading_room_loss_confirm_keyboard(uid, 4))
            return True
        if stage == 4:
            context.user_data["trading_room_loss_confirm_stage"] = 5
            await safe_send_message(context.bot, chat_id=uid, text=t("المرحلة الأخيرة. لو بدأت الآن، التزم بالمبلغ والحدود ولا تكسر الخطة. اختر بوعي.", "Final step. If you start now, respect the amount and limits. Do not break the plan. Choose consciously."), reply_markup=get_trading_room_loss_confirm_keyboard(uid, 5))
            return True
        context.user_data["trading_room_loss_confirm_stage"] = None
        context.user_data["step"] = "trading_room_waiting_balance"
        await safe_send_message(context.bot, chat_id=uid, text=t(_tr_room_text(user.id, "تمام، القرار قرارك. اكتب رصيد الحساب الحالي بالدولار لنبدأ جلسة جديدة.", "Okay, the decision is yours. Send your current account balance in dollars to start a new session."), "Okay, the decision is yours. Send your current account balance in dollars to start a new session."), reply_markup=get_trading_room_menu_keyboard(uid))
        return True

    return True

async def handle_admin_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    data = query.data or ""

    if data.startswith("tr_"):
        handled = await handle_trading_room_callback(update, context)
        if handled:
            return

    if not is_admin(user.id):
        await query.answer("هذا الزر للأدمن فقط", show_alert=True)
        return

    await query.answer()

    try:
        action, target_id_str = data.split(":")
        target_id = int(target_id_str)
    except Exception:
        await query.answer("بيانات غير صالحة", show_alert=True)
        return

    user_data = get_user_record(target_id)
    target_name = user_data.get("name", "المستخدم") if user_data else "المستخدم"

    if action == "message_user":
        context.user_data["admin_message_target_id"] = target_id
        context.user_data["step"] = "admin_direct_message_waiting"
        await query.message.reply_text(
            f"💬 اكتب الآن الرسالة التي تريد إرسالها إلى {target_name}\n"
            f"🆔 Telegram ID: <code>{target_id}</code>",
            parse_mode="HTML",
            reply_markup=admin_main_keyboard
        )
        return

    if action == "approve_week":
        set_user_expiry(target_id, "week")
        await query.edit_message_text(
            f"✅ تم تفعيل {target_name} لمدة أسبوع\n"
            f"🆔 Telegram ID: <code>{target_id}</code>",
            parse_mode="HTML"
        )
        try:
            await safe_send_message(context.bot,
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
            await safe_send_message(context.bot,
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
            await safe_send_message(context.bot,
                chat_id=target_id,
                text="✅ تم تفعيل حسابك بنجاح بشكل دائم\n\nأصبح بإمكانك الآن استخدام بوت TRADING TIME.\nاضغط /start للدخول إلى القائمة الرئيسية."
            )
        except Exception:
            pass
        return

    if action == "reject":
        force_reject_pending_user(target_id)
        block_user(target_id)
        await query.edit_message_text(
            f"❌ تم رفض/حظر {target_name}\n"
            f"🆔 Telegram ID: <code>{target_id}</code>",
            parse_mode="HTML"
        )
        try:
            await safe_send_message(context.bot,
                chat_id=target_id,
                text="❌ تم رفض طلبك أو إيقافه\n\nإذا كنت ترى أن هذا بالخطأ، تواصل مع الأدمن.",
                reply_markup=welcome_keyboard
            )
        except Exception:
            pass
        return


# ===== Main Handlers =====

async def show_otc_list_manager_panel(update: Update):
    user = update.effective_user
    try:
        save_user_record(user.id, {
            "telegram_id": user.id,
            "name": user.full_name,
            "username": user.username or "",
            "last_seen": now_iso(),
            "role": "otc_list_manager",
        })
    except Exception:
        pass

    await update.message.reply_text(
        "🧾 لوحة فحص ليستات OTC 👇",
        reply_markup=otc_list_manager_keyboard
    )


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

    if not has_selected_language(user.id, context):
        await ask_language(update)
        return

    lang = get_user_language(user.id, context)

    if not get_bot_enabled():
        await send_maintenance_message(update, context, lang)
        return

    # مشرف الليستات: إذا كان مفعّلًا يظهر له كيبورد الليستات+الإشارات، وإذا غير مفعّل يرجع لمسار البداية.
    if is_otc_list_manager(user.id):
        if is_approved(user.id):
            await show_otc_list_manager_panel(update)
            return
        await send_welcome_flow(update, lang)
        return

    if is_approved(user.id):
        if lang == "en":
            await update.message.reply_text(
                f"✅ Welcome {user.first_name}\n"
                "Welcome back to TRADING TIME Bot.",
                reply_markup=build_main_menu_for_user(user.id, "en")
            )
        else:
            await update.message.reply_text(
                f"✅ أهلًا {user.first_name}\n"
                "مرحبًا بك من جديد في بوت TRADING TIME.",
                reply_markup=build_main_menu_for_user(user.id)
            )
    else:
        await send_welcome_flow(update, lang)


def get_latest_otc_list_job(user_id: int) -> tuple[str | None, dict]:
    try:
        raw = otc_list_jobs_ref(user_id).get() or {}
        if not isinstance(raw, dict) or not raw:
            return None, {}

        items = []
        for list_id, job in raw.items():
            if isinstance(job, dict):
                created_at = str(job.get("created_at", "") or "")
                items.append((created_at, str(list_id), job))

        if not items:
            return None, {}

        items.sort(key=lambda x: x[0])
        _, list_id, job = items[-1]
        return list_id, job
    except Exception as e:
        logger.exception("Could not read latest OTC list job: %s", e)
        return None, {}


def recover_otc_list_result_now(user_id: int) -> tuple[str | None, dict]:
    """إذا ضاعت jobs بسبب restart، احسب آخر ليستة فورًا من البيانات المتاحة الآن."""
    try:
        list_id, job = get_latest_otc_list_job(user_id)
        if not list_id or not job:
            return None, {}

        raw_text = job.get("raw_text") or ""
        trades = job.get("trades") or parse_otc_list_trades(raw_text)

        if not trades:
            return None, {}

        items = []
        for idx, trade in enumerate(trades):
            item = evaluate_otc_list_trade(trade)
            item["index"] = idx
            item["evaluated_at"] = now_iso()
            item["recovered"] = True
            items.append(item)
            try:
                save_otc_list_trade_result(user_id, list_id, idx, item)
            except Exception:
                pass

        items.sort(key=lambda x: int(x.get("index", 0)))
        result_text, meta = build_otc_list_results_message_from_items(items)
        result_text = prettify_existing_otc_result_text(result_text)

        save_ready_otc_list_result(user_id, raw_text, result_text, meta)

        try:
            get_otc_list_job_ref(user_id, list_id).update({
                "status": "ready",
                "ready_at": now_iso(),
                "recovered_at": now_iso(),
                "meta": meta,
            })
        except Exception:
            pass

        return result_text, meta

    except Exception as e:
        logger.exception("Could not recover OTC list result now: %s", e)
        return None, {}





def video_trial_permanent_ref(user_id: int):
    """سجل دائم للتجربة المجانية لا يُحذف عند إلغاء التفعيل."""
    return db.reference("video_trials").child(str(int(user_id)))


def has_used_video_trial_permanent(user_id: int) -> bool:
    try:
        data = video_trial_permanent_ref(user_id).get() or {}
        return bool(data.get("used"))
    except Exception:
        return False


def mark_video_trial_used_permanent(user_id: int, expires_at=None):
    try:
        payload = {
            "used": True,
            "used_at": now_iso(),
        }
        if expires_at:
            payload["expires_at"] = expires_at.isoformat() if hasattr(expires_at, "isoformat") else str(expires_at)
        video_trial_permanent_ref(user_id).update(payload)
    except Exception:
        pass


def video_trial_ref(user_id: int):
    return users_ref().child(str(user_id)).child("video_trial")


def has_used_video_trial(user_id: int) -> bool:
    uid = int(user_id)

    cached = _cache_get(f"video_trial:{uid}")
    if cached is not None:
        return bool(cached)

    # أولًا افحص السجل الدائم الذي لا يُحذف مع إلغاء التفعيل.
    if has_used_video_trial_permanent(uid):
        return _cache_set(f"video_trial:{uid}", True)

    try:
        data = video_trial_ref(uid).get() or {}
        used = bool(data.get("used"))
        if used:
            # ترحيل تلقائي للسجل القديم إلى السجل الدائم.
            mark_video_trial_used_permanent(uid, data.get("expires_at"))
        return _cache_set(f"video_trial:{uid}", used)
    except Exception:
        return _cache_set(f"video_trial:{uid}", False)



def mark_video_trial_started(user_id: int):
    try:
        video_trial_ref(user_id).update({
            "started_at": now_iso(),
            "eligible_after": int(time_module.time()) + int(VIDEO_TRIAL_DELAY_SECONDS),
        })
    except Exception:
        pass


def get_video_trial_eligible_after(user_id: int) -> int:
    try:
        data = video_trial_ref(user_id).get() or {}
        return int(data.get("eligible_after") or 0)
    except Exception:
        return 0


def activate_video_trial_for_user(user_id: int):
    expire_at = now_utc() + timedelta(seconds=int(VIDEO_TRIAL_DURATION_SECONDS))

    approved_ref().child(str(user_id)).set({
        "status": "approved",
        "mode": "video_trial",
        "expires_at": expire_at.isoformat(),
        "activated_at": now_iso(),
        "source": "youtube_video_trial",
    })

    video_trial_ref(user_id).update({
        "used": True,
        "activated_at": now_iso(),
        "expires_at": expire_at.isoformat(),
    })

    save_user_record(user_id, {
        "status": "approved",
        "trial_source": "youtube_video",
        "trial_used": True,
        "expires_at": expire_at.isoformat(),
        "updated_at": now_iso(),
    })

    try:
        clear_user_cache(user_id)
    except Exception:
        pass

    mark_video_trial_used_permanent(user_id, expire_at)
    clear_user_cache(user_id)

    return expire_at



def check_signal_usage_allowed_lang(user_id: int, amount: int = 1, lang: str = "ar") -> tuple[bool, str]:
    # Usage limits disabled by request.
    return True, ""
    allowed, msg = check_signal_usage_allowed(user_id, amount)
    if allowed or lang != "en":
        return allowed, msg

    uid = int(user_id)
    amount = max(1, int(amount or 1))
    plan = get_signal_plan_info(uid)
    usage = get_signal_usage_summary(uid)

    last_at = float(usage.get("last_at") or 0)
    now_ts = time_module.time()
    if SIGNAL_USAGE_COOLDOWN_SECONDS > 0 and last_at and now_ts - last_at < SIGNAL_USAGE_COOLDOWN_SECONDS:
        wait = int(max(1, SIGNAL_USAGE_COOLDOWN_SECONDS - (now_ts - last_at)))
        return False, f"⏳ Please wait {wait} seconds before requesting a new signal."

    limit = int(plan.get("limit") or 0)
    used = int(usage["total"] if plan["limit_type"] == "total" else usage["today"])
    remaining = max(0, limit - used)
    plan_names = {"trial": "Free Trial", "week": "Weekly Subscription", "month": "Monthly Subscription", "forever": "VIP Forever"}
    plan_label = plan_names.get(str(plan.get("key") or ""), str(plan.get("label") or "Subscription"))

    if remaining <= 0:
        if plan["limit_type"] == "total":
            return False, (
                "⛔ Your free trial signal limit has ended.\n\n"
                f"You used {used}/{limit} signals.\n"
                "To continue, contact the admin to activate your subscription."
            )
        return False, (
            "⛔ You reached your daily signal limit.\n\n"
            f"Plan: {plan_label}\n"
            f"Used today: {used}/{limit}\n"
            "The counter resets at the start of the day, UTC+3."
        )

    return False, (
        f"⚠️ You currently have only {remaining} signals remaining.\n"
        f"Your request needs {amount} signals.\n"
        "Choose a smaller count or wait for the daily limit to reset."
    )


def build_signals_message_en(pair: str, count: int, interval_minutes: int, signals: list[str]) -> str:
    return build_signals_message(pair, count, interval_minutes, signals, lang="en")


def translate_real_signal_message_to_en(msg: str) -> str:
    replacements = [
        ("🌍 السوق العالمي", "🌍 Global Market"),
        ("💱 الزوج:", "💱 Pair:"),
        ("🕒 الجلسة:", "🕒 Session:"),
        ("🧭 الفريم:", "🧭 Timeframe:"),
        ("📊 الثقة:", "📊 Confidence:"),
        ("📌 الإشارة:", "📌 Signal:"),
        ("⏰ وقت الدخول:", "⏰ Entry Time:"),
        ("💵 السعر الحالي:", "💵 Current Price:"),
        ("⏳ مدة الصفقة المقترحة:", "⏳ Suggested Trade Duration:"),
        ("⚠️ تنبيهات إضافية:", "⚠️ Additional Alerts:"),
        ("❌ لا توجد صفقة مباشرة الآن", "❌ No direct trade right now"),
        ("❌ لا توجد فرصة واضحة الآن", "❌ No clear opportunity right now"),
        ("السبب: شروط الدخول غير مكتملة بعد", "Reason: Entry conditions are not complete yet"),
        ("السبب: آخر شمعة مغلقة حيادية وتحتاج تأكيد", "Reason: The last closed candle is neutral and needs confirmation"),
        ("السبب: السوق متذبذب ولا توجد منطقة تأكيد واضحة", "Reason: The market is ranging and there is no clear confirmation area"),
        ("⚠️ لكن توجد فرص قريبة للمراقبة:", "⚠️ But there are nearby scenarios to watch:"),
        ("⚠️ فرصة مشروطة — لا تدخل مباشرة الآن", "⚠️ Conditional opportunity — do not enter directly now"),
        ("السبب:", "Reason:"),
        ("⏰ وقت المراقبة:", "⏰ Watch Time:"),
        ("📋 الخطة المقترحة:", "📋 Suggested Plan:"),
        ("📌 ملاحظات التحليل:", "📌 Analysis Notes:"),
        ("⚠️ السوق متذبذب — الأفضل انتظار رد فعل من المنطقة", "⚠️ The market is ranging — better wait for a reaction from the area"),
        ("السعر قريب من دعم", "price is near support"),
        ("السعر قريب من مقاومة", "price is near resistance"),
        ("ظهر رفض صاعد من دعم", "bullish rejection appeared from support"),
        ("ظهر رفض هابط من مقاومة", "bearish rejection appeared from resistance"),
        ("📈 الترند العام صاعد (EMA 9 فوق EMA 21)", "📈 The general trend is bullish (EMA 9 above EMA 21)"),
        ("📉 الترند العام هابط (EMA 9 تحت EMA 21)", "📉 The general trend is bearish (EMA 9 below EMA 21)"),
        ("🏗 بنية السوق صاعدة (قمم وقيعان أعلى)", "🏗 Market structure is bullish (higher highs and higher lows)"),
        ("🏗 بنية السوق هابطة (قمم وقيعان أدنى)", "🏗 Market structure is bearish (lower highs and lower lows)"),
        ("⚠️ بنية السوق متذبذبة — لا نعتمد على الترند وحده", "⚠️ Market structure is ranging — trend alone is not enough"),
        ("⚠️ آخر شمعة مغلقة قريبة من الدوجي — يلزم تأكيد إضافي", "⚠️ The last closed candle is close to a doji — extra confirmation is needed"),
        ("لا تطارد PUT بدون كسر واضح", "do not chase PUT without a clear break"),
        ("لا تطارد CALL بدون كسر واضح", "do not chase CALL without a clear break"),
        ("غير متاح الآن", "is not available now"),
        ("السوق مغلق", "market is closed"),
        ("فشل جلب بيانات السوق", "failed to fetch market data"),
        ("📋 سيناريوهات المراقبة:", "📋 Watch Scenarios:"),
        ("🔥 تم اختيار هذه الصفقة لأنها الأقوى بين 1M / 5M / 10M", "🔥 This trade was selected because it is the strongest among 1M / 5M / 10M"),
        ("🔥 لا توجد صفقة مباشرة قوية، لذلك تم عرض أفضل فرصة مشروطة بين 1M / 5M / 10M", "🔥 No strong direct trade is available, so the best conditional opportunity among 1M / 5M / 10M is shown"),
        ("📋 ملخص الفريمات المفحوصة:", "📋 Checked Timeframes Summary:"),
        ("جاهزة", "Ready"),
        ("لا", "No"),
        ("CALL", "CALL"),
        ("PUT", "PUT"),
        ("الترند العام صاعد", "The general trend is bullish"),
        ("الترند العام هابط", "The general trend is bearish"),
        ("بنية السوق صاعدة", "Market structure is bullish"),
        ("بنية السوق هابطة", "Market structure is bearish"),
        ("الزخم الأخير لصالح الصعود", "Recent momentum favors upside"),
        ("الزخم الأخير لصالح الهبوط", "Recent momentum favors downside"),
        ("آخر شمعة مغلقة قوية صاعدة", "The last closed candle is strongly bullish"),
        ("آخر شمعة مغلقة قوية هابطة", "The last closed candle is strongly bearish"),
        ("إذا كسر الدعم بإغلاق واضح → خذ PUT", "If support breaks with a clear close → take PUT"),
        ("إذا ظهر رفض صاعد من الدعم → خذ CALL", "If bullish rejection appears from support → take CALL"),
        ("لا تدخل PUT مباشرة قبل تأكيد الكسر", "Do not enter PUT directly before break confirmation"),
        ("إذا كسر المقاومة بإغلاق واضح → خذ CALL", "If resistance breaks with a clear close → take CALL"),
        ("إذا ظهر رفض هابط من المقاومة → خذ PUT", "If bearish rejection appears from resistance → take PUT"),
        ("لا تدخل CALL مباشرة قبل تأكيد الكسر", "Do not enter CALL directly before break confirmation"),
    ]
    out = str(msg or "")
    for ar, en in replacements:
        out = out.replace(ar, en)

    # Extra cleanup for common Arabic words that may appear inside dynamic global-market notes.
    cleanup = [
        ("دعم", "support"),
        ("مقاومة", "resistance"),
        ("كسر واضح", "clear break"),
        ("إغلاق واضح", "clear close"),
        ("رفض صاعد", "bullish rejection"),
        ("رفض هابط", "bearish rejection"),
        ("المنطقة", "the zone"),
        ("تأكيد", "confirmation"),
        ("السعر", "price"),
        ("قريب من", "near"),
        ("فوق", "above"),
        ("تحت", "below"),
    ]
    for ar, en in cleanup:
        out = out.replace(ar, en)
    return out


async def handle_message_en(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.message.text.strip()
    step = context.user_data.get("step")

    if text in {"🌐 Change Language", "Change Language", "🌐 تغيير اللغة", "تغيير اللغة", "Language"}:
        reset_signal_state(context)
        await ask_language(update)
        return

    # Admin owner can still open the admin panel while testing English mode.
    if is_admin(user.id) and text in {"🛠 Admin Panel", "Admin Panel", "🛠 لوحة الأدمن"}:
        reset_signal_state(context)
        await update.message.reply_text(
            "🛠 Admin panel opened.",
            reply_markup=admin_main_keyboard
        )
        return

    # Contact / tutorial / free trial public actions
    if text in {"📞 Contact Support", "Contact Support", "📞 تواصل مع المسؤول", "تواصل مع المسؤول"}:
        await update.message.reply_text(
            f"📞 Contact support here:\n{ADMIN_USERNAME}",
            reply_markup=welcome_keyboard_en if not is_approved(user.id) else build_main_menu_for_user(user.id, "en")
        )
        return

    if text in {"🔙 Back", "⬅️ Back", "Back", "🔙 رجوع", "⬅️ رجوع"}:
        reset_signal_state(context)
        await update.message.reply_text(
            "Back to menu.",
            reply_markup=build_main_menu_for_user(user.id, "en") if is_approved(user.id) else welcome_keyboard_en
        )
        return

    if text in {"🎁 Get Free Trial", "Get Free Trial"}:
        if has_used_video_trial(user.id):
            await update.message.reply_text(
                "ℹ️ You have already used the free trial before.\n\nYou can send an activation request from the menu.",
                reply_markup=welcome_keyboard_en if not is_approved(user.id) else build_main_menu_for_user(user.id, "en")
            )
            return
        mark_video_trial_started(user.id)
        await update.message.reply_text(
            "🎁 To get a full 1-hour free trial:\n\n"
            "1️⃣ Watch the full bot tutorial video here:\n"
            f"{YOUTUBE_TUTORIAL_URL}\n\n"
            "2️⃣ After a short time, this button will appear:\n"
            "✅ I Watched the Video\n\n"
            "Press it to activate your free trial automatically.",
            reply_markup=ReplyKeyboardMarkup([["🔙 Back"]], resize_keyboard=True)
        )
        try:
            context.job_queue.run_once(
                send_video_watched_button_job,
                when=VIDEO_TRIAL_DELAY_SECONDS,
                data={"user_id": user.id},
                name=f"video_trial_button_{user.id}_{int(time_module.time())}",
            )
        except Exception as e:
            logger.exception("Could not schedule video watched button: %s", e)
        return

    if text in {"🎥 Watch Bot Tutorial", "Watch Bot Tutorial"}:
        if has_used_video_trial(user.id):
            await update.message.reply_text(
                "🎥 Bot tutorial video:\n"
                f"{YOUTUBE_TUTORIAL_URL}\n\n"
                "ℹ️ You have already used the free trial before.",
                reply_markup=build_main_menu_for_user(user.id, "en") if is_approved(user.id) else welcome_keyboard_en
            )
            return
        mark_video_trial_started(user.id)
        await update.message.reply_text(
            "🎥 Watch the full bot tutorial video:\n"
            f"{YOUTUBE_TUTORIAL_URL}\n\n"
            "After watching, this button will appear:\n"
            "✅ I Watched the Video\n\n"
            "When you press it, you will get a full 1-hour free trial.",
            reply_markup=ReplyKeyboardMarkup([["🔙 Back"]], resize_keyboard=True)
        )
        try:
            context.job_queue.run_once(
                send_video_watched_button_job,
                when=VIDEO_TRIAL_DELAY_SECONDS,
                data={"user_id": user.id},
                name=f"video_trial_button_{user.id}_{int(time_module.time())}",
            )
        except Exception as e:
            logger.exception("Could not schedule video watched button: %s", e)
        return

    if text in {"✅ I Watched the Video", "I Watched the Video"}:
        if has_used_video_trial(user.id):
            await update.message.reply_text(
                "ℹ️ You have already used the free trial before.",
                reply_markup=build_main_menu_for_user(user.id, "en") if is_approved(user.id) else welcome_keyboard_en
            )
            return
        eligible_after = get_video_trial_eligible_after(user.id)
        now_ts = int(time_module.time())
        if eligible_after and now_ts < eligible_after:
            remaining = eligible_after - now_ts
            await update.message.reply_text(
                f"⏳ Please wait a little. You can activate the trial after about {remaining} seconds.",
                reply_markup=video_watched_keyboard_en
            )
            return
        expire_at = activate_video_trial_for_user(user.id)
        reset_signal_state(context)
        await update.message.reply_text(
            "✅ Your free trial has been activated for one full hour.\n\n"
            f"⏳ Trial ends at: {expire_at.strftime('%H:%M')} UTC\n\n"
            "You can now use the bot.",
            reply_markup=build_main_menu_for_user(user.id, "en")
        )
        return

    # Non-approved English start flow
    if not is_admin(user.id) and not is_approved(user.id):
        current_status = get_user_status(user.id)
        if current_status == "pending":
            await update.message.reply_text(
                "⏳ Your activation request is already under review.\n\nYou cannot send a new request before the current one is accepted or rejected.",
                reply_markup=welcome_keyboard_en
            )
            return
        if text in {"✅ Yes, I Joined", "Yes, I Joined"}:
            context.user_data["step"] = "waiting_quotex_id_en"
            await update.message.reply_text(
                "📩 Send your QUOTEX account ID so your account can be checked.\nAfter verification, the bot will be enabled for you for free.",
                reply_markup=ReplyKeyboardMarkup([["🔙 Back"]], resize_keyboard=True)
            )
            return
        if text in {"❌ No, I Haven't Joined", "No, I Haven't Joined"}:
            await update.message.reply_text(
                "📌 How to register and activate TRADING TIME Bot 👇\n\n"
                "1️⃣ Create a new Quotex account using the official team link.\n"
                "2️⃣ Send your account ID to the admin.\n"
                "3️⃣ After review and verification, your bot access will be activated.\n\n"
                f"📞 Admin: {ADMIN_USERNAME}\n"
                f"🎥 Tutorial: {YOUTUBE_TUTORIAL_URL}",
                reply_markup=welcome_keyboard_en
            )
            return
        if step == "waiting_quotex_id_en":
            quotex_id = text.strip()
            if not quotex_id or quotex_id in {"✅ Yes, I Joined", "❌ No, I Haven't Joined"}:
                await update.message.reply_text(
                    "📩 Send only your Quotex account ID.",
                    reply_markup=ReplyKeyboardMarkup([["🔙 Back"]], resize_keyboard=True)
                )
                return
            pending_data = {
                "telegram_id": user.id,
                "name": user.full_name,
                "username": user.username or "",
                "quotex_id": quotex_id,
                "status": "pending",
                "created_at": now_iso(),
                "language": "en",
            }
            save_pending_user(user.id, pending_data)
            save_user_record(user.id, {
                "quotex_id": quotex_id,
                "status": "pending",
                "name": user.full_name,
                "username": user.username or "",
                "language": "en",
                "updated_at": now_iso(),
            })
            await update.message.reply_text(
                "📩 Your request has been received successfully.\n\n"
                "Your Quotex ID was saved and sent to the admin for review.\n"
                "Please wait for admin approval ✅",
                reply_markup=welcome_keyboard_en
            )
            username_text = f"@{user.username}" if user.username else "no username"
            admin_message = (
                "📥 New activation request\n\n"
                f"👤 Name: {user.full_name}\n"
                f"🔗 Username: {username_text}\n"
                f"🆔 Telegram ID: <code>{user.id}</code>\n"
                f"💱 Quotex ID: <code>{quotex_id}</code>\n"
                "🌐 Language: English\n\n"
                "──────────────"
            )
            try:
                await safe_send_message(context.bot,
                    chat_id=ADMIN_TELEGRAM_ID,
                    text=admin_message,
                    parse_mode="HTML",
                    reply_markup=build_pending_request_keyboard(user.id)
                )
            except Exception as e:
                logger.exception("Could not notify admin about pending user: %s", e)
            context.user_data["step"] = None
            return

        await send_welcome_flow(update, "en")
        return

    # Approved English user flow
    if text in {"📊 Generate Signals", "Generate Signals", "📊 توليد إشارات", "توليد إشارات"}:
        allowed, limit_msg = check_signal_usage_allowed_lang(user.id, 1, "en")
        if not allowed:
            reset_signal_state(context)
            await update.message.reply_text(limit_msg, reply_markup=build_main_menu_for_user(user.id, "en"))
            return
        reset_signal_state(context)
        context.user_data["step"] = "choose_market_mode_en"
        await update.message.reply_text("📊 Choose market type 👇", reply_markup=market_mode_keyboard_en)
        return

    if text in {"👤 My Account", "My Account", "👤 حسابي", "👤 حالة حسابي"}:
        await update.message.reply_text(
            build_account_status_message(user, lang="en"),
            parse_mode="HTML",
            reply_markup=build_main_menu_for_user(user.id, "en")
        )
        return

    if step == "choose_market_mode_en":
        if text == "⚡ OTC":
            context.user_data["mode"] = "otc"
            context.user_data["step"] = "choose_otc_mode_en"
            await update.message.reply_text("⚡ Choose OTC signal type 👇", reply_markup=otc_mode_keyboard_en)
            return
        if text in {"🌍 Global Market", "Global Market"}:
            context.user_data["mode"] = "real"
            context.user_data["step"] = "choose_real_pair_en"
            await update.message.reply_text("🌍 Choose a global market pair 👇", reply_markup=real_pairs_keyboard_en)
            return
        await update.message.reply_text("📌 Choose a market type from the buttons 👇", reply_markup=market_mode_keyboard_en)
        return

    if step == "choose_otc_mode_en" and context.user_data.get("mode") == "otc":
        if text in {"🕒 Timed List", "Timed List"}:
            context.user_data["otc_submode"] = "timed"
            context.user_data["step"] = "choose_pair_en"
            await update.message.reply_text("💱 Choose an OTC pair for the timed list 👇", reply_markup=otc_pairs_keyboard_en)
            return
        if text in {"⚡ Live Trade", "Live Trade"}:
            context.user_data["otc_submode"] = "live_now"
            context.user_data["step"] = "choose_live_otc_action_en"
            await update.message.reply_text(
                "⚡ OTC Live Trade\n\nPress the button and the bot will search for the best M1 opportunity among all live OTC pairs.",
                reply_markup=otc_live_search_keyboard_en
            )
            return
        await update.message.reply_text("⚡ Choose an OTC signal type from the buttons 👇", reply_markup=otc_mode_keyboard_en)
        return

    if step == "choose_pair_en" and context.user_data.get("mode") == "otc":
        if text not in OTC_PAIRS:
            await update.message.reply_text("💱 Choose a pair from the buttons 👇", reply_markup=otc_pairs_keyboard_en)
            return
        context.user_data["pair"] = text
        context.user_data["step"] = "choose_count_en"
        await update.message.reply_text("📈 Choose the number of trades 👇", reply_markup=count_keyboard_en)
        return

    if step == "choose_count_en" and context.user_data.get("mode") == "otc":
        if text not in [str(x) for x in TRADE_COUNTS]:
            await update.message.reply_text("📈 Choose the number of trades from the buttons 👇", reply_markup=count_keyboard_en)
            return
        count = int(text)
        allowed, limit_msg = check_signal_usage_allowed_lang(user.id, count, "en")
        if not allowed:
            await update.message.reply_text(limit_msg, reply_markup=build_main_menu_for_user(user.id, "en"))
            reset_signal_state(context)
            return
        interval_minutes = 3
        pair = context.user_data["pair"]
        start_dt = next_full_minute(now_utc())
        signals = generate_signals(pair, count, interval_minutes, start_dt)
        message_text = build_signals_message(pair, count, interval_minutes, signals, lang="en")
        await update.message.reply_text(
            message_text,
            reply_markup=build_main_menu_for_user(user.id, "en"),
            parse_mode="Markdown"
        )
        record_signal_usage(user.id, count, "otc_timed")
        await publish_copy_timed_list_signals(pair, signals, interval_minutes, start_dt, source="timed_list", creator_user_id=user.id)
        reset_signal_state(context)
        return

    if step == "choose_live_otc_action_en" and context.user_data.get("mode") == "otc":
        if text != "🔎 Find a Trade Now":
            await update.message.reply_text(
                "Press the button and the bot will search for the best live trade right now 👇",
                reply_markup=otc_live_search_keyboard_en
            )
            return
        allowed, limit_msg = check_signal_usage_allowed_lang(user.id, 1, "en")
        if not allowed:
            await update.message.reply_text(limit_msg, reply_markup=build_main_menu_for_user(user.id, "en"))
            reset_signal_state(context)
            return
        await update.message.reply_text("🔎 Checking live OTC pairs on M1...")
        result = analyze_best_live_otc_now(lang="en")
        await update.message.reply_text(
            result["message"],
            reply_markup=build_main_menu_for_user(user.id, "en"),
            parse_mode="Markdown"
        )
        if result.get("ok"):
            record_signal_usage(user.id, 1, "otc_live_now")
            await maybe_publish_copy_signal(result, source="otc_live", enabled=COPY_SEND_OTC_LIVE_NOW, creator_user_id=user.id)
        reset_signal_state(context)
        return

    if step == "choose_real_pair_en":
        if text not in REAL_PAIRS:
            await update.message.reply_text("🌍 Choose a pair from the buttons 👇", reply_markup=real_pairs_keyboard_en)
            return
        context.user_data["pair"] = text
        context.user_data["step"] = "choose_interval_real_en"
        await update.message.reply_text("⏳ Choose timeframe, or let the bot find the best opportunity 👇", reply_markup=real_interval_keyboard_en)
        return

    if step == "choose_interval_real_en":
        interval_map = {"1 minute": 1, "5 minutes": 5, "10 minutes": 10}
        if text not in interval_map and text != "🔥 Best Opportunity":
            await update.message.reply_text("⏳ Choose the timeframe from the buttons 👇", reply_markup=real_interval_keyboard_en)
            return
        allowed, limit_msg = check_signal_usage_allowed_lang(user.id, 1, "en")
        if not allowed:
            await update.message.reply_text(limit_msg, reply_markup=build_main_menu_for_user(user.id, "en"))
            reset_signal_state(context)
            return
        pair = context.user_data["pair"]
        if text == "🔥 Best Opportunity":
            result = analyze_real_market_best(pair)
        else:
            result = analyze_real_market(pair, interval_map[text])
        result_msg = translate_real_signal_message_to_en(result.get("message", ""))
        await update.message.reply_text(
            result_msg,
            reply_markup=build_main_menu_for_user(user.id, "en")
        )
        if result.get("ok"):
            record_signal_usage(user.id, 1, "real_market")
            await maybe_publish_copy_signal(result, source="real_market", enabled=COPY_SEND_REAL_MARKET, creator_user_id=user.id)
        reset_signal_state(context)
        return

    await update.message.reply_text(
        "📌 Choose an option from the menu.",
        reply_markup=build_main_menu_for_user(user.id, "en")
    )


async def send_video_watched_button_job(context: ContextTypes.DEFAULT_TYPE):
    data = dict(context.job.data or {})
    user_id = int(data.get("user_id"))

    try:
        if has_used_video_trial(user_id):
            return

        if get_user_language(user_id) == "en":
            await safe_send_message(context.bot,
                chat_id=user_id,
                text=(
                    "✅ Did you watch the full bot tutorial video?\n\n"
                    "Press the button below to get a full 1-hour free trial."
                ),
                reply_markup=video_watched_keyboard_en
            )
        else:
            await safe_send_message(context.bot,
                chat_id=user_id,
                text=(
                    "✅ هل شاهدت فيديو شرح البوت كاملًا؟\n\n"
                    "اضغط الزر بالأسفل للحصول على تجربة مجانية لمدة ساعة كاملة."
                ),
                reply_markup=video_watched_keyboard
            )
    except Exception as e:
        logger.exception("Could not send video watched button: %s", e)



SIGNAL_BLOCK_KEYWORDS = [
    "توليد إشارات",
    "اختر نوع السوق",
    "OTC",
    "صفقة مباشرة",
    "ابحث عن صفقة",
    "سوق عالمي",
]


PUBLIC_KEYWORDS = [
    "نعم",
    "منضم",
    "لا",
    "مشترك",
    "تجربة مجانية",
    "فيديو شرح",
    "مشاهدة فيديو",
    "شاهدت الفيديو",
    "تواصل",
    "رجوع",
    "/start",
]


def is_public_start_flow_text(text: str) -> bool:
    raw = str(text or "").strip()

    if not raw:
        return False

    # أزرار البداية والتجربة والفيديو والانضمام مسموحة دائمًا لغير المفعّل.
    if "توليد إشارات" in raw:
        return False

    for keyword in PUBLIC_KEYWORDS:
        if keyword in raw:
            return True

    upper = raw.upper()
    if upper.startswith("ID") or "QUOTEX" in upper:
        return True

    return False



def is_start_flow_button(text: str) -> bool:
    raw = str(text or "").strip()
    compact = raw.replace(" ", "")

    keywords = [
        "نعم",
        "منضم",
        "لا",
        "مشترك",
        "الحصول على تجربة مجانية",
        "تجربة مجانية",
        "مشاهدة فيديو شرح البوت",
        "فيديو شرح",
        "شاهدت الفيديو",
        "تواصل مع المسؤول",
    ]

    if raw == "/start":
        return True

    for k in keywords:
        if k in raw:
            return True

    if "نعمأنامنضم" in compact or "نعم،أنامنضم" in compact:
        return True
    if "لالستمشترك" in compact or "لا،لستمشترك" in compact:
        return True
    if "الحصولعلىتجربةمجانية" in compact:
        return True
    if "مشاهدةفيديوشرحالبوت" in compact:
        return True

    return False


def is_signal_flow_text(text: str) -> bool:
    raw = str(text or "").strip()

    # لا تعتبر أزرار البداية إشارات حتى لو فيها كلمات عامة.
    if is_public_start_flow_text(raw):
        return False

    for keyword in SIGNAL_BLOCK_KEYWORDS:
        if keyword in raw:
            return True

    return False


def is_user_revoked_or_not_allowed(user_id: int) -> bool:
    uid = int(user_id)

    if is_admin(uid):
        return False

    # مشرف الليستات ليس تفعيل إشارات. يحتاج approved للإشارات.
    try:
        user_data = users_ref().child(str(uid)).get() or {}
        if isinstance(user_data, dict):
            status = str(user_data.get("status") or "").lower()
            if status in {"blocked", "cancelled", "rejected", "disabled", "expired"}:
                clear_user_cache(uid)
                _cache_set(f"approved:{uid}", False)
                _cache_set(f"user_status:{uid}", status)
                return True
    except Exception:
        pass

    return not is_approved(uid)


async def block_signal_for_unapproved_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reset_signal_state(context)
    await update.message.reply_text(
        "⛔ حسابك غير مفعّل حاليًا.\n\n"
        "يمكنك اختيار أحد خيارات البداية من القائمة بالأسفل:",
        reply_markup=welcome_keyboard
    )




async def handle_trading_room_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Handle Trading Session Room messages for both Arabic and English users before language-specific fallbacks."""
    if not update.message or not update.message.text:
        return False
    user = update.effective_user
    text = (update.message.text or "").strip()
    step = context.user_data.get("step")
    # ===== Trading session room =====
    if (is_admin(user.id) or is_approved(user.id)) and text in {"🧠 غرفة جلسة تداول", "🧠 Trading Session Room"}:
        reset_signal_state(context)
        await safe_send_message(
            context.bot,
            chat_id=user.id,
            text=build_trading_room_warning_message(get_user_language(user.id)),
            reply_markup=get_trading_room_menu_keyboard(user.id),
        )
        return True

    if (is_admin(user.id) or is_approved(user.id)) and text in {"✅ نعم، أنا مستعد", "نعم", "انا مستعد", "أنا مستعد", "جاهز", "✅ Yes, I am ready"}:
        state = get_trading_room_state(context, user.id)
        if not state or not state.get("active") or not state.get("pending_ready"):
            await update.message.reply_text(_tr_room_text(user.id, "لا توجد جلسة بانتظار التأكيد الآن.", "There is no session waiting for confirmation right now."), reply_markup=get_trading_room_menu_keyboard(user.id))
            return True
        state["pending_ready"] = False
        state["ready_confirmed"] = True
        await safe_send_message(
            context.bot,
            chat_id=user.id,
            text=_tr_room_text(user.id, "بسم الله، جاري البحث عن زوج مناسب...", "Starting now. Searching for a suitable pair..."),
            reply_markup=get_trading_room_active_keyboard(user.id),
        )
        try:
            context.job_queue.run_once(
                trading_room_begin_market_job,
                when=10,
                data={"admin_id": int(user.id)},
                name=f"trading_room_begin_{int(user.id)}_{int(time_module.time())}",
            )
        except Exception as e:
            logger.exception("Could not schedule trading room begin job: %s", e)
            await trading_room_start_market_flow(context, int(user.id))
        return True

    if (is_admin(user.id) or is_approved(user.id)) and text in {"❌ إلغاء الجلسة", "إلغاء الجلسة", "الغاء الجلسة", "الغاء", "إلغاء", "❌ Cancel Session"}:
        clear_trading_room_state(context, user.id)
        context.user_data["trading_room_loss_confirm_stage"] = None
        await safe_send_message(context.bot, chat_id=user.id, text=_tr_room_text(user.id, "تم إلغاء الجلسة.", "Session cancelled."), reply_markup=get_trading_room_menu_keyboard(user.id))
        return True

    if (is_admin(user.id) or is_approved(user.id)) and text in {"🛑 إنهاء اليوم", "🛑 End Today"}:
        context.user_data["trading_room_loss_confirm_stage"] = None
        await update.message.reply_text(
            _tr_room_text(user.id, "قرار ممتاز. الحفاظ على الربح والهدوء أهم من كثرة الصفقات.", "Excellent decision. Protecting profit and staying calm matters more than taking more trades."),
            reply_markup=get_trading_room_menu_keyboard(user.id),
        )
        return True

    if (is_admin(user.id) or is_approved(user.id)) and text in {"حسنا شكرا لتذكيري", "لا، خليني أتراجع", "Thanks for reminding me", "No, let me step back"}:
        context.user_data["trading_room_loss_confirm_stage"] = None
        await update.message.reply_text(
            _tr_room_text(user.id, "قرار ممتاز. إذا بتحب، فيك توقف غرفة التداول عندك نصف ساعة احتياطيًا حتى ما ترجع بتهور.", "Good decision. You can lock the trading room for 30 minutes if you want extra protection from emotional trading."),
            reply_markup=trading_room_retreat_keyboard,
        )
        return True

    if (is_admin(user.id) or is_approved(user.id)) and text in {"🧊 تعطيل غرفة التداول نصف ساعة", "أوقفني نصف ساعة", "تراجع وتعطيل نصف ساعة", "غالبًا غضب، أوقفني", "🧊 Lock room for 30 minutes", "Stop me for 30 minutes", "Step back and lock 30 minutes", "Probably anger, stop me"}:
        clear_trading_room_state(context, user.id)
        set_trading_room_cooldown(context, user.id, 1800)
        context.user_data["trading_room_loss_confirm_stage"] = None
        await update.message.reply_text(
            _tr_room_text(user.id, "🧊 تم تعطيل غرفة التداول عندك لمدة نصف ساعة.\n\nهذا القرار لحماية رأس المال ومنع التهور بعد الخسارة.", "🧊 Trading room has been locked for 30 minutes.\n\nThis protects your capital and prevents revenge trading after a loss."),
            reply_markup=get_trading_room_menu_keyboard(user.id),
        )
        return True

    if (is_admin(user.id) or is_approved(user.id)) and text in {"⏰ ذكرني بعد نصف ساعة", "⏰ Remind me in 30 minutes"}:
        try:
            context.job_queue.run_once(
                trading_room_half_hour_reminder_job,
                when=1800,
                data={"admin_id": int(user.id)},
                name=f"trading_room_reminder_{int(user.id)}_{int(time_module.time())}",
            )
        except Exception as e:
            logger.exception("Could not schedule trading room reminder: %s", e)
        await update.message.reply_text(
            "تمام، سأذكّرك بعد نصف ساعة. الأفضل الآن تبعد شوي عن الشاشة.",
            reply_markup=get_trading_room_menu_keyboard(user.id),
        )
        return True

    if (is_admin(user.id) or is_approved(user.id)) and text in {"🚀 جلسة جديدة", "🚀 New Session", "🚀 بدء جلسة جديدة", "🚀 Start New Session"}:
        # بعد جلسة خاسرة نمرر المستخدم على مراحل تهدئة قبل السماح بجلسة جديدة.
        if text in {"🚀 بدء جلسة جديدة", "🚀 Start New Session"}:
            context.user_data["trading_room_loss_confirm_stage"] = 1
            await update.message.reply_text(
                "لا يبدو هذا خيارًا صائبًا الآن. هل أنت متأكد أنك تريد جلسة جديدة مباشرة بعد الخسارة؟",
                reply_markup=trading_room_loss_confirm_keyboards[1],
            )
            return True
        context.user_data["step"] = "trading_room_waiting_balance"
        await safe_send_message(
            context.bot,
            chat_id=user.id,
            text=_tr_room_text(user.id, "💰 اكتب رصيد الحساب الحالي بالدولار.\n\nمثال: 50 أو 120.5", "💰 Send your current account balance in dollars.\n\nExample: 50 or 120.5"),
            reply_markup=get_trading_room_menu_keyboard(user.id),
        )
        return True

    if (is_admin(user.id) or is_approved(user.id)) and text in {"نعم متأكد", "لا يهمني دعنا نكمل", "عندي خطة واضحة", "أتحمل القرار", "أوافق، ابدأ جلسة جديدة", "Yes, I am sure", "I do not care, continue", "I have a clear plan", "I accept responsibility", "I agree, start a new session"}:
        stage = int(context.user_data.get("trading_room_loss_confirm_stage") or 0)
        if stage <= 0:
            await update.message.reply_text("لا يوجد تأكيد خسارة نشط الآن.", reply_markup=get_trading_room_menu_keyboard(user.id))
            return True
        if stage == 1:
            context.user_data["trading_room_loss_confirm_stage"] = 2
            await update.message.reply_text(
                "تذكر أن السوق لا يرحم. لا تنجر نحو الغضب أو محاولة الانتقام من السوق.",
                reply_markup=trading_room_loss_confirm_keyboards[2],
            )
            return True
        if stage == 2:
            context.user_data["trading_room_loss_confirm_stage"] = 3
            await update.message.reply_text(
                "قبل ما نكمل: هل قرارك مبني على خطة واضحة أم مجرد غضب من الخسارة؟",
                reply_markup=trading_room_loss_confirm_keyboards[3],
            )
            return True
        if stage == 3:
            context.user_data["trading_room_loss_confirm_stage"] = 4
            await update.message.reply_text(
                "آخر تنبيه جدي: جلسة ثانية بعد الخسارة تزيد خطر التهور. هل تتحمل القرار؟",
                reply_markup=trading_room_loss_confirm_keyboards[4],
            )
            return True
        if stage == 4:
            context.user_data["trading_room_loss_confirm_stage"] = 5
            await update.message.reply_text(
                "المرحلة الأخيرة. لو بدأت الآن، التزم بالمبلغ والحدود ولا تكسر الخطة. اختر بوعي.",
                reply_markup=trading_room_loss_confirm_keyboards[5],
            )
            return True
        context.user_data["trading_room_loss_confirm_stage"] = None
        context.user_data["step"] = "trading_room_waiting_balance"
        await update.message.reply_text(
            _tr_room_text(user.id, "تمام، القرار قرارك. اكتب رصيد الحساب الحالي بالدولار لنبدأ جلسة جديدة.", "Okay, the decision is yours. Send your current account balance in dollars to start a new session."),
            reply_markup=get_trading_room_menu_keyboard(user.id),
        )
        return True

    if (is_admin(user.id) or is_approved(user.id)) and text in {"🚀 بدء جلسة تداول", "بدء جلسة تداول", "بدء الجلسة", "🚀 Start Trading Session"}:
        remaining = get_trading_room_cooldown_remaining(context, user.id)
        if remaining > 0:
            await update.message.reply_text(
                f"🧊 غرفة التداول متوقفة احتياطيًا عندك لمدة {_cooldown_text(remaining)} تقريبًا.\n\nهذا لحماية رأس المال ومنع الدخول تحت تأثير الانفعال.",
                reply_markup=get_trading_room_menu_keyboard(user.id),
            )
            return True
        context.user_data["step"] = "trading_room_waiting_balance"
        await safe_send_message(
            context.bot,
            chat_id=user.id,
            text=_tr_room_text(user.id, "💰 اكتب رصيد الحساب الحالي بالدولار.\n\nمثال: 50 أو 120.5", "💰 Send your current account balance in dollars.\n\nExample: 50 or 120.5"),
            reply_markup=get_trading_room_menu_keyboard(user.id),
        )
        return True

    if (is_admin(user.id) or is_approved(user.id)) and text in {"📊 حالة الجلسة", "📊 Session Status"}:
        await update.message.reply_text(
            build_trading_room_state_message(get_trading_room_state(context, user.id)),
            reply_markup=get_trading_room_menu_keyboard(user.id)
        )
        return True

    if is_admin(user.id) and text in {"🩺 فحص بيانات OTC Live", "🩺 OTC Live Check"}:
        await update.message.reply_text(
            build_trading_room_market_data_status_message(),
            reply_markup=get_trading_room_menu_keyboard(user.id)
        )
        return True

    if (is_admin(user.id) or is_approved(user.id)) and text in {"▶️ متابعة الجلسة", "▶️ Continue session", "متابعة الجلسة", "Continue session"}:
        state = get_trading_room_state(context, user.id)
        if state and state.get("active") and state.get("smart_exit_waiting"):
            state["smart_exit_waiting"] = False
            state["smart_exit_reason"] = None
            state["smart_exit_last_suggested_at"] = time_module.time()
            await safe_send_message(
                context.bot,
                chat_id=user.id,
                text=_tr_room_text(user.id, "تمام، سنكمل الجلسة بحذر وبدون دخول عشوائي.", "Okay, I will continue the session carefully without random entries."),
                reply_markup=get_trading_room_active_keyboard(user.id),
            )
            return True
        await safe_send_message(context.bot, chat_id=user.id, text=_tr_room_text(user.id, "لا يوجد اقتراح إيقاف نشط الآن.", "There is no active stop suggestion right now."), reply_markup=get_trading_room_menu_keyboard(user.id))
        return True

    if (is_admin(user.id) or is_approved(user.id)) and text in {"🛑 إيقاف وحفظ النتيجة", "🛑 Stop and secure result", "إيقاف وحفظ النتيجة", "Stop and secure result"}:
        state = get_trading_room_state(context, user.id)
        if state and state.get("active"):
            net_profit = float(state.get("net_profit", 0.0) or 0.0)
            wins = int(state.get("wins", 0) or 0)
            losses = int(state.get("losses", 0) or 0)
            state["active"] = False
            state["smart_exit_waiting"] = False
            reason = _tr_room_text(user.id, "إيقاف ذكي لحفظ نتيجة الجلسة الحالية.", "Smart stop to secure the current session result.")
            end_text = (
                (_tr_room_text(user.id, "🏁 تم إيقاف جلسة التداول", "🏁 Trading session stopped") + "\n\n")
                + (_tr_room_text(user.id, "السبب: ", "Reason: ") + reason + "\n")
                + (_tr_room_text(user.id, f"النتيجة الحالية: {wins} ربح / {losses} خسارة\n", f"Current result: {wins} win / {losses} loss\n"))
                + (_tr_room_text(user.id, f"صافي الجلسة: {_money_signed(net_profit)}\n\n", f"Session net: {_money_signed(net_profit)}\n\n"))
                + (_tr_room_text(user.id, "قرار ممتاز. أحيانًا حماية النتيجة أهم من مطاردة التارجت الكامل.", "Excellent decision. Sometimes protecting the current result matters more than chasing the full target."))
            )
            await safe_send_message(context.bot, chat_id=user.id, text=end_text, reply_markup=get_trading_room_menu_keyboard(user.id))
            return True
        await safe_send_message(context.bot, chat_id=user.id, text=_tr_room_text(user.id, "لا توجد جلسة نشطة الآن.", "There is no active session right now."), reply_markup=get_trading_room_menu_keyboard(user.id))
        return True

    if (is_admin(user.id) or is_approved(user.id)) and text in {"🛑 إيقاف الجلسة", "إيقاف الجلسة", "ايقاف الجلسة", "وقف الجلسة", "إيقاف", "ايقاف", "🛑 Stop Session"}:
        context.user_data["trading_room_loss_confirm_stage"] = None
        clear_trading_room_state(context, user.id)
        await safe_send_message(
            context.bot,
            chat_id=user.id,
            text="🛑 تم إيقاف جلسة التداول التجريبية.",
            reply_markup=get_trading_room_menu_keyboard(user.id),
        )
        return True

    if (is_admin(user.id) or is_approved(user.id)) and step == "trading_room_waiting_balance":
        if text in {"🛑 إيقاف الجلسة", "إيقاف الجلسة", "ايقاف الجلسة", "وقف الجلسة", "إيقاف", "ايقاف", "🛑 Stop Session", "❌ إلغاء الجلسة", "إلغاء الجلسة", "الغاء الجلسة", "الغاء", "إلغاء", "❌ Cancel Session", "⬅️ رجوع", "🔙 رجوع", "رجوع", "🔙 Back", "Back"}:
            context.user_data["step"] = None
            context.user_data["trading_room_loss_confirm_stage"] = None
            clear_trading_room_state(context, user.id)
            await safe_send_message(
                context.bot,
                chat_id=user.id,
                text="🛑 تم إيقاف جلسة التداول التجريبية.",
                reply_markup=get_trading_room_menu_keyboard(user.id),
            )
            return True
        remaining = get_trading_room_cooldown_remaining(context, user.id)
        if remaining > 0:
            context.user_data["step"] = None
            await update.message.reply_text(
                f"🧊 غرفة التداول متوقفة احتياطيًا عندك لمدة {_cooldown_text(remaining)} تقريبًا.",
                reply_markup=get_trading_room_menu_keyboard(user.id),
            )
            return True
        balance = parse_balance_amount(text)
        if balance is None:
            await safe_send_message(
                context.bot,
                chat_id=user.id,
                text=_tr_room_text(user.id, "❌ لم أفهم الرصيد. اكتب رقم فقط، مثال: 50 أو 120.5", "❌ I could not understand the balance. Send a number only, example: 50 or 120.5"),
                reply_markup=get_trading_room_menu_keyboard(user.id),
            )
            return True
        context.user_data["step"] = None
        await start_trading_room_session(update, context, balance)
        return True

    return False

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    user = update.effective_user
    text = (update.message.text or "").strip()
    step = context.user_data.get("step")

    # ===== Language selection gate =====
    if text in {"🌐 تغيير اللغة", "🌐 Change Language", "Change Language", "تغيير اللغة", "Language"}:
        reset_signal_state(context)
        await ask_language(update)
        return

    if text in {"🇸🇦 العربية", "العربية", "Arabic"}:
        set_user_language(user.id, "ar", context)
        reset_signal_state(context)
        if is_approved(user.id):
            await update.message.reply_text(
                "✅ تم اختيار اللغة العربية.\nمرحبًا بك في بوت TRADING TIME.",
                reply_markup=build_main_menu_for_user(user.id, "ar")
            )
        else:
            await send_welcome_flow(update, "ar")
        return

    if text in {"🇬🇧 English", "English", "الإنجليزية"}:
        set_user_language(user.id, "en", context)
        reset_signal_state(context)
        if is_approved(user.id):
            await update.message.reply_text(
                "✅ English language selected.\nWelcome to TRADING TIME Bot.",
                reply_markup=build_main_menu_for_user(user.id, "en")
            )
        else:
            await send_welcome_flow(update, "en")
        return

    if (not is_admin(user.id)) and (not has_selected_language(user.id, context)):
        await ask_language(update)
        return

    lang = get_user_language(user.id, context)

    # ===== Maintenance mode =====
    # يسمح بتغيير اللغة فقط، لكن أي استخدام آخر أثناء الإيقاف يُسجل ليتم إعلامه عند عودة البوت.
    if not is_admin(user.id) and not get_bot_enabled():
        await send_maintenance_message(update, context, lang)
        return

    # Trading Session Room is shared between Arabic and English users; handle it before English fallback.
    if await handle_trading_room_message(update, context):
        return

    if lang == "en":
        await handle_message_en(update, context)
        return

    if "تواصل مع المسؤول" in text:
        await update.message.reply_text(
            "📞 للتواصل مع المسؤول:\n@coach_WAEL_trading",
            reply_markup=welcome_keyboard if not is_approved(user.id) else build_main_menu_for_user(user.id)
        )
        return


    # ===== SIGNAL-ONLY ACCESS GATE =====
    # غير المفعّل يُمنع فقط من الإشارات، وليس من أزرار البداية أو التجربة.
    if is_signal_flow_text(text) and is_user_revoked_or_not_allowed(user.id):
        await block_signal_for_unapproved_user(update, context)
        return


    if "تجربة مجانية" in text:
        if has_used_video_trial(user.id):
            await update.message.reply_text(
                "ℹ️ لقد استخدمت التجربة المجانية سابقًا.\n\n"
                "يمكنك إرسال طلب تفعيل من القائمة.",
                reply_markup=welcome_keyboard if not is_approved(user.id) else build_main_menu_for_user(user.id)
            )
            return

        mark_video_trial_started(user.id)

        await update.message.reply_text(
            "🎁 للحصول على تجربة مجانية لمدة ساعة كاملة:\n\n"
            "1️⃣ شاهد فيديو شرح البوت كاملًا من الرابط التالي:\n"
            f"{YOUTUBE_TUTORIAL_URL}\n\n"
            "2️⃣ بعد قليل سيظهر لك زر:\n"
            "✅ شاهدت الفيديو\n\n"
            "بعد الضغط عليه سيتم تفعيل التجربة المجانية تلقائيًا.",
            reply_markup=ReplyKeyboardMarkup([["🔙 رجوع"]], resize_keyboard=True)
        )

        try:
            context.job_queue.run_once(
                send_video_watched_button_job,
                when=VIDEO_TRIAL_DELAY_SECONDS,
                data={"user_id": user.id},
                name=f"video_trial_button_{user.id}_{int(time_module.time())}",
            )
        except Exception as e:
            logger.exception("Could not schedule video watched button: %s", e)

        return


    # ===== Video tutorial trial =====
    if "فيديو شرح" in text:
        if has_used_video_trial(user.id):
            await update.message.reply_text(
                "🎥 فيديو شرح البوت:\n"
                f"{YOUTUBE_TUTORIAL_URL}\n\n"
                "ℹ️ لقد استخدمت التجربة المجانية سابقًا.",
                reply_markup=build_main_menu_for_user(user.id) if is_approved(user.id) or is_admin(user.id) or is_otc_list_manager(user.id) else welcome_keyboard
            )
            return

        mark_video_trial_started(user.id)

        await update.message.reply_text(
            "🎥 شاهد فيديو شرح البوت كاملًا:\n"
            f"{YOUTUBE_TUTORIAL_URL}\n\n"
            "بعد مشاهدة الفيديو سيظهر لك زر:\n"
            "✅ شاهدت الفيديو\n\n"
            "عند الضغط عليه ستحصل على تجربة مجانية لمدة ساعة كاملة.",
            reply_markup=ReplyKeyboardMarkup([["🔙 رجوع"]], resize_keyboard=True)
        )

        try:
            context.job_queue.run_once(
                send_video_watched_button_job,
                when=VIDEO_TRIAL_DELAY_SECONDS,
                data={"user_id": user.id},
                name=f"video_trial_button_{user.id}_{int(time_module.time())}",
            )
        except Exception as e:
            logger.exception("Could not schedule video watched button: %s", e)

        return

    if "شاهدت الفيديو" in text:
        if has_used_video_trial(user.id):
            await update.message.reply_text(
                "ℹ️ لقد استخدمت التجربة المجانية سابقًا.",
                reply_markup=build_main_menu_for_user(user.id) if is_approved(user.id) or is_admin(user.id) or is_otc_list_manager(user.id) else welcome_keyboard
            )
            return

        eligible_after = get_video_trial_eligible_after(user.id)
        now_ts = int(time_module.time())

        if eligible_after and now_ts < eligible_after:
            remaining = eligible_after - now_ts
            await update.message.reply_text(
                f"⏳ انتظر قليلًا، يمكنك تفعيل التجربة بعد حوالي {remaining} ثانية.",
                reply_markup=video_watched_keyboard
            )
            return

        expire_at = activate_video_trial_for_user(user.id)
        reset_signal_state(context)

        await update.message.reply_text(
            "✅ تم تفعيل التجربة المجانية لمدة ساعة كاملة.\n\n"
            f"⏳ تنتهي التجربة عند: {expire_at.strftime('%H:%M')} UTC\n\n"
            "يمكنك الآن استخدام البوت.",
            reply_markup=build_main_menu_for_user(user.id)
        )
        return


    # ===== Limited OTC list manager hard gate =====
    if (not is_admin(user.id)) and is_otc_list_manager(user.id) and not is_start_flow_button(text):
        if "فيديو شرح" in text:
            await update.message.reply_text(
                "🎥 فيديو شرح البوت:\nhttps://www.youtube.com/watch?v=YPqgJcgvyFw",
                reply_markup=otc_list_manager_keyboard
            )
            return

        if text == "🧾 فحص ليستة OTC":
            context.user_data["step"] = "otc_list_waiting_text"
            await update.message.reply_text(
                "🧾 أرسل الآن ليستة OTC كاملة كما نشرتها بالقناة.\n"
                "سأراقبها وأخبرك عندما تنتهي آخر صفقة وتصبح النتائج جاهزة.",
                reply_markup=otc_list_manager_keyboard
            )
            return

        if step == "otc_list_waiting_text" or looks_like_otc_list_text(text):
            context.user_data["step"] = None
            await start_otc_list_watch_for_user(update, context, text, otc_list_manager_keyboard)
            return

        if text == "📋 عرض نتائج الليستة":
            saved_result = get_ready_otc_list_result(user.id)
            result_text = saved_result.get("result_text")
            if not result_text:
                recovered_text, _meta = recover_otc_list_result_now(user.id)
                if recovered_text:
                    result_text = recovered_text
            result_text = prettify_existing_otc_result_text(result_text) if result_text else result_text
            if not result_text:
                await update.message.reply_text(
                    "لا توجد نتيجة جاهزة بعد. أرسل ليستة أولًا أو انتظر رسالة الجاهزية.",
                    reply_markup=otc_list_manager_keyboard
                )
                return
            await update.message.reply_text(
                normalize_pretty_otc_result_for_telegram(result_text),
                reply_markup=otc_list_manager_keyboard,
                parse_mode="HTML"
            )
            return

        if text in {"🔙 رجوع", "⬅️ رجوع", "رجوع", "/start"}:
            context.user_data["step"] = None
            await show_otc_list_manager_panel(update)
            return

        # أزرار البوت العادي مسموحة لهذا الشخص أيضًا، لذلك نتركها تكمل للمعالجة العادية.
        if text in {"📊 توليد إشارات", "📞 تواصل مع المسؤول", "⚡ OTC", "🕒 زمني", "⚡ صفقة مباشرة", "🔎 ابحث عن صفقة الآن"}:
            pass
        else:
            context.user_data["step"] = None
            await update.message.reply_text("🧾 لوحة فحص ليستات OTC 👇", reply_markup=otc_list_manager_keyboard)
            return


    # ===== Absolute cancel guard for admin waiting states =====
    if is_admin(user.id) and text.strip() in {"رجوع", "⬅️ رجوع", "🔙 رجوع"}:
        if context.user_data.get("step") in {
            "admin_broadcast_waiting_message",
            "admin_message_user_waiting_text",
            "otc_stats_waiting_count",
            "otc_list_waiting_text",
            "otc_pair_diagnostics_waiting",
            "otc_candle_diagnostics_waiting",
            "copy_disable_waiting_token",
        }:
            context.user_data["step"] = None
            context.user_data.pop("target_user_id", None)
            context.user_data.pop("target_message_user_id", None)
            await update.message.reply_text("تم إلغاء العملية.", reply_markup=admin_main_keyboard)
            return

    save_user_record(user.id, {
        "telegram_id": user.id,
        "name": user.full_name,
        "username": user.username or "",
        "last_seen": now_iso(),
    })

    # ===== Maintenance mode =====
    if not is_admin(user.id) and not get_bot_enabled():
        await send_maintenance_message(update, context, get_user_language(user.id, context))
        return

    if "فيديو شرح" in text:
        await update.message.reply_text(
            "🎥 فيديو شرح البوت:\n"
            "https://www.youtube.com/watch?v=YPqgJcgvyFw",
            reply_markup=build_main_menu_for_user(user.id) if is_approved(user.id) or is_admin(user.id) else welcome_keyboard
        )
        return

    # ===== Non-approved users =====
    if not is_admin(user.id) and not is_approved(user.id):
        current_status = get_user_status(user.id)

        # فيديو الشرح مسموح حتى قبل التفعيل
        if "فيديو شرح" in text:
            await update.message.reply_text(
                "🎥 فيديو شرح البوت:\n"
                "https://www.youtube.com/watch?v=YPqgJcgvyFw",
                reply_markup=welcome_keyboard
            )
            return

        # رجوع للمستخدم غير المفعل يرجعه لقائمة الاشتراك، ولا يعلقه بخطوات قديمة
        if text in {"🔙 رجوع", "⬅️ رجوع", "رجوع"}:
            reset_signal_state(context)
            await update.message.reply_text("تم الرجوع.", reply_markup=welcome_keyboard)
            return

        # pending لا يرسل طلب ثاني قبل القرار
        if current_status == "pending":
            await update.message.reply_text(
                "⏳ لديك طلب تفعيل قيد المراجعة بالفعل.\n\n"
                "لا يمكنك إرسال طلب جديد قبل أن يتم قبول أو رفض الطلب السابق.",
                reply_markup=welcome_keyboard
            )
            return

        # نعم أنا منضم: مسموحة للجديد والمرفوض والملغى تفعيله والمنتهي
        if "نعم" in text and "منضم" in text:
            context.user_data["step"] = "waiting_quotex_id"
            await update.message.reply_text(
                "📩 أرسل ID الخاص بحسابك على QUOTEX ليتم فحص حسابك.\n"
                "بعد التأكد سيتم إتاحة البوت لك بشكل مجاني.",
                reply_markup=ReplyKeyboardMarkup([["🔙 رجوع"]], resize_keyboard=True)
            )
            return

        if "لا" in text and "مشترك" in text:
            await update.message.reply_text(WELCOME_MESSAGE, reply_markup=welcome_keyboard)
            return

        if step == "waiting_quotex_id":
            quotex_id = text.strip()

            if not quotex_id or quotex_id in {"✅ نعم، أنا منضم", "❌ لا، لست مشتركًا"}:
                await update.message.reply_text(
                    "📩 أرسل ID حسابك في Quotex فقط.",
                    reply_markup=ReplyKeyboardMarkup([["🔙 رجوع"]], resize_keyboard=True)
                )
                return

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
                "name": user.full_name,
                "username": user.username or "",
                "updated_at": now_iso(),
            })

            await update.message.reply_text(
                "📩 تم استلام طلبك بنجاح\n\n"
                "تم حفظ Quotex ID الخاص بك وإرساله للإدارة للمراجعة.\n"
                "بعد التأكد، سيتم تفعيل البوت لك مجانًا.\n\n"
                "يرجى انتظار موافقة الأدمن ✅",
                reply_markup=welcome_keyboard
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
                await safe_send_message(context.bot,
                    chat_id=ADMIN_TELEGRAM_ID,
                    text=admin_message,
                    parse_mode="HTML",
                    reply_markup=build_pending_request_keyboard(user.id)
                )
            except Exception:
                pass

            context.user_data["step"] = "pending_review"
            return

        # أي خيار آخر لغير المفعل يرجع لقائمة الاشتراك بدل تعليق المستخدم
        await send_welcome_flow(update)
        return

    # ===== Admin waiting inputs =====
    # ===== Admin direct message to one user =====
    if is_admin(user.id) and step == "admin_direct_message_waiting":
        target_id = context.user_data.get("admin_message_target_id")
        message = text.strip()

        if not target_id:
            context.user_data["step"] = None
            await update.message.reply_text("❌ لا يوجد مستخدم محدد.", reply_markup=admin_main_keyboard)
            return

        if not message:
            context.user_data["step"] = None
            await update.message.reply_text("تم إلغاء الرسالة لأنها فارغة.", reply_markup=admin_main_keyboard)
            return

        try:
            await safe_send_message(context.bot,
                chat_id=int(target_id),
                text="💬 رسالة من الأدمن\n\n" + message
            )
            await update.message.reply_text("✅ تم إرسال الرسالة للمستخدم.", reply_markup=admin_main_keyboard)
        except Exception as e:
            await update.message.reply_text(f"❌ فشل إرسال الرسالة: {e}", reply_markup=admin_main_keyboard)

        context.user_data["step"] = None
        context.user_data["admin_message_target_id"] = None
        return

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
        approved_users = get_all_approved_users() or {}
        sent_count = 0
        failed_count = 0
        skipped_count = 0
        if message in {"رجوع", "⬅️ رجوع", "🔙 رجوع"}:
            await update.message.reply_text("تم إلغاء الرسالة الجماعية.", reply_markup=admin_main_keyboard)
            return

        broadcast_text = "📢 رسالة من الأدمن\n\n" + message

        for uid in list(approved_users.keys()):
            try:
                user_id_int = int(uid)

                # نرسل فقط للمستخدمين المفعّلين حاليًا، ونتجاهل المحظور أو المنتهي.
                if not is_approved(user_id_int):
                    skipped_count += 1
                    continue

                await safe_send_message(context.bot,chat_id=user_id_int, text=broadcast_text)
                sent_count += 1
            except Exception:
                failed_count += 1

        await update.message.reply_text(
            f"📢 تم إرسال الرسالة الجماعية للمستخدمين المفعّلين فقط.\n\n"
            f"✅ وصل: {sent_count}\n"
            f"⏭️ تم تجاهل غير المفعّلين/المنتهيين: {skipped_count}\n"
            f"❌ فشل: {failed_count}",
            reply_markup=admin_main_keyboard
        )
        return

    if is_otc_list_manager(user.id) and step == "otc_list_waiting_text":
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


    # ===== Broadcast waiting input fixed =====
    if is_admin(user.id) and step == "admin_broadcast_waiting_message":
        message = text.strip()

        if message in {"رجوع", "⬅️ رجوع", "🔙 رجوع"}:
            context.user_data["step"] = None
            await update.message.reply_text("تم إلغاء الرسالة الجماعية.", reply_markup=admin_main_keyboard)
            return

        context.user_data["step"] = None

        if not message:
            await update.message.reply_text("الرسالة فارغة، تم الإلغاء.", reply_markup=admin_main_keyboard)
            return

        approved_users = get_all_approved_users() or {}
        sent_count = 0
        failed_count = 0
        skipped_count = 0
        if message in {"رجوع", "⬅️ رجوع", "🔙 رجوع"}:
            await update.message.reply_text("تم إلغاء الرسالة الجماعية.", reply_markup=admin_main_keyboard)
            return

        broadcast_text = "📢 رسالة من الأدمن\n\n" + message

        for uid in list(approved_users.keys()):
            try:
                user_id_int = int(uid)

                # نرسل فقط للمستخدمين المفعّلين حاليًا، ونتجاهل المحظور أو المنتهي.
                if not is_approved(user_id_int):
                    skipped_count += 1
                    continue

                await safe_send_message(context.bot,chat_id=user_id_int, text=broadcast_text)
                sent_count += 1
            except Exception:
                failed_count += 1

        await update.message.reply_text(
            f"📢 تم إرسال الرسالة الجماعية للمستخدمين المفعّلين فقط.\n\n"
            f"✅ وصل: {sent_count}\n"
            f"⏭️ تم تجاهل غير المفعّلين/المنتهيين: {skipped_count}\n"
            f"❌ فشل: {failed_count}",
            reply_markup=admin_main_keyboard
        )
        return

    if is_admin(user.id) and text == "📢 رسالة جماعية":
        context.user_data["step"] = "admin_broadcast_waiting_message"
        await update.message.reply_text(
            "📢 اكتب الآن الرسالة التي تريد إرسالها لجميع مستخدمي البوت.\n\n"
            "لإلغاء العملية اضغط رجوع.",
            reply_markup=admin_main_keyboard
        )
        return


    # ===== Trading session room =====
    if (is_admin(user.id) or is_approved(user.id)) and text in {"🧠 غرفة جلسة تداول", "🧠 Trading Session Room"}:
        reset_signal_state(context)
        await safe_send_message(
            context.bot,
            chat_id=user.id,
            text=build_trading_room_warning_message(get_user_language(user.id)),
            reply_markup=get_trading_room_menu_keyboard(user.id),
        )
        return

    if (is_admin(user.id) or is_approved(user.id)) and text in {"✅ نعم، أنا مستعد", "نعم", "انا مستعد", "أنا مستعد", "جاهز", "✅ Yes, I am ready"}:
        state = get_trading_room_state(context, user.id)
        if not state or not state.get("active") or not state.get("pending_ready"):
            await update.message.reply_text(_tr_room_text(user.id, "لا توجد جلسة بانتظار التأكيد الآن.", "There is no session waiting for confirmation right now."), reply_markup=get_trading_room_menu_keyboard(user.id))
            return
        state["pending_ready"] = False
        state["ready_confirmed"] = True
        await safe_send_message(
            context.bot,
            chat_id=user.id,
            text=_tr_room_text(user.id, "بسم الله، جاري البحث عن زوج مناسب...", "Starting now. Searching for a suitable pair..."),
            reply_markup=get_trading_room_active_keyboard(user.id),
        )
        try:
            context.job_queue.run_once(
                trading_room_begin_market_job,
                when=10,
                data={"admin_id": int(user.id)},
                name=f"trading_room_begin_{int(user.id)}_{int(time_module.time())}",
            )
        except Exception as e:
            logger.exception("Could not schedule trading room begin job: %s", e)
            await trading_room_start_market_flow(context, int(user.id))
        return

    if (is_admin(user.id) or is_approved(user.id)) and text in {"❌ إلغاء الجلسة", "إلغاء الجلسة", "الغاء الجلسة", "الغاء", "إلغاء", "❌ Cancel Session"}:
        clear_trading_room_state(context, user.id)
        context.user_data["trading_room_loss_confirm_stage"] = None
        await safe_send_message(context.bot, chat_id=user.id, text=_tr_room_text(user.id, "تم إلغاء الجلسة.", "Session cancelled."), reply_markup=get_trading_room_menu_keyboard(user.id))
        return

    if (is_admin(user.id) or is_approved(user.id)) and text in {"🛑 إنهاء اليوم", "🛑 End Today"}:
        context.user_data["trading_room_loss_confirm_stage"] = None
        await update.message.reply_text(
            _tr_room_text(user.id, "قرار ممتاز. الحفاظ على الربح والهدوء أهم من كثرة الصفقات.", "Excellent decision. Protecting profit and staying calm matters more than taking more trades."),
            reply_markup=get_trading_room_menu_keyboard(user.id),
        )
        return

    if (is_admin(user.id) or is_approved(user.id)) and text in {"حسنا شكرا لتذكيري", "لا، خليني أتراجع", "Thanks for reminding me", "No, let me step back"}:
        context.user_data["trading_room_loss_confirm_stage"] = None
        await update.message.reply_text(
            _tr_room_text(user.id, "قرار ممتاز. إذا بتحب، فيك توقف غرفة التداول عندك نصف ساعة احتياطيًا حتى ما ترجع بتهور.", "Good decision. You can lock the trading room for 30 minutes if you want extra protection from emotional trading."),
            reply_markup=trading_room_retreat_keyboard,
        )
        return

    if (is_admin(user.id) or is_approved(user.id)) and text in {"🧊 تعطيل غرفة التداول نصف ساعة", "أوقفني نصف ساعة", "تراجع وتعطيل نصف ساعة", "غالبًا غضب، أوقفني", "🧊 Lock room for 30 minutes", "Stop me for 30 minutes", "Step back and lock 30 minutes", "Probably anger, stop me"}:
        clear_trading_room_state(context, user.id)
        set_trading_room_cooldown(context, user.id, 1800)
        context.user_data["trading_room_loss_confirm_stage"] = None
        await update.message.reply_text(
            _tr_room_text(user.id, "🧊 تم تعطيل غرفة التداول عندك لمدة نصف ساعة.\n\nهذا القرار لحماية رأس المال ومنع التهور بعد الخسارة.", "🧊 Trading room has been locked for 30 minutes.\n\nThis protects your capital and prevents revenge trading after a loss."),
            reply_markup=get_trading_room_menu_keyboard(user.id),
        )
        return

    if (is_admin(user.id) or is_approved(user.id)) and text in {"⏰ ذكرني بعد نصف ساعة", "⏰ Remind me in 30 minutes"}:
        try:
            context.job_queue.run_once(
                trading_room_half_hour_reminder_job,
                when=1800,
                data={"admin_id": int(user.id)},
                name=f"trading_room_reminder_{int(user.id)}_{int(time_module.time())}",
            )
        except Exception as e:
            logger.exception("Could not schedule trading room reminder: %s", e)
        await update.message.reply_text(
            "تمام، سأذكّرك بعد نصف ساعة. الأفضل الآن تبعد شوي عن الشاشة.",
            reply_markup=get_trading_room_menu_keyboard(user.id),
        )
        return

    if (is_admin(user.id) or is_approved(user.id)) and text in {"🚀 جلسة جديدة", "🚀 New Session", "🚀 بدء جلسة جديدة", "🚀 Start New Session"}:
        # بعد جلسة خاسرة نمرر المستخدم على مراحل تهدئة قبل السماح بجلسة جديدة.
        if text in {"🚀 بدء جلسة جديدة", "🚀 Start New Session"}:
            context.user_data["trading_room_loss_confirm_stage"] = 1
            await update.message.reply_text(
                "لا يبدو هذا خيارًا صائبًا الآن. هل أنت متأكد أنك تريد جلسة جديدة مباشرة بعد الخسارة؟",
                reply_markup=trading_room_loss_confirm_keyboards[1],
            )
            return
        context.user_data["step"] = "trading_room_waiting_balance"
        await safe_send_message(
            context.bot,
            chat_id=user.id,
            text=_tr_room_text(user.id, "💰 اكتب رصيد الحساب الحالي بالدولار.\n\nمثال: 50 أو 120.5", "💰 Send your current account balance in dollars.\n\nExample: 50 or 120.5"),
            reply_markup=get_trading_room_menu_keyboard(user.id),
        )
        return

    if (is_admin(user.id) or is_approved(user.id)) and text in {"نعم متأكد", "لا يهمني دعنا نكمل", "عندي خطة واضحة", "أتحمل القرار", "أوافق، ابدأ جلسة جديدة", "Yes, I am sure", "I do not care, continue", "I have a clear plan", "I accept responsibility", "I agree, start a new session"}:
        stage = int(context.user_data.get("trading_room_loss_confirm_stage") or 0)
        if stage <= 0:
            await update.message.reply_text("لا يوجد تأكيد خسارة نشط الآن.", reply_markup=get_trading_room_menu_keyboard(user.id))
            return
        if stage == 1:
            context.user_data["trading_room_loss_confirm_stage"] = 2
            await update.message.reply_text(
                "تذكر أن السوق لا يرحم. لا تنجر نحو الغضب أو محاولة الانتقام من السوق.",
                reply_markup=trading_room_loss_confirm_keyboards[2],
            )
            return
        if stage == 2:
            context.user_data["trading_room_loss_confirm_stage"] = 3
            await update.message.reply_text(
                "قبل ما نكمل: هل قرارك مبني على خطة واضحة أم مجرد غضب من الخسارة؟",
                reply_markup=trading_room_loss_confirm_keyboards[3],
            )
            return
        if stage == 3:
            context.user_data["trading_room_loss_confirm_stage"] = 4
            await update.message.reply_text(
                "آخر تنبيه جدي: جلسة ثانية بعد الخسارة تزيد خطر التهور. هل تتحمل القرار؟",
                reply_markup=trading_room_loss_confirm_keyboards[4],
            )
            return
        if stage == 4:
            context.user_data["trading_room_loss_confirm_stage"] = 5
            await update.message.reply_text(
                "المرحلة الأخيرة. لو بدأت الآن، التزم بالمبلغ والحدود ولا تكسر الخطة. اختر بوعي.",
                reply_markup=trading_room_loss_confirm_keyboards[5],
            )
            return
        context.user_data["trading_room_loss_confirm_stage"] = None
        context.user_data["step"] = "trading_room_waiting_balance"
        await update.message.reply_text(
            _tr_room_text(user.id, "تمام، القرار قرارك. اكتب رصيد الحساب الحالي بالدولار لنبدأ جلسة جديدة.", "Okay, the decision is yours. Send your current account balance in dollars to start a new session."),
            reply_markup=get_trading_room_menu_keyboard(user.id),
        )
        return

    if (is_admin(user.id) or is_approved(user.id)) and text in {"🚀 بدء جلسة تداول", "بدء جلسة تداول", "بدء الجلسة", "🚀 Start Trading Session"}:
        remaining = get_trading_room_cooldown_remaining(context, user.id)
        if remaining > 0:
            await update.message.reply_text(
                f"🧊 غرفة التداول متوقفة احتياطيًا عندك لمدة {_cooldown_text(remaining)} تقريبًا.\n\nهذا لحماية رأس المال ومنع الدخول تحت تأثير الانفعال.",
                reply_markup=get_trading_room_menu_keyboard(user.id),
            )
            return
        context.user_data["step"] = "trading_room_waiting_balance"
        await safe_send_message(
            context.bot,
            chat_id=user.id,
            text=_tr_room_text(user.id, "💰 اكتب رصيد الحساب الحالي بالدولار.\n\nمثال: 50 أو 120.5", "💰 Send your current account balance in dollars.\n\nExample: 50 or 120.5"),
            reply_markup=get_trading_room_menu_keyboard(user.id),
        )
        return

    if (is_admin(user.id) or is_approved(user.id)) and text in {"📊 حالة الجلسة", "📊 Session Status"}:
        await update.message.reply_text(
            build_trading_room_state_message(get_trading_room_state(context, user.id)),
            reply_markup=get_trading_room_menu_keyboard(user.id)
        )
        return

    if is_admin(user.id) and text in {"🩺 فحص بيانات OTC Live", "🩺 OTC Live Check"}:
        await update.message.reply_text(
            build_trading_room_market_data_status_message(),
            reply_markup=get_trading_room_menu_keyboard(user.id)
        )
        return

    if (is_admin(user.id) or is_approved(user.id)) and text in {"🛑 إيقاف الجلسة", "إيقاف الجلسة", "ايقاف الجلسة", "وقف الجلسة", "إيقاف", "ايقاف", "🛑 Stop Session"}:
        context.user_data["trading_room_loss_confirm_stage"] = None
        clear_trading_room_state(context, user.id)
        await safe_send_message(
            context.bot,
            chat_id=user.id,
            text="🛑 تم إيقاف جلسة التداول التجريبية.",
            reply_markup=get_trading_room_menu_keyboard(user.id),
        )
        return

    if (is_admin(user.id) or is_approved(user.id)) and step == "trading_room_waiting_balance":
        if text in {"🛑 إيقاف الجلسة", "إيقاف الجلسة", "ايقاف الجلسة", "وقف الجلسة", "إيقاف", "ايقاف", "🛑 Stop Session", "❌ إلغاء الجلسة", "إلغاء الجلسة", "الغاء الجلسة", "الغاء", "إلغاء", "❌ Cancel Session", "⬅️ رجوع", "🔙 رجوع", "رجوع", "🔙 Back", "Back"}:
            context.user_data["step"] = None
            context.user_data["trading_room_loss_confirm_stage"] = None
            clear_trading_room_state(context, user.id)
            await safe_send_message(
                context.bot,
                chat_id=user.id,
                text="🛑 تم إيقاف جلسة التداول التجريبية.",
                reply_markup=get_trading_room_menu_keyboard(user.id),
            )
            return
        remaining = get_trading_room_cooldown_remaining(context, user.id)
        if remaining > 0:
            context.user_data["step"] = None
            await update.message.reply_text(
                f"🧊 غرفة التداول متوقفة احتياطيًا عندك لمدة {_cooldown_text(remaining)} تقريبًا.",
                reply_markup=get_trading_room_menu_keyboard(user.id),
            )
            return
        balance = parse_balance_amount(text)
        if balance is None:
            await safe_send_message(
                context.bot,
                chat_id=user.id,
                text=_tr_room_text(user.id, "❌ لم أفهم الرصيد. اكتب رقم فقط، مثال: 50 أو 120.5", "❌ I could not understand the balance. Send a number only, example: 50 or 120.5"),
                reply_markup=get_trading_room_menu_keyboard(user.id),
            )
            return
        context.user_data["step"] = None
        await start_trading_room_session(update, context, balance)
        return

    # ===== Common buttons =====
    if text == "🔙 رجوع":
        if is_admin(user.id) and step in {"otc_stats_waiting_count", "admin_broadcast_waiting_message", "otc_list_waiting_text", "otc_pair_diagnostics_waiting", "otc_candle_diagnostics_waiting", "otc_edge_waiting_pair", "otc_edge_watch_waiting_pair", "three_candle_waiting_daily_limit", "three_candle_waiting_summary_count", "trading_room_waiting_balance"}:
            context.user_data["step"] = None
            await update.message.reply_text("تم الرجوع.", reply_markup=admin_main_keyboard)
            return

        if is_admin(user.id) and step in {"otc_stats_waiting_count", "admin_broadcast_waiting_message"}:
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
        allowed, limit_msg = check_signal_usage_allowed(user.id, 1)
        if not allowed:
            reset_signal_state(context)
            await update.message.reply_text(limit_msg, reply_markup=build_main_menu_for_user(user.id))
            return

        reset_signal_state(context)
        context.user_data["step"] = "choose_market_mode"
        await update.message.reply_text("📊 اختر نوع السوق 👇", reply_markup=market_mode_keyboard)
        return

    if text in {"👤 حسابي", "👤 حالة حسابي"}:
        await update.message.reply_text(
            build_account_status_message(user),
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
            sent, failed = await notify_maintenance_waiters(context)
            msg = "✅ تم تشغيل البوت للعامة"
            if sent or failed:
                msg += f"\n\n📣 تم إعلام {sent} مستخدم حاولوا استخدام البوت أثناء الصيانة."
                if failed:
                    msg += f"\n⚠️ تعذر إرسال الإشعار إلى {failed} مستخدم."
            await update.message.reply_text(msg, reply_markup=admin_main_keyboard)
            return

        if text == "🔴 إيقاف البوت":
            set_bot_enabled(False)
            await update.message.reply_text("🛠 تم إيقاف البوت للعامة", reply_markup=admin_main_keyboard)
            return

        if text in {"🔐 Copy Trading", "Copy Trading"}:
            reset_signal_state(context)
            await update.message.reply_text(
                "🔐 لوحة Copy Trading\n\nإدارة الأكواد + توجيه شخصي: صفقات كل مستخدم تصل فقط لإضافته حسب Telegram ID.",
                reply_markup=copy_admin_keyboard
            )
            return

        if text == "🟢 تشغيل Copy":
            if set_copy_global_enabled(True, user.id):
                await update.message.reply_text("✅ تم تشغيل Copy Trading للجميع.", reply_markup=copy_admin_keyboard)
            else:
                await update.message.reply_text("❌ تعذر تشغيل Copy Trading. راجع لوج Render.", reply_markup=copy_admin_keyboard)
            return

        if text == "🔴 إيقاف Copy":
            if set_copy_global_enabled(False, user.id):
                await update.message.reply_text("🔴 تم إيقاف Copy Trading للجميع. الإضافات تبقى متصلة لكن لن تستقبل صفقات جديدة.", reply_markup=copy_admin_keyboard)
            else:
                await update.message.reply_text("❌ تعذر إيقاف Copy Trading. راجع لوج Render.", reply_markup=copy_admin_keyboard)
            return

        if text == "📡 حالة Copy":
            await update.message.reply_text(
                build_copy_status_message(),
                parse_mode="HTML",
                reply_markup=copy_admin_keyboard
            )
            return

        if text in {"🔑 كود أسبوع", "🔑 كود شهر", "🔑 كود دائم"}:
            plan = "week" if "أسبوع" in text else ("forever" if "دائم" in text else "month")
            try:
                record = create_copy_license(plan=plan, created_by=user.id, max_devices=COPY_LICENSE_DEFAULT_MAX_DEVICES)
                await update.message.reply_text(
                    build_copy_license_message(record),
                    parse_mode="HTML",
                    reply_markup=copy_admin_keyboard
                )
            except Exception as e:
                logger.exception("Could not create copy license: %s", e)
                await update.message.reply_text("❌ تعذر إنشاء كود التفعيل. راجع لوج Render.", reply_markup=copy_admin_keyboard)
            return

        if text == "📋 أكواد Copy":
            await update.message.reply_text(
                build_copy_licenses_list_message(20),
                parse_mode="HTML",
                reply_markup=copy_admin_keyboard
            )
            return

        if text == "♻️ تصفير جهاز كود":
            context.user_data["step"] = "copy_reset_device_waiting_token"
            await update.message.reply_text(
                "♻️ أرسل كود Copy الذي تريد تصفير الأجهزة المرتبطة به.\n\nبعد التصفير، أول جهاز و Telegram ID يفتح الإضافة بهذا الكود سيرتبط من جديد.",
                reply_markup=copy_admin_keyboard
            )
            return

        if text == "♻️ تصفير كل الأجهزة":
            context.user_data["step"] = "copy_reset_all_devices_confirm"
            await update.message.reply_text(
                "⚠️ هذا الخيار سيصفر الأجهزة و Telegram ID لكل الأكواد المنشأة من البوت.\n\n"
                "استخدمه بعد تحديثات الإضافة أو عندما تريد إعادة ربط الأكواد من جديد.\n\n"
                "أرسل: نعم\nللتأكيد، أو أي كلمة أخرى للإلغاء.",
                reply_markup=copy_admin_keyboard
            )
            return

        if text == "🧹 تنظيف الأكواد":
            context.user_data["step"] = "copy_cleanup_codes_confirm"
            await update.message.reply_text(
                "🧹 هذا الخيار سيحذف فقط الأكواد المعطلة أو المنتهية من Firebase.\n"
                "الأكواد النشطة لن تُحذف.\n\n"
                "أرسل: نعم\nللتأكيد، أو أي كلمة أخرى للإلغاء.",
                reply_markup=copy_admin_keyboard
            )
            return

        if text == "🗑 حذف كود":
            context.user_data["step"] = "copy_delete_waiting_token"
            await update.message.reply_text(
                "🗑 أرسل كود Copy الذي تريد حذفه نهائيًا من Firebase.\n\n"
                "ملاحظة: كود DEMO-111 أو أي كود موجود في Render Env لا يمكن حذفه من هنا؛ احذفه من COPY_LICENSES إذا أردت.",
                reply_markup=copy_admin_keyboard
            )
            return

        if text == "📌 رسالة تحديث":
            context.user_data["step"] = "copy_update_notice_waiting_text"
            await update.message.reply_text(
                "📌 أرسل رسالة التحديث التي تريد أن تظهر داخل الإضافة.\n\nأرسل كلمة: مسح\nلحذف رسالة التحديث الحالية.",
                reply_markup=copy_admin_keyboard
            )
            return

        if text == "⛔ إيقاف كود":
            context.user_data["step"] = "copy_disable_waiting_token"
            await update.message.reply_text(
                "⛔ أرسل كود Copy الذي تريد إيقافه.\n\nملاحظة: الأكواد الموجودة فقط في Render Env لا يمكن إيقافها من البوت، احذفها من COPY_LICENSES.",
                reply_markup=copy_admin_keyboard
            )
            return

        if step == "copy_reset_all_devices_confirm":
            context.user_data["step"] = None
            if str(text).strip().lower() in {"نعم", "yes", "y", "confirm", "تأكيد"}:
                result = reset_all_copy_license_devices()
                await update.message.reply_text(build_copy_reset_all_result_message(result), reply_markup=copy_admin_keyboard)
            else:
                await update.message.reply_text("تم إلغاء تصفير كل الأجهزة.", reply_markup=copy_admin_keyboard)
            return

        if step == "copy_cleanup_codes_confirm":
            context.user_data["step"] = None
            if str(text).strip().lower() in {"نعم", "yes", "y", "confirm", "تأكيد"}:
                result = cleanup_copy_licenses(delete_disabled=True, delete_expired=True)
                await update.message.reply_text(build_copy_cleanup_result_message(result), reply_markup=copy_admin_keyboard)
            else:
                await update.message.reply_text("تم إلغاء تنظيف الأكواد.", reply_markup=copy_admin_keyboard)
            return

        if step == "copy_delete_waiting_token":
            context.user_data["step"] = None
            token = normalize_copy_license_token(text)
            if delete_copy_license(token):
                await update.message.reply_text(f"🗑 تم حذف الكود نهائيًا:\n<code>{html.escape(token)}</code>", parse_mode="HTML", reply_markup=copy_admin_keyboard)
            else:
                await update.message.reply_text("❌ لم أستطع حذف الكود. تأكد أنه كود منشأ من البوت وليس من Render Env.", reply_markup=copy_admin_keyboard)
            return

        if step == "copy_disable_waiting_token":
            context.user_data["step"] = None
            token = normalize_copy_license_token(text)
            if disable_copy_license(token):
                await update.message.reply_text(f"✅ تم إيقاف الكود:\n<code>{html.escape(token)}</code>", parse_mode="HTML", reply_markup=copy_admin_keyboard)
            else:
                await update.message.reply_text("❌ لم أستطع إيقاف الكود. تأكد أنه كود منشأ من البوت وليس من Render Env.", reply_markup=copy_admin_keyboard)
            return

        if step == "copy_reset_device_waiting_token":
            context.user_data["step"] = None
            token = normalize_copy_license_token(text)
            if reset_copy_license_devices(token):
                await update.message.reply_text(f"✅ تم تصفير الأجهزة وربط Telegram للكود:\n<code>{html.escape(token)}</code>", parse_mode="HTML", reply_markup=copy_admin_keyboard)
            else:
                await update.message.reply_text("❌ لم أستطع تصفير الأجهزة. تأكد أن الكود منشأ من البوت وليس من Render Env.", reply_markup=copy_admin_keyboard)
            return

        if step == "copy_update_notice_waiting_text":
            context.user_data["step"] = None
            notice = "" if str(text).strip() == "مسح" else str(text).strip()
            if set_copy_update_notice(notice, user.id):
                msg = "✅ تم مسح رسالة التحديث." if not notice else "✅ تم حفظ رسالة التحديث للإضافة."
                await update.message.reply_text(msg, reply_markup=copy_admin_keyboard)
            else:
                await update.message.reply_text("❌ تعذر حفظ رسالة التحديث. راجع لوج Render.", reply_markup=copy_admin_keyboard)
            return


        if text == "🧠 OTC Edge Engine":
            reset_signal_state(context)
            await update.message.reply_text(
                build_otc_edge_menu_message(),
                reply_markup=admin_otc_edge_keyboard
            )
            return

        if text == "🔎 فحص السوق الآن":
            await update.message.reply_text("🔎 جاري فحص سلوك أزواج OTC الحية...")
            await update.message.reply_text(
                build_otc_edge_scan_message(),
                reply_markup=admin_otc_edge_keyboard
            )
            return

        if text == "🚀 مراقبة كل السوق":
            await update.message.reply_text(
                start_otc_edge_watcher(user.id, user.id, mode="all"),
                reply_markup=admin_otc_edge_keyboard
            )
            return

        if text == "🎯 مراقبة زوج محدد":
            context.user_data["step"] = "otc_edge_watch_waiting_pair"
            await update.message.reply_text(
                "🎯 أرسل الزوج الذي تريد مراقبته.\n\n"
                "يمكنك إرسال زوج واحد مثل:\nGBP/NZD (OTC)\n\n"
                "أو عدة أزواج كل زوج بسطر منفصل.\n"
                "وعندما يظهر Edge قوي على هذه الأزواج، البوت يرسل لك تنبيه دخول مباشر.",
                reply_markup=admin_otc_edge_keyboard
            )
            return

        if text == "🛑 إيقاف مراقبة Edge":
            await update.message.reply_text(
                stop_otc_edge_watcher(),
                reply_markup=admin_otc_edge_keyboard
            )
            return

        if text == "📋 حالة مراقبة Edge":
            await update.message.reply_text(
                build_otc_edge_watcher_status_message(),
                reply_markup=admin_otc_edge_keyboard
            )
            return

        if step == "otc_edge_watch_waiting_pair":
            pairs, errors = parse_otc_edge_watch_pairs(text)
            context.user_data["step"] = None
            if not pairs:
                err_text = "\n".join(f"• {x}" for x in errors[:8]) if errors else "لم أستطع قراءة الزوج."
                await update.message.reply_text(
                    "❌ لم يتم تشغيل المراقبة.\n"
                    f"الأزواج غير المفهومة:\n{err_text}\n\n"
                    "أرسل مثالًا بهذا الشكل:\nGBP/NZD (OTC)",
                    reply_markup=admin_otc_edge_keyboard
                )
                return
            msg = start_otc_edge_watcher(user.id, user.id, mode="pairs", pairs=pairs)
            if errors:
                msg += "\n\n⚠️ تم تجاهل بعض المدخلات غير المفهومة:\n" + "\n".join(f"• {x}" for x in errors[:6])
            await update.message.reply_text(msg, reply_markup=admin_otc_edge_keyboard)
            return

        if text == "📊 تقرير الأنماط":
            await update.message.reply_text(
                build_otc_edge_patterns_report(),
                reply_markup=admin_otc_edge_keyboard
            )
            return

        if text == "🧪 فحص زوج محدد":
            context.user_data["step"] = "otc_edge_waiting_pair"
            await update.message.reply_text(
                "🧪 أرسل اسم الزوج لفحص Edge عليه، مثال:\nUSD/BRL (OTC)\n\nأو أرسل الرمز الداخلي مثل: BRLUSD_otc",
                reply_markup=admin_otc_edge_keyboard
            )
            return

        if step == "otc_edge_waiting_pair":
            context.user_data["step"] = None
            await update.message.reply_text(
                build_otc_edge_single_pair_message(text),
                reply_markup=admin_otc_edge_keyboard
            )
            return


        if text == "📡 قناة 3 شموع":
            reset_signal_state(context)
            await update.message.reply_text(
                "📡 قناة 3 شموع - Beta\nاختر الإجراء المطلوب:",
                reply_markup=three_candle_admin_keyboard
            )
            return

        if text == "🟢 تشغيل نشر القناة":
            ok = _three_candle_set_enabled(True)
            await update.message.reply_text(
                "✅ تم تشغيل نشر قناة 3 شموع" if ok else "❌ تعذر تشغيل النشر. راجع اللوج.",
                reply_markup=three_candle_admin_keyboard
            )
            return

        if text == "🔴 إيقاف نشر القناة":
            ok = _three_candle_set_enabled(False)
            await update.message.reply_text(
                "⛔ تم إيقاف نشر قناة 3 شموع" if ok else "❌ تعذر إيقاف النشر. راجع اللوج.",
                reply_markup=three_candle_admin_keyboard
            )
            return

        if text == "🎯 حد صفقات اليوم":
            context.user_data["step"] = "three_candle_waiting_daily_limit"
            await update.message.reply_text(
                "🎯 أرسل عدد الصفقات التي تريد نشرها خلال اليوم.\nمثال: 10\n\nأرسل 0 لجعل النشر مفتوح.",
                reply_markup=three_candle_admin_keyboard
            )
            return

        if step == "three_candle_waiting_daily_limit":
            context.user_data["step"] = None
            value = safe_int(text, -1)
            if value < 0:
                await update.message.reply_text("❌ أرسل رقم صحيح، مثال: 10 أو 0 للنشر المفتوح.", reply_markup=three_candle_admin_keyboard)
                return
            ok = _three_candle_set_daily_limit(value)
            label = "مفتوح ♾" if value == 0 else str(value)
            await update.message.reply_text(
                f"✅ تم ضبط حد صفقات اليوم: {label}" if ok else "❌ تعذر حفظ الحد. راجع اللوج.",
                reply_markup=three_candle_admin_keyboard
            )
            return

        if text == "♾ نشر مفتوح":
            ok = _three_candle_set_daily_limit(0)
            await update.message.reply_text(
                "✅ تم جعل النشر مفتوح بدون حد يومي" if ok else "❌ تعذر حفظ الإعداد. راجع اللوج.",
                reply_markup=three_candle_admin_keyboard
            )
            return

        if text == "📊 ملخص القناة":
            context.user_data["step"] = "three_candle_waiting_summary_count"
            await update.message.reply_text(
                "📊 أرسل عدد النتائج التي تريد تلخيصها، مثال: 50\nأو أرسل: الكل\nلعرض الملخص من بداية القناة.",
                reply_markup=three_candle_admin_keyboard
            )
            return

        if step == "three_candle_waiting_summary_count":
            context.user_data["step"] = None
            raw = str(text or "").strip().lower()
            if raw in {"الكل", "كل", "all", "0"}:
                await update.message.reply_text(build_three_candle_channel_summary(None), reply_markup=three_candle_admin_keyboard)
                return
            count = safe_int(raw, -1)
            if count <= 0:
                await update.message.reply_text("❌ أرسل رقم صحيح أو كلمة: الكل", reply_markup=three_candle_admin_keyboard)
                return
            await update.message.reply_text(build_three_candle_channel_summary(count), reply_markup=three_candle_admin_keyboard)
            return

        if text == "📋 حالة القناة":
            await update.message.reply_text(build_three_candle_channel_status(), reply_markup=three_candle_admin_keyboard)
            return

        if text == "📊 إحصائيات البوت":
            await update.message.reply_text(
                build_bot_stats_message(),
                parse_mode="HTML",
                reply_markup=admin_main_keyboard
            )
            return

        if text == "📤 تصدير المستخدمين":
            try:
                csv_bytes = build_users_export_csv_bytes()
                bio = io.BytesIO(csv_bytes)
                bio.name = f"trading_time_users_{get_signal_day_key()}.csv"
                await update.message.reply_document(
                    document=bio,
                    filename=bio.name,
                    caption="📤 نسخة احتياطية من المستخدمين وحالة الاستخدام.",
                    reply_markup=admin_main_keyboard
                )
            except Exception as e:
                logger.exception("Users export failed: %s", e)
                await update.message.reply_text("❌ تعذر تصدير المستخدمين. راجع اللوج.", reply_markup=admin_main_keyboard)
            return

        if text == "📡 قنوات البوت":
            await update.message.reply_text(
                "ℹ️ تم حذف قسم قنوات النشر التلقائي من البوت.\n"
                "الخيارات الحالية موجودة في لوحة الأدمن الرئيسية.",
                reply_markup=admin_main_keyboard
            )
            return


        if is_admin(user.id):
            if text == "🩺 حالة OTC Live":
                await update.message.reply_text("⛔ قناة OTC Live التلقائية ملغاة من الملف. بث Quotex قد يبقى مستخدمًا للأقسام اليدوية فقط.", reply_markup=admin_otc_stats_keyboard)
                return

            if text == "📈 إحصائيات OTC مباشر":
                await update.message.reply_text("⛔ إحصائيات قناة OTC Live التلقائية ملغاة لأن القناة أُزيلت من النشر.", reply_markup=admin_otc_stats_keyboard)
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

            if text == "🌐 أزواج OTC الديناميكية":
                await update.message.reply_text(
                    format_otc_dynamic_universe_status(),
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
                    recovered_text, _meta = recover_otc_list_result_now(user.id)
                    if recovered_text:
                        result_text = recovered_text
                result_text = prettify_existing_otc_result_text(result_text) if result_text else result_text

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
                    normalize_pretty_otc_result_for_telegram(result_text),
                    reply_markup=admin_otc_stats_keyboard,
                    parse_mode="HTML"
                )
                return


        if is_otc_list_manager(user.id) and text == "📋 عرض نتائج الليستة":
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
                normalize_pretty_otc_result_for_telegram(result_text),
                reply_markup=admin_otc_stats_keyboard,
                parse_mode="HTML"
            )
            return

        if text == "📥 الطلبات المعلقة":
            data = get_all_pending_users()
            if not data:
                await update.message.reply_text("📭 لا يوجد طلبات معلقة حاليًا.", reply_markup=admin_main_keyboard)
                return

            await update.message.reply_text("📥 الطلبات المعلقة 👇", reply_markup=admin_main_keyboard)

            for _, item in data.items():
                username = f"@{item.get('username')}" if item.get("username") else "بدون username"
                target_id = int(item.get("telegram_id"))
                msg = (
                    "📥 طلب تفعيل\n\n"
                    f"👤 الاسم: {item.get('name')}\n"
                    f"🔗 اليوزر: {username}\n"
                    f"🆔 Telegram ID: <code>{target_id}</code>\n"
                    f"💱 Quotex ID: <code>{item.get('quotex_id')}</code>\n"
                    f"⏰ تاريخ الطلب: {format_dt_ar(item.get('created_at', ''))}\n\n"
                    "اختر الإجراء المناسب من الأزرار 👇"
                )

                await update.message.reply_text(
                    msg,
                    parse_mode="HTML",
                    reply_markup=build_pending_request_keyboard(target_id)
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

                user_data = all_users.get(user_id_str, {}) or {}
                approved_data = approved_users.get(user_id_str, {}) or {}
                merged = dict(user_data)
                merged.update({k: v for k, v in approved_data.items() if k not in merged or merged.get(k) in {None, "", "غير موجود"}})
                approved_active_list.append((user_id_str, merged))

            if not approved_active_list:
                await update.message.reply_text("📭 لا يوجد مستخدمون مفعّلون حاليًا.", reply_markup=admin_main_keyboard)
                return

            await update.message.reply_text(f"📋 كافة المستخدمين المفعّلين: {len(approved_active_list)}", reply_markup=admin_main_keyboard)

            for user_id_str, data in approved_active_list[:50]:
                name = data.get("name", "غير معروف")
                username = f"@{data.get('username')}" if data.get("username") else "بدون username"
                quotex_id = data.get("quotex_id", "غير موجود")
                expires_at = data.get("expires_at", "غير محدد")
                expires_text = "دائم" if expires_at == "forever" else format_dt_ar(expires_at)

                msg = (
                    f"👤 {name} | {username}\n"
                    f"🆔 Telegram ID: <code>{user_id_str}</code>\n"
                    f"💱 Quotex ID: <code>{quotex_id}</code>\n"
                    f"📌 الحالة: approved\n"
                    f"⏳ الصلاحية: {expires_text}\n"
                    "──────────────"
                )

                await update.message.reply_text(
                    msg,
                    parse_mode="HTML",
                    reply_markup=build_user_admin_inline_keyboard(int(user_id_str))
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
                    await safe_send_message(context.bot,
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
                    await safe_send_message(context.bot,
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
                    await safe_send_message(context.bot,
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
                    await send_revoked_welcome_keyboard(context, target_id)
                except Exception:
                    pass
                context.user_data["step"] = None
                return

            if text == "💬 إرسال رسالة":
                context.user_data["admin_message_target_id"] = target_id
                context.user_data["step"] = "admin_direct_message_waiting"
                await update.message.reply_text(
                    "💬 اكتب الآن الرسالة التي تريد إرسالها لهذا المستخدم.",
                    reply_markup=admin_main_keyboard
                )
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

        allowed, limit_msg = check_signal_usage_allowed(user.id, count)
        if not allowed:
            await update.message.reply_text(limit_msg, reply_markup=build_main_menu_for_user(user.id))
            reset_signal_state(context)
            return

        signals = generate_signals(pair, count, interval_minutes, start_dt)
        message_text = build_signals_message(pair, count, interval_minutes, signals)

        await update.message.reply_text(
            message_text,
            reply_markup=build_main_menu_for_user(user.id),
            parse_mode="Markdown"
        )
        record_signal_usage(user.id, count, "otc_timed")
        await publish_copy_timed_list_signals(pair, signals, interval_minutes, start_dt, source="timed_list", creator_user_id=user.id)

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

        allowed, limit_msg = check_signal_usage_allowed(user.id, 1)
        if not allowed:
            await update.message.reply_text(limit_msg, reply_markup=build_main_menu_for_user(user.id))
            reset_signal_state(context)
            return

        await update.message.reply_text("🔎 جاري فحص أزواج OTC الحية على فريم الدقيقة...")
        result = analyze_best_live_otc_now()

        await update.message.reply_text(
            result["message"],
            reply_markup=build_main_menu_for_user(user.id),
            parse_mode="Markdown"
        )
        if result.get("ok"):
            record_signal_usage(user.id, 1, "otc_live_now")
            await maybe_publish_copy_signal(result, source="otc_live", enabled=COPY_SEND_OTC_LIVE_NOW, creator_user_id=user.id)

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

        allowed, limit_msg = check_signal_usage_allowed(user.id, 1)
        if not allowed:
            await update.message.reply_text(limit_msg, reply_markup=build_main_menu_for_user(user.id))
            reset_signal_state(context)
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
        if result.get("ok"):
            record_signal_usage(user.id, 1, "real_market")
            await maybe_publish_copy_signal(result, source="real_market", enabled=COPY_SEND_REAL_MARKET, creator_user_id=user.id)

        reset_signal_state(context)
        return

    await update.message.reply_text(
        "📌 اختر خيارًا من القائمة.",
        reply_markup=build_main_menu_for_user(user.id)
    )




# ===== Embedded TRADING TIME COPY SERVER =====
# هذا السيرفر يستقبل الصفقات من البوت ويبثها لإضافات Chrome المتصلة على نفس رابط Render.
_copy_server_started = False
_copy_clients = {}
_copy_signal_history = []
_copy_client_events = []


def _copy_load_licenses() -> dict:
    # Backward compatibility for older server code. v0.23 uses Firebase-backed
    # validation through copy_validate_license_for_device().
    return copy_license_env_records()


def _copy_server_parse_iso(value):
    try:
        if not value:
            return None
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return None


def _copy_server_normalize_direction(direction: str) -> str:
    d = str(direction or "").strip().upper()
    if d in {"CALL", "BUY", "UP", "🟢 CALL"}:
        return "CALL"
    if d in {"PUT", "SELL", "DOWN", "🔴 PUT"}:
        return "PUT"
    raise ValueError("direction must be CALL or PUT")


def _copy_server_normalize_timeframe(value) -> str:
    if value is None:
        return "M1"
    text = str(value).strip().upper()
    if text.isdigit():
        return f"M{text}"
    if text.startswith("M") and text[1:].isdigit():
        return text
    return text or "M1"


def _copy_server_duration_seconds(data: dict) -> int:
    try:
        if data.get("duration_seconds"):
            return max(5, int(float(data.get("duration_seconds"))))
        if data.get("duration_minutes"):
            return max(5, int(float(data.get("duration_minutes")) * 60))
        tf = _copy_server_normalize_timeframe(data.get("timeframe"))
        if tf.startswith("M") and tf[1:].isdigit():
            return max(5, int(tf[1:]) * 60)
    except Exception:
        pass
    return 60


def _copy_server_make_signal_id(payload: dict) -> str:
    base = "|".join([
        str(payload.get("source", "bot")),
        str(payload.get("pair") or payload.get("pair_display") or ""),
        str(payload.get("platform_symbol") or ""),
        str(payload.get("direction") or ""),
        str(payload.get("entry_time") or ""),
        str(payload.get("created_at") or ""),
    ])
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:18]


def _copy_server_sanitize_signal(data: dict) -> dict:
    payload = dict(data or {})
    direction = _copy_server_normalize_direction(payload.get("direction"))
    timeframe = _copy_server_normalize_timeframe(payload.get("timeframe") or payload.get("duration_minutes") or "M1")
    duration_seconds = _copy_server_duration_seconds(payload)

    entry_dt = _copy_server_parse_iso(payload.get("entry_time") or payload.get("entry_time_iso"))
    created_dt = _copy_server_parse_iso(payload.get("created_at")) or datetime.now(UTC)
    if not entry_dt:
        entry_dt = created_dt

    expires_dt = _copy_server_parse_iso(payload.get("expires_at") or payload.get("valid_until"))
    if not expires_dt:
        expires_dt = entry_dt + timedelta(seconds=int(COPY_SIGNAL_VALIDITY_SECONDS))

    pair_display = str(payload.get("pair_display") or payload.get("pair") or "").strip()
    platform_symbol = str(payload.get("platform_symbol") or payload.get("symbol") or payload.get("pair") or "").strip()
    if not pair_display and not platform_symbol:
        raise ValueError("pair or platform_symbol is required")

    return {
        "type": "signal",
        "id": str(payload.get("id") or _copy_server_make_signal_id(payload)),
        "source": str(payload.get("source") or "bot"),
        "mode": str(payload.get("mode") or "copy"),
        "pair": pair_display or platform_symbol,
        "pair_display": pair_display or platform_symbol,
        "platform_symbol": platform_symbol or pair_display,
        "direction": direction,
        "timeframe": timeframe,
        "duration_seconds": duration_seconds,
        "entry_time": entry_dt.isoformat(),
        "expires_at": expires_dt.isoformat(),
        "created_at": created_dt.isoformat(),
        "quality": payload.get("quality"),
        "confidence": payload.get("confidence"),
        "entry_price": payload.get("entry_price"),
        "payout": payload.get("payout"),
        "note": str(payload.get("note") or "")[:500],
        "creator_user_id": normalize_copy_telegram_user_id(payload.get("creator_user_id") or payload.get("user_id")),
        "target_user_id": normalize_copy_telegram_user_id(payload.get("target_user_id") or payload.get("telegram_user_id")),
        "scope": "user" if normalize_copy_telegram_user_id(payload.get("target_user_id") or payload.get("telegram_user_id")) else "broadcast",
        "batch_id": payload.get("batch_id") or payload.get("list_batch_id") or payload.get("timed_list_batch_id"),
        "timed_list_batch_id": payload.get("timed_list_batch_id") or payload.get("list_batch_id") or payload.get("batch_id"),
        "list_index": payload.get("list_index"),
        "list_total": payload.get("list_total"),
    }


def _copy_server_check_secret(secret: str | None):
    if not secret or not hmac.compare_digest(str(secret), str(COPY_SERVER_SECRET)):
        raise ValueError("invalid server secret")


async def _copy_send_json_safe(ws, payload: dict) -> bool:
    try:
        await ws.send_json(payload)
        return True
    except Exception:
        return False


async def _copy_broadcast_signal(signal: dict) -> dict:
    if not is_copy_global_enabled():
        return {
            "online_clients": len(_copy_clients),
            "delivered": 0,
            "dead_removed": 0,
            "global_enabled": False,
            "reason": "copy trading stopped by admin",
        }

    target_user_id = normalize_copy_telegram_user_id((signal or {}).get("target_user_id"))
    delivered = 0
    skipped_scope = 0
    dead = []
    for client_id, client in list(_copy_clients.items()):
        client_user_id = normalize_copy_telegram_user_id(client.get("telegram_user_id"))
        if target_user_id and client_user_id != target_user_id:
            skipped_scope += 1
            continue
        ws = client.get("ws")
        ok = await _copy_send_json_safe(ws, {"type": "signal", "signal": signal})
        if ok:
            delivered += 1
            client["last_sent_at"] = now_iso()
        else:
            dead.append(client_id)
    for client_id in dead:
        _copy_clients.pop(client_id, None)
    return {
        "online_clients": len(_copy_clients),
        "delivered": delivered,
        "dead_removed": len(dead),
        "skipped_scope": skipped_scope,
        "global_enabled": True,
        "target_user_id": target_user_id or None,
        "scope": "user" if target_user_id else "broadcast",
    }

def create_embedded_copy_api():
    from fastapi import FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
    from fastapi.middleware.cors import CORSMiddleware

    copy_api = FastAPI(title="TRADING TIME COPY EMBEDDED SERVER", version="0.42.0")
    copy_api.add_middleware(
        CORSMiddleware,
        allow_origins=COPY_ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


    @copy_api.get("/health")
    async def copy_health():
        return {
            "ok": True,
            "app": "TRADING TIME COPY EMBEDDED SERVER",
            "version": "0.26.0",
            "time": now_iso(),
            "copy_settings": copy_public_settings_payload(),
            "online_clients": len(_copy_clients),
            "bot": "telegram-bot",
        }

    @copy_api.get("/copy/health")
    async def copy_health_alias():
        return await copy_health()

    @copy_api.get("/api/license/check")
    async def copy_license_check(token: str = Query(...), device_id: str = Query(default="unknown"), telegram_user_id: str = Query(default="")):
        ok, reason, record = copy_validate_license_for_device(token, device_id, telegram_user_id=telegram_user_id, touch=True)
        if not ok:
            raise HTTPException(status_code=401, detail=reason)
        return {
            "ok": True,
            "status": "active",
            "token": normalize_copy_license_token(token),
            "plan": (record or {}).get("plan"),
            "expires_at": (record or {}).get("expires_at"),
            "max_devices": (record or {}).get("max_devices"),
            "telegram_user_id": normalize_copy_telegram_user_id((record or {}).get("telegram_user_id") or telegram_user_id),
            "copy_settings": copy_public_settings_payload(),
        }

    @copy_api.get("/api/admin/status")
    async def copy_admin_status(x_ttcopy_secret: str | None = Header(default=None)):
        try:
            _copy_server_check_secret(x_ttcopy_secret)
        except ValueError as e:
            raise HTTPException(status_code=401, detail=str(e))
        return {
            "online_clients": len(_copy_clients),
            "clients": [
                {
                    "client_id": k,
                    "license": v.get("license"),
                    "device_id": v.get("device_id"),
                    "telegram_user_id": v.get("telegram_user_id"),
                    "connected_at": v.get("connected_at"),
                    "last_seen_at": v.get("last_seen_at"),
                    "last_sent_at": v.get("last_sent_at"),
                }
                for k, v in _copy_clients.items()
            ],
            "last_signals": _copy_signal_history[-20:],
        }

    @copy_api.post("/api/bot/signal")
    async def copy_bot_signal(request: Request, x_ttcopy_secret: str | None = Header(default=None)):
        try:
            _copy_server_check_secret(x_ttcopy_secret)
            body = await request.json()
            normalized = _copy_server_sanitize_signal(body)
        except ValueError as e:
            msg = str(e)
            code = 401 if "secret" in msg else 400
            raise HTTPException(status_code=code, detail=msg)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"invalid payload: {e}")

        normalized["received_at"] = now_iso()
        normalized["origin"] = "bot"
        _copy_signal_history.append(normalized)
        del _copy_signal_history[:-int(COPY_SIGNAL_HISTORY_LIMIT)]
        delivery = await _copy_broadcast_signal(normalized)
        return {"ok": True, "signal": normalized, "delivery": delivery}

    @copy_api.post("/api/admin/manual-signal")
    async def copy_manual_signal(request: Request, x_ttcopy_secret: str | None = Header(default=None)):
        try:
            _copy_server_check_secret(x_ttcopy_secret)
            body = await request.json()
            body["source"] = body.get("source") or "admin_manual"
            normalized = _copy_server_sanitize_signal(body)
        except ValueError as e:
            msg = str(e)
            code = 401 if "secret" in msg else 400
            raise HTTPException(status_code=code, detail=msg)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"invalid payload: {e}")

        normalized["received_at"] = now_iso()
        normalized["origin"] = "admin_manual"
        _copy_signal_history.append(normalized)
        del _copy_signal_history[:-int(COPY_SIGNAL_HISTORY_LIMIT)]
        delivery = await _copy_broadcast_signal(normalized)
        return {"ok": True, "signal": normalized, "delivery": delivery}

    @copy_api.websocket("/ws/extension")
    async def copy_extension_ws(websocket: WebSocket, token: str = Query(...), device_id: str = Query(default="unknown"), telegram_user_id: str = Query(default="")):
        await websocket.accept()
        token = normalize_copy_license_token(token)
        telegram_user_id = normalize_copy_telegram_user_id(telegram_user_id)
        ok, reason, license_record = copy_validate_license_for_device(token, device_id, telegram_user_id=telegram_user_id, touch=True)
        if not ok:
            await websocket.close(code=4401, reason=reason)
            return

        client_id = hashlib.sha256(f"{token}:{device_id}:{telegram_user_id}:{time_module.time()}".encode("utf-8")).hexdigest()[:16]
        _copy_clients[client_id] = {
            "ws": websocket,
            "license": token,
            "device_id": device_id,
            "telegram_user_id": telegram_user_id,
            "connected_at": now_iso(),
            "last_seen_at": now_iso(),
            "last_sent_at": None,
        }
        await _copy_send_json_safe(websocket, {
            "type": "hello",
            "ok": True,
            "client_id": client_id,
            "server_time": now_iso(),
            "message": "TRADING TIME COPY connected",
            "copy_settings": copy_public_settings_payload(),
            "license": {
                "token": token,
                "status": "active",
                "plan": (license_record or {}).get("plan"),
                "expires_at": (license_record or {}).get("expires_at"),
                "max_devices": (license_record or {}).get("max_devices"),
                "telegram_user_id": normalize_copy_telegram_user_id((license_record or {}).get("telegram_user_id") or telegram_user_id),
                "route_mode": "personal" if telegram_user_id else "broadcast_only_until_telegram_id_is_set",
            },
        })
        try:
            while True:
                raw = await websocket.receive_text()
                if client_id in _copy_clients:
                    _copy_clients[client_id]["last_seen_at"] = now_iso()
                ok, reason, _record = copy_validate_license_for_device(token, device_id, telegram_user_id=telegram_user_id, touch=True)
                if not ok:
                    await websocket.close(code=4401, reason=reason)
                    break
                try:
                    event = json.loads(raw)
                except Exception:
                    event = {"type": "raw", "raw": raw[:500]}
                event["client_id"] = client_id
                event["license"] = token
                event["server_received_at"] = now_iso()
                _copy_client_events.append(event)
                del _copy_client_events[:-int(COPY_SIGNAL_HISTORY_LIMIT)]

                if event.get("type") == "ping":
                    await _copy_send_json_safe(websocket, {
                        "type": "pong",
                        "server_time": now_iso(),
                        "copy_settings": copy_public_settings_payload(),
                    })
                elif event.get("type") == "ack":
                    await _copy_send_json_safe(websocket, {"type": "ack_saved", "signal_id": event.get("signal_id")})
                else:
                    await _copy_send_json_safe(websocket, {"type": "event_saved", "server_time": now_iso()})
        except WebSocketDisconnect:
            pass
        finally:
            _copy_clients.pop(client_id, None)

    return copy_api


def start_embedded_copy_server():
    """يشغل TRADING TIME COPY Server داخل نفس عملية البوت على Render."""
    global _copy_server_started
    if _copy_server_started or not COPY_EMBEDDED_SERVER_ENABLED:
        return
    try:
        import uvicorn
        port = int(os.getenv("PORT", "8080"))
        copy_api = create_embedded_copy_api()

        def _run():
            uvicorn.run(copy_api, host="0.0.0.0", port=port, log_level=str(COPY_UVICORN_LOG_LEVEL or "info"))

        thread = threading.Thread(target=_run, name="trading-time-copy-server", daemon=True)
        thread.start()
        _copy_server_started = True
        logger.warning("Embedded TRADING TIME COPY Server started on 0.0.0.0:%s", port)
    except Exception as e:
        logger.exception("Could not start embedded TRADING TIME COPY Server: %s", e)


# ===== App Runner =====

_last_admin_error_alert_at = 0.0


def should_send_admin_error_alert() -> bool:
    global _last_admin_error_alert_at
    try:
        now_ts = time_module.time()
        if now_ts - float(_last_admin_error_alert_at or 0) >= ADMIN_ERROR_ALERT_COOLDOWN_SECONDS:
            _last_admin_error_alert_at = now_ts
            return True
        return False
    except Exception:
        return True


async def telegram_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    # Ignore harmless Telegram edit errors caused by pressing the same button/menu again.
    try:
        if context.error and "Message is not modified" in str(context.error):
            logger.info("Ignored harmless Telegram error: %s", context.error)
            return
        if context.error and (context.error.__class__.__name__ == "TimedOut" or "Timed out" in str(context.error)):
            logger.warning("Ignored Telegram timeout error: %s", context.error)
            return
    except Exception:
        pass

    error_text = "".join(traceback.format_exception(None, context.error, context.error.__traceback__)) if context.error else ""
    logger.error(
        "Telegram handler error | update=%s | error=%s\n%s",
        update,
        context.error,
        error_text,
    )

    if should_send_admin_error_alert():
        try:
            msg = (
                "⚠️ Bot Error Alert\n\n"
                f"الخطأ: {html.escape(str(context.error))}\n\n"
                f"تفاصيل مختصرة:\n<code>{html.escape(error_text[-2500:] if error_text else 'no traceback')}</code>"
            )
            await safe_send_message(context.bot,
                chat_id=ADMIN_TELEGRAM_ID,
                text=msg[:3900],
                parse_mode="HTML"
            )
        except Exception:
            pass

def run_telegram_bot_only():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN غير موجود داخل ملف .env")

    # في نسخة Web Service، FastAPI/Uvicorn هو السيرفر الأساسي، لذلك لا نشغل سيرفر مدمج داخل thread هنا.

    app = Application.builder().token(BOT_TOKEN).build()

    # تشغيل بث Quotex OTC الحقيقي بالخلفية
    start_quotex_otc_feed()
    # Auto publish global market disabled: global channel removed from bot.

    # قناة OTC Live التلقائية ملغاة بالكامل: لا جدولة نشر تلقائي ولا تنبيهات قناة.
    logger.info("OTC Live auto publish channel is removed/disabled by owner request.")

    job_queue = app.job_queue
    # مراقبة OTC Edge خاصة بالأدمن فقط. لا ترسل للمستخدمين ولا تنشر على أي قناة.
    job_queue.run_repeating(
        otc_edge_watcher_job,
        interval=OTC_EDGE_WATCHER_SCAN_SECONDS,
        first=OTC_EDGE_WATCHER_SCAN_SECONDS,
        name="admin_otc_edge_watcher",
    )

    # قناة اختبار استراتيجية 3 شموع متتالية. تعمل فقط عند ضبط THREE_CANDLE_CHANNEL_ID وتفعيلها من env.
    job_queue.run_repeating(
        three_candle_channel_job,
        interval=THREE_CANDLE_SCAN_SECONDS,
        first=10,
        name="three_candle_channel_strategy",
    )

    if FIREBASE_24H_MONITOR_ENABLED:
        job_queue.run_repeating(
            send_firebase_24h_report_job,
            interval=FIREBASE_24H_REPORT_SECONDS,
            first=FIREBASE_24H_REPORT_SECONDS,
            name="firebase_24h_usage_report",
        )

    if FIREBASE_READ_DIAGNOSTICS_ENABLED:
        job_queue.run_repeating(
            firebase_diagnostics_report_job,
            interval=FIREBASE_READ_REPORT_SECONDS,
            first=FIREBASE_READ_REPORT_SECONDS,
            name="firebase_diagnostics_report",
        )


    # كل أنواع النشر التلقائي ملغاة بناءً على طلب الأدمن.
    # لا يتم جدولة OTC الزمني ولا OTC Live ولا Global.
    logger.info("All auto publishing jobs are disabled by owner request.")


    # ملخص OTC Live اليومي ملغى لأن قناة النشر التلقائي أُلغيت.

    install_firebase_diagnostics()

    install_firebase_24h_monitor()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_admin_buttons))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(telegram_error_handler)

    logger.info("Bot is running...")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    app.run_polling(drop_pending_updates=True, close_loop=False, stop_signals=None)



# ===== Render Web Service entrypoint =====
# هذا هو التطبيق الذي يجب أن يراه Render كـ Web Service حتى يفتح بورت فورًا.
# استخدم Start Command: python main.py
render_app = create_embedded_copy_api()
_telegram_bot_thread_started = False


def _start_telegram_bot_background_once():
    global _telegram_bot_thread_started
    if _telegram_bot_thread_started:
        return
    _telegram_bot_thread_started = True

    def _runner():
        try:
            logger.warning("Starting Telegram bot polling in background thread for Render Web Service...")
            run_telegram_bot_only()
        except Exception as e:
            logger.exception("Telegram bot background thread crashed: %s", e)

    thread = threading.Thread(target=_runner, name="telegram-bot-polling", daemon=True)
    thread.start()


@render_app.on_event("startup")
async def _render_app_startup():
    _start_telegram_bot_background_once()
    logger.warning("TRADING TIME Web Service startup complete. Copy API is listening and bot thread is starting.")


if __name__ == "__main__":
    # Render Web Service يحتاج أن يفتح التطبيق بورت فعلي.
    # لذلك نشغل uvicorn كعملية أساسية، والبوت يعمل داخل background thread عبر startup event.
    import uvicorn

    port = int(os.getenv("PORT", "8080"))
    logger.warning("Starting Render Web Service on 0.0.0.0:%s", port)
    uvicorn.run(render_app, host="0.0.0.0", port=port, log_level=str(COPY_UVICORN_LOG_LEVEL or "info"))
