import os
import json
import hashlib
import asyncio
import requests
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

REAL_PAIRS = [
    "EUR/USD",
    "GBP/USD",
    "USD/JPY",
    "USD/CHF",
    "USD/CAD",
    "AUD/USD",
    "NZD/USD",
]

REAL_PAIR_TO_YAHOO_SYMBOL = {
    "EUR/USD": "EURUSD=X",
    "GBP/USD": "GBPUSD=X",
    "USD/JPY": "USDJPY=X",
    "USD/CHF": "USDCHF=X",
    "USD/CAD": "USDCAD=X",
    "AUD/USD": "AUDUSD=X",
    "NZD/USD": "NZDUSD=X",
}

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
}

TRADE_COUNTS = [3, 5, 10, 15, 20]
INTERVALS = [1, 3, 5]
REAL_INTERVALS = [1, 5, 10]
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
        ["NZD/USD"],
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
        "   📊 Quotex Signals - OTC\n"
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
        res = requests.get(
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

        if res.status_code != 200:
            return None, f"API status={res.status_code}"

        data = res.json()
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

    if support is not None and support_state in {"near", "approaching"} and trend_bias != "bearish":
        lines.append(f"📍 دعم قريب عند {format_price(pair, support)} — انتظر تأكيد ارتداد ثم CALL")

    if resistance is not None and resistance_state in {"near", "approaching"} and trend_bias != "bullish":
        lines.append(f"📍 مقاومة قريبة عند {format_price(pair, resistance)} — انتظر رفض سعري ثم PUT")

    for lvl in get_round_levels(price, pair):
        state, _ = classify_distance(price, lvl, atr, pair)
        if state in {"touch", "near"}:
            direction_hint = "CALL" if lvl <= price and trend_bias != "bearish" else "PUT"
            lines.append(f"🔢 Round Number قريب عند {format_price(pair, lvl)} — راقب {direction_hint}")
            break

    # إزالة التكرار لو اجتمع نفس المستوى أكثر من مرة
    uniq = []
    for line in lines:
        if line not in uniq:
            uniq.append(line)
    return uniq


def analyze_real_market(pair: str, timeframe_minutes: int):
    if not is_real_pair_available(pair):
        return {
            "ok": False,
            "quality": 0,
            "timeframe": timeframe_minutes,
            "message": f"❌ الزوج {pair} غير متاح الآن\n⏰ السوق مغلق"
        }

    candles, error_msg = get_candles(pair, timeframe_minutes=timeframe_minutes, limit=220)
    if not candles or len(candles) < 35:
        return {
            "ok": False,
            "quality": 0,
            "timeframe": timeframe_minutes,
            "message": f"❌ فشل جلب بيانات السوق\n\nالسبب: {error_msg}"
        }

    ema9 = calculate_ema(candles, 9)
    ema21 = calculate_ema(candles, 21)
    atr = calculate_atr(candles, 14)

    # دائمًا نقرأ آخر شمعة مغلقة بعد تجميع الفريم المطلوب، وليس الشمعة الجارية.
    last_closed = candles[-2]
    prev_closed = candles[-3]

    candle = analyze_candle(last_closed)
    prev_candle = analyze_candle(prev_closed)
    rejection = is_rejection_candle(last_closed)

    supports, resistances = find_levels(candles[:-1], atr)
    price = last_closed["close"]

    support = nearest_level(price, supports, "support")
    resistance = nearest_level(price, resistances, "resistance")

    sup_state, _ = classify_distance(price, support, atr, pair)
    res_state, _ = classify_distance(price, resistance, atr, pair)

    nearest_round = min(get_round_levels(price, pair), key=lambda lvl: abs(lvl - price))
    round_state, _ = classify_distance(price, nearest_round, atr, pair)

    score_call = 0
    score_put = 0
    notes = []
    warnings = []

    trend_bias = "neutral"

    # Trend
    if ema9[-2] > ema21[-2]:
        score_call += 3
        trend_bias = "bullish"
        notes.append("📈 الترند العام صاعد (EMA 9 فوق EMA 21)")
    elif ema9[-2] < ema21[-2]:
        score_put += 3
        trend_bias = "bearish"
        notes.append("📉 الترند العام هابط (EMA 9 تحت EMA 21)")

    # Momentum: نعطيه وزنًا أعلى لأنه مهم في الصفقات السريعة.
    if last_closed["close"] > prev_closed["close"]:
        score_call += 3
        notes.append("⚡ الزخم الأخير لصالح الصعود")
    elif last_closed["close"] < prev_closed["close"]:
        score_put += 3
        notes.append("⚡ الزخم الأخير لصالح الهبوط")

    # Strong candle / candle quality
    if candle["strong"]:
        if candle["bullish"]:
            score_call += 3
            notes.append("🟢 آخر شمعة مغلقة قوية صاعدة")
        elif candle["bearish"]:
            score_put += 3
            notes.append("🔴 آخر شمعة مغلقة قوية هابطة")

    # إذا كانت الشمعة الحالية دوجي لا نلغي الصفقة مباشرة، بل نخفض الجودة فقط.
    # السبب: الإلغاء الكامل جعل البوت يرفض السوق كثيرًا حتى في حالات الزخم الواضح.
    if candle["doji"]:
        warnings.append("⚠️ آخر شمعة مغلقة قريبة من الدوجي — يلزم تأكيد إضافي")
        score_call -= 1
        score_put -= 1

    if prev_candle["strong"]:
        if prev_candle["bullish"] and trend_bias == "bullish":
            score_call += 1
        elif prev_candle["bearish"] and trend_bias == "bearish":
            score_put += 1

    # Rejection + support/resistance
    if support is not None:
        if sup_state == "touch" and rejection == "bullish":
            score_call += 4
            notes.append(f"🔥 ارتداد واضح من دعم {format_price(pair, support)}")
        elif sup_state == "near" and candle["bullish"]:
            score_call += 2
            notes.append(f"📍 السعر قريب من دعم {format_price(pair, support)}")
        elif sup_state in {"approaching", "near"}:
            warnings.append(f"📍 دعم قريب عند {format_price(pair, support)} — راقب CALL عند ظهور رفض سعري")

    if resistance is not None:
        if res_state == "touch" and rejection == "bearish":
            score_put += 4
            notes.append(f"🔥 ارتداد واضح من مقاومة {format_price(pair, resistance)}")
        elif res_state == "near" and candle["bearish"]:
            score_put += 2
            notes.append(f"📍 السعر قريب من مقاومة {format_price(pair, resistance)}")
        elif res_state in {"approaching", "near"}:
            warnings.append(f"📍 مقاومة قريبة عند {format_price(pair, resistance)} — راقب PUT عند ظهور رفض سعري")

    # Round number logic
    if round_state == "touch":
        if candle["bullish"] and trend_bias != "bearish":
            score_call += 2
            notes.append(f"🔢 ارتكاز على Round Number {format_price(pair, nearest_round)}")
        elif candle["bearish"] and trend_bias != "bullish":
            score_put += 2
            notes.append(f"🔢 رفض من Round Number {format_price(pair, nearest_round)}")
        else:
            warnings.append(f"🔢 السعر يلامس Round Number {format_price(pair, nearest_round)} — انتظر تأكيد")
    elif round_state == "near":
        warnings.append(f"🔢 Round Number قريب عند {format_price(pair, nearest_round)}")

    if rejection == "bullish":
        score_call += 1
    elif rejection == "bearish":
        score_put += 1

    # فلترة التناقض بدون قتل الإشارة بالكامل
    if rejection == "bearish" and trend_bias == "bullish":
        score_call -= 1
        warnings.append("⚠️ يوجد رفض هابط مقابل الترند الصاعد")
    if rejection == "bullish" and trend_bias == "bearish":
        score_put -= 1
        warnings.append("⚠️ يوجد رفض صاعد مقابل الترند الهابط")

    score_call = max(score_call, 0)
    score_put = max(score_put, 0)

    confidence = 0
    direction = None
    best_score = max(score_call, score_put)
    score_gap = abs(score_call - score_put)

    # القوة هنا تعني توافق عدة إشارات جيدة، وليس مجرد تقليل الصفقات لأي ثمن.
    if score_call >= 7 and score_call >= score_put + 2:
        direction = "CALL 📈"
        confidence = min(58 + score_call * 4 + min(score_gap, 3) * 3, 95)
    elif score_put >= 7 and score_put >= score_call + 2:
        direction = "PUT 📉"
        confidence = min(58 + score_put * 4 + min(score_gap, 3) * 3, 95)
    elif best_score >= 6 and score_gap >= 3:
        if score_call > score_put:
            direction = "CALL 📈"
            confidence = min(54 + score_call * 4, 88)
        else:
            direction = "PUT 📉"
            confidence = min(54 + score_put * 4, 88)

    entry_time = now_utc() + timedelta(minutes=timeframe_minutes)
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

    if direction:
        return {
            "ok": True,
            "quality": quality,
            "timeframe": timeframe_minutes,
            "direction": direction,
            "confidence": confidence,
            "message": (
                header
                + f"📊 الثقة: {confidence}%\n\n"
                + f"📌 الإشارة: {direction}\n"
                + f"⏰ وقت الدخول: {format_utc_plus_3(entry_time)}\n"
                + f"💵 السعر الحالي: {format_price(pair, price)}\n"
                + f"⏳ مدة الصفقة المقترحة: {timeframe_minutes}M\n\n"
                + "\n".join(notes[:6])
                + (
                    "\n\n⚠️ تنبيهات إضافية:\n" + "\n".join(nearby_lines[:3])
                    if nearby_lines else ""
                )
            )
        }

    if nearby_lines:
        return {
            "ok": False,
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

    return {
        "ok": False,
        "quality": quality,
        "timeframe": timeframe_minutes,
        "message": (
            header
            + "❌ لا توجد فرصة واضحة الآن\n\n"
            + reason
        )
    }


def analyze_real_market_best(pair: str):
    results = [analyze_real_market(pair, tf) for tf in REAL_INTERVALS]
    successful = [r for r in results if r.get("ok")]

    if successful:
        best = max(successful, key=lambda x: (x.get("quality", 0), x.get("confidence", 0)))
        best["message"] += "\n\n🔥 تم اختيار هذه الصفقة لأنها الأقوى بين 1M / 5M / 10M"
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

    # ===== Common buttons =====
    if text == "🔙 رجوع":
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

        if step in {"choose_market_mode", "choose_real_pair", "choose_pair"}:
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
        if text == "⚡ OTC":
            context.user_data["mode"] = "otc"
            context.user_data["step"] = "choose_pair"
            await update.message.reply_text("💱 اختر زوج OTC 👇", reply_markup=otc_pairs_keyboard)
            return

        if text == "🌍 سوق عالمي":
            context.user_data["mode"] = "real"
            context.user_data["step"] = "choose_real_pair"
            await update.message.reply_text("🌍 اختر الزوج العالمي 👇", reply_markup=real_pairs_keyboard)
            return

        await update.message.reply_text("📌 اختر نوع السوق من الأزرار 👇", reply_markup=market_mode_keyboard)
        return

    # ===== OTC flow =====
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
        context.user_data["step"] = "choose_interval"
        await update.message.reply_text("⏳ اختر الفاصل الزمني / نوع الفرصة 👇", reply_markup=real_interval_keyboard)
        return

    if step == "choose_interval" and context.user_data.get("mode") == "otc":
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
def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN غير موجود داخل ملف .env")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_admin_buttons))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Bot is running...")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    app.run_polling(drop_pending_updates=True, close_loop=False)


if __name__ == "__main__":
    main()
