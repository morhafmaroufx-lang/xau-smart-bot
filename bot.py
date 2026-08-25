import os
import asyncio
import threading
import requests
import pandas as pd
import numpy as np

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from flask import Flask
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)


# =========================================================
# XAU SMART TRADER v12
# =========================================================
# النسخة الجديدة تشمل:
# 1. الدعوم والمقاومات اليومية
# 2. مراقبة الصفقات تلقائياً
# 3. قائمة Start مختصرة
# 4. تحليل عربي منظم
# 5. تنبيهات جلسات السوق بتوقيت دمشق
# =========================================================


# =========================================================
# CONFIG
# =========================================================

TOKEN = os.environ.get("TELEGRAM_TOKEN")

app = Flask(__name__)

SYMBOL = "XAUUSD"

DATA_URL = "https://biquote.io/api/XAUUSD/ohlc"

DAMASCUS_TZ = ZoneInfo("Asia/Damascus")


# =========================================================
# AUTO TRADE CONFIG
# =========================================================

AUTO_TRADE_ENABLED = True

CHECK_INTERVAL = 300  # 5 دقائق

SIGNAL_COOLDOWN_MINUTES = 60

MIN_TRADE_SCORE = 70

AUTO_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

last_auto_signal = None
last_auto_signal_time = None

monitor_task = None


# =========================================================
# MARKET SESSION CONFIG
# =========================================================

SESSION_ALERT_MINUTES = 15

market_alerts_sent = set()


# =========================================================
# DATA
# =========================================================

def get_bars(interval, limit=300):

    url = f"{DATA_URL}?interval={interval}&limit={limit}"

    response = requests.get(
        url,
        timeout=20
    )

    response.raise_for_status()

    data = response.json()

    bars = data.get("bars", [])

    if not bars:
        raise ValueError(
            f"لا توجد بيانات للفريم {interval}"
        )

    df = pd.DataFrame(bars)

    required = [
        "open",
        "high",
        "low",
        "close",
        "tickVolume"
    ]

    for column in required:

        if column not in df.columns:

            raise ValueError(
                f"البيانات ناقصة: {column}"
            )

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        )

    if "openTime" in df.columns:

        df["openTime"] = pd.to_datetime(
            df["openTime"],
            errors="coerce",
            utc=True
        )

    df = df.dropna(
        subset=required
    )

    if "openTime" in df.columns:

        df = df.dropna(
            subset=["openTime"]
        )

        df = df.sort_values(
            "openTime"
        )

    df = df.reset_index(
        drop=True
    )

    return df


# =========================================================
# WEEKLY FROM DAILY
# =========================================================

def build_weekly_from_daily(d1):

    if "openTime" not in d1.columns:

        raise ValueError(
            "تعذر قراءة تواريخ D1"
        )

    if d1["openTime"].isna().all():

        raise ValueError(
            "تعذر قراءة تواريخ D1"
        )

    df = d1.copy()

    df["date"] = pd.to_datetime(
        df["openTime"],
        errors="coerce",
        utc=True
    )

    df = df.dropna(
        subset=["date"]
    )

    if len(df) < 5:

        raise ValueError(
            "بيانات D1 غير كافية لبناء W1"
        )

    df = df.set_index("date")

    weekly = df.resample(
        "W-SUN",
        label="right",
        closed="right"
    ).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "tickVolume": "sum"
        }
    )

    weekly = weekly.dropna(
        subset=[
            "open",
            "high",
            "low",
            "close"
        ]
    )

    weekly = weekly.reset_index()

    weekly["openTime"] = weekly["date"]

    weekly = weekly.drop(
        columns=["date"]
    )

    return weekly.reset_index(
        drop=True
    )


# =========================================================
# INDICATORS
# =========================================================

def ema(series, period):

    return series.ewm(
        span=period,
        adjust=False
    ).mean()


def rsi(series, period=14):

    delta = series.diff()

    gain = delta.clip(
        lower=0
    )

    loss = -delta.clip(
        upper=0
    )

    avg_gain = gain.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    rs = avg_gain / avg_loss.replace(
        0,
        np.nan
    )

    result = 100 - (
        100 / (1 + rs)
    )

    return result.fillna(50)


def macd(
    series,
    fast=8,
    slow=21,
    signal=5
):

    fast_ema = ema(
        series,
        fast
    )

    slow_ema = ema(
        series,
        slow
    )

    line = fast_ema - slow_ema

    signal_line = ema(
        line,
        signal
    )

    histogram = (
        line - signal_line
    )

    return (
        line,
        signal_line,
        histogram
    )


def atr(
    high,
    low,
    close,
    period=14
):

    previous_close = close.shift(1)

    tr1 = high - low

    tr2 = (
        high - previous_close
    ).abs()

    tr3 = (
        low - previous_close
    ).abs()

    true_range = pd.concat(
        [
            tr1,
            tr2,
            tr3
        ],
        axis=1
    ).max(axis=1)

    return true_range.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()


# =========================================================
# SUPPORT / RESISTANCE
# =========================================================

def calculate_daily_levels(df):

    if len(df) < 3:

        raise ValueError(
            "بيانات D1 غير كافية لحساب الدعوم والمقاومات"
        )

    previous = df.iloc[-2]

    high = float(
        previous["high"]
    )

    low = float(
        previous["low"]
    )

    close = float(
        previous["close"]
    )

    pivot = (
        high + low + close
    ) / 3

    resistance_1 = (
        2 * pivot - low
    )

    resistance_2 = (
        pivot + high - low
    )

    support_1 = (
        2 * pivot - high
    )

    support_2 = (
        pivot - high + low
    )

    return {
        "pivot": pivot,
        "r1": resistance_1,
        "r2": resistance_2,
        "s1": support_1,
        "s2": support_2,
        "previous_high": high,
        "previous_low": low,
        "previous_close": close
    }


def levels_message(levels, current_price=None):

    text = (
        "📍 الدعوم والمقاومات اليومية\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
    )

    if current_price is not None:

        text += (
            f"💰 السعر الحالي: "
            f"{current_price:.2f}\n\n"
        )

    text += (
        f"🔴 المقاومة 2: {levels['r2']:.2f}\n"
        f"🔴 المقاومة 1: {levels['r1']:.2f}\n"
        f"🟡 المحور: {levels['pivot']:.2f}\n"
        f"🟢 الدعم 1: {levels['s1']:.2f}\n"
        f"🟢 الدعم 2: {levels['s2']:.2f}\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📊 مستويات مبنية على حركة اليوم السابق."
    )

    return text


# =========================================================
# TIME
# =========================================================

def damascus_now():

    return datetime.now(
        DAMASCUS_TZ
    )


def format_12h(dt):

    return dt.strftime(
        "%I:%M %p"
    ).lstrip("0")


def arabic_ampm(dt):

    hour = dt.hour

    minute = dt.minute

    if hour >= 12:

        period = "مساءً"

    else:

        period = "صباحًا"

    hour12 = hour % 12

    if hour12 == 0:

        hour12 = 12

    return (
        f"{hour12}:{minute:02d} "
        f"{period}"
    )


# =========================================================
# MARKET SESSIONS
# =========================================================

MARKET_SESSIONS = [
    {
        "name": "🇬🇧 الجلسة الأوروبية",
        "hour": 10,
        "minute": 0
    },
    {
        "name": "🇺🇸 الجلسة الأمريكية",
        "hour": 15,
        "minute": 30
    }
]


def market_session_message(session):

    now = damascus_now()

    return (
        "🌍 تنبيه السوق\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        f"{session['name']}\n\n"

        f"⏰ الموعد: "
        f"{arabic_ampm(now)}\n\n"

        "📊 توقع السوق:\n"
        "قد ترتفع السيولة وحركة الذهب "
        "مع افتتاح الجلسة.\n\n"

        "🎯 راقب الدعوم والمقاومات "
        "ولا تدخل قبل تأكيد الحركة.\n\n"

        "⚠️ هذا التنبيه لا يعني وجود صفقة."
    )


# =========================================================
# SAFE NUMBER
# =========================================================

def safe_float(value, default=0.0):

    try:

        value = float(value)

        if np.isnan(value):

            return default

        return value

    except Exception:

        return default
        # =========================================================
# ANALYSIS ENGINE
# =========================================================

def analyze_standard(df):

    close = df["close"]

    current = safe_float(
        close.iloc[-1]
    )

    ema20_series = ema(
        close,
        20
    )

    ema50_series = ema(
        close,
        50
    )

    ema200_series = ema(
        close,
        200
    )

    ema20 = safe_float(
        ema20_series.iloc[-1]
    )

    ema50 = safe_float(
        ema50_series.iloc[-1]
    )

    ema200 = safe_float(
        ema200_series.iloc[-1]
    )

    rsi_value = safe_float(
        rsi(
            close,
            14
        ).iloc[-1],
        50
    )

    macd_line, signal_line, histogram = macd(
        close,
        8,
        21,
        5
    )

    macd_value = safe_float(
        macd_line.iloc[-1]
    )

    signal_value = safe_float(
        signal_line.iloc[-1]
    )

    histogram_value = safe_float(
        histogram.iloc[-1]
    )

    atr_value = safe_float(
        atr(
            df["high"],
            df["low"],
            close,
            14
        ).iloc[-1]
    )

    volume = safe_float(
        df["tickVolume"].iloc[-1]
    )

    avg_volume = safe_float(
        df["tickVolume"]
        .tail(20)
        .mean()
    )

    volume_ratio = (
        volume / avg_volume
        if avg_volume > 0
        else 0
    )

    bullish = 0
    bearish = 0

    warnings = []

    # =====================================================
    # TREND
    # =====================================================

    if (
        current > ema20
        and ema20 > ema50
    ):

        bullish += 35

        trend = "🟢 صاعد"

    elif (
        current < ema20
        and ema20 < ema50
    ):

        bearish += 35

        trend = "🔴 هابط"

    else:

        trend = "🟡 متذبذب"

        if current > ema20:

            bullish += 15

        elif current < ema20:

            bearish += 15

    # =====================================================
    # EMA200
    # =====================================================

    if len(df) >= 200:

        if current > ema200:

            bullish += 15

        elif current < ema200:

            bearish += 15

    else:

        warnings.append(
            "بيانات EMA200 محدودة"
        )

    # =====================================================
    # RSI
    # =====================================================

    if 50 <= rsi_value < 70:

        bullish += 20

        rsi_state = "🟢 إيجابي"

    elif 30 < rsi_value < 50:

        bearish += 20

        rsi_state = "🔴 سلبي"

    elif rsi_value >= 70:

        rsi_state = "🟠 مرتفع"

        warnings.append(
            "RSI مرتفع"
        )

    else:

        rsi_state = "🟠 منخفض"

        warnings.append(
            "RSI منخفض"
        )

    # =====================================================
    # MACD
    # =====================================================

    if macd_value > signal_value:

        bullish += 20

        macd_state = "🟢 إيجابي"

    elif macd_value < signal_value:

        bearish += 20

        macd_state = "🔴 سلبي"

    else:

        macd_state = "🟡 محايد"

        warnings.append(
            "MACD محايد"
        )

    # =====================================================
    # VOLUME
    # =====================================================

    if volume_ratio >= 1.20:

        if bullish > bearish:

            bullish += 10

            volume_state = "🟢 قوي"

        elif bearish > bullish:

            bearish += 10

            volume_state = "🟢 قوي"

        else:

            volume_state = "🟡 مرتفع"

    elif volume_ratio >= 0.80:

        volume_state = "🟡 طبيعي"

    else:

        volume_state = "🟠 ضعيف"

        warnings.append(
            "السيولة ضعيفة"
        )

    # =====================================================
    # FINAL DIRECTION
    # =====================================================

    if bullish > bearish:

        direction = "🟢 BUY"

        score = bullish

    elif bearish > bullish:

        direction = "🔴 SELL"

        score = bearish

    else:

        direction = "🟡 WAIT"

        score = 0

        score = int(
        max(
            0,
            min(score, 100)
        )
        )
       # =========================================================
# SUPPORT & RESISTANCE
# =========================================================

def calculate_support_resistance(df, lookback=50):

    data = df.tail(lookback).copy()

    if len(data) < 10:
        raise ValueError(
            "بيانات غير كافية لحساب الدعوم والمقاومات."
        )

    current = safe_float(
        data["close"].iloc[-1]
    )

    highs = data["high"].astype(float)
    lows = data["low"].astype(float)

    # أقرب مقاومات فوق السعر
    resistance_candidates = sorted(
        [
            float(x)
            for x in highs
            if x > current
        ]
    )

    # أقرب دعوم تحت السعر
    support_candidates = sorted(
        [
            float(x)
            for x in lows
            if x < current
        ],
        reverse=True
    )

    # إذا لم نجد مستويات مناسبة نستخدم أعلى/أدنى نطاق
    if resistance_candidates:

        resistance_1 = resistance_candidates[0]

    else:

        resistance_1 = float(
            highs.max()
        )

    if len(resistance_candidates) > 1:

        resistance_2 = resistance_candidates[1]

    else:

        resistance_2 = float(
            highs.max()
        )

    if support_candidates:

        support_1 = support_candidates[0]

    else:

        support_1 = float(
            lows.min()
        )

    if len(support_candidates) > 1:

        support_2 = support_candidates[1]

    else:

        support_2 = float(
            lows.min()
        )

    # ترتيب منطقي
    resistance_levels = sorted(
        set(
            [
                resistance_1,
                resistance_2
            ]
        )
    )

    support_levels = sorted(
        set(
            [
                support_1,
                support_2
            ]
        ),
        reverse=True
    )

    return {
        "current": current,
        "support_1": support_levels[0],
        "support_2": support_levels[-1],
        "resistance_1": resistance_levels[0],
        "resistance_2": resistance_levels[-1]
    }


def format_support_resistance(sr):

    return (
        "🧱 الدعوم والمقاومات اليومية\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"💰 السعر الحالي: {sr['current']:.2f}\n\n"

        f"🟢 الدعم القريب: "
        f"{sr['support_1']:.2f}\n"

        f"🟢 الدعم التالي: "
        f"{sr['support_2']:.2f}\n\n"

        f"🔴 المقاومة القريبة: "
        f"{sr['resistance_1']:.2f}\n"

        f"🔴 المقاومة التالية: "
        f"{sr['resistance_2']:.2f}\n"
    )


# =========================================================
# MARKET OPEN TIMES
# Damascus Time - 12 Hour Format
# =========================================================

MARKET_SCHEDULE = [

    {
        "name": "سيدني",
        "hour": 12,
        "minute": 0,
        "ampm": "ص",
        "market": "جلسة آسيا والمحيط الهادئ"
    },

    {
        "name": "طوكيو",
        "hour": 3,
        "minute": 0,
        "ampm": "ص",
        "market": "الجلسة الآسيوية"
    },

    {
        "name": "لندن",
        "hour": 10,
        "minute": 0,
        "ampm": "ص",
        "market": "الجلسة الأوروبية"
    },

    {
        "name": "نيويورك",
        "hour": 3,
        "minute": 0,
        "ampm": "م",
        "market": "الجلسة الأمريكية"
    }
]


def format_12h(hour, minute):

    suffix = "ص"

    if hour >= 12:

        suffix = "م"

    display_hour = hour % 12

    if display_hour == 0:

        display_hour = 12

    return (
        f"{display_hour}:{minute:02d} {suffix}"
    )


def market_schedule_text():

    lines = []

    for market in MARKET_SCHEDULE:

        time_text = format_12h(
            market["hour"],
            market["minute"]
        )

        lines.append(
            f"🌍 {market['name']}\n"
            f"🕐 {time_text} بتوقيت دمشق\n"
            f"📊 {market['market']}"
        )

    return "\n\n".join(lines)


# =========================================================
# MARKET OPEN SUMMARY
# =========================================================

def market_open_summary(
    market_name,
    price_change,
    trend
):

    if price_change > 0.20:

        movement = (
            "📈 الذهب يتحرك بإيجابية، "
            "وقد تزداد سرعة الحركة مع السيولة."
        )

    elif price_change < -0.20:

        movement = (
            "📉 الذهب يتحرك بسلبية، "
            "وقد تزداد سرعة الحركة مع السيولة."
        )

    else:

        movement = (
            "🟡 الحركة محدودة نسبيًا، "
            "وقد يظهر تذبذب قبل اتجاه واضح."
        )

    return (
        f"🌍 افتتاح سوق {market_name}\n\n"
        f"🧭 الاتجاه الحالي: {trend}\n"
        f"📊 تغير السعر: {price_change:+.2f}%\n\n"
        f"🧠 موجز السوق:\n"
        f"{movement}\n\n"
        "⚠️ الافتتاح قد يرفع التقلب والسيولة، "
        "لذلك يفضّل انتظار استقرار الحركة قبل الدخول."
    )


# =========================================================
# AUTOMATIC TRADE STATE
# =========================================================

AUTO_TRADE_ENABLED = True

last_auto_signal = None
last_auto_signal_time = None


# =========================================================
# AUTOMATIC TRADE ENGINE
# =========================================================

def generate_auto_trade():

    global last_auto_signal
    global last_auto_signal_time

    h1_df = get_bars(
        "1h",
        300
    )

    m15_df = get_bars(
        "15m",
        300
    )

    m5_df = get_bars(
        "5m",
        300
    )

    h1 = analyze_scalp(
        h1_df
    )

    m15 = analyze_scalp(
        m15_df
    )

    m5 = analyze_scalp(
        m5_df
    )

    results = [
        h1,
        m15,
        m5
    ]

    buy = sum(
        "BUY" in x["direction"]
        for x in results
    )

    sell = sum(
        "SELL" in x["direction"]
        for x in results
    )

    confidence = int(
        np.mean(
            [
                x["score"]
                for x in results
            ]
        )
    )

    # =====================================================
    # اتجاه موحد
    # =====================================================

    if buy == 3:

        direction = "🟢 BUY"

    elif sell == 3:

        direction = "🔴 SELL"

    else:

        return None

    # =====================================================
    # شروط الدخول
    # =====================================================

    no_extension = all(
        not x["extended"]
        for x in results
    )

    volume_ok = all(
        x["volume_ratio"] >= 0.80
        for x in results
    )

    score_ok = (
        confidence >= 70
    )

    entry_ready = (
        no_extension
        and volume_ok
        and score_ok
    )

    if not entry_ready:

        return None

    # =====================================================
    # بناء الصفقة
    # =====================================================

    entry, sl, tp1, tp2 = build_trade(
        direction,
        m5_df
    )

    # =====================================================
    # منع التكرار
    # =====================================================

    signal_key = (
        direction,
        round(entry, 1),
        round(sl, 1),
        round(tp1, 1)
    )

    if signal_key == last_auto_signal:

        return None

    last_auto_signal = signal_key

    return {
        "direction": direction,
        "confidence": confidence,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2
    }


# =========================================================
# AUTOMATIC TRADE MESSAGE
# =========================================================

def format_auto_trade(trade_data):

    return (
        "🚨 XAU SMART TRADER\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🎯 صفقة جديدة متاحة\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        f"📈 الاتجاه: "
        f"{trade_data['direction']}\n\n"

        f"📍 الدخول: "
        f"{trade_data['entry']:.2f}\n"

        f"🛑 وقف الخسارة: "
        f"{trade_data['sl']:.2f}\n"

        f"🎯 الهدف الأول: "
        f"{trade_data['tp1']:.2f}\n"

        f"🎯 الهدف الثاني: "
        f"{trade_data['tp2']:.2f}\n\n"

        f"💪 قوة التوافق: "
        f"{trade_data['confidence']}%\n\n"

        "🧠 الحالة:\n"
        "جميع شروط محرك الصفقة متوافقة.\n\n"

        "⚠️ هذه إشارة تحليلية وليست ضمانًا للربح.\n"
        "🚫 لا تدخل إذا تغير السعر بشكل كبير "
        "قبل التنفيذ."
    )


# =========================================================
# BOT USERS
# =========================================================

subscribed_users = set()


# =========================================================
# SUBSCRIBE
# =========================================================

async def subscribe(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user_id = update.effective_user.id

    subscribed_users.add(
        user_id
    )

    await update.message.reply_text(
        "🔔 تم تفعيل التنبيهات.\n\n"
        "سيقوم البوت بإرسال تنبيه تلقائي "
        "عندما تكتمل شروط الصفقة."
    )


# =========================================================
# UNSUBSCRIBE
# =========================================================

async def unsubscribe(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user_id = update.effective_user.id

    subscribed_users.discard(
        user_id
    )

    await update.message.reply_text(
        "🔕 تم إيقاف التنبيهات التلقائية."
    )


# =========================================================
# MARKET TIMES COMMAND
# =========================================================

async def market_times(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    message = (
        "🌍 مواعيد افتتاح الأسواق\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "🕐 جميع الأوقات بتوقيت دمشق\n"
        "⏰ نظام 12 ساعة\n\n"
        + market_schedule_text()
        + "\n\n"
        "⚠️ أوقات الأسواق قد تتغير موسميًا "
        "بسبب التوقيت الصيفي."
    )

    await update.message.reply_text(
        message
    )


# =========================================================
# SUPPORT & RESISTANCE COMMAND
# =========================================================

async def levels(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        d1 = get_bars(
            "1d",
            100
        )

        sr = calculate_support_resistance(
            d1
        )

        message = (
            "🤖 XAU SMART TRADER\n"
            "📊 المستويات اليومية\n\n"
            + format_support_resistance(
                sr
            )
            + "\n"
            "🧠 هذه المستويات تستخدم كمرجع "
            "للتأكيد وليست نقاط دخول مضمونة."
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ تعذر حساب الدعوم والمقاومات:\n"
            f"{str(e)}"
        )


# =========================================================
# AUTOMATIC MONITOR
# =========================================================

async def automatic_monitor(
    application
):

    global last_auto_signal_time

    while True:

        try:

            if AUTO_TRADE_ENABLED:

                trade_data = (
                    generate_auto_trade()
                )

                if trade_data:

                    message = format_auto_trade(
                        trade_data
                    )

                    for user_id in list(
                        subscribed_users
                    ):

                        try:

                            await application.bot.send_message(
                                chat_id=user_id,
                                text=message
                            )

                        except Exception:

                            subscribed_users.discard(
                                user_id
                            )

                    last_auto_signal_time = (
                        datetime.now(
                            ZoneInfo(
                                "Asia/Damascus"
                            )
                        )
                    )

        except Exception as e:

            print(
                "AUTO MONITOR ERROR:",
                e
            )

        await asyncio.sleep(
            60
        )
        # =========================================================
# V12 - FINAL CONNECTION / RUN SYSTEM
# =========================================================

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


DAMASCUS_TZ = ZoneInfo("Asia/Damascus")


# =========================================================
# SAFE FLOAT
# =========================================================

def safe_float(value, default=0.0):

    try:

        number = float(value)

        if np.isnan(number):
            return default

        if np.isinf(number):
            return default

        return number

    except Exception:

        return default


# =========================================================
# TRADE BUILDER
# =========================================================

def build_trade(direction, df):

    current = safe_float(
        df["close"].iloc[-1]
    )

    atr_value = safe_float(
        atr(
            df["high"],
            df["low"],
            df["close"],
            14
        ).iloc[-1]
    )

    if atr_value <= 0:

        atr_value = max(
            current * 0.001,
            1.0
        )

    if "BUY" in direction:

        entry = current

        sl = (
            entry
            - atr_value * 1.20
        )

        tp1 = (
            entry
            + atr_value * 1.30
        )

        tp2 = (
            entry
            + atr_value * 2.00
        )

    elif "SELL" in direction:

        entry = current

        sl = (
            entry
            + atr_value * 1.20
        )

        tp1 = (
            entry
            - atr_value * 1.30
        )

        tp2 = (
            entry
            - atr_value * 2.00
        )

    else:

        entry = current
        sl = current
        tp1 = current
        tp2 = current

    return (
        entry,
        sl,
        tp1,
        tp2
    )


# =========================================================
# START MENU
# =========================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user_id = update.effective_user.id

    notification_status = (
        "🟢 مفعلة"
        if user_id in subscribed_users
        else "🔕 غير مفعلة"
    )

    message = (
        "🤖 XAU SMART TRADER v12\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🥇 تحليل ذكي للذهب XAUUSD\n\n"

        "📊 التحليل\n"
        "────────────\n"
        "📅 /weekly  — الأسبوعي\n"
        "📊 /daily   — اليومي\n"
        "⚡ /scalp   — السريع\n"
        "📍 /levels  — الدعوم والمقاومات\n\n"

        "💰 التداول\n"
        "────────────\n"
        "🎯 /trade   — البحث عن صفقة\n"
        "💰 /price   — السعر والبيانات\n\n"

        "🔔 التنبيهات\n"
        "────────────\n"
        "🟢 /subscribe   — تفعيل الصفقات التلقائية\n"
        "🔕 /unsubscribe — إيقاف الصفقات التلقائية\n"
        f"📡 الحالة: {notification_status}\n\n"

        "🌍 الأسواق\n"
        "────────────\n"
        "🕐 /markets  — مواعيد افتتاح الأسواق\n\n"

        "⚙️ النظام\n"
        "────────────\n"
        "🟢 /status   — حالة البوت\n"
        "👨‍💻 /developer — المطور\n"
        "🆘 /support   — الدعم\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "🎯 اختر الأمر الذي تريده."
    )

    await update.message.reply_text(
        message
    )


# =========================================================
# STATUS
# =========================================================

async def status(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user_id = update.effective_user.id

    subscribed = (
        "🟢 مفعلة"
        if user_id in subscribed_users
        else "🔕 غير مفعلة"
    )

    message = (
        "🤖 XAU SMART TRADER v12\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        "🟢 حالة البوت: يعمل\n"
        "🥇 السوق: XAUUSD\n"
        "📡 البيانات: متصلة\n\n"

        "📊 التحليل متعدد الفريمات\n"
        "🟢 W1\n"
        "🟢 D1\n"
        "🟢 H4\n"
        "🟢 H1\n"
        "🟢 M15\n"
        "🟢 M5\n\n"

        "🎯 محرك الصفقات: 🟢 يعمل\n"
        "🔔 التنبيهات التلقائية: "
        f"{subscribed}\n"
        "🧱 الدعوم والمقاومات: 🟢 تعمل\n"
        "🌍 تنبيهات الأسواق: 🟢 تعمل\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "⏳ النظام يعمل بشكل مباشر."
    )

    await update.message.reply_text(
        message
    )

# =========================================================
# FINAL DECISION ENGINE
# =========================================================

def calculate_final(results):
    buy_score = 0.0
    sell_score = 0.0

    buy_count = 0
    sell_count = 0

    for result, weight in results:

        direction = result.get("direction", "")
        score = float(result.get("score", 0))

        if "BUY" in direction:
            buy_score += score * weight
            buy_count += 1

        elif "SELL" in direction:
            sell_score += score * weight
            sell_count += 1

    final_score = buy_score - sell_score

    confidence = int(
        min(
            abs(final_score),
            100
        )
    )

    if final_score >= 60:
        signal = "🟢 شراء"

    elif final_score <= -60:
        signal = "🔴 بيع"

    else:
        signal = "🟡 انتظار"

    if buy_score > sell_score:
        bias = "🟢 صاعد"

    elif sell_score > buy_score:
        bias = "🔴 هابط"

    else:
        bias = "🟡 متذبذب"

    return (
        signal,
        confidence,
        buy_count,
        sell_count,
        bias
    )
# =========================================================
# DAILY ANALYSIS
# =========================================================

async def daily(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        d1_df = get_bars(
            "1d",
            300
        )

        h4_df = get_bars(
            "4h",
            300
        )

        h1_df = get_bars(
            "1h",
            300
        )

        d1 = analyze_standard(
            d1_df
        )

        h4 = analyze_standard(
            h4_df
        )

        h1 = analyze_standard(
            h1_df
        )

        results = [

            (d1, 0.40),

            (h4, 0.35),

            (h1, 0.25)
        ]

        (
            signal,
            confidence,
            buy,
            sell,
            bias
        ) = calculate_final(
            results
        )

        sr = calculate_support_resistance(
            d1_df
        )

        message = (
            "🤖 XAU SMART TRADER v12\n"
            "📊 التحليل اليومي\n"
            "━━━━━━━━━━━━━━━━━━\n\n"

            + analysis_block_arabic(
                "D1 — الاتجاه اليومي",
                d1,
                d1_df
            )

            + "\n\n"

            + analysis_block_arabic(
                "H4 — تأكيد الاتجاه",
                h4,
                h4_df
            )

            + "\n\n"

            + analysis_block_arabic(
                "H1 — منطقة المتابعة",
                h1,
                h1_df
            )

            + "\n\n"

            + format_support_resistance(
                sr
            )

            + "\n"

            + summary_text(
                signal,
                confidence,
                buy,
                sell,
                bias
            )
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ تعذر تنفيذ التحليل اليومي.\n\n"
            f"التفاصيل: {str(e)}"
        )


# =========================================================
# WEEKLY ANALYSIS
# =========================================================

async def weekly(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        d1_df = get_bars(
            "1d",
            1000
        )

        w1_df = build_weekly_from_daily(
            d1_df
        )

        h4_df = get_bars(
            "4h",
            300
        )

        w1 = analyze_standard(
            w1_df
        )

        d1 = analyze_standard(
            d1_df
        )

        h4 = analyze_standard(
            h4_df
        )

        results = [

            (w1, 0.40),

            (d1, 0.35),

            (h4, 0.25)
        ]

        (
            signal,
            confidence,
            buy,
            sell,
            bias
        ) = calculate_final(
            results
        )

        message = (
            "🤖 XAU SMART TRADER v12\n"
            "📅 التحليل الأسبوعي\n"
            "━━━━━━━━━━━━━━━━━━\n\n"

            + analysis_block_arabic(
                "W1 — الاتجاه الرئيسي",
                w1,
                w1_df
            )

            + "\n\n"

            + analysis_block_arabic(
                "D1 — الاتجاه اليومي",
                d1,
                d1_df
            )

            + "\n\n"

            + analysis_block_arabic(
                "H4 — التأكيد",
                h4,
                h4_df
            )

            + "\n\n"

            + summary_text(
                signal,
                confidence,
                buy,
                sell,
                bias
            )
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ تعذر تنفيذ التحليل الأسبوعي.\n\n"
            f"التفاصيل: {str(e)}"
        )


# =========================================================
# SCALP ANALYSIS
# =========================================================

async def scalp(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        h1_df = get_bars(
            "1h",
            300
        )

        m15_df = get_bars(
            "15m",
            300
        )

        m5_df = get_bars(
            "5m",
            300
        )

        h1 = analyze_scalp(
            h1_df
        )

        m15 = analyze_scalp(
            m15_df
        )

        m5 = analyze_scalp(
            m5_df
        )

        results = [
            h1,
            m15,
            m5
        ]

        buy = sum(
            "BUY" in x["direction"]
            for x in results
        )

        sell = sum(
            "SELL" in x["direction"]
            for x in results
        )

        confidence = int(
            np.mean(
                [
                    x["score"]
                    for x in results
                ]
            )
        )

        if buy > sell:

            signal = "🟢 BUY"

        elif sell > buy:

            signal = "🔴 SELL"

        else:

            signal = "🟡 WAIT"

        message = (
            "🤖 XAU SMART TRADER v12\n"
            "⚡ التحليل السريع\n"
            "━━━━━━━━━━━━━━━━━━\n\n"

            + analysis_block_arabic(
                "H1 — الاتجاه",
                h1,
                h1_df,
                True
            )

            + "\n\n"

            + analysis_block_arabic(
                "M15 — التأكيد",
                m15,
                m15_df,
                True
            )

            + "\n\n"

            + analysis_block_arabic(
                "M5 — نقطة المتابعة",
                m5,
                m5_df,
                True
            )

            + "\n\n"

            "━━━━━━━━━━━━━━━━━━\n"
            "🎯 النتيجة النهائية\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"📌 الإشارة: {signal}\n"
            f"💪 قوة التوافق: {confidence}%\n"
            f"📊 BUY: {buy}/3\n"
            f"📊 SELL: {sell}/3\n\n"

            "⚠️ لا يتم الدخول لمجرد ظهور الإشارة؛ "
            "يجب انتظار تأكيد حركة السعر."
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ تعذر تنفيذ التحليل السريع.\n\n"
            f"التفاصيل: {str(e)}"
        )


# =========================================================
# TRADE COMMAND
# =========================================================

async def trade(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        h1_df = get_bars(
            "1h",
            300
        )

        m15_df = get_bars(
            "15m",
            300
        )

        m5_df = get_bars(
            "5m",
            300
        )

        h1 = analyze_scalp(
            h1_df
        )

        m15 = analyze_scalp(
            m15_df
        )

        m5 = analyze_scalp(
            m5_df
        )

        results = [
            h1,
            m15,
            m5
        ]

        buy = sum(
            "BUY" in x["direction"]
            for x in results
        )

        sell = sum(
            "SELL" in x["direction"]
            for x in results
        )

        confidence = int(
            np.mean(
                [
                    x["score"]
                    for x in results
                ]
            )
        )

        if buy == 3:

            direction = "🟢 BUY"

        elif sell == 3:

            direction = "🔴 SELL"

        else:

            direction = "🟡 WAIT"

        entry_ready = (
            (buy == 3 or sell == 3)
            and all(
                not x["extended"]
                for x in results
            )
            and all(
                x["volume_ratio"] >= 0.80
                for x in results
            )
            and confidence >= 70
        )

        if entry_ready:

            (
                entry,
                sl,
                tp1,
                tp2
            ) = build_trade(
                direction,
                m5_df
            )

            message = (
                "🤖 XAU SMART TRADER v12\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "🎯 صفقة متوافقة الآن\n"
                "━━━━━━━━━━━━━━━━━━\n\n"

                f"📈 الاتجاه: {direction}\n"
                f"💪 القوة: {confidence}%\n\n"

                f"📍 الدخول: {entry:.2f}\n"
                f"🛑 وقف الخسارة: {sl:.2f}\n"
                f"🎯 الهدف الأول: {tp1:.2f}\n"
                f"🎯 الهدف الثاني: {tp2:.2f}\n\n"

                "🧠 حالة الصفقة:\n"
                "🟢 الشروط الرئيسية مكتملة.\n\n"

                "⚠️ راقب السعر قبل التنفيذ، "
                "ولا تطارد الحركة."
            )

        else:

            message = (
                "🤖 XAU SMART TRADER v12\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "⏳ لا توجد صفقة الآن\n"
                "━━━━━━━━━━━━━━━━━━\n\n"

                f"📊 الاتجاه: {direction}\n"
                f"💪 قوة التوافق: {confidence}%\n\n"

                "🧠 السبب:\n"
                "الفريمات لم تحقق جميع شروط "
                "الدخول المطلوبة.\n\n"

                "🎯 القرار:\n"
                "⏳ الانتظار أفضل حاليًا."
            )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ تعذر تشغيل محرك الصفقة.\n\n"
            f"التفاصيل: {str(e)}"
        )


# =========================================================
# PRICE
# =========================================================

async def price(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        df = get_bars(
            "1d",
            10
        )

        current = safe_float(
            df["close"].iloc[-1]
        )

        previous = safe_float(
            df["close"].iloc[-2]
        )

        change = (
            current - previous
        )

        percent = (
            change / previous * 100
            if previous
            else 0
        )

        message = (
            "🥇 XAUUSD\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"💰 السعر: {current:.2f}\n"
            f"📊 التغير: {change:+.2f}\n"
            f"📈 النسبة: {percent:+.2f}%\n"
            "━━━━━━━━━━━━━━━━━━"
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ تعذر الحصول على السعر.\n"
            f"{str(e)}"
        )


# =========================================================
# DEVELOPER
# =========================================================

async def developer(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(
        "👨‍💻 المطور\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "Morhaf Marouf\n\n"
        "🤖 XAU SMART TRADER v12"
    )


# =========================================================
# SUPPORT
# =========================================================

async def support(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(
        "🆘 الدعم\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "للتواصل مع الدعم:\n"
        "👉 @Morhafsy"
    )


# =========================================================
# MARKET MONITOR
# =========================================================

async def market_monitor(
    application
):

    notified_markets = {}

    while True:

        try:

            now = datetime.now(
                DAMASCUS_TZ
            )

            current_date = (
                now.strftime("%Y-%m-%d")
            )

            current_hour = now.hour
            current_minute = now.minute

            for market in MARKET_SCHEDULE:

                market_key = (
                    f"{current_date}_"
                    f"{market['name']}"
                )

                market_hour = market["hour"]

                if market["ampm"] == "م":

                    if market_hour != 12:

                        market_hour += 12

                elif market_hour == 12:

                    market_hour = 0

                if (
                    current_hour == market_hour
                    and current_minute == market["minute"]
                    and market_key
                    not in notified_markets
                ):

                    try:

                        df = get_bars(
                            "1h",
                            10
                        )

                        current = safe_float(
                            df["close"].iloc[-1]
                        )

                        previous = safe_float(
                            df["close"].iloc[-2]
                        )

                        change = (
                            (
                                current
                                - previous
                            )
                            / previous
                            * 100
                            if previous
                            else 0
                        )

                        result = analyze_scalp(
                            df
                        )

                        message = market_open_summary(
                            market["name"],
                            change,
                            result["trend"]
                        )

                        for user_id in list(
                            subscribed_users
                        ):

                            try:

                                await application.bot.send_message(
                                    chat_id=user_id,
                                    text=message
                                )

                            except Exception:

                                pass

                    except Exception as e:

                        print(
                            "MARKET MONITOR ERROR:",
                            e
                        )

                    notified_markets[
                        market_key
                    ] = True

            # حذف السجلات القديمة
            if len(notified_markets) > 100:

                notified_markets.clear()

        except Exception as e:

            print(
                "MARKET LOOP ERROR:",
                e
            )

        await asyncio.sleep(
            30
        )


# =========================================================
# AUTOMATIC TRADE MONITOR WRAPPER
# =========================================================

async def automatic_trade_monitor(
    application
):

    while True:

        try:

            if AUTO_TRADE_ENABLED:

                trade_data = (
                    generate_auto_trade()
                )

                if trade_data:

                    message = (
                        format_auto_trade(
                            trade_data
                        )
                    )

                    for user_id in list(
                        subscribed_users
                    ):

                        try:

                            await application.bot.send_message(
                                chat_id=user_id,
                                text=message
                            )

                        except Exception:

                            subscribed_users.discard(
                                user_id
                            )

        except Exception as e:

            print(
                "AUTO TRADE ERROR:",
                e
            )

        await asyncio.sleep(
            60
        )


# =========================================================
# RUN BOT
# =========================================================

async def run_bot():

    if not TOKEN:

        raise RuntimeError(
            "TELEGRAM_TOKEN غير موجود في Environment Variables."
        )

    application = (
        Application.builder()
        .token(TOKEN)
        .build()
    )

    # -----------------------------------------------------
    # COMMANDS
    # -----------------------------------------------------

    application.add_handler(
        CommandHandler(
            "start",
            start
        )
    )

    application.add_handler(
        CommandHandler(
            "status",
            status
        )
    )

    application.add_handler(
        CommandHandler(
            "price",
            price
        )
    )

    application.add_handler(
        CommandHandler(
            "weekly",
            weekly
        )
    )

    application.add_handler(
        CommandHandler(
            "daily",
            daily
        )
    )

    application.add_handler(
        CommandHandler(
            "scalp",
            scalp
        )
    )

    application.add_handler(
        CommandHandler(
            "trade",
            trade
        )
    )

    application.add_handler(
        CommandHandler(
            "levels",
            levels
        )
    )

    application.add_handler(
        CommandHandler(
            "markets",
            market_times
        )
    )

    application.add_handler(
        CommandHandler(
            "subscribe",
            subscribe
        )
    )

    application.add_handler(
        CommandHandler(
            "unsubscribe",
            unsubscribe
        )
    )

    application.add_handler(
        CommandHandler(
            "developer",
            developer
        )
    )

    application.add_handler(
        CommandHandler(
            "support",
            support
        )
    )

    # -----------------------------------------------------
    # INITIALIZE
    # -----------------------------------------------------

    await application.initialize()

    await application.start()

    # -----------------------------------------------------
    # TELEGRAM POLLING
    #
    # drop_pending_updates=True
    # يمنع معالجة الرسائل القديمة بعد إعادة التشغيل.
    # -----------------------------------------------------

    await application.updater.start_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES
    )

    print(
        "===================================="
    )

    print(
        "XAU SMART TRADER v12"
    )

    print(
        "Telegram polling started."
    )

    print(
        "Automatic trade monitor started."
    )

    print(
        "Market monitor started."
    )

    print(
        "===================================="
    )

    # -----------------------------------------------------
    # BACKGROUND TASKS
    # -----------------------------------------------------

    auto_task = asyncio.create_task(
        automatic_trade_monitor(
            application
        )
    )

    market_task = asyncio.create_task(
        market_monitor(
            application
        )
    )

    try:

        while True:

            await asyncio.sleep(
                3600
            )

    except asyncio.CancelledError:

        pass

    finally:

        auto_task.cancel()

        market_task.cancel()

        try:

            await auto_task

        except asyncio.CancelledError:

            pass

        try:

            await market_task

        except asyncio.CancelledError:

            pass

        await application.updater.stop()

        await application.stop()

        await application.shutdown()


# =========================================================
# FLASK SERVER
# =========================================================

def run_server():

    port = int(
        os.environ.get(
            "PORT",
            10000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )


# =========================================================
# MAIN
# =========================================================

def main():

    server_thread = threading.Thread(
        target=run_server,
        daemon=True
    )

    server_thread.start()

    asyncio.run(
        run_bot()
    )


# =========================================================
# START
# =========================================================

if __name__ == "__main__":

    main()
