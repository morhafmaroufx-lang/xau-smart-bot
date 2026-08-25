import os
import asyncio
import threading
import time
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import pandas as pd
import numpy as np

from flask import Flask, request, jsonify
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)


# =========================================================
# CONFIG
# =========================================================

TOKEN = os.environ.get("TELEGRAM_TOKEN")

PORT = int(os.environ.get("PORT", "10000"))

RENDER_URL = os.environ.get(
    "RENDER_EXTERNAL_URL",
    "https://xau-smart-bot.onrender.com"
).rstrip("/")

WEBHOOK_PATH = "/telegram-webhook"
WEBHOOK_URL = f"{RENDER_URL}{WEBHOOK_PATH}"

SYMBOL = "XAUUSD"

DATA_URL = "https://biquote.io/api/XAUUSD/ohlc"

DAMASCUS = ZoneInfo("Asia/Damascus")

CACHE_SECONDS = 45

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# =========================================================
# GLOBAL STATE
# =========================================================

DATA_CACHE = {}

SUBSCRIBERS = set()

LAST_AUTO_SIGNAL = None
LAST_AUTO_TIME = 0

APPLICATION = None

BOT_STARTED = False


# =========================================================
# HEALTH
# =========================================================

@app.route("/", methods=["GET", "HEAD"])
def home():
    return "XAU SMART TRADER v13 - OK", 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "bot": "XAU SMART TRADER v13",
        "time": datetime.now(DAMASCUS).isoformat()
    }), 200


# =========================================================
# DATA CACHE
# =========================================================

def cache_key(interval, limit):
    return f"{interval}_{limit}"


def get_bars(interval, limit=300):

    key = cache_key(interval, limit)

    now = time.time()

    cached = DATA_CACHE.get(key)

    if cached:

        saved_time, cached_df = cached

        if now - saved_time < CACHE_SECONDS:

            return cached_df.copy()

    url = f"{DATA_URL}?interval={interval}&limit={limit}"

    response = requests.get(
        url,
        timeout=12
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

    if len(df) < 10:

        raise ValueError(
            f"بيانات {interval} غير كافية"
        )

    DATA_CACHE[key] = (
        now,
        df.copy()
    )

    return df


# =========================================================
# WEEKLY
# =========================================================

def build_weekly_from_daily(d1):

    if "openTime" not in d1.columns:
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

    if len(df) < 10:

        raise ValueError(
            "بيانات D1 غير كافية لبناء W1"
        )

    df = df.set_index("date")

    weekly = df.resample(
        "W-SUN",
        label="right",
        closed="right"
    ).agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "tickVolume": "sum"
    })

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
# SAFE NUMBER
# =========================================================

def safe_float(value, default=0.0):

    try:

        value = float(value)

        if np.isfinite(value):

            return value

    except Exception:
        pass

    return default


# =========================================================
# STANDARD ANALYSIS
# =========================================================

def analyze_standard(df):

    if df is None or len(df) < 20:

        raise ValueError(
            "البيانات غير كافية للتحليل"
        )

    close = df["close"]

    current = safe_float(
        close.iloc[-1]
    )

    ema20 = safe_float(
        ema(close, 20).iloc[-1]
    )

    ema50 = safe_float(
        ema(close, 50).iloc[-1]
    )

    ema200_series = ema(
        close,
        200
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
        df["tickVolume"].tail(20).mean()
    )

    volume_ratio = (
        volume / avg_volume
        if avg_volume > 0
        else 0
    )

    bullish = 0
    bearish = 0

    reasons = []
    warnings = []

    # -----------------------------------------------------
    # TREND
    # -----------------------------------------------------

    if (
        current > ema20
        and ema20 > ema50
    ):

        bullish += 30

        trend = "🟢 صاعد"

        reasons.append(
            "السعر أعلى المتوسطات القصيرة"
        )

    elif (
        current < ema20
        and ema20 < ema50
    ):

        bearish += 30

        trend = "🔴 هابط"

        reasons.append(
            "السعر أسفل المتوسطات القصيرة"
        )

    else:

        trend = "🟡 متذبذب"

        if current > ema20:

            bullish += 15

        else:

            bearish += 15

        warnings.append(
            "الاتجاه غير مكتمل"
        )

    # -----------------------------------------------------
    # EMA200
    # -----------------------------------------------------

    if len(df) >= 200:

        if current > ema200:

            bullish += 15

        elif current < ema200:

            bearish += 15

    # -----------------------------------------------------
    # RSI
    # -----------------------------------------------------

    if 50 <= rsi_value < 70:

        bullish += 20

    elif 30 < rsi_value < 50:

        bearish += 20

    elif rsi_value >= 70:

        warnings.append(
            "تشبع شرائي"
        )

    elif rsi_value <= 30:

        warnings.append(
            "تشبع بيعي"
        )

    # -----------------------------------------------------
    # MACD
    # -----------------------------------------------------

    if macd_value > signal_value:

        bullish += 20

    elif macd_value < signal_value:

        bearish += 20

    else:

        warnings.append(
            "MACD غير واضح"
        )

    # -----------------------------------------------------
    # VOLUME
    # -----------------------------------------------------

    if volume_ratio >= 1.20:

        if bullish > bearish:

            bullish += 15

        elif bearish > bullish:

            bearish += 15

    elif volume_ratio < 0.80:

        warnings.append(
            "السيولة ضعيفة"
        )

    # -----------------------------------------------------
    # DIRECTION
    # -----------------------------------------------------

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
            min(
                score,
                100
            )
        )
    )

    # -----------------------------------------------------
    # EXTENSION
    # -----------------------------------------------------

    extended = False

    if atr_value > 0:

        extended = (
            abs(current - ema20)
            > atr_value * 0.60
        )

    if extended:

        warnings.append(
            "السعر ممتد"
        )

    # -----------------------------------------------------
    # INDICATOR STATE
    # -----------------------------------------------------

    if len(warnings) == 0:

        indicator_state = "🟢 قوية"

    elif len(warnings) == 1:

        indicator_state = "🟢 جيدة"

    elif len(warnings) == 2:

        indicator_state = "🟡 تحتاج تأكيد"

    else:

        indicator_state = "🟠 مختلطة"

    return {
        "price": current,
        "ema20": ema20,
        "ema50": ema50,
        "ema200": ema200,
        "rsi": rsi_value,
        "macd": macd_value,
        "macd_signal": signal_value,
        "macd_histogram": histogram_value,
        "atr": atr_value,
        "volume": volume,
        "volume_ratio": volume_ratio,
        "trend": trend,
        "direction": direction,
        "score": score,
        "warnings": warnings,
        "reasons": reasons,
        "indicator_state": indicator_state,
        "extended": extended,
        "bars": len(df)
    }


# =========================================================
# SCALP ANALYSIS
# =========================================================

def analyze_scalp(df):

    if df is None or len(df) < 60:

        raise ValueError(
            "بيانات السكالب غير كافية"
        )

    close = df["close"]

    current = safe_float(
        close.iloc[-1]
    )

    ema9 = safe_float(
        ema(
            close,
            9
        ).iloc[-1]
    )

    ema20 = safe_float(
        ema(
            close,
            20
        ).iloc[-1]
    )

    ema50 = safe_float(
        ema(
            close,
            50
        ).iloc[-1]
    )

    rsi_value = safe_float(
        rsi(
            close,
            9
        ).iloc[-1],
        50
    )

    macd_line, signal_line, histogram = macd(
        close,
        5,
        13,
        4
    )

    macd_value = safe_float(
        macd_line.iloc[-1]
    )

    signal_value = safe_float(
        signal_line.iloc[-1]
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

    average_volume = safe_float(
        df["tickVolume"].tail(20).mean()
    )

    volume_ratio = (
        volume / average_volume
        if average_volume > 0
        else 0
    )

    bullish = 0
    bearish = 0

    warnings = []

    # -----------------------------------------------------
    # TREND
    # -----------------------------------------------------

    if (
        current > ema9
        and ema9 > ema20
        and ema20 > ema50
    ):

        bullish += 40

        trend = "🟢 صاعد"

    elif (
        current < ema9
        and ema9 < ema20
        and ema20 < ema50
    ):

        bearish += 40

        trend = "🔴 هابط"

    else:

        trend = "🟡 متذبذب"

        if current > ema20:

            bullish += 20

        else:

            bearish += 20

        warnings.append(
            "الاتجاه غير مكتمل"
        )

    # -----------------------------------------------------
    # RSI
    # -----------------------------------------------------

    if 50 <= rsi_value < 70:

        bullish += 20

    elif 30 < rsi_value < 50:

        bearish += 20

    elif rsi_value >= 70:

        warnings.append(
            "RSI مرتفع"
        )

    elif rsi_value <= 30:

        warnings.append(
            "RSI منخفض"
        )

    # -----------------------------------------------------
    # MACD
    # -----------------------------------------------------

    if macd_value > signal_value:

        bullish += 20

    elif macd_value < signal_value:

        bearish += 20

    else:

        warnings.append(
            "MACD ضعيف"
        )

    # -----------------------------------------------------
    # VOLUME
    # -----------------------------------------------------

    if volume_ratio >= 1.20:

        if bullish > bearish:

            bullish += 20

        elif bearish > bullish:

            bearish += 20

    elif volume_ratio < 0.80:

        warnings.append(
            "السيولة ضعيفة"
        )

    # -----------------------------------------------------
    # FINAL
    # -----------------------------------------------------

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
            min(
                score,
                100
            )
        )
    )

    extended = False

    if atr_value > 0:

        extended = (
            abs(current - ema20)
            > atr_value * 0.60
        )

    if extended:

        warnings.append(
            "السعر ممتد"
        )

    if len(warnings) == 0:

        state = "🟢 قوية"

    elif len(warnings) <= 1:

        state = "🟢 جيدة"

    elif len(warnings) == 2:

        state = "🟡 تحتاج تأكيد"

    else:

        state = "🟠 مختلطة"

    return {
        "price": current,
        "ema9": ema9,
        "ema20": ema20,
        "ema50": ema50,
        "rsi": rsi_value,
        "macd": macd_value,
        "signal": signal_value,
        "atr": atr_value,
        "volume_ratio": volume_ratio,
        "trend": trend,
        "direction": direction,
        "score": score,
        "warnings": warnings,
        "indicator_state": state,
        "extended": extended
    }


# =========================================================
# SUPPORT / RESISTANCE
# =========================================================

def calculate_support_resistance(
    df,
    lookback=60
):

    if df is None or len(df) < 20:

        raise ValueError(
            "بيانات المستويات غير كافية"
        )

    recent = df.tail(
        min(
            lookback,
            len(df)
        )
    ).copy()

    current = safe_float(
        recent["close"].iloc[-1]
    )

    lows = sorted(
        [
            safe_float(x)
            for x in recent["low"]
            if safe_float(x) < current
        ],
        reverse=True
    )

    highs = sorted(
        [
            safe_float(x)
            for x in recent["high"]
            if safe_float(x) > current
        ]
    )

    # fallback

    if len(lows) < 2:

        lows = sorted(
            recent["low"].astype(float)
        )

        supports = [
            x for x in lows
            if x < current
        ]

        supports = sorted(
            supports,
            reverse=True
        )

    else:

        supports = lows

    if len(highs) < 2:

        highs = sorted(
            recent["high"].astype(float)
        )

        resistances = [
            x for x in highs
            if x > current
        ]

    else:

        resistances = highs

    if not supports:

        support1 = current - (
            abs(
                current -
                safe_float(
                    recent["low"].min()
                )
            ) * 0.50
        )

        support2 = current - (
            abs(
                current -
                safe_float(
                    recent["low"].min()
                )
            ) * 0.80
        )

    else:

        support1 = supports[0]

        support2 = (
            supports[1]
            if len(supports) > 1
            else supports[0]
        )

    if not resistances:

        resistance1 = current + (
            abs(
                safe_float(
                    recent["high"].max()
                ) -
                current
            ) * 0.50
        )

        resistance2 = current + (
            abs(
                safe_float(
                    recent["high"].max()
                ) -
                current
            ) * 0.80
        )

    else:

        resistance1 = resistances[0]

        resistance2 = (
            resistances[1]
            if len(resistances) > 1
            else resistances[0]
        )

    return {
        "current": current,
        "support1": safe_float(support1),
        "support2": safe_float(support2),
        "resistance1": safe_float(resistance1),
        "resistance2": safe_float(resistance2)
    }


# =========================================================
# ENTRY ZONES
# =========================================================

def build_entry_zone(
    direction,
    current,
    levels,
    atr_value
):

    atr_value = max(
        safe_float(atr_value),
        0.01
    )

    if "BUY" in direction:

        support = levels["support1"]

        zone_high = min(
            current,
            support + atr_value * 0.35
        )

        zone_low = (
            support - atr_value * 0.20
        )

        return (
            zone_low,
            zone_high
        )

    if "SELL" in direction:

        resistance = levels["resistance1"]

        zone_low = max(
            current,
            resistance - atr_value * 0.35
        )

        zone_high = (
            resistance + atr_value * 0.20
        )

        return (
            zone_low,
            zone_high
        )

    return (
        current,
        current
    )


# =========================================================
# FINAL MULTI TIMEFRAME
# =========================================================

def calculate_final(results):

    weighted = 0.0

    buy = 0
    sell = 0

    for result, weight in results:

        if not result:
            continue

        direction = result.get(
            "direction",
            "🟡 WAIT"
        )

        score = safe_float(
            result.get(
                "score",
                0
            )
        )

        if "BUY" in direction:

            weighted += (
                score * weight
            )

            buy += 1

        elif "SELL" in direction:

            weighted -= (
                score * weight
            )

            sell += 1

    confidence = int(
        min(
            abs(weighted),
            100
        )
    )

    if weighted >= 60:

        signal = "🟢 BUY"

    elif weighted <= -60:

        signal = "🔴 SELL"

    else:

        signal = "🟡 WAIT"

    if buy > sell:

        bias = "🟢 صاعد"

    elif sell > buy:

        bias = "🔴 هابط"

    else:

        bias = "🟡 متذبذب"

    return (
        signal,
        confidence,
        buy,
        sell,
        bias
    )


# =========================================================
# FORMATTING
# =========================================================

def warning_text(warnings):

    if not warnings:

        return "🟢 لا توجد ملاحظات"

    unique = []

    for item in warnings:

        if item not in unique:

            unique.append(item)

    return " • ".join(
        unique
    )


def analysis_block(
    name,
    result
):

    return (
        f"📊 {name}\n\n"

        f"الاتجاه: {result.get('trend', 'غير متاح')}\n"
        f"الإشارة: {result.get('direction', '🟡 WAIT')}\n"
        f"قوة الإشارة: {result.get('score', 0)}%\n"
        f"حالة المؤشرات: {result.get('indicator_state', 'غير متاح')}\n\n"

        f"💰 السعر: {result.get('price', 0):.2f}\n"
        f"📈 EMA20: {result.get('ema20', 0):.2f}\n"
        f"📈 EMA50: {result.get('ema50', 0):.2f}\n"
        f"📈 EMA200: {result.get('ema200', 0):.2f}\n"
        f"📊 RSI: {result.get('rsi', 0):.1f}\n"
        f"📉 MACD: {result.get('macd', 0):.2f}\n"
        f"📊 ATR: {result.get('atr', 0):.2f}\n"
        f"📦 Volume: {result.get('volume_ratio', 0):.2f}× المتوسط\n\n"

        f"⚠️ ملاحظات: {warning_text(result.get('warnings', []))}"
    )


# =========================================================
# START
# =========================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    subscribed = (
        update.effective_chat.id
        in SUBSCRIBERS
    )

    auto_state = (
        "🟢 مفعلة"
        if subscribed
        else "🔕 غير مفعلة"
    )

    message = (
        "🤖 XAU SMART TRADER v13\n"
        "🥇 التحليل الذكي للذهب XAUUSD\n\n"

        "📊 التحليل\n\n"

        "📅 /weekly — التحليل الأسبوعي\n\n"
        "📊 /daily — التحليل اليومي\n\n"
        "⚡ /scalp — التحليل السريع\n\n"
        "📍 /levels — الدعوم والمقاومات\n\n"

        "💰 التداول\n\n"

        "🎯 /trade — البحث عن صفقة\n\n"
        "💰 /price — السعر والبيانات\n\n"

        "🔔 التنبيهات\n\n"

        "🟢 /subscribe — تفعيل الصفقات التلقائية\n\n"
        "🔕 /unsubscribe — إيقاف الصفقات التلقائية\n\n"
        f"📡 الحالة: {auto_state}\n\n"

        "🌍 الأسواق\n\n"

        "🕐 /markets — مواعيد افتتاح الأسواق\n\n"

        "⚙️ النظام\n\n"

        "🟢 /status — حالة البوت\n\n"
        "👨‍💻 /developer — المطور\n\n"
        "🆘 /support — الدعم\n\n"

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

    subscribed = (
        update.effective_chat.id
        in SUBSCRIBERS
    )

    state = (
        "🟢 مفعلة"
        if subscribed
        else "🔕 غير مفعلة"
    )

    await update.message.reply_text(

        "🤖 XAU SMART TRADER v13\n\n"

        "🟢 النظام: يعمل\n"
        "🌐 Webhook: يعمل\n"
        "🌐 Health Server: يعمل\n"
        "📡 مصدر البيانات: متاح\n"
        "⚡ Cache: مفعّل\n"
        f"🔔 الصفقات التلقائية: {state}\n\n"

        "📊 Weekly: ON\n"
        "📊 Daily: ON\n"
        "⚡ Scalp: ON\n"
        "📍 Levels: ON\n"
        "🎯 Trade Engine: ON\n\n"

        "🛡️ حماية الأخطاء: ON\n"
        "🔒 منع تكرار الإشارات: ON\n\n"

        "⏳ الحالة: تشغيل مباشر"
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

        emoji = (
            "📈"
            if change >= 0
            else "📉"
        )

        message = (
            "🥇 XAUUSD\n\n"

            f"💰 السعر: {current:.2f}\n"
            f"{emoji} التغير: {change:+.2f}\n"
            f"📊 النسبة: {percent:+.2f}%\n\n"

            "📡 مصدر البيانات: يعمل"
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await safe_reply(
            update,
            f"❌ تعذر الحصول على السعر.\n\nالتفاصيل: {e}"
        )


# =========================================================
# LEVELS
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

        data = calculate_support_resistance(
            d1,
            60
        )

        message = (
            "🤖 XAU SMART TRADER v13\n"
            "📊 المستويات اليومية\n\n"

            "🧱 الدعوم والمقاومات\n\n"

            f"💰 السعر الحالي: {data['current']:.2f}\n\n"

            f"🟢 الدعم القريب: {data['support1']:.2f}\n\n"
            f"🟢 الدعم التالي: {data['support2']:.2f}\n\n"

            f"🔴 المقاومة القريبة: {data['resistance1']:.2f}\n\n"
            f"🔴 المقاومة التالية: {data['resistance2']:.2f}\n\n"

            "🧠 المستويات مرجع للتحليل والتأكيد، "
            "وليست نقاط دخول مضمونة."
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await safe_reply(
            update,
            f"❌ تعذر حساب المستويات.\n\nالتفاصيل: {e}"
        )


# =========================================================
# WEEKLY
# =========================================================

async def weekly(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        d1 = get_bars(
            "1d",
            300
        )

        w1 = build_weekly_from_daily(
            d1
        )

        h4 = get_bars(
            "4h",
            300
        )

        w1_result = analyze_standard(
            w1
        )

        d1_result = analyze_standard(
            d1
        )

        h4_result = analyze_standard(
            h4
        )

        results = [
            (w1_result, 0.45),
            (d1_result, 0.35),
            (h4_result, 0.20)
        ]

        signal, confidence, buy, sell, bias = calculate_final(
            results
        )

        levels_data = calculate_support_resistance(
            d1,
            60
        )

        zone_low, zone_high = build_entry_zone(
            signal,
            d1_result["price"],
            levels_data,
            h4_result["atr"]
        )

        message = (
            "🤖 XAU SMART TRADER v13\n"
            "📅 التحليل الأسبوعي\n\n"

            + analysis_block(
                "W1",
                w1_result
            )

            + "\n\n"

            + analysis_block(
                "D1",
                d1_result
            )

            + "\n\n"

            + analysis_block(
                "H4",
                h4_result
            )

            + "\n\n"

            "🎯 الخلاصة\n\n"

            f"الإشارة: {signal}\n"
            f"قوة التوافق: {confidence}%\n"
            f"التوافق: BUY {buy}/3 | SELL {sell}/3\n"
            f"الاتجاه العام: {bias}\n\n"

            "📍 منطقة محتملة\n"
            f"{zone_low:.2f} — {zone_high:.2f}\n\n"

            "🧠 ملاحظة: المنطقة احتمالية وليست أمر دخول تلقائي.\n"
            "🚫 لا تتم إضافة صفقة من هذا التحليل."
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await safe_reply(
            update,
            f"❌ تعذر تنفيذ التحليل الأسبوعي.\n\nالتفاصيل: {e}"
        )


# =========================================================
# DAILY
# =========================================================

async def daily(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        d1 = get_bars(
            "1d",
            300
        )

        h4 = get_bars(
            "4h",
            300
        )

        h1 = get_bars(
            "1h",
            300
        )

        d1_result = analyze_standard(
            d1
        )

        h4_result = analyze_standard(
            h4
        )

        h1_result = analyze_standard(
            h1
        )

        results = [
            (d1_result, 0.40),
            (h4_result, 0.35),
            (h1_result, 0.25)
        ]

        signal, confidence, buy, sell, bias = calculate_final(
            results
        )

        levels_data = calculate_support_resistance(
            d1,
            60
        )

        zone_low, zone_high = build_entry_zone(
            signal,
            d1_result["price"],
            levels_data,
            h1_result["atr"]
        )

        message = (
            "🤖 XAU SMART TRADER v13\n"
            "📊 التحليل اليومي\n\n"

            + analysis_block(
                "D1",
                d1_result
            )

            + "\n\n"

            + analysis_block(
                "H4",
                h4_result
            )

            + "\n\n"

            + analysis_block(
                "H1",
                h1_result
            )

            + "\n\n"

            "🎯 الخلاصة\n\n"

            f"الإشارة: {signal}\n"
            f"قوة التوافق: {confidence}%\n"
            f"التوافق: BUY {buy}/3 | SELL {sell}/3\n"
            f"الاتجاه العام: {bias}\n\n"

            "📍 منطقة دخول محتملة\n"
            f"{zone_low:.2f} — {zone_high:.2f}\n\n"

            f"🧱 دعم قريب: {levels_data['support1']:.2f}\n"
            f"🧱 دعم تالٍ: {levels_data['support2']:.2f}\n"
            f"🔴 مقاومة قريبة: {levels_data['resistance1']:.2f}\n"
            f"🔴 مقاومة تالية: {levels_data['resistance2']:.2f}\n\n"

            "🚫 هذا التحليل لا يضيف صفقة تلقائيًا."
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await safe_reply(
            update,
            f"❌ تعذر تنفيذ التحليل اليومي.\n\nالتفاصيل: {e}"
        )


# =========================================================
# SCALP
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

        levels_data = calculate_support_resistance(
            m5_df,
            60
        )

        zone_low, zone_high = build_entry_zone(
            signal,
            m5["price"],
            levels_data,
            m5["atr"]
        )

        message = (
            "🤖 XAU SMART TRADER v13\n"
            "⚡ التحليل السريع\n\n"

            + analysis_block(
                "H1",
                h1
            )

            + "\n\n"

            + analysis_block(
                "M15",
                m15
            )

            + "\n\n"

            + analysis_block(
                "M5",
                m5
            )

            + "\n\n"

            "🎯 النتيجة\n\n"

            f"الإشارة: {signal}\n"
            f"قوة التوافق: {confidence}%\n"
            f"التوافق: BUY {buy}/3 | SELL {sell}/3\n\n"

            "📍 منطقة الدخول المحتملة\n"
            f"{zone_low:.2f} — {zone_high:.2f}\n\n"

            "🧱 المستويات\n"
            f"الدعم: {levels_data['support1']:.2f}\n"
            f"المقاومة: {levels_data['resistance1']:.2f}\n\n"

            "🧠 انتظر تأكيد الشمعة ولا تطارد السعر."
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await safe_reply(
            update,
            f"❌ تعذر تنفيذ التحليل السريع.\n\nالتفاصيل: {e}"
        )


# =========================================================
# TRADE
# =========================================================

def determine_trade_type(
    h1,
    m15,
    m5
):

    if (
        h1["direction"]
        == m15["direction"]
        == m5["direction"]
        and h1["direction"] != "🟡 WAIT"
    ):

        return (
            "⚡ صفقة سريعة",
            "دقائق إلى أقل من ساعة"
        )

    if (
        h1["direction"]
        == m15["direction"]
        and h1["direction"] != "🟡 WAIT"
    ):

        return (
            "📊 صفقة متوسطة",
            "عدة ساعات"
        )

    return (
        "⏳ لا يوجد توافق كافٍ",
        "انتظار"
    )


def build_trade(
    direction,
    df
):

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

    atr_value = max(
        atr_value,
        0.10
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

    else:

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

    return (
        entry,
        sl,
        tp1,
        tp2
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

        if buy > sell:

            direction = "🟢 BUY"

        elif sell > buy:

            direction = "🔴 SELL"

        else:

            direction = "🟡 WAIT"

        trade_type, duration = determine_trade_type(
            h1,
            m15,
            m5
        )

        all_same_direction = (
            buy == 3
            or sell == 3
        )

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
            all_same_direction
            and no_extension
            and volume_ok
            and score_ok
        )

        levels_data = calculate_support_resistance(
            m5_df,
            60
        )

        zone_low, zone_high = build_entry_zone(
            direction,
            m5["price"],
            levels_data,
            m5["atr"]
        )

        if entry_ready:

            entry, sl, tp1, tp2 = build_trade(
                direction,
                m5_df
            )

            message = (
                "🤖 XAU SMART TRADER v13\n\n"

                "🎯 توجد صفقة محتملة الآن\n\n"

                f"📈 الاتجاه: {direction}\n"
                f"⚡ النوع: {trade_type}\n"
                f"⏱️ المدة: {duration}\n\n"

                "📍 منطقة الدخول\n"
                f"{zone_low:.2f} — {zone_high:.2f}\n\n"

                f"💰 السعر المرجعي: {entry:.2f}\n"
                f"🛑 SL: {sl:.2f}\n"
                f"🎯 TP1: {tp1:.2f}\n"
                f"🎯 TP2: {tp2:.2f}\n\n"

                f"💪 قوة الصفقة: {confidence}%\n"
                "🟢 شروط الدخول مكتملة\n\n"

                "⚠️ انتظر تأكيد الشمعة.\n"
                "🚫 لا تطارد السعر إذا ابتعد عن المنطقة."
            )

        else:

            reasons = []

            if not all_same_direction:

                reasons.append(
                    "الفريمات غير متوافقة بالكامل"
                )

            if not no_extension:

                reasons.append(
                    "السعر ممتد"
                )

            if not volume_ok:

                reasons.append(
                    "السيولة تحتاج تأكيد"
                )

            if not score_ok:

                reasons.append(
                    "قوة الإشارة غير كافية"
                )

            if not reasons:

                reasons.append(
                    "الشروط لم تكتمل"
                )

            message = (
                "🤖 XAU SMART TRADER v13\n\n"

                "⏳ لا توجد صفقة الآن\n\n"

                f"📊 الاتجاه: {direction}\n"
                f"💪 القوة: {confidence}%\n\n"

                "⚠️ السبب:\n"
                + "\n".join(
                    f"• {x}"
                    for x in reasons
                )
                + "\n\n"

                "📍 منطقة محتملة:\n"
                f"{zone_low:.2f} — {zone_high:.2f}\n\n"

                "🎯 القرار:\n"
                "⏳ انتظار اكتمال الشروط."
            )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await safe_reply(
            update,
            f"❌ تعذر تشغيل محرك الصفقة.\n\nالتفاصيل: {e}"
        )


# =========================================================
# AUTO SUBSCRIBE
# =========================================================

async def subscribe(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    chat_id = update.effective_chat.id

    SUBSCRIBERS.add(
        chat_id
    )

    await update.message.reply_text(

        "🟢 تم تفعيل الصفقات التلقائية.\n\n"

        "سيراقب البوت السوق، وعندما تكتمل "
        "الشروط المحددة سيقوم بإرسال تنبيه.\n\n"

        "🛡️ لن يتم إرسال نفس الإشارة بشكل متكرر.\n\n"

        "🔕 لإيقافها استخدم:\n"
        "/unsubscribe"
    )


async def unsubscribe(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    chat_id = update.effective_chat.id

    SUBSCRIBERS.discard(
        chat_id
    )

    await update.message.reply_text(

        "🔕 تم إيقاف الصفقات التلقائية.\n\n"

        "يمكنك تفعيلها في أي وقت باستخدام:\n"
        "/subscribe"
    )


# =========================================================
# AUTO TRADE CHECK
# =========================================================

def get_auto_signal():

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

        direction = "BUY"

    elif sell == 3:

        direction = "SELL"

    else:

        return None

    if confidence < 70:

        return None

    if any(
        x["extended"]
        for x in results
    ):

        return None

    if any(
        x["volume_ratio"] < 0.80
        for x in results
    ):

        return None

    entry, sl, tp1, tp2 = build_trade(
        direction,
        m5_df
    )

    return {
        "direction": direction,
        "confidence": confidence,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2
    }


async def auto_trade_loop():

    global LAST_AUTO_SIGNAL
    global LAST_AUTO_TIME

    while True:

        try:

            if SUBSCRIBERS:

                signal = await asyncio.to_thread(
                    get_auto_signal
                )

                if signal:

                    direction = signal["direction"]

                    signature = (
                        direction,
                        round(
                            signal["entry"],
                            1
                        )
                    )

                    now = time.time()

                    # منع التكرار لمدة ساعتين

                    if (
                        signature
                        != LAST_AUTO_SIGNAL
                        or
                        now - LAST_AUTO_TIME
                        > 7200
                    ):

                        text = (
                            "🚨 XAU SMART TRADER v13\n\n"

                            "🎯 فرصة تداول متوافقة\n\n"

                            f"📈 الاتجاه: {'🟢 BUY' if direction == 'BUY' else '🔴 SELL'}\n"
                            f"💪 القوة: {signal['confidence']}%\n\n"

                            f"📍 Entry: {signal['entry']:.2f}\n"
                            f"🛑 SL: {signal['sl']:.2f}\n"
                            f"🎯 TP1: {signal['tp1']:.2f}\n"
                            f"🎯 TP2: {signal['tp2']:.2f}\n\n"

                            "🕯️ يُفضّل انتظار تأكيد الشمعة.\n"
                            "⚠️ هذه إشارة تحليلية وليست ضمانًا للربح."
                        )

                        for chat_id in list(
                            SUBSCRIBERS
                        ):

                            try:

                                await APPLICATION.bot.send_message(
                                    chat_id=chat_id,
                                    text=text
                                )

                            except Exception as send_error:

                                logger.error(
                                    "Auto send error: %s",
                                    send_error
                                )

                        LAST_AUTO_SIGNAL = signature
                        LAST_AUTO_TIME = now

        except Exception as e:

            logger.error(
                "Auto trade error: %s",
                e
            )

        await asyncio.sleep(
            60
        )


# =========================================================
# MARKETS
# =========================================================

async def markets(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    now = datetime.now(
        DAMASCUS
    )

    message = (
        "🌍 مواعيد افتتاح الأسواق\n\n"

        "🕐 جميع الأوقات بتوقيت دمشق\n"
        "⏰ نظام 12 ساعة\n\n"

        "🌍 سيدني\n"
        "🕐 12:00 ص\n"
        "📊 جلسة آسيا والمحيط الهادئ\n\n"

        "🌍 طوكيو\n"
        "🕐 3:00 ص\n"
        "📊 الجلسة الآسيوية\n\n"

        "🌍 لندن\n"
        "🕐 10:00 ص\n"
        "📊 الجلسة الأوروبية\n\n"

        "🌍 نيويورك\n"
        "🕐 3:00 م\n"
        "📊 الجلسة الأمريكية\n\n"

        f"📅 الوقت الآن في دمشق: "
        f"{now.strftime('%I:%M %p')}\n\n"

        "⚠️ أوقات الأسواق قد تتغير موسميًا "
        "بسبب التوقيت الصيفي."
    )

    await update.message.reply_text(
        message
    )


# =========================================================
# DEVELOPER
# =========================================================

async def developer(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(

        "👨‍💻 المطور\n\n"

        "Morhaf Marouf\n\n"

        "🤖 XAU SMART TRADER v13\n"
        "🥇 نظام تحليل الذهب XAUUSD"
    )


# =========================================================
# SUPPORT
# =========================================================

async def support(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(

        "🆘 الدعم\n\n"

        "للتواصل مع الدعم:\n"
        "@Morhafsy"
    )


# =========================================================
# SAFE REPLY
# =========================================================

async def safe_reply(
    update,
    text
):

    try:

        if update and update.message:

            await update.message.reply_text(
                text
            )

    except Exception as e:

        logger.error(
            "Reply error: %s",
            e
        )


# =========================================================
# TELEGRAM WEBHOOK
# =========================================================

@app.route(
    WEBHOOK_PATH,
    methods=["POST"]
)
def telegram_webhook():

    global APPLICATION

    if APPLICATION is None:

        return "Bot not ready", 503

    try:

        update_data = request.get_json(
            force=True
        )

        update = Update.de_json(
            update_data,
            APPLICATION.bot
        )

        asyncio.run_coroutine_threadsafe(
            APPLICATION.process_update(
                update
            ),
            BOT_LOOP
        )

        return "OK", 200

    except Exception as e:

        logger.error(
            "Webhook error: %s",
            e
        )

        return "OK", 200


# =========================================================
# BOT LOOP
# =========================================================

BOT_LOOP = None


async def start_application():

    global APPLICATION
    global BOT_STARTED

    if not TOKEN:

        raise RuntimeError(
            "TELEGRAM_TOKEN غير موجود في Environment Variables."
        )

    APPLICATION = (
        Application.builder()
        .token(TOKEN)
        .build()
    )

    APPLICATION.add_handler(
        CommandHandler(
            "start",
            start
        )
    )

    APPLICATION.add_handler(
        CommandHandler(
            "status",
            status
        )
    )

    APPLICATION.add_handler(
        CommandHandler(
            "price",
            price
        )
    )

    APPLICATION.add_handler(
        CommandHandler(
            "levels",
            levels
        )
    )

    APPLICATION.add_handler(
        CommandHandler(
            "weekly",
            weekly
        )
    )

    APPLICATION.add_handler(
        CommandHandler(
            "daily",
            daily
        )
    )

    APPLICATION.add_handler(
        CommandHandler(
            "scalp",
            scalp
        )
    )

    APPLICATION.add_handler(
        CommandHandler(
            "trade",
            trade
        )
    )

    APPLICATION.add_handler(
        CommandHandler(
            "subscribe",
            subscribe
        )
    )

    APPLICATION.add_handler(
        CommandHandler(
            "unsubscribe",
            unsubscribe
        )
    )

    APPLICATION.add_handler(
        CommandHandler(
            "markets",
            markets
        )
    )

    APPLICATION.add_handler(
        CommandHandler(
            "developer",
            developer
        )
    )

    APPLICATION.add_handler(
        CommandHandler(
            "support",
            support
        )
    )

    await APPLICATION.initialize()

    await APPLICATION.start()

    await APPLICATION.bot.set_webhook(
        url=WEBHOOK_URL,
        allowed_updates=[
            "message"
        ],
        drop_pending_updates=True
    )

    BOT_STARTED = True

    logger.info(
        "XAU SMART TRADER v13 started."
    )

    logger.info(
        "Webhook: %s",
        WEBHOOK_URL
    )

    asyncio.create_task(
        auto_trade_loop()
    )

    while True:

        await asyncio.sleep(
            3600
        )


# =========================================================
# SERVER
# =========================================================

def run_server():

    app.run(
        host="0.0.0.0",
        port=PORT,
        debug=False,
        threaded=True,
        use_reloader=False
    )


# =========================================================
# MAIN
# =========================================================

def main():

    global BOT_LOOP

    server = threading.Thread(
        target=run_server,
        daemon=True
    )

    server.start()

    loop = asyncio.new_event_loop()

    BOT_LOOP = loop

    asyncio.set_event_loop(
        loop
    )

    try:

        loop.run_until_complete(
            start_application()
        )

    except KeyboardInterrupt:

        pass

    finally:

        loop.close()


if __name__ == "__main__":

    main()
