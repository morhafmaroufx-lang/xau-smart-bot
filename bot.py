import os
import asyncio
import threading
import time
import logging
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

import requests
import pandas as pd
import numpy as np

from flask import Flask, request, jsonify
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters
)

# =========================================================
# XAU SMART TRADER v16.1 AR
# =========================================================

TOKEN = os.environ.get("TELEGRAM_TOKEN")

PORT = int(os.environ.get("PORT", "10000"))

RENDER_URL = os.environ.get(
    "RENDER_EXTERNAL_URL",
    "https://xau-smart-bot.onrender.com"
).rstrip("/")

WEBHOOK_PATH = "/telegram-webhook"
WEBHOOK_URL = f"{RENDER_URL}{WEBHOOK_PATH}"

# =========================================================
# SYMBOL / DATA SOURCES
# =========================================================

SYMBOL = "XAUUSD"

# OHLC candles
DATA_URL = "https://biquote.io/api/XAUUSD/ohlc"

# السعر اللحظي
# مهم:
# هذا المصدر منفصل عن OHLC.
# إذا لم يعطِ سعرًا مباشرًا صالحًا، لن نستخدم OHLC كبديل.
LIVE_PRICE_URL = "https://biquote.io/api/XAUUSD"

# =========================================================
# TIMEZONE
# =========================================================

DAMASCUS = ZoneInfo("Asia/Damascus")
NEW_YORK = ZoneInfo("America/New_York")

# =========================================================
# ENGINE SETTINGS
# =========================================================

CACHE_SECONDS = 20

MIN_BARS = 30

S_R_CLUSTER_ATR = 0.35

TRADE_THRESHOLD = 73

STRICT_100_THRESHOLD = 99

SECRET_SCALP_THRESHOLD = 85

AUTO_CHECK_SECONDS = 60

AUTO_START = dtime(0, 0)
AUTO_END = dtime(23, 59)

# CMC غير مفعّل حتى يتم تعريف مصدره الحقيقي
CMC_ENABLED = False

# =========================================================
# FLASK
# =========================================================

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

COMMAND_LOCKS = {}

COMMAND_MESSAGES = {}

LAST_AUTO_SIGNAL = {}

LAST_AUTO_TIME = {}

APPLICATION = None

BOT_LOOP = None

BOT_STARTED = False

WEEKLY_REPORT = None

WEEKLY_REPORT_WEEK = None


# =========================================================
# HEALTH
# =========================================================

@app.route("/", methods=["GET", "HEAD"])
def home():
    return "XAU SMART TRADER v16.1 AR - OK", 200


@app.route("/health", methods=["GET"])
def health():

    return jsonify({
        "status": "ok",
        "bot": "XAU SMART TRADER v16.1 AR",
        "time": datetime.now(DAMASCUS).isoformat()
    }), 200


# =========================================================
# HELPERS
# =========================================================

def safe_float(value, default=0.0):

    try:

        if value is None:
            return default

        value = float(value)

        if np.isfinite(value):
            return value

        return default

    except Exception:

        return default


# =========================================================
# LIVE PRICE
# =========================================================

def _extract_live_price(data):
    """
    محاولة استخراج السعر اللحظي من عدة أشكال محتملة
    للاستجابة.

    لا تعتمد على OHLC.
    """

    if not isinstance(data, dict):
        return None

    # أشكال مباشرة محتملة
    possible_keys = [
        "price",
        "last",
        "lastPrice",
        "current",
        "currentPrice",
        "close"
    ]

    for key in possible_keys:

        if key in data:

            value = safe_float(
                data.get(key),
                None
            )

            if value is not None and value > 0:
                return value

    # أحيانًا تكون البيانات داخل data
    nested = data.get("data")

    if isinstance(nested, dict):

        for key in possible_keys:

            if key in nested:

                value = safe_float(
                    nested.get(key),
                    None
                )

                if value is not None and value > 0:
                    return value

    # أحيانًا response يحتوي quote
    quote = data.get("quote")

    if isinstance(quote, dict):

        for key in possible_keys:

            if key in quote:

                value = safe_float(
                    quote.get(key),
                    None
                )

                if value is not None and value > 0:
                    return value

    return None


def _extract_day_change(data):
    """
    استخراج التغير اليومي إن كان المصدر يرسله.
    """

    if not isinstance(data, dict):
        return 0.0

    keys = [
        "dayDiff",
        "dailyChange",
        "change",
        "change24h",
        "priceChange"
    ]

    for key in keys:

        if key in data:

            value = safe_float(
                data.get(key),
                None
            )

            if value is not None:
                return value

    nested = data.get("data")

    if isinstance(nested, dict):

        for key in keys:

            if key in nested:

                value = safe_float(
                    nested.get(key),
                    None
                )

                if value is not None:
                    return value

    return 0.0


def _extract_day_percent(data):
    """
    استخراج نسبة التغير اليومي.
    """

    if not isinstance(data, dict):
        return 0.0

    keys = [
        "dayPct",
        "dailyPercent",
        "changePercent",
        "change24hPercent",
        "percentChange"
    ]

    for key in keys:

        if key in data:

            value = safe_float(
                data.get(key),
                None
            )

            if value is not None:
                return value

    nested = data.get("data")

    if isinstance(nested, dict):

        for key in keys:

            if key in nested:

                value = safe_float(
                    nested.get(key),
                    None
                )

                if value is not None:
                    return value

    return 0.0


def get_live_price():
    """
    جلب السعر اللحظي الحقيقي.

    مهم جدًا:
    لا يتم استخدام OHLC هنا كبديل.

    إذا فشل المصدر أو لم نجد سعرًا صالحًا:
    يتم رفع Exception حتى يظهر للمستخدم أن
    السعر اللحظي غير متاح بدل إظهار سعر قديم.
    """

    response = requests.get(
        LIVE_PRICE_URL,
        timeout=10
    )

    response.raise_for_status()

    data = response.json()

    price = _extract_live_price(data)

    if price is None or price <= 0:

        raise ValueError(
            "مصدر السعر المباشر لم يُرجع سعرًا صالحًا"
        )

    day_diff = _extract_day_change(data)

    day_pct = _extract_day_percent(data)

    return {
        "price": price,
        "day_diff": day_diff,
        "day_pct": day_pct,
        "source": "live"
    }


# =========================================================
# OHLC DATA
# =========================================================

def cache_key(interval, limit):

    return f"{interval}_{limit}"


def get_bars(interval, limit=300):

    key = cache_key(interval, limit)

    now = time.time()

    cached = DATA_CACHE.get(key)

    if cached:

        cached_time, cached_df = cached

        if now - cached_time < CACHE_SECONDS:

            return cached_df.copy()

    url = (
        f"{DATA_URL}"
        f"?interval={interval}"
        f"&limit={limit}"
    )

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

    for col in required:

        if col not in df.columns:

            raise ValueError(
                f"البيانات ناقصة: {col}"
            )

        df[col] = pd.to_numeric(
            df[col],
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

    if len(df) < MIN_BARS:

        raise ValueError(
            f"بيانات {interval} غير كافية "
            f"({len(df)} شمعة فقط)"
        )

    DATA_CACHE[key] = (
        now,
        df.copy()
    )

    return df


# =========================================================
# END OF PART 1
# =========================================================
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

    gain = delta.clip(lower=0)

    loss = -delta.clip(upper=0)

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

    return (
        100 - 100 / (1 + rs)
    ).fillna(50)


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

    histogram = line - signal_line

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

    true_range = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs()
        ],
        axis=1
    ).max(axis=1)

    return true_range.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()


def adx(
    df,
    period=14
):

    high = df["high"]

    low = df["low"]

    close = df["close"]

    up = high.diff()

    down = -low.diff()

    plus_dm = pd.Series(
        np.where(
            (up > down) & (up > 0),
            up,
            0.0
        ),
        index=df.index
    )

    minus_dm = pd.Series(
        np.where(
            (down > up) & (down > 0),
            down,
            0.0
        ),
        index=df.index
    )

    previous_close = close.shift(1)

    true_range = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs()
        ],
        axis=1
    ).max(axis=1)

    atr_s = true_range.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    plus_di = (
        100
        * plus_dm.ewm(
            alpha=1 / period,
            adjust=False
        ).mean()
        / atr_s.replace(0, np.nan)
    )

    minus_di = (
        100
        * minus_dm.ewm(
            alpha=1 / period,
            adjust=False
        ).mean()
        / atr_s.replace(0, np.nan)
    )

    denominator = (
        plus_di + minus_di
    ).replace(
        0,
        np.nan
    )

    dx = (
        100
        * (plus_di - minus_di).abs()
        / denominator
    ).fillna(0)

    adx_value = dx.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    return (
        adx_value,
        plus_di.fillna(0),
        minus_di.fillna(0)
    )


# =========================================================
# FIBONACCI
# =========================================================

def fibonacci_levels(
    df,
    lookback=80
):

    x = df.tail(
        min(
            lookback,
            len(df)
        )
    )

    high = safe_float(
        x["high"].max()
    )

    low = safe_float(
        x["low"].min()
    )

    span = high - low

    if span <= 0:
        return {}

    return {
        "0.0": high,
        "23.6": high - span * 0.236,
        "38.2": high - span * 0.382,
        "50.0": high - span * 0.500,
        "61.8": high - span * 0.618,
        "78.6": high - span * 0.786,
        "100.0": low
    }


# =========================================================
# CANDLE / WICK
# =========================================================

def candle_wick_features(row):

    open_price = safe_float(
        row["open"]
    )

    high_price = safe_float(
        row["high"]
    )

    low_price = safe_float(
        row["low"]
    )

    close_price = safe_float(
        row["close"]
    )

    candle_range = max(
        high_price - low_price,
        1e-9
    )

    upper_wick = (
        high_price
        - max(
            open_price,
            close_price
        )
    )

    lower_wick = (
        min(
            open_price,
            close_price
        )
        - low_price
    )

    body = abs(
        close_price - open_price
    )

    return {
        "range": candle_range,
        "body": body,
        "upper_wick": max(
            upper_wick,
            0
        ),
        "lower_wick": max(
            lower_wick,
            0
        ),
        "upper_ratio": max(
            upper_wick,
            0
        ) / candle_range,
        "lower_ratio": max(
            lower_wick,
            0
        ) / candle_range
    }


# =========================================================
# MARKET STRUCTURE
# =========================================================

def structure_state(
    df,
    lookback=8
):

    x = df.tail(
        max(
            lookback,
            5
        )
    )

    if len(x) < 5:
        return "🟡 مختلط"

    last_high = safe_float(
        x["high"].iloc[-1]
    )

    previous_high = safe_float(
        x["high"].iloc[-4]
    )

    last_low = safe_float(
        x["low"].iloc[-1]
    )

    previous_low = safe_float(
        x["low"].iloc[-4]
    )

    higher_high = (
        last_high > previous_high
    )

    higher_low = (
        last_low > previous_low
    )

    lower_high = (
        last_high < previous_high
    )

    lower_low = (
        last_low < previous_low
    )

    if higher_high and higher_low:
        return "🟢 HH/HL"

    if lower_high and lower_low:
        return "🔴 LH/LL"

    return "🟡 مختلط"


# =========================================================
# LIQUIDITY
# =========================================================

def liquidity_state(df):

    volume = (
        df["tickVolume"]
        .astype(float)
    )

    average_volume = safe_float(
        volume.tail(20).mean(),
        1
    )

    current_volume = safe_float(
        volume.iloc[-1]
    )

    ratio = (
        current_volume / average_volume
        if average_volume > 0
        else 0
    )

    if ratio >= 1.30:

        state = "🟢 قوية"

    elif ratio >= 0.90:

        state = "🟡 طبيعية"

    else:

        state = "🔴 ضعيفة"

    return (
        state,
        ratio
    )


# =========================================================
# VOLUME
# =========================================================

def volume_state(df):

    volume = (
        df["tickVolume"]
        .astype(float)
    )

    short_average = safe_float(
        volume.tail(5).mean()
    )

    base_average = safe_float(
        volume.tail(30).mean(),
        1
    )

    ratio = (
        short_average / base_average
        if base_average > 0
        else 0
    )

    if ratio >= 1.40:

        return (
            "🟢 توسع حجمي",
            ratio
        )

    if ratio >= 0.90:

        return (
            "🟡 طبيعي",
            ratio
        )

    return (
        "🔴 ضعيف",
        ratio
    )


# =========================================================
# CONFLICT
# =========================================================

def conflict_analysis(
    direction,
    higher,
    lower
):

    if (
        direction == "BUY"
        and higher == "SELL"
    ):

        return (
            "🔴 تعارض قوي: "
            "الإشارة عكس الاتجاه الأكبر"
        )

    if (
        direction == "SELL"
        and higher == "BUY"
    ):

        return (
            "🔴 تعارض قوي: "
            "الإشارة عكس الاتجاه الأكبر"
        )

    if higher != lower:

        return (
            "🟡 تعارض جزئي بين الفريمات"
        )

    return (
        "🟢 لا يوجد تعارض رئيسي"
    )


# =========================================================
# CMC
# =========================================================

def cmc_confirmation(
    _df,
    _direction
):

    # CMC غير مستخدم في القرار حاليًا.
    # لا نسمح لأي بيانات غير مؤكدة
    # باختراع BUY / SELL.

    if not CMC_ENABLED:

        return (
            0,
            "⚪ CMC غير مفعّل"
        )

    return (
        0,
        "⚪ CMC غير مفعّل"
    )


# =========================================================
# CORE FRAME ANALYSIS
# =========================================================

def analyze_frame(
    df,
    scalp=False
):

    if df is None:

        raise ValueError(
            "البيانات غير موجودة"
        )

    if len(df) < 60:

        raise ValueError(
            "البيانات غير كافية للتحليل"
        )

    close = df["close"]

    current = safe_float(
        close.iloc[-1]
    )

    # إعدادات مختلفة للسكالب
    fast_period = (
        9 if scalp else 20
    )

    mid_period = (
        20 if scalp else 50
    )

    long_period = (
        50 if scalp else 200
    )

    ema_fast_value = safe_float(
        ema(
            close,
            fast_period
        ).iloc[-1]
    )

    ema_mid_value = safe_float(
        ema(
            close,
            mid_period
        ).iloc[-1]
    )

    ema_long_value = safe_float(
        ema(
            close,
            long_period
        ).iloc[-1]
    )

    rsi_value = safe_float(
        rsi(
            close,
            9 if scalp else 14
        ).iloc[-1],
        50
    )

    if scalp:

        macd_line, signal_line, histogram = macd(
            close,
            5,
            13,
            4
        )

    else:

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

    adx_value, plus_di, minus_di = adx(
        df,
        14
    )

    adx_now = safe_float(
        adx_value.iloc[-1]
    )

    plus_now = safe_float(
        plus_di.iloc[-1]
    )

    minus_now = safe_float(
        minus_di.iloc[-1]
    )

    liquidity, liquidity_ratio = (
        liquidity_state(df)
    )

    volume, volume_ratio = (
        volume_state(df)
    )

    structure = structure_state(
        df
    )

    fibonacci = fibonacci_levels(
        df
    )

    wick = candle_wick_features(
        df.iloc[-1]
    )

    bull = 0.0

    bear = 0.0

    warnings = []

    reasons = []

    # =====================================================
    # TREND
    # =====================================================

    if (
        current > ema_fast_value
        and ema_fast_value > ema_mid_value
    ):

        bull += 22

        reasons.append(
            "اتجاه قصير/متوسط صاعد"
        )

        trend = "🟢 صاعد"

    elif (
        current < ema_fast_value
        and ema_fast_value < ema_mid_value
    ):

        bear += 22

        reasons.append(
            "اتجاه قصير/متوسط هابط"
        )

        trend = "🔴 هابط"

    else:

        trend = "🟡 متردد"

        warnings.append(
            "الاتجاه القصير غير مكتمل"
        )

    # =====================================================
    # LONG TREND
    # =====================================================

    if len(df) >= long_period:

        if current > ema_long_value:

            bull += 12

        elif current < ema_long_value:

            bear += 12

    # =====================================================
    # RSI
    # =====================================================

    if 50 <= rsi_value < 68:

        bull += 13

    elif 32 < rsi_value < 50:

        bear += 13

    elif rsi_value >= 68:

        warnings.append(
            "RSI مرتفع"
        )

    elif rsi_value <= 32:

        warnings.append(
            "RSI منخفض"
        )

    # =====================================================
    # MACD
    # =====================================================

    if (
        macd_value > signal_value
        and histogram_value >= 0
    ):

        bull += 13

    elif (
        macd_value < signal_value
        and histogram_value <= 0
    ):

        bear += 13

    else:

        warnings.append(
            "MACD غير حاسم"
        )

    # =====================================================
    # ADX
    # =====================================================

    if adx_now >= 25:

        regime = "🟢 اتجاهي"

        if plus_now > minus_now:

            bull += 10

        elif minus_now > plus_now:

            bear += 10

    elif adx_now >= 18:

        regime = "🟡 اتجاه ضعيف"

        warnings.append(
            "ADX متوسط"
        )

    else:

        regime = "🟠 عرضي/غير مؤكد"

        warnings.append(
            "ADX منخفض"
        )

    # =====================================================
    # STRUCTURE
    # =====================================================

    if structure == "🟢 HH/HL":

        bull += 10

    elif structure == "🔴 LH/LL":

        bear += 10

    else:

        warnings.append(
            "هيكل السوق مختلط"
        )

    # =====================================================
    # LIQUIDITY
    # =====================================================

    if liquidity_ratio >= 1.20:

        if bull > bear:

            bull += 7

        elif bear > bull:

            bear += 7

    elif liquidity_ratio < 0.80:

        warnings.append(
            "السيولة/الحجم النسبي ضعيف"
        )

    # =====================================================
    # VOLUME
    # =====================================================

    if volume_ratio >= 1.20:

        if bull > bear:

            bull += 5

        elif bear > bull:

            bear += 5

    # =====================================================
    # WICK
    # =====================================================

    wick_bias = "neutral"

    if (
        wick["lower_ratio"] >= 0.45
        and wick["upper_ratio"] < 0.25
    ):

        wick_bias = (
            "bullish_rejection"
        )

        bull += 6

    elif (
        wick["upper_ratio"] >= 0.45
        and wick["lower_ratio"] < 0.25
    ):

        wick_bias = (
            "bearish_rejection"
        )

        bear += 6

    # =====================================================
    # FIBONACCI
    # =====================================================

    fib_bonus = 0

    if fibonacci:

        nearest = min(
            fibonacci.values(),
            key=lambda x:
                abs(x - current)
        )

        if (
            atr_value > 0
            and abs(
                nearest - current
            ) <= atr_value * 0.30
        ):

            fib_bonus = 5

            if current >= nearest:

                bull += 2.5

            else:

                bear += 2.5

    # =====================================================
    # CMC
    # =====================================================

    cmc_score, cmc_state = (
        cmc_confirmation(
            df,
            "BUY"
            if bull >= bear
            else "SELL"
        )
    )

    if cmc_score > 0:

        if bull >= bear:

            bull += cmc_score

        else:

            bear += cmc_score

    # =====================================================
    # FINAL DIRECTION
    # =====================================================

    if bull > bear:

        direction = "BUY"

        raw_score = bull

    elif bear > bull:

        direction = "SELL"

        raw_score = bear

    else:

        direction = "WAIT"

        raw_score = 0

    score = int(
        max(
            0,
            min(
                round(raw_score),
                100
            )
        )
    )

    # =====================================================
    # SIGNAL STATE
    # =====================================================

    if score >= 80:

        state = "🟢 قوية"

    elif score >= TRADE_THRESHOLD:

        state = "🟢 مؤهلة"

    elif score >= 65:

        state = "🟡 مراقبة"

    else:

        state = "🟠 ضعيفة"

    # =====================================================
    # EXTENSION
    # =====================================================

    extended = (
        atr_value > 0
        and abs(
            current - ema_fast_value
        ) > atr_value * 0.60
    )

    if extended:

        warnings.append(
            "السعر ممتد"
        )

    return {

        "price": current,

        "ema_fast": ema_fast_value,

        "ema_mid": ema_mid_value,

        "ema_long": ema_long_value,

        "rsi": rsi_value,

        "macd": macd_value,

        "macd_signal": signal_value,

        "macd_histogram": histogram_value,

        "atr": atr_value,

        "adx": adx_now,

        "plus_di": plus_now,

        "minus_di": minus_now,

        "trend": trend,

        "direction": direction,

        "score": score,

        "warnings": warnings,

        "reasons": reasons,

        "state": state,

        "extended": extended,

        "liquidity": liquidity,

        "liquidity_ratio": liquidity_ratio,

        "volume_state": volume,

        "volume_ratio": volume_ratio,

        "structure": structure,

        "wick": wick,

        "wick_bias": wick_bias,

        "fib": fibonacci,

        "fib_bonus": fib_bonus,

        "regime": regime,

        "cmc": cmc_state
    }


# =========================================================
# END OF PART 2
# =========================================================
