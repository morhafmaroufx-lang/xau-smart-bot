# ============================================================
# XAU SMART TRADER v17.1
# Structural Liquidity + Quantitative Momentum
# واجهة عربية بالكامل - توقيت دمشق
# ============================================================

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
    filters,
)

# ============================================================
# الإعدادات
# ============================================================

VERSION = "v17.1"

TOKEN = os.environ.get("TELEGRAM_TOKEN")
PORT = int(os.environ.get("PORT", "10000"))

RENDER_URL = os.environ.get(
    "RENDER_EXTERNAL_URL",
    "https://xau-smart-bot.onrender.com"
).rstrip("/")

WEBHOOK_PATH = "/telegram-webhook"
WEBHOOK_URL = RENDER_URL + WEBHOOK_PATH

SYMBOL = "XAUUSD"

DATA_URL = "https://biquote.io/api/XAUUSD/ohlc"
LIVE_URL = "https://biquote.io/api/XAUUSD"

DAMASCUS = ZoneInfo("Asia/Damascus")
NEW_YORK = ZoneInfo("America/New_York")

CACHE_SECONDS = 20
AUTO_SCAN_SECONDS = 60
MIN_BARS = 30
# ------------------------------------------------------------
# أهم تعديل:
# النظام ليس صارماً جداً.
#
# 60 = مراقبة
# 68 = مرشح
# 73 = إشارة مؤهلة
# 80 = قوية
# ------------------------------------------------------------

SIGNAL_THRESHOLD = 68
STRONG_THRESHOLD = 80

# السماح بالإشارة إذا تحققت معظم عوامل التلاقي.
# لا نشترط تحقق كل المؤشرات في نفس اللحظة.

AUTO_ENABLED = True

# فلتر الأخبار:
# يمكن تعطيله من Environment Variables عند الحاجة.
NEWS_FILTER_ENABLED = os.environ.get(
    "NEWS_FILTER_ENABLED", "true"
).lower() == "true"

NEWS_BEFORE_MIN = 30
NEWS_AFTER_MIN = 30

# ============================================================
# Flask
# ============================================================

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# ============================================================
# الحالة
# ============================================================

APPLICATION = None
BOT_LOOP = None

SUBSCRIBERS = set()

DATA_CACHE = {}

LAST_SIGNAL = {}
LAST_SIGNAL_TIME = {}

COMMAND_MESSAGES = {}
COMMAND_LOCKS = {}

NEWS_CACHE = {
    "time": 0,
    "events": []
}

# ============================================================
# الصحة
# ============================================================

@app.route("/", methods=["GET", "HEAD"])
def home():
    return f"XAU SMART TRADER {VERSION} - OK", 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "bot": VERSION,
        "symbol": SYMBOL,
        "timezone": "Asia/Damascus",
        "time": datetime.now(DAMASCUS).isoformat()
    }), 200


# ============================================================
# أدوات عامة
# ============================================================

def sf(value, default=0.0):
    try:
        x = float(value)
        if np.isfinite(x):
            return x
    except Exception:
        pass
    return default


def now_damascus():
    return datetime.now(DAMASCUS)


def fmt(value):
    if value is None:
        return "غير متوفر"
    return f"{sf(value):.2f}"


# ============================================================
# البيانات
# ============================================================

def get_bars(interval, limit=300):
    """
    جلب بيانات الذهب من Biquote.

    Biquote لا يدعم 1w مباشرة.
    لذلك يتم بناء W1 محلياً من بيانات D1.
    """

    # =====================================================
    # W1 — بناء أسبوعي محلي من D1
    # =====================================================
    if interval == "1w":

        cache_key_name = f"1w_{limit}"
        now = time.time()

        cached = DATA_CACHE.get(cache_key_name)

        if cached and now - cached[0] < CACHE_SECONDS:
            return cached[1].copy()

        # جلب D1 فقط
        daily_limit = min(
            max(limit * 7 + 30, 100),
            1000
        )

        daily_df = get_bars(
            "1d",
            daily_limit
        )

        if daily_df is None or daily_df.empty:
            raise ValueError(
                "لا توجد بيانات يومية لبناء الفريم الأسبوعي."
            )

        df = daily_df.copy()

        # -------------------------------------------------
        # الوقت
        # -------------------------------------------------
        if "openTime" not in df.columns:
            raise ValueError(
                "بيانات D1 لا تحتوي على openTime."
            )

        df["openTime"] = pd.to_datetime(
            df["openTime"],
            utc=True,
            errors="coerce"
        )

        df = df.dropna(
            subset=["openTime"]
        )

        df = df.set_index(
            "openTime"
        )

        df = df.sort_index()

        # -------------------------------------------------
        # الأعمدة الأساسية
        # -------------------------------------------------
        required = [
            "open",
            "high",
            "low",
            "close"
        ]

        for col in required:

            if col not in df.columns:
                raise ValueError(
                    f"بيانات D1 ناقصة: {col}"
                )

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        # -------------------------------------------------
        # Tick Volume
        # -------------------------------------------------
        if "tickVolume" in df.columns:

            df["tickVolume"] = pd.to_numeric(
                df["tickVolume"],
                errors="coerce"
            ).fillna(0)

        else:

            df["tickVolume"] = 0

        df = df.dropna(
            subset=[
                "open",
                "high",
                "low",
                "close"
            ]
        )

        if len(df) < MIN_BARS:
            raise ValueError(
                f"بيانات D1 غير كافية لبناء W1: "
                f"{len(df)} شمعة."
            )

        # -------------------------------------------------
        # بناء الشموع الأسبوعية
        # -------------------------------------------------
        weekly = pd.DataFrame(index=df.resample("W-SUN").size().index)

        weekly["open"] = (
            df["open"]
            .resample("W-SUN")
            .first()
        )

        weekly["high"] = (
            df["high"]
            .resample("W-SUN")
            .max()
        )

        weekly["low"] = (
            df["low"]
            .resample("W-SUN")
            .min()
        )

        weekly["close"] = (
            df["close"]
            .resample("W-SUN")
            .last()
        )

        weekly["tickVolume"] = (
            df["tickVolume"]
            .resample("W-SUN")
            .sum()
        )

        weekly = weekly.dropna(
            subset=[
                "open",
                "high",
                "low",
                "close"
            ]
        )

        weekly = weekly.tail(
            limit
        )

        if len(weekly) < MIN_BARS:
            raise ValueError(
                f"البيانات الأسبوعية غير كافية: "
                f"{len(weekly)} شمعة."
            )

        weekly = weekly.reset_index()

        weekly.rename(
            columns={
                weekly.columns[0]: "openTime"
            },
            inplace=True
        )

        DATA_CACHE[cache_key_name] = (
            now,
            weekly.copy()
        )

        return weekly.copy()

    # =====================================================
    # الفريمات التي يدعمها Biquote مباشرة
    # =====================================================
    supported_intervals = {
        "1m",
        "5m",
        "15m",
        "30m",
        "1h",
        "4h",
        "1d"
    }

    if interval not in supported_intervals:

        raise ValueError(
            f"الفريم {interval} غير مدعوم."
        )

    # =====================================================
    # Cache
    # =====================================================
    key = cache_key(
        interval,
        limit
    )

    now = time.time()

    cached = DATA_CACHE.get(key)

    if cached and now - cached[0] < CACHE_SECONDS:
        return cached[1].copy()

    # =====================================================
    # طلب Biquote
    # =====================================================
    try:

        response = requests.get(
            DATA_URL,
            params={
                "interval": interval,
                "limit": min(
                    int(limit),
                    1000
                )
            },
            timeout=15
        )

        # إذا حدث خطأ، أظهر رد Biquote الحقيقي
        if not response.ok:

            try:
                error_data = response.json()

            except Exception:
                error_data = response.text[:500]

            raise RuntimeError(
                f"Biquote HTTP {response.status_code}: "
                f"{error_data}"
            )

        data = response.json()

    except requests.RequestException as exc:

        raise RuntimeError(
            f"تعذر الاتصال بمصدر بيانات الذهب "
            f"للفريم {interval}: {exc}"
        ) from exc

    # =====================================================
    # استخراج الشموع
    # =====================================================
    if not isinstance(data, dict):

        raise ValueError(
            f"استجابة Biquote غير متوقعة للفريم {interval}."
        )

    bars = data.get(
        "bars",
        []
    )

    if not bars:

        raise ValueError(
            f"Biquote لم يعط بيانات للفريم {interval}."
        )

    df = pd.DataFrame(
        bars
    )

    required = [
        "open",
        "high",
        "low",
        "close"
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

    # =====================================================
    # Tick Volume
    # =====================================================
    if "tickVolume" in df.columns:

        df["tickVolume"] = pd.to_numeric(
            df["tickVolume"],
            errors="coerce"
        ).fillna(0)

    else:

        df["tickVolume"] = 0

    # =====================================================
    # الوقت
    # =====================================================
    if "openTime" in df.columns:

        df["openTime"] = pd.to_datetime(
            df["openTime"],
            utc=True,
            errors="coerce"
        )

        df = df.dropna(
            subset=["openTime"]
        )

        df = df.sort_values(
            "openTime"
        )

    # =====================================================
    # تنظيف
    # =====================================================
    df = df.dropna(
        subset=[
            "open",
            "high",
            "low",
            "close"
        ]
    )

    if len(df) < MIN_BARS:

        raise ValueError(
            f"البيانات غير كافية للفريم {interval}: "
            f"{len(df)} شمعة."
        )

    DATA_CACHE[key] = (
        now,
        df.copy()
    )

    return df.copy()

    # =====================================================
    # معالجة الفريم الأسبوعي
    # =====================================================
    if interval == "1w":
        cache_key_name = f"1w_{limit}"
        now = time.time()

        cached = DATA_CACHE.get(cache_key_name)
        if cached and now - cached[0] < CACHE_SECONDS:
            return cached[1].copy()

        # نحتاج بيانات يومية كافية لبناء الأسابيع
        daily_limit = min(max(limit * 7 + 30, 100), 1000)

        daily_df = get_bars("1d", daily_limit)

        if daily_df is None or daily_df.empty:
            raise ValueError("لا توجد بيانات يومية لبناء البيانات الأسبوعية")

        df = daily_df.copy()

        # التأكد من وجود الوقت
        if "openTime" in df.columns:
            df["openTime"] = pd.to_datetime(
                df["openTime"],
                utc=True,
                errors="coerce"
            )
            df = df.dropna(subset=["openTime"])
            df = df.set_index("openTime")

        elif not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("بيانات D1 لا تحتوي على وقت صالح")

        # ترتيب زمني صحيح
        df = df.sort_index()

        # التأكد من الأعمدة المطلوبة
        required = ["open", "high", "low", "close"]

        for col in required:
            if col not in df.columns:
                raise ValueError(
                    f"بيانات D1 ناقصة لبناء W1: {col}"
                )

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        # tickVolume اختياري
        if "tickVolume" in df.columns:
            df["tickVolume"] = pd.to_numeric(
                df["tickVolume"],
                errors="coerce"
            ).fillna(0)

        # =================================================
        # بناء الشموع الأسبوعية
        # =================================================
        weekly = pd.DataFrame()

        weekly["open"] = df["open"].resample("W-SUN").first()
        weekly["high"] = df["high"].resample("W-SUN").max()
        weekly["low"] = df["low"].resample("W-SUN").min()
        weekly["close"] = df["close"].resample("W-SUN").last()

        if "tickVolume" in df.columns:
            weekly["tickVolume"] = (
                df["tickVolume"].resample("W-SUN").sum()
            )
        else:
            weekly["tickVolume"] = 0

        weekly = weekly.dropna(
            subset=["open", "high", "low", "close"]
        )

        # نحتفظ بعدد الأسابيع المطلوبة
        weekly = weekly.tail(limit)

        if len(weekly) < MIN_BARS:
            raise ValueError(
                f"البيانات الأسبوعية غير كافية: "
                f"{len(weekly)} شمعة فقط"
            )

        # إعادة openTime كعمود
        weekly = weekly.reset_index()

        # توحيد اسم الوقت
        if "openTime" not in weekly.columns:
            weekly.rename(
                columns={weekly.columns[0]: "openTime"},
                inplace=True
            )

        DATA_CACHE[cache_key_name] = (
            now,
            weekly.copy()
        )

        return weekly.copy()

    # =====================================================
    # الفريمات المدعومة مباشرة من Biquote
    # =====================================================
    supported_intervals = {
        "1m",
        "5m",
        "15m",
        "30m",
        "1h",
        "4h",
        "1d"
    }

    if interval not in supported_intervals:
        raise ValueError(
            f"الفريم {interval} غير مدعوم من مصدر البيانات"
        )

    key = cache_key(interval, limit)
    now = time.time()

    cached = DATA_CACHE.get(key)

    if cached and now - cached[0] < CACHE_SECONDS:
        return cached[1].copy()

    try:
        response = requests.get(
            DATA_URL,
            params={
                "interval": interval,
                "limit": min(limit, 1000)
            },
            timeout=15
        )

        response.raise_for_status()

        data = response.json()

    except requests.RequestException as exc:
        raise RuntimeError(
            f"تعذر الاتصال بمصدر بيانات الذهب "
            f"للفريم {interval}: {exc}"
        ) from exc

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
        "close"
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

    # حجم التداول الحقيقي غير متوفر عادةً للذهب CFD،
    # لذلك نعتمد على Tick Volume.
    if "tickVolume" in df.columns:
        df["tickVolume"] = pd.to_numeric(
            df["tickVolume"],
            errors="coerce"
        ).fillna(0)
    else:
        df["tickVolume"] = 0

    # الوقت
    if "openTime" in df.columns:
        df["openTime"] = pd.to_datetime(
            df["openTime"],
            utc=True,
            errors="coerce"
        )

        df = df.dropna(
            subset=["openTime"]
        )

    # تنظيف
    df = df.dropna(
        subset=[
            "open",
            "high",
            "low",
            "close"
        ]
    )

    # ترتيب زمني
    if "openTime" in df.columns:
        df = df.sort_values(
            "openTime"
        )

    if len(df) < MIN_BARS:
        raise ValueError(
            f"البيانات غير كافية للفريم {interval}: "
            f"{len(df)} شمعة"
        )

    DATA_CACHE[key] = (
        now,
        df.copy()
    )

    return df.copy()


# ============================================================
# السعر اللحظي
# ============================================================

def live_price():
    errors = []

    # المصدر الأول
    try:
        r = requests.get(
            LIVE_URL,
            params={"allowStale": "false"},
            timeout=8
        )

        if r.ok:
            data = r.json()

            if isinstance(data, dict):

                price = sf(
                    data.get("mid"),
                    None
                )

                if price is None:
                    bid = sf(
                        data.get("bid"),
                        None
                    )
                    ask = sf(
                        data.get("ask"),
                        None
                    )

                    if bid and ask:
                        price = (bid + ask) / 2

                if price and price > 0:

                    return {
                        "price": price,
                        "source": "Biquote",
                        "age": data.get(
                            "quoteAgeSeconds"
                        )
                    }

    except Exception as e:
        errors.append(str(e))

    # مصدر بديل
    try:
        r = requests.get(
            "https://xaus.com/api/v1/spot",
            timeout=8
        )

        if r.ok:
            data = r.json()

            price = sf(
                data.get("spot_usd_oz"),
                None
            )

            if price and price > 0:

                state = data.get(
                    "data_state",
                    {}
                )

                return {
                    "price": price,
                    "source": "XAUS",
                    "age": state.get(
                        "age_seconds"
                    )
                }

    except Exception as e:
        errors.append(str(e))

    raise RuntimeError(
        "تعذر الحصول على السعر اللحظي: "
        + " | ".join(errors)
    )


# ============================================================
# المؤشرات
# ============================================================

def EMA(s, n):
    return s.ewm(
        span=n,
        adjust=False
    ).mean()


def RSI(s, n=14):
    delta = s.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(
        alpha=1 / n,
        adjust=False
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / n,
        adjust=False
    ).mean()

    rs = avg_gain / avg_loss.replace(
        0,
        np.nan
    )

    return (
        100 - 100 / (1 + rs)
    ).fillna(50)


def MACD(
    s,
    fast=8,
    slow=21,
    signal=5
):
    fast_line = EMA(s, fast)
    slow_line = EMA(s, slow)

    line = fast_line - slow_line
    sig = EMA(line, signal)

    hist = line - sig

    return line, sig, hist


def ATR(df, n=14):
    close = df["close"]

    prev = close.shift(1)

    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev).abs(),
            (df["low"] - prev).abs()
        ],
        axis=1
    ).max(axis=1)

    return tr.ewm(
        alpha=1 / n,
        adjust=False
    ).mean()


def ADX(df, n=14):

    high = df["high"]
    low = df["low"]
    close = df["close"]

    up = high.diff()
    down = -low.diff()

    plus_dm = pd.Series(
        np.where(
            (up > down) & (up > 0),
            up,
            0
        ),
        index=df.index
    )

    minus_dm = pd.Series(
        np.where(
            (down > up) & (down > 0),
            down,
            0
        ),
        index=df.index
    )

    prev = close.shift(1)

    tr = pd.concat(
        [
            high - low,
            (high - prev).abs(),
            (low - prev).abs()
        ],
        axis=1
    ).max(axis=1)

    atr = tr.ewm(
        alpha=1 / n,
        adjust=False
    ).mean()

    plus_di = (
        100 *
        plus_dm.ewm(
            alpha=1 / n,
            adjust=False
        ).mean()
        / atr
    )

    minus_di = (
        100 *
        minus_dm.ewm(
            alpha=1 / n,
            adjust=False
        ).mean()
        / atr
    )

    dx = (
        100 *
        (plus_di - minus_di).abs()
        /
        (plus_di + minus_di)
        .replace(0, np.nan)
    ).fillna(0)

    return dx.ewm(
        alpha=1 / n,
        adjust=False
    ).mean()


# ============================================================
# هيكل السوق
# ============================================================

def structure(df):

    if len(df) < 10:
        return "محايد"

    h_now = sf(df["high"].iloc[-1])
    h_old = sf(df["high"].iloc[-5])

    l_now = sf(df["low"].iloc[-1])
    l_old = sf(df["low"].iloc[-5])

    if h_now > h_old and l_now > l_old:
        return "صاعد"

    if h_now < h_old and l_now < l_old:
        return "هابط"

    return "محايد"


# ============================================================
# السيولة الحجمية
# ============================================================

def volume_analysis(df):

    volume = df["tickVolume"].astype(float)

    current = sf(
        volume.iloc[-1]
    )

    average = sf(
        volume.tail(20).mean(),
        1
    )

    ratio = (
        current / average
        if average > 0
        else 0
    )

    if ratio >= 1.40:
        state = "قوية جداً"
    elif ratio >= 1.10:
        state = "قوية"
    elif ratio >= 0.85:
        state = "طبيعية"
    else:
        state = "ضعيفة"

    return state, ratio


# ============================================================
# فيبوناتشي
# ============================================================

def fibonacci(df, lookback=120):

    x = df.tail(
        min(lookback, len(df))
    )

    high = sf(x["high"].max())
    low = sf(x["low"].min())

    span = high - low

    if span <= 0:
        return {}

    return {
        "0": high,
        "23.6": high - span * 0.236,
        "38.2": high - span * 0.382,
        "50": high - span * 0.500,
        "61.8": high - span * 0.618,
        "78.6": high - span * 0.786,
        "100": low
    }


# ============================================================
# FVG
# ============================================================

def find_fvg(df):

    if len(df) < 5:
        return None

    a = df.iloc[-3]
    b = df.iloc[-2]
    c = df.iloc[-1]

    # فجوة صاعدة
    if sf(c["low"]) > sf(a["high"]):

        return {
            "type": "صاعدة",
            "low": sf(a["high"]),
            "high": sf(c["low"])
        }

    # فجوة هابطة
    if sf(c["high"]) < sf(a["low"]):

        return {
            "type": "هابطة",
            "low": sf(c["high"]),
            "high": sf(a["low"])
        }

    return None


# ============================================================
# مناطق الدعم والمقاومة
# ============================================================

def support_resistance(
    df,
    lookback=120
):

    x = df.tail(
        min(lookback, len(df))
    )

    current = sf(
        x["close"].iloc[-1]
    )

    atr = sf(
        ATR(x).iloc[-1],
        1
    )

    radius = max(
        atr * 0.35,
        current * 0.00035
    )

    supports = []
    resistances = []

    for i in range(2, len(x) - 2):

        low = sf(
            x["low"].iloc[i]
        )

        high = sf(
            x["high"].iloc[i]
        )

        left_low = sf(
            x["low"].iloc[i-2:i].min()
        )

        right_low = sf(
            x["low"].iloc[i+1:i+3].min()
        )

        left_high = sf(
            x["high"].iloc[i-2:i].max()
        )

        right_high = sf(
            x["high"].iloc[i+1:i+3].max()
        )

        # دعم
        if (
            low <= left_low
            and low <= right_low
            and low < current
        ):
            supports.append(low)

        # مقاومة
        if (
            high >= left_high
            and high >= right_high
            and high > current
        ):
            resistances.append(high)

    def cluster(values):

        values = sorted(values)

        result = []

        for price in values:

            if not result:
                result.append([price])
                continue

            center = sum(
                result[-1]
            ) / len(result[-1])

            if abs(
                price - center
            ) <= radius:

                result[-1].append(price)

            else:
                result.append([price])

        zones = []

        for group in result:

            center = sum(group) / len(group)

            strength = min(
                100,
                40 + len(group) * 15
            )

            zones.append({
                "price": center,
                "strength": strength,
                "touches": len(group)
            })

        return zones

    s = cluster(supports)
    r = cluster(resistances)

    s = sorted(
        s,
        key=lambda z: abs(
            current - z["price"]
        )
    )[:3]

    r = sorted(
        r,
        key=lambda z: abs(
            current - z["price"]
        )
    )[:3]

    return {
        "support1": s[0] if len(s) > 0 else None,
        "support2": s[1] if len(s) > 1 else None,
        "support3": s[2] if len(s) > 2 else None,
        "resistance1": r[0] if len(r) > 0 else None,
        "resistance2": r[1] if len(r) > 1 else None,
        "resistance3": r[2] if len(r) > 2 else None,
        "atr": atr
    }


# ============================================================
# تحليل فريم واحد
# ============================================================

def analyze(df):

    close = df["close"]

    price = sf(
        close.iloc[-1]
    )

    ema50 = sf(
        EMA(close, 50).iloc[-1]
    )

    ema200 = sf(
        EMA(close, 200).iloc[-1]
    )

    rsi = sf(
        RSI(close, 14).iloc[-1],
        50
    )

    macd_line, macd_sig, macd_hist = MACD(
        close
    )

    ml = sf(
        macd_line.iloc[-1]
    )

    ms = sf(
        macd_sig.iloc[-1]
    )

    mh = sf(
        macd_hist.iloc[-1]
    )

    adx = sf(
        ADX(df).iloc[-1]
    )

    struct = structure(df)

    vol_state, vol_ratio = volume_analysis(
        df
    )

    fib = fibonacci(df)

    fvg = find_fvg(df)

    atr = sf(
        ATR(df).iloc[-1],
        1
    )

    bull = 0
    bear = 0

    reasons = []

    # الاتجاه
    if price > ema50:
        bull += 12
        reasons.append(
            "السعر فوق EMA50"
        )

    if price < ema50:
        bear += 12
        reasons.append(
            "السعر تحت EMA50"
        )

    # EMA200
    if price > ema200:
        bull += 15
        reasons.append(
            "السعر فوق EMA200"
        )

    elif price < ema200:
        bear += 15
        reasons.append(
            "السعر تحت EMA200"
        )

    # RSI
    if rsi < 30:
        bull += 15
        reasons.append(
            "RSI تشبع بيعي"
        )

    elif rsi > 70:
        bear += 15
        reasons.append(
            "RSI تشبع شرائي"
        )

    elif rsi > 50:
        bull += 7

    elif rsi < 50:
        bear += 7

    # MACD
    if ml > ms:
        bull += 12

        if mh > 0:
            bull += 4

    elif ml < ms:
        bear += 12

        if mh < 0:
            bear += 4

    # ADX
    if adx >= 25:

        if bull > bear:
            bull += 8

        elif bear > bull:
            bear += 8

    elif adx >= 18:

        if bull > bear:
            bull += 4

        elif bear > bull:
            bear += 4

    # الهيكل
    if struct == "صاعد":
        bull += 12
        reasons.append(
            "هيكل السوق صاعد"
        )

    elif struct == "هابط":
        bear += 12
        reasons.append(
            "هيكل السوق هابط"
        )

    # الحجم
    if vol_ratio >= 1.20:

        if bull > bear:
            bull += 8
            reasons.append(
                "توسع حجمي داعم للشراء"
            )

        elif bear > bull:
            bear += 8
            reasons.append(
                "توسع حجمي داعم للبيع"
            )

    elif vol_ratio >= 0.90:

        if bull > bear:
            bull += 3

        elif bear > bull:
            bear += 3

    # النتيجة
    if bull > bear:

        direction = "BUY"
        score = bull

    elif bear > bull:

        direction = "SELL"
        score = bear

    else:

        direction = "WAIT"
        score = 0

    score = int(
        max(
            0,
            min(
                round(score),
                100
            )
        )
    )

    if score >= 80:
        state = "قوية"

    elif score >= 68:
        state = "مؤهلة"

    elif score >= 60:
        state = "مراقبة"

    else:
        state = "ضعيفة"

    return {
        "price": price,
        "ema50": ema50,
        "ema200": ema200,
        "rsi": rsi,
        "macd": ml,
        "macd_signal": ms,
        "macd_hist": mh,
        "adx": adx,
        "structure": struct,
        "volume_state": vol_state,
        "volume_ratio": vol_ratio,
        "fib": fib,
        "fvg": fvg,
        "atr": atr,
        "direction": direction,
        "score": score,
        "state": state,
        "reasons": reasons
    }


# ============================================================
# التحليل متعدد الفريمات
# ============================================================

def multi_timeframe():

    w1 = analyze(
        get_bars("1w", 250)
    )

    d1 = analyze(
        get_bars("1d", 300)
    )

    h4 = analyze(
        get_bars("4h", 300)
    )

    h1 = analyze(
        get_bars("1h", 300)
    )

    m15 = analyze(
        get_bars("15m", 300)
    )

    # أوزان التحليل
    score_buy = (
        w1["score"] * 0.15
        + d1["score"] * 0.25
        + h4["score"] * 0.25
        + h1["score"] * 0.20
        + m15["score"] * 0.15
    )

    score_sell = score_buy

    directions = [
        w1["direction"],
        d1["direction"],
        h4["direction"],
        h1["direction"],
        m15["direction"]
    ]

    buy_count = directions.count(
        "BUY"
    )

    sell_count = directions.count(
        "SELL"
    )

    if buy_count > sell_count:
        final_direction = "BUY"

    elif sell_count > buy_count:
        final_direction = "SELL"

    else:
        final_direction = "WAIT"

    # إعادة حساب قوة التوافق حسب الاتجاه
    selected_scores = []

    for x in [w1, d1, h4, h1, m15]:

        if x["direction"] == final_direction:
            selected_scores.append(
                x["score"]
            )

    if selected_scores:

        final_score = int(
            np.mean(
                selected_scores
            )
        )

    else:

        final_score = int(
            max(
                w1["score"],
                d1["score"],
                h4["score"],
                h1["score"],
                m15["score"]
            ) * 0.75
        )

    return {
        "w1": w1,
        "d1": d1,
        "h4": h4,
        "h1": h1,
        "m15": m15,
        "direction": final_direction,
        "score": min(
            final_score,
            100
        )
    }


# ============================================================
# فلتر فيبوناتشي والسيولة
# ============================================================

def fibonacci_confluence(
    direction,
    h1,
    levels
):

    if not levels:
        return False

    price = h1["price"]
    atr = max(
        h1["atr"],
        0.10
    )

    fib = h1["fib"]

    if not fib:
        return False

    level = fib.get("61.8")

    if level is None:
        return False

    near = abs(
        price - level
    ) <= atr * 0.60

    if direction == "BUY":

        return near and price >= level - atr

    if direction == "SELL":

        return near and price <= level + atr

    return False


# ============================================================
# بناء الصفقة
# ============================================================

def build_trade(
    direction,
    h1,
    m15,
    levels
):

    price = m15["price"]

    atr = max(
        m15["atr"],
        0.50
    )

    s1 = levels.get(
        "support1"
    )

    s2 = levels.get(
        "support2"
    )

    r1 = levels.get(
        "resistance1"
    )

    r2 = levels.get(
        "resistance2"
    )

    if direction == "BUY":

        entry = price

        if s1:
            sl = min(
                entry - atr * 1.10,
                s1["price"] - atr * 0.20
            )
        else:
            sl = entry - atr * 1.20

        if r1 and r1["price"] > entry:
            tp1 = r1["price"]
        else:
            tp1 = entry + atr * 1.50

        if r2 and r2["price"] > tp1:
            tp2 = r2["price"]
        else:
            tp2 = entry + atr * 2.50

    elif direction == "SELL":

        entry = price

        if r1:
            sl = max(
                entry + atr * 1.10,
                r1["price"] + atr * 0.20
            )
        else:
            sl = entry + atr * 1.20

        if s1 and s1["price"] < entry:
            tp1 = s1["price"]
        else:
            tp1 = entry - atr * 1.50

        if s2 and s2["price"] < tp1:
            tp2 = s2["price"]
        else:
            tp2 = entry - atr * 2.50

    else:

        return None

    risk = abs(
        entry - sl
    )

    reward = abs(
        tp2 - entry
    )

    rr = (
        reward / risk
        if risk > 0
        else 0
    )

    return {
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "rr": rr
    }


# ============================================================
# تقييم الإشارة
# ============================================================

def evaluate_signal():

    mtf = multi_timeframe()

    direction = mtf["direction"]
    score = mtf["score"]

    d1 = mtf["d1"]
    h4 = mtf["h4"]
    h1 = mtf["h1"]
    m15 = mtf["m15"]

    quote = live_price()

    price = quote["price"]

    levels = support_resistance(
        get_bars("1h", 250)
    )

    # ----------------------------------------
    # عوامل التلاقي
    # ----------------------------------------

    confluence = 0
    factors = []

    # الاتجاه الكبير
    if direction == "BUY":

        if d1["price"] > d1["ema200"]:
            confluence += 15
            factors.append(
                "الاتجاه اليومي فوق EMA200"
            )

        if h4["price"] > h4["ema200"]:
            confluence += 15
            factors.append(
                "H4 فوق EMA200"
            )

    elif direction == "SELL":

        if d1["price"] < d1["ema200"]:
            confluence += 15
            factors.append(
                "الاتجاه اليومي تحت EMA200"
            )

        if h4["price"] < h4["ema200"]:
            confluence += 15
            factors.append(
                "H4 تحت EMA200"
            )

    # الهيكل
    if direction == "BUY" and h4["structure"] == "صاعد":
        confluence += 12
        factors.append(
            "هيكل H4 صاعد"
        )

    elif direction == "SELL" and h4["structure"] == "هابط":
        confluence += 12
        factors.append(
            "هيكل H4 هابط"
        )

    # RSI
    if direction == "BUY":

        if m15["rsi"] <= 35:
            confluence += 12
            factors.append(
                "RSI M15 في منطقة تشبع بيعي"
            )

        elif m15["rsi"] < 45:
            confluence += 6
            factors.append(
                "RSI M15 منخفض"
            )

    elif direction == "SELL":

        if m15["rsi"] >= 65:
            confluence += 12
            factors.append(
                "RSI M15 في منطقة تشبع شرائي"
            )

        elif m15["rsi"] > 55:
            confluence += 6
            factors.append(
                "RSI M15 مرتفع"
            )

    # MACD
    if direction == "BUY":

        if (
            m15["macd"]
            > m15["macd_signal"]
        ):
            confluence += 10
            factors.append(
                "تقاطع MACD إيجابي"
            )

    elif direction == "SELL":

        if (
            m15["macd"]
            < m15["macd_signal"]
        ):
            confluence += 10
            factors.append(
                "تقاطع MACD سلبي"
            )

    # الحجم
    if m15["volume_ratio"] >= 1.10:

        confluence += 10

        factors.append(
            "الحجم أعلى من الطبيعي"
        )

    # فيبوناتشي
    fib_ok = fibonacci_confluence(
        direction,
        h1,
        levels
    )

    if fib_ok:

        confluence += 12

        factors.append(
            "تلاقي مع فيبوناتشي 61.8%"
        )

    # FVG
    if m15["fvg"]:

        fvg = m15["fvg"]

        if direction == "BUY" and fvg["type"] == "صاعدة":

            confluence += 5

            factors.append(
                "وجود FVG صاعدة"
            )

        elif direction == "SELL" and fvg["type"] == "هابطة":

            confluence += 5

            factors.append(
                "وجود FVG هابطة"
            )

    # الدرجة النهائية
    final_score = int(
        min(
            100,
            round(
                score * 0.55
                + confluence * 0.45
            )
        )
    )

    # ----------------------------------------
    # فلتر الخبر
    # ----------------------------------------

    news_blocked, news_text = news_filter()

    if news_blocked:

        return {
            "signal": False,
            "direction": direction,
            "score": final_score,
            "price": price,
            "levels": levels,
            "news_blocked": True,
            "news": news_text,
            "factors": factors,
            "mtf": mtf
        }

    # ----------------------------------------
    # لا نريد الصرامة السابقة
    # ----------------------------------------

    valid = (
        direction in ("BUY", "SELL")
        and final_score >= SIGNAL_THRESHOLD
    )

    trade = None

    if valid:

        trade = build_trade(
            direction,
            h1,
            m15,
            levels
        )

        if trade and trade["rr"] < 1.20:

            valid = False

    return {
        "signal": valid,
        "direction": direction,
        "score": final_score,
        "price": price,
        "levels": levels,
        "news_blocked": False,
        "news": news_text,
        "factors": factors,
        "trade": trade,
        "mtf": mtf
    }


# ============================================================
# فلتر الأخبار
# ============================================================

def get_news_events():

    # محاولة استخدام تقويم اقتصادي عام.
    # إذا لم يتوفر المصدر، لا نخترع أخباراً.
    #
    # يمكن لاحقاً ربطه بمصدر اقتصادي مخصص.
    #
    # حالياً نعيد قائمة فارغة بأمان.

    return []


def news_filter():

    if not NEWS_FILTER_ENABLED:

        return False, "فلتر الأخبار غير مفعّل"

    try:

        now = time.time()

        if now - NEWS_CACHE["time"] > 300:

            NEWS_CACHE["events"] = (
                get_news_events()
            )

            NEWS_CACHE["time"] = now

        events = NEWS_CACHE["events"]

        current = now_damascus()

        for event in events:

            event_time = event.get(
                "time"
            )

            if not event_time:
                continue

            before = event_time - timedelta(
                minutes=NEWS_BEFORE_MIN
            )

            after = event_time + timedelta(
                minutes=NEWS_AFTER_MIN
            )

            if before <= current <= after:

                return (
                    True,
                    "🚨 التداول محجوب مؤقتاً بسبب خبر اقتصادي عالي التأثير."
                )

        return (
            False,
            "🟢 لا يوجد حجب إخباري مسجل حالياً."
        )

    except Exception:

        return (
            False,
            "🟡 تعذر تحديث المفكرة الاقتصادية؛ لم يتم اختراع خبر."
        )


# ============================================================
# تنسيق التحليل
# ============================================================

def frame_text(name, x):

    direction = {
        "BUY": "🟢 شراء",
        "SELL": "🔴 بيع",
        "WAIT": "⏳ انتظار"
    }.get(
        x["direction"],
        "⏳ انتظار"
    )

    return (
        f"📊 {name}\n"
        f"الاتجاه: {direction}\n"
        f"القوة: {x['score']}%\n"
        f"EMA50: {x['ema50']:.2f}\n"
        f"EMA200: {x['ema200']:.2f}\n"
        f"RSI: {x['rsi']:.1f}\n"
        f"MACD: {x['macd']:.2f}\n"
        f"ADX: {x['adx']:.1f}\n"
        f"الهيكل: {x['structure']}\n"
        f"الحجم: {x['volume_state']} "
        f"({x['volume_ratio']:.2f}x)"
    )


# ============================================================
# التحليل الكامل
# ============================================================

def build_analysis():

    result = evaluate_signal()

    mtf = result["mtf"]

    direction = {
        "BUY": "🟢 أفضلية شراء",
        "SELL": "🔴 أفضلية بيع",
        "WAIT": "🟡 حياد"
    }.get(
        result["direction"],
        "🟡 حياد"
    )

    levels = result["levels"]

    s1 = (
        levels["support1"]["price"]
        if levels["support1"]
        else None
    )

    s2 = (
        levels["support2"]["price"]
        if levels["support2"]
        else None
    )

    r1 = (
        levels["resistance1"]["price"]
        if levels["resistance1"]
        else None
    )

    r2 = (
        levels["resistance2"]["price"]
        if levels["resistance2"]
        else None
    )

    lines = [
        f"🤖 XAU SMART TRADER {VERSION}",
        "━━━━━━━━━━━━━━━━━━",
        "📊 التحليل الهيكلي والكمّي",
        "",
        f"💰 السعر: {result['price']:.2f}",
        f"🎯 التوجيه: {direction}",
        f"💪 درجة التوافق: {result['score']}%",
        "",
        "🧠 الأطر الزمنية",
        "",
        frame_text("W1 — الاتجاه الأكبر", mtf["w1"]),
        "",
        frame_text("D1 — الاتجاه اليومي", mtf["d1"]),
        "",
        frame_text("H4 — الهيكل الرئيسي", mtf["h4"]),
        "",
        frame_text("H1 — منطقة الدخول", mtf["h1"]),
        "",
        frame_text("M15 — الزخم والتأكيد", mtf["m15"]),
        "",
        "━━━━━━━━━━━━━━━━━━",
        "📍 الدعم والمقاومة",
        f"🟢 S1: {fmt(s1)}",
        f"🟢 S2: {fmt(s2)}",
        f"🔴 R1: {fmt(r1)}",
        f"🔴 R2: {fmt(r2)}",
        "",
        f"📰 الأخبار: {result['news']}",
        "",
        "🔎 عوامل التلاقي:"
    ]

    if result["factors"]:

        for factor in result["factors"]:

            lines.append(
                f"• {factor}"
            )

    else:

        lines.append(
            "• لا يوجد تلاقي كافٍ حالياً."
        )

    if result["signal"]:

        trade = result["trade"]

        lines += [
            "",
            "━━━━━━━━━━━━━━━━━━",
            "🚨 إشارة تداول مؤهلة",
            "",
            (
                "📈 نوع الصفقة: "
                + (
                    "🟢 شراء"
                    if trade
                    else "غير متوفر"
                )
            ),
            f"📍 الدخول: {trade['entry']:.2f}",
            f"🛑 وقف الخسارة: {trade['sl']:.2f}",
            f"🎯 TP1: {trade['tp1']:.2f}",
            f"🎯 TP2: {trade['tp2']:.2f}",
            f"⚖️ العائد/المخاطرة: 1:{trade['rr']:.2f}",
            "",
            (
                "🔥 الحالة: قوية"
                if result["score"] >= STRONG_THRESHOLD
                else "🎯 الحالة: مؤهلة"
            )
        ]

    else:

        lines += [
            "",
            "━━━━━━━━━━━━━━━━━━",
            "⏳ لا توجد صفقة جاهزة الآن.",
            "",
            "لكن التحليل مستمر،",
            "والبوت لا ينتظر تحقق كل المؤشرات حرفياً.",
            "سيبحث عن أفضل تلاقي متاح."
        ]

        if result["news_blocked"]:

            lines.append(
                "🚨 سبب الحجب: خبر اقتصادي."
            )

    lines += [
        "",
        "⚠️ التحليل مساعد لاتخاذ القرار اليدوي "
        "وليس ضماناً للربح."
    ]

    return "\n".join(lines)


# ============================================================
# أوامر Telegram
# ============================================================

async def reply(update, text):

    try:

        await update.message.reply_text(
            text
        )

    except Exception:

        logger.exception(
            "Telegram reply error"
        )


async def start(update, context):

    keyboard = [
        [
            "📊 التحليل الكامل",
            "⚡ التحليل السريع"
        ],
        [
            "🎯 صفقة الآن",
            "📍 الدعوم والمقاومات"
        ],
        [
            "📅 التحليل الأسبوعي",
            "📰 الأخبار"
        ],
        [
            "💰 سعر الذهب",
            "🌍 الأسواق"
        ],
        [
            "🔔 تفعيل التنبيهات",
            "🔕 إيقاف التنبيهات"
        ],
        [
            "🟢 حالة النظام"
        ]
    ]

    text = (
        f"🤖 XAU SMART TRADER {VERSION}\n\n"
        "🥇 محلل الذهب XAU/USD\n\n"
        "يعتمد على:\n"
        "W1 + D1 + H4 + H1 + M15\n"
        "Structure + Liquidity + Momentum\n"
        "Fibonacci + Volume + FVG\n\n"
        "🎯 عتبة الإشارة: "
        f"{SIGNAL_THRESHOLD}%\n\n"
        "اختر العملية 👇"
    )

    await update.message.reply_text(
        text,
        reply_markup=ReplyKeyboardMarkup(
            keyboard,
            resize_keyboard=True
        )
    )


async def full_analysis(update, context):

    try:

        text = await asyncio.to_thread(
            build_analysis
        )

        await reply(
            update,
            text
        )

    except Exception as e:

        await reply(
            update,
            "❌ تعذر تنفيذ التحليل.\n"
            f"السبب: {e}"
        )


async def quick_analysis(update, context):

    try:

        m15 = analyze(
            get_bars("15m", 200)
        )

        price = live_price()

        text = (
            f"⚡ التحليل السريع {VERSION}\n\n"
            f"💰 السعر: {price['price']:.2f}\n"
            f"📈 الاتجاه: "
            f"{'🟢 شراء' if m15['direction']=='BUY' else '🔴 بيع' if m15['direction']=='SELL' else '🟡 انتظار'}\n"
            f"💪 القوة: {m15['score']}%\n"
            f"RSI: {m15['rsi']:.1f}\n"
            f"MACD: {m15['macd']:.2f}\n"
            f"ADX: {m15['adx']:.1f}\n"
            f"الهيكل: {m15['structure']}\n"
            f"الحجم: {m15['volume_state']} "
            f"({m15['volume_ratio']:.2f}x)\n\n"
            "🔎 العوامل:\n"
            + "\n".join(
                "• " + x
                for x in m15["reasons"][:8]
            )
        )

        await reply(
            update,
            text
        )

    except Exception as e:

        await reply(
            update,
            f"❌ تعذر التحليل السريع: {e}"
        )


async def trade_now(update, context):

    try:

        result = await asyncio.to_thread(
            evaluate_signal
        )

        if result["news_blocked"]:

            await reply(
                update,
                "🚨 لا توجد صفقة الآن.\n\n"
                "تم تفعيل الحماية الإخبارية."
            )

            return

        if not result["signal"]:

            await reply(
                update,
                "⏳ لا توجد صفقة مؤهلة الآن.\n\n"
                f"💪 التوافق: {result['score']}%\n"
                f"🎯 المطلوب: {SIGNAL_THRESHOLD}%\n\n"
                "البوت يراقب السوق باستمرار."
            )

            return

        trade = result["trade"]

        direction = (
            "🟢 شراء"
            if result["direction"] == "BUY"
            else "🔴 بيع"
        )

        text = (
            "🚨 XAU SMART TRADER\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "🎯 إشارة تداول\n\n"
            f"📈 الصفقة: {direction}\n"
            f"💪 الثقة: {result['score']}%\n\n"
            f"📍 الدخول: {trade['entry']:.2f}\n"
            f"🛑 SL: {trade['sl']:.2f}\n"
            f"🎯 TP1: {trade['tp1']:.2f}\n"
            f"🎯 TP2: {trade['tp2']:.2f}\n"
            f"⚖️ R:R: 1:{trade['rr']:.2f}\n\n"
            "🧠 هذه إشارة تحليلية للتنفيذ اليدوي."
        )

        await reply(
            update,
            text
        )

    except Exception as e:

        await reply(
            update,
            f"❌ تعذر بناء الصفقة: {e}"
        )


async def show_levels(update, context):

    try:

        df = get_bars(
            "1h",
            250
        )

        levels = support_resistance(
            df
        )

        def lv(key):

            x = levels.get(key)

            if not x:
                return "غير متوفر"

            return (
                f"{x['price']:.2f} "
                f"(قوة {x['strength']}/100)"
            )

        text = (
            "📍 XAU/USD — مناطق السوق\n\n"
            f"🟢 S1: {lv('support1')}\n"
            f"🟢 S2: {lv('support2')}\n"
            f"🟢 S3: {lv('support3')}\n\n"
            f"🔴 R1: {lv('resistance1')}\n"
            f"🔴 R2: {lv('resistance2')}\n"
            f"🔴 R3: {lv('resistance3')}\n\n"
            "المناطق مبنية على القمم والقيعان "
            "الفعلية وليست مستويات عشوائية."
        )

        await reply(
            update,
            text
        )

    except Exception as e:

        await reply(
            update,
            f"❌ تعذر حساب المناطق: {e}"
        )


async def gold_price(update, context):

    try:

        q = await asyncio.to_thread(
            live_price
        )

        await reply(
            update,
            (
                "💰 XAU/USD — السعر اللحظي\n\n"
                f"السعر: {q['price']:.2f}\n"
                f"المصدر: {q['source']}\n"
                f"عمر السعر: {q.get('age')}\n"
                f"توقيت دمشق: "
                f"{now_damascus().strftime('%Y-%m-%d %H:%M:%S')}"
            )
        )

    except Exception as e:

        await reply(
            update,
            f"❌ تعذر جلب السعر: {e}"
        )


async def weekly_report(update, context):

    try:

        w1 = analyze(
            get_bars("1w", 250)
        )

        d1 = analyze(
            get_bars("1d", 300)
        )

        q = live_price()

        bias = (
            "🟢 صاعد"
            if w1["direction"] == "BUY"
            else
            "🔴 هابط"
            if w1["direction"] == "SELL"
            else
            "🟡 محايد"
        )

        text = (
            "📅 التقرير الاستراتيجي الأسبوعي\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            f"💰 الذهب: {q['price']:.2f}\n"
            f"🌍 اتجاه W1: {bias}\n"
            f"💪 قوة W1: {w1['score']}%\n\n"
            "📊 الأسبوعي\n"
            f"RSI: {w1['rsi']:.1f}\n"
            f"ADX: {w1['adx']:.1f}\n"
            f"الهيكل: {w1['structure']}\n\n"
            "📊 اليومي\n"
            f"الاتجاه: {d1['direction']}\n"
            f"القوة: {d1['score']}%\n"
            f"RSI: {d1['rsi']:.1f}\n"
            f"الهيكل: {d1['structure']}\n\n"
            "🎯 التوجيه:\n"
            + (
                "التركيز على فرص الشراء من التصحيحات."
                if w1["direction"] == "BUY"
                else
                "التركيز على فرص البيع من الارتدادات."
                if w1["direction"] == "SELL"
                else
                "الانتظار حتى تتضح البنية."
            )
        )

        await reply(
            update,
            text
        )

    except Exception as e:

        await reply(
            update,
            f"❌ تعذر إنشاء التقرير الأسبوعي: {e}"
        )


async def news_status(update, context):

    blocked, text = news_filter()

    await reply(
        update,
        (
            "📰 فلتر الأخبار\n\n"
            f"{'🚨 التداول محجوب' if blocked else '🟢 التداول غير محجوب'}\n\n"
            f"{text}\n\n"
            "الحماية: "
            f"{NEWS_BEFORE_MIN} دقيقة قبل الخبر + "
            f"{NEWS_AFTER_MIN} دقيقة بعده."
        )
    )


async def markets(update, context):

    now = now_damascus()

    await reply(
        update,
        (
            "🌍 جلسات السيولة\n\n"
            "🇯🇵 آسيا: تجميع ومراقبة\n"
            "🇬🇧 لندن: ارتفاع السيولة\n"
            "🇺🇸 نيويورك: أعلى التقلبات\n\n"
            f"🕐 توقيت دمشق الآن: "
            f"{now.strftime('%H:%M:%S')}\n\n"
            "⚠️ أوقات الافتتاح تتغير موسمياً "
            "بسبب التوقيت الصيفي."
        )
    )


async def status(update, context):

    await reply(
        update,
        (
            f"🟢 XAU SMART TRADER {VERSION}\n\n"
            "حالة النظام: يعمل\n"
            "Telegram: متصل\n"
            "Flask: يعمل\n"
            "البيانات: Biquote OHLC\n"
            "التحليل: W1/D1/H4/H1/M15\n"
            "الهيكل: مفعّل\n"
            "السيولة: مفعّلة\n"
            "الحجم: مفعّل\n"
            "RSI: مفعّل\n"
            "MACD: مفعّل\n"
            "ADX: مفعّل\n"
            "Fibonacci: مفعّل\n"
            "FVG: مفعّل\n"
            "فلتر الأخبار: "
            f"{'مفعّل' if NEWS_FILTER_ENABLED else 'متوقف'}\n\n"
            f"🎯 عتبة الإشارة: {SIGNAL_THRESHOLD}%\n"
            f"🔥 الإشارة القوية: {STRONG_THRESHOLD}%"
        )
    )


# ============================================================
# التنبيهات التلقائية
# ============================================================

async def subscribe(update, context):

    chat_id = update.effective_chat.id

    SUBSCRIBERS.add(
        chat_id
    )

    await reply(
        update,
        (
            "🔔 تم تفعيل التنبيهات.\n\n"
            "سيقوم البوت بمراقبة الذهب "
            "على مدار 24 ساعة.\n\n"
            f"🎯 حد الإشارة: {SIGNAL_THRESHOLD}%\n"
            "📰 الحماية الإخبارية مفعّلة."
        )
    )


async def unsubscribe(update, context):

    chat_id = update.effective_chat.id

    SUBSCRIBERS.discard(
        chat_id
    )

    LAST_SIGNAL.pop(
        chat_id,
        None
    )

    await reply(
        update,
        "🔕 تم إيقاف التنبيهات التلقائية."
    )


async def auto_loop():

    while True:

        try:

            if SUBSCRIBERS:

                result = await asyncio.to_thread(
                    evaluate_signal
                )

                if (
                    result["signal"]
                    and not result["news_blocked"]
                ):

                    trade = result["trade"]

                    signature = (
                        result["direction"],
                        round(
                            trade["entry"],
                            2
                        ),
                        round(
                            trade["sl"],
                            2
                        ),
                        round(
                            trade["tp1"],
                            2
                        )
                    )

                    for chat_id in list(
                        SUBSCRIBERS
                    ):

                        previous = LAST_SIGNAL.get(
                            chat_id
                        )

                        if previous == signature:
                            continue

                        direction = (
                            "🟢 شراء"
                            if result["direction"] == "BUY"
                            else "🔴 بيع"
                        )

                        text = (
                            "🚨🚨 إشارة ذهب جديدة\n"
                            "━━━━━━━━━━━━━━━━━━\n\n"
                            f"📈 الصفقة: {direction}\n"
                            f"💪 التوافق: "
                            f"{result['score']}%\n\n"
                            f"💰 السعر: "
                            f"{result['price']:.2f}\n"
                            f"📍 الدخول: "
                            f"{trade['entry']:.2f}\n"
                            f"🛑 SL: "
                            f"{trade['sl']:.2f}\n"
                            f"🎯 TP1: "
                            f"{trade['tp1']:.2f}\n"
                            f"🎯 TP2: "
                            f"{trade['tp2']:.2f}\n"
                            f"⚖️ R:R: "
                            f"1:{trade['rr']:.2f}\n\n"
                            "🧠 التلاقي:\n"
                            + "\n".join(
                                "• " + x
                                for x in result[
                                    "factors"
                                ][:7]
                            )
                            + "\n\n"
                            "⚠️ تنفيذ يدوي فقط."
                        )

                        try:

                            await APPLICATION.bot.send_message(
                                chat_id=chat_id,
                                text=text
                            )

                            LAST_SIGNAL[
                                chat_id
                            ] = signature

                        except Exception:

                            logger.exception(
                                "Signal send error"
                            )

        except Exception:

            logger.exception(
                "Auto loop error"
            )

        await asyncio.sleep(
            AUTO_SCAN_SECONDS
        )


# ============================================================
# Router
# ============================================================

async def router(update, context):

    text = (
        update.message.text or ""
    ).strip()

    routes = {

        "📊 التحليل الكامل":
            full_analysis,

        "⚡ التحليل السريع":
            quick_analysis,

        "🎯 صفقة الآن":
            trade_now,

        "📍 الدعوم والمقاومات":
            show_levels,

        "📅 التحليل الأسبوعي":
            weekly_report,

        "📰 الأخبار":
            news_status,

        "💰 سعر الذهب":
            gold_price,

        "🌍 الأسواق":
            markets,

        "🔔 تفعيل التنبيهات":
            subscribe,

        "🔕 إيقاف التنبيهات":
            unsubscribe,

        "🟢 حالة النظام":
            status
    }

    fn = routes.get(
        text
    )

    if fn:

        await fn(
            update,
            context
        )

    else:

        await start(
            update,
            context
        )


# ============================================================
# Webhook
# ============================================================

@app.route(
    WEBHOOK_PATH,
    methods=["POST"]
)
def webhook():

    if APPLICATION is None:

        return "Bot not ready", 503

    try:

        data = request.get_json(
            force=True
        )

        update = Update.de_json(
            data,
            APPLICATION.bot
        )

        asyncio.run_coroutine_threadsafe(
            APPLICATION.process_update(
                update
            ),
            BOT_LOOP
        )

        return "OK", 200

    except Exception:

        logger.exception(
            "Webhook error"
        )

        return "OK", 200


# ============================================================
# تشغيل Telegram
# ============================================================

async def start_bot():

    global APPLICATION

    if not TOKEN:

        raise RuntimeError(
            "TELEGRAM_TOKEN غير موجود في Render."
        )

    APPLICATION = (
        Application.builder()
        .token(TOKEN)
        .build()
    )

    # /start فقط كأمر مخفي للتوافق
    APPLICATION.add_handler(
        CommandHandler(
            "start",
            start
        )
    )

    APPLICATION.add_handler(
        MessageHandler(
            filters.TEXT
            & ~filters.COMMAND,
            router
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

    logger.info(
        "XAU SMART TRADER %s started",
        VERSION
    )

    logger.info(
        "Webhook: %s",
        WEBHOOK_URL
    )

    asyncio.create_task(
        auto_loop()
    )

    while True:

        await asyncio.sleep(
            3600
        )


# ============================================================
# Flask Server
# ============================================================

def run_flask():

    app.run(
        host="0.0.0.0",
        port=PORT,
        debug=False,
        threaded=True,
        use_reloader=False
    )


# ============================================================
# Main
# ============================================================

def main():

    global BOT_LOOP

    server = threading.Thread(
        target=run_flask,
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
            start_bot()
        )

    except KeyboardInterrupt:

        pass

    finally:

        loop.close()


if __name__ == "__main__":

    main()
