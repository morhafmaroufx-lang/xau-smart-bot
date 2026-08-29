# ============================================================
# XAU SMART TRADER v18.0
# Structural + Quantitative + Institutional Gold Analysis
# واجهة عربية بالكامل - توقيت دمشق
#
# ملاحظة:
# التحول إلى نظام الرسائل التلقائي الكامل مؤجل في هذه النسخة.
# ============================================================

import os
import asyncio
import threading
import time
import logging
import json
from datetime import datetime, timedelta
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

VERSION = "v18.0"

TOKEN = os.environ.get("TELEGRAM_TOKEN")

PORT = int(
    os.environ.get(
        "PORT",
        "10000"
    )
)

RENDER_URL = os.environ.get(
    "RENDER_EXTERNAL_URL",
    "https://xau-smart-bot.onrender.com"
).rstrip("/")

WEBHOOK_PATH = "/telegram-webhook"

WEBHOOK_URL = (
    RENDER_URL +
    WEBHOOK_PATH
)

SYMBOL = "XAUUSD"

DATA_URL = (
    "https://biquote.io/api/XAUUSD/ohlc"
)

LIVE_URL = (
    "https://biquote.io/api/XAUUSD"
)

DAMASCUS = ZoneInfo(
    "Asia/Damascus"
)

NEW_YORK = ZoneInfo(
    "America/New_York"
)


# ============================================================
# إعدادات التحليل
# ============================================================

CACHE_SECONDS = 20

MIN_BARS = 30

S_R_CLUSTER_ATR = 0.35

# ------------------------------------------------------------
# نظام النقاط الجديد
# ------------------------------------------------------------

MIN_TRADE_SCORE = 50

QUALITY_GOOD = 60
QUALITY_STRONG = 70
QUALITY_VERY_STRONG = 80
QUALITY_EXCELLENT = 90


# ============================================================
# الأخبار
# ============================================================

NEWS_BEFORE_MIN = 30
NEWS_AFTER_MIN = 30

NEWS_CACHE_SECONDS = 300

NEWS_FILTER_ENABLED = True


# ============================================================
# السجل
# ============================================================

MAX_TRADE_HISTORY = 500


# ============================================================
# Flask
# ============================================================

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    )
)

logger = logging.getLogger(
    __name__
)


# ============================================================
# الحالة
# ============================================================

APPLICATION = None

BOT_LOOP = None

DATA_CACHE = {}

NEWS_CACHE = {
    "time": 0,
    "events": []
}

TRADE_HISTORY = []

LAST_ANALYSIS = None


# ============================================================
# الصحة
# ============================================================

@app.route(
    "/",
    methods=["GET", "HEAD"]
)
def home():

    return (
        f"XAU SMART TRADER {VERSION} - OK",
        200
    )


@app.route(
    "/health",
    methods=["GET"]
)
def health():

    return jsonify(
        {
            "status": "ok",
            "bot": VERSION,
            "symbol": SYMBOL,
            "timezone": "Asia/Damascus",
            "time": now_damascus().isoformat()
        }
    ), 200


# ============================================================
# أدوات عامة
# ============================================================

def sf(
    value,
    default=0.0
):

    try:

        x = float(value)

        if np.isfinite(x):

            return x

    except Exception:

        pass

    return default


def now_damascus():

    return datetime.now(
        DAMASCUS
    )


def fmt(value):

    if value is None:

        return "غير متوفر"

    return f"{sf(value):.2f}"


def cache_key(
    interval,
    limit
):

    return (
        f"{SYMBOL}_"
        f"{interval}_"
        f"{limit}"
    )


# ============================================================
# البيانات
# ============================================================

def get_bars(
    interval,
    limit=300
):
    """
    جلب بيانات الذهب.

    Biquote لا يعتمد عليه مباشرة للفريم الأسبوعي،
    لذلك يتم بناء W1 محلياً من D1.
    """

    # ========================================================
    # الأسبوعي
    # ========================================================

    if interval == "1w":

        key = (
            f"W1_{limit}"
        )

        now = time.time()

        cached = DATA_CACHE.get(
            key
        )

        if (
            cached
            and
            now - cached[0]
            < CACHE_SECONDS
        ):

            return cached[1].copy()

        daily_limit = min(
            max(
                limit * 7 + 40,
                200
            ),
            1000
        )

        daily = get_bars(
            "1d",
            daily_limit
        )

        if (
            daily is None
            or daily.empty
        ):

            raise ValueError(
                "لا توجد بيانات D1 لبناء W1."
            )

        df = daily.copy()

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

        df = df.sort_values(
            "openTime"
        )

        df = df.set_index(
            "openTime"
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
                    f"بيانات D1 ناقصة: {col}"
                )

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        if "tickVolume" in df.columns:

            df["tickVolume"] = pd.to_numeric(
                df["tickVolume"],
                errors="coerce"
            ).fillna(0)

        else:

            df["tickVolume"] = 0

        df = df.dropna(
            subset=required
        )

        if len(df) < MIN_BARS:

            raise ValueError(
                "البيانات اليومية غير كافية لبناء W1."
            )

        weekly = pd.DataFrame()

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
            subset=required
        )

        weekly = weekly.tail(
            limit
        )

        if len(weekly) < MIN_BARS:

            raise ValueError(
                "البيانات الأسبوعية غير كافية."
            )

        weekly = weekly.reset_index()

        weekly.rename(
            columns={
                "openTime":
                    "openTime"
            },
            inplace=True
        )

        DATA_CACHE[key] = (
            now,
            weekly.copy()
        )

        return weekly.copy()


    # ========================================================
    # الفريمات المدعومة
    # ========================================================

    supported = {
        "1m",
        "5m",
        "15m",
        "30m",
        "1h",
        "4h",
        "1d"
    }

    if interval not in supported:

        raise ValueError(
            f"الفريم {interval} غير مدعوم."
        )


    # ========================================================
    # Cache
    # ========================================================

    key = cache_key(
        interval,
        limit
    )

    now = time.time()

    cached = DATA_CACHE.get(
        key
    )

    if (
        cached
        and
        now - cached[0]
        < CACHE_SECONDS
    ):

        return cached[1].copy()


    # ========================================================
    # Biquote
    # ========================================================

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

        if not response.ok:

            try:

                detail = response.json()

            except Exception:

                detail = response.text[:500]

            raise RuntimeError(
                f"Biquote HTTP "
                f"{response.status_code}: "
                f"{detail}"
            )

        data = response.json()

    except requests.RequestException as exc:

        raise RuntimeError(
            f"تعذر الاتصال بمصدر البيانات: "
            f"{exc}"
        ) from exc


    # ========================================================
    # الشموع
    # ========================================================

    if not isinstance(
        data,
        dict
    ):

        raise ValueError(
            "استجابة Biquote غير متوقعة."
        )

    bars = data.get(
        "bars",
        []
    )

    if not bars:

        raise ValueError(
            f"لا توجد بيانات للفريم {interval}."
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

    if "tickVolume" in df.columns:

        df["tickVolume"] = pd.to_numeric(
            df["tickVolume"],
            errors="coerce"
        ).fillna(0)

    else:

        df["tickVolume"] = 0


    # ========================================================
    # الوقت
    # ========================================================

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


    # ========================================================
    # تنظيف
    # ========================================================

    df = df.dropna(
        subset=required
    )

    if len(df) < MIN_BARS:

        raise ValueError(
            f"البيانات غير كافية للفريم "
            f"{interval}: {len(df)} شمعة."
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
            params={
                "allowStale": "false"
            },
            timeout=8
        )

        if r.ok:

            data = r.json()

            if isinstance(
                data,
                dict
            ):

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

                    if (
                        bid
                        and
                        ask
                    ):

                        price = (
                            bid + ask
                        ) / 2

                if (
                    price
                    and
                    price > 0
                ):

                    return {
                        "price": price,
                        "source": "Biquote",
                        "age":
                            data.get(
                                "quoteAgeSeconds"
                            )
                    }

    except Exception as e:

        errors.append(
            str(e)
        )


    # المصدر البديل
    try:

        r = requests.get(
            "https://xaus.com/api/v1/spot",
            timeout=8
        )

        if r.ok:

            data = r.json()

            price = sf(
                data.get(
                    "spot_usd_oz"
                ),
                None
            )

            if (
                price
                and
                price > 0
            ):

                state = data.get(
                    "data_state",
                    {}
                )

                return {
                    "price": price,
                    "source": "XAUS",
                    "age":
                        state.get(
                            "age_seconds"
                        )
                }

    except Exception as e:

        errors.append(
            str(e)
        )


    raise RuntimeError(
        "تعذر الحصول على السعر اللحظي: "
        +
        " | ".join(errors)
    )


# ============================================================
# المؤشرات
# ============================================================

def EMA(
    s,
    n
):

    return s.ewm(
        span=n,
        adjust=False
    ).mean()


def RSI(
    s,
    n=14
):

    delta = s.diff()

    gain = delta.clip(
        lower=0
    )

    loss = -delta.clip(
        upper=0
    )

    avg_gain = gain.ewm(
        alpha=1 / n,
        adjust=False
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / n,
        adjust=False
    ).mean()

    rs = (
        avg_gain
        /
        avg_loss.replace(
            0,
            np.nan
        )
    )

    return (
        100 -
        100 / (1 + rs)
    ).fillna(50)


def MACD(
    s,
    fast=8,
    slow=21,
    signal=5
):

    fast_line = EMA(
        s,
        fast
    )

    slow_line = EMA(
        s,
        slow
    )

    line = (
        fast_line -
        slow_line
    )

    sig = EMA(
        line,
        signal
    )

    hist = (
        line -
        sig
    )

    return (
        line,
        sig,
        hist
    )


def ATR(
    df,
    n=14
):

    close = df["close"]

    prev = close.shift(
        1
    )

    tr = pd.concat(
        [
            df["high"] -
            df["low"],

            (
                df["high"] -
                prev
            ).abs(),

            (
                df["low"] -
                prev
            ).abs()
        ],
        axis=1
    ).max(
        axis=1
    )

    return tr.ewm(
        alpha=1 / n,
        adjust=False
    ).mean()


def ADX(
    df,
    n=14
):

    high = df["high"]

    low = df["low"]

    close = df["close"]

    up = high.diff()

    down = -low.diff()

    plus_dm = pd.Series(
        np.where(
            (
                (up > down)
                &
                (up > 0)
            ),
            up,
            0
        ),
        index=df.index
    )

    minus_dm = pd.Series(
        np.where(
            (
                (down > up)
                &
                (down > 0)
            ),
            down,
            0
        ),
        index=df.index
    )

    prev = close.shift(
        1
    )

    tr = pd.concat(
        [
            high - low,
            (high - prev).abs(),
            (low - prev).abs()
        ],
        axis=1
    ).max(
        axis=1
    )

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
        /
        atr.replace(
            0,
            np.nan
        )
    )

    minus_di = (
        100 *
        minus_dm.ewm(
            alpha=1 / n,
            adjust=False
        ).mean()
        /
        atr.replace(
            0,
            np.nan
        )
    )

    dx = (
        100 *
        (
            plus_di -
            minus_di
        ).abs()
        /
        (
            plus_di +
            minus_di
        ).replace(
            0,
            np.nan
        )
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

    recent = df.tail(
        10
    )

    highs = recent[
        "high"
    ]

    lows = recent[
        "low"
    ]

    h_now = sf(
        highs.iloc[-1]
    )

    h_mid = sf(
        highs.iloc[-5]
    )

    l_now = sf(
        lows.iloc[-1]
    )

    l_mid = sf(
        lows.iloc[-5]
    )

    if (
        h_now > h_mid
        and
        l_now > l_mid
    ):

        return "صاعد"

    if (
        h_now < h_mid
        and
        l_now < l_mid
    ):

        return "هابط"

    return "محايد"


# ============================================================
# الحجم
# ============================================================

def volume_analysis(
    df
):

    volume = (
        df["tickVolume"]
        .astype(float)
    )

    current = sf(
        volume.iloc[-1]
    )

    average = sf(
        volume.tail(
            20
        ).mean(),
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

    return (
        state,
        ratio
    )


# ============================================================
# فيبوناتشي
# ============================================================

def fibonacci(
    df,
    lookback=120
):

    x = df.tail(
        min(
            lookback,
            len(df)
        )
    )

    high = sf(
        x["high"].max()
    )

    low = sf(
        x["low"].min()
    )

    span = high - low

    if span <= 0:

        return {}

    return {
        "0": high,
        "23.6":
            high -
            span * 0.236,
        "38.2":
            high -
            span * 0.382,
        "50":
            high -
            span * 0.500,
        "61.8":
            high -
            span * 0.618,
        "78.6":
            high -
            span * 0.786,
        "100": low
    }


# ============================================================
# FVG
# ============================================================

def find_fvg(
    df
):

    if len(df) < 5:

        return None

    a = df.iloc[-3]

    c = df.iloc[-1]

    if (
        sf(c["low"])
        >
        sf(a["high"])
    ):

        return {
            "type": "صاعدة",
            "low":
                sf(a["high"]),
            "high":
                sf(c["low"])
        }

    if (
        sf(c["high"])
        <
        sf(a["low"])
    ):

        return {
            "type": "هابطة",
            "low":
                sf(c["high"]),
            "high":
                sf(a["low"])
        }

    return None


# ============================================================
# الدعم والمقاومة
# ============================================================

def support_resistance(
    df,
    lookback=150
):

    x = df.tail(
        min(
            lookback,
            len(df)
        )
    ).copy()

    current = sf(
        x["close"].iloc[-1]
    )

    atr = sf(
        ATR(x).iloc[-1],
        1
    )

    radius = max(
        atr *
        S_R_CLUSTER_ATR,
        current *
        0.00035
    )

    supports = []

    resistances = []


    for i in range(
        2,
        len(x) - 2
    ):

        low = sf(
            x["low"].iloc[i]
        )

        high = sf(
            x["high"].iloc[i]
        )

        left_low = sf(
            x["low"]
            .iloc[i-2:i]
            .min()
        )

        right_low = sf(
            x["low"]
            .iloc[i+1:i+3]
            .min()
        )

        left_high = sf(
            x["high"]
            .iloc[i-2:i]
            .max()
        )

        right_high = sf(
            x["high"]
            .iloc[i+1:i+3]
            .max()
        )


        if (
            low <= left_low
            and
            low <= right_low
            and
            low < current
        ):

            supports.append(
                low
            )


        if (
            high >= left_high
            and
            high >= right_high
            and
            high > current
        ):

            resistances.append(
                high
            )


    def cluster(
        values
    ):

        values = sorted(
            values
        )

        groups = []

        for price in values:

            if not groups:

                groups.append(
                    [price]
                )

                continue

            center = (
                sum(
                    groups[-1]
                )
                /
                len(
                    groups[-1]
                )
            )

            if (
                abs(
                    price -
                    center
                )
                <= radius
            ):

                groups[-1].append(
                    price
                )

            else:

                groups.append(
                    [price]
                )

        zones = []

        for group in groups:

            center = (
                sum(group)
                /
                len(group)
            )

            touches = len(
                group
            )

            strength = min(
                100,
                35 +
                touches * 15
            )

            zones.append(
                {
                    "price":
                        center,
                    "strength":
                        strength,
                    "touches":
                        touches
                }
            )

        return zones


    supports = cluster(
        supports
    )

    resistances = cluster(
        resistances
    )


    supports = sorted(
        supports,
        key=lambda z:
            abs(
                current -
                z["price"]
            )
    )[:3]


    resistances = sorted(
        resistances,
        key=lambda z:
            abs(
                current -
                z["price"]
            )
    )[:3]


    return {
        "support1":
            supports[0]
            if len(supports) > 0
            else None,

        "support2":
            supports[1]
            if len(supports) > 1
            else None,

        "support3":
            supports[2]
            if len(supports) > 2
            else None,

        "resistance1":
            resistances[0]
            if len(resistances) > 0
            else None,

        "resistance2":
            resistances[1]
            if len(resistances) > 1
            else None,

        "resistance3":
            resistances[2]
            if len(resistances) > 2
            else None,

        "atr":
            atr
    }


# ============================================================
# تحليل فريم واحد
# ============================================================

def analyze(
    df
):

    close = df["close"]

    price = sf(
        close.iloc[-1]
    )

    ema50 = sf(
        EMA(
            close,
            50
        ).iloc[-1]
    )

    ema200 = sf(
        EMA(
            close,
            200
        ).iloc[-1]
    )

    rsi = sf(
        RSI(
            close,
            14
        ).iloc[-1],
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

    struct = structure(
        df
    )

    vol_state, vol_ratio = volume_analysis(
        df
    )

    fib = fibonacci(
        df
    )

    fvg = find_fvg(
        df
    )

    atr = sf(
        ATR(df).iloc[-1],
        1
    )


    # --------------------------------------------------------
    # نقاط الاتجاه
    # --------------------------------------------------------

    bull = 0

    bear = 0

    reasons = []


    if price > ema50:

        bull += 10

        reasons.append(
            "السعر فوق EMA50"
        )

    elif price < ema50:

        bear += 10

        reasons.append(
            "السعر تحت EMA50"
        )


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


    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

    if rsi < 30:

        bull += 10

        reasons.append(
            "تشبع بيعي"
        )

    elif rsi > 70:

        bear += 10

        reasons.append(
            "تشبع شرائي"
        )

    elif rsi > 50:

        bull += 5

    elif rsi < 50:

        bear += 5


    # --------------------------------------------------------
    # MACD
    # --------------------------------------------------------

    if ml > ms:

        bull += 10

        if mh > 0:

            bull += 4

    elif ml < ms:

        bear += 10

        if mh < 0:

            bear += 4


    # --------------------------------------------------------
    # ADX
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # الهيكل
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # الحجم
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # الاتجاه
    # --------------------------------------------------------

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
        min(
            max(
                score,
                0
            ),
            100
        )
    )


    return {
        "price":
            price,

        "ema50":
            ema50,

        "ema200":
            ema200,

        "rsi":
            rsi,

        "macd":
            ml,

        "macd_signal":
            ms,

        "macd_hist":
            mh,

        "adx":
            adx,

        "structure":
            struct,

        "volume_state":
            vol_state,

        "volume_ratio":
            vol_ratio,

        "fib":
            fib,

        "fvg":
            fvg,

        "atr":
            atr,

        "direction":
            direction,

        "score":
            score,

        "reasons":
            reasons
    }


# ============================================================
# التحليل متعدد الفريمات
# ============================================================

def multi_timeframe():

    w1 = analyze(
        get_bars(
            "1w",
            250
        )
    )

    d1 = analyze(
        get_bars(
            "1d",
            300
        )
    )

    h4 = analyze(
        get_bars(
            "4h",
            300
        )
    )

    h1 = analyze(
        get_bars(
            "1h",
            300
        )
    )

    m15 = analyze(
        get_bars(
            "15m",
            300
        )
    )


    frames = [
        w1,
        d1,
        h4,
        h1,
        m15
    ]


    weights = {
        "w1": 0.15,
        "d1": 0.25,
        "h4": 0.25,
        "h1": 0.20,
        "m15": 0.15
    }


    buy_score = 0

    sell_score = 0


    for name, x in [
        ("w1", w1),
        ("d1", d1),
        ("h4", h4),
        ("h1", h1),
        ("m15", m15)
    ]:

        weighted = (
            x["score"] *
            weights[name]
        )

        if x["direction"] == "BUY":

            buy_score += weighted

        elif x["direction"] == "SELL":

            sell_score += weighted


    if buy_score > sell_score:

        direction = "BUY"

        raw_score = buy_score

    elif sell_score > buy_score:

        direction = "SELL"

        raw_score = sell_score

    else:

        direction = "WAIT"

        raw_score = 0


    # --------------------------------------------------------
    # تعزيز الاتجاه إذا توافق أكثر من إطار
    # --------------------------------------------------------

    same_direction = sum(
        1
        for x in frames
        if x["direction"] == direction
    )


    if same_direction >= 4:

        raw_score += 8

    elif same_direction >= 3:

        raw_score += 4


    final_score = int(
        min(
            100,
            round(
                raw_score
            )
        )
    )


    return {
        "w1": w1,
        "d1": d1,
        "h4": h4,
        "h1": h1,
        "m15": m15,

        "direction":
            direction,

        "score":
            final_score,

        "buy_score":
            int(
                round(
                    buy_score
                )
            ),

        "sell_score":
            int(
                round(
                    sell_score
                )
            ),

        "agreement":
            same_direction
    }


# ============================================================
# جودة الصفقة
# ============================================================

def trade_quality(
    score
):

    score = int(
        score
    )

    if score >= 90:

        return (
            "ممتازة",
            "🔥🔥🔥"
        )

    if score >= 80:

        return (
            "قوية جداً",
            "🔥🔥"
        )

    if score >= 70:

        return (
            "قوية",
            "🔥"
        )

    if score >= 60:

        return (
            "جيدة",
            "🟢"
        )

    if score >= 50:

        return (
            "مقبولة",
            "🟡"
        )

    return (
        "ضعيفة",
        "⚪"
    )


# ============================================================
# دعم / مقاومة قريب
# ============================================================

def nearest_levels(
    direction,
    price,
    levels
):

    supports = []

    resistances = []


    for key in [
        "support1",
        "support2",
        "support3"
    ]:

        x = levels.get(
            key
        )

        if (
            x
            and
            x["price"] < price
        ):

            supports.append(
                x
            )


    for key in [
        "resistance1",
        "resistance2",
        "resistance3"
    ]:

        x = levels.get(
            key
        )

        if (
            x
            and
            x["price"] > price
        ):

            resistances.append(
                x
            )


    supports = sorted(
        supports,
        key=lambda x:
            price -
            x["price"]
    )

    resistances = sorted(
        resistances,
        key=lambda x:
            x["price"] -
            price
    )


    return (
        supports,
        resistances
    )


# ============================================================
# بناء الصفقة من المناطق
# ============================================================

def build_trade(
    direction,
    h1,
    m15,
    levels
):

    price = sf(
        m15["price"]
    )

    atr = max(
        sf(
            m15["atr"],
            1
        ),
        0.50
    )


    supports, resistances = nearest_levels(
        direction,
        price,
        levels
    )


    # ========================================================
    # BUY
    # ========================================================

    if direction == "BUY":

        # الأفضل الدخول قريباً من الدعم
        if supports:

            nearest_support = supports[0]

            distance = (
                price -
                nearest_support["price"]
            )

            if distance <= atr * 1.80:

                entry = (
                    nearest_support["price"]
                    +
                    atr * 0.15
                )

            else:

                entry = price

        else:

            entry = price


        if supports:

            sl = (
                supports[0]["price"]
                -
                atr * 0.25
            )

        else:

            sl = (
                entry -
                atr * 1.20
            )


        future_res = [
            x
            for x in resistances
            if x["price"] > entry
        ]


        if future_res:

            tp1 = future_res[0]["price"]

        else:

            tp1 = (
                entry +
                atr * 1.50
            )


        if len(future_res) >= 2:

            tp2 = future_res[1]["price"]

        else:

            tp2 = max(
                entry +
                atr * 2.50,
                tp1 +
                atr * 0.80
            )


    # ========================================================
    # SELL
    # ========================================================

    elif direction == "SELL":

        if resistances:

            nearest_resistance = resistances[0]

            distance = (
                nearest_resistance["price"]
                -
                price
            )

            if distance <= atr * 1.80:

                entry = (
                    nearest_resistance["price"]
                    -
                    atr * 0.15
                )

            else:

                entry = price

        else:

            entry = price


        if resistances:

            sl = (
                resistances[0]["price"]
                +
                atr * 0.25
            )

        else:

            sl = (
                entry +
                atr * 1.20
            )


        future_sup = [
            x
            for x in supports
            if x["price"] < entry
        ]


        if future_sup:

            tp1 = future_sup[0]["price"]

        else:

            tp1 = (
                entry -
                atr * 1.50
            )


        if len(future_sup) >= 2:

            tp2 = future_sup[1]["price"]

        else:

            tp2 = min(
                entry -
                atr * 2.50,
                tp1 -
                atr * 0.80
            )


    else:

        return None


    risk = abs(
        entry -
        sl
    )

    reward = abs(
        tp2 -
        entry
    )


    if risk <= 0:

        return None


    rr = (
        reward /
        risk
    )


    return {
        "entry":
            entry,

        "sl":
            sl,

        "tp1":
            tp1,

        "tp2":
            tp2,

        "rr":
            rr
    }


# ============================================================
# العامل المؤسسي
# ============================================================

def institutional_analysis():

    """
    طبقة المؤسسات والبنوك المركزية.

    لا يتم اختراع بيانات.
    إذا لم تتوفر بيانات خارجية حقيقية،
    تكون النتيجة محايدة.

    لاحقاً يمكن ربطها بمصادر:
    - احتياطيات الذهب للبنوك المركزية
    - مشتريات/مبيعات الذهب
    - تدفقات ETF مثل GLD
    - الفائدة الأمريكية
    - عوائد السندات
    - الدولار
    """

    result = {
        "score": 0,
        "direction": "NEUTRAL",
        "quality": "غير متوفر",
        "factors": []
    }


    # --------------------------------------------------------
    # بيانات البيئة المتاحة من Environment
    # --------------------------------------------------------

    fed_bias = os.environ.get(
        "FED_GOLD_BIAS"
    )

    china_bias = os.environ.get(
        "CHINA_GOLD_BIAS"
    )

    etf_bias = os.environ.get(
        "ETF_GOLD_BIAS"
    )


    # --------------------------------------------------------
    # الفيدرالي
    # --------------------------------------------------------

    if fed_bias:

        value = fed_bias.upper()

        if value == "BULLISH":

            result["score"] += 8

            result["factors"].append(
                "بيانات الفيدرالي: داعمة للذهب"
            )

        elif value == "BEARISH":

            result["score"] -= 8

            result["factors"].append(
                "بيانات الفيدرالي: ضاغطة على الذهب"
            )


    # --------------------------------------------------------
    # الصين
    # --------------------------------------------------------

    if china_bias:

        value = china_bias.upper()

        if value == "BULLISH":

            result["score"] += 8

            result["factors"].append(
                "بيانات الصين: داعمة للذهب"
            )

        elif value == "BEARISH":

            result["score"] -= 8

            result["factors"].append(
                "بيانات الصين: ضاغطة على الذهب"
            )


    # --------------------------------------------------------
    # ETF
    # --------------------------------------------------------

    if etf_bias:

        value = etf_bias.upper()

        if value == "BULLISH":

            result["score"] += 8

            result["factors"].append(
                "تدفقات ETF: داعمة للذهب"
            )

        elif value == "BEARISH":

            result["score"] -= 8

            result["factors"].append(
                "تدفقات ETF: ضاغطة على الذهب"
            )


    # --------------------------------------------------------
    # الاتجاه النهائي
    # --------------------------------------------------------

    if result["score"] >= 8:

        result["direction"] = "BULLISH"

    elif result["score"] <= -8:

        result["direction"] = "BEARISH"

    else:

        result["direction"] = "NEUTRAL"


    if result["score"] > 0:

        result["quality"] = "داعم للذهب"

    elif result["score"] < 0:

        result["quality"] = "ضاغط على الذهب"

    else:

        result["quality"] = "محايد"


    return result


# ============================================================
# دمج العامل المؤسسي
# ============================================================

def institutional_adjustment(
    direction,
    institutional
):

    score = 0

    factors = []

    inst_direction = institutional[
        "direction"
    ]

    if direction == "BUY":

        if inst_direction == "BULLISH":

            score += 8

            factors.append(
                "العامل المؤسسي داعم للشراء"
            )

        elif inst_direction == "BEARISH":

            score -= 8

            factors.append(
                "العامل المؤسسي يعاكس الشراء"
            )

    elif direction == "SELL":

        if inst_direction == "BEARISH":

            score += 8

            factors.append(
                "العامل المؤسسي داعم للبيع"
            )

        elif inst_direction == "BULLISH":

            score -= 8

            factors.append(
                "العامل المؤسسي يعاكس البيع"
            )

    return (
        score,
        factors
    )


# ============================================================
# فلتر فيبوناتشي
# ============================================================

def fibonacci_confluence(
    direction,
    h1
):

    fib = h1.get(
        "fib",
        {}
    )

    if not fib:

        return False


    price = h1["price"]

    atr = max(
        h1["atr"],
        0.10
    )


    level = fib.get(
        "61.8"
    )

    if level is None:

        return False


    near = (
        abs(
            price -
            level
        )
        <=
        atr * 0.80
    )


    if direction == "BUY":

        return (
            near
            and
            price >=
            level -
            atr
        )


    if direction == "SELL":

        return (
            near
            and
            price <=
            level +
            atr
        )


    return False


# ============================================================
# تقييم الصفقة
# ============================================================

def evaluate_signal():

    global LAST_ANALYSIS


    mtf = multi_timeframe()

    direction = mtf[
        "direction"
    ]

    base_score = mtf[
        "score"
    ]


    d1 = mtf["d1"]

    h4 = mtf["h4"]

    h1 = mtf["h1"]

    m15 = mtf["m15"]


    quote = live_price()

    price = quote[
        "price"
    ]


    levels = support_resistance(
        get_bars(
            "1h",
            250
        )
    )


    institutional = institutional_analysis()


    # ========================================================
    # نقاط التلاقي
    # ========================================================

    confluence = 0

    factors = []


    # --------------------------------------------------------
    # الاتجاه اليومي
    # --------------------------------------------------------

    if direction == "BUY":

        if d1["price"] > d1["ema200"]:

            confluence += 8

            factors.append(
                "الاتجاه اليومي داعم للشراء"
            )

        if h4["price"] > h4["ema200"]:

            confluence += 8

            factors.append(
                "H4 داعم للشراء"
            )


    elif direction == "SELL":

        if d1["price"] < d1["ema200"]:

            confluence += 8

            factors.append(
                "الاتجاه اليومي داعم للبيع"
            )

        if h4["price"] < h4["ema200"]:

            confluence += 8

            factors.append(
                "H4 داعم للبيع"
            )


    # --------------------------------------------------------
    # الهيكل
    # --------------------------------------------------------

    if (
        direction == "BUY"
        and
        h4["structure"] == "صاعد"
    ):

        confluence += 8

        factors.append(
            "هيكل H4 صاعد"
        )

    elif (
        direction == "SELL"
        and
        h4["structure"] == "هابط"
    ):

        confluence += 8

        factors.append(
            "هيكل H4 هابط"
        )


    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

    if direction == "BUY":

        if m15["rsi"] <= 35:

            confluence += 8

            factors.append(
                "RSI M15 في منطقة تشبع بيعي"
            )

        elif m15["rsi"] < 45:

            confluence += 4

            factors.append(
                "RSI M15 منخفض"
            )

    elif direction == "SELL":

        if m15["rsi"] >= 65:

            confluence += 8

            factors.append(
                "RSI M15 في منطقة تشبع شرائي"
            )

        elif m15["rsi"] > 55:

            confluence += 4

            factors.append(
                "RSI M15 مرتفع"
            )


    # --------------------------------------------------------
    # MACD
    # --------------------------------------------------------

    if direction == "BUY":

        if (
            m15["macd"]
            >
            m15["macd_signal"]
        ):

            confluence += 7

            factors.append(
                "MACD إيجابي"
            )

    elif direction == "SELL":

        if (
            m15["macd"]
            <
            m15["macd_signal"]
        ):

            confluence += 7

            factors.append(
                "MACD سلبي"
            )


    # --------------------------------------------------------
    # الحجم
    # --------------------------------------------------------

    if (
        m15["volume_ratio"]
        >= 1.10
    ):

        confluence += 7

        factors.append(
            "الحجم أعلى من الطبيعي"
        )


    # --------------------------------------------------------
    # Fibonacci
    # --------------------------------------------------------

    if fibonacci_confluence(
        direction,
        h1
    ):

        confluence += 6

        factors.append(
            "تلاقي Fibonacci 61.8%"
        )


    # --------------------------------------------------------
    # FVG
    # --------------------------------------------------------

    fvg = m15.get(
        "fvg"
    )

    if fvg:

        if (
            direction == "BUY"
            and
            fvg["type"] == "صاعدة"
        ):

            confluence += 4

            factors.append(
                "FVG صاعدة"
            )

        elif (
            direction == "SELL"
            and
            fvg["type"] == "هابطة"
        ):

            confluence += 4

            factors.append(
                "FVG هابطة"
            )


    # ========================================================
    # الدعم والمقاومة
    # ========================================================

    sr_points = 0

    if direction in (
        "BUY",
        "SELL"
    ):

        supports, resistances = nearest_levels(
            direction,
            price,
            levels
        )


        if direction == "BUY":

            if supports:

                distance = (
                    price -
                    supports[0]["price"]
                )

                if (
                    distance
                    <=
                    m15["atr"] * 1.80
                ):

                    sr_points += 10

                    factors.append(
                        "السعر قريب من دعم فعلي"
                    )


        elif direction == "SELL":

            if resistances:

                distance = (
                    resistances[0]["price"]
                    -
                    price
                )

                if (
                    distance
                    <=
                    m15["atr"] * 1.80
                ):

                    sr_points += 10

                    factors.append(
                        "السعر قريب من مقاومة فعلية"
                    )


    # --------------------------------------------------------
    # العامل المؤسسي
    # --------------------------------------------------------

    inst_points, inst_factors = institutional_adjustment(
        direction,
        institutional
    )

    factors.extend(
        inst_factors
    )


    # ========================================================
    # النقاط النهائية
    # ========================================================

    final_score = (
        base_score * 0.50
        +
        confluence * 0.35
        +
        sr_points
        +
        inst_points
    )


    final_score = int(
        max(
            0,
            min(
                100,
                round(
                    final_score
                )
            )
        )
    )


    quality, quality_icon = trade_quality(
        final_score
    )


    # ========================================================
    # الأخبار
    # ========================================================

    news_blocked, news_text = news_filter()


    if news_blocked:

        result = {
            "signal": False,
            "direction": direction,
            "score": final_score,
            "quality": quality,
            "quality_icon": quality_icon,
            "price": price,
            "levels": levels,
            "news_blocked": True,
            "news": news_text,
            "factors": factors,
            "mtf": mtf,
            "institutional": institutional,
            "trade": None
        }

        LAST_ANALYSIS = result

        return result


    # ========================================================
    # الصفقة
    # ========================================================

    valid = (
        direction in (
            "BUY",
            "SELL"
        )
        and
        final_score >=
        MIN_TRADE_SCORE
    )


    trade = None


    if valid:

        trade = build_trade(
            direction,
            h1,
            m15,
            levels
        )


        # السماح بصفقات أكثر،
        # لكن لا نقبل مخاطرة غير منطقية.

        if trade:

            if trade["rr"] < 1.00:

                valid = False

                factors.append(
                    "R:R غير مناسب"
                )


    result = {
        "signal":
            valid,

        "direction":
            direction,

        "score":
            final_score,

        "quality":
            quality,

        "quality_icon":
            quality_icon,

        "price":
            price,

        "levels":
            levels,

        "news_blocked":
            False,

        "news":
            news_text,

        "factors":
            factors,

        "trade":
            trade,

        "mtf":
            mtf,

        "institutional":
            institutional
    }


    LAST_ANALYSIS = result

    return result


# ============================================================
# الأخبار
# ============================================================

def get_news_events():

    """
    لا يتم اختراع أخبار.

    يمكن لاحقاً ربط هذه الدالة بمصدر اقتصادي حقيقي.
    """

    return []


def news_filter():

    if not NEWS_FILTER_ENABLED:

        return (
            False,
            "فلتر الأخبار غير مفعّل"
        )


    try:

        now = time.time()


        if (
            now -
            NEWS_CACHE["time"]
            >
            NEWS_CACHE_SECONDS
        ):

            NEWS_CACHE["events"] = (
                get_news_events()
            )

            NEWS_CACHE["time"] = now


        events = NEWS_CACHE[
            "events"
        ]


        current = now_damascus()


        for event in events:

            event_time = event.get(
                "time"
            )

            if not event_time:

                continue


            if isinstance(
                event_time,
                str
            ):

                event_time = datetime.fromisoformat(
                    event_time
                )


            before = (
                event_time -
                timedelta(
                    minutes=
                    NEWS_BEFORE_MIN
                )
            )

            after = (
                event_time +
                timedelta(
                    minutes=
                    NEWS_AFTER_MIN
                )
            )


            if (
                before
                <=
                current
                <=
                after
            ):

                return (
                    True,
                    "🚨 التداول محجوب بسبب خبر اقتصادي عالي التأثير."
                )


        return (
            False,
            "🟢 لا يوجد حجب إخباري مسجل حالياً."
        )


    except Exception as e:

        logger.exception(
            "News filter error"
        )

        return (
            False,
            "🟡 تعذر تحديث المفكرة الاقتصادية."
        )


# ============================================================
# تنسيق الفريم
# ============================================================

def frame_text(
    name,
    x
):

    direction = {
        "BUY":
            "🟢 شراء",

        "SELL":
            "🔴 بيع",

        "WAIT":
            "⏳ انتظار"
    }.get(
        x["direction"],
        "⏳ انتظار"
    )


    return (
        f"📊 {name}\n"
        f"الاتجاه: {direction}\n"
        f"النقاط: {x['score']}/100\n"
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
# مناطق النص
# ============================================================

def format_zone(
    levels,
    key
):

    x = levels.get(
        key
    )

    if not x:

        return "غير متوفر"

    return (
        f"{x['price']:.2f} "
        f"(قوة {x['strength']}/100 "
        f"| لمس {x['touches']})"
    )


# ============================================================
# التحليل الكامل
# ============================================================

def build_analysis():

    result = evaluate_signal()

    mtf = result["mtf"]

    levels = result["levels"]

    institutional = result[
        "institutional"
    ]


    direction = {
        "BUY":
            "🟢 أفضلية شراء",

        "SELL":
            "🔴 أفضلية بيع",

        "WAIT":
            "🟡 حياد"
    }.get(
        result["direction"],
        "🟡 حياد"
    )


    lines = [

        f"🤖 XAU SMART TRADER {VERSION}",

        "━━━━━━━━━━━━━━━━━━",

        "📊 التحليل الهيكلي والكمّي والمؤسسي",

        "",

        f"💰 السعر: "
        f"{result['price']:.2f}",

        f"🎯 التوجيه: "
        f"{direction}",

        f"💪 النقاط: "
        f"{result['score']}/100",

        f"🏆 جودة الصفقة: "
        f"{result['quality_icon']} "
        f"{result['quality']}",

        "",

        "🧠 الأطر الزمنية",

        "",

        frame_text(
            "W1 — الاتجاه الأكبر",
            mtf["w1"]
        ),

        "",

        frame_text(
            "D1 — الاتجاه اليومي",
            mtf["d1"]
        ),

        "",

        frame_text(
            "H4 — الهيكل الرئيسي",
            mtf["h4"]
        ),

        "",

        frame_text(
            "H1 — منطقة الدخول",
            mtf["h1"]
        ),

        "",

        frame_text(
            "M15 — الزخم والتأكيد",
            mtf["m15"]
        ),

        "",

        "━━━━━━━━━━━━━━━━━━",

        "📍 خريطة الدعم والمقاومة",

        f"🟢 S1: "
        f"{format_zone(levels, 'support1')}",

        f"🟢 S2: "
        f"{format_zone(levels, 'support2')}",

        f"🟢 S3: "
        f"{format_zone(levels, 'support3')}",

        "",

        f"🔴 R1: "
        f"{format_zone(levels, 'resistance1')}",

        f"🔴 R2: "
        f"{format_zone(levels, 'resistance2')}",

        f"🔴 R3: "
        f"{format_zone(levels, 'resistance3')}",

        "",

        "🏦 العامل المؤسسي",

        f"الاتجاه: "
        f"{institutional['direction']}",

        f"الحالة: "
        f"{institutional['quality']}",

        "",

        "📰 الأخبار",

        result["news"],

        "",

        "🔎 عوامل التلاقي:"
    ]


    if result["factors"]:

        for factor in result[
            "factors"
        ][:15]:

            lines.append(
                f"• {factor}"
            )

    else:

        lines.append(
            "• لا توجد عوامل إضافية."
        )


    # ========================================================
    # الصفقة
    # ========================================================

    if result["signal"]:

        trade = result[
            "trade"
        ]

        direction_text = (
            "🟢 شراء"
            if result["direction"]
            == "BUY"
            else
            "🔴 بيع"
        )


        lines += [

            "",

            "━━━━━━━━━━━━━━━━━━",

            "🚨 صفقة مرشحة",

            "",

            f"📈 النوع: "
            f"{direction_text}",

            f"💪 النقاط: "
            f"{result['score']}/100",

            f"🏆 الجودة: "
            f"{result['quality_icon']} "
            f"{result['quality']}",

            "",

            f"📍 الدخول: "
            f"{trade['entry']:.2f}",

            f"🛑 وقف الخسارة: "
            f"{trade['sl']:.2f}",

            f"🎯 TP1: "
            f"{trade['tp1']:.2f}",

            f"🎯 TP2: "
            f"{trade['tp2']:.2f}",

            f"⚖️ R:R: "
            f"1:{trade['rr']:.2f}"
        ]


    else:

        lines += [

            "",

            "━━━━━━━━━━━━━━━━━━",

            "⏳ لا توجد صفقة جاهزة الآن.",

            "",

            f"💪 النقاط الحالية: "
            f"{result['score']}/100",

            f"🎯 الحد الأدنى: "
            f"{MIN_TRADE_SCORE}/100",

            "",

            "المقصود هنا تخفيف القيود:",
            "لا نحتاج توافق جميع المؤشرات.",
            "تظهر الصفقة عند توفر مجموعة جيدة من العوامل."
        ]


        if result[
            "news_blocked"
        ]:

            lines.append(
                "🚨 التداول محجوب بسبب الأخبار."
            )


    lines += [

        "",

        "⚠️ التحليل مساعد لاتخاذ القرار "
        "وليس ضماناً للربح."
    ]


    return "\n".join(
        lines
    )


# ============================================================
# تسجيل الصفقة
# ============================================================

def register_trade(
    result
):

    if not result.get(
        "signal"
    ):

        return None


    trade = result.get(
        "trade"
    )

    if not trade:

        return None


    record = {

        "time":
            now_damascus()
            .isoformat(),

        "direction":
            result["direction"],

        "score":
            result["score"],

        "quality":
            result["quality"],

        "entry":
            trade["entry"],

        "sl":
            trade["sl"],

        "tp1":
            trade["tp1"],

        "tp2":
            trade["tp2"],

        "rr":
            trade["rr"],

        "status":
            "OPEN",

        "result":
            "OPEN"
    }


    TRADE_HISTORY.append(
        record
    )


    if len(
        TRADE_HISTORY
    ) > MAX_TRADE_HISTORY:

        del TRADE_HISTORY[
            :-MAX_TRADE_HISTORY
        ]


    return record


# ============================================================
# تقييم الصفقات المفتوحة
# ============================================================

def update_trade_results():

    if not TRADE_HISTORY:

        return


    try:

        quote = live_price()

        price = quote[
            "price"
        ]

    except Exception:

        return


    for trade in TRADE_HISTORY:

        if trade["status"] != "OPEN":

            continue


        direction = trade[
            "direction"
        ]


        if direction == "BUY":

            if price <= trade["sl"]:

                trade["status"] = "CLOSED"

                trade["result"] = "LOSS"

            elif price >= trade["tp2"]:

                trade["status"] = "CLOSED"

                trade["result"] = "WIN"

            elif price >= trade["tp1"]:

                trade["result"] = "TP1"


        elif direction == "SELL":

            if price >= trade["sl"]:

                trade["status"] = "CLOSED"

                trade["result"] = "LOSS"

            elif price <= trade["tp2"]:

                trade["status"] = "CLOSED"

                trade["result"] = "WIN"

            elif price <= trade["tp1"]:

                trade["result"] = "TP1"


# ============================================================
# التقرير اليومي
# ============================================================

def build_daily_report():

    update_trade_results()


    today = (
        now_damascus()
        .date()
    )


    trades = []


    for trade in TRADE_HISTORY:

        try:

            dt = datetime.fromisoformat(
                trade["time"]
            )

            if dt.date() == today:

                trades.append(
                    trade
                )

        except Exception:

            continue


    total = len(
        trades
    )

    wins = sum(
        1
        for x in trades
        if x["result"] == "WIN"
    )

    losses = sum(
        1
        for x in trades
        if x["result"] == "LOSS"
    )

    tp1 = sum(
        1
        for x in trades
        if x["result"] == "TP1"
    )

    open_trades = sum(
        1
        for x in trades
        if x["status"] == "OPEN"
    )


    if total > 0:

        win_rate = (
            wins /
            total
            *
            100
        )

    else:

        win_rate = 0


    lines = [

        "📅 التقرير اليومي للصفقات",

        "━━━━━━━━━━━━━━━━━━",

        f"📆 التاريخ: "
        f"{today.strftime('%Y-%m-%d')}",

        "",

        f"🎯 إجمالي الإشارات: "
        f"{total}",

        f"🟢 رابحة بالكامل: "
        f"{wins}",

        f"🔴 خاسرة: "
        f"{losses}",

        f"🟡 وصلت TP1: "
        f"{tp1}",

        f"⏳ مفتوحة: "
        f"{open_trades}",

        f"📊 نسبة الفوز التقريبية: "
        f"{win_rate:.1f}%",

        ""
    ]


    if trades:

        lines.append(
            "📋 الصفقات:"
        )

        lines.append("")


        for i, trade in enumerate(
            trades,
            1
        ):

            icon = {

                "WIN":
                    "🟢",

                "LOSS":
                    "🔴",

                "TP1":
                    "🟡",

                "OPEN":
                    "⏳"

            }.get(
                trade["result"],
                "⚪"
            )


            direction = (
                "شراء"
                if trade["direction"]
                == "BUY"
                else "بيع"
            )


            lines.append(

                f"{i}. {icon} "
                f"{direction} | "
                f"{trade['score']}/100 | "
                f"{trade['quality']} | "
                f"{trade['result']}"

            )


    else:

        lines.append(
            "لا توجد صفقات مسجلة اليوم."
        )


    lines += [

        "",

        "⚠️ النتائج الحالية مبنية على "
        "السجل الداخلي للصفقات المرشحة."
    ]


    return "\n".join(
        lines
    )


# ============================================================
# التقرير الأسبوعي
# ============================================================

def build_weekly_report():

    result = evaluate_signal()

    mtf = result[
        "mtf"
    ]

    w = mtf[
        "w1"
    ]

    d = mtf[
        "d1"
    ]

    h = mtf[
        "h4"
    ]

    levels = result[
        "levels"
    ]


    score = result[
        "score"
    ]


    if (
        w["direction"]
        == "BUY"
    ):

        weekly_bias = (
            "🟢 صاعد"
        )

    elif (
        w["direction"]
        == "SELL"
    ):

        weekly_bias = (
            "🔴 هابط"
        )

    else:

        weekly_bias = (
            "🟡 محايد"
        )


    if (
        result["direction"]
        == "BUY"
    ):

        scenario = (
            "الأولوية لصفقات الشراء "
            "من الدعم مع مراقبة اختراق المقاومات."
        )

    elif (
        result["direction"]
        == "SELL"
    ):

        scenario = (
            "الأولوية لصفقات البيع "
            "من المقاومة مع مراقبة كسر الدعوم."
        )

    else:

        scenario = (
            "السوق غير حاسم حالياً، "
            "والأفضل انتظار تأكيد من الدعم أو المقاومة."
        )


    return (

        "📅 التقرير الاستراتيجي الأسبوعي — XAU/USD\n"

        "━━━━━━━━━━━━━━━━━━━━\n\n"

        f"🕐 توقيت دمشق: "
        f"{now_damascus().strftime('%Y-%m-%d %H:%M')}\n"

        f"💰 السعر: "
        f"{result['price']:.2f}\n\n"

        "🧠 الرؤية الاستراتيجية\n"

        "━━━━━━━━━━━━━━━━━━━━\n"

        f"🌍 W1: "
        f"{weekly_bias}\n"

        f"📊 D1: "
        f"{d['direction']}\n"

        f"⏱ H4: "
        f"{h['direction']}\n"

        f"🏗 هيكل H4: "
        f"{h['structure']}\n"

        f"💪 النقاط: "
        f"{score}/100\n"

        f"🏆 الجودة: "
        f"{result['quality']}\n\n"

        "💧 خريطة السيولة\n"

        "━━━━━━━━━━━━━━━━━━━━\n"

        f"🟢 S1: "
        f"{format_zone(levels, 'support1')}\n"

        f"🟢 S2: "
        f"{format_zone(levels, 'support2')}\n"

        f"🟢 S3: "
        f"{format_zone(levels, 'support3')}\n\n"

        f"🔴 R1: "
        f"{format_zone(levels, 'resistance1')}\n"

        f"🔴 R2: "
        f"{format_zone(levels, 'resistance2')}\n"

        f"🔴 R3: "
        f"{format_zone(levels, 'resistance3')}\n\n"

        "🏦 العامل المؤسسي\n"

        "━━━━━━━━━━━━━━━━━━━━\n"

        f"الاتجاه: "
        f"{result['institutional']['direction']}\n"

        f"الحالة: "
        f"{result['institutional']['quality']}\n\n"

        "🧭 سيناريو الأسبوع\n"

        "━━━━━━━━━━━━━━━━━━━━\n"

        f"{scenario}\n\n"

        "📌 منهجية التعامل\n"

        "━━━━━━━━━━━━━━━━━━━━\n"

        "• الاتجاه الأكبر أولاً.\n"

        "• الهيكل والسلوك السعري ثانياً.\n"

        "• الدعم والمقاومة لتحديد مناطق الدخول والخروج.\n"

        "• العامل المؤسسي يدخل كعامل مستقل.\n"

        "• لا تعتمد الصفقة على مؤشر واحد.\n\n"

        "⚠️ التقرير تحليلي ولا يضمن الربح."
    )


# ============================================================
# Telegram Reply
# ============================================================

async def reply(
    update,
    text
):

    try:

        await update.message.reply_text(
            text
        )

    except Exception:

        logger.exception(
            "Telegram reply error"
        )


# ============================================================
# Start
# ============================================================

async def start(
    update,
    context
):

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
            "📅 نتائج اليوم"
        ],

        [
            "📰 الأخبار",
            "💰 سعر الذهب"
        ],

        [
            "🌍 الأسواق",
            "🟢 حالة النظام"
        ]
    ]


    text = (

        f"🤖 XAU SMART TRADER {VERSION}\n\n"

        "🥇 محلل الذهب XAU/USD\n\n"

        "يعتمد على:\n"

        "W1 + D1 + H4 + H1 + M15\n"

        "Structure + Momentum + Volume\n"

        "Fibonacci + FVG\n"

        "Support / Resistance\n"

        "Institutional Layer\n\n"

        f"🎯 الحد الأدنى للصفقة: "
        f"{MIN_TRADE_SCORE}/100\n\n"

        "اختر العملية 👇"
    )


    await update.message.reply_text(

        text,

        reply_markup=
        ReplyKeyboardMarkup(
            keyboard,
            resize_keyboard=True
        )
    )


# ============================================================
# التحليل الكامل
# ============================================================

async def full_analysis(
    update,
    context
):

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


# ============================================================
# التحليل السريع
# ============================================================

async def quick_analysis(
    update,
    context
):

    try:

        m15 = analyze(
            get_bars(
                "15m",
                200
            )
        )

        price = live_price()


        direction = {

            "BUY":
                "🟢 شراء",

            "SELL":
                "🔴 بيع",

            "WAIT":
                "🟡 انتظار"

        }.get(
            m15["direction"],
            "🟡 انتظار"
        )


        await reply(

            update,

            (

                f"⚡ التحليل السريع {VERSION}\n\n"

                f"💰 السعر: "
                f"{price['price']:.2f}\n"

                f"📈 الاتجاه: "
                f"{direction}\n"

                f"💪 النقاط: "
                f"{m15['score']}/100\n"

                f"RSI: "
                f"{m15['rsi']:.1f}\n"

                f"MACD: "
                f"{m15['macd']:.2f}\n"

                f"ADX: "
                f"{m15['adx']:.1f}\n"

                f"الهيكل: "
                f"{m15['structure']}\n"

                f"الحجم: "
                f"{m15['volume_state']} "
                f"({m15['volume_ratio']:.2f}x)"

            )
        )


    except Exception as e:

        await reply(
            update,
            f"❌ تعذر التحليل السريع: {e}"
        )


# ============================================================
# صفقة الآن
# ============================================================

async def trade_now(
    update,
    context
):

    try:

        result = await asyncio.to_thread(
            evaluate_signal
        )


        if result[
            "news_blocked"
        ]:

            await reply(

                update,

                "🚨 لا توجد صفقة الآن.\n\n"
                "تم تفعيل الحماية الإخبارية."
            )

            return


        if not result[
            "signal"
        ]:

            await reply(

                update,

                "⏳ لا توجد صفقة مرشحة حالياً.\n\n"

                f"💪 النقاط: "
                f"{result['score']}/100\n"

                f"🎯 المطلوب: "
                f"{MIN_TRADE_SCORE}/100\n"

                f"🏆 الجودة الحالية: "
                f"{result['quality']}\n\n"

                "تم تخفيف القيود السابقة، "
                "ولا يشترط توافق جميع المؤشرات."
            )

            return


        trade = result[
            "trade"
        ]


        register_trade(
            result
        )


        direction = (

            "🟢 شراء"

            if result["direction"]
            == "BUY"

            else

            "🔴 بيع"
        )


        text = (

            "🚨 XAU SMART TRADER\n"

            "━━━━━━━━━━━━━━━━━━\n"

            "🎯 صفقة مرشحة\n\n"

            f"📈 الصفقة: "
            f"{direction}\n"

            f"💪 النقاط: "
            f"{result['score']}/100\n"

            f"🏆 الجودة: "
            f"{result['quality_icon']} "
            f"{result['quality']}\n\n"

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

            "🧠 عوامل التلاقي:\n"

            +
            "\n".join(
                "• " + x
                for x in result[
                    "factors"
                ][:8]
            )

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


# ============================================================
# الدعوم والمقاومات
# ============================================================

async def show_levels(
    update,
    context
):

    try:

        levels = support_resistance(
            get_bars(
                "1h",
                250
            )
        )


        text = (

            "📍 XAU/USD — مناطق السوق\n\n"

            f"🟢 S1: "
            f"{format_zone(levels, 'support1')}\n"

            f"🟢 S2: "
            f"{format_zone(levels, 'support2')}\n"

            f"🟢 S3: "
            f"{format_zone(levels, 'support3')}\n\n"

            f"🔴 R1: "
            f"{format_zone(levels, 'resistance1')}\n"

            f"🔴 R2: "
            f"{format_zone(levels, 'resistance2')}\n"

            f"🔴 R3: "
            f"{format_zone(levels, 'resistance3')}\n\n"

            "📌 تستخدم هذه المناطق أيضاً "
            "في تحديد الدخول ووقف الخسارة والأهداف."
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


# ============================================================
# سعر الذهب
# ============================================================

async def gold_price(
    update,
    context
):

    try:

        q = await asyncio.to_thread(
            live_price
        )


        await reply(

            update,

            (

                "💰 XAU/USD — السعر اللحظي\n\n"

                f"السعر: "
                f"{q['price']:.2f}\n"

                f"المصدر: "
                f"{q['source']}\n"

                f"عمر السعر: "
                f"{q.get('age')}\n"

                f"توقيت دمشق: "
                f"{now_damascus().strftime('%Y-%m-%d %H:%M:%S')}"

            )
        )


    except Exception as e:

        await reply(
            update,
            f"❌ تعذر جلب السعر: {e}"
        )


# ============================================================
# التقرير الأسبوعي
# ============================================================

async def weekly_report(
    update,
    context
):

    try:

        text = await asyncio.to_thread(
            build_weekly_report
        )

        await reply(
            update,
            text
        )


    except Exception as e:

        await reply(

            update,

            "❌ تعذر إنشاء التقرير الأسبوعي.\n"
            f"السبب: {e}"
        )


# ============================================================
# التقرير اليومي
# ============================================================

async def daily_report(
    update,
    context
):

    try:

        text = await asyncio.to_thread(
            build_daily_report
        )

        await reply(
            update,
            text
        )


    except Exception as e:

        await reply(

            update,

            "❌ تعذر إنشاء التقرير اليومي.\n"
            f"السبب: {e}"
        )


# ============================================================
# الأخبار
# ============================================================

async def news_status(
    update,
    context
):

    blocked, text = news_filter()


    await reply(

        update,

        (

            "📰 فلتر الأخبار\n\n"

            f"{'🚨 التداول محجوب' if blocked else '🟢 التداول غير محجوب'}\n\n"

            f"{text}\n\n"

            f"الحماية: "
            f"{NEWS_BEFORE_MIN} دقيقة قبل الخبر + "
            f"{NEWS_AFTER_MIN} دقيقة بعده."

        )
    )


# ============================================================
# الأسواق
# ============================================================

async def markets(
    update,
    context
):

    now = now_damascus()


    await reply(

        update,

        (

            "🌍 جلسات الذهب\n\n"

            "🇯🇵 آسيا: سيولة آسيوية\n"

            "🇬🇧 لندن: ارتفاع السيولة\n"

            "🇺🇸 نيويورك: أعلى التقلبات عادةً\n\n"

            f"🕐 توقيت دمشق الآن: "
            f"{now.strftime('%H:%M:%S')}\n\n"

            "⚠️ أوقات الافتتاح والتداخل "
            "تتأثر بالتوقيت الصيفي."

        )
    )


# ============================================================
# الحالة
# ============================================================

async def status(
    update,
    context
):

    await reply(

        update,

        (

            f"🟢 XAU SMART TRADER {VERSION}\n\n"

            "حالة النظام: يعمل\n"

            "Telegram: متصل\n"

            "Flask: يعمل\n"

            "البيانات: Biquote OHLC\n"

            "التحليل: W1/D1/H4/H1/M15\n"

            "Structure: مفعّل\n"

            "Volume: مفعّل\n"

            "RSI: مفعّل\n"

            "MACD: مفعّل\n"

            "ADX: مفعّل\n"

            "Fibonacci: مفعّل\n"

            "FVG: مفعّل\n"

            "Support/Resistance: مفعّل\n"

            "Institutional Layer: مفعّل\n\n"

            f"🎯 الحد الأدنى للصفقة: "
            f"{MIN_TRADE_SCORE}/100\n\n"

            f"📊 الصفقات المسجلة: "
            f"{len(TRADE_HISTORY)}"

        )
    )


# ============================================================
# Router
# ============================================================

async def router(
    update,
    context
):

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

        "📅 نتائج اليوم":
            daily_report,

        "📰 الأخبار":
            news_status,

        "💰 سعر الذهب":
            gold_price,

        "🌍 الأسواق":
            markets,

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

        return (
            "Bot not ready",
            503
        )


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
# Telegram
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


    APPLICATION.add_handler(

        CommandHandler(
            "start",
            start
        )
    )


    APPLICATION.add_handler(

        MessageHandler(

            filters.TEXT
            &
            ~filters.COMMAND,

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


    # --------------------------------------------------------
    # لا يوجد هنا حالياً نظام إرسال صفقات كل 15 دقيقة.
    #
    # تم تأجيله عمداً حتى يتم التأكد من صحة التحليل.
    # --------------------------------------------------------


    while True:

        await asyncio.sleep(
            3600
        )


# ============================================================
# Flask
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


# ============================================================
# تشغيل
# ============================================================

if __name__ == "__main__":

    main()
