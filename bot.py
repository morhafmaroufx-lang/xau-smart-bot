import os
import asyncio
import threading
import requests
import pandas as pd
import numpy as np

from flask import Flask
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes


# =========================================================
# CONFIG
# =========================================================

TOKEN = os.environ.get("TELEGRAM_TOKEN")

DATA_URL = "https://biquote.io/api/XAUUSD/ohlc"

app = Flask(__name__)


# =========================================================
# DATA HELPERS
# =========================================================

def get_bars(interval, limit=250):
    """
    جلب بيانات XAUUSD ومعالجتها بشكل آمن.
    """

    response = requests.get(
        DATA_URL,
        params={
            "interval": interval,
            "limit": limit
        },
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

    # -----------------------------------------------------
    # التاريخ
    # -----------------------------------------------------

    if "openTime" in df.columns:

        try:

            raw_time = df["openTime"]

            if pd.api.types.is_numeric_dtype(raw_time):

                maximum = raw_time.dropna().max()

                if maximum > 10_000_000_000:

                    df["datetime"] = pd.to_datetime(
                        raw_time,
                        unit="ms",
                        errors="coerce",
                        utc=True
                    )

                elif maximum > 10_000_000:

                    df["datetime"] = pd.to_datetime(
                        raw_time,
                        unit="s",
                        errors="coerce",
                        utc=True
                    )

                else:

                    df["datetime"] = pd.to_datetime(
                        raw_time,
                        errors="coerce",
                        utc=True
                    )

            else:

                df["datetime"] = pd.to_datetime(
                    raw_time,
                    errors="coerce",
                    utc=True
                )

        except Exception:

            df["datetime"] = pd.NaT

    else:

        df["datetime"] = pd.NaT

    # -----------------------------------------------------
    # ترتيب البيانات
    # -----------------------------------------------------

    if df["datetime"].notna().sum() > 0:

        df = df.sort_values(
            "datetime"
        )

    else:

        df = df.iloc[::-1]

    df = df.dropna(
        subset=[
            "open",
            "high",
            "low",
            "close",
            "tickVolume"
        ]
    )

    df = df.reset_index(
        drop=True
    )

    return df


def get_live_price():
    """
    السعر الحالي من آخر شمعة M5.
    """

    df = get_bars(
        "5m",
        5
    )

    return float(
        df["close"].iloc[-1]
    )


# =========================================================
# WEEKLY BUILDER
# =========================================================

def build_weekly_from_daily(daily_df):

    """
    بناء W1 من D1 بطريقة آمنة.

    إذا كانت التواريخ غير متاحة:
    لا ينهار البوت، وإنما يعيد None.
    """

    if "datetime" not in daily_df.columns:

        return None

    valid = daily_df[
        daily_df["datetime"].notna()
    ].copy()

    if len(valid) < 2:

        return None

    valid = valid.set_index(
        "datetime"
    )

    weekly = valid.resample(
        "W-FRI"
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

    return weekly


# =========================================================
# INDICATORS
# =========================================================

def calculate_ema(series, period):

    return series.ewm(
        span=period,
        adjust=False
    ).mean()


def calculate_rsi(series, period=14):

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

    rsi = 100 - (
        100 / (1 + rs)
    )

    return rsi


def calculate_macd(
    series,
    fast=8,
    slow=21,
    signal=5
):

    ema_fast = calculate_ema(
        series,
        fast
    )

    ema_slow = calculate_ema(
        series,
        slow
    )

    macd = ema_fast - ema_slow

    signal_line = calculate_ema(
        macd,
        signal
    )

    histogram = (
        macd - signal_line
    )

    return (
        macd,
        signal_line,
        histogram
    )


def calculate_atr(
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

    atr = true_range.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    return atr


# =========================================================
# VOLUME RATIO
# =========================================================

def calculate_volume_ratio(
    volume_series
):

    if len(volume_series) < 6:

        return 0.0

    current = float(
        volume_series.iloc[-1]
    )

    # نستبعد الشمعة الحالية من المتوسط
    previous = volume_series.iloc[
        -21:-1
    ]

    if len(previous) == 0:

        previous = volume_series.iloc[
            :-1
        ]

    average = float(
        previous.mean()
    )

    if average <= 0:

        return 0.0

    return current / average


# =========================================================
# GENERAL SCORING ENGINE
# =========================================================

def calculate_signal_score(
    price,
    ema20,
    ema50,
    ema200,
    rsi,
    macd,
    signal,
    volume_ratio
):

    bullish = 0
    bearish = 0

    warnings = []

    # -----------------------------------------------------
    # EMA TREND
    # -----------------------------------------------------

    if (
        ema200 is not None
        and price > ema20 > ema50 > ema200
    ):

        bullish += 40

    elif (
        ema200 is not None
        and price < ema20 < ema50 < ema200
    ):

        bearish += 40

    elif price > ema20 > ema50:

        bullish += 30

    elif price < ema20 < ema50:

        bearish += 30

    elif price > ema20:

        bullish += 20

    elif price < ema20:

        bearish += 20

    # -----------------------------------------------------
    # RSI
    # -----------------------------------------------------

    if 50 <= rsi < 70:

        bullish += 15

    elif 30 < rsi < 50:

        bearish += 15

    elif rsi >= 70:

        warnings.append(
            "⚠️ RSI مرتفع - احتمال تصحيح"
        )

        if bullish > bearish:
            bullish += 5

    elif rsi <= 30:

        warnings.append(
            "⚠️ RSI منخفض - احتمال ارتداد"
        )

        if bearish > bullish:
            bearish += 5

    # -----------------------------------------------------
    # MACD
    # -----------------------------------------------------

    if macd > signal:

        bullish += 25

    elif macd < signal:

        bearish += 25

    # -----------------------------------------------------
    # MACD CONFLICT
    # -----------------------------------------------------

    if (
        bullish > bearish
        and macd < signal
    ):

        warnings.append(
            "⚠️ MACD لا يؤكد الصعود"
        )

    if (
        bearish > bullish
        and macd > signal
    ):

        warnings.append(
            "⚠️ MACD لا يؤكد الهبوط"
        )

    # -----------------------------------------------------
    # VOLUME
    # -----------------------------------------------------

    if volume_ratio >= 1.20:

        if bullish > bearish:

            bullish += 20

        elif bearish > bullish:

            bearish += 20

    else:

        warnings.append(
            "ℹ️ Volume ضعيف"
        )

    # -----------------------------------------------------
    # FINAL DIRECTION
    # -----------------------------------------------------

    if bullish > bearish:

        direction = "BUY"

        score = bullish

    elif bearish > bullish:

        direction = "SELL"

        score = bearish

    else:

        direction = "WAIT"

        score = 0

    score = max(
        0,
        min(
            score,
            100
        )
    )

    # -----------------------------------------------------
    # SIGNAL LEVEL
    # -----------------------------------------------------

    if score >= 70:

        signal = direction

    elif score >= 50:

        signal = "WATCH"

    else:

        signal = "WAIT"

    return (
        signal,
        direction,
        round(score),
        warnings
    )


# =========================================================
# START
# =========================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(

        "🤖 XAU SMART BOT v8\n"
        "🟢 البوت يعمل بنجاح\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📊 التحليلات\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        "📅 /weekly\n"
        "التحليل الأسبوعي\n\n"

        "📊 /daily\n"
        "التحليل اليومي\n\n"

        "⚡ /scalp\n"
        "التحليل اللحظي\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "💰 الأدوات\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        "💰 /price\n"
        "بيانات الذهب الحالية\n\n"

        "🟢 /status\n"
        "حالة البوت\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "🎯 اختر التحليل الذي تريده."

    )


# =========================================================
# STATUS
# =========================================================

async def status(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(

        "🟢 XAU Smart Bot v8 يعمل\n\n"

        "📈 السوق: XAUUSD\n"
        "🤖 النظام: Multi-Timeframe Analysis\n\n"

        "📅 Weekly: ON\n"
        "📊 Daily: ON\n"
        "⚡ Scalp: ON\n\n"

        "🎯 Entry Filter: ON\n"
        "🛡️ ATR Risk Filter: ON\n"
        "🔒 المؤشرات التفصيلية: مخفية\n\n"

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

        intervals = [
            ("5m", "M5"),
            ("15m", "M15"),
            ("1h", "H1"),
            ("4h", "H4"),
            ("1d", "D1")
        ]

        results = []

        live_price = get_live_price()

        for interval, name in intervals:

            try:

                df = get_bars(
                    interval,
                    5
                )

                last = df.iloc[-1]

                results.append(

                    f"✅ {name}\n"
                    f"Open: {last['open']:.2f}\n"
                    f"High: {last['high']:.2f}\n"
                    f"Low: {last['low']:.2f}\n"
                    f"Close: {last['close']:.2f}\n"
                    f"Volume: {last['tickVolume']:.0f}"

                )

            except Exception as e:

                results.append(
                    f"❌ {name}: {str(e)}"
                )

        # -------------------------------------------------
        # WEEKLY DATA
        # -------------------------------------------------

        try:

            d1 = get_bars(
                "1d",
                250
            )

            w1 = build_weekly_from_daily(
                d1
            )

            if w1 is not None and len(w1) > 0:

                last_w = w1.iloc[-1]

                results.append(

                    "📅 W1\n"
                    f"Open: {last_w['open']:.2f}\n"
                    f"High: {last_w['high']:.2f}\n"
                    f"Low: {last_w['low']:.2f}\n"
                    f"Close: {last_w['close']:.2f}\n"
                    f"Volume: {last_w['tickVolume']:.0f}"

                )

            else:

                results.append(
                    "⚠️ W1: تعذر بناء البيانات الأسبوعية"
                )

        except Exception:

            results.append(
                "⚠️ W1: تعذر بناء البيانات الأسبوعية"
            )

        message = (

            "🥇 XAUUSD DATA TEST v8\n\n"

            f"💰 LIVE PRICE: "
            f"{live_price:.2f}\n\n"

            + "\n\n".join(results)

            + "\n\n"

            "🟢 إذا ظهرت الفريمات بنجاح "
            "فمصدر البيانات يعمل."

        )

        await update.message.reply_text(
            message
        )

    except requests.exceptions.Timeout:

        await update.message.reply_text(
            "⏳ انتهت مهلة الاتصال بمصدر البيانات."
        )

    except Exception as e:

        await update.message.reply_text(
            f"❌ خطأ في اختبار البيانات:\n{str(e)}"
        )


# =========================================================
# ANALYSIS ENGINE
# =========================================================

def analyze_dataframe(
    df,
    mode="normal"
):

    if len(df) < 50:

        raise ValueError(
            f"بيانات غير كافية: {len(df)} شمعة"
        )

    close = df["close"]

    # -----------------------------------------------------
    # EMA
    # -----------------------------------------------------

    ema20 = calculate_ema(
        close,
        20
    ).iloc[-1]

    ema50 = calculate_ema(
        close,
        50
    ).iloc[-1]

    ema200 = None

    if len(df) >= 220:

        ema200 = calculate_ema(
            close,
            200
        ).iloc[-1]

    # -----------------------------------------------------
    # RSI
    # -----------------------------------------------------

    rsi_period = 9 if mode == "scalp" else 14

    rsi = calculate_rsi(
        close,
        rsi_period
    ).iloc[-1]

    # -----------------------------------------------------
    # MACD
    # -----------------------------------------------------

    if mode == "scalp":

        macd, signal, _ = calculate_macd(
            close,
            5,
            13,
            4
        )

    else:

        macd, signal, _ = calculate_macd(
            close,
            8,
            21,
            5
        )

    macd_value = macd.iloc[-1]

    signal_value = signal.iloc[-1]

    # -----------------------------------------------------
    # ATR
    # -----------------------------------------------------

    atr = calculate_atr(
        df["high"],
        df["low"],
        close,
        14
    ).iloc[-1]

    price = float(
        close.iloc[-1]
    )

    volume_ratio = calculate_volume_ratio(
        df["tickVolume"]
    )

    return {
        "price": price,
        "ema20": float(ema20),
        "ema50": float(ema50),
        "ema200": (
            float(ema200)
            if ema200 is not None
            else None
        ),
        "rsi": float(rsi),
        "macd": float(macd_value),
        "signal": float(signal_value),
        "atr": float(atr),
        "volume_ratio": float(volume_ratio)
    }


# =========================================================
# TREND
# =========================================================

def determine_trend(
    data
):

    price = data["price"]

    ema20 = data["ema20"]

    ema50 = data["ema50"]

    ema200 = data["ema200"]

    if (
        ema200 is not None
        and price > ema20 > ema50 > ema200
    ):

        return "🟢 صاعد قوي"

    if (
        price > ema20
        and ema20 > ema50
    ):

        return "🟢 صاعد"

    if (
        ema200 is not None
        and price < ema20 < ema50 < ema200
    ):

        return "🔴 هابط قوي"

    if (
        price < ema20
        and ema20 < ema50
    ):

        return "🔴 هابط"

    return "🟡 متذبذب"


# =========================================================
# DAILY
# =========================================================

async def daily(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        config = [
            ("1d", "D1", 0.40),
            ("4h", "H4", 0.35),
            ("1h", "H1", 0.25)
        ]

        results = []

        weighted = []

        buy_count = 0

        sell_count = 0

        for interval, name, weight in config:

            try:

                df = get_bars(
                    interval,
                    250
                )

                data = analyze_dataframe(
                    df,
                    "normal"
                )

                signal, direction, score, warnings = (
                    calculate_signal_score(
                        data["price"],
                        data["ema20"],
                        data["ema50"],
                        data["ema200"],
                        data["rsi"],
                        data["macd"],
                        data["signal"],
                        data["volume_ratio"]
                    )
                )

                trend = determine_trend(
                    data
                )

                if direction == "BUY":

                    directional = score
                    buy_count += 1

                elif direction == "SELL":

                    directional = -score
                    sell_count += 1

                else:

                    directional = 0

                weighted.append(
                    directional * weight
                )

                warning_text = (
                    " | ".join(warnings)
                    if warnings
                    else "لا توجد تحذيرات"
                )

                results.append(

                    f"📊 {name}\n"
                    f"Trend: {trend}\n"
                    f"Direction: {direction}\n"
                    f"Score: {score}%\n"
                    f"⚠️ {warning_text}"

                )

            except Exception as e:

                results.append(

                    f"⚠️ {name}\n"
                    f"تعذر تحليل الفريم"

                )

        # -------------------------------------------------
        # FINAL
        # -------------------------------------------------

        if not weighted:

            raise ValueError(
                "تعذر تحليل الفريمات."
            )

        final_score = sum(
            weighted
        )

        confidence = round(
            min(
                abs(final_score),
                100
            )
        )

        if final_score >= 65:

            final_signal = "🟢 BUY"

        elif final_score >= 45:

            final_signal = "🟡 WATCH BUY"

        elif final_score <= -65:

            final_signal = "🔴 SELL"

        elif final_score <= -45:

            final_signal = "🟠 WATCH SELL"

        else:

            final_signal = "🟡 WAIT"

        message = (

            "🤖 XAU SMART BOT v8\n"
            "📊 DAILY ANALYSIS\n\n"

            + "\n\n".join(results)

            + "\n\n"

            "━━━━━━━━━━━━━━━━━━\n"

            f"🎯 FINAL SIGNAL: "
            f"{final_signal}\n"

            f"💪 CONFIDENCE: "
            f"{confidence}%\n"

            f"📊 AGREEMENT: "
            f"🟢 BUY {buy_count}/3 | "
            f"🔴 SELL {sell_count}/3\n"

            "━━━━━━━━━━━━━━━━━━\n\n"

            "🔒 المؤشرات التفصيلية مخفية\n"
            "⚠️ Confidence = توافق الإشارات "
            "وليس احتمال الربح."

        )

        await update.message.reply_text(
            message
        )

    except requests.exceptions.Timeout:

        await update.message.reply_text(
            "⏳ انتهت مهلة الاتصال ببيانات الذهب."
        )

    except Exception as e:

        await update.message.reply_text(
            f"❌ خطأ في التحليل اليومي:\n{str(e)}"
        )


# =========================================================
# WEEKLY
# =========================================================

async def weekly(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        # -------------------------------------------------
        # D1 → W1
        # -------------------------------------------------

        d1 = get_bars(
            "1d",
            250
        )

        w1 = build_weekly_from_daily(
            d1
        )

        if w1 is None or len(w1) < 20:

            raise ValueError(
                "بيانات W1 غير كافية."
            )

        config = [
            ("W1", w1, 0.50),
            ("D1", d1, 0.30),
            ("H4", get_bars("4h", 250), 0.20)
        ]

        results = []

        weighted = []

        buy_count = 0

        sell_count = 0

        for name, df, weight in config:

            try:

                # W1 لا يحتاج EMA200 إذا لم تتوفر
                data = analyze_dataframe(
                    df,
                    "normal"
                )

                signal, direction, score, warnings = (
                    calculate_signal_score(
                        data["price"],
                        data["ema20"],
                        data["ema50"],
                        data["ema200"],
                        data["rsi"],
                        data["macd"],
                        data["signal"],
                        data["volume_ratio"]
                    )
                )

                trend = determine_trend(
                    data
                )

                if (
                    name == "W1"
                    and data["ema200"] is None
                ):

                    warnings.append(
                        "ℹ️ EMA200 الأسبوعي غير متاح "
                        "بسبب نقص التاريخ"
                    )

                if direction == "BUY":

                    weighted.append(
                        score * weight
                    )

                    buy_count += 1

                elif direction == "SELL":

                    weighted.append(
                        -score * weight
                    )

                    sell_count += 1

                else:

                    weighted.append(
                        0
                    )

                warning_text = (
                    " | ".join(warnings)
                    if warnings
                    else "لا توجد تحذيرات"
                )

                results.append(

                    f"📊 {name}\n"
                    f"Trend: {trend}\n"
                    f"Direction: {direction}\n"
                    f"Score: {score}%\n"
                    f"⚠️ {warning_text}"

                )

            except Exception:

                results.append(

                    f"⚠️ {name}\n"
                    f"تعذر تحليل الفريم"

                )

        final_score = sum(
            weighted
        )

        confidence = round(
            min(
                abs(final_score),
                100
            )
        )

        if final_score >= 65:

            final_signal = "🟢 BUY"

        elif final_score >= 45:

            final_signal = "🟡 WATCH BUY"

        elif final_score <= -65:

            final_signal = "🔴 SELL"

        elif final_score <= -45:

            final_signal = "🟠 WATCH SELL"

        else:

            final_signal = "🟡 WAIT"

        message = (

            "🤖 XAU SMART BOT v8\n"
            "📅 WEEKLY ANALYSIS\n\n"

            + "\n\n".join(results)

            + "\n\n"

            "━━━━━━━━━━━━━━━━━━\n"

            f"🎯 FINAL WEEKLY: "
            f"{final_signal}\n"

            f"💪 CONFIDENCE: "
            f"{confidence}%\n"

            f"📊 AGREEMENT: "
            f"🟢 BUY {buy_count}/3 | "
            f"🔴 SELL {sell_count}/3\n"

            "━━━━━━━━━━━━━━━━━━\n\n"

            "🔒 المؤشرات التفصيلية مخفية\n"
            "⚠️ Confidence = توافق الإشارات "
            "وليس احتمال الربح."

        )

        await update.message.reply_text(
            message
        )

    except requests.exceptions.Timeout:

        await update.message.reply_text(
            "⏳ انتهت مهلة بيانات التحليل الأسبوعي."
        )

    except Exception as e:

        await update.message.reply_text(
            f"❌ خطأ في التحليل الأسبوعي:\n{str(e)}"
        )


# =========================================================
# SCALP
# =========================================================

def scalp_score(
    data
):

    bullish = 0
    bearish = 0

    warnings = []

    price = data["price"]

    ema9 = data["ema9"]

    ema20 = data["ema20"]

    ema50 = data["ema50"]

    rsi = data["rsi"]

    macd = data["macd"]

    signal = data["signal"]

    volume_ratio = data["volume_ratio"]

    atr = data["atr"]

    # -----------------------------------------------------
    # TREND
    # -----------------------------------------------------

    if price > ema9 > ema20 > ema50:

        bullish += 40

    elif price < ema9 < ema20 < ema50:

        bearish += 40

    elif price > ema20 > ema50:

        bullish += 30

    elif price < ema20 < ema50:

        bearish += 30

    elif price > ema20:

        bullish += 20

    elif price < ema20:

        bearish += 20

    # -----------------------------------------------------
    # RSI
    # -----------------------------------------------------

    if 50 <= rsi < 70:

        bullish += 15

    elif 30 < rsi < 50:

        bearish += 15

    elif rsi >= 80:

        warnings.append(
            "⚠️ RSI تشبع شرائي قوي"
        )

    elif rsi >= 70:

        warnings.append(
            "⚠️ RSI مرتفع - لا نطارد السعر"
        )

    elif rsi <= 20:

        warnings.append(
            "⚠️ RSI تشبع بيعي قوي"
        )

    elif rsi <= 30:

        warnings.append(
            "⚠️ RSI منخفض"
        )

    # -----------------------------------------------------
    # MACD
    # -----------------------------------------------------

    if macd > signal:

        bullish += 25

    elif macd < signal:

        bearish += 25

    # -----------------------------------------------------
    # VOLUME
    # -----------------------------------------------------

    if volume_ratio >= 1.20:

        if bullish > bearish:

            bullish += 20

        elif bearish > bullish:

            bearish += 20

    else:

        warnings.append(
            "⚠️ Volume ضعيف"
        )

    # -----------------------------------------------------
    # DIRECTION
    # -----------------------------------------------------

    if bullish > bearish:

        direction = "BUY"
        score = bullish

    elif bearish > bullish:

        direction = "SELL"
        score = bearish

    else:

        direction = "WAIT"
        score = 0

    # -----------------------------------------------------
    # PRICE EXTENSION
    # -----------------------------------------------------

    extension = abs(
        price - ema20
    )

    if atr > 0:

        extension_ratio = (
            extension / atr
        )

        if extension_ratio >= 0.80:

            warnings.append(
                "⚠️ السعر ممتد عن EMA20"
            )

    # -----------------------------------------------------
    # SCORE
    # -----------------------------------------------------

    score = max(
        0,
        min(
            score,
            100
        )
    )

    return (
        direction,
        round(score),
        warnings
    )


async def scalp(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        config = [
            ("1h", "H1"),
            ("15m", "M15"),
            ("5m", "M5")
        ]

        results = []

        directional_scores = []

        buy_count = 0

        sell_count = 0

        all_warnings = []

        scalp_data = {}

        for interval, name in config:

            try:

                df = get_bars(
                    interval,
                    250
                )

                if len(df) < 60:

                    raise ValueError(
                        "بيانات غير كافية"
                    )

                close = df["close"]

                ema9 = calculate_ema(
                    close,
                    9
                ).iloc[-1]

                ema20 = calculate_ema(
                    close,
                    20
                ).iloc[-1]

                ema50 = calculate_ema(
                    close,
                    50
                ).iloc[-1]

                rsi = calculate_rsi(
                    close,
                    9
                ).iloc[-1]

                macd, signal, _ = (
                    calculate_macd(
                        close,
                        5,
                        13,
                        4
                    )
                )

                atr = calculate_atr(
                    df["high"],
                    df["low"],
                    close,
                    14
                ).iloc[-1]

                data = {

                    "price": float(
                        close.iloc[-1]
                    ),

                    "ema9": float(
                        ema9
                    ),

                    "ema20": float(
                        ema20
                    ),

                    "ema50": float(
                        ema50
                    ),

                    "rsi": float(
                        rsi
                    ),

                    "macd": float(
                        macd.iloc[-1]
                    ),

                    "signal": float(
                        signal.iloc[-1]
                    ),

                    "atr": float(
                        atr
                    ),

                    "volume_ratio":
                        calculate_volume_ratio(
                            df["tickVolume"]
                        )

                }

                direction, score, warnings = (
                    scalp_score(
                        data
                    )
                )

                scalp_data[name] = data

                if direction == "BUY":

                    directional_scores.append(
                        score
                    )

                    buy_count += 1

                elif direction == "SELL":

                    directional_scores.append(
                        -score
                    )

                    sell_count += 1

                else:

                    directional_scores.append(
                        0
                    )

                all_warnings.extend(
                    warnings
                )

                trend = "🟢 صاعد"

                if (
                    data["price"]
                    < data["ema20"]
                    < data["ema50"]
                ):

                    trend = "🔴 هابط"

                elif not (
                    data["price"]
                    > data["ema20"]
                    > data["ema50"]
                ):

                    trend = "🟡 متذبذب"

                warning_text = (
                    " | ".join(warnings)
                    if warnings
                    else "لا توجد تحذيرات"
                )

                results.append(

                    f"📊 {name}\n"
                    f"Trend: {trend}\n"
                    f"Direction: {direction}\n"
                    f"Score: {score}%\n"
                    f"⚠️ {warning_text}"

                )

            except Exception:

                results.append(

                    f"⚠️ {name}\n"
                    "تعذر تحليل الفريم"

                )

        # -------------------------------------------------
        # FINAL SCALP
        # -------------------------------------------------

        valid_scores = [
            x for x in directional_scores
            if x != 0
        ]

        if not valid_scores:

            final_signal = "🟡 WAIT"

            confidence = 0

            final_direction = "WAIT"

        else:

            average_score = (
                sum(valid_scores)
                / len(valid_scores)
            )

            confidence = round(
                abs(average_score)
            )

            if buy_count > sell_count:

                final_direction = "BUY"

            elif sell_count > buy_count:

                final_direction = "SELL"

            else:

                final_direction = "WAIT"

            # -------------------------------------------------
            # ENTRY FILTER
            # -------------------------------------------------

            entry_allowed = True

            m5 = scalp_data.get(
                "M5"
            )

            m15 = scalp_data.get(
                "M15"
            )

            if m5 and m15:

                # تمدد قوي
                if (
                    m5["price"] > m5["ema20"]
                    and
                    (
                        m5["price"]
                        - m5["ema20"]
                    )
                    >
                    m5["atr"] * 0.80
                ):

                    entry_allowed = False

                # RSI شديد الارتفاع
                if (
                    final_direction == "BUY"
                    and m5["rsi"] >= 80
                ):

                    entry_allowed = False

                # RSI شديد الانخفاض
                if (
                    final_direction == "SELL"
                    and m5["rsi"] <= 20
                ):

                    entry_allowed = False

            if (
                final_direction == "BUY"
                and confidence >= 70
                and entry_allowed
            ):

                final_signal = "🟢 BUY"

            elif (
                final_direction == "SELL"
                and confidence >= 70
                and entry_allowed
            ):

                final_signal = "🔴 SELL"

            elif (
                final_direction == "BUY"
                and confidence >= 50
            ):

                final_signal = "🟡 WATCH BUY"

            elif (
                final_direction == "SELL"
                and confidence >= 50
            ):

                final_signal = "🟠 WATCH SELL"

            else:

                final_signal = "🟡 WAIT"

        # -------------------------------------------------
        # ENTRY DECISION
        # -------------------------------------------------

        entry_text = (
            "🚫 لا يوجد Entry حاليًا."
        )

        if final_signal == "🟢 BUY":

            entry_text = (
                "🎯 Entry: متاح بعد تأكيد شمعة الدخول"
            )

        elif final_signal == "🔴 SELL":

            entry_text = (
                "🎯 Entry: متاح بعد تأكيد شمعة الدخول"
            )

        elif "WATCH" in final_signal:

            entry_text = (
                "⏳ Entry Filter: انتظار تصحيح "
                "أو تأكيد أفضل"
            )

        # إزالة التحذيرات المكررة
        unique_warnings = list(
            dict.fromkeys(
                all_warnings
            )
        )

        warning_text = (
            " | ".join(
                unique_warnings
            )
            if unique_warnings
            else "لا توجد تحذيرات"
        )

        message = (

            "🤖 XAU SMART BOT v8\n"
            "⚡ SCALP ANALYSIS\n\n"

            + "\n\n".join(results)

            + "\n\n"

            "━━━━━━━━━━━━━━━━━━\n"

            f"🎯 FINAL SCALP: "
            f"{final_signal}\n"

            f"💪 CONFIDENCE: "
            f"{confidence}%\n"

            f"📊 AGREEMENT: "
            f"🟢 BUY {buy_count}/3 | "
            f"🔴 SELL {sell_count}/3\n\n"

            f"{entry_text}\n\n"

            f"⚠️ {warning_text}\n"

            "━━━━━━━━━━━━━━━━━━\n\n"

            "🔒 المؤشرات التفصيلية مخفية\n"
            "⚠️ التحليل اللحظي لا يضمن الربح."

        )

        await update.message.reply_text(
            message
        )

    except requests.exceptions.Timeout:

        await update.message.reply_text(
            "⏳ انتهت مهلة بيانات التحليل اللحظي."
        )

    except Exception as e:

        await update.message.reply_text(
            f"❌ خطأ في التحليل اللحظي:\n{str(e)}"
        )


# =========================================================
# FLASK
# =========================================================

@app.route("/")
def home():

    return "XAU Smart Bot v8 is running!"


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
# TELEGRAM
# =========================================================

async def run_bot():

    if not TOKEN:

        raise RuntimeError(
            "TELEGRAM_TOKEN is missing."
        )

    application = (
        Application.builder()
        .token(TOKEN)
        .build()
    )

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

    await application.initialize()

    await application.start()

    await application.updater.start_polling()

    try:

        while True:

            await asyncio.sleep(
                3600
            )

    finally:

        await application.updater.stop()

        await application.stop()

        await application.shutdown()


# =========================================================
# MAIN
# =========================================================

def main():

    server = threading.Thread(
        target=run_server
    )

    server.daemon = True

    server.start()

    asyncio.run(
        run_bot()
    )


if __name__ == "__main__":

    main()
