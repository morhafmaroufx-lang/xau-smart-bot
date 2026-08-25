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

app = Flask(__name__)

SYMBOL = "XAUUSD"
DATA_URL = "https://biquote.io/api/XAUUSD/ohlc"


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
        raise ValueError(f"لا توجد بيانات للفريم {interval}")

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
# WEEKLY FROM D1
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


def macd(series, fast=8, slow=21, signal=5):
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
# ANALYSIS ENGINE
# =========================================================

def analyze_standard(
    df,
    ema200_required=False
):
    close = df["close"]

    current = float(
        close.iloc[-1]
    )

    ema20 = float(
        ema(close, 20).iloc[-1]
    )

    ema50 = float(
        ema(close, 50).iloc[-1]
    )

    ema200_series = ema(
        close,
        200
    )

    ema200 = float(
        ema200_series.iloc[-1]
    )

    rsi_value = float(
        rsi(
            close,
            14
        ).iloc[-1]
    )

    macd_line, signal_line, _ = macd(
        close,
        8,
        21,
        5
    )

    macd_value = float(
        macd_line.iloc[-1]
    )

    signal_value = float(
        signal_line.iloc[-1]
    )

    atr_value = float(
        atr(
            df["high"],
            df["low"],
            close,
            14
        ).iloc[-1]
    )

    volume = float(
        df["tickVolume"].iloc[-1]
    )

    avg_volume = float(
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

    # -----------------------------------------------------
    # TREND
    # -----------------------------------------------------

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
        else:
            bearish += 15

    # -----------------------------------------------------
    # EMA200
    # -----------------------------------------------------

    if len(df) >= 220:

        if (
            current > ema200
            and ema20 > ema50
        ):
            bullish += 15

        elif (
            current < ema200
            and ema20 < ema50
        ):
            bearish += 15

    else:
        warnings.append(
            "EMA200"
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
            bullish += 10

        elif bearish > bullish:
            bearish += 10

    else:

        warnings.append(
            "Volume ضعيف"
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
            min(score, 100)
        )
    )

    # -----------------------------------------------------
    # INDICATOR SUMMARY
    # -----------------------------------------------------

    if not warnings:

        indicator_state = "🟢 مؤكدة"

    elif len(warnings) <= 1:

        indicator_state = "🟢 جيدة"

    elif len(warnings) <= 2:

        indicator_state = "🟡 تحتاج تأكيد"

    else:

        indicator_state = "🟠 مختلطة"

    # -----------------------------------------------------
    # EXTENSION
    # -----------------------------------------------------

    extended = (
        abs(current - ema20)
        > atr_value * 0.45
    )

    if extended:

        warnings.append(
            "السعر ممتد"
        )

    return {
        "price": current,
        "ema20": ema20,
        "ema50": ema50,
        "ema200": ema200,
        "rsi": rsi_value,
        "macd": macd_value,
        "signal": signal_value,
        "atr": atr_value,
        "volume_ratio": volume_ratio,
        "trend": trend,
        "direction": direction,
        "score": score,
        "warnings": warnings,
        "indicator_state": indicator_state,
        "extended": extended,
        "bars": len(df)
    }


# =========================================================
# SCALP ENGINE
# =========================================================

def analyze_scalp(df):
    close = df["close"]

    current = float(
        close.iloc[-1]
    )

    ema9 = float(
        ema(
            close,
            9
        ).iloc[-1]
    )

    ema20 = float(
        ema(
            close,
            20
        ).iloc[-1]
    )

    ema50 = float(
        ema(
            close,
            50
        ).iloc[-1]
    )

    rsi_value = float(
        rsi(
            close,
            9
        ).iloc[-1]
    )

    macd_line, signal_line, _ = macd(
        close,
        5,
        13,
        4
    )

    macd_value = float(
        macd_line.iloc[-1]
    )

    signal_value = float(
        signal_line.iloc[-1]
    )

    atr_value = float(
        atr(
            df["high"],
            df["low"],
            close,
            14
        ).iloc[-1]
    )

    volume = float(
        df["tickVolume"].iloc[-1]
    )

    average_volume = float(
        df["tickVolume"]
        .tail(20)
        .mean()
    )

    volume_ratio = (
        volume / average_volume
        if average_volume > 0
        else 0
    )

    bullish = 0
    bearish = 0

    warnings = []

    # Trend

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

    # RSI

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

    # MACD

    if macd_value > signal_value:

        bullish += 20

    elif macd_value < signal_value:

        bearish += 20

    else:

        warnings.append(
            "MACD ضعيف"
        )

    # Volume

    if volume_ratio >= 1.20:

        if bullish > bearish:
            bullish += 20

        elif bearish > bullish:
            bearish += 20

    else:

        warnings.append(
            "Volume ضعيف"
        )

    # Extension

    extended = (
        abs(current - ema20)
        > atr_value * 0.45
    )

    if extended:

        warnings.append(
            "السعر ممتد"
        )

    if bullish > bearish:

        direction = "🟢 BUY"
        score = bullish

    elif bearish > bullish:

        direction = "🔴 SELL"
        score = bearish

    else:

        direction = "🟡 WAIT"
        score = 0

    if not warnings:

        indicator_state = "🟢 مؤكدة"

    elif len(warnings) <= 1:

        indicator_state = "🟢 جيدة"

    elif len(warnings) <= 2:

        indicator_state = "🟡 تحتاج تأكيد"

    else:

        indicator_state = "🟠 مختلطة"

    return {
        "price": current,
        "ema20": ema20,
        "atr": atr_value,
        "volume_ratio": volume_ratio,
        "trend": trend,
        "direction": direction,
        "score": int(
            min(score, 100)
        ),
        "warnings": warnings,
        "indicator_state": indicator_state,
        "extended": extended
    }


# =========================================================
# FORMATTING
# =========================================================

def warning_text(warnings):
    if not warnings:
        return "لا توجد"

    clean = []

    for item in warnings:

        if item == "EMA200":
            continue

        if item not in clean:
            clean.append(item)

    if not clean:
        return "🟢 مؤكدة"

    return " | ".join(
        clean
    )


def direction_plain(direction):
    return direction


def analysis_block(
    name,
    result
):
    return (
        f"📊 {name}\n"
        f"Trend: {result['trend']}\n"
        f"Direction: {direction_plain(result['direction'])}\n"
        f"Score: {result['score']}%\n"
        f"مؤشرات: {result['indicator_state']}\n"
        f"Warnings: {warning_text(result['warnings'])}"
    )


# =========================================================
# FINAL SUMMARY
# =========================================================

def calculate_final(results):
    scores = []

    buy = 0
    sell = 0

    for result, weight in results:

        if "BUY" in result["direction"]:

            scores.append(
                result["score"] * weight
            )

            buy += 1

        elif "SELL" in result["direction"]:

            scores.append(
                -result["score"] * weight
            )

            sell += 1

        else:

            scores.append(0)

    final_score = sum(
        scores
    )

    confidence = int(
        min(
            abs(final_score),
            100
        )
    )

    if final_score >= 60:

        signal = "🟢 BUY"

    elif final_score <= -60:

        signal = "🔴 SELL"

    else:

        signal = "🟡 WATCH"

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


def summary_text(
    signal,
    confidence,
    buy,
    sell,
    bias
):
    return (
        "━━━━━━━━━━━━━━━━━━\n"
        f"🎯 FINAL SIGNAL: {signal}\n"
        f"💪 CONFIDENCE: {confidence}%\n"
        f"📊 AGREEMENT: 🟢 BUY: {buy} | 🔴 SELL: {sell}\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🧠 الخلاصة:\n"
        f"الاتجاه العام {bias}.\n"
    )


# =========================================================
# START
# =========================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    message = (
        "🤖 XAU SMART TRADER v10\n"
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
        "🎯 التداول\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        "🎯 /trade\n"
        "أريد صفقة\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "💰 الأدوات\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        "💰 /price\n"
        "بيانات الذهب الحالية\n\n"

        "🟢 /status\n"
        "حالة البوت\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "🛠️ الدعم والمطور\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        "👨‍💻 /developer\n"
        "المطور\n\n"

        "🆘 /support\n"
        "الدعم\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "🎯 اختر ما تريد."
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
        "🤖 XAU SMART TRADER v10"
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
        "👉 @Morhafsy"
    )


# =========================================================
# STATUS
# =========================================================

async def status(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(

        "🟢 XAU SMART BOT v10 يعمل\n\n"

        "📈 السوق: XAUUSD\n"
        "🤖 النظام: Multi-Timeframe Analysis\n\n"

        "📅 Weekly: ON\n"
        "📊 Daily: ON\n"
        "⚡ Scalp: ON\n\n"

        "🎯 Trade Engine: ON\n"
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

        blocks = []

        for interval, name in intervals:

            df = get_bars(
                interval,
                5
            )

            last = df.iloc[-1]

            blocks.append(
                f"✅ {name}\n"
                f"Open: {last['open']:.2f}\n"
                f"High: {last['high']:.2f}\n"
                f"Low: {last['low']:.2f}\n"
                f"Close: {last['close']:.2f}\n"
                f"Volume: {last['tickVolume']:.0f}"
            )

        # W1

        try:

            d1 = get_bars(
                "1d",
                1000
            )

            w1 = build_weekly_from_daily(
                d1
            )

            last = w1.iloc[-1]

            blocks.append(
                "📅 W1\n"
                f"Open: {last['open']:.2f}\n"
                f"High: {last['high']:.2f}\n"
                f"Low: {last['low']:.2f}\n"
                f"Close: {last['close']:.2f}\n"
                f"Volume: {last['tickVolume']:.0f}"
            )

        except Exception:

            blocks.append(
                "⚠️ W1\n"
                "تعذر بناء البيانات الأسبوعية"
            )

        d1 = get_bars(
            "1d",
            10
        )

        live_price = float(
            d1["close"].iloc[-1]
        )

        message = (
            "🥇 XAUUSD DATA TEST v10\n\n"
            f"💰 LIVE PRICE: {live_price:.2f}\n\n"
            + "\n\n".join(blocks)
            + "\n\n"
            "🟢 مصدر البيانات يعمل."
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ خطأ في بيانات الذهب:\n"
            f"{str(e)}"
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
            1000
        )

        w1 = build_weekly_from_daily(
            d1
        )

        d1_result = analyze_standard(
            d1
        )

        h4 = get_bars(
            "4h",
            300
        )

        h4_result = analyze_standard(
            h4
        )

        w1_result = analyze_standard(
            w1
        )

        results = [

            (
                w1_result,
                0.40
            ),

            (
                d1_result,
                0.35
            ),

            (
                h4_result,
                0.25
            )

        ]

        final = calculate_final(
            results
        )

        signal, confidence, buy, sell, bias = final

        message = (
            "🤖 XAU SMART TRADER v10\n"
            "📅 WEEKLY ANALYSIS\n\n"

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

            + summary_text(
                signal,
                confidence,
                buy,
                sell,
                bias
            )

            + "\n"
            "🧠 الخلاصة:\n"
            "الاتجاه الأسبوعي هو "
            f"{bias}.\n\n"

            "🎯 القرار:\n"
            "⏳ انتظار تأكيد مناسب للدخول.\n\n"

            "🔒 تفاصيل المؤشرات مخفية.\n"
            "⚠️ Confidence = توافق الإشارات "
            "وليس احتمال الربح."
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ خطأ في التحليل الأسبوعي:\n"
            f"{str(e)}"
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

            (
                d1_result,
                0.40
            ),

            (
                h4_result,
                0.35
            ),

            (
                h1_result,
                0.25
            )

        ]

        signal, confidence, buy, sell, bias = calculate_final(
            results
        )

        message = (
            "🤖 XAU SMART TRADER v10\n"
            "📊 DAILY ANALYSIS\n\n"

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

            + summary_text(
                signal,
                confidence,
                buy,
                sell,
                bias
            )

            + "\n"
            "🧠 الخلاصة:\n"
            f"الاتجاه العام {bias}، "
            "ويجب الفصل بين الاتجاه والدخول.\n\n"

            "🎯 القرار:\n"
            "⏳ انتظار تأكيد مناسب قبل الدخول.\n\n"

            "🔒 تفاصيل المؤشرات مخفية.\n"
            "⚠️ Confidence = توافق الإشارات "
            "وليس احتمال الربح."
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ خطأ في التحليل اليومي:\n"
            f"{str(e)}"
        )


# =========================================================
# SCALP
# =========================================================

async def scalp(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        h1 = get_bars(
            "1h",
            300
        )

        m15 = get_bars(
            "15m",
            300
        )

        m5 = get_bars(
            "5m",
            300
        )

        h1_result = analyze_scalp(
            h1
        )

        m15_result = analyze_scalp(
            m15
        )

        m5_result = analyze_scalp(
            m5
        )

        results = [
            h1_result,
            m15_result,
            m5_result
        ]

        buy = sum(
            "BUY" in x["direction"]
            for x in results
        )

        sell = sum(
            "SELL" in x["direction"]
            for x in results
        )

        avg_score = int(
            np.mean(
                [
                    x["score"]
                    for x in results
                ]
            )
        )

        if buy > sell:

            final_signal = "🟢 BUY"

        elif sell > buy:

            final_signal = "🔴 SELL"

        else:

            final_signal = "🟡 WAIT"

        # Entry filter

        entry_allowed = (

            buy == 3
            or sell == 3
        )

        for result in results:

            if result["extended"]:
                entry_allowed = False

            if result["volume_ratio"] < 1.0:
                entry_allowed = False

            if (
                result["score"] < 65
            ):
                entry_allowed = False

        if entry_allowed:

            entry_status = (
                "🎯 Entry Filter: READY"
            )

            decision = (
                "🎯 الدخول ممكن بعد "
                "تأكيد الشمعة."
            )

        else:

            entry_status = (
                "⏳ Entry Filter: WAIT"
            )

            decision = (
                "⏳ انتظار تصحيح أو "
                "تأكيد أفضل."
            )

        message = (
            "🤖 XAU SMART TRADER v10\n"
            "⚡ SCALP ANALYSIS\n\n"

            + analysis_block(
                "H1",
                h1_result
            )

            + "\n\n"

            + analysis_block(
                "M15",
                m15_result
            )

            + "\n\n"

            + analysis_block(
                "M5",
                m5_result
            )

            + "\n\n"

            "━━━━━━━━━━━━━━━━━━\n"

            f"🎯 FINAL SCALP: {final_signal}\n"
            f"💪 CONFIDENCE: {avg_score}%\n"
            f"📊 AGREEMENT: 🟢 BUY {buy}/3 | 🔴 SELL {sell}/3\n\n"

            f"{entry_status}\n"
            f"🎯 القرار:\n{decision}\n"

            "━━━━━━━━━━━━━━━━━━\n"

            "🔒 تفاصيل المؤشرات مخفية.\n"
            "⚠️ التحليل اللحظي لا يضمن الربح."
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ خطأ في التحليل اللحظي:\n"
            f"{str(e)}"
        )


# =========================================================
# TRADE ENGINE
# =========================================================

def determine_trade_type(
    h1,
    m15,
    m5
):

    # سريع

    if (
        h1["direction"] == m15["direction"]
        == m5["direction"]
    ):

        return (
            "⚡ صفقة سريعة",
            "دقائق إلى أقل من ساعة"
        )

    # متوسطة

    if (
        h1["direction"]
        == m15["direction"]
    ):

        return (
            "📊 صفقة متوسطة",
            "عدة ساعات"
        )

    # طويلة

    return (
        "🏹 صفقة طويلة",
        "من يوم إلى عدة أيام"
    )


def build_trade(
    direction,
    df
):

    current = float(
        df["close"].iloc[-1]
    )

    atr_value = float(
        atr(
            df["high"],
            df["low"],
            df["close"],
            14
        ).iloc[-1]
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

        # =================================================
        # DIRECTION
        # =================================================

        if buy > sell:

            direction = "🟢 BUY"

        elif sell > buy:

            direction = "🔴 SELL"

        else:

            direction = "🟡 WAIT"

        # =================================================
        # TRADE TYPE
        # =================================================

        trade_type, duration = determine_trade_type(
            h1,
            m15,
            m5
        )

        # =================================================
        # ENTRY CONDITIONS
        # =================================================

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

        # =================================================
        # TRADE AVAILABLE
        # =================================================

        if entry_ready:

            source_df = m5_df

            (
                entry,
                sl,
                tp1,
                tp2
            ) = build_trade(
                direction,
                source_df
            )

            message = (
                "🤖 XAU SMART TRADER v10\n\n"
                "🎯 صفقة متاحة الآن\n\n"

                f"📈 الاتجاه: {direction}\n"
                f"⚡ النوع: {trade_type}\n"
                f"⏱️ المدة المتوقعة: {duration}\n\n"

                f"📍 Entry: {entry:.2f}\n"
                f"🛑 SL: {sl:.2f}\n"
                f"🎯 TP1: {tp1:.2f}\n"
                f"🎯 TP2: {tp2:.2f}\n\n"

                f"💪 قوة الصفقة: {confidence}%\n"
                "🎯 الحالة: شروط الدخول مكتملة\n\n"

                "⚠️ الإشارة صالحة فقط ما دامت "
                "شروط السوق قائمة.\n"
                "🚫 لا تطارد السعر إذا تحرك بعيدًا."
            )

        # =================================================
        # NO TRADE
        # =================================================

        else:

            reasons = []

            if not all_same_direction:
                reasons.append(
                    "اتجاه الفريمات غير متوافق بالكامل"
                )

            if not no_extension:
                reasons.append(
                    "السعر ممتد عن منطقة الدخول"
                )

            if not volume_ok:
                reasons.append(
                    "حجم التداول يحتاج تأكيد"
                )

            if not score_ok:
                reasons.append(
                    "قوة الإشارة غير كافية"
                )

            if not reasons:
                reasons.append(
                    "الشروط لم تكتمل"
                )

            reason_text = "\n".join(
                f"• {x}"
                for x in reasons
            )

            message = (
                "🤖 XAU SMART TRADER v10\n\n"

                "⏳ لا توجد صفقة الآن\n\n"

                f"📊 الاتجاه الحالي: {direction}\n"
                f"💪 Confidence: {confidence}%\n"
                f"⚡ النوع المتوقع: {trade_type}\n\n"

                "⚠️ السبب:\n"
                f"{reason_text}\n\n"

                "⏱️ إعادة الفحص المقترحة:\n"
                "10–20 دقيقة\n\n"

                "🎯 القرار:\n"
                "⏳ انتظار اكتمال الشروط.\n"
                "🚫 لا تطارد السعر.\n\n"

                "🔒 المؤشرات التفصيلية مخفية."
            )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ خطأ في محرك الصفقات:\n"
            f"{str(e)}"
        )


# =========================================================
# RUN BOT
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

    application.add_handler(
        CommandHandler(
            "trade",
            trade
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
# FLASK
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
