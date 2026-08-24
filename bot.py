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


# =========================================================
# TECHNICAL INDICATORS
# =========================================================

def calculate_ema(series, period):
    return series.ewm(
        span=period,
        adjust=False
    ).mean()


def calculate_rsi(series, period=14):
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

    rs = avg_gain / avg_loss.replace(0, np.nan)

    rsi = 100 - (100 / (1 + rs))

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

    histogram = macd - signal_line

    return macd, signal_line, histogram


def calculate_atr(
    high,
    low,
    close,
    period=14
):
    previous_close = close.shift(1)

    tr1 = high - low
    tr2 = (high - previous_close).abs()
    tr3 = (low - previous_close).abs()

    true_range = pd.concat(
        [tr1, tr2, tr3],
        axis=1
    ).max(axis=1)

    atr = true_range.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    return atr


# =========================================================
# SIGNAL SCORING
# =========================================================

def calculate_signal_score(
    price,
    ema20,
    ema50,
    ema200,
    rsi,
    macd,
    signal,
    tick_volume,
    average_volume
):

    bullish = 0
    bearish = 0

    warnings = []

    # -----------------------------------------------------
    # EMA TREND - 30 POINTS
    # -----------------------------------------------------

    if price > ema20 > ema50 > ema200:
        bullish += 30

    elif price < ema20 < ema50 < ema200:
        bearish += 30

    else:

        if price > ema20:
            bullish += 10

        elif price < ema20:
            bearish += 10

    # -----------------------------------------------------
    # RSI - 20 POINTS
    # -----------------------------------------------------

    if 50 <= rsi < 70:
        bullish += 20

    elif 30 < rsi < 50:
        bearish += 20

    elif rsi >= 70:

        warnings.append(
            "RSI تشبع شرائي"
        )

    elif rsi <= 30:

        warnings.append(
            "RSI تشبع بيعي"
        )

    # -----------------------------------------------------
    # MACD - 30 POINTS
    # -----------------------------------------------------

    if macd > signal:
        bullish += 30

    elif macd < signal:
        bearish += 30

    # -----------------------------------------------------
    # VOLUME - 20 POINTS
    # -----------------------------------------------------

    volume_confirmed = False

    if average_volume > 0:

        volume_ratio = (
            tick_volume / average_volume
        )

        if volume_ratio >= 1.20:

            volume_confirmed = True

            if bullish > bearish:
                bullish += 20

            elif bearish > bullish:
                bearish += 20

    # -----------------------------------------------------
    # SIGNAL
    # -----------------------------------------------------

    if bullish > bearish:

        signal_type = "BUY"
        raw_score = bullish

    elif bearish > bullish:

        signal_type = "SELL"
        raw_score = bearish

    else:

        signal_type = "WAIT"
        raw_score = 0

    # -----------------------------------------------------
    # CONFIDENCE
    # -----------------------------------------------------

    confidence = min(
        raw_score,
        100
    )

    # -----------------------------------------------------
    # MACD CONFLICT
    # -----------------------------------------------------

    if signal_type == "BUY" and macd < signal:

        confidence -= 15

        warnings.append(
            "MACD لا يؤكد الصعود"
        )

    if signal_type == "SELL" and macd > signal:

        confidence -= 15

        warnings.append(
            "MACD لا يؤكد الهبوط"
        )

    # -----------------------------------------------------
    # VOLUME WARNING
    # -----------------------------------------------------

    if not volume_confirmed:

        warnings.append(
            "Volume لا يعطي تأكيدًا قويًا"
        )

    # -----------------------------------------------------
    # LIMIT CONFIDENCE
    # -----------------------------------------------------

    confidence = max(
        0,
        min(confidence, 100)
    )

    # -----------------------------------------------------
    # WEAK SIGNAL = WAIT
    # -----------------------------------------------------

    if confidence < 60:

        final_signal = "WAIT"

    else:

        final_signal = signal_type

    return (
        final_signal,
        round(confidence),
        warnings
    )


# =========================================================
# FLASK
# =========================================================

@app.route("/")
def home():

    return "XAU Smart Bot v2 is running!"


# =========================================================
# TELEGRAM /START
# =========================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(

        "🤖 XAU Smart Bot v2\n\n"

        "🟢 البوت يعمل بنجاح!\n\n"

        "الأوامر المتاحة:\n"

        "💰 /price - سعر الذهب الحالي\n"
        "📅 /weekly - التحليل الأسبوعي\n"
        "📊 /daily - التحليل اليومي\n"
        "⚡ /scalp - التحليل اللحظي\n"
        "🟢 /status - حالة البوت"

    )


# =========================================================
# STATUS
# =========================================================

async def status(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(

        "🟢 XAU Smart Bot v2 يعمل\n"

        "📈 السوق: XAUUSD\n"

        "🤖 النظام: Multi-Timeframe Analysis\n"

        "⏳ الحالة: اختبار"

    )


# =========================================================
# PRICE TEST
# =========================================================

async def price(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        intervals = [
            "5m",
            "15m",
            "1h",
            "4h",
            "1d"
        ]

        results = []

        for interval in intervals:

            url = (
                "https://biquote.io/api/XAUUSD/ohlc"
                f"?interval={interval}&limit=5"
            )

            response = requests.get(
                url,
                timeout=15
            )

            if response.status_code != 200:

                results.append(
                    f"❌ {interval}: "
                    f"HTTP {response.status_code}"
                )

                continue

            data = response.json()

            bars = data.get(
                "bars",
                []
            )

            if not bars:

                results.append(
                    f"❌ {interval}: "
                    "لا توجد شموع"
                )

                continue

            last = bars[0]

            results.append(

                f"✅ {interval}\n"

                f"Open: {last.get('open')}\n"

                f"High: {last.get('high')}\n"

                f"Low: {last.get('low')}\n"

                f"Close: {last.get('close')}\n"

                f"Tick Volume: "
                f"{last.get('tickVolume')}"

            )

        message = (

            "🥇 XAUUSD - اختبار الفريمات\n\n"

            + "\n\n".join(results)

            + "\n\n"
            "🎯 إذا ظهرت جميع الفريمات ✅ "
            "فمصدر البيانات يعمل."

        )

        await update.message.reply_text(
            message
        )

    except requests.exceptions.Timeout:

        await update.message.reply_text(

            "⏳ انتهت مهلة الاتصال "
            "بمصدر البيانات."

        )

    except Exception as e:

        await update.message.reply_text(

            "❌ حدث خطأ:\n\n"
            f"{str(e)}"

        )


# =========================================================
# WEEKLY
# =========================================================

async def weekly(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(

        "📅 التحليل الأسبوعي XAUUSD\n\n"

        "W1 + D1\n"

        "EMA 20/50/200\n"

        "RSI 14\n"

        "MACD 8/21/5\n"

        "ATR 14\n"

        "Volume\n\n"

        "⏳ سيتم تفعيل التحليل الأسبوعي "
        "بعد الانتهاء من محرك D1/H4/H1."

    )


# =========================================================
# DAILY ANALYSIS
# =========================================================

async def daily(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        intervals = [
            "1d",
            "4h",
            "1h"
        ]

        names = {

            "1d": "D1",
            "4h": "H4",
            "1h": "H1"

        }

        weights = {

            "1d": 0.40,
            "4h": 0.35,
            "1h": 0.25

        }

        results = []

        weighted_scores = []

        for interval in intervals:

            # -------------------------------------------------
            # DATA
            # -------------------------------------------------

            url = (

                "https://biquote.io/api/XAUUSD/ohlc"

                f"?interval={interval}&limit=250"

            )

            response = requests.get(

                url,
                timeout=20

            )

            if response.status_code != 200:

                results.append(

                    f"❌ {names[interval]}: "
                    f"HTTP {response.status_code}"

                )

                continue

            data = response.json()

            bars = data.get(
                "bars",
                []
            )

            if len(bars) < 50:

                results.append(

                    f"⚠️ {names[interval]}: "
                    "بيانات غير كافية"

                )

                continue

            # -------------------------------------------------
            # DATAFRAME
            # -------------------------------------------------

            df = pd.DataFrame(
                bars
            )

            df = df.sort_values(
                "openTime"
            )

            for column in [

                "open",
                "high",
                "low",
                "close",
                "tickVolume"

            ]:

                df[column] = pd.to_numeric(

                    df[column],
                    errors="coerce"

                )

            df = df.dropna()

            close = df["close"]

            # -------------------------------------------------
            # INDICATORS
            # -------------------------------------------------

            ema20 = calculate_ema(
                close,
                20
            ).iloc[-1]

            ema50 = calculate_ema(
                close,
                50
            ).iloc[-1]

            ema200 = calculate_ema(
                close,
                200
            ).iloc[-1]

            rsi = calculate_rsi(
                close,
                14
            ).iloc[-1]

            macd, signal, histogram = calculate_macd(

                close,
                8,
                21,
                5

            )

            macd_value = macd.iloc[-1]

            signal_value = signal.iloc[-1]

            atr = calculate_atr(

                df["high"],
                df["low"],
                close,
                14

            ).iloc[-1]

            current_price = close.iloc[-1]

            tick_volume = (
                df["tickVolume"].iloc[-1]
            )

            average_volume = (

                df["tickVolume"]
                .tail(20)
                .mean()

            )

            # -------------------------------------------------
            # SIGNAL SCORE
            # -------------------------------------------------

            signal_type, score, warnings = (

                calculate_signal_score(

                    current_price,

                    ema20,

                    ema50,

                    ema200,

                    rsi,

                    macd_value,

                    signal_value,

                    tick_volume,

                    average_volume

                )

            )

            # -------------------------------------------------
            # DIRECTIONAL SCORE
            # -------------------------------------------------

            if signal_type == "BUY":

                directional_score = score

            elif signal_type == "SELL":

                directional_score = -score

            else:

                directional_score = 0

            weighted_scores.append(

                directional_score
                * weights[interval]

            )

            # -------------------------------------------------
            # TREND
            # -------------------------------------------------

            if (
                current_price > ema20
                and ema20 > ema50
            ):

                trend = "🟢 صاعد"

            elif (
                current_price < ema20
                and ema20 < ema50
            ):

                trend = "🔴 هابط"

            else:

                trend = "🟡 متذبذب"

            # -------------------------------------------------
            # DISPLAY
            # -------------------------------------------------

            warning_text = (

                " | ".join(warnings)

                if warnings

                else "لا توجد تحذيرات"

            )

            results.append(

                f"📊 {names[interval]}\n"

                f"💰 Price: "
                f"{current_price:.2f}\n"

                f"📈 EMA20: "
                f"{ema20:.2f}\n"

                f"📈 EMA50: "
                f"{ema50:.2f}\n"

                f"📈 EMA200: "
                f"{ema200:.2f}\n"

                f"RSI: "
                f"{rsi:.2f}\n"

                f"MACD: "
                f"{macd_value:.4f}\n"

                f"Signal: "
                f"{signal_value:.4f}\n"

                f"ATR: "
                f"{atr:.2f}\n"

                f"Volume: "
                f"{tick_volume:.0f}\n"

                f"Trend: "
                f"{trend}\n"

                f"Signal: "
                f"{signal_type}\n"

                f"Score: "
                f"{score}%\n"

                f"⚠️ "
                f"{warning_text}"

            )

        # =====================================================
        # MULTI-TIMEFRAME RESULT
        # =====================================================

        final_score = sum(
            weighted_scores
        )

        if final_score >= 60:

            final_signal = "🟢 BUY"

        elif final_score <= -60:

            final_signal = "🔴 SELL"

        else:

            final_signal = "🟡 WAIT"

        confidence = min(
            abs(final_score),
            100
        )

        # =====================================================
        # FINAL MESSAGE
        # =====================================================

        message = (

            "🤖 XAU SMART BOT\n\n"

            "📊 DAILY ANALYSIS\n\n"

            + "\n\n".join(results)

            + "\n\n"

            "━━━━━━━━━━━━━━\n"

            f"🎯 FINAL SIGNAL: "
            f"{final_signal}\n"

            f"💪 CONFIDENCE: "
            f"{confidence:.0f}%\n"

            "━━━━━━━━━━━━━━\n\n"

            "⚠️ Confidence = درجة توافق "
            "المؤشرات وليست احتمال ربح."

        )

        await update.message.reply_text(
            message
        )

    except requests.exceptions.Timeout:

        await update.message.reply_text(

            "⏳ انتهت مهلة الاتصال "
            "ببيانات الذهب."

        )

    except Exception as e:

        await update.message.reply_text(

            "❌ حدث خطأ في التحليل:\n\n"

            f"{str(e)}"

        )


# =========================================================
# SCALP
# =========================================================

async def scalp(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(

        "⚡ التحليل اللحظي XAUUSD\n\n"

        "H1 + M15 + M5\n"

        "EMA 9/20/50\n"

        "RSI 9\n"

        "MACD 5/13/4\n"

        "ATR 14\n"

        "Volume\n\n"

        "⏳ سيتم تفعيل محرك Scalp "
        "بعد تثبيت محرك Daily."

    )


# =========================================================
# RUN TELEGRAM BOT
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
