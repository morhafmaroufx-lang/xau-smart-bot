import os
import asyncio
import threading
import requests
import pandas as pd
import numpy as np
from flask import Flask
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

TOKEN = os.environ.get("TELEGRAM_TOKEN")

app = Flask(__name__)
# =========================
# Technical Indicators
# =========================

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
# =========================
# Signal Scoring
# =========================
# =========================
# Improved Signal Scoring
# =========================

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

    # =========================
    # EMA TREND - 30 points
    # =========================

    if price > ema20 > ema50 > ema200:
        bullish += 30

    elif price < ema20 < ema50 < ema200:
        bearish += 30

    else:
        # Partial trend
        if price > ema20:
            bullish += 10

        elif price < ema20:
            bearish += 10

    # =========================
    # RSI - 20 points
    # =========================

    if 50 <= rsi < 70:
        bullish += 20

    elif 30 < rsi < 50:
        bearish += 20

    elif rsi >= 70:
        warnings.append("⚠️ RSI تشبع شرائي")

    elif rsi <= 30:
        warnings.append("⚠️ RSI تشبع بيعي")

    # =========================
    # MACD - 30 points
    # =========================

    if macd > signal:
        bullish += 30

    elif macd < signal:
        bearish += 30

    # =========================
    # VOLUME - 20 points
    # =========================

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

    # =========================
    # Determine Signal
    # =========================

    if bullish > bearish:

        signal_type = "BUY"

        raw_score = bullish

    elif bearish > bullish:

        signal_type = "SELL"

        raw_score = bearish

    else:

        signal_type = "WAIT"

        raw_score = 0

    # =========================
    # Confidence
    # =========================

    confidence = min(
        raw_score,
        100
    )

    # =========================
    # Momentum Conflict
    # =========================

    if signal_type == "BUY" and macd < signal:

        confidence -= 15

        warnings.append(
            "⚠️ MACD لا يؤكد الصعود"
        )

    if signal_type == "SELL" and macd > signal:

        confidence -= 15

        warnings.append(
            "⚠️ MACD لا يؤكد الهبوط"
        )

    # =========================
    # Volume Warning
    # =========================

    if not volume_confirmed:

        warnings.append(
            "⚠️ Volume لا يعطي تأكيدًا قويًا"
        )

    # =========================
    # Final Confidence
    # =========================

    confidence = max(
        0,
        min(confidence, 100)
    )

    # إذا كانت الثقة ضعيفة
    if confidence < 60:

        final_signal = "WAIT"

    else:

        final_signal = signal_type

    return (
        final_signal,
        round(confidence),
        warnings
    )

@app.route("/")
def home():
    return "XAU Smart Bot v2 is running!"


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
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


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🟢 XAU Smart Bot v2 يعمل\n"
        "📈 السوق: XAUUSD\n"
        "🤖 النظام: Multi-Timeframe Analysis\n"
        "⏳ الحالة: اختبار"
    )

async def price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        intervals = ["5m", "15m", "1h", "4h", "1d"]

        results = []

        for interval in intervals:
            url = (
                f"https://biquote.io/api/XAUUSD/ohlc"
                f"?interval={interval}&limit=5"
            )

            response = requests.get(
                url,
                timeout=15
            )

            if response.status_code != 200:
                results.append(
                    f"❌ {interval}: HTTP {response.status_code}"
                )
                continue

            data = response.json()

            bars = data.get("bars", [])

            if not bars:
                results.append(
                    f"❌ {interval}: لا توجد شموع"
                )
                continue

            last = bars[0]

            results.append(
                f"✅ {interval}\n"
                f"Open: {last.get('open')}\n"
                f"High: {last.get('high')}\n"
                f"Low: {last.get('low')}\n"
                f"Close: {last.get('close')}\n"
                f"Tick Volume: {last.get('tickVolume')}"
            )

        message = (
            "🥇 XAUUSD - اختبار الفريمات\n\n"
            + "\n\n".join(results)
            + "\n\n"
            "🎯 إذا ظهرت جميع الفريمات ✅ "
            "نبدأ ببناء محرك التحليل."
        )

        await update.message.reply_text(message)

    except requests.exceptions.Timeout:
        await update.message.reply_text(
            "⏳ انتهت مهلة الاتصال بمصدر البيانات."
        )

    except Exception as e:
        await update.message.reply_text(
            "❌ حدث خطأ:\n\n"
            f"{str(e)}"
        )

async def weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📅 التحليل الأسبوعي XAUUSD\n\n"
        "W1 + D1\n"
        "EMA 20/50/200\n"
        "RSI 14\n"
        "MACD\n"
        "ATR 14\n"
        "Volume\n\n"
        "⏳ سيتم ربط البيانات الحقيقية في الخطوة التالية."
    )

async def daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        intervals = ["1d", "4h", "1h"]

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

            url = (
                f"https://biquote.io/api/XAUUSD/ohlc"
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
            bars = data.get("bars", [])

            if len(bars) < 50:
                results.append(
                    f"⚠️ {names[interval]}: "
                    f"بيانات غير كافية"
                )
                continue

            df = pd.DataFrame(bars)

            # ترتيب الشموع من الأقدم للأحدث
            df = df.sort_values("openTime")

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

            # =========================
            # Indicators
            # =========================

            ema20 = calculate_ema(
                close, 20
            ).iloc[-1]

            ema50 = calculate_ema(
                close, 50
            ).iloc[-1]

            ema200 = calculate_ema(
                close, 200
            ).iloc[-1]

            rsi = calculate_rsi(
                close, 14
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

            tick_volume = df["tickVolume"].iloc[-1]

            average_volume = (
                df["tickVolume"]
                .tail(20)
                .mean()
            )

            # =========================
            # Signal Score
            # =========================
signal_type, score, warnings = calculate_signal_score(
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

            # تحويل BUY إلى موجب
            # و SELL إلى سالب
            if signal_type == "BUY":
                signal_type, score, warnings =

            elif signal_type == "SELL":
                directional_score = -score

            else:
                directional_score = 0

            weighted_scores.append(
                directional_score * weights[interval]
            )

            # =========================
            # Trend
            # =========================

            if current_price > ema20 and ema20 > ema50:
                trend = "🟢 صاعد"

            elif current_price < ema20 and ema20 < ema50:
                trend = "🔴 هابط"

            else:
                trend = "🟡 متذبذب"

            results.append(
                f"📊 {names[interval]}\n"
                f"💰 Price: {current_price:.2f}\n"
                f"📈 EMA20: {ema20:.2f}\n"
                f"📈 EMA50: {ema50:.2f}\n"
                f"📈 EMA200: {ema200:.2f}\n"
                f"RSI: {rsi:.2f}\n"
                f"MACD: {macd_value:.4f}\n"
                f"Signal: {signal_value:.4f}\n"
                f"ATR: {atr:.2f}\n"
                f"Volume: {tick_volume:.0f}\n"
                f"Trend: {trend}\n"
                f"Signal: {signal_type}\n"
                f"Score: {score}%"
            )f"\n⚠️ {' | '.join(warnings) if warnings else 'لا توجد تحذيرات'}"

        # =========================
        # Multi-Timeframe Result
        # =========================

        final_score = sum(weighted_scores)

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

        message = (
            "🤖 XAU SMART BOT\n\n"
            "📊 DAILY ANALYSIS\n\n"
            + "\n\n".join(results)
            + "\n\n"
            "━━━━━━━━━━━━━━\n"
            f"🎯 FINAL SIGNAL: {final_signal}\n"
            f"💪 CONFIDENCE: {confidence:.0f}%\n"
            "━━━━━━━━━━━━━━\n\n"
            "⚠️ Confidence = درجة توافق المؤشرات "
            "وليست احتمال ربح."
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
            "❌ حدث خطأ في التحليل:\n\n"
            f"{str(e)}"
        )


async def scalp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⚡ التحليل اللحظي XAUUSD\n\n"
        "H1 + M15 + M5\n"
        "EMA 9/20/50\n"
        "RSI 9\n"
        "MACD 5/13/4\n"
        "ATR 14\n"
        "Volume\n\n"
        "⏳ سيتم ربط البيانات الحقيقية في الخطوة التالية."
    )


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

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("price", price))
    application.add_handler(CommandHandler("weekly", weekly))
    application.add_handler(CommandHandler("daily", daily))
    application.add_handler(CommandHandler("scalp", scalp))

    await application.initialize()
    await application.start()
    await application.updater.start_polling()

    try:
        while True:
            await asyncio.sleep(3600)

    finally:
        await application.updater.stop()
        await application.stop()
        await application.shutdown()


def run_server():

    port = int(os.environ.get("PORT", 10000))

    app.run(
        host="0.0.0.0",
        port=port
    )


def main():

    server = threading.Thread(
        target=run_server
    )

    server.daemon = True
    server.start()

    asyncio.run(run_bot())


if __name__ == "__main__":
    main()
