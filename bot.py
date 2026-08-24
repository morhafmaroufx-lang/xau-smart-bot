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
    await update.message.reply_text(
        "📊 التحليل اليومي XAUUSD\n\n"
        "D1 + H4 + H1\n"
        "EMA 20/50/200\n"
        "RSI 14\n"
        "MACD 8/21/5\n"
        "ATR 14\n"
        "Volume\n\n"
        "⏳ سيتم ربط البيانات الحقيقية في الخطوة التالية."
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
