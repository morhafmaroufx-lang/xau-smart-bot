import os
import asyncio
import threading
import requests

from flask import Flask
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

TOKEN = os.environ.get("TELEGRAM_TOKEN")

app = Flask(__name__)


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
        url = "https://biquote.io/api/XAUUSD/ohlc?interval=5m&limit=5"

        response = requests.get(
            url,
            timeout=15
        )

        if response.status_code != 200:
            await update.message.reply_text(
                f"⚠️ مصدر البيانات لم يستجب.\n"
                f"HTTP: {response.status_code}"
            )
            return

        data = response.json()

        await update.message.reply_text(
            "🥇 XAUUSD - اختبار مصدر البيانات\n\n"
            f"📊 البيانات المستلمة:\n"
            f"{data}\n\n"
            "✅ إذا ظهرت بيانات OHLC، فالمصدر يعمل."
        )

    except requests.exceptions.Timeout:
        await update.message.reply_text(
            "⏳ انتهت مهلة الاتصال بمصدر البيانات."
        )

    except Exception as e:
        await update.message.reply_text(
            "❌ حدث خطأ أثناء جلب بيانات XAUUSD:\n\n"
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
