import os
import asyncio
import threading
import math

from flask import Flask
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes


TOKEN = os.environ.get("TELEGRAM_TOKEN")

app = Flask(__name__)


# =========================
# Web Service
# =========================

@app.route("/")
def home():
    return "XAU Smart Bot v2 is running!"


# =========================
# Bot Commands
# =========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 XAU Smart Bot v2\n\n"
        "🟢 البوت يعمل بنجاح!\n\n"
        "الأوامر المتاحة:\n"
        "📅 /weekly - التحليل الأسبوعي\n"
        "📊 /daily - التحليل اليومي\n"
        "⚡ /scalp - التحليل اللحظي\n"
        "🟢 /status - حالة البوت"
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🟢 XAU Smart Bot v2 يعمل\n\n"
        "📈 السوق: XAUUSD\n"
        "🤖 النظام: Multi-Timeframe Analysis\n"
        "📊 المؤشرات: EMA + RSI + MACD + ATR + Volume\n"
        "⏳ الحالة: اختبار"
    )


# =========================
# Analysis Menu
# =========================

async def weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📅 التحليل الأسبوعي XAUUSD\n\n"
        "الفريمات المطلوبة:\n"
        "🟣 W1\n"
        "🔵 D1\n\n"
        "📊 سيتم تحليل:\n"
        "• الاتجاه العام\n"
        "• الدعم والمقاومة\n"
        "• EMA 20/50/200\n"
        "• RSI 14\n"
        "• MACD\n"
        "• ATR\n"
        "• Volume\n\n"
        "⏳ محرك التحليل جاهز لاستقبال البيانات."
    )


async def daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📊 التحليل اليومي XAUUSD\n\n"
        "الفريمات المطلوبة:\n"
        "🔵 D1\n"
        "🟢 H4\n"
        "🟡 H1\n\n"
        "📊 سيتم تحليل:\n"
        "• اتجاه اليوم\n"
        "• الدعم والمقاومة\n"
        "• EMA 20/50/200\n"
        "• RSI 14\n"
        "• MACD 8/21/5\n"
        "• ATR 14\n"
        "• Volume\n\n"
        "⏳ محرك التحليل جاهز لاستقبال البيانات."
    )


async def scalp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⚡ التحليل اللحظي XAUUSD\n\n"
        "الفريمات المطلوبة:\n"
        "🟢 H1\n"
        "🟡 M15\n"
        "🔴 M5\n\n"
        "📊 سيتم تحليل:\n"
        "• الاتجاه اللحظي\n"
        "• الزخم\n"
        "• Breakout / Retest\n"
        "• EMA 9/20/50\n"
        "• RSI 9\n"
        "• MACD 5/13/4\n"
        "• ATR 14\n"
        "• Volume\n\n"
        "⏳ محرك التحليل جاهز لاستقبال البيانات."
    )


# =========================
# Main Bot
# =========================

async def run_bot():

    if not TOKEN:
        raise RuntimeError(
            "TELEGRAM_TOKEN is missing. "
            "Add it in Render Environment Variables."
        )

    application = (
        Application.builder()
        .token(TOKEN)
        .build()
    )

    application.add_handler(
        CommandHandler("start", start)
    )

    application.add_handler(
        CommandHandler("status", status)
    )

    application.add_handler(
        CommandHandler("weekly", weekly)
    )

    application.add_handler(
        CommandHandler("daily", daily)
    )

    application.add_handler(
        CommandHandler("scalp", scalp)
    )

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


# =========================
# Flask Server
# =========================

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


# =========================
# Start Everything
# =========================

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
