import os
import asyncio
import threading
from flask import Flask
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

TOKEN = os.environ.get("TELEGRAM_TOKEN")

app = Flask(__name__)


@app.route("/")
def home():
    return "XAU Smart Bot is running!"


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 XAU Smart Bot يعمل بنجاح!\n\n"
        "📊 قريبًا سأقوم بتحليل XAUUSD وإرسال إشارات التداول."
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🟢 البوت يعمل\n"
        "📈 النظام: XAU Smart Bot\n"
        "⏳ الحالة: اختبار"
    )


async def run_bot():
    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("status", status))

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
    app.run(host="0.0.0.0", port=port)


def main():
    server = threading.Thread(target=run_server)
    server.daemon = True
    server.start()

    asyncio.run(run_bot())


if __name__ == "__main__":
    main()
