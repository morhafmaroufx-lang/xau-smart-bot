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

DATA_URL = "https://biquote.io/api/XAUUSD/ohlc"


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

    tr2 = (
        high - previous_close
    ).abs()

    tr3 = (
        low - previous_close
    ).abs()

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
# SIGNAL ENGINE
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

    # =====================================================
    # EMA TREND - 40 POINTS
    # =====================================================

    if price > ema20 > ema50 > ema200:

        bullish += 40

    elif price < ema20 < ema50 < ema200:

        bearish += 40

    elif price > ema20 > ema50:

        bullish += 30

    elif price < ema20 < ema50:

        bearish += 30

    elif price > ema20:

        bullish += 20

    elif price < ema20:

        bearish += 20

    # =====================================================
    # RSI - 15 POINTS
    # =====================================================

    if 50 <= rsi < 70:

        bullish += 15

    elif 30 < rsi < 50:

        bearish += 15

    elif rsi >= 70:

        if bullish > bearish:

            bullish += 5

            warnings.append(
                "⚠️ RSI مرتفع - احتمال تصحيح"
            )

        else:

            warnings.append(
                "⚠️ RSI تشبع شرائي"
            )

    elif rsi <= 30:

        if bearish > bullish:

            bearish += 5

            warnings.append(
                "⚠️ RSI منخفض - احتمال ارتداد"
            )

        else:

            warnings.append(
                "⚠️ RSI تشبع بيعي"
            )

    # =====================================================
    # MACD - 25 POINTS
    # =====================================================

    if macd > signal:

        bullish += 25

    elif macd < signal:

        bearish += 25

    # =====================================================
    # MACD WARNING
    # =====================================================

    if bullish > bearish and macd < signal:

        warnings.append(
            "⚠️ MACD لا يؤكد الصعود"
        )

    elif bearish > bullish and macd > signal:

        warnings.append(
            "⚠️ MACD لا يؤكد الهبوط"
        )

    # =====================================================
    # VOLUME - 20 POINTS
    # =====================================================

    volume_confirmed = False
    volume_ratio = 0

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

    if not volume_confirmed:

        warnings.append(
            "ℹ️ Volume لا يؤكد الحركة بقوة"
        )

    # =====================================================
    # DIRECTION
    # =====================================================

    if bullish > bearish:

        direction = "BUY"

        confidence = bullish

    elif bearish > bullish:

        direction = "SELL"

        confidence = bearish

    else:

        direction = "WAIT"

        confidence = 0

    # =====================================================
    # CONFIDENCE
    # =====================================================

    confidence = max(
        0,
        min(
            confidence,
            100
        )
    )

    # =====================================================
    # ACTION
    # =====================================================

    if confidence >= 65:

        action = direction

    elif confidence >= 50:

        action = "WATCH"

    else:

        action = "WAIT"

    # =====================================================
    # RETURN
    #
    # direction:
    # BUY / SELL / WAIT
    #
    # confidence:
    # 0 - 100
    #
    # warnings:
    # list
    #
    # action:
    # BUY / SELL / WATCH / WAIT
    # =====================================================

    return (
        direction,
        round(confidence),
        warnings,
        action
    )


# =========================================================
# DATA LOADER
# =========================================================

def get_market_data(
    interval,
    limit=250
):
    url = (
        f"{DATA_URL}"
        f"?interval={interval}"
        f"&limit={limit}"
    )

    response = requests.get(
        url,
        timeout=20
    )

    response.raise_for_status()

    data = response.json()

    bars = data.get(
        "bars",
        []
    )

    if not bars:

        raise ValueError(
            f"لا توجد بيانات للفريم {interval}"
        )

    df = pd.DataFrame(
        bars
    )

    required_columns = [
        "openTime",
        "open",
        "high",
        "low",
        "close",
        "tickVolume"
    ]

    missing = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing:

        raise ValueError(
            "أعمدة ناقصة: "
            + ", ".join(missing)
        )

    # ترتيب من الأقدم إلى الأحدث
    df = df.sort_values(
        "openTime"
    ).reset_index(
        drop=True
    )

    # تحويل الأرقام
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

    df = df.dropna(
        subset=[
            "high",
            "low",
            "close",
            "tickVolume"
        ]
    )

    if len(df) < 50:

        raise ValueError(
            f"بيانات غير كافية للفريم {interval}"
        )

    return df


# =========================================================
# ANALYZE ONE TIMEFRAME
# =========================================================

def analyze_timeframe(
    interval,
    name
):
    df = get_market_data(
        interval,
        250
    )

    close = df["close"]

    # =====================================================
    # INDICATORS
    # =====================================================

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

    histogram_value = histogram.iloc[-1]

    atr = calculate_atr(
        df["high"],
        df["low"],
        close,
        14
    ).iloc[-1]

    current_price = close.iloc[-1]

    tick_volume = df[
        "tickVolume"
    ].iloc[-1]

    # =====================================================
    # VOLUME
    #
    # مهم:
    # نستثني الشمعة الحالية من المتوسط
    # =====================================================

    previous_volumes = df[
        "tickVolume"
    ].iloc[-21:-1]

    if len(previous_volumes) > 0:

        average_volume = (
            previous_volumes.mean()
        )

    else:

        average_volume = (
            df["tickVolume"]
            .tail(20)
            .mean()
        )

    # =====================================================
    # SIGNAL
    # =====================================================

    (
        direction,
        confidence,
        warnings,
        action
    ) = calculate_signal_score(

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

    # =====================================================
    # TREND
    # =====================================================

    if (
        current_price > ema20
        and ema20 > ema50
        and ema50 > ema200
    ):

        trend = "🟢 صاعد قوي"

    elif (
        current_price > ema20
        and ema20 > ema50
    ):

        trend = "🟢 صاعد"

    elif (
        current_price < ema20
        and ema20 < ema50
        and ema50 < ema200
    ):

        trend = "🔴 هابط قوي"

    elif (
        current_price < ema20
        and ema20 < ema50
    ):

        trend = "🔴 هابط"

    else:

        trend = "🟡 متذبذب"

    # =====================================================
    # DIRECTIONAL SCORE
    #
    # نستخدم confidence مع اتجاه BUY/SELL
    # حتى لا تصبح WAIT = صفر تلقائياً.
    # =====================================================

    if direction == "BUY":

        directional_score = confidence

    elif direction == "SELL":

        directional_score = -confidence

    else:

        directional_score = 0

    # =====================================================
    # VOLUME RATIO
    # =====================================================

    if average_volume > 0:

        volume_ratio = (
            tick_volume
            / average_volume
        )

    else:

        volume_ratio = 0

    return {
        "interval": interval,
        "name": name,
        "price": current_price,
        "ema20": ema20,
        "ema50": ema50,
        "ema200": ema200,
        "rsi": rsi,
        "macd": macd_value,
        "signal": signal_value,
        "histogram": histogram_value,
        "atr": atr,
        "volume": tick_volume,
        "average_volume": average_volume,
        "volume_ratio": volume_ratio,
        "trend": trend,
        "direction": direction,
        "action": action,
        "confidence": confidence,
        "directional_score": directional_score,
        "warnings": warnings
    }


# =========================================================
# FLASK
# =========================================================

@app.route("/")
def home():

    return (
        "XAU Smart Bot v3 is running!"
    )


# =========================================================
# START
# =========================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(

        "🤖 XAU Smart Bot v3\n\n"

        "🟢 البوت يعمل بنجاح!\n\n"

        "الأوامر:\n"

        "💰 /price - اختبار السعر\n"

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

        "🟢 XAU Smart Bot v3 يعمل\n"

        "📈 السوق: XAUUSD\n"

        "🤖 النظام: Multi-Timeframe Analysis\n"

        "📊 D1 + H4 + H1\n"

        "📈 EMA 20/50/200\n"

        "RSI 14\n"

        "MACD 8/21/5\n"

        "ATR 14\n"

        "Volume\n"

        "⏳ الحالة: اختبار"

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
            "5m",
            "15m",
            "1h",
            "4h",
            "1d"
        ]

        results = []

        for interval in intervals:

            try:

                df = get_market_data(
                    interval,
                    5
                )

                last = df.iloc[-1]

                results.append(

                    f"✅ {interval}\n"

                    f"Open: {last['open']}\n"

                    f"High: {last['high']}\n"

                    f"Low: {last['low']}\n"

                    f"Close: {last['close']}\n"

                    f"Tick Volume: "
                    f"{last['tickVolume']}"

                )

            except Exception as e:

                results.append(

                    f"❌ {interval}: "
                    f"{str(e)}"

                )

        message = (

            "🥇 XAUUSD - اختبار الفريمات\n\n"

            + "\n\n".join(results)

            + "\n\n"

            "🎯 إذا ظهرت جميع الفريمات "
            "بشكل صحيح فمصدر البيانات يعمل."

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

        "⏳ محرك التحليل الأسبوعي "
        "سيتم تفعيله بعد تثبيت Daily."

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

        successful_timeframes = 0

        # =================================================
        # ANALYZE EACH TIMEFRAME
        # =================================================

        for interval in intervals:

            try:

                result = analyze_timeframe(
                    interval,
                    names[interval]
                )

                successful_timeframes += 1

                weighted_scores.append(

                    result[
                        "directional_score"
                    ]
                    * weights[interval]

                )

                warning_text = (

                    " | ".join(
                        result["warnings"]
                    )

                    if result["warnings"]

                    else "لا توجد تحذيرات"

                )

                results.append(

                    f"📊 {result['name']}\n"

                    f"💰 Price: "
                    f"{result['price']:.2f}\n"

                    f"📈 EMA20: "
                    f"{result['ema20']:.2f}\n"

                    f"📈 EMA50: "
                    f"{result['ema50']:.2f}\n"

                    f"📈 EMA200: "
                    f"{result['ema200']:.2f}\n"

                    f"RSI: "
                    f"{result['rsi']:.2f}\n"

                    f"MACD: "
                    f"{result['macd']:.4f}\n"

                    f"Signal: "
                    f"{result['signal']:.4f}\n"

                    f"ATR: "
                    f"{result['atr']:.2f}\n"

                    f"Volume: "
                    f"{result['volume']:.0f}\n"

                    f"Volume Ratio: "
                    f"{result['volume_ratio']:.2f}x\n"

                    f"Trend: "
                    f"{result['trend']}\n"

                    f"Signal: "
                    f"{result['action']}\n"

                    f"Direction: "
                    f"{result['direction']}\n"

                    f"Score: "
                    f"{result['confidence']}%\n"

                    f"⚠️ "
                    f"{warning_text}"

                )

            except Exception as e:

                results.append(

                    f"❌ {names[interval]}: "
                    f"{str(e)}"

                )

        # =================================================
        # CHECK DATA
        # =================================================

        if successful_timeframes == 0:

            await update.message.reply_text(

                "❌ لم أتمكن من تحليل أي فريم.\n\n"
                "تحقق من مصدر بيانات الذهب."

            )

            return

        # =================================================
        # MULTI-TIMEFRAME SCORE
        # =================================================

        final_score = sum(
            weighted_scores
        )

        # =================================================
        # FINAL SIGNAL
        # =================================================

        if final_score >= 65:

            final_signal = "🟢 BUY"

        elif final_score <= -65:

            final_signal = "🔴 SELL"

        elif final_score >= 35:

            final_signal = "🟡 WATCH BUY"

        elif final_score <= -35:

            final_signal = "🟡 WATCH SELL"

        else:

            final_signal = "🟡 WAIT"

        # =================================================
        # CONFIDENCE
        # =================================================

        confidence = min(
            abs(final_score),
            100
        )

        # =================================================
        # AGREEMENT
        # =================================================

        buy_count = 0
        sell_count = 0

        for interval in intervals:

            try:

                temp = analyze_timeframe(
                    interval,
                    names[interval]
                )

                if temp["direction"] == "BUY":

                    buy_count += 1

                elif temp["direction"] == "SELL":

                    sell_count += 1

            except Exception:

                pass

        if buy_count > sell_count:

            agreement = (
                f"🟢 BUY: "
                f"{buy_count}/{successful_timeframes}"
            )

        elif sell_count > buy_count:

            agreement = (
                f"🔴 SELL: "
                f"{sell_count}/{successful_timeframes}"
            )

        else:

            agreement = (
                "🟡 لا يوجد توافق واضح"
            )

        # =================================================
        # FINAL MESSAGE
        # =================================================

        message = (

            "🤖 XAU SMART BOT v3\n\n"

            "📊 DAILY ANALYSIS\n\n"

            + "\n\n".join(results)

            + "\n\n"

            "━━━━━━━━━━━━━━\n"

            f"🎯 FINAL SIGNAL: "
            f"{final_signal}\n"

            f"💪 CONFIDENCE: "
            f"{confidence:.0f}%\n"

            f"📊 AGREEMENT: "
            f"{agreement}\n"

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

        "⏳ محرك Scalp لم يتم تفعيله بعد."

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
