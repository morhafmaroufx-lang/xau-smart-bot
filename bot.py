import os
import asyncio
import threading
import requests
import pandas as pd
import numpy as np

from flask import Flask, request, jsonify
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes


# =========================================================
# XAU SMART TRADER v11
# Arabic + Stable Webhook + Multi Timeframe
# =========================================================

TOKEN = os.environ.get("TELEGRAM_TOKEN")

app = Flask(__name__)

SYMBOL = "XAUUSD"
DATA_URL = "https://biquote.io/api/XAUUSD/ohlc"

VERSION = "v11"

RENDER_URL = os.environ.get(
    "RENDER_EXTERNAL_URL",
    "https://xau-smart-bot.onrender.com"
)

WEBHOOK_SECRET = os.environ.get(
    "WEBHOOK_SECRET",
    "xau-smart-trader-v11"
)

BOT_LOOP = None
BOT_APPLICATION = None


# =========================================================
# DATA
# =========================================================

def get_bars(interval, limit=300):

    url = f"{DATA_URL}?interval={interval}&limit={limit}"

    last_error = None

    for attempt in range(3):

        try:

            response = requests.get(
                url,
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

                df = df.drop_duplicates(
                    subset=["openTime"],
                    keep="last"
                )

            df = df.reset_index(
                drop=True
            )

            if len(df) < 10:

                raise ValueError(
                    f"بيانات {interval} غير كافية"
                )

            return df

        except Exception as e:

            last_error = e

            if attempt < 2:
                import time
                time.sleep(1)

    raise RuntimeError(
        f"تعذر الحصول على بيانات {interval}: {last_error}"
    )


# =========================================================
# CLOSED CANDLES
# =========================================================

def closed_bars(df):

    """
    نعتمد على الشموع المغلقة في التحليل.
    إذا كانت البيانات تحتوي على شمعة حالية قيد التكوين،
    يتم استبعاد آخر شمعة.
    """

    if len(df) < 5:
        return df.copy()

    result = df.copy()

    # إذا كان openTime متاحاً، نستخدم آخر شمعة كشمعة حالية
    # ونستبعدها من التحليل.
    if "openTime" in result.columns:

        return result.iloc[:-1].copy().reset_index(
            drop=True
        )

    return result.iloc[:-1].copy().reset_index(
        drop=True
    )


def enough_data(df, minimum):

    return len(df) >= minimum


# =========================================================
# WEEKLY FROM DAILY
# =========================================================

def build_weekly_from_daily(d1):

    if "openTime" not in d1.columns:
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

    if len(df) < 10:
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


def macd(
    series,
    fast=8,
    slow=21,
    signal=5
):

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
# STANDARD ANALYSIS
# =========================================================

def analyze_standard(df):

    minimum = 60

    if len(df) < minimum:
        raise ValueError(
            "عدد الشموع غير كافٍ للتحليل"
        )

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

    # -----------------------------------------------------
    # TREND
    # -----------------------------------------------------

    if (
        current > ema20
        and ema20 > ema50
    ):

        bullish += 30

        trend = "صاعد"

    elif (
        current < ema20
        and ema20 < ema50
    ):

        bearish += 30

        trend = "هابط"

    else:

        trend = "متذبذب"

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

            bullish += 20

        elif (
            current < ema200
            and ema20 < ema50
        ):

            bearish += 20

    else:

        # لا نستخدم EMA200 عندما لا توجد بيانات كافية.
        # لا نعطي إشارة مزيفة.
        pass

    # -----------------------------------------------------
    # RSI
    # -----------------------------------------------------

    if 50 <= rsi_value < 70:

        bullish += 15

    elif 30 < rsi_value < 50:

        bearish += 15

    elif rsi_value >= 70:

        bullish += 5

    elif rsi_value <= 30:

        bearish += 5

    # -----------------------------------------------------
    # MACD
    # -----------------------------------------------------

    if macd_value > signal_value:

        bullish += 15

    elif macd_value < signal_value:

        bearish += 15

    # -----------------------------------------------------
    # VOLUME
    # -----------------------------------------------------

    if volume_ratio >= 1.20:

        if bullish > bearish:

            bullish += 20

        elif bearish > bullish:

            bearish += 20

    elif volume_ratio >= 0.80:

        if bullish > bearish:

            bullish += 5

        elif bearish > bullish:

            bearish += 5

    # -----------------------------------------------------
    # FINAL
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

    score = int(
        max(
            0,
            min(score, 100)
        )
    )

    extended = (
        abs(current - ema20)
        > atr_value * 0.45
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
        "extended": extended,
        "bars": len(df)
    }


# =========================================================
# SCALP ANALYSIS
# =========================================================

def analyze_scalp(df):

    if len(df) < 60:

        raise ValueError(
            "بيانات الفريم السريع غير كافية"
        )

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

    # -----------------------------------------------------
    # FAST TREND
    # -----------------------------------------------------

    if (
        current > ema9
        and ema9 > ema20
        and ema20 > ema50
    ):

        bullish += 40

        trend = "صاعد"

    elif (
        current < ema9
        and ema9 < ema20
        and ema20 < ema50
    ):

        bearish += 40

        trend = "هابط"

    else:

        trend = "متذبذب"

        if current > ema20:
            bullish += 20
        else:
            bearish += 20

    # -----------------------------------------------------
    # RSI
    # -----------------------------------------------------

    if 50 <= rsi_value < 70:

        bullish += 20

    elif 30 < rsi_value < 50:

        bearish += 20

    elif rsi_value >= 70:

        bullish += 5

    elif rsi_value <= 30:

        bearish += 5

    # -----------------------------------------------------
    # MACD
    # -----------------------------------------------------

    if macd_value > signal_value:

        bullish += 20

    elif macd_value < signal_value:

        bearish += 20

    # -----------------------------------------------------
    # VOLUME
    # -----------------------------------------------------

    if volume_ratio >= 1.20:

        if bullish > bearish:

            bullish += 20

        elif bearish > bullish:

            bearish += 20

    elif volume_ratio >= 0.80:

        if bullish > bearish:

            bullish += 5

        elif bearish > bullish:

            bearish += 5

    # -----------------------------------------------------
    # FINAL
    # -----------------------------------------------------

    if bullish > bearish:

        direction = "BUY"

    elif bearish > bullish:

        direction = "SELL"

    else:

        direction = "WAIT"

    score = int(
        max(
            bullish,
            bearish
        )
    )

    score = min(
        score,
        100
    )

    extended = (
        abs(current - ema20)
        > atr_value * 0.45
    )

    return {
        "price": current,
        "ema20": ema20,
        "atr": atr_value,
        "volume_ratio": volume_ratio,
        "trend": trend,
        "direction": direction,
        "score": score,
        "extended": extended
    }


# =========================================================
# FINAL MULTI-TIMEFRAME RESULT
# =========================================================

def calculate_final(results):

    weighted = 0

    buy = 0
    sell = 0

    for result, weight in results:

        direction = result["direction"]

        score = result["score"]

        if direction == "BUY":

            weighted += score * weight

            buy += 1

        elif direction == "SELL":

            weighted -= score * weight

            sell += 1

    confidence = int(
        min(
            abs(weighted),
            100
        )
    )

    if weighted >= 60:

        signal = "BUY"

    elif weighted <= -60:

        signal = "SELL"

    else:

        signal = "WAIT"

    if buy > sell:

        bias = "صاعد"

    elif sell > buy:

        bias = "هابط"

    else:

        bias = "متذبذب"

    return (
        signal,
        confidence,
        buy,
        sell,
        bias
    )


# =========================================================
# BREAKOUT CONFIRMATION
# =========================================================

def breakout_confirmation(df, direction):

    if len(df) < 5:

        return False

    last = df.iloc[-1]

    previous = df.iloc[-2]

    if direction == "BUY":

        # إغلاق كامل فوق قمة الشمعة السابقة
        return (
            float(last["close"])
            > float(previous["high"])
        )

    if direction == "SELL":

        # إغلاق كامل تحت قاع الشمعة السابقة
        return (
            float(last["close"])
            < float(previous["low"])
        )

    return False


# =========================================================
# TRADE TYPE
# =========================================================

def determine_trade_type(
    h1,
    m15,
    m5
):

    if (
        h1["direction"]
        == m15["direction"]
        == m5["direction"]
    ):

        return (
            "صفقة سريعة",
            "دقائق إلى أقل من ساعة"
        )

    if (
        h1["direction"]
        == m15["direction"]
    ):

        return (
            "صفقة متوسطة",
            "عدة ساعات"
        )

    return (
        "انتظار",
        "حتى تتوافق الفريمات"
    )


# =========================================================
# TRADE LEVELS
# =========================================================

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

    recent_high = float(
        df["high"]
        .tail(5)
        .max()
    )

    recent_low = float(
        df["low"]
        .tail(5)
        .min()
    )

    if direction == "BUY":

        entry = current

        atr_sl = (
            entry
            - atr_value * 1.20
        )

        structure_sl = (
            recent_low
            - atr_value * 0.20
        )

        sl = min(
            atr_sl,
            structure_sl
        )

        risk = entry - sl

        tp1 = (
            entry
            + risk * 1.20
        )

        tp2 = (
            entry
            + risk * 2.00
        )

    else:

        entry = current

        atr_sl = (
            entry
            + atr_value * 1.20
        )

        structure_sl = (
            recent_high
            + atr_value * 0.20
        )

        sl = max(
            atr_sl,
            structure_sl
        )

        risk = sl - entry

        tp1 = (
            entry
            - risk * 1.20
        )

        tp2 = (
            entry
            - risk * 2.00
        )

    return (
        entry,
        sl,
        tp1,
        tp2
    )


# =========================================================
# ARABIC HELPERS
# =========================================================

def arabic_direction(direction):

    if direction == "BUY":
        return "شراء 🟢"

    if direction == "SELL":
        return "بيع 🔴"

    return "انتظار 🟡"


def arabic_trend(trend):

    if trend == "صاعد":
        return "صاعد 🟢"

    if trend == "هابط":
        return "هابط 🔴"

    return "متذبذب 🟡"


# =========================================================
# START
# =========================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    message = (
        "🤖 XAU SMART TRADER v11\n\n"

        "أهلاً بك في بوت تحليل الذهب.\n"
        "يعمل البوت على تحليل الاتجاه والفريمات "
        "وإشارات الدخول داخلياً.\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📊 التحليل\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        "📅 /weekly\n"
        "التحليل الأسبوعي\n\n"

        "📊 /daily\n"
        "التحليل اليومي\n\n"

        "⚡ /scalp\n"
        "التحليل السريع\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "🎯 التداول\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        "🎯 /trade\n"
        "أريد صفقة\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "💰 الأدوات\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        "💰 /price\n"
        "سعر وبيانات الذهب\n\n"

        "🟢 /status\n"
        "حالة البوت\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "🛠️ الدعم\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        "👨‍💻 /developer\n"
        "المطور\n\n"

        "🆘 /support\n"
        "الدعم\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "🔒 تفاصيل المؤشرات مخفية.\n"
        "🎯 يظهر لك القرار النهائي فقط."
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
        "🤖 XAU SMART TRADER v11"
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

        "🟢 XAU SMART TRADER v11\n\n"

        "📈 السوق: الذهب XAUUSD\n"
        "📊 التحليل متعدد الفريمات: يعمل\n"
        "⚡ نظام التداول السريع: يعمل\n"
        "🎯 محرك الصفقات: يعمل\n"
        "🛡️ فلتر المخاطر: يعمل\n"
        "🔒 تفاصيل المؤشرات: مخفية\n"
        "🌐 الاتصال: Telegram Webhook\n\n"

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

        live_price = None

        for interval, name in intervals:

            df = get_bars(
                interval,
                10
            )

            last = df.iloc[-1]

            if live_price is None:

                live_price = float(
                    last["close"]
                )

            blocks.append(
                f"🔹 {name}\n"
                f"الفتح: {last['open']:.2f}\n"
                f"الأعلى: {last['high']:.2f}\n"
                f"الأدنى: {last['low']:.2f}\n"
                f"الإغلاق: {last['close']:.2f}"
            )

        d1 = get_bars(
            "1d",
            1000
        )

        w1 = build_weekly_from_daily(
            d1
        )

        last = w1.iloc[-1]

        blocks.append(
            "🔹 W1\n"
            f"الفتح: {last['open']:.2f}\n"
            f"الأعلى: {last['high']:.2f}\n"
            f"الأدنى: {last['low']:.2f}\n"
            f"الإغلاق: {last['close']:.2f}"
        )

        message = (
            "🥇 بيانات الذهب XAUUSD\n\n"
            f"💰 السعر الحالي: {live_price:.2f}\n\n"
            + "\n\n".join(blocks)
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ تعذر الحصول على بيانات الذهب.\n\n"
            "يرجى المحاولة مرة أخرى بعد قليل."
        )

        print(
            f"PRICE ERROR: {e}"
        )


# =========================================================
# WEEKLY
# =========================================================

async def weekly(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        d1_raw = get_bars(
            "1d",
            1500
        )

        d1 = closed_bars(
            d1_raw
        )

        w1 = build_weekly_from_daily(
            d1
        )

        w1 = closed_bars(
            w1
        )

        h4_raw = get_bars(
            "4h",
            350
        )

        h4 = closed_bars(
            h4_raw
        )

        if len(d1) < 60:
            raise ValueError(
                "بيانات D1 غير كافية"
            )

        if len(h4) < 60:
            raise ValueError(
                "بيانات H4 غير كافية"
            )

        if len(w1) < 30:
            raise ValueError(
                "بيانات W1 غير كافية"
            )

        d1_result = analyze_standard(
            d1
        )

        h4_result = analyze_standard(
            h4
        )

        w1_result = analyze_standard(
            w1
        )

        results = [
            (w1_result, 0.40),
            (d1_result, 0.35),
            (h4_result, 0.25)
        ]

        (
            signal,
            confidence,
            buy,
            sell,
            bias
        ) = calculate_final(
            results
        )

        if signal == "BUY":

            decision = (
                "الاتجاه يميل للشراء، "
                "لكن يجب انتظار تأكيد الدخول."
            )

        elif signal == "SELL":

            decision = (
                "الاتجاه يميل للبيع، "
                "لكن يجب انتظار تأكيد الدخول."
            )

        else:

            decision = (
                "لا يوجد توافق كافٍ للدخول الآن."
            )

        message = (
            "🤖 XAU SMART TRADER v11\n"
            "📅 التحليل الأسبوعي\n\n"

            "━━━━━━━━━━━━━━━━━━\n"

            f"📈 الاتجاه العام: "
            f"{arabic_trend(bias)}\n"

            f"🎯 الإشارة النهائية: "
            f"{arabic_direction(signal)}\n"

            f"💪 قوة التوافق: {confidence}%\n"

            f"📊 توافق الفريمات: "
            f"شراء {buy}/3 | بيع {sell}/3\n"

            "━━━━━━━━━━━━━━━━━━\n\n"

            f"🧠 القرار:\n{decision}\n\n"

            "🔒 تم تحليل W1 + D1 + H4 داخلياً.\n"
            "لا يتم عرض تفاصيل المؤشرات.\n\n"

            "⚠️ قوة التوافق ليست احتمالاً مضموناً للربح."
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        print(
            f"WEEKLY ERROR: {e}"
        )

        await update.message.reply_text(
            "❌ تعذر إكمال التحليل الأسبوعي.\n\n"
            "قد تكون بيانات أحد الفريمات غير كافية.\n"
            "حاول مرة أخرى بعد قليل."
        )


# =========================================================
# DAILY
# =========================================================

async def daily(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        d1 = closed_bars(
            get_bars(
                "1d",
                350
            )
        )

        h4 = closed_bars(
            get_bars(
                "4h",
                350
            )
        )

        h1 = closed_bars(
            get_bars(
                "1h",
                350
            )
        )

        if (
            len(d1) < 60
            or len(h4) < 60
            or len(h1) < 60
        ):

            raise ValueError(
                "بيانات غير كافية"
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
            (d1_result, 0.40),
            (h4_result, 0.35),
            (h1_result, 0.25)
        ]

        (
            signal,
            confidence,
            buy,
            sell,
            bias
        ) = calculate_final(
            results
        )

        if signal == "BUY":

            decision = (
                "الاتجاه اليومي يميل للشراء."
            )

        elif signal == "SELL":

            decision = (
                "الاتجاه اليومي يميل للبيع."
            )

        else:

            decision = (
                "السوق متذبذب ولا يوجد توافق كافٍ."
            )

        message = (
            "🤖 XAU SMART TRADER v11\n"
            "📊 التحليل اليومي\n\n"

            "━━━━━━━━━━━━━━━━━━\n"

            f"📈 الاتجاه العام: "
            f"{arabic_trend(bias)}\n"

            f"🎯 الإشارة النهائية: "
            f"{arabic_direction(signal)}\n"

            f"💪 قوة التوافق: {confidence}%\n"

            f"📊 التوافق: "
            f"شراء {buy}/3 | بيع {sell}/3\n"

            "━━━━━━━━━━━━━━━━━━\n\n"

            f"🧠 القرار:\n{decision}\n\n"

            "🎯 الدخول لا يعتمد على الاتجاه وحده.\n"
            "يجب انتظار تأكيد مناسب من الفريمات السريعة.\n\n"

            "🔒 تفاصيل المؤشرات مخفية.\n"
            "⚠️ قوة التوافق ليست احتمالاً مضموناً للربح."
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        print(
            f"DAILY ERROR: {e}"
        )

        await update.message.reply_text(
            "❌ تعذر إكمال التحليل اليومي.\n\n"
            "حاول مرة أخرى بعد قليل."
        )


# =========================================================
# SCALP
# =========================================================

async def scalp(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        h1 = closed_bars(
            get_bars(
                "1h",
                350
            )
        )

        m15 = closed_bars(
            get_bars(
                "15m",
                350
            )
        )

        m5 = closed_bars(
            get_bars(
                "5m",
                350
            )
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
            x["direction"] == "BUY"
            for x in results
        )

        sell = sum(
            x["direction"] == "SELL"
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

            signal = "BUY"

        elif sell > buy:

            signal = "SELL"

        else:

            signal = "WAIT"

        same_direction = (
            buy == 3
            or sell == 3
        )

        not_extended = all(
            not x["extended"]
            for x in results
        )

        volume_ok = all(
            x["volume_ratio"] >= 0.80
            for x in results
        )

        score_ok = (
            avg_score >= 65
        )

        if (
            same_direction
            and not_extended
            and volume_ok
            and score_ok
        ):

            entry_status = (
                "🟢 شروط الدخول قوية"
            )

            decision = (
                "يمكن مراقبة الدخول بعد "
                "تأكيد شمعة الإغلاق."
            )

        else:

            entry_status = (
                "🟡 لا يوجد دخول مباشر"
            )

            decision = (
                "انتظر تصحيحاً أو تأكيداً "
                "أوضح قبل الدخول."
            )

        message = (
            "🤖 XAU SMART TRADER v11\n"
            "⚡ التحليل السريع\n\n"

            "━━━━━━━━━━━━━━━━━━\n"

            f"🎯 الإشارة: "
            f"{arabic_direction(signal)}\n"

            f"💪 قوة التوافق: {avg_score}%\n"

            f"📊 توافق الفريمات: "
            f"شراء {buy}/3 | بيع {sell}/3\n"

            "━━━━━━━━━━━━━━━━━━\n\n"

            f"{entry_status}\n\n"

            f"🧠 القرار:\n{decision}\n\n"

            "📌 الفريمات المستخدمة:\n"
            "H1 + M15 + M5\n\n"

            "🔒 المؤشرات تعمل داخلياً "
            "ولا يتم عرض تفاصيلها.\n"

            "⚠️ لا تطارد السعر بعد حركة قوية."
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        print(
            f"SCALP ERROR: {e}"
        )

        await update.message.reply_text(
            "❌ تعذر إكمال التحليل السريع.\n\n"
            "حاول مرة أخرى بعد قليل."
        )


# =========================================================
# TRADE
# =========================================================

async def trade(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        h1 = closed_bars(
            get_bars(
                "1h",
                350
            )
        )

        m15 = closed_bars(
            get_bars(
                "15m",
                350
            )
        )

        m5 = closed_bars(
            get_bars(
                "5m",
                350
            )
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
            x["direction"] == "BUY"
            for x in results
        )

        sell = sum(
            x["direction"] == "SELL"
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

        if buy > sell:

            direction = "BUY"

        elif sell > buy:

            direction = "SELL"

        else:

            direction = "WAIT"

        trade_type, duration = determine_trade_type(
            h1_result,
            m15_result,
            m5_result
        )

        # -------------------------------------------------
        # BASIC FILTERS
        # -------------------------------------------------

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

        # -------------------------------------------------
        # M5 BREAKOUT
        # -------------------------------------------------

        breakout_ok = breakout_confirmation(
            m5,
            direction
        )

        # -------------------------------------------------
        # ENTRY LOGIC
        # -------------------------------------------------

        strong_setup = (
            all_same_direction
            and no_extension
            and volume_ok
            and score_ok
        )

        direct_entry = (
            strong_setup
            and breakout_ok
        )

        if direct_entry:

            (
                entry,
                sl,
                tp1,
                tp2
            ) = build_trade(
                direction,
                m5
            )

            message = (
                "🤖 XAU SMART TRADER v11\n\n"

                "🎯 صفقة متاحة\n\n"

                f"📈 الاتجاه: "
                f"{arabic_direction(direction)}\n"

                f"⚡ النوع: {trade_type}\n"

                f"⏱️ المدة المتوقعة: {duration}\n\n"

                "━━━━━━━━━━━━━━━━━━\n"

                f"📍 الدخول: {entry:.2f}\n"

                f"🛑 وقف الخسارة: {sl:.2f}\n"

                f"🎯 الهدف الأول: {tp1:.2f}\n"

                f"🎯 الهدف الثاني: {tp2:.2f}\n"

                "━━━━━━━━━━━━━━━━━━\n\n"

                f"💪 قوة الإشارة: {confidence}%\n"

                "🟢 حالة الدخول: مؤكدة فنياً\n\n"

                "📌 تم اعتماد إغلاق شمعة التأكيد.\n"
                "🚫 لا تدخل اعتماداً على ذيل الشمعة فقط.\n"
                "🚫 لا تطارد السعر إذا ابتعد عن الدخول."
            )

        elif strong_setup:

            message = (
                "🤖 XAU SMART TRADER v11\n\n"

                "🟡 الاتجاه واضح لكن الدخول لم يتأكد بعد.\n\n"

                f"📈 الاتجاه: "
                f"{arabic_direction(direction)}\n"

                f"💪 قوة الإشارة: {confidence}%\n\n"

                "🎯 القرار:\n"

                "⏳ انتظار إغلاق شمعة تأكيد الاختراق.\n\n"

                "📌 لا نعتبر ذيل الشمعة اختراقاً.\n"
                "📌 بعد الإغلاق فوق/تحت المستوى يتم إعادة الفحص.\n"
                "🚫 لا تطارد السعر."
            )

        else:

            reasons = []

            if not all_same_direction:

                reasons.append(
                    "الفريمات ليست متوافقة بالكامل"
                )

            if not no_extension:

                reasons.append(
                    "السعر ممتد عن منطقة مناسبة"
                )

            if not volume_ok:

                reasons.append(
                    "حجم التداول يحتاج تأكيد"
                )

            if not score_ok:

                reasons.append(
                    "قوة الإشارة أقل من المستوى المطلوب"
                )

            if direction == "WAIT":

                reasons.append(
                    "لا يوجد اتجاه واضح"
                )

            reason_text = "\n".join(
                f"• {x}"
                for x in reasons
            )

            message = (
                "🤖 XAU SMART TRADER v11\n\n"

                "⏳ لا توجد صفقة الآن\n\n"

                f"📈 الاتجاه الحالي: "
                f"{arabic_direction(direction)}\n"

                f"💪 قوة الإشارة: {confidence}%\n\n"

                "⚠️ السبب:\n"

                f"{reason_text}\n\n"

                "🎯 القرار:\n"
                "انتظار اكتمال الشروط.\n\n"

                "⏱️ يفضل إعادة الفحص خلال 10–20 دقيقة.\n"
                "🚫 لا تطارد السعر.\n\n"

                "🔒 تفاصيل المؤشرات مخفية."
            )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        print(
            f"TRADE ERROR: {e}"
        )

        await update.message.reply_text(
            "❌ تعذر إنشاء إشارة الصفقة.\n\n"
            "حاول مرة أخرى بعد قليل."
        )


# =========================================================
# TELEGRAM APPLICATION
# =========================================================

def build_application():

    if not TOKEN:

        raise RuntimeError(
            "TELEGRAM_TOKEN غير موجود."
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

    return application


# =========================================================
# FLASK
# =========================================================

@app.route(
    "/",
    methods=["GET", "HEAD"]
)
def home():

    return jsonify(
        {
            "status": "online",
            "bot": "XAU SMART TRADER",
            "version": VERSION,
            "telegram": "webhook"
        }
    )


@app.route(
    "/telegram-webhook",
    methods=["POST"]
)
def telegram_webhook():

    global BOT_LOOP
    global BOT_APPLICATION

    if BOT_LOOP is None:

        return (
            jsonify(
                {
                    "ok": False,
                    "error": "bot not ready"
                }
            ),
            503
        )

    received_secret = request.headers.get(
        "X-Telegram-Bot-Api-Secret-Token"
    )

    if received_secret != WEBHOOK_SECRET:

        return (
            jsonify(
                {
                    "ok": False,
                    "error": "unauthorized"
                }
            ),
            403
        )

    try:

        data = request.get_json(
            force=True
        )

        update = Update.de_json(
            data,
            BOT_APPLICATION.bot
        )

        future = asyncio.run_coroutine_threadsafe(
            BOT_APPLICATION.update_queue.put(update),
            BOT_LOOP
        )

        future.result(
            timeout=10
        )

        return jsonify(
            {
                "ok": True
            }
        )

    except Exception as e:

        print(
            f"WEBHOOK ERROR: {e}"
        )

        return (
            jsonify(
                {
                    "ok": False
                }
            ),
            500
        )


# =========================================================
# TELEGRAM LOOP
# =========================================================

async def run_bot():

    global BOT_LOOP

    BOT_LOOP = asyncio.get_running_loop()

    await BOT_APPLICATION.initialize()

    await BOT_APPLICATION.start()

    webhook_url = (
        RENDER_URL.rstrip("/")
        + "/telegram-webhook"
    )

    # تنظيف أي Webhook قديم ثم إنشاء الجديد
    await BOT_APPLICATION.bot.delete_webhook(
        drop_pending_updates=False
    )

    await BOT_APPLICATION.bot.set_webhook(
        url=webhook_url,
        secret_token=WEBHOOK_SECRET,
        drop_pending_updates=False
    )

    print(
        "======================================"
    )

    print(
        f"XAU SMART TRADER {VERSION}"
    )

    print(
        "Telegram: WEBHOOK"
    )

    print(
        f"Webhook: {webhook_url}"
    )

    print(
        "BOT READY"
    )

    print(
        "======================================"
    )

    try:

        while True:

            await asyncio.sleep(
                3600
            )

    finally:

        try:

            await BOT_APPLICATION.bot.delete_webhook(
                drop_pending_updates=False
            )

        except Exception as e:

            print(
                f"WEBHOOK CLEANUP ERROR: {e}"
            )

        await BOT_APPLICATION.stop()

        await BOT_APPLICATION.shutdown()


# =========================================================
# SERVER
# =========================================================

def run_server():

    port = int(
        os.environ.get(
            "PORT",
            "10000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )


# =========================================================
# MAIN
# =========================================================

def main():

    global BOT_APPLICATION

    BOT_APPLICATION = build_application()

    server = threading.Thread(
        target=run_server,
        daemon=True
    )

    server.start()

    asyncio.run(
        run_bot()
    )


if __name__ == "__main__":

    main()
