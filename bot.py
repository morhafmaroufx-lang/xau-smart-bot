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
BASE_URL = "https://biquote.io/api/XAUUSD/ohlc"

app = Flask(__name__)


# =========================================================
# INDICATORS
# =========================================================

def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


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

    return 100 - (100 / (1 + rs))


def calculate_macd(series, fast=8, slow=21, signal=5):
    ema_fast = calculate_ema(series, fast)
    ema_slow = calculate_ema(series, slow)

    macd = ema_fast - ema_slow
    signal_line = calculate_ema(macd, signal)
    histogram = macd - signal_line

    return macd, signal_line, histogram


def calculate_atr(high, low, close, period=14):
    previous_close = close.shift(1)

    tr1 = high - low
    tr2 = (high - previous_close).abs()
    tr3 = (low - previous_close).abs()

    true_range = pd.concat(
        [tr1, tr2, tr3],
        axis=1
    ).max(axis=1)

    return true_range.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()


# =========================================================
# DATA
# =========================================================

def get_bars(interval, limit=250):

    response = requests.get(
        BASE_URL,
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
                f"الحقل {column} غير موجود"
            )

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        )

    df = df.dropna(
        subset=required
    )

    return df


def sort_bars(df):

    if "openTime" in df.columns:

        numeric_time = pd.to_numeric(
            df["openTime"],
            errors="coerce"
        )

        if numeric_time.notna().sum() > 0:
            df = df.assign(
                _sort_time=numeric_time
            ).sort_values(
                "_sort_time"
            ).drop(
                columns=["_sort_time"]
            )

        else:
            try:
                parsed = pd.to_datetime(
                    df["openTime"],
                    errors="coerce",
                    utc=True
                )

                if parsed.notna().sum() > 0:
                    df = df.assign(
                        _sort_time=parsed
                    ).sort_values(
                        "_sort_time"
                    ).drop(
                        columns=["_sort_time"]
                    )
            except Exception:
                pass

    return df.reset_index(drop=True)


# =========================================================
# WEEKLY BUILDER
# =========================================================

def build_weekly_from_daily(df):

    if "openTime" not in df.columns:
        raise ValueError(
            "بيانات D1 لا تحتوي على openTime"
        )

    raw = df["openTime"]

    numeric = pd.to_numeric(
        raw,
        errors="coerce"
    )

    dates = pd.Series(
        pd.NaT,
        index=df.index,
        dtype="datetime64[ns, UTC]"
    )

    numeric_mask = numeric.notna()

    if numeric_mask.any():

        values = numeric[numeric_mask]

        # يدعم seconds / milliseconds / microseconds
        median_value = values.abs().median()

        if median_value > 1e17:
            unit = "ns"
        elif median_value > 1e14:
            unit = "us"
        elif median_value > 1e11:
            unit = "ms"
        else:
            unit = "s"

        dates.loc[numeric_mask] = pd.to_datetime(
            values,
            unit=unit,
            errors="coerce",
            utc=True
        )

    text_mask = dates.isna()

    if text_mask.any():

        parsed = pd.to_datetime(
            raw[text_mask],
            errors="coerce",
            utc=True
        )

        dates.loc[text_mask] = parsed

    if dates.notna().sum() < 10:
        raise ValueError(
            "تعذر قراءة تواريخ D1 لبناء W1"
        )

    work = df.copy()
    work["_date"] = dates

    work = work.dropna(
        subset=["_date"]
    )

    work = work.sort_values(
        "_date"
    )

    work["_week"] = (
        work["_date"]
        .dt.to_period("W-SUN")
        .dt.start_time
    )

    weekly = work.groupby(
        "_week",
        sort=True
    ).agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        tickVolume=("tickVolume", "sum")
    ).reset_index()

    weekly.rename(
        columns={"_week": "openTime"},
        inplace=True
    )

    return weekly


def get_weekly():

    # نحاول أولاً الحصول على W1 مباشرة
    try:

        df = get_bars(
            "1w",
            250
        )

        if len(df) >= 10:
            return sort_bars(df)

    except Exception:
        pass

    # fallback: بناء W1 من D1
    daily = get_bars(
        "1d",
        500
    )

    return build_weekly_from_daily(
        daily
    )


# =========================================================
# ANALYSIS ENGINE
# =========================================================

def analyze_frame(
    df,
    ema_fast=20,
    ema_slow=50,
    ema_long=200,
    rsi_period=14,
    macd_fast=8,
    macd_slow=21,
    macd_signal=5,
    use_long_ema=True
):

    df = sort_bars(df)

    close = df["close"]

    price = float(
        close.iloc[-1]
    )

    ema_fast_value = float(
        calculate_ema(
            close,
            ema_fast
        ).iloc[-1]
    )

    ema_slow_value = float(
        calculate_ema(
            close,
            ema_slow
        ).iloc[-1]
    )

    ema_long_value = None

    if use_long_ema and len(df) >= ema_long:

        ema_long_value = float(
            calculate_ema(
                close,
                ema_long
            ).iloc[-1]
        )

    rsi = float(
        calculate_rsi(
            close,
            rsi_period
        ).iloc[-1]
    )

    macd, signal, histogram = calculate_macd(
        close,
        macd_fast,
        macd_slow,
        macd_signal
    )

    macd_value = float(
        macd.iloc[-1]
    )

    signal_value = float(
        signal.iloc[-1]
    )

    atr = float(
        calculate_atr(
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

    # -----------------------------------------------------
    # TREND
    # -----------------------------------------------------

    if ema_long_value is not None:

        if price > ema_fast_value > ema_slow_value > ema_long_value:
            trend = "🟢 صاعد قوي"

        elif price < ema_fast_value < ema_slow_value < ema_long_value:
            trend = "🔴 هابط قوي"

        elif price > ema_fast_value > ema_slow_value:
            trend = "🟢 صاعد"

        elif price < ema_fast_value < ema_slow_value:
            trend = "🔴 هابط"

        else:
            trend = "🟡 متذبذب"

    else:

        if price > ema_fast_value > ema_slow_value:
            trend = "🟢 صاعد"

        elif price < ema_fast_value < ema_slow_value:
            trend = "🔴 هابط"

        else:
            trend = "🟡 متذبذب"

    # -----------------------------------------------------
    # SCORE
    # -----------------------------------------------------

    bullish = 0
    bearish = 0
    warnings = []

    # EMA
    if price > ema_fast_value > ema_slow_value:
        bullish += 30

    elif price < ema_fast_value < ema_slow_value:
        bearish += 30

    elif price > ema_fast_value:
        bullish += 15

    elif price < ema_fast_value:
        bearish += 15

    if ema_long_value is not None:

        if price > ema_long_value:
            bullish += 10

        elif price < ema_long_value:
            bearish += 10

    else:

        warnings.append(
            "ℹ️ EMA200 غير مستخدم: التاريخ غير كافٍ"
        )

    # RSI
    if 50 <= rsi < 70:
        bullish += 15

    elif 30 < rsi < 50:
        bearish += 15

    elif rsi >= 70:

        if bullish >= bearish:
            bullish += 5

        warnings.append(
            "⚠️ RSI مرتفع - احتمال تصحيح"
        )

    elif rsi <= 30:

        if bearish >= bullish:
            bearish += 5

        warnings.append(
            "⚠️ RSI منخفض - احتمال ارتداد"
        )

    # MACD
    if macd_value > signal_value:
        bullish += 25

    elif macd_value < signal_value:
        bearish += 25

    # MACD conflict
    if bullish > bearish and macd_value < signal_value:

        warnings.append(
            "⚠️ MACD لا يؤكد الصعود"
        )

    if bearish > bullish and macd_value > signal_value:

        warnings.append(
            "⚠️ MACD لا يؤكد الهبوط"
        )

    # Volume
    if volume_ratio >= 1.20:

        if bullish > bearish:
            bullish += 20

        elif bearish > bullish:
            bearish += 20

    else:

        warnings.append(
            "ℹ️ Volume ضعيف"
        )

    # Direction
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

    # Price extension
    extension = False

    if ema_fast_value != 0:

        extension_percent = (
            abs(price - ema_fast_value)
            / ema_fast_value
            * 100
        )

        if extension_percent >= 0.35:
            extension = True

    return {
        "price": price,
        "ema_fast": ema_fast_value,
        "ema_slow": ema_slow_value,
        "ema_long": ema_long_value,
        "rsi": rsi,
        "macd": macd_value,
        "signal": signal_value,
        "atr": atr,
        "volume": volume,
        "volume_ratio": volume_ratio,
        "trend": trend,
        "direction": direction,
        "score": score,
        "warnings": warnings,
        "extension": extension
    }


# =========================================================
# DISPLAY
# =========================================================

def format_frame(name, result):

    warning_text = ""

    if result["warnings"]:

        warning_text = (
            "\nWarnings:\n"
            + "\n".join(
                result["warnings"]
            )
        )

    return (
        f"📊 {name}\n"
        f"Trend: {result['trend']}\n"
        f"Direction: "
        f"{'🟢 BUY' if result['direction'] == 'BUY' else '🔴 SELL' if result['direction'] == 'SELL' else '🟡 WAIT'}\n"
        f"Score: {result['score']}%"
        f"{warning_text}"
    )


def agreement(results):

    buys = sum(
        1 for r in results
        if r["direction"] == "BUY"
    )

    sells = sum(
        1 for r in results
        if r["direction"] == "SELL"
    )

    total = len(results)

    return (
        f"🟢 BUY: {buys}/{total} | "
        f"🔴 SELL: {sells}/{total}"
    )


def final_direction(results):

    if not results:
        return "WAIT", 0

    buy_scores = [
        r["score"]
        for r in results
        if r["direction"] == "BUY"
    ]

    sell_scores = [
        r["score"]
        for r in results
        if r["direction"] == "SELL"
    ]

    buy_avg = (
        sum(buy_scores) / len(buy_scores)
        if buy_scores else 0
    )

    sell_avg = (
        sum(sell_scores) / len(sell_scores)
        if sell_scores else 0
    )

    buy_count = len(buy_scores)
    sell_count = len(sell_scores)

    if buy_count > sell_count:
        direction = "BUY"
        confidence = buy_avg * (
            buy_count / len(results)
        )

    elif sell_count > buy_count:
        direction = "SELL"
        confidence = sell_avg * (
            sell_count / len(results)
        )

    else:
        return "WAIT", 0

    confidence = int(
        round(
            min(
                confidence,
                100
            )
        )
    )

    return direction, confidence


def final_label(direction, confidence):

    if direction == "BUY":

        if confidence >= 70:
            return "🟢 BUY"

        if confidence >= 50:
            return "🟡 WATCH BUY"

    if direction == "SELL":

        if confidence >= 70:
            return "🔴 SELL"

        if confidence >= 50:
            return "🟡 WATCH SELL"

    return "🟡 WAIT"


# =========================================================
# SUMMARY
# =========================================================

def create_summary(
    results,
    mode="normal"
):

    direction, confidence = final_direction(
        results
    )

    all_warnings = []

    for result in results:
        all_warnings.extend(
            result["warnings"]
        )

    has_extension = any(
        r["extension"]
        for r in results
    )

    macd_conflict = any(
        "MACD لا يؤكد" in w
        for w in all_warnings
    )

    volume_weak = any(
        "Volume ضعيف" in w
        for w in all_warnings
    )

    if direction == "BUY":

        if has_extension:

            text = (
                "الاتجاه العام صاعد، "
                "لكن السعر ممتد. "
                "الأفضل انتظار تصحيح "
                "وتأكيد قبل الدخول."
            )

            decision = (
                "⏳ انتظار تصحيح وتأكيد"
            )

        elif macd_conflict or volume_weak:

            text = (
                "الاتجاه صاعد، لكن "
                "التأكيد غير مكتمل."
            )

            decision = (
                "⏳ انتظار تأكيد أفضل"
            )

        else:

            text = (
                "الاتجاه الصاعد متوافق "
                "والشروط جيدة."
            )

            decision = (
                "🟢 يمكن مراقبة دخول BUY"
            )

    elif direction == "SELL":

        if has_extension:

            text = (
                "الاتجاه هابط، لكن السعر "
                "ممتد. الأفضل انتظار "
                "تصحيح قبل البيع."
            )

            decision = (
                "⏳ انتظار تصحيح وتأكيد"
            )

        elif macd_conflict or volume_weak:

            text = (
                "الاتجاه هابط، لكن "
                "التأكيد غير مكتمل."
            )

            decision = (
                "⏳ انتظار تأكيد أفضل"
            )

        else:

            text = (
                "الاتجاه الهابط متوافق "
                "والشروط جيدة."
            )

            decision = (
                "🔴 يمكن مراقبة دخول SELL"
            )

    else:

        text = (
            "الفريمات غير متوافقة "
            "بشكل كافٍ."
        )

        decision = (
            "⏳ انتظار"
        )

    return (
        f"🧠 الخلاصة:\n{text}\n\n"
        f"🎯 القرار:\n{decision}"
    )


# =========================================================
# START
# =========================================================

async def start(update, context):

    await update.message.reply_text(

        "🤖 XAU SMART TRADER v9\n\n"

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

        "🎯 اختر ما تريد."
    )


# =========================================================
# STATUS
# =========================================================

async def status(update, context):

    await update.message.reply_text(

        "🟢 XAU Smart Bot v9 يعمل\n\n"

        "📈 السوق: XAUUSD\n"
        "🤖 النظام: Multi-Timeframe Analysis\n\n"

        "📅 Weekly: ON\n"
        "📊 Daily: ON\n"
        "⚡ Scalp: ON\n"
        "🎯 Trade Filter: ON\n"
        "🛡️ ATR Risk Filter: ON\n"
        "🔒 المؤشرات التفصيلية: مخفية\n\n"

        "⏳ الحالة: تشغيل مباشر"
    )


# =========================================================
# PRICE
# =========================================================

async def price(update, context):

    try:

        intervals = [
            ("5m", "M5"),
            ("15m", "M15"),
            ("1h", "H1"),
            ("4h", "H4"),
            ("1d", "D1")
        ]

        results = []

        live_price = None

        for interval, name in intervals:

            df = get_bars(
                interval,
                5
            )

            df = sort_bars(df)

            last = df.iloc[-1]

            live_price = float(
                last["close"]
            )

            results.append(

                f"✅ {name}\n"
                f"Open: {last['open']:.2f}\n"
                f"High: {last['high']:.2f}\n"
                f"Low: {last['low']:.2f}\n"
                f"Close: {last['close']:.2f}\n"
                f"Volume: {last['tickVolume']:.0f}"
            )

        # W1
        try:

            weekly = get_weekly()

            last = weekly.iloc[-1]

            results.append(

                "📅 W1\n"
                f"Open: {last['open']:.2f}\n"
                f"High: {last['high']:.2f}\n"
                f"Low: {last['low']:.2f}\n"
                f"Close: {last['close']:.2f}\n"
                f"Volume: {last['tickVolume']:.0f}"
            )

        except Exception as e:

            results.append(
                "⚠️ W1\n"
                f"تعذر البناء: {str(e)}"
            )

        message = (

            "🥇 XAUUSD DATA TEST v9\n\n"

            f"💰 LIVE PRICE: "
            f"{live_price:.2f}\n\n"

            + "\n\n".join(results)

            + "\n\n🟢 مصدر البيانات يعمل."
        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ خطأ في بيانات السعر:\n"
            f"{str(e)}"
        )


# =========================================================
# WEEKLY
# =========================================================

async def weekly(update, context):

    try:

        frames = [
            ("W1", get_weekly()),
            ("D1", get_bars("1d", 300)),
            ("H4", get_bars("4h", 300))
        ]

        results = []

        for name, df in frames:

            result = analyze_frame(
                df,
                20,
                50,
                200,
                14,
                8,
                21,
                5
            )

            results.append(
                (name, result)
            )

        only_results = [
            r for _, r in results
        ]

        direction, confidence = final_direction(
            only_results
        )

        output = [
            "🤖 XAU SMART TRADER v9",
            "📅 WEEKLY ANALYSIS",
            ""
        ]

        for name, result in results:

            output.append(
                format_frame(
                    name,
                    result
                )
            )

            output.append("")

        output.extend([

            "━━━━━━━━━━━━━━━━━━",

            f"🎯 FINAL WEEKLY: "
            f"{final_label(direction, confidence)}",

            f"💪 CONFIDENCE: "
            f"{confidence}%",

            f"📊 AGREEMENT: "
            f"{agreement(only_results)}",

            "",

            create_summary(
                only_results,
                "weekly"
            ),

            "",

            "━━━━━━━━━━━━━━━━━━",

            "🔒 المؤشرات التفصيلية مخفية",

            "⚠️ Confidence = توافق الإشارات وليس احتمال الربح."

        ])

        await update.message.reply_text(
            "\n".join(output)
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ خطأ في التحليل الأسبوعي:\n"
            f"{str(e)}"
        )


# =========================================================
# DAILY
# =========================================================

async def daily(update, context):

    try:

        frames = [
            ("D1", get_bars("1d", 300)),
            ("H4", get_bars("4h", 300)),
            ("H1", get_bars("1h", 300))
        ]

        results = []

        for name, df in frames:

            result = analyze_frame(
                df,
                20,
                50,
                200,
                14,
                8,
                21,
                5
            )

            results.append(
                (name, result)
            )

        only_results = [
            r for _, r in results
        ]

        direction, confidence = final_direction(
            only_results
        )

        output = [
            "🤖 XAU SMART TRADER v9",
            "📊 DAILY ANALYSIS",
            ""
        ]

        for name, result in results:

            output.append(
                format_frame(
                    name,
                    result
                )
            )

            output.append("")

        output.extend([

            "━━━━━━━━━━━━━━━━━━",

            f"🎯 FINAL SIGNAL: "
            f"{final_label(direction, confidence)}",

            f"💪 CONFIDENCE: "
            f"{confidence}%",

            f"📊 AGREEMENT: "
            f"{agreement(only_results)}",

            "",

            create_summary(
                only_results,
                "daily"
            ),

            "",

            "━━━━━━━━━━━━━━━━━━",

            "🔒 المؤشرات التفصيلية مخفية",

            "⚠️ Confidence = توافق الإشارات وليس احتمال الربح."

        ])

        await update.message.reply_text(
            "\n".join(output)
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ خطأ في التحليل اليومي:\n"
            f"{str(e)}"
        )


# =========================================================
# SCALP
# =========================================================

async def scalp(update, context):

    try:

        frames = [
            ("H1", get_bars("1h", 250)),
            ("M15", get_bars("15m", 250)),
            ("M5", get_bars("5m", 250))
        ]

        results = []

        for name, df in frames:

            result = analyze_frame(
                df,
                9,
                20,
                50,
                9,
                5,
                13,
                4,
                use_long_ema=False
            )

            results.append(
                (name, result)
            )

        only_results = [
            r for _, r in results
        ]

        direction, confidence = final_direction(
            only_results
        )

        output = [
            "🤖 XAU SMART TRADER v9",
            "⚡ SCALP ANALYSIS",
            ""
        ]

        for name, result in results:

            output.append(
                format_frame(
                    name,
                    result
                )
            )

            output.append("")

        # Entry decision
        all_same = (
            len(only_results) == 3
            and all(
                r["direction"] == direction
                for r in only_results
            )
        )

        strong_entry = (
            all_same
            and confidence >= 70
            and not any(
                r["extension"]
                for r in only_results
            )
        )

        output.extend([
            "━━━━━━━━━━━━━━━━━━",
            f"🎯 FINAL SCALP: "
            f"{final_label(direction, confidence)}",
            f"💪 CONFIDENCE: {confidence}%",
            f"📊 AGREEMENT: {agreement(only_results)}",
        ])

        if strong_entry:

            output.extend([
                "",
                "🟢 Entry متاح",
                "🎯 يمكن مراقبة الدخول مع تأكيد السعر.",
                "",
                create_summary(
                    only_results,
                    "scalp"
                )
            ])

        else:

            output.extend([
                "",
                "⏳ Entry Filter: ON",
                "🚫 لا يوجد Entry حاليًا.",
                "",
                "⏳ القرار:",
                "انتظار تصحيح أو تأكيد أفضل."
            ])

        output.extend([
            "",
            "━━━━━━━━━━━━━━━━━━",
            "🔒 المؤشرات التفصيلية مخفية",
            "⚠️ التحليل اللحظي لا يضمن الربح."
        ])

        await update.message.reply_text(
            "\n".join(output)
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ خطأ في التحليل اللحظي:\n"
            f"{str(e)}"
        )


# =========================================================
# TRADE — أريد صفقة
# =========================================================

async def trade(update, context):

    try:

        frames = [
            ("H1", get_bars("1h", 250)),
            ("M15", get_bars("15m", 250)),
            ("M5", get_bars("5m", 250))
        ]

        results = []

        for name, df in frames:

            result = analyze_frame(
                df,
                9,
                20,
                50,
                9,
                5,
                13,
                4,
                use_long_ema=False
            )

            results.append(
                (name, result)
            )

        only_results = [
            r for _, r in results
        ]

        direction, confidence = final_direction(
            only_results
        )

        same_direction = (
            len(only_results) == 3
            and all(
                r["direction"] == direction
                for r in only_results
            )
        )

        extension = any(
            r["extension"]
            for r in only_results
        )

        weak_volume = any(
            r["volume_ratio"] < 0.80
            for r in only_results
        )

        # شروط الصفقة
        trade_available = (
            direction in ["BUY", "SELL"]
            and same_direction
            and confidence >= 70
            and not extension
        )

        # السعر الحالي
        price = only_results[-1]["price"]

        atr = only_results[-1]["atr"]

        # =================================================
        # TRADE AVAILABLE
        # =================================================

        if trade_available:

            if direction == "BUY":

                entry_low = price - atr * 0.15
                entry_high = price + atr * 0.05

                sl = entry_low - atr * 0.80

                risk = entry_high - sl

                tp1 = entry_high + risk * 1.25
                tp2 = entry_high + risk * 2.00

                side = "🟢 BUY"

            else:

                entry_high = price + atr * 0.15
                entry_low = price - atr * 0.05

                sl = entry_high + atr * 0.80

                risk = sl - entry_low

                tp1 = entry_low - risk * 1.25
                tp2 = entry_low - risk * 2.00

                side = "🔴 SELL"

            output = [

                "🤖 XAU SMART TRADER v9",
                "",
                "🎯 صفقة متاحة",
                "",
                f"{side} XAUUSD",
                "",
                f"📍 Entry: "
                f"{entry_low:.2f} - {entry_high:.2f}",

                f"🛑 SL: {sl:.2f}",

                f"🎯 TP1: {tp1:.2f}",

                f"🎯 TP2: {tp2:.2f}",

                "",
                f"💪 Confidence: {confidence}%",

                f"📊 Agreement: "
                f"{agreement(only_results)}",

                "",
                "🧠 الخلاصة:",

                "الفريمات متوافقة "
                "والشروط الحالية تسمح "
                "بمراقبة دخول.",

                "",
                "🛡️ استخدم إدارة رأس المال.",

                "",
                "⚠️ الصفقة إشارة تحليلية "
                "وليست ضمانًا للربح."
            ]

        # =================================================
        # NO TRADE
        # =================================================

        else:

            reasons = []

            if not same_direction:
                reasons.append(
                    "الفريمات غير متوافقة"
                )

            if confidence < 70:
                reasons.append(
                    "قوة الإشارة غير كافية"
                )

            if extension:
                reasons.append(
                    "السعر ممتد عن المتوسط"
                )

            if weak_volume:
                reasons.append(
                    "Volume ضعيف"
                )

            if not reasons:
                reasons.append(
                    "الشروط لم تكتمل"
                )

            # مدة الانتظار
            if extension:
                wait_time = "10–20 دقيقة"

            elif not same_direction:
                wait_time = "15–30 دقيقة"

            elif weak_volume:
                wait_time = "10–15 دقيقة"

            else:
                wait_time = "5–10 دقائق"

            output = [

                "🤖 XAU SMART TRADER v9",
                "",
                "⏳ لا توجد صفقة الآن",
                "",
                f"📊 الاتجاه الحالي: "
                f"{'🟢 BUY' if direction == 'BUY' else '🔴 SELL' if direction == 'SELL' else '🟡 WAIT'}",

                f"💪 Confidence: {confidence}%",

                "",
                "⚠️ السبب:",

                *[
                    f"• {reason}"
                    for reason in reasons
                ],

                "",
                f"⏱️ إعادة الفحص المقترحة: "
                f"{wait_time}",

                "",
                "🎯 القرار:",

                "⏳ انتظار اكتمال الشروط.",

                "",
                "🚫 لا تطارد السعر.",

                "",
                "🔒 المؤشرات التفصيلية مخفية."
            ]

        await update.message.reply_text(
            "\n".join(output)
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ خطأ في البحث عن صفقة:\n"
            f"{str(e)}"
        )


# =========================================================
# FLASK
# =========================================================

@app.route("/")
def home():

    return "XAU Smart Trader v9 is running!"


# =========================================================
# BOT
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
# SERVER
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
