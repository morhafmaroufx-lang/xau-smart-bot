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
# XAU SMART BOT v5
# =========================================================

TOKEN = os.environ.get("TELEGRAM_TOKEN")

API_URL = "https://biquote.io/api/XAUUSD/ohlc"

app = Flask(__name__)


# =========================================================
# GENERAL HELPERS
# =========================================================

def fetch_bars(interval, limit=250):
    """
    Get XAUUSD OHLC data.
    Returns dataframe sorted oldest -> newest.
    """

    response = requests.get(
        API_URL,
        params={
            "interval": interval,
            "limit": limit
        },
        timeout=25
    )

    response.raise_for_status()

    data = response.json()

    bars = data.get("bars", [])

    if not bars:
        raise ValueError(
            f"لا توجد بيانات للفريم {interval}"
        )

    df = pd.DataFrame(bars)

    required_columns = [
        "openTime",
        "open",
        "high",
        "low",
        "close",
        "tickVolume"
    ]

    missing = [
        col for col in required_columns
        if col not in df.columns
    ]

    if missing:
        raise ValueError(
            f"أعمدة ناقصة: {', '.join(missing)}"
        )

    df = df.sort_values(
        "openTime"
    ).copy()

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
            "open",
            "high",
            "low",
            "close",
            "tickVolume"
        ]
    ).reset_index(drop=True)

    return df


def safe_float(value):
    try:
        value = float(value)

        if np.isfinite(value):
            return value

    except Exception:
        pass

    return 0.0


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

    rsi = 100 - (
        100 / (1 + rs)
    )

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

    histogram = (
        macd - signal_line
    )

    return (
        macd,
        signal_line,
        histogram
    )


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
        [
            tr1,
            tr2,
            tr3
        ],
        axis=1
    ).max(axis=1)

    atr = true_range.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    return atr


# =========================================================
# WEEKLY DATA BUILDER
# =========================================================

def build_weekly_from_daily(df):

    data = df.copy()

    if "openTime" in data.columns:

        data["date"] = pd.to_datetime(
            data["openTime"],
            unit="ms",
            errors="coerce"
        )

        if data["date"].isna().all():

            data["date"] = pd.to_datetime(
                data["openTime"],
                errors="coerce"
            )

    else:

        raise ValueError(
            "openTime غير موجود"
        )

    data = data.dropna(
        subset=["date"]
    )

    data = data.set_index(
        "date"
    )

    weekly = data.resample(
        "W-FRI"
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

    return weekly


# =========================================================
# STANDARD ANALYSIS
# =========================================================

def analyze_standard_dataframe(
    df,
    rsi_period=14,
    ema_periods=(20, 50, 200),
    macd_settings=(8, 21, 5)
):

    minimum_needed = max(
        max(ema_periods),
        macd_settings[1]
    ) + 20

    if len(df) < minimum_needed:

        raise ValueError(
            f"بيانات غير كافية. "
            f"المطلوب تقريبًا {minimum_needed} شمعة، "
            f"المتاح {len(df)}"
        )

    close = df["close"]

    ema20 = calculate_ema(
        close,
        ema_periods[0]
    ).iloc[-1]

    ema50 = calculate_ema(
        close,
        ema_periods[1]
    ).iloc[-1]

    ema200 = calculate_ema(
        close,
        ema_periods[2]
    ).iloc[-1]

    rsi = calculate_rsi(
        close,
        rsi_period
    ).iloc[-1]

    macd, signal, histogram = calculate_macd(
        close,
        macd_settings[0],
        macd_settings[1],
        macd_settings[2]
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

    tick_volume = df[
        "tickVolume"
    ].iloc[-1]

    volume_window = df[
        "tickVolume"
    ].tail(20)

    average_volume = volume_window.mean()

    if average_volume > 0:

        volume_ratio = (
            tick_volume
            / average_volume
        )

    else:

        volume_ratio = 0

    return {
        "price": safe_float(current_price),
        "ema20": safe_float(ema20),
        "ema50": safe_float(ema50),
        "ema200": safe_float(ema200),
        "rsi": safe_float(rsi),
        "macd": safe_float(macd_value),
        "signal": safe_float(signal_value),
        "histogram": safe_float(
            histogram.iloc[-1]
        ),
        "atr": safe_float(atr),
        "volume": safe_float(tick_volume),
        "average_volume": safe_float(
            average_volume
        ),
        "volume_ratio": safe_float(
            volume_ratio
        )
    }


# =========================================================
# DAILY / WEEKLY SCORING
# =========================================================

def calculate_signal_score(
    price,
    ema20,
    ema50,
    ema200,
    rsi,
    macd,
    signal,
    volume_ratio
):

    bullish = 0
    bearish = 0

    warnings = []

    # -----------------------------------------------------
    # EMA - 40
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # RSI - 15
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # MACD - 25
    # -----------------------------------------------------

    if macd > signal:

        bullish += 25

    elif macd < signal:

        bearish += 25

    # -----------------------------------------------------
    # MACD CONFLICT
    # -----------------------------------------------------

    if bullish > bearish and macd < signal:

        warnings.append(
            "⚠️ MACD لا يؤكد الصعود"
        )

    if bearish > bullish and macd > signal:

        warnings.append(
            "⚠️ MACD لا يؤكد الهبوط"
        )

    # -----------------------------------------------------
    # VOLUME - 20
    # -----------------------------------------------------

    if volume_ratio >= 1.20:

        if bullish > bearish:

            bullish += 20

        elif bearish > bullish:

            bearish += 20

    else:

        warnings.append(
            "ℹ️ Volume لا يؤكد الحركة بقوة"
        )

    # -----------------------------------------------------
    # DIRECTION
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

    # -----------------------------------------------------
    # SIGNAL
    # -----------------------------------------------------

    if score >= 65:

        final_signal = direction

    elif score >= 50:

        final_signal = "WATCH"

    else:

        final_signal = "WAIT"

    return (
        final_signal,
        direction,
        round(
            max(
                0,
                min(score, 100)
            )
        ),
        warnings
    )


# =========================================================
# TREND
# =========================================================

def get_trend(
    price,
    ema20,
    ema50,
    ema200
):

    if (
        price > ema20
        and ema20 > ema50
        and ema50 > ema200
    ):

        return "🟢 صاعد قوي"

    if (
        price > ema20
        and ema20 > ema50
    ):

        return "🟢 صاعد"

    if (
        price < ema20
        and ema20 < ema50
        and ema50 < ema200
    ):

        return "🔴 هابط قوي"

    if (
        price < ema20
        and ema20 < ema50
    ):

        return "🔴 هابط"

    return "🟡 متذبذب"


# =========================================================
# FORMAT STANDARD
# =========================================================

def format_standard_result(
    name,
    data,
    signal,
    direction,
    score,
    warnings
):

    warning_text = (
        " | ".join(warnings)
        if warnings
        else "لا توجد تحذيرات"
    )

    trend = get_trend(
        data["price"],
        data["ema20"],
        data["ema50"],
        data["ema200"]
    )

    return (

        f"📊 {name}\n"

        f"💰 Price: "
        f"{data['price']:.2f}\n"

        f"📈 EMA20: "
        f"{data['ema20']:.2f}\n"

        f"📈 EMA50: "
        f"{data['ema50']:.2f}\n"

        f"📈 EMA200: "
        f"{data['ema200']:.2f}\n"

        f"RSI: "
        f"{data['rsi']:.2f}\n"

        f"MACD: "
        f"{data['macd']:.4f}\n"

        f"Signal: "
        f"{data['signal']:.4f}\n"

        f"ATR: "
        f"{data['atr']:.2f}\n"

        f"Volume: "
        f"{data['volume']:.0f}\n"

        f"Volume Ratio: "
        f"{data['volume_ratio']:.2f}x\n"

        f"Trend: "
        f"{trend}\n"

        f"Signal: "
        f"{signal}\n"

        f"Direction: "
        f"{direction}\n"

        f"Score: "
        f"{score}%\n"

        f"⚠️ {warning_text}"

    )


# =========================================================
# SCALP ANALYSIS
# =========================================================

def analyze_scalp_dataframe(df):

    minimum_needed = 60

    if len(df) < minimum_needed:

        raise ValueError(
            f"بيانات غير كافية للـ Scalp "
            f"({len(df)} شمعة)"
        )

    close = df["close"]

    ema9 = calculate_ema(
        close,
        9
    ).iloc[-1]

    ema20 = calculate_ema(
        close,
        20
    ).iloc[-1]

    ema50 = calculate_ema(
        close,
        50
    ).iloc[-1]

    rsi = calculate_rsi(
        close,
        9
    ).iloc[-1]

    macd, signal, histogram = calculate_macd(
        close,
        5,
        13,
        4
    )

    macd_value = macd.iloc[-1]

    signal_value = signal.iloc[-1]

    atr = calculate_atr(
        df["high"],
        df["low"],
        close,
        14
    ).iloc[-1]

    price = close.iloc[-1]

    volume = df[
        "tickVolume"
    ].iloc[-1]

    average_volume = df[
        "tickVolume"
    ].tail(20).mean()

    volume_ratio = (
        volume / average_volume
        if average_volume > 0
        else 0
    )

    bullish = 0
    bearish = 0
    warnings = []

    # EMA
    if price > ema9 > ema20 > ema50:

        bullish += 35

    elif price < ema9 < ema20 < ema50:

        bearish += 35

    elif price > ema20:

        bullish += 25

    elif price < ema20:

        bearish += 25

    # RSI
    if 50 <= rsi < 70:

        bullish += 20

    elif 30 < rsi < 50:

        bearish += 20

    elif rsi >= 70:

        if bullish > bearish:

            bullish += 8

            warnings.append(
                "⚠️ RSI مرتفع - لا نطارد السعر"
            )

        else:

            warnings.append(
                "⚠️ RSI تشبع شرائي قوي"
            )

    elif rsi <= 30:

        if bearish > bullish:

            bearish += 8

            warnings.append(
                "⚠️ RSI منخفض - احتمال ارتداد"
            )

        else:

            warnings.append(
                "⚠️ RSI تشبع بيعي قوي"
            )

    # MACD
    if macd_value > signal_value:

        bullish += 25

    elif macd_value < signal_value:

        bearish += 25

    # Volume
    if volume_ratio >= 1.20:

        if bullish > bearish:

            bullish += 20

        elif bearish > bullish:

            bearish += 20

    else:

        warnings.append(
            "⚠️ Volume ضعيف"
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

    # -----------------------------------------------------
    # EXTENSION FILTER
    # -----------------------------------------------------

    extension_ratio = 0

    if atr > 0:

        extension_ratio = (
            abs(price - ema20)
            / atr
        )

    if extension_ratio >= 1.0:

        warnings.append(
            "⚠️ السعر ممتد فوق EMA20 - انتظار تصحيح أفضل"
            if direction == "BUY"
            else
            "⚠️ السعر ممتد تحت EMA20 - انتظار ارتداد أفضل"
        )

    # -----------------------------------------------------
    # FINAL SIGNAL
    # -----------------------------------------------------

    if score >= 75:

        final_signal = direction

    elif score >= 60:

        final_signal = "WATCH"

    else:

        final_signal = "WAIT"

    return {
        "price": safe_float(price),
        "ema9": safe_float(ema9),
        "ema20": safe_float(ema20),
        "ema50": safe_float(ema50),
        "rsi": safe_float(rsi),
        "macd": safe_float(macd_value),
        "signal": safe_float(signal_value),
        "atr": safe_float(atr),
        "volume_ratio": safe_float(volume_ratio),
        "direction": direction,
        "score": round(
            max(
                0,
                min(score, 100)
            )
        ),
        "final_signal": final_signal,
        "warnings": warnings
    }


def format_scalp_result(
    name,
    data
):

    warning_text = (
        " | ".join(data["warnings"])
        if data["warnings"]
        else "لا توجد تحذيرات"
    )

    if data["price"] > data["ema20"]:

        trend = "🟢 صاعد"

    elif data["price"] < data["ema20"]:

        trend = "🔴 هابط"

    else:

        trend = "🟡 متذبذب"

    return (

        f"📊 {name}\n"

        f"Price: "
        f"{data['price']:.2f}\n"

        f"EMA9: "
        f"{data['ema9']:.2f}\n"

        f"EMA20: "
        f"{data['ema20']:.2f}\n"

        f"EMA50: "
        f"{data['ema50']:.2f}\n"

        f"RSI9: "
        f"{data['rsi']:.2f}\n"

        f"MACD: "
        f"{data['macd']:.4f}\n"

        f"Signal: "
        f"{data['signal']:.4f}\n"

        f"ATR: "
        f"{data['atr']:.2f}\n"

        f"Volume Ratio: "
        f"{data['volume_ratio']:.2f}x\n"

        f"Trend: "
        f"{trend}\n"

        f"Direction: "
        f"{data['direction']}\n"

        f"Score: "
        f"{data['score']}%\n"

        f"⚠️ {warning_text}"

    )


# =========================================================
# FLASK
# =========================================================

@app.route("/")
def home():

    return (
        "🟢 XAU Smart Bot v5 is running!"
    )


# =========================================================
# START
# =========================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(

        "🤖 XAU Smart Bot v5\n\n"

        "🟢 البوت يعمل بنجاح!\n\n"

        "الأوامر المتاحة:\n"

        "💰 /price - اختبار البيانات\n"

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

        "🟢 XAU Smart Bot v5 يعمل\n"

        "📈 السوق: XAUUSD\n"

        "🤖 النظام: Multi-Timeframe Analysis\n"

        "🎯 Entry Filter: ON\n"

        "🛡️ ATR Risk Filter: ON\n"

        "📊 Weekly: ON\n"

        "⚡ Scalp: ON\n"

        "⏳ الحالة: اختبار مباشر"

    )


# =========================================================
# PRICE / DATA TEST
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

        live_price = None

        for interval in intervals:

            try:

                df = fetch_bars(
                    interval,
                    5
                )

                last = df.iloc[-1]

                if live_price is None:

                    live_price = last["close"]

                results.append(

                    f"✅ {interval}\n"

                    f"Open: "
                    f"{last['open']:.2f}\n"

                    f"High: "
                    f"{last['high']:.2f}\n"

                    f"Low: "
                    f"{last['low']:.2f}\n"

                    f"Close: "
                    f"{last['close']:.2f}\n"

                    f"Tick Volume: "
                    f"{last['tickVolume']:.0f}"

                )

            except Exception as e:

                results.append(

                    f"❌ {interval}: "
                    f"{str(e)}"

                )

        # -------------------------------------------------
        # BUILD W1 FROM D1
        # -------------------------------------------------

        try:

            d1_df = fetch_bars(
                "1d",
                250
            )

            w1_df = build_weekly_from_daily(
                d1_df
            )

            if not w1_df.empty:

                last_week = w1_df.iloc[-1]

                results.append(

                    "📅 W1 — مبني من D1\n"

                    f"Open: "
                    f"{last_week['open']:.2f}\n"

                    f"High: "
                    f"{last_week['high']:.2f}\n"

                    f"Low: "
                    f"{last_week['low']:.2f}\n"

                    f"Close: "
                    f"{last_week['close']:.2f}\n"

                    f"Tick Volume: "
                    f"{last_week['tickVolume']:.0f}"

                )

        except Exception as e:

            results.append(
                f"⚠️ W1: {str(e)}"
            )

        message = (

            "🥇 XAUUSD DATA TEST v5\n\n"

            f"💰 LIVE PRICE: "
            f"{live_price:.2f}\n\n"

            + "\n\n".join(results)

            + "\n\n"

            "🟢 إذا ظهرت الفريمات بنجاح "
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

            "❌ خطأ في اختبار البيانات:\n\n"
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

        directions = []

        for interval in intervals:

            df = fetch_bars(
                interval,
                250
            )

            data = analyze_standard_dataframe(
                df
            )

            (
                signal,
                direction,
                score,
                warnings
            ) = calculate_signal_score(

                data["price"],
                data["ema20"],
                data["ema50"],
                data["ema200"],
                data["rsi"],
                data["macd"],
                data["signal"],
                data["volume_ratio"]

            )

            # WATCH remains directional.
            # This is important for MTF scoring.

            if direction == "BUY":

                directional_score = score

            elif direction == "SELL":

                directional_score = -score

            else:

                directional_score = 0

            weighted_scores.append(

                directional_score
                * weights[interval]

            )

            directions.append(
                direction
            )

            results.append(

                format_standard_result(

                    names[interval],

                    data,

                    signal,

                    direction,

                    score,

                    warnings

                )

            )

        # =================================================
        # FINAL MTF
        # =================================================

        final_score = sum(
            weighted_scores
        )

        buy_count = directions.count(
            "BUY"
        )

        sell_count = directions.count(
            "SELL"
        )

        agreement_count = max(
            buy_count,
            sell_count
        )

        if final_score >= 65:

            final_signal = "🟢 BUY"

        elif final_score <= -65:

            final_signal = "🔴 SELL"

        elif final_score > 0:

            final_signal = "🟡 WATCH BUY"

        elif final_score < 0:

            final_signal = "🟠 WATCH SELL"

        else:

            final_signal = "🟡 WAIT"

        confidence = min(
            abs(final_score),
            100
        )

        if buy_count >= 2:

            agreement = (
                f"🟢 BUY: "
                f"{buy_count}/3"
            )

        elif sell_count >= 2:

            agreement = (
                f"🔴 SELL: "
                f"{sell_count}/3"
            )

        else:

            agreement = (
                f"🟡 Mixed: "
                f"BUY {buy_count}/3 | "
                f"SELL {sell_count}/3"
            )

        message = (

            "🤖 XAU SMART BOT v5\n\n"

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

            "❌ خطأ في التحليل اليومي:\n\n"
            f"{str(e)}"

        )


# =========================================================
# WEEKLY ANALYSIS
# =========================================================

async def weekly(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        # -------------------------------------------------
        # Get D1 and build W1
        # -------------------------------------------------

        d1_df = fetch_bars(
            "1d",
            250
        )

        w1_df = build_weekly_from_daily(
            d1_df
        )

        # We don't fabricate 200 weeks.
        # If the provider doesn't return enough
        # history, report it clearly.

        if len(w1_df) < 220:

            await update.message.reply_text(

                "⚠️ بيانات W1 الحالية غير كافية "
                "لحساب EMA200 الأسبوعي بشكل موثوق.\n\n"

                f"المتاح: {len(w1_df)} شمعة أسبوعية\n"

                "المطلوب: حوالي 220 شمعة أسبوعية "
                "على الأقل.\n\n"

                "📌 لذلك لن يخترع البوت EMA200 "
                "من بيانات ناقصة."

            )

            return

        data = analyze_standard_dataframe(
            w1_df
        )

        (
            signal,
            direction,
            score,
            warnings
        ) = calculate_signal_score(

            data["price"],
            data["ema20"],
            data["ema50"],
            data["ema200"],
            data["rsi"],
            data["macd"],
            data["signal"],
            data["volume_ratio"]

        )

        trend = get_trend(

            data["price"],
            data["ema20"],
            data["ema50"],
            data["ema200"]

        )

        warning_text = (

            " | ".join(warnings)

            if warnings

            else "لا توجد تحذيرات"

        )

        message = (

            "📅 XAU SMART BOT v5\n\n"

            "📊 WEEKLY ANALYSIS — W1\n\n"

            f"💰 Price: "
            f"{data['price']:.2f}\n"

            f"📈 EMA20: "
            f"{data['ema20']:.2f}\n"

            f"📈 EMA50: "
            f"{data['ema50']:.2f}\n"

            f"📈 EMA200: "
            f"{data['ema200']:.2f}\n"

            f"RSI: "
            f"{data['rsi']:.2f}\n"

            f"MACD: "
            f"{data['macd']:.4f}\n"

            f"Signal: "
            f"{data['signal']:.4f}\n"

            f"ATR: "
            f"{data['atr']:.2f}\n"

            f"Volume: "
            f"{data['volume']:.0f}\n"

            f"Volume Ratio: "
            f"{data['volume_ratio']:.2f}x\n"

            f"Trend: "
            f"{trend}\n"

            f"Signal: "
            f"{signal}\n"

            f"Direction: "
            f"{direction}\n"

            f"Score: "
            f"{score}%\n\n"

            f"⚠️ {warning_text}\n\n"

            "━━━━━━━━━━━━━━\n"

            f"🎯 WEEKLY SIGNAL: "
            f"{signal}\n"

            f"💪 CONFIDENCE: "
            f"{score}%\n"

            "━━━━━━━━━━━━━━\n\n"

            "⚠️ التحليل الأسبوعي للاتجاه العام "
            "وليس إشارة دخول لحظية."

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

            "❌ خطأ في التحليل الأسبوعي:\n\n"
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

        intervals = [
            "1h",
            "15m",
            "5m"
        ]

        names = {
            "1h": "H1",
            "15m": "M15",
            "5m": "M5"
        }

        weights = {
            "1h": 0.40,
            "15m": 0.35,
            "5m": 0.25
        }

        results = []

        weighted_scores = []

        directions = []

        analyses = {}

        for interval in intervals:

            df = fetch_bars(
                interval,
                250
            )

            data = analyze_scalp_dataframe(
                df
            )

            analyses[interval] = data

            results.append(

                format_scalp_result(

                    names[interval],
                    data

                )

            )

            if data["direction"] == "BUY":

                directional_score = (
                    data["score"]
                )

            elif data["direction"] == "SELL":

                directional_score = -(
                    data["score"]
                )

            else:

                directional_score = 0

            weighted_scores.append(

                directional_score
                * weights[interval]

            )

            directions.append(
                data["direction"]
            )

        # =================================================
        # FINAL SCALP
        # =================================================

        final_score = sum(
            weighted_scores
        )

        buy_count = directions.count(
            "BUY"
        )

        sell_count = directions.count(
            "SELL"
        )

        if final_score >= 70:

            final_direction = "BUY"

        elif final_score <= -70:

            final_direction = "SELL"

        elif final_score > 0:

            final_direction = "WATCH BUY"

        elif final_score < 0:

            final_direction = "WATCH SELL"

        else:

            final_direction = "WAIT"

        confidence = min(
            abs(final_score),
            100
        )

        # =================================================
        # ENTRY / RISK
        # =================================================

        m5 = analyses["5m"]

        entry = m5["price"]

        atr = m5["atr"]

        # ATR risk multiplier

        sl_distance = atr * 1.20

        tp1_distance = atr * 1.50

        tp2_distance = atr * 2.30

        warnings = []

        # Strong extension filter

        extension = 0

        if atr > 0:

            extension = (
                abs(
                    entry
                    - m5["ema20"]
                )
                / atr
            )

        # Do not recommend chasing
        # if price is heavily extended.

        if extension >= 1.0:

            warnings.append(
                "⚠️ السعر ممتد عن EMA20"
            )

        if m5["rsi"] >= 80:

            warnings.append(
                "⚠️ RSI M5 في تشبع شرائي قوي"
            )

        if m5["rsi"] <= 20:

            warnings.append(
                "⚠️ RSI M5 في تشبع بيعي قوي"
            )

        # -------------------------------------------------
        # BUY
        # -------------------------------------------------

        if final_direction in [
            "BUY",
            "WATCH BUY"
        ]:

            sl = entry - sl_distance

            tp1 = entry + tp1_distance

            tp2 = entry + tp2_distance

            if extension >= 1.0:

                entry_note = (
                    "انتظار تصحيح قرب EMA20 "
                    "أفضل من مطاردة السعر"
                )

            else:

                entry_note = (
                    "دخول مشروط بتأكيد M5"
                )

        # -------------------------------------------------
        # SELL
        # -------------------------------------------------

        elif final_direction in [
            "SELL",
            "WATCH SELL"
        ]:

            sl = entry + sl_distance

            tp1 = entry - tp1_distance

            tp2 = entry - tp2_distance

            if extension >= 1.0:

                entry_note = (
                    "انتظار ارتداد قرب EMA20 "
                    "أفضل من مطاردة الهبوط"
                )

            else:

                entry_note = (
                    "دخول مشروط بتأكيد M5"
                )

        else:

            sl = 0

            tp1 = 0

            tp2 = 0

            entry_note = (
                "لا توجد إشارة دخول واضحة"
            )

        # =================================================
        # AGREEMENT
        # =================================================

        if buy_count == 3:

            agreement = "🟢 BUY: 3/3"

        elif sell_count == 3:

            agreement = "🔴 SELL: 3/3"

        else:

            agreement = (
                f"BUY: {buy_count}/3 | "
                f"SELL: {sell_count}/3"
            )

        warning_text = (

            " | ".join(warnings)

            if warnings

            else "لا توجد تحذيرات إضافية"

        )

        message = (

            "🤖 XAU SMART BOT v5\n"

            "⚡ SCALP ANALYSIS\n\n"

            "━━━━━━━━━━━━━━\n"

            + "\n\n".join(results)

            + "\n\n"

            "━━━━━━━━━━━━━━\n"

            f"🎯 FINAL SCALP: "
            f"{final_direction}\n"

            f"💪 CONFIDENCE: "
            f"{confidence:.0f}%\n"

            f"📊 AGREEMENT: "
            f"{agreement}\n\n"

            f"🎯 Entry: "
            f"{entry:.2f}\n"

            f"🛑 SL: "
            f"{sl:.2f}\n"

            f"🎯 TP1: "
            f"{tp1:.2f}\n"

            f"🎯 TP2: "
            f"{tp2:.2f}\n\n"

            f"📌 Entry Filter: "
            f"{entry_note}\n"

            f"⚠️ {warning_text}\n"

            "━━━━━━━━━━━━━━\n\n"

            "⚠️ التحليل اللحظي لا يضمن الربح."

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

            "❌ خطأ في التحليل اللحظي:\n\n"
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
