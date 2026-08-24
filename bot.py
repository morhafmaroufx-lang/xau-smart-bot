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
SYMBOL = "XAUUSD"
BASE_URL = "https://biquote.io/api"

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
    ema_fast = calculate_ema(series, fast)
    ema_slow = calculate_ema(series, slow)

    macd = ema_fast - ema_slow
    signal_line = calculate_ema(macd, signal)
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

    return true_range.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()


# =========================================================
# DATA
# =========================================================

def fetch_ohlc(
    interval,
    limit=250,
    closed_only=True
):
    url = f"{BASE_URL}/{SYMBOL}/ohlc"

    response = requests.get(
        url,
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
            f"No bars returned for {interval}"
        )

    df = pd.DataFrame(bars)

    required = [
        "openTime",
        "open",
        "high",
        "low",
        "close",
        "tickVolume"
    ]

    for column in required:
        if column not in df.columns:
            raise ValueError(
                f"Missing column: {column}"
            )

    if closed_only and "isOpen" in df.columns:
        df = df[
            df["isOpen"] != True
        ].copy()

    df["openTime"] = pd.to_datetime(
        df["openTime"],
        errors="coerce"
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

    df = df.dropna(
        subset=[
            "open",
            "high",
            "low",
            "close",
            "tickVolume"
        ]
    )

    df = df.sort_values(
        "openTime"
    ).reset_index(
        drop=True
    )

    return df


def fetch_current_price():
    response = requests.get(
        f"{BASE_URL}/{SYMBOL}",
        timeout=15
    )

    response.raise_for_status()

    data = response.json()

    return float(
        data.get("mid")
    )


# =========================================================
# BUILD WEEKLY FROM DAILY
# =========================================================

def build_weekly_from_daily(df):
    temp = df.copy()

    temp["openTime"] = pd.to_datetime(
        temp["openTime"]
    )

    temp = temp.set_index(
        "openTime"
    )

    weekly = temp.resample(
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

    weekly = weekly.dropna()

    weekly = weekly.reset_index()

    return weekly


# =========================================================
# INDICATOR SNAPSHOT
# =========================================================

def calculate_snapshot(
    df,
    ema_fast=20,
    ema_mid=50,
    ema_slow=200,
    rsi_period=14,
    macd_fast=8,
    macd_slow=21,
    macd_signal=5
):

    if len(df) < max(
        ema_slow + 10,
        50
    ):
        raise ValueError(
            "بيانات غير كافية لحساب المؤشرات"
        )

    close = df["close"]

    ema20 = calculate_ema(
        close,
        ema_fast
    ).iloc[-1]

    ema50 = calculate_ema(
        close,
        ema_mid
    ).iloc[-1]

    ema200 = calculate_ema(
        close,
        ema_slow
    ).iloc[-1]

    rsi_series = calculate_rsi(
        close,
        rsi_period
    )

    rsi = rsi_series.iloc[-1]

    macd, signal, histogram = calculate_macd(
        close,
        macd_fast,
        macd_slow,
        macd_signal
    )

    macd_value = macd.iloc[-1]
    signal_value = signal.iloc[-1]
    histogram_value = histogram.iloc[-1]

    previous_histogram = (
        histogram.iloc[-2]
        if len(histogram) >= 2
        else histogram_value
    )

    atr = calculate_atr(
        df["high"],
        df["low"],
        close,
        14
    ).iloc[-1]

    price = close.iloc[-1]

    volume = df["tickVolume"].iloc[-1]

    average_volume = (
        df["tickVolume"]
        .tail(20)
        .mean()
    )

    volume_ratio = (
        volume / average_volume
        if average_volume > 0
        else 0
    )

    return {
        "price": float(price),
        "ema20": float(ema20),
        "ema50": float(ema50),
        "ema200": float(ema200),
        "rsi": float(rsi),
        "macd": float(macd_value),
        "signal": float(signal_value),
        "histogram": float(histogram_value),
        "previous_histogram": float(previous_histogram),
        "atr": float(atr),
        "volume": float(volume),
        "average_volume": float(average_volume),
        "volume_ratio": float(volume_ratio)
    }


# =========================================================
# TREND
# =========================================================

def get_trend(snapshot):

    p = snapshot["price"]
    e20 = snapshot["ema20"]
    e50 = snapshot["ema50"]
    e200 = snapshot["ema200"]

    if p > e20 > e50 > e200:
        return "BULLISH_STRONG"

    if p > e20 > e50:
        return "BULLISH"

    if p < e20 < e50 < e200:
        return "BEARISH_STRONG"

    if p < e20 < e50:
        return "BEARISH"

    return "RANGE"


def trend_text(trend):

    mapping = {
        "BULLISH_STRONG": "🟢 صاعد قوي",
        "BULLISH": "🟢 صاعد",
        "BEARISH_STRONG": "🔴 هابط قوي",
        "BEARISH": "🔴 هابط",
        "RANGE": "🟡 متذبذب"
    }

    return mapping.get(
        trend,
        "🟡 متذبذب"
    )


# =========================================================
# ENTRY ENGINE
# =========================================================

def entry_engine(
    snapshot,
    higher_trend=None,
    scalp=False
):

    score_buy = 0
    score_sell = 0

    reasons_buy = []
    reasons_sell = []
    warnings = []

    p = snapshot["price"]
    e20 = snapshot["ema20"]
    e50 = snapshot["ema50"]
    e200 = snapshot["ema200"]
    rsi = snapshot["rsi"]
    macd = snapshot["macd"]
    signal = snapshot["signal"]
    hist = snapshot["histogram"]
    prev_hist = snapshot["previous_histogram"]
    volume_ratio = snapshot["volume_ratio"]

    # =====================================================
    # EMA
    # =====================================================

    if p > e20 > e50 > e200:
        score_buy += 30
        reasons_buy.append(
            "EMA ترتيب صاعد كامل"
        )

    elif p > e20 > e50:
        score_buy += 25
        reasons_buy.append(
            "EMA صاعد"
        )

    elif p > e20:
        score_buy += 15

    if p < e20 < e50 < e200:
        score_sell += 30
        reasons_sell.append(
            "EMA ترتيب هابط كامل"
        )

    elif p < e20 < e50:
        score_sell += 25
        reasons_sell.append(
            "EMA هابط"
        )

    elif p < e20:
        score_sell += 15

    # =====================================================
    # RSI
    # =====================================================

    if 50 <= rsi < 68:
        score_buy += 15
        reasons_buy.append(
            "RSI يدعم الصعود"
        )

    elif 68 <= rsi < 75:
        score_buy += 8
        warnings.append(
            "RSI مرتفع - لا نطارد السعر"
        )

    elif rsi >= 75:
        warnings.append(
            "RSI تشبع شرائي قوي"
        )

    if 32 < rsi < 50:
        score_sell += 15
        reasons_sell.append(
            "RSI يدعم الهبوط"
        )

    elif 25 < rsi <= 32:
        score_sell += 8
        warnings.append(
            "RSI منخفض - لا نطارد الهبوط"
        )

    elif rsi <= 25:
        warnings.append(
            "RSI تشبع بيعي قوي"
        )

    # =====================================================
    # MACD
    # =====================================================

    if macd > signal:

        score_buy += 15

        if hist > prev_hist:
            score_buy += 10
            reasons_buy.append(
                "MACD يتحسن"
            )
        else:
            warnings.append(
                "MACD موجب لكن الزخم يتباطأ"
            )

    elif macd < signal:

        score_sell += 15

        if hist < prev_hist:
            score_sell += 10
            reasons_sell.append(
                "MACD يتحسن هبوطيًا"
            )
        else:
            warnings.append(
                "MACD سلبي لكن الزخم يتباطأ"
            )

    # =====================================================
    # VOLUME
    # =====================================================

    if volume_ratio >= 1.20:

        if score_buy > score_sell:
            score_buy += 15
            reasons_buy.append(
                "Volume يؤكد الحركة"
            )

        elif score_sell > score_buy:
            score_sell += 15
            reasons_sell.append(
                "Volume يؤكد الحركة"
            )

    elif volume_ratio < 0.80:

        warnings.append(
            "Volume ضعيف"
        )

    else:

        warnings.append(
            "Volume متوسط"
        )

    # =====================================================
    # HIGHER TIMEFRAME FILTER
    # =====================================================

    if higher_trend:

        if higher_trend in [
            "BULLISH",
            "BULLISH_STRONG"
        ]:

            score_buy += 10

        elif higher_trend in [
            "BEARISH",
            "BEARISH_STRONG"
        ]:

            score_sell += 10

    # =====================================================
    # DIRECTION
    # =====================================================

    if score_buy > score_sell:
        direction = "BUY"
        score = score_buy

    elif score_sell > score_buy:
        direction = "SELL"
        score = score_sell

    else:
        direction = "WAIT"
        score = 0

    # =====================================================
    # ENTRY QUALITY
    # =====================================================

    extended_buy = (
        p > e20
        and (p - e20) > snapshot["atr"] * 0.60
    )

    extended_sell = (
        p < e20
        and (e20 - p) > snapshot["atr"] * 0.60
    )

    if direction == "BUY" and extended_buy:

        warnings.append(
            "السعر ممتد فوق EMA20 - انتظار تصحيح أفضل"
        )

    if direction == "SELL" and extended_sell:

        warnings.append(
            "السعر ممتد تحت EMA20 - انتظار ارتداد أفضل"
        )

    # =====================================================
    # FINAL SETUP
    # =====================================================

    setup = "WAIT"

    if direction == "BUY":

        if (
            score >= 70
            and rsi < 75
            and not extended_buy
        ):
            setup = "BUY"

        elif score >= 50:
            setup = "WATCH BUY"

    elif direction == "SELL":

        if (
            score >= 70
            and rsi > 25
            and not extended_sell
        ):
            setup = "SELL"

        elif score >= 50:
            setup = "WATCH SELL"

    # =====================================================
    # ENTRY / SL / TP
    # =====================================================

    entry = None
    stop_loss = None
    tp1 = None
    tp2 = None

    atr = snapshot["atr"]

    if setup in [
        "BUY",
        "WATCH BUY"
    ]:

        entry = p
        stop_loss = entry - (
            atr * 1.20
        )

        risk = entry - stop_loss

        tp1 = entry + (
            risk * 1.20
        )

        tp2 = entry + (
            risk * 2.00
        )

    elif setup in [
        "SELL",
        "WATCH SELL"
    ]:

        entry = p
        stop_loss = entry + (
            atr * 1.20
        )

        risk = stop_loss - entry

        tp1 = entry - (
            risk * 1.20
        )

        tp2 = entry - (
            risk * 2.00
        )

    return {
        "setup": setup,
        "direction": direction,
        "score": min(score, 100),
        "warnings": warnings,
        "reasons_buy": reasons_buy,
        "reasons_sell": reasons_sell,
        "entry": entry,
        "stop_loss": stop_loss,
        "tp1": tp1,
        "tp2": tp2
    }


# =========================================================
# SUPPORT / RESISTANCE
# =========================================================

def calculate_levels(df, lookback=30):

    recent = df.tail(lookback)

    support = recent["low"].min()
    resistance = recent["high"].max()

    return (
        float(support),
        float(resistance)
    )


# =========================================================
# /START
# =========================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(

        "🤖 XAU Smart Bot v4\n\n"

        "🟢 البوت يعمل بنجاح!\n\n"

        "الأوامر:\n"

        "💰 /price - اختبار البيانات\n"

        "📅 /weekly - التحليل الأسبوعي\n"

        "📊 /daily - التحليل اليومي\n"

        "⚡ /scalp - التحليل اللحظي\n"

        "🟢 /status - حالة البوت"

    )


# =========================================================
# /STATUS
# =========================================================

async def status(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(

        "🟢 XAU Smart Bot v4 يعمل\n"

        "📈 السوق: XAUUSD\n"

        "🤖 النظام: Multi-Timeframe Analysis\n"

        "🎯 Entry Filter: ON\n"

        "🛡️ ATR Risk Filter: ON\n"

        "📊 Weekly: ON\n"

        "⚡ Scalp: ON\n"

        "⏳ الحالة: اختبار مباشر"

    )


# =========================================================
# /PRICE
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

        current_price = fetch_current_price()

        results.append(
            f"💰 LIVE PRICE: {current_price:.2f}"
        )

        for interval in intervals:

            try:

                df = fetch_ohlc(
                    interval,
                    5,
                    closed_only=False
                )

                last = df.iloc[-1]

                results.append(

                    f"\n✅ {interval}\n"

                    f"Open: {last['open']:.2f}\n"

                    f"High: {last['high']:.2f}\n"

                    f"Low: {last['low']:.2f}\n"

                    f"Close: {last['close']:.2f}\n"

                    f"Tick Volume: "
                    f"{last['tickVolume']:.0f}"

                )

            except Exception as e:

                results.append(
                    f"\n❌ {interval}: {str(e)}"
                )

        # Weekly test is built from D1
        try:

            daily_df = fetch_ohlc(
                "1d",
                20,
                closed_only=True
            )

            weekly_df = build_weekly_from_daily(
                daily_df
            )

            if len(weekly_df) > 0:

                last_week = weekly_df.iloc[-1]

                results.append(

                    "\n📅 W1 — مبني من D1\n"

                    f"Open: "
                    f"{last_week['open']:.2f}\n"

                    f"High: "
                    f"{last_week['high']:.2f}\n"

                    f"Low: "
                    f"{last_week['low']:.2f}\n"

                    f"Close: "
                    f"{last_week['close']:.2f}"

                )

        except Exception as e:

            results.append(
                f"\n❌ W1: {str(e)}"
            )

        message = (

            "🥇 XAUUSD DATA TEST v4\n\n"

            + "\n".join(results)

            + "\n\n"

            "🟢 إذا ظهرت الفريمات بنجاح "
            "فمصدر البيانات يعمل."

        )

        await update.message.reply_text(
            message
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ خطأ في اختبار البيانات:\n\n"
            f"{str(e)}"
        )


# =========================================================
# /WEEKLY
# =========================================================

async def weekly(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        daily_df = fetch_ohlc(
            "1d",
            1000,
            closed_only=True
        )

        weekly_df = build_weekly_from_daily(
            daily_df
        )

        if len(weekly_df) < 220:

            await update.message.reply_text(
                "⚠️ بيانات W1 غير كافية لحساب EMA200."
            )

            return

        # Last completed weekly candle
        weekly_snapshot = calculate_snapshot(
            weekly_df
        )

        daily_snapshot = calculate_snapshot(
            daily_df
        )

        h4_df = fetch_ohlc(
            "4h",
            300,
            closed_only=True
        )

        h4_snapshot = calculate_snapshot(
            h4_df
        )

        w1_trend = get_trend(
            weekly_snapshot
        )

        d1_trend = get_trend(
            daily_snapshot
        )

        h4_trend = get_trend(
            h4_snapshot
        )

        support, resistance = calculate_levels(
            daily_df,
            50
        )

        weekly_signal = entry_engine(
            weekly_snapshot,
            higher_trend=d1_trend
        )

        message = (

            "🤖 XAU SMART BOT v4\n\n"

            "📅 WEEKLY ANALYSIS\n\n"

            "━━━━━━━━━━━━━━\n"

            "📊 W1\n"

            f"💰 Price: "
            f"{weekly_snapshot['price']:.2f}\n"

            f"EMA20: "
            f"{weekly_snapshot['ema20']:.2f}\n"

            f"EMA50: "
            f"{weekly_snapshot['ema50']:.2f}\n"

            f"EMA200: "
            f"{weekly_snapshot['ema200']:.2f}\n"

            f"RSI: "
            f"{weekly_snapshot['rsi']:.2f}\n"

            f"MACD: "
            f"{weekly_snapshot['macd']:.4f}\n"

            f"Signal: "
            f"{weekly_snapshot['signal']:.4f}\n"

            f"ATR: "
            f"{weekly_snapshot['atr']:.2f}\n"

            f"Volume Ratio: "
            f"{weekly_snapshot['volume_ratio']:.2f}x\n"

            f"Trend: "
            f"{trend_text(w1_trend)}\n\n"

            "📊 D1\n"

            f"Trend: "
            f"{trend_text(d1_trend)}\n"

            f"RSI: "
            f"{daily_snapshot['rsi']:.2f}\n"

            f"Price: "
            f"{daily_snapshot['price']:.2f}\n\n"

            "📊 H4\n"

            f"Trend: "
            f"{trend_text(h4_trend)}\n"

            f"RSI: "
            f"{h4_snapshot['rsi']:.2f}\n"

            f"Price: "
            f"{h4_snapshot['price']:.2f}\n\n"

            "🧱 LEVELS\n"

            f"Support: {support:.2f}\n"

            f"Resistance: {resistance:.2f}\n\n"

            "━━━━━━━━━━━━━━\n"

            f"🎯 WEEKLY SIGNAL: "
            f"{weekly_signal['setup']}\n"

            f"📊 Direction: "
            f"{weekly_signal['direction']}\n"

            f"💪 Score: "
            f"{weekly_signal['score']}%\n"

            "━━━━━━━━━━━━━━\n\n"

            "⚠️ هذا تحليل اتجاهي وليس ضمانًا للربح."

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
            "❌ خطأ في التحليل الأسبوعي:\n\n"
            f"{str(e)}"
        )


# =========================================================
# FORMAT TRADE
# =========================================================

def trade_text(result):

    if result["entry"] is None:

        return (
            "🎯 لا توجد صفقة جاهزة الآن."
        )

    return (

        f"🎯 Entry: "
        f"{result['entry']:.2f}\n"

        f"🛑 SL: "
        f"{result['stop_loss']:.2f}\n"

        f"🎯 TP1: "
        f"{result['tp1']:.2f}\n"

        f"🎯 TP2: "
        f"{result['tp2']:.2f}"

    )


# =========================================================
# /DAILY
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

        snapshots = {}
        results = []

        for interval in intervals:

            df = fetch_ohlc(
                interval,
                300,
                closed_only=True
            )

            snapshot = calculate_snapshot(
                df
            )

            snapshots[interval] = snapshot

        d1_trend = get_trend(
            snapshots["1d"]
        )

        h4_trend = get_trend(
            snapshots["4h"]
        )

        h1_trend = get_trend(
            snapshots["1h"]
        )

        directions = []

        weighted_direction_score = 0

        for interval in intervals:

            higher = None

            if interval == "4h":
                higher = d1_trend

            elif interval == "1h":
                higher = h4_trend

            result = entry_engine(
                snapshots[interval],
                higher_trend=higher
            )

            snapshots[interval]["result"] = result

            if result["direction"] == "BUY":
                directional = result["score"]

            elif result["direction"] == "SELL":
                directional = -result["score"]

            else:
                directional = 0

            weighted_direction_score += (
                directional
                * weights[interval]
            )

            directions.append(
                result["direction"]
            )

        # =====================================================
        # AGREEMENT
        # =====================================================

        buy_count = directions.count(
            "BUY"
        )

        sell_count = directions.count(
            "SELL"
        )

        if buy_count > sell_count:
            final_direction = "BUY"

        elif sell_count > buy_count:
            final_direction = "SELL"

        else:
            final_direction = "WAIT"

        final_confidence = min(
            abs(weighted_direction_score),
            100
        )

        # =====================================================
        # FINAL ENTRY FILTER
        # =====================================================

        h1_result = snapshots[
            "1h"
        ]["result"]

        h4_result = snapshots[
            "4h"
        ]["result"]

        d1_result = snapshots[
            "1d"
        ]["result"]

        final_setup = "WAIT"

        if (
            final_direction == "BUY"
            and buy_count >= 2
            and h1_result["setup"] == "BUY"
            and h4_result["direction"] == "BUY"
        ):
            final_setup = "BUY"

        elif (
            final_direction == "SELL"
            and sell_count >= 2
            and h1_result["setup"] == "SELL"
            and h4_result["direction"] == "SELL"
        ):
            final_setup = "SELL"

        elif final_direction == "BUY":

            final_setup = "WATCH BUY"

        elif final_direction == "SELL":

            final_setup = "WATCH SELL"

        # =====================================================
        # DISPLAY EACH TIMEFRAME
        # =====================================================

        for interval in intervals:

            s = snapshots[
                interval
            ]

            r = s["result"]

            warning_text = (
                " | ".join(
                    r["warnings"]
                )
                if r["warnings"]
                else "لا توجد تحذيرات"
            )

            results.append(

                f"📊 {names[interval]}\n"

                f"💰 Price: "
                f"{s['price']:.2f}\n"

                f"📈 EMA20: "
                f"{s['ema20']:.2f}\n"

                f"📈 EMA50: "
                f"{s['ema50']:.2f}\n"

                f"📈 EMA200: "
                f"{s['ema200']:.2f}\n"

                f"RSI: "
                f"{s['rsi']:.2f}\n"

                f"MACD: "
                f"{s['macd']:.4f}\n"

                f"Signal: "
                f"{s['signal']:.4f}\n"

                f"Histogram: "
                f"{s['histogram']:.4f}\n"

                f"ATR: "
                f"{s['atr']:.2f}\n"

                f"Volume: "
                f"{s['volume']:.0f}\n"

                f"Volume Ratio: "
                f"{s['volume_ratio']:.2f}x\n"

                f"Trend: "
                f"{trend_text(get_trend(s))}\n"

                f"Signal: "
                f"{r['setup']}\n"

                f"Direction: "
                f"{r['direction']}\n"

                f"Score: "
                f"{r['score']}%\n"

                f"⚠️ {warning_text}"

            )

        # =====================================================
        # TRADE
        # =====================================================

        if final_setup in [
            "BUY",
            "SELL"
        ]:

            trade_result = (
                h1_result
            )

        elif final_setup in [
            "WATCH BUY",
            "WATCH SELL"
        ]:

            trade_result = (
                h1_result
            )

        else:

            trade_result = {
                "entry": None,
                "stop_loss": None,
                "tp1": None,
                "tp2": None
            }

        # =====================================================
        # FINAL MESSAGE
        # =====================================================

        agreement = (
            f"🟢 BUY: {buy_count}/3\n"
            f"🔴 SELL: {sell_count}/3"
        )

        message = (

            "🤖 XAU SMART BOT v4\n\n"

            "📊 DAILY ANALYSIS\n\n"

            + "\n\n".join(results)

            + "\n\n"

            "━━━━━━━━━━━━━━\n"

            f"🎯 FINAL SIGNAL: "
            f"{final_setup}\n"

            f"💪 CONFIDENCE: "
            f"{final_confidence:.0f}%\n\n"

            f"📊 AGREEMENT:\n"
            f"{agreement}\n\n"

            "━━━━━━━━━━━━━━\n"

            f"{trade_text(trade_result)}\n"

            "━━━━━━━━━━━━━━\n\n"

            "⚠️ Confidence = درجة توافق المؤشرات "
            "وليست احتمال ربح.\n"

            "⚠️ Entry/SL/TP تقديرات آلية تعتمد على ATR "
            "وليست ضمانًا للنتيجة."

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
            "❌ خطأ في التحليل اليومي:\n\n"
            f"{str(e)}"
        )


# =========================================================
# /SCALP
# =========================================================

async def scalp(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        # -----------------------------------------------------
        # H1
        # -----------------------------------------------------

        h1_df = fetch_ohlc(
            "1h",
            300,
            closed_only=True
        )

        h1 = calculate_snapshot(
            h1_df,
            ema_fast=9,
            ema_mid=20,
            ema_slow=50,
            rsi_period=9,
            macd_fast=5,
            macd_slow=13,
            macd_signal=4
        )

        h1_trend = get_trend(
            {
                "price": h1["price"],
                "ema20": h1["ema20"],
                "ema50": h1["ema50"],
                "ema200": h1["ema200"]
            }
        )

        # -----------------------------------------------------
        # M15
        # -----------------------------------------------------

        m15_df = fetch_ohlc(
            "15m",
            300,
            closed_only=True
        )

        m15 = calculate_snapshot(
            m15_df,
            ema_fast=9,
            ema_mid=20,
            ema_slow=50,
            rsi_period=9,
            macd_fast=5,
            macd_slow=13,
            macd_signal=4
        )

        # -----------------------------------------------------
        # M5
        # -----------------------------------------------------

        m5_df = fetch_ohlc(
            "5m",
            300,
            closed_only=True
        )

        m5 = calculate_snapshot(
            m5_df,
            ema_fast=9,
            ema_mid=20,
            ema_slow=50,
            rsi_period=9,
            macd_fast=5,
            macd_slow=13,
            macd_signal=4
        )

        # -----------------------------------------------------
        # ENGINE
        # -----------------------------------------------------

        h1_result = entry_engine(
            h1,
            higher_trend=None,
            scalp=True
        )

        m15_result = entry_engine(
            m15,
            higher_trend=h1_trend,
            scalp=True
        )

        m5_result = entry_engine(
            m5,
            higher_trend=get_trend(
                m15
            ),
            scalp=True
        )

        # -----------------------------------------------------
        # SCALP DIRECTION
        # -----------------------------------------------------

        if (
            h1_result["direction"] == "BUY"
            and m15_result["direction"] == "BUY"
            and m5_result["direction"] == "BUY"
        ):

            final_direction = "BUY"

        elif (
            h1_result["direction"] == "SELL"
            and m15_result["direction"] == "SELL"
            and m5_result["direction"] == "SELL"
        ):

            final_direction = "SELL"

        else:

            final_direction = "WAIT"

        # -----------------------------------------------------
        # FINAL SCALP SETUP
        # -----------------------------------------------------

        if (
            final_direction == "BUY"
            and m5_result["setup"] == "BUY"
        ):

            final_setup = "🟢 BUY"

        elif (
            final_direction == "SELL"
            and m5_result["setup"] == "SELL"
        ):

            final_setup = "🔴 SELL"

        elif final_direction == "BUY":

            final_setup = "🟡 WATCH BUY"

        elif final_direction == "SELL":

            final_setup = "🟡 WATCH SELL"

        else:

            final_setup = "🟡 WAIT"

        # -----------------------------------------------------
        # CONFIDENCE
        # -----------------------------------------------------

        average_score = (
            h1_result["score"]
            + m15_result["score"]
            + m5_result["score"]
        ) / 3

        # -----------------------------------------------------
        # TRADE
        # -----------------------------------------------------

        trade_result = m5_result

        if final_setup == "🟡 WAIT":

            trade_result = {
                "entry": None,
                "stop_loss": None,
                "tp1": None,
                "tp2": None
            }

        # -----------------------------------------------------
        # WARNINGS
        # -----------------------------------------------------

        warnings = (
            m5_result["warnings"]
            + m15_result["warnings"]
            + h1_result["warnings"]
        )

        unique_warnings = list(
            dict.fromkeys(
                warnings
            )
        )

        warning_text = (
            " | ".join(
                unique_warnings
            )
            if unique_warnings
            else "لا توجد تحذيرات"
        )

        # -----------------------------------------------------
        # MESSAGE
        # -----------------------------------------------------

        message = (

            "🤖 XAU SMART BOT v4\n\n"

            "⚡ SCALP ANALYSIS\n\n"

            "━━━━━━━━━━━━━━\n"

            "📊 H1\n"

            f"Price: {h1['price']:.2f}\n"

            f"EMA9: {h1['ema20']:.2f}\n"

            f"EMA20: {h1['ema50']:.2f}\n"

            f"EMA50: {h1['ema200']:.2f}\n"

            f"RSI9: {h1['rsi']:.2f}\n"

            f"MACD: {h1['macd']:.4f}\n"

            f"Signal: {h1['signal']:.4f}\n"

            f"Trend: {trend_text(h1_trend)}\n"

            f"Direction: {h1_result['direction']}\n"

            f"Score: {h1_result['score']}%\n\n"

            "📊 M15\n"

            f"Price: {m15['price']:.2f}\n"

            f"RSI9: {m15['rsi']:.2f}\n"

            f"MACD: {m15['macd']:.4f}\n"

            f"Signal: {m15['signal']:.4f}\n"

            f"Volume Ratio: "
            f"{m15['volume_ratio']:.2f}x\n"

            f"Direction: {m15_result['direction']}\n"

            f"Score: {m15_result['score']}%\n\n"

            "📊 M5\n"

            f"Price: {m5['price']:.2f}\n"

            f"RSI9: {m5['rsi']:.2f}\n"

            f"MACD: {m5['macd']:.4f}\n"

            f"Signal: {m5['signal']:.4f}\n"

            f"ATR: {m5['atr']:.2f}\n"

            f"Volume Ratio: "
            f"{m5['volume_ratio']:.2f}x\n"

            f"Direction: {m5_result['direction']}\n"

            f"Score: {m5_result['score']}%\n\n"

            "━━━━━━━━━━━━━━\n"

            f"🎯 FINAL SCALP: "
            f"{final_setup}\n"

            f"💪 CONFIDENCE: "
            f"{average_score:.0f}%\n\n"

            f"{trade_text(trade_result)}\n\n"

            f"⚠️ {warning_text}\n"

            "━━━━━━━━━━━━━━\n"

            "⚠️ التحليل اللحظي لا يضمن الربح."

        )

        await update.message.reply_text(
            message
        )

    except requests.exceptions.Timeout:

        await update.message.reply_text(
            "⏳ انتهت مهلة الاتصال ببيانات Scalp."
        )

    except Exception as e:

        await update.message.reply_text(
            "❌ خطأ في تحليل Scalp:\n\n"
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
# FLASK
# =========================================================

@app.route("/")
def home():

    return "XAU Smart Bot v4 is running!"


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
