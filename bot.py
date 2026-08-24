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
API_BASE = "https://biquote.io/api/XAUUSD/ohlc"

app = Flask(__name__)


# =========================================================
# GENERAL HELPERS
# =========================================================

def safe_float(value, default=np.nan):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def get_bars(interval, limit=250):
    url = f"{API_BASE}?interval={interval}&limit={limit}"

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

    return bars


def bars_to_dataframe(bars):
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
                f"الحقل {column} غير موجود في البيانات"
            )

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        )

    if "openTime" in df.columns:
        df = df.sort_values("openTime")

    df = df.dropna(
        subset=required
    ).reset_index(drop=True)

    return df


def volume_ratio(df, periods=20):
    if len(df) < 2:
        return 0.0

    current = safe_float(
        df["tickVolume"].iloc[-1],
        0
    )

    previous = df["tickVolume"].iloc[:-1].tail(
        periods
    )

    if previous.empty:
        return 0.0

    average = previous.mean()

    if average <= 0:
        return 0.0

    return current / average


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

    histogram = macd - signal_line

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
# SIGNAL ENGINE - DAILY / WEEKLY
# =========================================================

def calculate_signal_score(
    price,
    ema20,
    ema50,
    ema200,
    rsi,
    macd,
    signal,
    vol_ratio
):
    bullish = 0
    bearish = 0
    warnings = []

    # -----------------------------------------------------
    # EMA TREND
    # -----------------------------------------------------

    if (
        not np.isnan(ema200)
        and price > ema20 > ema50 > ema200
    ):
        bullish += 40

    elif (
        not np.isnan(ema200)
        and price < ema20 < ema50 < ema200
    ):
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
    # RSI
    # -----------------------------------------------------

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
        else:
            warnings.append(
                "⚠️ RSI تشبع شرائي"
            )

    elif rsi <= 30:

        if bearish >= bullish:
            bearish += 5

            warnings.append(
                "⚠️ RSI منخفض - احتمال ارتداد"
            )
        else:
            warnings.append(
                "⚠️ RSI تشبع بيعي"
            )

    # -----------------------------------------------------
    # MACD
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
    # VOLUME
    # -----------------------------------------------------

    if vol_ratio >= 1.20:

        if bullish > bearish:
            bullish += 20

        elif bearish > bullish:
            bearish += 20

    elif vol_ratio < 0.80:

        warnings.append(
            "ℹ️ Volume ضعيف"
        )

    else:

        warnings.append(
            "ℹ️ Volume لا يؤكد الحركة بقوة"
        )

    # -----------------------------------------------------
    # FINAL DIRECTION
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

    score = max(
        0,
        min(
            int(score),
            100
        )
    )

    if score >= 65:

        signal = direction

    elif score >= 50:

        signal = f"WATCH {direction}"

    else:

        signal = "WAIT"

    return (
        signal,
        direction,
        score,
        warnings
    )


# =========================================================
# TREND
# =========================================================

def get_trend(
    price,
    ema20,
    ema50,
    ema200=np.nan
):
    if (
        not np.isnan(ema200)
        and price > ema20 > ema50 > ema200
    ):
        return "🟢 صاعد قوي"

    if price > ema20 > ema50:
        return "🟢 صاعد"

    if (
        not np.isnan(ema200)
        and price < ema20 < ema50 < ema200
    ):
        return "🔴 هابط قوي"

    if price < ema20 < ema50:
        return "🔴 هابط"

    return "🟡 متذبذب"


# =========================================================
# ANALYZE STANDARD TIMEFRAME
# =========================================================

def analyze_standard_dataframe(
    df,
    use_ema200=True
):
    close = df["close"]

    price = close.iloc[-1]

    ema20 = calculate_ema(
        close,
        20
    ).iloc[-1]

    ema50 = calculate_ema(
        close,
        50
    ).iloc[-1]

    ema200 = np.nan

    ema_warning = None

    if use_ema200:

        if len(df) >= 200:

            ema200 = calculate_ema(
                close,
                200
            ).iloc[-1]

        else:

            ema_warning = (
                f"ℹ️ EMA200 غير مستخدم: "
                f"المتاح {len(df)} شمعة فقط"
            )

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

    atr = calculate_atr(
        df["high"],
        df["low"],
        close,
        14
    ).iloc[-1]

    vol_ratio = volume_ratio(
        df,
        20
    )

    signal_name, direction, score, warnings = (
        calculate_signal_score(
            price,
            ema20,
            ema50,
            ema200,
            rsi,
            macd_value,
            signal_value,
            vol_ratio
        )
    )

    if ema_warning:
        warnings.insert(
            0,
            ema_warning
        )

    trend = get_trend(
        price,
        ema20,
        ema50,
        ema200
    )

    return {
        "price": price,
        "ema20": ema20,
        "ema50": ema50,
        "ema200": ema200,
        "rsi": rsi,
        "macd": macd_value,
        "signal": signal_value,
        "atr": atr,
        "volume_ratio": vol_ratio,
        "trend": trend,
        "signal": signal_name,
        "direction": direction,
        "score": score,
        "warnings": warnings,
        "bars": len(df)
    }


# =========================================================
# WEEKLY DATA BUILDER
# =========================================================

def build_weekly_from_daily(daily_df):
    df = daily_df.copy()

    if "openTime" not in df.columns:
        raise ValueError(
            "openTime غير موجود لبناء W1"
        )

    raw_time = df["openTime"]

    try:

        numeric_time = pd.to_numeric(
            raw_time,
            errors="coerce"
        )

        if (
            numeric_time.notna().any()
            and numeric_time.dropna().iloc[0] > 10_000_000_000
        ):
            dates = pd.to_datetime(
                numeric_time,
                unit="ms",
                utc=True
            )
        else:

            dates = pd.to_datetime(
                numeric_time,
                unit="s",
                utc=True
            )

    except Exception:

        dates = pd.to_datetime(
            raw_time,
            errors="coerce",
            utc=True
        )

    df["date"] = dates

    df = df.dropna(
        subset=["date"]
    )

    if df.empty:
        raise ValueError(
            "تعذر قراءة تواريخ D1"
        )

    df = df.set_index("date")

    weekly = df.resample(
        "W-SUN"
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
    ).reset_index()

    weekly["openTime"] = (
        weekly["date"].astype("int64")
        // 10**6
    )

    return weekly


# =========================================================
# AGREEMENT
# =========================================================

def calculate_agreement(analyses):
    buy = sum(
        1
        for item in analyses
        if item["direction"] == "BUY"
    )

    sell = sum(
        1
        for item in analyses
        if item["direction"] == "SELL"
    )

    return buy, sell


def final_mtf_result(
    analyses,
    weights
):
    weighted = 0
    total_weight = 0

    for item in analyses:

        key = item["interval"]

        if key not in weights:
            continue

        direction = item["direction"]
        score = item["score"]
        weight = weights[key]

        if direction == "BUY":

            weighted += score * weight

        elif direction == "SELL":

            weighted -= score * weight

        total_weight += weight

    if total_weight <= 0:

        return (
            "🟡 WAIT",
            0
        )

    final_score = (
        weighted / total_weight
    )

    confidence = int(
        min(
            abs(final_score),
            100
        )
    )

    buy_count, sell_count = (
        calculate_agreement(
            analyses
        )
    )

    if (
        buy_count >= 2
        and final_score >= 65
    ):

        final_signal = "🟢 BUY"

    elif (
        sell_count >= 2
        and final_score <= -65
    ):

        final_signal = "🔴 SELL"

    elif final_score > 0:

        final_signal = "🟡 WATCH BUY"

    elif final_score < 0:

        final_signal = "🟡 WATCH SELL"

    else:

        final_signal = "🟡 WAIT"

    return (
        final_signal,
        confidence
    )


# =========================================================
# ENTRY ENGINE
# =========================================================

def calculate_scalp_entry(
    h1,
    m15,
    m5
):
    warnings = []

    all_buy = all(
        x["direction"] == "BUY"
        for x in [
            h1,
            m15,
            m5
        ]
    )

    all_sell = all(
        x["direction"] == "SELL"
        for x in [
            h1,
            m15,
            m5
        ]
    )

    # -----------------------------------------------------
    # BUY SETUP
    # -----------------------------------------------------

    if all_buy:

        price = m5["price"]
        ema20 = m5["ema20"]
        atr = m5["atr"]

        extension = (
            price - ema20
        )

        extension_limit = (
            atr * 0.80
        )

        if extension > extension_limit:

            warnings.append(
                "⚠️ السعر ممتد فوق EMA20"
            )

        if m15["rsi"] >= 70:

            warnings.append(
                "⚠️ RSI M15 مرتفع - لا نطارد السعر"
            )

        if m5["rsi"] >= 75:

            warnings.append(
                "⚠️ RSI M5 في تشبع شرائي"
            )

        if m5["macd"] <= m5["signal"]:

            warnings.append(
                "⚠️ MACD M5 لا يؤكد الدخول"
            )

        if m5["volume_ratio"] < 1.00:

            warnings.append(
                "⚠️ Volume M5 ضعيف"
            )

        # Entry requires confirmation.
        entry_ready = (
            extension <= extension_limit
            and m5["macd"] > m5["signal"]
            and m5["volume_ratio"] >= 1.00
            and m5["rsi"] < 75
        )

        if entry_ready:

            entry = price

            sl = (
                entry - (
                    atr * 1.20
                )
            )

            tp1 = (
                entry + (
                    atr * 1.50
                )
            )

            tp2 = (
                entry + (
                    atr * 2.25
                )
            )

            return {
                "decision": "BUY",
                "entry": entry,
                "sl": sl,
                "tp1": tp1,
                "tp2": tp2,
                "warnings": warnings
            }

        return {
            "decision": "WAIT BUY",
            "entry": None,
            "sl": None,
            "tp1": None,
            "tp2": None,
            "warnings": warnings
        }

    # -----------------------------------------------------
    # SELL SETUP
    # -----------------------------------------------------

    if all_sell:

        price = m5["price"]
        ema20 = m5["ema20"]
        atr = m5["atr"]

        extension = (
            ema20 - price
        )

        extension_limit = (
            atr * 0.80
        )

        if extension > extension_limit:

            warnings.append(
                "⚠️ السعر ممتد تحت EMA20"
            )

        if m15["rsi"] <= 30:

            warnings.append(
                "⚠️ RSI M15 منخفض - لا نطارد الهبوط"
            )

        if m5["rsi"] <= 25:

            warnings.append(
                "⚠️ RSI M5 في تشبع بيعي"
            )

        if m5["macd"] >= m5["signal"]:

            warnings.append(
                "⚠️ MACD M5 لا يؤكد الدخول"
            )

        if m5["volume_ratio"] < 1.00:

            warnings.append(
                "⚠️ Volume M5 ضعيف"
            )

        entry_ready = (
            extension <= extension_limit
            and m5["macd"] < m5["signal"]
            and m5["volume_ratio"] >= 1.00
            and m5["rsi"] > 25
        )

        if entry_ready:

            entry = price

            sl = (
                entry + (
                    atr * 1.20
                )
            )

            tp1 = (
                entry - (
                    atr * 1.50
                )
            )

            tp2 = (
                entry - (
                    atr * 2.25
                )
            )

            return {
                "decision": "SELL",
                "entry": entry,
                "sl": sl,
                "tp1": tp1,
                "tp2": tp2,
                "warnings": warnings
            }

        return {
            "decision": "WAIT SELL",
            "entry": None,
            "sl": None,
            "tp1": None,
            "tp2": None,
            "warnings": warnings
        }

    # -----------------------------------------------------
    # MIXED DIRECTION
    # -----------------------------------------------------

    warnings.append(
        "⚠️ الفريمات غير متفقة بالكامل"
    )

    return {
        "decision": "WAIT",
        "entry": None,
        "sl": None,
        "tp1": None,
        "tp2": None,
        "warnings": warnings
    }


# =========================================================
# /START
# =========================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):
    message = (
        "🤖 XAU SMART BOT v7\n\n"
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
        "💰 الأدوات\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        "💰 /price\n"
        "بيانات الذهب الحالية\n\n"

        "🟢 /status\n"
        "حالة البوت\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "🎯 اختر التحليل الذي تريده."
    )

    await update.message.reply_text(
        message
    )


# =========================================================
# /STATUS
# =========================================================

async def status(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):
    await update.message.reply_text(
        "🟢 XAU Smart Bot v7 يعمل\n\n"
        "📈 السوق: XAUUSD\n"
        "🤖 النظام: Multi-Timeframe Analysis\n"
        "📅 Weekly: ON\n"
        "📊 Daily: ON\n"
        "⚡ Scalp: ON\n"
        "🎯 Entry Filter: ON\n"
        "🛡️ ATR Risk Filter: ON\n"
        "🔒 المؤشرات التفصيلية: مخفية\n"
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

        names = {
            "5m": "M5",
            "15m": "M15",
            "1h": "H1",
            "4h": "H4",
            "1d": "D1"
        }

        results = []

        for interval in intervals:

            try:

                bars = get_bars(
                    interval,
                    5
                )

                df = bars_to_dataframe(
                    bars
                )

                last = df.iloc[-1]

                results.append(
                    f"✅ {names[interval]}\n"
                    f"Open: {last['open']:.2f}\n"
                    f"High: {last['high']:.2f}\n"
                    f"Low: {last['low']:.2f}\n"
                    f"Close: {last['close']:.2f}\n"
                    f"Volume: {last['tickVolume']:.0f}"
                )

            except Exception as e:

                results.append(
                    f"❌ {names[interval]}\n"
                    f"تعذر الحصول على البيانات: {e}"
                )

        # -------------------------------------------------
        # WEEKLY FROM D1
        # -------------------------------------------------

        try:

            daily_bars = get_bars(
                "1d",
                250
            )

            daily_df = bars_to_dataframe(
                daily_bars
            )

            weekly_df = build_weekly_from_daily(
                daily_df
            )

            if not weekly_df.empty:

                last = weekly_df.iloc[-1]

                results.append(
                    "📅 W1 — مبني من D1\n"
                    f"Open: {last['open']:.2f}\n"
                    f"High: {last['high']:.2f}\n"
                    f"Low: {last['low']:.2f}\n"
                    f"Close: {last['close']:.2f}\n"
                    f"Volume: {last['tickVolume']:.0f}"
                )

        except Exception as e:

            results.append(
                f"⚠️ W1: {e}"
            )

        live_price = None

        try:

            bars = get_bars(
                "5m",
                2
            )

            df = bars_to_dataframe(
                bars
            )

            live_price = df["close"].iloc[-1]

        except Exception:
            pass

        header = (
            "🥇 XAUUSD DATA TEST v7\n\n"
        )

        if live_price is not None:

            header += (
                f"💰 LIVE PRICE: "
                f"{live_price:.2f}\n\n"
            )

        message = (
            header
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
            "⏳ انتهت مهلة الاتصال بمصدر البيانات."
        )

    except Exception as e:

        await update.message.reply_text(
            f"❌ خطأ في اختبار البيانات:\n{e}"
        )


# =========================================================
# WEEKLY ANALYSIS
# =========================================================

async def weekly(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):
    try:

        daily_bars = get_bars(
            "1d",
            250
        )

        daily_df = bars_to_dataframe(
            daily_bars
        )

        weekly_df = build_weekly_from_daily(
            daily_df
        )

        if len(weekly_df) < 20:

            await update.message.reply_text(
                "❌ بيانات W1 غير كافية "
                "حتى لحساب EMA20."
            )

            return

        # W1
        w1 = analyze_standard_dataframe(
            weekly_df,
            use_ema200=True
        )

        # D1
        d1 = analyze_standard_dataframe(
            daily_df,
            use_ema200=True
        )

        analyses = [
            {
                **w1,
                "interval": "w1"
            },
            {
                **d1,
                "interval": "d1"
            }
        ]

        final_signal, confidence = (
            final_mtf_result(
                analyses,
                {
                    "w1": 0.60,
                    "d1": 0.40
                }
            )
        )

        buy_count, sell_count = (
            calculate_agreement(
                analyses
            )
        )

        # -------------------------------------------------
        # DISPLAY ONLY RESULT
        # -------------------------------------------------

        def compact(item, name):

            warning_text = ""

            if item["warnings"]:

                warning_text = (
                    "\n"
                    + " | ".join(
                        item["warnings"]
                    )
                )

            return (
                f"📊 {name}\n"
                f"Trend: {item['trend']}\n"
                f"Direction: {item['direction']}\n"
                f"Score: {item['score']}%"
                f"{warning_text}"
            )

        message = (
            "🤖 XAU SMART BOT v7\n"
            "📅 WEEKLY ANALYSIS\n\n"

            + compact(
                w1,
                "W1"
            )

            + "\n\n"

            + compact(
                d1,
                "D1"
            )

            + "\n\n"
            "━━━━━━━━━━━━━━━━━━\n"

            f"🎯 FINAL WEEKLY: "
            f"{final_signal}\n"

            f"💪 CONFIDENCE: "
            f"{confidence}%\n"

            f"📊 AGREEMENT: "
            f"🟢 BUY {buy_count}/2 | "
            f"🔴 SELL {sell_count}/2\n"

            "━━━━━━━━━━━━━━━━━━\n\n"

            "⚠️ Confidence = درجة توافق "
            "المؤشرات وليست احتمال ربح."
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
            f"❌ خطأ في التحليل الأسبوعي:\n{e}"
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

        analyses = []

        for interval in intervals:

            bars = get_bars(
                interval,
                250
            )

            df = bars_to_dataframe(
                bars
            )

            if len(df) < 50:

                raise ValueError(
                    f"{names[interval]}: "
                    f"بيانات غير كافية"
                )

            result = analyze_standard_dataframe(
                df,
                use_ema200=True
            )

            result["interval"] = interval

            analyses.append(
                result
            )

        final_signal, confidence = (
            final_mtf_result(
                analyses,
                weights
            )
        )

        buy_count, sell_count = (
            calculate_agreement(
                analyses
            )
        )

        def compact(item):

            warning_text = ""

            if item["warnings"]:

                warning_text = (
                    "\n"
                    + " | ".join(
                        item["warnings"]
                    )
                )

            return (
                f"📊 {names[item['interval']]}\n"
                f"Trend: {item['trend']}\n"
                f"Direction: {item['direction']}\n"
                f"Score: {item['score']}%"
                f"{warning_text}"
            )

        blocks = [
            compact(item)
            for item in analyses
        ]

        message = (
            "🤖 XAU SMART BOT v7\n"
            "📊 DAILY ANALYSIS\n\n"

            + "\n\n".join(blocks)

            + "\n\n"
            "━━━━━━━━━━━━━━━━━━\n"

            f"🎯 FINAL SIGNAL: "
            f"{final_signal}\n"

            f"💪 CONFIDENCE: "
            f"{confidence}%\n"

            f"📊 AGREEMENT: "
            f"🟢 BUY: {buy_count}/3 | "
            f"🔴 SELL: {sell_count}/3\n"

            "━━━━━━━━━━━━━━━━━━\n\n"

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
            f"❌ خطأ في التحليل اليومي:\n{e}"
        )


# =========================================================
# SCALP ANALYSIS
# =========================================================

def analyze_scalp_dataframe(
    df
):
    close = df["close"]

    price = close.iloc[-1]

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

    rsi9 = calculate_rsi(
        close,
        9
    ).iloc[-1]

    macd, signal, histogram = (
        calculate_macd(
            close,
            5,
            13,
            4
        )
    )

    macd_value = macd.iloc[-1]
    signal_value = signal.iloc[-1]

    atr = calculate_atr(
        df["high"],
        df["low"],
        close,
        14
    ).iloc[-1]

    vol_ratio = volume_ratio(
        df,
        20
    )

    bullish = 0
    bearish = 0
    warnings = []

    # EMA
    if price > ema9 > ema20 > ema50:

        bullish += 40

    elif price < ema9 < ema20 < ema50:

        bearish += 40

    elif price > ema20 > ema50:

        bullish += 30

    elif price < ema20 < ema50:

        bearish += 30

    elif price > ema20:

        bullish += 20

    elif price < ema20:

        bearish += 20

    # RSI
    if 50 <= rsi9 < 70:

        bullish += 15

    elif 30 < rsi9 < 50:

        bearish += 15

    elif rsi9 >= 70:

        if bullish >= bearish:

            bullish += 5

            warnings.append(
                "⚠️ RSI مرتفع - لا نطارد السعر"
            )

    elif rsi9 <= 30:

        if bearish >= bullish:

            bearish += 5

            warnings.append(
                "⚠️ RSI منخفض - لا نطارد الهبوط"
            )

    # MACD
    if macd_value > signal_value:

        bullish += 25

    elif macd_value < signal_value:

        bearish += 25

    # Volume
    if vol_ratio >= 1.20:

        if bullish > bearish:

            bullish += 20

        elif bearish > bullish:

            bearish += 20

    elif vol_ratio < 0.80:

        warnings.append(
            "⚠️ Volume ضعيف"
        )

    else:

        warnings.append(
            "ℹ️ Volume لا يؤكد الحركة بقوة"
        )

    if bullish > bearish:

        direction = "BUY"
        score = bullish

    elif bearish > bullish:

        direction = "SELL"
        score = bearish

    else:

        direction = "WAIT"
        score = 0

    # Extension warning
    extension = abs(
        price - ema20
    )

    if extension > atr * 0.80:

        if price > ema20:

            warnings.append(
                "⚠️ السعر ممتد فوق EMA20"
            )

        else:

            warnings.append(
                "⚠️ السعر ممتد تحت EMA20"
            )

    return {
        "price": price,
        "ema9": ema9,
        "ema20": ema20,
        "ema50": ema50,
        "rsi9": rsi9,
        "macd": macd_value,
        "signal": signal_value,
        "atr": atr,
        "volume_ratio": vol_ratio,
        "direction": direction,
        "score": min(
            int(score),
            100
        ),
        "warnings": warnings,
        "trend": (
            "🟢 صاعد"
            if price > ema20 > ema50
            else
            "🔴 هابط"
            if price < ema20 < ema50
            else
            "🟡 متذبذب"
        )
    }


async def scalp(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):
    try:

        h1_df = bars_to_dataframe(
            get_bars(
                "1h",
                250
            )
        )

        m15_df = bars_to_dataframe(
            get_bars(
                "15m",
                250
            )
        )

        m5_df = bars_to_dataframe(
            get_bars(
                "5m",
                250
            )
        )

        h1 = analyze_scalp_dataframe(
            h1_df
        )

        m15 = analyze_scalp_dataframe(
            m15_df
        )

        m5 = analyze_scalp_dataframe(
            m5_df
        )

        h1["interval"] = "h1"
        m15["interval"] = "m15"
        m5["interval"] = "m5"

        analyses = [
            h1,
            m15,
            m5
        ]

        buy_count, sell_count = (
            calculate_agreement(
                analyses
            )
        )

        weighted_score = (
            h1["score"] * 0.40
            + m15["score"] * 0.35
            + m5["score"] * 0.25
        )

        if buy_count >= 2:

            final_direction = "BUY"

        elif sell_count >= 2:

            final_direction = "SELL"

        else:

            final_direction = "WAIT"

        confidence = int(
            min(
                weighted_score,
                100
            )
        )

        entry_data = calculate_scalp_entry(
            h1,
            m15,
            m5
        )

        # -------------------------------------------------
        # FINAL SCALP SIGNAL
        # -------------------------------------------------

        if (
            entry_data["decision"] == "BUY"
            and buy_count == 3
        ):

            final_signal = "🟢 BUY"

        elif (
            entry_data["decision"] == "SELL"
            and sell_count == 3
        ):

            final_signal = "🔴 SELL"

        elif final_direction == "BUY":

            final_signal = "🟡 WATCH BUY"

        elif final_direction == "SELL":

            final_signal = "🟡 WATCH SELL"

        else:

            final_signal = "🟡 WAIT"

        # -------------------------------------------------
        # WARNINGS
        # -------------------------------------------------

        all_warnings = []

        for item in analyses:

            all_warnings.extend(
                item["warnings"]
            )

        all_warnings.extend(
            entry_data["warnings"]
        )

        # Remove duplicates
        unique_warnings = []

        for warning in all_warnings:

            if warning not in unique_warnings:

                unique_warnings.append(
                    warning
                )

        warning_text = (
            " | ".join(
                unique_warnings
            )
            if unique_warnings
            else
            "لا توجد تحذيرات رئيسية"
        )

        # -------------------------------------------------
        # ENTRY DISPLAY
        # -------------------------------------------------

        if (
            entry_data["entry"] is not None
        ):

            entry_block = (
                f"🎯 Entry: "
                f"{entry_data['entry']:.2f}\n"

                f"🛑 SL: "
                f"{entry_data['sl']:.2f}\n"

                f"🎯 TP1: "
                f"{entry_data['tp1']:.2f}\n"

                f"🎯 TP2: "
                f"{entry_data['tp2']:.2f}"
            )

        else:

            entry_block = (
                "🚫 لا يوجد Entry حاليًا\n"
                "⏳ ننتظر تصحيحًا وتأكيدًا أفضل."
            )

        # -------------------------------------------------
        # FINAL MESSAGE
        # -------------------------------------------------

        message = (
            "🤖 XAU SMART BOT v7\n"
            "⚡ SCALP ANALYSIS\n\n"

            "📊 H1\n"
            f"Trend: {h1['trend']}\n"
            f"Direction: {h1['direction']}\n"
            f"Score: {h1['score']}%\n\n"

            "📊 M15\n"
            f"Trend: {m15['trend']}\n"
            f"Direction: {m15['direction']}\n"
            f"Score: {m15['score']}%\n\n"

            "📊 M5\n"
            f"Trend: {m5['trend']}\n"
            f"Direction: {m5['direction']}\n"
            f"Score: {m5['score']}%\n\n"

            "━━━━━━━━━━━━━━━━━━\n"

            f"🎯 FINAL SCALP: "
            f"{final_signal}\n"

            f"💪 CONFIDENCE: "
            f"{confidence}%\n"

            f"📊 AGREEMENT: "
            f"🟢 BUY: {buy_count}/3 | "
            f"🔴 SELL: {sell_count}/3\n\n"

            f"📌 Entry Decision: "
            f"{entry_data['decision']}\n"

            f"{entry_block}\n\n"

            "⚠️ "
            f"{warning_text}\n\n"

            "━━━━━━━━━━━━━━━━━━\n"
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
            f"❌ خطأ في التحليل اللحظي:\n{e}"
        )


# =========================================================
# TELEGRAM BOT
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

@app.route("/")
def home():

    return "XAU Smart Bot v7 is running!"


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
