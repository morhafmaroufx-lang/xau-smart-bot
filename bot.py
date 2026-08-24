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

SYMBOL = "XAUUSD"

EMA_FAST = 20
EMA_MID = 50
EMA_SLOW = 200

DAILY_WEIGHTS = {
    "1d": 0.40,
    "4h": 0.35,
    "1h": 0.25
}

SCALP_WEIGHTS = {
    "1h": 0.45,
    "15m": 0.30,
    "5m": 0.25
}


# =========================================================
# DATA HELPERS
# =========================================================

def get_bars(interval, limit=250):
    url = (
        f"https://biquote.io/api/{SYMBOL}/ohlc"
        f"?interval={interval}&limit={limit}"
    )

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
                f"البيانات ناقصة: {column}"
            )

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        )

    if "openTime" in df.columns:

        numeric_time = pd.to_numeric(
            df["openTime"],
            errors="coerce"
        )

        if numeric_time.notna().sum() > 0:

            max_value = numeric_time.max()

            if max_value > 100000000000:
                unit = "ms"

            elif max_value > 1000000000:
                unit = "s"

            else:
                unit = None

            if unit:

                df["datetime"] = pd.to_datetime(
                    numeric_time,
                    unit=unit,
                    errors="coerce"
                )

            else:

                df["datetime"] = pd.to_datetime(
                    df["openTime"],
                    errors="coerce"
                )

        else:

            df["datetime"] = pd.to_datetime(
                df["openTime"],
                errors="coerce"
            )

    else:

        df["datetime"] = pd.RangeIndex(
            start=0,
            stop=len(df)
        )

    df = df.dropna(
        subset=required
    )

    df = df.sort_values(
        "datetime"
    )

    df = df.drop_duplicates(
        subset="datetime",
        keep="last"
    )

    df = df.reset_index(
        drop=True
    )

    return df


# =========================================================
# WEEKLY BUILDER
# =========================================================

def build_weekly_from_daily(df_daily):

    if "datetime" not in df_daily.columns:
        return pd.DataFrame()

    temp = df_daily.copy()

    temp["datetime"] = pd.to_datetime(
        temp["datetime"],
        errors="coerce"
    )

    temp = temp.dropna(
        subset=["datetime"]
    )

    if temp.empty:
        return pd.DataFrame()

    temp = temp.set_index(
        "datetime"
    )

    weekly = temp.resample(
        "W-FRI"
    ).agg({

        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "tickVolume": "sum"

    })

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

    rs = (
        avg_gain
        /
        avg_loss.replace(
            0,
            np.nan
        )
    )

    rsi = 100 - (
        100 /
        (1 + rs)
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

    macd = (
        ema_fast -
        ema_slow
    )

    signal_line = calculate_ema(
        macd,
        signal
    )

    histogram = (
        macd -
        signal_line
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
        high -
        previous_close
    ).abs()

    tr3 = (
        low -
        previous_close
    ).abs()

    true_range = pd.concat(
        [
            tr1,
            tr2,
            tr3
        ],
        axis=1
    ).max(
        axis=1
    )

    atr = true_range.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    return atr


# =========================================================
# INDICATOR SNAPSHOT
# =========================================================

def calculate_indicators(
    df,
    scalp=False
):

    close = df["close"]

    if scalp:

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

    else:

        ema9 = None

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

    average_volume = (
        df["tickVolume"]
        .tail(20)
        .mean()
    )

    if average_volume > 0:

        volume_ratio = (
            volume /
            average_volume
        )

    else:

        volume_ratio = 0

    result = {

        "price": price,

        "ema9": ema9,

        "ema20": ema20,

        "ema50": ema50,

        "ema200": (
            ema200
            if not scalp
            else None
        ),

        "rsi": rsi,

        "macd": macd.iloc[-1],

        "signal": signal.iloc[-1],

        "histogram": histogram.iloc[-1],

        "atr": atr,

        "volume": volume,

        "average_volume": average_volume,

        "volume_ratio": volume_ratio,

        "bars": len(df)

    }

    return result


# =========================================================
# DAILY / WEEKLY SCORING
# =========================================================

def calculate_direction_score(
    ind
):

    bullish = 0
    bearish = 0
    warnings = []

    price = ind["price"]
    ema20 = ind["ema20"]
    ema50 = ind["ema50"]
    ema200 = ind["ema200"]
    rsi = ind["rsi"]
    macd = ind["macd"]
    signal = ind["signal"]
    volume_ratio = ind["volume_ratio"]

    # -----------------------------------------------------
    # EMA
    # -----------------------------------------------------

    if ema200 is not None:

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

    else:

        if price > ema20 > ema50:

            bullish += 35

        elif price < ema20 < ema50:

            bearish += 35

        elif price > ema20:

            bullish += 20

        elif price < ema20:

            bearish += 20

        warnings.append(
            "ℹ️ EMA200 غير مستخدم لعدم كفاية التاريخ"
        )

    # -----------------------------------------------------
    # RSI
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
    # RESULT
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
            score,
            100
        )
    )

    return (
        direction,
        round(score),
        warnings
    )


# =========================================================
# TREND DESCRIPTION
# =========================================================

def get_trend(
    ind
):

    price = ind["price"]
    ema20 = ind["ema20"]
    ema50 = ind["ema50"]
    ema200 = ind["ema200"]

    if ema200 is not None:

        if price > ema20 > ema50 > ema200:

            return "🟢 صاعد قوي"

        if price < ema20 < ema50 < ema200:

            return "🔴 هابط قوي"

    if price > ema20 > ema50:

        return "🟢 صاعد"

    if price < ema20 < ema50:

        return "🔴 هابط"

    return "🟡 متذبذب"


# =========================================================
# ENTRY FILTER
# =========================================================

def scalp_entry_filter(
    ind,
    direction
):

    warnings = []

    price = ind["price"]
    ema20 = ind["ema20"]
    rsi = ind["rsi"]
    atr = ind["atr"]
    volume_ratio = ind["volume_ratio"]
    macd = ind["macd"]
    signal = ind["signal"]

    if atr <= 0:

        return (
            False,
            "ATR غير صالح",
            warnings
        )

    distance = abs(
        price - ema20
    )

    atr_distance = (
        distance /
        atr
    )

    # -----------------------------------------------------
    # BUY
    # -----------------------------------------------------

    if direction == "BUY":

        if rsi >= 80:

            warnings.append(
                "⚠️ RSI في تشبع شرائي قوي"
            )

        elif rsi >= 72:

            warnings.append(
                "⚠️ RSI مرتفع - لا نطارد السعر"
            )

        if atr_distance > 0.80:

            warnings.append(
                "⚠️ السعر ممتد فوق EMA20"
            )

        if volume_ratio < 0.60:

            warnings.append(
                "⚠️ Volume ضعيف"
            )

        if macd <= signal:

            warnings.append(
                "⚠️ MACD لا يؤكد الدخول"
            )

        # STRICT ENTRY
        if (
            rsi < 72
            and atr_distance <= 0.80
            and volume_ratio >= 0.60
            and macd > signal
        ):

            return (
                True,
                "دخول BUY مقبول",
                warnings
            )

        return (
            False,
            "انتظار تصحيح وتأكيد أفضل",
            warnings
        )

    # -----------------------------------------------------
    # SELL
    # -----------------------------------------------------

    if direction == "SELL":

        if rsi <= 20:

            warnings.append(
                "⚠️ RSI في تشبع بيعي قوي"
            )

        elif rsi <= 28:

            warnings.append(
                "⚠️ RSI منخفض - لا نطارد الهبوط"
            )

        if atr_distance > 0.80:

            warnings.append(
                "⚠️ السعر ممتد تحت EMA20"
            )

        if volume_ratio < 0.60:

            warnings.append(
                "⚠️ Volume ضعيف"
            )

        if macd >= signal:

            warnings.append(
                "⚠️ MACD لا يؤكد الدخول"
            )

        if (
            rsi > 28
            and atr_distance <= 0.80
            and volume_ratio >= 0.60
            and macd < signal
        ):

            return (
                True,
                "دخول SELL مقبول",
                warnings
            )

        return (
            False,
            "انتظار ارتداد وتأكيد أفضل",
            warnings
        )

    return (
        False,
        "لا يوجد اتجاه واضح",
        warnings
    )


# =========================================================
# PRICE LEVELS
# =========================================================

def calculate_trade_levels(
    price,
    atr,
    direction
):

    if atr <= 0:

        return (
            None,
            None,
            None
        )

    if direction == "BUY":

        entry = price

        sl = price - (
            atr * 1.20
        )

        tp1 = price + (
            atr * 1.50
        )

        tp2 = price + (
            atr * 2.30
        )

    elif direction == "SELL":

        entry = price

        sl = price + (
            atr * 1.20
        )

        tp1 = price - (
            atr * 1.50
        )

        tp2 = price - (
            atr * 2.30
        )

    else:

        return (
            None,
            None,
            None
        )

    return (
        entry,
        sl,
        tp1,
        tp2
    )


# =========================================================
# FLASK
# =========================================================

@app.route("/")
def home():

    return "XAU Smart Bot v6 is running!"


# =========================================================
# START
# =========================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(

        "🤖 XAU Smart Bot v6\n\n"

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

        "🟢 XAU Smart Bot v6 يعمل\n"

        "📈 السوق: XAUUSD\n"

        "🤖 النظام: Multi-Timeframe Analysis\n"

        "🎯 Entry Filter: ON\n"

        "🛡️ ATR Risk Filter: ON\n"

        "📊 Weekly: ON\n"

        "⚡ Scalp: ON\n"

        "🧠 Anti-Chasing Filter: ON\n"

        "⏳ الحالة: اختبار مباشر"

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

        live_price = None

        for interval in intervals:

            try:

                bars = get_bars(
                    interval,
                    5
                )

                df = bars_to_dataframe(
                    bars
                )

                if df.empty:

                    raise ValueError(
                        "لا توجد شموع"
                    )

                last = df.iloc[-1]

                live_price = float(
                    last["close"]
                )

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
                    f"❌ {interval}: {str(e)}"
                )

        # -------------------------------------------------
        # W1
        # -------------------------------------------------

        try:

            daily_bars = get_bars(
                "1d",
                1000
            )

            daily_df = bars_to_dataframe(
                daily_bars
            )

            weekly_df = build_weekly_from_daily(
                daily_df
            )

            if not weekly_df.empty:

                last_week = weekly_df.iloc[-1]

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
                    f"{last_week['tickVolume']:.0f}\n"

                    f"Weeks available: "
                    f"{len(weekly_df)}"

                )

        except Exception as e:

            results.append(
                f"❌ W1: {str(e)}"
            )

        message = (

            "🥇 XAUUSD DATA TEST v6\n\n"

            + (
                f"💰 LIVE PRICE: "
                f"{live_price:.2f}\n\n"
                if live_price is not None
                else ""
            )

            + "\n\n".join(results)

            + "\n\n🟢 مصدر البيانات تم اختباره."

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
# WEEKLY
# =========================================================

async def weekly(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        # -------------------------------------------------
        # D1
        # -------------------------------------------------

        daily_bars = get_bars(
            "1d",
            1000
        )

        daily_df = bars_to_dataframe(
            daily_bars
        )

        if len(daily_df) < 50:

            raise ValueError(
                "بيانات D1 غير كافية"
            )

        # -------------------------------------------------
        # BUILD W1
        # -------------------------------------------------

        weekly_df = build_weekly_from_daily(
            daily_df
        )

        if len(weekly_df) < 20:

            raise ValueError(
                "بيانات W1 غير كافية"
            )

        # -------------------------------------------------
        # W1
        # -------------------------------------------------

        w1_ind = calculate_indicators(
            weekly_df,
            scalp=False
        )

        # EMA200 reliability

        w1_warnings = []

        if len(weekly_df) < 220:

            w1_warnings.append(

                "⚠️ EMA200 W1 غير متاح "
                "بدرجة موثوقية كافية"

            )

            w1_ind["ema200"] = None

        w1_direction, w1_score, w1_score_warnings = (
            calculate_direction_score(
                w1_ind
            )
        )

        w1_warnings.extend(
            w1_score_warnings
        )

        w1_trend = get_trend(
            w1_ind
        )

        # -------------------------------------------------
        # D1
        # -------------------------------------------------

        d1_ind = calculate_indicators(
            daily_df,
            scalp=False
        )

        d1_warnings = []

        if len(daily_df) < 220:

            d1_warnings.append(

                f"ℹ️ D1 يحتوي على "
                f"{len(daily_df)} شمعة فقط؛ "
                "EMA200 أقل موثوقية من التاريخ الكامل."

            )

        d1_direction, d1_score, d1_score_warnings = (
            calculate_direction_score(
                d1_ind
            )
        )

        d1_warnings.extend(
            d1_score_warnings
        )

        d1_trend = get_trend(
            d1_ind
        )

        # -------------------------------------------------
        # H4
        # -------------------------------------------------

        h4_bars = get_bars(
            "4h",
            250
        )

        h4_df = bars_to_dataframe(
            h4_bars
        )

        if len(h4_df) < 50:

            raise ValueError(
                "بيانات H4 غير كافية"
            )

        h4_ind = calculate_indicators(
            h4_df,
            scalp=False
        )

        h4_direction, h4_score, h4_warnings = (
            calculate_direction_score(
                h4_ind
            )
        )

        h4_trend = get_trend(
            h4_ind
        )

        # -------------------------------------------------
        # WEEKLY AGREEMENT
        # -------------------------------------------------

        direction_list = [
            w1_direction,
            d1_direction,
            h4_direction
        ]

        buy_count = direction_list.count(
            "BUY"
        )

        sell_count = direction_list.count(
            "SELL"
        )

        if buy_count > sell_count:

            final_direction = "BUY"

        elif sell_count > buy_count:

            final_direction = "SELL"

        else:

            final_direction = "WAIT"

        final_score = (
            w1_score * 0.45
            +
            d1_score * 0.35
            +
            h4_score * 0.20
        )

        if final_direction == "SELL":

            final_score = -final_score

        # -------------------------------------------------
        # WEEKLY ACTION
        # -------------------------------------------------

        if abs(final_score) >= 65:

            final_action = final_direction

        elif abs(final_score) >= 50:

            final_action = (
                f"WATCH {final_direction}"
            )

        else:

            final_action = "WAIT"

        confidence = round(
            abs(final_score)
        )

        # -------------------------------------------------
        # OUTPUT
        # -------------------------------------------------

        def format_block(
            name,
            ind,
            trend,
            direction,
            score,
            warnings
        ):

            warning_text = (
                " | ".join(warnings)
                if warnings
                else "لا توجد تحذيرات"
            )

            ema200_text = (

                f"{ind['ema200']:.2f}"
                if ind["ema200"] is not None
                else "N/A"

            )

            return (

                f"📊 {name}\n"

                f"💰 Price: "
                f"{ind['price']:.2f}\n"

                f"📈 EMA20: "
                f"{ind['ema20']:.2f}\n"

                f"📈 EMA50: "
                f"{ind['ema50']:.2f}\n"

                f"📈 EMA200: "
                f"{ema200_text}\n"

                f"RSI: "
                f"{ind['rsi']:.2f}\n"

                f"MACD: "
                f"{ind['macd']:.4f}\n"

                f"Signal: "
                f"{ind['signal']:.4f}\n"

                f"ATR: "
                f"{ind['atr']:.2f}\n"

                f"Volume: "
                f"{ind['volume']:.0f}\n"

                f"Volume Ratio: "
                f"{ind['volume_ratio']:.2f}x\n"

                f"Trend: "
                f"{trend}\n"

                f"Direction: "
                f"{direction}\n"

                f"Score: "
                f"{score}%\n"

                f"⚠️ "
                f"{warning_text}"

            )

        message = (

            "🤖 XAU SMART BOT v6\n\n"

            "📅 WEEKLY ANALYSIS\n\n"

            + format_block(
                "W1",
                w1_ind,
                w1_trend,
                w1_direction,
                w1_score,
                w1_warnings
            )

            + "\n\n"

            + format_block(
                "D1",
                d1_ind,
                d1_trend,
                d1_direction,
                d1_score,
                d1_warnings
            )

            + "\n\n"

            + format_block(
                "H4",
                h4_ind,
                h4_trend,
                h4_direction,
                h4_score,
                h4_warnings
            )

            + "\n\n━━━━━━━━━━━━━━\n"

            + f"🎯 FINAL WEEKLY: "
            f"{final_action}\n"

            + f"💪 CONFIDENCE: "
            f"{confidence}%\n"

            + f"📊 AGREEMENT: "
            f"🟢 BUY {buy_count}/3 | "
            f"🔴 SELL {sell_count}/3\n"

            + "━━━━━━━━━━━━━━\n\n"

            + "⚠️ Confidence = درجة توافق "
            "المؤشرات وليست احتمال ربح."

        )

        await update.message.reply_text(
            message
        )

    except requests.exceptions.Timeout:

        await update.message.reply_text(

            "⏳ انتهت مهلة الاتصال "
            "ببيانات التحليل الأسبوعي."

        )

    except Exception as e:

        await update.message.reply_text(

            "❌ خطأ في التحليل الأسبوعي:\n\n"
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

        results = []

        weighted_scores = []

        directions = []

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
                    "بيانات غير كافية"
                )

            ind = calculate_indicators(
                df,
                scalp=False
            )

            direction, score, warnings = (
                calculate_direction_score(
                    ind
                )
            )

            trend = get_trend(
                ind
            )

            # -------------------------------------------------
            # EMA200 WARNING
            # -------------------------------------------------

            if len(df) < 220:

                warnings.insert(
                    0,
                    f"ℹ️ {names[interval]}: "
                    f"{len(df)} شمعة فقط؛ "
                    "EMA200 أقل موثوقية."
                )

            directions.append(
                direction
            )

            weighted_scores.append(

                (
                    score
                    if direction == "BUY"
                    else
                    -score
                    if direction == "SELL"
                    else 0
                )
                *
                DAILY_WEIGHTS[interval]

            )

            warning_text = (

                " | ".join(warnings)
                if warnings
                else "لا توجد تحذيرات"

            )

            ema200_text = (

                f"{ind['ema200']:.2f}"

                if ind["ema200"] is not None

                else "N/A"

            )

            results.append(

                f"📊 {names[interval]}\n"

                f"💰 Price: "
                f"{ind['price']:.2f}\n"

                f"📈 EMA20: "
                f"{ind['ema20']:.2f}\n"

                f"📈 EMA50: "
                f"{ind['ema50']:.2f}\n"

                f"📈 EMA200: "
                f"{ema200_text}\n"

                f"RSI: "
                f"{ind['rsi']:.2f}\n"

                f"MACD: "
                f"{ind['macd']:.4f}\n"

                f"Signal: "
                f"{ind['signal']:.4f}\n"

                f"ATR: "
                f"{ind['atr']:.2f}\n"

                f"Volume: "
                f"{ind['volume']:.0f}\n"

                f"Volume Ratio: "
                f"{ind['volume_ratio']:.2f}x\n"

                f"Trend: "
                f"{trend}\n"

                f"Direction: "
                f"{direction}\n"

                f"Score: "
                f"{score}%\n"

                f"⚠️ "
                f"{warning_text}"

            )

        # -----------------------------------------------------
        # FINAL SCORE
        # -----------------------------------------------------

        final_score = sum(
            weighted_scores
        )

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

        if abs(final_score) >= 65:

            final_signal = (
                f"🟢 {final_direction}"
                if final_direction == "BUY"
                else
                f"🔴 {final_direction}"
                if final_direction == "SELL"
                else
                "🟡 WAIT"
            )

        elif abs(final_score) >= 50:

            final_signal = (
                f"🟡 WATCH {final_direction}"
            )

        else:

            final_signal = "🟡 WAIT"

        confidence = round(
            abs(final_score)
        )

        # -----------------------------------------------------
        # MESSAGE
        # -----------------------------------------------------

        message = (

            "🤖 XAU SMART BOT v6\n\n"

            "📊 DAILY ANALYSIS\n\n"

            + "\n\n".join(
                results
            )

            + "\n\n━━━━━━━━━━━━━━\n"

            + f"🎯 FINAL SIGNAL: "
            f"{final_signal}\n"

            + f"💪 CONFIDENCE: "
            f"{confidence}%\n"

            + f"📊 AGREEMENT: "
            f"🟢 BUY: {buy_count}/3 | "
            f"🔴 SELL: {sell_count}/3\n"

            + "━━━━━━━━━━━━━━\n\n"

            + "⚠️ Confidence = درجة توافق "
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

        results = []

        directions = []

        directional_scores = []

        indicators = {}

        warnings_all = []

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
                    "بيانات غير كافية"
                )

            ind = calculate_indicators(
                df,
                scalp=True
            )

            indicators[interval] = ind

            direction, score, warnings = (
                calculate_direction_score(
                    {
                        **ind,
                        "ema200": None
                    }
                )
            )

            # -------------------------------------------------
            # SCALP-SPECIFIC WARNINGS
            # -------------------------------------------------

            if ind["rsi"] >= 80:

                warnings.append(
                    "⚠️ RSI تشبع شرائي قوي"
                )

            elif ind["rsi"] >= 72:

                warnings.append(
                    "⚠️ RSI مرتفع - لا نطارد السعر"
                )

            elif ind["rsi"] <= 20:

                warnings.append(
                    "⚠️ RSI تشبع بيعي قوي"
                )

            elif ind["rsi"] <= 28:

                warnings.append(
                    "⚠️ RSI منخفض - لا نطارد الهبوط"
                )

            distance = abs(
                ind["price"]
                -
                ind["ema20"]
            )

            atr_distance = (
                distance /
                ind["atr"]
                if ind["atr"] > 0
                else 999
            )

            if atr_distance > 0.80:

                if direction == "BUY":

                    warnings.append(
                        "⚠️ السعر ممتد فوق EMA20"
                    )

                elif direction == "SELL":

                    warnings.append(
                        "⚠️ السعر ممتد تحت EMA20"
                    )

            if ind["volume_ratio"] < 0.60:

                warnings.append(
                    "⚠️ Volume ضعيف"
                )

            trend = get_trend(
                {
                    **ind,
                    "ema200": None
                }
            )

            directions.append(
                direction
            )

            directional_scores.append(

                (
                    score
                    if direction == "BUY"
                    else
                    -score
                    if direction == "SELL"
                    else 0
                )
                *
                SCALP_WEIGHTS[interval]

            )

            warning_text = (

                " | ".join(
                    warnings
                )
                if warnings
                else "لا توجد تحذيرات"

            )

            results.append(

                f"📊 {names[interval]}\n"

                f"Price: "
                f"{ind['price']:.2f}\n"

                f"EMA9: "
                f"{ind['ema9']:.2f}\n"

                f"EMA20: "
                f"{ind['ema20']:.2f}\n"

                f"EMA50: "
                f"{ind['ema50']:.2f}\n"

                f"RSI9: "
                f"{ind['rsi']:.2f}\n"

                f"MACD: "
                f"{ind['macd']:.4f}\n"

                f"Signal: "
                f"{ind['signal']:.4f}\n"

                f"ATR: "
                f"{ind['atr']:.2f}\n"

                f"Volume Ratio: "
                f"{ind['volume_ratio']:.2f}x\n"

                f"Trend: "
                f"{trend}\n"

                f"Direction: "
                f"{direction}\n"

                f"Score: "
                f"{score}%\n"

                f"⚠️ "
                f"{warning_text}"

            )

            warnings_all.extend(
                warnings
            )

        # -----------------------------------------------------
        # FINAL DIRECTION
        # -----------------------------------------------------

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

        final_score = sum(
            directional_scores
        )

        confidence = round(
            abs(final_score)
        )

        # -----------------------------------------------------
        # AGREEMENT
        # -----------------------------------------------------

        full_agreement = (
            buy_count == 3
            or
            sell_count == 3
        )

        # -----------------------------------------------------
        # ENTRY FILTER
        # -----------------------------------------------------

        entry_allowed = False

        entry_reason = ""

        entry_warnings = []

        if final_direction in (
            "BUY",
            "SELL"
        ):

            m5 = indicators["5m"]

            entry_allowed, entry_reason, entry_warnings = (
                scalp_entry_filter(
                    m5,
                    final_direction
                )
            )

        else:

            entry_reason = (
                "لا يوجد اتجاه متفق عليه"
            )

        # -----------------------------------------------------
        # FINAL ACTION
        # -----------------------------------------------------

        if (
            final_direction != "WAIT"
            and full_agreement
            and confidence >= 65
            and entry_allowed
        ):

            final_action = (
                f"🟢 {final_direction}"
            )

        elif (
            final_direction != "WAIT"
            and confidence >= 50
        ):

            final_action = (
                f"🟡 WATCH {final_direction}"
            )

        else:

            final_action = "🟡 WAIT"

        # -----------------------------------------------------
        # TRADE LEVELS
        # -----------------------------------------------------

        entry = None
        sl = None
        tp1 = None
        tp2 = None

        if (
            entry_allowed
            and final_action.startswith("🟢")
        ):

            m5 = indicators["5m"]

            levels = calculate_trade_levels(
                m5["price"],
                m5["atr"],
                final_direction
            )

            entry = levels[0]
            sl = levels[1]
            tp1 = levels[2]
            tp2 = levels[3]

        # -----------------------------------------------------
        # ENTRY FILTER DISPLAY
        # -----------------------------------------------------

        if entry_allowed:

            entry_filter_text = (
                "🟢 Entry Filter: الدخول مقبول"
            )

        else:

            entry_filter_text = (
                "⏳ Entry Filter: "
                "انتظار تصحيح/تأكيد أفضل"
            )

        all_warnings = (
            warnings_all
            +
            entry_warnings
        )

        unique_warnings = list(
            dict.fromkeys(
                all_warnings
            )
        )

        # -----------------------------------------------------
        # MESSAGE
        # -----------------------------------------------------

        message = (

            "🤖 XAU SMART BOT v6\n\n"

            "⚡ SCALP ANALYSIS\n"

            "━━━━━━━━━━━━━━\n"

            + "\n\n".join(
                results
            )

            + "\n\n━━━━━━━━━━━━━━\n"

            + f"🎯 FINAL SCALP: "
            f"{final_action}\n"

            + f"💪 CONFIDENCE: "
            f"{confidence}%\n"

            + f"📊 AGREEMENT: "
            f"🟢 BUY: {buy_count}/3 | "
            f"🔴 SELL: {sell_count}/3\n\n"

            + f"{entry_filter_text}\n"

            + f"📌 Entry Decision: "
            f"{entry_reason}\n"

        )

        if entry is not None:

            message += (

                "\n🎯 Entry: "
                f"{entry:.2f}\n"

                "🛑 SL: "
                f"{sl:.2f}\n"

                "🎯 TP1: "
                f"{tp1:.2f}\n"

                "🎯 TP2: "
                f"{tp2:.2f}\n"

            )

        else:

            message += (

                "\n🚫 لا يوجد Entry حاليًا.\n"

                "⏳ ننتظر تصحيحًا وتأكيدًا "
                "قبل مطاردة السعر.\n"

            )

        if unique_warnings:

            message += (

                "\n⚠️ "
                +
                " | ".join(
                    unique_warnings
                )

            )

        message += (

            "\n\n━━━━━━━━━━━━━━\n"

            "⚠️ التحليل اللحظي لا يضمن الربح."

        )

        await update.message.reply_text(
            message
        )

    except requests.exceptions.Timeout:

        await update.message.reply_text(

            "⏳ انتهت مهلة الاتصال "
            "ببيانات التحليل اللحظي."

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
