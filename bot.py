import os
import asyncio
import threading
import time
import logging
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

import requests
import pandas as pd
import numpy as np

from flask import Flask, request, jsonify
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

# =========================================================
# XAU SMART TRADER v16.0
# =========================================================

TOKEN = os.environ.get("TELEGRAM_TOKEN")
PORT = int(os.environ.get("PORT", "10000"))
RENDER_URL = os.environ.get(
    "RENDER_EXTERNAL_URL",
    "https://xau-smart-bot.onrender.com"
).rstrip("/")
WEBHOOK_PATH = "/telegram-webhook"
WEBHOOK_URL = f"{RENDER_URL}{WEBHOOK_PATH}"

SYMBOL = "XAUUSD"
DATA_URL = "https://biquote.io/api/XAUUSD/ohlc"

DAMASCUS = ZoneInfo("Asia/Damascus")
NEW_YORK = ZoneInfo("America/New_York")

CACHE_SECONDS = 20
# Minimum bars required per timeframe; price command can fall back to H1.
MIN_BARS = 30
S_R_CLUSTER_ATR = 0.35
TRADE_THRESHOLD = 73
STRICT_100_THRESHOLD = 99
AUTO_CHECK_SECONDS = 60
AUTO_START = dtime(0, 0)
AUTO_END = dtime(23, 59)

# CMC is deliberately left as an adapter until its exact definition/source
# is confirmed. It must never invent a BUY/SELL signal.
CMC_ENABLED = False

app = Flask(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

DATA_CACHE = {}
SUBSCRIBERS = set()
COMMAND_LOCKS = {}
COMMAND_MESSAGES = {}  # (chat_id, command) -> list of bot message_ids
LAST_AUTO_SIGNAL = {}
LAST_AUTO_TIME = {}
APPLICATION = None
BOT_LOOP = None
BOT_STARTED = False

WEEKLY_REPORT = None
WEEKLY_REPORT_WEEK = None


# =========================================================
# HEALTH
# =========================================================

@app.route("/", methods=["GET", "HEAD"])
def home():
    return "XAU SMART TRADER v16.0 - OK", 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "bot": "XAU SMART TRADER v16.0",
        "time": datetime.now(DAMASCUS).isoformat()
    }), 200


# =========================================================
# DATA
# =========================================================

def cache_key(interval, limit):
    return f"{interval}_{limit}"


def get_bars(interval, limit=300):
    key = cache_key(interval, limit)
    now = time.time()
    cached = DATA_CACHE.get(key)

    if cached and now - cached[0] < CACHE_SECONDS:
        return cached[1].copy()

    url = f"{DATA_URL}?interval={interval}&limit={limit}"
    response = requests.get(url, timeout=12)
    response.raise_for_status()
    data = response.json()
    bars = data.get("bars", [])

    if not bars:
        raise ValueError(f"لا توجد بيانات للفريم {interval}")

    df = pd.DataFrame(bars)

    required = ["open", "high", "low", "close", "tickVolume"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"البيانات ناقصة: {col}")
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if "openTime" in df.columns:
        df["openTime"] = pd.to_datetime(
            df["openTime"], errors="coerce", utc=True
        )

    df = df.dropna(subset=required)
    if "openTime" in df.columns:
        df = df.dropna(subset=["openTime"]).sort_values("openTime")

    df = df.reset_index(drop=True)

    if len(df) < MIN_BARS:
        raise ValueError(f"بيانات {interval} غير كافية")

    DATA_CACHE[key] = (now, df.copy())
    return df


def _first_numeric(data, keys):
    if not isinstance(data, dict):
        return None
    for key in keys:
        value = safe_float(data.get(key), None)
        if value is not None and value > 0:
            return value
    return None


def _extract_live_price(data):
    # Accept the common Biquote shapes without ever using OHLC candles.
    price = _first_numeric(data, ("mid", "price", "last"))
    if price is not None:
        return price

    if isinstance(data, dict):
        for container in ("quote", "data", "tick", "result"):
            nested = data.get(container)
            if isinstance(nested, dict):
                price = _first_numeric(nested, ("mid", "price", "last"))
                if price is not None:
                    return price
                bid = _first_numeric(nested, ("bid",))
                ask = _first_numeric(nested, ("ask",))
                if bid is not None and ask is not None:
                    return (bid + ask) / 2.0

        bid = _first_numeric(data, ("bid",))
        ask = _first_numeric(data, ("ask",))
        if bid is not None and ask is not None:
            return (bid + ask) / 2.0

    return None


def _extract_day_change(data):
    if not isinstance(data, dict):
        return 0.0
    return _first_numeric(data, ("dayDiff", "change", "changeValue")) or 0.0


def _extract_day_percent(data):
    if not isinstance(data, dict):
        return 0.0
    value = _first_numeric(data, ("dayDiffPercent", "dayPct", "changePercent", "percent"))
    return value if value is not None else 0.0


def get_live_price():
    """Fetch the live XAUUSD quote only from Biquote's quote endpoint.

    IMPORTANT: OHLC candles are never used as a fallback here. If the live
    quote is unavailable or invalid, an exception is raised so the user sees
    that the live price is unavailable instead of receiving an old candle.

    This function is intentionally synchronous because price() runs it with
    asyncio.to_thread(). This prevents the 'coroutine was never awaited'
    RuntimeWarning that affected the previous v16 build.
    """
    url = f"https://biquote.io/api/{SYMBOL}"
    response = requests.get(
        url,
        params={"allowStale": "false"},
        timeout=10,
    )
    response.raise_for_status()
    data = response.json()

    price = _extract_live_price(data)
    if price is None or price <= 0:
        raise ValueError("مصدر السعر المباشر لم يُرجع سعرًا صالحًا")

    day_diff = _extract_day_change(data)
    day_pct = _extract_day_percent(data)

    # If only the percentage is supplied, derive the displayed day change.
    if day_diff == 0.0 and day_pct != 0.0:
        day_diff = price * day_pct / 100.0

    return {
        "price": price,
        "day_diff": day_diff,
        "day_pct": day_pct,
        "source": "Biquote Live Quote",
        "raw": data,
    }


# =========================================================
# INDICATORS
# =========================================================

def safe_float(value, default=0.0):
    try:
        value = float(value)
        return value if np.isfinite(value) else default
    except Exception:
        return default


def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).fillna(50)


def macd(series, fast=8, slow=21, signal=5):
    fast_ema = ema(series, fast)
    slow_ema = ema(series, slow)
    line = fast_ema - slow_ema
    signal_line = ema(line, signal)
    return line, signal_line, line - signal_line


def atr(high, low, close, period=14):
    prev = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev).abs(),
        (low - prev).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False).mean()


def adx(df, period=14):
    high, low, close = df["high"], df["low"], df["close"]
    up = high.diff()
    down = -low.diff()

    plus_dm = pd.Series(
        np.where((up > down) & (up > 0), up, 0.0), index=df.index
    )
    minus_dm = pd.Series(
        np.where((down > up) & (down > 0), down, 0.0), index=df.index
    )

    prev = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev).abs(),
        (low - prev).abs()
    ], axis=1).max(axis=1)

    atr_s = tr.ewm(alpha=1 / period, adjust=False).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr_s
    minus_di = 100 * minus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr_s

    dx = (100 * (plus_di - minus_di).abs() /
          (plus_di + minus_di).replace(0, np.nan)).fillna(0)

    return dx.ewm(alpha=1 / period, adjust=False).mean(), plus_di, minus_di


def fibonacci_levels(df, lookback=80):
    x = df.tail(min(lookback, len(df)))
    hi = safe_float(x["high"].max())
    lo = safe_float(x["low"].min())
    span = hi - lo

    if span <= 0:
        return {}

    return {
        "0.0": hi,
        "23.6": hi - span * 0.236,
        "38.2": hi - span * 0.382,
        "50.0": hi - span * 0.500,
        "61.8": hi - span * 0.618,
        "78.6": hi - span * 0.786,
        "100.0": lo
    }


def candle_wick_features(row):
    o = safe_float(row["open"])
    h = safe_float(row["high"])
    l = safe_float(row["low"])
    c = safe_float(row["close"])

    rng = max(h - l, 1e-9)
    upper = h - max(o, c)
    lower = min(o, c) - l
    body = abs(c - o)

    return {
        "range": rng,
        "body": body,
        "upper_wick": max(upper, 0),
        "lower_wick": max(lower, 0),
        "upper_ratio": max(upper, 0) / rng,
        "lower_ratio": max(lower, 0) / rng
    }


def structure_state(df, lookback=8):
    x = df.tail(max(lookback, 5))
    highs = x["high"].rolling(3, center=True).max()
    lows = x["low"].rolling(3, center=True).min()

    hh = safe_float(x["high"].iloc[-1]) > safe_float(x["high"].iloc[-4])
    hl = safe_float(x["low"].iloc[-1]) > safe_float(x["low"].iloc[-4])
    lh = safe_float(x["high"].iloc[-1]) < safe_float(x["high"].iloc[-4])
    ll = safe_float(x["low"].iloc[-1]) < safe_float(x["low"].iloc[-4])

    if hh and hl:
        return "🟢 HH/HL"
    if lh and ll:
        return "🔴 LH/LL"
    return "🟡 مختلط"


def liquidity_state(df):
    vol = df["tickVolume"].astype(float)
    avg = safe_float(vol.tail(20).mean(), 1)
    current = safe_float(vol.iloc[-1])
    ratio = current / avg if avg > 0 else 0

    # Proxy only: tick-volume/liquidity quality, not exchange order-book liquidity.
    if ratio >= 1.30:
        state = "🟢 قوية"
    elif ratio >= 0.90:
        state = "🟡 طبيعية"
    else:
        state = "🔴 ضعيفة"

    return state, ratio


def volume_state(df):
    vol = df["tickVolume"].astype(float)
    short = safe_float(vol.tail(5).mean())
    base = safe_float(vol.tail(30).mean(), 1)
    ratio = short / base if base > 0 else 0

    if ratio >= 1.40:
        return "🟢 توسع حجمي", ratio
    if ratio >= 0.90:
        return "🟡 طبيعي", ratio
    return "🔴 ضعيف", ratio


def conflict_analysis(direction, higher, lower):
    if direction == "BUY" and higher == "SELL":
        return "🔴 تعارض قوي: الإشارة عكس الاتجاه الأكبر"
    if direction == "SELL" and higher == "BUY":
        return "🔴 تعارض قوي: الإشارة عكس الاتجاه الأكبر"
    if higher != lower:
        return "🟡 تعارض جزئي بين الفريمات"
    return "🟢 لا يوجد تعارض رئيسي"


def cmc_confirmation(_df, _direction):
    # Placeholder by design. CMC must be defined from the user's intended
    # indicator/strategy before it is allowed to affect a trade score.
    return 0, "⚪ CMC غير مفعّل"


# =========================================================
# CORE ANALYSIS
# =========================================================

def analyze_frame(df, scalp=False):
    if df is None or len(df) < 60:
        raise ValueError("البيانات غير كافية للتحليل")

    close = df["close"]
    current = safe_float(close.iloc[-1])

    fast = 9 if scalp else 20
    mid = 20 if scalp else 50
    long = 50 if scalp else 200

    e_fast = safe_float(ema(close, fast).iloc[-1])
    e_mid = safe_float(ema(close, mid).iloc[-1])
    e_long = safe_float(ema(close, long).iloc[-1])

    rsi_v = safe_float(rsi(close, 9 if scalp else 14).iloc[-1], 50)
    macd_line, signal_line, hist = macd(
        close, 5, 13, 4
    ) if scalp else macd(close, 8, 21, 5)

    macd_v = safe_float(macd_line.iloc[-1])
    signal_v = safe_float(signal_line.iloc[-1])
    hist_v = safe_float(hist.iloc[-1])
    atr_v = safe_float(atr(
        df["high"], df["low"], close, 14
    ).iloc[-1])

    adx_v, plus_di, minus_di = adx(df, 14)
    adx_now = safe_float(adx_v.iloc[-1])
    plus_now = safe_float(plus_di.iloc[-1])
    minus_now = safe_float(minus_di.iloc[-1])

    liq_state, liq_ratio = liquidity_state(df)
    vol_state, vol_ratio = volume_state(df)
    structure = structure_state(df)
    fib = fibonacci_levels(df)

    wick = candle_wick_features(df.iloc[-1])

    bull = 0.0
    bear = 0.0
    warnings = []
    reasons = []

    # Trend
    if current > e_fast > e_mid:
        bull += 22
        reasons.append("اتجاه قصير/متوسط صاعد")
        trend = "🟢 صاعد"
    elif current < e_fast < e_mid:
        bear += 22
        reasons.append("اتجاه قصير/متوسط هابط")
        trend = "🔴 هابط"
    else:
        trend = "🟡 متردد"
        warnings.append("الاتجاه القصير غير مكتمل")

    # Higher trend anchor
    if len(df) >= long:
        if current > e_long:
            bull += 12
        elif current < e_long:
            bear += 12

    # Momentum
    if 50 <= rsi_v < 68:
        bull += 13
    elif 32 < rsi_v < 50:
        bear += 13
    elif rsi_v >= 68:
        warnings.append("RSI مرتفع")
    elif rsi_v <= 32:
        warnings.append("RSI منخفض")

    # MACD
    if macd_v > signal_v and hist_v >= 0:
        bull += 13
    elif macd_v < signal_v and hist_v <= 0:
        bear += 13
    else:
        warnings.append("MACD غير حاسم")

    # ADX / regime
    if adx_now >= 25:
        regime = "🟢 اتجاهي"
        if plus_now > minus_now:
            bull += 10
        elif minus_now > plus_now:
            bear += 10
    elif adx_now >= 18:
        regime = "🟡 اتجاه ضعيف"
        warnings.append("ADX متوسط")
    else:
        regime = "🟠 عرضي/غير مؤكد"
        warnings.append("ADX منخفض")

    # Structure
    if "HH/HL" in structure:
        bull += 10
    elif "LH/LL" in structure:
        bear += 10
    else:
        warnings.append("هيكل السوق مختلط")

    # Liquidity / volume
    if liq_ratio >= 1.20:
        if bull > bear:
            bull += 7
        elif bear > bull:
            bear += 7
    elif liq_ratio < 0.80:
        warnings.append("السيولة/الحجم النسبي ضعيف")

    if vol_ratio >= 1.20:
        if bull > bear:
            bull += 5
        elif bear > bull:
            bear += 5

    # Wick confirmation, especially valuable on M15
    wick_bias = "neutral"
    if wick["lower_ratio"] >= 0.45 and wick["upper_ratio"] < 0.25:
        wick_bias = "bullish_rejection"
        bull += 6
    elif wick["upper_ratio"] >= 0.45 and wick["lower_ratio"] < 0.25:
        wick_bias = "bearish_rejection"
        bear += 6

    # Fibonacci proximity
    fib_bonus = 0
    if fib:
        nearest = min(
            fib.values(),
            key=lambda x: abs(x - current)
        )
        if atr_v > 0 and abs(nearest - current) <= atr_v * 0.30:
            fib_bonus = 5
            if current >= nearest:
                bull += 2.5
            else:
                bear += 2.5

    # CMC adapter
    cmc_score, cmc_state = cmc_confirmation(
        df, "BUY" if bull >= bear else "SELL"
    )
    if cmc_score > 0:
        if bull >= bear:
            bull += cmc_score
        else:
            bear += cmc_score

    if bull > bear:
        direction = "BUY"
        raw = bull
    elif bear > bull:
        direction = "SELL"
        raw = bear
    else:
        direction = "WAIT"
        raw = 0

    score = int(max(0, min(round(raw), 100)))

    if score >= 80:
        state = "🟢 قوية"
    elif score >= 73:
        state = "🟢 مؤهلة"
    elif score >= 65:
        state = "🟡 مراقبة"
    else:
        state = "🟠 ضعيفة"

    extended = atr_v > 0 and abs(current - e_fast) > atr_v * 0.60

    if extended:
        warnings.append("السعر ممتد")

    return {
        "price": current,
        "ema_fast": e_fast,
        "ema_mid": e_mid,
        "ema_long": e_long,
        "rsi": rsi_v,
        "macd": macd_v,
        "macd_signal": signal_v,
        "macd_histogram": hist_v,
        "atr": atr_v,
        "adx": adx_now,
        "plus_di": plus_now,
        "minus_di": minus_now,
        "trend": trend,
        "direction": direction,
        "score": score,
        "warnings": warnings,
        "reasons": reasons,
        "state": state,
        "extended": extended,
        "liquidity": liq_state,
        "liquidity_ratio": liq_ratio,
        "volume_state": vol_state,
        "volume_ratio": vol_ratio,
        "structure": structure,
        "wick": wick,
        "wick_bias": wick_bias,
        "fib": fib,
        "fib_bonus": fib_bonus,
        "regime": regime,
        "cmc": cmc_state
    }


# =========================================================
# LEVELS / ENTRY
# =========================================================

def calculate_support_resistance(df, lookback=80):
    """Build S/R from confirmed swing pivots, clustered by ATR.

    This avoids treating every candle high/low as an independent level, which
    was the reason v15 could output very noisy levels such as 4610.38/4611.33.
    """
    x = df.tail(min(lookback, len(df))).copy().reset_index(drop=True)
    current = safe_float(x["close"].iloc[-1])
    atr_v = max(safe_float(atr(x["high"], x["low"], x["close"], 14).iloc[-1]), 0.05)
    radius = atr_v * S_R_CLUSTER_ATR

    candidates_support, candidates_resistance = [], []
    for i in range(2, len(x) - 2):
        h = safe_float(x["high"].iloc[i])
        l = safe_float(x["low"].iloc[i])
        if h >= safe_float(x["high"].iloc[i-2:i+3].max()):
            candidates_resistance.append(h)
        if l <= safe_float(x["low"].iloc[i-2:i+3].min()):
            candidates_support.append(l)

    def cluster(values):
        values = sorted(values)
        clusters = []
        for value in values:
            if not clusters or abs(value - clusters[-1][-1]) > radius:
                clusters.append([value])
            else:
                clusters[-1].append(value)
        # Weighted by number of touches; newest/nearby level gets preference later.
        return [(sum(c)/len(c), len(c)) for c in clusters]

    supports = [(v, touches) for v, touches in cluster(candidates_support) if v < current]
    resistances = [(v, touches) for v, touches in cluster(candidates_resistance) if v > current]

    supports.sort(key=lambda z: (abs(current-z[0]), -z[1]))
    resistances.sort(key=lambda z: (abs(z[0]-current), -z[1]))

    def fill(items, side):
        out = [v for v, _ in items]
        step = max(atr_v * 0.85, 0.10)
        if side == 'support':
            base = out[-1] if out else current - step
            while len(out) < 3:
                base -= step
                out.append(base)
        else:
            base = out[-1] if out else current + step
            while len(out) < 3:
                base += step
                out.append(base)
        return out[:3]

    s = fill(supports, 'support')
    r = fill(resistances, 'resistance')
    return {
        "current": current,
        "support1": s[0], "support2": s[1], "support3": s[2],
        "resistance1": r[0], "resistance2": r[1], "resistance3": r[2],
        "atr": atr_v,
        "support_touches": supports[0][1] if supports else 0,
        "resistance_touches": resistances[0][1] if resistances else 0
    }


def build_entry_zone(direction, current, levels, atr_value):
    atr_value = max(safe_float(atr_value), 0.01)

    if direction == "BUY":
        return (
            levels["support1"] - atr_value * 0.15,
            min(current, levels["support1"] + atr_value * 0.35)
        )
    if direction == "SELL":
        return (
            max(current, levels["resistance1"] - atr_value * 0.35),
            levels["resistance1"] + atr_value * 0.15
        )
    return current, current


def mtf_engine(m15, m5, m1):
    """MTF scalp engine.

    M15 is the market context, M5 is confirmation/pullback state and M1 is
    the entry trigger.  A short M5/M1 correction is not allowed to flip a
    clear M15 trend into the opposite market direction.
    """
    weighted = (
        m15["score"] * 0.45 +
        m5["score"] * 0.35 +
        m1["score"] * 0.20
    )

    # Context comes from the M15 trend first, not from a single short-term
    # indicator. This is the key protection against "bearish pullback" being
    # misclassified as a bearish market.
    if m15["trend"] == "🟢 صاعد":
        context = "BUY"
    elif m15["trend"] == "🔴 هابط":
        context = "SELL"
    else:
        context = m15["direction"] if m15["direction"] in ("BUY", "SELL") else "WAIT"

    m5_dir = m5["direction"]
    m1_dir = m1["direction"]

    # A counter-direction M5 reading is treated as a pullback when M15
    # structure still agrees with the larger context.
    bullish_pullback = (
        context == "BUY"
        and m5_dir == "SELL"
        and m15["structure"] == "🟢 HH/HL"
        and m5["structure"] != "🔴 LH/LL"
    )
    bearish_pullback = (
        context == "SELL"
        and m5_dir == "BUY"
        and m15["structure"] == "🔴 LH/LL"
        and m5["structure"] != "🟢 HH/HL"
    )

    if bullish_pullback:
        phase = "🟢 Bullish Pullback"
    elif bearish_pullback:
        phase = "🔴 Bearish Pullback"
    elif context == "BUY" and m5_dir == "BUY":
        phase = "🟢 Bullish Confirmation"
    elif context == "SELL" and m5_dir == "SELL":
        phase = "🔴 Bearish Confirmation"
    elif context in ("BUY", "SELL"):
        phase = "🟡 تصحيح/تعارض قصير الأجل"
    else:
        phase = "🟡 سياق غير واضح"

    # Entry direction requires the M1 trigger to return to the M15 context.
    # This prevents entering while the correction is still running.
    if context == "BUY" and m1_dir == "BUY":
        direction = "BUY"
    elif context == "SELL" and m1_dir == "SELL":
        direction = "SELL"
    else:
        direction = "WAIT"

    conflict = conflict_analysis(direction, context, m5_dir)

    wick_ok = (
        (context == "BUY" and m15["wick_bias"] == "bullish_rejection") or
        (context == "SELL" and m15["wick_bias"] == "bearish_rejection")
    )

    if direction == "BUY":
        if m15["structure"] == "🟢 HH/HL":
            weighted += 4
        if m5["structure"] == "🟢 HH/HL":
            weighted += 3
        if bullish_pullback:
            weighted += 5
    elif direction == "SELL":
        if m15["structure"] == "🔴 LH/LL":
            weighted += 4
        if m5["structure"] == "🔴 LH/LL":
            weighted += 3
        if bearish_pullback:
            weighted += 5

    if wick_ok:
        weighted += 3

    risk_flags = 0
    if "🔴" in conflict:
        risk_flags += 2
    if m15["liquidity_ratio"] < 0.80:
        risk_flags += 1
    if m5["liquidity_ratio"] < 0.80:
        risk_flags += 1
    if m1["extended"]:
        risk_flags += 1

    risk = "🟢 منخفض"
    if risk_flags >= 3:
        risk = "🔴 مرتفع"
    elif risk_flags >= 1:
        risk = "🟡 متوسط"

    score = int(max(0, min(round(weighted), 100)))

    # 73% is only a confluence threshold. An actual entry also needs context,
    # a returned M1 trigger, acceptable risk and sufficient market quality.
    strict_conditions = (
        direction in ("BUY", "SELL")
        and score >= TRADE_THRESHOLD
        and risk != "🔴 مرتفع"
        and not m1["extended"]
        and m15["adx"] >= 18
        and m5["adx"] >= 15
        and m15["liquidity_ratio"] >= 0.80
        and m5["liquidity_ratio"] >= 0.80
        and (m5_dir in (direction, "SELL" if direction == "BUY" else "BUY"))
        and not (
            (direction == "BUY" and m5_dir == "SELL" and m5["structure"] == "🔴 LH/LL")
            or
            (direction == "SELL" and m5_dir == "BUY" and m5["structure"] == "🟢 HH/HL")
        )
    )

    perfect_conditions = (
        direction in ("BUY", "SELL")
        and score >= STRICT_100_THRESHOLD
        and risk == "🟢 منخفض"
        and context == direction
        and m15["direction"] == m5["direction"] == m1["direction"] == direction
        and not any(x["extended"] for x in (m15, m5, m1))
        and m15["adx"] >= 25
        and m5["adx"] >= 20
        and m1["adx"] >= 15
        and m15["liquidity_ratio"] >= 1.0
        and m5["liquidity_ratio"] >= 1.0
        and wick_ok
    )

    # Secret Scalp is intentionally stricter than the normal 73% gate.
    secret_ready = (
        strict_conditions
        and score >= 85
        and context == direction
        and m1_dir == direction
        and (m5_dir == direction or bullish_pullback or bearish_pullback)
        and m15["structure"] in ("🟢 HH/HL", "🔴 LH/LL")
        and m5["structure"] in ("🟢 HH/HL", "🔴 LH/LL")
        and wick_ok
    )

    return {
        "direction": direction,
        "context": context,
        "phase": phase,
        "score": min(score, 100),
        "risk": risk,
        "conflict": conflict,
        "wick_ok": wick_ok,
        "bullish_pullback": bullish_pullback,
        "bearish_pullback": bearish_pullback,
        "strict_ready": strict_conditions,
        "secret_ready": secret_ready,
        "perfect": perfect_conditions,
    }


def build_trade(direction, df):
    current = safe_float(df["close"].iloc[-1])
    atr_v = max(safe_float(
        atr(df["high"], df["low"], df["close"], 14).iloc[-1]
    ), 0.10)

    levels = calculate_support_resistance(df, 80)

    if direction == "BUY":
        entry = current
        sl = min(entry - atr_v * 1.20, levels["support1"] - atr_v * 0.10)
        tp1 = entry + atr_v * 1.30
        tp2 = entry + atr_v * 2.00
    else:
        entry = current
        sl = max(entry + atr_v * 1.20, levels["resistance1"] + atr_v * 0.10)
        tp1 = entry - atr_v * 1.30
        tp2 = entry - atr_v * 2.00

    return entry, sl, tp1, tp2, levels


# =========================================================
# FORMATTING
# =========================================================

def warning_text(items):
    if not items:
        return "🟢 لا توجد ملاحظات"
    return " • ".join(dict.fromkeys(items))


def frame_block(name, r):
    return (
        f"📊 {name}\n"
        f"الاتجاه: {r['trend']}\n"
        f"الإشارة: {r['direction']}\n"
        f"القوة: {r['score']}%\n"
        f"ADX: {r['adx']:.1f} — {r['regime']}\n"
        f"RSI: {r['rsi']:.1f}\n"
        f"MACD: {r['macd']:.2f}\n"
        f"الهيكل: {r['structure']}\n"
        f"السيولة: {r['liquidity']} ({r['liquidity_ratio']:.2f}×)\n"
        f"الحجم: {r['volume_state']} ({r['volume_ratio']:.2f}×)\n"
        f"ذيل الشمعة: {r['wick_bias']}\n"
        f"⚠️ {warning_text(r['warnings'])}"
    )


# =========================================================
# WEEKLY AUTOMATIC REPORT
# =========================================================

def current_week_key():
    now = datetime.now(DAMASCUS)
    return now.strftime("%G-W%V")


def weekly_allowed():
    now = datetime.now(DAMASCUS)
    # Generate only after Sunday 19:00 Damascus; keep through Sunday 23:59.
    # The report itself is intended to represent the new week.
    return now.weekday() == 6 and now.time() >= dtime(19, 0)


def build_weekly_report():
    d1 = get_bars("1d", 300)
    h4 = get_bars("4h", 300)
    w1 = d1.copy()

    if "openTime" in w1.columns:
        w1["openTime"] = pd.to_datetime(w1["openTime"], utc=True)
        w1 = w1.set_index("openTime").resample("W-SUN").agg({
            "open": "first", "high": "max", "low": "min",
            "close": "last", "tickVolume": "sum"
        }).dropna().reset_index()

    wr = analyze_frame(w1, scalp=False)
    dr = analyze_frame(d1, scalp=False)
    hr = analyze_frame(h4, scalp=False)
    levels = calculate_support_resistance(d1, 100)

    score = int(round(wr["score"] * .45 + dr["score"] * .35 + hr["score"] * .20))
    bias = "🟢 صاعد" if wr["direction"] == "BUY" else "🔴 هابط" if wr["direction"] == "SELL" else "🟡 محايد"

    return (
        "📅 التحليل الأسبوعي — XAUUSD\n\n"
        f"بتوقيت دمشق: {datetime.now(DAMASCUS).strftime('%Y-%m-%d %I:%M %p')}\n"
        f"السعر الحالي: {dr['price']:.2f}\n\n"
        "🎯 الخلاصة التنفيذية\n"
        f"الاتجاه العام: {bias}\n"
        f"الزخم: RSI {dr['rsi']:.1f}\n"
        f"السيولة: {dr['liquidity']}\n"
        f"حالة السوق: {dr['regime']}\n"
        f"نسبة التوافق: {score}%\n\n"
        "🧱 المستويات الأسبوعية\n"
        f"الدعم الأقرب: {levels['support1']:.2f}\n"
        f"الدعم التالي: {levels['support2']:.2f}\n"
        f"الدعم الثالث: {levels['support3']:.2f}\n"
        f"المقاومة الأقرب: {levels['resistance1']:.2f}\n"
        f"المقاومة التالية: {levels['resistance2']:.2f}\n"
        f"المقاومة الثالثة: {levels['resistance3']:.2f}\n\n"
        "🟢 السيناريو الإيجابي\n"
        "يحتاج ثباتًا فوق المقاومة/الدعم المحوري مع استمرار الزخم والحجم.\n\n"
        "🔴 السيناريو السلبي\n"
        "يحتاج كسرًا واضحًا للدعم المحوري مع تأكيد حجمي وهيكلي.\n\n"
        "⚠️ هذا التقرير تحليلي وليس ضمانًا للربح."
    )


async def weekly_job(context: ContextTypes.DEFAULT_TYPE):
    global WEEKLY_REPORT, WEEKLY_REPORT_WEEK
    now = datetime.now(DAMASCUS)
    if now.weekday() == 6 and now.time() >= dtime(19, 0):
        key = current_week_key()
        if WEEKLY_REPORT_WEEK != key:
            try:
                WEEKLY_REPORT = build_weekly_report()
                WEEKLY_REPORT_WEEK = key
                for chat_id in list(SUBSCRIBERS):
                    await context.bot.send_message(chat_id=chat_id, text=WEEKLY_REPORT)
            except Exception:
                logger.exception("Weekly report error")

    # Keep the latest report available until the next weekly report replaces it.


# =========================================================
# COMMAND STATE / MENU
# =========================================================

async def command_lock(update, command):
    """Allow the command and remove ALL previous results for that command."""
    chat_id = update.effective_chat.id
    key = (chat_id, command)
    now = time.time()
    previous = COMMAND_LOCKS.get(key, 0)

    if now - previous < 1.5:
        return False

    COMMAND_LOCKS[key] = now

    old_message_ids = COMMAND_MESSAGES.get(key, [])
    if APPLICATION is not None:
        for message_id in list(old_message_ids):
            try:
                await APPLICATION.bot.delete_message(
                    chat_id=chat_id,
                    message_id=message_id
                )
            except Exception as exc:
                logger.debug(
                    "Old command message could not be deleted (%s): %s",
                    message_id, exc
                )

    COMMAND_MESSAGES[key] = []
    return True


async def command_reply(update, command, text):
    """Send a command result and remember it for complete replacement."""
    chat_id = update.effective_chat.id
    msg = await update.message.reply_text(text)
    COMMAND_MESSAGES[(chat_id, command)] = [msg.message_id]
    return msg


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    subscribed = update.effective_chat.id in SUBSCRIBERS
    auto_button = (
        "🟢 الصفقة التلقائية: تشغيل"
        if subscribed else
        "🔴 الصفقة التلقائية: إيقاف"
    )
    state = "🟢 مفعّلة" if subscribed else "🔴 متوقفة"
    keyboard = [
        ["📊 التحليل اليومي", "⚡ التحليل السريع"],
        ["📅 التحليل الأسبوعي", "🕵️ Secret Scalp"],
        ["🎯 صفقة الآن", "💰 سعر الذهب"],
        [auto_button],
        ["🌍 الأسواق", "🟢 حالة النظام"],
        ["🆘 الدعم"],
    ]
    text = (
        "🤖 XAU SMART TRADER\n"
        "🥇 محرك XAUUSD متعدد العوامل\n\n"
        "🎯 73% = عتبة تأهيل قوية\n"
        "🕵️ M15 → M5 → M1\n"
        "🛡️ Structure + Liquidity + Volume + Risk\n\n"
        f"📡 الصفقات التلقائية: {state}\n\n"
        "اختر العملية 👇"
    )
    await update.message.reply_text(
        text,
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Compatibility only: /start is not part of the visible menu.
    await show_main_menu(update, context)


async def button_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    routes = {
        "📊 التحليل اليومي": daily,
        "⚡ التحليل السريع": quick,
        "📅 التحليل الأسبوعي": weekly,
        "🕵️ Secret Scalp": scalp,
        "🎯 صفقة الآن": trade,
        "📍 الدعوم والمقاومات": levels,
        "💰 سعر الذهب": price,
        "🟢 الصفقة التلقائية: تشغيل": toggle_auto,
        "🔴 الصفقة التلقائية: إيقاف": toggle_auto,
        "🔔 تفعيل الصفقات التلقائية": subscribe,
        "🔕 إيقاف الصفقات التلقائية": unsubscribe,
        "🌍 الأسواق": markets,
        "🟢 حالة النظام": status,
        "🆘 الدعم": support,
    }
    fn = routes.get(text)
    if fn:
        await fn(update, context)
    else:
        await show_main_menu(update, context)


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    subscribed = update.effective_chat.id in SUBSCRIBERS
    await update.message.reply_text(
        "🤖 XAU SMART TRADER v16.0\n\n"
        "🟢 النظام: يعمل\n"
        "🌐 Webhook: يعمل\n"
        "📡 البيانات: متاحة\n"
        "🧠 Multi-Factor: ON\n"
        "🕵️ Secret Scalp M15/M5/M1: مفعّل\n"
        "🧱 S/R Cluster: مفعّل\n"
        "🏦 Institutional Layer: إطار مفعّل بدون بيانات مخترعة\n"
        f"🎯 عتبة التأهيل: {TRADE_THRESHOLD}%\n"
        f"💯 الوضع الصارم 100%: مفعّل\n"
        "💧 السيولة: مفعّلة\n"
        "📦 الحجم: مفعّل\n"
        "📈 ADX: مفعّل\n"
        "🧱 الهيكل السعري: مفعّل\n"
        "🕯️ ذيل M15: مفعّل\n"
        "🔢 فيبوناتشي: مفعّل\n"
        "🧠 كشف التعارض: مفعّل\n"
        f"🔔 الصفقات التلقائية: {'🟢 مفعّلة' if subscribed else '🔕 متوقفة'}"
    )


async def price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await command_lock(update, "price"):
        return
    try:
        # get_live_price is synchronous; run it outside the Telegram event loop.
        quote = await asyncio.to_thread(get_live_price)
        current = quote["price"]
        change = quote["day_diff"]
        pct = quote["day_pct"]

        raw = quote.get("raw", {})
        market_state = raw.get("marketState", raw.get("status", "غير معروف")) if isinstance(raw, dict) else "غير معروف"
        quote_age = raw.get("quoteAgeSeconds", "غير متاح") if isinstance(raw, dict) else "غير متاح"

        text = (
            "🥇 XAUUSD — السعر اللحظي\n\n"
            f"💰 السعر: {current:.2f}\n"
            f"📊 التغير اليومي: {change:+.2f} ({pct:+.2f}%)\n"
            f"📡 حالة السوق: {market_state}\n"
            f"⚡ عمر السعر: {quote_age} ثانية\n"
            "🛰️ المصدر: Biquote Live Quote\n"
            f"🕐 دمشق: {datetime.now(DAMASCUS).strftime('%Y-%m-%d %I:%M:%S %p')}"
        )
        await command_reply(update, "price", text)
    except Exception as e:
        await safe_reply(
            update,
            "❌ تعذر الحصول على السعر اللحظي.\n"
            "لم يتم استخدام OHLC كبديل حتى لا يظهر سعر قديم على أنه لحظي.\n"
            f"السبب: {e}"
        )


async def levels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await command_lock(update, "levels"):
        return
    try:
        d1 = get_bars("1d", 150)
        x = calculate_support_resistance(d1, 80)
        await command_reply(update, "levels",
            "📍 XAUUSD — مستويات رئيسية\n\n"
            f"💰 السعر: {x['current']:.2f}\n\n"
            f"🟢 دعم 1: {x['support1']:.2f}\n"
            f"🟢 دعم 2: {x['support2']:.2f}\n"
            f"🟢 S3: {x['support3']:.2f}\n\n"
            f"🔴 مقاومة 1: {x['resistance1']:.2f}\n"
            f"🔴 مقاومة 2: {x['resistance2']:.2f}\n"
            f"🔴 R3: {x['resistance3']:.2f}\n\n"
            f"🎯 دقة المنطقة: S1 لمس {x.get('support_touches', 0)} / R1 لمس {x.get('resistance_touches', 0)}\n"
            "⚠️ المستويات مناطق وليست أرقامًا سحرية؛ التأكيد السعري والحجمي مطلوب قبل التنفيذ."
        )
    except Exception as e:
        await safe_reply(update, f"❌ تعذر حساب المستويات: {e}")


async def weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await command_lock(update, "weekly"):
        return
    global WEEKLY_REPORT
    if WEEKLY_REPORT:
        await command_reply(update, "weekly", WEEKLY_REPORT)
    else:
        await command_reply(
            update, "weekly",
            "📅 التقرير الأسبوعي\n\n"
            "سيُنشأ تلقائيًا كل أحد الساعة 7:00 مساءً بتوقيت دمشق.\n"
            "ويبقى متاحًا خلال الأسبوع بدل اختفائه يوم الاثنين."
        )


async def daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await command_lock(update, "daily"):
        return
    try:
        d1 = get_bars("1d", 300)
        h4 = get_bars("4h", 300)
        h1 = get_bars("1h", 300)

        dr, hr, h1r = [analyze_frame(x) for x in (d1, h4, h1)]
        levels = calculate_support_resistance(d1, 80)
        score = int(round(dr["score"]*.45 + hr["score"]*.35 + h1r["score"]*.20))
        bias = "🟢 صاعد" if score >= 50 and dr["direction"] == "BUY" else \
               "🔴 هابط" if score >= 50 and dr["direction"] == "SELL" else "🟡 مختلط"

        await command_reply(update, "daily",
            "📊 تحليل الذهب\n"
            f"ليوم {datetime.now(DAMASCUS).strftime('%Y-%m-%d')} "
            f"بتوقيت دمشق {datetime.now(DAMASCUS).strftime('%I:%M %p')}\n\n"
            f"💰 السعر الحالي: {dr['price']:.2f}\n\n"
            "🎯 الخلاصة التنفيذية\n"
            f"1. الاتجاه العام: {bias}\n"
            f"2. الزخم: RSI {dr['rsi']:.1f}\n"
            f"3. السيولة: {dr['liquidity']}\n"
            f"4. حالة السوق: {dr['regime']}\n"
            f"5. نسبة التوافق: {score}%\n\n"
            "📍 المناطق المهمة\n"
            f"1. الدعم الأقرب: {levels['support1']:.2f}\n"
            f"2. المقاومة الأقرب: {levels['resistance1']:.2f}\n"
            f"3. الدعوم التالية: {levels['support2']:.2f} / {levels['support3']:.2f}\n"
            f"4. المقاومات التالية: {levels['resistance2']:.2f} / {levels['resistance3']:.2f}\n"
            f"5. منطقة السيولة: {dr['liquidity']}\n\n"
            "🟢 السيناريو الإيجابي\n"
            "استمرار فوق الدعم المحوري + تحسن الزخم والحجم + عدم ظهور تعارض هيكلي.\n\n"
            "🔴 السيناريو السلبي\n"
            "كسر الدعم المحوري مع تأكيد حجمي وهيكلي.\n\n"
            "⏳ الانتظار أم المخاطرة؟\n"
            f"{'المخاطرة مشروطة بالتأكيد' if score >= 73 else 'الانتظار أفضل حتى يتحسن التوافق'}.\n\n"
            "📝 الملاحظة\n"
            f"الترجيح يميل إلى {bias} لأن الاتجاه والزخم والهيكل يدعمونه بدرجات متفاوتة.\n"
            "المتغير الأساسي: كسر أقرب دعم أو مقاومة."
        )
    except Exception as e:
        await safe_reply(update, f"❌ تعذر تنفيذ التحليل اليومي: {e}")


async def quick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await command_lock(update, "quick"):
        return
    try:
        m15_df = get_bars("15m", 300)
        m5_df = get_bars("5m", 300)
        m1_df = get_bars("1m", 300)
        m15, m5, m1 = [analyze_frame(x, scalp=True) for x in (m15_df, m5_df, m1_df)]
        engine = mtf_engine(m15, m5, m1)
        levels = calculate_support_resistance(m5_df, 80)
        result = (
            "⚡ XAU SMART TRADER v16.0 — التحليل السريع\n\n"
            "📖 قراءة أولية\n"
            f"الاتجاه الحالي: {m15['trend']}\n"
            f"منطقة الاهتمام: دعم {levels['support1']:.2f} / مقاومة {levels['resistance1']:.2f}\n"
            f"نسبة التوافق: {engine['score']}%\n\n"
            "🕵️ المحرك متعدد الفريمات\n"
            f"سياق M15: {m15['direction']} — {m15['score']}%\n"
            f"تأكيد M5: {m5['direction']} — {m5['score']}%\n"
            f"إشارة M1: {m1['direction']} — {m1['score']}%\n\n"
            f"🎯 النتيجة: {'🟢 شراء' if engine['direction']=='BUY' else '🔴 بيع' if engine['direction']=='SELL' else '⏳ انتظار'}\n"
            f"💪 التوافق: {engine['score']}%\n"
            f"🛡️ الخطر: {engine['risk']}\n"
            f"🧠 التعارضات: {engine['conflict']}\n"
            f"🕯️ ذيل M15: {'🟢 مؤكد' if engine['wick_ok'] else '🟡 غير مؤكد'}\n\n"
            f"{'🎯 شروط 73% مكتملة' if engine['strict_ready'] else '⏳ لم تكتمل شروط 73%'}\n"
            f"{'💯 إشارة 100% صارمة' if engine['perfect'] else '🔒 لا توجد حالة 100% الآن'}"
        )
        await command_reply(update, "quick", result)
    except Exception as e:
        await safe_reply(update, f"❌ تعذر تنفيذ التحليل السريع: {e}")


async def scalp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await command_lock(update, "scalp"):
        return
    try:
        m15_df = get_bars("15m", 300)
        m5_df = get_bars("5m", 300)
        m1_df = get_bars("1m", 300)

        m15, m5, m1 = [analyze_frame(x, scalp=True)
                        for x in (m15_df, m5_df, m1_df)]
        engine = mtf_engine(m15, m5, m1)

        levels = calculate_support_resistance(m5_df, 80)

        await command_reply(update, "scalp",
            "🕵️ XAU SMART TRADER — Secret Scalp\n\n"
            "📖 قراءة أولية\n"
            f"الاتجاه الحالي: {m15['trend']}\n"
            f"منطقة الاهتمام: دعم {levels['support1']:.2f} / مقاومة {levels['resistance1']:.2f}\n"
            f"نسبة التوافق: {engine['score']}%\n\n"
            "🕵️ المحرك متعدد الفريمات\n"
            f"سياق M15: {m15['direction']} — {m15['score']}%\n"
            f"تأكيد M5: {m5['direction']} — {m5['score']}%\n"
            f"إشارة M1: {m1['direction']} — {m1['score']}%\n\n"
            f"🧭 السياق الأكبر M15: {'🟢 صاعد' if engine['context']=='BUY' else '🔴 هابط' if engine['context']=='SELL' else '🟡 غير واضح'}\n"
            f"🔄 حالة M5: {engine['phase']}\n"
            f"🎯 Trigger M1: {'🟢 شراء' if engine['direction']=='BUY' else '🔴 بيع' if engine['direction']=='SELL' else '⏳ لم يتأكد'}\n"
            f"💪 التوافق: {engine['score']}%\n"
            f"🛡️ الخطر: {engine['risk']}\n"
            f"🧠 التعارضات: {engine['conflict']}\n"
            f"🕯️ ذيل M15: {'🟢 مؤكد' if engine['wick_ok'] else '🟡 غير مؤكد'}\n\n"
            f"{'🎯 شروط 73% مكتملة' if engine['strict_ready'] else '⏳ لم تكتمل شروط 73%'}\n"
            f"{'🕵️ Secret Scalp 85%+ جاهز' if engine['secret_ready'] else '🔒 Secret Scalp غير مكتمل'}\n"
            f"{'💯 إشارة 100% صارمة' if engine['perfect'] else '🔒 لا توجد حالة 100% الآن'}"
        )
    except Exception as e:
        await safe_reply(update, f"❌ تعذر تنفيذ التحليل السريع: {e}")


async def trade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await command_lock(update, "trade"):
        return
    try:
        m15_df = get_bars("15m", 300)
        m5_df = get_bars("5m", 300)
        m1_df = get_bars("1m", 300)

        m15, m5, m1 = [analyze_frame(x, scalp=True)
                        for x in (m15_df, m5_df, m1_df)]
        engine = mtf_engine(m15, m5, m1)

        if not engine["strict_ready"]:
            await command_reply(update, "trade",
                "⏳ لا توجد صفقة عالية الدقة الآن.\n\n"
                f"التوافق: {engine['score']}%\n"
                f"الخطر: {engine['risk']}\n"
                f"التعارض: {engine['conflict']}\n\n"
                "المحرك يراقب M15 → M5 → M1 ولا يطارد الحركة."
            )
            return

        entry, sl, tp1, tp2, levels = build_trade(engine["direction"], m5_df)
        label = "💯 إشارة صارمة 100%" if engine["perfect"] else "🎯 دقة عالية 73%+"

        await command_reply(update, "trade",
            "🤖 XAU SMART TRADER v16.0\n\n"
            f"{label}\n\n"
            f"📈 الاتجاه: {'🟢 شراء' if engine['direction']=='BUY' else '🔴 بيع'}\n"
            f"💪 التوافق: {engine['score']}%\n"
            f"🛡️ الخطر: {engine['risk']}\n"
            f"🧠 التعارض: {engine['conflict']}\n\n"
            f"📍 الدخول: {entry:.2f}\n"
            f"🛑 وقف الخسارة: {sl:.2f}\n"
            f"🎯 الهدف الأول: {tp1:.2f}\n"
            f"🎯 الهدف الثاني: {tp2:.2f}\n\n"
            f"🧱 دعم 1: {levels['support1']:.2f}\n"
            f"🔴 R1: {levels['resistance1']:.2f}\n\n"
            "⚠️ الإشارة تحليلية وليست ضمانًا للربح."
        )
    except Exception as e:
        await safe_reply(update, f"❌ تعذر تشغيل محرك الصفقة: {e}")


# =========================================================
# الصفقات التلقائية
# =========================================================

def auto_window_open():
    now = datetime.now(DAMASCUS).time()
    return AUTO_START <= now <= AUTO_END


def get_auto_signal():
    m15_df = get_bars("15m", 300)
    m5_df = get_bars("5m", 300)
    m1_df = get_bars("1m", 300)

    m15, m5, m1 = [analyze_frame(x, scalp=True)
                    for x in (m15_df, m5_df, m1_df)]
    engine = mtf_engine(m15, m5, m1)

    if not engine["secret_ready"]:
        return None

    direction = engine["direction"]
    entry, sl, tp1, tp2, levels = build_trade(direction, m5_df)
    candle_key = None
    if "openTime" in m5_df.columns and len(m5_df):
        candle_key = str(m5_df["openTime"].iloc[-1])

    return {
        "direction": direction,
        "confidence": engine["score"],
        "risk": engine["risk"],
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "perfect": engine["perfect"],
        "candle_key": candle_key
    }


async def auto_trade_loop():
    while True:
        try:
            if SUBSCRIBERS and auto_window_open():
                signal = await asyncio.to_thread(get_auto_signal)

                if signal:
                    now = time.time()
                    signature = (
                        signal["direction"],
                        round(signal["entry"], 2),
                        round(signal["sl"], 2),
                        round(signal["tp1"], 2),
                        signal.get("candle_key"),
                        signal["perfect"]
                    )

                    for chat_id in list(SUBSCRIBERS):
                        previous = LAST_AUTO_SIGNAL.get(chat_id)
                        previous_time = LAST_AUTO_TIME.get(chat_id, 0)

                        # Replace old pending signal instead of accumulating duplicates.
                        if signature == previous and now - previous_time < 7200:
                            continue

                        text = (
                            "🚨 XAU SMART TRADER v16.0\n\n"
                            f"{'💯 إشارة صارمة 100%' if signal['perfect'] else '🕵️ Secret Scalp 85%+'}\n\n"
                            f"📈 الاتجاه: {'🟢 شراء' if signal['direction']=='BUY' else '🔴 بيع'}\n"
                            f"💪 التوافق: {signal['confidence']}%\n"
                            f"🛡️ الخطر: {signal['risk']}\n\n"
                            f"📍 الدخول: {signal['entry']:.2f}\n"
                            f"🛑 وقف الخسارة: {signal['sl']:.2f}\n"
                            f"🎯 الهدف الأول: {signal['tp1']:.2f}\n"
                            f"🎯 الهدف الثاني: {signal['tp2']:.2f}\n\n"
                            "🕯️ يفضّل انتظار تأكيد الشمعة."
                        )

                        try:
                            await APPLICATION.bot.send_message(
                                chat_id=chat_id, text=text
                            )
                            LAST_AUTO_SIGNAL[chat_id] = signature
                            LAST_AUTO_TIME[chat_id] = now
                        except Exception:
                            logger.exception("Auto send error")

        except Exception:
            logger.exception("Auto trade error")

        await asyncio.sleep(AUTO_CHECK_SECONDS)


async def toggle_auto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if chat_id in SUBSCRIBERS:
        SUBSCRIBERS.discard(chat_id)
        LAST_AUTO_SIGNAL.pop(chat_id, None)
        LAST_AUTO_TIME.pop(chat_id, None)
        state = "🔴 إيقاف"
        message = "🔴 تم إيقاف الصفقات التلقائية."
    else:
        SUBSCRIBERS.add(chat_id)
        state = "🟢 تشغيل"
        message = (
            "🟢 تم تشغيل الصفقات التلقائية.\n"
            "لن تتراكم الإشارات المكررة، وتعمل داخل نافذة اليوم فقط."
        )

    await update.message.reply_text(message)
    await show_main_menu(update, context)


async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Compatibility for /subscribe.
    SUBSCRIBERS.add(update.effective_chat.id)
    LAST_AUTO_SIGNAL.pop(update.effective_chat.id, None)
    LAST_AUTO_TIME.pop(update.effective_chat.id, None)
    await update.message.reply_text("🟢 تم تشغيل الصفقات التلقائية.")
    await show_main_menu(update, context)


async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Compatibility for /unsubscribe.
    SUBSCRIBERS.discard(update.effective_chat.id)
    LAST_AUTO_SIGNAL.pop(update.effective_chat.id, None)
    LAST_AUTO_TIME.pop(update.effective_chat.id, None)
    await update.message.reply_text("🔴 تم إيقاف الصفقات التلقائية.")
    await show_main_menu(update, context)


# =========================================================
# MARKETS / SUPPORT
# =========================================================

async def markets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(DAMASCUS)
    await update.message.reply_text(
        "🌍 مواعيد الأسواق — توقيت دمشق\n\n"
        "🌏 سيدني: 12:00 ص\n"
        "🇯🇵 طوكيو: 3:00 ص\n"
        "🇬🇧 لندن: 10:00 ص\n"
        "🇺🇸 نيويورك: 4:00 م\n\n"
        "🔔 التنبيه: قبل افتتاح الجلسة بـ15 دقيقة.\n"
        f"🕐 الآن: {now.strftime('%I:%M %p')}\n\n"
        "⚠️ ملاحظة: التوقيتات العالمية تتأثر بالتوقيت الصيفي؛ "
        "موعد نيويورك المعروض هنا مضبوط على طلب v14."
    )


async def support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🆘 الدعم\n\n"
        "للتواصل مع الدعم:\n"
        "@Morhafsy"
    )


# =========================================================
# SAFE REPLY
# =========================================================

async def safe_reply(update, text):
    try:
        if update and update.message:
            await update.message.reply_text(text)
    except Exception:
        logger.exception("Reply error")


# =========================================================
# WEBHOOK
# =========================================================

@app.route(WEBHOOK_PATH, methods=["POST"])
def telegram_webhook():
    if APPLICATION is None:
        return "Bot not ready", 503

    try:
        update_data = request.get_json(force=True)
        update = Update.de_json(update_data, APPLICATION.bot)
        asyncio.run_coroutine_threadsafe(
            APPLICATION.process_update(update),
            BOT_LOOP
        )
        return "OK", 200
    except Exception:
        logger.exception("Webhook error")
        return "OK", 200


# =========================================================
# APPLICATION
# =========================================================

async def start_application():
    global APPLICATION, BOT_STARTED

    if not TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN غير موجود في Environment Variables.")

    APPLICATION = Application.builder().token(TOKEN).build()

    # الأوامر البرمجية تبقى مخفية عن لوحة المستخدم، وتستخدم للتوافق فقط.
    handlers = {
        "status": status, "price": price, "levels": levels,
        "weekly": weekly, "daily": daily, "scalp": scalp,
        "trade": trade, "subscribe": subscribe, "unsubscribe": unsubscribe,
        "markets": markets, "support": support,
    }
    for name, fn in handlers.items():
        APPLICATION.add_handler(CommandHandler(name, fn))

    # /start توافق خلفي فقط؛ لا يظهر ضمن لوحة البوت.
    APPLICATION.add_handler(CommandHandler("start", show_main_menu))
    # لوحة عربية مباشرة: كل زر يشغّل الوظيفة المقابلة بدون أوامر ظاهرة.
    APPLICATION.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, button_router))

    await APPLICATION.initialize()
    await APPLICATION.start()

    if not WEBHOOK_URL.startswith(("https://", "http://")):
        raise RuntimeError(f"رابط Webhook غير صالح: {WEBHOOK_URL}")

    await APPLICATION.bot.set_webhook(
        url=WEBHOOK_URL,
        allowed_updates=["message"],
        drop_pending_updates=True
    )

    BOT_STARTED = True
    logger.info("XAU SMART TRADER v16.0 started: %s", WEBHOOK_URL)

    # Daily/weekly scheduler. If JobQueue is unavailable in the installed
    # python-telegram-bot package, the explicit loops below still work.
    asyncio.create_task(auto_trade_loop())
    asyncio.create_task(scheduler_loop())

    while True:
        await asyncio.sleep(3600)


async def scheduler_loop():
    last_weekly_key = None
    last_market_alert = None

    while True:
        try:
            now = datetime.now(DAMASCUS)

            # Weekly report every Sunday at 19:00.
            if now.weekday() == 6 and now.time() >= dtime(19, 0):
                key = current_week_key()
                if key != last_weekly_key:
                    try:
                        report = build_weekly_report()
                        for chat_id in list(SUBSCRIBERS):
                            await APPLICATION.bot.send_message(
                                chat_id=chat_id, text=report
                            )
                        last_weekly_key = key
                    except Exception:
                        logger.exception("Weekly scheduler error")

            # Keep the latest weekly report available all week. It is replaced
            # by the new report next Sunday at 19:00 Damascus.

            # 15-minute pre-New-York alert. المطلوب في الإصدار 16:00 Damascus.
            alert_at = (datetime.combine(
                now.date(), dtime(16, 0), tzinfo=DAMASCUS
            ) - timedelta(minutes=15))

            alert_key = alert_at.strftime("%Y-%m-%d")
            if (
                now.hour == alert_at.hour
                and now.minute == alert_at.minute
                and last_market_alert != alert_key
            ):
                for chat_id in list(SUBSCRIBERS):
                    try:
                        await APPLICATION.bot.send_message(
                            chat_id=chat_id,
                            text="🔔 تنبيه: تبقى 15 دقيقة على افتتاح نيويورك حسب توقيت v14.1 (4:00 م دمشق)."
                        )
                    except Exception:
                        logger.exception("Market alert error")
                last_market_alert = alert_key

        except Exception:
            logger.exception("Scheduler error")

        await asyncio.sleep(30)


# =========================================================
# SERVER
# =========================================================

def run_server():
    app.run(
        host="0.0.0.0",
        port=PORT,
        debug=False,
        threaded=True,
        use_reloader=False
    )


def main():
    global BOT_LOOP

    server = threading.Thread(target=run_server, daemon=True)
    server.start()

    loop = asyncio.new_event_loop()
    BOT_LOOP = loop
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(start_application())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()
