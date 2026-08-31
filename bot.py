# ============================================================
# XAU SMART TRADER v18.0
# Structural Liquidity + Quantitative Momentum
# واجهة عربية بالكامل - توقيت دمشق
#
# v17.2:
# - إصلاح كامل للتقرير الأسبوعي
# - إضافة التقرير التوضيحي الأسبوعي
# - إضافة التقرير التوضيحي اليومي
# - توليد السيناريوهات من بيانات السوق الفعلية
# - ربط السيناريوهات بالدعوم والمقاومات والأهداف
# - إظهار جودة التحليل بالنقاط
# - الحفاظ على التحليل W1/D1/H4/H1/M15
# ============================================================

import os
import asyncio
import threading
import time
import logging
import json
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import pandas as pd
import numpy as np

from flask import Flask, request, jsonify
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

# ============================================================
# الإعدادات
# ============================================================

VERSION = "v18.0"
TOKEN = os.environ.get("TELEGRAM_TOKEN")
PORT = int(os.environ.get("PORT", "10000"))

RENDER_URL = os.environ.get(
    "RENDER_EXTERNAL_URL",
    "https://xau-smart-bot.onrender.com"
).rstrip("/")

WEBHOOK_PATH = "/telegram-webhook"
WEBHOOK_URL = RENDER_URL + WEBHOOK_PATH

SYMBOL = "XAUUSD"
DATA_URL = "https://biquote.io/api/XAUUSD/ohlc"
LIVE_URL = "https://biquote.io/api/XAUUSD"

DAMASCUS = ZoneInfo("Asia/Damascus")

CACHE_SECONDS = 20
AUTO_SCAN_SECONDS = 60
MIN_BARS = 30

# عتبات الإشارة الحالية في هذا الإصدار
MIN_TRADE_SCORE = 50
QUALITY_GOOD = 60
QUALITY_STRONG = 70
QUALITY_VERY_STRONG = 80
QUALITY_EXCELLENT = 90
SIGNAL_THRESHOLD = MIN_TRADE_SCORE
STRONG_THRESHOLD = QUALITY_VERY_STRONG

AUTO_ENABLED = True
NEWS_FILTER_ENABLED = os.environ.get("NEWS_FILTER_ENABLED", "true").lower() == "true"
NEWS_BEFORE_MIN = 30
NEWS_AFTER_MIN = 30
NEWS_CACHE_SECONDS = 300
# ============================================================
# 💳 نظام الاشتراكات - الباقات والأسعار
# ============================================================

PLANS = {
    "FREE": {
        "name": "🆓 FREE",
        "price": 0,
        "trade_limit": 1,
        "trade_period_days": 7,
    },

    "BASIC": {
        "name": "🥉 BASIC",
        "price": 10,
        "trade_limit": 5,
        "trade_period_days": 30,
    },

    "PRO": {
        "name": "🥈 PRO",
        "price": 20,
        "trade_limit": 20,
        "trade_period_days": 30,
    },

    "PREMIUM": {
        "name": "🥇 PREMIUM",
        "price": 35,
        "trade_limit": 50,
        "trade_period_days": 30,
    },

    "VIP": {
        "name": "💎 VIP",
        "price": 50,
        "trade_limit": None,
        "trade_period_days": 30,
    },
}

# الحد الأقصى للصفقات حسب الباقة
TRADE_LIMITS = {
    "FREE": 1,
    "BASIC": 5,
    "PRO": 20,
    "PREMIUM": 50,
    "VIP": None,
}
# ============================================================
# 💳 قاعدة بيانات المستخدمين والاشتراكات
# ============================================================

SUBSCRIPTION_DB = "subscriptions.db"


def init_subscription_db():
    conn = sqlite3.connect(SUBSCRIPTION_DB)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS subscribers (
            chat_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            plan TEXT NOT NULL DEFAULT 'FREE',
            status TEXT NOT NULL DEFAULT 'active',
            start_date TEXT,
            expiry_date TEXT,
            trades_used INTEGER DEFAULT 0,
            referral_code TEXT,
            referred_by INTEGER
        )
    """)

    conn.commit()
    conn.close()


def get_subscription(chat_id):
    conn = sqlite3.connect(SUBSCRIPTION_DB)
    conn.row_factory = sqlite3.Row

    row = conn.execute(
        "SELECT * FROM subscribers WHERE chat_id = ?",
        (chat_id,)
    ).fetchone()

    conn.close()
    return row


def create_free_user(chat_id, username=None, first_name=None):
    if get_subscription(chat_id):
        return

    conn = sqlite3.connect(SUBSCRIPTION_DB)

    conn.execute("""
        INSERT INTO subscribers (
            chat_id,
            username,
            first_name,
            plan,
            status,
            start_date,
            trades_used,
            referral_code
        )
        VALUES (?, ?, ?, 'FREE', 'active', ?, 0, ?)
    """, (
        chat_id,
        username,
        first_name,
        datetime.now().isoformat(),
        f"REF{chat_id}"
    ))

    conn.commit()
    conn.close()


def get_user_plan(chat_id):
    user = get_subscription(chat_id)

    if not user:
        return "FREE"

    return user["plan"]


def is_subscription_active(chat_id):
    user = get_subscription(chat_id)

    if not user:
        return True

    if user["plan"] == "FREE":
        return True

    if not user["expiry_date"]:
        return False

    try:
        expiry = datetime.fromisoformat(user["expiry_date"])
        return datetime.now() < expiry
    except Exception:
        return False
app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ============================================================
# الحالة
# ============================================================

APPLICATION = None
BOT_LOOP = None
SUBSCRIBERS = set()
DATA_CACHE = {}
LAST_SIGNAL = {}
NEWS_CACHE = {"time": 0, "events": []}
TRADE_HISTORY = []
LAST_ANALYSIS = None
MAX_TRADE_HISTORY = 500

# ============================================================
# Flask
# ============================================================

@app.route("/", methods=["GET", "HEAD"])
def home():
    return f"XAU SMART TRADER {VERSION} - OK", 200

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "bot": VERSION,
        "symbol": SYMBOL,
        "timezone": "Asia/Damascus",
        "time": datetime.now(DAMASCUS).isoformat()
    }), 200

# ============================================================
# أدوات عامة
# ============================================================

def sf(value, default=0.0):
    try:
        x = float(value)
        if np.isfinite(x):
            return x
    except Exception:
        pass
    return default


def now_damascus():
    return datetime.now(DAMASCUS)


def fmt(value):
    if value is None:
        return "غير متوفر"
    return f"{sf(value):.2f}"


def cache_key(interval, limit):
    return f"{SYMBOL}_{interval}_{limit}"

# ============================================================
# البيانات
# ============================================================

def get_bars(interval, limit=300):
    """جلب بيانات Biquote وبناء W1 محلياً من D1."""

    if interval == "1w":
        key = f"1w_{limit}"
        now = time.time()
        cached = DATA_CACHE.get(key)
        if cached and now - cached[0] < CACHE_SECONDS:
            return cached[1].copy()

        daily_limit = min(max(limit * 7 + 30, 100), 1000)
        daily_df = get_bars("1d", daily_limit)
        if daily_df is None or daily_df.empty:
            raise ValueError("لا توجد بيانات يومية لبناء W1.")
        if "openTime" not in daily_df.columns:
            raise ValueError("بيانات D1 لا تحتوي على openTime.")

        df = daily_df.copy()
        df["openTime"] = pd.to_datetime(df["openTime"], utc=True, errors="coerce")
        df = df.dropna(subset=["openTime"]).set_index("openTime").sort_index()

        for col in ["open", "high", "low", "close"]:
            if col not in df.columns:
                raise ValueError(f"بيانات D1 ناقصة: {col}")
            df[col] = pd.to_numeric(df[col], errors="coerce")

        if "tickVolume" in df.columns:
            df["tickVolume"] = pd.to_numeric(df["tickVolume"], errors="coerce").fillna(0)
        else:
            df["tickVolume"] = 0

        df = df.dropna(subset=["open", "high", "low", "close"])
        if len(df) < MIN_BARS:
            raise ValueError(f"بيانات D1 غير كافية لبناء W1: {len(df)} شمعة.")

        weekly = pd.DataFrame(index=df.resample("W-SUN").size().index)
        weekly["open"] = df["open"].resample("W-SUN").first()
        weekly["high"] = df["high"].resample("W-SUN").max()
        weekly["low"] = df["low"].resample("W-SUN").min()
        weekly["close"] = df["close"].resample("W-SUN").last()
        weekly["tickVolume"] = df["tickVolume"].resample("W-SUN").sum()
        weekly = weekly.dropna(subset=["open", "high", "low", "close"]).tail(limit).reset_index()
        weekly.rename(columns={weekly.columns[0]: "openTime"}, inplace=True)

        if len(weekly) < MIN_BARS:
            raise ValueError(f"البيانات الأسبوعية غير كافية: {len(weekly)} شمعة.")

        DATA_CACHE[key] = (now, weekly.copy())
        return weekly.copy()

    supported = {"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
    if interval not in supported:
        raise ValueError(f"الفريم {interval} غير مدعوم.")

    key = cache_key(interval, limit)
    now = time.time()
    cached = DATA_CACHE.get(key)
    if cached and now - cached[0] < CACHE_SECONDS:
        return cached[1].copy()

    try:
        response = requests.get(
            DATA_URL,
            params={"interval": interval, "limit": min(int(limit), 1000)},
            timeout=15
        )
        if not response.ok:
            try:
                detail = response.json()
            except Exception:
                detail = response.text[:500]
            raise RuntimeError(f"Biquote HTTP {response.status_code}: {detail}")
        data = response.json()
    except requests.RequestException as exc:
        raise RuntimeError(f"تعذر الاتصال بمصدر البيانات للفريم {interval}: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError(f"استجابة Biquote غير متوقعة للفريم {interval}.")

    bars = data.get("bars", [])
    if not bars:
        raise ValueError(f"Biquote لم يعط بيانات للفريم {interval}.")

    df = pd.DataFrame(bars)
    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"البيانات ناقصة: {col}")
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if "tickVolume" in df.columns:
        df["tickVolume"] = pd.to_numeric(df["tickVolume"], errors="coerce").fillna(0)
    else:
        df["tickVolume"] = 0

    if "openTime" in df.columns:
        df["openTime"] = pd.to_datetime(df["openTime"], utc=True, errors="coerce")
        df = df.dropna(subset=["openTime"]).sort_values("openTime")

    df = df.dropna(subset=["open", "high", "low", "close"])
    if len(df) < MIN_BARS:
        raise ValueError(f"البيانات غير كافية للفريم {interval}: {len(df)} شمعة.")

    DATA_CACHE[key] = (now, df.copy())
    return df.copy()

# ============================================================
# السعر اللحظي
# ============================================================

def live_price():
    errors = []
    try:
        r = requests.get(LIVE_URL, params={"allowStale": "false"}, timeout=8)
        if r.ok:
            data = r.json()
            if isinstance(data, dict):
                price = sf(data.get("mid"), None)
                if price is None:
                    bid = sf(data.get("bid"), None)
                    ask = sf(data.get("ask"), None)
                    if bid and ask:
                        price = (bid + ask) / 2
                if price and price > 0:
                    return {"price": price, "source": "Biquote", "age": data.get("quoteAgeSeconds")}
    except Exception as e:
        errors.append(str(e))

    try:
        r = requests.get("https://xaus.com/api/v1/spot", timeout=8)
        if r.ok:
            data = r.json()
            price = sf(data.get("spot_usd_oz"), None)
            if price and price > 0:
                state = data.get("data_state", {})
                return {"price": price, "source": "XAUS", "age": state.get("age_seconds")}
    except Exception as e:
        errors.append(str(e))

    raise RuntimeError("تعذر الحصول على السعر اللحظي: " + " | ".join(errors))

# ============================================================
# المؤشرات
# ============================================================

def EMA(s, n):
    return s.ewm(span=n, adjust=False).mean()


def RSI(s, n=14):
    delta = s.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / n, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / n, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).fillna(50)


def MACD(s, fast=8, slow=21, signal=5):
    fast_line = EMA(s, fast)
    slow_line = EMA(s, slow)
    line = fast_line - slow_line
    sig = EMA(line, signal)
    return line, sig, line - sig


def ATR(df, n=14):
    prev = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev).abs(),
        (df["low"] - prev).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / n, adjust=False).mean()


def ADX(df, n=14):
    high, low, close = df["high"], df["low"], df["close"]
    up = high.diff()
    down = -low.diff()
    plus_dm = pd.Series(np.where((up > down) & (up > 0), up, 0), index=df.index)
    minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0), index=df.index)
    prev = close.shift(1)
    tr = pd.concat([high - low, (high - prev).abs(), (low - prev).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / n, adjust=False).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / n, adjust=False).mean() / atr.replace(0, np.nan)
    minus_di = 100 * minus_dm.ewm(alpha=1 / n, adjust=False).mean() / atr.replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return dx.fillna(0).ewm(alpha=1 / n, adjust=False).mean()

# ============================================================
# هيكل السوق والسيولة والفيبوناتشي و FVG
# ============================================================

def structure(df):
    if len(df) < 10:
        return "محايد"
    h_now, h_old = sf(df["high"].iloc[-1]), sf(df["high"].iloc[-5])
    l_now, l_old = sf(df["low"].iloc[-1]), sf(df["low"].iloc[-5])
    if h_now > h_old and l_now > l_old:
        return "صاعد"
    if h_now < h_old and l_now < l_old:
        return "هابط"
    return "محايد"


def volume_analysis(df):
    volume = df["tickVolume"].astype(float)
    current = sf(volume.iloc[-1])
    average = sf(volume.tail(20).mean(), 1)
    ratio = current / average if average > 0 else 0
    if ratio >= 1.40:
        state = "قوية جداً"
    elif ratio >= 1.10:
        state = "قوية"
    elif ratio >= 0.85:
        state = "طبيعية"
    else:
        state = "ضعيفة"
    return state, ratio


def fibonacci(df, lookback=120):
    x = df.tail(min(lookback, len(df)))
    high, low = sf(x["high"].max()), sf(x["low"].min())
    span = high - low
    if span <= 0:
        return {}
    return {
        "0": high,
        "23.6": high - span * 0.236,
        "38.2": high - span * 0.382,
        "50": high - span * 0.500,
        "61.8": high - span * 0.618,
        "78.6": high - span * 0.786,
        "100": low
    }


def find_fvg(df):
    if len(df) < 5:
        return None
    a, c = df.iloc[-3], df.iloc[-1]
    if sf(c["low"]) > sf(a["high"]):
        return {"type": "صاعدة", "low": sf(a["high"]), "high": sf(c["low"])}
    if sf(c["high"]) < sf(a["low"]):
        return {"type": "هابطة", "low": sf(c["high"]), "high": sf(a["low"])}
    return None


def support_resistance(df, lookback=120):
    x = df.tail(min(lookback, len(df)))
    current = sf(x["close"].iloc[-1])
    atr = sf(ATR(x).iloc[-1], 1)
    radius = max(atr * 0.35, current * 0.00035)
    supports, resistances = [], []

    for i in range(2, len(x) - 2):
        low, high = sf(x["low"].iloc[i]), sf(x["high"].iloc[i])
        left_low = sf(x["low"].iloc[i-2:i].min())
        right_low = sf(x["low"].iloc[i+1:i+3].min())
        left_high = sf(x["high"].iloc[i-2:i].max())
        right_high = sf(x["high"].iloc[i+1:i+3].max())
        if low <= left_low and low <= right_low and low < current:
            supports.append(low)
        if high >= left_high and high >= right_high and high > current:
            resistances.append(high)

    def cluster(values):
        values = sorted(values)
        groups = []
        for price in values:
            if not groups:
                groups.append([price])
                continue
            center = sum(groups[-1]) / len(groups[-1])
            if abs(price - center) <= radius:
                groups[-1].append(price)
            else:
                groups.append([price])
        zones = []
        for group in groups:
            center = sum(group) / len(group)
            strength = min(100, 40 + len(group) * 15)
            zones.append({"price": center, "strength": strength, "touches": len(group)})
        return zones

    s = sorted(cluster(supports), key=lambda z: abs(current - z["price"]))[:3]
    r = sorted(cluster(resistances), key=lambda z: abs(current - z["price"]))[:3]
    return {
        "support1": s[0] if len(s) > 0 else None,
        "support2": s[1] if len(s) > 1 else None,
        "support3": s[2] if len(s) > 2 else None,
        "resistance1": r[0] if len(r) > 0 else None,
        "resistance2": r[1] if len(r) > 1 else None,
        "resistance3": r[2] if len(r) > 2 else None,
        "atr": atr
    }

# ============================================================
# تحليل فريم
# ============================================================

def trade_quality(score):
    score = int(max(0, min(100, score)))
    if score >= QUALITY_EXCELLENT:
        return "ممتازة", "🏆"
    if score >= QUALITY_VERY_STRONG:
        return "قوية جداً", "🔥"
    if score >= QUALITY_STRONG:
        return "قوية", "💪"
    if score >= QUALITY_GOOD:
        return "جيدة", "🎯"
    if score >= MIN_TRADE_SCORE:
        return "مراقبة", "🟡"
    return "ضعيفة", "⚪"


def institutional_analysis(df):
    """قراءة مؤسسية مبسطة من السيولة والحجم والبنية، بلا ادعاء بيانات مؤسسية مباشرة."""
    price = sf(df["close"].iloc[-1])
    atr = max(sf(ATR(df).iloc[-1], 1), 1e-9)
    vol_state, vol_ratio = volume_analysis(df)
    struct = structure(df)
    recent = df.tail(min(20, len(df)))
    high = sf(recent["high"].max())
    low = sf(recent["low"].min())
    pos = (price-low)/(high-low) if high > low else 0.5
    bull = 0; bear = 0; factors=[]
    if struct == "صاعد": bull += 30; factors.append("بنية سعرية صاعدة")
    elif struct == "هابط": bear += 30; factors.append("بنية سعرية هابطة")
    if vol_ratio >= 1.20:
        if price > recent["close"].iloc[-2]: bull += 25; factors.append("حجم مرتفع مع دفع سعري")
        elif price < recent["close"].iloc[-2]: bear += 25; factors.append("حجم مرتفع مع ضغط سعري")
    if pos <= 0.25: bull += 20; factors.append("السعر في الجزء السفلي من النطاق")
    elif pos >= 0.75: bear += 20; factors.append("السعر في الجزء العلوي من النطاق")
    if bull > bear: direction="BUY"; quality=min(100,bull)
    elif bear > bull: direction="SELL"; quality=min(100,bear)
    else: direction="WAIT"; quality=0
    return {"direction":direction,"quality":quality,"bull":bull,"bear":bear,"volume_ratio":vol_ratio,"atr":atr,"factors":factors}


def institutional_adjustment(direction, institutional):
    if direction not in ("BUY","SELL"):
        return 0, []
    points=0; factors=[]
    if institutional.get("direction") == direction:
        points += 10; factors.append("العامل المؤسسي متوافق مع الاتجاه")
    elif institutional.get("direction") not in ("WAIT", direction):
        points -= 5; factors.append("العامل المؤسسي يعارض الاتجاه")
    if institutional.get("volume_ratio",0) >= 1.20:
        points += 5; factors.append("نشاط حجمي مرتفع")
    return max(-10,min(15,points)), factors


def analyze(df):
    close=df["close"]; price=sf(close.iloc[-1])
    ema50=sf(EMA(close,50).iloc[-1]); ema200=sf(EMA(close,200).iloc[-1])
    rsi=sf(RSI(close,14).iloc[-1],50)
    macd_line, macd_sig, macd_hist=MACD(close)
    ml,ms,mh=sf(macd_line.iloc[-1]),sf(macd_sig.iloc[-1]),sf(macd_hist.iloc[-1])
    adx=sf(ADX(df).iloc[-1]); struct=structure(df)
    vol_state,vol_ratio=volume_analysis(df); fib=fibonacci(df); fvg=find_fvg(df); atr=sf(ATR(df).iloc[-1],1)
    bull=bear=0; reasons=[]
    if price>ema50: bull+=12; reasons.append("السعر فوق EMA50")
    elif price<ema50: bear+=12; reasons.append("السعر تحت EMA50")
    if price>ema200: bull+=15; reasons.append("السعر فوق EMA200")
    elif price<ema200: bear+=15; reasons.append("السعر تحت EMA200")
    if rsi<30: bull+=15; reasons.append("RSI تشبع بيعي")
    elif rsi>70: bear+=15; reasons.append("RSI تشبع شرائي")
    elif rsi>50: bull+=7
    elif rsi<50: bear+=7
    if ml>ms: bull+=12; bull += 4 if mh>0 else 0; reasons.append("MACD إيجابي")
    elif ml<ms: bear+=12; bear += 4 if mh<0 else 0; reasons.append("MACD سلبي")
    if adx>=25:
        if bull>bear: bull+=8
        elif bear>bull: bear+=8
    elif adx>=18:
        if bull>bear: bull+=4
        elif bear>bull: bear+=4
    if struct=="صاعد": bull+=12; reasons.append("هيكل السوق صاعد")
    elif struct=="هابط": bear+=12; reasons.append("هيكل السوق هابط")
    if vol_ratio>=1.20:
        if bull>bear: bull+=8; reasons.append("توسع حجمي داعم للشراء")
        elif bear>bull: bear+=8; reasons.append("توسع حجمي داعم للبيع")
    elif vol_ratio>=0.90:
        if bull>bear: bull+=3
        elif bear>bull: bear+=3
    if bull>bear: direction,score="BUY",bull
    elif bear>bull: direction,score="SELL",bear
    else: direction,score="WAIT",0
    score=int(max(0,min(100,round(score))))
    quality,quality_icon=trade_quality(score)
    return {"price":price,"ema50":ema50,"ema200":ema200,"rsi":rsi,"macd":ml,"macd_signal":ms,"macd_hist":mh,"adx":adx,"structure":struct,"volume_state":vol_state,"volume_ratio":vol_ratio,"fib":fib,"fvg":fvg,"atr":atr,"direction":direction,"score":score,"state":quality,"quality":quality,"quality_icon":quality_icon,"reasons":reasons}


# ============================================================
# التحليل متعدد الفريمات
# ============================================================

def multi_timeframe():
    w1 = analyze(get_bars("1w", 250))
    d1 = analyze(get_bars("1d", 300))
    h4 = analyze(get_bars("4h", 300))
    h1 = analyze(get_bars("1h", 300))
    m15 = analyze(get_bars("15m", 300))

    frames = [w1, d1, h4, h1, m15]
    weights = [0.15, 0.25, 0.25, 0.20, 0.15]
    buy_score = sum(x["score"] * w for x, w in zip(frames, weights) if x["direction"] == "BUY")
    sell_score = sum(x["score"] * w for x, w in zip(frames, weights) if x["direction"] == "SELL")

    if buy_score > sell_score:
        final_direction = "BUY"
    elif sell_score > buy_score:
        final_direction = "SELL"
    else:
        final_direction = "WAIT"

    directional_weight = sum(w for x, w in zip(frames, weights) if x["direction"] == final_direction)
    if directional_weight > 0:
        final_score = int(round(
            sum(x["score"] * w for x, w in zip(frames, weights) if x["direction"] == final_direction)
            / directional_weight
        ))
    else:
        final_score = 0

    return {
        "w1": w1, "d1": d1, "h4": h4, "h1": h1, "m15": m15,
        "direction": final_direction, "score": min(final_score, 100),
        "buy_score": round(buy_score, 1), "sell_score": round(sell_score, 1)
    }

# ============================================================
# أدوات السيناريوهات
# ============================================================

def zone_price(levels, key):
    z = levels.get(key)
    return z["price"] if z else None


def nearest_support(levels, price):
    vals = [zone_price(levels, k) for k in ("support1", "support2", "support3")]
    vals = [v for v in vals if v is not None and v < price]
    return max(vals) if vals else None


def next_support(levels, price):
    vals = [zone_price(levels, k) for k in ("support1", "support2", "support3")]
    vals = sorted(v for v in vals if v is not None and v < price)
    return vals[-2] if len(vals) >= 2 else (vals[0] if vals else None)


def nearest_resistance(levels, price):
    vals = [zone_price(levels, k) for k in ("resistance1", "resistance2", "resistance3")]
    vals = [v for v in vals if v is not None and v > price]
    return min(vals) if vals else None


def next_resistance(levels, price):
    vals = sorted(v for v in [zone_price(levels, k) for k in ("resistance1", "resistance2", "resistance3")] if v is not None and v > price)
    return vals[1] if len(vals) >= 2 else (vals[0] if vals else None)


def format_zone(levels, key):
    z = levels.get(key)
    if not z:
        return "غير متوفر"
    return f"{z['price']:.2f} | قوة {z['strength']}/100 | لمسات {z['touches']}"


def scenario_quality(mtf, direction, levels, price, preferred=True):
    score = 0
    f = []
    d1, h4, h1, m15 = mtf["d1"], mtf["h4"], mtf["h1"], mtf["m15"]

    if direction == "BUY":
        if d1["direction"] == "BUY": score += 15; f.append("D1 داعم للشراء")
        if h4["direction"] == "BUY": score += 15; f.append("H4 داعم للشراء")
        if h4["structure"] == "صاعد": score += 12; f.append("هيكل H4 صاعد")
        if m15["direction"] == "BUY": score += 10; f.append("زخم M15 إيجابي")
        if m15["volume_ratio"] >= 1.10: score += 8; f.append("حجم داعم")
        s = nearest_support(levels, price)
        r = nearest_resistance(levels, price)
        if s is not None and abs(price - s) <= max(m15["atr"] * 1.5, price * 0.002):
            score += 15; f.append("السعر قريب من دعم مهم")
        if r is not None and r > price:
            score += 5
    else:
        if d1["direction"] == "SELL": score += 15; f.append("D1 داعم للبيع")
        if h4["direction"] == "SELL": score += 15; f.append("H4 داعم للبيع")
        if h4["structure"] == "هابط": score += 12; f.append("هيكل H4 هابط")
        if m15["direction"] == "SELL": score += 10; f.append("زخم M15 سلبي")
        if m15["volume_ratio"] >= 1.10: score += 8; f.append("حجم داعم")
        r = nearest_resistance(levels, price)
        s = nearest_support(levels, price)
        if r is not None and abs(price - r) <= max(m15["atr"] * 1.5, price * 0.002):
            score += 15; f.append("السعر قريب من مقاومة مهمة")
        if s is not None and s < price:
            score += 5

    if not preferred:
        score = max(0, score - 8)
    return min(score, 100), f


def build_trade(direction, h1, m15, levels):
    price = m15["price"]
    atr = max(m15["atr"], 0.50)
    s1, s2 = levels.get("support1"), levels.get("support2")
    r1, r2 = levels.get("resistance1"), levels.get("resistance2")

    if direction == "BUY":
        entry = price
        sl = min(entry - atr * 1.10, s1["price"] - atr * 0.20) if s1 else entry - atr * 1.20
        tp1 = r1["price"] if r1 and r1["price"] > entry else entry + atr * 1.50
        tp2 = r2["price"] if r2 and r2["price"] > tp1 else entry + atr * 2.50
    elif direction == "SELL":
        entry = price
        sl = max(entry + atr * 1.10, r1["price"] + atr * 0.20) if r1 else entry + atr * 1.20
        tp1 = s1["price"] if s1 and s1["price"] < entry else entry - atr * 1.50
        tp2 = s2["price"] if s2 and s2["price"] < tp1 else entry - atr * 2.50
    else:
        return None

    risk = abs(entry - sl)
    reward = abs(tp2 - entry)
    rr = reward / risk if risk > 0 else 0
    return {"entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "rr": rr}

# ============================================================
# الأخبار
# ============================================================

def get_news_events():
    # لا نخترع أخباراً. يمكن ربط مصدر اقتصادي لاحقاً.
    return []


def news_filter():
    if not NEWS_FILTER_ENABLED:
        return False, "فلتر الأخبار غير مفعّل"
    try:
        now = time.time()
        if now - NEWS_CACHE["time"] > NEWS_CACHE_SECONDS:
            NEWS_CACHE["events"] = get_news_events()
            NEWS_CACHE["time"] = now
        current = now_damascus()
        for event in NEWS_CACHE["events"]:
            event_time = event.get("time")
            if not event_time:
                continue
            before = event_time - timedelta(minutes=NEWS_BEFORE_MIN)
            after = event_time + timedelta(minutes=NEWS_AFTER_MIN)
            if before <= current <= after:
                return True, "🚨 التداول محجوب مؤقتاً بسبب خبر اقتصادي عالي التأثير."
        return False, "🟢 لا يوجد حجب إخباري مسجل حالياً."
    except Exception:
        return False, "🟡 تعذر تحديث المفكرة الاقتصادية؛ لم يتم اختراع خبر."

# ============================================================
# تقييم الإشارة
# ============================================================

def evaluate_signal():
    global LAST_ANALYSIS
    mtf=multi_timeframe(); direction=mtf["direction"]; base_score=mtf["score"]
    d1,h4,h1,m15=mtf["d1"],mtf["h4"],mtf["h1"],mtf["m15"]
    quote=live_price(); price=quote["price"]
    levels=support_resistance(get_bars("1h",250))
    institutional=institutional_analysis(get_bars("1h",250))
    confluence=0; factors=[]
    if direction=="BUY":
        if d1["price"]>d1["ema200"]: confluence+=15; factors.append("الاتجاه اليومي فوق EMA200")
        if h4["price"]>h4["ema200"]: confluence+=15; factors.append("H4 فوق EMA200")
        if h4["structure"]=="صاعد": confluence+=12; factors.append("هيكل H4 صاعد")
        if m15["macd"]>m15["macd_signal"]: confluence+=10; factors.append("MACD M15 إيجابي")
    elif direction=="SELL":
        if d1["price"]<d1["ema200"]: confluence+=15; factors.append("الاتجاه اليومي تحت EMA200")
        if h4["price"]<h4["ema200"]: confluence+=15; factors.append("H4 تحت EMA200")
        if h4["structure"]=="هابط": confluence+=12; factors.append("هيكل H4 هابط")
        if m15["macd"]<m15["macd_signal"]: confluence+=10; factors.append("MACD M15 سلبي")
    if m15["volume_ratio"]>=1.10: confluence+=10; factors.append("الحجم أعلى من الطبيعي")
    fib=h1.get("fib",{}).get("61.8")
    if fib is not None and abs(price-fib)<=max(h1["atr"]*0.60,price*0.0005): confluence+=8; factors.append("تلاقي مع فيبوناتشي 61.8%")
    if m15["fvg"] and ((direction=="BUY" and m15["fvg"]["type"]=="صاعدة") or (direction=="SELL" and m15["fvg"]["type"]=="هابطة")): confluence+=5; factors.append("FVG متوافقة مع الاتجاه")
    sr_points=0
    if direction=="BUY":
        s=nearest_support(levels,price)
        if s is not None and price-s<=m15["atr"]*1.8: sr_points+=10; factors.append("السعر قريب من دعم فعلي")
    elif direction=="SELL":
        r=nearest_resistance(levels,price)
        if r is not None and r-price<=m15["atr"]*1.8: sr_points+=10; factors.append("السعر قريب من مقاومة فعلية")
    inst_points,inst_factors=institutional_adjustment(direction,institutional); factors.extend(inst_factors)
    final_score=int(max(0,min(100,round(base_score*0.50+confluence*0.35+sr_points+inst_points))))
    quality,quality_icon=trade_quality(final_score)
    news_blocked,news_text=news_filter()
    valid=direction in ("BUY","SELL") and final_score>=MIN_TRADE_SCORE and not news_blocked
    trade=build_trade(direction,h1,m15,levels) if valid else None
    if trade and trade["rr"]<1.00: valid=False; factors.append("R:R غير مناسب")
    result={"signal":valid,"direction":direction,"score":final_score,"quality":quality,"quality_icon":quality_icon,"price":price,"levels":levels,"news_blocked":news_blocked,"news":news_text,"factors":factors,"trade":trade,"mtf":mtf,"institutional":institutional}
    LAST_ANALYSIS=result; return result


def register_trade(result):
    if not result.get("signal") or not result.get("trade"): return None
    t=result["trade"]; record={"time":now_damascus().isoformat(),"direction":result["direction"],"score":result["score"],"quality":result["quality"],"entry":t["entry"],"sl":t["sl"],"tp1":t["tp1"],"tp2":t["tp2"],"rr":t["rr"],"status":"OPEN","result":"OPEN"}
    TRADE_HISTORY.append(record)
    if len(TRADE_HISTORY)>MAX_TRADE_HISTORY: del TRADE_HISTORY[:-MAX_TRADE_HISTORY]
    return record


def update_trade_results():
    if not TRADE_HISTORY: return
    try: price=live_price()["price"]
    except Exception: return
    for trade in TRADE_HISTORY:
        if trade["status"]!="OPEN": continue
        if trade["direction"]=="BUY":
            if price<=trade["sl"]: trade["status"]="CLOSED"; trade["result"]="LOSS"
            elif price>=trade["tp2"]: trade["status"]="CLOSED"; trade["result"]="WIN"
            elif price>=trade["tp1"]: trade["result"]="TP1"
        else:
            if price>=trade["sl"]: trade["status"]="CLOSED"; trade["result"]="LOSS"
            elif price<=trade["tp2"]: trade["status"]="CLOSED"; trade["result"]="WIN"
            elif price<=trade["tp1"]: trade["result"]="TP1"

# ============================================================
# بناء التحليل العام
# ============================================================

def frame_text(name, x):
    direction = {"BUY": "🟢 شراء", "SELL": "🔴 بيع", "WAIT": "⏳ انتظار"}.get(x["direction"], "⏳ انتظار")
    return (
        f"📊 {name}\n"
        f"الاتجاه: {direction}\n"
        f"القوة: {x['score']} نقطة\n"
        f"EMA50: {x['ema50']:.2f}\n"
        f"EMA200: {x['ema200']:.2f}\n"
        f"RSI: {x['rsi']:.1f}\n"
        f"MACD: {x['macd']:.2f}\n"
        f"ADX: {x['adx']:.1f}\n"
        f"الهيكل: {x['structure']}\n"
        f"الحجم: {x['volume_state']} ({x['volume_ratio']:.2f}x)"
    )


def build_analysis():
    result = evaluate_signal()
    mtf = result["mtf"]
    direction = {"BUY": "🟢 أفضلية شراء", "SELL": "🔴 أفضلية بيع", "WAIT": "🟡 حياد"}.get(result["direction"], "🟡 حياد")
    levels = result["levels"]
    lines = [
        f"🤖 XAU SMART TRADER {VERSION}", "━━━━━━━━━━━━━━━━━━",
        "📊 التحليل الهيكلي والكمّي والمؤسسي", "",
        f"💰 السعر: {result['price']:.2f}",
        f"🎯 التوجيه: {direction}",
        f"💪 درجة التحليل: {result['score']} نقطة من 100", "",
        "🧠 الأطر الزمنية", "",
        frame_text("W1 — الاتجاه الأكبر", mtf["w1"]), "",
        frame_text("D1 — الاتجاه اليومي", mtf["d1"]), "",
        frame_text("H4 — الهيكل الرئيسي", mtf["h4"]), "",
        frame_text("H1 — منطقة الدخول", mtf["h1"]), "",
        frame_text("M15 — الزخم والتأكيد", mtf["m15"]), "",
        "━━━━━━━━━━━━━━━━━━", "📍 الدعم والمقاومة",
        f"🟢 S1: {format_zone(levels, 'support1')}",
        f"🟢 S2: {format_zone(levels, 'support2')}",
        f"🟢 S3: {format_zone(levels, 'support3')}",
        f"🔴 R1: {format_zone(levels, 'resistance1')}",
        f"🔴 R2: {format_zone(levels, 'resistance2')}",
        f"🔴 R3: {format_zone(levels, 'resistance3')}", "",
        "🏦 العامل المؤسسي",
        f"الاتجاه: {result['institutional']['direction']}",
        f"الحالة: {result['institutional']['quality']}/100",
        "",
        f"📰 الأخبار: {result['news']}", "", "🔎 عوامل التلاقي:"
    ]
    lines.extend(["• " + x for x in result["factors"]] or ["• لا يوجد تلاقي إضافي مسجل حالياً."])
    if result["signal"] and result["trade"]:
        t = result["trade"]
        d = "🟢 شراء" if result["direction"] == "BUY" else "🔴 بيع"
        quality = "🔥 قوية" if result["score"] >= STRONG_THRESHOLD else "🎯 مؤهلة"
        lines += [
            "", "━━━━━━━━━━━━━━━━━━", "🚨 إشارة تداول", "",
            f"📈 الصفقة: {d}", f"💪 الجودة: {result['score']} نقطة", quality,
            f"📍 الدخول: {t['entry']:.2f}", f"🛑 وقف الخسارة: {t['sl']:.2f}",
            f"🎯 TP1: {t['tp1']:.2f}", f"🎯 TP2: {t['tp2']:.2f}", f"⚖️ R:R: 1:{t['rr']:.2f}"
        ]
    else:
        lines += ["", "━━━━━━━━━━━━━━━━━━", "⏳ لا توجد صفقة مؤهلة الآن.", f"💪 الدرجة الحالية: {result['score']} نقطة", "البحث مستمر عن فرصة أفضل."]
        if result["news_blocked"]:
            lines.append("🚨 سبب الحجب: خبر اقتصادي.")
    lines += ["", "⚠️ التحليل مساعد لاتخاذ القرار اليدوي وليس ضماناً للربح."]
    return "\n".join(lines)

# ============================================================
# التقرير التوضيحي — المحرك الأساسي
# ============================================================

def _quality_label(score):
    if score >= 80: return "قوية جداً"
    if score >= 65: return "جيدة"
    if score >= 50: return "متوسطة"
    return "ضعيفة"


def _scenario_score(frames_data, frames, direction, levels, price, atr):
    """درجة سيناريو مستقلة للفترة المطلوبة، بدون خلط الفريمات."""
    score = 0
    factors = []
    for name, weight in frames:
        x = frames_data[name]
        if x["direction"] == direction:
            score += weight
            factors.append(f"{name} داعم لل{'شراء' if direction == 'BUY' else 'بيع'}")
        if direction == "BUY" and x["structure"] == "صاعد":
            score += 5
            factors.append(f"هيكل {name} صاعد")
        elif direction == "SELL" and x["structure"] == "هابط":
            score += 5
            factors.append(f"هيكل {name} هابط")
        if x["adx"] >= 25:
            score += 3
        if direction == "BUY" and x["rsi"] < 35:
            score += 3
        elif direction == "SELL" and x["rsi"] > 65:
            score += 3

    if direction == "BUY":
        s = nearest_support(levels, price)
        if s is not None and abs(price - s) <= max(atr * 1.8, price * 0.003):
            score += 10; factors.append("السعر قريب من دعم مهم")
    else:
        r = nearest_resistance(levels, price)
        if r is not None and abs(price - r) <= max(atr * 1.8, price * 0.003):
            score += 10; factors.append("السعر قريب من مقاومة مهمة")
    return min(100, int(score)), factors


def _make_scenarios(frames_data, levels, price, atr, horizon):
    if horizon == "weekly":
        frame_defs = [("W1", 30), ("D1", 25), ("H4", 20)]
    else:
        frame_defs = [("H1", 30), ("M15", 25), ("M5", 20)]

    def score(direction):
        return _scenario_score(frames_data, frame_defs, direction, levels, price, atr)

    buy_score, buy_factors = score("BUY")
    sell_score, sell_factors = score("SELL")
    direction = "BUY" if buy_score > sell_score else "SELL" if sell_score > buy_score else "WAIT"

    s1 = nearest_support(levels, price)
    s2 = next_support(levels, price)
    r1 = nearest_resistance(levels, price)
    r2 = next_resistance(levels, price)

    if direction == "BUY":
        primary = {"title": "استمرار الاتجاه الصاعد / بناء مركز شراء", "quality": buy_score,
                   "mechanism": f"تبقى الرؤية الشرائية مفضلة ما دام السعر يحافظ على منطقة الدعم {fmt(s1)} ولا يظهر كسر هيكلي هابط مؤكد.",
                   "trigger": f"تثبيت السعر فوق {fmt(s1)} مع تأكيد الاتجاه على الفريمات المعنية.",
                   "targets": [r1, r2], "stop": s1, "factors": buy_factors}
        alternative = {"title": "السيناريو البديل — تحول هابط", "quality": sell_score,
                       "mechanism": f"يتحول الميزان إلى الهبوط عند فقدان الدعم {fmt(s1)} مع تأكيد كسر هيكلي وليس مجرد ذيل سعري.",
                       "trigger": f"إغلاق واضح أسفل {fmt(s1)} ثم فشل استعادة المستوى.",
                       "targets": [s2, None], "stop": r1, "factors": sell_factors}
    elif direction == "SELL":
        primary = {"title": "استمرار الاتجاه الهابط / البيع من المقاومة", "quality": sell_score,
                   "mechanism": f"تبقى الرؤية البيعية مفضلة ما دام السعر أسفل المقاومة {fmt(r1)} والهيكل يدعم الضغط الهابط.",
                   "trigger": f"رفض سعري وتأكيد هابط أسفل {fmt(r1)}." if r1 else "تأكيد هابط من منطقة مقاومة واضحة.",
                   "targets": [s1, s2], "stop": r1, "factors": sell_factors}
        alternative = {"title": "السيناريو البديل — استعادة الاتجاه الصاعد", "quality": buy_score,
                       "mechanism": f"يتحول الميزان إذا اخترق السعر {fmt(r1)} وثبت فوقه مع تحسن الهيكل والزخم.",
                       "trigger": f"إغلاق فوق {fmt(r1)} ثم تثبيت المستوى." if r1 else "اختراق قمة مهمة والثبات فوقها.",
                       "targets": [r2, None], "stop": s1, "factors": buy_factors}
    else:
        primary = {"title": "سيناريو الانتظار — لا أفضلية اتجاهية كافية", "quality": max(buy_score, sell_score),
                   "mechanism": "الفريمات المحددة لا تمنح أفضلية واضحة؛ القرار يعتمد على كسر أحد طرفي النطاق مع تأكيد.",
                   "trigger": f"كسر {fmt(r1)} صعوداً أو {fmt(s1)} هبوطاً مع تثبيت السعر.",
                   "targets": [r1, r2], "stop": None, "factors": []}
        alternative = {"title": "السيناريو البديل — استمرار التذبذب", "quality": max(buy_score, sell_score),
                       "mechanism": f"قد يبقى الذهب داخل النطاق بين {fmt(s1)} و{fmt(r1)} حتى يظهر محفز أقوى.",
                       "trigger": "توسع واضح في الزخم والحجم ثم كسر النطاق.",
                       "targets": [s1, r1], "stop": None, "factors": []}
    return primary, alternative, buy_score, sell_score


def _format_explanatory_report(frames_data, levels, price, primary, alternative, horizon, extra_lines):
    def target_text(targets):
        vals = [fmt(x) for x in targets if x is not None]
        return " ثم ".join(vals) if vals else "غير محددة من المستويات الحالية"

    label = "الأسبوعي" if horizon == "weekly" else "اليومي"
    lines = [
        f"📝 التقرير التوضيحي {label} — XAU/USD", "━━━━━━━━━━━━━━━━━━━━",
        f"🕐 توقيت دمشق: {now_damascus().strftime('%Y-%m-%d %H:%M')}",
        f"💰 السعر الحالي: {fmt(price)}",
        f"🎯 جودة السيناريو الرئيسي: {primary['quality']} نقطة / 100 — {_quality_label(primary['quality'])}",
        f"🧭 الأفق: {'الاستراتيجية المحتملة خلال الأسبوع' if horizon == 'weekly' else 'الحركة المحتملة خلال اليوم'}",
        ""
    ]
    lines.extend(extra_lines)
    lines += [
        "🟢 السيناريو الأول — المرجح", "━━━━━━━━━━━━━━━━━━━━",
        primary["title"],
        f"• الآلية: {primary['mechanism']}",
        f"• شرط التفعيل: {primary['trigger']}",
        f"• الأهداف المحتملة: {target_text(primary['targets'])}",
        f"• مستوى إبطال الفكرة: {fmt(primary['stop'])}",
    ]
    if primary["factors"]:
        lines.append("• أسباب الترجيح:")
        lines.extend("  - " + x for x in primary["factors"][:7])
    lines += ["", "🔴 السيناريو الثاني — البديل", "━━━━━━━━━━━━━━━━━━━━",
              alternative["title"], f"• الآلية: {alternative['mechanism']}",
              f"• شرط التحول: {alternative['trigger']}",
              f"• الأهداف المحتملة: {target_text(alternative['targets'])}",
              f"• جودة السيناريو البديل: {alternative['quality']} نقطة / 100", "",
              "📍 خريطة القرار السعري", "━━━━━━━━━━━━━━━━━━━━",
              f"🟢 S1: {format_zone(levels, 'support1')}",
              f"🟢 S2: {format_zone(levels, 'support2')}",
              f"🟢 S3: {format_zone(levels, 'support3')}",
              f"🔴 R1: {format_zone(levels, 'resistance1')}",
              f"🔴 R2: {format_zone(levels, 'resistance2')}",
              f"🔴 R3: {format_zone(levels, 'resistance3')}", "",
              "🗒 خطة العمل المقترحة", "━━━━━━━━━━━━━━━━━━━━"]
    if horizon == "weekly":
        lines += ["• قراءة W1 أولاً لتحديد اتجاه الأسبوع قبل التفكير في الدخول.",
                  "• D1 يحدد ما إذا كان الاتجاه الأسبوعي يكتسب دعماً أم يفقده.",
                  "• H4 يستخدم لتحديد منطقة التنفيذ الاستراتيجي، وليس M15 لتحديد اتجاه الأسبوع.",
                  "• الإغلاق الأسبوعي فوق/تحت مستويات القرار هو العامل الأهم لتغيير السيناريو."]
    else:
        lines += ["• مراقبة السيولة والزخم على H1 ثم M15 ثم M5.",
                  "• لا تتم مطاردة السعر بعد حركة قوية بعيدة عن منطقة القرار.",
                  "• المطلوب توافق الاتجاه + الهيكل + الزخم + المنطقة قبل الدخول.",
                  "• كسر مستوى القرار مع تثبيت السعر ينقلنا إلى السيناريو البديل."]
    lines += ["", "🧠 ملخص الفريمات", " | ".join(f"{k}: {v['direction']}" for k, v in frames_data.items()),
              "", "⚠️ جودة السيناريو مقياس تحليلي وليست احتمالاً مضموناً للربح."]
    return "\n".join(lines)


def build_daily_analysis():
    """التحليل اليومي: H1 + M15 + M5 للحركة داخل اليوم."""
    h1 = analyze(get_bars("1h", 300)); m15 = analyze(get_bars("15m", 300)); m5 = analyze(get_bars("5m", 300))
    q = live_price(); price = q["price"]; levels = support_resistance(get_bars("1h", 250))
    frames = {"H1": h1, "M15": m15, "M5": m5}
    buy, _ = _scenario_score([("H1",35),("M15",30),("M5",25)], "BUY", levels, price, m15["atr"])
    sell, _ = _scenario_score([("H1",35),("M15",30),("M5",25)], "SELL", levels, price, m15["atr"])
    direction = "BUY" if buy > sell else "SELL" if sell > buy else "WAIT"
    return f"📊 التحليل اليومي {VERSION}\n━━━━━━━━━━━━━━━━━━\n💰 السعر: {price:.2f}\n🎯 التوجيه: {'🟢 شراء' if direction=='BUY' else '🔴 بيع' if direction=='SELL' else '🟡 انتظار'}\n💪 قوة اليوم: {max(buy,sell)} نقطة\n\nH1: {h1['direction']} | RSI {h1['rsi']:.1f} | ADX {h1['adx']:.1f}\nM15: {m15['direction']} | RSI {m15['rsi']:.1f} | ADX {m15['adx']:.1f}\nM5: {m5['direction']} | RSI {m5['rsi']:.1f} | ADX {m5['adx']:.1f}\n\n📍 S1: {format_zone(levels,'support1')}\n📍 R1: {format_zone(levels,'resistance1')}"


def build_weekly_analysis():
    """التحليل الأسبوعي: W1 + D1 + H4 فقط."""
    w1 = analyze(get_bars("1w", 250)); d1 = analyze(get_bars("1d", 300)); h4 = analyze(get_bars("4h", 300))
    q = live_price(); price = q["price"]; levels = support_resistance(get_bars("1d", 250))
    frames = {"W1": w1, "D1": d1, "H4": h4}
    buy, _ = _scenario_score([("W1",35),("D1",30),("H4",25)], "BUY", levels, price, d1["atr"])
    sell, _ = _scenario_score([("W1",35),("D1",30),("H4",25)], "SELL", levels, price, d1["atr"])
    direction = "BUY" if buy > sell else "SELL" if sell > buy else "WAIT"
    return f"📅 التحليل الأسبوعي {VERSION}\n━━━━━━━━━━━━━━━━━━\n💰 السعر: {price:.2f}\n🎯 الاتجاه الاستراتيجي: {'🟢 شراء' if direction=='BUY' else '🔴 بيع' if direction=='SELL' else '🟡 حياد'}\n💪 قوة الاتجاه: {max(buy,sell)} نقطة\n\nW1: {w1['direction']} | قوة {w1['score']} | RSI {w1['rsi']:.1f} | ADX {w1['adx']:.1f}\nD1: {d1['direction']} | قوة {d1['score']} | RSI {d1['rsi']:.1f} | ADX {d1['adx']:.1f}\nH4: {h4['direction']} | قوة {h4['score']} | RSI {h4['rsi']:.1f} | ADX {h4['adx']:.1f}\n\n📍 الدعم الأسبوعي: {format_zone(levels,'support1')}\n📍 المقاومة الأسبوعية: {format_zone(levels,'resistance1')}"


def build_daily_report():
    frames_data = {"H1": analyze(get_bars("1h",300)), "M15": analyze(get_bars("15m",300)), "M5": analyze(get_bars("5m",300))}
    q = live_price(); price=q["price"]; levels=support_resistance(get_bars("1h",250)); atr=frames_data["M15"]["atr"]
    primary, alternative, _, _ = _make_scenarios(frames_data, levels, price, atr, "daily")
    return _format_explanatory_report(frames_data, levels, price, primary, alternative, "daily", [
        "📌 التركيز: الاتجاه داخل اليوم وليس الاتجاه الاستراتيجي الطويل.",
        "⏱ تسلسل القرار: H1 للسياق → M15 للتأكيد → M5 للزناد."])


def build_weekly_report():
    frames_data = {"W1": analyze(get_bars("1w",250)), "D1": analyze(get_bars("1d",300)), "H4": analyze(get_bars("4h",300))}
    q = live_price(); price=q["price"]; levels=support_resistance(get_bars("1d",250)); atr=frames_data["D1"]["atr"]
    primary, alternative, _, _ = _make_scenarios(frames_data, levels, price, atr, "weekly")
    return _format_explanatory_report(frames_data, levels, price, primary, alternative, "weekly", [
        "📌 التركيز: اتجاه الأسبوع وبناء الرؤية الاستراتيجية.",
        f"📊 W1: {frames_data['W1']['direction']} | D1: {frames_data['D1']['direction']} | H4: {frames_data['H4']['direction']}",
        "🧭 لا يدخل M15 أو M5 في تحديد الاتجاه الاستراتيجي الأسبوعي."])


def build_explanatory_report(timeframe="daily"):
    return build_weekly_report() if timeframe == "weekly" else build_daily_report()

# ============================================================
# أوامر Telegram
# ============================================================

async def reply(update, text):
    try:
        await update.message.reply_text(text)
    except Exception:
        logger.exception("Telegram reply error")


async def start(update, context):
    async def start(update, context):
    # 👤 تسجيل المستخدم تلقائياً في نظام الاشتراكات
    create_free_user(
        update.effective_chat.id,
        update.effective_user.username,
        update.effective_user.first_name
    )


        keyboard = [
    ["📊 التحليل الكامل", "📊 التحليل اليومي"],
    ["⚡ التحليل السريع", "🎯 صفقة الآن"],
    ["💳 الباقات", "👤 اشتراكي"]
    ]
    keyboard = [
        ["📊 التحليل الكامل", "📊 التحليل اليومي"],
        ["⚡ التحليل السريع", "🎯 صفقة الآن"],
        ["📍 الدعوم والمقاومات"],
        ["📝 التقرير التوضيحي اليومي", "📅 التقرير التوضيحي الأسبوعي"],
        ["📅 التحليل الأسبوعي", "📝 التوضيحي اليومي"],
        ["📰 الأخبار", "💰 سعر الذهب"],
        ["🌍 الأسواق", "🔔 التنبيهات"],
        ["🟢 حالة النظام"]
    ]
    text = (
        f"🤖 XAU SMART TRADER {VERSION}\n\n"
        "🥇 محلل الذهب XAU/USD\n\n"
        "W1 + D1 + H4 + H1 + M15\n"
        "Structure + Momentum + Volume + Fibonacci + FVG\n\n"
        f"🎯 حد الإشارة: {SIGNAL_THRESHOLD} نقطة\n"
        "📝 تمت إضافة التقرير التوضيحي اليومي والأسبوعي.\n\n"
        "اختر العملية 👇"
    )
    await update.message.reply_text(text, reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))


async def full_analysis(update, context):
    try:
        await reply(update, await asyncio.to_thread(build_analysis))
    except Exception as e:
        await reply(update, f"❌ تعذر تنفيذ التحليل.\nالسبب: {e}")


async def quick_analysis(update, context):
    try:
        m15 = analyze(get_bars("15m", 200))
        price = live_price()
        d = "🟢 شراء" if m15["direction"] == "BUY" else "🔴 بيع" if m15["direction"] == "SELL" else "🟡 انتظار"
        text = (
            f"⚡ التحليل السريع {VERSION}\n\n"
            f"💰 السعر: {price['price']:.2f}\n📈 الاتجاه: {d}\n💪 القوة: {m15['score']} نقطة\n"
            f"RSI: {m15['rsi']:.1f}\nMACD: {m15['macd']:.2f}\nADX: {m15['adx']:.1f}\n"
            f"الهيكل: {m15['structure']}\nالحجم: {m15['volume_state']} ({m15['volume_ratio']:.2f}x)\n\n"
            "🔎 العوامل:\n" + "\n".join("• " + x for x in m15["reasons"][:8])
        )
        await reply(update, text)
    except Exception as e:
        await reply(update, f"❌ تعذر التحليل السريع: {e}")


async def trade_now(update, context):
    try:
        result = await asyncio.to_thread(evaluate_signal)
        if result["news_blocked"]:
            await reply(update, "🚨 لا توجد صفقة الآن.\n\nتم تفعيل الحماية الإخبارية.")
            return
        if not result["signal"]:
            await reply(update, f"⏳ لا توجد صفقة مؤهلة الآن.\n\n💪 الدرجة: {result['score']} نقطة\n🎯 الحد: {SIGNAL_THRESHOLD} نقطة\n\nالبوت يراقب السوق.")
            return
        trade = result["trade"]
        register_trade(result)
        direction = "🟢 شراء" if result["direction"] == "BUY" else "🔴 بيع"
        quality = "🔥 قوية" if result["score"] >= STRONG_THRESHOLD else "🎯 مؤهلة"
        text = (
            "🚨 XAU SMART TRADER\n━━━━━━━━━━━━━━━━━━\n"
            f"📈 الصفقة: {direction}\n💪 الجودة: {result['score']} نقطة — {quality}\n\n"
            f"📍 الدخول: {trade['entry']:.2f}\n🛑 SL: {trade['sl']:.2f}\n"
            f"🎯 TP1: {trade['tp1']:.2f}\n🎯 TP2: {trade['tp2']:.2f}\n⚖️ R:R: 1:{trade['rr']:.2f}\n\n"
            "🧠 إشارة تحليلية للتنفيذ اليدوي."
        )
        await reply(update, text)
    except Exception as e:
        await reply(update, f"❌ تعذر بناء الصفقة: {e}")


async def show_levels(update, context):
    try:
        levels = support_resistance(get_bars("1h", 250))
        text = (
            "📍 XAU/USD — مناطق السوق\n\n"
            f"🟢 S1: {format_zone(levels, 'support1')}\n"
            f"🟢 S2: {format_zone(levels, 'support2')}\n"
            f"🟢 S3: {format_zone(levels, 'support3')}\n\n"
            f"🔴 R1: {format_zone(levels, 'resistance1')}\n"
            f"🔴 R2: {format_zone(levels, 'resistance2')}\n"
            f"🔴 R3: {format_zone(levels, 'resistance3')}\n\n"
            "المناطق مبنية على القمم والقيعان المجمعة حسب ATR."
        )
        await reply(update, text)
    except Exception as e:
        await reply(update, f"❌ تعذر حساب المناطق: {e}")


async def gold_price(update, context):
    try:
        q = await asyncio.to_thread(live_price)
        await reply(update, f"💰 XAU/USD — السعر اللحظي\n\nالسعر: {q['price']:.2f}\nالمصدر: {q['source']}\nعمر السعر: {q.get('age')}\nتوقيت دمشق: {now_damascus().strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        await reply(update, f"❌ تعذر جلب السعر: {e}")


async def daily_analysis(update, context):
    try:
        await reply(update, await asyncio.to_thread(build_daily_analysis))
    except Exception as e:
        logger.exception("Daily analysis error")
        await reply(update, f"❌ تعذر إنشاء التحليل اليومي.\nالسبب: {e}")


async def weekly_analysis(update, context):
    try:
        await reply(update, await asyncio.to_thread(build_weekly_analysis))
    except Exception as e:
        logger.exception("Weekly analysis error")
        await reply(update, f"❌ تعذر إنشاء التحليل الأسبوعي.\nالسبب: {e}")


async def weekly_report(update, context):
    try:
        await reply(update, await asyncio.to_thread(build_weekly_report))
    except Exception as e:
        logger.exception("Weekly report error")
        await reply(update, f"❌ تعذر إنشاء التقرير الأسبوعي.\nالسبب: {e}")


async def daily_report(update, context):
    try:
        await reply(update, await asyncio.to_thread(build_daily_report))
    except Exception as e:
        logger.exception("Daily report error")
        await reply(update, f"❌ تعذر إنشاء التقرير التوضيحي اليومي.\nالسبب: {e}")


async def news_status(update, context):
    blocked, text = news_filter()
    await reply(update, f"📰 فلتر الأخبار\n\n{'🚨 التداول محجوب' if blocked else '🟢 التداول غير محجوب'}\n\n{text}\n\nالحماية: {NEWS_BEFORE_MIN} دقيقة قبل الخبر + {NEWS_AFTER_MIN} دقيقة بعده.")


async def markets(update, context):
    now = now_damascus()
    await reply(update, f"🌍 جلسات السيولة\n\n🇯🇵 آسيا: تجميع ومراقبة\n🇬🇧 لندن: ارتفاع السيولة\n🇺🇸 نيويورك: أعلى التقلبات\n\n🕐 توقيت دمشق الآن: {now.strftime('%H:%M:%S')}\n\n⚠️ أوقات الافتتاح تتغير موسمياً بسبب التوقيت الصيفي.")


async def status(update, context):
    await reply(update, (
        f"🟢 XAU SMART TRADER {VERSION}\n\n"
        "حالة النظام: يعمل\nTelegram: متصل\nFlask: يعمل\nالبيانات: Biquote OHLC\n"
        "التحليل: W1/D1/H4/H1/M15\nالهيكل: مفعّل\nالحجم: مفعّل\nRSI: مفعّل\nMACD: مفعّل\nADX: مفعّل\nFibonacci: مفعّل\nFVG: مفعّل\n"
        f"فلتر الأخبار: {'مفعّل' if NEWS_FILTER_ENABLED else 'متوقف'}\n\n"
        f"🎯 حد الإشارة: {SIGNAL_THRESHOLD} نقطة\n🔥 الإشارة القوية: {STRONG_THRESHOLD} نقطة\n"
        "📝 التقرير التوضيحي: يومي + أسبوعي"
    ))

# ============================================================
# التنبيهات الحالية — لا يتم تشغيلها إلا للمشتركين
# ============================================================

async def subscribe(update, context):
    chat_id = update.effective_chat.id
    SUBSCRIBERS.add(chat_id)
    await reply(update, f"🔔 تم تفعيل التنبيهات.\n\n🎯 حد الإشارة: {SIGNAL_THRESHOLD} نقطة\n📰 الحماية الإخبارية مفعّلة.")


async def unsubscribe(update, context):
    chat_id = update.effective_chat.id
    SUBSCRIBERS.discard(chat_id)
    LAST_SIGNAL.pop(chat_id, None)
    await reply(update, "🔕 تم إيقاف التنبيهات التلقائية.")


async def auto_loop():
    while True:
        try:
            update_trade_results()
            if AUTO_ENABLED and SUBSCRIBERS:
                result = await asyncio.to_thread(evaluate_signal)
                if result["signal"] and not result["news_blocked"] and result["trade"]:
                    trade = result["trade"]
                    signature = (result["direction"], round(trade["entry"], 2), round(trade["sl"], 2), round(trade["tp1"], 2))
                    for chat_id in list(SUBSCRIBERS):
                        if LAST_SIGNAL.get(chat_id) == signature:
                            continue
                        direction = "🟢 شراء" if result["direction"] == "BUY" else "🔴 بيع"
                        quality = "🔥 قوية" if result["score"] >= STRONG_THRESHOLD else "🎯 مؤهلة"
                        text = (
                            "🚨 إشارة ذهب جديدة\n━━━━━━━━━━━━━━━━━━\n\n"
                            f"📈 الصفقة: {direction}\n💪 الجودة: {result['score']} نقطة — {quality}\n\n"
                            f"💰 السعر: {result['price']:.2f}\n📍 الدخول: {trade['entry']:.2f}\n"
                            f"🛑 SL: {trade['sl']:.2f}\n🎯 TP1: {trade['tp1']:.2f}\n🎯 TP2: {trade['tp2']:.2f}\n"
                            f"⚖️ R:R: 1:{trade['rr']:.2f}\n\n🧠 التلاقي:\n" +
                            "\n".join("• " + x for x in result["factors"][:7]) +
                            "\n\n⚠️ تنفيذ يدوي فقط."
                        )
                        try:
                            await APPLICATION.bot.send_message(chat_id=chat_id, text=text)
                            LAST_SIGNAL[chat_id] = signature
                        except Exception:
                            logger.exception("Signal send error")
        except Exception:
            logger.exception("Auto loop error")
        await asyncio.sleep(AUTO_SCAN_SECONDS)

# ============================================================
# Router
# ============================================================

async def router(update, context):
    text = (update.message.text or "").strip()
    routes = {
        "📊 التحليل الكامل": full_analysis,
        "⚡ التحليل السريع": quick_analysis,
        "🎯 صفقة الآن": trade_now,
        "📍 الدعوم والمقاومات": show_levels,
        "📅 التحليل الأسبوعي": weekly_analysis,
        "📅 التقرير التوضيحي الأسبوعي": weekly_report,
        "📝 التقرير التوضيحي اليومي": daily_report,
        "📝 التوضيحي اليومي": daily_report,
        "📊 التحليل اليومي": daily_analysis,
        "📰 الأخبار": news_status,
        "💰 سعر الذهب": gold_price,
        "🌍 الأسواق": markets,
        "🔔 التنبيهات": subscribe,
        "🔔 تفعيل التنبيهات": subscribe,
        "🔕 إيقاف التنبيهات": unsubscribe,
        "🟢 حالة النظام": status
    }
    fn = routes.get(text)
    if fn:
        await fn(update, context)
    else:
        await start(update, context)

# ============================================================
# Webhook
# ============================================================

@app.route(WEBHOOK_PATH, methods=["POST"])
def webhook():
    if APPLICATION is None:
        return "Bot not ready", 503
    try:
        data = request.get_json(force=True)
        update = Update.de_json(data, APPLICATION.bot)
        asyncio.run_coroutine_threadsafe(APPLICATION.process_update(update), BOT_LOOP)
        return "OK", 200
    except Exception:
        logger.exception("Webhook error")
        return "OK", 200

# ============================================================
# تشغيل Telegram
# ============================================================

async def start_bot():
    global APPLICATION
    if not TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN غير موجود في Render.")

    APPLICATION = Application.builder().token(TOKEN).build()
    APPLICATION.add_handler(CommandHandler("start", start))
    APPLICATION.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, router))

    await APPLICATION.initialize()
    await APPLICATION.start()
    await APPLICATION.bot.set_webhook(url=WEBHOOK_URL, allowed_updates=["message"], drop_pending_updates=True)

    logger.info("XAU SMART TRADER %s started", VERSION)
    logger.info("Webhook: %s", WEBHOOK_URL)
    asyncio.create_task(auto_loop())

    while True:
        await asyncio.sleep(3600)

# ============================================================
# Flask Server
# ============================================================

def run_flask():
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True, use_reloader=False)

# ============================================================
# Main
# ============================================================

def main():
        init_subscription_db()
    global BOT_LOOP
    server = threading.Thread(target=run_flask, daemon=True)
    server.start()
    loop = asyncio.new_event_loop()
    BOT_LOOP = loop
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(start_bot())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()

if __name__ == "__main__":
    main()
