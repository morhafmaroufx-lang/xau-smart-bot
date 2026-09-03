# ============================================================
# XAU SMART TRADER v18.5
# Structural Liquidity + Quantitative Momentum
# واجهة عربية بالكامل - توقيت دمشق
#
# v18.5:
# - توحيد كل التعديلات السابقة في نسخة مرقمة مستقلة
# - واجهة رئيسية جديدة بدون أزرار التحليل اليومية/الأسبوعية القديمة
# - تقارير توضيحية يومية وأسبوعية + سجل صفقات
# - تحليل سريع EMA 9/21 + RSI/MACD/ADX + S/R + R:R
# - فحص تلقائي كل 15 دقيقة وتحديثات لا تكرر الصفقة في السجل
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
import math
import sqlite3
from functools import wraps
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import pandas as pd
import numpy as np

from flask import Flask, request, jsonify
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters

# ============================================================
# الإعدادات
# ============================================================

VERSION = "v18.20"
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
AUTO_SCAN_SECONDS = 15 * 60
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
# الاشتراكات والصلاحيات
# ============================================================
SUBSCRIPTION_DB_PATH = os.environ.get("SUBSCRIPTION_DB_PATH", "subscriptions.db")
def _load_admin_ids():
    """قراءة معرفات الإدارة من ADMIN_IDS أو ADMIN_ID مع دعم الفواصل والمسافات."""
    raw = os.environ.get("ADMIN_IDS", "").strip()
    single = os.environ.get("ADMIN_ID", "").strip()
    values = []
    if raw:
        values.extend(raw.replace(";", ",").split(","))
    if single:
        values.append(single)
    result = set()
    for value in values:
        value = value.strip()
        if value.isdigit():
            result.add(int(value))
    return result

ADMIN_IDS = _load_admin_ids()

def is_admin_chat(chat_id):
    """تحقق موحّد من صلاحية الإدارة باستخدام ADMIN_IDS."""
    try:
        return int(chat_id) in ADMIN_IDS
    except (TypeError, ValueError):
        return False

ADMIN_CONTACT = os.environ.get("ADMIN_CONTACT", "").strip()

PLANS = {
    "FREE": {
        "name": "🆓 FREE", "price": 0, "trade_limit": 1, "trade_period": "weekly",
        "features": {"gold_price", "markets", "status", "quick_analysis", "trade_access"}
    },
    "BASIC": {
        "name": "🥉 BASIC", "price": 10, "trade_limit": 5, "trade_period": "monthly",
        "features": {"gold_price", "markets", "status", "quick_analysis", "sr", "daily_report", "trade_access"}
    },
    "PRO": {
        "name": "🥈 PRO", "price": 20, "trade_limit": 20, "trade_period": "monthly",
        "features": {"gold_price", "markets", "status", "quick_analysis", "sr", "daily_report", "weekly_report", "full_analysis", "trade_now", "trade_alerts", "trade_access", "trade_history"}
    },
    "PREMIUM": {
        "name": "🥇 PREMIUM", "price": 35, "trade_limit": 50, "trade_period": "monthly",
        "features": {"gold_price", "markets", "status", "quick_analysis", "sr", "daily_report", "weekly_report", "full_analysis", "trade_now", "trade_alerts", "trade_access", "trade_history", "institutional", "news_alerts", "market_alerts"}
    },
    "VIP": {
        "name": "💎 VIP", "price": 50, "trade_limit": None, "trade_period": "monthly",
        "features": {"gold_price", "markets", "status", "quick_analysis", "sr", "daily_report", "weekly_report", "full_analysis", "trade_now", "trade_alerts", "trade_access", "trade_history", "institutional", "news_alerts", "market_alerts", "vip"}
    },
}

TRADE_LIMIT_TEXT = {
    "FREE": "صفقة تجريبية واحدة كل 7 أيام",
    "BASIC": "5 صفقات شهرياً",
    "PRO": "20 صفقة شهرياً",
    "PREMIUM": "50 صفقة شهرياً",
    "VIP": "♾️ صفقات غير محدودة",
}

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
TRADE_DB_PATH = os.environ.get("TRADE_DB_PATH", "trades.db")
TRADE_LOCK = threading.RLock()

# حالات محرك السيولة وإعادة الاختبار — تحفظ في الذاكرة وتُحدّث مع كل فحص.
LIQUIDITY_STATE = {}
MAX_LIQUIDITY_STATES = 200

# دورة حياة السيولة: كل حالة لها عمر محدد بعدد الشموع المكتملة،
# والحالات النهائية لا تعود مرشحة لاتخاذ القرار.
LIQUIDITY_SWEEP_MAX_BARS = int(os.environ.get("LIQUIDITY_SWEEP_MAX_BARS", "24"))
LIQUIDITY_DISPLACEMENT_MAX_BARS = int(os.environ.get("LIQUIDITY_DISPLACEMENT_MAX_BARS", "3"))
LIQUIDITY_BOS_MAX_BARS = int(os.environ.get("LIQUIDITY_BOS_MAX_BARS", "12"))
LIQUIDITY_RETEST_MAX_BARS = int(os.environ.get("LIQUIDITY_RETEST_MAX_BARS", "12"))
LIQUIDITY_MAX_AGE_BARS = int(os.environ.get("LIQUIDITY_MAX_AGE_BARS", "36"))
LIQUIDITY_CONFIRM_MAX_BARS = int(os.environ.get("LIQUIDITY_CONFIRM_MAX_BARS", "1"))


# ============================================================
# محرك الاشتراكات
# ============================================================

def _db():
    conn = sqlite3.connect(SUBSCRIPTION_DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE IF NOT EXISTS users (
        chat_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, plan TEXT NOT NULL DEFAULT 'FREE',
        status TEXT NOT NULL DEFAULT 'active', start_date TEXT, expiry_date TEXT, referral_code TEXT UNIQUE, referred_by INTEGER,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS usage (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL, feature TEXT NOT NULL, used_at TEXT NOT NULL,
        UNIQUE(chat_id, feature, used_at)
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS subscription_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL, plan TEXT NOT NULL,
        requested_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING'
    )""")
    conn.commit()
    return conn


def _ensure_user(update):
    chat = update.effective_chat
    user = update.effective_user
    now = now_damascus().isoformat()
    code = f"ref_{chat.id}"
    conn = _db()
    try:
        row = conn.execute("SELECT * FROM users WHERE chat_id=?", (chat.id,)).fetchone()
        admin = is_admin_chat(chat.id) or (user is not None and is_admin_chat(user.id))
        if not row:
            conn.execute("INSERT OR IGNORE INTO users(chat_id, username, first_name, plan, status, referral_code, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?)",
                         (chat.id, getattr(user, 'username', None), getattr(user, 'first_name', None), 'VIP' if admin else 'FREE', 'active', code, now, now))
            conn.commit()
        else:
            if admin:
                conn.execute("UPDATE users SET username=?, first_name=?, plan='VIP', status='active', expiry_date=NULL, updated_at=? WHERE chat_id=?",
                             (getattr(user, 'username', None), getattr(user, 'first_name', None), now, chat.id))
            else:
                conn.execute("UPDATE users SET username=?, first_name=?, updated_at=? WHERE chat_id=?",
                             (getattr(user, 'username', None), getattr(user, 'first_name', None), now, chat.id))
            conn.commit()
        return conn.execute("SELECT * FROM users WHERE chat_id=?", (chat.id,)).fetchone()
    finally:
        conn.close()


def get_member(chat_id):
    conn = _db()
    try:
        row = conn.execute("SELECT * FROM users WHERE chat_id=?", (chat_id,)).fetchone()
        if not row:
            return None
        if row['expiry_date']:
            try:
                expiry = datetime.fromisoformat(row['expiry_date'])
                if expiry.tzinfo is None:
                    expiry = expiry.replace(tzinfo=DAMASCUS)
                if now_damascus() >= expiry and row['plan'] != 'FREE':
                    conn.execute("UPDATE users SET plan='FREE', status='expired', updated_at=? WHERE chat_id=?", (now_damascus().isoformat(), chat_id))
                    conn.commit()
                    row = conn.execute("SELECT * FROM users WHERE chat_id=?", (chat_id,)).fetchone()
            except Exception:
                pass
        return row
    finally:
        conn.close()


def has_feature(chat_id, feature):
    if is_admin_chat(chat_id):
        return True
    row = get_member(chat_id)
    plan = row['plan'] if row else 'FREE'
    return feature in PLANS.get(plan, PLANS['FREE'])['features']


def usage_count(chat_id, plan=None):
    row = get_member(chat_id)
    plan = plan or (row['plan'] if row else 'FREE')
    if plan == 'VIP':
        return 0
    period_start = now_damascus() - timedelta(days=7 if PLANS[plan]['trade_period'] == 'weekly' else 30)
    conn = _db()
    try:
        return conn.execute("SELECT COUNT(*) FROM usage WHERE chat_id=? AND feature='trade' AND used_at>=?", (chat_id, period_start.isoformat())).fetchone()[0]
    finally:
        conn.close()


def trade_quota(chat_id):
    if is_admin_chat(chat_id):
        return 'VIP', 0, None
    row = get_member(chat_id)
    plan = row['plan'] if row else 'FREE'
    limit = PLANS[plan]['trade_limit']
    used = usage_count(chat_id, plan)
    return plan, used, limit


def can_receive_trade(chat_id):
    plan, used, limit = trade_quota(chat_id)
    return limit is None or used < limit


def consume_trade(chat_id):
    if is_admin_chat(chat_id):
        return True
    if not can_receive_trade(chat_id):
        return False
    conn = _db()
    try:
        conn.execute("INSERT INTO usage(chat_id, feature, used_at) VALUES(?,?,?)", (chat_id, 'trade', now_damascus().isoformat()))
        conn.commit()
        return True
    finally:
        conn.close()


def alert_subscribers():
    """إرجاع جميع المستخدمين ذوي الباقات التي تتضمن تنبيهات الصفقات."""
    conn = _db()
    try:
        rows = conn.execute("SELECT chat_id FROM users WHERE status='active' AND plan IN ('PRO','PREMIUM','VIP')").fetchall()
        result = set(ADMIN_IDS)
        for row in rows:
            chat_id = int(row["chat_id"])
            if has_feature(chat_id, "trade_alerts"):
                result.add(chat_id)
        return result
    finally:
        conn.close()


def plan_status_text(chat_id):
    if is_admin_chat(chat_id):
        return ("💎 VIP — إدارة | وصول كامل\n"
                "🎯 الصفقات: ♾️ غير محدودة\n"
                "📊 المتبقي: ♾️ غير محدود\n"
                "📅 الانتهاء: لا يوجد انتهاء — صلاحية الإدارة")
    row = get_member(chat_id)
    plan = row['plan'] if row else 'FREE'
    info = PLANS.get(plan, PLANS['FREE'])
    if plan == 'VIP':
        quota = "♾️ غير محدودة"
    else:
        used = usage_count(chat_id, plan)
        quota = f"{max(0, info['trade_limit'] - used)} متبقية"
    expiry = row['expiry_date'] if row and row['expiry_date'] else "لا يوجد انتهاء — مجاني"
    return f"{info['name']} | ${info['price']} / شهر\n🎯 الصفقات: {TRADE_LIMIT_TEXT[plan]}\n📊 المتبقي: {quota}\n📅 الانتهاء: {expiry}"


def plans_text():
    lines = ["💳 باقات XAU SMART TRADER", "━━━━━━━━━━━━━━━━━━"]
    for key in ("FREE", "BASIC", "PRO", "PREMIUM", "VIP"):
        p = PLANS[key]
        lines += [f"{p['name']} — ${p['price']} / شهر", f"🎯 الصفقات: {TRADE_LIMIT_TEXT[key]}"]
        if key == 'FREE':
            lines.append("🎁 لتجربة النظام قبل الترقية")
        lines.append("")
    lines += ["🚀 كلما ارتفعت الباقة زاد وصولك إلى الإشارات والتحليلات والتنبيهات.", "💡 نظام الصفقات يمنح كل باقة تجربة حقيقية مع حدود متفاوتة."]
    return "\n".join(lines)


def feature_guard(feature, upgrade_text=True):
    async def checker(update, context):
        _ensure_user(update)
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id if update.effective_user else chat_id
        if is_admin_chat(chat_id) or is_admin_chat(user_id) or has_feature(chat_id, feature):
            return True
        if upgrade_text:
            await reply(update, f"🔒 هذه الميزة غير متاحة ضمن باقتك الحالية.\n\n💳 استخدم /plans لرؤية الباقات والترقية.")
        return False
    return checker


def _format_trade_message(result, title="🚨 إشارة ذهب", record=None):
    # في التحديثات، سجل الصفقة هو المصدر المرجعي لمستويات الدخول/SL/TP.
    trade = record if record else result['trade']
    direction = "🟢 شراء" if result['direction'] == "BUY" else "🔴 بيع"
    quality = "🔥 قوية" if result['score'] >= STRONG_THRESHOLD else "🎯 مؤهلة"
    status = record.get("status", "NEW") if record else "NEW"
    result_text = record.get("result", "OPEN") if record else "OPEN"
    status_ar = {
        "NEW": "🆕 جديدة", "ACTIVE": "🟢 نشطة", "TP1": "🎯 TP1 تحقق",
        "TP2": "🎯 TP2 تحقق", "CLOSED": "🔒 مغلقة"
    }.get(status, status)
    return (f"{title}\n━━━━━━━━━━━━━━━━━━\n\n"
            f"📈 الصفقة: {direction}\n💪 الجودة: {result['score']} نقطة — {quality}\n"
            f"📌 الحالة: {status_ar}\n\n"
            f"💰 السعر الحالي: {result['price']:.2f}\n📍 الدخول: {trade['entry']:.2f}\n"
            f"🛑 SL: {trade['sl']:.2f}\n🎯 TP1: {trade['tp1']:.2f}\n"
            f"🎯 TP2: {trade['tp2']:.2f}\n🎯 TP3: {trade['tp3']:.2f}\n"
            f"⚖️ R:R النهائي: 1:{trade['rr']:.2f}\n\n"
            f"🧠 التلاقي:\n" + "\n".join("• " + x for x in result['factors'][:7]) +
            f"\n\n🔁 إعادة الاختبار: {liquidity_retest_summary(result.get('mtf', {}).get('m15', {}).get('liquidity', {}))}\n"
            f"📊 نتيجة المتابعة: {result_text}\n"
            "⚠️ إشارة تحليلية للتنفيذ اليدوي.")


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



def liquidity_analysis(df, lookback=80):
    """محرك دورة السيولة الحقيقي.

    State Machine:
        SWEEP -> DISPLACEMENT -> BOS -> RETEST -> CONFIRMED
        ثم: CONFIRMED -> CONSUMED
        أو أي مرحلة فعالة -> EXPIRED / INVALIDATED عند فشل الشروط أو انتهاء العمر.

    ملاحظة: هذا استدلال من OHLC وبنية السعر وليس قراءة مباشرة لدفتر الأوامر.
    """
    base = {"bias":"محايدة", "buy_side":[], "sell_side":[], "nearest_buy":None,
            "nearest_sell":None, "sweep":None,
            "sweep_text":"لا يوجد سحب سيولة واضح في آخر شمعة مكتملة.", "score":0,
            "factors":[], "retest":{"status":"لا توجد حالة نشطة", "level":None,
            "state":None, "displacement":False, "bos":False}}
    if df is None or len(df) < 30:
        base["sweep_text"] = "لا توجد بيانات كافية لتحليل السيولة."
        return base

    x = df.tail(min(lookback, len(df))).copy()
    price = sf(x["close"].iloc[-1])
    completed = x.iloc[:-1].copy() if len(x) > 2 else x.copy()
    if len(completed) < 22:
        return base

    tf_key = "unknown"
    try:
        if "openTime" in x.columns:
            ts = pd.to_datetime(x["openTime"], utc=True, errors="coerce").dropna()
            if len(ts) >= 3:
                secs = max(1, int(ts.diff().dt.total_seconds().dropna().median()))
                tf_key = f"{secs}s"
    except Exception:
        pass

    # مستويات السيولة من swing highs/lows مكتملة فقط.
    highs, lows = [], []
    for i in range(2, len(completed)-2):
        hi = sf(completed["high"].iloc[i]); lo = sf(completed["low"].iloc[i])
        if hi >= sf(completed["high"].iloc[i-2:i].max()) and hi >= sf(completed["high"].iloc[i+1:i+3].max()):
            highs.append(hi)
        if lo <= sf(completed["low"].iloc[i-2:i].min()) and lo <= sf(completed["low"].iloc[i+1:i+3].min()):
            lows.append(lo)

    atr = max(sf(ATR(x).iloc[-1], 1), 1e-9)
    radius = max(atr * 0.30, price * 0.00030)

    def cluster(values):
        groups = []
        for v in sorted(values):
            if not groups or abs(v - sum(groups[-1])/len(groups[-1])) > radius:
                groups.append([v])
            else:
                groups[-1].append(v)
        return [sf(sum(g)/len(g), 2) for g in groups]

    buy_side = sorted(v for v in cluster(highs) if v > price)
    sell_side = sorted((v for v in cluster(lows) if v < price), reverse=True)
    nearest_buy = buy_side[0] if buy_side else None
    nearest_sell = sell_side[0] if sell_side else None

    sweep = None
    sweep_text = "لا يوجد سحب سيولة واضح في آخر شمعة مكتملة."
    factors = []
    score = 0
    sweep_level = None
    sweep_time = None
    current_idx = completed.index[-1]

    c = completed.iloc[-1]
    prior = completed.iloc[:-1].tail(20)
    ph = sf(prior["high"].max()); pl = sf(prior["low"].min())
    ch, cl, cc = sf(c["high"]), sf(c["low"]), sf(c["close"])
    if ch > ph and cc < ph:
        sweep = "BUY_SIDE_SWEEP"
        sweep_level = ph
        sweep_time = str(current_idx)
        sweep_text = f"سحب سيولة شرائية فوق {ph:.2f} ثم إغلاق أسفلها."
        factors.append("سحب سيولة شرائية")
        score = -8
    elif cl < pl and cc > pl:
        sweep = "SELL_SIDE_SWEEP"
        sweep_level = pl
        sweep_time = str(current_idx)
        sweep_text = f"سحب سيولة بيعية تحت {pl:.2f} ثم إغلاق أعلى منها."
        factors.append("سحب سيولة بيعية")
        score = 8

    # --------------------------------------------------------
    # 1) إنشاء Sweep مرة واحدة فقط. لا نعيد ضبط الحالة في كل دورة.
    # --------------------------------------------------------
    state_key = None
    if sweep and sweep_level is not None:
        state_key = f"{tf_key}:{sweep_time}:{round(sf(sweep_level), 4)}:{sweep}"
        if state_key not in LIQUIDITY_STATE:
            LIQUIDITY_STATE[state_key] = {
                "key": state_key, "sweep": sweep, "level": sweep_level,
                "sweep_time": sweep_time, "sweep_bar_index": len(completed)-1,
                "status": "SWEEP", "retest_status": "بانتظار الاندفاع بعد سحب السيولة",
                "retest_level": sweep_level, "retest_time": None, "retest_price": None,
                "retest_result": None, "last_time": sweep_time, "last_bar_index": len(completed)-1,
                "displacement": False, "displacement_time": None,
                "bos": False, "bos_time": None, "retest_start_index": None,
                "confirmed_bar_index": None, "invalidated_reason": None,
                "tf_key": tf_key,
            }

    # --------------------------------------------------------
    # 2) Lifecycle maintenance: expire/consume old states first.
    #    Terminal states remain in history but are never active again.
    # --------------------------------------------------------
    terminal = {"CONSUMED", "EXPIRED", "INVALIDATED"}
    for st in list(LIQUIDITY_STATE.values()):
        if st.get("tf_key") != tf_key or st.get("status") in terminal:
            continue
        age = (len(completed)-1) - int(st.get("sweep_bar_index", len(completed)-1))
        st["age_bars"] = max(0, age)
        st["last_bar_index"] = len(completed)-1
        st["last_time"] = str(current_idx)
        if age > LIQUIDITY_MAX_AGE_BARS:
            st["status"] = "EXPIRED"
            st["retest_status"] = "انتهى عمر حالة السيولة"
            st["retest_result"] = "EXPIRED"
            st["invalidated_reason"] = "MAX_AGE"

    # --------------------------------------------------------
    # 3) Select only the newest NON-TERMINAL state.
    #    This prevents an old INVALIDATED/CONFIRMED state from blocking.
    # --------------------------------------------------------
    candidates = [
        st for st in LIQUIDITY_STATE.values()
        if st.get("tf_key") == tf_key and st.get("status") not in terminal
    ]
    active = max(candidates, key=lambda z: int(z.get("sweep_bar_index", -1))) if candidates else None

    if active:
        level = sf(active["level"])
        sweep_pos = int(active.get("sweep_bar_index", -1))
        if sweep_pos < 0 or sweep_pos >= len(completed):
            active["status"] = "EXPIRED"
            active["retest_status"] = "انتهى عمر الحالة"
            active["retest_result"] = "EXPIRED"
            active["invalidated_reason"] = "INVALID_INDEX"
            active = None

    if active:
        # كل الحدود الزمنية محسوبة بعدد الشموع المكتملة، وليس بعدد مرات استدعاء الدالة.
        age = (len(completed)-1) - int(active.get("sweep_bar_index", len(completed)-1))
        active["age_bars"] = max(0, age)
        tol = max(atr * 0.18, price * 0.00020, 0.20)
        after = completed.iloc[int(active["sweep_bar_index"])+1:]

        # SWEEP -> DISPLACEMENT: فقط أول N شموع بعد السحب.
        if active["status"] == "SWEEP":
            checked = min(LIQUIDITY_DISPLACEMENT_MAX_BARS, len(after))
            for j in range(checked):
                cnd = after.iloc[j]
                oh, clo, hi, lo = sf(cnd["open"]), sf(cnd["close"]), sf(cnd["high"]), sf(cnd["low"])
                rng = max(hi-lo, 1e-9); body_ratio = abs(clo-oh)/rng
                away = (clo > level + tol*0.25) if active["sweep"] == "SELL_SIDE_SWEEP" else (clo < level - tol*0.25)
                if body_ratio >= 0.60 and rng >= atr*0.75 and away:
                    active["displacement"] = True
                    active["displacement_time"] = str(after.index[j])
                    active["displacement_bar_index"] = int(active["sweep_bar_index"]) + 1 + j
                    active["status"] = "DISPLACEMENT"
                    active["retest_status"] = "بانتظار تأكيد كسر الهيكل"
                    break
            else:
                if age >= LIQUIDITY_DISPLACEMENT_MAX_BARS:
                    active["status"] = "EXPIRED"
                    active["retest_status"] = "انتهى وقت الاندفاع"
                    active["retest_result"] = "EXPIRED"
                    active["invalidated_reason"] = "DISPLACEMENT_TIMEOUT"

        # DISPLACEMENT -> BOS: خلال نافذة محددة بعد الاندفاع.
        if active and active["status"] == "DISPLACEMENT":
            pre = completed.iloc[max(0, int(active["sweep_bar_index"])-20):int(active["sweep_bar_index"])]
            if len(pre) >= 4:
                pre_high = sf(pre["high"].max()); pre_low = sf(pre["low"].min())
                start = int(active.get("displacement_bar_index", active["sweep_bar_index"]+1))
                bos_window = completed.iloc[start+1:start+1+LIQUIDITY_BOS_MAX_BARS]
                for idx, row in bos_window.iterrows():
                    close = sf(row["close"])
                    if active["sweep"] == "SELL_SIDE_SWEEP" and close > pre_high:
                        active["bos"] = True; active["bos_time"] = str(idx)
                        active["bos_bar_index"] = int(completed.index.get_loc(idx))
                        active["status"] = "BOS"
                        active["retest_status"] = "بانتظار إعادة الاختبار"
                        active["retest_start_index"] = active["bos_bar_index"] + 1
                        break
                    if active["sweep"] == "BUY_SIDE_SWEEP" and close < pre_low:
                        active["bos"] = True; active["bos_time"] = str(idx)
                        active["bos_bar_index"] = int(completed.index.get_loc(idx))
                        active["status"] = "BOS"
                        active["retest_status"] = "بانتظار إعادة الاختبار"
                        active["retest_start_index"] = active["bos_bar_index"] + 1
                        break
            if active["status"] == "DISPLACEMENT":
                disp_age = (len(completed)-1) - int(active.get("displacement_bar_index", active["sweep_bar_index"]))
                if disp_age > LIQUIDITY_BOS_MAX_BARS:
                    active["status"] = "EXPIRED"
                    active["retest_status"] = "انتهى وقت تأكيد كسر الهيكل"
                    active["retest_result"] = "EXPIRED"
                    active["invalidated_reason"] = "BOS_TIMEOUT"

        # BOS -> RETEST -> CONFIRMED / INVALIDATED.
        if active and active["status"] == "BOS":
            retest_start = int(active.get("retest_start_index", len(completed)))
            retest_bars = completed.iloc[retest_start:]
            retest_age = (len(completed)-1) - int(active.get("bos_bar_index", len(completed)-1))
            if retest_age > LIQUIDITY_RETEST_MAX_BARS:
                active["status"] = "EXPIRED"
                active["retest_status"] = "انتهى وقت إعادة الاختبار"
                active["retest_result"] = "EXPIRED"
                active["invalidated_reason"] = "RETEST_TIMEOUT"
            elif len(retest_bars):
                # RETEST state is explicit as soon as the market reaches the retest window.
                active["status"] = "RETEST"
                last = retest_bars.iloc[-1]
                lc, lh, ll = sf(last["close"]), sf(last["high"]), sf(last["low"])
                if active["sweep"] == "SELL_SIDE_SWEEP":
                    touched = ll <= level + tol and lh >= level - tol
                    rejected = touched and lc > level and (level-ll) >= tol*0.25
                    invalid = lc < level - tol
                else:
                    touched = lh >= level - tol and ll <= level + tol
                    rejected = touched and lc < level and (lh-level) >= tol*0.25
                    invalid = lc > level + tol

                active["retest_time"] = str(retest_bars.index[-1])
                active["retest_price"] = lc
                active["last_time"] = str(retest_bars.index[-1])
                if invalid:
                    active["status"] = "INVALIDATED"
                    active["retest_status"] = "إعادة الاختبار فاشلة — تم إبطال المستوى"
                    active["retest_result"] = "FAILED"
                    active["invalidated_reason"] = "RETEST_INVALIDATED"
                elif rejected:
                    active["status"] = "CONFIRMED"
                    active["retest_status"] = "إعادة الاختبار ناجحة"
                    active["retest_result"] = "SUCCESS"
                    active["confirmed_bar_index"] = len(completed)-1
                elif touched:
                    active["status"] = "RETEST"
                    active["retest_status"] = "إعادة الاختبار قيد التقييم"

        # RETEST يمكن أن يبقى حتى نافذته، ثم ينتهي.
        if active and active["status"] == "RETEST":
            retest_age = (len(completed)-1) - int(active.get("bos_bar_index", len(completed)-1))
            if retest_age > LIQUIDITY_RETEST_MAX_BARS:
                active["status"] = "EXPIRED"
                active["retest_status"] = "انتهى وقت إعادة الاختبار"
                active["retest_result"] = "EXPIRED"
                active["invalidated_reason"] = "RETEST_TIMEOUT"

        # CONFIRMED حالة قابلة للاستخدام مرة واحدة فقط؛ في الشمعة التالية تصبح CONSUMED.
        if active and active["status"] == "CONFIRMED":
            confirmed_age = (len(completed)-1) - int(active.get("confirmed_bar_index", len(completed)-1))
            if confirmed_age > LIQUIDITY_CONFIRM_MAX_BARS:
                active["status"] = "CONSUMED"
                active["retest_status"] = "تم استهلاك حالة إعادة الاختبار"
                active["retest_result"] = "CONSUMED"

    # تنظيف الذاكرة مع إبقاء الحالات الحديثة فقط.
    if len(LIQUIDITY_STATE) > MAX_LIQUIDITY_STATES:
        ordered = sorted(LIQUIDITY_STATE.items(), key=lambda kv: int(kv[1].get("sweep_bar_index", -1)))
        for k, _ in ordered[:-MAX_LIQUIDITY_STATES]:
            LIQUIDITY_STATE.pop(k, None)

    # لا تستخدم الحالة terminal كحالة فعالة لاتخاذ القرار.
    terminal = {"CONSUMED", "EXPIRED", "INVALIDATED"}
    active_candidates = [st for st in LIQUIDITY_STATE.values()
                         if st.get("tf_key") == tf_key and st.get("status") not in terminal]
    active = max(active_candidates, key=lambda z: int(z.get("sweep_bar_index", -1))) if active_candidates else None

    if active:
        state = active.get("status")
        if state == "CONFIRMED":
            score += 12 if active["sweep"] == "SELL_SIDE_SWEEP" else -12
            factors.append("إعادة اختبار مؤكدة — حالة قابلة للاستخدام مرة واحدة")
        elif state == "RETEST":
            factors.append("إعادة الاختبار قيد التقييم")
        elif state == "BOS":
            factors.append("كسر الهيكل مؤكد — بانتظار إعادة الاختبار")
        elif state == "DISPLACEMENT":
            factors.append("الاندفاع مؤكد — بانتظار كسر الهيكل")
        elif state == "SWEEP":
            factors.append("سحب السيولة مؤكد — بانتظار الاندفاع")
    else:
        # إذا كانت آخر حالة انتهت، لا نعيد استخدامها ولا نمنحها نقاطاً.
        latest_terminal = [st for st in LIQUIDITY_STATE.values() if st.get("tf_key") == tf_key]
        if latest_terminal:
            latest_terminal = max(latest_terminal, key=lambda z: int(z.get("sweep_bar_index", -1)))
            if latest_terminal.get("status") == "INVALIDATED":
                factors.append("آخر حالة سيولة أُبطلت — بانتظار Sweep جديد")
            elif latest_terminal.get("status") == "EXPIRED":
                factors.append("انتهى عمر آخر حالة سيولة — بانتظار Sweep جديد")
            elif latest_terminal.get("status") == "CONSUMED":
                factors.append("تم استهلاك آخر إعادة اختبار مؤكدة — بانتظار Sweep جديد")

    if nearest_buy is not None and nearest_sell is not None:
        up = nearest_buy-price; down = price-nearest_sell
        bias = "أقرب سيولة شرائية" if up < down*0.75 else "أقرب سيولة بيعية" if down < up*0.75 else "متوازنة"
    elif nearest_buy is not None:
        bias = "سيولة شرائية فوق السعر"
    elif nearest_sell is not None:
        bias = "سيولة بيعية تحت السعر"
    else:
        bias = "محايدة"
    if sweep == "BUY_SIDE_SWEEP": bias = "سحب سيولة شرائية"
    elif sweep == "SELL_SIDE_SWEEP": bias = "سحب سيولة بيعية"
    if nearest_buy is not None: factors.append(f"تجمع سيولة شرائية عند {nearest_buy:.2f}")
    if nearest_sell is not None: factors.append(f"تجمع سيولة بيعية عند {nearest_sell:.2f}")

    if active:
        retest = {
            "status": active.get("retest_status", "لا توجد حالة نشطة"),
            "level": active.get("level"), "time": active.get("retest_time"),
            "price": active.get("retest_price"), "result": active.get("retest_result"),
            "state": active.get("status"), "displacement": active.get("displacement", False),
            "bos": active.get("bos", False), "age_bars": active.get("age_bars", 0),
            "sweep_time": active.get("sweep_time"), "sweep_bar_index": active.get("sweep_bar_index"),
            "bos_time": active.get("bos_time"), "key": active.get("key")
        }
    else:
        # نُظهر آخر حالة فقط كمعلومة، لكن state terminal لا يدخل كحالة فعالة.
        terminal_states = [st for st in LIQUIDITY_STATE.values() if st.get("tf_key") == tf_key]
        last = max(terminal_states, key=lambda z: int(z.get("sweep_bar_index", -1))) if terminal_states else None
        retest = {
            "status": (last.get("retest_status") if last else "لا توجد حالة نشطة"),
            "level": (last.get("level") if last else None),
            "time": (last.get("retest_time") if last else None),
            "price": (last.get("retest_price") if last else None),
            "result": (last.get("retest_result") if last else None),
            "state": (last.get("status") if last else None),
            "displacement": bool(last.get("displacement")) if last else False,
            "bos": bool(last.get("bos")) if last else False,
            "age_bars": last.get("age_bars", 0) if last else 0,
            "sweep_time": last.get("sweep_time") if last else None,
            "sweep_bar_index": last.get("sweep_bar_index") if last else None,
            "bos_time": last.get("bos_time") if last else None,
            "key": last.get("key") if last else None,
        }

    return {"bias":bias, "buy_side":buy_side[:3], "sell_side":sell_side[:3],
            "nearest_buy":nearest_buy, "nearest_sell":nearest_sell,
            "sweep":sweep or (active.get("sweep") if active else (retest.get("sweep") if isinstance(retest,dict) else None)),
            "sweep_text": sweep_text if sweep else (f"آخر سحب مسجل: {retest.get('sweep_time')} عند {sf(retest.get('level')):.2f}." if retest.get("level") is not None else sweep_text),
            "score":score, "factors":factors[:8], "retest":retest}


def liquidity_retest_summary(liq):
    r=liq.get("retest",{}) if isinstance(liq,dict) else {}
    state=r.get("state")
    if state=="RETEST_CONFIRMED": return f"🟢 إعادة الاختبار ناجحة عند {fmt(r.get('level'))}"
    if state=="RETEST_PENDING": return f"🟡 إعادة الاختبار قيد التقييم عند {fmt(r.get('level'))}"
    if state=="INVALIDATED": return f"🔴 إعادة الاختبار فاشلة — تم إبطال المستوى {fmt(r.get('level'))}"
    if state=="SWEPT": return f"🔵 بانتظار الاندفاع بعد سحب السيولة عند {fmt(r.get('level'))}"
    if state=="DISPLACEMENT_CONFIRMED": return "🟡 الاندفاع مؤكد — بانتظار تأكيد كسر الهيكل"
    if state=="STRUCTURE_CONFIRMED": return f"🟠 كسر الهيكل مؤكد — بانتظار إعادة الاختبار عند {fmt(r.get('level'))}"
    if state=="RETEST_PENDING": return f"🔵 بانتظار إعادة الاختبار للمستوى {fmt(r.get('level'))}"
    return "⚪ لا توجد إعادة اختبار مؤكدة حالياً"


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


def _zone_price(level):
    """استخراج سعر المنطقة بأمان سواء كانت dict أو قيمة رقمية."""
    if level is None:
        return None
    try:
        if isinstance(level, dict):
            value = level.get("price")
        else:
            value = level
        value = float(value)
        return value if math.isfinite(value) and value > 0 else None
    except (TypeError, ValueError):
        return None


def nearest_support(levels, price):
    """أقرب دعم فعلي تحت السعر الحالي."""
    try:
        p = float(price)
        if not math.isfinite(p):
            return None
        candidates = []
        for key in ("support1", "support2", "support3"):
            z = _zone_price(levels.get(key) if isinstance(levels, dict) else None)
            if z is not None and z < p:
                candidates.append(z)
        return max(candidates) if candidates else None
    except (TypeError, ValueError):
        return None


def nearest_resistance(levels, price):
    """أقرب مقاومة فعلية فوق السعر الحالي."""
    try:
        p = float(price)
        if not math.isfinite(p):
            return None
        candidates = []
        for key in ("resistance1", "resistance2", "resistance3"):
            z = _zone_price(levels.get(key) if isinstance(levels, dict) else None)
            if z is not None and z > p:
                candidates.append(z)
        return min(candidates) if candidates else None
    except (TypeError, ValueError):
        return None


def format_zone(levels, key):
    """تنسيق منطقة دعم/مقاومة للرسائل، مع حماية من None."""
    try:
        level = levels.get(key) if isinstance(levels, dict) else None
        if level is None:
            return "غير متاح"
        price = _zone_price(level)
        if price is None:
            return "غير متاح"
        if isinstance(level, dict):
            strength = level.get("strength")
            touches = level.get("touches")
            parts = [f"{price:.2f}"]
            if strength is not None:
                try:
                    parts.append(f"قوة {float(strength):.0f}")
                except (TypeError, ValueError):
                    pass
            if touches is not None:
                try:
                    parts.append(f"{int(touches)} لمسات")
                except (TypeError, ValueError):
                    pass
            return " | ".join(parts)
        return f"{price:.2f}"
    except Exception:
        return "غير متاح"

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
    """تحليل فريم مستقل بمعادلة متوازنة من 100 نقطة.

    الأوزان الثابتة:
      EMA Trend       25
      RSI Momentum    10
      MACD            15
      ADX + DI        10
      Market Structure15
      Volume          10
      Liquidity       15
    المجموع = 100 نقطة كحد أقصى.

    كل عامل يصوّت بشكل مستقل BUY أو SELL أو NEUTRAL؛ وبعد جمع الأصوات تُحسب
    الدرجة النهائية كقوة صافية = الفائز - المعارض، لذلك يظهر التعارض داخل الدرجة.
    """
    if df is None or len(df) < 30:
        raise ValueError("بيانات غير كافية للتحليل")

    # نستخدم آخر شمعة مكتملة للمؤشرات لتقليل تغيّر الدرجة أثناء تكوّن الشمعة.
    # إذا كانت البيانات لا تسمح بذلك، نستخدم آخر صف متاح بأمان.
    work = df.iloc[:-1].copy() if len(df) >= 31 else df.copy()
    if len(work) < 30:
        work = df.copy()

    close = work["close"]
    price = sf(close.iloc[-1])
    ema50 = sf(EMA(close, 50).iloc[-1])
    ema200 = sf(EMA(close, 200).iloc[-1])

    # RSI محسوب هنا بصورة صريحة لمعالجة حالة avg_loss=0 بدلاً من تحويلها إلى 50.
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
    avg_loss = loss.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    rsi_series = 100 - (100 / (1 + rs))
    # الحالات الحدية: صعود متواصل = 100، هبوط متواصل = 0،
    # وسوق مسطح بلا مكاسب/خسائر = 50 محايد، وليس SELL تلقائياً.
    rsi_series = rsi_series.where(~((avg_loss == 0) & (avg_gain > 0)), 100.0)
    rsi_series = rsi_series.where(~((avg_gain == 0) & (avg_loss > 0)), 0.0)
    rsi_series = rsi_series.where(~((avg_gain == 0) & (avg_loss == 0)), 50.0)
    rsi = sf(rsi_series.iloc[-1], 50)

    macd_line, macd_sig, macd_hist = MACD(close)
    ml = sf(macd_line.iloc[-1]); ms = sf(macd_sig.iloc[-1]); mh = sf(macd_hist.iloc[-1])
    adx = sf(ADX(work).iloc[-1])
    struct = structure(work)
    vol_state, vol_ratio = volume_analysis(work)
    fib = fibonacci(work)
    fvg = find_fvg(work)
    atr = sf(ATR(work).iloc[-1], 1)
    liquidity = liquidity_analysis(df)

    # ---------- مكوّنات الاتجاه: كل مكوّن مستقل ----------
    bull = 0.0
    bear = 0.0
    reasons = []
    components = {
        "ema": {"buy": 0, "sell": 0, "max": 25},
        "rsi": {"buy": 0, "sell": 0, "max": 10},
        "macd": {"buy": 0, "sell": 0, "max": 15},
        "adx_di": {"buy": 0, "sell": 0, "max": 10},
        "structure": {"buy": 0, "sell": 0, "max": 15},
        "volume": {"buy": 0, "sell": 0, "max": 10},
        "liquidity": {"buy": 0, "sell": 0, "max": 15},
    }

    # 1) EMA Trend = 25 نقطة: EMA50 (10) + EMA200 (15)
    if price > ema50:
        components["ema"]["buy"] += 10
        reasons.append("السعر فوق EMA50")
    elif price < ema50:
        components["ema"]["sell"] += 10
        reasons.append("السعر تحت EMA50")
    if price > ema200:
        components["ema"]["buy"] += 15
        reasons.append("السعر فوق EMA200")
    elif price < ema200:
        components["ema"]["sell"] += 15
        reasons.append("السعر تحت EMA200")
    bull += components["ema"]["buy"]; bear += components["ema"]["sell"]

    # 2) RSI Momentum = 10 نقطة. لا نعكس الاتجاه تلقائياً عند التشبع؛
    # التشبع القوي يعني زخماً قوياً، مع ترك القرار النهائي لبقية المكونات.
    if rsi >= 60:
        p = 10 if rsi >= 65 else 7
        components["rsi"]["buy"] = p; bull += p
        reasons.append("RSI يدعم الزخم الصاعد")
    elif rsi >= 55:
        components["rsi"]["buy"] = 5; bull += 5
        reasons.append("RSI إيجابي")
    elif rsi <= 40:
        p = 10 if rsi <= 35 else 7
        components["rsi"]["sell"] = p; bear += p
        reasons.append("RSI يدعم الزخم الهابط")
    elif rsi <= 45:
        components["rsi"]["sell"] = 5; bear += 5
        reasons.append("RSI سلبي")

    # 3) MACD = 15 نقطة: الخط/الإشارة 10 + الهيستوغرام 5.
    if ml > ms:
        components["macd"]["buy"] += 10
        bull += 10
        if mh > 0:
            components["macd"]["buy"] += 5; bull += 5
        reasons.append("MACD يدعم الشراء")
    elif ml < ms:
        components["macd"]["sell"] += 10
        bear += 10
        if mh < 0:
            components["macd"]["sell"] += 5; bear += 5
        reasons.append("MACD يدعم البيع")

    # 4) ADX + DI = 10 نقطة. ADX يقيس القوة وDI يحدد الجهة.
    high = work["high"]; low = work["low"]
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = tr1.combine(tr2, max).combine(tr3, max)
    atr14 = tr.ewm(alpha=1/14, adjust=False).mean().replace(0, float("nan"))
    plus_di = 100 * plus_dm.ewm(alpha=1/14, adjust=False).mean() / atr14
    minus_di = 100 * minus_dm.ewm(alpha=1/14, adjust=False).mean() / atr14
    pdi = sf(plus_di.iloc[-1], 0); mdi = sf(minus_di.iloc[-1], 0)
    adx_points = 10 if adx >= 25 else 6 if adx >= 18 else 2
    if pdi > mdi:
        components["adx_di"]["buy"] = adx_points; bull += adx_points
        reasons.append(f"ADX {adx:.1f} مع +DI متفوق")
    elif mdi > pdi:
        components["adx_di"]["sell"] = adx_points; bear += adx_points
        reasons.append(f"ADX {adx:.1f} مع -DI متفوق")

    # 5) Market Structure = 15 نقطة
    if struct == "صاعد":
        components["structure"]["buy"] = 15; bull += 15
        reasons.append("هيكل السوق صاعد")
    elif struct == "هابط":
        components["structure"]["sell"] = 15; bear += 15
        reasons.append("هيكل السوق هابط")

    # 6) Volume = 10 نقطة. الحجم تأكيد وليس اتجاهًا بذاته؛ الاتجاه يستمد من
    # جسم الشمعة المكتملة الأخيرة مقارنة بالإغلاق السابق.
    if len(work) >= 2:
        last_open = sf(work["open"].iloc[-1], price)
        last_close = sf(work["close"].iloc[-1], price)
        prev_close = sf(work["close"].iloc[-2], price)
        if vol_ratio >= 1.40:
            vp = 10
        elif vol_ratio >= 1.20:
            vp = 8
        elif vol_ratio >= 1.00:
            vp = 5
        elif vol_ratio >= 0.90:
            vp = 2
        else:
            vp = 0
        if vp:
            if last_close > last_open and last_close >= prev_close:
                components["volume"]["buy"] = vp; bull += vp
                reasons.append("الحجم يؤكد الدفع الصاعد")
            elif last_close < last_open and last_close <= prev_close:
                components["volume"]["sell"] = vp; bear += vp
                reasons.append("الحجم يؤكد الضغط الهابط")

    # 7) Liquidity = 15 نقطة مستقلة.
    # إعادة الاختبار الناجحة هي أقوى إشارة؛ السحب وحده أقل قوة؛ الحالة
    # المحايدة لا تمنح نقاطاً حتى لا نخلق أفضلية وهمية.
    retest = liquidity.get("retest", {}) if isinstance(liquidity, dict) else {}
    liq_state = retest.get("state")
    sweep = liquidity.get("sweep") if isinstance(liquidity, dict) else None
    if liq_state == "RETEST_CONFIRMED":
        if sweep == "SELL_SIDE_SWEEP":
            components["liquidity"]["buy"] = 15; bull += 15
            reasons.append("إعادة اختبار ناجحة بعد سحب سيولة بيعية")
        elif sweep == "BUY_SIDE_SWEEP":
            components["liquidity"]["sell"] = 15; bear += 15
            reasons.append("إعادة اختبار ناجحة بعد سحب سيولة شرائية")
    elif sweep == "SELL_SIDE_SWEEP":
        components["liquidity"]["buy"] = 10; bull += 10
        reasons.append("سحب سيولة بيعية يدعم الارتداد الصاعد")
    elif sweep == "BUY_SIDE_SWEEP":
        components["liquidity"]["sell"] = 10; bear += 10
        reasons.append("سحب سيولة شرائية يدعم الضغط الهابط")
    else:
        bias = liquidity.get("bias") if isinstance(liquidity, dict) else None
        if bias in ("أقرب سيولة بيعية", "سيولة بيعية تحت السعر"):
            components["liquidity"]["sell"] = 5; bear += 5
            reasons.append("السيولة القريبة تميل للضغط الهابط")
        elif bias in ("أقرب سيولة شرائية", "سيولة شرائية فوق السعر"):
            components["liquidity"]["buy"] = 5; bull += 5
            reasons.append("السيولة القريبة تميل للدفع الصاعد")

    # الدرجة النهائية = القوة الصافية، وليس قوة الجانب الفائز وحده.
    # مثال: 80 BUY مقابل 20 SELL => الدرجة 60، وليس 80.
    # هذا يمنع تضخيم الدرجة عندما توجد أدلة معاكسة قوية.
    if bull > bear:
        direction = "BUY"
        raw_winner = bull
        raw_loser = bear
    elif bear > bull:
        direction = "SELL"
        raw_winner = bear
        raw_loser = bull
    else:
        direction = "WAIT"
        raw_winner = 0.0
        raw_loser = 0.0

    net_score = abs(bull - bear) if direction != "WAIT" else 0.0
    score = int(max(0, min(100, round(net_score))))
    quality, quality_icon = trade_quality(score)
    conflict_points = int(round(min(bull, bear)))

    return {
        "price": price, "ema50": ema50, "ema200": ema200, "rsi": rsi,
        "macd": ml, "macd_signal": ms, "macd_hist": mh, "adx": adx,
        "plus_di": pdi, "minus_di": mdi, "structure": struct,
        "volume_state": vol_state, "volume_ratio": vol_ratio, "fib": fib,
        "fvg": fvg, "atr": atr, "liquidity": liquidity,
        "direction": direction, "score": score, "net_score": score,
        "raw_winner_score": int(round(raw_winner)),
        "raw_loser_score": int(round(raw_loser)),
        "bull_score": int(round(bull)), "bear_score": int(round(bear)),
        "conflict_points": conflict_points,
        "score_components": components, "state": quality, "quality": quality,
        "quality_icon": quality_icon, "reasons": reasons,
    }


# ============================================================
# التحليل متعدد الفريمات
# ============================================================

def multi_timeframe():
    """تحليل متعدد الفريمات بدرجة صافية موزونة من 100 نقطة.

    كل فريم يحتفظ بدرجته الصافية 0..100 القادمة من analyze().
    الوزن يحدد مقدار مساهمة الفريم، ولا تتم إعادة تطبيع الدرجة على وزن
    الاتجاه الفائز؛ لأن إعادة التطبيع قد تضخم الإشارة عندما تكون فريمات
    كثيرة محايدة أو معارضة.
    """
    w1 = analyze(get_bars("1w", 250))
    d1 = analyze(get_bars("1d", 300))
    h4 = analyze(get_bars("4h", 300))
    h1 = analyze(get_bars("1h", 300))
    m15 = analyze(get_bars("15m", 300))

    frames = [w1, d1, h4, h1, m15]
    weights = [0.15, 0.25, 0.25, 0.20, 0.15]
    names = ["w1", "d1", "h4", "h1", "m15"]

    # الدرجة الموزونة لكل جانب. مجموع الأوزان = 1.00.
    buy_score = sum(x["score"] * w for x, w in zip(frames, weights) if x["direction"] == "BUY")
    sell_score = sum(x["score"] * w for x, w in zip(frames, weights) if x["direction"] == "SELL")
    buy_weight = sum(w for x, w in zip(frames, weights) if x["direction"] == "BUY")
    sell_weight = sum(w for x, w in zip(frames, weights) if x["direction"] == "SELL")
    wait_weight = sum(w for x, w in zip(frames, weights) if x["direction"] not in ("BUY", "SELL"))

    # الاتجاه يحدده صافي الدليل الموزون.
    net_score = buy_score - sell_score
    if net_score > 0:
        final_direction = "BUY"
    elif net_score < 0:
        final_direction = "SELL"
    else:
        final_direction = "WAIT"

    # لا نقسم على وزن الاتجاه الفائز.
    # بما أن الأوزان مجموعها 100% وكل score بين 0 و100، فإن صافي الدرجة
    # يبقى طبيعياً ضمن -100..+100 ولا يتضخم بسبب الفريمات المحايدة.
    final_score = int(max(0, min(100, round(abs(net_score)))))

    bullish_frames = [names[i] for i, x in enumerate(frames) if x["direction"] == "BUY"]
    bearish_frames = [names[i] for i, x in enumerate(frames) if x["direction"] == "SELL"]
    neutral_frames = [names[i] for i, x in enumerate(frames) if x["direction"] not in ("BUY", "SELL")]

    if final_direction == "WAIT":
        agreement = "محايد"
    else:
        coverage = buy_weight if final_direction == "BUY" else sell_weight
        opposite_weight = sell_weight if final_direction == "BUY" else buy_weight
        if coverage >= 0.75 and opposite_weight == 0 and wait_weight == 0:
            agreement = "قوي جداً"
        elif coverage >= 0.75 and opposite_weight == 0:
            agreement = "قوي مع فريمات محايدة"
        elif coverage >= 0.75:
            agreement = "قوي مع تعارض"
        elif coverage >= 0.50:
            agreement = "متوسط"
        else:
            agreement = "ضعيف"

    return {
        "w1": w1, "d1": d1, "h4": h4, "h1": h1, "m15": m15,
        "direction": final_direction,
        "score": final_score,
        "buy_score": round(buy_score, 1),
        "sell_score": round(sell_score, 1),
        "net_score": round(net_score, 1),
        "buy_weight": round(buy_weight, 2),
        "sell_weight": round(sell_weight, 2),
        "wait_weight": round(wait_weight, 2),
        "bullish_frames": bullish_frames,
        "bearish_frames": bearish_frames,
        "neutral_frames": neutral_frames,
        "agreement": agreement,
        "conflict": bool(bullish_frames and bearish_frames),
    }


def build_trade(direction, h1, m15, levels):
    """بناء صفقة منضبطة لا تضخم درجة الإشارة.

    القاعدة: هذه الدالة لا تمنح أي نقاط ولا تعدّل score؛ مهمتها فقط
    تحويل الإشارة المعتمدة إلى Entry/SL/TP1/TP2/TP3 مع تحقق صارم من
    الاتجاه، المسافات، وصحة R:R. إذا تعذر بناء صفقة منطقية تعيد None.
    """
    try:
        direction = str(direction).upper().strip()
        if direction not in ("BUY", "SELL"):
            return None

        m15 = m15 or {}
        h1 = h1 or {}
        levels = levels or {}

        entry = float(m15.get("price", 0.0))
        m15_atr = float(m15.get("atr", 0.0))
        h1_atr = float(h1.get("atr", 0.0))
        atr_candidates = [x for x in (m15_atr, h1_atr) if math.isfinite(x) and x > 0]
        if not math.isfinite(entry) or entry <= 0 or not atr_candidates:
            return None

        # نعتمد ATR الـM15 أساساً، مع حد أدنى صغير يمنع مسافات صفرية.
        atr = max(m15_atr if math.isfinite(m15_atr) and m15_atr > 0 else h1_atr, 0.50)
        if not math.isfinite(atr) or atr <= 0:
            return None

        def level_price(name):
            item = levels.get(name)
            if not isinstance(item, dict):
                return None
            value = item.get("price")
            try:
                value = float(value)
            except Exception:
                return None
            return value if math.isfinite(value) and value > 0 else None

        supports = sorted(
            [p for p in (level_price("support1"), level_price("support2"), level_price("support3"))
             if p is not None and p < entry],
            reverse=True
        )
        resistances = sorted(
            [p for p in (level_price("resistance1"), level_price("resistance2"), level_price("resistance3"))
             if p is not None and p > entry]
        )

        # حد منطقي للمخاطرة: لا نقبل SL قريباً جداً أو بعيداً جداً.
        min_risk = max(atr * 0.80, entry * 0.00035)
        max_risk = atr * 3.00

        if direction == "BUY":
            candidate_sl = supports[0] - atr * 0.15 if supports else entry - atr
            sl = min(candidate_sl, entry - min_risk)
            risk = entry - sl
            if risk > max_risk:
                # إذا كان الدعم بعيداً جداً، نستخدم مخاطرة قياسية بدلاً من مطاردة الدعم.
                sl = entry - min_risk
                risk = entry - sl

            if not (sl < entry and min_risk <= risk <= max_risk):
                return None

            # الأهداف مبنية على R مع احترام المقاومات القريبة.
            natural_tp1 = entry + max(risk * 1.00, atr * 1.00)
            natural_tp2 = entry + max(risk * 1.70, atr * 1.70)
            natural_tp3 = entry + max(risk * 2.40, atr * 2.40)

            tp1 = natural_tp1
            tp2 = natural_tp2
            tp3 = natural_tp3

            # المقاومة الأولى/الثانية يمكن أن تكون أهدافاً واقعية، لكن لا نضع الهدف خلفها.
            if resistances:
                r1 = resistances[0]
                if entry + risk * 0.85 <= r1 <= entry + atr * 1.60:
                    tp1 = r1
            if len(resistances) >= 2:
                r2 = resistances[1]
                if tp1 < r2 <= entry + max(atr * 2.60, risk * 2.20):
                    tp2 = r2
            if len(resistances) >= 3:
                r3 = resistances[2]
                if tp2 < r3 <= entry + max(atr * 3.80, risk * 3.20):
                    tp3 = r3

            # ترتيب صارم للأهداف، مع مسافة دنيا بين كل هدف.
            tp1 = max(tp1, entry + risk * 0.90)
            tp2 = max(tp2, tp1 + atr * 0.35, entry + risk * 1.60)
            tp3 = max(tp3, tp2 + atr * 0.50, entry + risk * 2.40)

        else:  # SELL
            candidate_sl = resistances[0] + atr * 0.15 if resistances else entry + atr
            sl = max(candidate_sl, entry + min_risk)
            risk = sl - entry
            if risk > max_risk:
                sl = entry + min_risk
                risk = sl - entry

            if not (sl > entry and min_risk <= risk <= max_risk):
                return None

            natural_tp1 = entry - max(risk * 1.00, atr * 1.00)
            natural_tp2 = entry - max(risk * 1.70, atr * 1.70)
            natural_tp3 = entry - max(risk * 2.40, atr * 2.40)

            tp1 = natural_tp1
            tp2 = natural_tp2
            tp3 = natural_tp3

            if supports:
                s1 = supports[0]
                if entry - risk * 0.85 >= s1 >= entry - atr * 1.60:
                    tp1 = s1
            if len(supports) >= 2:
                s2 = supports[1]
                if tp1 > s2 >= entry - max(atr * 2.60, risk * 2.20):
                    tp2 = s2
            if len(supports) >= 3:
                s3 = supports[2]
                if tp2 > s3 >= entry - max(atr * 3.80, risk * 3.20):
                    tp3 = s3

            tp1 = min(tp1, entry - risk * 0.90)
            tp2 = min(tp2, tp1 - atr * 0.35, entry - risk * 1.60)
            tp3 = min(tp3, tp2 - atr * 0.50, entry - risk * 2.40)

        values = (entry, sl, tp1, tp2, tp3, risk)
        if not all(math.isfinite(v) for v in values) or risk <= 0:
            return None

        if direction == "BUY":
            if not (sl < entry < tp1 < tp2 < tp3):
                return None
        else:
            if not (sl > entry > tp1 > tp2 > tp3):
                return None

        reward = abs(tp3 - entry)
        rr = reward / risk
        if not math.isfinite(rr) or rr < 1.20:
            return None

        return {
            "entry": round(entry, 2),
            "sl": round(sl, 2),
            "tp1": round(tp1, 2),
            "tp2": round(tp2, 2),
            "tp3": round(tp3, 2),
            "rr": round(rr, 2),
        }
    except Exception:
        return None


def _parse_news_datetime(value, time_value=None):
    """تحويل وقت الخبر إلى توقيت دمشق مع دعم صيغ CSV الشائعة."""
    try:
        raw = str(value or "").strip()
        if time_value is not None and str(time_value).strip():
            raw = f"{raw} {str(time_value).strip()}"
        if not raw or raw.lower() in {"nan", "none", "date"}:
            return None
        raw = raw.replace("Z", "+00:00")
        dt = pd.to_datetime(raw, errors="coerce")
        if pd.isna(dt):
            return None
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.tz_localize("Europe/London")
        else:
            dt = dt.tz_convert("Europe/London")
        return dt.to_pydatetime().astimezone(DAMASCUS)
    except Exception:
        return None


def get_news_events(force=False):
    """جلب تقويم اقتصادي عالي التأثير من Forex Factory Weekly CSV.

    أوقات التقويم المصدرية هي Europe/London؛ يتم تحويلها إلى توقيت دمشق.
    عند تعذر المصدر لا نعيد قائمة فارغة صامتاً، بل نرفع خطأ كي يستطيع
    news_filter() إيقاف التداول احترازياً بدلاً من السماح بإشارة بلا حماية.
    """
    global NEWS_CACHE
    now_ts = time.time()
    if not force and NEWS_CACHE.get("events") and now_ts - NEWS_CACHE.get("time", 0) < NEWS_CACHE_SECONDS:
        return NEWS_CACHE["events"]

    url = os.environ.get(
        "NEWS_CALENDAR_URL",
        "https://www.forexfactory.com/calendar?export=csv&week=this"
    )
    headers = {"User-Agent": "Mozilla/5.0 XAU-Smart-Trader/18.17"}
    response = requests.get(url, timeout=12, headers=headers)
    response.raise_for_status()
    text = response.text
    if not text.strip():
        raise RuntimeError("تقويم الأخبار فارغ")

    from io import StringIO
    try:
        frame = pd.read_csv(StringIO(text))
    except Exception:
        frame = pd.read_csv(StringIO(text), sep=None, engine="python")

    if frame.empty:
        raise RuntimeError("لم يتم العثور على أحداث في التقويم")

    cols = {str(c).strip().lower(): c for c in frame.columns}
    def col(*names):
        for name in names:
            if name in cols:
                return cols[name]
        return None

    date_col = col("date", "date/time", "datetime", "event date")
    time_col = col("time", "event time")
    currency_col = col("currency", "curr")
    impact_col = col("impact", "importance")
    event_col = col("event", "title", "detail", "name")
    if not date_col or not currency_col or not impact_col:
        raise RuntimeError("صيغة تقويم الأخبار غير متوافقة")

    events = []
    for _, row in frame.iterrows():
        impact = str(row.get(impact_col, "")).strip().lower()
        currency = str(row.get(currency_col, "")).strip().upper()
        # Forex Factory يميز High Impact؛ نقبل أيضاً صيغاً نصية بديلة.
        if impact not in {"high", "high impact", "3", "red", "high impact expected"}:
            continue
        dt = _parse_news_datetime(row.get(date_col), row.get(time_col) if time_col else None)
        if dt is None:
            continue
        event_name = str(row.get(event_col, "خبر اقتصادي مرتفع التأثير")) if event_col else "خبر اقتصادي مرتفع التأثير"
        events.append({
            "time": dt,
            "currency": currency,
            "impact": "HIGH",
            "event": event_name.strip(),
        })

    NEWS_CACHE = {"time": now_ts, "events": events}
    return events


def news_filter():
    """بوابة حماية الأخبار قبل/بعد الأخبار عالية التأثير.

    عند فشل تحديث المصدر مع تفعيل الحماية، نمنع الصفقة احترازياً حتى لا
    تتحول مشكلة المصدر إلى تصريح تداول غير محمي.
    """
    if not NEWS_FILTER_ENABLED:
        return False, "🟢 فلتر الأخبار متوقف."
    try:
        now = now_damascus()
        events = get_news_events()
        active = []
        for event in events:
            if event.get("impact") != "HIGH":
                continue
            event_time = event.get("time")
            if not isinstance(event_time, datetime):
                continue
            delta_min = (now - event_time).total_seconds() / 60.0
            if -NEWS_BEFORE_MIN <= delta_min <= NEWS_AFTER_MIN:
                active.append((event, delta_min))

        if active:
            details = []
            for event, delta in sorted(active, key=lambda item: abs(item[1]))[:3]:
                event_time = event["time"].strftime("%Y-%m-%d %H:%M")
                details.append(f"🚨 {event.get('currency','')} — {event.get('event','خبر مرتفع التأثير')} | {event_time} دمشق")
            return True, (
                f"🚫 التداول محجوب بسبب خبر عالي التأثير ضمن نافذة ±{NEWS_BEFORE_MIN}/{NEWS_AFTER_MIN} دقيقة.\n"
                + "\n".join(details)
            )

        return False, f"🟢 لا يوجد خبر عالي التأثير ضمن نافذة ±{NEWS_BEFORE_MIN}/{NEWS_AFTER_MIN} دقيقة."
    except Exception as exc:
        logger.exception("News calendar error")
        return True, f"🚫 التداول محجوب احترازياً: تعذر تحديث تقويم الأخبار ({exc})."


def evaluate_signal():
    global LAST_ANALYSIS
    mtf = multi_timeframe()
    direction = mtf["direction"]
    base_score = float(mtf["score"])
    d1, h4, h1, m15 = mtf["d1"], mtf["h4"], mtf["h1"], mtf["m15"]
    quote = live_price()
    price = float(quote["price"])
    levels = support_resistance(get_bars("1h", 250))
    institutional = institutional_analysis(get_bars("1h", 250))
    factors = []

    # --------------------------------------------------------
    # بوابة الأخبار: يتم تقييمها قبل السماح النهائي بالصفقة.
    # --------------------------------------------------------
    news_blocked, news_text = news_filter()
    if news_blocked:
        factors.append("التداول محجوب بسبب حماية الأخبار")

    # --------------------------------------------------------
    # التلاقي الثانوي: لا يملك صلاحية تجاوز MTF الضعيف.
    # --------------------------------------------------------
    confluence = 0
    if direction == "BUY":
        if d1["price"] > d1["ema200"]:
            confluence += 15; factors.append("الاتجاه اليومي فوق EMA200")
        if h4["price"] > h4["ema200"]:
            confluence += 15; factors.append("H4 فوق EMA200")
        if h4["structure"] == "صاعد":
            confluence += 12; factors.append("هيكل H4 صاعد")
        if m15["macd"] > m15["macd_signal"]:
            confluence += 10; factors.append("MACD M15 إيجابي")
    elif direction == "SELL":
        if d1["price"] < d1["ema200"]:
            confluence += 15; factors.append("الاتجاه اليومي تحت EMA200")
        if h4["price"] < h4["ema200"]:
            confluence += 15; factors.append("H4 تحت EMA200")
        if h4["structure"] == "هابط":
            confluence += 12; factors.append("هيكل H4 هابط")
        if m15["macd"] < m15["macd_signal"]:
            confluence += 10; factors.append("MACD M15 سلبي")

    if float(m15.get("volume_ratio", 0)) >= 1.10:
        confluence += 10; factors.append("الحجم أعلى من الطبيعي")
    fib = h1.get("fib", {}).get("61.8")
    if fib is not None and abs(price - float(fib)) <= max(float(h1.get("atr", 0)) * 0.60, price * 0.0005):
        confluence += 8; factors.append("تلاقي مع فيبوناتشي 61.8%")
    fvg = m15.get("fvg")
    if fvg and ((direction == "BUY" and fvg.get("type") == "صاعدة") or (direction == "SELL" and fvg.get("type") == "هابطة")):
        confluence += 5; factors.append("FVG متوافقة مع الاتجاه")

    sr_points = 0
    m15_atr = float(m15.get("atr", 0) or 0)
    if direction == "BUY":
        support = nearest_support(levels, price)
        if support is not None and m15_atr > 0 and price - support <= m15_atr * 1.8:
            sr_points = 10; factors.append("السعر قريب من دعم فعلي")
    elif direction == "SELL":
        resistance = nearest_resistance(levels, price)
        if resistance is not None and m15_atr > 0 and resistance - price <= m15_atr * 1.8:
            sr_points = 10; factors.append("السعر قريب من مقاومة فعلية")

    inst_points, inst_factors = institutional_adjustment(direction, institutional)
    factors.extend(inst_factors)

    confluence_score = (confluence / 75.0) * 25.0
    institutional_score = max(-5.0, min(5.0, float(inst_points)))
    final_score = int(max(0, min(100, round(
        base_score * 0.60 + confluence_score + sr_points + institutional_score
    ))))
    quality, quality_icon = trade_quality(final_score)

    # --------------------------------------------------------
    # Liquidity Retest Gate: INVALIDATED = ممنوع تداول.
    # نستخدم M15 لأنه فريم التنفيذ في الإشارة.
    # --------------------------------------------------------
    liquidity = m15.get("liquidity", {}) if isinstance(m15.get("liquidity", {}), dict) else {}
    retest = liquidity.get("retest", {}) if isinstance(liquidity.get("retest", {}), dict) else {}
    liquidity_state = retest.get("state")
    liquidity_blocked = liquidity_state == "INVALIDATED"
    if liquidity_blocked:
        factors.append("إعادة اختبار السيولة فاشلة — تم إبطال المستوى")

    valid = (
        direction in ("BUY", "SELL")
        and base_score >= 40
        and final_score >= MIN_TRADE_SCORE
        and not news_blocked
        and not liquidity_blocked
    )

    trade = build_trade(direction, h1, m15, levels) if valid else None
    if valid and trade is None:
        valid = False
        factors.append("تعذر بناء صفقة منطقية من السعر وATR والدعوم/المقاومات")
    elif trade and trade.get("rr", 0) < 1.20:
        valid = False
        trade = None
        factors.append("R:R أقل من 1:1.20")

    result = {
        "signal": valid, "direction": direction, "score": final_score,
        "quality": quality, "quality_icon": quality_icon, "price": price,
        "levels": levels, "news_blocked": news_blocked, "news": news_text,
        "liquidity_blocked": liquidity_blocked,
        "liquidity_state": liquidity_state,
        "liquidity": liquidity,
        "factors": factors, "trade": trade, "mtf": mtf,
        "institutional": institutional,
    }
    LAST_ANALYSIS = result
    return result


def _trade_key(trade):
    try:
        return "|".join([str(trade.get("direction", ""))] + [f"{float(trade.get(k, 0)):.4f}" for k in ("entry","sl","tp1","tp2","tp3")])
    except Exception:
        return None


def _trade_db_connect():
    conn = sqlite3.connect(TRADE_DB_PATH, timeout=10)
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("CREATE TABLE IF NOT EXISTS trades (trade_key TEXT PRIMARY KEY, payload TEXT NOT NULL, updated_at TEXT NOT NULL)")
    conn.execute("""CREATE TABLE IF NOT EXISTS trade_notifications (
        trade_key TEXT NOT NULL, chat_id INTEGER NOT NULL, status TEXT NOT NULL,
        result TEXT NOT NULL, sent_at TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'SENT',
        PRIMARY KEY(trade_key, chat_id, status, result)
    )""")
    # ترقية قواعد v18.17 القديمة التي لا تحتوي state.
    cols = {row[1] for row in conn.execute("PRAGMA table_info(trade_notifications)").fetchall()}
    if "state" not in cols:
        conn.execute("ALTER TABLE trade_notifications ADD COLUMN state TEXT NOT NULL DEFAULT 'SENT'")
    conn.commit()
    return conn


def _save_trade_locked(record):
    key = _trade_key(record)
    if not key: return False
    conn = None
    try:
        conn = _trade_db_connect()
        conn.execute("INSERT INTO trades(trade_key,payload,updated_at) VALUES(?,?,?) ON CONFLICT(trade_key) DO UPDATE SET payload=excluded.payload, updated_at=excluded.updated_at", (key, json.dumps(record, ensure_ascii=False, default=str), str(record.get("last_update", ""))))
        conn.commit(); return True
    except Exception:
        logger.exception("تعذر حفظ الصفقة")
        return False
    finally:
        if conn: conn.close()


def _load_trade_history():
    conn = None
    try:
        conn = _trade_db_connect()
        rows = conn.execute("SELECT payload FROM trades ORDER BY updated_at DESC LIMIT ?", (MAX_TRADE_HISTORY,)).fetchall()
        loaded=[]
        for (payload,) in reversed(rows):
            try:
                item=json.loads(payload)
                if isinstance(item,dict): loaded.append(item)
            except Exception: pass
        TRADE_HISTORY.clear(); TRADE_HISTORY.extend(loaded)
        logger.info("تم تحميل %d صفقة محفوظة", len(TRADE_HISTORY))
    except Exception:
        logger.exception("تعذر تحميل سجل الصفقات")
    finally:
        if conn: conn.close()


def _mark_trade_state(trade, status, result, stamp, extra=None):
    trade["status"]=status; trade["result"]=result; trade["last_update"]=stamp
    if extra: trade.update(extra)


def _bar_stamp(bar):
    try:
        dt=pd.to_datetime(bar.get("openTime"), utc=True, errors="coerce")
        if not pd.isna(dt): return dt.tz_convert(DAMASCUS).isoformat()
    except Exception: pass
    return now_damascus().isoformat()


def _update_from_m1(trade, bars):
    direction=trade.get("direction"); sl=float(trade["sl"]); tp1=float(trade["tp1"]); tp2=float(trade["tp2"]); tp3=float(trade["tp3"])
    state=trade.get("status","ACTIVE"); changed=False
    # لا نعيد تفسير شموع سبقت فتح/آخر تحديث للصفقة.
    cutoff = pd.to_datetime(trade.get("last_update"), utc=True, errors="coerce")
    for _,bar in bars.iterrows():
        try:
            bar_dt = pd.to_datetime(bar.get("openTime"), utc=True, errors="coerce")
            if cutoff is not None and not pd.isna(cutoff) and bar_dt is not None and not pd.isna(bar_dt) and bar_dt <= cutoff:
                continue
        except Exception:
            pass
        try:
            high=float(bar["high"]); low=float(bar["low"])
            if not (math.isfinite(high) and math.isfinite(low)): continue
        except Exception: continue
        stamp=_bar_stamp(bar)
        if direction=="BUY": sl_hit,t1,t2,t3=low<=sl,high>=tp1,high>=tp2,high>=tp3
        else: sl_hit,t1,t2,t3=high>=sl,low<=tp1,low<=tp2,low<=tp3
        # إذا لامس SL وTP في نفس M1 لا يمكن معرفة الترتيب من OHLC؛ نعتمد SL محافظاً.
        if sl_hit:
            _mark_trade_state(trade,"CLOSED","LOSS",stamp,{"close_time":stamp,"close_reason":"SL hit"}); return True
        if t3:
            trade["tp1_time"]=trade.get("tp1_time") or stamp; trade["tp2_time"]=trade.get("tp2_time") or stamp; trade["tp3_time"]=stamp
            _mark_trade_state(trade,"CLOSED","TP3 / WIN",stamp,{"close_time":stamp}); return True
        if t2 and state in ("ACTIVE","TP1"):
            trade["tp1_time"]=trade.get("tp1_time") or stamp; trade["tp2_time"]=stamp; state="TP2"; _mark_trade_state(trade,"TP2","TP2",stamp); changed=True
        elif t1 and state=="ACTIVE":
            trade["tp1_time"]=trade.get("tp1_time") or stamp; state="TP1"; _mark_trade_state(trade,"TP1","TP1",stamp); changed=True
    return changed


try: _load_trade_history()
except Exception: logger.exception("فشل تهيئة سجل الصفقات")


def register_trade(result):
    if not result.get("signal") or not result.get("trade"): return None, False
    t=result["trade"]
    with TRADE_LOCK:
        price=float(result.get("price",t["entry"])); atr=max(float(result.get("mtf",{}).get("m15",{}).get("atr",0) or 0),0.50)
        entry_tol=max(atr*0.30,price*0.00025,0.50); sl_tol=max(atr*0.45,price*0.00035,0.75); tp_tol=max(atr*0.55,price*0.00045,1.00)
        for record in reversed(TRADE_HISTORY):
            if record.get("status") not in ("ACTIVE","TP1","TP2") or record.get("direction")!=result["direction"]: continue
            if abs(float(record.get("entry",0))-float(t["entry"]))>entry_tol or abs(float(record.get("sl",0))-float(t["sl"]))>sl_tol: continue
            if abs(float(record.get("tp1",0))-float(t["tp1"]))>tp_tol or abs(float(record.get("tp2",0))-float(t["tp2"]))>tp_tol*1.5 or abs(float(record.get("tp3",0))-float(t["tp3"]))>tp_tol*2: continue
            record.update({"score":result["score"],"quality":result["quality"],"last_update":now_damascus().isoformat()}); _save_trade_locked(record); return record,False
        now=now_damascus().isoformat()
        record={"time":now,"direction":result["direction"],"score":result["score"],"quality":result["quality"],"entry":float(t["entry"]),"sl":float(t["sl"]),"tp1":float(t["tp1"]),"tp2":float(t["tp2"]),"tp3":float(t["tp3"]),"rr":float(t["rr"]),"status":"ACTIVE","result":"OPEN","liquidity_state":result.get("mtf",{}).get("m15",{}).get("liquidity",{}).get("retest",{}).get("state"),"retest_status":liquidity_retest_summary(result.get("mtf",{}).get("m15",{}).get("liquidity",{})),"last_update":now,"last_price":price,"tp1_time":None,"tp2_time":None,"tp3_time":None,"close_time":None}
        TRADE_HISTORY.append(record)
        if len(TRADE_HISTORY)>MAX_TRADE_HISTORY: del TRADE_HISTORY[:-MAX_TRADE_HISTORY]
        _save_trade_locked(record); return record,True


def update_trade_results():
    """تحديث الصفقات المفتوحة وإرجاع نسخ الصفقات التي تغيرت حالتها فقط."""
    changed_records = []
    with TRADE_LOCK:
        active=[t for t in TRADE_HISTORY if t.get("status") in ("ACTIVE","TP1","TP2")]
        if not active: return changed_records
        try:
            bars=get_bars("1m",100)
            if bars is not None and not bars.empty and "openTime" in bars.columns:
                bars=bars.sort_values("openTime")
                # آخر شمعة قد تكون ما زالت قيد التكوين؛ نعتمد الشموع المكتملة فقط.
                if len(bars) > 1:
                    bars=bars.iloc[:-1].copy()
        except Exception:
            logger.exception("تعذر جلب M1 لتحديث الصفقات"); bars=None
        try: current_price=float(live_price()["price"])
        except Exception: current_price=None
        now=now_damascus().isoformat()
        for trade in active:
            changed=False
            if bars is not None and {"high","low"}.issubset(bars.columns): changed=_update_from_m1(trade,bars)
            if not changed and current_price is not None:
                p=current_price; state=trade.get("status","ACTIVE")
                if trade.get("direction")=="BUY": sl_hit,t1,t2,t3=p<=trade["sl"],p>=trade["tp1"],p>=trade["tp2"],p>=trade["tp3"]
                else: sl_hit,t1,t2,t3=p>=trade["sl"],p<=trade["tp1"],p<=trade["tp2"],p<=trade["tp3"]
                if sl_hit: _mark_trade_state(trade,"CLOSED","LOSS",now,{"close_time":now,"close_reason":"SL hit"}); changed=True
                elif t3: trade["tp1_time"]=trade.get("tp1_time") or now; trade["tp2_time"]=trade.get("tp2_time") or now; trade["tp3_time"]=now; _mark_trade_state(trade,"CLOSED","TP3 / WIN",now,{"close_time":now}); changed=True
                elif t2 and state in ("ACTIVE","TP1"): trade["tp1_time"]=trade.get("tp1_time") or now; trade["tp2_time"]=now; _mark_trade_state(trade,"TP2","TP2",now); changed=True
                elif t1 and state=="ACTIVE": trade["tp1_time"]=trade.get("tp1_time") or now; _mark_trade_state(trade,"TP1","TP1",now); changed=True
            if current_price is not None: trade["last_price"]=current_price
            trade["last_update"]=now
            if changed or current_price is not None: _save_trade_locked(trade)
            if changed:
                changed_records.append(dict(trade))
        return changed_records


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
        f"الحجم: {x['volume_state']} ({x['volume_ratio']:.2f}x)\n"
        f"السيولة: {x['liquidity']['bias']}\n"
        f"سحب السيولة: {x['liquidity']['sweep_text']}\n"
        f"إعادة الاختبار: {liquidity_retest_summary(x['liquidity'])}"
    )


def build_analysis(include_trade=True):
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
        "💧 السيولة",
        f"الانحياز: {mtf['m15']['liquidity']['bias']}",
        f"Buy-side الأقرب: {fmt(mtf['m15']['liquidity']['nearest_buy'])}",
        f"Sell-side الأقرب: {fmt(mtf['m15']['liquidity']['nearest_sell'])}",
        f"السحب: {mtf['m15']['liquidity']['sweep_text']}",
        "",
        "🏦 العامل المؤسسي",
        f"الاتجاه: {result['institutional']['direction']}",
        f"الحالة: {result['institutional']['quality']}/100",
        "",
        f"📰 الأخبار: {result['news']}", "", "🔎 عوامل التلاقي:"
    ]
    lines.extend(["• " + x for x in result["factors"]] or ["• لا يوجد تلاقي إضافي مسجل حالياً."])
    if include_trade and result["signal"] and result["trade"]:
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
    liq_frame = frames_data.get("H1" if horizon == "daily" else "D1", {}).get("liquidity", {})
    if direction == "BUY" and liq_frame.get("sweep") == "SELL_SIDE_SWEEP":
        score += 8; factors.append("سحب سيولة بيعية يدعم السيناريو الشرائي")
    elif direction == "SELL" and liq_frame.get("sweep") == "BUY_SIDE_SWEEP":
        score += 8; factors.append("سحب سيولة شرائية يدعم السيناريو البيعي")
    if liq_frame.get("retest", {}).get("state") == "RETEST_CONFIRMED":
        if (direction == "BUY" and liq_frame.get("sweep") == "SELL_SIDE_SWEEP") or (direction == "SELL" and liq_frame.get("sweep") == "BUY_SIDE_SWEEP"):
            score += 12; factors.append("إعادة اختبار ناجحة")
    elif liq_frame.get("retest", {}).get("state") == "INVALIDATED":
        score = max(0, score - 12); factors.append("إعادة اختبار فاشلة")
    return min(100, int(score)), factors


def _breakout_confirmation(df, level, direction, horizon, atr):
    """محرك تأكيد الاختراق/الثبات الديناميكي.
    اليومي يعتمد H1، والأسبوعي يعتمد D1. لا نستخدم الشمعة الحالية غير المكتملة.
    متطلبات التأكيد ترتفع تلقائياً مع ارتفاع مدى الشمعة مقارنةً بـ ATR.
    """
    candle_tf = "D1" if horizon == "weekly" else "H1"
    if level is None or df is None or len(df) < 20:
        return {
            "tf": candle_tf, "risk": "غير محدد", "confidence": 0,
            "status": "غير متاح", "rule": f"إغلاق {candle_tf} فوق/تحت المستوى المطلوب حسب السيناريو."
        }
    try:
        x = df.iloc[-2]  # آخر شمعة مكتملة
        o, h, l, c = map(float, (x["open"], x["high"], x["low"], x["close"]))
        rng = max(h-l, 1e-9)
        body = abs(c-o)
        body_ratio = body/rng
        close_pos = (c-l)/rng
        atr_v = max(float(atr or 0), 1e-9)
        range_ratio = rng/atr_v

        if range_ratio >= 2.0:
            risk = "شديدة"
            required = "إغلاق الشمعة + تأكيد الشمعة التالية أو إعادة اختبار ناجحة"
        elif range_ratio >= 1.35:
            risk = "عالية"
            required = "إغلاق قوي + مراقبة إعادة الاختبار"
        elif range_ratio >= 0.85:
            risk = "متوسطة"
            required = "إغلاق الشمعة بجسم واضح"
        else:
            risk = "منخفضة"
            required = "إغلاق الشمعة فوق/تحت المستوى يكفي مبدئياً"

        if direction == "BUY":
            distance = c-level
            side_ok = distance > 0
            close_quality = close_pos if side_ok else max(0.0, close_pos-0.5)
        else:
            distance = level-c
            side_ok = distance > 0
            close_quality = (1.0-close_pos) if side_ok else max(0.0, 0.5-close_pos)

        body_quality = min(1.0, body_ratio/0.60)
        atr_quality = 1.0 if 0.60 <= range_ratio <= 1.60 else (0.75 if range_ratio < 0.60 else 0.65)
        confidence = int(round(100 * (0.45*max(0.0,min(1.0,close_quality)) + 0.35*body_quality + 0.20*atr_quality)))
        if side_ok:
            status = "🟢 مؤكد مبدئياً" if confidence >= 70 else "🟡 ضعيف / يحتاج تأكيد"
        else:
            status = "🔴 غير مؤكد"

        return {
            "tf": candle_tf, "risk": risk, "confidence": max(0,min(100,confidence)),
            "status": status, "rule": required, "close": c, "range_ratio": range_ratio,
            "body_ratio": body_ratio, "side_ok": side_ok
        }
    except Exception:
        return {
            "tf": candle_tf, "risk": "غير محدد", "confidence": 0,
            "status": "غير متاح", "rule": f"إغلاق {candle_tf} واضح فوق/تحت المستوى المطلوب."
        }


def _confirmation_text(conf, level, direction, label="تأكيد الثبات"):
    if level is None:
        return f"🕯️ {label}: غير متاح حالياً."
    side = "فوق" if direction == "BUY" else "تحت"
    return (
        f"🕯️ {label}: {conf['tf']} — الإغلاق المطلوب {side} {fmt(level)}\n"
        f"   • خطورة الحركة: {conf['risk']} | ثقة الشمعة: {conf['confidence']}/100\n"
        f"   • الحالة: {conf['status']}\n"
        f"   • القاعدة: {conf['rule']}"
    )


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

    # شمعة التأكيد تختلف حسب الأفق: H1 لليومي و D1 للأسبوعي.
    confirm_df = get_bars("1d", 250) if horizon == "weekly" else get_bars("1h", 300)
    confirm_level = r1 if direction == "BUY" else s1
    confirm = _breakout_confirmation(confirm_df, confirm_level, direction, horizon, atr)

    if direction == "BUY":
        primary = {"title": "استمرار الاتجاه الصاعد / بناء مركز شراء", "quality": buy_score,
                   "mechanism": f"تبقى الرؤية الشرائية مفضلة ما دام السعر يحافظ على منطقة الدعم {fmt(s1)} ولا يظهر كسر هيكلي هابط مؤكد.",
                   "trigger": _confirmation_text(confirm, s1, "BUY", "تأكيد الثبات فوق الدعم") if s1 else "تأكيد صاعد من منطقة القرار.",
                   "targets": [r1, r2], "stop": s1, "factors": buy_factors}
        alternative = {"title": "السيناريو البديل — تحول هابط", "quality": sell_score,
                       "mechanism": f"يتحول الميزان إلى الهبوط عند فقدان الدعم {fmt(s1)} مع تأكيد كسر هيكلي وليس مجرد ذيل سعري.",
                       "trigger": _confirmation_text(_breakout_confirmation(confirm_df, s1, "SELL", horizon, atr), s1, "SELL", "تأكيد كسر الدعم") if s1 else "تأكيد هابط بكسر الدعم.",
                       "targets": [s2, None], "stop": r1, "factors": sell_factors}
    elif direction == "SELL":
        primary = {"title": "استمرار الاتجاه الهابط / البيع من المقاومة", "quality": sell_score,
                   "mechanism": f"تبقى الرؤية البيعية مفضلة ما دام السعر أسفل المقاومة {fmt(r1)} والهيكل يدعم الضغط الهابط.",
                   "trigger": _confirmation_text(confirm, r1, "SELL", "تأكيد استمرار الهبوط أسفل مستوى القرار") if r1 else "تأكيد هابط من منطقة مقاومة واضحة.",
                   "targets": [s1, s2], "stop": r1, "factors": sell_factors}
        alternative = {"title": "السيناريو البديل — استعادة الاتجاه الصاعد", "quality": buy_score,
                       "mechanism": f"يتحول الميزان إذا اخترق السعر {fmt(r1)} وثبت فوقه مع تحسن الهيكل والزخم.",
                       "trigger": _confirmation_text(_breakout_confirmation(confirm_df, r1, "BUY", horizon, atr), r1, "BUY", "تأكيد اختراق المقاومة") if r1 else "اختراق قمة مهمة مع تأكيد الشمعة المطلوبة.",
                       "targets": [r2, None], "stop": s1, "factors": buy_factors}
    else:
        primary = {"title": "سيناريو الانتظار — لا أفضلية اتجاهية كافية", "quality": max(buy_score, sell_score),
                   "mechanism": "الفريمات المحددة لا تمنح أفضلية واضحة؛ القرار يعتمد على كسر أحد طرفي النطاق مع تأكيد.",
                   "trigger": (f"{_confirmation_text(_breakout_confirmation(confirm_df, r1, "BUY", horizon, atr), r1, "BUY", "تأكيد كسر المقاومة")}\nأو\n{_confirmation_text(_breakout_confirmation(confirm_df, s1, "SELL", horizon, atr), s1, "SELL", "تأكيد كسر الدعم")}"),
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
        "🕯️ محرك تأكيد الحركة", "━━━━━━━━━━━━━━━━━━━━",
        f"• الأفق: {"أسبوعي" if horizon == "weekly" else "يومي"}",
        f"• شمعة التأكيد الأساسية: {"D1" if horizon == "weekly" else "H1"}",
        f"• مستوى الخطورة: يُحسب لكل مستوى حسب ATR والشمعة المكتملة.",
        "• كلما ارتفعت خطورة الحركة، ترتفع متطلبات التأكيد تلقائياً.",
        "",
        "💧 قراءة السيولة", "━━━━━━━━━━━━━━━━━━━━",
        f"• الانحياز: {frames_data['H1' if horizon == 'daily' else 'D1']['liquidity']['bias']}",
        f"• Buy-side liquidity الأقرب: {fmt(frames_data['H1' if horizon == 'daily' else 'D1']['liquidity']['nearest_buy'])}",
        f"• Sell-side liquidity الأقرب: {fmt(frames_data['H1' if horizon == 'daily' else 'D1']['liquidity']['nearest_sell'])}",
        f"• آخر شمعة مكتملة: {frames_data['H1' if horizon == 'daily' else 'D1']['liquidity']['sweep_text']}",
        f"• حالة إعادة الاختبار: {liquidity_retest_summary(frames_data['H1' if horizon == 'daily' else 'D1']['liquidity'])}",
        "",
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
                  "• D1 هو شمعة التأكيد الأساسية لمستويات التقرير الأسبوعي.",
                  "• عند الحركة عالية/شديدة الخطورة لا يكفي لمس المستوى؛ ننتظر إغلاق D1 قوي وقد نحتاج الشمعة التالية أو Retest.",
                  "• H4 يستخدم لتحديد منطقة التنفيذ الاستراتيجي، وليس M15 لتحديد اتجاه الأسبوع."]
    else:
        lines += ["• مراقبة السيولة والزخم على H1 ثم M15 ثم M5.",
                  "• H1 هي شمعة التأكيد الأساسية للتقرير اليومي.",
                  "• إذا كانت الحركة عالية الخطورة، لا يكفي الإغلاق وحده؛ نراقب Retest أو الشمعة التالية حسب الحالة.",
                  "• كسر مستوى القرار مع إغلاق مؤكد ينقلنا إلى السيناريو البديل."]
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
    return f"📊 التحليل اليومي {VERSION}\n━━━━━━━━━━━━━━━━━━\n💰 السعر: {price:.2f}\n🎯 التوجيه: {'🟢 شراء' if direction=='BUY' else '🔴 بيع' if direction=='SELL' else '🟡 انتظار'}\n💪 قوة اليوم: {max(buy,sell)} نقطة\n\nH1: {h1['direction']} | RSI {h1['rsi']:.1f} | ADX {h1['adx']:.1f}\nM15: {m15['direction']} | RSI {m15['rsi']:.1f} | ADX {m15['adx']:.1f}\nM5: {m5['direction']} | RSI {m5['rsi']:.1f} | ADX {m5['adx']:.1f}\n\n📍 S1: {format_zone(levels,'support1')}\n📍 R1: {format_zone(levels,'resistance1')}\n\n💧 السيولة: {m15['liquidity']['bias']}\n🧲 Buy-side: {fmt(m15['liquidity']['nearest_buy'])} | Sell-side: {fmt(m15['liquidity']['nearest_sell'])}\n🔄 السحب: {m15['liquidity']['sweep_text']}"


def build_weekly_analysis():
    """التحليل الأسبوعي: W1 + D1 + H4 فقط."""
    w1 = analyze(get_bars("1w", 250)); d1 = analyze(get_bars("1d", 300)); h4 = analyze(get_bars("4h", 300))
    q = live_price(); price = q["price"]; levels = support_resistance(get_bars("1d", 250))
    frames = {"W1": w1, "D1": d1, "H4": h4}
    buy, _ = _scenario_score([("W1",35),("D1",30),("H4",25)], "BUY", levels, price, d1["atr"])
    sell, _ = _scenario_score([("W1",35),("D1",30),("H4",25)], "SELL", levels, price, d1["atr"])
    direction = "BUY" if buy > sell else "SELL" if sell > buy else "WAIT"
    return f"📅 التحليل الأسبوعي {VERSION}\n━━━━━━━━━━━━━━━━━━\n💰 السعر: {price:.2f}\n🎯 الاتجاه الاستراتيجي: {'🟢 شراء' if direction=='BUY' else '🔴 بيع' if direction=='SELL' else '🟡 حياد'}\n💪 قوة الاتجاه: {max(buy,sell)} نقطة\n\nW1: {w1['direction']} | قوة {w1['score']} | RSI {w1['rsi']:.1f} | ADX {w1['adx']:.1f}\nD1: {d1['direction']} | قوة {d1['score']} | RSI {d1['rsi']:.1f} | ADX {d1['adx']:.1f}\nH4: {h4['direction']} | قوة {h4['score']} | RSI {h4['rsi']:.1f} | ADX {h4['adx']:.1f}\n\n📍 الدعم الأسبوعي: {format_zone(levels,'support1')}\n📍 المقاومة الأسبوعية: {format_zone(levels,'resistance1')}\n\n💧 السيولة: {d1['liquidity']['bias']}\n🧲 Buy-side: {fmt(d1['liquidity']['nearest_buy'])} | Sell-side: {fmt(d1['liquidity']['nearest_sell'])}\n🔄 السحب: {d1['liquidity']['sweep_text']}"


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
    _ensure_user(update)
    keyboard = [
        ["📊 التحليل الكامل"],
        ["⚡ التحليل السريع", "🎯 صفقة الآن"],
        ["📍 الدعوم والمقاومات", "📜 سجل الصفقات"],
        ["📝 التقرير التوضيحي اليومي", "📅 التقرير التوضيحي الأسبوعي"],
        ["📰 الأخبار", "💰 سعر الذهب"],
        ["🌍 الأسواق", "🔔 التنبيهات"],
        ["💳 الباقات", "👤 اشتراكي"],
        ["🟢 حالة النظام"]
    ]
    text = (
        f"🤖 XAU SMART TRADER {VERSION}\n\n"
        "🥇 محلل الذهب XAU/USD\n\n"
        "W1 + D1 + H4 + H1 + M15\n"
        "Structure + Momentum + Volume + Fibonacci + FVG\n\n"
        f"🎯 حد الإشارة: {SIGNAL_THRESHOLD} نقطة\n"
        "📝 تمت إضافة التقرير التوضيحي اليومي والأسبوعي.\n\n"
        "💳 الاشتراكات: استخدم /plans\n"
        "اختر العملية 👇"
    )
    await update.message.reply_text(text, reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))


async def full_analysis(update, context):
    if not await feature_guard("full_analysis")(update, context): return
    try:
        await reply(update, await asyncio.to_thread(build_analysis, can_receive_trade(update.effective_chat.id)))
    except Exception as e:
        await reply(update, f"❌ تعذر تنفيذ التحليل.\nالسبب: {e}")


async def quick_analysis(update, context):
    if not await feature_guard("quick_analysis")(update, context): return
    try:
        df = get_bars("15m", 220)
        m15 = analyze(df)
        result = await asyncio.to_thread(evaluate_signal)
        price = float(result["price"])
        close = df["close"]
        ema9 = float(EMA(close, 9).iloc[-1])
        ema21 = float(EMA(close, 21).iloc[-1])
        macd_val = float(m15["macd"])
        macd_signal = float(m15.get("macd_signal", macd_val))
        ema_dir = "إيجابي" if ema9 > ema21 else "سلبي" if ema9 < ema21 else "محايد"
        rsi = float(m15["rsi"])
        if rsi >= 70:
            rsi_text = "تشبع شرائي — خطر الانعكاس مرتفع"
        elif rsi >= 60:
            rsi_text = "زخم شرائي قوي"
        elif rsi <= 30:
            rsi_text = "تشبع بيعي — احتمال ارتداد قائم"
        elif rsi <= 40:
            rsi_text = "زخم بيعي واضح"
        else:
            rsi_text = "ضمن النطاق المحايد"
        macd_text = "إيجابي" if macd_val > 0 else "سلبي" if macd_val < 0 else "محايد"
        adx = float(m15["adx"])
        adx_text = "اتجاه قوي" if adx >= 25 else "اتجاه ضعيف/متذبذب"
        direction = result["direction"] if result["direction"] in ("BUY", "SELL") else ("BUY" if ema9 > ema21 else "SELL" if ema9 < ema21 else "WAIT")
        d_text = ("🟢 صعود حاد — زخم مرتفع" if direction == "BUY" and adx >= 25 else
                  "🔴 هبوط حاد — زخم مرتفع" if direction == "SELL" and adx >= 25 else
                  "🟢 صعود" if direction == "BUY" else "🔴 هبوط" if direction == "SELL" else "🟡 حياد")
        levels = result["levels"]
        s1 = nearest_support(levels, price)
        s2 = next_support(levels, price)
        r1 = nearest_resistance(levels, price)
        r2 = next_resistance(levels, price)
        atr = max(float(m15["atr"]), 0.50)
        min_target_distance = max(atr * 0.50, price * 0.00030, 1.00)
        if direction == "BUY":
            candidates = [v for v in (r1, r2) if v is not None and v > price and v - price >= min_target_distance]
            target = candidates[0] if candidates else price + max(atr * 1.10, min_target_distance)
            stop = s1 - atr * 0.15 if s1 is not None and price - s1 <= atr * 1.10 else price - atr * 0.95
        elif direction == "SELL":
            candidates = [v for v in (s1, s2) if v is not None and v < price and price - v >= min_target_distance]
            target = candidates[0] if candidates else price - max(atr * 1.10, min_target_distance)
            stop = r1 + atr * 0.15 if r1 is not None and r1 - price <= atr * 1.10 else price + atr * 0.95
        else:
            target = price + max(atr * 1.10, min_target_distance)
            stop = price - atr * 0.95
        risk = abs(price - stop)
        reward = abs(target - price)
        rr = reward / risk if risk else 0.0
        structure = result["mtf"]["h4"]["structure"]
        structure_dir = "🟢 صاعد" if structure == "صاعد" else "🔴 هابط" if structure == "هابط" else "🟡 محايد"
        conflict = ((direction == "BUY" and structure == "هابط") or (direction == "SELL" and structure == "صاعد"))
        score = int(result["score"])
        quality = ("ممتازة" if score >= 90 else "قوية جداً" if score >= 80 else "قوية" if score >= 70 else "جيدة" if score >= 60 else "مؤهلة" if score >= 50 else "ضعيفة")
        if score >= MIN_TRADE_SCORE and not conflict and not result.get("news_blocked"):
            rec = "🟢 فرصة شراء سكالبينج" if direction == "BUY" else "🔴 فرصة بيع سكالبينج"
        elif direction == "BUY":
            rec = "🟡 مراقبة شراء — تحتاج تأكيد"
        elif direction == "SELL":
            rec = "🟡 مراقبة بيع — تحتاج تأكيد"
        else:
            rec = "🟡 مراقبة — لا يوجد اتجاه مؤكد"
        if conflict:
            conclusion = "الزخم اللحظي متعارض مع الهيكل الأكبر؛ انتظر تأكيداً من المستوى القريب قبل الدخول."
        elif score >= MIN_TRADE_SCORE:
            conclusion = "الزخم والهيكل متوافقان نسبياً؛ الأفضل انتظار إعادة اختبار المستوى وتجنب مطاردة السعر."
        else:
            conclusion = "الإشارة لم تصل إلى حد الصفقة؛ الأفضل المراقبة وانتظار تأكيد إضافي."
        if result.get("news_blocked"):
            conclusion += " التداول محجوب حالياً بسبب فلتر الأخبار."
        level_lines = [
            "📍 المستويات اللحظية:",
            f"• السعر الحالي: {price:.2f}",
            f"• دعم 1: {s1:.2f}" if s1 is not None else "• دعم 1: غير متوفر",
            f"• مقاومة 1: {r1:.2f}" if r1 is not None else "• مقاومة 1: غير متوفر",
            f"• الهدف اللحظي: {target:.2f}",
            f"• وقف الخسارة المقترح: {stop:.2f}",
            f"• R:R التقريبي: 1:{rr:.2f}",
        ]
        text = (
            f"⚡ التحليل السريع {VERSION}\n━━━━━━━━━━━━━━━━━━\n"
            f"🎯 الاتجاه اللحظي: {d_text}\n\n"
            "📊 قراءة المؤشرات:\n"
            f"• EMA 9: {ema9:.2f} — EMA 21: {ema21:.2f} ({ema_dir})\n"
            f"• RSI: {rsi:.1f} — {rsi_text}\n"
            f"• MACD: {macd_val:.2f} ({macd_text}) — Signal: {macd_signal:.2f}\n"
            f"• ADX: {adx:.1f} — {adx_text}\n\n"
            + "\n".join(level_lines) + "\n\n"
            f"⚡ التوصية الفورية: {rec}\n"
            "⚠️ شرط الدخول: لا تطارد السعر بعد اختراق المقاومة/الدعم القريب؛ انتظر إعادة اختبار أو تأكيد شمعة.\n\n"
            f"🧠 قراءة الهيكل: {structure_dir}" + (" — يوجد تعارض مع الاتجاه اللحظي." if conflict else " — متوافق نسبياً مع الاتجاه اللحظي.") + "\n"
            f"\n💧 قراءة السيولة: {result['mtf']['m15']['liquidity']['bias']}\n"
            f"🧲 Buy-side: {fmt(result['mtf']['m15']['liquidity']['nearest_buy'])} | Sell-side: {fmt(result['mtf']['m15']['liquidity']['nearest_sell'])}\n"
            f"🔄 السحب: {result['mtf']['m15']['liquidity']['sweep_text']}\n"
            f"💪 جودة الفرصة: {score} نقطة — {quality}\n"
            f"🔎 الخلاصة: {conclusion}"
        )
        await reply(update, text)
    except Exception as e:
        logger.exception("Quick analysis error")
        await reply(update, f"❌ تعذر التحليل السريع: {e}")


async def trade_now(update, context):
    if not await feature_guard("trade_now")(update, context): return
    try:
        result = await asyncio.to_thread(evaluate_signal)
        if result["news_blocked"]:
            await reply(update, "🚨 لا توجد صفقة الآن.\n\nتم تفعيل الحماية الإخبارية.")
            return
        if not result["signal"]:
            await reply(update, f"⏳ لا توجد صفقة مؤهلة الآن.\n\n💪 الدرجة: {result['score']} نقطة\n🎯 الحد: {SIGNAL_THRESHOLD} نقطة\n\nالبوت يراقب السوق.")
            return
        trade = result["trade"]
        chat_id = update.effective_chat.id
        if not can_receive_trade(chat_id):
            await reply(update, "⛔ تم الوصول إلى حد الصفقات في باقتك الحالية.\n\n🚀 استخدم /plans للترقية.")
            return
        record, is_new_trade = register_trade(result)
        if is_new_trade and not consume_trade(chat_id):
            await reply(update, "⛔ تم الوصول إلى حد الصفقات في باقتك الحالية.\n\n🚀 استخدم /plans للترقية.")
            return
        direction = "🟢 شراء" if result["direction"] == "BUY" else "🔴 بيع"
        quality = "🔥 قوية" if result["score"] >= STRONG_THRESHOLD else "🎯 مؤهلة"
        text = (
            "🚨 XAU SMART TRADER\n━━━━━━━━━━━━━━━━━━\n"
            f"📈 الصفقة: {direction}\n💪 الجودة: {result['score']} نقطة — {quality}\n\n"
            f"📍 الدخول: {trade['entry']:.2f}\n🛑 SL: {trade['sl']:.2f}\n"
            f"🎯 TP1: {trade['tp1']:.2f}\n🎯 TP2: {trade['tp2']:.2f}\n🎯 TP3: {trade['tp3']:.2f}\n⚖️ R:R النهائي: 1:{trade['rr']:.2f}\n\n"
            "🧠 إشارة تحليلية للتنفيذ اليدوي."
        )
        await reply(update, text)
    except Exception as e:
        await reply(update, f"❌ تعذر بناء الصفقة: {e}")


async def show_levels(update, context):
    if not await feature_guard("sr")(update, context): return
    try:
        levels = support_resistance(get_bars("1h", 250))
        price = live_price()["price"]
        lines = [
            "📍 XAU/USD — مناطق السوق",
            "━━━━━━━━━━━━━━━━━━",
            "",
            f"💰 السعر الحالي: {price:.2f}",
            "",
            f"🟢 دعم 1: {format_zone(levels, 'support1')}",
            f"🟢 دعم 2: {format_zone(levels, 'support2')}",
            f"🟢 دعم 3: {format_zone(levels, 'support3')}",
            "",
            f"🔴 مقاومة 1: {format_zone(levels, 'resistance1')}",
            f"🔴 مقاومة 2: {format_zone(levels, 'resistance2')}",
            f"🔴 مقاومة 3: {format_zone(levels, 'resistance3')}",
            "",
            "💧 السيولة",
            f"• الانحياز: {liquidity_analysis(get_bars('1h', 250))['bias']}",
            f"• Buy-side الأقرب: {fmt(liquidity_analysis(get_bars('1h', 250))['nearest_buy'])}",
            f"• Sell-side الأقرب: {fmt(liquidity_analysis(get_bars('1h', 250))['nearest_sell'])}",
            f"• السحب: {liquidity_analysis(get_bars('1h', 250))['sweep_text']}",
            "",
            "الترتيب: الأقرب للسعر أولاً، مع إبقاء الدعوم أسفل السعر والمقاومات أعلى السعر.",
            "المناطق مبنية على القمم والقيعان المجمعة حسب ATR.",
        ]
        await reply(update, "\n".join(lines))
    except Exception as e:
        await reply(update, f"❌ تعذر حساب المناطق: {e}")


async def trade_history(update, context):
    if not await feature_guard("trade_history")(update, context): return
    update_trade_results()
    if not TRADE_HISTORY:
        await reply(update, "📜 سجل الصفقات\n━━━━━━━━━━━━━━━━━━\n\nلا توجد صفقات مسجلة بعد.")
        return
    lines = ["📜 سجل الصفقات", "━━━━━━━━━━━━━━━━━━"]
    for i, trade in enumerate(reversed(TRADE_HISTORY[-15:]), 1):
        direction = "🟢 شراء" if trade["direction"] == "BUY" else "🔴 بيع"
        status_ar = {"ACTIVE": "🟢 نشطة", "TP1": "🎯 TP1 تحقق", "TP2": "🎯 TP2 تحقق", "CLOSED": "🔒 مغلقة"}.get(trade["status"], trade["status"])
        lines += [
            f"\n#{i} {direction} | {trade['score']} نقطة — {trade['quality']}",
            f"💰 دخول: {trade['entry']:.2f} | SL: {trade['sl']:.2f}",
            f"🎯 TP1: {trade['tp1']:.2f} | TP2: {trade['tp2']:.2f} | TP3: {trade['tp3']:.2f}",
            f"⚖️ R:R النهائي: 1:{trade['rr']:.2f}",
            f"📌 الحالة: {status_ar} — {trade['result']}",
            f"🔁 إعادة الاختبار: {trade.get('retest_status', 'غير موثقة')}",
            f"🕐 فتح: {trade['time']}",
            f"🔄 آخر تحديث: {trade.get('last_update', '—')}"
        ]
    await reply(update, "\n".join(lines))


async def gold_price(update, context):
    if not await feature_guard("gold_price")(update, context): return
    try:
        q = await asyncio.to_thread(live_price)
        await reply(update, f"💰 XAU/USD — السعر اللحظي\n\nالسعر: {q['price']:.2f}\nالمصدر: {q['source']}\nعمر السعر: {q.get('age')}\nتوقيت دمشق: {now_damascus().strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        await reply(update, f"❌ تعذر جلب السعر: {e}")


async def daily_analysis(update, context):
    if not await feature_guard("daily_analysis")(update, context): return
    try:
        await reply(update, await asyncio.to_thread(build_daily_analysis))
    except Exception as e:
        logger.exception("Daily analysis error")
        await reply(update, f"❌ تعذر إنشاء التحليل اليومي.\nالسبب: {e}")


async def weekly_analysis(update, context):
    if not await feature_guard("weekly_analysis")(update, context): return
    try:
        await reply(update, await asyncio.to_thread(build_weekly_analysis))
    except Exception as e:
        logger.exception("Weekly analysis error")
        await reply(update, f"❌ تعذر إنشاء التحليل الأسبوعي.\nالسبب: {e}")


async def weekly_report(update, context):
    if not await feature_guard("weekly_report")(update, context): return
    try:
        await reply(update, await asyncio.to_thread(build_weekly_report))
    except Exception as e:
        logger.exception("Weekly report error")
        await reply(update, f"❌ تعذر إنشاء التقرير الأسبوعي.\nالسبب: {e}")


async def daily_report(update, context):
    if not await feature_guard("daily_report")(update, context): return
    try:
        await reply(update, await asyncio.to_thread(build_daily_report))
    except Exception as e:
        logger.exception("Daily report error")
        await reply(update, f"❌ تعذر إنشاء التقرير التوضيحي اليومي.\nالسبب: {e}")


async def news_status(update, context):
    if not await feature_guard("news_alerts")(update, context): return
    blocked, text = news_filter()
    await reply(update, f"📰 فلتر الأخبار\n\n{'🚨 التداول محجوب' if blocked else '🟢 التداول غير محجوب'}\n\n{text}\n\nالحماية: {NEWS_BEFORE_MIN} دقيقة قبل الخبر + {NEWS_AFTER_MIN} دقيقة بعده.")


async def markets(update, context):
    if not await feature_guard("markets")(update, context): return
    now = now_damascus()
    await reply(update, f"🌍 جلسات السيولة\n\n🇯🇵 آسيا: تجميع ومراقبة\n🇬🇧 لندن: ارتفاع السيولة\n🇺🇸 نيويورك: أعلى التقلبات\n\n🕐 توقيت دمشق الآن: {now.strftime('%H:%M:%S')}\n\n⚠️ أوقات الافتتاح تتغير موسمياً بسبب التوقيت الصيفي.")


async def status(update, context):
    if not await feature_guard("status")(update, context): return
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
    _ensure_user(update)
    chat_id = update.effective_chat.id
    if not has_feature(chat_id, "trade_alerts"):
        await reply(update, "🔒 تنبيهات الصفقات متاحة من PRO فما فوق.\n\n💳 استخدم زر الباقات للترقية.")
        return
    SUBSCRIBERS.add(chat_id)
    await reply(update, f"🔔 تنبيهات الصفقات مفعّلة تلقائياً ضمن باقتك.\n\n{plan_status_text(chat_id)}")


async def unsubscribe(update, context):
    chat_id = update.effective_chat.id
    SUBSCRIBERS.discard(chat_id)
    LAST_SIGNAL.pop(chat_id, None)
    await reply(update, "🔕 تم إيقاف التنبيهات التلقائية.")


async def plans(update, context):
    _ensure_user(update)
    text = plans_text() + "\n\n⬇️ اختر الباقة لمعرفة التفاصيل:"
    keyboard = [
        [InlineKeyboardButton("🆓 FREE", callback_data="plan_FREE")],
        [InlineKeyboardButton("🥉 BASIC — $10", callback_data="plan_BASIC")],
        [InlineKeyboardButton("🥈 PRO — $20", callback_data="plan_PRO")],
        [InlineKeyboardButton("🥇 PREMIUM — $35", callback_data="plan_PREMIUM")],
        [InlineKeyboardButton("💎 VIP — $50", callback_data="plan_VIP")],
    ]
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def plan_callback(update, context):
    query = update.callback_query
    await query.answer()
    plan_key = query.data.replace("plan_", "", 1)
    plan = PLANS.get(plan_key)
    if not plan:
        await query.edit_message_text("❌ الباقة غير موجودة.")
        return
    feature_names = {
        "gold_price": "💰 سعر الذهب", "markets": "🌍 الأسواق", "status": "🟢 حالة النظام",
        "quick_analysis": "⚡ التحليل السريع", "sr": "📍 الدعوم والمقاومات",
        "daily_report": "📝 التقرير التوضيحي اليومي", "weekly_report": "📅 التقرير التوضيحي الأسبوعي",
        "full_analysis": "📊 التحليل الكامل", "trade_now": "🎯 صفقة الآن",
        "trade_alerts": "🔔 تنبيهات الصفقات", "news_alerts": "📰 تنبيهات/حماية الأخبار",
        "market_alerts": "🌍 تنبيهات الأسواق", "institutional": "🏦 التحليل المؤسسي",
        "trade_history": "📜 سجل الصفقات", "vip": "💎 وصول VIP الكامل"
    }
    features = [feature_names.get(x, x) for x in sorted(plan["features"])]
    details = "\n".join("• " + x for x in features) or "• لا توجد ميزات إضافية"
    limit = TRADE_LIMIT_TEXT[plan_key]
    text = (
        f"{plan['name']}\n━━━━━━━━━━━━━━━━━━\n"
        f"💰 السعر: ${plan['price']} / شهر\n"
        f"🎯 حد الصفقات: {limit}\n\n"
        f"🔐 المزايا المتاحة: \n{details}\n\n"
        "👇 اختر الإجراء:"
    )
    buttons = []
    if plan_key != "FREE":
        buttons.append([InlineKeyboardButton(f"📩 طلب الاشتراك في {plan['name']}", callback_data=f"request_{plan_key}")])
    buttons.append([InlineKeyboardButton("🔙 العودة للباقات", callback_data="back_to_plans")])
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(buttons))


async def subscription_request_callback(update, context):
    query = update.callback_query
    await query.answer()
    plan_key = query.data.replace("request_", "", 1)
    plan = PLANS.get(plan_key)
    if not plan or plan_key == "FREE":
        await query.edit_message_text("❌ طلب الاشتراك غير صالح.")
        return
    chat_id = query.message.chat_id
    _ensure_user(update)
    conn = _db()
    try:
        conn.execute("INSERT INTO subscription_requests(chat_id, plan, requested_at, status) VALUES(?,?,?, 'PENDING')", (chat_id, plan_key, now_damascus().isoformat()))
        conn.commit()
    finally:
        conn.close()
    contact = ADMIN_CONTACT
    if contact:
        contact_text = f"\n\n📩 تواصل مع الإدارة: {contact}"
    else:
        contact_text = "\n\n📩 أرسل Chat ID الخاص بك للإدارة ليتم تفعيل الباقة يدوياً."
    await query.edit_message_text(
        f"📩 طلب الاشتراك — {plan['name']}\n━━━━━━━━━━━━━━━━━━\n"
        f"💰 السعر: ${plan['price']} / شهر\n"
        f"🎯 {TRADE_LIMIT_TEXT[plan_key]}\n\n"
        "تم تسجيل طلبك. التفعيل المدفوع يتم بعد تأكيد الإدارة." + contact_text,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 العودة للباقات", callback_data="back_to_plans")]])
    )


async def callback_router(update, context):
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    if data.startswith("plan_"):
        await plan_callback(update, context)
    elif data.startswith("request_"):
        await subscription_request_callback(update, context)
    elif data == "back_to_plans":
        await plans(update, context)
    else:
        await query.answer("زر غير معروف.", show_alert=True)


async def my_subscription(update, context):
    _ensure_user(update)
    await reply(update, "👤 اشتراكي\n━━━━━━━━━━━━━━━━━━\n" + plan_status_text(update.effective_chat.id))


async def referral(update, context):
    _ensure_user(update)
    row = get_member(update.effective_chat.id)
    await reply(update, f"👥 رابط دعوتك\n\nhttps://t.me/" + (await APPLICATION.bot.get_me()).username + f"?start={row['referral_code']}\n\n🎁 نظام الإحالة جاهز للمكافآت والترقية.")


async def admin_command(update, context):
    user_id = update.effective_user.id if update.effective_user else update.effective_chat.id
    if not (is_admin_chat(user_id) or is_admin_chat(update.effective_chat.id)):
        await reply(update, "⛔ هذا الأمر مخصص للإدارة.")
        return
    args = context.args
    if not args:
        await reply(update, "🛠 /admin\n\n/activate USER_ID PLAN DAYS\nمثال: /activate 123456789 PRO 30")
        return
    if args[0].lower() != 'activate' or len(args) < 4:
        await reply(update, "الصيغة: /admin activate USER_ID PLAN DAYS")
        return
    try:
        user_id, plan, days = int(args[1]), args[2].upper(), int(args[3])
        if plan not in PLANS or plan == 'FREE':
            raise ValueError("الباقة غير صحيحة")
        now = now_damascus(); expiry = now + timedelta(days=days)
        conn = _db()
        conn.execute("INSERT OR IGNORE INTO users(chat_id, plan, status, referral_code, created_at, updated_at) VALUES(?,?,?,?,?,?)", (user_id,'FREE','active',f'ref_{user_id}',now.isoformat(),now.isoformat()))
        conn.execute("UPDATE users SET plan=?, status='active', start_date=?, expiry_date=?, updated_at=? WHERE chat_id=?", (plan, now.isoformat(), expiry.isoformat(), now.isoformat(), user_id))
        conn.commit(); conn.close()
        await reply(update, f"✅ تم تفعيل {PLANS[plan]['name']} للمستخدم {user_id} حتى {expiry.strftime('%Y-%m-%d %H:%M')}")
    except Exception as e:
        await reply(update, f"❌ تعذر التفعيل: {e}")


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
        "📜 سجل الصفقات": trade_history,
        "📅 التقرير التوضيحي الأسبوعي": weekly_report,
        "📝 التقرير التوضيحي اليومي": daily_report,
        "📰 الأخبار": news_status,
        "💰 سعر الذهب": gold_price,
        "🌍 الأسواق": markets,
        "🔔 التنبيهات": subscribe,
        "🔔 تفعيل التنبيهات": subscribe,
        "🔕 إيقاف التنبيهات": unsubscribe,
        "💳 الباقات": plans,
        "👤 اشتراكي": my_subscription,
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
# المراقبة التلقائية — فحص كل 15 دقيقة
# ============================================================

def _market_holiday_dates():
    """تواريخ إغلاق السوق الإضافية بصيغة YYYY-MM-DD من متغير البيئة MARKET_HOLIDAYS."""
    raw = os.environ.get("MARKET_HOLIDAYS", "")
    result = set()
    for item in raw.replace(";", ",").split(","):
        value = item.strip()
        if not value:
            continue
        try:
            result.add(datetime.strptime(value, "%Y-%m-%d").date())
        except ValueError:
            logger.warning("تاريخ عطلة غير صالح في MARKET_HOLIDAYS: %s", value)
    return result


def market_closed_reason(now=None):
    """بوابة واحدة لإيقاف فتح صفقات جديدة في عطلة نهاية الأسبوع/العطل المحددة."""
    now = now or now_damascus()
    if now.weekday() >= 5:
        return "WEEKEND"
    if now.date() in _market_holiday_dates():
        return "HOLIDAY"
    return None


def _trade_notification_signature(record):
    """بصمة إشعار: لا نرسل تحديثًا ما لم تتغير حالة الصفقة أو نتيجتها."""
    if not record:
        return None
    return (
        _trade_key(record),
        str(record.get("status", "")),
        str(record.get("result", "")),
    )


def _notification_already_sent(chat_id, signature):
    if not signature:
        return False
    return LAST_SIGNAL.get(chat_id) == signature


def _mark_notification_sent(chat_id, signature):
    if signature:
        LAST_SIGNAL[chat_id] = signature


def _reserve_trade_quota(chat_id):
    """حجز حصة صفقة بشكل ذري؛ يعيد token يمكن تأكيده أو إلغاؤه."""
    if is_admin_chat(chat_id):
        return {"chat_id": chat_id, "reserved": True, "admin": True, "used_at": None}
    conn = None
    used_at = now_damascus().isoformat()
    try:
        conn = _db()
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT plan, status, expiry_date FROM users WHERE chat_id=?", (chat_id,)).fetchone()
        if not row:
            conn.rollback()
            return None
        plan = row["plan"] or "FREE"
        if row["status"] != "active":
            conn.rollback()
            return None
        if row["expiry_date"] and plan != "FREE":
            try:
                expiry = datetime.fromisoformat(row["expiry_date"])
                if expiry.tzinfo is None:
                    expiry = expiry.replace(tzinfo=DAMASCUS)
                if now_damascus() >= expiry:
                    conn.execute(
                        "UPDATE users SET plan='FREE', status='expired', updated_at=? WHERE chat_id=?",
                        (now_damascus().isoformat(), chat_id),
                    )
                    conn.commit()
                    return None
            except Exception:
                conn.rollback()
                return None
        limit = PLANS.get(plan, PLANS["FREE"]).get("trade_limit")
        if limit is None:
            conn.commit()
            return {"chat_id": chat_id, "reserved": True, "admin": False, "used_at": used_at}
        period_days = 7 if PLANS[plan]["trade_period"] == "weekly" else 30
        period_start = now_damascus() - timedelta(days=period_days)
        used = conn.execute(
            "SELECT COUNT(*) FROM usage WHERE chat_id=? AND feature='trade' AND used_at>=?",
            (chat_id, period_start.isoformat()),
        ).fetchone()[0]
        if used >= limit:
            conn.rollback()
            return None
        cur = conn.execute(
            "INSERT INTO usage(chat_id, feature, used_at) VALUES(?,?,?)",
            (chat_id, "trade", used_at),
        )
        usage_id = cur.lastrowid
        conn.commit()
        return {"chat_id": chat_id, "reserved": True, "admin": False, "usage_id": usage_id}
    except Exception:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        logger.exception("تعذر حجز حصة الصفقة للمستخدم %s", chat_id)
        return None
    finally:
        if conn:
            conn.close()


def _release_trade_quota(token):
    """إلغاء حجز الحصة فقط إذا فشل إرسال الرسالة."""
    if not token or token.get("admin") or not token.get("reserved"):
        return True
    usage_id = token.get("usage_id")
    chat_id = token.get("chat_id")
    if not usage_id or chat_id is None:
        return False
    conn = None
    try:
        conn = _db()
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "DELETE FROM usage WHERE id=? AND chat_id=? AND feature='trade'",
            (usage_id, chat_id),
        )
        conn.commit()
        return True
    except Exception:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        logger.exception("تعذر إرجاع حصة الصفقة للمستخدم %s", chat_id)
        return False
    finally:
        if conn:
            conn.close()


def _notification_db_key(record):
    key = _trade_key(record)
    if not key:
        return None
    return key, str(record.get("status", "")), str(record.get("result", ""))


def _claim_notification(chat_id, signature):
    """حجز إشعار ذريًا لمنع إرساله مرتين بالتزامن؛ SENDING القديم يعاد بعد 15 دقيقة."""
    if not signature:
        return False
    key, status, result = signature
    now = now_damascus()
    conn = None
    try:
        conn = _trade_db_connect()
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT state, sent_at FROM trade_notifications WHERE trade_key=? AND chat_id=? AND status=? AND result=?",
            (key, int(chat_id), status, result),
        ).fetchone()
        if row:
            if row[0] == "SENT":
                conn.rollback()
                return False
            try:
                pending_at = datetime.fromisoformat(row[1])
                if pending_at.tzinfo is None:
                    pending_at = pending_at.replace(tzinfo=DAMASCUS)
                stale = (now - pending_at).total_seconds() >= 15 * 60
            except Exception:
                stale = True
            if not stale:
                conn.rollback()
                return False
            conn.execute(
                "UPDATE trade_notifications SET state='SENDING', sent_at=? WHERE trade_key=? AND chat_id=? AND status=? AND result=?",
                (now.isoformat(), key, int(chat_id), status, result),
            )
        else:
            conn.execute(
                "INSERT INTO trade_notifications(trade_key,chat_id,status,result,sent_at,state) VALUES(?,?,?,?,?,?)",
                (key, int(chat_id), status, result, now.isoformat(), "SENDING"),
            )
        conn.commit()
        return True
    except Exception:
        if conn:
            try: conn.rollback()
            except Exception: pass
        logger.exception("تعذر حجز إشعار الصفقة")
        return False
    finally:
        if conn: conn.close()


def _complete_notification(chat_id, signature):
    if not signature:
        return False
    key, status, result = signature
    conn = None
    try:
        conn = _trade_db_connect()
        conn.execute(
            "UPDATE trade_notifications SET state='SENT', sent_at=? WHERE trade_key=? AND chat_id=? AND status=? AND result=?",
            (now_damascus().isoformat(), key, int(chat_id), status, result),
        )
        conn.commit()
        return True
    except Exception:
        if conn:
            try: conn.rollback()
            except Exception: pass
        logger.exception("تعذر تأكيد إرسال إشعار الصفقة")
        return False
    finally:
        if conn: conn.close()


def _cancel_notification_claim(chat_id, signature):
    if not signature:
        return False
    key, status, result = signature
    conn = None
    try:
        conn = _trade_db_connect()
        conn.execute(
            "DELETE FROM trade_notifications WHERE trade_key=? AND chat_id=? AND status=? AND result=? AND state='SENDING'",
            (key, int(chat_id), status, result),
        )
        conn.commit()
        return True
    except Exception:
        if conn:
            try: conn.rollback()
            except Exception: pass
        logger.exception("تعذر إلغاء حجز إشعار الصفقة")
        return False
    finally:
        if conn: conn.close()


async def _send_trade_notification(chat_id, result, record, title, consume_quota=False):
    """إرسال إشعار واحد بشكل آمن؛ الصفقة الجديدة فقط تحجز حصة."""
    signature = _trade_notification_signature(record)
    if not signature:
        return False
    if _notification_already_sent(chat_id, signature):
        return False
    if not _claim_notification(chat_id, signature):
        return False

    quota_token = _reserve_trade_quota(chat_id) if consume_quota else None
    if consume_quota and not quota_token:
        _cancel_notification_claim(chat_id, signature)
        return False
    if consume_quota and not quota_token:
        logger.info("Trade quota exhausted/blocked for %s", chat_id)
        return False

    try:
        await APPLICATION.bot.send_message(
            chat_id=chat_id,
            text=_format_trade_message(result, title, record),
        )
        _complete_notification(chat_id, signature)
        _mark_notification_sent(chat_id, signature)
        return True
    except Exception:
        _cancel_notification_claim(chat_id, signature)
        if consume_quota:
            _release_trade_quota(quota_token)
        logger.exception("Signal send error for %s", chat_id)
        return False


async def auto_loop():
    """دورة آلية كل 15 دقيقة مع بوابات السوق والحصة والإشعارات."""
    while True:
        try:
            # تحديث الصفقات القائمة أولاً. تغيّر TP/SL/الحالة لا يستهلك حصة جديدة.
            changed_records = update_trade_results()

            eligible = alert_subscribers()
            SUBSCRIBERS.update(eligible)
            SUBSCRIBERS.intersection_update(eligible)

            if SUBSCRIBERS and changed_records:
                for record in changed_records:
                    result_update = {
                        "direction": record.get("direction"),
                        "score": record.get("score", 0),
                        "quality": record.get("quality", ""),
                        "price": record.get("last_price", record.get("entry", 0)),
                        "trade": record,
                        "factors": [],
                    }
                    title = "🔄 تحديث الصفقة — حالة جديدة"
                    for chat_id in list(SUBSCRIBERS):
                        await _send_trade_notification(chat_id, result_update, record, title, consume_quota=False)

            if not AUTO_ENABLED or not SUBSCRIBERS:
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue

            # لا نفتح صفقات جديدة في عطلة نهاية الأسبوع أو العطل المعلنة.
            closed_reason = market_closed_reason()
            if closed_reason:
                logger.info("Auto scan skipped: market closed (%s)", closed_reason)
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue

            result = await asyncio.to_thread(evaluate_signal)

            # هذه البوابات إلزامية قبل إنشاء/إرسال أي صفقة جديدة.
            if not result.get("signal"):
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue
            if result.get("news_blocked"):
                logger.info("Auto trade blocked by news filter")
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue
            if result.get("liquidity_blocked"):
                logger.info("Auto trade blocked by liquidity retest gate")
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue
            if not result.get("trade"):
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue

            record, is_new = register_trade(result)
            if not record:
                logger.warning("Auto trade registration failed")
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue

            # صفقة جديدة: حصة واحدة فقط لكل مستخدم. صفقة قائمة: لا نرسل إلا عند تغير الحالة.
            title = "🚨 إشارة ذهب — صفقة جديدة" if is_new else "🔄 تحديث الصفقة"
            for chat_id in list(SUBSCRIBERS):
                if not is_new:
                    await _send_trade_notification(chat_id, result, record, title, consume_quota=False)
                else:
                    await _send_trade_notification(chat_id, result, record, title, consume_quota=True)

        except Exception:
            logger.exception("Auto scan error")
        await asyncio.sleep(AUTO_SCAN_SECONDS)


# ============================================================
# تشغيل Telegram
# ============================================================

async def start_bot():
    global APPLICATION
    if not TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN غير موجود في Render.")

    APPLICATION = Application.builder().token(TOKEN).build()
    APPLICATION.add_handler(CommandHandler("start", start))
    APPLICATION.add_handler(CommandHandler("plans", plans))
    APPLICATION.add_handler(CommandHandler("subscription", my_subscription))
    APPLICATION.add_handler(CommandHandler("referral", referral))
    APPLICATION.add_handler(CommandHandler("admin", admin_command))
    APPLICATION.add_handler(CommandHandler("activate", admin_command))
    APPLICATION.add_handler(CallbackQueryHandler(callback_router))
    APPLICATION.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, router))

    await APPLICATION.initialize()
    await APPLICATION.start()
    await APPLICATION.bot.set_webhook(url=WEBHOOK_URL, allowed_updates=["message", "callback_query"], drop_pending_updates=True)

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
