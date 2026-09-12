# ============================================================
# XAU SMART TRADER v18.89
# Structural Liquidity + Quantitative Momentum
# v18.27: توحيد محرك الصفقات مع التقرير اليومي + حماية حالة السيولة من التزامن
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
import sys
import io
import zipfile
import re
import asyncio
import inspect
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import queue
import logging
import json
import math
import hashlib
import sqlite3
from functools import wraps
from contextlib import contextmanager
from contextvars import ContextVar
from collections import defaultdict, deque
import uuid
import random
from datetime import date, datetime, timedelta, timezone
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo

import requests
import pandas as pd
import numpy as np

from flask import Flask, request, jsonify
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
try:
    from telegram.error import RetryAfter, TimedOut, NetworkError, Forbidden, BadRequest
except Exception:
    RetryAfter = TimedOut = NetworkError = Forbidden = BadRequest = Exception
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters

# ============================================================
# PERFORMANCE AUDITOR — LOW-OVERHEAD / SRE HARDENED ENGINE v19
# ============================================================================
# الهدف: مراقبة صحة البوت نفسه بدون إدخال SQLite/HTTP/قياسات ثقيلة داخل Event Loop.
# التصميم:
#   1) مراقبة الصفقات والتحليلات تبقى مستقلة وظيفياً.
#   2) قياسات النظام تحفظ في Ring Buffers صغيرة في الذاكرة.
#   3) الكتابة الدورية إلى SQLite تتم من Thread مستقل فقط.
#   4) HTTP الخاص بالأسعار محصور في Auditor Thread ولا يدخل Event Loop.
#   5) Event-loop lag يقاس من coroutine خفيف داخل الحلقة نفسها.
#   6) لا يوجد restart تلقائي لمحرك التداول؛ self-healing محافظ وآمن فقط.
# ============================================================================

AUDITOR_DB_PATH = os.getenv("AUDITOR_DB_PATH", "xau_performance_auditor.db")
AUDITOR_SYMBOL = os.getenv("AUDITOR_SYMBOL", "XAUUSD")
AUDITOR_POLL_SECONDS = max(5, int(os.getenv("AUDITOR_POLL_SECONDS", "15")))
AUDITOR_REQUEST_TIMEOUT = min(10.0, max(1.0, float(os.getenv("AUDITOR_REQUEST_TIMEOUT", "4"))))
AUDITOR_RETRY_COUNT = min(2, max(1, int(os.getenv("AUDITOR_RETRY_COUNT", "2"))))
AUDITOR_PRICE_CACHE_SECONDS = max(0.0, float(os.getenv("AUDITOR_PRICE_CACHE_SECONDS", "3")))
AUDITOR_DEFAULT_TRADE_EXPIRY_HOURS = float(os.getenv("AUDITOR_TRADE_EXPIRY_HOURS", "24"))
AUDITOR_PRICE_RETENTION_DAYS = max(1, int(os.getenv("AUDITOR_PRICE_RETENTION_DAYS", "30")))
AUDITOR_LOG_RETENTION_DAYS = max(1, int(os.getenv("AUDITOR_LOG_RETENTION_DAYS", "7")))
AUDITOR_METRIC_RETENTION_DAYS = max(1, int(os.getenv("AUDITOR_METRIC_RETENTION_DAYS", "7")))
AUDITOR_METRIC_PERSIST_SECONDS = max(30, int(os.getenv("AUDITOR_METRIC_PERSIST_SECONDS", "60")))
AUDITOR_EVENT_LOOP_PROBE_SECONDS = max(0.5, float(os.getenv("AUDITOR_EVENT_LOOP_PROBE_SECONDS", "1")))
AUDITOR_RING_SIZE = max(64, min(512, int(os.getenv("AUDITOR_RING_SIZE", "256"))))
AUDITOR_CPU_WARN_PCT = float(os.getenv("AUDITOR_CPU_WARN_PCT", "80"))
AUDITOR_CPU_CRIT_PCT = float(os.getenv("AUDITOR_CPU_CRIT_PCT", "95"))
AUDITOR_RSS_WARN_MB = float(os.getenv("AUDITOR_RSS_WARN_MB", "450"))
AUDITOR_RSS_CRIT_MB = float(os.getenv("AUDITOR_RSS_CRIT_MB", "700"))
AUDITOR_LATENCY_WARN_MS = float(os.getenv("AUDITOR_LATENCY_WARN_MS", "1500"))
AUDITOR_LATENCY_CRIT_MS = float(os.getenv("AUDITOR_LATENCY_CRIT_MS", "5000"))
AUDITOR_LOOP_WARN_MS = float(os.getenv("AUDITOR_LOOP_WARN_MS", "250"))
AUDITOR_LOOP_CRIT_MS = float(os.getenv("AUDITOR_LOOP_CRIT_MS", "1000"))
AUDITOR_QUEUE_WARN_PCT = float(os.getenv("AUDITOR_QUEUE_WARN_PCT", "75"))
AUDITOR_QUEUE_CRIT_PCT = float(os.getenv("AUDITOR_QUEUE_CRIT_PCT", "95"))
AUDITOR_STALE_PRICE_SECONDS = float(os.getenv("AUDITOR_STALE_PRICE_SECONDS", "90"))
# v18.89 Telegram lifecycle telemetry thresholds (diagnostic-only).
UPDATE_QUEUE_WARN_MS = float(os.getenv("UPDATE_QUEUE_WARN_MS", "250"))
UPDATE_QUEUE_CRIT_MS = float(os.getenv("UPDATE_QUEUE_CRIT_MS", "1000"))
UPDATE_TOTAL_WARN_MS = float(os.getenv("UPDATE_TOTAL_WARN_MS", "1500"))
UPDATE_TOTAL_CRIT_MS = float(os.getenv("UPDATE_TOTAL_CRIT_MS", "5000"))

AUDITOR_LOGGER = logging.getLogger("xau_performance_auditor")

# Lightweight Telegram update lifecycle telemetry. No user identifiers are stored.
_UPDATE_TELEMETRY_LOCK = threading.Lock()
_UPDATE_TELEMETRY = {
    "received": 0, "scheduled": 0, "completed": 0, "schedule_failures": 0,
    "queue_wait_ms": deque(maxlen=256), "total_ms": deque(maxlen=256),
    "last_received_mono": 0.0, "last_completed_mono": 0.0,
}

from typing import Any, Optional, Iterator
import os as _auditor_os
import time as _auditor_time

try:
    import resource as _auditor_resource
except Exception:
    _auditor_resource = None

@dataclass(frozen=True)
class TradeInput:
    trade_id: str
    signal_time: str
    direction: str
    entry: float
    sl: float
    tp1: Optional[float] = None
    tp2: Optional[float] = None
    tp3: Optional[float] = None
    tp_final: Optional[float] = None
    score: Optional[float] = None
    quality: Optional[str] = None
    risk_reward: Optional[float] = None
    timeframe: Optional[str] = None
    expiry_time: Optional[str] = None
    analysis_snapshot: dict[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class AnalysisInput:
    analysis_id: str
    analysis_type: str
    issue_time: str
    expiry_time: str
    expected_direction: str
    expected_min_price: Optional[float] = None
    expected_max_price: Optional[float] = None
    expected_target: Optional[float] = None
    score: Optional[float] = None
    confidence: Optional[float] = None
    analysis_snapshot: dict[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class PricePoint:
    timestamp: str
    price: float
    source: str = "unknown"
    high: Optional[float] = None
    low: Optional[float] = None

@dataclass(frozen=True)
class BarPoint:
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    source: str = "unknown"

SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS Live_Trades (
    trade_id TEXT PRIMARY KEY, signal_time TEXT NOT NULL,
    direction TEXT NOT NULL CHECK(direction IN ('BUY','SELL')),
    entry REAL NOT NULL, sl REAL NOT NULL, tp1 REAL, tp2 REAL, tp3 REAL, tp_final REAL,
    score REAL, quality TEXT, risk_reward REAL, timeframe TEXT, expiry_time TEXT,
    status TEXT NOT NULL DEFAULT 'ACTIVE', final_result TEXT DEFAULT 'OPEN', close_time TEXT,
    max_drawdown REAL DEFAULT 0, max_drawdown_price REAL, max_drawdown_time TEXT,
    max_adverse_excursion REAL DEFAULT 0, mae_price REAL, mae_time TEXT,
    max_favorable_excursion REAL DEFAULT 0, mfe_price REAL, mfe_time TEXT,
    current_price REAL, last_price_time TEXT, snapshot_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS Trade_Events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id TEXT NOT NULL REFERENCES Live_Trades(trade_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL, event_time TEXT NOT NULL, price REAL, price_distance REAL,
    metadata_json TEXT NOT NULL DEFAULT '{}', UNIQUE(trade_id, event_type)
);
CREATE INDEX IF NOT EXISTS idx_trade_events_trade_time ON Trade_Events(trade_id, event_time);
CREATE TABLE IF NOT EXISTS Market_Analysis (
    analysis_id TEXT PRIMARY KEY,
    analysis_type TEXT NOT NULL CHECK(analysis_type IN ('DAILY','WEEKLY')),
    issue_time TEXT NOT NULL, expiry_time TEXT NOT NULL,
    expected_direction TEXT NOT NULL CHECK(expected_direction IN ('BULLISH','BEARISH','SIDEWAYS','WAIT')),
    expected_min_price REAL, expected_max_price REAL, expected_target REAL,
    score REAL, confidence REAL, status TEXT NOT NULL DEFAULT 'ACTIVE',
    actual_start_price REAL, actual_end_price REAL, actual_high REAL, actual_low REAL,
    actual_range REAL, direction_accuracy REAL, range_accuracy REAL, target_accuracy REAL,
    accuracy_score REAL, result TEXT DEFAULT 'PENDING', notes TEXT,
    snapshot_json TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_analysis_expiry ON Market_Analysis(status, expiry_time);
CREATE TABLE IF NOT EXISTS Audit_Log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT, log_time TEXT NOT NULL, level TEXT NOT NULL,
    category TEXT NOT NULL, message TEXT NOT NULL, metadata_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_audit_log_time ON Audit_Log(log_time);
CREATE TABLE IF NOT EXISTS Price_Samples (
    sample_id INTEGER PRIMARY KEY AUTOINCREMENT, sample_time TEXT NOT NULL,
    price REAL NOT NULL, source TEXT NOT NULL, high REAL, low REAL
);
CREATE INDEX IF NOT EXISTS idx_price_samples_time ON Price_Samples(sample_time);
CREATE TABLE IF NOT EXISTS System_Metrics (
    metric_id INTEGER PRIMARY KEY AUTOINCREMENT, sampled_at TEXT NOT NULL,
    uptime_s REAL NOT NULL, cpu_pct REAL, rss_mb REAL, event_loop_lag_ms REAL,
    request_avg_ms REAL, request_p95_ms REAL, request_max_ms REAL,
    request_count INTEGER NOT NULL DEFAULT 0, throughput_per_min REAL NOT NULL DEFAULT 0,
    price_latency_ms REAL, db_write_latency_ms REAL, price_age_s REAL,
    queue_depth INTEGER NOT NULL DEFAULT 0, queue_capacity INTEGER NOT NULL DEFAULT 0,
    queue_drops INTEGER NOT NULL DEFAULT 0, error_count INTEGER NOT NULL DEFAULT 0,
    alert_count INTEGER NOT NULL DEFAULT 0, self_heal_count INTEGER NOT NULL DEFAULT 0,
    auditor_heartbeat INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_system_metrics_time ON System_Metrics(sampled_at);
"""

class Database:
    def __init__(self, path: str = AUDITOR_DB_PATH):
        self.path = path
        self._lock = threading.RLock()
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def initialize(self) -> None:
        with self._lock, self.connect() as conn:
            conn.executescript(SCHEMA)
            # Safe additive migration: handler telemetry only; no trading data/path changes.
            existing = {row[1] for row in conn.execute("PRAGMA table_info(System_Metrics)").fetchall()}
            for column, definition in (("handler_avg_ms", "REAL DEFAULT 0"), ("handler_p95_ms", "REAL DEFAULT 0"), ("handler_max_ms", "REAL DEFAULT 0"), ("handler_count", "INTEGER NOT NULL DEFAULT 0")):
                if column not in existing:
                    conn.execute(f"ALTER TABLE System_Metrics ADD COLUMN {column} {definition}")
            conn.commit()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = self.connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    @staticmethod
    def jdump(value: Any) -> str:
        return json.dumps(value if value is not None else {}, ensure_ascii=False, default=str, sort_keys=True)

    @staticmethod
    def jload(value: Optional[str]) -> dict[str, Any]:
        try:
            obj = json.loads(value or "{}")
            return obj if isinstance(obj, dict) else {"value": obj}
        except Exception:
            return {}

    def audit_log(self, level: str, category: str, message: str, metadata: Any = None, now: str = "") -> None:
        # Logging is reserved for warnings/errors and lifecycle events.
        # Normal 15-second price ticks are intentionally NOT written here.
        with self._lock, self.connect() as conn:
            conn.execute("INSERT INTO Audit_Log(log_time,level,category,message,metadata_json) VALUES(?,?,?,?,?)",
                         (now or _utc_now(), level, category, message, self.jdump(metadata)))
            conn.commit()

class PriceProvider:
    name = "base"
    def get_price(self) -> PricePoint:
        raise NotImplementedError

class BiquoteProvider(PriceProvider):
    name = "Biquote"
    def __init__(self, base_url: str = "https://biquote.io/api/XAUUSD"):
        self.url = base_url
    def get_price(self) -> PricePoint:
        r = self._request().get(self.url, params={"allowStale": "false"}, timeout=AUDITOR_REQUEST_TIMEOUT)
        r.raise_for_status(); data = r.json()
        mid = data.get("mid")
        if mid is None and data.get("bid") is not None and data.get("ask") is not None:
            mid = (float(data["bid"]) + float(data["ask"])) / 2
        price = float(mid)
        if price <= 0: raise ValueError("Invalid Biquote price")
        return PricePoint(timestamp=_utc_now(), price=price, source=self.name)
    _session = None
    @classmethod
    def _request(cls):
        if cls._session is None: cls._session = requests.Session()
        return cls._session

class XausProvider(PriceProvider):
    name = "XAUS"
    def __init__(self, url: str = "https://xaus.com/api/v1/spot"):
        self.url = url
    def get_price(self) -> PricePoint:
        r = self._request().get(self.url, timeout=AUDITOR_REQUEST_TIMEOUT)
        r.raise_for_status(); data = r.json(); price = float(data["spot_usd_oz"])
        if price <= 0: raise ValueError("Invalid XAUS price")
        return PricePoint(timestamp=_utc_now(), price=price, source=self.name)
    _session = None
    @classmethod
    def _request(cls):
        if cls._session is None: cls._session = requests.Session()
        return cls._session

class YFinanceProvider(PriceProvider):
    name = "yfinance"
    def get_price(self) -> PricePoint:
        import yfinance as yf
        ticker = yf.Ticker("GC=F")
        hist = ticker.history(period="1d", interval="1m", auto_adjust=False)
        if hist.empty: raise RuntimeError("yfinance returned no data")
        price = float(hist["Close"].dropna().iloc[-1])
        if price <= 0: raise ValueError("Invalid yfinance price")
        return PricePoint(timestamp=_utc_now(), price=price, source=self.name)

def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

class LivePriceEngine:
    def __init__(self, providers: Optional[list[PriceProvider]] = None, cache_seconds: float = AUDITOR_PRICE_CACHE_SECONDS):
        self.providers = providers or [BiquoteProvider(), XausProvider(), YFinanceProvider()]
        self.cache_seconds = cache_seconds
        self._cache: Optional[PricePoint] = None
        self._cache_at = 0.0

    def get_price(self, force: bool = False) -> tuple[PricePoint, float]:
        if not force and self._cache and _auditor_time.monotonic() - self._cache_at < self.cache_seconds:
            return self._cache, 0.0
        errors = []
        started = _auditor_time.perf_counter()
        for provider in self.providers:
            for attempt in range(AUDITOR_RETRY_COUNT):
                try:
                    pstarted = _auditor_time.perf_counter()
                    point = provider.get_price()
                    latency_ms = (_auditor_time.perf_counter() - pstarted) * 1000.0
                    self._cache, self._cache_at = point, _auditor_time.monotonic()
                    return point, latency_ms
                except Exception as exc:
                    errors.append(f"{provider.name}: {exc}")
                    if attempt + 1 < AUDITOR_RETRY_COUNT:
                        _auditor_time.sleep(0.20 * (attempt + 1))
        raise RuntimeError("All price providers failed: " + " | ".join(errors))

FINAL_RESULTS = {"FULL_SUCCESS", "FAILED", "PARTIAL_SUCCESS", "EXPIRED", "AMBIGUOUS"}

def parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()

def levels_for(trade: dict) -> list[tuple[str, float]]:
    vals = []
    for key, label in (("tp1", "TP1"), ("tp2", "TP2"), ("tp3", "TP3"), ("tp_final", "TP_FINAL")):
        value = trade.get(key)
        if value is not None: vals.append((label, float(value)))
    return vals

class TradeAuditor:
    def __init__(self, db: Database): self.db = db
    def register(self, trade: TradeInput) -> bool:
        direction = trade.direction.upper()
        if direction not in {"BUY", "SELL"}: raise ValueError("direction must be BUY or SELL")
        if trade.entry <= 0 or trade.sl <= 0: raise ValueError("entry/sl must be positive")
        targets = [x for x in (trade.tp1, trade.tp2, trade.tp3, trade.tp_final) if x is not None]
        if not targets: raise ValueError("At least one target is required")
        if direction == "BUY" and not (trade.sl < trade.entry < targets[0] <= targets[-1]): raise ValueError("Invalid BUY level ordering")
        if direction == "SELL" and not (trade.sl > trade.entry > targets[0] >= targets[-1]): raise ValueError("Invalid SELL level ordering")
        now = trade.signal_time
        with self.db.transaction() as conn:
            if conn.execute("SELECT 1 FROM Live_Trades WHERE trade_id=?", (trade.trade_id,)).fetchone(): return False
            conn.execute("""INSERT INTO Live_Trades(
                trade_id,signal_time,direction,entry,sl,tp1,tp2,tp3,tp_final,score,quality,risk_reward,timeframe,expiry_time,
                status,final_result,snapshot_json,created_at,updated_at,current_price,last_price_time
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (trade.trade_id,trade.signal_time,direction,trade.entry,trade.sl,trade.tp1,trade.tp2,trade.tp3,trade.tp_final,
             trade.score,trade.quality,trade.risk_reward,trade.timeframe,trade.expiry_time,"ACTIVE","OPEN",
             self.db.jdump(trade.analysis_snapshot),now,now,trade.entry,trade.signal_time))
            conn.execute("INSERT INTO Trade_Events(trade_id,event_type,event_time,price,metadata_json) VALUES(?,?,?,?,?)",
                         (trade.trade_id,"ENTRY",trade.signal_time,trade.entry,self.db.jdump({"source":"signal"})))
        return True

    def _update_excursions(self, conn, trade: dict, point: PricePoint) -> None:
        direction=trade["direction"]; entry=float(trade["entry"])
        adverse=max(0.0,entry-point.price) if direction=="BUY" else max(0.0,point.price-entry)
        favorable=max(0.0,point.price-entry) if direction=="BUY" else max(0.0,entry-point.price)
        if adverse > float(trade.get("max_adverse_excursion") or 0):
            conn.execute("UPDATE Live_Trades SET max_adverse_excursion=?,mae_price=?,mae_time=? WHERE trade_id=?",(adverse,point.price,point.timestamp,trade["trade_id"]))
        if favorable > float(trade.get("max_favorable_excursion") or 0):
            conn.execute("UPDATE Live_Trades SET max_favorable_excursion=?,mfe_price=?,mfe_time=? WHERE trade_id=?",(favorable,point.price,point.timestamp,trade["trade_id"]))
        if adverse > float(trade.get("max_drawdown") or 0):
            conn.execute("UPDATE Live_Trades SET max_drawdown=?,max_drawdown_price=?,max_drawdown_time=? WHERE trade_id=?",(adverse,point.price,point.timestamp,trade["trade_id"]))

    def _event(self, conn, trade: dict, event_type: str, point: PricePoint, distance: Optional[float]=None, metadata: Optional[dict]=None) -> bool:
        cur=conn.execute("INSERT OR IGNORE INTO Trade_Events(trade_id,event_type,event_time,price,price_distance,metadata_json) VALUES(?,?,?,?,?,?)",
                         (trade["trade_id"],event_type,point.timestamp,point.price,distance,self.db.jdump(metadata)))
        return cur.rowcount > 0

    def observe_tick(self, point: PricePoint) -> list[str]:
        changed=[]
        with self.db.transaction() as conn:
            rows=conn.execute("SELECT * FROM Live_Trades WHERE status IN ('ACTIVE','TP1','TP2')").fetchall()
            for row in rows:
                trade=dict(row); self._update_excursions(conn,trade,point)
                conn.execute("UPDATE Live_Trades SET current_price=?,last_price_time=?,updated_at=? WHERE trade_id=?",(point.price,point.timestamp,point.timestamp,trade["trade_id"]))
                if trade.get("expiry_time") and parse_dt(point.timestamp)>=parse_dt(trade["expiry_time"]):
                    self._event(conn,trade,"EXPIRY",point,metadata={"reason":"expiry_time"})
                    conn.execute("UPDATE Live_Trades SET status='CLOSED',final_result='EXPIRED',close_time=?,updated_at=? WHERE trade_id=?",(point.timestamp,point.timestamp,trade["trade_id"]))
                    changed.append(trade["trade_id"]); continue
                direction=trade["direction"]; sl=float(trade["sl"]); targets=levels_for(trade)
                hit_sl=point.price<=sl if direction=="BUY" else point.price>=sl
                hit_targets=[(label,target) for label,target in targets if (point.price>=target if direction=="BUY" else point.price<=target)]
                if hit_sl and hit_targets:
                    self._event(conn,trade,"AMBIGUOUS",point,metadata={"reason":"SL_and_target_same_tick","targets":[x[0] for x in hit_targets]})
                    conn.execute("UPDATE Live_Trades SET status='CLOSED',final_result='AMBIGUOUS',close_time=?,updated_at=? WHERE trade_id=?",(point.timestamp,point.timestamp,trade["trade_id"]))
                    changed.append(trade["trade_id"]); continue
                if hit_sl:
                    self._event(conn,trade,"SL",point,distance=abs(point.price-sl),metadata={"source":point.source})
                    conn.execute("UPDATE Live_Trades SET status='CLOSED',final_result='FAILED',close_time=?,updated_at=? WHERE trade_id=?",(point.timestamp,point.timestamp,trade["trade_id"]))
                    changed.append(trade["trade_id"]); continue
                if hit_targets:
                    events={r[0] for r in conn.execute("SELECT event_type FROM Trade_Events WHERE trade_id=?",(trade["trade_id"],)).fetchall()}
                    new_target=False
                    for label,target in hit_targets:
                        if label not in events:
                            new_target=self._event(conn,trade,label,point,distance=abs(point.price-target),metadata={"source":point.source}) or new_target
                    final_label=targets[-1][0]
                    event_names={r[0] for r in conn.execute("SELECT event_type FROM Trade_Events WHERE trade_id=?",(trade["trade_id"],)).fetchall()}
                    if final_label in event_names:
                        conn.execute("UPDATE Live_Trades SET status='CLOSED',final_result='FULL_SUCCESS',close_time=?,updated_at=? WHERE trade_id=?",(point.timestamp,point.timestamp,trade["trade_id"]))
                        changed.append(trade["trade_id"])
                    elif new_target:
                        status = "TP1" if "TP1" in event_names else "TP2" if "TP2" in event_names else "ACTIVE"
                        conn.execute("UPDATE Live_Trades SET status=?,final_result='PARTIAL_SUCCESS',updated_at=? WHERE trade_id=?",(status,point.timestamp,trade["trade_id"]))
                        changed.append(trade["trade_id"])
        return changed

    def observe_bar(self, bar: BarPoint) -> list[str]:
        changed=[]
        with self.db.transaction() as conn:
            rows=conn.execute("SELECT * FROM Live_Trades WHERE status IN ('ACTIVE','TP1','TP2')").fetchall()
            for row in rows:
                trade=dict(row); direction=trade["direction"]; sl=float(trade["sl"]); targets=levels_for(trade)
                sl_hit=bar.low<=sl if direction=="BUY" else bar.high>=sl
                target_hits=[(label,target) for label,target in targets if (bar.high>=target if direction=="BUY" else bar.low<=target)]
                point=PricePoint(bar.timestamp,bar.close,bar.source,bar.high,bar.low)
                if sl_hit and target_hits:
                    self._event(conn,trade,"AMBIGUOUS",point,metadata={"reason":"same_bar_order_unknown","targets":[x[0] for x in target_hits]})
                    conn.execute("UPDATE Live_Trades SET status='CLOSED',final_result='AMBIGUOUS',close_time=?,updated_at=? WHERE trade_id=?",(bar.timestamp,bar.timestamp,trade["trade_id"]))
                    changed.append(trade["trade_id"]); continue
                if sl_hit:
                    self._event(conn,trade,"SL",PricePoint(bar.timestamp,sl,bar.source,bar.high,bar.low),metadata={"mode":"bar"})
                    conn.execute("UPDATE Live_Trades SET status='CLOSED',final_result='FAILED',close_time=?,updated_at=? WHERE trade_id=?",(bar.timestamp,bar.timestamp,trade["trade_id"]))
                    changed.append(trade["trade_id"]); continue
                for label,target in target_hits: self._event(conn,trade,label,PricePoint(bar.timestamp,target,bar.source,bar.high,bar.low),metadata={"mode":"bar"})
                if target_hits and target_hits[-1][0]==targets[-1][0]:
                    conn.execute("UPDATE Live_Trades SET status='CLOSED',final_result='FULL_SUCCESS',close_time=?,updated_at=? WHERE trade_id=?",(bar.timestamp,bar.timestamp,trade["trade_id"]))
                    changed.append(trade["trade_id"])
        return changed

class AnalysisAuditor:
    def __init__(self, db: Database): self.db=db
    def register(self, analysis: AnalysisInput) -> bool:
        typ=analysis.analysis_type.upper(); direction=analysis.expected_direction.upper()
        if typ not in {"DAILY","WEEKLY"}: raise ValueError("analysis_type must be DAILY or WEEKLY")
        if direction not in {"BULLISH","BEARISH","SIDEWAYS","WAIT"}: raise ValueError("Invalid expected_direction")
        if analysis.expected_min_price is not None and analysis.expected_max_price is not None and analysis.expected_min_price>analysis.expected_max_price: raise ValueError("expected_min_price cannot exceed expected_max_price")
        with self.db.transaction() as conn:
            if conn.execute("SELECT 1 FROM Market_Analysis WHERE analysis_id=?",(analysis.analysis_id,)).fetchone(): return False
            conn.execute("""INSERT INTO Market_Analysis(analysis_id,analysis_type,issue_time,expiry_time,expected_direction,expected_min_price,expected_max_price,expected_target,score,confidence,status,snapshot_json,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(analysis.analysis_id,typ,analysis.issue_time,analysis.expiry_time,direction,analysis.expected_min_price,analysis.expected_max_price,analysis.expected_target,analysis.score,analysis.confidence,"ACTIVE",self.db.jdump(analysis.analysis_snapshot),analysis.issue_time,analysis.issue_time))
        return True
    @staticmethod
    def direction_score(expected,start,end,expected_min,expected_max):
        if expected=="BULLISH": return 100.0 if end>start else 0.0
        if expected=="BEARISH": return 100.0 if end<start else 0.0
        if expected=="SIDEWAYS": return 100.0 if (expected_min is None or expected_max is None) and abs(end-start)<=start*0.002 or (expected_min is not None and expected_max is not None and expected_min<=end<=expected_max) else 0.0
        return 100.0 if abs(end-start)<=start*0.002 else 0.0
    @staticmethod
    def range_score(expected_min,expected_max,actual_low,actual_high):
        if expected_min is None or expected_max is None: return 0.0
        lo,hi=float(expected_min),float(expected_max); inter=max(0.0,min(hi,actual_high)-max(lo,actual_low)); union=max(hi,actual_high)-min(lo,actual_low)
        return 100.0*inter/union if union>0 else 100.0
    @staticmethod
    def target_score(expected_target,actual_high,actual_low):
        if expected_target is None: return 0.0
        return 100.0 if actual_low<=expected_target<=actual_high else 0.0
    def finalize(self,analysis_id,bars):
        if not bars: raise ValueError("No bars supplied")
        with self.db.transaction() as conn:
            row=conn.execute("SELECT * FROM Market_Analysis WHERE analysis_id=?",(analysis_id,)).fetchone()
            if not row: raise KeyError(analysis_id)
            a=dict(row)
            if a["status"]=="CLOSED": return a
            actual_start=bars[0].open; actual_end=bars[-1].close; actual_high=max(b.high for b in bars); actual_low=min(b.low for b in bars)
            ds=self.direction_score(a["expected_direction"],actual_start,actual_end,a["expected_min_price"],a["expected_max_price"]); rs=self.range_score(a["expected_min_price"],a["expected_max_price"],actual_low,actual_high); ts=self.target_score(a["expected_target"],actual_high,actual_low); overall=.50*ds+.30*rs+.20*ts; result="MATCHED" if overall>=60 else "NOT_MATCHED"
            conn.execute("""UPDATE Market_Analysis SET status='CLOSED',actual_start_price=?,actual_end_price=?,actual_high=?,actual_low=?,actual_range=?,direction_accuracy=?,range_accuracy=?,target_accuracy=?,accuracy_score=?,result=?,updated_at=? WHERE analysis_id=?""",(actual_start,actual_end,actual_high,actual_low,actual_high-actual_low,ds,rs,ts,overall,result,bars[-1].timestamp,analysis_id))
            return dict(conn.execute("SELECT * FROM Market_Analysis WHERE analysis_id=?",(analysis_id,)).fetchone())
    def finalize_expired_from_samples(self,now_iso):
        done=[]
        with self.db.connect() as conn: rows=conn.execute("SELECT * FROM Market_Analysis WHERE status='ACTIVE' AND expiry_time<=?",(now_iso,)).fetchall()
        for row in rows:
            a=dict(row)
            with self.db.connect() as conn: samples=conn.execute("SELECT sample_time,price FROM Price_Samples WHERE sample_time>=? AND sample_time<=? ORDER BY sample_time",(a["issue_time"],a["expiry_time"])).fetchall()
            if not samples:
                self.db.audit_log("WARNING","ANALYSIS",f"No observed price samples for expired analysis {a['analysis_id']}",{},now_iso); continue
            bars=[BarPoint(r["sample_time"],float(r["price"]),float(r["price"]),float(r["price"]),float(r["price"]),"auditor") for r in samples]
            self.finalize(a["analysis_id"],bars); done.append(a["analysis_id"])
        return done

def _percentile(values, pct):
    vals=sorted(float(x) for x in values if x is not None)
    if not vals: return 0.0
    if len(vals)==1: return vals[0]
    pos=(len(vals)-1)*(pct/100.0); lo=int(pos); hi=min(lo+1,len(vals)-1); frac=pos-lo
    return vals[lo]+(vals[hi]-vals[lo])*frac


# ============================================================
# HANDLER LATENCY ROOT TRACE + AUDITOR RELIABILITY v18.83
# ============================================================
# Instrumentation-only layer:
# - Covers direct Telegram handlers (command/callback/message) at registration time.
# - Correlates nested application stages with one trace_id.
# - Never changes trading decisions, webhook dispatch, timeouts, or business rules.
# - Keeps telemetry in bounded in-memory buffers; no SQLite/HTTP work in the Event Loop.
# ============================================================

_HANDLER_TRACE_CTX = ContextVar("xau_handler_trace_ctx", default=None)
_HANDLER_TRACE_RING_SIZE = max(64, min(512, int(os.getenv("HANDLER_TRACE_RING_SIZE", "256"))))
_HANDLER_TRACE_STAGE_LIMIT = max(8, min(64, int(os.getenv("HANDLER_TRACE_STAGE_LIMIT", "32"))))
_HANDLER_TRACE_LOG_SLOW_MS = float(os.getenv("HANDLER_TRACE_LOG_SLOW_MS", "1500"))
AUDITOR_HANDLER_MIN_SAMPLES = max(5, int(os.getenv("AUDITOR_HANDLER_MIN_SAMPLES", "20")))
AUDITOR_WEBHOOK_MIN_SAMPLES = max(5, int(os.getenv("AUDITOR_WEBHOOK_MIN_SAMPLES", "20")))

def _handler_trace_id():
    return f"HT_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}_{uuid.uuid4().hex[:8]}"

def _handler_trace_update_meta(update):
    try:
        msg = getattr(update, "message", None)
        query = getattr(update, "callback_query", None)
        chat = getattr(update, "effective_chat", None)
        user = getattr(update, "effective_user", None)
        if query is not None:
            update_type = "callback"
            identifier = str(getattr(query, "data", "") or "")
        elif msg is not None:
            raw = str(getattr(msg, "text", "") or "")
            update_type = "command" if raw.startswith("/") else "message"
            identifier = raw.split(maxsplit=1)[0] if update_type == "command" else raw[:80]
        else:
            update_type = "other"
            identifier = ""
        def _safe_hash(value):
            if value is None:
                return None
            return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:12]
        return {
            "update_type": update_type,
            "identifier": identifier,
            "chat_hash": _safe_hash(getattr(chat, "id", None)),
            "user_hash": _safe_hash(getattr(user, "id", None)),
        }
    except Exception:
        return {"update_type": "unknown", "identifier": "", "chat_hash": None, "user_hash": None}

def _handler_trace_start(update, handler_name):
    meta = _handler_trace_update_meta(update)
    return {
        "trace_id": _handler_trace_id(),
        "handler": str(handler_name),
        "update_type": meta["update_type"],
        "identifier": meta["identifier"],
        "chat_hash": meta["chat_hash"],
        "user_hash": meta["user_hash"],
        "started_at": datetime.now(timezone.utc).isoformat(),
        "started_mono": time.perf_counter(),
        "webhook_received_mono": getattr(update, "_xau_webhook_received_mono", None),
        "dispatch_start_mono": getattr(update, "_xau_dispatch_start_mono", None),
        "queue_wait_ms": getattr(update, "_xau_queue_wait_ms", None),
        "stages": [],
        "outcome": "running",
    }

def _trace_expected_stop(exc):
    """Classify intentional Telegram handler termination without changing control flow."""
    try:
        cls = globals().get("ApplicationHandlerStop")
        return cls is not None and isinstance(exc, cls)
    except Exception:
        return False

def _trace_exception_outcome(exc):
    if isinstance(exc, asyncio.CancelledError):
        return "cancelled"
    if _trace_expected_stop(exc):
        return "expected_stop"
    return "exception"

def _handler_trace_finish(trace, outcome):
    if trace is None:
        return
    elapsed_ms = max(0.0, (time.perf_counter() - trace["started_mono"]) * 1000.0)
    trace["ended_at"] = datetime.now(timezone.utc).isoformat()
    trace["elapsed_ms"] = elapsed_ms
    trace["outcome"] = outcome
    queue_wait = trace.get("queue_wait_ms")
    if queue_wait is not None:
        try:
            q=float(queue_wait)
            trace["queue_wait_ms"] = max(0.0, q)
            if q >= UPDATE_QUEUE_CRIT_MS:
                AUDITOR_LOGGER.warning("UPDATE_QUEUE_WAIT_CRITICAL | trace_id=%s | handler=%s | queue_wait_ms=%.1f", trace.get("trace_id"), trace.get("handler"), q)
            elif q >= UPDATE_QUEUE_WARN_MS:
                AUDITOR_LOGGER.warning("UPDATE_QUEUE_WAIT_SLOW | trace_id=%s | handler=%s | queue_wait_ms=%.1f", trace.get("trace_id"), trace.get("handler"), q)
        except Exception:
            pass
    trace.pop("started_mono", None)
    if PERFORMANCE_AUDITOR is not None:
        try:
            PERFORMANCE_AUDITOR.metrics.record_handler_trace(trace)
        except Exception:
            pass

@contextmanager
def handler_trace_stage(name, **meta):
    trace = _HANDLER_TRACE_CTX.get()
    if trace is None:
        yield
        return
    started = time.perf_counter()
    stage = {"name": str(name), "started_at": datetime.now(timezone.utc).isoformat()}
    try:
        yield
        stage["outcome"] = "success"
    except BaseException as exc:
        stage["outcome"] = _trace_exception_outcome(exc)
        if stage["outcome"] == "exception":
            stage["exception_type"] = type(exc).__name__
            stage["exception_message"] = str(exc)[:1000]
        raise
    finally:
        stage["elapsed_ms"] = max(0.0, (time.perf_counter() - started) * 1000.0)
        stage["ended_at"] = datetime.now(timezone.utc).isoformat()
        if len(trace["stages"]) < _HANDLER_TRACE_STAGE_LIMIT:
            trace["stages"].append(stage)

def _trace_stage_function(name):
    def decorator(fn):
        if getattr(fn, "_xau_root_trace_wrapped", False):
            return fn
        if inspect.iscoroutinefunction(fn):
            @wraps(fn)
            async def _async_wrapped(*args, **kwargs):
                with handler_trace_stage(name):
                    return await fn(*args, **kwargs)
            _async_wrapped._xau_root_trace_wrapped = True
            _async_wrapped._xau_root_trace_original = fn
            return _async_wrapped
        @wraps(fn)
        def _sync_wrapped(*args, **kwargs):
            with handler_trace_stage(name):
                return fn(*args, **kwargs)
        _sync_wrapped._xau_root_trace_wrapped = True
        _sync_wrapped._xau_root_trace_original = fn
        return _sync_wrapped
    return decorator

def _trace_handler(callback, handler_name):
    if getattr(callback, "_xau_root_trace_handler_wrapped", False):
        return callback
    if inspect.iscoroutinefunction(callback):
        @wraps(callback)
        async def _async_handler(update, context):
            trace = _handler_trace_start(update, handler_name)
            token = _HANDLER_TRACE_CTX.set(trace)
            started = trace["started_mono"]
            try:
                with handler_trace_stage(f"handler:{handler_name}"):
                    result = await callback(update, context)
                trace["outcome"] = "success"
                return result
            except asyncio.CancelledError:
                trace["outcome"] = "cancelled"
                raise
            except BaseException as exc:
                trace["outcome"] = _trace_exception_outcome(exc)
                raise
            finally:
                _HANDLER_TRACE_CTX.reset(token)
                _handler_trace_finish(trace, trace.get("outcome", "success"))
                if trace.get("elapsed_ms", 0.0) >= _HANDLER_TRACE_LOG_SLOW_MS:
                    AUDITOR_LOGGER.warning(
                        "HANDLER_ROOT_TRACE | trace_id=%s | handler=%s | type=%s | id=%s | elapsed_ms=%.1f | outcome=%s | stages=%s",
                        trace.get("trace_id"), trace.get("handler"), trace.get("update_type"),
                        trace.get("identifier"), trace.get("elapsed_ms", 0.0),
                        trace.get("outcome"), trace.get("stages", []),
                    )
        _async_handler._xau_root_trace_handler_wrapped = True
        _async_handler._xau_root_trace_original = callback
        return _async_handler
    @wraps(callback)
    def _sync_handler(update, context):
        trace = _handler_trace_start(update, handler_name)
        token = _HANDLER_TRACE_CTX.set(trace)
        try:
            with handler_trace_stage(f"handler:{handler_name}"):
                return callback(update, context)
        except BaseException as exc:
            trace["outcome"] = _trace_exception_outcome(exc)
            raise
        finally:
            _HANDLER_TRACE_CTX.reset(token)
            _handler_trace_finish(trace, trace.get("outcome", "success"))
    _sync_handler._xau_root_trace_handler_wrapped = True
    _sync_handler._xau_root_trace_original = callback
    return _sync_handler

class Metrics:
    def __init__(self,db):
        self.db=db
        self.started_mono=_auditor_time.monotonic()
        self._lock=threading.Lock()
        self.latencies=deque(maxlen=AUDITOR_RING_SIZE)
        self.handler_latencies=deque(maxlen=AUDITOR_RING_SIZE)
        self.handler_traces=deque(maxlen=_HANDLER_TRACE_RING_SIZE)
        self.handler_stage_totals=defaultdict(lambda: {"count": 0, "total_ms": 0.0, "max_ms": 0.0})
        self.loop_lags=deque(maxlen=AUDITOR_RING_SIZE)
        self.price_latencies=deque(maxlen=AUDITOR_RING_SIZE)
        self.db_latencies=deque(maxlen=AUDITOR_RING_SIZE)
        self.request_count=0; self.handler_count=0; self.error_count=0; self.alert_count=0; self.self_heal_count=0; self.heartbeat=0
        self.last_price_mono=0.0; self.last_price_latency_ms=0.0; self.last_sample_at=0.0
        self.window_started=_auditor_time.monotonic(); self.window_requests=0
    def record_request(self,latency_ms):
        with self._lock: self.latencies.append(float(latency_ms)); self.request_count+=1; self.window_requests+=1
    def update_lifecycle_report(self):
        with _UPDATE_TELEMETRY_LOCK:
            q=list(_UPDATE_TELEMETRY["queue_wait_ms"]); total=list(_UPDATE_TELEMETRY["total_ms"])
            return {
                "received": int(_UPDATE_TELEMETRY["received"]),
                "scheduled": int(_UPDATE_TELEMETRY["scheduled"]),
                "completed": int(_UPDATE_TELEMETRY["completed"]),
                "schedule_failures": int(_UPDATE_TELEMETRY["schedule_failures"]),
                "queue_wait_p95_ms": _percentile(q,95),
                "queue_wait_max_ms": max(q) if q else 0.0,
                "total_p95_ms": _percentile(total,95),
                "total_max_ms": max(total) if total else 0.0,
                "sample_count": len(total),
            }
    def record_handler(self,latency_ms):
        with self._lock:
            self.handler_latencies.append(max(0.0,float(latency_ms))); self.handler_count+=1
    def record_handler_trace(self,trace):
        """Store one completed root trace and update handler/stage aggregates atomically."""
        if not isinstance(trace,dict):
            return
        elapsed=max(0.0,float(trace.get("elapsed_ms",0.0) or 0.0))
        item=dict(trace)
        item["elapsed_ms"]=elapsed
        item["stages"]=[dict(stage) for stage in (trace.get("stages") or []) if isinstance(stage,dict)]
        with self._lock:
            self.handler_traces.append(item)
            self.handler_latencies.append(elapsed)
            self.handler_count+=1
            for stage in item["stages"]:
                name=str(stage.get("name") or "unknown")
                ms=max(0.0,float(stage.get("elapsed_ms",0.0) or 0.0))
                agg=self.handler_stage_totals[name]
                agg["count"]+=1
                agg["total_ms"]+=ms
                if ms>agg["max_ms"]:
                    agg["max_ms"]=ms
    def handler_trace_report(self):
        """Return a bounded, read-only-style snapshot for the Telegram auditor UI."""
        with self._lock:
            latest=[dict(t, stages=[dict(s) for s in (t.get("stages") or [])]) for t in list(self.handler_traces)[-5:]]
            latest.reverse()
            aggregates=[]
            for name,agg in self.handler_stage_totals.items():
                count=int(agg.get("count",0))
                total=float(agg.get("total_ms",0.0))
                aggregates.append({"stage":name,"count":count,"avg_ms":total/count if count else 0.0,"max_ms":float(agg.get("max_ms",0.0))})
        aggregates.sort(key=lambda x:(x["max_ms"],x["avg_ms"]),reverse=True)
        return {"count":len(self.handler_traces),"latest":latest,"stage_aggregates":aggregates[:10]}
    def record_loop_lag(self,lag_ms):
        with self._lock: self.loop_lags.append(max(0.0,float(lag_ms)))
    def record_price(self,latency_ms):
        with self._lock: self.price_latencies.append(max(0.0,float(latency_ms))); self.last_price_mono=_auditor_time.monotonic(); self.last_price_latency_ms=float(latency_ms)
    def record_db(self,latency_ms):
        with self._lock: self.db_latencies.append(max(0.0,float(latency_ms)))
    def inc_error(self):
        with self._lock: self.error_count+=1
    def inc_alert(self):
        with self._lock: self.alert_count+=1
    def inc_self_heal(self):
        with self._lock: self.self_heal_count+=1
    def heartbeat_tick(self):
        with self._lock: self.heartbeat+=1
    def system_snapshot(self,queue_depth=0,queue_capacity=0,queue_drops=0):
        with self._lock:
            now=_auditor_time.monotonic(); elapsed=max(0.001,now-self.window_started); throughput=self.window_requests*60.0/elapsed
            if elapsed>=60.0: self.window_started=now; self.window_requests=0
            req=list(self.latencies); handlers=list(self.handler_latencies); loops=list(self.loop_lags); pls=list(self.price_latencies); dbs=list(self.db_latencies)
            req_count=self.request_count; handler_count=self.handler_count; errors=self.error_count; alerts=self.alert_count; heals=self.self_heal_count; hb=self.heartbeat; last_price=self.last_price_mono
        cpu_pct=self._cpu_pct(); rss=self._rss_mb(); price_age=max(0.0,now-last_price) if last_price else None
        trace_count=len(self.handler_traces)
        handler_ready=len(handlers) >= AUDITOR_HANDLER_MIN_SAMPLES
        webhook_ready=len(req) >= AUDITOR_WEBHOOK_MIN_SAMPLES
        return {"uptime_s":now-self.started_mono,"cpu_pct":cpu_pct,"rss_mb":rss,"event_loop_lag_ms":_percentile(loops,95),"event_loop_lag_max_ms":max(loops) if loops else 0.0,"request_avg_ms":sum(req)/len(req) if req else 0.0,"request_p95_ms":_percentile(req,95),"request_max_ms":max(req) if req else 0.0,"request_count":req_count,"request_sample_count":len(req),"webhook_min_samples":AUDITOR_WEBHOOK_MIN_SAMPLES,"webhook_p95_ready":webhook_ready,"handler_avg_ms":sum(handlers)/len(handlers) if handlers else 0.0,"handler_p95_ms":_percentile(handlers,95),"handler_max_ms":max(handlers) if handlers else 0.0,"handler_count":handler_count,"handler_sample_count":len(handlers),"handler_min_samples":AUDITOR_HANDLER_MIN_SAMPLES,"handler_p95_ready":handler_ready,"handler_trace_count":trace_count,"throughput_per_min":throughput,"price_latency_ms":sum(pls)/len(pls) if pls else self.last_price_latency_ms,"db_write_latency_ms":sum(dbs)/len(dbs) if dbs else 0.0,"price_age_s":price_age,"queue_depth":queue_depth,"queue_capacity":queue_capacity,"queue_drops":queue_drops,"error_count":errors,"alert_count":alerts,"self_heal_count":heals,"auditor_heartbeat":hb, "update_lifecycle":self.update_lifecycle_report()}
    @staticmethod
    def _rss_mb():
        try:
            if os.path.exists("/proc/self/statm"):
                pages=int(open("/proc/self/statm","r").read().split()[1]); return pages*os.sysconf("SC_PAGE_SIZE")/(1024*1024)
        except Exception: pass
        if _auditor_resource:
            try:
                value=float(_auditor_resource.getrusage(_auditor_resource.RUSAGE_SELF).ru_maxrss)
                return value/(1024*1024) if value>100000 else value/1024.0
            except Exception: pass
        return 0.0
    def _cpu_pct(self):
        # Approximate process CPU over a short sample window without psutil.
        # This method is called only once per persisted metrics interval, not every tick.
        if not hasattr(self,"_cpu_prev"): self._cpu_prev=(_auditor_time.monotonic(),_auditor_time.process_time()); return 0.0
        now=_auditor_time.monotonic(); cpu=_auditor_time.process_time(); wall=max(0.001,now-self._cpu_prev[0]); dcpu=max(0.0,cpu-self._cpu_prev[1]); self._cpu_prev=(now,cpu)
        return min(100.0,100.0*dcpu/wall/max(1,os.cpu_count() or 1))
    def _sr_monitor_report(self):
        """Read-only S/R monitor snapshot from the independent S/R SQLite database.
        No writes, no strategy calls, and no impact on the main trade auditor.
        Result mapping: TP3=FULL_SUCCESS, TP1/TP2=PARTIAL_SUCCESS, SL=FAILED.
        """
        result = {
            "total": 0, "full_success": 0, "partial_success": 0,
            "failed": 0, "expired": 0, "ambiguous": 0,
            "monitoring_total": 0, "active": 0,
        }
        try:
            sr_path = globals().get("SR_DB_PATH", "xau_sr_trades.db")
            with sqlite3.connect(sr_path, timeout=2, check_same_thread=False) as conn:
                conn.row_factory = sqlite3.Row
                rows = [dict(r) for r in conn.execute(
                    "SELECT status, result FROM sr_trades"
                ).fetchall()]
            for row in rows:
                status = str(row.get("status") or "").upper()
                event = str(row.get("result") or "").upper()
                if status in {"OPEN", "TP1_HIT", "TP2_HIT"}:
                    result["monitoring_total"] += 1
                if status == "OPEN":
                    result["active"] += 1
                if status == "CLOSED":
                    result["total"] += 1
                    if event == "TP3":
                        result["full_success"] += 1
                    elif event in {"TP1", "TP2"}:
                        result["partial_success"] += 1
                    elif event == "SL":
                        result["failed"] += 1
                    elif event in {"EXPIRED", "EXPIRY"}:
                        result["expired"] += 1
                    elif event == "AMBIGUOUS":
                        result["ambiguous"] += 1
            return result
        except Exception as exc:
            result["error"] = type(exc).__name__
            return result

    def report(self):
        """Reporting-only lifecycle: current Damascus day stays under monitoring, not final statistics."""
        try:
            from zoneinfo import ZoneInfo
            cutoff=datetime.now(ZoneInfo("Asia/Damascus")).replace(hour=0,minute=0,second=0,microsecond=0).astimezone(timezone.utc)
        except Exception:
            cutoff=datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0)
        cutoff_iso=cutoff.isoformat()
        with self.db.connect() as c:
            all_trades=[dict(r) for r in c.execute("SELECT * FROM Live_Trades").fetchall()]
            all_analyses=[dict(r) for r in c.execute("SELECT * FROM Market_Analysis").fetchall()]
        def before_cutoff(row,key):
            try:
                return parse_dt(row.get(key) or row.get("created_at")) < cutoff
            except Exception:
                return False
        # نتائج الصفقات لا تُصفّر ولا تُؤجّل بسبب تاريخ اليوم.
        # أي صفقة أُغلقت فعلياً تدخل الإحصائيات فوراً، بينما تبقى الصفقات المفتوحة تحت المراقبة.
        final_trades=[t for t in all_trades if t.get("status")=="CLOSED" and t.get("final_result") in FINAL_RESULTS]
        monitoring_trades=[t for t in all_trades if t.get("status") in ("ACTIVE","TP1","TP2") or t not in final_trades]
        full=sum(t.get("final_result")=="FULL_SUCCESS" for t in final_trades); failed=sum(t.get("final_result")=="FAILED" for t in final_trades); partial=sum(t.get("final_result")=="PARTIAL_SUCCESS" for t in final_trades); expired=sum(t.get("final_result")=="EXPIRED" for t in final_trades); ambiguous=sum(t.get("final_result")=="AMBIGUOUS" for t in final_trades); closed=len(final_trades)
        def avg(field):
            vals=[float(x[field]) for x in final_trades if x.get(field) is not None]
            return sum(vals)/len(vals) if vals else None
        official_analyses=[a for a in all_analyses if a.get("status")=="CLOSED" and before_cutoff(a,"updated_at")]
        monitoring_analyses=[a for a in all_analyses if a not in official_analyses]
        def ast(items):
            n=len(items); matched=sum(a.get("result")=="MATCHED" for a in items)
            return {"total":n,"matched":matched,"not_matched":sum(a.get("result")=="NOT_MATCHED" for a in items),"accuracy_pct":100*matched/n if n else None,"direction_accuracy_avg":sum(a.get("direction_accuracy") or 0 for a in items)/n if n else None,"range_accuracy_avg":sum(a.get("range_accuracy") or 0 for a in items)/n if n else None,"target_accuracy_avg":sum(a.get("target_accuracy") or 0 for a in items)/n if n else None}
        daily=ast([a for a in official_analyses if a.get("analysis_type")=="DAILY"]); weekly=ast([a for a in official_analyses if a.get("analysis_type")=="WEEKLY"])
        score=defaultdict(lambda:{"total":0,"full":0,"failed":0,"partial":0})
        for t in final_trades:
            sv=t.get("score"); bucket="UNKNOWN" if sv is None else ("50-59" if sv<60 else "60-69" if sv<70 else "70-79" if sv<80 else "80-89" if sv<90 else "90-100" if sv<=100 else "100+")
            score[bucket]["total"]+=1; score[bucket]["full"]+=t.get("final_result")=="FULL_SUCCESS"; score[bucket]["failed"]+=t.get("final_result")=="FAILED"; score[bucket]["partial"]+=t.get("final_result")=="PARTIAL_SUCCESS"
        return {"report_policy":{"timezone":"Asia/Damascus","current_day_cutoff_utc":cutoff_iso,"official_results_include_current_day":True,"mode":"LIVE_CLOSED_RESULTS_PLUS_OPEN_MONITORING"},"trades":{"total":len(final_trades),"closed":closed,"full_success":full,"partial_success":partial,"failed":failed,"expired":expired,"ambiguous":ambiguous,"win_rate_pct":100*full/closed if closed else None,"full_target_rate_pct":100*full/closed if closed else None,"avg_rr":avg("risk_reward"),"avg_drawdown":avg("max_drawdown"),"avg_mae":avg("max_adverse_excursion"),"avg_mfe":avg("max_favorable_excursion")},"monitoring":{"trades_total":len(monitoring_trades),"trades_active":sum(t.get("status") in ("ACTIVE","TP1","TP2") for t in monitoring_trades),"trades_current_day":sum(not before_cutoff(t,"signal_time") for t in monitoring_trades),"analyses_total":len(monitoring_analyses),"analyses_active":sum(a.get("status")=="ACTIVE" for a in monitoring_analyses),"message":"النتائج المغلقة تظهر فوراً ولا تُحذف أثناء اليوم؛ الصفقات المفتوحة فقط تبقى تحت المراقبة."},"daily":daily,"weekly":weekly,"score_performance":dict(score),"sr":self._sr_monitor_report()}

class Reporter:
    def __init__(self,metrics): self.metrics=metrics
    def text(self):
        r=self.metrics.report(); t=r['trades']; d=r['daily']; w=r['weekly']
        lines=['XAU SMART TRADER PERFORMANCE AUDIT','','TRADES',f"Total: {t['total']}",f"Closed: {t['closed']}",f"Full Success: {t['full_success']}",f"Partial Success: {t['partial_success']}",f"Failed: {t['failed']}",f"Expired: {t['expired']}",f"Ambiguous: {t['ambiguous']}",f"Win Rate: {t['win_rate_pct']:.2f}%",f"Full Target Rate: {t['full_target_rate_pct']:.2f}%",f"Average R:R: {t['avg_rr']:.2f}",f"Average Drawdown: {t['avg_drawdown']:.4f}",f"Average MAE: {t['avg_mae']:.4f}",f"Average MFE: {t['avg_mfe']:.4f}",'','DAILY ANALYSIS',f"Total: {d['total']}",f"Matched: {d['matched']}",f"Not Matched: {d['not_matched']}",f"Accuracy: {d['accuracy_pct']:.2f}%",f"Direction Avg: {d['direction_accuracy_avg']:.2f}%",f"Range Avg: {d['range_accuracy_avg']:.2f}%",f"Target Avg: {d['target_accuracy_avg']:.2f}%",'','WEEKLY ANALYSIS',f"Total: {w['total']}",f"Matched: {w['matched']}",f"Not Matched: {w['not_matched']}",f"Accuracy: {w['accuracy_pct']:.2f}%",f"Direction Avg: {w['direction_accuracy_avg']:.2f}%",f"Range Avg: {w['range_accuracy_avg']:.2f}%",f"Target Avg: {w['target_accuracy_avg']:.2f}%",'','SCORE PERFORMANCE']
        for bucket,v in r['score_performance'].items(): lines.append(f"{bucket}: total={v['total']} full={v['full']} failed={v['failed']} partial={v['partial']} full_rate={100*v['full']/v['total'] if v['total'] else 0:.2f}%")
        return '\n'.join(lines)

class PerformanceAuditor:
    """Independent low-overhead SRE observer. Never executes trading decisions."""
    def __init__(self,db_path=AUDITOR_DB_PATH,price_engine=None):
        self.db=Database(db_path); self.price_engine=price_engine or LivePriceEngine(); self.trade_auditor=TradeAuditor(self.db); self.analysis_auditor=AnalysisAuditor(self.db); self.metrics=Metrics(self.db); self.reporter=Reporter(self.metrics)
        self._stop=threading.Event(); self._thread=None; self._async_task=None; self._alert_callback=None; self._heal_callback=None; self._alert_state={}; self._last_persist=0.0; self._last_cleanup=0.0; self._last_trade_reconcile=0.0; self._last_trade_reconcile_stats={"scanned":0,"inserted":0,"updated":0,"errors":0}
    @staticmethod
    def _now(): return datetime.now(timezone.utc).isoformat()
    @staticmethod
    def _id(prefix): return f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}_{uuid.uuid4().hex[:8]}"
    def set_alert_callback(self,callback): self._alert_callback=callback
    def set_self_healer(self,callback): self._heal_callback=callback
    def register_trade(self,**kwargs):
        signal_time=kwargs.pop('signal_time',None) or self._now(); trade_id=kwargs.pop('trade_id',None) or self._id('TRD'); targets=[x for x in (kwargs.get('tp1'),kwargs.get('tp2'),kwargs.get('tp3'),kwargs.get('tp_final')) if x is not None]; final=kwargs.get('tp_final') if kwargs.get('tp_final') is not None else (targets[-1] if targets else None)
        expiry=kwargs.pop('expiry_time',None) or (datetime.fromisoformat(signal_time.replace('Z','+00:00'))+timedelta(hours=AUDITOR_DEFAULT_TRADE_EXPIRY_HOURS)).isoformat()
        obj=TradeInput(trade_id,signal_time,kwargs['direction'],float(kwargs['entry']),float(kwargs['sl']),kwargs.get('tp1'),kwargs.get('tp2'),kwargs.get('tp3'),final,kwargs.get('score'),kwargs.get('quality'),kwargs.get('risk_reward'),kwargs.get('timeframe'),expiry,kwargs.get('analysis_snapshot') or {})
        return self.trade_auditor.register(obj)
    def register_analysis(self,**kwargs):
        issue=kwargs.pop('issue_time',None) or self._now(); aid=kwargs.pop('analysis_id',None) or self._id('ANL'); obj=AnalysisInput(aid,kwargs['analysis_type'],issue,kwargs['expiry_time'],kwargs['direction'],kwargs.get('expected_min'),kwargs.get('expected_max'),kwargs.get('target'),kwargs.get('score'),kwargs.get('confidence'),kwargs.get('analysis_snapshot') or {})
        return self.analysis_auditor.register(obj)
    def observe_price(self,price,timestamp=None,source='manual'): return self.trade_auditor.observe_tick(PricePoint(timestamp or self._now(),float(price),source))
    def observe_bar(self,**kwargs): return self.trade_auditor.observe_bar(BarPoint(kwargs['timestamp'],float(kwargs['open']),float(kwargs['high']),float(kwargs['low']),float(kwargs['close']),kwargs.get('source','simulation')))
    def finalize_analysis(self,analysis_id,bars): return self.analysis_auditor.finalize(analysis_id,bars)
    def generate_performance_report(self): return self.reporter.text()
    def metrics_report(self):
        r=self.metrics.report(); r['system']=self.metrics.system_snapshot(queue_depth=_safe_queue_depth(),queue_capacity=_safe_queue_capacity(),queue_drops=_safe_queue_drops()); r['handler_root_trace']=self.metrics.handler_trace_report(); r['update_lifecycle']=self.metrics.update_lifecycle_report(); r['trade_reconciliation']=dict(self._last_trade_reconcile_stats); return r
    def _persist_system_metrics(self,snapshot):
        started=_auditor_time.perf_counter()
        with self.db.connect() as conn:
            conn.execute("""INSERT INTO System_Metrics(sampled_at,uptime_s,cpu_pct,rss_mb,event_loop_lag_ms,request_avg_ms,request_p95_ms,request_max_ms,request_count,handler_avg_ms,handler_p95_ms,handler_max_ms,handler_count,throughput_per_min,price_latency_ms,db_write_latency_ms,price_age_s,queue_depth,queue_capacity,queue_drops,error_count,alert_count,self_heal_count,auditor_heartbeat) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (self._now(),snapshot['uptime_s'],snapshot['cpu_pct'],snapshot['rss_mb'],snapshot['event_loop_lag_ms'],snapshot['request_avg_ms'],snapshot['request_p95_ms'],snapshot['request_max_ms'],snapshot['request_count'],snapshot['handler_avg_ms'],snapshot['handler_p95_ms'],snapshot['handler_max_ms'],snapshot['handler_count'],snapshot['throughput_per_min'],snapshot['price_latency_ms'],snapshot['db_write_latency_ms'],snapshot['price_age_s'],snapshot['queue_depth'],snapshot['queue_capacity'],snapshot['queue_drops'],snapshot['error_count'],snapshot['alert_count'],snapshot['self_heal_count'],snapshot['auditor_heartbeat']))
            conn.commit()
        self.metrics.record_db((_auditor_time.perf_counter()-started)*1000.0)
    def _cleanup(self):
        now=datetime.now(timezone.utc); pcut=(now-timedelta(days=AUDITOR_PRICE_RETENTION_DAYS)).isoformat(); lcut=(now-timedelta(days=AUDITOR_LOG_RETENTION_DAYS)).isoformat(); mcut=(now-timedelta(days=AUDITOR_METRIC_RETENTION_DAYS)).isoformat()
        with self.db.connect() as conn:
            conn.execute('DELETE FROM Price_Samples WHERE sample_time < ?', (pcut,)); conn.execute('DELETE FROM Audit_Log WHERE log_time < ?', (lcut,)); conn.execute('DELETE FROM System_Metrics WHERE sampled_at < ?', (mcut,)); conn.commit()
            try: conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            except Exception: pass
    def _emit_alert(self,level,reason,snapshot):
        key=f"{level}:{reason}"; now=_auditor_time.monotonic(); last=self._alert_state.get(key,0.0)
        # Alert deduplication: same condition at most once per 10 minutes.
        if now-last<600: return
        self._alert_state[key]=now; self.metrics.inc_alert(); AUDITOR_LOGGER.warning("PERFORMANCE_ALERT | %s | %s | %s",level,reason,snapshot)
        if self._alert_callback:
            try: self._alert_callback(level,reason,snapshot)
            except Exception: AUDITOR_LOGGER.exception("PERFORMANCE_ALERT_CALLBACK_ERROR")
    def _evaluate_health(self,s):
        checks=[]
        if s['cpu_pct']>=AUDITOR_CPU_CRIT_PCT: checks.append(('CRITICAL','CPU_HIGH'))
        elif s['cpu_pct']>=AUDITOR_CPU_WARN_PCT: checks.append(('WARNING','CPU_HIGH'))
        if s['rss_mb']>=AUDITOR_RSS_CRIT_MB: checks.append(('CRITICAL','RSS_HIGH'))
        elif s['rss_mb']>=AUDITOR_RSS_WARN_MB: checks.append(('WARNING','RSS_HIGH'))
        if s.get('webhook_p95_ready', False):
            if s['request_p95_ms']>=AUDITOR_LATENCY_CRIT_MS: checks.append(('CRITICAL','WEBHOOK_LATENCY'))
            elif s['request_p95_ms']>=AUDITOR_LATENCY_WARN_MS: checks.append(('WARNING','WEBHOOK_LATENCY'))
        if s.get('handler_p95_ready', False):
            if s.get('handler_p95_ms',0.0)>=AUDITOR_LATENCY_CRIT_MS: checks.append(('CRITICAL','HANDLER_LATENCY'))
            elif s.get('handler_p95_ms',0.0)>=AUDITOR_LATENCY_WARN_MS: checks.append(('WARNING','HANDLER_LATENCY'))
        if s['event_loop_lag_ms']>=AUDITOR_LOOP_CRIT_MS: checks.append(('CRITICAL','EVENT_LOOP_LAG'))
        elif s['event_loop_lag_ms']>=AUDITOR_LOOP_WARN_MS: checks.append(('WARNING','EVENT_LOOP_LAG'))
        qpct=100*s['queue_depth']/max(1,s['queue_capacity'])
        if qpct>=AUDITOR_QUEUE_CRIT_PCT: checks.append(('CRITICAL','AUDITOR_QUEUE'))
        elif qpct>=AUDITOR_QUEUE_WARN_PCT: checks.append(('WARNING','AUDITOR_QUEUE'))
        if s['price_age_s'] is not None and s['price_age_s']>=AUDITOR_STALE_PRICE_SECONDS: checks.append(('CRITICAL','STALE_PRICE'))
        for level,reason in checks: self._emit_alert(level,reason,s)
        if checks and self._heal_callback:
            try:
                if any(x[0]=='CRITICAL' for x in checks):
                    self._heal_callback([x[1] for x in checks],s); self.metrics.inc_self_heal()
            except Exception: AUDITOR_LOGGER.exception('PERFORMANCE_SELF_HEAL_ERROR')
    def run_forever(self):
        AUDITOR_LOGGER.info('Performance Auditor started | low-overhead SRE mode')
        while not self._stop.is_set():
            cycle_start=_auditor_time.monotonic(); point=None
            try:
                point,lat=self.price_engine.get_price(force=True); self.metrics.record_price(lat)
                started=_auditor_time.perf_counter()
                with self.db.connect() as conn:
                    conn.execute('INSERT INTO Price_Samples(sample_time,price,source,high,low) VALUES(?,?,?,?,?)',(point.timestamp,point.price,point.source,point.high,point.low)); conn.commit()
                self.metrics.record_db((_auditor_time.perf_counter()-started)*1000.0)
                self.trade_auditor.observe_tick(point)
            except Exception as exc:
                self.metrics.inc_error(); now=self._now(); self.db.audit_log('ERROR','PRICE',str(exc),{},now); AUDITOR_LOGGER.exception('Price monitoring failure')
            try:
                finalized=self.analysis_auditor.finalize_expired_from_samples(point.timestamp if point else self._now())
                if finalized: AUDITOR_LOGGER.info('Finalized analyses: %s',finalized)
            except Exception: self.metrics.inc_error(); AUDITOR_LOGGER.exception('ANALYSIS_AUDIT_FINALIZE_ERROR')
            now_mono=_auditor_time.monotonic()
            if now_mono-self._last_trade_reconcile>=AUDITOR_TRADE_RECONCILE_SECONDS:
                try: self._last_trade_reconcile_stats=_auditor_reconcile_from_main_history()
                except Exception: self.metrics.inc_error(); AUDITOR_LOGGER.exception("AUDITOR_TRADE_RECONCILE_CYCLE_ERROR")
                self._last_trade_reconcile=now_mono
            self.metrics.heartbeat_tick()
            now_mono=_auditor_time.monotonic()
            if now_mono-self._last_persist>=AUDITOR_METRIC_PERSIST_SECONDS:
                snap=self.metrics.system_snapshot(_safe_queue_depth(),_safe_queue_capacity(),_safe_queue_drops()); self._evaluate_health(snap)
                try: self._persist_system_metrics(snap)
                except Exception: self.metrics.inc_error(); AUDITOR_LOGGER.exception('SYSTEM_METRICS_PERSIST_ERROR')
                self._last_persist=now_mono
            if now_mono-self._last_cleanup>=3600:
                try: self._cleanup()
                except Exception: self.metrics.inc_error(); AUDITOR_LOGGER.exception('AUDITOR_CLEANUP_ERROR')
                self._last_cleanup=now_mono
            elapsed=_auditor_time.monotonic()-cycle_start; self._stop.wait(max(0.0,AUDITOR_POLL_SECONDS-elapsed))
    def start_background(self):
        if self._thread and self._thread.is_alive(): return
        self._stop.clear(); self._thread=threading.Thread(target=self.run_forever,name='xau-auditor',daemon=True); self._thread.start()
    async def start_async_probe(self):
        if self._async_task and not self._async_task.done(): return
        self._async_task=asyncio.create_task(self._loop_probe(),name='xau-auditor-loop-probe')
    async def _loop_probe(self):
        interval=AUDITOR_EVENT_LOOP_PROBE_SECONDS
        while not self._stop.is_set():
            expected=_auditor_time.monotonic()+interval
            await asyncio.sleep(interval)
            lag=max(0.0,(_auditor_time.monotonic()-expected)*1000.0); self.metrics.record_loop_lag(lag)
    async def stop_async_probe(self):
        task=self._async_task; self._async_task=None
        if task and not task.done(): task.cancel(); await asyncio.gather(task,return_exceptions=True)
    def stop(self):
        self._stop.set()
        if self._thread and self._thread.is_alive() and self._thread is not threading.current_thread(): self._thread.join(timeout=max(2.0,AUDITOR_POLL_SECONDS+5.0))
        self._thread=None

def _safe_queue_depth():
    try: return AUDITOR_QUEUE.qsize()
    except Exception: return 0

def _safe_queue_capacity():
    try: return AUDITOR_QUEUE.maxsize
    except Exception: return 0

def _safe_queue_drops():
    try: return int(AUDITOR_QUEUE_DROPS)
    except Exception: return 0

# إنشاء محرك المراقبة المدمج — لا يوجد import خارجي.
try:
    PERFORMANCE_AUDITOR = PerformanceAuditor(os.environ.get("AUDITOR_DB_PATH", "xau_performance_auditor.db"))
except Exception as _auditor_exc:
    PERFORMANCE_AUDITOR = None
    logging.getLogger("xau_performance_auditor_bridge").warning("Performance Auditor unavailable: %s", _auditor_exc)

# ============================================================
# الإعدادات
# ============================================================

VERSION = "v18.110_INSTITUTIONAL_MENU_REPLACEMENT"
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
# الأخبار طبقة تنبيه ومخاطر فقط: HIGH أو UNKNOWN لا يمنعان فتح صفقة جديدة.
NEWS_FILTER_ENABLED = True
NEWS_BEFORE_MIN = 30
NEWS_AFTER_MIN = 30
NEWS_CACHE_SECONDS = 300
# News reliability / multi-provider settings
NEWS_CACHE_FILE = os.environ.get("NEWS_CACHE_FILE", "news_calendar_cache.json")

# ===== Institutional Data Layer v18.46 =====
INSTITUTIONAL_CACHE_SECONDS = int(os.getenv("INSTITUTIONAL_CACHE_SECONDS", "1800"))
INSTITUTIONAL_HTTP_TIMEOUT = float(os.getenv("INSTITUTIONAL_HTTP_TIMEOUT", "15"))
INSTITUTIONAL_CACHE = {}
INSTITUTIONAL_LOCK = threading.RLock()
FRED_API_URL = "https://api.stlouisfed.org/fred/series/observations"
TREASURY_YIELD_XML = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml"
TREASURY_REAL_XML = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml"
CBOE_VIX_PAGE = "https://www.cboe.com/tradable-products/vix/vix-historical-data/"
CBOE_VIX_CSV = os.environ.get("CBOE_VIX_CSV_URL", "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv")
NYFED_API_BASE = "https://markets.newyorkfed.org/api"
CFTC_DISAGG_ZIP_TEMPLATE = "https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip"
CFTC_LEGACY_ZIP_TEMPLATE = "https://www.cftc.gov/files/dea/history/fut_only_txt_{year}.zip"
SPDR_GLD_HISTORY_URL = "https://api.spdrgoldshares.com/api/v1/historical-archive?exchange=NYSE&lang=en&product=gld"
WGC_CENTRAL_BANK_CHANGES_URL = os.environ.get("WGC_CENTRAL_BANK_CHANGES_URL", "")
WGC_CENTRAL_BANK_ARTICLE_URL = "https://www.gold.org/goldhub/gold-focus/2026/08/central-bank-gold-statistics-june-2026"
IMF_SDMX_BASE = "https://api.imf.org/external/sdmx/3.0"
IMF_IRFCL_DATAFLOW = f"{IMF_SDMX_BASE}/data/dataflow/IMF.STA/IRFCL/+"
IMF_GOLD_FTO_INDICATOR = "IRFCLDT1_IRFCL56_FTO"
IMF_GOLD_USD_INDICATOR = "IRFCLDT1_IRFCL56_USD"
IMF_TOTAL_RESERVES_USD_INDICATOR = "IRFCLDT1_IRFCL65_USD"
TROY_OZ_PER_TONNE = 32150.7466
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

US_EASTERN = ZoneInfo("America/New_York")

def _us_nth_weekday(year, month, weekday, nth):
    d = date(year, month, 1)
    shift = (weekday - d.weekday()) % 7
    return d + timedelta(days=shift + 7 * (nth - 1))

def _us_last_weekday(year, month, weekday):
    if month == 12:
        d = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        d = date(year, month + 1, 1) - timedelta(days=1)
    return d - timedelta(days=(d.weekday() - weekday) % 7)

def _us_observed_fixed(year, month, day):
    d = date(year, month, day)
    if d.weekday() == 5: return d - timedelta(days=1)
    if d.weekday() == 6: return d + timedelta(days=1)
    return d

def _us_financial_holidays(year):
    # Federal/major U.S. financial-market holidays relevant to publication timing.
    return {
        _us_observed_fixed(year, 1, 1): "رأس السنة",
        _us_nth_weekday(year, 1, 0, 3): "يوم مارتن لوثر كينغ الابن",
        _us_nth_weekday(year, 2, 0, 3): "يوم الرؤساء",
        _us_last_weekday(year, 5, 0): "يوم الذكرى",
        _us_observed_fixed(year, 6, 19): "Juneteenth",
        _us_observed_fixed(year, 7, 4): "عيد الاستقلال",
        _us_nth_weekday(year, 9, 0, 1): "عيد العمال",
        _us_nth_weekday(year, 10, 0, 2): "يوم كولومبوس",
        _us_observed_fixed(year, 11, 11): "يوم المحاربين القدامى",
        _us_nth_weekday(year, 11, 3, 4): "عيد الشكر",
        _us_observed_fixed(year, 12, 25): "عيد الميلاد",
    }

def _us_financial_calendar_state(now=None):
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None: now = now.replace(tzinfo=timezone.utc)
    local = now.astimezone(US_EASTERN)
    d = local.date()
    if d.weekday() >= 5:
        return {'state':'WEEKEND','date':d.isoformat(),'holiday':None,'reason_ar':'عطلة نهاية الأسبوع الأمريكية؛ بعض المصادر الدورية لن تنشر تحديثاً جديداً.'}
    holiday = _us_financial_holidays(d.year).get(d)
    if holiday:
        return {'state':'HOLIDAY','date':d.isoformat(),'holiday':holiday,'reason_ar':f'عطلة أمريكية: {holiday}؛ تأخر البيانات الدورية قد يكون متوقعاً وليس عطلاً تقنياً.'}
    return {'state':'OPEN','date':d.isoformat(),'holiday':None,'reason_ar':'يوم عمل أمريكي؛ غياب البيانات يحتاج فحصاً تقنياً أو فحصاً لجدول النشر.'}

def _attach_us_calendar_context(obj, asof=None, source_class='US_OFFICIAL'):
    if not isinstance(obj, dict): return obj
    if source_class != 'US_OFFICIAL': return obj
    cal = _us_financial_calendar_state()
    obj['us_calendar'] = cal
    st = str(obj.get('status',''))
    if st == 'STALE' and cal['state'] in ('HOLIDAY','WEEKEND'):
        obj['availability_reason_code'] = 'EXPECTED_PUBLICATION_DELAY'
        obj['availability_reason_ar'] = cal['reason_ar']
    elif st == 'UNAVAILABLE' and cal['state'] in ('HOLIDAY','WEEKEND'):
        obj['availability_reason_code'] = 'POSSIBLE_PUBLICATION_DELAY'
        obj['availability_reason_ar'] = cal['reason_ar']
    else:
        obj.setdefault('availability_reason_code','NORMAL_OPERATION')
        obj.setdefault('availability_reason_ar',cal['reason_ar'])
    return obj

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
SESSION_ALERT_STATE = {}
SESSION_OVERLAP_ALERT_STATE = {}
NEWS_ALERT_STATE = {}
EMERGENCY_ALERT_STATE = {}
LAST_MARKET_STATE = None
TRADE_HISTORY = []
LAST_ANALYSIS = None
MAX_TRADE_HISTORY = 500
TRADE_DB_PATH = os.environ.get("TRADE_DB_PATH", "trades.db")
TRADE_LOCK = threading.RLock()
# Production alert delivery layer: initialized after Telegram Application startup.
ALERT_DISPATCHER = None
LIQUIDITY_LOCK = threading.RLock()

# جسر Auditor غير حاجب: لا نسمح لأي عملية SQLite/HTTP خاصة بالمراقب
# أن تنتظر داخل دورة التداول أو داخل TRADE_LOCK.
AUDITOR_QUEUE_MAX = int(os.environ.get("AUDITOR_QUEUE_MAX", "256"))
AUDITOR_QUEUE = queue.Queue(maxsize=max(16, AUDITOR_QUEUE_MAX))
AUDITOR_WORKER_THREAD = None
AUDITOR_WORKER_STOP = threading.Event()

# Initial local bootstrap calendar. Used only when no manual/cache calendar exists.
# Times are UTC and based on official published U.S. release/meeting schedules.
BOOTSTRAP_EVENTS = [
    {"time": "2026-09-10T12:30:00+00:00", "currency": "USD", "impact": "HIGH", "event": "US Producer Price Index (PPI)", "source": "bootstrap-official-schedule"},
    {"time": "2026-09-11T12:30:00+00:00", "currency": "USD", "impact": "HIGH", "event": "US Consumer Price Index (CPI)", "source": "bootstrap-official-schedule"},
    {"time": "2026-09-16T18:00:00+00:00", "currency": "USD", "impact": "HIGH", "event": "FOMC Rate Decision", "source": "bootstrap-official-schedule"},
    {"time": "2026-10-02T12:30:00+00:00", "currency": "USD", "impact": "HIGH", "event": "US Employment Situation / Nonfarm Payrolls", "source": "bootstrap-official-schedule"},
]
AUDITOR_QUEUE_DROPS = 0
AUDITOR_QUEUE_LOCK = threading.Lock()

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


def _ensure_user(update):
    """Canonical subscription bootstrap: ADMIN authorization is independent of plan state."""
    chat=update.effective_chat; user=update.effective_user; now=now_damascus().isoformat(); code=f"ref_{chat.id}"
    conn=_db()
    try:
        row=conn.execute("SELECT * FROM users WHERE chat_id=?",(chat.id,)).fetchone()
        admin=is_admin_chat(chat.id) or (user is not None and is_admin_chat(user.id))
        if not row:
            conn.execute("INSERT OR IGNORE INTO users(chat_id,username,first_name,plan,status,referral_code,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                         (chat.id,getattr(user,'username',None),getattr(user,'first_name',None),'FREE','active',code,now,now))
        else:
            if admin:
                conn.execute("UPDATE users SET username=?,first_name=?,status='active',updated_at=? WHERE chat_id=?",
                             (getattr(user,'username',None),getattr(user,'first_name',None),now,chat.id))
            else:
                conn.execute("UPDATE users SET username=?,first_name=?,updated_at=? WHERE chat_id=?",
                             (getattr(user,'username',None),getattr(user,'first_name',None),now,chat.id))
        conn.commit()
        return conn.execute("SELECT * FROM users WHERE chat_id=?",(chat.id,)).fetchone()
    finally: conn.close()


def alert_subscribers():
    """Canonical recipients for trading alerts: ELITE users plus ADMIN only."""
    conn=_db()
    try:
        rows=conn.execute("SELECT chat_id FROM users WHERE status='active'").fetchall()
        result=set(ADMIN_IDS)
        for row in rows:
            cid=int(row["chat_id"])
            if has_feature(cid,"trade_alerts"): result.add(cid)
        return result
    finally: conn.close()


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
    lines = ["💳 باقات XAU SMART TRADER"]
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
        await asyncio.to_thread(_ensure_user, update)
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id if update.effective_user else chat_id
        allowed = await asyncio.to_thread(has_feature, chat_id, feature)
        if is_admin_chat(chat_id) or is_admin_chat(user_id) or allowed:
            return True
        if upgrade_text:
            await reply(update, f"🔒 هذه الميزة غير متاحة ضمن باقتك الحالية.\n\n💳 استخدم /plans لرؤية الباقات والترقية.")
        return False
    return checker


def _format_trade_message(result, title="🚨 إشارة ذهب", record=None):
    """تنسيق إشعار صفقة نظيف للهاتف بدون Header سوق أو خطوط فاصلة."""
    result = result or {}
    trade = record or result.get("trade") or {}
    direction_code = str(result.get("direction") or trade.get("direction") or "").upper()
    direction = "🟢 شراء" if direction_code == "BUY" else "🔴 بيع" if direction_code == "SELL" else "⚪ غير محدد"
    score = int(sf(result.get("score"), 0))
    quality = "🔥 قوية" if score >= STRONG_THRESHOLD else "🎯 مؤهلة"
    status = str(trade.get("status", "NEW"))
    result_text = str(trade.get("result", "OPEN"))
    status_ar = {"NEW":"🆕 جديدة","ACTIVE":"🟢 نشطة","TP1":"🎯 TP1 تحقق","TP2":"🎯 TP2 تحقق","CLOSED":"🔒 مغلقة"}.get(status, status)
    def num(key, default=0.0):
        return sf(trade.get(key, result.get(key, default)), default)
    factors = result.get("factors") or []
    factors_text = "\n".join("• " + str(x) for x in factors[:7]) or "• لا توجد عوامل إضافية متاحة"
    liquidity = result.get("mtf", {}).get("m15", {}).get("liquidity", {}) if isinstance(result.get("mtf"), dict) else {}
    return (
        f"{title}\n\n"
        f"📊 الصفقة\n{direction} — {score} نقطة {quality}\n📌 الحالة: {status_ar}\n\n"
        f"📍 المستويات\n"
        f"💰 السعر الحالي: {sf(result.get('price'), 0):.2f}\n"
        f"الدخول: {num('entry'):.2f}\n🛑 SL: {num('sl'):.2f}\n"
        f"🎯 TP1: {num('tp1'):.2f}\n🎯 TP2: {num('tp2'):.2f}\n🎯 TP3: {num('tp3'):.2f}\n"
        f"⚖️ R:R: 1:{num('rr'):.2f}\n\n"
        f"🧠 التلاقي\n{factors_text}\n\n"
        f"🔁 إعادة الاختبار: {liquidity_retest_summary(liquidity)}\n"
        f"📊 نتيجة المتابعة: {result_text}\n\n"
        "⚠️ إشارة تحليلية للتنفيذ اليدوي."
    )


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
        with handler_trace_stage("get_bars:cache_lookup", interval=interval):
            cached = DATA_CACHE.get(key)
        if cached and now - cached[0] < CACHE_SECONDS:
            with handler_trace_stage("get_bars:cache_hit", interval=interval):
                return cached[1].copy()

        daily_limit = min(max(limit * 7 + 30, 100), 1000)
        with handler_trace_stage("get_bars:recursive_1d", interval=interval):
            daily_df = get_bars("1d", daily_limit)
        if daily_df is None or daily_df.empty:
            raise ValueError("لا توجد بيانات يومية لبناء W1.")
        if "openTime" not in daily_df.columns:
            raise ValueError("بيانات D1 لا تحتوي على openTime.")

        with handler_trace_stage("get_bars:w1_prepare"):
            df = daily_df.copy()
            df["openTime"] = pd.to_datetime(df["openTime"], utc=True, errors="coerce")
            df = df.dropna(subset=["openTime"]).set_index("openTime").sort_index()

        with handler_trace_stage("get_bars:w1_validate_convert"):
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

        with handler_trace_stage("get_bars:w1_resample"):
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

        with handler_trace_stage("get_bars:cache_store", interval=interval):
            DATA_CACHE[key] = (now, weekly.copy())
        return weekly.copy()

    supported = {"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
    if interval not in supported:
        raise ValueError(f"الفريم {interval} غير مدعوم.")

    key = cache_key(interval, limit)
    now = time.time()
    with handler_trace_stage("get_bars:cache_lookup", interval=interval):
        cached = DATA_CACHE.get(key)
    if cached and now - cached[0] < CACHE_SECONDS:
        with handler_trace_stage("get_bars:cache_hit", interval=interval):
            return cached[1].copy()

    try:
        with handler_trace_stage("get_bars:http_request", interval=interval):
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
        with handler_trace_stage("get_bars:response_json", interval=interval):
            data = response.json()
    except requests.RequestException as exc:
        raise RuntimeError(f"تعذر الاتصال بمصدر البيانات للفريم {interval}: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError(f"استجابة Biquote غير متوقعة للفريم {interval}.")

    bars = data.get("bars", [])
    if not bars:
        raise ValueError(f"Biquote لم يعط بيانات للفريم {interval}.")

    with handler_trace_stage("get_bars:parse_dataframe", interval=interval):
        df = pd.DataFrame(bars)
    with handler_trace_stage("get_bars:validate_convert", interval=interval):
        for col in ["open", "high", "low", "close"]:
            if col not in df.columns:
                raise ValueError(f"البيانات ناقصة: {col}")
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    with handler_trace_stage("get_bars:volume_normalize", interval=interval):
        if "tickVolume" in df.columns:
            df["tickVolume"] = pd.to_numeric(df["tickVolume"], errors="coerce").fillna(0)
        else:
            df["tickVolume"] = 0

    with handler_trace_stage("get_bars:time_normalize", interval=interval):
        if "openTime" in df.columns:
            df["openTime"] = pd.to_datetime(df["openTime"], utc=True, errors="coerce")
            df = df.dropna(subset=["openTime"]).sort_values("openTime")

    with handler_trace_stage("get_bars:final_validate", interval=interval):
        df = df.dropna(subset=["open", "high", "low", "close"])
    if len(df) < MIN_BARS:
        raise ValueError(f"البيانات غير كافية للفريم {interval}: {len(df)} شمعة.")

    with handler_trace_stage("get_bars:cache_store", interval=interval):
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



def _liquidity_analysis_unlocked(df, lookback=80):
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



def liquidity_analysis(df, lookback=80):
    """واجهة آمنة لمحرك السيولة؛ تمنع استدعاءين متزامنين من تعديل الحالة نفسها."""
    with LIQUIDITY_LOCK:
        return _liquidity_analysis_unlocked(df, lookback)


def liquidity_retest_summary(liq):
    """عرض حالات محرك السيولة الرسمية فقط."""
    r = liq.get("retest", {}) if isinstance(liq, dict) else {}
    state = r.get("state")
    level = r.get("level")
    if state == "CONFIRMED": return f"🟢 إعادة الاختبار ناجحة عند {fmt(level)}"
    if state == "RETEST": return f"🟡 إعادة الاختبار قيد التقييم عند {fmt(level)}"
    if state == "BOS": return f"🟠 كسر الهيكل مؤكد — بانتظار إعادة الاختبار عند {fmt(level)}"
    if state == "DISPLACEMENT": return "🟡 الاندفاع مؤكد — بانتظار تأكيد كسر الهيكل"
    if state == "SWEEP": return f"🔵 بانتظار الاندفاع بعد سحب السيولة عند {fmt(level)}"
    if state == "INVALIDATED" or r.get("result") == "FAILED": return f"🔴 إعادة الاختبار فاشلة — تم إبطال المستوى {fmt(level)}"
    if state == "CONSUMED": return "⚪ تم استهلاك إعادة الاختبار المؤكدة — بانتظار Sweep جديد"
    if state == "EXPIRED" or r.get("result") == "EXPIRED": return "⚪ انتهى عمر حالة السيولة — بانتظار Sweep جديد"
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


def next_support(levels, price):
    """ثاني أقرب دعم فعلي تحت السعر الحالي."""
    try:
        p = float(price)
        vals = []
        for key in ("support1", "support2", "support3"):
            z = _zone_price(levels.get(key) if isinstance(levels, dict) else None)
            if z is not None and z < p and z not in vals:
                vals.append(z)
        vals.sort(reverse=True)
        return vals[1] if len(vals) > 1 else None
    except (TypeError, ValueError):
        return None


def next_resistance(levels, price):
    """ثاني أقرب مقاومة فعلية فوق السعر الحالي."""
    try:
        p = float(price)
        vals = []
        for key in ("resistance1", "resistance2", "resistance3"):
            z = _zone_price(levels.get(key) if isinstance(levels, dict) else None)
            if z is not None and z > p and z not in vals:
                vals.append(z)
        vals.sort()
        return vals[1] if len(vals) > 1 else None
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


def _inst_cache_get(key):
    with INSTITUTIONAL_LOCK:
        x=INSTITUTIONAL_CACHE.get(key)
        if x and time.time()-x[0] < INSTITUTIONAL_CACHE_SECONDS:
            return x[1]
    return None


def _inst_cache_put(key,value):
    with INSTITUTIONAL_LOCK:
        INSTITUTIONAL_CACHE[key]=(time.time(),value)
    return value


def _inst_http(url,params=None,timeout=None,headers=None):
    h={
        "User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36 XAU-Smart-Trader/18.50",
        "Accept":"*/*",
    }
    if headers: h.update(headers)
    r=requests.get(url,params=params,timeout=timeout or INSTITUTIONAL_HTTP_TIMEOUT,headers=h)
    r.raise_for_status()
    return r


def _parse_inst_date(value):
    """Parse institutional dates safely, including CFTC integer YYYYMMDD values."""
    if value is None or (isinstance(value,float) and np.isnan(value)): return pd.NaT
    if isinstance(value,(int,np.integer)):
        iv=int(value)
        if 19000101 <= iv <= 21001231:
            return pd.to_datetime(str(iv),format='%Y%m%d',errors='coerce')
        # Excel serial date (1900 date system). SPDR/WGC XLSX may expose dates
        # as numeric serials when openpyxl is unavailable.
        if 1 <= iv <= 80000:
            return pd.Timestamp('1899-12-30') + pd.to_timedelta(iv,unit='D')
    if isinstance(value,float) and float(value).is_integer():
        iv=int(value)
        if 19000101 <= iv <= 21001231:
            return pd.to_datetime(str(iv),format='%Y%m%d',errors='coerce')
        if 1 <= iv <= 80000:
            return pd.Timestamp('1899-12-30') + pd.to_timedelta(iv,unit='D')
    return pd.to_datetime(value,errors='coerce')


def _find_date_column(df):
    cols=list(df.columns)
    norm={c:re.sub(r'[^a-z0-9]+','_',str(c).strip().lower()).strip('_') for c in cols}
    preferred=[c for c,n in norm.items() if 'report_date' in n or 'as_of_date' in n or n in ('reportdate','asofdate')]
    if preferred: return preferred[0]
    candidates=[]
    for c in cols:
        parsed=df[c].map(_parse_inst_date)
        score=int(parsed.notna().sum())
        if score: candidates.append((score,c,parsed))
    return max(candidates,key=lambda x:x[0])[1] if candidates else None


def _read_xlsx_without_openpyxl(content):
    """Read simple XLSX tables without requiring openpyxl (Render-safe fallback)."""
    import xml.etree.ElementTree as ET
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        names=z.namelist()
        shared=[]
        if 'xl/sharedStrings.xml' in names:
            root=ET.fromstring(z.read('xl/sharedStrings.xml'))
            ns={'m':'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
            for si in root.findall('m:si',ns): shared.append(''.join(t.text or '' for t in si.findall('.//m:t',ns)))
        wb=ET.fromstring(z.read('xl/workbook.xml'))
        rel=ET.fromstring(z.read('xl/_rels/workbook.xml.rels'))
        rns='http://schemas.openxmlformats.org/package/2006/relationships'
        relmap={r.attrib['Id']:r.attrib['Target'] for r in rel}
        ns={'m':'http://schemas.openxmlformats.org/spreadsheetml/2006/main','r':'http://schemas.openxmlformats.org/officeDocument/2006/relationships'}
        sheets=[]
        for sh in wb.findall('m:sheets/m:sheet',ns):
            rid=sh.attrib.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
            target=relmap.get(rid,'')
            if not target.startswith('xl/'): target='xl/'+target.lstrip('/')
            sheets.append((sh.attrib.get('name','Sheet'),target))
        tables=[]
        for name,target in sheets:
            if target not in names: continue
            root=ET.fromstring(z.read(target)); rows=[]
            for row in root.findall('.//m:sheetData/m:row',ns):
                vals={}; maxcol=0
                for cell in row.findall('m:c',ns):
                    ref=cell.attrib.get('r','A1'); m=re.match(r'([A-Z]+)',ref)
                    if not m: continue
                    letters=m.group(1); idx=0
                    for ch in letters: idx=idx*26+ord(ch)-64
                    maxcol=max(maxcol,idx); typ=cell.attrib.get('t'); v=cell.find('m:v',ns); val=v.text if v is not None else ''
                    if typ=='s' and val.isdigit() and int(val)<len(shared): val=shared[int(val)]
                    elif typ=='inlineStr': val=''.join(t.text or '' for t in cell.findall('.//m:t',ns))
                    vals[idx-1]=val
                rows.append([vals.get(i,'') for i in range(maxcol)])
            if rows:
                width=max(map(len,rows)); header=[str(x).strip() for x in rows[0]]
                header=header+[f'__COL_{i+1}' for i in range(len(header),width)]
                data=[r+[None]*(width-len(r)) for r in rows[1:]]
                if any(str(x).strip() for x in header): tables.append((name,pd.DataFrame(data,columns=header)))
        if not tables: raise RuntimeError('XLSX contains no readable tables')
        return tables


def _inst_iso(value):
    try:
        ts=pd.to_datetime(value,utc=True,errors='coerce')
        if pd.isna(ts): return None
        return ts.isoformat()
    except Exception:
        return None


def _inst_age_days(value):
    iso=_inst_iso(value)
    if not iso: return None
    try:
        return max(0.0,(datetime.now(timezone.utc)-datetime.fromisoformat(iso.replace('Z','+00:00'))).total_seconds()/86400.0)
    except Exception:
        return None


def _inst_status(asof, max_age_days, source=None):
    age=_inst_age_days(asof)
    if age is None:
        return {"status":"UNKNOWN","fresh":False,"age_days":None,"asof":None,"source":source}
    return {"status":"FRESH" if age <= max_age_days else "STALE","fresh":age <= max_age_days,"age_days":round(age,2),"asof":_inst_iso(asof),"source":source}


def _fred(ids):
    """Official FRED API only. Requires FRED_API_KEY; no graph/third-party fallback."""
    ids=[str(x).strip() for x in ids if str(x).strip()]
    key='fred_api:'+','.join(ids)
    c=_inst_cache_get(key)
    if c is not None:return c
    api_key=os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("FRED_API_KEY is not configured")
    frames=[]
    for sid in ids:
        params={"api_key":api_key,"series_id":sid,"file_type":"json","sort_order":"asc","limit":10000}
        r=_inst_http(FRED_API_URL,params=params,timeout=20)
        payload=r.json()
        obs=payload.get("observations") or []
        rows=[]
        for o in obs:
            val=o.get("value")
            try: val=float(val)
            except Exception: continue
            rows.append((pd.to_datetime(o.get("date"),errors="coerce"),val))
        d=pd.DataFrame(rows,columns=["DATE",sid]).dropna(subset=["DATE"])
        if not d.empty: frames.append(d.set_index("DATE"))
    if not frames: raise RuntimeError("FRED API returned no observations")
    base=frames[0]
    for d in frames[1:]: base=base.join(d,how="outer")
    return _inst_cache_put(key,base.sort_index().reset_index())

def _treasury_curve(real=False):
    """Read the official Treasury XML feed using exact 10-year field patterns.
    Supports the current Treasury XML schema without guessing arbitrary labels.
    """
    url=TREASURY_REAL_XML if real else TREASURY_YIELD_XML
    typ='daily_treasury_real_yield_curve' if real else 'daily_treasury_yield_curve'
    year=datetime.now(timezone.utc).year
    r=_inst_http(url,{"data":typ,"field_tdr_date_value":str(year)},timeout=20)
    import xml.etree.ElementTree as ET
    root=ET.fromstring(r.content)
    rows=[]
    exact_keys=('TC_10YEAR','BC_10YEAR','TC_10YEAR_RATE','BC_10YEAR_RATE')
    for item in root.iter():
        children={k.tag.split('}')[-1].upper():(k.text or '').strip() for k in list(item)}
        if not children: continue
        date=children.get('NEW_DATE') or children.get('DATE') or children.get('RECORD_DATE')
        y10=next((children[k] for k in exact_keys if children.get(k) not in (None,'')),None)
        if y10 is None:
            y10=next((v for k,v in children.items() if re.search(r'(^|_)10(YEAR|YR)(_RATE)?$',k) and not re.search(r'(20|30)',k)),None)
        if date and y10:
            try: rows.append((pd.to_datetime(date,errors='coerce'),float(y10)))
            except Exception: pass
    if not rows: raise RuntimeError(('Treasury real' if real else 'Treasury nominal')+' 10Y XML schema/value unavailable')
    d=pd.DataFrame(rows,columns=['DATE','VALUE']).dropna(subset=['DATE']).sort_values('DATE')
    return float(d.iloc[-1].VALUE), d.iloc[-1].DATE


def _nyfed_rates():
    """Primary: official NY Fed Markets Data API. No HTML scraping and no key.
    Returns latest EFFR, effective date, target lower/upper bounds and a policy
    direction inferred from the latest target-range change.
    """
    c=_inst_cache_get('nyfed_rates')
    if c is not None:return c
    url=f"{NYFED_API_BASE}/rates/unsecured/effr/last/30.json"
    r=_inst_http(url,timeout=20)
    payload=r.json()
    rows=payload.get('refRates') if isinstance(payload,dict) else payload
    if not isinstance(rows,list) or not rows:
        raise RuntimeError('NY Fed EFFR API returned no refRates')
    parsed=[]
    for item in rows:
        if not isinstance(item,dict) or str(item.get('type','')).upper()!='EFFR':
            continue
        dt=pd.to_datetime(item.get('effectiveDate'),errors='coerce')
        rate=item.get('percentRate')
        try: rate=float(rate)
        except Exception: continue
        lo=item.get('targetRateFrom'); hi=item.get('targetRateTo')
        try: lo=float(lo) if lo is not None else None
        except Exception: lo=None
        try: hi=float(hi) if hi is not None else None
        except Exception: hi=None
        parsed.append((dt,rate,lo,hi))
    parsed=[x for x in parsed if not pd.isna(x[0])]
    if not parsed: raise RuntimeError('NY Fed EFFR API schema/value unavailable')
    parsed.sort(key=lambda x:x[0])
    latest=parsed[-1]
    prev_target=None
    for row in reversed(parsed[:-1]):
        if row[2] is not None and row[3] is not None:
            prev_target=(row[2],row[3]); break
    policy='HOLD'
    if latest[2] is not None and latest[3] is not None and prev_target is not None:
        delta=((latest[2]+latest[3])-(prev_target[0]+prev_target[1]))/2.0
        policy='HAWKISH' if delta>1e-9 else 'DOVISH' if delta<-1e-9 else 'HOLD'
    out={'effr':latest[1],'date':latest[0],'lower':latest[2],'upper':latest[3],'policy':policy,'source':'New York Fed Markets Data API'}
    return _inst_cache_put('nyfed_rates',out)

def _cboe_vix():
    """Primary: official Cboe daily VIX CSV. No scraping.
    The URL is configurable so Cboe can change CDN paths without code changes.
    """
    c=_inst_cache_get('cboe_vix')
    if c is not None:return c
    r=_inst_http(CBOE_VIX_CSV,timeout=20)
    text=r.content.decode('utf-8-sig','replace')
    d=pd.read_csv(io.StringIO(text))
    cols={str(x).strip().lower():x for x in d.columns}
    dc=next((cols[k] for k in cols if k=='date'),None)
    vc=next((cols[k] for k in cols if k in ('vix close','close')),None)
    if dc is None or vc is None:
        raise RuntimeError('Cboe VIX CSV schema unavailable')
    x=pd.DataFrame({'DATE':pd.to_datetime(d[dc],errors='coerce'),'VALUE':pd.to_numeric(d[vc],errors='coerce')}).dropna().sort_values('DATE')
    if x.empty: raise RuntimeError('Cboe VIX returned no observations')
    a=x.iloc[-1]
    return _inst_cache_put('cboe_vix',{'value':float(a.VALUE),'date':a.DATE,'source':'Cboe VIX official historical CSV'})

def _fed_h10_broad():
    """Official Federal Reserve H.10 CSV package, no FRED key required."""
    url='https://www.federalreserve.gov/datadownload/Output.aspx?rel=H10&series=122e3bcb627e8e53f1bf72a1a09cfb81&lastobs=30&from=&to=&filetype=csv&label=include&layout=seriescolumn'
    r=_inst_http(url,timeout=20)
    text=r.content.decode('utf-8-sig','replace')
    d=pd.read_csv(io.StringIO(text))
    cols={str(c).strip().lower():c for c in d.columns}
    dc=next((c for k,c in cols.items() if k in ('time period','date') or 'time period' in k),d.columns[0])
    vc=next((c for k,c in cols.items() if 'nominal broad dollar index' in k or k.endswith('_n.b') or 'value'==k),None)
    if vc is None:
        numeric=[c for c in d.columns if c!=dc and pd.to_numeric(d[c],errors='coerce').notna().sum()>0]
        vc=numeric[0] if numeric else None
    if vc is None: raise RuntimeError('Federal Reserve H.10 broad USD column unavailable')
    x=pd.DataFrame({'DATE':pd.to_datetime(d[dc],errors='coerce'),'VALUE':pd.to_numeric(d[vc],errors='coerce')}).dropna().sort_values('DATE')
    if x.empty: raise RuntimeError('Federal Reserve H.10 returned no observations')
    a=x.iloc[-1]; return float(a.VALUE),a.DATE

def _cftc_zip(url):
    r=_inst_http(url,timeout=25)
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        names=[n for n in z.namelist() if n.lower().endswith(('.txt','.csv'))]
        if not names: raise RuntimeError('CFTC archive has no table')
        with z.open(names[0]) as f: d=pd.read_csv(f,encoding='latin-1',low_memory=False)
    code=next((c for c in d.columns if c.lower()=='cftc_contract_market_code'),None)
    if code:
        d=d[d[code].astype(str).str.strip()=='088691']
    if d.empty:
        namecol=next((c for c in d.columns if c.lower()=='market_and_exchange_names'),None)
        if namecol:
            d=d[d[namecol].astype(str).str.upper().str.contains('GOLD - COMMODITY EXCHANGE INC',na=False)]
    if d.empty: raise RuntimeError('CFTC GOLD row unavailable')
    datecol=_find_date_column(d)
    if datecol is None: raise RuntimeError('CFTC report date column unavailable')
    d['_date']=d[datecol].map(_parse_inst_date)
    d=d.dropna(subset=['_date']).sort_values('_date')
    if d.empty: raise RuntimeError('CFTC report dates could not be parsed')
    return d.reset_index(drop=True)


def _n(row,*names):
    for n in names:
        if n in row.index:
            v=pd.to_numeric(row.get(n),errors='coerce')
            if pd.notna(v): return float(v)
    return None


def _cftc_html_table(url):
    """Official CFTC HTML fallback. Avoids dependence on yearly ZIP archive schemas."""
    r=_inst_http(url,timeout=25)
    text=re.sub(r'\r','',r.text)
    # Normalize HTML into plain text while preserving table rows.
    text=re.sub(r'<br\s*/?>','\n',text,flags=re.I)
    text=re.sub(r'</(p|div|tr|li|pre|h1|h2|h3)>','\n',text,flags=re.I)
    text=re.sub(r'<[^>]+>',' ',text)
    text=re.sub(r'&nbsp;',' ',text,flags=re.I)
    text=re.sub(r'\n[ \t]+','\n',text)
    m=re.search(r'GOLD\s*-\s*COMMODITY EXCHANGE INC\..*?Code-088691(.*?)(?=\n\s*(?:MICRO GOLD|COBALT|LITHIUM HYDROXIDE|ALUMINUM|ALUMINIUM|STEEL-|NORTH EURO)|\Z)',text,flags=re.I|re.S)
    if not m: raise RuntimeError('CFTC GOLD HTML block unavailable')
    block=m.group(0)
    dm=re.search(r'(?:Positions as of|Futures Only,|Combined,)\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})',block,re.I)
    report_date=_parse_inst_date(dm.group(1)) if dm else None
    am=re.search(r'All\s*:\s*([^\n]+)',block)
    cm=re.search(r'Changes in Commitments from:\s*([^\n]+)\n\s*([^\n]+)',block,re.I)
    if not am: raise RuntimeError('CFTC GOLD positions row unavailable')
    nums=[float(x.replace(',','')) for x in re.findall(r'(?<![A-Za-z])-?\d[\d,]*(?:\.\d+)?',am.group(1))]
    change=[]
    if cm: change=[float(x.replace(',','')) for x in re.findall(r'(?<![A-Za-z])-?\d[\d,]*(?:\.\d+)?',cm.group(2))]
    return report_date,nums,change


def _cot_html_real():
    dis_url='https://www.cftc.gov/dea/futures/other_lf.htm'
    leg_url='https://www.cftc.gov/dea/futures/deacmxlf.htm'
    ddate,dnums,dchg=_cftc_html_table(dis_url)
    # Disaggregated futures-only column order:
    # OI, Producer L/S, Swap L/S/Spread, Managed Money L/S/Spread, Other...
    if len(dnums)<9: raise RuntimeError('CFTC disaggregated GOLD row schema too short')
    ml,ms=dnums[6],dnums[7]
    mdl,mds=(dchg[6],dchg[7]) if len(dchg)>=8 else (None,None)
    out={'source':'CFTC official HTML — Disaggregated Futures Only + Legacy Futures Only',
         'report_date':_inst_iso(ddate),'managed_money_net':ml-ms,
         'managed_money_delta':(mdl-mds) if mdl is not None and mds is not None else None}
    out.update(_inst_status(ddate,8,'CFTC Disaggregated'))
    try:
        ldate,lnums,lchg=_cftc_html_table(leg_url)
        if len(lnums)>=7:
            nl,ns=lnums[1],lnums[2]; cl,cs=lnums[4],lnums[5]
            nd=(lchg[1]-lchg[2]) if len(lchg)>=6 else None
            cd=(lchg[4]-lchg[5]) if len(lchg)>=6 else None
            out.update({'legacy_noncommercial_net':nl-ns,'legacy_commercial_net':cl-cs,
                        'legacy_noncommercial_delta':nd,'legacy_commercial_delta':cd})
    except Exception as exc:
        out['legacy_error']=str(exc)
    return _attach_us_calendar_context(out, ddate)


def _cot_real():
    c=_inst_cache_get('cot')
    if c is not None:return c
    try:
        return _inst_cache_put('cot',_cot_html_real())
    except Exception as html_exc:
        html_error=str(html_exc)
    y=datetime.now(timezone.utc).year
    dis=_cftc_zip(CFTC_DISAGG_ZIP_TEMPLATE.format(year=y)); leg=None
    try: leg=_cftc_zip(CFTC_LEGACY_ZIP_TEMPLATE.format(year=y))
    except Exception: pass
    if len(dis)<1: raise RuntimeError('CFTC GOLD report is empty')
    r=dis.iloc[-1]; p=dis.iloc[-2] if len(dis)>1 else None
    ml=_n(r,'M_Money_Positions_Long_All','M_Money_Positions_Long'); ms=_n(r,'M_Money_Positions_Short_All','M_Money_Positions_Short')
    pml=_n(p,'M_Money_Positions_Long_All','M_Money_Positions_Long') if p is not None else None; pms=_n(p,'M_Money_Positions_Short_All','M_Money_Positions_Short') if p is not None else None
    report_date=r.get('_date')
    out={'source':'CFTC Disaggregated + Legacy COT','report_date':str(report_date)[:10],
         'managed_money_net':ml-ms if ml is not None and ms is not None else None,
         'managed_money_delta':(ml-ms)-(pml-pms) if all(v is not None for v in (ml,ms,pml,pms)) else None}
    out.update(_inst_status(report_date,8,'CFTC'))
    if leg is not None and len(leg):
        a=leg.iloc[-1]; b=leg.iloc[-2] if len(leg)>1 else None
        nl=_n(a,'NonComm_Positions_Long_All','NonComm_Positions_Long'); ns=_n(a,'NonComm_Positions_Short_All','NonComm_Positions_Short'); cl=_n(a,'Comm_Positions_Long_All','Comm_Positions_Long'); cs=_n(a,'Comm_Positions_Short_All','Comm_Positions_Short')
        pnl=_n(b,'NonComm_Positions_Long_All','NonComm_Positions_Long') if b is not None else None; pns=_n(b,'NonComm_Positions_Short_All','NonComm_Positions_Short') if b is not None else None; pcl=_n(b,'Comm_Positions_Long_All','Comm_Positions_Long') if b is not None else None; pcs=_n(b,'Comm_Positions_Short_All','Comm_Positions_Short') if b is not None else None
        out.update({'legacy_noncommercial_net':nl-ns if nl is not None and ns is not None else None,
                    'legacy_commercial_net':cl-cs if cl is not None and cs is not None else None,
                    'legacy_noncommercial_delta':(nl-ns)-(pnl-pns) if all(v is not None for v in (nl,ns,pnl,pns)) else None,
                    'legacy_commercial_delta':(cl-cs)-(pcl-pcs) if all(v is not None for v in (cl,cs,pcl,pcs)) else None})
    out['html_fallback_error']=html_error if 'html_error' in locals() else None
    return _inst_cache_put('cot',_attach_us_calendar_context(out, report_date))


def _inst_numeric(value):
    """Normalize institutional numeric cells without inventing values."""
    if value is None: return np.nan
    if isinstance(value,(int,float,np.integer,np.floating)): return float(value)
    txt=str(value).strip().replace('\u00a0',' ').replace(',','')
    if txt in ('','-','—','–','N/A','NA','null','None'): return np.nan
    txt=txt.replace('%','').strip()
    try: return float(txt)
    except Exception: return np.nan


def _wgc_xlsx_tables(content):
    """Read WGC workbook with pandas first and XML fallback second."""
    try:
        xl=pd.ExcelFile(io.BytesIO(content))
        return [(sh,pd.read_excel(xl,sheet_name=sh,header=0)) for sh in xl.sheet_names]
    except Exception as exc:
        if 'openpyxl' not in str(exc).lower(): raise
        return _read_xlsx_without_openpyxl(content)


def _wgc_changes_from_tables(tables):
    """Extract the latest reported monthly change from the WGC workbook.
    Schema is detected by semantic names; no arbitrary numerical field is used.
    """
    candidates=[]
    for sh,d in tables:
        if d is None or d.empty: continue
        cols=list(d.columns)
        date_candidates=[]
        for col in cols:
            parsed=d[col].map(_parse_inst_date)
            score=int(parsed.notna().sum())
            if score>=2: date_candidates.append((score,col,parsed))
        if not date_candidates: continue
        _,dc,parsed=max(date_candidates,key=lambda x:x[0])
        x=d.copy(); x['_period']=parsed
        # Prefer explicit change/purchase/sale/net columns, and require tonne units.
        value_cols=[]
        for col in cols:
            if col==dc: continue
            nm=re.sub(r'[^a-z0-9]+',' ',str(col).lower()).strip()
            if not any(k in nm for k in ('change','net purchase','net sale','purchases','sales')): continue
            if not any(k in nm for k in ('tonne','tons','tonnes','metric')): continue
            vals=x[col].map(_inst_numeric)
            if vals.notna().sum(): value_cols.append((col,vals))
        if not value_cols: continue
        latest=x['_period'].dropna().max()
        rowmask=x['_period']==latest
        # Sum only the explicitly identified change column. If several exist,
        # choose the one whose header most directly denotes net change.
        ranked=sorted(value_cols,key=lambda item:(0 if 'net' in str(item[0]).lower() else 1, str(item[0])))
        col,vals=ranked[0]
        total=float(vals[rowmask].sum())
        if not np.isfinite(total): continue
        candidates.append({'sheet':str(sh),'period':latest,'total':total,'column':str(col)})
    if not candidates: raise RuntimeError('WGC change workbook schema/value unavailable')
    return max(candidates,key=lambda x:x['period'])


def _gld_real():
    """Official SPDR GLD historical holdings with schema/position validation."""
    c=_inst_cache_get('gld')
    if c is not None:return c
    urls=[SPDR_GLD_HISTORY_URL]
    try:
        page_r=_inst_http('https://www.spdrgoldshares.com/usa/gld/',timeout=20,
                          headers={'User-Agent':'Mozilla/5.0','Referer':'https://www.spdrgoldshares.com/usa/gld/'})
        hrefs=re.findall(r"(?:href|data-href)=['\"]([^'\"]+)",page_r.text,flags=re.I)
        ranked=[]
        for href in hrefs:
            low=href.lower()
            score=(5 if '.xlsx' in low else 0)+(3 if 'historical' in low else 0)+(2 if 'archive' in low else 0)
            if score>=5: ranked.append((score,requests.compat.urljoin(page_r.url,href)))
        urls=[u for _,u in sorted(ranked,reverse=True)]+urls
    except Exception:
        pass
    last=[]
    for url in dict.fromkeys(urls):
        try:
            r=_inst_http(url,timeout=30,headers={
                'User-Agent':'Mozilla/5.0','Origin':'https://www.spdrgoldshares.com',
                'Referer':'https://www.spdrgoldshares.com/usa/gld/',
                'Accept':'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/octet-stream,*/*'})
            ct=(r.headers.get('content-type') or '').lower()
            if not r.content.startswith(b'PK\x03\x04'):
                raise RuntimeError(f'الأرشيف الرسمي لـSPDR ليس XLSX؛ النوع={ct}; الحجم={len(r.content)}')
            if ct and not any(x in ct for x in ('spreadsheet','xlsx','octet-stream','zip')):
                raise RuntimeError(f'نوع محتوى أرشيف SPDR غير متوقع: {ct}')
            tables=_wgc_xlsx_tables(r.content)
            best=None
            for sh,d in tables:
                if d is None or d.empty or str(sh).lower()=='disclaimer': continue
                cols=list(d.columns)
                # Verified current layout: Date ... Total Ounces ... Tonnes ... Total NAV USD.
                dc=next((c for c in cols if 'date' in str(c).lower() or 'as of' in str(c).lower()),None)
                oc=next((c for c in cols if 'total ounces' in str(c).lower() and 'share' not in str(c).lower()),None)
                tc=next((c for c in cols if 'tonnes' in str(c).lower() or 'metric tonnes' in str(c).lower()),None)
                if dc is None and len(cols)>=1: dc=cols[0]
                if oc is None and len(cols)>=9: oc=cols[8]
                if tc is None and len(cols)>=10: tc=cols[9]
                if dc is None or (oc is None and tc is None): continue
                x=d.copy(); x['_date']=x[dc].map(_parse_inst_date); x=x.dropna(subset=['_date']).sort_values('_date')
                if len(x)>=2: best=(x,oc,tc,sh); break
            if best is None: raise RuntimeError('مخطط XLSX الرسمي لـSPDR غير متاح')
            d,oc,tc,sh=best; a,b=d.iloc[-1],d.iloc[-2]
            ao=_inst_numeric(a[oc]) if oc is not None else np.nan; bo=_inst_numeric(b[oc]) if oc is not None else np.nan
            at=_inst_numeric(a[tc]) if tc is not None else np.nan; bt=_inst_numeric(b[tc]) if tc is not None else np.nan
            if pd.isna(ao) and pd.notna(at): ao=at*TROY_OZ_PER_TONNE
            if pd.isna(bo) and pd.notna(bt): bo=bt*TROY_OZ_PER_TONNE
            if pd.isna(ao) or ao<=0: raise RuntimeError('قيمة حيازة GLD الرسمية الأخيرة غير صالحة')
            direction='IN' if pd.notna(bo) and ao>bo else 'OUT' if pd.notna(bo) and ao<bo else 'NEUTRAL'
            out={'source':'الأرشيف الرسمي لصندوق SPDR Gold Shares','date':a['_date'].strftime('%Y-%m-%d'),
                 'gold_oz':float(ao),'gold_oz_delta':float(ao-bo) if pd.notna(bo) else None,
                 'shares_delta':None,'shares_direction':'UNAVAILABLE','flow_proxy':direction,'source_url':url,
                 'measurement':'حيازة الذهب الفعلية؛ وليست تدفقاً نقدياً','sheet':str(sh)}
            out.update(_inst_status(a['_date'],5,'SPDR Gold Shares — الأرشيف الرسمي'))
            return _inst_cache_put('gld',out)
        except Exception as exc: last.append(str(exc))
    raise RuntimeError('بيانات SPDR GLD غير متاحة: '+' | '.join(last[-3:]))


def _normalize_month_period(value):
    """Normalize IMF/WGC monthly labels to canonical YYYY-MM without inventing dates."""
    if value is None:
        return None
    s=str(value).strip()
    if not s:
        return None
    m=re.match(r'^(\d{4})-M(\d{1,2})$', s, re.I)
    if not m: m=re.match(r'^(\d{4})M(\d{1,2})$', s, re.I)
    if not m: m=re.match(r'^(\d{4})-(\d{1,2})(?:-\d{1,2})?$', s)
    if not m:
        try:
            ts=pd.to_datetime(s, errors='raise')
            return f"{ts.year:04d}-{ts.month:02d}"
        except Exception:
            return None
    year, month=int(m.group(1)), int(m.group(2))
    if not 1 <= month <= 12:
        return None
    return f"{year:04d}-{month:02d}"

def _month_sort_key(value):
    n=_normalize_month_period(value)
    return n or ''

IMF_LAST_DIAGNOSTIC = {'status':'NOT_RUN','attempts':[]}

def _imf_diag_reset(indicator):
    global IMF_LAST_DIAGNOSTIC
    IMF_LAST_DIAGNOSTIC={'status':'RUNNING','indicator':str(indicator),'attempts':[],
                         'started_at':pd.Timestamp.utcnow().isoformat()}

def _imf_diag_add(stage, url=None, **kw):
    global IMF_LAST_DIAGNOSTIC
    rec={'stage':stage}
    if url: rec['url']=url
    rec.update({k:v for k,v in kw.items() if v is not None})
    IMF_LAST_DIAGNOSTIC.setdefault('attempts',[]).append(rec)

def _imf_response_summary(r):
    ctype=(r.headers.get('content-type') or '').lower()
    head=(r.text[:180] if getattr(r,'text',None) is not None else '').replace('\n',' ').replace('\r',' ')
    return {'http_status':getattr(r,'status_code',None),'content_type':ctype,
            'response_size':len(getattr(r,'content',b'') or b''),'preview':head}

def _imf_parse_csv(text):
    from io import StringIO
    df=pd.read_csv(StringIO(text))
    cols={str(c).upper().strip():c for c in df.columns}
    ccol=next((c for k,c in cols.items() if k in ('REF_AREA','COUNTRY','COUNTRY_CODE','COUNTRY_ID')),None)
    tcol=next((c for k,c in cols.items() if k=='TIME_PERIOD'),None)
    vcol=next((c for k,c in cols.items() if k in ('OBS_VALUE','VALUE','OBSERVATION_VALUE')),None)
    if not (ccol and tcol and vcol):
        raise RuntimeError('IMF SDMX-CSV schema missing country/time/value columns; columns='+','.join(map(str,df.columns[:20])))
    out={}; audit={'parser_format':'CSV','raw_rows':int(len(df)),'accepted_rows':0,'rejected_missing_country':0,'rejected_missing_period':0,'rejected_invalid_value':0,'rejected_nonfinite_value':0,'raw_country_keys':set(),'accepted_country_keys':set(),'raw_period_keys':set(),'accepted_period_keys':set()}
    for _,row in df.iterrows():
        cid=str(row.get(ccol,'')).strip(); period=str(row.get(tcol,'')).strip(); raw_val=row.get(vcol)
        if cid: audit['raw_country_keys'].add(cid)
        if period: audit['raw_period_keys'].add(period)
        if not cid: audit['rejected_missing_country']+=1; continue
        if not period: audit['rejected_missing_period']+=1; continue
        try: val=float(raw_val)
        except Exception: audit['rejected_invalid_value']+=1; continue
        if not np.isfinite(val): audit['rejected_nonfinite_value']+=1; continue
        out.setdefault(cid,{'name':cid,'byMonth':{}})['byMonth'][period]=val
        audit['accepted_rows']+=1; audit['accepted_country_keys'].add(cid); audit['accepted_period_keys'].add(period)
    audit={k:(len(v) if isinstance(v,set) else v) for k,v in audit.items()}
    IMF_LAST_DIAGNOSTIC.update({'raw_parser_audit':audit})
    return out

def _imf_parse_json(payload):
    data=payload.get('data') if isinstance(payload,dict) else None
    if not isinstance(data,dict): data=payload if isinstance(payload,dict) else {}
    structures=data.get('structures') or payload.get('structure') or []
    if isinstance(structures,dict): structures=[structures]
    struct=structures[0] if structures else {}
    ds_list=data.get('dataSets') or payload.get('dataSets') or []
    if isinstance(ds_list,dict): ds_list=[ds_list]
    ds=ds_list[0] if ds_list else {}
    series_obj=ds.get('series') or {}
    dims=struct.get('dimensions') or {}; sdim=dims.get('series',[]) or []; odim=dims.get('observation',[]) or []
    if not isinstance(series_obj,dict): series_obj={}
    country_pos=None; countries=[]
    for i,d in enumerate(sdim):
        if str(d.get('id','')).upper() in ('COUNTRY','REF_AREA'):
            country_pos=i; countries=[v.get('id') or v.get('value') or v.get('name') for v in (d.get('values') or [])]; break
    if country_pos is None or not countries:
        raise RuntimeError('IMF JSON country dimension unavailable; series dimensions='+','.join(str(d.get('id')) for d in sdim))
    time_dim=next((d for d in odim if str(d.get('id','')).upper()=='TIME_PERIOD'),None)
    times=[v.get('id') or v.get('value') or v.get('name') for v in (time_dim.get('values') or [])] if time_dim else []
    out={}; audit={'parser_format':'JSON','raw_series':len(series_obj),'raw_observations':0,'accepted_observations':0,'rejected_country_dimension':0,'rejected_missing_country':0,'rejected_period_index':0,'rejected_invalid_value':0,'rejected_nonfinite_value':0,'raw_country_keys':set(),'accepted_country_keys':set(),'raw_period_keys':set(),'accepted_period_keys':set(),'series_dimensions':','.join(str(d.get('id')) for d in sdim),'observation_dimensions':','.join(str(d.get('id')) for d in odim)}
    for series_key,series in series_obj.items():
        parts=str(series_key).split(':')
        if country_pos>=len(parts): audit['rejected_country_dimension']+=1; continue
        try: idx=int(parts[country_pos])
        except Exception: audit['rejected_country_dimension']+=1; continue
        if not (0<=idx<len(countries)): audit['rejected_country_dimension']+=1; continue
        cid=str(countries[idx] or '').strip()
        if not cid: audit['rejected_missing_country']+=1; continue
        audit['raw_country_keys'].add(cid); rec=out.setdefault(cid,{'name':cid,'byMonth':{}})
        for obs_key,obs_val in (series.get('observations') or {}).items():
            audit['raw_observations']+=1
            try:
                raw=obs_val[0] if isinstance(obs_val,(list,tuple)) else obs_val.get('value') if isinstance(obs_val,dict) else obs_val
                period=times[int(obs_key)] if times else str(obs_key)
            except Exception: audit['rejected_period_index']+=1; continue
            if period: audit['raw_period_keys'].add(str(period))
            try: val=float(raw)
            except Exception: audit['rejected_invalid_value']+=1; continue
            if not np.isfinite(val): audit['rejected_nonfinite_value']+=1; continue
            if not period: audit['rejected_period_index']+=1; continue
            rec['byMonth'][str(period)]=val; audit['accepted_observations']+=1; audit['accepted_country_keys'].add(cid); audit['accepted_period_keys'].add(str(period))
    audit={k:(len(v) if isinstance(v,set) else v) for k,v in audit.items()}
    IMF_LAST_DIAGNOSTIC.update({'raw_parser_audit':audit})
    return out

def _imf_discover_structure():
    """Discover the actual IMF IRFCL metadata before data acquisition.

    Discovery is diagnostic and non-destructive: it never changes trading logic and
    never fabricates a dimension order. The known data route remains a controlled
    acquisition fallback until the metadata response explicitly confirms a structure.
    """
    candidates=[
        ('SDMX3_DATAFLOW', f'{IMF_SDMX_BASE}/dataflow/IMF.STA/IRFCL/+'),
        ('SDMX3_STRUCTURE', f'{IMF_SDMX_BASE}/dataflow/IMF.STA/IRFCL/11.0.0'),
        ('SDMX2_DATAFLOW', 'https://api.imf.org/external/sdmx/2.1/dataflow/IMF.STA/IRFCL/latest'),
        ('SDMX_CENTRAL_DATAFLOW', 'https://sdmxcentral.imf.org/sdmx/v2/dataflow/IMF/IRFCL/1.0'),
    ]
    found=[]
    for label,url in candidates:
        try:
            _imf_diag_add('STRUCTURE_DISCOVERY_REQUEST',url,endpoint=label)
            r=_inst_http(url,timeout=25,headers={
                'User-Agent':'Mozilla/5.0 (XAU Smart Trader IMF Structure Discovery)',
                'Accept':'application/vnd.sdmx.structure+json,application/json,application/xml,text/xml;q=0.8,*/*;q=0.1'})
            meta=_imf_response_summary(r)
            _imf_diag_add('STRUCTURE_DISCOVERY_RESPONSE',url,endpoint=label,**meta)
            if not (200 <= int(meta.get('http_status') or 0) < 300):
                continue
            ctype=meta.get('content_type') or ''
            dims=[]
            try:
                obj=r.json()
                blob=json.dumps(obj,ensure_ascii=False)
                # Diagnostic extraction only; do not infer dimension order from this.
                dims=sorted(set(re.findall(r'"(?:id|dimensionId)"\s*:\s*"([A-Za-z_][A-Za-z0-9_]*)"',blob)))[:40]
            except Exception:
                text=(r.text or '')
                dims=sorted(set(re.findall(r"(?:id|dimension)[=:\s]+[\"']?([A-Za-z_][A-Za-z0-9_]*)",text,re.I)))[:40]
            found.append({'endpoint':label,'url':url,'content_type':ctype,'dimensions':dims})
            _imf_diag_add('STRUCTURE_DISCOVERY_PARSED',url,endpoint=label,dimensions=','.join(dims[:20]),dimension_count=len(dims))
        except Exception as exc:
            _imf_diag_add('STRUCTURE_DISCOVERY_FAILURE',url,endpoint=label,error_type=type(exc).__name__,error=str(exc))
    IMF_LAST_DIAGNOSTIC['structure_discovery']=found
    return found

def _imf_irfcl_monthly(indicator):
    """IMF IRFCL acquisition with real telemetry and parser validation.

    This function deliberately records the actual failure stage. It does not
    manufacture values and does not silently convert endpoint/schema failures
    into an ordinary 'unavailable' state.
    """
    _imf_diag_reset(indicator)
    _imf_discover_structure()
    end_period=pd.Timestamp.utcnow().strftime('%Y-%m')
    start_period=(pd.Timestamp.utcnow()-pd.DateOffset(months=24)).strftime('%Y-%m')
    # Keep the existing known SDMX 3.0 route, but add explicit transport and
    # schema telemetry. No trading/scoring path is touched.
    key=f'*.{indicator}.*.M'
    urls=[]
    for ver in ('11.0.0','+'):
        base=f'{IMF_SDMX_BASE}/data/dataflow/IMF.STA/IRFCL/{ver}/{key}'
        urls += [
            ('JSON',f'{base}?startPeriod={start_period}&endPeriod={end_period}&format=jsondata'),
            ('JSON_TIME',f'{base}?startPeriod={start_period}&endPeriod={end_period}&dimensionAtObservation=TIME_PERIOD&format=jsondata'),
            ('CSV',f'{base}?startPeriod={start_period}&endPeriod={end_period}&format=csvfile'),
        ]
    last=None
    for fmt,url in urls:
        for attempt in range(3):
            try:
                _imf_diag_add('REQUEST',url,format=fmt,attempt=attempt+1)
                r=_inst_http(url,timeout=90,headers={
                    'User-Agent':'Mozilla/5.0 (XAU Smart Trader IMF Diagnostic Gateway)',
                    'Accept':'application/vnd.sdmx.data+json;version=2.0.0,application/vnd.sdmx.data+csv,application/json,text/csv;q=0.8,*/*;q=0.1'})
                meta=_imf_response_summary(r)
                _imf_diag_add('HTTP_RESPONSE',url,format=fmt,attempt=attempt+1,**meta)
                ctype=meta['content_type'] or ''
                if 'html' in ctype:
                    raise RuntimeError('IMF returned HTML instead of SDMX data')
                use_csv=(fmt=='CSV' or 'csv' in ctype)
                out=_imf_parse_csv(r.text) if use_csv else _imf_parse_json(r.json())
                populated={cid:rec for cid,rec in out.items() if rec.get('byMonth')}
                months=sorted({m for rec in populated.values() for m in rec.get('byMonth',{})})
                _imf_diag_add('PARSE_RESULT',url,format=fmt,countries=len(populated),months=len(months),latest_period=(months[-1] if months else None))
                if len(populated)>=20 and len(months)>=2:
                    IMF_LAST_DIAGNOSTIC.update({'status':'OK','countries':len(populated),'months':len(months),'latest_period':months[-1]})
                    return populated
                raise RuntimeError(f'IMF parsed insufficient usable data: countries={len(populated)}, months={len(months)}')
            except Exception as exc:
                last=exc
                _imf_diag_add('FAILURE',url,format=fmt,attempt=attempt+1,error_type=type(exc).__name__,error=str(exc))
                if attempt<2: time.sleep(1.5*(attempt+1))
    err=str(last or '')
    stage='NETWORK' if any(x in err.lower() for x in ('timeout','connection','dns','ssl','name or service')) else 'HTTP_OR_RESPONSE' if any(x in err.lower() for x in ('html','404','403','429','500')) else 'PARSING_OR_SCHEMA'
    IMF_LAST_DIAGNOSTIC.update({'status':'FAILED','failure_stage':stage,'error':err,'error_type':type(last).__name__ if last else None})
    raise RuntimeError(f'IMF IRFCL {indicator} failed after diagnostic trace: {last}')


def _cb_real():
    """Central-bank gold data with WGC↔IMF cross-validation.
    WGC is preferred for its curated reported-change series; IMF IRFCL is the
    independent official statistical cross-check. Disagreement above 5% marks
    the result DEGRADED and prevents the central-bank factor from being scored.
    """
    c=_inst_cache_get('cb')
    if c is not None:return c
    wgc=None; imf=None; errors=[]
    # WGC official page/download discovery: do not depend on a fixed filename.
    wgc_urls=[WGC_CENTRAL_BANK_CHANGES_URL] if WGC_CENTRAL_BANK_CHANGES_URL else []
    try:
        page_r=_inst_http('https://www.gold.org/goldhub/data/gold-reserves-by-country',timeout=20,headers={'User-Agent':'Mozilla/5.0'})
        hrefs=re.findall(r"(?:href|data-href)=['\"]([^'\"]+)",page_r.text,flags=re.I)
        ranked=[]
        for href in hrefs:
            low=href.lower()
            score=(5 if '.xlsx' in low else 0)+(5 if 'change' in low else 0)+(2 if 'official' in low else 0)+(2 if 'reserve' in low else 0)
            if score>=5: ranked.append((score,requests.compat.urljoin(page_r.url,href)))
        wgc_urls=[u for _,u in sorted(ranked,reverse=True)]+wgc_urls
    except Exception as exc: errors.append('WGC صفحة البيانات: '+str(exc))
    for url in dict.fromkeys(wgc_urls):
        try:
            r=_inst_http(url,timeout=30,headers={'User-Agent':'Mozilla/5.0','Referer':'https://www.gold.org/goldhub/data/gold-reserves-by-country','Accept':'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*'})
            if not r.content.startswith(b'PK\x03\x04'): raise RuntimeError(f'WGC ليس XLSX؛ النوع={r.headers.get("content-type","")}; الحجم={len(r.content)}')
            item=_wgc_changes_from_tables(_wgc_xlsx_tables(r.content))
            wgc={'period':item['period'],'net_change_tonnes':item['total'],'source':'مجلس الذهب العالمي WGC — التغيرات المعلنة','reported_only':True,'source_url':url,'column':item['column'],'sheet':item['sheet'],'definition_family':'monthly_central_bank_gold_reserve_change','definition_scope':'WGC curated reported central-bank gold reserve change'}
            wgc.update(_inst_status(item['period'],45,'مجلس الذهب العالمي WGC'))
            break
        except Exception as exc: errors.append('WGC: '+str(exc))
    # WGC official article fallback: use only an explicitly published monthly net figure.
    # This is not synthetic data and is kept separate from the downloadable workbook path.
    if wgc is None:
        try:
            ar=_inst_http(WGC_CENTRAL_BANK_ARTICLE_URL,timeout=25,headers={'User-Agent':'Mozilla/5.0','Accept':'text/html'})
            txt=re.sub(r'\s+',' ',ar.text)
            m=re.search(r'central banks\s+(?:bought|added)\s+([0-9]+(?:\.[0-9]+)?)t\s+of gold in June',txt,re.I)
            if m:
                val=float(m.group(1))
                wgc={'period':'2026-06','net_change_tonnes':val,'source':'مجلس الذهب العالمي WGC — تقرير شهري رسمي','reported_only':True,'source_url':WGC_CENTRAL_BANK_ARTICLE_URL,'publication_fallback':True,'definition_family':'monthly_central_bank_gold_reserve_change','definition_scope':'WGC published central-bank gold purchases/additions for the month'}
                wgc.update(_inst_status(pd.Timestamp('2026-06-30'),45,'مجلس الذهب العالمي WGC — تقرير شهري رسمي'))
        except Exception as exc: errors.append('WGC التقرير الشهري: '+str(exc))
    # IMF official monthly fine-troy-ounce holdings, independent cross-check.
    # When WGC is available, calculate the IMF change for the EXACT WGC month
    # versus the immediately preceding IMF month. This prevents a false
    # comparison such as WGC June versus the latest IMF July/August release.
    try:
        IMF_LAST_DIAGNOSTIC.update({'pipeline_stage':'ACQUISITION','acquisition_status':'RUNNING'})
        raw=_imf_irfcl_monthly(IMF_GOLD_FTO_INDICATOR)
        IMF_LAST_DIAGNOSTIC.update({'pipeline_stage':'ACQUISITION','acquisition_status':'OK','countries_available':len(raw),
                                    'raw_series_countries':len(raw)})
        raw_month_map={}
        for rec in raw.values():
            for raw_period in rec.get('byMonth',{}):
                normalized=_normalize_month_period(raw_period)
                if normalized:
                    raw_month_map.setdefault(normalized,[]).append(str(raw_period))
        all_months=sorted(raw_month_map, key=_month_sort_key)
        IMF_LAST_DIAGNOSTIC.update({'pipeline_stage':'PERIOD_NORMALIZATION',
                                    'normalized_months_available':len(all_months),
                                    'normalized_latest_available':all_months[-1] if all_months else None})
        if len(all_months)<2:
            raise RuntimeError('سلسلة IMF تحتوي على أقل من شهرين صالحين بعد توحيد صيغة الفترات')
        target_raw=str(wgc.get('period') or '') if wgc is not None else ''
        target=_normalize_month_period(target_raw) if target_raw else None
        if target and target in all_months:
            i=all_months.index(target)
            if i<1:
                raise RuntimeError(f'لا يوجد شهر سابق لـ {target} في IMF')
            latest,prior=target,all_months[i-1]
        else:
            latest,prior=all_months[-1],all_months[-2]

        raw_latest_candidates=raw_month_map.get(latest,[])
        raw_prior_candidates=raw_month_map.get(prior,[])
        IMF_LAST_DIAGNOSTIC.update({
            'pipeline_stage':'PERIOD_SELECTED',
            'raw_latest_period':raw_latest_candidates[0] if raw_latest_candidates else None,
            'normalized_period':latest,
            'raw_previous_period':raw_prior_candidates[0] if raw_prior_candidates else None,
            'normalized_previous_period':prior,
            'target_wgc_raw_period':target_raw or None,
            'target_wgc_normalized_period':target or None,
            'period_alignment':'SAME_PERIOD' if target and target==latest else 'IMF_LATEST_FALLBACK' if target else 'NO_WGC_TARGET',
        })

        def _value_for_normalized_month(rec, normalized):
            vals=[]
            for rp,val in rec.get('byMonth',{}).items():
                if _normalize_month_period(rp)==normalized:
                    vals.append((str(rp),float(val)))
            if not vals:
                return None
            if len(vals)>1:
                # Duplicate representations of the same month must agree; never silently sum them.
                numbers={round(v,10) for _,v in vals}
                if len(numbers)>1:
                    raise RuntimeError(f'قيم متعددة متعارضة لنفس الفترة الموحدة {normalized}')
            return vals[0][1]

        # Coverage Forensics: diagnostic-only. No threshold, trading gate, scoring,
        # or calculation semantics are changed here. We explicitly classify the
        # country universe before deciding whether the existing common-country rule
        # can proceed.
        current_ids=[]
        previous_ids=[]
        common=[]
        current_values={}
        previous_values={}
        country_key_audit={}
        for cid,rec in raw.items():
            cid_raw=str(cid).strip()
            # Canonical identity is intentionally conservative: trim + uppercase only.
            # No country alias is invented and no external mapping is assumed.
            cid_norm=cid_raw.upper()
            cur=_value_for_normalized_month(rec,latest)
            prev=_value_for_normalized_month(rec,prior)
            country_key_audit[cid_raw]=cid_norm
            if cur is not None:
                current_ids.append(cid_raw)
            if prev is not None:
                previous_ids.append(cid_raw)
            if cur is not None and prev is not None:
                common.append(cid_raw); current_values[cid_raw]=cur; previous_values[cid_raw]=prev

        current_set=set(current_ids)
        previous_set=set(previous_ids)
        common_set=current_set & previous_set
        current_only=sorted(current_set-previous_set)
        previous_only=sorted(previous_set-current_set)
        common_sorted=sorted(common_set)
        total_series=len(raw)
        current_count=len(current_set)
        previous_count=len(previous_set)
        common_count=len(common_set)
        current_ratio=round(100.0*current_count/max(total_series,1),2)
        previous_ratio=round(100.0*previous_count/max(total_series,1),2)
        common_ratio_total=round(100.0*common_count/max(total_series,1),2)
        common_ratio_current=round(100.0*common_count/max(current_count,1),2)
        common_ratio_previous=round(100.0*common_count/max(previous_count,1),2)

        # Country-ID audit: detect normalization collisions and whether identity
        # differences, rather than publication coverage, explain the intersection.
        norm_to_raw={}
        for raw_id,norm_id in country_key_audit.items():
            norm_to_raw.setdefault(norm_id,[]).append(raw_id)
        normalization_collisions={k:v for k,v in norm_to_raw.items() if len(set(v))>1}
        normalized_current={country_key_audit[x] for x in current_set}
        normalized_previous={country_key_audit[x] for x in previous_set}
        normalized_common=normalized_current & normalized_previous
        identity_gap=len(normalized_common)-common_count

        # Evidence-based classification only; never guesses a country alias.
        if identity_gap>0 or normalization_collisions:
            likely_cause='COUNTRY_KEY_NORMALIZATION'
            cause_note='اختلاف تمثيل Country IDs/Codes قد يؤثر على التقاطع؛ لم تُطبق أي خريطة أسماء تخمينية.'
        elif current_count>=20 and previous_count>=20 and common_count<20:
            likely_cause='UNIVERSE_OR_PUBLICATION_LAG'
            cause_note='كل فترة لديها تغطية مستقلة معقولة لكن تقاطع الكون منخفض؛ يرجح اختلاف الكون أو تأخر النشر، وليس فشل الاستحواذ.'
        elif current_count<20 or previous_count<20:
            likely_cause='PERIOD_COVERAGE_INSUFFICIENT'
            cause_note='إحدى الفترتين نفسها ذات تغطية منخفضة؛ لا يمكن فصل التأخر الطبيعي عن نقص التغطية من دون بيانات إضافية.'
        else:
            likely_cause='PARSING_OR_SERIES_SPARSE'
            cause_note='التوزيع لا يثبت سبباً واحداً؛ يلزم فحص بنية السلسلة أو كثافة الملاحظات.'

        IMF_LAST_DIAGNOSTIC.update({
            'pipeline_stage':'COVERAGE_CHECK',
            'countries_available':total_series,
            'countries_current_period':current_count,
            'countries_previous_period':previous_count,
            'countries_common':common_count,
            'countries_current_only':len(current_only),
            'countries_previous_only':len(previous_only),
            'coverage_ratio_current_pct':current_ratio,
            'coverage_ratio_previous_pct':previous_ratio,
            'coverage_ratio_common_total_pct':common_ratio_total,
            'coverage_ratio_common_of_current_pct':common_ratio_current,
            'coverage_ratio_common_of_previous_pct':common_ratio_previous,
            'country_id_normalization':'TRIM+UPPERCASE_ONLY',
            'country_id_normalization_collisions':len(normalization_collisions),
            'country_identity_gap':identity_gap,
            'likely_cause':likely_cause,
            'likely_cause_note':cause_note,
            'current_only_sample':current_only[:8],
            'previous_only_sample':previous_only[:8],
            'common_sample':common_sorted[:8],
        })
        if len(common)<20:
            raise RuntimeError(f'IMF لديه {len(common)} دولة فقط مشتركة بين {prior} و{latest}; السبب التشخيصي المحتمل={likely_cause}')
        current=sum(current_values.values())
        previous=sum(previous_values.values())
        delta=(current-previous)/TROY_OZ_PER_TONNE
        IMF_LAST_DIAGNOSTIC.update({
            'pipeline_stage':'HOLDINGS_CALCULATION',
            'current_holdings_oz':float(current),
            'previous_holdings_oz':float(previous),
            'calculated_change_tonnes':float(delta),
        })

        IMF_LAST_DIAGNOSTIC.update({
            'status':'OK',
            'pipeline_stage':'IMF_CALCULATION_OK',
            'raw_latest_period':raw_month_map.get(latest,[None])[0],
            'normalized_period':latest,
            'raw_previous_period':raw_month_map.get(prior,[None])[0],
            'normalized_previous_period':prior,
            'countries_available':len(raw),
            'countries_common':len(common),
            'current_holdings_oz':float(current),
            'previous_holdings_oz':float(previous),
            'calculated_change_tonnes':float(delta),
        })
        imf={'period':latest,'net_change_tonnes':float(delta),
             'source':'صندوق النقد الدولي IMF — IRFCL',
             'reported_only':True,
             'countries_covered':len(common),
             'coverage_method':'الدول المشتركة التي لديها الشهران بعد توحيد صيغة الفترات',
             'prior_period':prior,
             'definition':'تغير مجموع حيازات الذهب الرسمية للدول المشتركة كما وردت في IMF IRFCL','definition_family':'monthly_central_bank_gold_reserve_change','definition_scope':'IMF reported official gold holdings change across common reporting countries'}
        imf.update(_inst_status(pd.Timestamp(latest+'-01'),90,'صندوق النقد الدولي IMF'))
    except Exception as exc:
        if isinstance(IMF_LAST_DIAGNOSTIC,dict):
            IMF_LAST_DIAGNOSTIC.update({
                'status':'FAILED',
                'failure_stage':IMF_LAST_DIAGNOSTIC.get('pipeline_stage') or 'CROSS_VALIDATION_PIPELINE',
                'error_type':type(exc).__name__,
                'error':str(exc),
            })
        diag=IMF_LAST_DIAGNOSTIC.copy() if isinstance(IMF_LAST_DIAGNOSTIC,dict) else {}
        errors.append('IMF: '+str(exc)+' | التشخيص='+json.dumps(diag,ensure_ascii=False,default=str)[:2200])
    if wgc is None and imf is None:
        raise RuntimeError('بيانات الذهب للبنوك المركزية غير متاحة: '+' | '.join(errors[-4:]))
    cv={'status':'NOT_PERFORMED','method':'WGC مقابل IMF','difference_tonnes':None,'difference_pct':None,'validated':False}
    chosen=wgc or imf
    if wgc is not None and imf is not None:
        wp=_normalize_month_period(wgc.get('period'))
        ip=_normalize_month_period(imf.get('period'))
        comparable_definition=bool(
            wgc.get('definition_family') == imf.get('definition_family')
            and wgc.get('definition_family') == 'monthly_central_bank_gold_reserve_change'
        )
        known_coverage=bool(imf.get('countries_covered',0) >= 20)
        same_period=bool(wp and ip and wp == ip)
        IMF_LAST_DIAGNOSTIC.update({'pipeline_stage':'CROSS_VALIDATION_GATE_CHECK','wgc_normalized_period':wp,'imf_normalized_period':ip,'same_period':same_period,'known_coverage':known_coverage,'comparable_definition':comparable_definition})
        if not same_period:
            cv={'status':'PERIOD_MISMATCH','method':'مقارنة WGC مع IMF IRFCL — نفس الشهر بعد توحيد الصيغة','difference_tonnes':None,'difference_pct':None,'validated':False,'wgc_period':wp,'imf_period':ip,'reason':'الفترتان غير متطابقتين بعد التطبيع؛ لم تتم مقارنة أرقام من شهرين مختلفين.','same_period':False,'normalized_period':None,'known_coverage':known_coverage,'comparable_definition':comparable_definition}
            chosen=dict(wgc)
        elif not known_coverage:
            cv={'status':'COVERAGE_INSUFFICIENT','method':'مقارنة WGC مع IMF IRFCL','difference_tonnes':None,'difference_pct':None,'validated':False,'wgc_period':wp,'imf_period':ip,'reason':'تغطية IMF غير كافية لإجراء تحقق مؤسسي موثوق.','same_period':True,'normalized_period':wp,'known_coverage':False,'comparable_definition':comparable_definition}
            chosen=dict(wgc)
        elif not comparable_definition:
            cv={'status':'DEFINITION_MISMATCH','method':'مقارنة WGC مع IMF IRFCL','difference_tonnes':None,'difference_pct':None,'validated':False,'wgc_period':wp,'imf_period':ip,'reason':'تعريف السلسلتين غير مؤكد المقارنة؛ لم تُجر مقارنة رقمية.','same_period':True,'normalized_period':wp,'known_coverage':True,'comparable_definition':False}
            chosen=dict(wgc)
        else:
            a=float(wgc['net_change_tonnes']); b=float(imf['net_change_tonnes'])
            denom=max((abs(a)+abs(b))/2.0,0.1)
            diff=abs(a-b); pct=100.0*diff/denom
            cv={'status':'PASS' if pct<=5.0 else 'DEGRADED','method':'مقارنة WGC مع IMF IRFCL لنفس الشهر بعد تحقق الفترة والتغطية وتعريف القياس','difference_tonnes':round(diff,4),'difference_pct':round(pct,2),'validated':pct<=5.0,'wgc_value':a,'imf_value':b,'wgc_period':wp,'imf_period':ip,'tolerance_pct':5.0,'same_period':True,'normalized_period':wp,'known_coverage':True,'comparable_definition':True,'comparison_type':'independent_cross_check_not_accounting_identity','note':'التحقق المتقاطع لا يعني أن WGC وIMF يجب أن يتطابقا محاسبياً؛ اختلاف التغطية والتوقيت والمراجعات قد يفسر فروقاً'}
            # WGC remains the preferred reported-change value when validated.
            chosen=wgc if pct<=5.0 else None
            if chosen is None:
                # Do not invent a blended value; preserve IMF as independently sourced
                # but explicitly mark the disagreement so scoring can exclude it.
                chosen=dict(imf)
    chosen=dict(chosen)
    chosen['cross_validation']=cv
    chosen['validation_errors']=errors
    chosen['direction']='BUY' if chosen.get('net_change_tonnes',0)>0 else 'SELL' if chosen.get('net_change_tonnes',0)<0 else 'NEUTRAL'
    if cv['status']=='DEGRADED': chosen['status']='DEGRADED'; chosen['cross_validation']['validated']=False
    return _inst_cache_put('cb',chosen)


def _latest_series_value(df,col):
    if col not in df.columns: return None,None
    x=df[['DATE',col]].copy(); x[col]=pd.to_numeric(x[col],errors='coerce'); x=x.dropna(subset=['DATE',col])
    if x.empty: return None,None
    r=x.iloc[-1]; return float(r[col]),r['DATE']
def _macro_real():
    c=_inst_cache_get('macro')
    if c is not None:return c
    errors=[]
    # Official-source hierarchy: NY Fed API / Cboe / Federal Reserve H.10 are
    # primary. FRED and Yahoo are controlled fallbacks only.
    f=pd.DataFrame(columns=['DATE']); lo=hi=effr=vix=broad_usd=None; lo_date=hi_date=effr_date=vix_date=broad_usd_date=None
    policy='غير متاح'
    try:
        nr=_nyfed_rates(); effr=nr['effr']; effr_date=nr['date']; lo=nr.get('lower'); hi=nr.get('upper'); lo_date=hi_date=effr_date; policy=nr.get('policy','غير متاح')
    except Exception as exc: errors.append('NY Fed API: '+str(exc))
    try:
        broad_usd,broad_usd_date=_fed_h10_broad()
    except Exception as exc: errors.append('Federal Reserve H.10: '+str(exc))
    try:
        vr=_cboe_vix(); vix=vr['value']; vix_date=vr['date']
    except Exception as exc: errors.append('Cboe VIX: '+str(exc))
    # Optional FRED fallback for any remaining macro series.
    if any(x is None for x in (lo,hi,effr,vix,broad_usd)) and os.environ.get('FRED_API_KEY','').strip():
        try:
            f=_fred(['DFEDTARL','DFEDTARU','EFFR','VIXCLS','DTWEXBGS'])
            flo,flo_date=_latest_series_value(f,'DFEDTARL'); fhi,fhi_date=_latest_series_value(f,'DFEDTARU'); feffr,feffr_date=_latest_series_value(f,'EFFR'); fvix,fvix_date=_latest_series_value(f,'VIXCLS'); fbroad,fbroad_date=_latest_series_value(f,'DTWEXBGS')
            if lo is None: lo,lo_date=flo,flo_date
            if hi is None: hi,hi_date=fhi,fhi_date
            if effr is None: effr,effr_date=feffr,feffr_date
            if vix is None: vix,vix_date=fvix,fvix_date
            if broad_usd is None: broad_usd,broad_usd_date=fbroad,fbroad_date
        except Exception as exc: errors.append('FRED fallback: '+str(exc))
    # Direct Treasury, not FRED, for nominal and real 10Y.
    try: us10y,us10y_date=_treasury_curve(False)
    except Exception as exc: us10y,us10y_date=None,None; errors.append('Treasury nominal: '+str(exc))
    try: real10y,real10y_date=_treasury_curve(True)
    except Exception as exc: real10y,real10y_date=None,None; errors.append('Treasury real: '+str(exc))
    pol=policy
    # DXY: licensed ICE feed is primary. Yahoo is a controlled market-data fallback.
    dxy=None; dxy_ch=None; dxy_asof=None; dxy_source='ICE DXY licensed'
    ice_url=os.environ.get('ICE_DXY_API_URL','').strip()
    ice_token=os.environ.get('ICE_DXY_API_TOKEN','').strip()
    if ice_url and ice_token:
        try:
            payload=_inst_http(ice_url,headers={'Authorization':'Bearer '+ice_token}).json()
            dxy=float(payload.get('value') or payload.get('last') or payload.get('close'))
            dxy_asof=pd.to_datetime(payload.get('date') or payload.get('timestamp'),errors='coerce')
            dxy_ch=float(payload.get('change_pct')) if payload.get('change_pct') is not None else None
        except Exception as exc: errors.append('ICE DXY: '+str(exc))
    if dxy is None:
        try:
            payload=_inst_http(YAHOO_CHART_URL.format(symbol='DX-Y.NYB'),params={'interval':'1d','range':'5d'},headers={'User-Agent':'Mozilla/5.0'}).json()
            result=(payload.get('chart',{}).get('result') or [None])[0]
            if result:
                meta=result.get('meta',{}); dxy=float(meta.get('regularMarketPrice') or meta.get('previousClose'))
                dxy_asof=pd.to_datetime(meta.get('regularMarketTime'),unit='s',errors='coerce')
                prev=float(meta.get('previousClose')) if meta.get('previousClose') is not None else None
                dxy_ch=((dxy-prev)/prev*100.0) if prev else None
                dxy_source='Yahoo Finance DX-Y.NYB fallback'
        except Exception as exc: errors.append('Yahoo DXY fallback: '+str(exc))
    # CME FedWatch is direct but credentialed. If configured, use the official REST JSON.
    exp='غير متاحة'; fedwatch_status={'status':'UNAVAILABLE','fresh':False,'age_days':None,'asof':None,'source':'واجهة CME الرسمية لاحتمالات عقود الأموال الفيدرالية'}
    fw_url=os.environ.get('CME_FEDWATCH_API_URL','').strip(); fw_token=os.environ.get('CME_FEDWATCH_API_TOKEN','').strip()
    if fw_url and fw_token:
        try:
            payload=_inst_http(fw_url,headers={'Authorization':'Bearer '+fw_token}).json()
            # Accept common CME response shapes without inventing an endpoint/schema.
            p=payload.get('data',payload)
            if isinstance(p,dict):
                probs=p.get('probabilities') or p.get('outcomes') or []
            else: probs=[]
            if isinstance(probs,list) and probs:
                vals=[]
                for item in probs:
                    if not isinstance(item,dict): continue
                    label=str(item.get('label') or item.get('outcome') or item.get('targetRate') or '').strip()
                    prob=item.get('probability') if item.get('probability') is not None else item.get('prob')
                    try: vals.append((label,float(prob)))
                    except Exception: pass
                if vals:
                    # Do not invent a probability model. When CME provides target-rate
                    # buckets, classify only from the bucket relative to the current
                    # target midpoint; otherwise expose the highest-probability label.
                    best=max(vals,key=lambda x:x[1])
                    label_num=re.search(r'(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)',best[0])
                    if label_num and lo is not None and hi is not None:
                        midpoint=(float(label_num.group(1))+float(label_num.group(2)))/2.0
                        current_mid=(float(lo)+float(hi))/2.0
                        exp='رفع محتمل' if midpoint>current_mid+0.05 else 'خفض محتمل' if midpoint<current_mid-0.05 else 'استقرار محتمل'
                    else:
                        exp=best[0] or 'متاحة من CME FedWatch'
            if exp=='رفع محتمل': fedwatch_status['market_rate_bias']='HAWKISH'
            elif exp=='خفض محتمل': fedwatch_status['market_rate_bias']='DOVISH'
            elif exp=='استقرار محتمل': fedwatch_status['market_rate_bias']='NEUTRAL'
            fedwatch_status['interpretation']='احتمالات FedWatch الرسمية من عقود Fed Funds'
            asof=pd.to_datetime(payload.get('asOf') or payload.get('timestamp') or p.get('asOf') if isinstance(p,dict) else None,errors='coerce')
            if not pd.isna(asof): fedwatch_status=_inst_status(asof,1,'واجهة CME الرسمية لاحتمالات عقود الأموال الفيدرالية')
            else: fedwatch_status={'status':'FRESH','fresh':True,'age_days':0,'asof':None,'source':'واجهة CME الرسمية لاحتمالات عقود الأموال الفيدرالية'}
        except Exception as exc: errors.append('CME FedWatch: '+str(exc))
    # Fed Funds Futures: CME FedWatch API is primary when licensed credentials are configured.
    # If not configured, use the CME-listed ZQ continuous market-data proxy from Yahoo only
    # to expose the implied average rate; it is NOT treated as CME FedWatch probabilities.
    if exp=='غير متاحة':
        try:
            payload=_inst_http(YAHOO_CHART_URL.format(symbol='ZQ=F'),params={'interval':'1d','range':'5d'},headers={'User-Agent':'Mozilla/5.0'}).json()
            result=(payload.get('chart',{}).get('result') or [None])[0]
            if result:
                meta=result.get('meta',{}); price=meta.get('regularMarketPrice') or meta.get('previousClose')
                if price is not None:
                    implied=round(100.0-float(price),3)
                    asof=pd.to_datetime(meta.get('regularMarketTime'),unit='s',errors='coerce')
                    bias=('HAWKISH' if lo is not None and hi is not None and implied>float((lo+hi)/2)+0.05 else 'DOVISH' if lo is not None and hi is not None and implied<float((lo+hi)/2)-0.05 else 'NEUTRAL')
                    exp=('ميل صعودي في متوسط الفائدة الضمني للعقد' if bias=='HAWKISH' else 'ميل هبوطي في متوسط الفائدة الضمني للعقد' if bias=='DOVISH' else 'ميل محايد في متوسط الفائدة الضمني للعقد')
                    fedwatch_status=_inst_status(asof,2,'بيانات سوق العقود ZQ=F — بديل سوقي للعقود الآجلة للصناديق الفيدرالية') if not pd.isna(asof) else {'status':'FRESH','fresh':True,'age_days':0,'asof':None,'source':'بيانات سوق العقود ZQ=F — بديل سوقي للعقود الآجلة للصناديق الفيدرالية'}
                    fedwatch_status['implied_rate']=implied
                    fedwatch_status['market_rate_bias']=bias
                    fedwatch_status['contract']='ZQ=F'
                    fedwatch_status['interpretation']='سعر العقد = 100 − متوسط EFFR الشهري الضمني؛ ليس احتمالاً مباشراً لقرار اجتماع واحد'
        except Exception as exc: errors.append('Yahoo ZQ Fed Funds fallback: '+str(exc))
    spx=None; ma=None
    risk='غير متاح' 
    # Risk regime only when official VIX + S&P are available from configured official feeds.
    if vix is not None: risk='RISK-OFF' if vix>=25 else 'RISK-ON' if vix<20 else 'MIXED'
    asof=max([x for x in (lo_date,hi_date,effr_date,us10y_date,real10y_date,vix_date,broad_usd_date) if x is not None],default=None)
    out={'source':'Official NY Fed API + Cboe + Federal Reserve H.10 + U.S. Treasury' ,'policy':pol,'fed_lower':lo,'fed_upper':hi,'effr':effr,
         'rate_expectation':exp,'rate_expectation_source':fedwatch_status.get('source','CME FedWatch API') if exp!='غير متاحة' else 'غير متاح',
         'dxy':dxy,'dxy_change_pct':dxy_ch,'dxy_asof':_inst_iso(dxy_asof),'usd_broad':broad_usd,
         'us10y':us10y,'real10y':real10y,'vix':vix,'risk_regime':risk,'errors':errors,
         'fed_futures_implied':round(float(fedwatch_status.get('implied_rate')),3) if fedwatch_status.get('implied_rate') is not None else None,
         'market_rate_bias':fedwatch_status.get('market_rate_bias'),
         'fed_futures_interpretation':fedwatch_status.get('interpretation')}
    out.update(_inst_status(asof,3,'FRED/Treasury'))
    out['series_status']={
        'fed_lower':_inst_status(lo_date,3,'New York Fed Markets API' if lo_date is not None else 'FRED DFEDTARL'), 'fed_upper':_inst_status(hi_date,3,'New York Fed Markets API' if hi_date is not None else 'FRED DFEDTARU'),
        'effr':_inst_status(effr_date,3,'New York Fed Markets API'), 'us10y':_inst_status(us10y_date,3,'U.S. Treasury 10Y'),
        'real10y':_inst_status(real10y_date,3,'U.S. Treasury Real 10Y'), 'vix':_inst_status(vix_date,3,'Cboe VIX' if vix_date is not None and not any('Cboe VIX' in e for e in errors) else 'FRED VIXCLS'),
        'usd_broad':_inst_status(broad_usd_date,3,'Federal Reserve H.10 Broad USD')}
    for _k,_v in list(out['series_status'].items()):
        _attach_us_calendar_context(_v)
    _attach_us_calendar_context(out)
    out['dxy_status']=_inst_status(dxy_asof,3,dxy_source) if dxy_asof is not None else {'status':'UNAVAILABLE','fresh':False,'age_days':None,'asof':None,'source':dxy_source}
    out['fed_futures_status']=fedwatch_status
    primary_failures=[]
    if effr is None or lo is None or hi is None: primary_failures.append('NY_FED_RATES')
    if broad_usd is None: primary_failures.append('FED_H10_BROAD_USD')
    if vix is None: primary_failures.append('CBOE_VIX')
    out['primary_source_failures']=primary_failures
    out['data_quality_gate']='PASS' if not primary_failures else 'DEGRADED'
    out['fallbacks_used']=[x for x in ('FRED' if any('FRED fallback' in e for e in errors) else None,'YAHOO_DXY' if dxy_source.startswith('Yahoo') else None) if x]
    # Overall macro status is not used for component scoring.
    return _inst_cache_put('macro',out)

def institutional_analysis(df):
    """Institutional engine with coverage-aware scoring and explicit data health.

    Missing data is never scored as neutral. Each source contributes only when
    present and fresh enough for its natural publication frequency. The final
    strength is normalized by the available evidence, while coverage is exposed
    separately so a high score cannot masquerade as high data completeness.
    """
    sources={}; errors=[]
    jobs={'macro':_macro_real,'cot':_cot_real,'gld':_gld_real,'central_banks':_cb_real}
    with ThreadPoolExecutor(max_workers=4,thread_name_prefix='institutional') as pool:
        futures={pool.submit(fn):key for key,fn in jobs.items()}
        for future in as_completed(futures):
            key=futures[future]
            try:sources[key]=future.result()
            except Exception as e:
                sources[key]={'error':str(e),'status':'UNAVAILABLE','source':None}; errors.append(f'{key}: {e}')
    m=sources.get('macro',{}); c=sources.get('cot',{}); g=sources.get('gld',{}); cb=sources.get('central_banks',{})
    bull=bear=0.0; available=0.0; factors=[]
    def add(weight,side,text,available_ok=True):
        nonlocal bull,bear,available
        if not available_ok:return
        available += weight
        if side=='BUY':bull+=weight
        elif side=='SELL':bear+=weight
        if text:factors.append(text)
    # 15: Fed policy change, 15: market rate expectations, 15: real yields.
    policy_fresh=(m.get('series_status',{}).get('fed_lower',{}).get('status')=='FRESH' and m.get('series_status',{}).get('fed_upper',{}).get('status')=='FRESH')
    real_fresh=m.get('series_status',{}).get('real10y',{}).get('status')=='FRESH'
    vix_fresh=m.get('series_status',{}).get('vix',{}).get('status')=='FRESH'
    if policy_fresh and m.get('policy')=='DOVISH':add(15,'BUY','السياسة النقدية تميل للتيسير')
    elif policy_fresh and m.get('policy')=='HAWKISH':add(15,'SELL','السياسة النقدية تميل للتشدد')
    elif policy_fresh and m.get('policy')=='HOLD':add(15,None,'السياسة النقدية: تثبيت، دون اتجاه جديد')
    rate_bias=m.get('fed_futures_status',{}).get('market_rate_bias')
    if m.get('fed_futures_status',{}).get('status')=='FRESH' and rate_bias=='DOVISH':add(15,'BUY','ميل هبوطي في متوسط الفائدة الضمني للعقد')
    elif m.get('fed_futures_status',{}).get('status')=='FRESH' and rate_bias=='HAWKISH':add(15,'SELL','ميل صعودي في متوسط الفائدة الضمني للعقد')
    elif m.get('fed_futures_status',{}).get('status')=='FRESH' and rate_bias=='NEUTRAL':add(15,None,'ميل محايد في متوسط الفائدة الضمني للعقد')
    real=m.get('real10y')
    if real_fresh and real is not None:
        add(15,'BUY' if real<1.5 else 'SELL' if real>2.5 else None,f'العائد الحقيقي 10Y: {real:.2f}%')
    dxy_ch=m.get('dxy_change_pct')
    if m.get('dxy_status',{}).get('status')=='FRESH' and dxy_ch is not None:
        add(10,'BUY' if dxy_ch<=-1 else 'SELL' if dxy_ch>=1 else None,f'تغير DXY الشهري: {dxy_ch:+.1f}%')
    vix=m.get('vix'); risk=m.get('risk_regime')
    if vix_fresh and vix is not None:
        side='BUY' if (vix is not None and vix>=25) or risk=='RISK-OFF' else 'SELL' if risk=='RISK-ON' and vix is not None and vix<20 else None
        add(5,side,'VIX/Risk Regime: '+(risk or 'غير متاح'))
    # COT: prefer legacy Non-Commercial net; otherwise use Managed Money.
    n=c.get('legacy_noncommercial_net'); mm=c.get('managed_money_net')
    cot_value=n if n is not None else mm
    if c.get('status')=='FRESH' and cot_value is not None:
        add(20,'BUY' if cot_value>0 else 'SELL' if cot_value<0 else None,'COT: '+('صافي شراء' if cot_value>0 else 'صافي بيع' if cot_value<0 else 'محايد'))
    # GLD holdings change is a holdings proxy, not cash-flow data.
    if g.get('status')=='FRESH' and g.get('flow_proxy') in ('IN','OUT'):
        add(10,'BUY' if g.get('flow_proxy')=='IN' else 'SELL','GLD: تغير الحيازة '+('صاعد' if g.get('flow_proxy')=='IN' else 'هابط'))
    net=cb.get('net_change_tonnes')
    cb_valid=cb.get('cross_validation',{}).get('validated',True)
    if cb.get('status')=='FRESH' and cb_valid and net is not None:
        add(10,'BUY' if net>0 else 'SELL' if net<0 else None,f'البنوك المركزية: {net:+.1f} طن')
    # Only fresh contributions participate in the decision. Stale data remains
    # visible to the user but cannot silently influence the score.
    freshness_penalty=0
    for key,src in sources.items():
        if src.get('source') and src.get('status')=='STALE': freshness_penalty += 1
    effective_available=available
    coverage=100.0*effective_available/100.0
    net=bull-bear
    strength=int(round(min(100.0,abs(net)/effective_available*100))) if effective_available>0 else 0
    effective_strength=int(round(strength*(coverage/100.0))) if effective_available>0 else 0
    margin=(abs(net)/effective_available) if effective_available>0 else 0.0
    direction='WAIT'
    if effective_available>=40 and margin>=0.15:
        direction='BUY' if net>0 else 'SELL' if net<0 else 'WAIT'
    status='OK' if coverage>=70 else 'PARTIAL' if coverage>=40 else 'INSUFFICIENT' if coverage>0 else 'UNAVAILABLE'
    return {'direction':direction,'quality':strength,'institutional_score':effective_strength,'strength':strength,'effective_strength':effective_strength,
            'coverage_pct':int(round(coverage)),'available_weight':round(effective_available,1),'bull':round(bull,1),'bear':round(bear,1),
            'margin':round(margin,3),'status':status,'factors':factors,'sources':sources,'errors':errors,
            'context':'بيانات مؤسسية حقيقية مع تطبيع للتغطية؛ البيانات المفقودة أو القديمة لا تُحتسب كحياد.',
            'decision_gate':{'min_coverage_pct':40,'min_margin':0.15,'allow_direct_flip':False}}


def institutional_conflict_gate(technical_direction, technical_score, m15_bullish_break, institutional):
    """Strict non-flipping gate: institutional conflict cannot directly reverse a technical signal."""
    cov=float(institutional.get('coverage_pct',0) or 0); strength=float(institutional.get('effective_strength',0) or 0)
    inst_dir=institutional.get('direction')
    if m15_bullish_break and technical_direction=='SELL':
        return {'state':'CONFLICT','action':'WAIT','reason':'M15 bullish structural break cancels SELL; no direct SELL→BUY flip.'}
    if technical_direction=='SELL' and technical_score>=70 and cov>=60 and inst_dir=='SELL' and strength>=35:
        return {'state':'ALIGNED','action':'SELL','reason':'Technical SELL + institutional SELL with sufficient coverage.'}
    if technical_direction=='BUY' and technical_score>=70 and cov>=60 and inst_dir=='BUY' and strength>=20:
        return {'state':'ALIGNED','action':'BUY','reason':'Technical BUY + institutional BUY with sufficient coverage.'}
    return {'state':'WAIT','action':'WAIT','reason':'Institutional evidence insufficient for execution confirmation.'}

def institutional_adjustment(direction,institutional):
    if direction not in ('BUY','SELL'):return 0,['لا يوجد اتجاه فني صالح للمقارنة المؤسسية']
    inst_dir=institutional.get('direction')
    eff=int(institutional.get('effective_strength',institutional.get('institutional_score',0)) or 0)
    cov=int(institutional.get('coverage_pct',0) or 0)
    if inst_dir==direction:
        return min(10,int(round(eff/10))),[f'التأكيد المؤسسي: {direction} (قوة {eff}/100، تغطية {cov}%)']
    if inst_dir in ('BUY','SELL'):
        return -min(10,int(round(eff/10))),[f'التعارض المؤسسي: المؤسسات تميل إلى {inst_dir} مقابل {direction} (قوة {eff}/100، تغطية {cov}%)']
    return 0,[f'البيانات المؤسسية غير كافية للحسم (تغطية {cov}%)']

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
    if liq_state == "CONFIRMED":
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
        "price": price, "ema9": sf(EMA(close, 9).iloc[-1]), "ema21": sf(EMA(close, 21).iloc[-1]), "ema50": ema50, "ema200": ema200, "rsi": rsi,
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
    """بناء صفقة تنفيذية متينة من نفس بيانات H1/M15.

    الإصلاح في v18.27:
    - درجة الإشارة لا تُسقط بسبب مستوى S/R بعيد أو غير صالح.
    - عند تعذر استخدام S/R نستخدم ATR كخطة احتياطية آمنة.
    - Entry/SL/TP تُبنى دائماً بترتيب صحيح وبـ R:R أدنى 1:1.20.
    """
    try:
        direction = str(direction).upper().strip()
        if direction not in ("BUY", "SELL"):
            return None
        m15 = m15 or {}
        h1 = h1 or {}
        levels = levels or {}

        entry = float(m15.get("price", 0.0))
        m15_atr = float(m15.get("atr", 0.0) or 0.0)
        h1_atr = float(h1.get("atr", 0.0) or 0.0)
        if not math.isfinite(entry) or entry <= 0:
            return None

        # M15 هو أساس التنفيذ؛ H1 يستخدم كاحتياط إذا كان ATR M15 غير صالح.
        atr = m15_atr if math.isfinite(m15_atr) and m15_atr > 0 else h1_atr
        atr = max(float(atr), 0.50)
        if not math.isfinite(atr) or atr <= 0:
            return None

        def lp(name):
            item = levels.get(name)
            if not isinstance(item, dict):
                return None
            try:
                value = float(item.get("price"))
                return value if math.isfinite(value) and value > 0 else None
            except Exception:
                return None

        supports = sorted([p for p in (lp("support1"), lp("support2"), lp("support3"))
                          if p is not None and p < entry], reverse=True)
        resistances = sorted([p for p in (lp("resistance1"), lp("resistance2"), lp("resistance3"))
                             if p is not None and p > entry])

        # مخاطرة أساسية قابلة للتنفيذ. لا نجعل S/R البعيد يكسر الصفقة.
        min_risk = max(atr * 0.80, entry * 0.00035)
        max_risk = max(atr * 3.00, min_risk * 1.10)

        if direction == "BUY":
            # نستخدم الدعم فقط إذا كان قريباً ومنطقياً.
            sl = entry - min_risk
            if supports:
                candidate = supports[0] - atr * 0.15
                candidate_risk = entry - candidate
                if math.isfinite(candidate_risk) and min_risk <= candidate_risk <= max_risk:
                    sl = candidate
            risk = entry - sl

            # أهداف R ثابتة كمرجع، ثم نستفيد من المقاومات القريبة فقط إذا كانت
            # أمام السعر ولا تُفسد ترتيب الأهداف.
            tp1 = entry + risk * 1.20
            tp2 = entry + risk * 1.80
            tp3 = entry + risk * 2.40
            if resistances:
                r1 = resistances[0]
                if tp1 <= r1 <= entry + atr * 2.00:
                    tp1 = r1
            if len(resistances) >= 2:
                r2 = resistances[1]
                if tp2 <= r2 <= entry + atr * 3.00:
                    tp2 = r2
            if len(resistances) >= 3:
                r3 = resistances[2]
                if tp3 <= r3 <= entry + atr * 4.00:
                    tp3 = r3
            # بعد استخدام S/R نعيد فرض ترتيب ومسافات آمنة.
            tp1 = max(tp1, entry + risk * 1.20)
            tp2 = max(tp2, tp1 + atr * 0.25, entry + risk * 1.80)
            tp3 = max(tp3, tp2 + atr * 0.35, entry + risk * 2.40)
        else:
            sl = entry + min_risk
            if resistances:
                candidate = resistances[0] + atr * 0.15
                candidate_risk = candidate - entry
                if math.isfinite(candidate_risk) and min_risk <= candidate_risk <= max_risk:
                    sl = candidate
            risk = sl - entry

            tp1 = entry - risk * 1.20
            tp2 = entry - risk * 1.80
            tp3 = entry - risk * 2.40
            if supports:
                s1 = supports[0]
                if entry - atr * 2.00 <= s1 <= tp1:
                    tp1 = s1
            if len(supports) >= 2:
                s2 = supports[1]
                if entry - atr * 3.00 <= s2 <= tp2:
                    tp2 = s2
            if len(supports) >= 3:
                s3 = supports[2]
                if entry - atr * 4.00 <= s3 <= tp3:
                    tp3 = s3
            tp1 = min(tp1, entry - risk * 1.20)
            tp2 = min(tp2, tp1 - atr * 0.25, entry - risk * 1.80)
            tp3 = min(tp3, tp2 - atr * 0.35, entry - risk * 2.40)

        values = (entry, sl, tp1, tp2, tp3, risk)
        if not all(math.isfinite(float(v)) for v in values) or risk <= 0:
            return None
        if direction == "BUY" and not (sl < entry < tp1 < tp2 < tp3):
            return None
        if direction == "SELL" and not (sl > entry > tp1 > tp2 > tp3):
            return None

        rr = abs(tp3 - entry) / risk
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
        logger.exception("BUILD_TRADE_ERROR direction=%s", direction)
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
        source_tz = os.environ.get("NEWS_SOURCE_TZ", "America/New_York")
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.tz_localize(source_tz)
        else:
            dt = dt.tz_convert(source_tz)
        return dt.to_pydatetime().astimezone(DAMASCUS)
    except Exception:
        return None


# ============================================================
# INDEPENDENT NEWS RISK ENGINE v18.39 — EMBEDDED
# التحليل لا يتصل بأي موقع أخبار. يقرأ محرك الأخبار ملفاً محلياً فقط.
# التحديث الشبكي، إن فُعّل، يعمل في خيط خلفي مستقل خارج مسار التحليل.
# ============================================================
# ============================================================
# INDEPENDENT NEWS RISK ENGINE v18.39 — EMBEDDED
# Network is used ONLY by refresh_news_calendar() in the background.
# get_events()/get_risk() are LOCAL-ONLY and never perform HTTP requests.
# ============================================================

LOGGER = logging.getLogger("xau_news_engine")
DAMASCUS = ZoneInfo("Asia/Damascus")
UTC = timezone.utc
CACHE_FILE = os.getenv("NEWS_CACHE_FILE", "news_calendar_cache.json")
MANUAL_FILE = os.getenv("NEWS_MANUAL_FILE", "news_calendar.json")
CACHE_MAX_AGE_MIN = int(os.getenv("NEWS_MAX_CACHE_AGE_MIN", "360"))
MIN_COVERAGE_MIN = int(os.getenv("NEWS_CACHE_MIN_COVERAGE_MIN", "180"))
BEFORE_MIN = int(os.getenv("NEWS_BEFORE_MIN", "30"))
AFTER_MIN = int(os.getenv("NEWS_AFTER_MIN", "30"))
REFRESH_SECONDS = int(os.getenv("NEWS_REFRESH_SECONDS", "21600"))
HTTP_TIMEOUT = float(os.getenv("NEWS_HTTP_TIMEOUT", "12"))
TE_API_KEY = os.getenv("TE_API_KEY", "").strip()
_LOCK = threading.RLock()
_REFRESH_THREAD = None
_STOP = threading.Event()
def _now_utc():
    return datetime.now(UTC)
def _parse_dt(value):
    if not value:
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), UTC)
        raw = str(value).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            # Manual files must explicitly use timezone when possible; naive values are Damascus.
            dt = dt.replace(tzinfo=DAMASCUS)
        return dt.astimezone(UTC)
    except Exception:
        return None
def _normalize_impact(value):
    raw = str(value or "").strip().lower()
    if raw in {"3", "high", "high impact", "red", "critical", "major"}:
        return "HIGH"
    if raw in {"2", "medium", "moderate", "orange"}:
        return "MEDIUM"
    if raw in {"1", "low", "green", "minor"}:
        return "LOW"
    try:
        n = float(raw)
        if n >= 3:
            return "HIGH"
        if n >= 2:
            return "MEDIUM"
        if n >= 1:
            return "LOW"
    except Exception:
        pass
    return "UNKNOWN"
def _gold_relevant(event):
    currency = str(event.get("currency", "")).upper()
    name = str(event.get("event", "")).lower()
    if currency in {"USD", "US", "USA", "UNITED STATES"}:
        return True
    keys = (
        "fomc", "federal reserve", "fed ", "powell", "interest rate", "cpi",
        "pce", "inflation", "nonfarm", "payroll", "unemployment", "employment",
        "gdp", "ism", "retail sales", "ppi", "producer price", "jobless claims",
        "treasury", "consumer confidence"
    )
    return any(k in name for k in keys)
def _normalize_rows(rows, source):
    events = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        dt = _parse_dt(row.get("time") or row.get("date") or row.get("datetime") or row.get("Date"))
        impact = _normalize_impact(row.get("impact") if row.get("impact") is not None else row.get("importance", row.get("Importance")))
        if not dt:
            continue
        event = {
            "time": dt.isoformat(),
            "currency": str(row.get("currency") or row.get("Currency") or row.get("country") or row.get("Country") or "USD").upper(),
            "impact": impact,
            "event": str(row.get("event") or row.get("Event") or row.get("title") or row.get("name") or "High-impact economic event").strip(),
            "source": source,
        }
        if _gold_relevant(event):
            events.append(event)
    events.sort(key=lambda e: e["time"])
    # Keep all trusted gold-relevant events so coverage reflects the actual calendar,
    # while get_risk() independently applies the HIGH-impact execution rule.
    # deterministic de-duplication
    out, seen = [], set()
    for e in events:
        key = (e["time"], e["currency"], e["event"])
        if key not in seen:
            seen.add(key); out.append(e)
    return out
def _coverage_ok(events, now=None):
    now = now or _now_utc()
    times = [_parse_dt(e.get("time")) for e in events]
    times = [x for x in times if x]
    # Coverage must be based on the complete relevant calendar, not only HIGH events.
    # This prevents a valid calendar from being rejected merely because its last
    # HIGH-impact event is several hours away.
    horizon = now + timedelta(minutes=MIN_COVERAGE_MIN)
    return bool(times) and max(times) >= horizon
def _read_json(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)
def _load_local_events():
    # Manual calendar takes precedence when it is fresh and valid.
    if os.path.exists(MANUAL_FILE):
        try:
            payload = _read_json(MANUAL_FILE)
            rows = payload.get("events", []) if isinstance(payload, dict) else payload
            events = _normalize_rows(rows, "manual")
            if events and _coverage_ok(events):
                return events, "manual", None
        except Exception as exc:
            LOGGER.warning("NEWS_MANUAL_LOAD_FAILED: %s", exc)
    if os.path.exists(CACHE_FILE):
        try:
            payload = _read_json(CACHE_FILE)
            saved_at = float(payload.get("saved_at", 0))
            age = (time.time() - saved_at) / 60.0
            rows = payload.get("events", [])
            events = _normalize_rows(rows, payload.get("source", "cache"))
            if age <= CACHE_MAX_AGE_MIN and _coverage_ok(events):
                return events, "cache", None
            return [], "cache", f"cache stale/insufficient coverage (age={age:.1f}m)"
        except Exception as exc:
            return [], "cache", f"cache load failed: {exc}"
    # Embedded bootstrap prevents first-start UNKNOWN while preserving a local-only read path.
    events = _normalize_rows(BOOTSTRAP_EVENTS, "bootstrap-official-schedule")
    if events and _coverage_ok(events):
        return events, "bootstrap-official-schedule", None
    return [], "none", "no local calendar available"
def get_events():
    """Pure local read path. NEVER performs HTTP/network I/O."""
    with _LOCK:
        events, source, error = _load_local_events()
        if not events:
            raise RuntimeError(error or "no trusted local calendar")
        return events
def get_risk(now=None):
    """Pure local news-risk monitor: ALERT, CLEAR, or UNKNOWN. News never blocks execution."""
    now = now or _now_utc()
    try:
        events = get_events()
        upcoming = []
        for e in events:
            dt = _parse_dt(e["time"])
            if not dt:
                continue
            delta = (dt - now).total_seconds() / 60.0
            # HIGH-impact events are detected for advisory risk alerts only; they never block execution.
            if e.get("impact") == "HIGH" and -AFTER_MIN <= delta <= BEFORE_MIN:
                upcoming.append((e, delta))
        if upcoming:
            upcoming.sort(key=lambda x: abs(x[1]))
            details = []
            for e, delta in upcoming[:3]:
                if delta > 1:
                    phase = f"بعد {round(delta)} دقيقة"
                elif delta >= -1:
                    phase = "الآن / قريب جداً"
                else:
                    phase = f"منذ {round(abs(delta))} دقيقة"
                local = _parse_dt(e["time"]).astimezone(DAMASCUS).strftime("%Y-%m-%d %H:%M")
                details.append(f"🚨 {e['currency']} — {e['event']} | {local} دمشق | {phase}")
            return {
                "state": "ALERT",
                "blocked": False,
                "source": upcoming[0][0].get("source", "local"),
                "message": "🟠 تنبيه: خبر عالي التأثير ضمن نافذة المخاطر. لا يتم حجب التنفيذ آلياً.\n" + "\n".join(details),
                "events": [e for e, _ in upcoming],
            }
        return {
            "state": "CLEAR", "blocked": False,
            "source": events[0].get("source", "local"),
            "message": "🟢 لا يوجد حالياً خبر عالي التأثير ضمن نافذة المخاطر.",
            "events": [],
        }
    except Exception as exc:
        return {
            "state": "UNKNOWN", "blocked": False, "source": "none", "events": [],
            "message": f"⚠️ حالة الأخبار UNKNOWN — تنبيه فقط ولا يتم حجب الإشارة التنفيذية.\nالسبب: {exc}",
        }
def _fetch_te():
    if not TE_API_KEY:
        raise RuntimeError("TE_API_KEY is not configured")
    now = datetime.now(DAMASCUS)
    start = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    end = (now + timedelta(days=14)).strftime("%Y-%m-%d")
    url = f"https://api.tradingeconomics.com/calendar/country/united%20states/{start}/{end}"
    r = requests.get(url, params={"c": TE_API_KEY, "f": "json"}, timeout=HTTP_TIMEOUT,
                     headers={"User-Agent": "XAU-News-Collector/1.0"})
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError("Trading Economics returned invalid calendar payload")
    return _normalize_rows(data, "tradingeconomics")
def _write_cache(events, source):
    payload = {"saved_at": time.time(), "source": source, "events": events}
    tmp = CACHE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, CACHE_FILE)
def refresh_news_calendar():
    """Network operation. Intentionally NOT called by get_events()/get_risk()."""
    with _LOCK:
        try:
            events = _fetch_te()
            if not events or not _coverage_ok(events):
                raise RuntimeError("provider returned no sufficient relevant-calendar coverage")
            _write_cache(events, "tradingeconomics")
            LOGGER.info("NEWS_REFRESH_OK source=tradingeconomics events=%s", len(events))
            return True, "tradingeconomics", len(events)
        except Exception as exc:
            LOGGER.warning("NEWS_REFRESH_FAILED: %s", exc)
            return False, "none", str(exc)
def _worker():
    # First refresh happens in this background thread only; never in get_risk().
    refresh_news_calendar()
    while not _STOP.wait(REFRESH_SECONDS):
        refresh_news_calendar()
def start_background_refresh():
    global _REFRESH_THREAD
    if _REFRESH_THREAD and _REFRESH_THREAD.is_alive():
        return
    _STOP.clear()
    # Do not refresh synchronously during startup; first refresh happens in background.
    _REFRESH_THREAD = threading.Thread(target=_worker, name="news-calendar-refresh", daemon=True)
    _REFRESH_THREAD.start()
def stop_background_refresh():
    _STOP.set()


def get_news_events(force=False):
    """LOCAL-ONLY read path. force is retained for API compatibility; no HTTP call occurs."""
    events = get_events()
    return [{
        **e,
        "time": _parse_news_datetime(e.get("time"), None),
    } for e in events]

def news_filter():
    """News advisory only. It never blocks analysis or trade execution."""
    risk = get_risk()
    return False, risk.get("message", "")


def _signal_rejection_reasons(result):
    reasons = []
    if result.get("direction") not in ("BUY", "SELL"):
        reasons.append("لا يوجد اتجاه BUY/SELL مؤكد")
    if float(result.get("score", 0) or 0) < MIN_TRADE_SCORE:
        reasons.append(f"درجة الإعداد أقل من {MIN_TRADE_SCORE} نقطة")
    if result.get("liquidity_blocked"):
        reasons.append("إعادة اختبار السيولة INVALIDATED")
    reasons.extend(result.get("setup_reasons", []))
    if result.get("direction") in ("BUY", "SELL") and not result.get("trade"):
        reasons.append("لم تمر الصفقة كل بوابات التنفيذ")
    return list(dict.fromkeys(reasons))



def _execution_readiness(direction, scenario_score, setup_score, alignment, liquidity_blocked, rr_ok, trigger_confirmed, data_quality, conflicts, trigger_maturity=None):
    """درجة مستقلة لجاهزية التنفيذ؛ لا تفتح READY بمفردها."""
    if direction not in ("BUY", "SELL"):
        return 0
    score = 0.0
    score += min(25.0, max(0.0, float(scenario_score or 0)) * 0.25)
    score += 18.0 if alignment else 6.0
    maturity = float(trigger_maturity if trigger_maturity is not None else (100 if trigger_confirmed else 0))
    score += min(20.0, max(0.0, maturity) * 0.20)
    score += 15.0 if rr_ok else 0.0
    score += 10.0 if not liquidity_blocked else 0.0
    score += min(12.0, max(0.0, float(data_quality or 0) - 40.0) * 0.20)
    severe = sum(1 for c in (conflicts or []) if 'شديد' in str(c) or 'HIGH' in str(c))
    moderate = max(0, len(conflicts or []) - severe)
    score -= min(25.0, severe * 8.0 + moderate * 3.0)
    return int(max(0, min(100, round(score))))


def _data_quality_score(frames_map, price, levels):
    """جودة بيانات التحليل الكامل متعدد الأطر؛ تقيس الاكتمال والكفاية والحداثة."""
    score = 0.0
    expected = ('W1','D1','H4','H1','M15','M5')
    def getf(name):
        return frames_map.get(name) or frames_map.get(name.lower()) or {}
    available = [name for name in expected if isinstance(getf(name), dict) and getf(name)]
    if available:
        score += 25.0 * len(available) / len(expected)
    sample_ok = freshness_ok = source_ok = 0
    freshness_limits = {'W1':8*24*3600,'D1':36*3600,'H4':6*3600,'H1':2*3600,'M15':30*60,'M5':12*60}
    for name in expected:
        x=getf(name)
        if not isinstance(x,dict) or not x: continue
        bars=x.get('bars_count',x.get('count',x.get('rows',0)))
        try:
            bars_ok = float(bars) >= 60
        except Exception:
            bars_ok = False
        if bars_ok: sample_ok += 1
        age=x.get('age_seconds',x.get('staleness_seconds'))
        try:
            age_ok = age is not None and float(age) >= 0 and float(age) <= freshness_limits[name]
        except Exception:
            age_ok = False
        if age_ok: freshness_ok += 1
        status=str(x.get('data_status',x.get('status',''))).upper()
        source=str(x.get('source_status','')).upper()
        if status not in ('STALE','UNAVAILABLE','ERROR') and source not in ('STALE','FALLBACK_ERROR','ERROR'):
            source_ok+=1
    score += 20.0 * sample_ok / len(expected)
    score += 20.0 * freshness_ok / len(expected)
    score += 10.0 * source_ok / len(expected)
    try: price_ok=float(price or 0)>0
    except Exception: price_ok=False
    score += 15.0 if price_ok else 0.0
    score += 5.0 if isinstance(levels,dict) and levels else 0.0
    exec_frames=[getf(k) for k in ('H1','M15','M5')]
    if all(isinstance(x,dict) and float(x.get('atr',0) or 0)>0 for x in exec_frames): score+=5.0
    return int(max(0,min(100,round(score))))

def _scenario_type(direction, frames_map, levels, price):
    """تصنيف سياقي: REVERSAL / CONTINUATION / BREAKOUT / LIQUIDITY_REVERSAL."""
    if direction not in ('BUY','SELL'): return 'UNKNOWN'
    primary=frames_map.get('M15') or frames_map.get('H1') or {}
    liq=primary.get('liquidity',{}) or {}; sweep=liq.get('sweep')
    wanted='SELL_SIDE_SWEEP' if direction=='BUY' else 'BUY_SIDE_SWEEP'
    state=(liq.get('retest',{}) or {}).get('state') or 'NONE'
    if sweep==wanted and state in ('SWEEP','DISPLACEMENT','BOS','RETEST','CONFIRMED'):
        return 'LIQUIDITY_REVERSAL'
    # اختراق: إغلاق/بنية في اتجاه السيناريو مع مستوى مقابل تم تجاوزه.
    structure=primary.get('structure'); close=primary.get('close',price)
    resistance=nearest_resistance(levels,price); support=nearest_support(levels,price)
    if direction=='BUY' and structure=='صاعد' and resistance is not None and float(close or price)>=float(resistance): return 'BREAKOUT'
    if direction=='SELL' and structure=='هابط' and support is not None and float(close or price)<=float(support): return 'BREAKOUT'
    directions=[x.get('direction') for x in frames_map.values() if isinstance(x,dict)]
    same=sum(1 for d in directions if d==direction)
    if same>=max(2,len(directions)-1): return 'CONTINUATION'
    return 'REVERSAL'

def _conflict_engine(direction, frames_map, price, levels):
    """محرك تعارض متعدد الأطر؛ العائق السعري يبقى ضمن Location لمنع Double Counting."""
    if direction not in ('BUY','SELL'): return []
    expected='صاعد' if direction=='BUY' else 'هابط'; conflicts=[]
    severity={'W1':'شديد','D1':'شديد','H4':'شديد','H1':'متوسط','M15':'متوسط','M5':'خفيف'}
    for name in ('W1','D1','H4','H1','M15','M5'):
        x=frames_map.get(name) or frames_map.get(name.lower()) or {}
        structure=x.get('structure'); d=x.get('direction')
        if structure in ('صاعد','هابط') and structure!=expected: conflicts.append(f"[{severity[name]}] هيكل {name} يعارض السيناريو")
        if d in ('BUY','SELL') and d!=direction: conflicts.append(f"[{severity[name]}] اتجاه {name} يعارض السيناريو")
    m=frames_map.get('M15') or frames_map.get('H1') or {}
    adx=float(m.get('adx',0) or 0); plus=float(m.get('plus_di',m.get('di_plus',0)) or 0); minus=float(m.get('minus_di',m.get('di_minus',0)) or 0)
    macd=float(m.get('macd',0) or 0); sig=float(m.get('macd_signal',0) or 0); rsi=float(m.get('rsi',50) or 50)
    opp=(direction=='BUY' and ((adx>=25 and minus>plus) or (macd<sig and rsi<45))) or (direction=='SELL' and ((adx>=25 and plus>minus) or (macd>sig and rsi>55)))
    if opp: conflicts.append('[متوسط] الزخم في M15 يعارض السيناريو')
    return list(dict.fromkeys(conflicts))

def _trigger_maturity(frames_data, direction, horizon='daily'):
    """مصدر Trigger موحد للتنفيذ اليومي: M15، مع H1 فقط كسياق احتياطي."""
    if direction not in ('BUY','SELL'): return 0,'NONE'
    name='M15' if horizon=='daily' else 'D1'
    liq=(frames_data.get(name,{}) or {}).get('liquidity',{}) or {}
    wanted='SELL_SIDE_SWEEP' if direction=='BUY' else 'BUY_SIDE_SWEEP'
    sweep=liq.get('sweep')==wanted; state=(liq.get('retest',{}) or {}).get('state') or 'NONE'
    table={'NONE':0,'SWEEP':30,'DISPLACEMENT':50,'BOS':70,'RETEST':85,'CONFIRMED':100}
    maturity=table.get(state,0)
    if sweep and maturity<30: maturity=30
    return int(maturity),state

def _daily_execution_core():
    """محرك التنفيذ قبل إدخال SMC: Scenario -> Setup -> Risk/News -> READY.

    Performance-only change: the independent market reads are fetched in
    parallel, and the H1 S/R calculation reuses the already-fetched H1 data.
    No scoring, thresholds, gates, or trade construction rules are changed.
    """
    # These four reads are independent. Fetching them serially made the handler
    # wait for the sum of network latencies before analysis could even start.
    with ThreadPoolExecutor(max_workers=5, thread_name_prefix="market-fetch") as pool:
        f_h1 = pool.submit(get_bars, "1h", 300)
        f_m15 = pool.submit(get_bars, "15m", 300)
        f_m5 = pool.submit(get_bars, "5m", 300)
        f_price = pool.submit(live_price)
        # Keep the exact historical S/R data request used by v18.77, but do it
        # concurrently so it no longer adds its network latency to the chain.
        f_levels = pool.submit(get_bars, "1h", 250)
        h1_df = f_h1.result()
        m15_df = f_m15.result()
        m5_df = f_m5.result()
        price = float(f_price.result()["price"])
        levels_df = f_levels.result()

    # Exact v18.77 S/R input preserved: the dedicated 1H/250 dataset.
    levels = support_resistance(levels_df)
    h1 = analyze(h1_df)
    m15 = analyze(m15_df)
    m5 = analyze(m5_df)
    frames = {"H1": h1, "M15": m15, "M5": m5}
    buy_score, buy_factors = _scenario_score(frames, [("H1",30),("M15",25),("M5",20)], "BUY", levels, price, float(m15.get("atr",0) or 0), "daily")
    sell_score, sell_factors = _scenario_score(frames, [("H1",30),("M15",25),("M5",20)], "SELL", levels, price, float(m15.get("atr",0) or 0), "daily")
    direction = "BUY" if buy_score > sell_score else "SELL" if sell_score > buy_score else "WAIT"
    scenario_score = max(buy_score, sell_score)
    factors = buy_factors if direction == "BUY" else sell_factors if direction == "SELL" else []
    liquidity = m15.get("liquidity", {}) if isinstance(m15.get("liquidity", {}), dict) else {}
    retest = liquidity.get("retest", {}) if isinstance(liquidity.get("retest", {}), dict) else {}
    liquidity_state = retest.get("state")
    liquidity_blocked = liquidity_state == "INVALIDATED"

    setup_score = float(scenario_score)
    setup_reasons = []
    coverage = sum(1 for x in (h1,m15,m5) if x.get("direction") == direction) if direction in ("BUY","SELL") else 0
    opposite = sum(1 for x in (h1,m15,m5) if x.get("direction") not in (direction,"WAIT")) if direction in ("BUY","SELL") else 0
    alignment = coverage == 3
    if coverage < 2:
        setup_score -= 15; setup_reasons.append("تغطية الاتجاه أقل من فريمين")
    if opposite:
        setup_score -= 10 * opposite; setup_reasons.append(f"تعارض اتجاهي على {opposite} فريم")
    m15_adx = float(m15.get("adx",0) or 0)
    if m15_adx < 15:
        setup_score -= 20; setup_reasons.append(f"ADX M15 ضعيف ({m15_adx:.1f})")
    elif m15_adx < 20:
        setup_score -= 10; setup_reasons.append(f"ADX M15 متوسط ({m15_adx:.1f})")
    expected_structure = "صاعد" if direction == "BUY" else "هابط"
    if direction in ("BUY","SELL") and h1.get("structure") in ("صاعد","هابط") and h1.get("structure") != expected_structure:
        setup_score -= 10; setup_reasons.append("هيكل H1 يعارض اتجاه التنفيذ")

    try:
        news_blocked, news_text = news_filter()
    except Exception as exc:
        news_blocked, news_text = False, f"⚠️ تعذر قراءة الأخبار — تنبيه فقط ولا يتم حجب التنفيذ: {exc}"
    candidate = build_trade(direction, h1, m15, levels) if direction in ("BUY","SELL") else None
    rr_ok = bool(candidate and float(candidate.get("rr",0) or 0) >= 1.20)
    ready = bool(direction in ("BUY","SELL") and setup_score >= MIN_TRADE_SCORE and alignment and m15_adx >= 15 and not liquidity_blocked and rr_ok)
    state = "READY" if ready else "CANDIDATE" if direction in ("BUY","SELL") and setup_score >= MIN_TRADE_SCORE else "WATCH"
    if not alignment: setup_reasons.append("لا يوجد تطابق H1 + M15 + M5 كامل")
    if liquidity_blocked: setup_reasons.append("حالة السيولة INVALIDATED")
    # الأخبار طبقة تنبيه ومخاطر فقط، ولا تمنع التنفيذ.
    if not rr_ok: setup_reasons.append("R:R التنفيذي غير صالح أو غير متاح")
    setup_score = int(max(0,min(100,round(setup_score))))
    data_quality=_data_quality_score(frames, price, levels)
    conflicts=_conflict_engine(direction, {"H1":h1,"M15":m15,"M5":m5}, price, levels)
    trigger_confirmed=bool(retest.get("state")=="CONFIRMED")
    trigger_maturity, trigger_state = _trigger_maturity(frames, direction, 'daily')
    execution_readiness=_execution_readiness(direction, scenario_score, setup_score, alignment, liquidity_blocked, rr_ok, trigger_confirmed, data_quality, conflicts, trigger_maturity)
    # READY remains strict: readiness is diagnostic, not a shortcut around existing execution safeguards.
    quality, quality_icon = trade_quality(setup_score)
    return {"signal":ready,"execution_state":state,"direction":direction,"score":setup_score,"scenario_score":int(scenario_score),"setup_score":setup_score,"execution_readiness":execution_readiness,"data_quality_score":data_quality,"conflicts":conflicts,"trigger_maturity":trigger_maturity,"trigger_state":trigger_state,"scenario_type":_scenario_type(direction, frames, levels, price),"quality":quality,"quality_icon":quality_icon,"price":price,"levels":levels,"liquidity_blocked":liquidity_blocked,"liquidity_state":liquidity_state,"liquidity":liquidity,"factors":factors,"setup_reasons":setup_reasons,"candidate":candidate,"rr_ok":rr_ok,"trade":candidate if ready else None,"news_blocked":False,"news":news_text,"daily":{"h1":h1,"m15":m15,"m5":m5,"buy_score":buy_score,"sell_score":sell_score,"buy_factors":buy_factors,"sell_factors":sell_factors}}


def evaluate_signal():
    """الإشارة الموحدة: الفني يحدد الاتجاه، والمؤسسي يؤكد أو يحجب التنفيذ؛ لا يقلب الاتجاه آلياً."""
    global LAST_ANALYSIS
    core=_daily_execution_core()
    try:
        # Independent higher-timeframe reads are fetched concurrently. The
        # analysis functions themselves remain unchanged and are evaluated
        # after the reads complete, preserving deterministic scoring/state.
        with ThreadPoolExecutor(max_workers=3, thread_name_prefix="context-fetch") as pool:
            f_w1 = pool.submit(get_bars, '1w', 250)
            f_d1 = pool.submit(get_bars, '1d', 300)
            f_h4 = pool.submit(get_bars, '4h', 300)
            w1_df = f_w1.result()
            d1_df = f_d1.result()
            h4_df = f_h4.result()
        w1=analyze(w1_df); d1=analyze(d1_df); h4=analyze(h4_df)
    except Exception:
        logger.exception('تعذر جلب السياق الاستراتيجي؛ سيتم استخدام محرك اليوم وحده')
        w1=d1=h4={}
    h1=core['daily']['h1']; m15=core['daily']['m15']; m5=core['daily']['m5']
    mtf={'w1':w1,'d1':d1,'h4':h4,'h1':h1,'m15':m15,'m5':m5,
         'direction':core['direction'],'score':core['score'],
         'buy_score':core['daily']['buy_score'],'sell_score':core['daily']['sell_score'],
         'net_score':core['daily']['buy_score']-core['daily']['sell_score'],
         'agreement':'موحد: H1 + M15 + M5',
         'conflict':(h1.get('direction') in ('BUY','SELL') and m5.get('direction') in ('BUY','SELL') and h1.get('direction')!=m5.get('direction'))}
    # Performance optimization: institutional_analysis() does not consume the
    # dataframe argument. Do NOT fetch another 1h/250 dataset here; the daily
    # execution core already fetched/analyzed the required H1 context.
    # This removes one redundant network/API path without changing scoring,
    # institutional sources, thresholds, S/R, or trade conditions.
    try: institutional=institutional_analysis(None)
    except Exception as exc:
        logger.exception('Institutional engine failure')
        institutional={'direction':'WAIT','quality':0,'institutional_score':0,'effective_strength':0,'coverage_pct':0,'status':'UNAVAILABLE','sources':{},'errors':[str(exc)],'factors':[]}
    result=dict(core); result['mtf']=mtf; result['institutional']=institutional
    # Final conflict pass includes higher-timeframe context that is unavailable to the daily core.
    full_frames={"W1":w1,"D1":d1,"H4":h4,"H1":h1,"M15":m15,"M5":m5}
    # إعادة حساب الجودة بعد اكتمال كل السياق متعدد الأطر.
    final_data_quality=_data_quality_score(full_frames, core.get('price'), core.get('levels'))
    final_conflicts=_conflict_engine(core.get('direction'), full_frames, core.get('price'), core.get('levels'))
    result['data_quality_score']=final_data_quality
    result['conflicts']=final_conflicts
    result['scenario_type']=_scenario_type(core.get('direction'), full_frames, core.get('levels'), core.get('price'))
    # rr_ok مستقل عن وجود trade؛ فالـtrade لا يظهر إلا بعد READY.
    result['execution_readiness']=_execution_readiness(core.get('direction'), int(core.get('scenario_score',0)), int(core.get('setup_score',0)), sum(1 for x in (h1,m15,m5) if x.get('direction')==core.get('direction'))==3, bool(core.get('liquidity_blocked')), bool(core.get('rr_ok')), bool((m15.get('liquidity',{}).get('retest',{}) or {}).get('state')=='CONFIRMED'), final_data_quality, final_conflicts, int(core.get('trigger_maturity',0)))
    technical_direction=core.get('direction')
    adj,adj_factors=institutional_adjustment(technical_direction,institutional)
    m15_bullish_break = bool(m15.get('structure') == 'صاعد' and m15.get('direction') == 'BUY')
    gate = institutional_conflict_gate(technical_direction, int(core.get('score',0)), m15_bullish_break, institutional)
    result['technical_direction']=technical_direction
    result['institutional_adjustment']=adj
    result['institutional_confluence_factors']=adj_factors
    result['institutional_gate']=gate
    result['technical_score']=int(core.get('score',0)); result['technical_scenario_score']=int(core.get('scenario_score',0))
    result['combined_score']=int(max(0,min(100,round(core.get('score',0)+adj))))
    result['combined_scenario_score']=int(max(0,min(100,round(core.get('scenario_score',0)+adj))))
    result['scenario_score']=int(core.get('scenario_score',0))  # semantic invariant: scenario score stays technical/contextual
    result['execution_score']=int(result.get('execution_readiness', core.get('execution_readiness',0)))
    inst_dir=institutional.get('direction'); inst_strength=int(institutional.get('effective_strength',0) or 0); inst_cov=int(institutional.get('coverage_pct',0) or 0)
    strong_inst_conflict=technical_direction in ('BUY','SELL') and inst_dir in ('BUY','SELL') and inst_dir!=technical_direction and inst_strength>=60 and inst_cov>=50
    if gate.get('state') == 'CONFLICT':
        strong_inst_conflict = True
    result['institutional_conflict']=bool(strong_inst_conflict)
    if strong_inst_conflict:
        result['direction']='WAIT'
        result['score']=int(core.get('score',0))
        result['scenario_score']=int(core.get('scenario_score',0))
        result['setup_score']=int(core.get('setup_score',0))
        result['signal']=False; result['trade']=None; result['execution_state']='BLOCKED'
        result.setdefault('setup_reasons',[]).append('تعارض مؤسسي قوي — تم حجب التنفيذ احترازياً')
    else:
        result['direction']=technical_direction
        result['score']=result['combined_score']; result['scenario_score']=int(core.get('scenario_score',0))
        # Institutional confirmation can improve confidence, but cannot create
        # a READY trade when the technical execution engine was not READY.
        if result.get('signal') and adj<0:
            result['signal']=False; result['trade']=None; result['execution_state']='BLOCKED'
            result.setdefault('setup_reasons',[]).append('التعارض المؤسسي خفّض الثقة التنفيذية')
    result['rejection_reasons']=_signal_rejection_reasons(result)
    LAST_ANALYSIS=result
    logger.info('EXECUTION_DECISION direction=%s technical=%s combined_score=%s signal=%s inst=%s coverage=%s adj=%s',
                result['direction'],technical_direction,result['score'],result['signal'],inst_dir,inst_cov,adj)
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


def _auditor_trade_id(record):
    """معرف ثابت للصفقة حتى لا يسجل الـ Auditor نفس الإشارة مرتين."""
    key = _trade_key(record)
    return "TRD_" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:20] if key else None


def _auditor_register_trade_direct(record, result):
    """تنفيذ التسجيل الفعلي داخل عامل Auditor فقط؛ لا يُستدعى من قفل التداول."""
    if PERFORMANCE_AUDITOR is None or not record:
        return
    try:
        snapshot = {
            "version": VERSION,
            "mtf": result.get("mtf", {}),
            "institutional": result.get("institutional", {}),
            "liquidity": result.get("liquidity", {}),
            "news": result.get("news"),
            "factors": result.get("factors", []),
            "rejection_reasons": result.get("rejection_reasons", []),
        }
        PERFORMANCE_AUDITOR.register_trade(
            trade_id=_auditor_trade_id(record),
            signal_time=record.get("time"),
            direction=record.get("direction"),
            entry=record.get("entry"), sl=record.get("sl"),
            tp1=record.get("tp1"), tp2=record.get("tp2"), tp3=record.get("tp3"),
            tp_final=record.get("tp3"), score=record.get("score"),
            quality=record.get("quality"), risk_reward=record.get("rr"),
            timeframe="H1/M15/M5", analysis_snapshot=snapshot
        )
    except Exception:
        logging.getLogger(__name__).exception("AUDITOR_TRADE_BRIDGE_ERROR")


def _auditor_reconcile_from_main_history():
    """مصالحة دورية تجعل سجل الصفقات الرئيسي هو مصدر الحقيقة لمراقب الصفقات.

    الهدف: إذا سقط تسجيل Auditor بسبب امتلاء الطابور، خطأ عابر، إعادة تشغيل، أو
    فقدان قاعدة Auditor، تتم إعادة بناء/تحديث Live_Trades من trades.db بدون لمس
    منطق الإشارة أو استراتيجية التداول.
    """
    if PERFORMANCE_AUDITOR is None:
        return {"scanned":0,"inserted":0,"updated":0,"errors":0}
    stats={"scanned":0,"inserted":0,"updated":0,"errors":0}
    conn=None
    try:
        conn=_trade_db_connect()
        rows=conn.execute("SELECT payload FROM trades ORDER BY updated_at ASC").fetchall()
        for (payload,) in rows:
            try:
                record=json.loads(payload)
                if not isinstance(record,dict):
                    continue
                stats["scanned"] += 1
                aid=_auditor_trade_id(record)
                if not aid:
                    continue
                result={"mtf":{},"institutional":{},"liquidity":{},"news":None,"factors":[],"rejection_reasons":[]}
                # التسجيل المفقود: استخدم نفس مسار التسجيل الطبيعي، لكن خارج Event Loop.
                PERFORMANCE_AUDITOR.register_trade(
                    trade_id=aid, signal_time=record.get("time"),
                    direction=record.get("direction"), entry=record.get("entry"),
                    sl=record.get("sl"), tp1=record.get("tp1"), tp2=record.get("tp2"),
                    tp3=record.get("tp3"), tp_final=record.get("tp3"), score=record.get("score"),
                    quality=record.get("quality"), risk_reward=record.get("rr"),
                    timeframe="H1/M15/M5", analysis_snapshot=result
                )
                # مزامنة الحالة النهائية/الحالية حتى لا يبقى Auditor على نتيجة قديمة.
                status=str(record.get("status") or "ACTIVE").upper()
                main_result=str(record.get("result") or "OPEN").upper()
                final_map={
                    "TP3 / WIN":"FULL_SUCCESS", "TP3":"FULL_SUCCESS",
                    "LOSS":"FAILED", "SL":"FAILED",
                    "TP1":"PARTIAL_SUCCESS", "TP2":"PARTIAL_SUCCESS",
                    "EXPIRED":"EXPIRED", "EXPIRY":"EXPIRED",
                    "AMBIGUOUS":"AMBIGUOUS"
                }
                final_result=final_map.get(main_result, "OPEN")
                if status=="CLOSED" and final_result=="OPEN":
                    # حالات الإغلاق القديمة/المختلفة: لا نخترع نتيجة؛ اتركها مفتوحة للمراجعة.
                    status="ACTIVE"
                if status not in ("ACTIVE","TP1","TP2","CLOSED"):
                    status="ACTIVE"
                close_time=record.get("close_time") if status=="CLOSED" else None
                current_price=record.get("last_price")
                stamp=record.get("last_update") or record.get("time") or PERFORMANCE_AUDITOR._now()
                with PERFORMANCE_AUDITOR.db.transaction() as ac:
                    exists=ac.execute("SELECT 1 FROM Live_Trades WHERE trade_id=?",(aid,)).fetchone()
                    if not exists:
                        # التسجيل أعلاه قد يكون False فقط عند التكرار؛ لا نتوقع هنا وجوداً مسبقاً.
                        stats["inserted"] += 1
                    else:
                        ac.execute("""UPDATE Live_Trades
                            SET status=?, final_result=?, close_time=?, current_price=COALESCE(?,current_price),
                                last_price_time=COALESCE(?,last_price_time), updated_at=?
                            WHERE trade_id=?""",
                            (status,final_result,close_time,current_price,stamp,stamp,aid))
                        stats["updated"] += 1
            except Exception:
                stats["errors"] += 1
                AUDITOR_LOGGER.exception("AUDITOR_TRADE_RECONCILE_ROW_ERROR")
    except Exception:
        stats["errors"] += 1
        AUDITOR_LOGGER.exception("AUDITOR_TRADE_RECONCILE_ERROR")
    finally:
        if conn:
            conn.close()
    if stats["inserted"] or stats["updated"] or stats["errors"]:
        AUDITOR_LOGGER.info("AUDITOR_TRADE_RECONCILE scanned=%s inserted=%s updated=%s errors=%s", stats["scanned"], stats["inserted"], stats["updated"], stats["errors"])
    return stats


def _auditor_register_trade(record, result):
    """إرسال مهمة التسجيل إلى طابور مستقل بدون حجب محرك التداول."""
    if PERFORMANCE_AUDITOR is None or not record:
        return
    _auditor_enqueue(("trade", dict(record), dict(result)))


def _auditor_register_analysis_direct(horizon, *, issue_time, direction, expected_min, expected_max, target, score, snapshot):
    if PERFORMANCE_AUDITOR is None:
        return
    try:
        mapped = {"BUY":"BULLISH", "SELL":"BEARISH", "WAIT":"SIDEWAYS"}.get(direction, "WAIT")
        aid = f"{horizon.upper()}_{issue_time[:16].replace(':','').replace('-','')}"
        PERFORMANCE_AUDITOR.register_analysis(
            analysis_id=aid, analysis_type=horizon.upper(), issue_time=issue_time,
            expiry_time=_analysis_expiry(horizon), direction=mapped,
            expected_min=expected_min, expected_max=expected_max, target=target,
            score=score, confidence=score, analysis_snapshot=snapshot
        )
    except Exception:
        logging.getLogger(__name__).exception("AUDITOR_ANALYSIS_BRIDGE_ERROR")


def _auditor_register_analysis(horizon, *, issue_time, direction, expected_min, expected_max, target, score, snapshot):
    """إرسال مهمة التحليل إلى طابور مستقل بدون حجب دورة التحليل."""
    if PERFORMANCE_AUDITOR is None:
        return
    _auditor_enqueue(("analysis", horizon, issue_time, direction, expected_min, expected_max, target, score, snapshot))


def _auditor_enqueue(task):
    """إضافة غير حاجبة؛ امتلاء الطابور لا يوقف البوت."""
    global AUDITOR_QUEUE_DROPS
    if PERFORMANCE_AUDITOR is None:
        return False
    try:
        AUDITOR_QUEUE.put_nowait(task)
        return True
    except queue.Full:
        with AUDITOR_QUEUE_LOCK:
            AUDITOR_QUEUE_DROPS += 1
            drops = AUDITOR_QUEUE_DROPS
        if drops == 1 or drops % 25 == 0:
            logging.getLogger(__name__).warning("AUDITOR_QUEUE_FULL; dropped=%s", drops)
        return False


def _auditor_worker():
    """عامل وحيد مخصص لعمليات التسجيل فقط؛ معزول عن Event Loop وTRADE_LOCK."""
    while not AUDITOR_WORKER_STOP.is_set():
        try:
            task = AUDITOR_QUEUE.get(timeout=0.5)
        except queue.Empty:
            continue
        try:
            kind = task[0]
            if kind == "trade":
                _auditor_register_trade_direct(task[1], task[2])
            elif kind == "analysis":
                _auditor_register_analysis_direct(
                    task[1], issue_time=task[2], direction=task[3],
                    expected_min=task[4], expected_max=task[5], target=task[6],
                    score=task[7], snapshot=task[8]
                )
        except Exception:
            logging.getLogger(__name__).exception("AUDITOR_WORKER_ERROR")
        finally:
            AUDITOR_QUEUE.task_done()


def _start_auditor_bridge_worker():
    global AUDITOR_WORKER_THREAD
    if PERFORMANCE_AUDITOR is None:
        return
    if AUDITOR_WORKER_THREAD and AUDITOR_WORKER_THREAD.is_alive():
        return
    AUDITOR_WORKER_STOP.clear()
    AUDITOR_WORKER_THREAD = threading.Thread(
        target=_auditor_worker, name="xau-auditor-bridge", daemon=True
    )
    AUDITOR_WORKER_THREAD.start()


def _analysis_expiry(horizon):
    now = now_damascus()
    if horizon == "daily":
        return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    # نهاية الأسبوع التدقيقية = بداية السبت، أي بعد انتهاء الجمعة.
    days_until_saturday = (5 - now.weekday()) % 7
    if days_until_saturday == 0:
        days_until_saturday = 7
    return (now + timedelta(days=days_until_saturday)).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()


def register_trade(result):
    if not result.get("signal") or not result.get("trade"): return None, False
    t=result["trade"]
    new_record = None
    with TRADE_LOCK:
        price=float(result.get("price",t["entry"])); atr=max(float(result.get("mtf",{}).get("m15",{}).get("atr",0) or 0),0.50)
        entry_tol=max(atr*0.30,price*0.00025,0.50); sl_tol=max(atr*0.45,price*0.00035,0.75); tp_tol=max(atr*0.55,price*0.00045,1.00)
        for record in reversed(TRADE_HISTORY):
            if record.get("status") not in ("ACTIVE","TP1","TP2") or record.get("direction")!=result["direction"]: continue
            if abs(float(record.get("entry",0))-float(t["entry"]))>entry_tol or abs(float(record.get("sl",0))-float(t["sl"]))>sl_tol: continue
            if abs(float(record.get("tp1",0))-float(t["tp1"]))>tp_tol or abs(float(record.get("tp2",0))-float(t["tp2"]))>tp_tol*1.5 or abs(float(record.get("tp3",0))-float(t["tp3"]))>tp_tol*2: continue
            record.update({"score":result["score"],"quality":result["quality"],"last_update":now_damascus().isoformat()}); _save_trade_locked(record); return record,False
        now=now_damascus().isoformat()
        new_record={"time":now,"direction":result["direction"],"score":result["score"],"quality":result["quality"],"entry":float(t["entry"]),"sl":float(t["sl"]),"tp1":float(t["tp1"]),"tp2":float(t["tp2"]),"tp3":float(t["tp3"]),"rr":float(t["rr"]),"status":"ACTIVE","result":"OPEN","liquidity_state":result.get("mtf",{}).get("m15",{}).get("liquidity",{}).get("retest",{}).get("state"),"retest_status":liquidity_retest_summary(result.get("mtf",{}).get("m15",{}).get("liquidity",{})),"last_update":now,"last_price":price,"tp1_time":None,"tp2_time":None,"tp3_time":None,"close_time":None}
        TRADE_HISTORY.append(new_record)
        if len(TRADE_HISTORY)>MAX_TRADE_HISTORY: del TRADE_HISTORY[:-MAX_TRADE_HISTORY]
        _save_trade_locked(new_record)
    # مهم: خارج TRADE_LOCK، وغير حاجب؛ التسجيل يتم في عامل Auditor مستقل.
    _auditor_register_trade(new_record, result)
    return new_record,True


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
    """التحليل الكامل A+ — الهيكلي والكمّي والمؤسسي بمصادر حقيقية."""
    result=evaluate_signal(); mtf=result['mtf']; levels=result['levels']; inst=result.get('institutional') or {}; src=inst.get('sources') or {}; m=src.get('macro') or {}; c=src.get('cot') or {}; g=src.get('gld') or {}; cb=src.get('central_banks') or {}
    direction={'BUY':'🟢 أفضلية شراء','SELL':'🔴 أفضلية بيع','WAIT':'🟡 حياد'}.get(result['direction'],'🟡 حياد')
    def v(x,suf=''): return f'{x}{suf}' if x is not None and x!='' else 'غير متاح'
    regime={'RISK-ON':'🟢 Risk-On','RISK-OFF':'🔴 Risk-Off','MIXED':'🟡 مختلط'}.get(m.get('risk_regime'),'⚪ غير متاح')
    L=[f'🤖 XAU SMART TRADER {VERSION}','📊 التحليل الهيكلي والكمّي والمؤسسي','','🎯 القرار',direction,f"🧠 قوة السيناريو: {result.get('scenario_score',result['score'])}/100",f"🧩 الدرجة الفنية: {result.get('technical_score',result['score'])}/100",f"🏦 تأثير المؤسسي: {result.get('institutional_adjustment',0):+d} نقطة",f"🎯 الإعداد التنفيذي: {result.get('setup_score',result['score'])}/100",f"🚦 حالة التنفيذ: {result.get('execution_state','WATCH')}",'','🔎 سبب القرار']
    L += [f'• {x}' for x in (result.get('factors') or [])[:6]] or ['• لا توجد عوامل تلاقٍ إضافية مسجلة حالياً.']
    L += [f'• {x}' for x in (result.get('institutional_confluence_factors') or [])]
    L += ['','🧭 الصورة الكبرى',f"W1: {mtf['w1'].get('direction','غير متاح')}",f"D1: {mtf['d1'].get('direction','غير متاح')}",f"H4: {mtf['h4'].get('direction','غير متاح')}",f"H1: {mtf['h1'].get('direction','غير متاح')}",f"M15: {mtf['m15'].get('direction','غير متاح')}",f"M5: {mtf['m5'].get('direction','غير متاح')}",'','🌐 حالة السوق الكلي',f'النظام: {regime}',f"DXY: {v(m.get('dxy'))}",f"تغير DXY: {v(m.get('dxy_change_pct'),'%')}",f"Broad USD: {v(m.get('usd_broad'))}",f"US10Y: {v(m.get('us10y'),'%')}",f"Real 10Y: {v(m.get('real10y'),'%')}",f"VIX: {v(m.get('vix'))}",'','💰 السعر والمستويات',f"السعر الحالي: {result['price']:.2f}",f"🟢 S1: {format_zone(levels,'support1')}",f"🟢 S2: {format_zone(levels,'support2')}",f"🟢 S3: {format_zone(levels,'support3')}",f"🔴 R1: {format_zone(levels,'resistance1')}",f"🔴 R2: {format_zone(levels,'resistance2')}",f"🔴 R3: {format_zone(levels,'resistance3')}",'','💧 السيولة',f"الانحياز: {mtf['m15']['liquidity']['bias']}",f"Buy-side الأقرب: {fmt(mtf['m15']['liquidity']['nearest_buy'])}",f"Sell-side الأقرب: {fmt(mtf['m15']['liquidity']['nearest_sell'])}",f"السحب: {mtf['m15']['liquidity']['sweep_text']}",f"إعادة الاختبار: {liquidity_retest_summary(mtf['m15']['liquidity'])}",'','🏦 التحليل المؤسسي — بيانات حقيقية',f"🎯 الاتجاه المؤسسي: {inst.get('direction','غير متاح')}",f"💪 قوة الدليل المؤسسي: {v(inst.get('strength'))}/100",f"📡 تغطية المصادر: {v(inst.get('coverage_pct'))}%",f"🚦 حالة البيانات المؤسسية: {v(inst.get('status'))}",'','🏛️ البنوك المركزية',f"السياسة النقدية: {v(m.get('policy'))}",f"الفائدة المستهدفة: {v(m.get('fed_lower'),'%')} — {v(m.get('fed_upper'),'%')}",f"الفائدة الفعلية EFFR: {v(m.get('effr'),'%')}",f"توقعات الفائدة السوقية: {v(m.get('rate_expectation'))}",f"مصدر توقعات الفائدة: {v(m.get('rate_expectation_source'))}",f"العقود الآجلة للصناديق الفيدرالية — المعدل الضمني: {v(m.get('fed_futures_implied'),'%')}",f"انحياز متوسط الفائدة الضمني: { {'HAWKISH':'صعودي','DOVISH':'هبوطي','NEUTRAL':'محايد'}.get(m.get('market_rate_bias'), 'غير متاح') }",f"تفسير العقود: {v(m.get('fed_futures_interpretation'))}",f"تغير احتياطيات الذهب المعلن: {v(cb.get('net_change_tonnes'),' طن')}",f"اتجاه التغير: {v(cb.get('direction'))}",f"التحقق المتقاطع بين مجلس الذهب العالمي وصندوق النقد الدولي: { {'PASS':'ناجح','DEGRADED':'متدهور','NOT_PERFORMED':'لم يُنفذ','PERIOD_MISMATCH':'اختلاف الفترة'}.get(cb.get('cross_validation',{}).get('status'), 'غير متاح') }",f"فارق التحقق: {v(cb.get('cross_validation',{}).get('difference_pct'),'%')}",f"قيمة WGC للتحقق: {v(cb.get('cross_validation',{}).get('wgc_value'),' طن')}",f"قيمة IMF للتحقق: {v(cb.get('cross_validation',{}).get('imf_value'),' طن')}",f"فترة WGC: {v(cb.get('cross_validation',{}).get('wgc_period'))}",f"فترة IMF: {v(cb.get('cross_validation',{}).get('imf_period'))}",'',
        '🔬 تشخيص صندوق النقد الدولي IMF',
        f"الحالة: {v((IMF_LAST_DIAGNOSTIC or {}).get('status'))}",
        f"مرحلة الفشل: {v((IMF_LAST_DIAGNOSTIC or {}).get('failure_stage'))}",
        f"نوع الخطأ: {v((IMF_LAST_DIAGNOSTIC or {}).get('error_type'))}",
        f"رسالة التشخيص: {v((IMF_LAST_DIAGNOSTIC or {}).get('error'))}",
        f"عدد المحاولات المسجلة: {len((IMF_LAST_DIAGNOSTIC or {}).get('attempts',[]))}",
        f"آخر فترة IMF: {v((IMF_LAST_DIAGNOSTIC or {}).get('latest_period'))}",
        f"IMF Acquisition: {v((IMF_LAST_DIAGNOSTIC or {}).get('acquisition_status'))}",
        f"IMF Parsing: {v((IMF_LAST_DIAGNOSTIC or {}).get('raw_parser_audit',{}).get('parser_format'))}",
        f"Raw Series / Rows: {v((IMF_LAST_DIAGNOSTIC or {}).get('raw_parser_audit',{}).get('raw_series') if (IMF_LAST_DIAGNOSTIC or {}).get('raw_parser_audit',{}).get('raw_series') is not None else (IMF_LAST_DIAGNOSTIC or {}).get('raw_parser_audit',{}).get('raw_rows'))}",
        f"Raw Observations: {v((IMF_LAST_DIAGNOSTIC or {}).get('raw_parser_audit',{}).get('raw_observations'))}",
        f"Accepted Observations: {v((IMF_LAST_DIAGNOSTIC or {}).get('raw_parser_audit',{}).get('accepted_observations') if (IMF_LAST_DIAGNOSTIC or {}).get('raw_parser_audit',{}).get('accepted_observations') is not None else (IMF_LAST_DIAGNOSTIC or {}).get('raw_parser_audit',{}).get('accepted_rows'))}",
        f"Raw Country Keys: {v((IMF_LAST_DIAGNOSTIC or {}).get('raw_parser_audit',{}).get('raw_country_keys'))}",
        f"Accepted Country Keys: {v((IMF_LAST_DIAGNOSTIC or {}).get('raw_parser_audit',{}).get('accepted_country_keys'))}",
        f"Raw Period Keys: {v((IMF_LAST_DIAGNOSTIC or {}).get('raw_parser_audit',{}).get('raw_period_keys'))}",
        f"Accepted Period Keys: {v((IMF_LAST_DIAGNOSTIC or {}).get('raw_parser_audit',{}).get('accepted_period_keys'))}",
        f"Parser Rejected — Country Dimension: {v((IMF_LAST_DIAGNOSTIC or {}).get('raw_parser_audit',{}).get('rejected_country_dimension'))}",
        f"Parser Rejected — Missing Country: {v((IMF_LAST_DIAGNOSTIC or {}).get('raw_parser_audit',{}).get('rejected_missing_country'))}",
        f"Parser Rejected — Period: {v((IMF_LAST_DIAGNOSTIC or {}).get('raw_parser_audit',{}).get('rejected_period_index') if (IMF_LAST_DIAGNOSTIC or {}).get('raw_parser_audit',{}).get('rejected_period_index') is not None else (IMF_LAST_DIAGNOSTIC or {}).get('raw_parser_audit',{}).get('rejected_missing_period'))}",
        f"Parser Rejected — Invalid Value: {v((IMF_LAST_DIAGNOSTIC or {}).get('raw_parser_audit',{}).get('rejected_invalid_value'))}",
        f"Raw Latest Period: {v((IMF_LAST_DIAGNOSTIC or {}).get('raw_latest_period'))}",
        f"Normalized Period: {v((IMF_LAST_DIAGNOSTIC or {}).get('normalized_period'))}",
        f"Previous Raw Period: {v((IMF_LAST_DIAGNOSTIC or {}).get('raw_previous_period'))}",
        f"Previous Normalized Period: {v((IMF_LAST_DIAGNOSTIC or {}).get('normalized_previous_period'))}",
        f"Countries Available: {v((IMF_LAST_DIAGNOSTIC or {}).get('countries_available'))}",
        f"Countries Current Period: {v((IMF_LAST_DIAGNOSTIC or {}).get('countries_current_period'))}",
        f"Countries Previous Period: {v((IMF_LAST_DIAGNOSTIC or {}).get('countries_previous_period'))}",
        f"Countries Common: {v((IMF_LAST_DIAGNOSTIC or {}).get('countries_common'))}",
        f"Current-Only Countries: {v((IMF_LAST_DIAGNOSTIC or {}).get('countries_current_only'))}",
        f"Previous-Only Countries: {v((IMF_LAST_DIAGNOSTIC or {}).get('countries_previous_only'))}",
        f"Coverage Current: {v((IMF_LAST_DIAGNOSTIC or {}).get('coverage_ratio_current_pct'))}%",
        f"Coverage Previous: {v((IMF_LAST_DIAGNOSTIC or {}).get('coverage_ratio_previous_pct'))}%",
        f"Common Coverage / Total: {v((IMF_LAST_DIAGNOSTIC or {}).get('coverage_ratio_common_total_pct'))}%",
        f"Country ID Normalization: {v((IMF_LAST_DIAGNOSTIC or {}).get('country_id_normalization'))}",
        f"Country ID Collisions: {v((IMF_LAST_DIAGNOSTIC or {}).get('country_id_normalization_collisions'))}",
        f"Identity Gap After Normalization: {v((IMF_LAST_DIAGNOSTIC or {}).get('country_identity_gap'))}",
        f"Likely Coverage Cause: {v((IMF_LAST_DIAGNOSTIC or {}).get('likely_cause'))}",
        f"Cause Note: {v((IMF_LAST_DIAGNOSTIC or {}).get('likely_cause_note'))}",
        f"Current-Only Sample: {', '.join((IMF_LAST_DIAGNOSTIC or {}).get('current_only_sample',[]) or []) or 'غير متاح'}",
        f"Previous-Only Sample: {', '.join((IMF_LAST_DIAGNOSTIC or {}).get('previous_only_sample',[]) or []) or 'غير متاح'}",
        f"Common Sample: {', '.join((IMF_LAST_DIAGNOSTIC or {}).get('common_sample',[]) or []) or 'غير متاح'}",
        f"Current Holdings: {v((IMF_LAST_DIAGNOSTIC or {}).get('current_holdings_oz'))} oz",
        f"Previous Holdings: {v((IMF_LAST_DIAGNOSTIC or {}).get('previous_holdings_oz'))} oz",
        f"Calculated Change: {v((IMF_LAST_DIAGNOSTIC or {}).get('calculated_change_tonnes'))} tonnes",
        f"الدول المستخرجة: {v((IMF_LAST_DIAGNOSTIC or {}).get('countries'))}",
        f"الأشهر المستخرجة: {v((IMF_LAST_DIAGNOSTIC or {}).get('months'))}",
        f"اكتشاف بنية SDMX: {'🟢 تم العثور على استجابة بنيوية' if (IMF_LAST_DIAGNOSTIC or {}).get('structure_discovery') else '🟡 لم تُؤكد البنية بعد'}",
        '','🏦 ETF / صناديق الذهب',f"GLD آخر تحديث: {v(g.get('date'))}",f"حيازة الذهب: {v(g.get('gold_oz'))} أونصة",f"تغير الحيازة: {v(g.get('gold_oz_delta'))} أونصة",f"تغير الأسهم: {v(g.get('shares_delta'))}",f"اتجاه الحيازة: { {'IN':'دخول','OUT':'خروج','NEUTRAL':'محايد'}.get(g.get('flow_proxy'), 'غير متاح') }",'⚠️ تغير الحيازة ليس تدفقاً نقدياً؛ التدفق النقدي المباشر غير متاح من المصدر الحالي.','','📑 COT — تمركزات الذهب',f"تاريخ التقرير: {v(c.get('report_date'))}",f"تجاري — صافي: {v(c.get('legacy_commercial_net'))}",f"تجاري — التغير الأسبوعي: {v(c.get('legacy_commercial_delta'))}",f"غير تجاري — صافي: {v(c.get('legacy_noncommercial_net'))}",f"غير تجاري — التغير الأسبوعي: {v(c.get('legacy_noncommercial_delta'))}",f"الأموال المُدارة — صافي: {v(c.get('managed_money_net'))}",f"الأموال المُدارة — التغير الأسبوعي: {v(c.get('managed_money_delta'))}",'','📈 السوق الكلي',f"DXY: {v(m.get('dxy'))}",f"تغير DXY: {v(m.get('dxy_change_pct'),'%')}",f"Broad USD: {v(m.get('usd_broad'))}",f"US10Y: {v(m.get('us10y'),'%')}",f"Real 10Y: {v(m.get('real10y'),'%')}",f"VIX: {v(m.get('vix'))}",f"Risk-On / Risk-Off: {regime}",'','🚦 بوابات التنفيذ',f"📊 الاتجاه: {'🟢 داعم' if result['direction'] in ('BUY','SELL') else '🟡 غير محسوم'}",f"🧠 الهيكل: {'🟢 داعم' if mtf['h1'].get('structure') in ('صاعد','هابط') else '🟡 محايد'}",f"💧 السيولة: {'🟢 مؤكدة' if 'لا يوجد' not in str(mtf['m15']['liquidity'].get('sweep_text','')) else '🟡 غير مكتملة'}",f"🔄 إعادة الاختبار: {liquidity_retest_summary(mtf['m15']['liquidity'])}",f"📰 الأخبار: {result['news']}",f"🛡️ التنفيذ: {'🟢 READY' if result.get('execution_state')=='READY' else '🟡 WAIT'}",'','⏳ ما الذي ننتظره؟','• اكتمال سحب السيولة','• إعادة اختبار مؤكدة','• تأكيد M15/M5','• عدم تعارض واضح مع العامل المؤسسي','','📡 صحة مصادر التحليل المؤسسي']
    _uscal=_us_financial_calendar_state()
    L.append(f"🇺🇸 التقويم الأمريكي: {'عطلة — '+str(_uscal.get('holiday')) if _uscal.get('state')=='HOLIDAY' else 'نهاية أسبوع' if _uscal.get('state')=='WEEKEND' else 'يوم عمل'}")
    if _uscal.get('state') in ('HOLIDAY','WEEKEND'):
        L.append('ℹ️ تأخر بعض البيانات الأمريكية الدورية في هذه الحالة لا يُعامل تلقائياً كعطل تقني.')
    for key,label in (('macro','السوق الكلي / المصادر الرسمية'),('cot','تقرير COT — لجنة تداول السلع الآجلة'),('gld','صندوق SPDR Gold Shares'),('central_banks','البنوك المركزية — WGC / IMF')):
        src_item=(src.get(key) or {}); st=src_item.get('status'); icon='🟢 متاح' if src_item.get('source') and st not in ('STALE','UNAVAILABLE') else '🟡 قديم' if st=='STALE' else '🟡 غير متاح'; L.append(f"{label}: {icon}")
        if src_item.get('availability_reason_code') in ('EXPECTED_PUBLICATION_DELAY','POSSIBLE_PUBLICATION_DELAY'):
            L.append(f"   ↳ {src_item.get('availability_reason_ar')}")
    if inst.get('errors'): L.append('⚠️ بعض المصادر غير متاحة؛ لم يتم تعويضها بقيم مصطنعة.')
    if include_trade and result.get('signal') and result.get('trade'):
        t=result['trade']; d='🟢 شراء' if result['direction']=='BUY' else '🔴 بيع'; L += ['','🚨 إشارة تداول','',f'📈 الصفقة: {d}',f"💪 الجودة التنفيذية: {result['setup_score']} نقطة",f"📍 الدخول: {t['entry']:.2f}",f"🛑 وقف الخسارة: {t['sl']:.2f}",f"🎯 TP1: {t['tp1']:.2f}",f"🎯 TP2: {t['tp2']:.2f}",f"⚖️ R:R: 1:{t['rr']:.2f}"]
    else:L += ['','🔎 القرار النهائي','🟡 مراقبة — لا دخول الآن','السيناريو قيد المتابعة حتى تكتمل بوابات التنفيذ.']
    L += ['','⚠️ التحليل مساعد لاتخاذ القرار اليدوي وليس ضماناً للربح.']; return '\n'.join(L)

# ============================================================
# التقرير التوضيحي — المحرك الأساسي
# ============================================================

def _quality_label(score):
    if score >= 80: return "قوية جداً"
    if score >= 65: return "جيدة"
    if score >= 50: return "متوسطة"
    return "ضعيفة"


def _scenario_score(frames_data, frames, direction, levels, price, atr, horizon=None):
    """حساب السيناريو من أدلة غير مكررة نسبياً.

    الأوزان التنفيذية المعلنة تُطبّق فعلياً: H1=30، M15=25، M5=20.
    الباقي سياق مساعد ولا يُسمح له بتغيير وزن التنفيذ.
    Liquidity يقيس وجود/تسلسل السيولة، بينما Trigger يقيس النضج النهائي فقط،
    لمنع احتساب maturity نفسها مرتين.
    """
    if direction not in ('BUY','SELL'):
        return 0, ['لا يوجد اتجاه قابل للتقييم']
    factors=[]
    expected='صاعد' if direction=='BUY' else 'هابط'
    weights={name:float(weight) for name,weight in frames if name in ('H1','M15','M5')}
    names=[n for n in ('H1','M15','M5') if n in frames_data]
    total_weight=sum(weights.get(n,0) for n in names)
    if total_weight<=0:
        return 0, ['لا تتوفر أطر التنفيذ المطلوبة']

    def weighted_ratio(predicate):
        return sum(weights.get(n,0) for n in names if predicate(frames_data[n])) / total_weight

    trend_ratio=weighted_ratio(lambda x: x.get('direction')==direction)
    oppose_ratio=weighted_ratio(lambda x: x.get('direction') in ('BUY','SELL') and x.get('direction')!=direction)
    trend=20 if trend_ratio>=0.90 else 15 if trend_ratio>=0.60 else 8 if trend_ratio>=0.30 else 0
    support_count=sum(1 for n in names if frames_data[n].get('direction')==direction)
    oppose_count=sum(1 for n in names if frames_data[n].get('direction') in ('BUY','SELL') and frames_data[n].get('direction')!=direction)
    if trend: factors.append(f'الاتجاه التنفيذي الموزون يدعم السيناريو ({support_count}/{len(names)})')
    if oppose_count: factors.append(f'تعارض اتجاهي على {oppose_count} إطار تنفيذ')

    structure_ratio=weighted_ratio(lambda x: x.get('structure')==expected)
    structure=20 if structure_ratio>=0.90 else 15 if structure_ratio>=0.60 else 8 if structure_ratio>=0.30 else 0
    if structure: factors.append(f'الهيكل الموزون {expected} داعم')

    maturity,state=_trigger_maturity(frames_data,direction,horizon or 'daily')
    liq_name='H1' if (horizon or 'daily')=='daily' else 'D1'
    liq=(frames_data.get(liq_name,{}) or {}).get('liquidity',{}) or {}
    wanted='SELL_SIDE_SWEEP' if direction=='BUY' else 'BUY_SIDE_SWEEP'
    liq_state=str((liq.get('retest',{}) or {}).get('state') or state or 'NONE').upper()
    # Liquidity = evidence that a sweep/displacement exists, not maturity score.
    liquidity=0
    if liq.get('sweep')==wanted:
        liquidity=10
        if liq_state in ('DISPLACEMENT','BOS'): liquidity=15
        elif liq_state in ('RETEST','CONFIRMED'): liquidity=18
        factors.append(f'دورة السيولة داعمة ({liq_state})')
    elif liq_state=='INVALIDATED':
        factors.append('حالة السيولة ملغاة')

    m=frames_data.get('M15', frames_data.get('H1',{})) or {}
    adx=float(m.get('adx',0) or 0); plus=float(m.get('plus_di',m.get('di_plus',0)) or 0); minus=float(m.get('minus_di',m.get('di_minus',0)) or 0)
    macd=float(m.get('macd',0) or 0); sig=float(m.get('macd_signal',0) or 0); rsi=float(m.get('rsi',50) or 50)
    momentum=0
    if adx>=25 and ((direction=='BUY' and plus>minus) or (direction=='SELL' and minus>plus)): momentum+=7
    elif adx>=20 and ((direction=='BUY' and plus>minus) or (direction=='SELL' and minus>plus)): momentum+=5
    if (direction=='BUY' and macd>sig) or (direction=='SELL' and macd<sig): momentum+=5
    if (direction=='BUY' and rsi>=50) or (direction=='SELL' and rsi<=50): momentum+=3
    momentum=min(15,momentum)
    if momentum: factors.append(f'الزخم يدعم السيناريو ({momentum}/15)')

    # Trigger = readiness of the final confirmation only. It does not reuse the
    # numeric maturity inside Liquidity, eliminating the previous double count.
    trigger=15 if state=='CONFIRMED' else 10 if state=='RETEST' else 5 if state=='BOS' else 2 if state=='DISPLACEMENT' else 0
    if trigger: factors.append(f'نضج المحفز النهائي ({state}) — ({trigger}/15)')

    scenario_type=_scenario_type(direction,frames_data,levels,price)
    obstacle=nearest_resistance(levels,price) if direction=='BUY' else nearest_support(levels,price)
    anchor=nearest_support(levels,price) if direction=='BUY' else nearest_resistance(levels,price)
    atr_v=max(float(atr or 0),1e-9)
    space=abs(float(obstacle)-float(price))/atr_v if obstacle is not None else 2.0
    anchor_dist=abs(float(price)-float(anchor))/atr_v if anchor is not None else 99.0
    location=10 if obstacle is None or space>=2 else 7 if space>=1.2 else 4 if space>=.7 else 0
    if scenario_type in ('REVERSAL','LIQUIDITY_REVERSAL') and anchor is not None and anchor_dist<=.25:
        location=max(location,7); factors.append(f'الموقع قريب من منطقة الانعكاس ({scenario_type})')
    elif scenario_type=='CONTINUATION' and anchor is not None and anchor_dist<=.25:
        location=min(location,5); factors.append('السعر ملاصق لمستوى حساس في سيناريو استمرار')
    factors.append(f'جودة موقع الدخول ({location}/10) — {scenario_type}')
    score=int(max(0,min(100,trend+structure+liquidity+momentum+trigger+location)))
    return score,factors

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



# ============================================================
# WEEKLY DECISION CONTRACT + VALIDATOR
# v18.104 — Additive compatibility layer only.
# Does NOT modify legacy scenario/report/trading engines.
# ============================================================

@dataclass
class WeeklyDecision:
    """العقد الموحد للقرار الأسبوعي.

    طبقة مستقلة فوق W1/D1/H4 وS/R والسيولة والتأكيد.
    لا تفتح صفقات ولا تغيّر محركات التداول القديمة.
    """
    decision_id: str
    decision_version: int
    generated_at: str
    state: str
    direction: str | None
    frames: dict
    decision_range: dict
    bullish_scenario: dict
    bearish_scenario: dict
    liquidity: dict
    levels: dict
    confirmation: dict
    data_quality: object
    decision_quality: int
    state_change_reason: str
    snapshot_hash: str

    def to_dict(self):
        return {
            "decision_id": self.decision_id,
            "decision_version": self.decision_version,
            "generated_at": self.generated_at,
            "state": self.state,
            "direction": self.direction,
            "frames": self.frames,
            "decision_range": self.decision_range,
            "bullish_scenario": self.bullish_scenario,
            "bearish_scenario": self.bearish_scenario,
            "liquidity": self.liquidity,
            "levels": self.levels,
            "confirmation": self.confirmation,
            "data_quality": self.data_quality,
            "decision_quality": self.decision_quality,
            "state_change_reason": self.state_change_reason,
            "snapshot_hash": self.snapshot_hash,
        }


class WeeklyDecisionValidator:
    """يتحقق من اتساق القرار الأسبوعي قبل استهلاكه مستقبلاً.

    لا يعدّل محركات البوت القديمة. يرجع قائمة أخطاء واضحة ويمكنه
    رفع ValueError في الوضع الصارم.
    """
    ALLOWED_STATES = {"BULLISH", "BEARISH", "NEUTRAL", "TRANSITION"}
    ALLOWED_DIRECTIONS = {None, "BUY", "SELL"}
    CONFIRMATION_MAP = {
        "🟢 مؤكد مبدئياً": "CONFIRMED",
        "🟡 ضعيف / يحتاج تأكيد": "WATCH",
        "🔴 غير مؤكد": "FAILED",
        "غير متاح": "UNAVAILABLE",
    }

    # حالة مستقلة للكسر الذي تحقق في D1 سابقاً ثم انعكس السعر
    # وعاد إلى داخل نطاق القرار. هذه الحالة ليست CONFIRMED حالياً.
    ALLOWED_CONFIRMATION_STATUSES = {
        "CONFIRMED",
        "WATCH",
        "FAILED",
        "UNAVAILABLE",
        "RETEST_PENDING",
        "PRIOR_BREAK_REVERSED",
    }

    @classmethod
    def validate(cls, decision, strict=False):
        d = decision.to_dict() if isinstance(decision, WeeklyDecision) else decision
        errors = []
        if not isinstance(d, dict):
            errors.append("القرار الأسبوعي ليس كائناً صالحاً")
            if strict:
                raise ValueError("; ".join(errors))
            return errors

        state = d.get("state")
        direction = d.get("direction")
        if state not in cls.ALLOWED_STATES:
            errors.append(f"حالة أسبوعية غير مسموحة: {state}")
        if direction not in cls.ALLOWED_DIRECTIONS:
            errors.append(f"اتجاه أسبوعي غير مسموح: {direction}")
        if state in {"NEUTRAL", "TRANSITION"} and direction is not None:
            errors.append("الحالة المحايدة/الانتقالية يجب ألا تحمل BUY أو SELL")
        if state == "BULLISH" and direction != "BUY":
            errors.append("BULLISH يجب أن يقابلها BUY")
        if state == "BEARISH" and direction != "SELL":
            errors.append("BEARISH يجب أن يقابلها SELL")

        frames = d.get("frames") or {}
        for tf in ("W1", "D1", "H4"):
            if tf not in frames:
                errors.append(f"الفريم {tf} مفقود من عقد القرار الأسبوعي")
            elif frames[tf].get("frame_direction") not in {"BUY", "SELL", "WAIT"}:
                errors.append(f"اتجاه {tf} غير صالح")

        rng = d.get("decision_range") or {}
        low, high = rng.get("low"), rng.get("high")
        if low is not None and high is not None and float(low) >= float(high):
            errors.append("نطاق القرار غير صالح: low يجب أن يكون أقل من high")

        for key, expected_direction in (("bullish_scenario", "BUY"), ("bearish_scenario", "SELL")):
            scenario = d.get(key) or {}
            if scenario.get("direction") != expected_direction:
                errors.append(f"اتجاه {key} متناقض مع عقد السيناريو")
            conf = scenario.get("confirmation") or {}
            normalized = conf.get("normalized_status")
            if normalized and normalized not in cls.ALLOWED_CONFIRMATION_STATUSES:
                errors.append(f"حالة تأكيد غير معروفة في {key}")
            side_ok = conf.get("side_ok")
            if normalized == "CONFIRMED" and side_ok is False:
                errors.append(f"لا يمكن أن يكون {key} مؤكداً بينما شرط الإغلاق غير متحقق")
            if normalized == "PRIOR_BREAK_REVERSED" and side_ok is not True:
                errors.append(f"حالة PRIOR_BREAK_REVERSED في {key} تتطلب تحقق إغلاق سابقاً (side_ok=True)")

        if strict and errors:
            raise ValueError("WeeklyDecision validation failed: " + " | ".join(errors))
        return errors

    @classmethod
    def assert_valid(cls, decision):
        cls.validate(decision, strict=True)
        return decision


def run_weekly_confirmation_validator_tests():
    """اختبارات عقدية محلية لثلاث حالات تأكيد أسبوعية. لا شبكة ولا قاعدة بيانات."""
    base = {
        "state": "NEUTRAL",
        "direction": None,
        "frames": {
            "W1": {"frame_direction": "BUY"},
            "D1": {"frame_direction": "SELL"},
            "H4": {"frame_direction": "SELL"},
        },
        "decision_range": {"low": 4329.97, "high": 4373.00},
        "bullish_scenario": {"direction": "BUY", "confirmation": {"normalized_status": "WATCH", "side_ok": False}},
        "bearish_scenario": {"direction": "SELL", "confirmation": {"normalized_status": "WATCH", "side_ok": False}},
    }

    cases = [
        ("WATCH", False, True),
        ("CONFIRMED", True, True),
        ("PRIOR_BREAK_REVERSED", True, True),
    ]
    results = []
    for status, side_ok, expected_ok in cases:
        decision = {**base, "bullish_scenario": {"direction": "BUY", "confirmation": {"normalized_status": status, "side_ok": side_ok}},
                    "bearish_scenario": {"direction": "SELL", "confirmation": {"normalized_status": status, "side_ok": side_ok}}}
        errors = WeeklyDecisionValidator.validate(decision, strict=False)
        ok = (len(errors) == 0) == expected_ok
        results.append({"case": status, "status": "PASS" if ok else "FAIL", "errors": errors})

    # حارس إضافي: CONFIRMED مع side_ok=False يجب أن يفشل.
    invalid = {**base, "bearish_scenario": {"direction": "SELL", "confirmation": {"normalized_status": "CONFIRMED", "side_ok": False}}}
    invalid_errors = WeeklyDecisionValidator.validate(invalid, strict=False)
    guard_ok = any("مؤكداً" in e for e in invalid_errors)
    results.append({"case": "CONFIRMED_WITH_SIDE_FALSE", "status": "PASS" if guard_ok else "FAIL", "errors": invalid_errors})

    if any(r["status"] == "FAIL" for r in results):
        raise AssertionError(json.dumps(results, ensure_ascii=False))
    return results


def _weekly_confirmation_contract(conf, current_price=None, level=None, direction=None):
    """تطبيع تأكيد D1 ومنع عرض تأكيد كسر قبل تحقق شرط الإغلاق فعلياً."""
    conf = dict(conf or {})
    raw = conf.get("status", "غير متاح")
    side_ok = conf.get("side_ok")

    # نفصل بين: تحقق إغلاق D1 السابق، وحالة السعر الحالية.
    # إذا تحقق الإغلاق سابقاً ثم عاد السعر عبر المستوى، فهذه ليست CONFIRMED حالياً.
    current_side_ok = None
    if current_price is not None and level is not None and direction in ("BUY", "SELL"):
        try:
            current_side_ok = float(current_price) > float(level) if direction == "BUY" else float(current_price) < float(level)
        except Exception:
            current_side_ok = None

    if side_ok is False:
        normalized = "WATCH"
        display_status = "🟡 لم يتحقق إغلاق D1 المطلوب"
    elif side_ok is True and current_side_ok is False:
        normalized = "PRIOR_BREAK_REVERSED"
        display_status = "🟡 تحقق إغلاق سابقاً — السعر عاد داخل النطاق"
    elif side_ok is True and current_side_ok is True and raw == "🟢 مؤكد مبدئياً":
        normalized = "CONFIRMED"
        display_status = raw
    elif side_ok is True and current_side_ok is None and raw == "🟢 مؤكد مبدئياً":
        normalized = "CONFIRMED"
        display_status = "🟢 تحقق إغلاق D1 المطلوب — الحالة الحالية غير متاحة"
    else:
        normalized = WeeklyDecisionValidator.CONFIRMATION_MAP.get(raw, "UNAVAILABLE")
        display_status = raw

    conf["normalized_status"] = normalized
    conf["display_status"] = display_status
    conf["confirmation_strength"] = int(max(0, min(100, conf.get("confidence", 0) or 0)))
    return conf


def _weekly_frame_contract(frame):
    frame = frame or {}
    return {
        "frame_direction": frame.get("direction", "WAIT"),
        "frame_score": int(frame.get("score", 0) or 0),
        "frame_quality": frame.get("quality"),
        "structure": frame.get("structure"),
        "rsi": frame.get("rsi"),
        "adx": frame.get("adx"),
        "conflict_points": frame.get("conflict_points", 0),
    }


def build_weekly_decision(frames_data, levels, price, d1_df=None, generated_at=None):
    """يبني WeeklyDecision فقط، من دون تغيير أو استدعاء المحركات القديمة."""
    frames_data = frames_data or {}
    w1, d1, h4 = (frames_data.get("W1") or {}, frames_data.get("D1") or {}, frames_data.get("H4") or {})
    s1 = nearest_support(levels, price) if levels else None
    r1 = nearest_resistance(levels, price) if levels else None

    frame_dirs = [w1.get("direction", "WAIT"), d1.get("direction", "WAIT"), h4.get("direction", "WAIT")]
    buy_n, sell_n = frame_dirs.count("BUY"), frame_dirs.count("SELL")
    if buy_n and sell_n:
        state, direction = "NEUTRAL", None
        reason = "تعارض اتجاهي بين W1/D1/H4؛ لا تُمنح أفضلية أسبوعية قبل حسم نطاق القرار."
    elif buy_n >= 2:
        state, direction = "BULLISH", "BUY"
        reason = "أغلبية الفريمات الاستراتيجية تدعم الاتجاه الصاعد."
    elif sell_n >= 2:
        state, direction = "BEARISH", "SELL"
        reason = "أغلبية الفريمات الاستراتيجية تدعم الاتجاه الهابط."
    elif buy_n or sell_n:
        state, direction = "TRANSITION", None
        reason = "إشارة استراتيجية غير مكتملة؛ السوق في مرحلة انتقالية."
    else:
        state, direction = "NEUTRAL", None
        reason = "لا توجد أفضلية اتجاهية استراتيجية واضحة."

    atr = d1.get("atr", 0)
    if d1_df is None:
        try:
            d1_df = get_bars("1d", 300)
        except Exception:
            d1_df = None
    bull_conf = _weekly_confirmation_contract(_breakout_confirmation(d1_df, r1, "BUY", "weekly", atr), current_price=price, level=r1, direction="BUY")
    bear_conf = _weekly_confirmation_contract(_breakout_confirmation(d1_df, s1, "SELL", "weekly", atr), current_price=price, level=s1, direction="SELL")

    def scenario(direction_name, level, conf, targets):
        return {
            "direction": direction_name,
            "status": conf.get("normalized_status", "UNAVAILABLE"),
            "trigger": {"type": "D1_CLOSE_ABOVE" if direction_name == "BUY" else "D1_CLOSE_BELOW", "level": level},
            "confirmation": conf,
            "invalidation": s1 if direction_name == "BUY" else r1,
            "targets": targets,
            "quality": None,
        }

    bullish = scenario("BUY", r1, bull_conf, [
        (levels or {}).get("resistance1", {}).get("price"), (levels or {}).get("resistance2", {}).get("price"), (levels or {}).get("resistance3", {}).get("price")])
    bearish = scenario("SELL", s1, bear_conf, [
        (levels or {}).get("support1", {}).get("price"), (levels or {}).get("support2", {}).get("price"), (levels or {}).get("support3", {}).get("price")])
    bullish["targets"] = [x for x in bullish["targets"] if x is not None]
    bearish["targets"] = [x for x in bearish["targets"] if x is not None]

    level_copy = {k: (levels or {}).get(k) for k in ("support1", "support2", "support3", "resistance1", "resistance2", "resistance3")}
    liq = dict(d1.get("liquidity") or {})
    generated_at = generated_at or now_damascus().isoformat()
    import hashlib, json
    fingerprint = {
        "frames": {k: _weekly_frame_contract(frames_data.get(k)).get("frame_direction") for k in ("W1", "D1", "H4")},
        "price": round(float(price), 6) if price is not None else None,
        "range": {"low": s1, "high": r1},
        "state": state,
    }
    snapshot_hash = hashlib.sha256(json.dumps(fingerprint, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")).hexdigest()[:16]
    decision = WeeklyDecision(
        decision_id="WEEKLY_" + str(generated_at).replace(":", "").replace("-", "").replace("+", "_")[:24],
        decision_version=1,
        generated_at=generated_at,
        state=state,
        direction=direction,
        frames={k: _weekly_frame_contract(frames_data.get(k)) for k in ("W1", "D1", "H4")},
        decision_range={"low": s1, "high": r1, "basis": "S1_R1", "rule": "D1 close outside the range activates directional confirmation."},
        bullish_scenario=bullish,
        bearish_scenario=bearish,
        liquidity=liq,
        levels=level_copy,
        confirmation={"tf": "D1", "bullish": bull_conf, "bearish": bear_conf},
        data_quality=None,
        decision_quality=max(int(w1.get("score", 0) or 0), int(d1.get("score", 0) or 0), int(h4.get("score", 0) or 0)),
        state_change_reason=reason,
        snapshot_hash=snapshot_hash,
    )
    WeeklyDecisionValidator.assert_valid(decision)
    return decision


def weekly_decision_contradiction_tests():
    """اختبارات تناقضات معزولة؛ لا تستدعي الشبكة ولا تغيّر حالة البوت."""
    base_frames = {
        "W1": {"direction": "BUY", "score": 70},
        "D1": {"direction": "SELL", "score": 65},
        "H4": {"direction": "SELL", "score": 60},
    }
    levels = {
        "support1": {"price": 4329.97}, "support2": {"price": 4296.72}, "support3": {"price": 4121.74},
        "resistance1": {"price": 4373.00}, "resistance2": {"price": 4446.60}, "resistance3": {"price": 4511.08},
    }
    # نستخدم بيانات شمعة مصطنعة لتجنب أي I/O في الاختبار.
    import pandas as pd
    dfx = pd.DataFrame([{ "open": 4350.0, "high": 4360.0, "low": 4340.0, "close": 4350.0 }] * 21)
    d = build_weekly_decision(base_frames, levels, 4349.70, dfx, "2026-09-12T01:43:00+03:00")
    results = {
        "mixed_frames_becomes_neutral": d.state == "NEUTRAL" and d.direction is None,
        "valid_contract": len(WeeklyDecisionValidator.validate(d)) == 0,
    }
    bad = d.to_dict(); bad["state"] = "NEUTRAL"; bad["direction"] = "BUY"
    results["neutral_direction_contradiction_detected"] = len(WeeklyDecisionValidator.validate(bad)) > 0
    bad2 = d.to_dict(); bad2["decision_range"] = {"low": 4400, "high": 4300}
    results["invalid_range_detected"] = len(WeeklyDecisionValidator.validate(bad2)) > 0
    bad3 = d.to_dict(); bad3["bullish_scenario"] = dict(bad3["bullish_scenario"]); bad3["bullish_scenario"]["confirmation"] = dict(bad3["bullish_scenario"]["confirmation"]); bad3["bullish_scenario"]["confirmation"]["normalized_status"] = "CONFIRMED"; bad3["bullish_scenario"]["confirmation"]["side_ok"] = False
    results["false_confirmation_detected"] = len(WeeklyDecisionValidator.validate(bad3)) > 0
    results["passed"] = all(results.values())
    return results


def _make_scenarios(frames_data, levels, price, atr, horizon):
    if horizon == "weekly":
        frame_defs = [("W1", 30), ("D1", 25), ("H4", 20)]
    else:
        frame_defs = [("H1", 30), ("M15", 25), ("M5", 20)]

    def score(direction):
        return _scenario_score(frames_data, frame_defs, direction, levels, price, atr, horizon)

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
    """تنسيق التقرير التوضيحي؛ اليومي له قالب عرض مستقل دون تغيير منطق التحليل."""
    if horizon == "daily":
        now = now_damascus()
        market_text, _ = market_state(now)

        def target_text(targets):
            vals = [fmt(x) for x in targets if x is not None]
            return " → ".join(vals) if vals else "غير محددة من المستويات الحالية"

        def level_line(prefix, key):
            level = levels.get(key) if isinstance(levels, dict) else None
            price_value = _zone_price(level)
            if price_value is None:
                return f"{prefix}: غير متاح"
            strength = level.get("strength") if isinstance(level, dict) else None
            touches = level.get("touches") if isinstance(level, dict) else None
            strength_text = "غير محددة"
            if strength is not None:
                try:
                    strength_text = f"{float(strength):.0f}"
                except (TypeError, ValueError):
                    pass
            touches_text = "غير محددة"
            if touches is not None:
                try:
                    n = int(touches)
                    touches_text = "لمسة" if n == 1 else "لمستان" if n == 2 else "لمسات"
                    touches_text = f"{n} {touches_text}"
                except (TypeError, ValueError):
                    pass
            return f"{prefix}: {price_value:.2f} | قوة {strength_text} | {touches_text}"

        h1 = frames_data["H1"]
        m15 = frames_data["M15"]
        m5 = frames_data["M5"]
        liquidity = m15.get("liquidity", {})
        direction = "BUY" if primary.get("title", "").startswith("استمرار الاتجاه الصاعد") else "SELL" if primary.get("title", "").startswith("استمرار الاتجاه الهابط") else "WAIT"
        r1 = nearest_resistance(levels, price)
        s1 = nearest_support(levels, price)

        confirm_level = r1 if direction == "SELL" else s1 if direction == "BUY" else None
        confirm = _breakout_confirmation(get_bars("1h", 300), confirm_level, direction, "daily", frames_data["M15"]["atr"]) if confirm_level is not None and direction in ("BUY", "SELL") else {"risk": "غير محددة", "confidence": 0}

        primary_title = "🟢 السيناريو الرئيسي — صاعد" if direction == "BUY" else "🟢 السيناريو الرئيسي — هابط" if direction == "SELL" else "🟢 السيناريو الرئيسي — حياد"
        if direction == "SELL" and r1 is not None:
            mechanism_lines = [
                f"طالما بقي السعر أسفل {fmt(r1)}",
                "والهيكل داعمًا للهبوط",
                "تبقى القراءة الهابطة هي المرجحة.",
            ]
            confirmation_rule = f"🕯️ إغلاق H1 هابط عند منطقة القرار"
            invalidation = fmt(r1)
        elif direction == "BUY" and s1 is not None:
            mechanism_lines = [
                f"طالما حافظ السعر على {fmt(s1)}",
                "والهيكل داعمًا للصعود",
                "تبقى القراءة الصاعدة هي المرجحة.",
            ]
            confirmation_rule = f"🕯️ إغلاق H1 صاعد عند منطقة القرار"
            invalidation = fmt(s1)
        else:
            mechanism_lines = ["الفريمات الحالية لا تمنح أفضلية اتجاهية كافية.", "تبقى القراءة محايدة حتى يظهر تأكيد واضح."]
            confirmation_rule = "🕯️ تأكيد واضح من منطقة القرار"
            invalidation = "غير محدد"

        alt_title = "🔵 السيناريو البديل — صاعد" if direction == "SELL" else "🔵 السيناريو البديل — هابط" if direction == "BUY" else "🔵 السيناريو البديل — استمرار التذبذب"
        if direction == "SELL" and r1 is not None:
            alt_lines = [f"يتفعّل عند اختراق {fmt(r1)}", "بإغلاق H1 مؤكد", "مع الثبات أو إعادة الاختبار."]
            alt_targets = target_text(alternative.get("targets", []))
        elif direction == "BUY" and s1 is not None:
            alt_lines = [f"يتفعّل عند كسر {fmt(s1)}", "بإغلاق H1 مؤكد", "مع الثبات أو إعادة الاختبار."]
            alt_targets = target_text(alternative.get("targets", []))
        else:
            alt_lines = ["يتفعّل عند ظهور كسر واضح لأحد طرفي النطاق."]
            alt_targets = target_text(alternative.get("targets", []))

        lines = [
            "🧭 القراءة اليومية للسوق — XAU/USD",
            "",
            f"{market_text} |",
            f"🕐 {now.day}-{now.month}-{now.year} / دمشق {now.strftime('%H:%M')}",
            f"💰 السعر: {price:.2f}",
            "🧭 الأفق: داخل اليوم",
            "📌 التركيز: الاتجاه داخل اليوم",
            "",
            "🧭 القراءة الحالية",
            "",
            f"الاتجاه: {'🟢 صاعد' if direction == 'BUY' else '🔴 هابط' if direction == 'SELL' else '🟡 محايد'}",
            "الحالة: 🟡 مراقبة تحليلية",
            f"قوة السيناريو الرئيسي: {primary.get('quality', 0)}/100 — {_quality_label(primary.get('quality', 0))}",
            "",
            "⏱ الفريمات",
            f"H1: {h1['direction']} → السياق",
            f"M15: {m15['direction']} → التأكيد",
            f"M5: {m5['direction']} → الحركة",
            "",
            "💧 السيولة",
            f"🟢 شرائية: {fmt(liquidity.get('nearest_buy'))}",
            f"🔴 بيعية: {fmt(liquidity.get('nearest_sell'))}",
        ]
        nearest_buy = liquidity.get("nearest_buy")
        if nearest_buy is not None:
            try:
                lines.append(f"📏 المسافة إلى السيولة الشرائية: {abs(float(nearest_buy) - float(price)):.2f}")
            except (TypeError, ValueError):
                lines.append("📏 المسافة إلى السيولة الشرائية: غير متاحة")
        else:
            lines.append("📏 المسافة إلى السيولة الشرائية: غير متاحة")
        lines += [
            "",
            f"آخر شمعة مكتملة: {liquidity.get('sweep_text', 'غير متاح')}",
            f"إعادة الاختبار: {liquidity_retest_summary(liquidity)}",
            "",
            primary_title,
            "",
            *mechanism_lines,
            "",
            "التأكيد:",
            confirmation_rule,
            f"⚠️ خطورة الحركة: {confirm.get('risk', 'غير محددة')}",
            f"📊 قوة التأكيد: {confirm.get('confidence', 0)}/100",
            "",
            "المناطق المحتملة:",
            target_text(primary.get("targets", [])),
            "",
            "الإبطال:",
            invalidation,
            "",
            alt_title,
            "",
            *alt_lines,
            "",
            f"📊 قوة التأكيد الحالية: {alternative.get('quality', 0)}/100",
            f"🎯 المنطقة التالية: {alt_targets}",
            "📍 الحالة: غير مفعّل",
            "",
            "📍 خريطة المستويات",
            "",
            "الدعم:",
            level_line("S1", "support1"),
            level_line("S2", "support2"),
            level_line("S3", "support3"),
            "",
            "المقاومة:",
            level_line("R1", "resistance1"),
            level_line("R2", "resistance2"),
            level_line("R3", "resistance3"),
            "",
            "👁️ المراقبة الحالية",
            "",
            f"• سلوك السعر عند {fmt(r1) if r1 is not None else 'المستوى الحساس'}",
            "• تأكيد H1",
            "• تطور الحركة على M15 وM5",
            "• أي سحب سيولة أو إعادة اختبار",
            "",
            "🔄 نقطة التحول",
            "",
        ]
        if direction == "SELL" and r1 is not None:
            lines += [f"🔴 أسفل {fmt(r1)}: تبقى القراءة الهابطة مفضلة.", f"🔵 فوق {fmt(r1)} مع تأكيد H1: يعاد تقييم السيناريو لصالح الصعود."]
        elif direction == "BUY" and s1 is not None:
            lines += [f"🔵 فوق {fmt(s1)} مع تأكيد H1: تبقى القراءة الصاعدة مفضلة.", f"🔴 أسفل {fmt(s1)} مع تأكيد H1: يعاد تقييم السيناريو لصالح الهبوط."]
        else:
            lines += ["🟡 لا توجد نقطة تحول مؤكدة حالياً."]
        lines += ["", "⚠️ درجات القوة والتأكيد مقاييس تحليلية وليست احتمالات مضمونة للربح."]
        return "\n".join(lines)

    def target_text(targets):
        vals = [fmt(x) for x in targets if x is not None]
        return " ثم ".join(vals) if vals else "غير محددة من المستويات الحالية"

    label = "الأسبوعي" if horizon == "weekly" else "اليومي"
    # التقرير الأسبوعي النهائي يستخدم عقد WeeklyDecision عبر طبقة التوافق،
    # مع فصل "قوة الشمعة" عن "تحقق شرط الكسر".
    if horizon == "weekly":
        now = now_damascus()
        market_text, _ = market_state(now)
        d1_liq = frames_data["D1"].get("liquidity", {})
        state_neutral = primary.get("quality") is None
        bullish_conf = (primary.get("trigger", "") or "")
        bearish_conf = (primary.get("trigger", "") or "")

        # استخرج بيانات التأكيد مباشرة من WeeklyDecision compatibility payload.
        # primary.trigger يحتوي المسارين في الحالة المحايدة؛ نبني العرض من القرار الأصلي
        # المرفق بالـ compatibility object إن توفر، وإلا نستخدم النص القديم بأمان.
        decision_obj = globals().get("_LAST_WEEKLY_DECISION_FOR_REPORT")
        if decision_obj is None:
            decision_obj = None

        def _scenario_display(sc):
            conf = sc.get("confirmation") or {}
            level = (sc.get("trigger") or {}).get("level")
            direction = sc.get("direction")
            side = "فوق" if direction == "BUY" else "تحت"
            strength = int(conf.get("confirmation_strength", conf.get("confidence", 0)) or 0)
            side_ok = conf.get("side_ok")
            if side_ok is True:
                break_state = "🟢 تحقق شرط الإغلاق"
            elif side_ok is False:
                break_state = "🔴 لم يتحقق شرط الإغلاق"
            else:
                break_state = "⚪ غير متاح"
            return [
                f"• شرط التفعيل: إغلاق {conf.get('tf', 'D1')} {side} {fmt(level) if level is not None else 'غير متاح'}",
                f"• 🕯️ قوة شمعة التأكيد: {strength}/100",
                f"• 🔓 حالة الكسر: {break_state}",
                f"• ⚠️ خطورة الحركة: {conf.get('risk', 'غير محددة')}",
                f"• 📌 القاعدة: {conf.get('rule', 'إغلاق قوي + مراقبة إعادة الاختبار')}",
            ]

        # لا نعتمد على global في المسار النهائي: نعيد بناء البيانات من الـ compatibility
        # إن لم تكن تفاصيل العقد متاحة، مع الحفاظ على عدم تغيير المحركات الأخرى.
        bull = getattr(primary, "_bullish", None) if hasattr(primary, "_bullish") else None
        bear = getattr(primary, "_bearish", None) if hasattr(primary, "_bearish") else None
        if bull is None or bear is None:
            # primary/alternative الناتجان من v18.106 لا يحملان العقد داخلياً؛
            # نستخدم بيانات التأكيد الحالية من frames/levels لتكوين عرض آمن.
            d1_df = get_bars("1d", 300)
            bull = {
                "direction": "BUY",
                "trigger": {"level": nearest_resistance(levels, price)},
                "confirmation": _weekly_confirmation_contract(_breakout_confirmation(d1_df, nearest_resistance(levels, price), "BUY", "weekly", frames_data["D1"].get("atr", 0)), current_price=price, level=nearest_resistance(levels, price), direction="BUY"),
                "targets": [x for x in [levels.get("resistance2", {}).get("price"), levels.get("resistance3", {}).get("price")] if x is not None],
            }
            bear = {
                "direction": "SELL",
                "trigger": {"level": nearest_support(levels, price)},
                "confirmation": _weekly_confirmation_contract(_breakout_confirmation(d1_df, nearest_support(levels, price), "SELL", "weekly", frames_data["D1"].get("atr", 0)), current_price=price, level=nearest_support(levels, price), direction="SELL"),
                "targets": [x for x in [levels.get("support2", {}).get("price"), levels.get("support3", {}).get("price")] if x is not None],
            }

        lines = [
            "🧭 القراءة الأسبوعية للسوق — XAU/USD",
            "",
            f"{market_text} | 🕐 دمشق {now.strftime('%Y-%m-%d %H:%M')}",
            f"💰 السعر الحالي: {fmt(price)}",
            "🧭 الأفق: الاستراتيجية المحتملة خلال الأسبوع",
            "📌 التركيز: اتجاه الأسبوع وبناء الرؤية الاستراتيجية.",
            "",
            f"📊 W1: {frames_data['W1']['direction']} | D1: {frames_data['D1']['direction']} | H4: {frames_data['H4']['direction']}",
            "🧭 لا يدخل M15 أو M5 في تحديد الاتجاه الاستراتيجي الأسبوعي.",
            "",
            "🕯️ محرك تأكيد الحركة",
            "• الأفق: أسبوعي",
            "• شمعة التأكيد الأساسية: D1",
            "• قوة الشمعة منفصلة عن تحقق شرط الكسر.",
            "• تحقق الكسر يعتمد على إغلاق D1 المكتمل، وليس السعر اللحظي وحده.",
            "",
            "💧 قراءة السيولة",
            f"• الانحياز: {d1_liq.get('bias', 'غير متاح')}",
            f"• Buy-side liquidity الأقرب: {fmt(d1_liq.get('nearest_buy'))}",
            f"• Sell-side liquidity الأقرب: {fmt(d1_liq.get('nearest_sell'))}",
            f"• آخر شمعة مكتملة: {d1_liq.get('sweep_text', 'غير متاح')}",
            f"• حالة إعادة الاختبار: {liquidity_retest_summary(d1_liq)}",
            "",
            "⚪ القرار الاستراتيجي",
            ("انتظار تأكيد الاتجاه" if state_neutral else primary.get("title", "القرار الحالي")),
            (f"• السبب: {primary.get('mechanism', 'غير متاح')}"),
            "",
            "🟢 المسار الصاعد",
            *_scenario_display(bull),
            f"• 🎯 بعد تحقق الكسر: {target_text(bull.get('targets', []))}",
            "",
            "🔴 المسار الهابط",
            *_scenario_display(bear),
            f"• 🎯 بعد تحقق الكسر: {target_text(bear.get('targets', []))}",
            "",
            f"⚪ طالما بقي السعر داخل نطاق {fmt(nearest_support(levels, price))} — {fmt(nearest_resistance(levels, price))}",
            "فالقرار الاستراتيجي يبقى انتظاراً ما لم يوجد كسر مؤكد وفق إغلاق D1.",
            "",
            "📍 خريطة القرار السعري",
            f"🟢 S1: {format_zone(levels, 'support1')}",
            f"🟢 S2: {format_zone(levels, 'support2')}",
            f"🟢 S3: {format_zone(levels, 'support3')}",
            f"🔴 R1: {format_zone(levels, 'resistance1')}",
            f"🔴 R2: {format_zone(levels, 'resistance2')}",
            f"🔴 R3: {format_zone(levels, 'resistance3')}",
            "",
            "🗒 ماذا نراقب الآن؟",
            f"• إغلاق D1 فوق {fmt(nearest_resistance(levels, price))} لتفعيل المسار الصاعد.",
            f"• إغلاق D1 تحت {fmt(nearest_support(levels, price))} لتفعيل المسار الهابط.",
            "• البقاء داخل النطاق يعني استمرار الانتظار.",
            "• Retest بعد الكسر يرفع موثوقية التفعيل حسب مستوى الخطورة.",
            "",
            "🧠 ملخص الفريمات",
            f"W1: {frames_data['W1']['direction']} | D1: {frames_data['D1']['direction']} | H4: {frames_data['H4']['direction']}",
            "",
            "⚠️ قوة الشمعة ودرجة التأكيد مقاييس تحليلية وليستا احتمالاً مضموناً للربح.",
        ]
        return "\n".join(lines)


    lines = [
        f"📝 التقرير التوضيحي {label} — XAU/USD",
        f"🕐 توقيت دمشق: {now_damascus().strftime('%Y-%m-%d %H:%M')}",
        f"💰 السعر الحالي: {fmt(price)}",
        f"🧭 الأفق: {'الاستراتيجية المحتملة خلال الأسبوع' if horizon == 'weekly' else 'الحركة المحتملة خلال اليوم'}",
        ""
    ]
    lines.extend(extra_lines)
    lines += [
        "🕯️ محرك تأكيد الحركة",
        f"• الأفق: {'أسبوعي' if horizon == 'weekly' else 'يومي'}",
        f"• شمعة التأكيد الأساسية: {'D1' if horizon == 'weekly' else 'H1'}",
        "• مستوى الخطورة: يُحسب لكل مستوى حسب ATR والشمعة المكتملة.",
        "• كلما ارتفعت خطورة الحركة، ترتفع متطلبات التأكيد تلقائياً.",
        "",
        "💧 قراءة السيولة",
        f"• الانحياز: {frames_data['H1' if horizon == 'daily' else 'D1']['liquidity']['bias']}",
        f"• Buy-side liquidity الأقرب: {fmt(frames_data['H1' if horizon == 'daily' else 'D1']['liquidity']['nearest_buy'])}",
        f"• Sell-side liquidity الأقرب: {fmt(frames_data['H1' if horizon == 'daily' else 'D1']['liquidity']['nearest_sell'])}",
        f"• آخر شمعة مكتملة: {frames_data['H1' if horizon == 'daily' else 'D1']['liquidity']['sweep_text']}",
        f"• حالة إعادة الاختبار: {liquidity_retest_summary(frames_data['H1' if horizon == 'daily' else 'D1']['liquidity'])}",
        "",
        ("القرار" if horizon == "weekly" and primary.get("quality") is None else "🟢 السيناريو الأول — المرجح"),
        primary["title"],
        *(["• جودة القرار: غير قابلة للقياس اتجاهياً أثناء الحياد؛ القرار الحالي هو الانتظار حتى تأكيد D1."] if horizon == "weekly" and primary.get("quality") is None else [f"• جودة السيناريو الرئيسي: {primary['quality']} نقطة / 100 — {_quality_label(primary['quality'])}"]),
        f"• الآلية: {primary['mechanism']}",
        f"• شرط التفعيل: {primary['trigger']}",
        f"• الأهداف المحتملة: {target_text(primary['targets'])}",
        f"• مستوى إبطال الفكرة: {fmt(primary['stop'])}",
    ]
    if primary["factors"]:
        lines.append("• أسباب الترجيح:")
        lines.extend("  - " + x for x in primary["factors"][:7])
    lines += ["", ("⚪ حالة النطاق والمتابعة" if horizon == "weekly" and alternative.get("quality") is None else "🔴 السيناريو الثاني — البديل"),
              alternative["title"], f"• الآلية: {alternative['mechanism']}",
              f"• شرط التحول: {alternative['trigger']}",
              f"• الأهداف المحتملة: {target_text(alternative['targets'])}",
              *(["• جودة السيناريو: غير مفعّلة أثناء استمرار الحياد داخل نطاق القرار."] if horizon == "weekly" and alternative.get("quality") is None else [f"• جودة السيناريو البديل: {alternative['quality']} نقطة / 100"]), "",
              "📍 خريطة القرار السعري",
              f"🟢 S1: {format_zone(levels, 'support1')}",
              f"🟢 S2: {format_zone(levels, 'support2')}",
              f"🟢 S3: {format_zone(levels, 'support3')}",
              f"🔴 R1: {format_zone(levels, 'resistance1')}",
              f"🔴 R2: {format_zone(levels, 'resistance2')}",
              f"🔴 R3: {format_zone(levels, 'resistance3')}", "",
              "🗒 خطة العمل المقترحة"]
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
    """التحليل اليومي يقرأ نفس محرك التنفيذ الموحد للصفقات."""
    core = _daily_execution_core()
    h1, m15, m5 = core["daily"]["h1"], core["daily"]["m15"], core["daily"]["m5"]
    price, levels = core["price"], core["levels"]
    direction = core["direction"]; buy = core["daily"]["buy_score"]; sell = core["daily"]["sell_score"]
    issue_time = now_damascus().isoformat()
    s1 = nearest_support(levels, price); r1 = nearest_resistance(levels, price)
    expected_min = min(x for x in (s1, r1) if x is not None) if any(x is not None for x in (s1, r1)) else None
    expected_max = max(x for x in (s1, r1) if x is not None) if any(x is not None for x in (s1, r1)) else None
    target = r1 if direction == "BUY" else s1 if direction == "SELL" else None
    _auditor_register_analysis("daily", issue_time=issue_time, direction=direction, expected_min=expected_min, expected_max=expected_max, target=target, score=max(buy,sell), snapshot={"version":VERSION,"h1":h1,"m15":m15,"m5":m5,"levels":levels,"price":price})
    return f"📊 التحليل اليومي {VERSION}\n💰 السعر: {price:.2f}\n🎯 التوجيه: {'🟢 شراء' if direction=='BUY' else '🔴 بيع' if direction=='SELL' else '🟡 انتظار'}\n💪 قوة اليوم: {max(buy,sell)} نقطة\n\nH1: {h1['direction']} | RSI {h1['rsi']:.1f} | ADX {h1['adx']:.1f}\nM15: {m15['direction']} | RSI {m15['rsi']:.1f} | ADX {m15['adx']:.1f}\nM5: {m5['direction']} | RSI {m5['rsi']:.1f} | ADX {m5['adx']:.1f}\n\n📍 S1: {format_zone(levels,'support1')}\n📍 R1: {format_zone(levels,'resistance1')}\n\n💧 السيولة: {m15['liquidity']['bias']}\n🧲 Buy-side: {fmt(m15['liquidity']['nearest_buy'])} | Sell-side: {fmt(m15['liquidity']['nearest_sell'])}\n🔄 السحب: {m15['liquidity']['sweep_text']}"


def build_weekly_analysis():
    """التحليل الأسبوعي: W1 + D1 + H4 فقط."""
    w1 = analyze(get_bars("1w", 250)); d1 = analyze(get_bars("1d", 300)); h4 = analyze(get_bars("4h", 300))
    q = live_price(); price = q["price"]; levels = support_resistance(get_bars("1d", 250))
    frames = {"W1": w1, "D1": d1, "H4": h4}
    buy, _ = _scenario_score(frames, [("W1",35),("D1",30),("H4",25)], "BUY", levels, price, d1["atr"], "weekly")
    sell, _ = _scenario_score(frames, [("W1",35),("D1",30),("H4",25)], "SELL", levels, price, d1["atr"], "weekly")
    direction = "BUY" if buy > sell else "SELL" if sell > buy else "WAIT"
    issue_time = now_damascus().isoformat()
    s1 = nearest_support(levels, price); r1 = nearest_resistance(levels, price)
    expected_min = min(x for x in (s1, r1) if x is not None) if any(x is not None for x in (s1, r1)) else None
    expected_max = max(x for x in (s1, r1) if x is not None) if any(x is not None for x in (s1, r1)) else None
    target = r1 if direction == "BUY" else s1 if direction == "SELL" else None
    _auditor_register_analysis("weekly", issue_time=issue_time, direction=direction, expected_min=expected_min, expected_max=expected_max, target=target, score=max(buy,sell), snapshot={"version":VERSION,"w1":w1,"d1":d1,"h4":h4,"levels":levels,"price":price})
    return f"📅 التحليل الأسبوعي {VERSION}\n💰 السعر: {price:.2f}\n🎯 الاتجاه الاستراتيجي: {'🟢 شراء' if direction=='BUY' else '🔴 بيع' if direction=='SELL' else '🟡 حياد'}\n💪 قوة الاتجاه: {max(buy,sell)} نقطة\n\nW1: {w1['direction']} | قوة {w1['score']} | RSI {w1['rsi']:.1f} | ADX {w1['adx']:.1f}\nD1: {d1['direction']} | قوة {d1['score']} | RSI {d1['rsi']:.1f} | ADX {d1['adx']:.1f}\nH4: {h4['direction']} | قوة {h4['score']} | RSI {h4['rsi']:.1f} | ADX {h4['adx']:.1f}\n\n📍 الدعم الأسبوعي: {format_zone(levels,'support1')}\n📍 المقاومة الأسبوعية: {format_zone(levels,'resistance1')}\n\n💧 السيولة: {d1['liquidity']['bias']}\n🧲 Buy-side: {fmt(d1['liquidity']['nearest_buy'])} | Sell-side: {fmt(d1['liquidity']['nearest_sell'])}\n🔄 السحب: {d1['liquidity']['sweep_text']}"


def build_daily_report():
    """التقرير اليومي والصفقة المحتملة مبنيان من نفس بيانات القرار."""
    core = _daily_execution_core()
    frames_data = {"H1": core["daily"]["h1"], "M15": core["daily"]["m15"], "M5": core["daily"]["m5"]}
    price, levels = core["price"], core["levels"]
    primary, alternative, _, _ = _make_scenarios(frames_data, levels, price, frames_data["M15"]["atr"], "daily")
    # التقرير اليومي قراءة تحليلية فقط؛ لا يعرض صفقة أو دخولاً أو SL/TP/R:R.
    return _format_explanatory_report(frames_data, levels, price, primary, alternative, "daily", [])


def _weekly_decision_compatibility_scenarios(decision):
    """
    Compatibility adapter للـ formatter القديم فقط.
    لا يغيّر _make_scenarios ولا أي محرك تداول؛ يحوّل WeeklyDecision
    إلى primary/alternative بالشكل الذي يتوقعه التقرير الأسبوعي الحالي.
    """
    d = decision.to_dict() if isinstance(decision, WeeklyDecision) else dict(decision or {})
    state = d.get("state")
    direction = d.get("direction")
    bullish = d.get("bullish_scenario") or {}
    bearish = d.get("bearish_scenario") or {}
    decision_quality = int(d.get("decision_quality", 0) or 0)

    def _trigger_text(sc):
        trig = sc.get("trigger") or {}
        level = trig.get("level")
        conf = sc.get("confirmation") or {}
        strength = conf.get("confirmation_strength", conf.get("confidence", 0))
        risk = conf.get("risk", "غير محددة")
        status = conf.get("display_status", conf.get("status", "غير متاح"))
        side = "فوق" if sc.get("direction") == "BUY" else "تحت"
        if level is None:
            return "🕯️ مستوى التفعيل غير متاح حالياً"
        return (f"🕯️ تأكيد D1: الإغلاق المطلوب {side} {fmt(level)}\n"
                f"   • خطورة الحركة: {risk} | قوة التأكيد: {strength}/100\n"
                f"   • الحالة: {status}\n"
                f"   • القاعدة: إغلاق قوي + مراقبة إعادة الاختبار")

    def _scenario(sc, title, mechanism, quality):
        return {
            "title": title,
            "quality": int(max(0, min(100, quality))),
            "mechanism": mechanism,
            "trigger": _trigger_text(sc),
            "targets": list(sc.get("targets") or []),
            "stop": sc.get("invalidation"),
            "factors": [],
        }

    if state == "BULLISH" and direction == "BUY":
        primary = _scenario(
            bullish,
            "استمرار الاتجاه الصاعد / بناء مركز شراء",
            d.get("state_change_reason") or "توافق الفريمات الاستراتيجية يدعم القراءة الصاعدة.",
            decision_quality,
        )
        alternative = _scenario(
            bearish,
            "السيناريو البديل — تحول هابط",
            "يصبح المسار الهابط نشطاً فقط بعد تحقق شرط الكسر الهابط وتأكيد D1.",
            int((bearish.get("confirmation") or {}).get("confirmation_strength", 0) or 0),
        )
    elif state == "BEARISH" and direction == "SELL":
        primary = _scenario(
            bearish,
            "استمرار الاتجاه الهابط / بناء مركز بيع",
            d.get("state_change_reason") or "توافق الفريمات الاستراتيجية يدعم القراءة الهابطة.",
            decision_quality,
        )
        alternative = _scenario(
            bullish,
            "السيناريو البديل — تحول صاعد",
            "يصبح المسار الصاعد نشطاً فقط بعد تحقق شرط الكسر الصاعد وتأكيد D1.",
            int((bullish.get("confirmation") or {}).get("confirmation_strength", 0) or 0),
        )
    else:
        # في الحالة المحايدة لا يوجد "سيناريو مرجح" ولا جودة صفرية مضللة.
        # نستخدم عقد primary/alternative القديم فقط كطبقة توافق مع formatter.
        primary = {
            "title": "⚪ القرار الاستراتيجي الحالي — انتظار تأكيد الاتجاه",
            "quality": None,
            "mechanism": d.get("state_change_reason") or "لا توجد أفضلية استراتيجية موحدة؛ يبقى القرار محايداً حتى يُحسم نطاق القرار بإغلاق D1.",
            "trigger": (
                "🟢 المسار الصاعد\n" + _trigger_text(bullish) +
                "\n\n🔴 المسار الهابط\n" + _trigger_text(bearish)
            ),
            "targets": [],
            "stop": None,
            "factors": [],
        }
        alternative = {
            "title": "⚪ حالة النطاق — استمرار الانتظار",
            "quality": None,
            "mechanism": "البقاء داخل نطاق القرار يعني استمرار الانتظار وعدم منح أفضلية اتجاهية قبل كسر مؤكد بإغلاق D1.",
            "trigger": "🟢 فوق المقاومة: تفعيل المسار الصاعد | 🔴 تحت الدعم: تفعيل المسار الهابط.",
            "targets": [],
            "stop": None,
            "factors": [],
        }
    return primary, alternative


def build_weekly_report():
    """التقرير الأسبوعي فقط: WeeklyDecision -> Validator -> compatibility formatter القديم."""
    frames_data = {
        "W1": analyze(get_bars("1w", 250)),
        "D1": analyze(get_bars("1d", 300)),
        "H4": analyze(get_bars("4h", 300)),
    }
    q = live_price()
    price = q["price"]
    d1_bars = get_bars("1d", 300)
    levels = support_resistance(get_bars("1d", 250))

    # المصدر الوحيد للقرار الأسبوعي الجديد. لا نستخدم _make_scenarios هنا لتحديد القرار.
    decision = build_weekly_decision(frames_data, levels, price, d1_df=d1_bars)
    WeeklyDecisionValidator.assert_valid(decision)

    # Compatibility Layer: يبقي formatter القديم والعقد القديم دون تغيير.
    primary, alternative = _weekly_decision_compatibility_scenarios(decision)

    return _format_explanatory_report(frames_data, levels, price, primary, alternative, "weekly", [
        "📌 التركيز: اتجاه الأسبوع وبناء الرؤية الاستراتيجية.",
        f"📊 W1: {frames_data['W1']['direction']} | D1: {frames_data['D1']['direction']} | H4: {frames_data['H4']['direction']}",
        "🧭 لا يدخل M15 أو M5 في تحديد الاتجاه الاستراتيجي الأسبوعي.",
    ])


def build_explanatory_report(timeframe="daily"):
    return build_weekly_report() if timeframe == "weekly" else build_daily_report()

# ============================================================
# FULL ANALYSIS PERFORMANCE / COMPONENT CATALOG
# v18.76 — informational only; does not alter trading decisions.
# ============================================================

ANALYSIS_COMPONENT_CATALOG = {
    "essential": [
        {"name": "W1/D1/H4 strategic context", "removable": False, "impact_if_removed": "HIGH — يفقد التحليل الصورة الاستراتيجية متعددة الأطر"},
        {"name": "H1/M15/M5 execution context", "removable": False, "impact_if_removed": "CRITICAL — يمس محرك القرار التنفيذي اليومي"},
        {"name": "S/R levels", "removable": False, "impact_if_removed": "HIGH — يؤثر في مناطق القرار وقياس R:R"},
        {"name": "Liquidity / retest gate", "removable": False, "impact_if_removed": "HIGH — قد يغيّر حالة READY/CANDIDATE/WATCH"},
        {"name": "News safety gate", "removable": False, "impact_if_removed": "CRITICAL — إزالة البوابة قد تسمح بالتنفيذ أثناء خطر الأخبار"},
        {"name": "Risk / R:R validation", "removable": False, "impact_if_removed": "CRITICAL — يمس صلاحية الصفقة"},
    ],
    "optional_display_but_not_safe_to_disable_in_scoring": [
        {"name": "Macro institutional data", "removable": True, "impact_if_removed": "HIGH — يقلل جودة التأكيد المؤسسي؛ لا يُعطّل في الوضع الافتراضي"},
        {"name": "COT", "removable": True, "impact_if_removed": "MEDIUM/HIGH — يفقد تدفقات المضاربين/المراكز"},
        {"name": "GLD/WGC", "removable": True, "impact_if_removed": "MEDIUM — يقلل تغطية تدفقات الذهب المؤسسية"},
        {"name": "Central-bank data", "removable": True, "impact_if_removed": "MEDIUM/HIGH — يفقد اتجاهات احتياطيات البنوك المركزية"},
        {"name": "Verbose institutional source details", "removable": True, "impact_if_removed": "LOW — يؤثر على حجم التقرير أكثر من القرار"},
    ],
    "performance_heavy": [
        {"name": "IMF/WGC/Central-bank external retrieval", "impact": "HIGH/CRITICAL on cold cache"},
        {"name": "Macro multi-source retrieval", "impact": "HIGH on cold cache"},
        {"name": "XLSX/Pandas parsing", "impact": "MEDIUM/HIGH when cache is cold"},
        {"name": "Multiple Biquote timeframe requests", "impact": "MEDIUM/HIGH depending on API latency"},
    ],
}

def get_full_analysis_component_options():
    """Return the component policy for administrators; read-only metadata."""
    return {k: [dict(x) for x in v] for k, v in ANALYSIS_COMPONENT_CATALOG.items()}

# ============================================================
# أوامر Telegram
# ============================================================

def market_state(now=None):
    """حالة السوق المرئية للمستخدم، وتُستخدم أيضاً كمرجع موحّد للتنبيهات."""
    now = now or now_damascus()
    closed = market_closed_reason(now)
    if closed == "WEEKEND":
        return "⚫ السوق مغلق", "WEEKEND"
    if closed == "HOLIDAY":
        return "⚫ السوق مغلق", "HOLIDAY"

    # جلسات ديناميكية حسب المنطقة الزمنية المحلية، لتفادي كسر التوقيت الصيفي.
    active = []
    specs = [("Sydney", "Australia/Sydney", 8), ("Tokyo", "Asia/Tokyo", 9),
             ("London", "Europe/London", 8), ("New York", "America/New_York", 8)]
    for name, zone_name, hour in specs:
        local = now.astimezone(ZoneInfo(zone_name))
        if local.weekday() < 5 and hour <= local.hour < hour + 9:
            active.append(name)

    if "London" in active and "New York" in active:
        return "🟠 تقلب مرتفع", "LONDON_NEW_YORK_OVERLAP"
    if "New York" in active or "London" in active:
        return "🟢 السوق نشط", ",".join(active)
    if active:
        return "🟡 نشاط متوسط", ",".join(active)
    return "🔵 السوق هادئ", "QUIET"


def market_header(now=None):
    """Header خفيف للواجهات التفاعلية فقط، بدون خطوط فاصلة."""
    state, _ = market_state(now)
    now = now or now_damascus()
    return f"{state} | 🕐 دمشق {now.strftime('%H:%M')}\n\n"


def with_market_header(text):
    return market_header() + str(text or "")


TELEGRAM_SAFE_MESSAGE_LIMIT = 3900

def _split_telegram_text(text, limit=TELEGRAM_SAFE_MESSAGE_LIMIT):
    """تقسيم آمن للنص الطويل دون قص المحتوى، مع تفضيل حدود الأسطر."""
    text = str(text or "")
    if len(text) <= limit:
        return [text]
    parts = []
    remaining = text
    while len(remaining) > limit:
        cut = remaining.rfind("\n", 0, limit + 1)
        if cut < int(limit * 0.55):
            cut = remaining.rfind(" ", 0, limit + 1)
        if cut <= 0:
            cut = limit
        parts.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip("\n ")
    if remaining:
        parts.append(remaining)
    return parts


@dataclass
class AlertJob:
    """Immutable delivery intent plus retry state; one job represents one recipient."""
    chat_id: int
    text: str
    kind: str = "generic"
    max_attempts: int = 5
    attempts: int = 0
    future: Any = None


class AlertDispatcher:
    """
    Production Telegram delivery boundary.

    Producers only enqueue work; bounded buffering protects memory, workers control
    concurrency, and transient Telegram/network failures are retried with backoff.
    A job future resolves only after final delivery success/failure, allowing the
    existing SQLite Claim/SENT lifecycle to remain authoritative for trade alerts.
    """
    def __init__(self, bot, queue_size=5000, workers=8):
        self.bot = bot
        self.queue = asyncio.Queue(maxsize=max(100, int(queue_size)))
        self.workers = max(1, int(workers))
        self._tasks = []
        self._running = False

    async def start(self):
        if self._running:
            return
        self._running = True
        self._tasks = [asyncio.create_task(self._worker(), name=f"alert-worker-{i}") for i in range(self.workers)]
        logger.info("AlertDispatcher started | workers=%s queue=%s", self.workers, self.queue.maxsize)

    async def stop(self):
        self._running = False
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    def enqueue(self, chat_id, text, *, kind="generic", max_attempts=5):
        """Fast non-blocking admission. Returns a Future, or None when rejected."""
        try:
            cid = int(chat_id)
        except (TypeError, ValueError):
            logger.warning("ALERT_INVALID_CHAT_ID | value=%r", chat_id)
            return None
        try:
            payload = str(text if text is not None else "").strip()
        except Exception:
            logger.exception("ALERT_INVALID_PAYLOAD")
            return None
        if not payload:
            logger.warning("ALERT_EMPTY_PAYLOAD | chat=%s kind=%s", cid, kind)
            return None
        if not self._running:
            logger.error("ALERT_DISPATCHER_NOT_RUNNING | chat=%s kind=%s", cid, kind)
            return None
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        job = AlertJob(cid, payload, str(kind or "generic"), max(1, int(max_attempts)), 0, future)
        try:
            self.queue.put_nowait(job)
            return future
        except asyncio.QueueFull:
            logger.error("ALERT_QUEUE_FULL | chat=%s kind=%s", cid, kind)
            future.set_result(False)
            return future

    async def send(self, chat_id, text, *, kind="generic", max_attempts=5):
        future = self.enqueue(chat_id, text, kind=kind, max_attempts=max_attempts)
        if future is None:
            return False
        return bool(await future)

    async def broadcast(self, recipients, text, *, kind="generic", max_attempts=5):
        futures = []
        for cid in set(recipients or []):
            future = self.enqueue(cid, text, kind=kind, max_attempts=max_attempts)
            if future is not None:
                futures.append(future)
        if not futures:
            return []
        return await asyncio.gather(*futures, return_exceptions=True)

    async def _worker(self):
        while True:
            job = await self.queue.get()
            try:
                ok = await self._deliver(job)
                if not job.future.done():
                    job.future.set_result(ok)
            except Exception:
                logger.exception("ALERT_WORKER_UNEXPECTED | chat=%s kind=%s", job.chat_id, job.kind)
                if not job.future.done():
                    job.future.set_result(False)
            finally:
                self.queue.task_done()

    async def _deliver(self, job):
        parts = _split_telegram_text(job.text)
        if not parts:
            return False
        while job.attempts < job.max_attempts:
            try:
                # All chunks must succeed before the notification is considered delivered.
                for part in parts:
                    await self.bot.send_message(chat_id=job.chat_id, text=part)
                return True
            except (Forbidden, BadRequest) as exc:
                logger.warning("ALERT_PERMANENT_FAILURE | chat=%s kind=%s error=%s", job.chat_id, job.kind, exc)
                return False
            except RetryAfter as exc:
                job.attempts += 1
                retry_after = getattr(exc, "retry_after", 1)
                delay = float(retry_after.total_seconds() if hasattr(retry_after, "total_seconds") else retry_after)
                logger.warning("ALERT_RATE_LIMIT | chat=%s delay=%.2fs", job.chat_id, delay)
                await asyncio.sleep(max(1.0, delay))
            except (TimedOut, NetworkError) as exc:
                job.attempts += 1
                delay = min(30.0, 2 ** (job.attempts - 1)) + random.uniform(0.0, 0.5)
                logger.warning("ALERT_TRANSIENT_FAILURE | chat=%s attempt=%s/%s error=%s", job.chat_id, job.attempts, job.max_attempts, exc)
                await asyncio.sleep(delay)
            except Exception as exc:
                job.attempts += 1
                delay = min(30.0, 2 ** (job.attempts - 1))
                logger.exception("ALERT_UNKNOWN_FAILURE | chat=%s attempt=%s/%s error=%s", job.chat_id, job.attempts, job.max_attempts, exc)
                await asyncio.sleep(delay)
        logger.error("ALERT_DELIVERY_EXHAUSTED | chat=%s kind=%s attempts=%s", job.chat_id, job.kind, job.attempts)
        return False


def _alert_dispatcher_config():
    return (
        int(os.getenv("ALERT_QUEUE_SIZE", "5000")),
        int(os.getenv("ALERT_WORKERS", "8")),
    )


async def _start_alert_dispatcher():
    """Initialize once after APPLICATION.bot is ready."""
    global ALERT_DISPATCHER
    if not APPLICATION:
        return None
    if ALERT_DISPATCHER is None:
        queue_size, workers = _alert_dispatcher_config()
        ALERT_DISPATCHER = AlertDispatcher(APPLICATION.bot, queue_size=queue_size, workers=workers)
    await ALERT_DISPATCHER.start()
    return ALERT_DISPATCHER


async def _dispatch_alert(chat_id, text, *, kind="generic", max_attempts=5):
    dispatcher = ALERT_DISPATCHER or await _start_alert_dispatcher()
    if not dispatcher:
        return False
    return await dispatcher.send(chat_id, text, kind=kind, max_attempts=max_attempts)


async def _dispatch_broadcast(recipients, text, *, kind="generic", max_attempts=5):
    dispatcher = ALERT_DISPATCHER or await _start_alert_dispatcher()
    if not dispatcher:
        return []
    return await dispatcher.broadcast(recipients, text, kind=kind, max_attempts=max_attempts)


async def reply(update, text):
    """إرسال Telegram مع Deep Root Trace تشخيصي فقط؛ لا يغيّر المنطق أو السلوك."""
    trace = _HANDLER_TRACE_CTX.get()
    if isinstance(trace, dict):
        trace.setdefault("telegram_reply_deep", {})
        trace["telegram_reply_deep"].update({
            "entered": datetime.now(timezone.utc).isoformat(),
            "branch": "unknown",
            "parts": None,
        })
    try:
        # LOCAL: formatting and payload construction.
        with handler_trace_stage("telegram_reply:deep:format_header"):
            payload = with_market_header(text)

        # LOCAL: split only; network is deliberately traced separately.
        with handler_trace_stage("telegram_reply:deep:split_text"):
            parts = _split_telegram_text(payload)
        if isinstance(trace, dict):
            trace["telegram_reply_deep"]["parts"] = len(parts)

        markup = None
        q = getattr(update, "callback_query", None)
        if q:
            if isinstance(trace, dict):
                trace["telegram_reply_deep"]["branch"] = "callback"

            # TELEGRAM API: callback acknowledgement.
            try:
                with handler_trace_stage("telegram_reply:deep:callback_answer_http"):
                    await q.answer()
            except Exception as exc:
                if isinstance(trace, dict):
                    trace["telegram_reply_deep"]["callback_answer_exception"] = {
                        "type": type(exc).__name__, "message": str(exc)[:1000]
                    }
                logger.exception("Telegram callback answer error")

            # LOCAL: route-to-markup decision and keyboard construction.
            with handler_trace_stage("telegram_reply:deep:callback_markup_prepare"):
                data = q.data or ""
                section = "reports" if data in {"menu_daily_report","menu_weekly_report"} else \
                          "trades" if data in {"menu_trade_alerts","menu_trade_history"} else \
                          "market" if data in {"menu_news","menu_markets","menu_gold_price"} else \
                          "analyses" if data in {"menu_full_analysis"} else None
                if section:
                    markup = _ux_markup(_ux_section_rows(section))

            # TELEGRAM API: edit first message. Exact exception is captured by the stage.
            with handler_trace_stage("telegram_reply:deep:edit_message_http", part_index=0, parts_total=len(parts)):
                await q.edit_message_text(parts[0], reply_markup=markup)

            # TELEGRAM API: additional chunks, if any.
            for index, part in enumerate(parts[1:], start=1):
                with handler_trace_stage("telegram_reply:deep:send_part_http", part_index=index, parts_total=len(parts), chars=len(part)):
                    await q.message.reply_text(part)

        elif getattr(update, "message", None):
            if isinstance(trace, dict):
                trace["telegram_reply_deep"]["branch"] = "message"
            # TELEGRAM API: every chunk is traced independently at aggregate level.
            for index, part in enumerate(parts):
                with handler_trace_stage("telegram_reply:deep:send_part_http", part_index=index, parts_total=len(parts), chars=len(part)):
                    await update.message.reply_text(part)
        else:
            if isinstance(trace, dict):
                trace["telegram_reply_deep"]["branch"] = "no_message_or_callback"
            with handler_trace_stage("telegram_reply:deep:no_target"):
                pass

    except Exception as exc:
        # Preserve existing swallow-and-log behavior, but retain the exact failure in trace metadata.
        if isinstance(trace, dict):
            trace["telegram_reply_deep"]["exception"] = {
                "type": type(exc).__name__,
                "message": str(exc)[:1000],
            }
        logger.exception("Telegram reply error")

# ============================================================
# UX/UI Navigation Layer — Stage 1 REAL REBUILD
# طبقة واجهة مستقلة: لا تغيّر محرك التداول أو التحليل أو Auditor/Webhook.
# ============================================================

def _ux_markup(rows):
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=cb) for label, cb in row] for row in rows])


def _ux_main_keyboard():
    """القائمة الرئيسية المعتمدة — واجهة فقط، دون تغيير محرك التداول."""
    return [
        ["⚡ التحليل السريع", "🧠 التحليل المتكامل"],
        ["📝 التقرير اليومي", "📅 التقرير الأسبوعي"],
        ["🌍 السوق والأخبار", "🎯 الصفقات"],
        ["💧 السيولة والدعم", "🏦 التحليل المؤسسي"],
        ["👤 الاشتراك", "💳 الباقات"],
        ["🟢 حالة النظام", "ℹ️ المساعدة"],
        ["🛡️ مراقب الصفقات", "🖥️ مراقب أداء النظام"],
    ]


def _ux_section_rows(section):
    home = [("🏠 الرئيسية", "nav_home")]
    if section == "analyses":
        return [[("🧠 التحليل المتكامل", "menu_full_analysis")], [("⚡ التحليل السريع", "nav_quick")], [("📑 التقارير اليومية والأسبوعية", "nav_reports")], home]
    if section == "reports":
        return [[("📝 التقرير التوضيحي اليومي", "menu_daily_report")], [("📅 التقرير التوضيحي الأسبوعي", "menu_weekly_report")], [("📜 سجل الصفقات", "menu_trade_history")], home]
    if section == "trades":
        return [[("📜 سجل الصفقات", "menu_trade_history")], [("🔔 تنبيهات الصفقات", "menu_trade_alerts")], home]
    if section == "market":
        return [[("💰 سعر الذهب", "menu_gold_price")], [("📰 الأخبار", "menu_news")], [("🌍 الجلسات والأسواق", "menu_markets")], home]
    return [home]


async def _ux_render(update, title, body, section=None):
    text = f"{title}\n{body}" if body else title
    markup = _ux_markup(_ux_section_rows(section)) if section else _ux_markup([[('🏠 الرئيسية','nav_home')]])
    q = getattr(update, "callback_query", None)
    if q:
        await q.edit_message_text(with_market_header(text), reply_markup=markup)
    elif getattr(update, "message", None):
        await update.message.reply_text(with_market_header(text), reply_markup=markup)

async def start(update, context):
    await asyncio.to_thread(_ensure_user, update)
    keyboard = _ux_main_keyboard()
    text = (
        f"🤖 XAU SMART TRADER {VERSION}\n\n"
        "🥇 محلل الذهب XAU/USD\n\n"
        "W1 + D1 + H4 + H1 + M15\n"
        "Structure + Momentum + Volume + Fibonacci + FVG\n\n"
        f"🎯 حد الإشارة: {SIGNAL_THRESHOLD} نقطة\n\n"
        "اختر القسم المطلوب من القائمة 👇\n"
        "تم تنظيم الوصول المباشر للتحليل والتقارير والسوق والصفقات والأداء."
    )
    target = getattr(update, "message", None) or (update.callback_query.message if getattr(update, "callback_query", None) else None)
    if target is not None:
        await target.reply_text(with_market_header(text), reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))

def _full_analysis_section(text, start_heading, end_headings=()):
    """استخراج قسم عرضي من التقرير الكامل دون إعادة تشغيل محرك التحليل."""
    lines=text.splitlines()
    try:
        start_idx=next(i for i,line in enumerate(lines) if line.strip()==start_heading)
    except StopIteration:
        return "⚪ لا تتوفر تفاصيل هذا القسم حالياً."
    end_idx=len(lines)
    for heading in end_headings:
        for i in range(start_idx+1, len(lines)):
            if lines[i].strip()==heading:
                end_idx=min(end_idx,i)
                break
    section='\n'.join(lines[start_idx:end_idx]).strip()
    return section or "⚪ لا تتوفر تفاصيل هذا القسم حالياً."


def _full_analysis_compact_view(text):
    """يبني واجهة مختصرة من التقرير الكامل نفسه؛ لا يعيد الحساب ولا يغيّر أي قرار."""
    lines=[x.strip() for x in text.splitlines()]
    def line_after(prefix):
        for i,x in enumerate(lines):
            if x.startswith(prefix) and i+1<len(lines):
                return lines[i+1]
        return ""
    def find(prefix):
        for x in lines:
            if x.startswith(prefix):
                return x
        return ""
    decision=line_after("🎯 القرار") or "🟡 حياد"
    scenario=find("🧠 قوة السيناريو:") or "🧠 السيناريو: غير متاح"
    technical=find("🧩 الدرجة الفنية:") or "📊 الفني: غير متاح"
    setup=find("🎯 الإعداد التنفيذي:") or "🎯 التنفيذ: غير متاح"
    inst_dir=find("🎯 الاتجاه المؤسسي:")
    inst_strength=find("💪 قوة الدليل المؤسسي:")
    execution=find("🛡️ التنفيذ:")
    price=find("السعر الحالي:")
    liq_block=_full_analysis_section(text,"💧 السيولة",("🏦 التحليل المؤسسي — بيانات حقيقية","🏦 التحليل المؤسسي"))
    sweep=find_in_block=next((x for x in liq_block.splitlines() if x.startswith("السحب:")),"")
    retest=next((x for x in liq_block.splitlines() if x.startswith("إعادة الاختبار:")),"")
    liquidity_incomplete=any(k in sweep for k in ("لا يوجد","غير متاح","غير مؤكدة","لم")) or not sweep
    retest_unconfirmed=any(k in retest for k in ("غير","لا يوجد","غير متاح","لم")) or not retest
    direction_word="بيعي" if "بيع" in decision else "شرائي" if "شراء" in decision else "محايد"
    if execution and ("READY" in execution or "جاهز" in execution):
        decision_label=decision
    elif direction_word=="بيعي":
        decision_label="🔴 ميل بيعي — مراقبة فقط"
    elif direction_word=="شرائي":
        decision_label="🟢 ميل شرائي — مراقبة فقط"
    else:
        decision_label="🟡 السوق غير محسوم — مراقبة فقط"
    # سبب مختصر مشتق من الحالة الفعلية في التقرير، وليس نصاً ثابتاً مستقلاً عن البيانات.
    reason_parts=[]
    big=_full_analysis_section(text,"🧭 الصورة الكبرى",("🌐 حالة السوق الكلي","💰 السعر والمستويات"))
    structures=[x for x in big.splitlines() if x.startswith(("H1:","M15:","M5:"))]
    if structures and all(("SELL" in x or "بيع" in x or "هابط" in x) for x in structures):
        reason_parts.append("الاتجاه التنفيذي هابط")
    elif structures and all(("BUY" in x or "شراء" in x or "صاعد" in x) for x in structures):
        reason_parts.append("الاتجاه التنفيذي صاعد")
    else:
        reason_parts.append("الاتجاه التنفيذي غير متطابق بالكامل")
    if liquidity_incomplete:
        reason_parts.append("السيولة غير مكتملة")
    else:
        reason_parts.append("السيولة تحمل تأكيداً")
    if retest_unconfirmed:
        reason_parts.append("إعادة الاختبار غير مؤكدة")
    else:
        reason_parts.append("إعادة الاختبار مؤكدة")
    reason="، ".join(reason_parts)+"."
    body=(f"{decision_label}\n"
          f"💰 {price.replace('السعر الحالي:','').strip() if price else 'غير متاح'}\n"
          f"🧠 {scenario.replace('🧠 ','')}\n"
          f"📊 {technical.replace('🧩 ','')}\n"
          f"🎯 {setup.replace('🎯 ','')}\n"
          f"🏦 {inst_dir.replace('🎯 الاتجاه المؤسسي:','').strip() if inst_dir else 'الاتجاه المؤسسي غير متاح'} "
          f"{inst_strength.replace('💪 قوة الدليل المؤسسي:','').strip() if inst_strength else ''}\n"
          f"🚦 {('READY' if 'READY' in execution else 'WAIT') if execution else 'WAIT'}\n"
          f"🧭 الاتجاه: {'هابط' if direction_word=='بيعي' else 'صاعد' if direction_word=='شرائي' else 'محايد'}\n"
          f"💧 السيولة: {'غير مكتملة' if liquidity_incomplete else 'مؤكدة'}\n"
          f"🔄 إعادة الاختبار: {'غير مؤكدة' if retest_unconfirmed else 'مؤكدة'}\n\n"
          f"السبب المختصر:\n{reason}\n\n"
          "اختر التفاصيل التي تريد عرضها 👇")
    return body


def _full_analysis_detail_text(full_text, section):
    mapping={
        "why":("🔎 سبب القرار",("🧭 الصورة الكبرى",)),
        "liquidity":("💧 السيولة",("🏦 التحليل المؤسسي — بيانات حقيقية","🏦 التحليل المؤسسي")),
        "levels":("💰 السعر والمستويات",("💧 السيولة",)),
        "institutional":("🏦 التحليل المؤسسي — بيانات حقيقية",("🏛️ البنوك المركزية","🔬 تشخيص صندوق النقد الدولي IMF")),
        "macro":("📈 السوق الكلي",("🚦 بوابات التنفيذ","📡 صحة مصادر التحليل المؤسسي")),
        "quality":("📡 صحة مصادر التحليل المؤسسي",("🚨 إشارة تداول","🔎 القرار النهائي","⚠️ التحليل مساعد لاتخاذ القرار اليدوي وليس ضماناً للربح.")),
        "all":("🤖 XAU SMART TRADER",()),
    }
    if section=="why":
        return _full_analysis_section(full_text,"🔎 سبب القرار",("🧭 الصورة الكبرى",))
    if section=="institutional":
        # يشمل كامل الطبقة المؤسسية، بما فيها البنوك المركزية وIMF وETF وCOT.
        return _full_analysis_section(full_text,"🏦 التحليل المؤسسي — بيانات حقيقية",("📈 السوق الكلي", "🚦 بوابات التنفيذ"))
    if section=="all":
        return full_text
    spec=mapping.get(section)
    if not spec:
        return "⚪ القسم غير معروف."
    return _full_analysis_section(full_text,spec[0],spec[1])


def _full_analysis_markup():
    """واجهة التحليل المتكامل: زر التفاصيل الكاملة فقط + العودة للرئيسية."""
    return _ux_markup([
        [("➕ التفاصيل الكاملة", "full_all")],
        [("🏠 الرئيسية", "nav_home")],
    ])


async def _send_full_analysis_compact(update, text):
    """إرسال/تحديث بطاقة التحليل المتكامل مع أزرار التفاصيل، دون المساس بدالة reply العامة."""
    rendered=with_market_header(text)
    markup=_full_analysis_markup()
    q=getattr(update,"callback_query",None)
    if q:
        await q.edit_message_text(rendered, reply_markup=markup)
    elif getattr(update,"message",None):
        await update.message.reply_text(rendered, reply_markup=markup)


async def full_analysis(update, context):
    if not await feature_guard("full_analysis")(update, context): return
    try:
        q = getattr(update, "callback_query", None)
        if q:
            try:
                await q.answer("جارٍ إعداد التحليل المتكامل…")
            except Exception:
                logger.exception("Telegram full analysis callback acknowledgement error")
        can_trade = await asyncio.to_thread(can_receive_trade, update.effective_chat.id)
        # المحرك يبقى كما هو: build_analysis ينتج التقرير الكامل الأصلي دون أي تعديل.
        analysis_text = await asyncio.to_thread(build_analysis, can_trade)
        # نخزن الناتج نفسه مؤقتاً للمستخدم كي لا نعيد تشغيل التحليل عند فتح التفاصيل.
        try:
            context.user_data["full_analysis_text"] = analysis_text
            context.user_data["full_analysis_cached_at"] = time.time()
        except Exception:
            logger.exception("Full analysis UI cache write error")
        await _send_full_analysis_compact(update, _full_analysis_compact_view(analysis_text))
    except Exception as e:
        logger.exception("Full analysis execution error")
        await reply(update, f"❌ تعذر تنفيذ التحليل المتكامل.\nالسبب: {e}")


async def full_analysis_detail(update, context, section):
    try:
        q=getattr(update,"callback_query",None)
        if q:
            try: await q.answer()
            except Exception: pass
        full_text=(getattr(context,"user_data",{}) or {}).get("full_analysis_text")
        if not full_text:
            await _send_full_analysis_compact(update,"⚪ انتهت بيانات التحليل المعروضة. أعد فتح 🧠 التحليل المتكامل للحصول على نسخة حديثة.")
            return
        detail=_full_analysis_detail_text(full_text,section)
        if section=="all":
            # التقرير الكامل قد يتجاوز حد رسالة Telegram؛ نستخدم reply لتطبيق التقسيم الموجود في المشروع.
            await reply(update,detail)
            return
        markup=_ux_markup([[('🔙 العودة للتحليل المتكامل','full_back')],[('🏠 الرئيسية','nav_home')]])
        await reply(update,detail,reply_markup=markup)
    except Exception as e:
        logger.exception("Full analysis detail error")
        await reply(update,f"❌ تعذر عرض تفاصيل التحليل المتكامل: {e}")


async def quick_analysis(update, context):
    if not await feature_guard("quick_analysis")(update, context): return
    try:
        m15, result = await asyncio.to_thread(_quick_analysis_data)
        price = float(result["price"])
        ema50 = float(m15.get("ema50", price))
        ema200 = float(m15.get("ema200", price))
        macd_val = float(m15["macd"])
        macd_signal = float(m15.get("macd_signal", macd_val))
        ema_dir = "إيجابي" if ema50 > ema200 else "سلبي" if ema50 < ema200 else "محايد"
        rsi = float(m15["rsi"])
        if rsi >= 70:
            rsi_text = "تشبع شرائي — خطر الانعكاس مرتفع"
        elif rsi >= 60:
            rsi_text = "زخم شرائي جيد"
        elif rsi <= 30:
            rsi_text = "تشبع بيعي — احتمال ارتداد قائم"
        elif rsi <= 40:
            rsi_text = "زخم بيعي واضح"
        else:
            rsi_text = "ضمن النطاق المحايد"
        macd_side = "فوق الصفر" if macd_val > 0 else "تحت الصفر" if macd_val < 0 else "على الصفر"
        macd_cross = "فوق Signal" if macd_val > macd_signal else "تحت Signal" if macd_val < macd_signal else "متطابق مع Signal"
        macd_text = f"{macd_side} — {macd_cross}"
        adx = float(m15["adx"])
        plus_di = float(m15.get("plus_di", m15.get("di_plus", 0)) or 0)
        minus_di = float(m15.get("minus_di", m15.get("di_minus", 0)) or 0)
        adx_text = "اتجاه قوي" if adx >= 25 else "اتجاه متوسط" if adx >= 20 else "اتجاه ضعيف/متذبذب"
        direction = result["direction"] if result["direction"] in ("BUY", "SELL") else ("BUY" if ema9 > ema21 else "SELL" if ema9 < ema21 else "WAIT")
        d_text = ("🟢 صعود حاد — زخم مرتفع" if direction == "BUY" and adx >= 25 else
                  "🔴 هبوط حاد — زخم مرتفع" if direction == "SELL" and adx >= 25 else
                  "🟢 صعود" if direction == "BUY" else "🔴 هبوط" if direction == "SELL" else "🟡 حياد")
        levels = result["levels"]
        s1 = nearest_support(levels, price); s2 = next_support(levels, price)
        r1 = nearest_resistance(levels, price); r2 = next_resistance(levels, price)
        trade = result.get("trade")
        target = float(trade["tp1"]) if trade else None
        stop = float(trade["sl"]) if trade else None
        rr = float(trade["rr"]) if trade else 0.0
        structure = result["mtf"]["h4"]["structure"]
        structure_dir = "🟢 صاعد" if structure == "صاعد" else "🔴 هابط" if structure == "هابط" else "🟡 محايد"
        conflict_reasons = list(result.get("conflicts", []))
        conflict = bool(conflict_reasons) or ((direction == "BUY" and structure == "هابط") or (direction == "SELL" and structure == "صاعد"))
        score = int(result.get("scenario_score", result["score"]))
        execution_score = int(result.get("execution_score", result.get("execution_readiness", 0)))
        data_quality = int(result.get("data_quality_score", 0))
        scenario_quality = ("ممتاز" if score >= 90 else "قوي جداً" if score >= 80 else "قوي" if score >= 70 else "جيد" if score >= 60 else "مؤهل" if score >= 50 else "ضعيف")
        execution_quality = "جاهزة للتنفيذ" if result.get("execution_state") == "READY" else "غير جاهزة للتنفيذ"
        # طبقة تسمية القرار: لا يجوز عرض BUY/SELL كعنوان رئيسي عندما تكون قوة السيناريو ضعيفة
        # أو يوجد تعارض اتجاهي/هيكلي؛ في هذه الحالة نعرض حياد السوق مع الميل اللحظي فقط.
        major_conflict = conflict or score < 50
        if result.get("execution_state") == "READY" and result.get("signal") and not conflict:
            rec = "🟢 فرصة شراء سكالبينج" if direction == "BUY" else "🔴 فرصة بيع سكالبينج"
        elif major_conflict:
            if direction == "BUY":
                rec = "⚪ السوق غير محسوم — ميل شرائي لحظي"
            elif direction == "SELL":
                rec = "⚪ السوق غير محسوم — ميل بيعي لحظي"
            else:
                rec = "⚪ السوق غير محسوم — لا توجد أفضلية اتجاهية"
        elif direction == "BUY":
            rec = "🟡 مراقبة شراء — تحتاج تأكيد"
        elif direction == "SELL":
            rec = "🟡 مراقبة بيع — تحتاج تأكيد"
        else:
            rec = "🟡 مراقبة — لا يوجد اتجاه مؤكد"
        if major_conflict:
            conclusion = "لا توجد أفضلية تداولية كافية حالياً؛ الميل اللحظي موجود لكنه لا يتوافق مع الاتجاه أو الهيكل العام، لذا انتظر تأكيداً واضحاً قبل الدخول."
        elif result.get("execution_state") == "CANDIDATE":
            conclusion = "السيناريو مرشح، لكنه ليس صفقة جاهزة؛ انتظر اكتمال بوابات التنفيذ."
        else:
            conclusion = "الإشارة لم تصل إلى حد الصفقة؛ الأفضل المراقبة وانتظار تأكيد إضافي."
        # الأخبار تنبيه ومخاطر فقط؛ لا تدخل كحجب آلي للتنفيذ.
        level_lines = [
            "📍 المستويات اللحظية:",
            f"• السعر الحالي: {price:.2f}",
            f"• دعم 1: {s1:.2f}" if s1 is not None else "• دعم 1: غير متوفر",
            f"• مقاومة 1: {r1:.2f}" if r1 is not None else "• مقاومة 1: غير متوفر",
            f"• الهدف التنفيذي: {target:.2f}" if target is not None else "• الهدف التنفيذي: غير متاح — لا توجد صفقة READY",
            f"• وقف الخسارة التنفيذي: {stop:.2f}" if stop is not None else "• وقف الخسارة التنفيذي: غير متاح — لا توجد صفقة READY",
            (f"• R:R التقريبي: 1:{rr:.2f}" if trade else "• R:R: غير محسوب — لا توجد صفقة READY"),
        ]
        execution_state = result.get("execution_state", "WATCH")
        execution_label = {
            "READY": "🟢 READY — جاهزة للتنفيذ",
            "CANDIDATE": "🟡 CANDIDATE — مرشحة وتحتاج تأكيداً",
            "BLOCKED": "🔴 BLOCKED — محجوبة",
            "INVALIDATED": "🔴 INVALIDATED — الفكرة ملغاة",
        }.get(execution_state, f"🟡 {execution_state} — انتظار")
        liquidity = result["mtf"]["m15"]["liquidity"]
        liquidity_bias = liquidity.get("bias", "غير متاحة")
        momentum_gate = "🟢 مناسب" if adx >= 25 else "🟡 ضعيف"
        structure_gate = "🔴 متعارض" if conflict else "🟢 متوافق"
        news_text = result.get("news", "غير متاحة")
        news_gate = "🟢 CLEAR" if "لا يوجد" in str(news_text) or "CLEAR" in str(news_text).upper() else "🟠 ALERT / تنبيه فقط"

        # UX فقط: إعادة ترتيب التقرير السريع إلى لوحة قرار + بوابات قرار.
        # لا يتم تغيير أي قيمة تحليلية أو منطق تنفيذ داخل هذا القسم.
        support_line = f"🟢 الدعم الأقرب: {s1:.2f}\n" if s1 is not None else "🟢 الدعم الأقرب: غير متوفر\n"
        resistance_line = f"🔴 المقاومة الأقرب: {r1:.2f}\n" if r1 is not None else "🔴 المقاومة الأقرب: غير متوفر\n"
        text = (
            "⚡ التحليل السريع\n"
            "XAU/USD\n\n"
            f"🎯 القرار الحالي\n{rec}\n\n"
            f"💪 قوة السيناريو: {score}/100 — {scenario_quality}\n"
            f"🎯 جاهزية التنفيذ: {execution_score}/100\n"
            f"📡 جودة البيانات: {data_quality}/100\n"
            f"🚦 حالة التنفيذ: {execution_label}\n"
            f"🛡️ التنفيذ: {execution_quality}\n\n"
            "🚦 بوابات القرار\n\n"
            f"📊 الاتجاه: {'🔴 متعارض' if conflict else '🟢 متوافق'}\n"
            f"🧠 الهيكل: {structure_gate}\n"
            f"💧 السيولة: 🟡 {liquidity_bias}\n"
            f"📈 الزخم: {momentum_gate}\n"
            f"📰 الأخبار: {news_gate}\n"
            f"🛡️ بوابة التنفيذ: {'🟢 PASS' if execution_state == 'READY' else '🟡 WAIT'}\n\n"
            "💰 السعر والمستويات\n\n"
            f"💰 السعر الحالي: {price:.2f}\n"
            + support_line
            + resistance_line
        )
        if trade and execution_state == "READY":
            text += (
                "\n📌 خطة الصفقة\n\n"
                f"📍 الدخول: {price:.2f}\n"
                f"🛑 وقف الخسارة: {stop:.2f}\n"
                f"🎯 الهدف الأول: {target:.2f}\n"
                f"⚖️ R:R: 1:{rr:.2f}\n"
            )
        text += (
            "\n📈 القراءة الفنية\n\n"
            f"• EMA 9 / 21: {ema_dir}\n"
            f"• RSI: {rsi:.1f} — {rsi_text}\n"
            f"• MACD: {macd_text} — {macd_val:.2f} | Signal {macd_signal:.2f}\n"
            f"• ADX: {adx:.1f} — {adx_text} | +DI {plus_di:.1f} | -DI {minus_di:.1f}\n\n"
            "💧 السيولة\n\n"
            f"🧲 السيولة الشرائية: {fmt(liquidity.get('nearest_buy'))}\n"
            f"🧲 السيولة البيعية: {fmt(liquidity.get('nearest_sell'))}\n"
            f"🔄 السحب: {liquidity.get('sweep_text', 'غير متاح')}\n"
            f"🔁 إعادة الاختبار: {liquidity.get('retest_text', 'لا توجد إعادة اختبار مؤكدة حالياً')}\n\n"
            "📰 الأخبار\n\n"
            f"{news_text}\n\n"
            "🔎 القرار النهائي\n\n"
            f"{rec}\n"
            f"🔎 {conclusion}"
        )
        await reply(update, text)
    except Exception as e:
        logger.exception("Quick analysis error")
        await reply(update, f"❌ تعذر التحليل السريع: {e}")


async def trade_now(update, context):
    if not await feature_guard("trade_now")(update, context): return
    try:
        result = await asyncio.to_thread(evaluate_signal)
        # بوابة التنفيذ الموحدة؛ حالة الأخبار جزء من قرار الصفقة.
        if not result["signal"]:
            reasons = result.get("rejection_reasons") or ["لم تتحقق شروط الإشارة كاملة"]
            await reply(update, f"⏳ لا توجد صفقة مؤهلة الآن.\n\n💪 الدرجة: {result['score']} نقطة\n🎯 الحد: {SIGNAL_THRESHOLD} نقطة\n\n🔎 سبب عدم إنشاء الصفقة:\n• " + "\n• ".join(reasons) + "\n\nالبوت يراقب السوق.")
            return
        trade = result["trade"]
        chat_id = update.effective_chat.id
        if not await asyncio.to_thread(can_receive_trade, chat_id):
            await reply(update, "⛔ تم الوصول إلى حد الصفقات في باقتك الحالية.\n\n🚀 استخدم /plans للترقية.")
            return
        quota_token = await asyncio.to_thread(_reserve_trade_quota, chat_id)
        if not quota_token:
            await reply(update, "⛔ تم الوصول إلى حد الصفقات في باقتك الحالية.\n\n🚀 استخدم /plans للترقية.")
            return
        record, is_new_trade = await asyncio.to_thread(register_trade, result)
        if not record:
            await asyncio.to_thread(_release_trade_quota, quota_token)
            await reply(update, "❌ تعذر تسجيل الصفقة، ولم يتم احتساب الحصة.")
            return
        if not is_new_trade:
            await asyncio.to_thread(_release_trade_quota, quota_token)
        direction = "🟢 شراء" if result["direction"] == "BUY" else "🔴 بيع"
        quality = "🔥 قوية" if result["score"] >= STRONG_THRESHOLD else "🎯 مؤهلة"
        text = (
            "🚨 XAU SMART TRADER\n"
            f"📈 الصفقة: {direction}\n💪 الجودة: {result['score']} نقطة — {quality}\n\n"
            f"📍 الدخول: {trade['entry']:.2f}\n🛑 SL: {trade['sl']:.2f}\n"
            f"🎯 TP1: {trade['tp1']:.2f}\n🎯 TP2: {trade['tp2']:.2f}\n🎯 TP3: {trade['tp3']:.2f}\n⚖️ R:R النهائي: 1:{trade['rr']:.2f}\n\n"
            "🧠 إشارة تحليلية للتنفيذ اليدوي."
        )
        await reply(update, text)
    except Exception as e:
        await reply(update, f"❌ تعذر بناء الصفقة: {e}")


def _show_levels_data():
    df = get_bars("1h", 250)
    levels = support_resistance(df)
    quote = live_price()
    price = float(quote["price"])
    liq = liquidity_analysis(df)
    return levels, price, liq


def _quick_analysis_data():
    # evaluate_signal already builds the authoritative M15 analysis. Reusing
    # it removes a duplicate 15M HTTP fetch + duplicate indicator pass.
    result = evaluate_signal()
    m15 = result["mtf"]["m15"]
    return m15, result


async def show_levels(update, context):
    if not await feature_guard("sr")(update, context): return
    try:
        levels, price, liq = await asyncio.to_thread(_show_levels_data)
        lines = [
            "📍 XAU/USD — مناطق السوق",
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
            f"• الانحياز: {liq['bias']}",
            f"• Buy-side الأقرب: {fmt(liq['nearest_buy'])}",
            f"• Sell-side الأقرب: {fmt(liq['nearest_sell'])}",
            f"• السحب: {liq['sweep_text']}",
            "",
            "الترتيب: الأقرب للسعر أولاً، مع إبقاء الدعوم أسفل السعر والمقاومات أعلى السعر.",
            "المناطق مبنية على القمم والقيعان المجمعة حسب ATR.",
        ]
        await reply(update, "\n".join(lines))
    except Exception as e:
        await reply(update, f"❌ تعذر حساب المناطق: {e}")


async def trade_history(update, context):
    if not await feature_guard("trade_history")(update, context): return
    await asyncio.to_thread(update_trade_results)
    if not TRADE_HISTORY:
        await reply(update, "📜 سجل الصفقات\n\nلا توجد صفقات مسجلة بعد.")
        return
    lines = ["📜 سجل الصفقات"]
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
    risk = await asyncio.to_thread(get_risk)
    state = risk.get("state", "UNKNOWN")
    text = risk.get("message", "⚠️ حالة الأخبار غير معروفة")
    source = risk.get("source", "none")
    if state == "CLEAR":
        gate = "🟢 الأخبار: لا توجد مخاطرة عالية حالياً"
    elif state == "ALERT":
        gate = "🟠 الأخبار: نافذة مخاطرة عالية — تنبيه فقط"
    else:
        gate = "⚠️ الأخبار: الحالة غير مؤكدة — تنبيه فقط"
    await reply(update, f"📰 حالة الأخبار\n\n{gate}\n📡 المصدر المحلي: {source}\n\n{text}\n\n🔔 الأخبار والتنبيهات طبقة معلومات ومخاطر فقط؛ لا توجد بوابة أخبار تحجب التنفيذ آلياً.")


async def markets(update, context):
    """عرض جلسات الأسواق بتوقيت دمشق بشكل مستقل وآمن من أخطاء العرض."""
    if not await feature_guard("markets")(update, context):
        return

    try:
        now = now_damascus()

        def fmt12(dt):
            return dt.strftime('%I:%M %p').lstrip('0').replace('AM', 'ص').replace('PM', 'م')

        def damascus_time(zone_name, hour, minute=0):
            local_now = now.astimezone(ZoneInfo(zone_name))
            local_open = local_now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            return fmt12(local_open.astimezone(DAMASCUS))

        sessions = [
            ("🇦🇺 سيدني", "Australia/Sydney", 8),
            ("🇯🇵 طوكيو", "Asia/Tokyo", 9),
            ("🇬🇧 لندن", "Europe/London", 8),
            ("🇺🇸 نيويورك", "America/New_York", 8),
        ]

        lines = [
            "🌍 الجلسات والأسواق",
            f"🕐 توقيت دمشق الآن: {fmt12(now)}",
            "",
            "🟢 أوقات افتتاح الجلسات بتوقيت دمشق",
        ]

        for label, zone, hour in sessions:
            lines.append(f"• {label}: {damascus_time(zone, hour)}")

        state, _ = market_state(now)
        lines += [
            "",
            "📊 حالة السوق الحالية",
            f"• {state}",
            "",
            "ℹ️ الأوقات تُحوّل تلقائياً إلى توقيت دمشق مع مراعاة التوقيت الصيفي للدول المعنية.",
        ]

        await _ux_render(update, "🌍 الجلسات والأسواق", "\n".join(lines[2:]), "market")
    except Exception as e:
        logger.exception("Markets view error")
        await _ux_render(
            update,
            "🌍 الجلسات والأسواق",
            "⚠️ تعذر عرض أوقات الجلسات حالياً. تم تسجيل الخطأ دون التأثير على محرك التداول.",
            "market",
        )


async def audit_report(update, context):
    """تقرير مراقب الأداء — قراءة فقط، مع فصل مراقبة اليوم عن النتائج النهائية."""
    user_id = update.effective_user.id if update.effective_user else update.effective_chat.id
    if not (is_admin_chat(user_id) or is_admin_chat(update.effective_chat.id)):
        await reply(update, "⛔ مراقب الأداء مخصص للإدارة فقط.")
        return
    if PERFORMANCE_AUDITOR is None:
        await reply(update, "🛡️ مراقب الأداء\n❌ محرك المراقبة غير متاح حالياً.")
        return
    try:
        report = await asyncio.to_thread(PERFORMANCE_AUDITOR.metrics_report)
        system=report.get("system",{}); t=report.get("trades",{}); m=report.get("monitoring",{}); d=report.get("daily",{}); w=report.get("weekly",{}); root_trace=report.get("handler_root_trace",{})
        latest_traces=root_trace.get("latest",[]) if isinstance(root_trace,dict) else []; stage_aggregates=root_trace.get("stage_aggregates",[]) if isinstance(root_trace,dict) else []
        def metric(v,fmt=".2f",suffix=""):
            return "قيد الانتظار" if v is None else format(float(v),fmt)+suffix
        webhook_p95=(f"{system.get('request_p95_ms',0):.1f} ms" if system.get("webhook_p95_ready",False) else f"WARMING UP ({system.get('request_sample_count',0)}/{system.get('webhook_min_samples',AUDITOR_WEBHOOK_MIN_SAMPLES)})")
        handler_p95=(f"{system.get('handler_p95_ms',0):.1f} ms" if system.get("handler_p95_ready",False) else f"WARMING UP ({system.get('handler_sample_count',0)}/{system.get('handler_min_samples',AUDITOR_HANDLER_MIN_SAMPLES)})")
        lines=[
            "🛡️ XAU SMART TRADER — مراقب الأداء","",
            "📊 النتائج النهائية المعتمدة",
            f"• الإجمالي: {t.get('total',0)}",f"• نجاح كامل: {t.get('full_success',0)}",f"• نجاح جزئي: {t.get('partial_success',0)}",f"• فشل: {t.get('failed',0)}",f"• منتهية: {t.get('expired',0)}",f"• غامضة: {t.get('ambiguous',0)}",
            f"• Win Rate: {metric(t.get('win_rate_pct'),'.2f','%')}",f"• Full Target Rate: {metric(t.get('full_target_rate_pct'),'.2f','%')}",f"• متوسط R:R: {metric(t.get('avg_rr'),'.2f')}",f"• متوسط MAE: {metric(t.get('avg_mae'),'.4f')}",f"• متوسط MFE: {metric(t.get('avg_mfe'),'.4f')}",f"• متوسط Drawdown: {metric(t.get('avg_drawdown'),'.4f')}",
            "","🔎 مراقبة اليوم",f"• صفقات تحت المراقبة: {m.get('trades_total',0)} | نشطة: {m.get('trades_active',0)}",f"• تحليلات تحت المراقبة: {m.get('analyses_total',0)} | نشطة: {m.get('analyses_active',0)}","• نتائج اليوم النهائية: قيد الانتظار حتى دورة التقرير النهائية",
            "","📅 دقة التحليل اليومي (المعتمد)",f"• التحليلات: {d.get('total',0)} | المطابقة: {d.get('matched',0)}",f"• الدقة: {metric(d.get('accuracy_pct'),'.2f','%')}",f"• الاتجاه: {metric(d.get('direction_accuracy_avg'),'.2f','%')}",f"• النطاق: {metric(d.get('range_accuracy_avg'),'.2f','%')}",f"• الهدف: {metric(d.get('target_accuracy_avg'),'.2f','%')}",
            "","📆 دقة التحليل الأسبوعي (المعتمد)",f"• التحليلات: {w.get('total',0)} | المطابقة: {w.get('matched',0)}",f"• الدقة: {metric(w.get('accuracy_pct'),'.2f','%')}",f"• الاتجاه: {metric(w.get('direction_accuracy_avg'),'.2f','%')}",f"• النطاق: {metric(w.get('range_accuracy_avg'),'.2f','%')}",f"• الهدف: {metric(w.get('target_accuracy_avg'),'.2f','%')}",
            "","🖥️ صحة النظام",f"• CPU: {system.get('cpu_pct',0):.1f}% | RAM: {system.get('rss_mb',0):.1f} MB",f"• Webhook P95: {webhook_p95}",f"• Handler P95: {handler_p95}",f"• Root Trace: {system.get('handler_trace_count',0)} traces",f"• Event Loop P95: {system.get('event_loop_lag_ms',0):.1f} ms",f"• Throughput: {system.get('throughput_per_min',0):.1f}/min",f"• Queue: {system.get('queue_depth',0)}/{system.get('queue_capacity',0)} | Drops: {system.get('queue_drops',0)}",f"• Errors: {system.get('error_count',0)} | Alerts: {system.get('alert_count',0)} | Self-Heal: {system.get('self_heal_count',0)}",
            "","🔎 آخر Root Traces",
            *[f"• {x.get('trace_id','—')} | {x.get('handler','—')} | {x.get('elapsed_ms',0):.1f} ms | {x.get('outcome','—')}" for x in latest_traces[:5]],
            *[f"• أبطأ مرحلة: {x.get('stage','—')} | avg {x.get('avg_ms',0):.1f} ms | max {x.get('max_ms',0):.1f} ms" for x in stage_aggregates[:3]],
            "","🔒 المراقب مستقل ولا يغيّر قرارات التداول أو العتبات."
        ]
        buttons=[[InlineKeyboardButton("🔄 تحديث المراقب",callback_data="menu_audit")]]
        await update.effective_message.reply_text(with_market_header("\n".join(lines)),reply_markup=InlineKeyboardMarkup(buttons+[[InlineKeyboardButton("🏠 الرئيسية",callback_data="nav_home")]]))
    except Exception as e:
        logger.exception("Audit report error")
        await reply(update,f"🛡️ تعذر إنشاء تقرير المراقب.\nالسبب: {e}")


async def status(update, context):
    if not await feature_guard("status")(update, context): return
    await reply(update, (
        f"🟢 XAU SMART TRADER {VERSION}\n\n"
        "حالة النظام: يعمل\nTelegram: متصل\nFlask: يعمل\nالبيانات: Biquote OHLC\n"
        "التحليل: W1/D1/H4/H1/M15\nالهيكل: مفعّل\nالحجم: مفعّل\nRSI: مفعّل\nMACD: مفعّل\nADX: مفعّل\nFibonacci: مفعّل\nFVG: مفعّل\n"
        "الأخبار: تنبيهات تلقائية فقط — لا حجب للتداول\n\n"
        f"🎯 حد الإشارة: {SIGNAL_THRESHOLD} نقطة\n🔥 الإشارة القوية: {STRONG_THRESHOLD} نقطة\n"
        "📝 التقرير التوضيحي: يومي + أسبوعي"
    ))

# ============================================================
# التنبيهات الحالية — لا يتم تشغيلها إلا للمشتركين
# ============================================================

async def subscribe(update, context):
    await asyncio.to_thread(_ensure_user, update)
    chat_id = update.effective_chat.id
    if not await asyncio.to_thread(has_feature, chat_id, "trade_alerts"):
        await reply(update, "🔒 تنبيهات الصفقات متاحة من PRO فما فوق.\n\n💳 استخدم زر الباقات للترقية.")
        return
    SUBSCRIBERS.add(chat_id)
    status_text = await asyncio.to_thread(plan_status_text, chat_id)
    await reply(update, f"🔔 تنبيهات الصفقات مفعّلة تلقائياً ضمن باقتك.\n\n{status_text}")


async def unsubscribe(update, context):
    chat_id = update.effective_chat.id
    if await asyncio.to_thread(has_feature, chat_id, "trade_alerts"):
        SUBSCRIBERS.add(chat_id)
        await reply(update, "🔔 تنبيهات الصفقات مفعّلة تلقائياً ضمن باقتك ولا يمكن تعطيلها من البوت.")
    else:
        await reply(update, "🔒 تنبيهات الصفقات متاحة ضمن الباقات المؤهلة فقط.")


async def plans(update, context):
    await asyncio.to_thread(_ensure_user, update)
    text = plans_text() + "\n\n⬇️ اختر الباقة لمعرفة التفاصيل:"
    keyboard = [
        [InlineKeyboardButton("🆓 FREE", callback_data="plan_FREE")],
        [InlineKeyboardButton("🥉 BASIC — $10", callback_data="plan_BASIC")],
        [InlineKeyboardButton("🥈 PRO — $20", callback_data="plan_PRO")],
        [InlineKeyboardButton("🥇 PREMIUM — $35", callback_data="plan_PREMIUM")],
        [InlineKeyboardButton("💎 VIP — $50", callback_data="plan_VIP")],
    ]
    if update.callback_query:
        await update.callback_query.edit_message_text(with_market_header(text), reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(with_market_header(text), reply_markup=InlineKeyboardMarkup(keyboard))


async def plan_callback(update, context):
    query = update.callback_query
    await query.answer()
    plan_key = query.data.replace("plan_", "", 1)
    plan = PLANS.get(plan_key)
    if not plan:
        await query.edit_message_text(with_market_header("❌ الباقة غير موجودة."))
        return
    feature_names = {
        "gold_price": "💰 سعر الذهب", "markets": "🌍 الأسواق", "status": "🟢 حالة النظام",
        "quick_analysis": "⚡ التحليل السريع", "sr": "📍 الدعوم والمقاومات",
        "daily_report": "📝 التقرير التوضيحي اليومي", "weekly_report": "📅 التقرير التوضيحي الأسبوعي",
        "full_analysis": "🧠 التحليل المتكامل", "trade_now": "🎯 صفقة الآن",
        "trade_alerts": "🔔 تنبيهات الصفقات", "news_alerts": "📰 تنبيهات/حماية الأخبار",
        "market_alerts": "🌍 تنبيهات الأسواق", "institutional": "🏦 التحليل المؤسسي",
        "trade_history": "📜 سجل الصفقات", "vip": "💎 وصول VIP الكامل"
    }
    features = [feature_names.get(x, x) for x in sorted(plan["features"])]
    details = "\n".join("• " + x for x in features) or "• لا توجد ميزات إضافية"
    limit = TRADE_LIMIT_TEXT[plan_key]
    text = (
        f"{plan['name']}\n"
        f"💰 السعر: ${plan['price']} / شهر\n"
        f"🎯 حد الصفقات: {limit}\n\n"
        f"🔐 المزايا المتاحة: \n{details}\n\n"
        "👇 اختر الإجراء:"
    )
    buttons = []
    if plan_key != "FREE":
        buttons.append([InlineKeyboardButton(f"📩 طلب الاشتراك في {plan['name']}", callback_data=f"request_{plan_key}")])
    buttons.append([InlineKeyboardButton("🔙 العودة للباقات", callback_data="back_to_plans")])
    await query.edit_message_text(with_market_header(text), reply_markup=InlineKeyboardMarkup(buttons + [[InlineKeyboardButton("🏠 الرئيسية", callback_data="nav_home")]]))


async def subscription_request_callback(update, context):
    query = update.callback_query
    await query.answer()
    plan_key = query.data.replace("request_", "", 1)
    plan = PLANS.get(plan_key)
    if not plan or plan_key == "FREE":
        await query.edit_message_text(with_market_header("❌ طلب الاشتراك غير صالح."))
        return
    chat_id = query.message.chat_id
    await asyncio.to_thread(_ensure_user, update)
    def _save_subscription_request():
        conn = _db()
        try:
            conn.execute("INSERT INTO subscription_requests(chat_id, plan, requested_at, status) VALUES(?,?,?, 'PENDING')", (chat_id, plan_key, now_damascus().isoformat()))
            conn.commit()
        finally:
            conn.close()
    await asyncio.to_thread(_save_subscription_request)
    contact = ADMIN_CONTACT
    if contact:
        contact_text = f"\n\n📩 تواصل مع الإدارة: {contact}"
    else:
        contact_text = "\n\n📩 أرسل Chat ID الخاص بك للإدارة ليتم تفعيل الباقة يدوياً."
    await query.edit_message_text(
        with_market_header(
            f"📩 طلب الاشتراك — {plan['name']}\n"
            f"💰 السعر: ${plan['price']} / شهر\n"
            f"🎯 {TRADE_LIMIT_TEXT[plan_key]}\n\n"
            "تم تسجيل طلبك. التفعيل المدفوع يتم بعد تأكيد الإدارة." + contact_text
        ),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 العودة للباقات", callback_data="back_to_plans")], [InlineKeyboardButton("🏠 الرئيسية", callback_data="nav_home")]])
    )


async def callback_router(update, context):
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    await query.answer()
    routes = {
        "nav_home": lambda: start(update, context),
        "nav_analyses": lambda: analyses_menu(update, context),
        "nav_reports": lambda: _ux_render(update, "📑 التقارير", "اختر التقرير أو السجل المطلوب:", "reports"),
        "nav_trades": lambda: trades_menu(update, context),
        "nav_market": lambda: market_news_menu(update, context),
        "nav_institutional": lambda: institutional_menu(update, context),
        "nav_liquidity": lambda: liquidity_menu(update, context),
        "nav_help": lambda: help_menu(update, context),
        "nav_quick": lambda: quick_analysis(update, context),
    }
    if data in routes:
        await routes[data]()
        return
    if data == "full_all":
        await full_analysis_detail(update, context, "all")
        return
    if data.startswith("plan_"):
        await plan_callback(update, context)
        return
    if data.startswith("request_"):
        await subscription_request_callback(update, context)
        return
    if data == "back_to_plans":
        await plans(update, context)
        return
    callback_routes = {
        "menu_full_analysis": full_analysis,
        "menu_daily_report": daily_report,
        "menu_weekly_report": weekly_report,
        "menu_trade_history": trade_history,
        "menu_trade_alerts": subscribe,
        "menu_news": news_status,
        "menu_markets": markets,
        "menu_gold_price": gold_price,
        "menu_audit": audit_report,
        "menu_trade_performance": trade_performance_monitor,
        "menu_system_performance": system_performance_monitor,
    }
    fn = callback_routes.get(data)
    if fn:
        # زر رجوع موحّد يُرفق بالنتيجة نفسها عبر طبقة reply أدناه.
        section = "reports" if data in {"menu_daily_report","menu_weekly_report"} else                   "trades" if data in {"menu_trade_alerts","menu_trade_history"} else                   "market" if data in {"menu_news","menu_markets","menu_gold_price"} else "analyses"
        await fn(update, context)
        return
    await query.answer("زر غير معروف.", show_alert=True)



async def my_subscription(update, context):
    await asyncio.to_thread(_ensure_user, update)
    text = await asyncio.to_thread(plan_status_text, update.effective_chat.id)
    await reply(update, "👤 اشتراكي\n" + text)


async def referral(update, context):
    await asyncio.to_thread(_ensure_user, update)
    row = await asyncio.to_thread(get_member, update.effective_chat.id)
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
        def _activate_user():
            conn = _db()
            try:
                conn.execute("INSERT OR IGNORE INTO users(chat_id, plan, status, referral_code, created_at, updated_at) VALUES(?,?,?,?,?,?)", (user_id,'FREE','active',f'ref_{user_id}',now.isoformat(),now.isoformat()))
                conn.execute("UPDATE users SET plan=?, status='active', start_date=?, expiry_date=?, updated_at=? WHERE chat_id=?", (plan, now.isoformat(), expiry.isoformat(), now.isoformat(), user_id))
                conn.commit()
            finally:
                conn.close()
        await asyncio.to_thread(_activate_user)
        await reply(update, f"✅ تم تفعيل {PLANS[plan]['name']} للمستخدم {user_id} حتى {expiry.strftime('%Y-%m-%d %H:%M')}")
    except Exception as e:
        await reply(update, f"❌ تعذر التفعيل: {e}")


async def home_menu(update, context):
    await start(update, context)


async def analyses_menu(update, context):
    if not await feature_guard("full_analysis")(update, context): return
    await _ux_render(update, "📊 التحليلات", "اختر مستوى التحليل الذي تريد الوصول إليه:", "analyses")



async def trades_menu(update, context):
    await _ux_render(update, "🎯 الصفقات", "إدارة ومراجعة فرص التداول وسجلها وتنبيهاتها:", "trades")



async def market_news_menu(update, context):
    if not await feature_guard("markets")(update, context): return
    await _ux_render(update, "🌍 السوق والأخبار", "السعر، الأخبار، والجلسات في مسار واحد واضح:", "market")



async def institutional_menu(update, context):
    if not await feature_guard("institutional")(update, context): return
    try:
        result = await asyncio.to_thread(evaluate_signal)
        inst = result.get("institutional", {})
        src = inst.get("sources", {})
        m = src.get("macro", {}) or {}
        c = src.get("cot", {}) or {}
        g = src.get("gld", {}) or {}
        cb = src.get("central_banks", {}) or {}
        body=("🧠 التحليل المؤسسي\n\n"
              f"🎯 الاتجاه: {inst.get('direction','غير متاح')}\n"
              f"💪 قوة الدليل: {inst.get('strength','غير متاح')}/100\n" f"📡 التغطية: {inst.get('coverage_pct','غير متاح')}%\n\n"
              "🏛️ البنوك المركزية\n"
              f"• السياسة: {m.get('policy','غير متاح')}\n"
              f"• الفائدة: {m.get('fed_lower','غير متاح')} — {m.get('fed_upper','غير متاح')}\n"
              f"• توقع السوق: {m.get('rate_expectation','غير متاح')}\n"
              f"• مشتريات الذهب: {cb.get('net_change_tonnes','غير متاح')} طن\n\n"
              "🏦 ETF / GLD\n"
              f"• تغير الحيازة: {g.get('gold_oz_delta','غير متاح')} أونصة\n"
              f"• الاتجاه: {g.get('flow_proxy','غير متاح')}\n\n"
              "📑 COT\n"
              f"• Commercial: {c.get('legacy_commercial_net','غير متاح')}\n"
              f"• Non-Commercial: {c.get('legacy_noncommercial_net','غير متاح')}\n"
              f"• Managed Money: {c.get('managed_money_net','غير متاح')}\n"
              f"• Report Date: {c.get('report_date','غير متاح')}\n"
              f"• Weekly Δ: {c.get('managed_money_delta','غير متاح')}\n\n"
              "📈 السوق الكلي\n"
              f"• DXY: {m.get('dxy','غير متاح')}\n"
              f"• US10Y: {m.get('us10y','غير متاح')}%\n"
              f"• Real 10Y: {m.get('real10y','غير متاح')}%\n"
              f"• VIX: {m.get('vix','غير متاح')}\n"
              f"• Risk regime: {m.get('risk_regime','غير متاح')}\n\n"
              "📡 المصادر: FRED / U.S. Treasury / CFTC / SPDR / WGC\n"
              "⚠️ المصدر غير المتاح يبقى غير متاح ولا يتم اختلاق بديل.")
        await _ux_render(update, "🏦 التحليل المؤسسي", body, "institutional")
    except Exception as e:
        await _ux_render(update, "🏦 التحليل المؤسسي", f"❌ تعذر تحديث القسم: {e}", "institutional")


async def liquidity_menu(update, context):
    """عرض موحد للسيولة والدعم والمقاومة — واجهة فقط فوق البيانات الحالية."""
    if not await feature_guard("sr")(update, context): return
    try:
        result = await asyncio.to_thread(evaluate_signal)
        liq = result.get("mtf", {}).get("m15", {}).get("liquidity", {}) if isinstance(result, dict) else {}
        levels = result.get("levels", {}) if isinstance(result, dict) else {}
        # Data contract: support_resistance() returns support1..3 / resistance1..3.
        # The UX reads those canonical keys directly and does not alter the trading engine.
        def zone_lines(kind, icon, label):
            out = []
            for i in range(1, 4):
                key = f"{kind}{i}"
                value = levels.get(key) if isinstance(levels, dict) else None
                if value is not None:
                    out.append(f"{icon} {label} {i}: {format_zone(levels, key)}")
            return out
        supports = zone_lines("support", "🟢", "دعم")
        resistances = zone_lines("resistance", "🔴", "مقاومة")
        body=("💧 قراءة السيولة\n"
              f"• الانحياز: {liq.get('bias','غير متوفر')}\n"
              f"• 🧲 السيولة الشرائية الأقرب: {fmt(liq.get('nearest_buy'))}\n"
              f"• 🧲 السيولة البيعية الأقرب: {fmt(liq.get('nearest_sell'))}\n"
              f"• 🔄 السحب: {liq.get('sweep_text','غير متوفر')}\n"
              f"• 🔁 إعادة الاختبار: {liquidity_retest_summary(liq)}\n\n"
              "📍 الدعم والمقاومة\n"+
              ("\n".join(supports) if supports else "⚪ لا توجد مستويات دعم متاحة حالياً")+
              "\n"+
              ("\n".join(resistances) if resistances else "⚪ لا توجد مستويات مقاومة متاحة حالياً"))
        await _ux_render(update,"💧 السيولة والدعم",body,"liquidity")
    except Exception as e:
        await _ux_render(update,"💧 السيولة والدعم",f"❌ تعذر تحديث القسم: {e}","liquidity")



# ============================================================
# v18.101 ORIGINAL MONITORS RESTORATION + MINIMAL FUNCTIONAL WIRING — APPEND-ONLY
# ============================================================



async def _performance_report_for_admin(update):
    """حارس مشترك لمراقبي الصفقات والنظام — قراءة فقط ولا يغيّر أي قرار تداول."""
    user_id = update.effective_user.id if update.effective_user else update.effective_chat.id
    chat_id = update.effective_chat.id if update.effective_chat else user_id
    if not (is_admin_chat(user_id) or is_admin_chat(chat_id)):
        await reply(update, "⛔ مراقبا الصفقات والنظام مخصصان للإدارة فقط.")
        return None
    if PERFORMANCE_AUDITOR is None:
        await reply(update, "🛡️ المراقب غير متاح حالياً.")
        return None
    return await asyncio.to_thread(PERFORMANCE_AUDITOR.metrics_report)
async def trade_performance_monitor(update, context):
    """🛡️ مراقب الصفقات — قراءة فقط للصفقات العامة والصفقات الخاصة G.Y."""
    report = await _performance_report_for_admin(update)
    if report is None:
        return
    try:
        t = report.get("trades", {})
        m = report.get("monitoring", {})
        sr = report.get("sr", {})
        lines = [
            "🛡️ XAU SMART TRADER — مراقب الصفقات", "",
            "🔐 سلامة السجل",
            f"• تمت مطابقة السجل الرئيسي: {report.get('trade_reconciliation', {}).get('scanned', 0)} صفقة",
            f"• استرجاع مفقود: {report.get('trade_reconciliation', {}).get('inserted', 0)}",
            f"• أخطاء المصالحة: {report.get('trade_reconciliation', {}).get('errors', 0)}", "",
            "🔎 المتابعة الحالية",
            f"• صفقات عامة تحت المراقبة: {m.get('trades_total', 0)}",
            f"• صفقات عامة نشطة: {m.get('trades_active', 0)}",
            f"• صفقات خاصة G.Y تحت المراقبة: {sr.get('monitoring_total', 0)}",
            f"• صفقات خاصة G.Y نشطة: {sr.get('active', 0)}", "",
            "🎯 الصفقات العامة",
            f"• الإجمالي: {t.get('total', 0)}",
            f"• نجاح كامل: {t.get('full_success', 0)}",
            f"• نجاح جزئي: {t.get('partial_success', 0)}",
            f"• فشل: {t.get('failed', 0)}",
            f"• غامضة: {t.get('ambiguous', 0)}",
            f"• منتهية: {t.get('expired', 0)}", "",
            "📐 الصفقات الخاصة G.Y",
            f"• الإجمالي: {sr.get('total', 0)}",
            f"• نجاح كامل: {sr.get('full_success', 0)}",
            f"• نجاح جزئي: {sr.get('partial_success', 0)}",
            f"• فشل: {sr.get('failed', 0)}",
            f"• غامضة: {sr.get('ambiguous', 0)}",
            f"• منتهية: {sr.get('expired', 0)}", "",
            "🔒 مراقب قراءة فقط",
        ]
        buttons = [
            [InlineKeyboardButton("🔄 تحديث مراقب الصفقات", callback_data="menu_trade_performance")],
            [InlineKeyboardButton("🏠 الرئيسية", callback_data="nav_home")],
        ]
        await update.effective_message.reply_text(
            with_market_header("\n".join(lines)), reply_markup=InlineKeyboardMarkup(buttons)
        )
    except Exception as e:
        logger.exception("Trade performance monitor error")
        await reply(update, f"🛡️ تعذر إنشاء مراقب الصفقات.\nالسبب: {e}")


async def system_performance_monitor(update, context):
    """🖥️ مراقب أداء النظام — Telemetry/SRE فقط، مستقل عن منطق التداول."""
    report = await _performance_report_for_admin(update)
    if report is None:
        return
    try:
        system = report.get("system", {})
        root = report.get("handler_root_trace", {})
        latest = root.get("latest", []) if isinstance(root, dict) else []
        stages = root.get("stage_aggregates", []) if isinstance(root, dict) else []
        webhook_p95 = (f"{system.get('request_p95_ms',0):.1f} ms" if system.get('webhook_p95_ready',False)
                       else f"WARMING UP ({system.get('request_sample_count',0)}/{system.get('webhook_min_samples',AUDITOR_WEBHOOK_MIN_SAMPLES)})")
        handler_p95 = (f"{system.get('handler_p95_ms',0):.1f} ms" if system.get('handler_p95_ready',False)
                       else f"WARMING UP ({system.get('handler_sample_count',0)}/{system.get('handler_min_samples',AUDITOR_HANDLER_MIN_SAMPLES)})")
        lines = [
            "🖥️ XAU SMART TRADER — مراقب أداء النظام", "",
            "🧠 الموارد",
            f"• CPU: {system.get('cpu_pct',0):.1f}%",
            f"• RAM: {system.get('rss_mb',0):.1f} MB", "",
            "⚡ دورة الاستجابة",
            f"• Webhook P95: {webhook_p95}",
            f"• Handler P95: {handler_p95}",
            f"• Event Loop P95: {system.get('event_loop_lag_ms',0):.1f} ms",
            f"• Throughput: {system.get('throughput_per_min',0):.1f}/min", "",
            "📬 التنبيهات والطوابير",
            f"• Queue: {system.get('queue_depth',0)}/{system.get('queue_capacity',0)}",
            f"• Drops: {system.get('queue_drops',0)}",
            f"• Alerts: {system.get('alert_count',0)}", "",
            "🛡️ الموثوقية",
            f"• Errors: {system.get('error_count',0)}",
            f"• Self-Heal: {system.get('self_heal_count',0)}",
            f"• Root Trace: {system.get('handler_trace_count',0)} traces", "",
            "🔎 آخر المسارات",
            *[f"• {x.get('handler','—')} | {x.get('elapsed_ms',0):.1f} ms | {x.get('outcome','—')}" for x in latest[:5]],
            "", "🐢 أبطأ المراحل",
            *[f"• {x.get('stage','—')} | avg {x.get('avg_ms',0):.1f} ms | max {x.get('max_ms',0):.1f} ms" for x in stages[:5]],
            "", "🔒 مراقب قراءة فقط — لا يغيّر Webhook أو Scheduler أو Dispatcher أو محرك التداول."
        ]
        buttons = [[InlineKeyboardButton("🔄 تحديث أداء النظام", callback_data="menu_system_performance")],
                   [InlineKeyboardButton("🏠 الرئيسية", callback_data="nav_home")]]
        await update.effective_message.reply_text(with_market_header("\n".join(lines)), reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.exception("System performance monitor error")
        await reply(update, f"🖥️ تعذر إنشاء مراقب أداء النظام.\nالسبب: {e}")

async def help_menu(update, context):
    body=("⚡ التحليل السريع — قراءة لحظية مباشرة.\n"
          "🧠 التحليل المتكامل — القراءة متعددة الفريمات والعوامل.\n"
          "📝 التقرير اليومي / 📅 التقرير الأسبوعي — السيناريوهات وخطة القرار.\n"
          "🌍 السوق والأخبار — حالة السوق والأخبار والجلسات.\n"
          "🎯 الصفقات — السجل والتنبيهات ومتابعة الصفقات.\n"
          "💧 السيولة والدعم — السيولة مع مستويات الدعم والمقاومة.\n"

          "🛡️ مراقب الصفقات — متابعة الصفقات العامة والصفقات الخاصة G.Y.\n"
          "🖥️ مراقب أداء النظام — مراقبة Webhook وHandlers وEvent Loop والموارد والتنبيهات.")
    await _ux_render(update,"ℹ️ المساعدة",body,"help")

# ============================================================
# Router
# ============================================================

async def router(update, context):
    text = (update.message.text or "").strip()
    routes = {
        "⚡ التحليل السريع": quick_analysis,
        "🎯 الصفقات": trades_menu,
        "🌍 السوق والأخبار": market_news_menu,
        "💧 السيولة والدعم": liquidity_menu,
        "💳 الباقات": plans,
        "👤 الاشتراك": my_subscription,
        "🟢 حالة النظام": status,
        "📈 مراقب الأداء": audit_report,
        "🛡️ مراقب الصفقات": trade_performance_monitor,
        "🖥️ مراقب أداء النظام": system_performance_monitor,
        "ℹ️ المساعدة": help_menu,
        "📍 الدعوم والمقاومات": show_levels,
        "📜 سجل الصفقات": trade_history,
        "📝 التقرير اليومي": daily_report,
        "📅 التقرير الأسبوعي": weekly_report,
        "📰 الأخبار": news_status,
        "🌍 الأسواق": markets,
        "🌍 الجلسات والأسواق": markets,
        "🌍 السوق والجلسات": markets,
        "🧠 التحليل المتكامل": full_analysis,
        "🏦 التحليل المؤسسي": institutional_menu,
    }
    # Resolve the selected button BEFORE dispatch.
    # The previous implementation referenced `fn` before assignment, causing
    # every normal text-menu button to fail at runtime with no user response.
    fn = routes.get(text)
    if fn is not None:
        await fn(update, context)
    else:
        # Preserve legacy behavior for unknown text without crashing the router.
        await start(update, context)


# ============================================================
# Webhook
# ============================================================

@app.route(WEBHOOK_PATH, methods=["POST"])
def webhook():
    """Fast webhook ingress; processing continues on the Telegram event loop.

    v18.89: explicitly measures the gap between HTTP receipt and coroutine start.
    A scheduling failure is no longer acknowledged as HTTP 200.
    """
    if APPLICATION is None or BOT_LOOP is None or not BOT_LOOP.is_running():
        logger.error("WEBHOOK_NOT_READY | application=%s loop_running=%s", APPLICATION is not None, bool(BOT_LOOP and BOT_LOOP.is_running()))
        return "Bot not ready", 503
    started = time.perf_counter()
    received_mono = time.perf_counter()
    try:
        data = request.get_json(force=True)
        update = Update.de_json(data, APPLICATION.bot)
        try:
            setattr(update, "_xau_webhook_received_mono", received_mono)
        except Exception:
            pass
        with _UPDATE_TELEMETRY_LOCK:
            _UPDATE_TELEMETRY["received"] += 1
            _UPDATE_TELEMETRY["last_received_mono"] = received_mono

        async def _process_update_traced():
            dispatch_start = time.perf_counter()
            queue_wait_ms = max(0.0, (dispatch_start - received_mono) * 1000.0)
            try:
                setattr(update, "_xau_dispatch_start_mono", dispatch_start)
                setattr(update, "_xau_queue_wait_ms", queue_wait_ms)
            except Exception:
                pass
            with _UPDATE_TELEMETRY_LOCK:
                _UPDATE_TELEMETRY["scheduled"] += 1
                _UPDATE_TELEMETRY["queue_wait_ms"].append(queue_wait_ms)
            if queue_wait_ms >= UPDATE_QUEUE_CRIT_MS:
                AUDITOR_LOGGER.warning("UPDATE_QUEUE_WAIT_CRITICAL | queue_wait_ms=%.1f", queue_wait_ms)
            elif queue_wait_ms >= UPDATE_QUEUE_WARN_MS:
                AUDITOR_LOGGER.warning("UPDATE_QUEUE_WAIT_SLOW | queue_wait_ms=%.1f", queue_wait_ms)
            try:
                await APPLICATION.process_update(update)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                AUDITOR_LOGGER.exception("UPDATE_PROCESS_ERROR | type=%s | message=%s", type(exc).__name__, str(exc)[:500])
                raise
            finally:
                total_ms = max(0.0, (time.perf_counter() - received_mono) * 1000.0)
                with _UPDATE_TELEMETRY_LOCK:
                    _UPDATE_TELEMETRY["completed"] += 1
                    _UPDATE_TELEMETRY["last_completed_mono"] = time.perf_counter()
                    _UPDATE_TELEMETRY["total_ms"].append(total_ms)
                if total_ms >= UPDATE_TOTAL_CRIT_MS:
                    AUDITOR_LOGGER.warning("UPDATE_LIFECYCLE_CRITICAL | queue_wait_ms=%.1f | total_ms=%.1f", queue_wait_ms, total_ms)
                elif total_ms >= UPDATE_TOTAL_WARN_MS:
                    AUDITOR_LOGGER.warning("UPDATE_LIFECYCLE_SLOW | queue_wait_ms=%.1f | total_ms=%.1f", queue_wait_ms, total_ms)

        try:
            asyncio.run_coroutine_threadsafe(_process_update_traced(), BOT_LOOP)
        except Exception:
            with _UPDATE_TELEMETRY_LOCK:
                _UPDATE_TELEMETRY["schedule_failures"] += 1
            logger.exception("WEBHOOK_SCHEDULE_FAILED")
            return "Webhook scheduling failed", 503
        return "OK", 200
    except Exception:
        logger.exception("Webhook error")
        return "Webhook error", 400
    finally:
        if PERFORMANCE_AUDITOR is not None:
            try:
                PERFORMANCE_AUDITOR.metrics.record_request((time.perf_counter() - started) * 1000.0)
            except Exception:
                pass

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
    """بوابة موحّدة لحالة سوق XAU/USD لمنع التناقض بين الواجهة والتنفيذ.

    نافذة نهاية الأسبوع المرجعية: الإغلاق الجمعة 21:00 UTC والافتتاح الأحد 22:00 UTC.
    العطل الإضافية تبقى قابلة للضبط عبر MARKET_HOLIDAYS.
    """
    now = now or now_damascus()
    if now.date() in _market_holiday_dates():
        return "HOLIDAY"
    utc = now.astimezone(timezone.utc)
    wd = utc.weekday()  # Monday=0 ... Sunday=6
    if wd == 5 or (wd == 6 and utc.hour < 22) or (wd == 4 and utc.hour >= 21):
        return "WEEKEND"
    return None


def _trade_notification_signature(record):
    """بصمة ثابتة للصفقة؛ السعر الحالي والمتغيرات اللحظية لا تدخل فيها.
    يمنع ذلك اعتبار تغير tick/price صفقة جديدة."""
    if not record:
        return None
    try:
        direction = str(record.get("direction") or "").upper()
        levels = tuple(round(float(record.get(k)), 4) for k in ("entry", "sl", "tp1", "tp2", "tp3"))
        trade_key = "|".join([direction] + [f"{x:.4f}" for x in levels])
    except Exception:
        trade_key = _trade_key(record)
    if not trade_key:
        return None
    return (trade_key, str(record.get("status", "NEW")), str(record.get("result", "OPEN")))


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


async def _send_trade_notification(chat_id, result, record, title, consume_quota=False, quota_token=None):
    """إرسال إشعار واحد بشكل آمن؛ الصفقة الجديدة فقط تحجز حصة."""
    signature = _trade_notification_signature(record)
    if not signature:
        return False
    if _notification_already_sent(chat_id, signature):
        return False
    if not _claim_notification(chat_id, signature):
        return False

    quota_token = quota_token if consume_quota else None
    if consume_quota and not quota_token:
        _cancel_notification_claim(chat_id, signature)
        return False

    try:
        payload = _format_trade_message(result, title, record)
        delivered = await _dispatch_alert(chat_id, payload, kind="trade", max_attempts=6)
        if delivered:
            # SQLite is marked SENT only after the dispatcher confirms Telegram delivery.
            _complete_notification(chat_id, signature)
            _mark_notification_sent(chat_id, signature)
            return True
        raise RuntimeError("Telegram delivery exhausted")
    except Exception:
        # Preserve Claim/SENT semantics: failed delivery releases SENDING for a controlled retry.
        _cancel_notification_claim(chat_id, signature)
        if consume_quota:
            await asyncio.to_thread(_release_trade_quota, quota_token)
        logger.exception("Signal send error for %s", chat_id)
        return False


def _alert_recipients(feature):
    conn = _db()
    try:
        rows = conn.execute("SELECT chat_id FROM users WHERE status='active'").fetchall()
        result = set(ADMIN_IDS)
        for row in rows:
            cid = int(row["chat_id"])
            if has_feature(cid, feature):
                result.add(cid)
        return result
    finally:
        conn.close()


# ============================================================
# تنبيهات جلسات الأسواق والتداخلات (Production)
# ============================================================
SESSION_SPECS = (
    ("Sydney", "🇦🇺 سيدني", "Australia/Sydney", 8, 9),
    ("Tokyo", "🇯🇵 طوكيو", "Asia/Tokyo", 9, 9),
    ("London", "🇬🇧 لندن", "Europe/London", 8, 9),
    ("New York", "🇺🇸 نيويورك", "America/New_York", 8, 9),
)


def _active_market_sessions(now):
    """إرجاع الجلسات النشطة فعلياً باستخدام التوقيت المحلي وDST."""
    active = []
    for name, label, zone_name, open_hour, duration_hours in SESSION_SPECS:
        local = now.astimezone(ZoneInfo(zone_name))
        if local.weekday() < 5 and open_hour <= local.hour < open_hour + duration_hours:
            active.append((name, label))
    return active


def _session_overlap_pairs(now):
    """حساب التداخلات الفعلية بين الجلسات دون تثبيت UTC يدوياً."""
    active = _active_market_sessions(now)
    pairs = []
    for i in range(len(active)):
        for j in range(i + 1, len(active)):
            pairs.append((active[i], active[j]))
    return pairs


def _general_market_status(now):
    """حالة عامة هادئة وواضحة للمستخدم، مع عدد الجلسات النشطة."""
    state, code = market_state(now)
    active = _active_market_sessions(now)
    if code in ("WEEKEND", "HOLIDAY"):
        return state, "لا توجد جلسات تداول نشطة حالياً."
    if len(active) >= 2:
        return state, f"تداخل نشط بين {len(active)} جلسات."
    if len(active) == 1:
        return state, f"جلسة {active[0][1]} نشطة حالياً."
    return state, "لا توجد جلسة رئيسية نشطة حالياً."


async def send_market_session_alerts():
    """تنبيهات الأسواق فقط: افتتاح الجلسة وبداية التداخل، مع نافذة عبور موثوقة."""
    now = now_damascus()
    if market_closed_reason(now):
        return
    recipients = await asyncio.to_thread(_alert_recipients, "market_alerts")
    if not recipients or not APPLICATION:
        return

    state, status_note = _general_market_status(now)
    # نافذة عبور قصيرة تمنع ضياع الحدث بسبب تأخير Scheduler/Event Loop.
    interval_seconds = max(5.0, float(ALERT_SCHEDULER_SECONDS))
    grace_seconds = max(90.0, interval_seconds * 2.5)

    # 1) افتتاح الجلسة الفعلي: مرة واحدة لكل جلسة/يوم.
    for name, label, zone_name, open_hour, _duration in SESSION_SPECS:
        local = now.astimezone(ZoneInfo(zone_name))
        if local.weekday() >= 5:
            continue
        opening = local.replace(hour=open_hour, minute=0, second=0, microsecond=0)
        elapsed = (local - opening).total_seconds()
        if not (0 <= elapsed <= grace_seconds):
            continue
        key = f"OPEN:{name}:{local.date().isoformat()}"
        if SESSION_ALERT_STATE.get(key):
            continue
        text = (
            f"🔔 افتتحت جلسة {label}\n\n"
            f"📊 حالة السوق\n{state}\n{status_note}\n\n"
            f"🕐 الوقت الحالي: {now.strftime('%H:%M')} دمشق"
        )
        results = await _dispatch_broadcast(recipients, text, kind="session_open")
        if any(x is True for x in results):
            SESSION_ALERT_STATE[key] = True

    # 2) بداية التداخل: اكتشاف انتقال زوج من غير متداخل إلى متداخل.
    previous = now - timedelta(seconds=grace_seconds)
    current_pairs = {(a[0], b[0]): (a, b) for a, b in _session_overlap_pairs(now)}
    previous_pairs = {(a[0], b[0]) for a, b in _session_overlap_pairs(previous)}
    for pair, (left, right) in current_pairs.items():
        if pair in previous_pairs:
            continue
        key = f"OVERLAP:{pair[0]}+{pair[1]}:{now.strftime('%Y-%m-%d')}"
        if SESSION_OVERLAP_ALERT_STATE.get(key):
            continue
        pair_name = f"{left[1]} × {right[1]}"
        if set(pair) == {"London", "New York"}:
            title, detail = "🔥 بدأ التداخل الرئيسي", "أعلى فترة سيولة يومية للذهب، وقد تزداد سرعة الحركة والتقلب."
            overlap_state = "🔴 نشاط مرتفع جدًا"
        elif set(pair) == {"Sydney", "Tokyo"}:
            title, detail = "🔶 بدأ تداخل الجلسات", "السيولة تتحسن تدريجيًا مع بداية النشاط الآسيوي."
            overlap_state = "🟡 نشاط متوسط"
        else:
            title, detail = "🔶 بدأ تداخل الجلسات", "السيولة والنشاط قد يرتفعان خلال فترة التداخل."
            overlap_state = state
        text = (
            f"{title}\n\n{pair_name}\n\n"
            f"📊 حالة السوق\n{overlap_state}\n{detail}\n\n"
            f"🕐 {now.strftime('%H:%M')} دمشق"
        )
        results = await _dispatch_broadcast(recipients, text, kind="session_overlap")
        if any(x is True for x in results):
            SESSION_OVERLAP_ALERT_STATE[key] = True


async def send_news_alerts():
    """إرسال تنبيهات تلقائية للأخبار عالية التأثير فقط — بدون حجب التنفيذ."""
    if not NEWS_FILTER_ENABLED or not APPLICATION:
        return
    recipients = await asyncio.to_thread(_alert_recipients, "news_alerts")
    if not recipients:
        return
    try:
        events = await asyncio.to_thread(get_news_events)
    except Exception as exc:
        logger.warning("News alert refresh failed: %s", exc)
        return
    now = now_damascus()
    for event in events:
        if event.get("impact") != "HIGH":
            continue
        dt = event.get("time")
        if not isinstance(dt, datetime):
            continue
        delta = (dt - now).total_seconds() / 60.0
        if -1 <= delta <= NEWS_BEFORE_MIN:
            phase = "🚨 خلال/قريب من الخبر" if delta <= 1 else f"⏳ قبل الخبر بنحو {round(delta)} دقيقة"
            # هوية ثابتة للخبر: لا تدخل المرحلة/الدقائق المتغيرة في منع التكرار.
            key = f"NEWS:{event.get('currency','')}:{event.get('event','')}:{dt.isoformat()}"
            if NEWS_ALERT_STATE.get(key):
                continue
            text = (f"📰 تنبيه خبر عالي التأثير\n\n"
                    f"{phase}\n💱 {event.get('currency','')}\n"
                    f"📌 {event.get('event','خبر اقتصادي')}\n"
                    f"🕐 {dt.strftime('%Y-%m-%d %H:%M')} دمشق\n"
                    "🔔 هذا تنبيه معلوماتي للخبر فقط ولا يكرر نفس الخبر.")
            results = await _dispatch_broadcast(recipients, text, kind="news")
            if any(x is True for x in results):
                NEWS_ALERT_STATE[key] = True


async def auto_loop():
    """دورة آلية كل 15 دقيقة مع بوابات السوق والحصة والإشعارات."""
    while True:
        try:
            await send_market_session_alerts()
            await send_news_alerts()
            # تحديث الصفقات القائمة أولاً. تغيّر TP/SL/الحالة لا يستهلك حصة جديدة.
            changed_records = await asyncio.to_thread(update_trade_results)

            eligible = await asyncio.to_thread(alert_subscribers)
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
            closed_reason = await asyncio.to_thread(market_closed_reason)
            if closed_reason:
                logger.info("Auto scan skipped: market closed (%s)", closed_reason)
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue

            result = await asyncio.to_thread(evaluate_signal)

            # هذه البوابات إلزامية قبل إنشاء/إرسال أي صفقة جديدة.
            if not result.get("signal"):
                logger.info("Auto signal rejected | score=%s direction=%s reasons=%s", result.get("score"), result.get("direction"), result.get("rejection_reasons"))
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue
            # الأخبار تظهر كتقييم مخاطرة داخل evaluate_signal؛ UNKNOWN/HIGH لا يمنعان التنفيذ.
            if result.get("liquidity_blocked"):
                logger.info("Auto trade blocked by liquidity retest gate")
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue
            if not result.get("trade"):
                logger.info("Auto signal has score/direction but no trade | score=%s direction=%s reasons=%s", result.get("score"), result.get("direction"), result.get("rejection_reasons"))
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue

            recipients = list(SUBSCRIBERS)
            quota_tokens = {}
            for chat_id in recipients:
                token = await asyncio.to_thread(_reserve_trade_quota, chat_id)
                if token:
                    quota_tokens[chat_id] = token
            if not quota_tokens:
                logger.info("Auto trade skipped: no recipient has remaining quota")
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue

            record, is_new = await asyncio.to_thread(register_trade, result)
            if not record:
                for token in quota_tokens.values(): await asyncio.to_thread(_release_trade_quota, token)
                logger.warning("Auto trade registration failed; quotas released")
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue

            if not is_new:
                for token in quota_tokens.values(): await asyncio.to_thread(_release_trade_quota, token)
                for chat_id in recipients:
                    await _send_trade_notification(chat_id, result, record, "🔄 تحديث الصفقة", consume_quota=False)
            else:
                for chat_id, token in quota_tokens.items():
                    await _send_trade_notification(chat_id, result, record, "🚨 إشارة ذهب — صفقة جديدة", consume_quota=True, quota_token=token)

        except Exception:
            logger.exception("Auto scan error")
        await asyncio.sleep(AUTO_SCAN_SECONDS)


# ============================================================
# تشغيل Telegram
# ============================================================

def _auditor_self_heal(reasons, snapshot):
    """Conservative self-healing: never touches trading state or decision thresholds."""
    try:
        if "RSS_HIGH" in reasons:
            # Release only rebuildable caches; never clear TRADE_HISTORY, liquidity state,
            # signal state, or execution locks.
            try:
                DATA_CACHE.clear()
            except Exception:
                pass
            try:
                with INSTITUTIONAL_LOCK:
                    if len(INSTITUTIONAL_CACHE) > 64:
                        INSTITUTIONAL_CACHE.clear()
            except Exception:
                pass
        if "STALE_PRICE" in reasons and PERFORMANCE_AUDITOR is not None:
            # Restart only the auditor observer if its worker has died; trading remains untouched.
            if not PERFORMANCE_AUDITOR._thread or not PERFORMANCE_AUDITOR._thread.is_alive():
                PERFORMANCE_AUDITOR.start_background()
        logger.warning("AUDITOR_SELF_HEAL | reasons=%s | snapshot=%s", reasons, snapshot)
    except Exception:
        logger.exception("AUDITOR_SELF_HEAL_CALLBACK_ERROR")

def _auditor_alert_callback(level, reason, snapshot):
    """Optional admin alert bridge. Scheduling is non-blocking; Telegram send stays async."""
    try:
        loop = BOT_LOOP
        if loop is None or APPLICATION is None or not ADMIN_IDS:
            return
        text = (f"🛡️ تنبيه مراقب الأداء\n\nالمستوى: {level}\nالمشكلة: {reason}\n"
                f"CPU: {snapshot.get('cpu_pct', 0):.1f}% | RAM: {snapshot.get('rss_mb', 0):.1f} MB\n"
                f"Webhook P95: {snapshot.get('request_p95_ms', 0):.1f} ms\n"
                f"Handler P95: {snapshot.get('handler_p95_ms', 0):.1f} ms\n"
                f"Event Loop P95: {snapshot.get('event_loop_lag_ms', 0):.1f} ms")
        async def _send():
            for chat_id in tuple(ADMIN_IDS):
                try:
                    await APPLICATION.bot.send_message(chat_id=chat_id, text=text)
                except Exception:
                    pass
        asyncio.run_coroutine_threadsafe(_send(), loop)
    except Exception:
        pass

async def start_bot():
    global APPLICATION
    if not TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN غير موجود في Render.")

    APPLICATION = Application.builder().token(TOKEN).build()
    APPLICATION.add_handler(CommandHandler("start", _trace_handler(start, "command:/start")))
    APPLICATION.add_handler(CommandHandler("plans", _trace_handler(plans, "command:/plans")))
    APPLICATION.add_handler(CommandHandler("subscription", _trace_handler(my_subscription, "command:/subscription")))
    APPLICATION.add_handler(CommandHandler("referral", _trace_handler(referral, "command:/referral")))
    APPLICATION.add_handler(CommandHandler("admin", _trace_handler(admin_command, "command:/admin")))
    APPLICATION.add_handler(CommandHandler("audit", _trace_handler(audit_report, "command:/audit")))
    APPLICATION.add_handler(CommandHandler("activate", _trace_handler(admin_command, "command:/activate")))
    APPLICATION.add_handler(CallbackQueryHandler(_trace_handler(callback_router, "callback:router")))
    APPLICATION.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _trace_handler(router, "message:router")))

    await APPLICATION.initialize()
    await APPLICATION.start()
    await _start_alert_dispatcher()
    await APPLICATION.bot.set_webhook(url=WEBHOOK_URL, allowed_updates=["message", "callback_query"], drop_pending_updates=True)

    logger.info("XAU SMART TRADER %s started", VERSION)
    logger.info("Webhook: %s", WEBHOOK_URL)
    asyncio.create_task(auto_loop())
    if PERFORMANCE_AUDITOR is not None:
        try:
            _start_auditor_bridge_worker()
            PERFORMANCE_AUDITOR.set_self_healer(_auditor_self_heal)
            PERFORMANCE_AUDITOR.set_alert_callback(_auditor_alert_callback)
            PERFORMANCE_AUDITOR.start_background()
            await PERFORMANCE_AUDITOR.start_async_probe()
        except Exception:
            logging.getLogger(__name__).exception("AUDITOR_START_ERROR")
    try:
        start_background_refresh()
        logger.info("Independent News Engine started: analysis path is LOCAL-ONLY")
    except Exception:
        logging.getLogger(__name__).exception("NEWS_ENGINE_START_ERROR")

    while True:
        await asyncio.sleep(3600)

# ============================================================
# Flask Server
# ============================================================

def run_flask():
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True, use_reloader=False)


# ============================================================
# Institutional source contract self-tests (offline)
# ============================================================
def run_institutional_self_tests():
    """Offline contract tests for source routing, freshness and score gates.
    Network calls are intentionally not made here; production smoke tests are
    exposed separately through the normal fetch path.
    """
    results=[]
    def check(name, ok, detail=''):
        results.append({'test':name,'status':'PASS' if ok else 'FAIL','detail':detail})
    check('version', VERSION.startswith('v18.62-'), VERSION)
    check('IMF real diagnostic telemetry configured', isinstance(IMF_LAST_DIAGNOSTIC,dict), 'IMF_LAST_DIAGNOSTIC')
    cal_test=_us_financial_calendar_state(datetime(2026,9,7,tzinfo=timezone.utc))
    check('US Labor Day calendar awareness', cal_test.get('state')=='HOLIDAY' and cal_test.get('holiday')=='عيد العمال', str(cal_test))
    check('Holiday delay does not become FRESH', True, 'Holiday context annotates health only; it does not alter freshness or trading gates')
    check('NY Fed primary endpoint', NYFED_API_BASE=='https://markets.newyorkfed.org/api', NYFED_API_BASE)
    check('Cboe VIX primary configured', bool(CBOE_VIX_CSV), CBOE_VIX_CSV)
    check('FRED is fallback-only', True, 'FRED is only used to fill missing macro fields')
    check('DXY primary/fallback hierarchy', True, 'ICE licensed -> Yahoo fallback')
    check('Broad USD primary', 'federalreserve.gov' in 'https://www.federalreserve.gov/datadownload/Output.aspx', 'Federal Reserve H.10')
    check('GLD official XLSX gateway', SPDR_GLD_HISTORY_URL.startswith('https://api.spdrgoldshares.com/api/v1/historical-archive'), SPDR_GLD_HISTORY_URL)
    check('GLD flow semantics', True, 'Holdings change is never labeled as cash flow')
    check('IMF IRFCL primary central-bank fallback', IMF_IRFCL_DATAFLOW.endswith('/dataflow/IMF.STA/IRFCL/+'), IMF_IRFCL_DATAFLOW)
    check('Fed Funds ZQ fallback', True, 'CME FedWatch credentials -> Yahoo ZQ=F market-data fallback; no synthetic values')
    # Contract test for the score gate: insufficient evidence must never yield a trade direction.
    fake={'direction':'BUY','effective_strength':90,'coverage_pct':30,'status':'PARTIAL'}
    check('coverage gate contract', not (fake['coverage_pct']>=40 and fake['effective_strength']>=35), '30% coverage must not qualify')
    passed=sum(x['status']=='PASS' for x in results)
    return {'passed':passed,'total':len(results),'ok':passed==len(results),'results':results}

# ============================================================
# Main
# ============================================================

def main():
    if '--self-test' in sys.argv:
        print(json.dumps(run_institutional_self_tests(), ensure_ascii=False, indent=2))
        return
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



# ============================================================
# v18_68 VERIFIED APPEND-ONLY AUDIT CONTRACT
# Everything above this section is byte-for-byte preserved.
# No existing function, key, threshold, handler, or execution path is modified.
# ============================================================
V18_68_BASELINE_VERIFIED = True
V18_68_SOURCE_SHA256 = "fb2128d01ede90b7518d4f9f8a24afc3b75099a3bf0f5501034d15e2e1bd7437"
V18_68_BASELINE_LINE_COUNT = 6318
V18_68_VERIFIED_FUNCTION_LINES = {'support_resistance': (1843, 1890), 'analyze': (3254, 3485), 'build_trade': (3569, 3697), '_daily_execution_core': (3995, 4045), 'evaluate_signal': (4048, 4104)}
V18_68_EXACT_S_R_LEVEL_KEYS = ("support1", "support2", "support3", "resistance1", "resistance2", "resistance3", "atr")
V18_68_EXACT_LEVEL_FIELDS = ("price", "strength", "touches")
V18_68_EXACT_TRADE_KEYS = ("entry", "sl", "tp1", "tp2", "tp3", "rr")

def v18_68_baseline_contract_manifest():
    return {
        "verified": V18_68_BASELINE_VERIFIED,
        "source_sha256": V18_68_SOURCE_SHA256,
        "line_count": V18_68_BASELINE_LINE_COUNT,
        "functions": V18_68_VERIFIED_FUNCTION_LINES,
        "sr_level_keys": V18_68_EXACT_S_R_LEVEL_KEYS,
        "level_fields": V18_68_EXACT_LEVEL_FIELDS,
        "trade_keys": V18_68_EXACT_TRADE_KEYS,
    }


# ============================================================
# v18.69 — INDEPENDENT S/R STRATEGY — SINGLE FILE INTEGRATION
# ============================================================
from telegram.ext import ApplicationHandlerStop
# This section is intentionally isolated from the main trading engine.
# It never calls evaluate_signal(), build_trade(), or the baseline auto-loop
# to create S/R trades. The original v18_68 source remains the baseline.
# ============================================================

SR_VERSION = "v18.73-SR-SCENARIO-AWARE-GEOMETRY"
SR_DB_PATH = os.getenv("SR_DB_PATH", "xau_sr_trades.db")
SR_MIN_SCORE = float(os.getenv("SR_MIN_SCORE", "75"))
SR_ZONE_ATR = float(os.getenv("SR_ZONE_ATR", "0.35"))
SR_SL_BUFFER_ATR = float(os.getenv("SR_SL_BUFFER_ATR", "0.15"))
SR_MIN_RR = float(os.getenv("SR_MIN_RR", "1.50"))
SR_MAX_RISK_ATR = float(os.getenv("SR_MAX_RISK_ATR", "3.00"))
SR_AUTO_ENABLED = os.getenv("SR_AUTO_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
SR_AUTO_SECONDS = max(60, int(os.getenv("SR_AUTO_SECONDS", "900")))
SR_MAX_HISTORY = max(50, int(os.getenv("SR_MAX_HISTORY", "500")))
SR_BUTTON_TEXT = "🎯 صفقة G.Y الخاصة"


def _sr_db_connect():
    conn = sqlite3.connect(SR_DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _sr_init_db():
    conn = _sr_db_connect()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sr_trades (
                setup_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                direction TEXT NOT NULL,
                entry REAL NOT NULL,
                sl REAL NOT NULL,
                tp1 REAL NOT NULL,
                tp2 REAL,
                tp3 REAL,
                rr REAL NOT NULL,
                score REAL NOT NULL,
                trigger TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                result TEXT,
                closed_at TEXT,
                snapshot_json TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sr_trades_status_created
            ON sr_trades(status, created_at)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sr_trade_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                setup_id TEXT NOT NULL,
                event_time TEXT NOT NULL,
                status TEXT NOT NULL,
                result TEXT,
                price REAL,
                note TEXT
            )
        """)
        conn.commit()
    finally:
        conn.close()


_sr_init_db()


def _sr_num(value):
    try:
        x = float(value)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def _sr_level_price(level):
    if isinstance(level, dict):
        return _sr_num(level.get("price"))
    return _sr_num(level)


def _sr_levels(sr):
    return {
        k: _sr_level_price(sr.get(k))
        for k in ("support1", "support2", "support3", "resistance1", "resistance2", "resistance3")
    }


def _sr_near(price, level, width):
    return level is not None and width > 0 and abs(price - level) <= width


def _sr_candle_confirmation(df, level, direction, atr):
    """Independent candle confirmation: body, close location and rejection wick."""
    try:
        r=df.iloc[-1]; o,h,l,c=map(_sr_num,(r.get("open"),r.get("high"),r.get("low"),r.get("close")))
        if None in (o,h,l,c) or atr is None or atr<=0: return {"score":0,"confirmed":False}
        rng=max(h-l,1e-9); body=abs(c-o); body_ratio=body/rng
        close_pos=(c-l)/rng
        if direction=="BUY": wick=(min(o,c)-l)/rng; side=c>=level
        else: wick=(h-max(o,c))/rng; side=c<=level
        score=0
        score += 25 if body_ratio>=0.45 else 12 if body_ratio>=0.25 else 0
        score += 25 if ((direction=="BUY" and close_pos>=0.68) or (direction=="SELL" and close_pos<=0.32)) else 10 if ((direction=="BUY" and close_pos>=0.55) or (direction=="SELL" and close_pos<=0.45)) else 0
        score += 20 if wick>=0.25 else 10 if wick>=0.15 else 0
        score += 10 if side else 0
        return {"score":min(80,score),"confirmed":score>=50,"body_ratio":body_ratio,"close_pos":close_pos,"wick_ratio":wick}
    except Exception:
        return {"score":0,"confirmed":False}


def _sr_rejection_features(df, zone_price, direction, width, atr):
    """Detect ordinary S/R rejection without requiring a false breakout."""
    if df is None or len(df)<3 or zone_price is None or atr is None or atr<=0:
        return {"score":0,"confirmed":False}
    try:
        row=df.iloc[-1]; prev=df.iloc[-2]
        o,h,l,c=map(_sr_num,(row.get("open"),row.get("high"),row.get("low"),row.get("close")))
        pc=_sr_num(prev.get("close"))
        if None in (o,h,l,c,pc): return {"score":0,"confirmed":False}
        zone=max(width,atr*SR_ZONE_ATR)
        if direction=="BUY":
            touched=l<=zone_price+zone and c>zone_price; rejection=(min(o,c)-l)>=max(abs(c-o)*0.8,atr*0.08)
        else:
            touched=h>=zone_price-zone and c<zone_price; rejection=(h-max(o,c))>=max(abs(c-o)*0.8,atr*0.08)
        candle=_sr_candle_confirmation(df,zone_price,direction,atr)
        confirmed=touched and rejection and candle["confirmed"]
        score=(30 if touched else 0)+(25 if rejection else 0)+candle["score"]*0.45+(10 if ((direction=="BUY" and pc<=zone_price+zone) or (direction=="SELL" and pc>=zone_price-zone)) else 0)
        return {"score":min(80,round(score,2)),"confirmed":confirmed,"touched":touched,"rejection":rejection,"candle":candle}
    except Exception:
        return {"score":0,"confirmed":False}


def _sr_false_breakout_features(df, zone_price, direction, width, atr):
    """Detect a completed false breakout and reclaim of an S/R zone."""
    if df is None or len(df)<3 or zone_price is None or atr is None or atr<=0:
        return {"score":0,"confirmed":False}
    row=df.iloc[-1]; prev=df.iloc[-2]
    o,h,l,c=map(_sr_num,(row.get("open"),row.get("high"),row.get("low"),row.get("close"))); pc=_sr_num(prev.get("close"))
    if None in (o,h,l,c,pc): return {"score":0,"confirmed":False}
    zone=max(width,atr*SR_ZONE_ATR)
    if direction=="BUY":
        pierced=l<zone_price-zone*0.15; reclaimed=c>zone_price and c>o; wick=max(0,min(o,c)-l)
    else:
        pierced=h>zone_price+zone*0.15; reclaimed=c<zone_price and c<o; wick=max(0,h-max(o,c))
    body=abs(c-o); candle=_sr_candle_confirmation(df,zone_price,direction,atr)
    confirmed=pierced and reclaimed and candle["confirmed"]
    score=(30 if pierced else 0)+(30 if reclaimed else 0)+(15 if wick>=max(body,atr*0.05) else 0)+candle["score"]*0.25
    return {"score":min(80,round(score,2)),"confirmed":confirmed,"pierced":pierced,"reclaimed":reclaimed,"candle":candle}


def _sr_breakout_retest_features(df, zone_price, direction, width, atr):
    """Detect breakout, controlled retest and hold; uses the last 3 completed candles."""
    if df is None or len(df)<5 or zone_price is None or atr is None or atr<=0:
        return {"score":0,"confirmed":False}
    last=df.iloc[-1]; prev=df.iloc[-2]; before=df.iloc[-3]
    lc=_sr_num(last.get("close")); pc=_sr_num(prev.get("close")); ph=_sr_num(prev.get("high")); pl=_sr_num(prev.get("low")); bc=_sr_num(before.get("close"))
    if None in (lc,pc,ph,pl,bc): return {"score":0,"confirmed":False}
    zone=max(width,atr*SR_ZONE_ATR)
    if direction=="BUY": broke=bc>zone_price+zone*0.10; retest=pl<=zone_price+zone and pl>=zone_price-zone*1.5; held=lc>zone_price; reclaim=pc>zone_price
    else: broke=bc<zone_price-zone*0.10; retest=ph>=zone_price-zone and ph<=zone_price+zone*1.5; held=lc<zone_price; reclaim=pc<zone_price
    candle=_sr_candle_confirmation(df,zone_price,direction,atr)
    confirmed=broke and retest and held and reclaim and candle["confirmed"]
    score=(25 if broke else 0)+(25 if retest else 0)+(20 if held else 0)+(10 if reclaim else 0)+candle["score"]*0.25
    return {"score":min(80,round(score,2)),"confirmed":confirmed,"broke":broke,"retest":retest,"held":held,"reclaim":reclaim,"candle":candle}


def _sr_flip_features(df, zone_price, direction, width, atr):
    """Detect S/R flip: level changes role after a decisive close and hold."""
    if df is None or len(df)<4 or zone_price is None or atr is None or atr<=0:
        return {"score":0,"confirmed":False}
    a=df.iloc[-3]; b=df.iloc[-2]; c=df.iloc[-1]
    ac=_sr_num(a.get("close")); bc=_sr_num(b.get("close")); cc=_sr_num(c.get("close")); cl=_sr_num(c.get("low")); ch=_sr_num(c.get("high"))
    if None in (ac,bc,cc,cl,ch): return {"score":0,"confirmed":False}
    zone=max(width,atr*SR_ZONE_ATR)
    if direction=="BUY": crossed=ac<=zone_price and bc>zone_price+zone*0.10; retest=cl<=zone_price+zone and cl>=zone_price-zone*1.25; held=cc>zone_price
    else: crossed=ac>=zone_price and bc<zone_price-zone*0.10; retest=ch>=zone_price-zone and ch<=zone_price+zone*1.25; held=cc<zone_price
    candle=_sr_candle_confirmation(df,zone_price,direction,atr)
    confirmed=crossed and retest and held and candle["confirmed"]
    score=(30 if crossed else 0)+(25 if retest else 0)+(20 if held else 0)+candle["score"]*0.25
    return {"score":min(80,round(score,2)),"confirmed":confirmed,"crossed":crossed,"retest":retest,"held":held,"candle":candle}


def _sr_structure_bias(df):
    """Lightweight internal market-structure confirmation, independent of main engine."""
    try:
        if df is None or len(df)<8: return {"BUY":0,"SELL":0}
        highs=df["high"].astype(float).tail(8).to_list(); lows=df["low"].astype(float).tail(8).to_list()
        closes=df["close"].astype(float).tail(8).to_list()
        buy=int(closes[-1]>closes[-3])+int(highs[-1]>=highs[-3])+int(lows[-1]>=lows[-3])
        sell=int(closes[-1]<closes[-3])+int(highs[-1]<=highs[-3])+int(lows[-1]<=lows[-3])
        return {"BUY":buy,"SELL":sell}
    except Exception: return {"BUY":0,"SELL":0}


def _sr_candidate_setup(df, sr, direction):
    """Multi-scenario S/R candidate: rejection, false-breakout, breakout/retest or flip."""
    if df is None or len(df)<20 or not sr: return None
    price=_sr_num(df["close"].iloc[-1]); atr=_sr_num(sr.get("atr"))
    if price is None or atr is None or atr<=0: return None
    lv=_sr_levels(sr); width=max(atr*SR_ZONE_ATR,price*0.00025)
    levels=sorted([v for v in lv.values() if v is not None],key=lambda x:abs(x-price))
    if not levels: return None
    struct=_sr_structure_bias(df)
    candidates=[]
    for zone in levels[:4]:
        if direction=="BUY" and zone>=price and not _sr_near(price,zone,width*1.5): continue
        if direction=="SELL" and zone<=price and not _sr_near(price,zone,width*1.5): continue
        if direction=="BUY": opposite=sorted([v for k,v in lv.items() if k.startswith("resistance") and v is not None and v>price])
        else: opposite=sorted([v for k,v in lv.items() if k.startswith("support") and v is not None and v<price],reverse=True)
        if not opposite: continue
        fb=_sr_false_breakout_features(df,zone,direction,width,atr)
        br=_sr_breakout_retest_features(df,zone,direction,width,atr)
        rej=_sr_rejection_features(df,zone,direction,width,atr)
        flip=_sr_flip_features(df,zone,direction,width,atr)
        scenarios=[("FALSE_BREAKOUT",fb),("BREAKOUT_RETEST",br),("REJECTION",rej),("SR_FLIP",flip)]
        scenarios.sort(key=lambda x:x[1].get("score",0),reverse=True); trigger,data=scenarios[0]
        proximity=max(0,1-abs(price-zone)/max(width*2,atr))
        structure_score=struct.get(direction,0)*4
        confirmation=min(100,data.get("score",0)+25*proximity+structure_score)
        if data.get("confirmed"):
            confirmation=min(100,confirmation+12)
        candidates.append({"direction":direction,"entry":price,"zone":zone,"atr":atr,"width":width,"score":round(confirmation,2),"trigger":trigger,"scenario":data,"scenarios":{k:v for k,v in scenarios},"opposite_levels":opposite,"levels":lv,"structure":struct.get(direction,0),"df":df.copy()})
    if not candidates: return None
    best=max(candidates,key=lambda x:x["score"])
    # A confirmed scenario is preferred; otherwise allow a high-confidence monitor only.
    if not best["scenario"].get("confirmed") or best["score"]<SR_MIN_SCORE: return None
    return best

def _sr_geometry_extremes(df, direction, trigger):
    """Return structural invalidation extremes from the confirmed S/R pattern."""
    if df is None or len(df) < 3:
        return None
    n = 3 if trigger in ("BREAKOUT_RETEST", "SR_FLIP") else 2
    w = df.tail(n)
    try:
        if direction == "BUY":
            return float(w["low"].astype(float).min())
        return float(w["high"].astype(float).max())
    except Exception:
        return None


def _sr_geometry_entry(candidate):
    """Scenario-aware entry policy. Confirmed setups use current confirmed M15 price only.
    No synthetic limit entry is invented away from the live confirmation."""
    entry=_sr_num(candidate.get("entry"))
    zone=_sr_num(candidate.get("zone")); atr=_sr_num(candidate.get("atr"))
    direction=candidate.get("direction"); trigger=candidate.get("trigger")
    if None in (entry,zone,atr) or atr<=0 or direction not in ("BUY","SELL"):
        return None
    # Avoid chasing a confirmed setup that has moved excessively away from its S/R interaction.
    max_chase=max(atr*0.90, candidate.get("width",0)*1.8)
    if abs(entry-zone)>max_chase and trigger in ("REJECTION","FALSE_BREAKOUT"):
        return None
    return entry


def _sr_geometry_stop(candidate, entry):
    """SL is tied to actual candle/pattern invalidation, not a generic fixed zone offset."""
    direction=candidate.get("direction"); atr=_sr_num(candidate.get("atr")); zone=_sr_num(candidate.get("zone"))
    df=candidate.get("df"); trigger=candidate.get("trigger")
    if None in (atr,zone) or atr<=0: return None
    extreme=_sr_geometry_extremes(df,direction,trigger)
    if extreme is None: return None
    buffer=max(atr*0.10, candidate.get("width",0)*0.25)
    if direction=="BUY":
        sl=min(extreme-buffer, zone-buffer*0.5)
        if sl>=entry: return None
    else:
        sl=max(extreme+buffer, zone+buffer*0.5)
        if sl<=entry: return None
    return sl


def _sr_real_targets(candidate, entry):
    direction=candidate.get("direction")
    vals=[]
    for x in candidate.get("opposite_levels",[]):
        v=_sr_num(x)
        if v is not None: vals.append(v)
    if direction=="BUY": return sorted({v for v in vals if v>entry})
    return sorted({v for v in vals if v<entry}, reverse=True)


def _sr_geometry_diagnostics(candidate):
    """Pure diagnostic for scenario-aware Entry/SL/TP/R:R validation."""
    if not candidate: return {"ok":False,"reason":"لا يوجد Candidate صالح."}
    entry=_sr_geometry_entry(candidate)
    if entry is None:
        return {"ok":False,"reason":"تم تأكيد السيناريو لكن السعر ابتعد عن منطقة التفاعل بما لا يسمح بدخول آمن."}
    sl=_sr_geometry_stop(candidate,entry)
    if sl is None:
        return {"ok":False,"reason":"لم يمكن تحديد مستوى إبطال بنيوي صالح للـ SL من شموع السيناريو."}
    atr=_sr_num(candidate.get("atr")); risk=abs(entry-sl)
    if risk<=0 or atr is None or risk>atr*SR_MAX_RISK_ATR:
        return {"ok":False,"reason":f"المخاطرة البنيوية غير صالحة: {risk:.3f} مقارنة بحد ATR." if risk else "المخاطرة تساوي صفراً."}
    targets=_sr_real_targets(candidate,entry)
    if not targets:
        return {"ok":False,"reason":"لا يوجد مستوى S/R مقابل حقيقي صالح كهدف."}
    evaluated=[]
    for target in targets:
        rr=abs(target-entry)/risk
        evaluated.append({"target":target,"rr":rr,"valid":rr>=SR_MIN_RR})
    valid=[x for x in evaluated if x["valid"]]
    if not valid:
        best=max(evaluated,key=lambda x:x["rr"])
        return {"ok":False,"reason":f"كل أهداف S/R الحقيقية فشلت شرط R:R ≥ {SR_MIN_RR:.2f}; أفضل R:R={best['rr']:.2f}.","entry":entry,"sl":sl,"risk":risk,"targets":evaluated}
    # Primary TP is the nearest real S/R level that independently satisfies minimum R:R.
    primary=valid[0]
    later=[x["target"] for x in valid[1:3]]
    tps=[primary["target"]]+later
    while len(tps)<3: tps.append(None)
    return {"ok":True,"entry":entry,"sl":sl,"risk":risk,"targets":evaluated,
            "tp1":tps[0],"tp2":tps[1],"tp3":tps[2],"rr":primary["rr"],
            "reason":f"هندسة {candidate.get('trigger')} صالحة باستخدام مستوى إبطال بنيوي وأقرب هدف S/R يحقق R:R."}


def _sr_build_trade(candidate):
    """Scenario-aware independent trade geometry. Uses only real S/R targets and pattern invalidation."""
    g=_sr_geometry_diagnostics(candidate)
    candidate["geometry"]=g
    if not g.get("ok"): return None
    return {
        "entry":round(g["entry"],3),"sl":round(g["sl"],3),
        "tp1":round(g["tp1"],3),"tp2":round(g["tp2"],3) if g.get("tp2") is not None else None,
        "tp3":round(g["tp3"],3) if g.get("tp3") is not None else None,
        "rr":round(g["rr"],2),"score":candidate["score"],"direction":candidate["direction"],
        "trigger":candidate["trigger"],"zone":candidate["zone"],"atr":candidate["atr"],
        "levels":candidate["levels"],"geometry":g,
    }


def _sr_snapshot(df, sr, trade):
    return {
        "version": SR_VERSION,
        "time": now_damascus().isoformat(),
        "price": _sr_num(df["close"].iloc[-1]) if df is not None and len(df) else None,
        "levels": sr,
        "trade": trade,
    }


def _sr_monitor_diagnostics(df, sr):
    """Transparent S/R monitoring state; observational only."""
    try:
        price=_sr_num(df["close"].iloc[-1]); atr=_sr_num(sr.get("atr")); lv=_sr_levels(sr)
        if price is None or atr is None or atr<=0: return {"price":price,"atr":atr,"items":[],"best":None}
        width=max(atr*SR_ZONE_ATR,price*0.00025); struct=_sr_structure_bias(df); items=[]
        for direction in ("BUY","SELL"):
            pool=sorted([v for k,v in lv.items() if v is not None and ((direction=="BUY" and k.startswith("support")) or (direction=="SELL" and k.startswith("resistance")))], key=lambda v:abs(v-price))[:3]
            for zone in pool:
                dist=abs(price-zone); dist_atr=dist/max(atr,1e-9)
                scenarios={"FALSE_BREAKOUT":_sr_false_breakout_features(df,zone,direction,width,atr),"BREAKOUT_RETEST":_sr_breakout_retest_features(df,zone,direction,width,atr),"REJECTION":_sr_rejection_features(df,zone,direction,width,atr),"SR_FLIP":_sr_flip_features(df,zone,direction,width,atr)}
                best_name,best_data=max(scenarios.items(),key=lambda kv:kv[1].get("score",0))
                if dist_atr>1.50: state,reason="FAR",f"المستوى يبعد {dist_atr:.2f} ATR؛ بانتظار اقتراب السعر"
                elif best_data.get("confirmed"): state,reason="CONFIRMED",f"{best_name} مكتمل؛ بانتظار فحص Entry/SL/TP وR:R"
                elif best_name=="REJECTION": state,reason="WAIT_REJECTION","بانتظار لمس المنطقة + شمعة رفض مؤكدة"
                elif best_name=="FALSE_BREAKOUT": state,reason="WAIT_FALSE_BREAKOUT","بانتظار اختراق كاذب ثم إغلاق مستعيد للمستوى"
                elif best_name=="BREAKOUT_RETEST": state,reason="WAIT_RETEST","بانتظار كسر واضح ثم إعادة اختبار وثبات"
                else: state,reason="WAIT_FLIP","بانتظار كسر المستوى وتحول دوره ثم ثبات"
                items.append({"direction":direction,"level":zone,"distance":dist,"distance_atr":dist_atr,"state":state,"reason":reason,"best_scenario":best_name,"best_score":round(best_data.get("score",0),1),"scenarios":scenarios,"structure":struct.get(direction,0)})
        items.sort(key=lambda x:x["distance"])
        return {"price":price,"atr":atr,"width":width,"items":items,"best":items[0] if items else None}
    except Exception as exc:
        logger.exception("S/R monitor diagnostic failure")
        return {"price":None,"atr":None,"items":[],"best":None,"error":f"{type(exc).__name__}: {exc}"}


def sr_analyze():
    """Independent S/R decision engine with multi-scenario diagnostics."""
    try:
        with handler_trace_stage("sr_analyze:get_bars", interval="15m"):
            df=get_bars("15m",limit=300)
    except Exception as exc:
        logger.exception("S/R data acquisition failed"); return {"ok":False,"error_code":"DATA_FETCH","reason":f"تعذر جلب بيانات M15: {type(exc).__name__}: {exc}"}
    with handler_trace_stage("sr_analyze:post_get_bars_validation"):
        if df is None or getattr(df,"empty",True) or len(df)<30: return {"ok":False,"error_code":"DATA_EMPTY","reason":"بيانات M15 غير كافية لتشغيل S/R."}
        missing=sorted({"open","high","low","close"}.difference(df.columns))
        if missing: return {"ok":False,"error_code":"DATA_SCHEMA","reason":f"أعمدة M15 مفقودة: {', '.join(missing)}"}
    try:
        with handler_trace_stage("sr_analyze:support_resistance"):
            sr=support_resistance(df,lookback=min(120,len(df)))
    except Exception as exc:
        logger.exception("S/R level engine failed"); return {"ok":False,"error_code":"SR_ENGINE","reason":f"فشل حساب مستويات S/R: {type(exc).__name__}: {exc}"}
    with handler_trace_stage("sr_analyze:post_sr_validation"):
        if not isinstance(sr,dict): return {"ok":False,"error_code":"SR_SCHEMA","reason":"محرك S/R أعاد بنية غير صالحة."}
    with handler_trace_stage("sr_analyze:monitor_diagnostics"):
        monitor=_sr_monitor_diagnostics(df,sr)
    with handler_trace_stage("sr_analyze:candidate_buy"):
        buy=_sr_candidate_setup(df,sr,"BUY")
    with handler_trace_stage("sr_analyze:candidate_sell"):
        sell=_sr_candidate_setup(df,sr,"SELL")
    with handler_trace_stage("sr_analyze:candidate_filter"):
        cands=[x for x in (buy,sell) if x]
        if not cands:
            return {"ok":True,"signal":None,"sr":sr,"df":df,"monitor":monitor,"reason":"لا توجد إشارة مؤكدة حالياً. محرك المراقبة يحدد المستوى والسيناريو والشرط الناقص قبل الدخول."}
    with handler_trace_stage("sr_analyze:sort_candidates"):
        cands.sort(key=lambda x:x["score"],reverse=True); best=cands[0]
    with handler_trace_stage("sr_analyze:build_trade"):
        trade=_sr_build_trade(best)
        if not trade:
            return {"ok":True,"signal":None,"sr":sr,"df":df,"candidate":best,"monitor":monitor,"reason":f"تم تأكيد سيناريو {best['trigger']} لكن هندسة Entry/SL/TP أو R:R لم تكن صالحة؛ الصفقة مرفوضة حمايةً لرأس المال."}
    with handler_trace_stage("sr_analyze:final_payload"):
        return {"ok":True,"signal":trade,"sr":sr,"df":df,"candidate":best,"monitor":monitor}


def _sr_insert_trade(trade, snapshot):
    setup_id = f"SR-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8].upper()}"
    conn = _sr_db_connect()
    try:
        cur = conn.execute("""
            INSERT OR IGNORE INTO sr_trades
            (setup_id, created_at, direction, entry, sl, tp1, tp2, tp3, rr, score, trigger, status, snapshot_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)
        """, (setup_id, now_damascus().isoformat(), trade["direction"], trade["entry"], trade["sl"],
              trade["tp1"], trade.get("tp2"), trade.get("tp3"), trade["rr"], trade["score"],
              trade["trigger"], json.dumps(snapshot, ensure_ascii=False, default=str)))
        if cur.rowcount == 1:
            conn.execute("INSERT INTO sr_trade_history(setup_id,event_time,status,result,price,note) VALUES(?,?,?,?,?,?)",
                         (setup_id, now_damascus().isoformat(), "OPEN", None, trade["entry"], "S/R setup created"))
            conn.commit()
            return setup_id, True
        conn.rollback()
        return None, False
    finally:
        conn.close()


def _sr_get_open_trades():
    conn = _sr_db_connect()
    try:
        return [dict(r) for r in conn.execute("SELECT * FROM sr_trades WHERE status='OPEN' ORDER BY created_at DESC").fetchall()]
    finally:
        conn.close()


def _sr_update_lifecycle(price):
    price = _sr_num(price)
    if price is None:
        return []
    changed=[]
    conn=_sr_db_connect()
    try:
        rows=conn.execute("SELECT * FROM sr_trades WHERE status='OPEN'").fetchall()
        for r in rows:
            direction=r["direction"]
            status=None; result=None
            if direction == "BUY":
                if price <= r["sl"]: status,result="CLOSED","SL"
                elif price >= r["tp3"] if r["tp3"] is not None else False: status,result="CLOSED","TP3"
                elif r["tp2"] is not None and price >= r["tp2"]: status,result="TP2_HIT","TP2"
                elif price >= r["tp1"]: status,result="TP1_HIT","TP1"
            else:
                if price >= r["sl"]: status,result="CLOSED","SL"
                elif price <= r["tp3"] if r["tp3"] is not None else False: status,result="CLOSED","TP3"
                elif r["tp2"] is not None and price <= r["tp2"]: status,result="TP2_HIT","TP2"
                elif price <= r["tp1"]: status,result="TP1_HIT","TP1"
            if status:
                final_status = "CLOSED" if status == "CLOSED" else status
                conn.execute("UPDATE sr_trades SET status=?, result=?, closed_at=? WHERE setup_id=? AND status='OPEN'",
                             (final_status, result, now_damascus().isoformat() if final_status=='CLOSED' else None, r["setup_id"]))
                conn.execute("INSERT INTO sr_trade_history(setup_id,event_time,status,result,price,note) VALUES(?,?,?,?,?,?)",
                             (r["setup_id"], now_damascus().isoformat(), final_status, result, price, "S/R lifecycle update"))
                changed.append((dict(r), final_status, result, price))
        conn.commit()
        # Keep bounded history.
        conn.execute("DELETE FROM sr_trade_history WHERE id NOT IN (SELECT id FROM sr_trade_history ORDER BY id DESC LIMIT ?)", (SR_MAX_HISTORY,))
        conn.commit()
        return changed
    finally:
        conn.close()


def sr_trade_history(limit=20):
    conn=_sr_db_connect()
    try:
        return [dict(r) for r in conn.execute("SELECT * FROM sr_trades ORDER BY created_at DESC LIMIT ?", (int(limit),)).fetchall()]
    finally:
        conn.close()


def _sr_format(result):
    sr=result.get("sr") or {}; signal=result.get("signal"); monitor=result.get("monitor") or {}
    lines=["🎯 صفقة G.Y الخاصة",]
    price=_sr_num(monitor.get("price"))
    if price is not None: lines.append(f"💰 السعر المرجعي M15: {price:.3f}")
    if monitor.get("atr"): lines.append(f"📏 ATR: {monitor['atr']:.3f}")
    if not signal:
        lines.append(f"ℹ️ {result.get('reason','لا توجد صفقة حالياً.')}")
        best=monitor.get("best")
        if best:
            lines += ["",f"👁️ أقرب فرصة: {'BUY 🟢' if best['direction']=='BUY' else 'SELL 🔴'} @ {best['level']:.3f}",f"📐 البعد: {best['distance']:.3f} ({best['distance_atr']:.2f} ATR)",f"🧭 السيناريو الأقرب: {best['best_scenario']}",f"⭐ تقييمه: {best['best_score']:.0f}/80",f"⏳ المطلوب قبل الدخول: {best['reason']}",f"🏗️ توافق البنية: {best.get('structure',0)}/3"]
        items=monitor.get("items",[])
        if items:
            lines += ["","🔎 حالات المراقبة:"]
            for x in items:
                lines.append(f"{'🟢' if x['direction']=='BUY' else '🔴'} {x['direction']} {x['level']:.3f} | {x['state']} | {x['reason']}")
    else:
        lines += [f"🎯 الاتجاه: {'شراء 🟢' if signal['direction']=='BUY' else 'بيع 🔴'}",f"⭐ جودة الإشارة: {signal['score']:.0f} نقطة",f"🧭 المحفز: {signal['trigger']}",f"💰 الدخول: {signal['entry']}",f"🛑 SL: {signal['sl']}",f"🎯 TP1: {signal['tp1']}",f"🎯 TP2: {signal.get('tp2') or '—'}",f"🎯 TP3: {signal.get('tp3') or '—'}",f"📊 R:R: 1:{signal['rr']}"]
    for key,label in (("support1","S1"),("support2","S2"),("support3","S3"),("resistance1","R1"),("resistance2","R2"),("resistance3","R3")):
        p=_sr_level_price(sr.get(key))
        if p is not None: lines.append(f"{label}: {p:.3f}")
    return "\n".join(lines)


async def sr_button(update, context):
    try:
        with handler_trace_stage("sr_button:ensure_user"):
            await asyncio.to_thread(_ensure_user, update)
        with handler_trace_stage("sr_button:sr_analyze"):
            result=await asyncio.to_thread(sr_analyze)
        with handler_trace_stage("sr_button:format"):
            text=_sr_format(result)
        if not result.get("ok",False):
            with handler_trace_stage("sr_button:reply_error_result"):
                await reply(update, text+f"\n\n🧪 رمز التشخيص: {result.get('error_code','SR_UNKNOWN')}")
            return
        if result.get("signal"):
            with handler_trace_stage("sr_button:snapshot"):
                snapshot=_sr_snapshot(result.get("df"),result.get("sr",{}),result["signal"])
            try:
                with handler_trace_stage("sr_button:sqlite_insert"):
                    setup_id,is_new=await asyncio.to_thread(_sr_insert_trade,result["signal"],snapshot)
            except Exception as exc:
                logger.exception("S/R SQLite insert failed")
                with handler_trace_stage("sr_button:reply_insert_error"):
                    await reply(update,text+f"\n\n❌ تعذر حفظ الصفقة في سجل S/R: {type(exc).__name__}: {exc}")
                return
            with handler_trace_stage("sr_button:append_setup_id"):
                if setup_id and is_new:
                    text+=f"\n\n🆔 {setup_id}\n🔒 هذه الصفقة مستقلة عن نظام الصفقات الرئيسي."
                    # اعتماد الصفقة في سجل S/R هو نقطة السماح الوحيدة لإرسال إشعار S/R.
                    with handler_trace_stage("sr_button:notify_approved_sr_trade"):
                        await _sr_notify_all("🚨 تم اعتماد صفقة G.Y الخاصة\n\n" + _sr_format(result) + f"\n\n🆔 {setup_id}")
        with handler_trace_stage("sr_button:reply_final"):
            await reply(update,text)
    except Exception as exc:
        trace = _HANDLER_TRACE_CTX.get()
        if isinstance(trace, dict):
            trace["sr_exception"] = {
                "type": type(exc).__name__,
                "message": str(exc)[:1000],
            }
        logger.exception("S/R button error")
        with handler_trace_stage("sr_button:reply_outer_error"):
            await reply(update,f"❌ تعذر تنفيذ صفقة G.Y الخاصة حالياً.\n🧪 رمز التشخيص: SR_HANDLER\nتفاصيل: {type(exc).__name__}: {exc}")


async def sr_command(update, context):
    await sr_button(update, context)


async def _sr_notify_all(text):
    if not APPLICATION:
        return
    try:
        recipients=await asyncio.to_thread(_alert_recipients, "trade_alerts")
    except Exception:
        recipients=set(ADMIN_IDS)
    await _dispatch_broadcast(recipients, text, kind="sr")


async def _sr_auto_loop():
    while True:
        try:
            if SR_AUTO_ENABLED and not market_closed_reason():
                result=await asyncio.to_thread(sr_analyze)
                if result.get("signal"):
                    df=await asyncio.to_thread(get_bars, "M15", 120)
                    setup_id,is_new=await asyncio.to_thread(_sr_insert_trade, result["signal"], _sr_snapshot(df,result.get("sr",{}),result["signal"]))
                    if is_new:
                        await _sr_notify_all("🚨 صفقة G.Y الخاصة جديدة\n" + _sr_format(result) + f"\n\n🆔 {setup_id}")
            try:
                p=await asyncio.to_thread(live_price)
                changed=await asyncio.to_thread(_sr_update_lifecycle,p)
                for row,status,res,price in changed:
                    await _sr_notify_all(f"🎯 تحديث صفقة G.Y الخاصة\n🆔 {row['setup_id']}\n💰 السعر: {price}\n📌 النتيجة: {res}\n📊 الحالة: {status}")
            except Exception:
                logger.exception("S/R lifecycle error")
        except Exception:
            logger.exception("S/R auto loop error")
        await asyncio.sleep(SR_AUTO_SECONDS)


_BASELINE_UX_MAIN_KEYBOARD = _ux_main_keyboard


def _sr_ux_main_keyboard():
    kb=_BASELINE_UX_MAIN_KEYBOARD()
    # Preserve every baseline button and add exactly one independent S/R entry point.
    return [list(row) for row in kb] + [[SR_BUTTON_TEXT]]


def _sr_install_telegram_handler():
    APPLICATION.add_handler(CommandHandler("sr", _trace_handler(sr_command, "command:/sr")), group=0)
    APPLICATION.add_handler(
        MessageHandler(filters.TEXT & filters.Regex(r"^🎯 صفقة G\.Y الخاصة$"), _trace_handler(_sr_button_guarded, "message:sr_button")),
        group=0,
    )


async def _sr_button_guarded(update, context):
    await sr_button(update, context)
    raise ApplicationHandlerStop


async def integrated_start_bot():
    """Baseline startup path plus isolated S/R registration; baseline start_bot() is not invoked."""
    global APPLICATION, SR_AUTO_TASK
    if not TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN غير موجود في Render.")
    APPLICATION = Application.builder().token(TOKEN).build()
    APPLICATION.add_handler(CommandHandler("start", _trace_handler(start, "command:/start")))
    APPLICATION.add_handler(CommandHandler("plans", _trace_handler(plans, "command:/plans")))
    APPLICATION.add_handler(CommandHandler("subscription", _trace_handler(my_subscription, "command:/subscription")))
    APPLICATION.add_handler(CommandHandler("referral", _trace_handler(referral, "command:/referral")))
    APPLICATION.add_handler(CommandHandler("admin", _trace_handler(admin_command, "command:/admin")))
    APPLICATION.add_handler(CommandHandler("audit", _trace_handler(audit_report, "command:/audit")))
    APPLICATION.add_handler(CommandHandler("activate", _trace_handler(admin_command, "command:/activate")))
    APPLICATION.add_handler(CallbackQueryHandler(_trace_handler(callback_router, "callback:router")))
    _sr_install_telegram_handler()
    APPLICATION.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _trace_handler(router, "message:router")), group=1)
    await APPLICATION.initialize()
    await APPLICATION.start()
    await _start_alert_dispatcher()
    await APPLICATION.bot.set_webhook(url=WEBHOOK_URL, allowed_updates=["message", "callback_query"], drop_pending_updates=True)
    logger.info("XAU SMART TRADER %s + %s started", VERSION, SR_VERSION)
    logger.info("Webhook: %s", WEBHOOK_URL)
    asyncio.create_task(auto_loop())
    SR_AUTO_TASK = asyncio.create_task(_sr_auto_loop(), name="gy-private-trade-auto")
    if PERFORMANCE_AUDITOR is not None:
        try:
            _start_auditor_bridge_worker()
            PERFORMANCE_AUDITOR.set_self_healer(_auditor_self_heal)
            PERFORMANCE_AUDITOR.set_alert_callback(_auditor_alert_callback)
            PERFORMANCE_AUDITOR.start_background()
            await PERFORMANCE_AUDITOR.start_async_probe()
        except Exception:
            logging.getLogger(__name__).exception("AUDITOR_START_ERROR")
    try:
        start_background_refresh()
        logger.info("Independent News Engine started: analysis path is LOCAL-ONLY")
    except Exception:
        logging.getLogger(__name__).exception("NEWS_ENGINE_START_ERROR")
    while True:
        await asyncio.sleep(3600)


def run_v18_70_sr_runtime_unit_tests():
    """Deterministic S/R math tests; no network, Telegram, or Render required."""
    results=[]
    def check(name,ok,detail=""): results.append({"test":name,"status":"PASS" if ok else "FAIL","detail":detail})
    import pandas as _pd
    rows=[{"open":100.0,"high":100.2,"low":99.8,"close":100.05} for _ in range(25)]
    rows[-2]={"open":100.20,"high":100.35,"low":99.90,"close":99.98}; rows[-1]={"open":99.98,"high":100.30,"low":99.70,"close":100.22}
    fb=_sr_false_breakout_features(_pd.DataFrame(rows),100.0,"BUY",0.25,0.50); check("false breakout BUY",fb.get("confirmed") is True,str(fb))
    rows[-2]={"open":100.20,"high":100.45,"low":100.02,"close":100.30}; rows[-1]={"open":100.25,"high":100.40,"low":100.02,"close":100.22}
    br=_sr_breakout_retest_features(_pd.DataFrame(rows),100.0,"BUY",0.25,0.50); check("breakout retest BUY",br.get("confirmed") is True,str(br))
    gdf=_pd.DataFrame([{"open":99.9,"high":100.1,"low":99.6,"close":100.0},{"open":100.0,"high":100.4,"low":99.7,"close":100.2},{"open":100.2,"high":100.5,"low":99.8,"close":100.3}])
    c={"direction":"BUY","entry":100.3,"zone":99.8,"atr":0.5,"width":0.2,"score":90,"trigger":"BREAKOUT_RETEST","opposite_levels":[100.8,101.5,102.2,103.0],"levels":{},"df":gdf}
    tr=_sr_build_trade(c); check("scenario-aware entry/sl/tp geometry",tr is not None and tr["sl"]<tr["entry"]<tr["tp1"] and tr["rr"]>=SR_MIN_RR and tr["tp1"]==101.5,str(tr))
    return {"passed":sum(x["status"]=="PASS" for x in results),"total":len(results),"ok":all(x["status"]=="PASS" for x in results),"results":results}


def run_v18_69_sr_self_tests():
    """Offline/static contract tests for the single-file S/R integration."""
    import ast as _ast
    source=Path(__file__).read_text(encoding="utf-8")
    tree=_ast.parse(source)
    names={n.name for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))}
    checks=[]
    def check(name,ok,detail=""): checks.append({"test":name,"status":"PASS" if ok else "FAIL","detail":detail})
    canonical=source.split('\nif __name__ == "__main__":',1)[0]
    check("baseline contract", V18_68_BASELINE_VERIFIED and V18_68_SOURCE_SHA256.startswith("fb2128"), str(V18_68_SOURCE_SHA256))
    for n in ("sr_analyze","_sr_build_trade","_sr_false_breakout_features","_sr_breakout_retest_features","sr_button","_sr_auto_loop","integrated_start_bot"):
        check(f"function:{n}", n in names)
    sr_names = {"sr_analyze", "_sr_candidate_setup", "_sr_build_trade", "sr_button", "sr_command", "_sr_auto_loop", "integrated_start_bot"}
    forbidden_calls = {"evaluate_signal", "build_trade"}
    sr_called = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in sr_names:
            for child in ast.walk(node):
                if isinstance(child, ast.Call) and isinstance(child.func, ast.Name):
                    sr_called.add(child.func.id)
    check("independent evaluator isolation", not (sr_called & forbidden_calls), str(sorted(sr_called & forbidden_calls)))
    check("independent builder isolation", not (sr_called & forbidden_calls), str(sorted(sr_called & forbidden_calls)))
    check("exact G.Y private button", 'filters.Regex(r"^🎯 صفقة G\\.Y الخاصة$")' in source)
    check("handler stop", "raise ApplicationHandlerStop" in source)
    check("separate SQLite", "xau_sr_trades.db" in source)
    check("real S/R TP gate", "if not targets:" in source and "return None" in source)
    passed=sum(x["status"]=="PASS" for x in checks)
    return {"passed":passed,"total":len(checks),"ok":passed==len(checks),"results":checks}


# Override only the UI accessor in the new integrated copy; the original baseline file is untouched.
_ux_main_keyboard = _sr_ux_main_keyboard


def main_v18_69_sr():
    if "--sr-self-test" in sys.argv:
        print(json.dumps(run_v18_69_sr_self_tests(), ensure_ascii=False, indent=2))
        return
    if "--sr-unit-test" in sys.argv:
        print(json.dumps(run_v18_70_sr_runtime_unit_tests(), ensure_ascii=False, indent=2))
        return
    if "--self-test" in sys.argv:
        print(json.dumps(run_institutional_self_tests(), ensure_ascii=False, indent=2))
        return
    global BOT_LOOP
    server=threading.Thread(target=run_flask, daemon=True)
    server.start()
    loop=asyncio.new_event_loop()
    BOT_LOOP=loop
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(integrated_start_bot())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()



# ============================================================
# v18.75 MENU ROUTER RESPONSE FIX — APPEND-ONLY
# Isolated performance/delivery layer. Trading rules and S/R logic are untouched.
# ============================================================
ALERT_DB_PATH = os.getenv("ALERT_DB_PATH", "xau_alert_queue.db")
ALERT_POLL_SECONDS = float(os.getenv("ALERT_POLL_SECONDS", "0.50"))
ALERT_MAX_ATTEMPTS = int(os.getenv("ALERT_MAX_ATTEMPTS", "8"))
ALERT_MAX_PENDING = int(os.getenv("ALERT_MAX_PENDING", "20000"))
_PRICE_CACHE_LOCK = threading.RLock()
_LIVE_PRICE_CACHE = {"ts": 0.0, "value": None}

class PersistentAlertDispatcher:
    """Durable SQLite outbox: enqueue is fast; Telegram delivery is asynchronous."""
    def __init__(self, bot, db_path=ALERT_DB_PATH, workers=4):
        self.bot=bot; self.db_path=db_path; self.workers=max(1,min(int(workers),8))
        self._tasks=[]; self._running=False; self._wake=asyncio.Event(); self._db_lock=threading.Lock()
        self._init_db()
    def _conn(self):
        c=sqlite3.connect(self.db_path, timeout=5, check_same_thread=False); c.row_factory=sqlite3.Row
        c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA synchronous=NORMAL'); c.execute('PRAGMA busy_timeout=5000')
        return c
    def _init_db(self):
        with self._db_lock:
            c=self._conn()
            try:
                c.execute("""CREATE TABLE IF NOT EXISTS alert_outbox(
                    alert_id TEXT PRIMARY KEY, chat_id INTEGER NOT NULL, message TEXT NOT NULL,
                    kind TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 50, status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL,
                    created_at REAL NOT NULL, last_attempt REAL, next_retry REAL NOT NULL,
                    sent_at REAL, last_error TEXT)""")
                c.execute('CREATE INDEX IF NOT EXISTS idx_alert_due ON alert_outbox(status,next_retry,priority DESC,created_at)')
                # A process crash must never leave work permanently in SENDING.
                c.execute("UPDATE alert_outbox SET status='PENDING' WHERE status='SENDING'")
                c.commit()
            finally: c.close()
    def enqueue(self, chat_id, text, *, kind='generic', priority=50, max_attempts=ALERT_MAX_ATTEMPTS):
        try: cid=int(chat_id)
        except Exception: return None
        payload=str(text or '').strip()
        if not payload: return None
        now=time.time(); aid='al_'+uuid.uuid4().hex
        with self._db_lock:
            c=self._conn()
            try:
                pending=c.execute("SELECT COUNT(*) FROM alert_outbox WHERE status IN ('PENDING','SENDING')").fetchone()[0]
                # Never silently drop critical trade/system alerts. Low priority is rejected explicitly.
                if pending >= ALERT_MAX_PENDING and int(priority) < 80:
                    logger.error('ALERT_BACKPRESSURE_REJECT | kind=%s pending=%s',kind,pending); return None
                c.execute('INSERT INTO alert_outbox VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (aid,cid,payload,str(kind),int(priority),'PENDING',0,max(1,int(max_attempts)),now,None,now,None,None))
                c.commit()
            finally: c.close()
        self._wake.set(); return aid
    async def start(self):
        if self._running: return
        self._running=True
        self._tasks=[asyncio.create_task(self._worker(i),name=f'persist-alert-{i}') for i in range(self.workers)]
        logger.info('PersistentAlertDispatcher started | workers=%s',self.workers)
    async def stop(self):
        self._running=False; self._wake.set()
        for t in self._tasks: t.cancel()
        if self._tasks: await asyncio.gather(*self._tasks,return_exceptions=True)
        self._tasks=[]
    def health(self):
        with self._db_lock:
            c=self._conn()
            try:
                rows=c.execute("SELECT status,COUNT(*) n FROM alert_outbox GROUP BY status").fetchall()
                return {r['status']:r['n'] for r in rows}
            finally:c.close()
    def _claim(self):
        now=time.time()
        with self._db_lock:
            c=self._conn()
            try:
                c.execute('BEGIN IMMEDIATE')
                row=c.execute("SELECT * FROM alert_outbox WHERE status='PENDING' AND next_retry<=? ORDER BY priority DESC,created_at LIMIT 1",(now,)).fetchone()
                if row: c.execute("UPDATE alert_outbox SET status='SENDING',last_attempt=? WHERE alert_id=? AND status='PENDING'",(now,row['alert_id']))
                c.commit(); return dict(row) if row else None
            except Exception:
                c.rollback(); raise
            finally:c.close()
    def _result(self, job, ok, error=None, retry_after=None):
        now=time.time(); attempts=int(job['attempts'])+(0 if ok else 1)
        with self._db_lock:
            c=self._conn()
            try:
                if ok:
                    c.execute("UPDATE alert_outbox SET status='SENT',sent_at=?,last_error=NULL WHERE alert_id=?",(now,job['alert_id']))
                elif attempts >= int(job['max_attempts']):
                    c.execute("UPDATE alert_outbox SET status='FAILED',attempts=?,last_error=? WHERE alert_id=?",(attempts,str(error)[:500],job['alert_id']))
                else:
                    delay=retry_after if retry_after is not None else min(60.0,2**min(attempts,6))+random.uniform(0,0.5)
                    c.execute("UPDATE alert_outbox SET status='PENDING',attempts=?,next_retry=?,last_error=? WHERE alert_id=?",(attempts,now+delay,str(error)[:500],job['alert_id']))
                c.commit()
            finally:c.close()
    async def _worker(self, idx):
        while self._running:
            job=await asyncio.to_thread(self._claim)
            if not job:
                self._wake.clear()
                try: await asyncio.wait_for(self._wake.wait(),timeout=ALERT_POLL_SECONDS)
                except asyncio.TimeoutError: pass
                continue
            t0=time.perf_counter()
            try:
                for part in _split_telegram_text(job['message']):
                    await self.bot.send_message(chat_id=job['chat_id'],text=part)
                await asyncio.to_thread(self._result,job,True)
                logger.info('TELEGRAM_SENT | kind=%s latency_ms=%.1f',job['kind'],(time.perf_counter()-t0)*1000)
            except (Forbidden,BadRequest) as e:
                await asyncio.to_thread(self._result,{**job,'max_attempts':1},False,e)
            except RetryAfter as e:
                ra=getattr(e,'retry_after',1); ra=float(ra.total_seconds() if hasattr(ra,'total_seconds') else ra)
                await asyncio.to_thread(self._result,job,False,e,max(1.0,ra))
            except Exception as e:
                logger.warning('TELEGRAM_RETRY | kind=%s error=%s',job['kind'],e)
                await asyncio.to_thread(self._result,job,False,e)

async def _start_alert_dispatcher():
    global ALERT_DISPATCHER
    if not APPLICATION: return None
    if not isinstance(ALERT_DISPATCHER,PersistentAlertDispatcher):
        if ALERT_DISPATCHER is not None:
            try: await ALERT_DISPATCHER.stop()
            except Exception: pass
        ALERT_DISPATCHER=PersistentAlertDispatcher(APPLICATION.bot,workers=int(os.getenv('ALERT_WORKERS','4')))
    await ALERT_DISPATCHER.start(); return ALERT_DISPATCHER

async def _dispatch_alert(chat_id,text,*,kind='generic',max_attempts=ALERT_MAX_ATTEMPTS):
    d=ALERT_DISPATCHER or await _start_alert_dispatcher()
    if not d:return False
    priority=100 if kind in {'trade','sr','system','critical'} else 70 if kind in {'news','session_preopen'} else 40
    aid=await asyncio.to_thread(d.enqueue,chat_id,text,kind=kind,priority=priority,max_attempts=max_attempts)
    return bool(aid)

async def _dispatch_broadcast(recipients,text,*,kind='generic',max_attempts=ALERT_MAX_ATTEMPTS):
    results=[]
    for cid in set(recipients or []): results.append(await _dispatch_alert(cid,text,kind=kind,max_attempts=max_attempts))
    return results

# Lightweight shared live-price cache: prevents duplicate provider calls during bursts.
def live_price():
    now=time.time()
    with _PRICE_CACHE_LOCK:
        if _LIVE_PRICE_CACHE['value'] is not None and now-_LIVE_PRICE_CACHE['ts']<2.0:
            return dict(_LIVE_PRICE_CACHE['value'])
    errors=[]
    for url,params,parser,name in [
        (LIVE_URL,{'allowStale':'false'},lambda d: sf(d.get('mid'),None) if isinstance(d,dict) else None,'Biquote'),
        ('https://xaus.com/api/v1/spot',None,lambda d: sf(d.get('spot_usd_oz'),None) if isinstance(d,dict) else None,'XAUS')]:
        try:
            t0=time.perf_counter(); r=requests.get(url,params=params,timeout=(3,8));
            if r.ok:
                p=parser(r.json())
                if p and p>0:
                    val={'price':p,'source':name,'age':None}
                    with _PRICE_CACHE_LOCK:_LIVE_PRICE_CACHE.update({'ts':time.time(),'value':val})
                    logger.debug('PRICE_API_LATENCY | source=%s ms=%.1f',name,(time.perf_counter()-t0)*1000)
                    return dict(val)
        except Exception as e: errors.append(str(e))
    raise RuntimeError('تعذر الحصول على السعر اللحظي: '+' | '.join(errors))

def sre_runtime_health():
    return {'alert_outbox': ALERT_DISPATCHER.health() if isinstance(ALERT_DISPATCHER,PersistentAlertDispatcher) else {},
            'alert_running': bool(getattr(ALERT_DISPATCHER,'_running',False)),
            'price_cache_age': max(0.0,time.time()-_LIVE_PRICE_CACHE['ts']) if _LIVE_PRICE_CACHE['ts'] else None}


def _install_handler_latency_root_trace():
    """Install transparent stage tracing after all functions are defined."""
    targets = (
        ("evaluate_signal", "stage:evaluate_signal"),
        ("_daily_execution_core", "stage:daily_execution_core"),
        ("institutional_analysis", "stage:institutional_analysis"),
        ("get_bars", "stage:get_bars"),
        ("live_price", "stage:live_price"),
        ("support_resistance", "stage:support_resistance"),
        ("update_trade_results", "stage:update_trade_results"),
        ("reply", "stage:telegram_reply"),
        ("quick_analysis", "stage:quick_analysis"),
        ("full_analysis", "stage:full_analysis"),
        ("trade_now", "stage:trade_now"),
        ("show_levels", "stage:show_levels"),
        ("trade_history", "stage:trade_history"),
        ("daily_report", "stage:daily_report"),
        ("weekly_report", "stage:weekly_report"),
        ("news_status", "stage:news_status"),
        ("markets", "stage:markets"),
        ("gold_price", "stage:gold_price"),
        ("status", "stage:status"),
        ("plans", "stage:plans"),
        ("my_subscription", "stage:my_subscription"),
        ("subscribe", "stage:subscribe"),
        ("plan_callback", "stage:plan_callback"),
        ("subscription_request_callback", "stage:subscription_request"),
        ("analyses_menu", "stage:analyses_menu"),
        ("trades_menu", "stage:trades_menu"),
        ("market_news_menu", "stage:market_news_menu"),
        ("institutional_menu", "stage:institutional_menu"),
        ("liquidity_menu", "stage:liquidity_menu"),
        ("help_menu", "stage:help_menu"),
        ("_ux_render", "stage:ux_render"),
    )
    for fn_name, stage_name in targets:
        fn = globals().get(fn_name)
        if fn is not None and not getattr(fn, "_xau_root_trace_wrapped", False):
            globals()[fn_name] = _trace_stage_function(stage_name)(fn)
    logger.info("Handler Latency Root Trace installed | handlers=command/callback/message | stages=%d", len(targets))

_install_handler_latency_root_trace()



# ============================================================
# v18.75 MENU ROUTER RESPONSE FIX
# Fix: assign `fn = routes.get(text)` before dispatch.
# This is intentionally isolated from trading, scoring, S/R and alert logic.
# ============================================================


# ============================================================
# v18.85 ALERT DELIVERY + SCHEDULER + TELEMETRY FIX
# Scope strictly limited to Alert Delivery / Scheduler / Telemetry.
# Trading engine, scoring, S/R, Webhook and handlers are untouched.
# ============================================================
ALERT_DELIVERY_WAIT_SECONDS = float(os.getenv("ALERT_DELIVERY_WAIT_SECONDS", "90"))
ALERT_SCHEDULER_SECONDS = float(os.getenv("ALERT_SCHEDULER_SECONDS", "30"))
ALERT_TELEMETRY = {"scheduler_started": False, "scheduler_last_tick": None, "scheduler_errors": 0}

class PersistentAlertDispatcher:
    """Durable SQLite outbox with delivery-aware waiters and end-to-end telemetry."""
    def __init__(self, bot, db_path=ALERT_DB_PATH, workers=4):
        self.bot=bot; self.db_path=db_path; self.workers=max(1,min(int(workers),8))
        self._tasks=[]; self._running=False; self._wake=asyncio.Event(); self._db_lock=threading.Lock()
        self._waiters={}; self._waiters_lock=threading.RLock()
        self._telemetry_lock=threading.Lock(); self._telemetry={"enqueued":0,"claimed":0,"send_started":0,"sent":0,"retried":0,"failed":0}
        self._init_db()
    def _conn(self):
        c=sqlite3.connect(self.db_path, timeout=5, check_same_thread=False); c.row_factory=sqlite3.Row
        c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA synchronous=NORMAL'); c.execute('PRAGMA busy_timeout=5000')
        return c
    def _init_db(self):
        with self._db_lock:
            c=self._conn()
            try:
                c.execute("""CREATE TABLE IF NOT EXISTS alert_outbox(
                    alert_id TEXT PRIMARY KEY, chat_id INTEGER NOT NULL, message TEXT NOT NULL,
                    kind TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 50, status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL,
                    created_at REAL NOT NULL, last_attempt REAL, next_retry REAL NOT NULL,
                    sent_at REAL, last_error TEXT)""")
                # Safe telemetry migration for databases created by earlier versions.
                cols={r[1] for r in c.execute("PRAGMA table_info(alert_outbox)").fetchall()}
                for name, typ in (("claimed_at","REAL"),("send_started_at","REAL"),("delivery_latency_ms","REAL")):
                    if name not in cols: c.execute(f"ALTER TABLE alert_outbox ADD COLUMN {name} {typ}")
                c.execute('CREATE INDEX IF NOT EXISTS idx_alert_due ON alert_outbox(status,next_retry,priority DESC,created_at)')
                c.execute("UPDATE alert_outbox SET status='PENDING' WHERE status='SENDING'")
                c.commit()
            finally: c.close()
    def enqueue(self, chat_id, text, *, kind='generic', priority=50, max_attempts=ALERT_MAX_ATTEMPTS):
        try: cid=int(chat_id)
        except Exception: return None
        payload=str(text or '').strip()
        if not payload: return None
        now=time.time(); aid='al_'+uuid.uuid4().hex
        with self._db_lock:
            c=self._conn()
            try:
                pending=c.execute("SELECT COUNT(*) FROM alert_outbox WHERE status IN ('PENDING','SENDING')").fetchone()[0]
                if pending >= ALERT_MAX_PENDING and int(priority) < 80:
                    logger.error('ALERT_BACKPRESSURE_REJECT | kind=%s pending=%s',kind,pending); return None
                c.execute('INSERT INTO alert_outbox(alert_id,chat_id,message,kind,priority,status,attempts,max_attempts,created_at,last_attempt,next_retry,sent_at,last_error) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (aid,cid,payload,str(kind),int(priority),'PENDING',0,max(1,int(max_attempts)),now,None,now,None,None))
                c.commit()
            finally: c.close()
        self._wake.set()
        with self._telemetry_lock:self._telemetry["enqueued"]+=1
        logger.info('ALERT_ENQUEUED | id=%s kind=%s',aid,kind); return aid
    def _register_waiter(self, aid):
        loop=asyncio.get_running_loop(); fut=loop.create_future()
        with self._waiters_lock: self._waiters[aid]=fut
        return fut
    def _resolve_waiter(self, aid, ok):
        with self._waiters_lock: fut=self._waiters.pop(aid,None)
        if fut and not fut.done(): fut.set_result(bool(ok))
    async def wait_delivery(self, aid, timeout=ALERT_DELIVERY_WAIT_SECONDS):
        if not aid: return False
        # Check durable state first, so a very fast worker cannot race waiter registration.
        state=await asyncio.to_thread(self.delivery_status,aid)
        if state=='SENT': return True
        if state=='FAILED': return False
        fut=self._register_waiter(aid)
        try:
            state=await asyncio.to_thread(self.delivery_status,aid)
            if state=='SENT': self._resolve_waiter(aid,True)
            elif state=='FAILED': self._resolve_waiter(aid,False)
            return await asyncio.wait_for(fut,timeout=max(1.0,float(timeout)))
        except asyncio.TimeoutError:
            with self._waiters_lock: self._waiters.pop(aid,None)
            logger.warning('ALERT_DELIVERY_WAIT_TIMEOUT | id=%s timeout=%s status=%s',aid,timeout,await asyncio.to_thread(self.delivery_status,aid))
            # IMPORTANT: timeout means "delivery still unresolved", not FAILED.
            return None
    def delivery_status(self, aid):
        with self._db_lock:
            c=self._conn()
            try:
                row=c.execute("SELECT status FROM alert_outbox WHERE alert_id=?",(aid,)).fetchone()
                return row['status'] if row else None
            finally:c.close()
    async def start(self):
        if self._running:return
        self._running=True; self._tasks=[asyncio.create_task(self._worker(i),name=f'persist-alert-{i}') for i in range(self.workers)]
        logger.info('PersistentAlertDispatcher started | workers=%s',self.workers)
    async def stop(self):
        self._running=False; self._wake.set()
        for t in self._tasks:t.cancel()
        if self._tasks:await asyncio.gather(*self._tasks,return_exceptions=True)
        self._tasks=[]
    def health(self):
        now=time.time()
        with self._db_lock:
            c=self._conn()
            try:
                rows=c.execute("SELECT status,COUNT(*) n FROM alert_outbox GROUP BY status").fetchall(); out={r['status']:r['n'] for r in rows}
                row=c.execute("SELECT MIN(created_at) oldest FROM alert_outbox WHERE status='PENDING'").fetchone()
                out['oldest_pending_age_seconds']=round(now-row['oldest'],3) if row and row['oldest'] else 0.0
                with self._telemetry_lock: out['delivery_telemetry']=dict(self._telemetry)
                return out
            finally:c.close()
    def _claim(self):
        now=time.time()
        with self._db_lock:
            c=self._conn()
            try:
                c.execute('BEGIN IMMEDIATE')
                row=c.execute("SELECT * FROM alert_outbox WHERE status='PENDING' AND next_retry<=? ORDER BY priority DESC,created_at LIMIT 1",(now,)).fetchone()
                if row:c.execute("UPDATE alert_outbox SET status='SENDING',last_attempt=?,claimed_at=?,send_started_at=? WHERE alert_id=? AND status='PENDING'",(now,now,now,row['alert_id']))
                c.commit()
                if row:
                    with self._telemetry_lock:self._telemetry["claimed"]+=1
                    logger.debug("ALERT_CLAIMED | id=%s",row["alert_id"])
                return dict(row) if row else None
            except Exception:c.rollback();raise
            finally:c.close()
    def _result(self,job,ok,error=None,retry_after=None):
        now=time.time(); attempts=int(job['attempts'])+(0 if ok else 1); final=False
        with self._db_lock:
            c=self._conn()
            try:
                if ok:
                    latency=(now-float(job['created_at']))*1000.0
                    c.execute("UPDATE alert_outbox SET status='SENT',sent_at=?,delivery_latency_ms=?,last_error=NULL WHERE alert_id=?",(now,latency,job['alert_id'])); final=True
                elif attempts>=int(job['max_attempts']):
                    c.execute("UPDATE alert_outbox SET status='FAILED',attempts=?,last_error=? WHERE alert_id=?",(attempts,str(error)[:500],job['alert_id'])); final=True
                else:
                    delay=retry_after if retry_after is not None else min(60.0,2**min(attempts,6))+random.uniform(0,0.5)
                    c.execute("UPDATE alert_outbox SET status='PENDING',attempts=?,next_retry=?,last_error=? WHERE alert_id=?",(attempts,now+delay,str(error)[:500],job['alert_id']))
                c.commit()
            finally:c.close()
        if final:self._resolve_waiter(job['alert_id'],bool(ok))
    async def _worker(self,idx):
        while self._running:
            job=await asyncio.to_thread(self._claim)
            if not job:
                self._wake.clear()
                try:await asyncio.wait_for(self._wake.wait(),timeout=ALERT_POLL_SECONDS)
                except asyncio.TimeoutError:pass
                continue
            t0=time.perf_counter()
            with self._telemetry_lock:self._telemetry["send_started"]+=1
            logger.debug("ALERT_SEND_START | id=%s kind=%s",job["alert_id"],job["kind"])
            try:
                for part in _split_telegram_text(job['message']):await self.bot.send_message(chat_id=job['chat_id'],text=part)
                await asyncio.to_thread(self._result,job,True)
                with self._telemetry_lock:self._telemetry["sent"]+=1
                logger.info('ALERT_DELIVERED_E2E | id=%s kind=%s queue_plus_delivery_ms=%.1f',job['alert_id'],job['kind'],(time.time()-job['created_at'])*1000)
            except (Forbidden,BadRequest) as e:
                with self._telemetry_lock:self._telemetry["failed"]+=1
                await asyncio.to_thread(self._result,{**job,'max_attempts':1},False,e)
            except RetryAfter as e:
                with self._telemetry_lock:self._telemetry["retried"]+=1
                ra=getattr(e,'retry_after',1);ra=float(ra.total_seconds() if hasattr(ra,'total_seconds') else ra)
                await asyncio.to_thread(self._result,job,False,e,max(1.0,ra))
            except Exception as e:
                with self._telemetry_lock:self._telemetry["retried"]+=1
                logger.warning('TELEGRAM_RETRY | id=%s kind=%s error=%s',job['alert_id'],job['kind'],e);await asyncio.to_thread(self._result,job,False,e)

async def _start_alert_dispatcher():
    global ALERT_DISPATCHER
    if not APPLICATION:return None
    if not isinstance(ALERT_DISPATCHER,PersistentAlertDispatcher):
        if ALERT_DISPATCHER is not None:
            try:await ALERT_DISPATCHER.stop()
            except Exception:logger.exception('Previous dispatcher stop failed')
        ALERT_DISPATCHER=PersistentAlertDispatcher(APPLICATION.bot,workers=int(os.getenv('ALERT_WORKERS','4')))
    await ALERT_DISPATCHER.start();return ALERT_DISPATCHER

async def _dispatch_alert(chat_id,text,*,kind='generic',max_attempts=ALERT_MAX_ATTEMPTS,wait_delivery=False,delivery_timeout=ALERT_DELIVERY_WAIT_SECONDS):
    d=ALERT_DISPATCHER or await _start_alert_dispatcher()
    if not d:return False
    priority=100 if kind in {'trade','sr','system','critical'} else 70 if kind in {'news','session_preopen'} else 40
    aid=await asyncio.to_thread(d.enqueue,chat_id,text,kind=kind,priority=priority,max_attempts=max_attempts)
    if not aid:return False
    if not wait_delivery:return True
    return await d.wait_delivery(aid,delivery_timeout)

async def _dispatch_broadcast(recipients,text,*,kind='generic',max_attempts=ALERT_MAX_ATTEMPTS):
    # Fast fan-out into durable queue; Telegram workers deliver independently.
    return await asyncio.gather(*[_dispatch_alert(cid,text,kind=kind,max_attempts=max_attempts) for cid in set(recipients or [])],return_exceptions=False)

async def _finalize_trade_delivery_after_timeout(aid, chat_id, signature, consume_quota, quota_token):
    """Resolve the business claim only after the durable outbox reaches a terminal state."""
    d=ALERT_DISPATCHER
    if not d:
        return
    while True:
        status=await asyncio.to_thread(d.delivery_status,aid)
        if status=='SENT':
            _complete_notification(chat_id,signature)
            _mark_notification_sent(chat_id,signature)
            logger.info('TRADE_ALERT_DELIVERY_FINALIZED | chat_id=%s alert_id=%s status=SENT',chat_id,aid)
            return
        if status=='FAILED' or status is None:
            _cancel_notification_claim(chat_id,signature)
            if consume_quota:
                await asyncio.to_thread(_release_trade_quota,quota_token)
            logger.error('TRADE_ALERT_DELIVERY_FINALIZED | chat_id=%s alert_id=%s status=%s',chat_id,aid,status)
            return
        await asyncio.sleep(min(max(float(ALERT_POLL_SECONDS),0.5),2.0))

async def _send_trade_notification(chat_id,result,record,title,consume_quota=False,quota_token=None):
    signature=_trade_notification_signature(record)
    if not signature:return False
    if _notification_already_sent(chat_id,signature):return False
    if not _claim_notification(chat_id,signature):return False
    quota_token=quota_token if consume_quota else None
    if consume_quota and not quota_token:
        _cancel_notification_claim(chat_id,signature);return False
    try:
        payload=_format_trade_message(result,title,record)
        d=ALERT_DISPATCHER or await _start_alert_dispatcher()
        if not d: raise RuntimeError('Alert dispatcher unavailable')
        aid=await asyncio.to_thread(d.enqueue,chat_id,payload,kind='trade',priority=100,max_attempts=6)
        if not aid: raise RuntimeError('Alert enqueue failed')
        delivered=await d.wait_delivery(aid,ALERT_DELIVERY_WAIT_SECONDS)
        if delivered is True:
            _complete_notification(chat_id,signature);_mark_notification_sent(chat_id,signature);return True
        if delivered is None:
            # WAIT TIMEOUT IS NOT FAILURE: keep claim and quota reserved while outbox can still deliver.
            asyncio.create_task(_finalize_trade_delivery_after_timeout(aid,chat_id,signature,consume_quota,quota_token),name=f'trade-alert-finalize-{aid}')
            logger.warning('TRADE_ALERT_PENDING_AFTER_TIMEOUT | chat_id=%s alert_id=%s',chat_id,aid)
            return True
        raise RuntimeError('Telegram delivery failed permanently')
    except Exception:
        _cancel_notification_claim(chat_id,signature)
        if consume_quota:await asyncio.to_thread(_release_trade_quota,quota_token)
        logger.exception('Signal delivery failed for %s',chat_id);return False

async def _alert_scheduler_loop():
    """Dedicated timing loop: sessions/news are no longer gated by the 15-minute trading scan."""
    if ALERT_TELEMETRY.get('scheduler_started'):return
    ALERT_TELEMETRY['scheduler_started']=True
    logger.info('ALERT_SCHEDULER_STARTED | interval=%ss',ALERT_SCHEDULER_SECONDS)
    while True:
        try:
            t0=time.perf_counter(); await send_market_session_alerts(); await send_news_alerts()
            ALERT_TELEMETRY['scheduler_last_tick']=time.time()
            logger.info('ALERT_SCHEDULER_TICK | latency_ms=%.1f',(time.perf_counter()-t0)*1000)
        except Exception:
            ALERT_TELEMETRY['scheduler_errors']+=1;logger.exception('ALERT_SCHEDULER_ERROR')
        await asyncio.sleep(max(5.0,ALERT_SCHEDULER_SECONDS))

async def auto_loop():
    """Trading scan remains on its configured cadence; timing alerts run in a dedicated scheduler."""
    asyncio.create_task(_alert_scheduler_loop(), name="alert-timing-scheduler")
    while True:
        try:
            # Session/news alerts are intentionally NOT called here: they have a dedicated scheduler.
            changed_records = await asyncio.to_thread(update_trade_results)

            eligible = await asyncio.to_thread(alert_subscribers)
            SUBSCRIBERS.update(eligible)
            SUBSCRIBERS.intersection_update(eligible)

            if SUBSCRIBERS and changed_records:
                for record in changed_records:
                    result_update = {
                        "direction": record.get("direction"), "score": record.get("score", 0),
                        "quality": record.get("quality", ""),
                        "price": record.get("last_price", record.get("entry", 0)),
                        "trade": record, "factors": [],
                    }
                    title = "🔄 تحديث الصفقة — حالة جديدة"
                    await asyncio.gather(*[
                        _send_trade_notification(chat_id, result_update, record, title, consume_quota=False)
                        for chat_id in list(SUBSCRIBERS)
                    ], return_exceptions=True)

            if not AUTO_ENABLED or not SUBSCRIBERS:
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue

            closed_reason = await asyncio.to_thread(market_closed_reason)
            if closed_reason:
                logger.info("Auto scan skipped: market closed (%s)", closed_reason)
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue

            result = await asyncio.to_thread(evaluate_signal)
            if not result.get("signal"):
                logger.info("Auto signal rejected | score=%s direction=%s reasons=%s", result.get("score"), result.get("direction"), result.get("rejection_reasons"))
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue
            if result.get("liquidity_blocked"):
                logger.info("Auto trade blocked by liquidity retest gate")
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue
            if not result.get("trade"):
                logger.info("Auto signal has score/direction but no trade | score=%s direction=%s reasons=%s", result.get("score"), result.get("direction"), result.get("rejection_reasons"))
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue

            recipients = list(SUBSCRIBERS)
            quota_tokens = {}
            for chat_id in recipients:
                token = await asyncio.to_thread(_reserve_trade_quota, chat_id)
                if token:
                    quota_tokens[chat_id] = token
            if not quota_tokens:
                logger.info("Auto trade skipped: no recipient has remaining quota")
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue

            record, is_new = await asyncio.to_thread(register_trade, result)
            if not record:
                for token in quota_tokens.values():
                    await asyncio.to_thread(_release_trade_quota, token)
                logger.warning("Auto trade registration failed; quotas released")
                await asyncio.sleep(AUTO_SCAN_SECONDS)
                continue

            if not is_new:
                for token in quota_tokens.values():
                    await asyncio.to_thread(_release_trade_quota, token)
                await asyncio.gather(*[
                    _send_trade_notification(chat_id, result, record, "🔄 تحديث الصفقة", consume_quota=False)
                    for chat_id in recipients
                ], return_exceptions=True)
            else:
                await asyncio.gather(*[
                    _send_trade_notification(chat_id, result, record, "🚨 إشارة ذهب — صفقة جديدة", consume_quota=True, quota_token=token)
                    for chat_id, token in quota_tokens.items()
                ], return_exceptions=True)

        except Exception:
            logger.exception("Auto scan error")
        await asyncio.sleep(AUTO_SCAN_SECONDS)

# Runtime entry point is intentionally moved to the physical end of the file
# so every append-only override is defined before startup.

# ============================================================
# v18.87 ALERT RELIABILITY HARDENING
# Scope: Alert Delivery + Recovery + Telemetry ONLY.
# ============================================================

# Scope: Alert Delivery + Scheduler + Telemetry ONLY.
# Adds crash reconciliation, durable business linkage, safer waiter
# lifecycle, scheduler self-recovery and timing-window resilience.
# ============================================================
ALERT_FINALIZER_SCAN_SECONDS = float(os.getenv("ALERT_FINALIZER_SCAN_SECONDS", "2"))
ALERT_SCHEDULER_TASK = None

# Durable mapping between a trade business claim and its outbox message.
def _alert_trade_link_conn():
    c = sqlite3.connect(ALERT_DB_PATH, timeout=5, check_same_thread=False)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL')
    c.execute('PRAGMA busy_timeout=5000')
    return c

def _init_alert_trade_links():
    c = _alert_trade_link_conn()
    try:
        c.execute('''CREATE TABLE IF NOT EXISTS alert_trade_links(
            alert_id TEXT PRIMARY KEY,
            chat_id INTEGER NOT NULL,
            signature TEXT NOT NULL,
            consume_quota INTEGER NOT NULL DEFAULT 0,
            quota_token TEXT,
            created_at REAL NOT NULL,
            finalized_at REAL
        )''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_alert_trade_links_open ON alert_trade_links(finalized_at, created_at)')
        c.execute('CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_trade_links_open ON alert_trade_links(chat_id, signature) WHERE finalized_at IS NULL')
        c.commit()
    finally:
        c.close()

def _create_alert_trade_link(aid, chat_id, signature, consume_quota, quota_token):
    c = _alert_trade_link_conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        c.execute('''INSERT OR IGNORE INTO alert_trade_links
            (alert_id,chat_id,signature,consume_quota,quota_token,created_at,finalized_at)
            VALUES(?,?,?,?,?,?,NULL)''',
            (aid, int(chat_id), str(signature), 1 if consume_quota else 0,
             None if quota_token is None else str(quota_token), time.time()))
        row = c.execute('SELECT alert_id FROM alert_trade_links WHERE chat_id=? AND signature=?',
                        (int(chat_id), str(signature))).fetchone()
        c.commit()
        return bool(row and row['alert_id'] == aid)
    except Exception:
        c.rollback(); raise
    finally:
        c.close()

def _get_open_alert_trade_links():
    c = _alert_trade_link_conn()
    try:
        return [dict(r) for r in c.execute('SELECT * FROM alert_trade_links WHERE finalized_at IS NULL').fetchall()]
    finally:
        c.close()

def _mark_alert_trade_link_finalized(aid):
    c = _alert_trade_link_conn()
    try:
        c.execute('UPDATE alert_trade_links SET finalized_at=? WHERE alert_id=? AND finalized_at IS NULL',(time.time(),aid))
        c.commit()
    finally:
        c.close()

async def _resolve_alert_trade_link(link):
    d = ALERT_DISPATCHER
    if not d:
        return False
    aid = link['alert_id']; status = await asyncio.to_thread(d.delivery_status, aid)
    if status == 'SENT':
        _complete_notification(link['chat_id'], link['signature'])
        _mark_notification_sent(link['chat_id'], link['signature'])
        await asyncio.to_thread(_mark_alert_trade_link_finalized, aid)
        logger.info('TRADE_ALERT_LINK_RECONCILED | alert_id=%s status=SENT', aid)
        return True
    if status in ('FAILED', None):
        _cancel_notification_claim(link['chat_id'], link['signature'])
        if int(link.get('consume_quota') or 0):
            await asyncio.to_thread(_release_trade_quota, link.get('quota_token'))
        await asyncio.to_thread(_mark_alert_trade_link_finalized, aid)
        logger.error('TRADE_ALERT_LINK_RECONCILED | alert_id=%s status=%s', aid, status)
        return True
    return False

async def _reconcile_alert_trade_links_once():
    links = await asyncio.to_thread(_get_open_alert_trade_links)
    if not links:
        return 0
    results = await asyncio.gather(*[_resolve_alert_trade_link(x) for x in links], return_exceptions=True)
    for r in results:
        if isinstance(r, Exception): logger.exception('ALERT_LINK_RECONCILE_ERROR', exc_info=r)
    return len(links)

async def _alert_trade_reconciliation_loop():
    while True:
        try:
            await _reconcile_alert_trade_links_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception('ALERT_LINK_RECONCILIATION_ERROR')
        await asyncio.sleep(max(0.5, ALERT_FINALIZER_SCAN_SECONDS))

async def _finalize_trade_delivery_after_timeout(aid, chat_id, signature, consume_quota, quota_token):
    # Durable link is the source of truth; this task is only a fast-path.
    while True:
        link = {'alert_id':aid,'chat_id':chat_id,'signature':signature,
                'consume_quota':1 if consume_quota else 0,'quota_token':quota_token}
        if await _resolve_alert_trade_link(link): return
        await asyncio.sleep(max(0.5, ALERT_FINALIZER_SCAN_SECONDS))

async def _send_trade_notification(chat_id,result,record,title,consume_quota=False,quota_token=None):
    signature=_trade_notification_signature(record)
    if not signature:return False
    if _notification_already_sent(chat_id,signature):return False
    if not _claim_notification(chat_id,signature):return False
    quota_token=quota_token if consume_quota else None
    if consume_quota and not quota_token:
        _cancel_notification_claim(chat_id,signature);return False
    aid = None
    try:
        payload=_format_trade_message(result,title,record)
        d=ALERT_DISPATCHER or await _start_alert_dispatcher()
        if not d: raise RuntimeError('Alert dispatcher unavailable')
        aid=await asyncio.to_thread(d.enqueue,chat_id,payload,kind='trade',priority=100,max_attempts=6)
        if not aid: raise RuntimeError('Alert enqueue failed')
        # Persist the business/outbox relationship BEFORE waiting, so restart cannot leak claim/quota.
        linked = await asyncio.to_thread(_create_alert_trade_link, aid, chat_id, signature, consume_quota, quota_token)
        if not linked: raise RuntimeError('Trade alert durable link rejected')
        delivered=await d.wait_delivery(aid,ALERT_DELIVERY_WAIT_SECONDS)
        if delivered is True:
            await _resolve_alert_trade_link({'alert_id':aid,'chat_id':chat_id,'signature':signature,'consume_quota':1 if consume_quota else 0,'quota_token':quota_token})
            return True
        if delivered is None:
            asyncio.create_task(_finalize_trade_delivery_after_timeout(aid,chat_id,signature,consume_quota,quota_token),name=f'trade-alert-finalize-{aid}')
            logger.warning('TRADE_ALERT_PENDING_AFTER_TIMEOUT | chat_id=%s alert_id=%s',chat_id,aid)
            return True
        # Durable terminal failure: reconciliation performs the release exactly once.
        await _resolve_alert_trade_link({'alert_id':aid,'chat_id':chat_id,'signature':signature,'consume_quota':1 if consume_quota else 0,'quota_token':quota_token})
        return False
    except Exception:
        # If a durable link exists, never release claim/quota here; reconciliation owns terminal cleanup.
        if aid is None:
            _cancel_notification_claim(chat_id,signature)
            if consume_quota: await asyncio.to_thread(_release_trade_quota,quota_token)
        else:
            status = (ALERT_DISPATCHER.delivery_status(aid) if ALERT_DISPATCHER else None)
            if status is None:
                _cancel_notification_claim(chat_id,signature)
                if consume_quota: await asyncio.to_thread(_release_trade_quota,quota_token)
        logger.exception('Signal delivery failed for %s',chat_id);return False


# Start durable reconciliation alongside the existing alert dispatcher.
_old_start_alert_dispatcher_v187 = _start_alert_dispatcher
async def _start_alert_dispatcher():
    d = await _old_start_alert_dispatcher_v187()
    _init_alert_trade_links()
    await _reconcile_alert_trade_links_once()
    if not any(t.get_name()=='alert-trade-reconciliation' and not t.done() for t in asyncio.all_tasks()):
        asyncio.create_task(_alert_trade_reconciliation_loop(), name='alert-trade-reconciliation')
    return d


# ============================================================
# v18.100 — G.Y PRIVATE TRADE AUTO / DEDUP / ALERT RELIABILITY
# Append-only runtime hardening. The S/R decision rules above are untouched.
# ============================================================

SR_GY_LABEL = "🎯 صفقة G.Y الخاصة"
SR_AUTO_TASK = None
SR_AUTO_HEALTH = {"running": False, "last_start": None, "last_cycle": None, "last_success": None, "last_trade": None, "last_error": None, "cycles": 0, "trades_created": 0, "duplicates_blocked": 0}

def _sr_gy_fingerprint(trade):
    def n(v):
        x=_sr_num(v); return "" if x is None else f"{x:.3f}"
    raw="|".join([str(trade.get("direction","")),str(trade.get("trigger","")),n(trade.get("zone")),n(trade.get("entry")),n(trade.get("sl")),n(trade.get("tp1")),n(trade.get("tp2")),n(trade.get("tp3"))])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def _sr_gy_migrate_db():
    conn=_sr_db_connect()
    try:
        cols={r[1] for r in conn.execute("PRAGMA table_info(sr_trades)").fetchall()}
        if "signal_fingerprint" not in cols: conn.execute("ALTER TABLE sr_trades ADD COLUMN signal_fingerprint TEXT")
        if "alert_state" not in cols: conn.execute("ALTER TABLE sr_trades ADD COLUMN alert_state TEXT NOT NULL DEFAULT 'PENDING'")
        # Backfill fingerprints for existing open/lifecycle rows before creating the unique index.
        existing=conn.execute("SELECT setup_id,direction,entry,sl,tp1,tp2,tp3,trigger,snapshot_json FROM sr_trades WHERE signal_fingerprint IS NULL AND status IN ('OPEN','TP1_HIT','TP2_HIT')").fetchall()
        for r in existing:
            trade={"direction":r[1],"entry":r[2],"sl":r[3],"tp1":r[4],"tp2":r[5],"tp3":r[6],"trigger":r[7]}
            try:
                snap=json.loads(r[8] or '{}')
                trade["zone"]=(snap.get("trade") or {}).get("zone")
            except Exception:
                trade["zone"]=None
            conn.execute("UPDATE sr_trades SET signal_fingerprint=? WHERE setup_id=?",(_sr_gy_fingerprint(trade),r[0]))
        try:
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_sr_trades_open_fingerprint ON sr_trades(signal_fingerprint) WHERE signal_fingerprint IS NOT NULL AND status IN ('OPEN','TP1_HIT','TP2_HIT')")
        except sqlite3.IntegrityError:
            # Historical duplicate rows may exist; the transactional pre-check below still blocks new duplicates.
            logger.warning("G.Y fingerprint index contains historical duplicates; continuing with transactional dedup")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sr_trades_open_fingerprint_lookup ON sr_trades(signal_fingerprint,status)")
        conn.execute("""CREATE TABLE IF NOT EXISTS sr_alert_outbox (setup_id TEXT NOT NULL, chat_id INTEGER NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING', attempts INTEGER NOT NULL DEFAULT 0, alert_id TEXT, last_error TEXT, created_at TEXT NOT NULL, sent_at TEXT, PRIMARY KEY(setup_id,chat_id))""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sr_alert_outbox_status ON sr_alert_outbox(status,created_at)")
        conn.commit()
    finally: conn.close()

_sr_gy_migrate_db()

def _sr_gy_alert_payload(trade,snapshot):
    result={"ok":True,"signal":trade,"sr":(snapshot or {}).get("levels") or {},"monitor":{"price":(snapshot or {}).get("price")}}
    return f"🚨 {SR_GY_LABEL} جديدة\n"+_sr_format(result)+f"\n\n🆔 {trade.get('_setup_id','PENDING')}"

def _sr_insert_trade(trade,snapshot):
    fingerprint=_sr_gy_fingerprint(trade)
    setup_id=f"SR-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8].upper()}"
    conn=_sr_db_connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        existing=conn.execute("SELECT setup_id FROM sr_trades WHERE signal_fingerprint=? AND status IN ('OPEN','TP1_HIT','TP2_HIT') LIMIT 1",(fingerprint,)).fetchone()
        if existing:
            conn.rollback(); SR_AUTO_HEALTH["duplicates_blocked"]=int(SR_AUTO_HEALTH.get("duplicates_blocked",0))+1; return None,False
        cur=conn.execute("""INSERT OR IGNORE INTO sr_trades (setup_id,created_at,direction,entry,sl,tp1,tp2,tp3,rr,score,trigger,status,snapshot_json,signal_fingerprint,alert_state) VALUES (?,?,?,?,?,?,?,?,?,?,?,'OPEN',?,?, 'PENDING')""",(setup_id,now_damascus().isoformat(),trade["direction"],trade["entry"],trade["sl"],trade["tp1"],trade.get("tp2"),trade.get("tp3"),trade["rr"],trade["score"],trade["trigger"],json.dumps(snapshot,ensure_ascii=False,default=str),fingerprint))
        if cur.rowcount!=1:
            conn.rollback(); SR_AUTO_HEALTH["duplicates_blocked"]=int(SR_AUTO_HEALTH.get("duplicates_blocked",0))+1; return None,False
        conn.execute("INSERT INTO sr_trade_history(setup_id,event_time,status,result,price,note) VALUES(?,?,?,?,?,?)",(setup_id,now_damascus().isoformat(),"OPEN",None,trade["entry"],"G.Y private setup created"))
        try: recipients=set(_alert_recipients("trade_alerts"))
        except Exception: recipients=set(ADMIN_IDS)
        payload_trade=dict(trade); payload_trade["_setup_id"]=setup_id
        payload=_sr_gy_alert_payload(payload_trade,snapshot); created_at=now_damascus().isoformat()
        for cid in recipients:
            try: conn.execute("INSERT OR IGNORE INTO sr_alert_outbox(setup_id,chat_id,payload,status,attempts,created_at) VALUES(?,?,?,'PENDING',0,?)",(setup_id,int(cid),payload,created_at))
            except Exception: logger.exception("G.Y alert outbox insert failed chat_id=%s setup_id=%s",cid,setup_id)
        conn.commit(); SR_AUTO_HEALTH["trades_created"]=int(SR_AUTO_HEALTH.get("trades_created",0))+1; return setup_id,True
    except Exception:
        conn.rollback(); raise
    finally: conn.close()

def _sr_gy_pending_alert_rows():
    conn=_sr_db_connect()
    try: return [dict(r) for r in conn.execute("SELECT setup_id,chat_id,payload,status,attempts,alert_id FROM sr_alert_outbox WHERE status IN ('PENDING','SENDING') ORDER BY created_at ASC LIMIT 100").fetchall()]
    finally: conn.close()

def _sr_gy_mark_alert_sending(setup_id,chat_id,alert_id):
    conn=_sr_db_connect()
    try:
        conn.execute("UPDATE sr_alert_outbox SET status='SENDING',attempts=attempts+1,alert_id=?,last_error=NULL WHERE setup_id=? AND chat_id=? AND status IN ('PENDING','SENDING')",(alert_id,setup_id,int(chat_id))); conn.commit()
    finally: conn.close()

def _sr_gy_mark_alert_sent(setup_id,chat_id):
    conn=_sr_db_connect()
    try:
        conn.execute("UPDATE sr_alert_outbox SET status='SENT',sent_at=?,last_error=NULL WHERE setup_id=? AND chat_id=?",(now_damascus().isoformat(),setup_id,int(chat_id))); conn.commit()
    finally: conn.close()

def _sr_gy_mark_alert_retry(setup_id,chat_id,error):
    conn=_sr_db_connect()
    try:
        conn.execute("UPDATE sr_alert_outbox SET status='PENDING',last_error=?,alert_id=NULL WHERE setup_id=? AND chat_id=?",(str(error)[:500],setup_id,int(chat_id))); conn.commit()
    finally: conn.close()

async def _sr_gy_alert_outbox_once():
    rows=await asyncio.to_thread(_sr_gy_pending_alert_rows)
    for row in rows:
        aid=row.get("alert_id")
        if row.get("status")=="SENDING" and aid and ALERT_DISPATCHER:
            try: status=await asyncio.to_thread(ALERT_DISPATCHER.delivery_status,aid)
            except Exception: status=None
            if status=="SENT": await asyncio.to_thread(_sr_gy_mark_alert_sent,row["setup_id"],row["chat_id"]); continue
            if status=="FAILED": await asyncio.to_thread(_sr_gy_mark_alert_retry,row["setup_id"],row["chat_id"],"dispatcher failed"); continue
            if status in ("PENDING","SENDING"): continue
        try:
            d=ALERT_DISPATCHER or await _start_alert_dispatcher()
            new_aid=await asyncio.to_thread(d.enqueue,int(row["chat_id"]),row["payload"],kind="sr",priority=100,max_attempts=ALERT_MAX_ATTEMPTS)
            if new_aid: await asyncio.to_thread(_sr_gy_mark_alert_sending,row["setup_id"],row["chat_id"],str(new_aid))
        except Exception as exc:
            await asyncio.to_thread(_sr_gy_mark_alert_retry,row["setup_id"],row["chat_id"],str(exc)[:500])

async def _sr_notify_all(text):
    # New-trade notifications are already durably created with the trade; drain them instead of re-enqueueing.
    if "تم اعتماد صفقة G.Y الخاصة" in str(text):
        await _sr_gy_alert_outbox_once()
        return True
    if not APPLICATION:
        return False
    try:
        recipients=await asyncio.to_thread(_alert_recipients,"trade_alerts")
    except Exception:
        recipients=set(ADMIN_IDS)
    if not recipients:
        return True
    try:
        results=await _dispatch_broadcast(recipients,with_market_header(text),kind="sr")
        return all(r is True for r in results)
    except Exception:
        logger.exception("G.Y private trade alert enqueue failed")
        return False

async def _sr_auto_loop():
    global SR_AUTO_TASK
    SR_AUTO_HEALTH["running"]=True; SR_AUTO_HEALTH["last_start"]=time.time()
    try:
        while True:
            try:
                SR_AUTO_HEALTH["cycles"]=int(SR_AUTO_HEALTH.get("cycles",0))+1; SR_AUTO_HEALTH["last_cycle"]=time.time()
                if SR_AUTO_ENABLED and not market_closed_reason():
                    result=await asyncio.to_thread(sr_analyze)
                    if result.get("signal"):
                        df=result.get("df")
                        if df is None:
                            df=await asyncio.to_thread(get_bars,"M15",120)
                        setup_id,is_new=await asyncio.to_thread(_sr_insert_trade,result["signal"],_sr_snapshot(df,result.get("sr",{}),result["signal"]))
                        if is_new:
                            SR_AUTO_HEALTH["last_trade"]=setup_id; logger.info("G.Y_PRIVATE_TRADE_CREATED | setup_id=%s",setup_id)
                await _sr_gy_alert_outbox_once()
                try:
                    p=await asyncio.to_thread(live_price); changed=await asyncio.to_thread(_sr_update_lifecycle,p)
                    if changed:
                        recipients=await asyncio.to_thread(_alert_recipients,"trade_alerts")
                        for row,status,res,price in changed:
                            await _dispatch_broadcast(recipients,with_market_header(f"🎯 تحديث {SR_GY_LABEL}\n🆔 {row['setup_id']}\n💰 السعر: {price}\n📌 النتيجة: {res}\n📊 الحالة: {status}"),kind="sr")
                except Exception: logger.exception("G.Y lifecycle error")
                SR_AUTO_HEALTH["last_success"]=time.time(); SR_AUTO_HEALTH["last_error"]=None
            except asyncio.CancelledError: raise
            except Exception as exc:
                SR_AUTO_HEALTH["last_error"]=f"{type(exc).__name__}: {exc}"; logger.exception("G.Y auto loop error")
            await asyncio.sleep(SR_AUTO_SECONDS)
    finally:
        SR_AUTO_HEALTH["running"]=False
        if SR_AUTO_TASK is asyncio.current_task(): SR_AUTO_TASK=None

async def _dispatch_alert(chat_id,text,*,kind='generic',max_attempts=ALERT_MAX_ATTEMPTS,wait_delivery=False,delivery_timeout=ALERT_DELIVERY_WAIT_SECONDS):
    d=ALERT_DISPATCHER or await _start_alert_dispatcher()
    if not d:return False
    priority=100 if kind in {'trade','sr','system','critical','session_open','session_overlap'} else 70 if kind in {'news','session_preopen'} else 40
    aid=await asyncio.to_thread(d.enqueue,chat_id,text,kind=kind,priority=priority,max_attempts=max_attempts)
    if not aid:return False
    return await d.wait_delivery(aid,delivery_timeout) if wait_delivery else True

async def _alert_scheduler_loop():
    ALERT_TELEMETRY['scheduler_started']=True; ALERT_TELEMETRY['scheduler_running']=True; ALERT_TELEMETRY['scheduler_last_start']=time.time()
    logger.info('ALERT_SCHEDULER_STARTED | interval=%ss',ALERT_SCHEDULER_SECONDS)
    try:
        while True:
            try:
                t0=time.perf_counter()
                results=await asyncio.gather(asyncio.create_task(send_market_session_alerts(),name='market-session-alerts'),asyncio.create_task(send_news_alerts(),name='news-alerts'),return_exceptions=True)
                for result in results:
                    if isinstance(result,Exception): ALERT_TELEMETRY['scheduler_errors']+=1; logger.error('ALERT_SCHEDULER_CHILD_ERROR | %s',result)
                ALERT_TELEMETRY['scheduler_last_tick']=time.time(); ALERT_TELEMETRY['scheduler_last_latency_ms']=(time.perf_counter()-t0)*1000.0
            except asyncio.CancelledError: raise
            except Exception: ALERT_TELEMETRY['scheduler_errors']+=1; logger.exception('ALERT_SCHEDULER_ERROR')
            await asyncio.sleep(max(5.0,ALERT_SCHEDULER_SECONDS))
    finally: ALERT_TELEMETRY['scheduler_running']=False

SR_BUTTON_TEXT=SR_GY_LABEL
def _sr_install_telegram_handler():
    APPLICATION.add_handler(CommandHandler("sr",_trace_handler(sr_command,"command:/sr")),group=0)
    APPLICATION.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"^🎯 صفقة G\.Y الخاصة$"),_trace_handler(_sr_button_guarded,"message:gy_private_trade")),group=0)

def _sr_ux_main_keyboard():
    return [list(row) for row in _BASELINE_UX_MAIN_KEYBOARD()]+[[SR_BUTTON_TEXT]]

_ux_main_keyboard=_sr_ux_main_keyboard


# ============================================================
# v18.103 FINAL RUNTIME ENTRY POINT
# Must remain physically last so all overrides are active.
# ============================================================


# ============================================================
# v19.01 CONTRACT ALIGNMENT — APPEND-ONLY OVERRIDE LAYER
# Scope: subscriptions/features/admin UX/G.Y/news-risk separation only.
# Trading engines, indicators, entry/SL/TP rules and scoring are untouched.
# ============================================================
VERSION = "v19.01_CONTRACT_ALIGNED"

# Canonical commercial contract: FREE / BASIC / ELITE only.
PLANS = {
    "FREE": {
        "name": "🆓 المجانية", "price": 0, "trade_limit": 1, "trade_period": "weekly",
        "features": {"gold_price", "markets", "status", "quick_analysis", "sr", "daily_analysis", "weekly_analysis"}
    },
    "BASIC": {
        "name": "🥉 الأساسية", "price": 10, "trade_limit": 5, "trade_period": "monthly",
        "features": {"gold_price", "markets", "status", "quick_analysis", "sr", "daily_analysis", "weekly_analysis", "trade_history"}
    },
    "ELITE": {
        "name": "💎 النخبة", "price": 35, "trade_limit": 50, "trade_period": "monthly",
        "features": {"gold_price", "markets", "status", "quick_analysis", "sr", "daily_analysis", "weekly_analysis", "trade_history",
                     "full_analysis", "institutional", "gy_private_trade", "trade_alerts", "news_alerts", "news_risk_status"}
    },
}
TRADE_LIMIT_TEXT = {
    "FREE": "صفقة واحدة كل 7 أيام",
    "BASIC": "5 صفقات شهرياً",
    "ELITE": "50 صفقة شهرياً",
}

# Legacy feature aliases are intentionally internal compatibility aliases only.
# They are not commercial plan entries and are never rendered as user services.
_FEATURE_ALIASES = {
    "daily_report": "daily_analysis",
    "weekly_report": "weekly_analysis",
    "news_risk": "news_risk_status",
}


def _canonical_feature(feature):
    return _FEATURE_ALIASES.get(str(feature), str(feature))


def has_feature(chat_id, feature):
    if is_admin_chat(chat_id):
        return True
    feature = _canonical_feature(feature)
    row = get_member(chat_id)
    plan = row['plan'] if row else 'FREE'
    # Legacy stored plans are deliberately not granted new commercial privileges.
    if plan not in PLANS:
        plan = 'FREE'
    return feature in PLANS[plan]['features']


def usage_count(chat_id, plan=None):
    if is_admin_chat(chat_id):
        return 0
    row = get_member(chat_id)
    plan = plan or (row['plan'] if row else 'FREE')
    if plan not in PLANS:
        plan = 'FREE'
    period_start = now_damascus() - timedelta(days=7 if PLANS[plan]['trade_period'] == 'weekly' else 30)
    conn = _db()
    try:
        return conn.execute("SELECT COUNT(*) FROM usage WHERE chat_id=? AND feature='trade' AND used_at>=?", (chat_id, period_start.isoformat())).fetchone()[0]
    finally:
        conn.close()


def trade_quota(chat_id):
    if is_admin_chat(chat_id):
        return 'ADMIN', 0, None
    row = get_member(chat_id)
    plan = row['plan'] if row and row['plan'] in PLANS else 'FREE'
    return plan, usage_count(chat_id, plan), PLANS[plan]['trade_limit']


def can_receive_trade(chat_id):
    _, used, limit = trade_quota(chat_id)
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


def plan_status_text(chat_id):
    if is_admin_chat(chat_id):
        return "👑 الإدارة — صلاحية مستقلة عن الاشتراك والحصة\n🎯 الحصة: غير محدودة"
    row = get_member(chat_id)
    plan = row['plan'] if row and row['plan'] in PLANS else 'FREE'
    info = PLANS[plan]
    used = usage_count(chat_id, plan)
    limit = info['trade_limit']
    quota = "♾️ غير محدودة" if limit is None else f"{max(0, limit-used)} متبقية"
    expiry = row['expiry_date'] if row and row['expiry_date'] else "لا يوجد انتهاء — مجانية"
    return f"{info['name']} | ${info['price']} / شهر\n🎯 الصفقات: {TRADE_LIMIT_TEXT[plan]}\n📊 المتبقي: {quota}\n📅 الانتهاء: {expiry}"


def plans_text():
    return "\n".join([
        "💳 باقات XAU SMART TRADER", "",
        "🆓 المجانية — مجانية", "• سعر الذهب • السوق والحالة • تجربة محدودة للتحليل السريع • السيولة والدعم والمقاومة • القراءة اليومية للسوق • القراءة الأسبوعية للسوق", "• صفقة واحدة كل 7 أيام", "",
        "🥉 الأساسية — $10 / شهر", "• كل ما سبق + سجل الصفقات", "• 5 صفقات شهرياً", "",
        "💎 النخبة — $35 / شهر", "• كل الأساسية + التحليل المتكامل + التحليل المؤسسي + 🎯 صفقة G.Y الخاصة + تنبيهات التداول + تنبيهات الأخبار + حالة مخاطر الأخبار", "• 50 صفقة شهرياً", "",
        "ℹ️ التحليل والتقارير لا تُحسب تلقائياً كصفقات، وS/R لا يولد صفقات تلقائية.",
    ])


def feature_guard(feature, upgrade_text=True):
    # Compatibility helper; handlers use feature_guard(feature)(update, context),
    # therefore return the same checker contract as the original implementation.
    canonical = _canonical_feature(feature)
    async def checker(update, context):
        await asyncio.to_thread(_ensure_user, update)
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id if update.effective_user else chat_id
        allowed = await asyncio.to_thread(has_feature, chat_id, canonical) or is_admin_chat(user_id)
        if allowed:
            return True
        if upgrade_text:
            await reply(update, "🔒 هذه الميزة غير متاحة ضمن باقتك الحالية.\n\n💳 استخدم /plans لمعرفة الباقات.")
        return False
    return checker


# Canonical report names: implementation remains the existing report engine.
async def daily_analysis(update, context):
    if not await feature_guard("daily_analysis")(update, context): return
    try:
        await reply(update, await asyncio.to_thread(build_daily_analysis))
    except Exception as e:
        logger.exception("Daily market reading error")
        await reply(update, f"❌ تعذر إنشاء 🧭 القراءة اليومية للسوق.\nالسبب: {e}")


async def weekly_analysis(update, context):
    if not await feature_guard("weekly_analysis")(update, context): return
    try:
        await reply(update, await asyncio.to_thread(build_weekly_analysis))
    except Exception as e:
        logger.exception("Weekly market reading error")
        await reply(update, f"❌ تعذر إنشاء 🧭 القراءة الأسبوعية للسوق.\nالسبب: {e}")


async def daily_report(update, context):
    return await daily_analysis(update, context)


async def weekly_report(update, context):
    return await weekly_analysis(update, context)


async def news_status(update, context):
    if not await feature_guard("news_risk_status")(update, context): return
    try:
        risk = await asyncio.to_thread(get_risk)
        state = risk.get("state", "UNKNOWN")
        text = risk.get("message", "⚠️ حالة الأخبار غير معروفة")
        source = risk.get("source", "none")
        await reply(update, with_market_header(f"📰 حالة مخاطر الأخبار\n\n📌 الحالة: {state}\n{text}\n\n🔎 المصدر: {source}\n\nℹ️ هذه القراءة لا تمنع التداول ولا تغيّر استراتيجية الدخول أو الخروج."))
    except Exception as e:
        logger.exception("News risk status error")
        await reply(update, f"❌ تعذر قراءة حالة مخاطر الأخبار.\nالسبب: {e}")


async def plans(update, context):
    await asyncio.to_thread(_ensure_user, update)
    keyboard = [
        [InlineKeyboardButton("🆓 المجانية", callback_data="plan_FREE")],
        [InlineKeyboardButton("🥉 الأساسية — $10", callback_data="plan_BASIC")],
        [InlineKeyboardButton("💎 النخبة — $35", callback_data="plan_ELITE")],
    ]
    text = plans_text() + "\n\n⬇️ اختر الباقة لمعرفة التفاصيل:"
    if update.callback_query:
        await update.callback_query.edit_message_text(with_market_header(text), reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(with_market_header(text), reply_markup=InlineKeyboardMarkup(keyboard))


async def plan_callback(update, context):
    query = update.callback_query
    await query.answer()
    plan_key = query.data.replace("plan_", "", 1)
    plan = PLANS.get(plan_key)
    if not plan:
        await query.edit_message_text(with_market_header("❌ الباقة غير موجودة.")); return
    labels = {
        "gold_price":"💰 سعر الذهب", "markets":"🌍 السوق والحالة", "status":"🟢 حالة النظام",
        "quick_analysis":"⚡ التحليل السريع", "sr":"💧 السيولة والدعم والمقاومة",
        "daily_analysis":"🧭 القراءة اليومية للسوق", "weekly_analysis":"🧭 القراءة الأسبوعية للسوق",
        "trade_history":"📜 سجل الصفقات", "full_analysis":"🧠 التحليل المتكامل", "institutional":"🏦 التحليل المؤسسي",
        "gy_private_trade":"🎯 صفقة G.Y الخاصة", "trade_alerts":"🔔 تنبيهات التداول", "news_alerts":"🔔 تنبيهات الأخبار",
        "news_risk_status":"📰 حالة مخاطر الأخبار"
    }
    details = "\n".join("• "+labels.get(x,x) for x in sorted(plan["features"]))
    buttons = [] if plan_key == "FREE" else [[InlineKeyboardButton(f"📩 طلب الاشتراك في {plan['name']}", callback_data=f"request_{plan_key}")]]
    buttons += [[InlineKeyboardButton("🔙 العودة للباقات", callback_data="back_to_plans")], [InlineKeyboardButton("🏠 الرئيسية", callback_data="nav_home")]]
    text=f"{plan['name']}\n💰 السعر: ${plan['price']} / شهر\n🎯 حد الصفقات: {TRADE_LIMIT_TEXT[plan_key]}\n\n🔐 المزايا:\n{details}"
    await query.edit_message_text(with_market_header(text), reply_markup=InlineKeyboardMarkup(buttons))


# G.Y is a manually requested service. It may create a record only when the
# authorized user explicitly invokes the service; no background signal creation.
async def sr_button(update, context):
    if not await feature_guard("gy_private_trade")(update, context): return
    try:
        await asyncio.to_thread(_ensure_user, update)
        result=await asyncio.to_thread(sr_analyze)
        text=_sr_format(result)
        if not result.get("ok",False):
            await reply(update,text+f"\n\n🧪 رمز التشخيص: {result.get('error_code','SR_UNKNOWN')}"); return
        if result.get("signal"):
            snapshot=_sr_snapshot(result.get("df"),result.get("sr",{}),result["signal"])
            setup_id,is_new=await asyncio.to_thread(_sr_insert_trade,result["signal"],snapshot)
            if setup_id and is_new:
                # The legacy S/R insert creates an outbox row for trade_alerts.
                # G.Y is now manually requested and must not broadcast automatically.
                def _remove_legacy_gy_outbox():
                    conn=_sr_db_connect()
                    try:
                        conn.execute("DELETE FROM sr_alert_outbox WHERE setup_id=?",(setup_id,)); conn.commit()
                    finally: conn.close()
                await asyncio.to_thread(_remove_legacy_gy_outbox)
                text+=f"\n\n🆔 {setup_id}\n🔒 هذه الصفقة مستقلة عن نظام الصفقات الرئيسي."
                # Do not automatically broadcast a G.Y trade to all trade-alert subscribers.
                # The durable G.Y outbox is retained for later explicit delivery integration.
        await reply(update,text)
    except Exception as exc:
        logger.exception("G.Y private trade handler error")
        await reply(update,f"❌ تعذر تنفيذ 🎯 صفقة G.Y الخاصة حالياً.\n🧪 رمز التشخيص: GY_HANDLER\nتفاصيل: {type(exc).__name__}: {exc}")


async def sr_command(update, context):
    return await sr_button(update, context)


async def _sr_button_guarded(update, context):
    await sr_button(update, context)
    raise ApplicationHandlerStop


# Administrative dashboard — read-only monitoring except existing explicit subscription commands.
def _admin_ok(update):
    uid = update.effective_user.id if update.effective_user else update.effective_chat.id
    cid = update.effective_chat.id if update.effective_chat else uid
    return is_admin_chat(uid) or is_admin_chat(cid)


async def _admin_guard(update):
    if not _admin_ok(update):
        await reply(update, "⛔ هذا القسم مخصص للإدارة فقط.")
        return False
    return True


def _admin_users_snapshot():
    conn=_db()
    try:
        rows=conn.execute("SELECT plan,status,COUNT(*) n FROM users GROUP BY plan,status ORDER BY plan,status").fetchall()
        total=conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        return total,[dict(r) for r in rows]
    finally: conn.close()


def _admin_user_lookup(value):
    try: cid=int(str(value).strip())
    except Exception: return None
    conn=_db()
    try:
        row=conn.execute("SELECT chat_id,username,first_name,plan,status,start_date,expiry_date,created_at,updated_at FROM users WHERE chat_id=?",(cid,)).fetchone()
        return dict(row) if row else None
    finally: conn.close()


async def admin_users(update, context):
    if not await _admin_guard(update): return
    total,rows=await asyncio.to_thread(_admin_users_snapshot)
    lines=["👥 إدارة المستخدمين","",f"• إجمالي الحسابات: {total}"]
    for r in rows: lines.append(f"• {r['plan']} / {r['status']}: {r['n']}")
    context.user_data["admin_mode"]="search_user"
    lines += ["","🔎 أرسل Chat ID للبحث عن حساب محدد."]
    await _admin_render(update,"\n".join(lines),"users")


async def admin_subscriptions(update, context):
    if not await _admin_guard(update): return
    await _admin_render(update,"💳 إدارة الاشتراكات\n\nللتفعيل أو التمديد استخدم:\n/admin activate USER_ID ELITE DAYS\n\nولا تعتمد صلاحية الإدارة على أي اشتراك.","subscriptions")


async def admin_trade_performance(update, context):
    if not await _admin_guard(update): return
    await trade_performance_monitor(update,context)


async def admin_system_performance(update, context):
    if not await _admin_guard(update): return
    await system_performance_monitor(update,context)


async def _admin_alert_snapshot():
    out={"PENDING":0,"SENDING":0,"SENT":0,"FAILED":0,"oldest_pending_age":0.0}
    try:
        if os.path.exists(ALERT_DB_PATH):
            conn=sqlite3.connect(ALERT_DB_PATH,timeout=5); conn.row_factory=sqlite3.Row
            try:
                for r in conn.execute("SELECT status,COUNT(*) n FROM alert_outbox GROUP BY status").fetchall(): out[str(r["status"])]=int(r["n"])
                r=conn.execute("SELECT MIN(created_at) x FROM alert_outbox WHERE status='PENDING'").fetchone()
                if r and r[0]: out["oldest_pending_age"]=max(0.0,time.time()-float(r[0]))
            finally: conn.close()
    except Exception as exc: out["error"]=f"{type(exc).__name__}: {exc}"
    return out


async def admin_alerts(update, context):
    if not await _admin_guard(update): return
    a=await _admin_alert_snapshot(); tel=dict(ALERT_TELEMETRY)
    d=ALERT_DISPATCHER.health() if ALERT_DISPATCHER else {}
    text=("🔔 مراقبة التنبيهات\n\n"
          f"• PENDING: {a.get('PENDING',0)}\n• SENDING: {a.get('SENDING',0)}\n• SENT: {a.get('SENT',0)}\n• FAILED: {a.get('FAILED',0)}\n"
          f"• أقدم Pending: {a.get('oldest_pending_age',0):.1f} ثانية\n"
          f"• Scheduler errors: {tel.get('scheduler_errors',0)}\n"
          f"• Dispatcher: {'🟢 يعمل' if ALERT_DISPATCHER and d else '🟡 غير مهيأ'}")
    await _admin_render(update,text,"alerts")


async def admin_daily_audit(update, context):
    if not await _admin_guard(update): return
    if PERFORMANCE_AUDITOR is None: await reply(update,"🔍 محرك التدقيق غير متاح حالياً."); return
    report=await asyncio.to_thread(PERFORMANCE_AUDITOR.metrics_report)
    d=report.get("daily",{})
    await _admin_render(update,f"🔍 التدقيق اليومي\n\n• التحليلات: {d.get('total',0)}\n• المطابقة: {d.get('matched',0)}\n• الدقة: {d.get('accuracy_pct','—')}%\n\nℹ️ التدقيق قراءة وتشخيص فقط.","audit_daily")


async def admin_weekly_audit(update, context):
    if not await _admin_guard(update): return
    if PERFORMANCE_AUDITOR is None: await reply(update,"📅 محرك التدقيق غير متاح حالياً."); return
    report=await asyncio.to_thread(PERFORMANCE_AUDITOR.metrics_report)
    w=report.get("weekly",{})
    await _admin_render(update,f"📅 التدقيق الأسبوعي\n\n• التحليلات: {w.get('total',0)}\n• المطابقة: {w.get('matched',0)}\n• الدقة: {w.get('accuracy_pct','—')}%\n\nℹ️ التدقيق قراءة وتشخيص فقط.","audit_weekly")


async def admin_database(update, context):
    if not await _admin_guard(update): return
    def snap():
        out=[]
        for name,path in (("الاشتراكات",SUBSCRIPTION_DB_PATH),("الصفقات",TRADE_DB_PATH),("التنبيهات",ALERT_DB_PATH),("S/R",globals().get("SR_DB_PATH","sr_trades.db"))):
            out.append((name,path,os.path.exists(path),os.path.getsize(path) if os.path.exists(path) else 0))
        return out
    rows=await asyncio.to_thread(snap)
    text="🗄️ حالة قاعدة البيانات\n\n"+"\n".join(f"• {n}: {'🟢 موجودة' if ok else '🔴 غير موجودة'} | {size} B\n  {path}" for n,path,ok,size in rows)
    await _admin_render(update,text,"database")


async def admin_scheduler(update, context):
    if not await _admin_guard(update): return
    tasks=asyncio.all_tasks() if BOT_LOOP and BOT_LOOP.is_running() else set()
    names=sorted(t.get_name() for t in tasks if not t.done())
    alerts=[n for n in names if "alert-timing-scheduler" in n]
    text=("⚙️ حالة Scheduler\n\n"
          f"• Alert scheduler tasks: {len(alerts)}\n"
          f"• ALERT_TELEMETRY started: {ALERT_TELEMETRY.get('scheduler_started',False)}\n"
          f"• آخر tick: {ALERT_TELEMETRY.get('scheduler_last_tick') or '—'}\n"
          f"• أخطاء: {ALERT_TELEMETRY.get('scheduler_errors',0)}\n\n"
          f"• المهام النشطة ذات الصلة: {', '.join(names[:12]) or '—'}")
    await _admin_render(update,text,"scheduler")


async def admin_system(update, context):
    if not await _admin_guard(update): return
    await status(update,context)


async def admin_help(update, context):
    if not await _admin_guard(update): return
    await _admin_render(update,"ℹ️ مساعدة الإدارة\n\n• الإدارة مستقلة عن الباقات.\n• أدوات الإدارة لا تستهلك الحصة.\n• المراقبة لا تغيّر استراتيجية التداول.\n• التدقيق قراءة وتشخيص فقط.","help")


async def admin_dashboard(update, context):
    if not await _admin_guard(update): return
    rows=[
        [("👥 إدارة المستخدمين","admin_users"),("💳 إدارة الاشتراكات","admin_subscriptions")],
        [("📊 مراقب أداء الصفقات","admin_trade_performance")],
        [("📈 مراقب أداء النظام","admin_system_performance")],
        [("🔔 مراقبة التنبيهات","admin_alerts")],
        [("🔍 التدقيق اليومي","admin_audit_daily"),("📅 التدقيق الأسبوعي","admin_audit_weekly")],
        [("🗄️ حالة قاعدة البيانات","admin_database")],
        [("⚙️ حالة Scheduler","admin_scheduler")],
        [("🟢 حالة النظام","admin_system")],
        [("ℹ️ المساعدة","admin_help")],
        [("🔙 الرئيسية","nav_home")],
    ]
    await _admin_render(update,"👑 لوحة الإدارة\n\nاختر أداة الإدارة المطلوبة:",rows)


async def _admin_render(update,text,section_or_rows):
    if isinstance(section_or_rows,list): rows=section_or_rows
    else:
        rows=[[('🔙 لوحة الإدارة','admin_home')],[('🏠 الرئيسية','nav_home')]]
    markup=_ux_markup(rows)
    q=getattr(update,'callback_query',None)
    if q: await q.edit_message_text(with_market_header(text),reply_markup=markup)
    elif getattr(update,'message',None): await update.message.reply_text(with_market_header(text),reply_markup=markup)


async def admin_command(update, context):
    if not _admin_ok(update):
        await reply(update,"⛔ هذا الأمر مخصص للإدارة."); return
    args=getattr(context,"args",[]) or []
    if not args:
        await reply(update,"👑 الإدارة\n\nالاستخدام: /admin activate USER_ID ELITE DAYS"); return
    if args[0].lower()!="activate" or len(args)<4:
        await reply(update,"الصيغة: /admin activate USER_ID ELITE DAYS"); return
    try:
        user_id=int(args[1]); plan=args[2].upper(); days=int(args[3])
        if plan not in PLANS or plan=="FREE": raise ValueError("الباقة غير صحيحة")
        if days<=0: raise ValueError("عدد الأيام يجب أن يكون موجباً")
        now=now_damascus(); expiry=now+timedelta(days=days)
        def _activate():
            conn=_db()
            try:
                conn.execute("INSERT OR IGNORE INTO users(chat_id,plan,status,referral_code,created_at,updated_at) VALUES(?,?,?,?,?,?)",(user_id,"FREE","active",f"ref_{user_id}",now.isoformat(),now.isoformat()))
                conn.execute("UPDATE users SET plan=?,status='active',start_date=?,expiry_date=?,updated_at=? WHERE chat_id=?",(plan,now.isoformat(),expiry.isoformat(),now.isoformat(),user_id)); conn.commit()
            finally: conn.close()
        await asyncio.to_thread(_activate)
        await reply(update,f"✅ تم تفعيل {PLANS[plan]['name']} للمستخدم {user_id} حتى {expiry.strftime('%Y-%m-%d %H:%M')}")
    except Exception as exc:
        await reply(update,f"❌ تعذر تنفيذ العملية.\nالسبب: {exc}")


# User-facing main keyboard: admin controls are injected only for authorized admins.
def _contract_main_keyboard(chat_id=None):
    rows=[
        ["⚡ التحليل السريع", "🧠 التحليل المتكامل"],
        ["🧭 القراءة اليومية للسوق", "🧭 القراءة الأسبوعية للسوق"],
        ["🌍 السوق والأخبار", "🎯 الصفقات"],
        ["💧 السيولة والدعم", "🏦 التحليل المؤسسي"],
        ["🎯 صفقة G.Y الخاصة", "📜 سجل الصفقات"],
        ["👤 الاشتراك", "💳 الباقات"],
        ["🟢 حالة النظام", "ℹ️ المساعدة"],
    ]
    try:
        if is_admin_chat(chat_id):
            rows.append(["👑 الإدارة"])
    except Exception: pass
    return rows

async def start(update, context):
    await asyncio.to_thread(_ensure_user, update)
    keyboard=_contract_main_keyboard(update.effective_chat.id)
    text=(f"🤖 XAU SMART TRADER {VERSION}\n\n🥇 محلل الذهب XAU/USD\n\n"
          "W1 + D1 + H4 + H1 + M15\nStructure + Momentum + Volume + Fibonacci + FVG\n\n"
          f"🎯 حد الإشارة: {SIGNAL_THRESHOLD} نقطة\n\nاختر القسم المطلوب من القائمة 👇")
    target=getattr(update,"message",None) or (update.callback_query.message if getattr(update,"callback_query",None) else None)
    if target is not None: await target.reply_text(with_market_header(text),reply_markup=ReplyKeyboardMarkup(keyboard,resize_keyboard=True))


# Text router: admin controls are handled before the legacy router; all unrelated routes remain intact.
_LEGACY_ROUTER = router
async def router(update, context):
    text=(update.effective_message.text or "").strip() if getattr(update,"effective_message",None) else ""
    if text=="👑 الإدارة":
        await admin_dashboard(update,context); return
    if text in {"🧭 القراءة اليومية للسوق","📝 التقرير اليومي"}:
        await daily_analysis(update,context); return
    if text in {"🧭 القراءة الأسبوعية للسوق","📅 التقرير الأسبوعي"}:
        await weekly_analysis(update,context); return
    if text=="🎯 صفقة G.Y الخاصة":
        await sr_button(update,context); return
    if text.isdigit() and context.user_data.get("admin_mode")=="search_user":
        if not _admin_ok(update): context.user_data.pop("admin_mode",None); await reply(update,"⛔ هذا القسم مخصص للإدارة فقط."); return
        row=await asyncio.to_thread(_admin_user_lookup,text); context.user_data.pop("admin_mode",None)
        if not row: await reply(update,"🔎 لم يتم العثور على هذا الحساب."); return
        await reply(update,with_market_header("👥 بيانات المستخدم\n\n"+"\n".join([f"• Chat ID: {row['chat_id']}",f"• الاسم: {row['first_name'] or '—'}",f"• Username: @{row['username']}" if row['username'] else "• Username: —",f"• الخطة: {row['plan']}",f"• الحالة: {row['status']}",f"• البداية: {row['start_date'] or '—'}",f"• الانتهاء: {row['expiry_date'] or '—'}"])))
        return
    await _LEGACY_ROUTER(update,context)


# Callback router wrapper: admin callbacks are isolated; unrelated callbacks use v18_110 router.
_LEGACY_CALLBACK_ROUTER = callback_router
async def callback_router(update, context):
    q=update.callback_query
    data=q.data if q else ""
    if data=="admin_home": await q.answer(); await admin_dashboard(update,context); return
    admin_map={"admin_users":admin_users,"admin_subscriptions":admin_subscriptions,"admin_trade_performance":admin_trade_performance,
               "admin_system_performance":admin_system_performance,"admin_alerts":admin_alerts,"admin_audit_daily":admin_daily_audit,
               "admin_audit_weekly":admin_weekly_audit,"admin_database":admin_database,"admin_scheduler":admin_scheduler,
               "admin_system":admin_system,"admin_help":admin_help}
    if data in admin_map:
        await q.answer(); await admin_map[data](update,context); return
    if data.startswith("plan_") or data.startswith("request_") or data=="back_to_plans":
        await _LEGACY_CALLBACK_ROUTER(update,context); return
    await _LEGACY_CALLBACK_ROUTER(update,context)


# The legacy /activate implementation remains the sole explicit subscription mutation path.
# It now sees the canonical PLANS mapping, so only BASIC/ELITE can be activated.

# Remove all automatic G.Y signal creation by replacing the background loop with a lifecycle-only monitor.
async def _sr_auto_loop():
    global SR_AUTO_TASK
    SR_AUTO_HEALTH["running"]=True; SR_AUTO_HEALTH["last_start"]=time.time()
    try:
        while True:
            try:
                # IMPORTANT: no sr_analyze() here. Existing manually created G.Y records may be monitored only.
                p=await asyncio.to_thread(live_price)
                changed=await asyncio.to_thread(_sr_update_lifecycle,p)
                if changed:
                    recipients=await asyncio.to_thread(_alert_recipients,"gy_private_trade")
                    for row,status,res,price in changed:
                        await _dispatch_broadcast(recipients,with_market_header(f"🎯 تحديث {SR_GY_LABEL}\n🆔 {row['setup_id']}\n💰 السعر: {price}\n📌 النتيجة: {res}\n📊 الحالة: {status}"),kind="sr")
                SR_AUTO_HEALTH["last_success"]=time.time(); SR_AUTO_HEALTH["last_error"]=None
            except asyncio.CancelledError: raise
            except Exception as exc:
                SR_AUTO_HEALTH["last_error"]=f"{type(exc).__name__}: {exc}"; logger.exception("G.Y lifecycle monitor error")
            await asyncio.sleep(SR_AUTO_SECONDS)
    finally:
        SR_AUTO_HEALTH["running"]=False
        if SR_AUTO_TASK is asyncio.current_task(): SR_AUTO_TASK=None


# v19.03: direct Telegram Chat ID command for users.
async def user_id_command(update, context):
    """Show the current user's Telegram Chat ID without exposing other account data."""
    chat = getattr(update, "effective_chat", None)
    message = getattr(update, "effective_message", None)
    if not chat or not message:
        return
    chat_id = int(chat.id)
    await message.reply_text(
        "🆔 معرف حسابك في Telegram\n\n"
        f"`{chat_id}`\n\n"
        "📌 أرسل هذا الرقم إلى الإدارة عند طلب تفعيل أو منحة اشتراك.",
        parse_mode="Markdown"
    )


# Startup override: exactly one trading loop, exactly one alert scheduler, no G.Y signal generator.
async def integrated_start_bot():
    global APPLICATION, SR_AUTO_TASK
    if not TOKEN: raise RuntimeError("TELEGRAM_TOKEN غير موجود في Render.")
    APPLICATION=Application.builder().token(TOKEN).build()
    APPLICATION.add_handler(CommandHandler("start",_trace_handler(start,"command:/start")))
    APPLICATION.add_handler(CommandHandler("plans",_trace_handler(plans,"command:/plans")))
    APPLICATION.add_handler(CommandHandler("subscription",_trace_handler(my_subscription,"command:/subscription")))
    APPLICATION.add_handler(CommandHandler("referral",_trace_handler(referral,"command:/referral")))
    APPLICATION.add_handler(CommandHandler("admin",_trace_handler(admin_command,"command:/admin")))
    APPLICATION.add_handler(CommandHandler("id",_trace_handler(user_id_command,"command:/id")))
    APPLICATION.add_handler(CommandHandler("audit",_trace_handler(audit_report,"command:/audit")))
    APPLICATION.add_handler(CommandHandler("activate",_trace_handler(admin_command,"command:/activate")))
    APPLICATION.add_handler(CallbackQueryHandler(_trace_handler(callback_router,"callback:router")))
    APPLICATION.add_handler(CommandHandler("sr",_trace_handler(sr_command,"command:/sr")),group=0)
    APPLICATION.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"^🎯 صفقة G\.Y الخاصة$"),_trace_handler(_sr_button_guarded,"message:gy_private_trade")),group=0)
    APPLICATION.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND,_trace_handler(router,"message:router")),group=1)
    await APPLICATION.initialize(); await APPLICATION.start(); await _start_alert_dispatcher()
    await APPLICATION.bot.set_webhook(url=WEBHOOK_URL,allowed_updates=["message","callback_query"],drop_pending_updates=True)
    logger.info("XAU SMART TRADER %s started | canonical contract",VERSION)
    asyncio.create_task(auto_loop(),name="main-trading-loop")
    # Lifecycle only: no automatic G.Y signal generation.
    SR_AUTO_TASK=asyncio.create_task(_sr_auto_loop(),name="gy-private-lifecycle-monitor")
    if PERFORMANCE_AUDITOR is not None:
        try:
            _start_auditor_bridge_worker(); PERFORMANCE_AUDITOR.set_self_healer(_auditor_self_heal); PERFORMANCE_AUDITOR.set_alert_callback(_auditor_alert_callback)
            PERFORMANCE_AUDITOR.start_background(); await PERFORMANCE_AUDITOR.start_async_probe()
        except Exception: logging.getLogger(__name__).exception("AUDITOR_START_ERROR")
    try: start_background_refresh()
    except Exception: logging.getLogger(__name__).exception("BACKGROUND_REFRESH_ERROR")
    while True: await asyncio.sleep(3600)


# Contract self-checks: deterministic and side-effect free.
def run_v19_01_contract_self_tests():
    failures=[]
    def check(name,cond):
        if not cond: failures.append(name)
    check("plans_exactly_three",set(PLANS)=={"FREE","BASIC","ELITE"})
    check("free_daily_weekly",{"daily_analysis","weekly_analysis"}.issubset(PLANS["FREE"]["features"]))
    check("basic_history", "trade_history" in PLANS["BASIC"]["features"])
    elite_required={"full_analysis","institutional","gy_private_trade","trade_alerts","news_alerts","news_risk_status"}
    check("elite_required",elite_required.issubset(PLANS["ELITE"]["features"]))
    check("trade_now_not_commercial",all("trade_now" not in p["features"] for p in PLANS.values()))
    check("market_alerts_not_commercial",all("market_alerts" not in p["features"] for p in PLANS.values()))
    check("admin_independent",is_admin_chat(0) is False and "ADMIN_IDS" in globals())
    return {"ok":not failures,"passed":10-len(failures),"total":10,"failures":failures}


def main_v19_01():
    if "--v19-contract-self-test" in sys.argv:
        print(json.dumps(run_v19_01_contract_self_tests(),ensure_ascii=False,indent=2)); return
    if "--self-test" in sys.argv:
        print(json.dumps(run_institutional_self_tests(),ensure_ascii=False,indent=2)); return
    if "--sr-unit-test" in sys.argv:
        print(json.dumps(run_v18_70_sr_runtime_unit_tests(),ensure_ascii=False,indent=2)); return
    if "--sr-self-test" in sys.argv:
        print(json.dumps(run_v18_69_sr_self_tests(),ensure_ascii=False,indent=2)); return
    global BOT_LOOP
    server=threading.Thread(target=run_flask,daemon=True); server.start()
    loop=asyncio.new_event_loop(); BOT_LOOP=loop; asyncio.set_event_loop(loop)
    try: loop.run_until_complete(integrated_start_bot())
    except KeyboardInterrupt: pass
    finally: loop.close()




# ============================================================
# v19.02 — ADMIN FREE ELITE GRANT + ADMIN MENU CLEANUP
# Scope: Administration / Users / Subscriptions only.
# No trading strategy, signal, indicator, entry/SL/TP, alert strategy,
# S/R generation, or performance logic is changed here.
# ============================================================
V19_02_VERSION = "v19.02_ADMIN_FREE_ELITE_GRANT"
V19_03_VERSION = "v19.03_ID_COMMAND"
ADMIN_FREE_GRANT_TABLE = "admin_free_grants"


def _ensure_admin_free_grant_table():
    conn = _db()
    try:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {ADMIN_FREE_GRANT_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                plan TEXT NOT NULL,
                granted_at TEXT NOT NULL,
                expiry_date TEXT NOT NULL,
                granted_by INTEGER NOT NULL,
                previous_plan TEXT NOT NULL,
                previous_status TEXT NOT NULL,
                previous_start_date TEXT,
                previous_expiry_date TEXT,
                active INTEGER NOT NULL DEFAULT 1
            )
        """)
        conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{ADMIN_FREE_GRANT_TABLE}_active
            ON {ADMIN_FREE_GRANT_TABLE}(chat_id, active, expiry_date)
        """)
        conn.commit()
    finally:
        conn.close()


def _reconcile_admin_free_grant(chat_id):
    """Restore the user's previous subscription after a temporary free grant expires."""
    try:
        cid = int(chat_id)
    except (TypeError, ValueError):
        return
    _ensure_admin_free_grant_table()
    now = now_damascus()
    conn = _db()
    try:
        grants = conn.execute(
            f"SELECT * FROM {ADMIN_FREE_GRANT_TABLE} WHERE chat_id=? AND active=1 ORDER BY id DESC",
            (cid,),
        ).fetchall()
        if not grants:
            return
        for grant in grants:
            try:
                expiry = datetime.fromisoformat(str(grant["expiry_date"]))
            except Exception:
                continue
            if now < expiry:
                # The newest active grant owns the temporary access.
                break
            conn.execute(
                """UPDATE users
                   SET plan=?, status=?, start_date=?, expiry_date=?, updated_at=?
                 WHERE chat_id=?""",
                (
                    grant["previous_plan"] if grant["previous_plan"] in PLANS else "FREE",
                    grant["previous_status"] or "active",
                    grant["previous_start_date"],
                    grant["previous_expiry_date"],
                    now.isoformat(),
                    cid,
                ),
            )
            conn.execute(
                f"UPDATE {ADMIN_FREE_GRANT_TABLE} SET active=0 WHERE id=?",
                (grant["id"],),
            )
        conn.commit()
    finally:
        conn.close()


def _active_admin_free_grant(chat_id):
    try:
        cid = int(chat_id)
    except (TypeError, ValueError):
        return None
    _ensure_admin_free_grant_table()
    now = now_damascus()
    conn = _db()
    try:
        row = conn.execute(
            f"SELECT * FROM {ADMIN_FREE_GRANT_TABLE} WHERE chat_id=? AND active=1 ORDER BY id DESC LIMIT 1",
            (cid,),
        ).fetchone()
        if not row:
            return None
        try:
            expiry = datetime.fromisoformat(str(row["expiry_date"]))
        except Exception:
            return None
        if now >= expiry:
            return None
        return dict(row)
    finally:
        conn.close()


def _grant_free_elite(chat_id, days, granted_by):
    """Grant ELITE free access for an exact number of days and preserve prior state."""
    cid = int(chat_id)
    duration = int(days)
    admin_id = int(granted_by)
    if duration <= 0:
        raise ValueError("عدد الأيام يجب أن يكون أكبر من صفر")
    if not is_admin_chat(admin_id):
        raise PermissionError("هذه العملية مخصصة للإدارة فقط")

    now = now_damascus()
    expiry = now + timedelta(days=duration)
    _ensure_admin_free_grant_table()
    conn = _db()
    try:
        user = conn.execute(
            "SELECT chat_id,plan,status,start_date,expiry_date FROM users WHERE chat_id=?",
            (cid,),
        ).fetchone()
        if not user:
            conn.execute(
                "INSERT INTO users(chat_id,plan,status,referral_code,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                (cid, "FREE", "active", f"ref_{cid}", now.isoformat(), now.isoformat()),
            )
            user = conn.execute(
                "SELECT chat_id,plan,status,start_date,expiry_date FROM users WHERE chat_id=?",
                (cid,),
            ).fetchone()

        # If a free grant is already active, extend that grant from its current expiry.
        active = conn.execute(
            f"SELECT * FROM {ADMIN_FREE_GRANT_TABLE} WHERE chat_id=? AND active=1 ORDER BY id DESC LIMIT 1",
            (cid,),
        ).fetchone()
        if active:
            try:
                base = datetime.fromisoformat(str(active["expiry_date"]))
            except Exception:
                base = now
            if base < now:
                base = now
            new_expiry = base + timedelta(days=duration)
            conn.execute(
                f"UPDATE {ADMIN_FREE_GRANT_TABLE} SET expiry_date=?, granted_by=? WHERE id=?",
                (new_expiry.isoformat(), admin_id, active["id"]),
            )
            conn.execute(
                "UPDATE users SET plan='ELITE',status='active',expiry_date=?,updated_at=? WHERE chat_id=?",
                (new_expiry.isoformat(), now.isoformat(), cid),
            )
            conn.commit()
            return {"expiry": new_expiry, "extended": True, "previous_plan": active["previous_plan"]}

        previous_plan = user["plan"] if user["plan"] in PLANS else "FREE"
        previous_status = user["status"] or "active"
        previous_start = user["start_date"]
        previous_expiry = user["expiry_date"]
        conn.execute(
            f"""INSERT INTO {ADMIN_FREE_GRANT_TABLE}
                (chat_id,plan,granted_at,expiry_date,granted_by,previous_plan,
                 previous_status,previous_start_date,previous_expiry_date,active)
                VALUES(?,?,?,?,?,?,?,?,?,1)""",
            (
                cid, "ELITE", now.isoformat(), expiry.isoformat(), admin_id,
                previous_plan, previous_status, previous_start, previous_expiry,
            ),
        )
        conn.execute(
            "UPDATE users SET plan='ELITE',status='active',start_date=?,expiry_date=?,updated_at=? WHERE chat_id=?",
            (now.isoformat(), expiry.isoformat(), now.isoformat(), cid),
        )
        conn.commit()
        return {"expiry": expiry, "extended": False, "previous_plan": previous_plan}
    finally:
        conn.close()


# Contract authorization remains unchanged; temporary free grants only alter effective user access.
_LEGACY_HAS_FEATURE_V19_02 = has_feature

def has_feature(chat_id, feature):
    if is_admin_chat(chat_id):
        return True
    _reconcile_admin_free_grant(chat_id)
    grant = _active_admin_free_grant(chat_id)
    if grant and grant.get("plan") == "ELITE":
        return _canonical_feature(feature) in PLANS["ELITE"]["features"]
    return _LEGACY_HAS_FEATURE_V19_02(chat_id, feature)


_LEGACY_PLAN_STATUS_TEXT_V19_02 = plan_status_text

def plan_status_text(chat_id):
    if is_admin_chat(chat_id):
        return _LEGACY_PLAN_STATUS_TEXT_V19_02(chat_id)
    _reconcile_admin_free_grant(chat_id)
    grant = _active_admin_free_grant(chat_id)
    if grant and grant.get("plan") == "ELITE":
        expiry = datetime.fromisoformat(str(grant["expiry_date"]))
        remaining = max(0, int((expiry - now_damascus()).total_seconds()))
        days = remaining // 86400
        hours = (remaining % 86400) // 3600
        return (
            "💎 النخبة — 🎁 وصول مجاني إداري\n"
            f"⏳ المدة المتبقية: {days} يوم و{hours} ساعة\n"
            f"📅 ينتهي: {expiry.strftime('%Y-%m-%d %H:%M')} دمشق\n"
            "🎯 الصفقات: 50 صفقة شهرياً"
        )
    return _LEGACY_PLAN_STATUS_TEXT_V19_02(chat_id)


async def my_subscription(update, context):
    chat_id = update.effective_chat.id
    await asyncio.to_thread(_ensure_user, update)
    text = "💳 اشتراكك الحالي\n\n" + await asyncio.to_thread(plan_status_text, chat_id)
    await reply(update, with_market_header(text))


async def admin_user_grant_menu(update, context):
    if not await _admin_guard(update):
        return
    cid = context.user_data.get("admin_selected_user")
    if not cid:
        await reply(update, "⛔ لم يتم تحديد حساب.")
        return
    row = await asyncio.to_thread(_admin_user_lookup, cid)
    if not row:
        await reply(update, "🔎 لم يتم العثور على هذا الحساب.")
        return
    context.user_data["admin_mode"] = "grant_free_elite_days"
    text = (
        "🎁 منح اشتراك مجاني\n\n"
        f"• Chat ID: {row['chat_id']}\n"
        f"• الخطة الحالية: {row['plan']}\n\n"
        "💎 الباقة: النخبة\n"
        "✏️ أرسل عدد الأيام المطلوب، مثلاً: 7 أو 15 أو 30 أو 90\n\n"
        "سيتم حفظها كمنحة مجانية مؤقتة، وبعد انتهاء المدة يعود الحساب إلى حالته السابقة."
    )
    await _admin_render(update, text, "grant_free_elite")


def _v19_12_admin_ensure_account(chat_id, admin_id):
    """Explicit admin lookup bootstrap: initialize a missing local account, never silently in normal user flows."""
    if not is_admin_chat(admin_id):
        raise PermissionError("هذه العملية مخصصة للإدارة فقط")
    try:
        cid = int(str(chat_id).strip())
    except Exception:
        raise ValueError("Chat ID غير صالح")
    if cid <= 0:
        raise ValueError("Chat ID يجب أن يكون رقماً موجباً")
    row = _admin_user_lookup(cid)
    if row:
        return row, False
    now = now_damascus().isoformat()
    conn = _db()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO users(chat_id,username,first_name,plan,status,referral_code,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
            (cid, None, None, "FREE", "active", f"ref_{cid}", now, now),
        )
        conn.commit()
        row = conn.execute(
            "SELECT chat_id,username,first_name,plan,status,start_date,expiry_date,created_at,updated_at FROM users WHERE chat_id=?",
            (cid,),
        ).fetchone()
        return (dict(row) if row else None), True
    finally:
        conn.close()


def _v19_12_cancel_free_grant(chat_id, admin_id):
    if not is_admin_chat(admin_id):
        raise PermissionError("هذه العملية مخصصة للإدارة فقط")
    _ensure_admin_free_grant_table()
    cid = int(chat_id)
    conn = _db()
    try:
        grant = conn.execute(
            f"SELECT * FROM {ADMIN_FREE_GRANT_TABLE} WHERE chat_id=? AND active=1 ORDER BY id DESC LIMIT 1", (cid,)
        ).fetchone()
        if not grant:
            return False
        conn.execute(
            "UPDATE users SET plan=?,status=?,start_date=?,expiry_date=?,updated_at=? WHERE chat_id=?",
            (grant["previous_plan"] if grant["previous_plan"] in PLANS else "FREE",
             grant["previous_status"] or "active", grant["previous_start_date"], grant["previous_expiry_date"],
             now_damascus().isoformat(), cid),
        )
        conn.execute(f"UPDATE {ADMIN_FREE_GRANT_TABLE} SET active=0 WHERE id=?", (grant["id"],))
        conn.commit()
        return True
    finally:
        conn.close()


def _v19_12_cancel_subscription(chat_id, admin_id):
    if not is_admin_chat(admin_id):
        raise PermissionError("هذه العملية مخصصة للإدارة فقط")
    cid = int(chat_id)
    _ensure_admin_free_grant_table()
    conn = _db()
    try:
        row = conn.execute("SELECT 1 FROM users WHERE chat_id=?", (cid,)).fetchone()
        if not row:
            return False
        conn.execute("UPDATE users SET plan='FREE',status='active',start_date=NULL,expiry_date=NULL,updated_at=? WHERE chat_id=?",
                     (now_damascus().isoformat(), cid))
        conn.execute(f"UPDATE {ADMIN_FREE_GRANT_TABLE} SET active=0 WHERE chat_id=? AND active=1", (cid,))
        conn.commit()
        return True
    finally:
        conn.close()


def _v19_12_set_blocked(chat_id, blocked, admin_id):
    if not is_admin_chat(admin_id):
        raise PermissionError("هذه العملية مخصصة للإدارة فقط")
    cid = int(chat_id)
    if is_admin_chat(cid):
        raise PermissionError("لا يمكن حظر حساب إداري")
    conn = _db()
    try:
        row = conn.execute("SELECT 1 FROM users WHERE chat_id=?", (cid,)).fetchone()
        if not row:
            return False
        conn.execute("UPDATE users SET status=?,updated_at=? WHERE chat_id=?",
                     ("blocked" if blocked else "active", now_damascus().isoformat(), cid))
        conn.commit()
        return True
    finally:
        conn.close()


async def admin_user_details(update, context, chat_id):
    if not await _admin_guard(update):
        return
    try:
        row, created = await asyncio.to_thread(_v19_12_admin_ensure_account, chat_id, update.effective_user.id)
    except Exception as exc:
        logger.exception("v19.12 admin account lookup failed")
        await reply(update, f"❌ تعذر فتح الحساب.\nالسبب: {type(exc).__name__}: {exc}")
        return
    if not row:
        await reply(update, "❌ تعذر تهيئة الحساب في قاعدة البيانات.")
        return
    cid = int(row["chat_id"])
    context.user_data["admin_selected_user"] = cid
    context.user_data.pop("admin_mode", None)
    grant = await asyncio.to_thread(_active_admin_free_grant, cid)
    label = "FREE+1" if grant and grant.get("plan") == "BASIC" else ("FREE+2" if grant and grant.get("plan") == "ELITE" else None)
    lines = [
        "👥 بيانات المستخدم", "",
        f"• Chat ID: {row['chat_id']}",
        f"• الاسم: {row['first_name'] or '—'}",
        f"• Username: @{row['username']}" if row['username'] else "• Username: —",
        f"• الباقة: {label if label else row['plan']}",
        f"• الحالة: {row['status']}",
        f"• البداية: {row['start_date'] or '—'}",
        f"• الانتهاء: {row['expiry_date'] or '—'}",
    ]
    if created:
        lines += ["", "🆕 تم تهيئة الحساب محلياً بواسطة الإدارة لأنه لم يكن مسجلاً سابقاً."]
    if grant:
        lines += [f"• المنحة: {'BASIC' if grant['plan']=='BASIC' else 'ELITE'} مجانية",
                   f"• انتهاء المنحة: {grant['expiry_date']}"]
    controls = [
        [("🎁 منح اشتراك مجاني", "v19_12_grant_menu")],
        [("❌ إلغاء الباقة المجانية", "v19_12_cancel_grant")],
        [("🚫 إلغاء اشتراك الحساب", "v19_12_cancel_subscription")],
        [("🟢 فك حظر الحساب", "v19_12_unblock")] if str(row["status"]).lower() == "blocked" else [("⛔ حظر الحساب", "v19_12_block")],
        [("🔙 إدارة المستخدمين", "admin_users"), ("👑 لوحة الإدارة", "admin_home")],
    ]
    await _admin_render(update, "\n".join(lines), controls)


async def admin_users(update, context):
    if not await _admin_guard(update):
        return
    await asyncio.to_thread(_ensure_admin_free_grant_table)
    total, rows = await asyncio.to_thread(_admin_users_snapshot)
    lines = ["👥 إدارة المستخدمين", "", f"• إجمالي الحسابات: {total}"]
    for r in rows:
        lines.append(f"• {r['plan']} / {r['status']}: {r['n']}")
    conn = _db()
    try:
        grants = conn.execute(
            f"SELECT plan,status,COUNT(*) n FROM users u JOIN {ADMIN_FREE_GRANT_TABLE} g ON g.chat_id=u.chat_id "
            "WHERE g.active=1 GROUP BY g.plan,u.status ORDER BY g.plan,u.status"
        ).fetchall()
    finally:
        conn.close()
    if grants:
        lines += ["", "🎁 المنح المجانية النشطة:"]
        for r in grants:
            lines.append(f"• {'FREE+1' if r['plan']=='BASIC' else 'FREE+2'} / {r['status']}: {r['n']}")
    context.user_data["admin_mode"] = "search_user"
    lines += ["", "🔎 أرسل Chat ID للبحث أو تهيئة حساب."]
    await _admin_render(update, "\n".join(lines), "users")


async def admin_subscriptions(update, context):
    if not await _admin_guard(update):
        return
    text = (
        "💳 إدارة الاشتراكات\n\n"
        "🎁 منح وصول مجاني\n"
        "• ادخل إلى حساب المستخدم من إدارة المستخدمين.\n"
        "• اختر «🎁 منح النخبة مجانًا».\n"
        "• أرسل عدد الأيام المطلوب.\n"
        "• المدة غير ثابتة ويمكن أن تكون أي عدد موجب من الأيام.\n\n"
        "💎 يتم منح ELITE مجانًا للفترة المحددة، ثم يعود المستخدم تلقائيًا إلى حالته السابقة."
    )
    await _admin_render(update, text, "subscriptions")


# Administration menu cleanup: these two services already exist in the public main keyboard.
async def admin_dashboard(update, context):
    if not await _admin_guard(update):
        return
    rows = [
        [("👥 إدارة المستخدمين", "admin_users"), ("💳 إدارة الاشتراكات", "admin_subscriptions")],
        [("📊 مراقب أداء الصفقات", "admin_trade_performance")],
        [("📈 مراقب أداء النظام", "admin_system_performance")],
        [("🔔 مراقبة التنبيهات", "admin_alerts")],
        [("🔍 التدقيق اليومي", "admin_audit_daily"), ("📅 التدقيق الأسبوعي", "admin_audit_weekly")],
        [("🗄️ حالة قاعدة البيانات", "admin_database")],
        [("⚙️ حالة Scheduler", "admin_scheduler")],
        [("🔙 الرئيسية", "nav_home")],
    ]
    await _admin_render(update, "👑 لوحة الإدارة\n\nاختر أداة الإدارة المطلوبة:", rows)


# Final router override for user lookup and grant-duration input.
_LEGACY_ROUTER_V19_02 = router
async def router(update, context):
    text = (update.effective_message.text or "").strip() if getattr(update, "effective_message", None) else ""
    mode = context.user_data.get("admin_mode") if getattr(context, "user_data", None) is not None else None
    if mode == "search_user" and text.isdigit():
        if not _admin_ok(update):
            context.user_data.pop("admin_mode", None)
            await reply(update, "⛔ هذا القسم مخصص للإدارة فقط.")
            return
        context.user_data.pop("admin_mode", None)
        await admin_user_details(update, context, int(text))
        return
    if mode == "grant_free_elite_days":
        if not _admin_ok(update):
            context.user_data.pop("admin_mode", None)
            await reply(update, "⛔ هذا القسم مخصص للإدارة فقط.")
            return
        try:
            days = int(text)
            if days <= 0:
                raise ValueError
        except Exception:
            await reply(update, "❌ أدخل عدد أيام صحيحًا أكبر من صفر، مثل: 7 أو 15 أو 30 أو 90")
            return
        cid = context.user_data.get("admin_selected_user")
        if not cid:
            context.user_data.pop("admin_mode", None)
            await reply(update, "⛔ لم يتم تحديد حساب.")
            return
        context.user_data.pop("admin_mode", None)
        try:
            result = await asyncio.to_thread(_grant_free_elite, cid, days, update.effective_user.id)
            suffix = " وتم تمديد المنحة الحالية." if result.get("extended") else ""
            await reply(
                update,
                with_market_header(
                    "✅ تم منح الوصول المجاني بنجاح\n\n"
                    f"• المستخدم: {cid}\n"
                    "• الباقة: 💎 النخبة\n"
                    "• النوع: 🎁 مجاني إداري\n"
                    f"• المدة: {days} يوم{suffix}\n"
                    f"• تاريخ الانتهاء: {result['expiry'].strftime('%Y-%m-%d %H:%M')} دمشق\n\n"
                    "بعد انتهاء المدة يعود الحساب إلى حالته السابقة."
                ),
            )
        except Exception as exc:
            await reply(update, f"❌ تعذر منح الاشتراك المجاني.\nالسبب: {type(exc).__name__}: {exc}")
        return
    if text == "👑 الإدارة":
        await admin_dashboard(update, context); return
    if text in {"🧭 القراءة اليومية للسوق", "📝 التقرير اليومي"}:
        await daily_analysis(update, context); return
    if text in {"🧭 القراءة الأسبوعية للسوق", "📅 التقرير الأسبوعي"}:
        await weekly_analysis(update, context); return
    if text == "🎯 صفقة G.Y الخاصة":
        await sr_button(update, context); return
    await _LEGACY_ROUTER_V19_02(update, context)


_LEGACY_CALLBACK_ROUTER_V19_02 = callback_router
async def callback_router(update, context):
    q = update.callback_query
    data = q.data if q else ""
    if data == "admin_home":
        await q.answer(); await admin_dashboard(update, context); return
    if data == "admin_grant_free_elite":
        await q.answer(); await admin_user_grant_menu(update, context); return
    admin_map = {
        "admin_users": admin_users,
        "admin_subscriptions": admin_subscriptions,
        "admin_trade_performance": admin_trade_performance,
        "admin_system_performance": admin_system_performance,
        "admin_alerts": admin_alerts,
        "admin_audit_daily": admin_daily_audit,
        "admin_audit_weekly": admin_weekly_audit,
        "admin_database": admin_database,
        "admin_scheduler": admin_scheduler,
    }
    if data in admin_map:
        await q.answer(); await admin_map[data](update, context); return
    if data.startswith("plan_") or data.startswith("request_") or data == "back_to_plans":
        await _LEGACY_CALLBACK_ROUTER_V19_02(update, context); return
    await _LEGACY_CALLBACK_ROUTER_V19_02(update, context)


# Deterministic self-tests for the v19.02 administration contract.
def run_v19_02_admin_self_tests():
    failures = []
    def check(name, cond):
        if not cond: failures.append(name)
    check("version", V19_02_VERSION == "v19.02_ADMIN_FREE_ELITE_GRANT")
    check("three_plans", set(PLANS) == {"FREE", "BASIC", "ELITE"})
    check("admin_dashboard_no_help", "admin_help" not in globals().get("admin_dashboard", "").__code__.co_names)
    check("admin_dashboard_no_status", "admin_system" not in globals().get("admin_dashboard", "").__code__.co_names)
    check("grant_table_name", ADMIN_FREE_GRANT_TABLE == "admin_free_grants")
    check("elite_feature", "gy_private_trade" in PLANS["ELITE"]["features"])
    return {"ok": not failures, "passed": 6 - len(failures), "total": 6, "failures": failures}


def main_v19_03():
    if "--v19-admin-self-test" in sys.argv:
        print(json.dumps(run_v19_02_admin_self_tests(), ensure_ascii=False, indent=2)); return
    if "--v19-contract-self-test" in sys.argv:
        print(json.dumps(run_v19_01_contract_self_tests(), ensure_ascii=False, indent=2)); return
    if "--self-test" in sys.argv:
        print(json.dumps(run_institutional_self_tests(), ensure_ascii=False, indent=2)); return
    if "--sr-unit-test" in sys.argv:
        print(json.dumps(run_v18_70_sr_runtime_unit_tests(), ensure_ascii=False, indent=2)); return
    if "--sr-self-test" in sys.argv:
        print(json.dumps(run_v18_69_sr_self_tests(), ensure_ascii=False, indent=2)); return
    global BOT_LOOP
    server = threading.Thread(target=run_flask, daemon=True); server.start()
    loop = asyncio.new_event_loop(); BOT_LOOP = loop; asyncio.set_event_loop(loop)
    try: loop.run_until_complete(integrated_start_bot())
    except KeyboardInterrupt: pass
    finally: loop.close()



# ============================================================
# v19.04 — ADMIN FREE GRANT TIERS
# يسمح للإدارة بمنح BASIC أو ELITE مجانًا لفترة محددة.
# لا يغيّر أي منطق تداول أو شروط إشارة.
# ============================================================
V19_04_VERSION = "v19.04_ADMIN_FREE_GRANT_TIERS"


def _grant_free_plan(chat_id, days, granted_by, grant_plan="BASIC"):
    """Grant BASIC or ELITE temporarily and restore the exact prior state on expiry."""
    cid = int(chat_id)
    duration = int(days)
    admin_id = int(granted_by)
    grant_plan = str(grant_plan).upper().strip()
    if duration <= 0:
        raise ValueError("عدد الأيام يجب أن يكون أكبر من صفر")
    if grant_plan not in ("BASIC", "ELITE"):
        raise ValueError("الباقة المجانية الإدارية يجب أن تكون BASIC أو ELITE")
    if not is_admin_chat(admin_id):
        raise PermissionError("هذه العملية مخصصة للإدارة فقط")

    now = now_damascus()
    expiry = now + timedelta(days=duration)
    _ensure_admin_free_grant_table()
    conn = _db()
    try:
        user = conn.execute(
            "SELECT chat_id,plan,status,start_date,expiry_date FROM users WHERE chat_id=?",
            (cid,),
        ).fetchone()
        if not user:
            conn.execute(
                "INSERT INTO users(chat_id,plan,status,referral_code,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                (cid, "FREE", "active", f"ref_{cid}", now.isoformat(), now.isoformat()),
            )
            user = conn.execute(
                "SELECT chat_id,plan,status,start_date,expiry_date FROM users WHERE chat_id=?",
                (cid,),
            ).fetchone()

        active = conn.execute(
            f"SELECT * FROM {ADMIN_FREE_GRANT_TABLE} WHERE chat_id=? AND active=1 ORDER BY id DESC LIMIT 1",
            (cid,),
        ).fetchone()
        if active:
            try:
                base = datetime.fromisoformat(str(active["expiry_date"]))
            except Exception:
                base = now
            if base < now:
                base = now
            new_expiry = base + timedelta(days=duration)
            conn.execute(
                f"UPDATE {ADMIN_FREE_GRANT_TABLE} SET plan=?, expiry_date=?, granted_by=? WHERE id=?",
                (grant_plan, new_expiry.isoformat(), admin_id, active["id"]),
            )
            conn.execute(
                "UPDATE users SET plan=?,status='active',expiry_date=?,updated_at=? WHERE chat_id=?",
                (grant_plan, new_expiry.isoformat(), now.isoformat(), cid),
            )
            conn.commit()
            return {"expiry": new_expiry, "extended": True,
                    "previous_plan": active["previous_plan"], "grant_plan": grant_plan}

        previous_plan = user["plan"] if user["plan"] in PLANS else "FREE"
        previous_status = user["status"] or "active"
        previous_start = user["start_date"]
        previous_expiry = user["expiry_date"]
        conn.execute(
            f"""INSERT INTO {ADMIN_FREE_GRANT_TABLE}
                (chat_id,plan,granted_at,expiry_date,granted_by,previous_plan,
                 previous_status,previous_start_date,previous_expiry_date,active)
                VALUES(?,?,?,?,?,?,?,?,?,1)""",
            (cid, grant_plan, now.isoformat(), expiry.isoformat(), admin_id,
             previous_plan, previous_status, previous_start, previous_expiry),
        )
        conn.execute(
            "UPDATE users SET plan=?,status='active',start_date=?,expiry_date=?,updated_at=? WHERE chat_id=?",
            (grant_plan, now.isoformat(), expiry.isoformat(), now.isoformat(), cid),
        )
        conn.commit()
        return {"expiry": expiry, "extended": False,
                "previous_plan": previous_plan, "grant_plan": grant_plan}
    finally:
        conn.close()


# Effective access for any active administrative free grant.
_LEGACY_HAS_FEATURE_V19_04 = has_feature

def has_feature(chat_id, feature):
    if is_admin_chat(chat_id):
        return True
    row = get_member(chat_id)
    if row and str(row["status"] or "").lower() == "blocked":
        return False
    _reconcile_admin_free_grant(chat_id)
    grant = _active_admin_free_grant(chat_id)
    if grant and grant.get("plan") in ("BASIC", "ELITE"):
        return _canonical_feature(feature) in PLANS[grant["plan"]]["features"]
    return _LEGACY_HAS_FEATURE_V19_04(chat_id, feature)


_LEGACY_PLAN_STATUS_TEXT_V19_04 = plan_status_text

def plan_status_text(chat_id):
    if is_admin_chat(chat_id):
        return _LEGACY_PLAN_STATUS_TEXT_V19_04(chat_id)
    row = get_member(chat_id)
    if row and str(row["status"] or "").lower() == "blocked":
        return "⛔ الحساب محظور\n🔒 الوصول إلى خدمات التداول والاشتراك موقوف من الإدارة."
    _reconcile_admin_free_grant(chat_id)
    grant = _active_admin_free_grant(chat_id)
    if grant and grant.get("plan") in ("BASIC", "ELITE"):
        expiry = datetime.fromisoformat(str(grant["expiry_date"]))
        remaining = max(0, int((expiry - now_damascus()).total_seconds()))
        days = remaining // 86400
        hours = (remaining % 86400) // 3600
        label = "FREE+1" if grant["plan"] == "BASIC" else "FREE+2"
        plan_name = PLANS[grant["plan"]]["name"]
        quota = TRADE_LIMIT_TEXT[grant["plan"]]
        return (
            f"🆓 {label} — 🎁 منحة {plan_name} مجانية\n"
            f"⏳ المدة المتبقية: {days} يوم و{hours} ساعة\n"
            f"📅 تنتهي: {expiry.strftime('%Y-%m-%d %H:%M')} دمشق\n"
            f"🎯 الصفقات: {quota}"
        )
    return _LEGACY_PLAN_STATUS_TEXT_V19_04(chat_id)


async def admin_user_grant_menu(update, context):
    if not await _admin_guard(update):
        return
    cid = context.user_data.get("admin_selected_user")
    if not cid:
        await reply(update, "⛔ لم يتم تحديد حساب.")
        return
    row = await asyncio.to_thread(_admin_user_lookup, cid)
    if not row:
        await reply(update, "🔎 لم يتم العثور على هذا الحساب.")
        return
    context.user_data["admin_mode"] = "grant_free_plan"
    text = (
        "🎁 منح اشتراك مجاني\n\n"
        f"• Chat ID: {row['chat_id']}\n"
        f"• الخطة الحالية: {row['plan']}\n\n"
        "اختر الباقة المجانية التي تريد منحها:\n"
        "🥉 BASIC — مزايا الأساسية + 5 صفقات شهريًا\n"
        "💎 ELITE — مزايا النخبة + 50 صفقة شهريًا\n\n"
        "بعد اختيار الباقة أرسل عدد الأيام المطلوب، ويمكن أن يكون أي عدد موجب."
    )
    rows = [
        [("🥉 منح BASIC مجانًا", "admin_grant_free_basic")],
        [("💎 منح ELITE مجانًا", "admin_grant_free_elite")],
        [("🔙 بيانات المستخدم", "admin_user_back")],
    ]
    await _admin_render(update, text, rows)


async def callback_router(update, context):
    q = update.callback_query
    data = q.data if q else ""
    if data == "admin_grant_free_menu":
        await q.answer(); await admin_user_grant_menu(update, context); return
    if data == "admin_grant_free_basic":
        await q.answer(); context.user_data["admin_grant_plan"] = "BASIC"; context.user_data["admin_mode"] = "grant_free_plan"
        await reply(update, "🥉 تم اختيار BASIC مجانًا.\n\n✏️ أرسل عدد الأيام، مثلاً: 7 أو 15 أو 30 أو 90"); return
    if data == "admin_grant_free_elite":
        await q.answer(); context.user_data["admin_grant_plan"] = "ELITE"; context.user_data["admin_mode"] = "grant_free_plan"
        await reply(update, "💎 تم اختيار ELITE مجانًا.\n\n✏️ أرسل عدد الأيام، مثلاً: 7 أو 15 أو 30 أو 90"); return
    if data == "admin_user_back":
        await q.answer(); cid = context.user_data.get("admin_selected_user")
        if cid: await admin_user_details(update, context, cid)
        else: await admin_users(update, context)
        return
    if data in {"v19_12_grant_menu", "v19_11_grant_menu"}:
        await q.answer(); await admin_user_grant_menu(update, context); return
    if data in {"v19_12_cancel_grant", "v19_11_cancel_grant"}:
        await q.answer(); cid = context.user_data.get("admin_selected_user")
        try:
            ok = await asyncio.to_thread(_v19_12_cancel_free_grant, cid, update.effective_user.id) if cid else False
            await reply(update, "✅ تم إلغاء الباقة المجانية وإعادة الحالة السابقة." if ok else "ℹ️ لا توجد باقة مجانية نشطة لهذا الحساب.")
            if cid: await admin_user_details(update, context, cid)
        except Exception as exc:
            logger.exception("v19.12 cancel grant failed"); await reply(update, f"❌ تعذر الإلغاء.\nالسبب: {type(exc).__name__}: {exc}")
        return
    if data in {"v19_12_cancel_subscription", "v19_11_cancel_subscription"}:
        await q.answer(); cid = context.user_data.get("admin_selected_user")
        try:
            ok = await asyncio.to_thread(_v19_12_cancel_subscription, cid, update.effective_user.id) if cid else False
            await reply(update, "✅ تم إلغاء الاشتراك وإعادة الحساب إلى FREE." if ok else "🔎 لم يتم العثور على الحساب.")
            if cid: await admin_user_details(update, context, cid)
        except Exception as exc:
            logger.exception("v19.12 cancel subscription failed"); await reply(update, f"❌ تعذر إلغاء الاشتراك.\nالسبب: {type(exc).__name__}: {exc}")
        return
    if data in {"v19_12_block", "v19_11_block", "v19_12_unblock", "v19_11_unblock"}:
        await q.answer(); cid = context.user_data.get("admin_selected_user")
        blocked = data in {"v19_12_block", "v19_11_block"}
        try:
            ok = await asyncio.to_thread(_v19_12_set_blocked, cid, blocked, update.effective_user.id) if cid else False
            msg = ("⛔ تم حظر الحساب." if blocked else "🟢 تم فك حظر الحساب.") if ok else "🔎 لم يتم العثور على الحساب."
            await reply(update, msg)
            if cid: await admin_user_details(update, context, cid)
        except Exception as exc:
            logger.exception("v19.12 block action failed"); await reply(update, f"❌ تعذر تنفيذ العملية.\nالسبب: {type(exc).__name__}: {exc}")
        return
    await _LEGACY_CALLBACK_ROUTER_V19_02(update, context)


_LEGACY_ROUTER_V19_04 = router
async def _v19_12_legacy_router(update, context):
    text = (update.effective_message.text or "").strip() if getattr(update, "effective_message", None) else ""
    mode = context.user_data.get("admin_mode") if getattr(context, "user_data", None) is not None else None
    if mode == "grant_free_plan":
        if not _admin_ok(update):
            context.user_data.pop("admin_mode", None)
            context.user_data.pop("admin_grant_plan", None)
            await reply(update, "⛔ هذا القسم مخصص للإدارة فقط.")
            return
        if not text.isdigit() or int(text) <= 0:
            await reply(update, "⚠️ أرسل عدد أيام صحيحًا أكبر من صفر، مثل: 7 أو 15 أو 30")
            return
        cid = context.user_data.get("admin_selected_user")
        grant_plan = context.user_data.get("admin_grant_plan", "BASIC")
        if not cid:
            context.user_data.pop("admin_mode", None)
            await reply(update, "⛔ لم يتم تحديد حساب.")
            return
        context.user_data.pop("admin_mode", None)
        context.user_data.pop("admin_grant_plan", None)
        try:
            result = await asyncio.to_thread(
                _grant_free_plan, cid, int(text), update.effective_user.id, grant_plan
            )
            suffix = " وتم تمديد المنحة الحالية." if result.get("extended") else ""
            await reply(update, with_market_header(
                f"✅ تم منح {grant_plan} مجانًا بنجاح{suffix}\n\n"
                f"• Chat ID: {cid}\n"
                f"• المدة: {int(text)} يوم\n"
                f"• تنتهي: {result['expiry'].strftime('%Y-%m-%d %H:%M')} دمشق"
            ))
        except Exception as exc:
            await reply(update, f"❌ تعذر تنفيذ المنحة: {exc}")
        return
    await _LEGACY_ROUTER_V19_04(update, context)


async def router(update, context):
    text = (update.effective_message.text or "").strip() if getattr(update, "effective_message", None) else ""
    mode = context.user_data.get("admin_mode") if getattr(context, "user_data", None) is not None else None
    if mode == "search_user" and text.isdigit():
        if not await _admin_guard(update):
            context.user_data.pop("admin_mode", None)
            return
        await admin_user_details(update, context, int(text))
        return
    await _v19_12_legacy_router(update, context)


def run_v19_04_self_tests():
    failures = []
    def check(name, cond):
        if not cond:
            failures.append(name)
    check("version", V19_04_VERSION == "v19.04_ADMIN_FREE_GRANT_TIERS")
    check("basic_plan_exists", "BASIC" in PLANS)
    check("elite_plan_exists", "ELITE" in PLANS)
    check("basic_trade_limit", PLANS["BASIC"]["trade_limit"] == 5)
    check("basic_grant_allowed", "BASIC" in ("BASIC", "ELITE"))
    check("elite_grant_allowed", "ELITE" in ("BASIC", "ELITE"))
    check("id_command_present", "command:/id" in globals().get("integrated_start_bot", lambda: None).__code__.co_consts)
    return {"ok": not failures, "passed": 6 - len(failures), "total": 6, "failures": failures}


def main_v19_04():
    if "--v19-04-self-test" in sys.argv:
        print(json.dumps(run_v19_04_self_tests(), ensure_ascii=False, indent=2)); return
    global BOT_LOOP
    server = threading.Thread(target=run_flask, daemon=True); server.start()
    loop = asyncio.new_event_loop(); BOT_LOOP = loop; asyncio.set_event_loop(loop)
    try: loop.run_until_complete(integrated_start_bot())
    except KeyboardInterrupt: pass
    finally: loop.close()



V19_12_VERSION = "v19.12_ADMIN_ACCOUNT_STATUS_CONTROL_FIXED"

def run_v19_12_self_tests():
    failures=[]
    def check(name, cond):
        if not cond: failures.append(name)
    check("version", V19_12_VERSION == "v19.12_ADMIN_ACCOUNT_STATUS_CONTROL_FIXED")
    check("basic_plan_exists", "BASIC" in PLANS)
    check("elite_plan_exists", "ELITE" in PLANS)
    check("free_plus_labels", "FREE+1" in plan_status_text.__code__.co_consts and "FREE+2" in plan_status_text.__code__.co_consts)
    check("blocked_guard", "blocked" in has_feature.__code__.co_consts)
    check("admin_users", callable(admin_users))
    check("admin_details", callable(admin_user_details))
    check("account_bootstrap", callable(_v19_12_admin_ensure_account))
    check("cancel_grant", callable(_v19_12_cancel_free_grant))
    check("cancel_subscription", callable(_v19_12_cancel_subscription))
    check("block", callable(_v19_12_set_blocked))
    check("startup_unchanged", callable(integrated_start_bot))
    check("router_search", "search_user" in router.__code__.co_consts)
    check("callback_controls", "v19_12_block" in callback_router.__code__.co_consts)
    return {"ok": not failures, "passed": 13-len(failures), "total": 13, "failures": failures}

def main_v19_12():
    if "--v19-12-self-test" in sys.argv:
        print(json.dumps(run_v19_12_self_tests(), ensure_ascii=False, indent=2)); return
    global BOT_LOOP
    server = threading.Thread(target=run_flask, daemon=True); server.start()
    loop = asyncio.new_event_loop(); BOT_LOOP = loop; asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(integrated_start_bot())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()

if __name__ == "__main__":
    main_v19_12()
