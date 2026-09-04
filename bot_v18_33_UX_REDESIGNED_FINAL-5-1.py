# ============================================================
# XAU SMART TRADER v18.30 — MONOLITHIC AUDITOR | SECURITY & PERFORMANCE REVIEWED
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
import asyncio
import threading
import time
import queue
import logging
import json
import math
import hashlib
import hmac
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dataclasses import dataclass, field
from contextlib import contextmanager
from typing import Any, Iterator, Optional
from collections import defaultdict
import uuid

import requests
import pandas as pd
import numpy as np

from flask import Flask, request, jsonify
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters

# ============================================================
# PERFORMANCE AUDITOR — MONOLITHIC EMBEDDED ENGINE v18.30
# المحرك مدمج بالكامل داخل هذا الملف ولا يعتمد على مجلد خارجي.
# العزل: التسجيل يتم عبر Queue غير حاجبة، ومحرك التدقيق يعمل بخيط مستقل.
# ============================================================

# --- Embedded Auditor configuration ---
AUDITOR_DB_PATH = os.getenv("AUDITOR_DB_PATH", "xau_performance_auditor.db")
AUDITOR_SYMBOL = os.getenv("AUDITOR_SYMBOL", "XAUUSD")
AUDITOR_POLL_SECONDS = int(os.getenv("AUDITOR_POLL_SECONDS", "15"))
AUDITOR_REQUEST_TIMEOUT = float(os.getenv("AUDITOR_REQUEST_TIMEOUT", "10"))
AUDITOR_RETRY_COUNT = int(os.getenv("AUDITOR_RETRY_COUNT", "3"))
AUDITOR_PRICE_CACHE_SECONDS = float(os.getenv("AUDITOR_PRICE_CACHE_SECONDS", "3"))
AUDITOR_DEFAULT_TRADE_EXPIRY_HOURS = float(os.getenv("AUDITOR_TRADE_EXPIRY_HOURS", "24"))
AUDITOR_PRICE_RETENTION_DAYS = int(os.getenv("AUDITOR_PRICE_RETENTION_DAYS", "30"))

AUDITOR_LOGGER = logging.getLogger("xau_performance_auditor")


# --- Embedded Auditor models ---
from typing import Any, Optional

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


# --- Embedded Auditor database ---
from typing import Any, Iterator, Optional

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS Live_Trades (
    trade_id TEXT PRIMARY KEY,
    signal_time TEXT NOT NULL,
    direction TEXT NOT NULL CHECK(direction IN ('BUY','SELL')),
    entry REAL NOT NULL,
    sl REAL NOT NULL,
    tp1 REAL,
    tp2 REAL,
    tp3 REAL,
    tp_final REAL,
    score REAL,
    quality TEXT,
    risk_reward REAL,
    timeframe TEXT,
    expiry_time TEXT,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    final_result TEXT DEFAULT 'OPEN',
    close_time TEXT,
    max_drawdown REAL DEFAULT 0,
    max_drawdown_price REAL,
    max_drawdown_time TEXT,
    max_adverse_excursion REAL DEFAULT 0,
    mae_price REAL,
    mae_time TEXT,
    max_favorable_excursion REAL DEFAULT 0,
    mfe_price REAL,
    mfe_time TEXT,
    current_price REAL,
    last_price_time TEXT,
    snapshot_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS Trade_Events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id TEXT NOT NULL REFERENCES Live_Trades(trade_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    event_time TEXT NOT NULL,
    price REAL,
    price_distance REAL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(trade_id, event_type)
);
CREATE INDEX IF NOT EXISTS idx_trade_events_trade_time ON Trade_Events(trade_id, event_time);

CREATE TABLE IF NOT EXISTS Market_Analysis (
    analysis_id TEXT PRIMARY KEY,
    analysis_type TEXT NOT NULL CHECK(analysis_type IN ('DAILY','WEEKLY')),
    issue_time TEXT NOT NULL,
    expiry_time TEXT NOT NULL,
    expected_direction TEXT NOT NULL CHECK(expected_direction IN ('BULLISH','BEARISH','SIDEWAYS','WAIT')),
    expected_min_price REAL,
    expected_max_price REAL,
    expected_target REAL,
    score REAL,
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    actual_start_price REAL,
    actual_end_price REAL,
    actual_high REAL,
    actual_low REAL,
    actual_range REAL,
    direction_accuracy REAL,
    range_accuracy REAL,
    target_accuracy REAL,
    accuracy_score REAL,
    result TEXT DEFAULT 'PENDING',
    notes TEXT,
    snapshot_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_analysis_expiry ON Market_Analysis(status, expiry_time);

CREATE TABLE IF NOT EXISTS Audit_Log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    log_time TEXT NOT NULL,
    level TEXT NOT NULL,
    category TEXT NOT NULL,
    message TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS Price_Samples (
    sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_time TEXT NOT NULL,
    price REAL NOT NULL,
    source TEXT NOT NULL,
    high REAL,
    low REAL
);
CREATE INDEX IF NOT EXISTS idx_price_samples_time ON Price_Samples(sample_time);
"""

class Database:
    def __init__(self, path: str = AUDITOR_DB_PATH):
        self.path = path
        self._lock = threading.RLock()
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def initialize(self) -> None:
        with self._lock, self.connect() as conn:
            conn.executescript(SCHEMA)
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
        with self._lock, self.connect() as conn:
            conn.execute("INSERT INTO Audit_Log(log_time,level,category,message,metadata_json) VALUES(?,?,?,?,?)", (now, level, category, message, self.jdump(metadata)))
            conn.commit()


# --- Embedded Auditor price engine ---
from typing import Optional

class PriceProvider:
    name = "base"
    def get_price(self) -> PricePoint:
        raise NotImplementedError

class BiquoteProvider(PriceProvider):
    name = "Biquote"
    def __init__(self, base_url: str = "https://biquote.io/api/XAUUSD"):
        self.url = base_url
    def get_price(self) -> PricePoint:
        r = requests.get(self.url, params={"allowStale": "false"}, timeout=AUDITOR_REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        mid = data.get("mid")
        if mid is None and data.get("bid") is not None and data.get("ask") is not None:
            mid = (float(data["bid"]) + float(data["ask"])) / 2
        price = float(mid)
        if price <= 0:
            raise ValueError("Invalid Biquote price")
        return PricePoint(timestamp=_utc_now(), price=price, source=self.name)

class XausProvider(PriceProvider):
    name = "XAUS"
    def __init__(self, url: str = "https://xaus.com/api/v1/spot"):
        self.url = url
    def get_price(self) -> PricePoint:
        r = requests.get(self.url, timeout=AUDITOR_REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        price = float(data["spot_usd_oz"])
        if price <= 0:
            raise ValueError("Invalid XAUS price")
        return PricePoint(timestamp=_utc_now(), price=price, source=self.name)

class YFinanceProvider(PriceProvider):
    name = "yfinance"
    def get_price(self) -> PricePoint:
        import yfinance as yf
        ticker = yf.Ticker("GC=F")
        hist = ticker.history(period="1d", interval="1m", auto_adjust=False)
        if hist.empty:
            raise RuntimeError("yfinance returned no data")
        price = float(hist["Close"].dropna().iloc[-1])
        if price <= 0:
            raise ValueError("Invalid yfinance price")
        return PricePoint(timestamp=_utc_now(), price=price, source=self.name)

def _utc_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()

class LivePriceEngine:
    def __init__(self, providers: Optional[list[PriceProvider]] = None, cache_seconds: float = AUDITOR_PRICE_CACHE_SECONDS):
        self.providers = providers or [BiquoteProvider(), XausProvider(), YFinanceProvider()]
        self.cache_seconds = cache_seconds
        self._cache: Optional[PricePoint] = None
        self._cache_at = 0.0

    def get_price(self, force: bool = False) -> PricePoint:
        if not force and self._cache and time.time() - self._cache_at < self.cache_seconds:
            return self._cache
        errors = []
        for provider in self.providers:
            for attempt in range(AUDITOR_RETRY_COUNT):
                try:
                    point = provider.get_price()
                    self._cache, self._cache_at = point, time.time()
                    return point
                except Exception as exc:
                    errors.append(f"{provider.name}: {exc}")
                    if attempt + 1 < AUDITOR_RETRY_COUNT:
                        time.sleep(min(1.0, 0.2 * (attempt + 1)))
        raise RuntimeError("All price providers failed: " + " | ".join(errors))


# --- Embedded Auditor trade auditor ---
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
        if value is not None:
            vals.append((label, float(value)))
    return vals

class TradeAuditor:
    def __init__(self, db: Database):
        self.db = db

    def register(self, trade: TradeInput) -> bool:
        direction = trade.direction.upper()
        if direction not in {"BUY", "SELL"}:
            raise ValueError("direction must be BUY or SELL")
        if trade.entry <= 0 or trade.sl <= 0:
            raise ValueError("entry/sl must be positive")
        targets = [x for x in (trade.tp1, trade.tp2, trade.tp3, trade.tp_final) if x is not None]
        if not targets:
            raise ValueError("At least one target is required")
        if direction == "BUY" and not (trade.sl < trade.entry < targets[0] <= targets[-1]):
            raise ValueError("Invalid BUY level ordering")
        if direction == "SELL" and not (trade.sl > trade.entry > targets[0] >= targets[-1]):
            raise ValueError("Invalid SELL level ordering")
        now = trade.signal_time
        with self.db.transaction() as conn:
            existing = conn.execute("SELECT trade_id FROM Live_Trades WHERE trade_id=?", (trade.trade_id,)).fetchone()
            if existing:
                return False
            conn.execute("""INSERT INTO Live_Trades(
                trade_id,signal_time,direction,entry,sl,tp1,tp2,tp3,tp_final,score,quality,risk_reward,timeframe,expiry_time,
                status,final_result,snapshot_json,created_at,updated_at,current_price,last_price_time
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                trade.trade_id, trade.signal_time, direction, trade.entry, trade.sl, trade.tp1, trade.tp2, trade.tp3,
                trade.tp_final, trade.score, trade.quality, trade.risk_reward, trade.timeframe, trade.expiry_time,
                "ACTIVE", "OPEN", self.db.jdump(trade.analysis_snapshot), now, now, trade.entry, trade.signal_time
            ))
            conn.execute("INSERT INTO Trade_Events(trade_id,event_type,event_time,price,metadata_json) VALUES(?,?,?,?,?)",
                         (trade.trade_id, "ENTRY", trade.signal_time, trade.entry, self.db.jdump({"source": "signal"})))
        return True

    def _update_excursions(self, conn, trade: dict, point: PricePoint) -> None:
        direction = trade["direction"]
        entry = float(trade["entry"])
        adverse = max(0.0, entry - point.price) if direction == "BUY" else max(0.0, point.price - entry)
        favorable = max(0.0, point.price - entry) if direction == "BUY" else max(0.0, entry - point.price)
        dd = adverse
        if adverse > float(trade.get("max_adverse_excursion") or 0):
            conn.execute("UPDATE Live_Trades SET max_adverse_excursion=?, mae_price=?, mae_time=? WHERE trade_id=?", (adverse, point.price, point.timestamp, trade["trade_id"]))
        if favorable > float(trade.get("max_favorable_excursion") or 0):
            conn.execute("UPDATE Live_Trades SET max_favorable_excursion=?, mfe_price=?, mfe_time=? WHERE trade_id=?", (favorable, point.price, point.timestamp, trade["trade_id"]))
        if dd > float(trade.get("max_drawdown") or 0):
            conn.execute("UPDATE Live_Trades SET max_drawdown=?, max_drawdown_price=?, max_drawdown_time=? WHERE trade_id=?", (dd, point.price, point.timestamp, trade["trade_id"]))

    def _event(self, conn, trade: dict, event_type: str, point: PricePoint, distance: Optional[float] = None, metadata: Optional[dict] = None) -> bool:
        cur = conn.execute("INSERT OR IGNORE INTO Trade_Events(trade_id,event_type,event_time,price,price_distance,metadata_json) VALUES(?,?,?,?,?,?)",
                           (trade["trade_id"], event_type, point.timestamp, point.price, distance, self.db.jdump(metadata)))
        return cur.rowcount > 0

    def observe_tick(self, point: PricePoint) -> list[str]:
        changed = []
        with self.db.transaction() as conn:
            rows = conn.execute("SELECT * FROM Live_Trades WHERE status='ACTIVE'").fetchall()
            for row in rows:
                trade = dict(row)
                self._update_excursions(conn, trade, point)
                conn.execute("UPDATE Live_Trades SET current_price=?,last_price_time=?,updated_at=? WHERE trade_id=?", (point.price, point.timestamp, point.timestamp, trade["trade_id"]))
                if trade.get("expiry_time") and parse_dt(point.timestamp) >= parse_dt(trade["expiry_time"]):
                    self._event(conn, trade, "EXPIRY", point, metadata={"reason": "expiry_time"})
                    conn.execute("UPDATE Live_Trades SET status='CLOSED',final_result='EXPIRED',close_time=?,updated_at=? WHERE trade_id=?", (point.timestamp, point.timestamp, trade["trade_id"]))
                    changed.append(trade["trade_id"]); continue
                direction = trade["direction"]
                sl = float(trade["sl"])
                targets = levels_for(trade)
                hit_sl = point.price <= sl if direction == "BUY" else point.price >= sl
                hit_targets = [(label, target) for label, target in targets if (point.price >= target if direction == "BUY" else point.price <= target)]
                if hit_sl and hit_targets:
                    self._event(conn, trade, "AMBIGUOUS", point, metadata={"reason": "SL_and_target_same_tick", "targets": [x[0] for x in hit_targets]})
                    conn.execute("UPDATE Live_Trades SET status='CLOSED',final_result='AMBIGUOUS',close_time=?,updated_at=? WHERE trade_id=?", (point.timestamp, point.timestamp, trade["trade_id"]))
                    changed.append(trade["trade_id"]); continue
                if hit_sl:
                    self._event(conn, trade, "SL", point, distance=abs(point.price-sl), metadata={"source": point.source})
                    conn.execute("UPDATE Live_Trades SET status='CLOSED',final_result='FAILED',close_time=?,updated_at=? WHERE trade_id=?", (point.timestamp, point.timestamp, trade["trade_id"]))
                    changed.append(trade["trade_id"]); continue
                if hit_targets:
                    # Only report a real audit state transition. Previously PARTIAL_SUCCESS was
                    # appended on every polling tick after TP1/TP2, causing the same trade ID
                    # to be logged forever. INSERT OR IGNORE makes event creation idempotent.
                    events = {r[0] for r in conn.execute("SELECT event_type FROM Trade_Events WHERE trade_id=?", (trade["trade_id"],)).fetchall()}
                    new_target_event = False
                    for label, target in hit_targets:
                        if label not in events:
                            new_target_event = self._event(
                                conn, trade, label, point, distance=abs(point.price-target),
                                metadata={"source": point.source}
                            ) or new_target_event
                    final_label = targets[-1][0]
                    event_names = {r[0] for r in conn.execute("SELECT event_type FROM Trade_Events WHERE trade_id=?", (trade["trade_id"],)).fetchall()}
                    if final_label in event_names:
                        conn.execute("UPDATE Live_Trades SET status='CLOSED',final_result='FULL_SUCCESS',close_time=?,updated_at=? WHERE trade_id=?", (point.timestamp, point.timestamp, trade["trade_id"]))
                        changed.append(trade["trade_id"])
                    elif new_target_event:
                        conn.execute("UPDATE Live_Trades SET status='ACTIVE',final_result='PARTIAL_SUCCESS',updated_at=? WHERE trade_id=?", (point.timestamp, trade["trade_id"]))
                        changed.append(trade["trade_id"])
        return changed

    def observe_bar(self, bar: BarPoint) -> list[str]:
        """Simulation/historical candle auditing. If SL and a target are both inside one candle, order is unknown => AMBIGUOUS."""
        changed = []
        with self.db.transaction() as conn:
            rows = conn.execute("SELECT * FROM Live_Trades WHERE status='ACTIVE'").fetchall()
            for row in rows:
                trade = dict(row); direction = trade["direction"]
                sl = float(trade["sl"]); targets = levels_for(trade)
                sl_hit = bar.low <= sl if direction == "BUY" else bar.high >= sl
                target_hits = [(label, target) for label, target in targets if (bar.high >= target if direction == "BUY" else bar.low <= target)]
                if sl_hit and target_hits:
                    self._event(conn, trade, "AMBIGUOUS", PricePoint(bar.timestamp, bar.close, bar.source, bar.high, bar.low), metadata={"reason":"same_bar_order_unknown", "targets":[x[0] for x in target_hits]})
                    conn.execute("UPDATE Live_Trades SET status='CLOSED',final_result='AMBIGUOUS',close_time=?,updated_at=? WHERE trade_id=?", (bar.timestamp,bar.timestamp,trade["trade_id"]))
                    changed.append(trade["trade_id"]); continue
                if sl_hit:
                    p = sl; self._event(conn, trade, "SL", PricePoint(bar.timestamp,p,bar.source,bar.high,bar.low), metadata={"mode":"bar"})
                    conn.execute("UPDATE Live_Trades SET status='CLOSED',final_result='FAILED',close_time=?,updated_at=? WHERE trade_id=?", (bar.timestamp,bar.timestamp,trade["trade_id"]))
                    changed.append(trade["trade_id"]); continue
                hits = [(label,target) for label,target in target_hits]
                for label,target in hits:
                    self._event(conn, trade, label, PricePoint(bar.timestamp,target,bar.source,bar.high,bar.low), metadata={"mode":"bar"})
                if hits and hits[-1][0] == targets[-1][0]:
                    conn.execute("UPDATE Live_Trades SET status='CLOSED',final_result='FULL_SUCCESS',close_time=?,updated_at=? WHERE trade_id=?", (bar.timestamp,bar.timestamp,trade["trade_id"]))
                    changed.append(trade["trade_id"])
        return changed


# --- Embedded Auditor analysis auditor ---

def parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

class AnalysisAuditor:
    def __init__(self, db: Database): self.db = db

    def register(self, analysis: AnalysisInput) -> bool:
        typ = analysis.analysis_type.upper()
        direction = analysis.expected_direction.upper()
        if typ not in {"DAILY","WEEKLY"}: raise ValueError("analysis_type must be DAILY or WEEKLY")
        if direction not in {"BULLISH","BEARISH","SIDEWAYS","WAIT"}: raise ValueError("Invalid expected_direction")
        if analysis.expected_min_price is not None and analysis.expected_max_price is not None and analysis.expected_min_price > analysis.expected_max_price:
            raise ValueError("expected_min_price cannot exceed expected_max_price")
        with self.db.transaction() as conn:
            if conn.execute("SELECT 1 FROM Market_Analysis WHERE analysis_id=?", (analysis.analysis_id,)).fetchone(): return False
            conn.execute("""INSERT INTO Market_Analysis(
                analysis_id,analysis_type,issue_time,expiry_time,expected_direction,expected_min_price,expected_max_price,expected_target,score,confidence,status,snapshot_json,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                analysis.analysis_id,typ,analysis.issue_time,analysis.expiry_time,direction,analysis.expected_min_price,analysis.expected_max_price,
                analysis.expected_target,analysis.score,analysis.confidence,"ACTIVE",self.db.jdump(analysis.analysis_snapshot),analysis.issue_time,analysis.issue_time))
        return True

    @staticmethod
    def direction_score(expected: str, start: float, end: float, expected_min: float|None, expected_max: float|None) -> float:
        if expected == "BULLISH": return 100.0 if end > start else 0.0
        if expected == "BEARISH": return 100.0 if end < start else 0.0
        if expected == "SIDEWAYS":
            if expected_min is None or expected_max is None: return 100.0 if abs(end-start) <= start*0.002 else 0.0
            return 100.0 if expected_min <= end <= expected_max else 0.0
        return 100.0 if abs(end-start) <= start*0.002 else 0.0

    @staticmethod
    def range_score(expected_min: float|None, expected_max: float|None, actual_low: float, actual_high: float) -> float:
        if expected_min is None or expected_max is None: return 0.0
        lo, hi = float(expected_min), float(expected_max)
        inter = max(0.0, min(hi, actual_high) - max(lo, actual_low))
        union = max(hi, actual_high) - min(lo, actual_low)
        return 100.0 * inter / union if union > 0 else 100.0

    @staticmethod
    def target_score(expected_target: float|None, actual_high: float, actual_low: float) -> float:
        if expected_target is None: return 0.0
        return 100.0 if actual_low <= expected_target <= actual_high else 0.0

    def finalize(self, analysis_id: str, bars: list[BarPoint]) -> dict:
        if not bars: raise ValueError("No bars supplied")
        with self.db.transaction() as conn:
            row = conn.execute("SELECT * FROM Market_Analysis WHERE analysis_id=?", (analysis_id,)).fetchone()
            if not row: raise KeyError(analysis_id)
            a = dict(row)
            if a["status"] == "CLOSED": return a
            actual_start = bars[0].open; actual_end = bars[-1].close; actual_high=max(b.high for b in bars); actual_low=min(b.low for b in bars)
            ds = self.direction_score(a["expected_direction"], actual_start, actual_end, a["expected_min_price"], a["expected_max_price"])
            rs = self.range_score(a["expected_min_price"], a["expected_max_price"], actual_low, actual_high)
            ts = self.target_score(a["expected_target"], actual_high, actual_low)
            # Explicit weighted score: direction 50%, range 30%, target 20%.
            overall = 0.50*ds + 0.30*rs + 0.20*ts
            result = "MATCHED" if overall >= 60.0 else "NOT_MATCHED"
            conn.execute("""UPDATE Market_Analysis SET status='CLOSED',actual_start_price=?,actual_end_price=?,actual_high=?,actual_low=?,actual_range=?,direction_accuracy=?,range_accuracy=?,target_accuracy=?,accuracy_score=?,result=?,updated_at=? WHERE analysis_id=?""",
                         (actual_start,actual_end,actual_high,actual_low,actual_high-actual_low,ds,rs,ts,overall,result,bars[-1].timestamp,analysis_id))
            return dict(conn.execute("SELECT * FROM Market_Analysis WHERE analysis_id=?", (analysis_id,)).fetchone())
    def finalize_expired_from_samples(self, now_iso: str) -> list[str]:
        """Finalize analyses whose expiry has passed using observed Price_Samples.
        This is intentionally based only on prices actually observed by the Auditor.
        Missing samples are never invented.
        """
        done = []
        now = parse_dt(now_iso)
        with self.db.connect() as conn:
            rows = conn.execute("SELECT * FROM Market_Analysis WHERE status='ACTIVE' AND expiry_time<=?", (now_iso,)).fetchall()
        for row in rows:
            a = dict(row)
            with self.db.connect() as conn:
                samples = conn.execute("SELECT sample_time,price FROM Price_Samples WHERE sample_time>=? AND sample_time<=? ORDER BY sample_time", (a['issue_time'], a['expiry_time'])).fetchall()
            if not samples:
                self.db.audit_log('WARNING','ANALYSIS',f"No observed price samples for expired analysis {a['analysis_id']}",{},now_iso)
                continue
            bars = [type('ObservedBar', (), {'timestamp': r['sample_time'], 'open': float(r['price']), 'high': float(r['price']), 'low': float(r['price']), 'close': float(r['price'])})() for r in samples]
            self.finalize(a['analysis_id'], bars)
            done.append(a['analysis_id'])
        return done


# --- Embedded Auditor metrics ---

class Metrics:
    def __init__(self, db: Database): self.db=db
    def report(self) -> dict:
        with self.db.connect() as c:
            trades=[dict(r) for r in c.execute("SELECT * FROM Live_Trades").fetchall()]
            analyses=[dict(r) for r in c.execute("SELECT * FROM Market_Analysis WHERE status='CLOSED'").fetchall()]
        total=len(trades); full=sum(x['final_result']=='FULL_SUCCESS' for x in trades); failed=sum(x['final_result']=='FAILED' for x in trades); partial=sum(x['final_result']=='PARTIAL_SUCCESS' for x in trades); expired=sum(x['final_result']=='EXPIRED' for x in trades); ambiguous=sum(x['final_result']=='AMBIGUOUS' for x in trades)
        closed=full+failed+partial+expired+ambiguous
        # Win Rate is deliberately defined as FULL_SUCCESS / all closed trades.
        win_rate=100*full/closed if closed else 0.0
        full_rate=100*full/total if total else 0.0
        def avg(field):
            vals=[float(x[field]) for x in trades if x[field] is not None]
            return sum(vals)/len(vals) if vals else 0.0
        daily=[a for a in analyses if a['analysis_type']=='DAILY']; weekly=[a for a in analyses if a['analysis_type']=='WEEKLY']
        def analysis_stats(items):
            return {'total':len(items),'matched':sum(a['result']=='MATCHED' for a in items),'not_matched':sum(a['result']=='NOT_MATCHED' for a in items),'accuracy_pct':100*sum(a['result']=='MATCHED' for a in items)/len(items) if items else 0.0,'direction_accuracy_avg':sum(a['direction_accuracy'] or 0 for a in items)/len(items) if items else 0.0,'range_accuracy_avg':sum(a['range_accuracy'] or 0 for a in items)/len(items) if items else 0.0,'target_accuracy_avg':sum(a['target_accuracy'] or 0 for a in items)/len(items) if items else 0.0}
        score=defaultdict(lambda:{'total':0,'full':0,'failed':0,'partial':0})
        for t in trades:
            s=t['score']; bucket='UNKNOWN' if s is None else ('50-59' if s<60 else '60-69' if s<70 else '70-79' if s<80 else '80-89' if s<90 else '90-100' if s<=100 else '100+')
            score[bucket]['total']+=1; score[bucket]['full']+=t['final_result']=='FULL_SUCCESS'; score[bucket]['failed']+=t['final_result']=='FAILED'; score[bucket]['partial']+=t['final_result']=='PARTIAL_SUCCESS'
        return {'trades':{'total':total,'closed':closed,'full_success':full,'partial_success':partial,'failed':failed,'expired':expired,'ambiguous':ambiguous,'win_rate_pct':win_rate,'full_target_rate_pct':full_rate,'avg_rr':avg('risk_reward'),'avg_drawdown':avg('max_drawdown'),'avg_mae':avg('max_adverse_excursion'),'avg_mfe':avg('max_favorable_excursion')},'daily':analysis_stats(daily),'weekly':analysis_stats(weekly),'score_performance':dict(score)}


# --- Embedded Auditor reports ---
class Reporter:
    def __init__(self, metrics: Metrics): self.metrics=metrics
    def text(self) -> str:
        r=self.metrics.report(); t=r['trades']; d=r['daily']; w=r['weekly']
        lines=['XAU SMART TRADER PERFORMANCE AUDIT','='*44,'','TRADES',f"Total: {t['total']}",f"Closed: {t['closed']}",f"Full Success: {t['full_success']}",f"Partial Success: {t['partial_success']}",f"Failed: {t['failed']}",f"Expired: {t['expired']}",f"Ambiguous: {t['ambiguous']}",f"Win Rate: {t['win_rate_pct']:.2f}%",f"Full Target Rate: {t['full_target_rate_pct']:.2f}%",f"Average R:R: {t['avg_rr']:.2f}",f"Average Drawdown: {t['avg_drawdown']:.4f}",f"Average MAE: {t['avg_mae']:.4f}",f"Average MFE: {t['avg_mfe']:.4f}",'','DAILY ANALYSIS',f"Total: {d['total']}",f"Matched: {d['matched']}",f"Not Matched: {d['not_matched']}",f"Accuracy: {d['accuracy_pct']:.2f}%",f"Direction Avg: {d['direction_accuracy_avg']:.2f}%",f"Range Avg: {d['range_accuracy_avg']:.2f}%",f"Target Avg: {d['target_accuracy_avg']:.2f}%",'','WEEKLY ANALYSIS',f"Total: {w['total']}",f"Matched: {w['matched']}",f"Not Matched: {w['not_matched']}",f"Accuracy: {w['accuracy_pct']:.2f}%",f"Direction Avg: {w['direction_accuracy_avg']:.2f}%",f"Range Avg: {w['range_accuracy_avg']:.2f}%",f"Target Avg: {w['target_accuracy_avg']:.2f}%",'','SCORE PERFORMANCE']
        for bucket,v in r['score_performance'].items():
            wr=100*v['full']/v['total'] if v['total'] else 0
            lines.append(f"{bucket}: total={v['total']} full={v['full']} failed={v['failed']} partial={v['partial']} full_rate={wr:.2f}%")
        return '\n'.join(lines)


# --- Embedded Auditor service ---
from datetime import datetime, timezone, timedelta

class PerformanceAuditor:
    """Independent Observe -> Record -> Audit -> Report service."""
    def __init__(self, db_path: str = AUDITOR_DB_PATH, price_engine: Optional[LivePriceEngine] = None):
        self.db=Database(db_path); self.price_engine=price_engine or LivePriceEngine(); self.trade_auditor=TradeAuditor(self.db); self.analysis_auditor=AnalysisAuditor(self.db); self.metrics=Metrics(self.db); self.reporter=Reporter(self.metrics); self._stop=threading.Event(); self._thread=None

    @staticmethod
    def _now() -> str: return datetime.now(timezone.utc).isoformat()
    @staticmethod
    def _id(prefix: str) -> str: return f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}_{uuid.uuid4().hex[:8]}"

    def register_trade(self, *, trade_id: Optional[str]=None, signal_time: Optional[str]=None, direction: str, entry: float, sl: float, tp1: Optional[float]=None, tp2: Optional[float]=None, tp3: Optional[float]=None, tp_final: Optional[float]=None, score: Optional[float]=None, quality: Optional[str]=None, risk_reward: Optional[float]=None, timeframe: Optional[str]=None, expiry_time: Optional[str]=None, analysis_snapshot: Optional[dict[str,Any]]=None) -> str:
        signal_time=signal_time or self._now(); trade_id=trade_id or self._id('TRD'); targets=[x for x in (tp1,tp2,tp3,tp_final) if x is not None]; final=tp_final if tp_final is not None else targets[-1]
        if expiry_time is None: expiry_time=(datetime.fromisoformat(signal_time.replace('Z','+00:00'))+timedelta(hours=AUDITOR_DEFAULT_TRADE_EXPIRY_HOURS)).isoformat()
        obj=TradeInput(trade_id,signal_time,direction,float(entry),float(sl),tp1,tp2,tp3,final,score,quality,risk_reward,timeframe,expiry_time,analysis_snapshot or {})
        self.trade_auditor.register(obj); return trade_id

    def register_analysis(self, *, analysis_id: Optional[str]=None, analysis_type: str, issue_time: Optional[str]=None, expiry_time: str, direction: str, expected_min: Optional[float]=None, expected_max: Optional[float]=None, target: Optional[float]=None, score: Optional[float]=None, confidence: Optional[float]=None, analysis_snapshot: Optional[dict[str,Any]]=None) -> str:
        issue_time=issue_time or self._now(); analysis_id=analysis_id or self._id('ANL'); obj=AnalysisInput(analysis_id,analysis_type,issue_time,expiry_time,direction,expected_min,expected_max,target,score,confidence,analysis_snapshot or {})
        self.analysis_auditor.register(obj); return analysis_id

    def observe_price(self, price: float, timestamp: Optional[str]=None, source: str='manual') -> list[str]:
        point=PricePoint(timestamp or self._now(),float(price),source); return self.trade_auditor.observe_tick(point)

    def observe_bar(self, *, timestamp: str, open: float, high: float, low: float, close: float, source: str='simulation') -> list[str]:
        return self.trade_auditor.observe_bar(BarPoint(timestamp,float(open),float(high),float(low),float(close),source))

    def finalize_analysis(self, analysis_id: str, bars: list[BarPoint]) -> dict: return self.analysis_auditor.finalize(analysis_id,bars)
    def generate_performance_report(self) -> str: return self.reporter.text()
    def metrics_report(self) -> dict: return self.metrics.report()

    def _cleanup_old_samples(self) -> None:
        """Bound the Auditor database growth while retaining enough history for audits."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=max(1, AUDITOR_PRICE_RETENTION_DAYS))).isoformat()
        with self.db.connect() as conn:
            conn.execute('DELETE FROM Price_Samples WHERE sample_time < ?', (cutoff,))
            conn.commit()

    def run_forever(self) -> None:
        AUDITOR_LOGGER.info('Performance Auditor started')
        cleanup_counter = 0
        while not self._stop.is_set():
            point = None
            try:
                point=self.price_engine.get_price(force=True)
                self.db.audit_log('INFO','PRICE','Price sample',{'price':point.price,'source':point.source},point.timestamp)
                with self.db.connect() as conn:
                    conn.execute('INSERT INTO Price_Samples(sample_time,price,source,high,low) VALUES(?,?,?,?,?)',(point.timestamp,point.price,point.source,point.high,point.low)); conn.commit()
                changed=self.trade_auditor.observe_tick(point)
                if changed: AUDITOR_LOGGER.info('Audited trade changes: %s',changed)
            except Exception as exc:
                now=self._now(); self.db.audit_log('ERROR','PRICE',str(exc),{},now); AUDITOR_LOGGER.exception('Price monitoring failure')
            try:
                finalize_time = point.timestamp if point is not None else self._now()
                finalized = self.analysis_auditor.finalize_expired_from_samples(finalize_time)
                if finalized:
                    AUDITOR_LOGGER.info('Finalized analyses: %s', finalized)
            except Exception:
                AUDITOR_LOGGER.exception('ANALYSIS_AUDIT_FINALIZE_ERROR')
            cleanup_counter += 1
            # Cleanup once per ~hour at the default 15s polling cadence.
            if cleanup_counter >= max(1, int(3600 / max(1, AUDITOR_POLL_SECONDS))):
                cleanup_counter = 0
                try:
                    self._cleanup_old_samples()
                except Exception:
                    AUDITOR_LOGGER.exception('PRICE_SAMPLE_CLEANUP_ERROR')
            self._stop.wait(AUDITOR_POLL_SECONDS)
    def start_background(self) -> None:
        if self._thread and self._thread.is_alive(): return
        self._stop.clear(); self._thread=threading.Thread(target=self.run_forever,name='xau-auditor',daemon=True); self._thread.start()
    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive() and self._thread is not threading.current_thread():
            self._thread.join(timeout=max(2.0, AUDITOR_POLL_SECONDS + 1.0))
        self._thread = None



# إنشاء محرك المراقبة المدمج — لا يوجد import خارجي.
try:
    PERFORMANCE_AUDITOR = PerformanceAuditor(os.environ.get("AUDITOR_DB_PATH", "xau_performance_auditor.db"))
except Exception as _auditor_exc:
    PERFORMANCE_AUDITOR = None
    logging.getLogger("xau_performance_auditor_bridge").warning("Performance Auditor unavailable: %s", _auditor_exc)

# ============================================================
# الإعدادات
# ============================================================

VERSION = "v18.33"
TOKEN = os.environ.get("TELEGRAM_TOKEN")
PORT = int(os.environ.get("PORT", "10000"))

RENDER_URL = os.environ.get(
    "RENDER_EXTERNAL_URL",
    "https://xau-smart-bot.onrender.com"
).rstrip("/")

WEBHOOK_PATH = "/telegram-webhook"
WEBHOOK_URL = RENDER_URL + WEBHOOK_PATH
# Telegram webhook authenticity: deterministic secret derived from the bot token
# when no explicit secret is supplied. This prevents arbitrary internet clients
# from injecting fake Telegram updates into the public Flask endpoint.
TELEGRAM_WEBHOOK_SECRET = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "").strip()
# Do not derive a webhook secret from TELEGRAM_TOKEN. Telegram only sends the
# X-Telegram-Bot-Api-Secret-Token header when a secret was explicitly configured
# in setWebhook(). An internally derived secret can make an otherwise healthy
# deployment reject every update with HTTP 403, producing a completely silent bot.


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
# الأخبار للتنبيهات فقط — لا تُستخدم أبداً لحجب التداول.
NEWS_FILTER_ENABLED = True
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
# Reject oversized webhook bodies early to reduce trivial memory/DoS pressure.
app.config["MAX_CONTENT_LENGTH"] = int(os.environ.get("WEBHOOK_MAX_BYTES", "262144"))
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ============================================================
# الحالة
# ============================================================

APPLICATION = None
BOT_LOOP = None
SUBSCRIBERS = set()
DATA_CACHE = {}
DATA_CACHE_LOCK = threading.RLock()
LAST_SIGNAL = {}
NEWS_CACHE = {"time": 0, "events": []}
NEWS_CACHE_LOCK = threading.RLock()
STATE_LOCK = threading.RLock()
SHUTDOWN_EVENT = threading.Event()
AUTO_TASK = None
SESSION_ALERT_STATE = {}
NEWS_ALERT_STATE = {}
EMERGENCY_ALERT_STATE = {}


def _prune_alert_state(state: dict, max_items: int = 2000) -> None:
    """Bound in-memory alert state so long-running bots cannot grow indefinitely."""
    if len(state) <= max_items:
        return
    # Alert keys are chronological in normal operation; keep the newest half.
    for key in list(state.keys())[:-max_items // 2]:
        state.pop(key, None)
LAST_MARKET_STATE = None
TRADE_HISTORY = []
LAST_ANALYSIS = None
MAX_TRADE_HISTORY = 500
TRADE_DB_PATH = os.environ.get("TRADE_DB_PATH", "trades.db")
TRADE_LOCK = threading.RLock()
LIQUIDITY_LOCK = threading.RLock()

# جسر Auditor غير حاجب: لا نسمح لأي عملية SQLite/HTTP خاصة بالمراقب
# أن تنتظر داخل دورة التداول أو داخل TRADE_LOCK.
AUDITOR_QUEUE_MAX = int(os.environ.get("AUDITOR_QUEUE_MAX", "256"))
AUDITOR_QUEUE = queue.Queue(maxsize=max(16, AUDITOR_QUEUE_MAX))
AUDITOR_WORKER_THREAD = None
AUDITOR_WORKER_STOP = threading.Event()
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
    """إرجاع مشتركي التنبيهات بقراءة واحدة من قاعدة الاشتراك (بدون N+1 queries)."""
    conn = _db()
    try:
        rows = conn.execute("SELECT chat_id, plan, expiry_date FROM users WHERE status='active'").fetchall()
        result = set(ADMIN_IDS)
        now = now_damascus()
        for row in rows:
            plan = row["plan"] or "FREE"
            if plan not in PLANS or "trade_alerts" not in PLANS[plan]["features"]:
                continue
            expiry_raw = row["expiry_date"]
            if expiry_raw and plan != "FREE":
                try:
                    expiry = datetime.fromisoformat(expiry_raw)
                    if expiry.tzinfo is None:
                        expiry = expiry.replace(tzinfo=DAMASCUS)
                    if now >= expiry:
                        continue
                except (TypeError, ValueError):
                    continue
            result.add(int(row["chat_id"]))
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
    return (f"{market_header()}{title}\n━━━━━━━━━━━━━━━━━━\n\n"
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

def _completed_bars(df, interval):
    """Return only completed candles; never allow the in-progress candle into analysis."""
    if df is None or df.empty or "openTime" not in df.columns:
        return df
    out = df.copy()
    out["openTime"] = pd.to_datetime(out["openTime"], utc=True, errors="coerce")
    out = out.dropna(subset=["openTime"]).sort_values("openTime")
    if out.empty:
        return out
    durations = {"1m": pd.Timedelta(minutes=1), "5m": pd.Timedelta(minutes=5),
                 "15m": pd.Timedelta(minutes=15), "30m": pd.Timedelta(minutes=30),
                 "1h": pd.Timedelta(hours=1), "4h": pd.Timedelta(hours=4),
                 "1d": pd.Timedelta(days=1), "1w": pd.Timedelta(days=7)}
    duration = durations.get(interval)
    if duration is None:
        return out
    now = pd.Timestamp.now(tz="UTC")
    cutoff = now - duration
    return out[out["openTime"] + duration <= now].copy()


def get_bars(interval, limit=300):
    """جلب بيانات Biquote وبناء W1 محلياً من D1."""

    if interval == "1w":
        key = f"1w_{limit}"
        now = time.time()
        with DATA_CACHE_LOCK:
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

        weekly = _completed_bars(weekly, "1w")
        if len(weekly) < MIN_BARS:
            raise ValueError(f"البيانات الأسبوعية المكتملة غير كافية: {len(weekly)} شمعة.")
        with DATA_CACHE_LOCK:
            DATA_CACHE[key] = (now, weekly.copy())
        return weekly.copy()

    supported = {"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
    if interval not in supported:
        raise ValueError(f"الفريم {interval} غير مدعوم.")

    key = cache_key(interval, limit)
    now = time.time()
    with DATA_CACHE_LOCK:
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
    df = _completed_bars(df, interval)
    if len(df) < MIN_BARS:
        raise ValueError(f"البيانات غير كافية للفريم {interval}: {len(df)} شمعة.")

    with DATA_CACHE_LOCK:
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


def get_news_events(force=False):
    """جلب تقويم اقتصادي عالي التأثير من CSV لاستخدامه في التنبيهات فقط.

    عند تعذر المصدر يُرفع الخطأ إلى طبقة التنبيهات، ولا يُستخدم إطلاقاً لحجب التداول.
    """
    global NEWS_CACHE
    now_ts = time.time()
    with NEWS_CACHE_LOCK:
        cached_time = NEWS_CACHE.get("time", 0)
        cached_events = list(NEWS_CACHE.get("events", []))
    if not force and cached_time and now_ts - cached_time < NEWS_CACHE_SECONDS:
        return cached_events

    urls = []
    configured = os.environ.get("NEWS_CALENDAR_URL", "").strip()
    if configured:
        urls.append(configured)
    urls.extend([
        "https://nfs.faireconomy.media/ff_calendar_thisweek.csv",
        "https://www.forexfactory.com/calendar?export=csv&week=this",
    ])
    headers = {"User-Agent": "Mozilla/5.0 XAU-Smart-Trader/18.23"}
    last_error = None
    text = ""
    for url in dict.fromkeys(urls):
        try:
            response = requests.get(url, timeout=12, headers=headers)
            response.raise_for_status()
            if response.text.strip():
                text = response.text
                break
            last_error = RuntimeError(f"مصدر الأخبار فارغ: {url}")
        except Exception as exc:
            last_error = exc
            logger.warning("News source failed: %s | %s", url, exc)
    if not text.strip():
        raise RuntimeError(f"تعذر الحصول على تقويم الأخبار من جميع المصادر: {last_error}")

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

    with NEWS_CACHE_LOCK:
        NEWS_CACHE = {"time": now_ts, "events": list(events)}
    return events


def news_filter():
    """حالة الأخبار للمعلومات فقط — لا يمكنها حجب التداول بأي شكل."""
    try:
        events = get_news_events()
        now = now_damascus()
        upcoming = []
        for event in events:
            if event.get("impact") != "HIGH":
                continue
            event_time = event.get("time")
            if not isinstance(event_time, datetime):
                continue
            delta_min = (event_time - now).total_seconds() / 60.0
            if -NEWS_AFTER_MIN <= delta_min <= NEWS_BEFORE_MIN:
                upcoming.append((event, delta_min))

        if upcoming:
            details = []
            for event, delta in sorted(upcoming, key=lambda item: abs(item[1]))[:3]:
                event_time = event["time"].strftime("%Y-%m-%d %H:%M")
                if delta > 1:
                    phase = f"بعد {round(delta)} دقيقة"
                elif delta >= -1:
                    phase = "الآن / قريب جداً"
                else:
                    phase = f"منذ {round(abs(delta))} دقيقة"
                details.append(
                    f"🚨 {event.get('currency','')} — {event.get('event','خبر مرتفع التأثير')} | {event_time} دمشق | {phase}"
                )
            return False, "🔔 يوجد خبر عالي التأثير — تنبيه معلوماتي فقط.\n" + "\n".join(details)

        return False, "🟢 لا يوجد حالياً خبر عالي التأثير ضمن نافذة التنبيه."
    except Exception as exc:
        logger.warning("News calendar unavailable (alerts only): %s", exc)
        return False, f"⚠️ تعذر تحديث بيانات الأخبار حالياً: {exc}\n🔔 التداول مستمر دون حجب بسبب الأخبار."


def _signal_rejection_reasons(result):
    reasons = []
    if result.get("direction") not in ("BUY", "SELL"):
        reasons.append("لا يوجد اتجاه BUY/SELL مؤكد من MTF")
    if float(result.get("score", 0) or 0) < MIN_TRADE_SCORE:
        reasons.append(f"الدرجة أقل من {MIN_TRADE_SCORE} نقطة")
    if result.get("liquidity_blocked"):
        reasons.append("إعادة اختبار السيولة INVALIDATED")
    if not result.get("trade") and result.get("direction") in ("BUY", "SELL"):
        reasons.append("فشل بناء Entry/SL/TP أو تحقق R:R")
    return reasons



def _daily_execution_core():
    """المحرك الموحد للصفقات داخل اليوم.

    التقرير التوضيحي اليومي و trade_now و auto_loop يجب أن يقرأوا نفس القرار:
    H1 للسياق -> M15 للتأكيد -> M5 للزناد. لا يوجد محرك ثانٍ مختلف للصفقة.
    """
    h1 = analyze(get_bars("1h", 300))
    m15 = analyze(get_bars("15m", 300))
    m5 = analyze(get_bars("5m", 300))
    price = float(live_price()["price"])
    levels = support_resistance(get_bars("1h", 250))
    frames = {"H1": h1, "M15": m15, "M5": m5}
    buy_score, buy_factors = _scenario_score(frames, [("H1", 30), ("M15", 25), ("M5", 20)], "BUY", levels, price, float(m15.get("atr", 0) or 0), "daily")
    sell_score, sell_factors = _scenario_score(frames, [("H1", 30), ("M15", 25), ("M5", 20)], "SELL", levels, price, float(m15.get("atr", 0) or 0), "daily")
    direction = "BUY" if buy_score > sell_score else "SELL" if sell_score > buy_score else "WAIT"
    scenario_score = max(buy_score, sell_score)
    factors = buy_factors if direction == "BUY" else sell_factors if direction == "SELL" else []

    # السيولة التنفيذية هي M15 لأنها أقرب فريم للدخول. الحالات النهائية القديمة
    # لا تُعامل كحالة نشطة، لكن INVALIDATED الحالية تمنع الدخول فقط.
    liquidity = m15.get("liquidity", {}) if isinstance(m15.get("liquidity", {}), dict) else {}
    retest = liquidity.get("retest", {}) if isinstance(liquidity.get("retest", {}), dict) else {}
    liquidity_state = retest.get("state")
    liquidity_blocked = liquidity_state == "INVALIDATED"

    # لا نضيف نقاطاً اصطناعية؛ درجة السيناريو نفسها هي درجة قرار اليوم.
    final_score = int(max(0, min(100, scenario_score)))
    quality, quality_icon = trade_quality(final_score)
    valid = (direction in ("BUY", "SELL") and final_score >= MIN_TRADE_SCORE and not liquidity_blocked)
    trade = build_trade(direction, h1, m15, levels) if valid else None
    if valid and trade is None:
        valid = False
        factors.append("تعذر بناء Entry/SL/TP من نفس بيانات التقرير")
    elif trade and float(trade.get("rr", 0) or 0) < 1.20:
        valid = False
        trade = None
        factors.append("R:R أقل من 1:1.20")

    return {
        "signal": valid, "direction": direction, "score": final_score,
        "quality": quality, "quality_icon": quality_icon, "price": price,
        "levels": levels, "liquidity_blocked": liquidity_blocked,
        "liquidity_state": liquidity_state, "liquidity": liquidity,
        "factors": factors, "trade": trade,
        "daily": {"h1": h1, "m15": m15, "m5": m5,
                  "buy_score": buy_score, "sell_score": sell_score,
                  "buy_factors": buy_factors, "sell_factors": sell_factors},
    }


def evaluate_signal():
    """الإشارة الموحدة: قرار الصفقة اليومي هو المرجع التنفيذي الوحيد."""
    global LAST_ANALYSIS
    core = _daily_execution_core()
    # الفريمات الأعلى تبقى للسياق فقط ولا تملك صلاحية إسقاط قرار H1/M15/M5.
    try:
        w1 = analyze(get_bars("1w", 250))
        d1 = analyze(get_bars("1d", 300))
        h4 = analyze(get_bars("4h", 300))
    except Exception:
        logger.exception("تعذر جلب السياق الاستراتيجي؛ سيتم استخدام محرك اليوم وحده")
        w1 = d1 = h4 = {}

    h1 = core["daily"]["h1"]; m15 = core["daily"]["m15"]; m5 = core["daily"]["m5"]
    mtf = {
        "w1": w1, "d1": d1, "h4": h4, "h1": h1, "m15": m15, "m5": m5,
        "direction": core["direction"], "score": core["score"],
        "buy_score": core["daily"]["buy_score"],
        "sell_score": core["daily"]["sell_score"],
        "net_score": core["daily"]["buy_score"] - core["daily"]["sell_score"],
        "agreement": "موحد: H1 + M15 + M5",
        "conflict": (h1.get("direction") in ("BUY", "SELL") and m5.get("direction") in ("BUY", "SELL") and h1.get("direction") != m5.get("direction")),
    }

    try:
        institutional = institutional_analysis(get_bars("1h", 250))
    except Exception:
        institutional = {}

    result = dict(core)
    result["mtf"] = mtf
    result["institutional"] = institutional
    result["news_blocked"] = False
    # الأخبار معلوماتية فقط ولا تمنع الصفقة.
    try:
        _, result["news"] = news_filter()
    except Exception:
        result["news"] = "تعذر تحديث الأخبار؛ التداول غير محجوب."
    result["rejection_reasons"] = _signal_rejection_reasons(result)
    LAST_ANALYSIS = result
    logger.info(
        "EXECUTION_DECISION direction=%s score=%s signal=%s buy=%s sell=%s liquidity=%s reasons=%s",
        result["direction"], result["score"], result["signal"],
        result["daily"]["buy_score"], result["daily"]["sell_score"],
        result["liquidity_state"], result["rejection_reasons"]
    )
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
    # last_update is a wall-clock persistence timestamp and must NOT be used
    # as the M1 market-data cursor. Use a dedicated bar cursor instead.
    cutoff = pd.to_datetime(trade.get("last_m1_bar_time") or trade.get("time"), utc=True, errors="coerce")
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
        # Record the last processed completed M1 bar independently of wall-clock persistence time.
        trade["last_m1_bar_time"] = bar_dt.isoformat() if bar_dt is not None and not pd.isna(bar_dt) else stamp
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
        new_record={"time":now,"direction":result["direction"],"score":result["score"],"quality":result["quality"],"entry":float(t["entry"]),"sl":float(t["sl"]),"tp1":float(t["tp1"]),"tp2":float(t["tp2"]),"tp3":float(t["tp3"]),"rr":float(t["rr"]),"status":"ACTIVE","result":"OPEN","liquidity_state":result.get("mtf",{}).get("m15",{}).get("liquidity",{}).get("retest",{}).get("state"),"retest_status":liquidity_retest_summary(result.get("mtf",{}).get("m15",{}).get("liquidity",{})),"last_update":now,"last_m1_bar_time":now,"last_price":price,"tp1_time":None,"tp2_time":None,"tp3_time":None,"close_time":None}
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
        # الأخبار تنبيهات فقط ولا تمنع إنشاء/عرض الصفقة.
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


def _scenario_score(frames_data, frames, direction, levels, price, atr, horizon=None):
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
    if horizon is None:
        horizon = "weekly" if any(name == "W1" for name, _ in frames) else "daily"
    liq_frame = frames_data.get("H1" if horizon == "daily" else "D1", {}).get("liquidity", {})
    if direction == "BUY" and liq_frame.get("sweep") == "SELL_SIDE_SWEEP":
        score += 8; factors.append("سحب سيولة بيعية يدعم السيناريو الشرائي")
    elif direction == "SELL" and liq_frame.get("sweep") == "BUY_SIDE_SWEEP":
        score += 8; factors.append("سحب سيولة شرائية يدعم السيناريو البيعي")
    if liq_frame.get("retest", {}).get("state") == "CONFIRMED":
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
    def target_text(targets):
        vals = [fmt(x) for x in targets if x is not None]
        return " ثم ".join(vals) if vals else "غير محددة من المستويات الحالية"

    label = "الأسبوعي" if horizon == "weekly" else "اليومي"
    lines = [
        f"📝 التقرير التوضيحي {label} — XAU/USD", "━━━━━━━━━━━━━━━━━━━━",
        f"🕐 توقيت دمشق: {now_damascus().strftime('%Y-%m-%d %H:%M')}",
        f"💰 السعر الحالي: {fmt(price)}",
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
        f"• جودة السيناريو الرئيسي: {primary['quality']} نقطة / 100 — {_quality_label(primary['quality'])}",
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
    return f"📊 التحليل اليومي {VERSION}\n━━━━━━━━━━━━━━━━━━\n💰 السعر: {price:.2f}\n🎯 التوجيه: {'🟢 شراء' if direction=='BUY' else '🔴 بيع' if direction=='SELL' else '🟡 انتظار'}\n💪 قوة اليوم: {max(buy,sell)} نقطة\n\nH1: {h1['direction']} | RSI {h1['rsi']:.1f} | ADX {h1['adx']:.1f}\nM15: {m15['direction']} | RSI {m15['rsi']:.1f} | ADX {m15['adx']:.1f}\nM5: {m5['direction']} | RSI {m5['rsi']:.1f} | ADX {m5['adx']:.1f}\n\n📍 S1: {format_zone(levels,'support1')}\n📍 R1: {format_zone(levels,'resistance1')}\n\n💧 السيولة: {m15['liquidity']['bias']}\n🧲 Buy-side: {fmt(m15['liquidity']['nearest_buy'])} | Sell-side: {fmt(m15['liquidity']['nearest_sell'])}\n🔄 السحب: {m15['liquidity']['sweep_text']}"


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
    return f"📅 التحليل الأسبوعي {VERSION}\n━━━━━━━━━━━━━━━━━━\n💰 السعر: {price:.2f}\n🎯 الاتجاه الاستراتيجي: {'🟢 شراء' if direction=='BUY' else '🔴 بيع' if direction=='SELL' else '🟡 حياد'}\n💪 قوة الاتجاه: {max(buy,sell)} نقطة\n\nW1: {w1['direction']} | قوة {w1['score']} | RSI {w1['rsi']:.1f} | ADX {w1['adx']:.1f}\nD1: {d1['direction']} | قوة {d1['score']} | RSI {d1['rsi']:.1f} | ADX {d1['adx']:.1f}\nH4: {h4['direction']} | قوة {h4['score']} | RSI {h4['rsi']:.1f} | ADX {h4['adx']:.1f}\n\n📍 الدعم الأسبوعي: {format_zone(levels,'support1')}\n📍 المقاومة الأسبوعية: {format_zone(levels,'resistance1')}\n\n💧 السيولة: {d1['liquidity']['bias']}\n🧲 Buy-side: {fmt(d1['liquidity']['nearest_buy'])} | Sell-side: {fmt(d1['liquidity']['nearest_sell'])}\n🔄 السحب: {d1['liquidity']['sweep_text']}"


def build_daily_report():
    """التقرير اليومي والصفقة المحتملة مبنيان من نفس بيانات القرار."""
    core = _daily_execution_core()
    frames_data = {"H1": core["daily"]["h1"], "M15": core["daily"]["m15"], "M5": core["daily"]["m5"]}
    price, levels = core["price"], core["levels"]
    primary, alternative, _, _ = _make_scenarios(frames_data, levels, price, frames_data["M15"]["atr"], "daily")
    execution_lines = [
        "📌 التركيز: الاتجاه داخل اليوم وليس الاتجاه الاستراتيجي الطويل.",
        "⏱ تسلسل القرار الموحد: H1 للسياق → M15 للتأكيد → M5 للزناد.",
        f"🎯 محرك الصفقة الموحد: {'🟢 صفقة مؤهلة' if core['signal'] else '🟡 مراقبة — لا صفقة الآن'} | {core['direction']} | {core['score']} نقطة",
    ]
    if core["signal"] and core.get("trade"):
        t = core["trade"]
        execution_lines.append(f"📍 Entry: {t['entry']:.2f} | SL: {t['sl']:.2f} | TP1: {t['tp1']:.2f} | TP2: {t['tp2']:.2f} | TP3: {t['tp3']:.2f} | R:R 1:{t['rr']:.2f}")
    else:
        reasons = core.get("rejection_reasons") or []
        if reasons:
            execution_lines.append("🔎 سبب الانتظار: " + " | ".join(reasons))
    return _format_explanatory_report(frames_data, levels, price, primary, alternative, "daily", execution_lines)


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
    state, _ = market_state(now)
    now = now or now_damascus()
    return f"{state} | 🕐 دمشق {now.strftime('%H:%M')}\n━━━━━━━━━━━━━━━━━━\n"


def with_market_header(text):
    return market_header() + text


async def _ui_show(update, text, reply_markup=None, *, edit_callback=True):
    """Unified Telegram UI renderer: edit callback screens, reply to normal messages."""
    payload = with_market_header(text)
    try:
        if edit_callback and getattr(update, "callback_query", None):
            message = update.callback_query.message
            await message.edit_text(payload, reply_markup=reply_markup)
        elif getattr(update, "message", None):
            await update.message.reply_text(payload, reply_markup=reply_markup)
        elif getattr(update, "effective_message", None):
            await update.effective_message.reply_text(payload, reply_markup=reply_markup)
    except Exception as exc:
        # A repeated refresh may legitimately produce MessageNotModified; do not hide other failures.
        if exc.__class__.__name__ == "MessageNotModified":
            return
        logger.exception("Telegram UI render error")
        if edit_callback and getattr(update, "callback_query", None):
            try:
                await update.callback_query.message.reply_text(payload, reply_markup=reply_markup)
            except Exception:
                logger.exception("Telegram UI fallback render error")


async def reply(update, text, reply_markup=None):
    """Backward-compatible reply helper with callback-message editing."""
    await _ui_show(update, text, reply_markup=reply_markup)


def _kb(rows):
    return InlineKeyboardMarkup(rows)


def _nav_row(back="nav_home", back_text="↩️ رجوع"):
    return [InlineKeyboardButton(back_text, callback_data=back)]


def _main_inline_keyboard():
    return _kb([
        [InlineKeyboardButton("📊 السوق الآن", callback_data="nav_market"), InlineKeyboardButton("🧠 التحليل", callback_data="nav_analysis")],
        [InlineKeyboardButton("🎯 الصفقة", callback_data="nav_trade"), InlineKeyboardButton("💧 المستويات", callback_data="nav_levels")],
        [InlineKeyboardButton("📑 التقارير", callback_data="nav_reports"), InlineKeyboardButton("👤 الحساب", callback_data="nav_account")],
    ])


async def start(update, context):
    await asyncio.to_thread(_ensure_user, update)
    keyboard = [
        ["📊 السوق الآن", "🧠 التحليل"],
        ["🎯 الصفقة", "💧 المستويات"],
        ["📑 التقارير", "👤 حسابي"],
    ]
    if is_admin_chat(update.effective_user.id if update.effective_user else update.effective_chat.id):
        keyboard.append(["🛡️ مراقب الأداء"])
    text = (
        f"🤖 XAU SMART TRADER {VERSION}\n\n"
        "🥇 محلل الذهب XAU/USD\n\n"
        "📌 W1 • D1 • H4 • H1 • M15\n"
        "🧠 Structure • Momentum • Volume • Fibonacci • FVG\n\n"
        f"🎯 حد الإشارة: {SIGNAL_THRESHOLD} نقطة\n"
        "🔔 التنبيهات تعمل تلقائياً حسب الباقة\n\n"
        "اختر القسم الذي تريد الوصول إليه 👇"
    )
    await update.message.reply_text(
        with_market_header(text),
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
    )


async def _market_menu(update, context):
    if not await feature_guard("markets")(update, context): return
    await _ui_show(update, "📊 السوق الآن\n\nاختر ما تريد متابعته:", _kb([
        [InlineKeyboardButton("💰 سعر الذهب", callback_data="market_price"), InlineKeyboardButton("🌍 الجلسات", callback_data="market_sessions")],
        [InlineKeyboardButton("📰 الأخبار", callback_data="market_news"), InlineKeyboardButton("🟢 حالة النظام", callback_data="market_status")],
        _nav_row(),
    ]))


async def _analysis_menu(update, context):
    if not await feature_guard("full_analysis")(update, context): return
    await _ui_show(update, "🧠 التحليل\n\nاختر مستوى التحليل:", _kb([
        [InlineKeyboardButton("⚡ سريع", callback_data="analysis_quick"), InlineKeyboardButton("📊 كامل", callback_data="analysis_full")],
        [InlineKeyboardButton("📝 اليومي", callback_data="analysis_daily"), InlineKeyboardButton("📅 الأسبوعي", callback_data="analysis_weekly")],
        [InlineKeyboardButton("🏦 السياق المؤسسي", callback_data="analysis_institutional")],
        _nav_row(),
    ]))


async def _trade_menu(update, context):
    await _ui_show(update, "🎯 الصفقات\n\nأهم أدوات التداول في مكان واحد:", _kb([
        [InlineKeyboardButton("🚨 الصفقة الآن", callback_data="trade_now_ui"), InlineKeyboardButton("📜 سجل الصفقات", callback_data="trade_history_ui")],
        [InlineKeyboardButton("🔔 التنبيهات", callback_data="trade_alerts_ui")],
        _nav_row(),
    ]))


async def _levels_menu(update, context):
    if not await feature_guard("sr")(update, context): return
    await _ui_show(update, "💧 المستويات والسيولة\n\nاختر ما تريد مراقبته:", _kb([
        [InlineKeyboardButton("📍 الدعم والمقاومة", callback_data="levels_sr"), InlineKeyboardButton("💧 مناطق السيولة", callback_data="levels_liquidity")],
        [InlineKeyboardButton("🔄 حالة إعادة الاختبار", callback_data="levels_retest")],
        _nav_row(),
    ]))


async def _retest_menu(update, context):
    if not await feature_guard("sr")(update, context):
        return
    try:
        result = await asyncio.to_thread(evaluate_signal)
        liq = result.get("mtf", {}).get("m15", {}).get("liquidity", {})
        retest = liq.get("retest", {}) if isinstance(liq, dict) else {}
        text = ("🔄 حالة إعادة الاختبار\n━━━━━━━━━━━━━━━━━━\n"
                f"• الحالة: {liquidity_retest_summary(liq)}\n"
                f"• المستوى: {fmt(retest.get('level'))}\n"
                f"• النتيجة: {retest.get('result') or 'قيد التقييم'}")
        await _ui_show(update, text, _kb([_nav_row("nav_levels")]))
    except Exception as e:
        await _ui_show(update, f"🔄 حالة إعادة الاختبار\n\n❌ تعذر التحديث: {e}", _kb([_nav_row("nav_levels")]))


async def _reports_menu(update, context):
    await _ui_show(update, "📑 التقارير\n\nابدأ بالملخص، ثم افتح التفاصيل عند الحاجة:", _kb([
        [InlineKeyboardButton("📝 التقرير اليومي", callback_data="report_daily"), InlineKeyboardButton("📅 التقرير الأسبوعي", callback_data="report_weekly")],
        [InlineKeyboardButton("📈 أداء الصفقات", callback_data="report_performance")],
        _nav_row(),
    ]))


async def _account_menu(update, context):
    await _ui_show(update, "👤 الحساب\n\nإدارة اشتراكك والوصول إلى المزايا:", _kb([
        [InlineKeyboardButton("💎 اشتراكي", callback_data="account_subscription"), InlineKeyboardButton("💳 الباقات", callback_data="account_plans")],
        [InlineKeyboardButton("👥 الإحالة", callback_data="account_referral"), InlineKeyboardButton("ℹ️ المساعدة", callback_data="account_help")],
        _nav_row(),
    ]))


async def _trade_alerts_ui(update, context):
    await subscribe(update, context)


async def _performance_ui(update, context):
    if not await feature_guard("trade_history")(update, context):
        return
    try:
        await asyncio.to_thread(update_trade_results)
        trades = list(TRADE_HISTORY)
        total = len(trades)
        closed = [t for t in trades if t.get("status") == "CLOSED" or t.get("result") not in (None, "OPEN", "ACTIVE")]
        wins = sum(1 for t in closed if str(t.get("result", "")).upper() in {"TP3", "FULL_SUCCESS", "SUCCESS"})
        failures = sum(1 for t in closed if str(t.get("result", "")).upper() in {"SL", "FAILED", "LOSS"})
        avg_score = sum(float(t.get("score") or 0) for t in trades) / total if total else 0.0
        avg_rr = sum(float(t.get("rr") or 0) for t in trades) / total if total else 0.0
        win_rate = 100.0 * wins / len(closed) if closed else 0.0
        text = ("📈 أداء الصفقات\n━━━━━━━━━━━━━━━━━━\n"
                f"• إجمالي الصفقات: {total}\n• الصفقات المغلقة: {len(closed)}\n"
                f"• الناجحة: {wins}\n• الخاسرة: {failures}\n"
                f"• Win Rate: {win_rate:.1f}%\n• متوسط النقاط: {avg_score:.1f}\n"
                f"• متوسط R:R: 1:{avg_rr:.2f}")
        await _ui_show(update, text, _kb([
            [InlineKeyboardButton("📜 سجل الصفقات", callback_data="trade_history_ui")],
            _nav_row("nav_reports"),
        ]))
    except Exception as e:
        await _ui_show(update, f"📈 أداء الصفقات\n\n❌ تعذر إنشاء التقرير: {e}", _kb([_nav_row("nav_reports")]))


async def full_analysis(update, context):
    if not await feature_guard("full_analysis")(update, context): return
    try:
        can_trade = await asyncio.to_thread(can_receive_trade, update.effective_chat.id)
        await reply(update, await asyncio.to_thread(build_analysis, can_trade))
    except Exception as e:
        await reply(update, f"❌ تعذر تنفيذ التحليل.\nالسبب: {e}")


async def quick_analysis(update, context):
    if not await feature_guard("quick_analysis")(update, context): return
    try:
        df, m15, result = await asyncio.to_thread(_quick_analysis_data)
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
        if score >= MIN_TRADE_SCORE and not conflict:
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
        # الأخبار معلوماتية فقط ولا تغيّر التوصية.
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
        # لا يوجد أي حجب للصفقة بسبب الأخبار. الأخبار تصل كتنبيهات مستقلة.
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
            "🚨 XAU SMART TRADER\n━━━━━━━━━━━━━━━━━━\n"
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
    df = get_bars("15m", 220)
    m15 = analyze(df)
    result = evaluate_signal()
    return df, m15, result


async def show_levels(update, context):
    if not await feature_guard("sr")(update, context): return
    try:
        levels, price, liq = await asyncio.to_thread(_show_levels_data)
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
        await reply(update, f"💰 XAU/USD — السعر اللحظي\n\nالسعر: {q['price']:.2f}\nالمصدر: {q['source']}\nعمر السعر: {q.get('age')}\nتوقيت دمشق: {now_damascus().strftime('%Y-%m-%d %H:%M:%S')}", _kb([_nav_row("nav_market")]))
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
    _, text = await asyncio.to_thread(news_filter)
    await reply(update, f"📰 تنبيهات الأخبار\n\n🟢 التداول غير محجوب بسبب الأخبار\n\n{text}\n\n🔔 الأخبار تصل كتنبيهات تلقائية ولا توقف الصفقات.")


async def markets(update, context):
    if not await feature_guard("markets")(update, context): return
    now = now_damascus()
    await reply(update, f"🌍 جلسات السيولة\n\n🇯🇵 آسيا: تجميع ومراقبة\n🇬🇧 لندن: ارتفاع السيولة\n🇺🇸 نيويورك: أعلى التقلبات\n\n🕐 توقيت دمشق الآن: {now.strftime('%H:%M:%S')}\n\n⚠️ أوقات الافتتاح تتغير موسمياً بسبب التوقيت الصيفي.")


async def audit_report(update, context):
    """تقرير مراقب الأداء — للإدارة فقط، قراءة فقط ولا يؤثر على التداول."""
    user_id = update.effective_user.id if update.effective_user else update.effective_chat.id
    if not (is_admin_chat(user_id) or is_admin_chat(update.effective_chat.id)):
        await reply(update, "⛔ مراقب الأداء مخصص للإدارة فقط.")
        return
    if PERFORMANCE_AUDITOR is None:
        await reply(update, "🛡️ مراقب الأداء\n━━━━━━━━━━━━━━━━━━\n❌ محرك المراقبة غير متاح حالياً.\nتحقق من متطلبات تشغيل النسخة الموحّدة.")
        return
    try:
        report = await asyncio.to_thread(PERFORMANCE_AUDITOR.metrics_report)
        t = report.get("trades", {})
        d = report.get("daily", {})
        w = report.get("weekly", {})
        lines = [
            "🛡️ XAU SMART TRADER — مراقب الأداء",
            "━━━━━━━━━━━━━━━━━━",
            "",
            "📊 الصفقات",
            f"• الإجمالي: {t.get('total', 0)}",
            f"• نجاح كامل: {t.get('full_success', 0)}",
            f"• نجاح جزئي: {t.get('partial_success', 0)}",
            f"• فشل: {t.get('failed', 0)}",
            f"• منتهية: {t.get('expired', 0)}",
            f"• غامضة: {t.get('ambiguous', 0)}",
            f"• Win Rate: {t.get('win_rate_pct', 0):.2f}%",
            f"• Full Target Rate: {t.get('full_target_rate_pct', 0):.2f}%",
            f"• متوسط R:R: {t.get('avg_rr', 0):.2f}",
            f"• متوسط MAE: {t.get('avg_mae', 0):.4f}",
            f"• متوسط MFE: {t.get('avg_mfe', 0):.4f}",
            f"• متوسط Drawdown: {t.get('avg_drawdown', 0):.4f}",
            "",
            "📅 دقة التحليل اليومي",
            f"• التحليلات: {d.get('total', 0)} | المطابقة: {d.get('matched', 0)}",
            f"• الدقة: {d.get('accuracy_pct', 0):.2f}%",
            f"• الاتجاه: {d.get('direction_accuracy_avg', 0):.2f}%",
            f"• النطاق: {d.get('range_accuracy_avg', 0):.2f}%",
            f"• الهدف: {d.get('target_accuracy_avg', 0):.2f}%",
            "",
            "📆 دقة التحليل الأسبوعي",
            f"• التحليلات: {w.get('total', 0)} | المطابقة: {w.get('matched', 0)}",
            f"• الدقة: {w.get('accuracy_pct', 0):.2f}%",
            f"• الاتجاه: {w.get('direction_accuracy_avg', 0):.2f}%",
            f"• النطاق: {w.get('range_accuracy_avg', 0):.2f}%",
            f"• الهدف: {w.get('target_accuracy_avg', 0):.2f}%",
            "",
            "🔒 المراقب مستقل ولا يغيّر قرارات التداول أو العتبات.",
        ]
        buttons = [
            [InlineKeyboardButton("🔄 تحديث المراقب", callback_data="menu_audit")],
            _nav_row("nav_home"),
        ]
        await _ui_show(update, "\n".join(lines), InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.exception("Audit report error")
        await reply(update, f"🛡️ تعذر إنشاء تقرير المراقب.\nالسبب: {e}")


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
    await query.edit_message_text(with_market_header(text), reply_markup=InlineKeyboardMarkup(buttons))


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
            f"📩 طلب الاشتراك — {plan['name']}\n━━━━━━━━━━━━━━━━━━\n"
            f"💰 السعر: ${plan['price']} / شهر\n"
            f"🎯 {TRADE_LIMIT_TEXT[plan_key]}\n\n"
            "تم تسجيل طلبك. التفعيل المدفوع يتم بعد تأكيد الإدارة." + contact_text
        ),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 العودة للباقات", callback_data="back_to_plans")]])
    )


async def callback_router(update, context):
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    routes = {
        "nav_home": home_menu,
        "nav_market": _market_menu,
        "nav_analysis": _analysis_menu,
        "nav_trade": _trade_menu,
        "nav_levels": _levels_menu,
        "nav_reports": _reports_menu,
        "nav_account": _account_menu,
        "market_price": gold_price,
        "market_sessions": markets,
        "market_news": news_status,
        "market_status": status,
        "analysis_quick": quick_analysis,
        "analysis_full": full_analysis,
        "analysis_daily": daily_analysis,
        "analysis_weekly": weekly_analysis,
        "analysis_institutional": institutional_menu,
        "trade_now_ui": trade_now,
        "trade_history_ui": trade_history,
        "trade_alerts_ui": _trade_alerts_ui,
        "levels_sr": show_levels,
        "levels_liquidity": liquidity_menu,
        "levels_retest": _retest_menu,
        "report_daily": daily_report,
        "report_weekly": weekly_report,
        "report_performance": _performance_ui,
        "account_subscription": my_subscription,
        "account_plans": plans,
        "account_referral": referral,
        "account_help": help_menu,
        "menu_audit": audit_report,
    }
    if data.startswith("plan_"):
        await plan_callback(update, context)
        return
    if data.startswith("request_"):
        await subscription_request_callback(update, context)
        return
    if data == "back_to_plans":
        await plans(update, context)
        return
    fn = routes.get(data)
    if fn:
        await query.answer()
        await fn(update, context)
    else:
        await query.answer("زر غير معروف.", show_alert=True)


async def my_subscription(update, context):
    await asyncio.to_thread(_ensure_user, update)
    text = await asyncio.to_thread(plan_status_text, update.effective_chat.id)
    await reply(update, "👤 اشتراكي\n━━━━━━━━━━━━━━━━━━\n" + text)


async def referral(update, context):
    await asyncio.to_thread(_ensure_user, update)
    row = await asyncio.to_thread(get_member, update.effective_chat.id)
    await reply(update, f"👥 رابط دعوتك\n\nhttps://t.me/" + (await APPLICATION.bot.get_me()).username + f"?start={row['referral_code']}\n\n🎁 نظام الإحالة جاهز للمكافآت والترقية.")


async def admin_command(update, context):
    user_id = update.effective_user.id if update.effective_user else update.effective_chat.id
    if not (is_admin_chat(user_id) or is_admin_chat(update.effective_chat.id)):
        await reply(update, "⛔ هذا الأمر مخصص للإدارة.")
        return
    args = list(context.args or [])
    command_name = (update.message.text or "").split()[0].lstrip("/").split("@")[0].lower() if getattr(update, "message", None) else "admin"
    if command_name == "activate":
        if len(args) != 3:
            await reply(update, "الصيغة: /activate USER_ID PLAN DAYS")
            return
        user_arg, plan_arg, days_arg = args
    else:
        if not args:
            await reply(update, "🛠 /admin\n\n/admin activate USER_ID PLAN DAYS\nمثال: /activate 123456789 PRO 30")
            return
        if args[0].lower() != "activate" or len(args) != 4:
            await reply(update, "الصيغة: /admin activate USER_ID PLAN DAYS")
            return
        _, user_arg, plan_arg, days_arg = args
    try:
        user_id, plan, days = int(user_arg), plan_arg.upper(), int(days_arg)
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
    if getattr(update, "callback_query", None):
        await _ui_show(update, "🏠 الرئيسية\n\nاختر القسم الذي تريد الوصول إليه:", _main_inline_keyboard())
    else:
        await start(update, context)


async def analyses_menu(update, context):
    await _analysis_menu(update, context)


async def trades_menu(update, context):
    await _trade_menu(update, context)


async def market_news_menu(update, context):
    await _market_menu(update, context)


async def institutional_menu(update, context):
    if not await feature_guard("institutional")(update, context): return
    try:
        result = await asyncio.to_thread(evaluate_signal)
        inst = result.get("institutional", {}) if isinstance(result, dict) else {}
        context_text = inst.get("context", "السياق المؤسسي المستنتج من السعر والهيكل والسيولة والحجم.") if isinstance(inst, dict) else "السياق المؤسسي المستنتج من السعر والهيكل والسيولة والحجم."
        await reply(update, "🏦 السياق المؤسسي\n━━━━━━━━━━━━━━━━━━\n🧠 القراءة\n" + str(context_text) + "\n\n📡 البيانات المباشرة\n⚪ تعتمد على توفر مصدر مباشر؛ لا يتم اختلاق بيانات غير متاحة.", _kb([_nav_row("nav_analysis")]))
    except Exception as e:
        await reply(update, f"🏦 السياق المؤسسي\n\n❌ تعذر إنشاء القسم: {e}", _kb([_nav_row("nav_analysis")]))


async def liquidity_menu(update, context):
    if not await feature_guard("sr")(update, context): return
    try:
        result = await asyncio.to_thread(evaluate_signal)
        liq = result.get("mtf", {}).get("m15", {}).get("liquidity", {})
        await reply(update, "💧 مناطق السيولة\n━━━━━━━━━━━━━━━━━━\n"
                    f"🧭 الانحياز: {liq.get('bias','غير متوفر')}\n"
                    f"🧲 Buy-side: {fmt(liq.get('nearest_buy'))}\n"
                    f"🧲 Sell-side: {fmt(liq.get('nearest_sell'))}\n"
                    f"🔄 السحب: {liq.get('sweep_text','غير متوفر')}\n"
                    f"🔁 إعادة الاختبار: {liquidity_retest_summary(liq)}", _kb([_nav_row("nav_levels")]))
    except Exception as e:
        await reply(update, f"💧 مناطق السيولة\n\n❌ تعذر تحديث السيولة: {e}", _kb([_nav_row("nav_levels")]))


async def help_menu(update, context):
    await reply(update, "ℹ️ المساعدة\n\nاستخدم الأقسام الرئيسية للوصول إلى السوق والتحليل والصفقات والمستويات والتقارير والحساب.\n\n⚠️ جميع الإشارات تحليلية وليست ضماناً للربح.", _kb([_nav_row("nav_account")]))


def _normalize_ui_text(value):
    """Normalize Telegram ReplyKeyboard text so visually identical labels route identically."""
    import unicodedata
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = text.replace("\ufe0f", "").replace("\ufe0e", "")
    text = text.replace("\u200b", "").replace("\u200c", "").replace("\u200d", "")
    return " ".join(text.split()).strip()


async def router(update, context):
    raw_text = update.message.text if getattr(update, "message", None) else ""
    text = _normalize_ui_text(raw_text)
    routes = {
        "🏠 الرئيسية": home_menu,
        "📊 السوق الآن": _market_menu,
        "🧠 التحليل": _analysis_menu,
        "🎯 الصفقة": _trade_menu,
        "💧 المستويات": _levels_menu,
        "📑 التقارير": _reports_menu,
        "👤 حسابي": _account_menu,
        "🛡️ مراقب الأداء": audit_report,
        # Legacy visible text remains supported without being exposed in the main UI.
        "⚡ التحليل السريع": quick_analysis,
        "📊 التحليلات": analyses_menu,
        "🎯 الصفقات": trades_menu,
        "🌍 السوق والأخبار": market_news_menu,
        "🏦 التحليل المؤسسي": institutional_menu,
        "🎯 مناطق السيولة": liquidity_menu,
        "🔔 التنبيهات": subscribe,
        "💰 سعر الذهب": gold_price,
        "💳 الباقات": plans,
        "👤 اشتراكي": my_subscription,
        "🟢 حالة النظام": status,
        "ℹ️ المساعدة": help_menu,
        "🎯 صفقة الآن": trade_now,
        "📍 الدعوم والمقاومات": show_levels,
        "📜 سجل الصفقات": trade_history,
        "📝 التقرير التوضيحي اليومي": daily_report,
        "📅 التقرير التوضيحي الأسبوعي": weekly_report,
        "📰 الأخبار": news_status,
        "🌍 الأسواق": markets,
        "📊 التحليل الكامل": full_analysis,
    }
    normalized_routes = {_normalize_ui_text(key): fn for key, fn in routes.items()}
    fn = normalized_routes.get(text)
    if fn:
        logger.info("UI route: %r -> %s", raw_text, getattr(fn, "__name__", repr(fn)))
        await fn(update, context)
    else:
        logger.info("UI route not found: %r", raw_text)
        await start(update, context)


# ============================================================
# Webhook
# ============================================================

def _log_webhook_future(future):
    """Log asynchronous update failures without blocking the HTTP webhook response."""
    try:
        future.result()
    except Exception:
        logger.exception("Webhook update processing error")


@app.route(WEBHOOK_PATH, methods=["POST"])
def webhook():
    if APPLICATION is None or BOT_LOOP is None or BOT_LOOP.is_closed():
        return "Bot not ready", 503
    # Verify Telegram's secret header before parsing attacker-controlled JSON.
    supplied_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    # Secret-token validation is enforced only when a secret is configured.
    # If TELEGRAM_WEBHOOK_SECRET is empty, Telegram is intentionally configured
    # without a secret and valid webhook requests must not be rejected.
    if TELEGRAM_WEBHOOK_SECRET:
        if not supplied_secret or not hmac.compare_digest(supplied_secret, TELEGRAM_WEBHOOK_SECRET):
            return "Forbidden", 403
    try:
        data = request.get_json(silent=False)
        if not isinstance(data, dict):
            return "Bad Request", 400
        update = Update.de_json(data, APPLICATION.bot)
        # Critical architecture rule: webhook acknowledgement is independent of handler
        # execution. Telegram must receive HTTP 200 immediately after the update is safely
        # scheduled; slow analysis/DB/Telegram work must never turn a valid update into 503.
        future = asyncio.run_coroutine_threadsafe(APPLICATION.process_update(update), BOT_LOOP)
        future.add_done_callback(_log_webhook_future)
        return "OK", 200
    except Exception:
        logger.exception("WEBHOOK_SCHEDULING_ERROR")
        return "Service Unavailable", 503

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
            await asyncio.to_thread(_release_trade_quota, quota_token)
        logger.exception("Signal send error for %s", chat_id)
        return False


def _alert_recipients(feature):
    """Return eligible recipients in one DB read; avoids per-user DB lookups."""
    conn = _db()
    try:
        rows = conn.execute("SELECT chat_id, plan, expiry_date FROM users WHERE status='active'").fetchall()
        result = set(ADMIN_IDS)
        now = now_damascus()
        for row in rows:
            plan = row["plan"] or "FREE"
            if plan not in PLANS or feature not in PLANS[plan]["features"]:
                continue
            expiry_raw = row["expiry_date"]
            if expiry_raw and plan != "FREE":
                try:
                    expiry = datetime.fromisoformat(expiry_raw)
                    if expiry.tzinfo is None:
                        expiry = expiry.replace(tzinfo=DAMASCUS)
                    if now >= expiry:
                        continue
                except (TypeError, ValueError):
                    continue
            result.add(int(row["chat_id"]))
        return result
    finally:
        conn.close()


async def send_market_session_alerts():
    """تنبيه افتتاح الجلسات/التداخلات مرة واحدة لكل حدث."""
    now = now_damascus()
    recipients = await asyncio.to_thread(_alert_recipients, "market_alerts")
    if not recipients or not APPLICATION:
        return
    specs = [("🇦🇺 سيدني", "Australia/Sydney", 8), ("🇯🇵 طوكيو", "Asia/Tokyo", 9),
             ("🇬🇧 لندن", "Europe/London", 8), ("🇺🇸 نيويورك", "America/New_York", 8)]
    for label, zone_name, hour in specs:
        local = now.astimezone(ZoneInfo(zone_name))
        if local.weekday() >= 5 or local.hour != hour or local.minute > 14:
            continue
        key = f"{label}:{local.date().isoformat()}"
        if SESSION_ALERT_STATE.get(key):
            continue
        SESSION_ALERT_STATE[key] = True
        _prune_alert_state(SESSION_ALERT_STATE)
        state, _ = market_state(now)
        text = (f"🔔 افتتاح جلسة جديدة\n━━━━━━━━━━━━━━━━━━\n{label}\n"
                f"{state}\n🕐 دمشق: {now.strftime('%H:%M')}\n"
                "📌 تابع السيولة والسبريد قبل اتخاذ أي قرار.")
        for cid in recipients:
            try:
                await APPLICATION.bot.send_message(chat_id=cid, text=with_market_header(text))
            except Exception:
                logger.exception("Session alert error for %s", cid)


async def send_news_alerts():
    """إرسال تنبيهات تلقائية للأخبار عالية التأثير فقط — بدون حجب للتداول."""
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
            key = f"{event.get('currency')}:{event.get('event')}:{dt.isoformat()}:{phase}"
            if NEWS_ALERT_STATE.get(key):
                continue
            NEWS_ALERT_STATE[key] = True
            _prune_alert_state(NEWS_ALERT_STATE)
            text = (f"📰 تنبيه خبر عالي التأثير\n━━━━━━━━━━━━━━━━━━\n"
                    f"{phase}\n💱 {event.get('currency','')}\n"
                    f"📌 {event.get('event','خبر اقتصادي')}\n"
                    f"🕐 {dt.strftime('%Y-%m-%d %H:%M')} دمشق\n"
                    "🔔 تنبيه معلوماتي فقط — التداول لا يُحجب بسبب الخبر.")
            for cid in recipients:
                try:
                    await APPLICATION.bot.send_message(chat_id=cid, text=with_market_header(text))
                except Exception:
                    logger.exception("News alert error for %s", cid)


async def _auto_wait():
    """Interruptible sleep so shutdown does not wait for the full 15-minute interval."""
    try:
        await asyncio.wait_for(SHUTDOWN_EVENT.wait(), timeout=AUTO_SCAN_SECONDS)
    except asyncio.TimeoutError:
        pass


async def auto_loop():
    """دورة آلية كل 15 دقيقة مع إيقاف منظم وآمن."""
    while not SHUTDOWN_EVENT.is_set():
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
                try:
                    await asyncio.wait_for(SHUTDOWN_EVENT.wait(), timeout=AUTO_SCAN_SECONDS)
                except asyncio.TimeoutError:
                    pass
                continue

            # لا نفتح صفقات جديدة في عطلة نهاية الأسبوع أو العطل المعلنة.
            closed_reason = await asyncio.to_thread(market_closed_reason)
            if closed_reason:
                logger.info("Auto scan skipped: market closed (%s)", closed_reason)
                await _auto_wait()
                continue

            result = await asyncio.to_thread(evaluate_signal)

            # هذه البوابات إلزامية قبل إنشاء/إرسال أي صفقة جديدة.
            if not result.get("signal"):
                logger.info("Auto signal rejected | score=%s direction=%s reasons=%s", result.get("score"), result.get("direction"), result.get("rejection_reasons"))
                await _auto_wait()
                continue
            # الأخبار لا تملك أي صلاحية لحجب التداول. التنبيهات تعمل بشكل مستقل.
            if result.get("liquidity_blocked"):
                logger.info("Auto trade blocked by liquidity retest gate")
                await _auto_wait()
                continue
            if not result.get("trade"):
                logger.info("Auto signal has score/direction but no trade | score=%s direction=%s reasons=%s", result.get("score"), result.get("direction"), result.get("rejection_reasons"))
                await _auto_wait()
                continue

            recipients = list(SUBSCRIBERS)
            quota_tokens = {}
            for chat_id in recipients:
                token = await asyncio.to_thread(_reserve_trade_quota, chat_id)
                if token:
                    quota_tokens[chat_id] = token
            if not quota_tokens:
                logger.info("Auto trade skipped: no recipient has remaining quota")
                await _auto_wait()
                continue

            record, is_new = await asyncio.to_thread(register_trade, result)
            if not record:
                for token in quota_tokens.values(): await asyncio.to_thread(_release_trade_quota, token)
                logger.warning("Auto trade registration failed; quotas released")
                await _auto_wait()
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
        await _auto_wait()


# ============================================================
# تشغيل Telegram
# ============================================================

async def shutdown_bot():
    """Graceful shutdown for Telegram, auto loop, auditor bridge, and event loop resources."""
    global APPLICATION, AUTO_TASK
    SHUTDOWN_EVENT.set()
    if AUTO_TASK is not None:
        AUTO_TASK.cancel()
        try:
            await AUTO_TASK
        except asyncio.CancelledError:
            pass
        AUTO_TASK = None
    try:
        AUDITOR_WORKER_STOP.set()
        if AUDITOR_WORKER_THREAD and AUDITOR_WORKER_THREAD.is_alive() and AUDITOR_WORKER_THREAD is not threading.current_thread():
            AUDITOR_WORKER_THREAD.join(timeout=2.0)
        AUDITOR_WORKER_THREAD = None
        if PERFORMANCE_AUDITOR is not None:
            PERFORMANCE_AUDITOR.stop()
    except Exception:
        logger.exception("AUDITOR_SHUTDOWN_ERROR")
    if APPLICATION is not None:
        try:
            await APPLICATION.stop()
        except Exception:
            logger.exception("TELEGRAM_STOP_ERROR")
        try:
            await APPLICATION.shutdown()
        except Exception:
            logger.exception("TELEGRAM_SHUTDOWN_ERROR")


async def start_bot():
    global APPLICATION, AUTO_TASK
    SHUTDOWN_EVENT.clear()
    if not TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN غير موجود في Render.")

    APPLICATION = Application.builder().token(TOKEN).build()
    APPLICATION.add_handler(CommandHandler("start", start))
    APPLICATION.add_handler(CommandHandler("plans", plans))
    APPLICATION.add_handler(CommandHandler("subscription", my_subscription))
    APPLICATION.add_handler(CommandHandler("referral", referral))
    APPLICATION.add_handler(CommandHandler("admin", admin_command))
    APPLICATION.add_handler(CommandHandler("audit", audit_report))
    APPLICATION.add_handler(CommandHandler("activate", admin_command))
    APPLICATION.add_handler(CallbackQueryHandler(callback_router))
    APPLICATION.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, router))

    await APPLICATION.initialize()
    await APPLICATION.start()
    webhook_kwargs = {
        "url": WEBHOOK_URL,
        "allowed_updates": ["message", "callback_query"],
        "drop_pending_updates": True,
    }
    if TELEGRAM_WEBHOOK_SECRET:
        webhook_kwargs["secret_token"] = TELEGRAM_WEBHOOK_SECRET
    await APPLICATION.bot.set_webhook(**webhook_kwargs)
    webhook_info = await APPLICATION.bot.get_webhook_info()
    logger.info(
        "WEBHOOK_READY url=%s pending=%s last_error=%r last_error_date=%r",
        webhook_info.url,
        webhook_info.pending_update_count,
        webhook_info.last_error_message,
        webhook_info.last_error_date,
    )

    logger.info("XAU SMART TRADER %s started", VERSION)
    logger.info("Webhook: %s", WEBHOOK_URL)
    AUTO_TASK = asyncio.create_task(auto_loop(), name="xau-auto-loop")
    if PERFORMANCE_AUDITOR is not None:
        try:
            _start_auditor_bridge_worker()
            PERFORMANCE_AUDITOR.start_background()
        except Exception:
            logging.getLogger(__name__).exception("AUDITOR_START_ERROR")

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
        if not loop.is_closed():
            try:
                loop.run_until_complete(shutdown_bot())
            except Exception:
                logger.exception("GRACEFUL_SHUTDOWN_ERROR")
        loop.close()

if __name__ == "__main__":
    main()
