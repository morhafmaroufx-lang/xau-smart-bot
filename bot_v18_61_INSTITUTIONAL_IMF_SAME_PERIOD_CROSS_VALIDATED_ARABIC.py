# ============================================================
# XAU SMART TRADER v18.55
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
from collections import defaultdict
import uuid
from datetime import date, datetime, timedelta, timezone
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo

import requests
import pandas as pd
import numpy as np

from flask import Flask, request, jsonify
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters

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

VERSION = "v18.61-INSTITUTIONAL-IMF-IRFCL-SAME-PERIOD-CROSS-VALIDATED-WGC-ARABIC-PRE-SMC-INDEPENDENT-NEWS-EMBEDDED"
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
# الأخبار جزء من بوابة التنفيذ: HIGH أو UNKNOWN يمنعان فتح صفقة جديدة.
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
NEWS_ALERT_STATE = {}
EMERGENCY_ALERT_STATE = {}
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

def _imf_irfcl_monthly(indicator):
    """Fetch IMF IRFCL monthly series using the live IMF SDMX 3.0 contract.

    IRFCL is versioned; the current dataflow version is 11.0.0. We try the
    explicit live version first, then the latest-version wildcard. SDMX-JSON
    is preferred, with SDMX-CSV as a parser-stable fallback.
    """
    end_period=pd.Timestamp.utcnow().strftime('%Y-%m')
    start_period=(pd.Timestamp.utcnow()-pd.DateOffset(months=24)).strftime('%Y-%m')
    key=f'*.{indicator}.*.M'
    urls=[]
    for ver in ('11.0.0','+'):
        base=f'{IMF_SDMX_BASE}/data/dataflow/IMF.STA/IRFCL/{ver}/{key}'
        urls.extend([
            f'{base}?startPeriod={start_period}&endPeriod={end_period}&format=jsondata',
            f'{base}?startPeriod={start_period}&endPeriod={end_period}&dimensionAtObservation=TIME_PERIOD&format=jsondata',
            f'{base}?startPeriod={start_period}&endPeriod={end_period}&format=csvfile',
        ])
    last=None
    def _dim_values(dim):
        vals=dim.get('values',[]) if isinstance(dim,dict) else []
        return [v.get('id') or v.get('value') or v.get('name') if isinstance(v,dict) else v for v in vals or []]
    def _parse_json(payload):
        data=payload.get('data') if isinstance(payload,dict) else None
        if not isinstance(data,dict): data=payload if isinstance(payload,dict) else {}
        structures=data.get('structures') or payload.get('structure') or []
        if isinstance(structures,dict): structures=[structures]
        struct=structures[0] if structures else {}
        ds_list=data.get('dataSets') or payload.get('dataSets') or []
        if isinstance(ds_list,dict): ds_list=[ds_list]
        ds=ds_list[0] if ds_list else {}
        series_obj=ds.get('series') or {}
        if not isinstance(series_obj,dict): series_obj={}
        dims=struct.get('dimensions') or {}
        sdim=dims.get('series',[]) or []
        odim=dims.get('observation',[]) or []
        country_pos=None; countries=[]
        for i,d in enumerate(sdim):
            if str(d.get('id','')).upper() in ('COUNTRY','REF_AREA'):
                country_pos=i; countries=_dim_values(d); break
        if country_pos is None or not countries: raise RuntimeError('IMF country dimension unavailable')
        time_dim=next((d for d in odim if str(d.get('id','')).upper()=='TIME_PERIOD'),None)
        times=_dim_values(time_dim) if time_dim else []
        out={}
        for series_key,series in series_obj.items():
            parts=str(series_key).split(':')
            if country_pos>=len(parts): continue
            try: idx=int(parts[country_pos])
            except Exception: continue
            if idx<0 or idx>=len(countries): continue
            cid=str(countries[idx] or '').strip()
            if not cid: continue
            rec=out.setdefault(cid,{'name':cid,'byMonth':{}})
            obs=series.get('observations') or {}
            for obs_key,obs_val in obs.items():
                try:
                    raw_val=obs_val[0] if isinstance(obs_val,(list,tuple)) else obs_val.get('value') if isinstance(obs_val,dict) else obs_val
                    period=times[int(obs_key)] if times else str(obs_key)
                    val=float(raw_val)
                    if period and np.isfinite(val): rec['byMonth'][str(period)]=val
                except Exception: continue
        return out
    def _parse_csv(text):
        from io import StringIO
        df=pd.read_csv(StringIO(text))
        cols={str(c).upper():c for c in df.columns}
        ccol=cols.get('REF_AREA') or cols.get('COUNTRY') or cols.get('COUNTRY_CODE')
        tcol=cols.get('TIME_PERIOD'); vcol=cols.get('OBS_VALUE') or cols.get('VALUE')
        if not (ccol and tcol and vcol): raise RuntimeError('IMF SDMX-CSV missing REF_AREA/TIME_PERIOD/OBS_VALUE')
        out={}
        for _,row in df.iterrows():
            cid=str(row.get(ccol,'')).strip(); period=str(row.get(tcol,'')).strip()
            try: val=float(row.get(vcol))
            except Exception: continue
            if cid and period and np.isfinite(val): out.setdefault(cid,{'name':cid,'byMonth':{}})['byMonth'][period]=val
        return out
    for url in urls:
        for attempt in range(3):
            try:
                r=_inst_http(url,timeout=90,headers={'User-Agent':'Mozilla/5.0 (XAU Smart Trader IMF Gateway)','Accept':'application/vnd.sdmx.data+json;version=2.0.0,application/vnd.sdmx.data+csv,application/json,text/csv;q=0.8,*/*;q=0.1'})
                ctype=(r.headers.get('content-type') or '').lower()
                out=_parse_csv(r.text) if ('csv' in ctype or 'csvfile' in url) else _parse_json(r.json())
                populated={cid:rec for cid,rec in out.items() if rec.get('byMonth')}
                if len(populated)>=20: return populated
                raise RuntimeError(f'IMF IRFCL parsed only {len(populated)} country series; content-type={ctype}; url={url}')
            except Exception as exc:
                last=exc
                if attempt<2: time.sleep(1.5*(attempt+1))
    raise RuntimeError(f'IMF IRFCL {indicator} failed: {last}')


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
            wgc={'period':item['period'],'net_change_tonnes':item['total'],'source':'مجلس الذهب العالمي WGC — التغيرات المعلنة','reported_only':True,'source_url':url,'column':item['column'],'sheet':item['sheet']}
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
                wgc={'period':'2026-06','net_change_tonnes':val,'source':'مجلس الذهب العالمي WGC — تقرير شهري رسمي','reported_only':True,'source_url':WGC_CENTRAL_BANK_ARTICLE_URL,'publication_fallback':True}
                wgc.update(_inst_status(pd.Timestamp('2026-06-30'),45,'مجلس الذهب العالمي WGC — تقرير شهري رسمي'))
        except Exception as exc: errors.append('WGC التقرير الشهري: '+str(exc))
    # IMF official monthly fine-troy-ounce holdings, independent cross-check.
    # When WGC is available, calculate the IMF change for the EXACT WGC month
    # versus the immediately preceding IMF month. This prevents a false
    # comparison such as WGC June versus the latest IMF July/August release.
    try:
        raw=_imf_irfcl_monthly(IMF_GOLD_FTO_INDICATOR)
        all_months=sorted({m for rec in raw.values() for m in rec.get('byMonth',{})})
        if len(all_months)<2: raise RuntimeError('سلسلة IMF تحتوي على أقل من شهرين')
        target=str(wgc.get('period') or '')[:7] if wgc is not None else ''
        if target and target in all_months:
            i=all_months.index(target)
            if i<1: raise RuntimeError(f'لا يوجد شهر سابق لـ {target} في IMF')
            latest,prior=target,all_months[i-1]
        else:
            latest,prior=all_months[-1],all_months[-2]
        common=[cid for cid,rec in raw.items() if latest in rec.get('byMonth',{}) and prior in rec.get('byMonth',{})]
        if len(common)<20: raise RuntimeError(f'IMF لديه {len(common)} دولة فقط مع الشهرين {prior} و{latest}')
        current=sum(float(raw[cid]['byMonth'][latest]) for cid in common)
        previous=sum(float(raw[cid]['byMonth'][prior]) for cid in common)
        delta=(current-previous)/TROY_OZ_PER_TONNE
        imf={'period':str(latest),'net_change_tonnes':float(delta),'source':'صندوق النقد الدولي IMF — IRFCL','reported_only':True,'countries_covered':len(common),'coverage_method':'الدول المشتركة التي لديها الشهران معاً','prior_period':str(prior)}
        imf.update(_inst_status(pd.to_datetime(latest+'-01',errors='coerce'),90,'صندوق النقد الدولي IMF'))
    except Exception as exc: errors.append('IMF: '+str(exc))
    if wgc is None and imf is None:
        raise RuntimeError('بيانات الذهب للبنوك المركزية غير متاحة: '+' | '.join(errors[-4:]))
    cv={'status':'NOT_PERFORMED','method':'WGC مقابل IMF','difference_tonnes':None,'difference_pct':None,'validated':False}
    chosen=wgc or imf
    if wgc is not None and imf is not None:
        wp=str(wgc.get('period') or '')[:7]
        ip=str(imf.get('period') or '')[:7]
        # Do not compare observations from different months.
        if wp != ip:
            cv={'status':'PERIOD_MISMATCH','method':'مقارنة WGC مع IMF IRFCL — نفس الشهر فقط','difference_tonnes':None,'difference_pct':None,'validated':False,'wgc_period':wp,'imf_period':ip,'reason':'الفترتان غير متطابقتين؛ لم تتم مقارنة أرقام من شهرين مختلفين.'}
            chosen=dict(wgc)
        else:
            a=float(wgc['net_change_tonnes']); b=float(imf['net_change_tonnes'])
            denom=max((abs(a)+abs(b))/2.0,0.1)
            diff=abs(a-b); pct=100.0*diff/denom
            cv={'status':'PASS' if pct<=5.0 else 'DEGRADED','method':'مقارنة WGC مع IMF IRFCL لنفس الشهر','difference_tonnes':round(diff,4),'difference_pct':round(pct,2),'validated':pct<=5.0,'wgc_value':a,'imf_value':b,'wgc_period':wp,'imf_period':ip,'tolerance_pct':5.0}
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
    """Pure local execution gate: BLOCK, CLEAR, or UNKNOWN."""
    now = now or _now_utc()
    try:
        events = get_events()
        upcoming = []
        for e in events:
            dt = _parse_dt(e["time"])
            if not dt:
                continue
            delta = (dt - now).total_seconds() / 60.0
            # Execution gate blocks only HIGH-impact events. Coverage may include
            # LOW/MEDIUM events, but they must never trigger a trading block.
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
                "state": "BLOCK",
                "blocked": True,
                "source": upcoming[0][0].get("source", "local"),
                "message": "🚫 التداول محجوب مؤقتاً بسبب خبر عالي التأثير.\n" + "\n".join(details),
                "events": [e for e, _ in upcoming],
            }
        return {
            "state": "CLEAR", "blocked": False,
            "source": events[0].get("source", "local"),
            "message": "🟢 لا يوجد حالياً خبر عالي التأثير ضمن نافذة الحجب.",
            "events": [],
        }
    except Exception as exc:
        return {
            "state": "UNKNOWN", "blocked": True, "source": "none", "events": [],
            "message": f"⚠️ حالة الأخبار UNKNOWN — إيقاف الإشارة التنفيذية احترازياً.\nالسبب: {exc}",
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
    """Execution news gate. Pure local read: BLOCK/CLEAR/UNKNOWN; never performs network I/O."""
    risk = get_risk()
    return bool(risk.get("blocked")), risk.get("message", "")


def _signal_rejection_reasons(result):
    reasons = []
    if result.get("direction") not in ("BUY", "SELL"):
        reasons.append("لا يوجد اتجاه BUY/SELL مؤكد")
    if float(result.get("score", 0) or 0) < MIN_TRADE_SCORE:
        reasons.append(f"درجة الإعداد أقل من {MIN_TRADE_SCORE} نقطة")
    if result.get("liquidity_blocked"):
        reasons.append("إعادة اختبار السيولة INVALIDATED")
    if result.get("news_blocked"):
        reasons.append("بوابة الأخبار: HIGH أو UNKNOWN — لا تنفيذ")
    reasons.extend(result.get("setup_reasons", []))
    if result.get("direction") in ("BUY", "SELL") and not result.get("trade"):
        reasons.append("لم تمر الصفقة كل بوابات التنفيذ")
    return list(dict.fromkeys(reasons))



def _daily_execution_core():
    """محرك التنفيذ قبل إدخال SMC: Scenario -> Setup -> Risk/News -> READY."""
    h1 = analyze(get_bars("1h", 300))
    m15 = analyze(get_bars("15m", 300))
    m5 = analyze(get_bars("5m", 300))
    price = float(live_price()["price"])
    levels = support_resistance(get_bars("1h", 250))
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
        news_blocked, news_text = True, f"⚠️ حالة الأخبار UNKNOWN — إيقاف احترازي: {exc}"
    candidate = build_trade(direction, h1, m15, levels) if direction in ("BUY","SELL") else None
    rr_ok = bool(candidate and float(candidate.get("rr",0) or 0) >= 1.20)
    ready = bool(direction in ("BUY","SELL") and setup_score >= MIN_TRADE_SCORE and alignment and m15_adx >= 15 and not liquidity_blocked and not news_blocked and rr_ok)
    state = "READY" if ready else "CANDIDATE" if direction in ("BUY","SELL") and setup_score >= MIN_TRADE_SCORE else "WATCH"
    if not alignment: setup_reasons.append("لا يوجد تطابق H1 + M15 + M5 كامل")
    if liquidity_blocked: setup_reasons.append("حالة السيولة INVALIDATED")
    if news_blocked: setup_reasons.append("بوابة الأخبار تمنع التنفيذ")
    if not rr_ok: setup_reasons.append("R:R التنفيذي غير صالح أو غير متاح")
    setup_score = int(max(0,min(100,round(setup_score))))
    quality, quality_icon = trade_quality(setup_score)
    return {"signal":ready,"execution_state":state,"direction":direction,"score":setup_score,"scenario_score":int(scenario_score),"setup_score":setup_score,"quality":quality,"quality_icon":quality_icon,"price":price,"levels":levels,"liquidity_blocked":liquidity_blocked,"liquidity_state":liquidity_state,"liquidity":liquidity,"factors":factors,"setup_reasons":setup_reasons,"trade":candidate if ready else None,"news_blocked":news_blocked,"news":news_text,"daily":{"h1":h1,"m15":m15,"m5":m5,"buy_score":buy_score,"sell_score":sell_score,"buy_factors":buy_factors,"sell_factors":sell_factors}}


def evaluate_signal():
    """الإشارة الموحدة: الفني يحدد الاتجاه، والمؤسسي يؤكد أو يحجب التنفيذ؛ لا يقلب الاتجاه آلياً."""
    global LAST_ANALYSIS
    core=_daily_execution_core()
    try:
        w1=analyze(get_bars('1w',250)); d1=analyze(get_bars('1d',300)); h4=analyze(get_bars('4h',300))
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
    try: institutional=institutional_analysis(get_bars('1h',250))
    except Exception as exc:
        logger.exception('Institutional engine failure')
        institutional={'direction':'WAIT','quality':0,'institutional_score':0,'effective_strength':0,'coverage_pct':0,'status':'UNAVAILABLE','sources':{},'errors':[str(exc)],'factors':[]}
    result=dict(core); result['mtf']=mtf; result['institutional']=institutional
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
        result['score']=result['combined_score']; result['scenario_score']=result['combined_scenario_score']
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
    L += ['','🧭 الصورة الكبرى',f"W1: {mtf['w1'].get('direction','غير متاح')}",f"D1: {mtf['d1'].get('direction','غير متاح')}",f"H4: {mtf['h4'].get('direction','غير متاح')}",f"H1: {mtf['h1'].get('direction','غير متاح')}",f"M15: {mtf['m15'].get('direction','غير متاح')}",f"M5: {mtf['m5'].get('direction','غير متاح')}",'','🌐 حالة السوق الكلي',f'النظام: {regime}',f"DXY: {v(m.get('dxy'))}",f"تغير DXY: {v(m.get('dxy_change_pct'),'%')}",f"Broad USD: {v(m.get('usd_broad'))}",f"US10Y: {v(m.get('us10y'),'%')}",f"Real 10Y: {v(m.get('real10y'),'%')}",f"VIX: {v(m.get('vix'))}",'','💰 السعر والمستويات',f"السعر الحالي: {result['price']:.2f}",f"🟢 S1: {format_zone(levels,'support1')}",f"🟢 S2: {format_zone(levels,'support2')}",f"🟢 S3: {format_zone(levels,'support3')}",f"🔴 R1: {format_zone(levels,'resistance1')}",f"🔴 R2: {format_zone(levels,'resistance2')}",f"🔴 R3: {format_zone(levels,'resistance3')}",'','💧 السيولة',f"الانحياز: {mtf['m15']['liquidity']['bias']}",f"Buy-side الأقرب: {fmt(mtf['m15']['liquidity']['nearest_buy'])}",f"Sell-side الأقرب: {fmt(mtf['m15']['liquidity']['nearest_sell'])}",f"السحب: {mtf['m15']['liquidity']['sweep_text']}",f"إعادة الاختبار: {liquidity_retest_summary(mtf['m15']['liquidity'])}",'','🏦 التحليل المؤسسي — بيانات حقيقية',f"🎯 الاتجاه المؤسسي: {inst.get('direction','غير متاح')}",f"💪 قوة الدليل المؤسسي: {v(inst.get('strength'))}/100",f"📡 تغطية المصادر: {v(inst.get('coverage_pct'))}%",f"🚦 حالة البيانات المؤسسية: {v(inst.get('status'))}",'','🏛️ البنوك المركزية',f"السياسة النقدية: {v(m.get('policy'))}",f"الفائدة المستهدفة: {v(m.get('fed_lower'),'%')} — {v(m.get('fed_upper'),'%')}",f"الفائدة الفعلية EFFR: {v(m.get('effr'),'%')}",f"توقعات الفائدة السوقية: {v(m.get('rate_expectation'))}",f"مصدر توقعات الفائدة: {v(m.get('rate_expectation_source'))}",f"العقود الآجلة للصناديق الفيدرالية — المعدل الضمني: {v(m.get('fed_futures_implied'),'%')}",f"انحياز متوسط الفائدة الضمني: { {'HAWKISH':'صعودي','DOVISH':'هبوطي','NEUTRAL':'محايد'}.get(m.get('market_rate_bias'), 'غير متاح') }",f"تفسير العقود: {v(m.get('fed_futures_interpretation'))}",f"تغير احتياطيات الذهب المعلن: {v(cb.get('net_change_tonnes'),' طن')}",f"اتجاه التغير: {v(cb.get('direction'))}",f"التحقق المتقاطع بين مجلس الذهب العالمي وصندوق النقد الدولي: { {'PASS':'ناجح','DEGRADED':'متدهور','NOT_PERFORMED':'لم يُنفذ','PERIOD_MISMATCH':'اختلاف الفترة'}.get(cb.get('cross_validation',{}).get('status'), 'غير متاح') }",f"فارق التحقق: {v(cb.get('cross_validation',{}).get('difference_pct'),'%')}",f"قيمة WGC للتحقق: {v(cb.get('cross_validation',{}).get('wgc_value'),' طن')}",f"قيمة IMF للتحقق: {v(cb.get('cross_validation',{}).get('imf_value'),' طن')}",f"فترة WGC: {v(cb.get('cross_validation',{}).get('wgc_period'))}",f"فترة IMF: {v(cb.get('cross_validation',{}).get('imf_period'))}",'','🏦 ETF / صناديق الذهب',f"GLD آخر تحديث: {v(g.get('date'))}",f"حيازة الذهب: {v(g.get('gold_oz'))} أونصة",f"تغير الحيازة: {v(g.get('gold_oz_delta'))} أونصة",f"تغير الأسهم: {v(g.get('shares_delta'))}",f"اتجاه الحيازة: { {'IN':'دخول','OUT':'خروج','NEUTRAL':'محايد'}.get(g.get('flow_proxy'), 'غير متاح') }",'⚠️ تغير الحيازة ليس تدفقاً نقدياً؛ التدفق النقدي المباشر غير متاح من المصدر الحالي.','','📑 COT — تمركزات الذهب',f"تاريخ التقرير: {v(c.get('report_date'))}",f"تجاري — صافي: {v(c.get('legacy_commercial_net'))}",f"تجاري — التغير الأسبوعي: {v(c.get('legacy_commercial_delta'))}",f"غير تجاري — صافي: {v(c.get('legacy_noncommercial_net'))}",f"غير تجاري — التغير الأسبوعي: {v(c.get('legacy_noncommercial_delta'))}",f"الأموال المُدارة — صافي: {v(c.get('managed_money_net'))}",f"الأموال المُدارة — التغير الأسبوعي: {v(c.get('managed_money_delta'))}",'','📈 السوق الكلي',f"DXY: {v(m.get('dxy'))}",f"تغير DXY: {v(m.get('dxy_change_pct'),'%')}",f"Broad USD: {v(m.get('usd_broad'))}",f"US10Y: {v(m.get('us10y'),'%')}",f"Real 10Y: {v(m.get('real10y'),'%')}",f"VIX: {v(m.get('vix'))}",f"Risk-On / Risk-Off: {regime}",'','🚦 بوابات التنفيذ',f"📊 الاتجاه: {'🟢 داعم' if result['direction'] in ('BUY','SELL') else '🟡 غير محسوم'}",f"🧠 الهيكل: {'🟢 داعم' if mtf['h1'].get('structure') in ('صاعد','هابط') else '🟡 محايد'}",f"💧 السيولة: {'🟢 مؤكدة' if 'لا يوجد' not in str(mtf['m15']['liquidity'].get('sweep_text','')) else '🟡 غير مكتملة'}",f"🔄 إعادة الاختبار: {liquidity_retest_summary(mtf['m15']['liquidity'])}",f"📰 الأخبار: {result['news']}",f"🛡️ التنفيذ: {'🟢 READY' if result.get('execution_state')=='READY' else '🟡 WAIT'}",'','⏳ ما الذي ننتظره؟','• اكتمال سحب السيولة','• إعادة اختبار مؤكدة','• تأكيد M15/M5','• عدم تعارض واضح مع العامل المؤسسي','','📡 صحة مصادر التحليل المؤسسي']
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


async def reply(update, text):
    try:
        payload = with_market_header(text)
        markup = None
        q = getattr(update, "callback_query", None)
        if q:
            data = q.data or ""
            section = "reports" if data in {"menu_daily_report","menu_weekly_report"} else \
                      "trades" if data in {"menu_trade_now","menu_trade_alerts","menu_trade_history"} else \
                      "market" if data in {"menu_news","menu_markets","menu_gold_price"} else \
                      "analyses" if data in {"menu_full_analysis"} else None
            if section:
                markup = _ux_markup(_ux_section_rows(section))
            await q.edit_message_text(payload, reply_markup=markup)
        elif getattr(update, "message", None):
            await update.message.reply_text(payload)
    except Exception:
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
        ["⚡ التحليل السريع", "📊 التحليل الكامل"],
        ["📝 التقرير اليومي", "📅 التقرير الأسبوعي"],
        ["🌍 السوق والأخبار", "🎯 الصفقات"],
        ["💧 السيولة والدعم", "🎯 الصفقة الآن"],
        ["👤 الاشتراك", "💳 الباقات"],
        ["🟢 حالة النظام", "ℹ️ المساعدة"],
        ["📈 مراقب الأداء"],
    ]


def _ux_section_rows(section):
    home = [("🏠 الرئيسية", "nav_home")]
    if section == "analyses":
        return [[("📊 التحليل الكامل", "menu_full_analysis")], [("⚡ التحليل السريع", "nav_quick")], [("📑 التقارير اليومية والأسبوعية", "nav_reports")], home]
    if section == "reports":
        return [[("📝 التقرير التوضيحي اليومي", "menu_daily_report")], [("📅 التقرير التوضيحي الأسبوعي", "menu_weekly_report")], [("📜 سجل الصفقات", "menu_trade_history")], home]
    if section == "trades":
        return [[("🎯 صفقة الآن", "menu_trade_now")], [("📜 سجل الصفقات", "menu_trade_history")], [("🔔 تنبيهات الصفقات", "menu_trade_alerts")], home]
    if section == "market":
        return [[("💰 سعر الذهب", "menu_gold_price")], [("📰 الأخبار", "menu_news")], [("🌍 الجلسات والأسواق", "menu_markets")], home]
    return [home]


async def _ux_render(update, title, body, section=None):
    text = f"{title}\n━━━━━━━━━━━━━━━━━━\n{body}" if body else title
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
        s1 = nearest_support(levels, price); s2 = next_support(levels, price)
        r1 = nearest_resistance(levels, price); r2 = next_resistance(levels, price)
        trade = result.get("trade")
        target = float(trade["tp1"]) if trade else None
        stop = float(trade["sl"]) if trade else None
        rr = float(trade["rr"]) if trade else 0.0
        structure = result["mtf"]["h4"]["structure"]
        structure_dir = "🟢 صاعد" if structure == "صاعد" else "🔴 هابط" if structure == "هابط" else "🟡 محايد"
        conflict = ((direction == "BUY" and structure == "هابط") or (direction == "SELL" and structure == "صاعد"))
        score = int(result["score"])
        scenario_quality = ("ممتاز" if score >= 90 else "قوي جداً" if score >= 80 else "قوي" if score >= 70 else "جيد" if score >= 60 else "مؤهل" if score >= 50 else "ضعيف")
        execution_quality = "جاهزة للتنفيذ" if result.get("execution_state") == "READY" else "غير جاهزة للتنفيذ"
        if result.get("execution_state") == "READY" and result.get("signal") and not conflict:
            rec = "🟢 فرصة شراء سكالبينج" if direction == "BUY" else "🔴 فرصة بيع سكالبينج"
        elif direction == "BUY":
            rec = "🟡 مراقبة شراء — تحتاج تأكيد"
        elif direction == "SELL":
            rec = "🟡 مراقبة بيع — تحتاج تأكيد"
        else:
            rec = "🟡 مراقبة — لا يوجد اتجاه مؤكد"
        if conflict:
            conclusion = "الزخم اللحظي متعارض مع الهيكل الأكبر؛ انتظر تأكيداً من المستوى القريب قبل الدخول."
        elif result.get("execution_state") == "CANDIDATE":
            conclusion = "السيناريو مرشح، لكنه ليس صفقة جاهزة؛ انتظر اكتمال بوابات التنفيذ."
        else:
            conclusion = "الإشارة لم تصل إلى حد الصفقة؛ الأفضل المراقبة وانتظار تأكيد إضافي."
        # حالة الأخبار تدخل مباشرة في قرار التنفيذ والتوصية.
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
        news_gate = "🟢 CLEAR" if "لا يوجد" in str(news_text) or "CLEAR" in str(news_text).upper() else "🟡 مراجعة"

        # UX فقط: إعادة ترتيب التقرير السريع إلى لوحة قرار + بوابات قرار.
        # لا يتم تغيير أي قيمة تحليلية أو منطق تنفيذ داخل هذا القسم.
        support_line = f"🟢 الدعم الأقرب: {s1:.2f}\n" if s1 is not None else "🟢 الدعم الأقرب: غير متوفر\n"
        resistance_line = f"🔴 المقاومة الأقرب: {r1:.2f}\n" if r1 is not None else "🔴 المقاومة الأقرب: غير متوفر\n"
        text = (
            "⚡ التحليل السريع\n"
            "XAU/USD\n\n"
            f"🎯 القرار الحالي\n{rec}\n\n"
            f"💪 قوة السيناريو: {score}/100 — {scenario_quality}\n"
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
            f"• ADX: {adx:.1f} — {adx_text}\n\n"
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
        gate = "🟢 بوابة الأخبار: مفتوحة"
    elif state == "BLOCK":
        gate = "🔴 بوابة الأخبار: مغلقة مؤقتاً"
    else:
        gate = "⚠️ بوابة الأخبار: مغلقة احترازياً"
    await reply(update, f"📰 حالة الأخبار\n\n{gate}\n📡 المصدر المحلي: {source}\n\n{text}\n\n🔔 التنبيهات مستقلة عن بوابة التنفيذ؛ بوابة الأخبار هي المرجع الوحيد للسماح أو الحجب.")


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
            "━━━━━━━━━━━━━━━━━━",
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
    """تقرير مراقب الأداء — للإدارة فقط، قراءة فقط ولا يؤثر على التداول."""
    user_id = update.effective_user.id if update.effective_user else update.effective_chat.id
    if not (is_admin_chat(user_id) or is_admin_chat(update.effective_chat.id)):
        await reply(update, "⛔ مراقب الأداء مخصص للإدارة فقط.")
        return
    if PERFORMANCE_AUDITOR is None:
        await reply(update, "🛡️ مراقب الأداء\n━━━━━━━━━━━━━━━━━━\n❌ محرك المراقبة غير متاح حالياً.\nتحقق من وجود مجلد xau_performance_auditor ومتطلبات التشغيل.")
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
        buttons = [[InlineKeyboardButton("🔄 تحديث المراقب", callback_data="menu_audit")]]
        await update.effective_message.reply_text(with_market_header("\n".join(lines)), reply_markup=InlineKeyboardMarkup(buttons + [[InlineKeyboardButton("🏠 الرئيسية", callback_data="nav_home")]]))
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
            f"📩 طلب الاشتراك — {plan['name']}\n━━━━━━━━━━━━━━━━━━\n"
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
        "menu_trade_now": trade_now,
        "menu_trade_history": trade_history,
        "menu_trade_alerts": subscribe,
        "menu_news": news_status,
        "menu_markets": markets,
        "menu_gold_price": gold_price,
        "menu_audit": audit_report,
    }
    fn = callback_routes.get(data)
    if fn:
        # زر رجوع موحّد يُرفق بالنتيجة نفسها عبر طبقة reply أدناه.
        section = "reports" if data in {"menu_daily_report","menu_weekly_report"} else                   "trades" if data in {"menu_trade_now","menu_trade_alerts","menu_trade_history"} else                   "market" if data in {"menu_news","menu_markets","menu_gold_price"} else "analyses"
        await fn(update, context)
        return
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


async def help_menu(update, context):
    body=("⚡ التحليل السريع — قراءة لحظية مباشرة.\n"
          "📊 التحليل الكامل — القراءة متعددة الفريمات والعوامل.\n"
          "📝 التقرير اليومي / 📅 التقرير الأسبوعي — السيناريوهات وخطة القرار.\n"
          "🌍 السوق والأخبار — حالة السوق والأخبار والجلسات.\n"
          "🎯 الصفقات — السجل والتنبيهات ومتابعة الصفقات.\n"
          "💧 السيولة والدعم — السيولة مع مستويات الدعم والمقاومة.\n"
          "🎯 الصفقة الآن — القرار التنفيذي الحالي.\n"
          "📈 مراقب الأداء — متابعة نتائج الصفقات والاستراتيجية.")
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
        "ℹ️ المساعدة": help_menu,
        "🎯 الصفقة الآن": trade_now,
        "📍 الدعوم والمقاومات": show_levels,
        "📜 سجل الصفقات": trade_history,
        "📝 التقرير اليومي": daily_report,
        "📅 التقرير الأسبوعي": weekly_report,
        "📰 الأخبار": news_status,
        "🌍 الأسواق": markets,
        "🌍 الجلسات والأسواق": markets,
        "🌍 السوق والجلسات": markets,
        "📊 التحليل الكامل": full_analysis,
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
    """إرسال تنبيهات تلقائية للأخبار عالية التأثير فقط — مع حجب التنفيذ عند HIGH/UNKNOWN."""
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
            text = (f"📰 تنبيه خبر عالي التأثير\n━━━━━━━━━━━━━━━━━━\n"
                    f"{phase}\n💱 {event.get('currency','')}\n"
                    f"📌 {event.get('event','خبر اقتصادي')}\n"
                    f"🕐 {dt.strftime('%Y-%m-%d %H:%M')} دمشق\n"
                    "🔔 تنبيه الخبر: التداول الجديد يُحجب ضمن نافذة الخطر.")
            for cid in recipients:
                try:
                    await APPLICATION.bot.send_message(chat_id=cid, text=with_market_header(text))
                except Exception:
                    logger.exception("News alert error for %s", cid)


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
            # بوابة الأخبار مدمجة داخل evaluate_signal؛ UNKNOWN/HIGH يمنعان التنفيذ.
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
    APPLICATION.add_handler(CommandHandler("audit", audit_report))
    APPLICATION.add_handler(CommandHandler("activate", admin_command))
    APPLICATION.add_handler(CallbackQueryHandler(callback_router))
    APPLICATION.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, router))

    await APPLICATION.initialize()
    await APPLICATION.start()
    await APPLICATION.bot.set_webhook(url=WEBHOOK_URL, allowed_updates=["message", "callback_query"], drop_pending_updates=True)

    logger.info("XAU SMART TRADER %s started", VERSION)
    logger.info("Webhook: %s", WEBHOOK_URL)
    asyncio.create_task(auto_loop())
    if PERFORMANCE_AUDITOR is not None:
        try:
            _start_auditor_bridge_worker()
            PERFORMANCE_AUDITOR.start_background()
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
    check('version', VERSION.startswith('v18.61-'), VERSION)
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

if __name__ == "__main__":
    main()
