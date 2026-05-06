"""
SAIYAN OCC Scalping Bot - Binance USDT Pairs
Based on Pine Script strategy v6_1_23
Runs on 3-minute timeframe with full risk management
"""

import time
import json
import threading
import logging
import math
import random
from datetime import datetime, timedelta
from collections import deque
from dataclasses import dataclass, field, asdict
from typing import Optional
import os
import sqlite3

# ── logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(), 
        logging.FileHandler("bot.log", encoding="utf-8")
    ]
)
log = logging.getLogger("SaiyanBot")

# ══════════════════════════════════════════════════════════════════════════════
# SQLITE PERSISTENCE
# ══════════════════════════════════════════════════════════════════════════════

DB_PATH = "saiyan_bot.db"

def db_init():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS trades (
            id              TEXT PRIMARY KEY,
            symbol          TEXT NOT NULL,
            side            TEXT NOT NULL,
            entry_price     REAL NOT NULL,
            entry_time      TEXT NOT NULL,
            qty             REAL NOT NULL,
            tp1             REAL,
            tp2             REAL,
            tp3             REAL,
            sl              REAL,
            status          TEXT NOT NULL DEFAULT 'open',
            exit_price      REAL DEFAULT 0,
            exit_time       TEXT DEFAULT '',
            exit_reason     TEXT DEFAULT '',
            realized_pnl    REAL DEFAULT 0,
            tp1_hit         INTEGER DEFAULT 0,
            tp2_hit         INTEGER DEFAULT 0,
            tp3_hit         INTEGER DEFAULT 0,
            regime_at_entry TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS equity_curve (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            ts       TEXT NOT NULL,
            equity   REAL NOT NULL,
            balance  REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bot_config (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
        CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
        CREATE INDEX IF NOT EXISTS idx_equity_ts     ON equity_curve(ts);
    """)
    con.commit()
    con.close()
    log.info(f"SQLite DB initialised at {DB_PATH}")

def db_upsert_trade(trade: "Trade"):
    """Insert or update a trade row — safe against partial writes."""
    try:
        con = sqlite3.connect(DB_PATH, timeout=10)
        cur = con.cursor()
        cur.execute("""
            INSERT INTO trades
                (id,symbol,side,entry_price,entry_time,qty,
                 tp1,tp2,tp3,sl,status,exit_price,exit_time,
                 exit_reason,realized_pnl,tp1_hit,tp2_hit,tp3_hit,regime_at_entry)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                qty           = excluded.qty,
                sl            = excluded.sl,
                status        = excluded.status,
                exit_price    = excluded.exit_price,
                exit_time     = excluded.exit_time,
                exit_reason   = excluded.exit_reason,
                realized_pnl  = excluded.realized_pnl,
                tp1_hit       = excluded.tp1_hit,
                tp2_hit       = excluded.tp2_hit,
                tp3_hit       = excluded.tp3_hit
        """, (
            trade.id, trade.symbol, trade.side,
            trade.entry_price, trade.entry_time, trade.qty,
            trade.tp1, trade.tp2, trade.tp3, trade.sl,
            trade.status, trade.exit_price, trade.exit_time,
            trade.exit_reason, trade.realized_pnl,
            int(trade.tp1_hit), int(trade.tp2_hit), int(trade.tp3_hit),
            trade.regime_at_entry,
        ))
        con.commit()
        con.close()
    except sqlite3.Error as e:
        log.error(f"DB upsert trade {trade.id}: {e}")

def db_save_equity(ts: str, equity: float, balance: float):
    try:
        con = sqlite3.connect(DB_PATH, timeout=10)
        con.execute(
            "INSERT INTO equity_curve (ts,equity,balance) VALUES (?,?,?)",
            (ts, round(equity, 4), round(balance, 4))
        )
        con.commit()
        con.close()
    except sqlite3.Error as e:
        log.error(f"DB equity write: {e}")

def db_load_trades() -> tuple[dict, list]:
    """Restore open and closed trades from DB on restart."""
    open_trades: dict[str, "Trade"] = {}
    closed_trades: list["Trade"] = []
    try:
        con = sqlite3.connect(DB_PATH, timeout=10)
        cur = con.cursor()
        cur.execute("SELECT * FROM trades ORDER BY rowid ASC")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        con.close()
        for row in rows:
            d = dict(zip(cols, row))
            t = Trade(
                id=d["id"], symbol=d["symbol"], side=d["side"],
                entry_price=d["entry_price"], entry_time=d["entry_time"],
                qty=d["qty"], tp1=d["tp1"] or 0, tp2=d["tp2"] or 0,
                tp3=d["tp3"] or 0, sl=d["sl"] or 0,
                status=d["status"],
                exit_price=d["exit_price"] or 0,
                exit_time=d["exit_time"] or "",
                exit_reason=d["exit_reason"] or "",
                realized_pnl=d["realized_pnl"] or 0,
                tp1_hit=bool(d["tp1_hit"]), tp2_hit=bool(d["tp2_hit"]),
                tp3_hit=bool(d["tp3_hit"]),
                regime_at_entry=d["regime_at_entry"] or "",
            )
            if t.status == "open":
                open_trades[t.id] = t
            else:
                closed_trades.append(t)
        log.info(f"DB restored: {len(open_trades)} open, {len(closed_trades)} closed trades")
    except sqlite3.Error as e:
        log.error(f"DB load trades: {e}")
    return open_trades, closed_trades

def db_load_equity() -> list:
    try:
        con = sqlite3.connect(DB_PATH, timeout=10)
        cur = con.cursor()
        cur.execute("SELECT ts, equity, balance FROM equity_curve ORDER BY id DESC LIMIT 500")
        rows = [{"time": r[0], "equity": r[1], "balance": r[2]} for r in reversed(cur.fetchall())]
        con.close()
        return rows
    except sqlite3.Error as e:
        log.error(f"DB load equity: {e}")
        return []

# ══════════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class BotConfig:
    # Mode
    paper_mode: bool = True
    trade_direction: str = "BOTH"          # LONG | SHORT | BOTH
    market_regime_auto: bool = True
    active_regime: str = "DETECTING"

    # MA settings  (mirrors Pine)
    tf_minutes: int = 3
    basis_type: str = "ALMA"
    basis_len: int = 8
    offset_sigma: int = 6
    offset_alma: float = 0.85
    use_res: bool = True
    int_res: int = 16                       # multiplier → 48-min HTF confirmation

    # Risk
    tp1_pct: float = 0.8
    tp2_pct: float = 1.2
    tp3_pct: float = 1.6
    sl_pct:  float = 0.8
    tp1_qty: float = 75.0
    tp2_qty: float = 20.0
    tp3_qty: float = 5.0

    # Position sizing
    equity_pct: float = 10.0
    max_open_trades: int = 3

    # Swing / S&D
    swing_length: int = 10
    history_to_keep: int = 20
    box_width: float = 2.5

    # Pair filter
    min_volume_usdt: float = 5_000_000     # 24h volume
    top_n_pairs: int = 30

    # Lock: when True, regime auto-mode won't overwrite TP/SL/qty values
    params_locked: bool = False

    # API (empty = paper)
    api_key: str = ""
    api_secret: str = ""


@dataclass
class Candle:
    timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class Trade:
    id: str
    symbol: str
    side: str                   # long | short
    entry_price: float
    entry_time: str
    qty: float                  # base asset
    tp1: float = 0.0
    tp2: float = 0.0
    tp3: float = 0.0
    sl:  float = 0.0
    status: str = "open"        # open | closed
    exit_price: float = 0.0
    exit_time: str = ""
    exit_reason: str = ""
    realized_pnl: float = 0.0
    tp1_hit: bool = False
    tp2_hit: bool = False
    tp3_hit: bool = False
    regime_at_entry: str = ""
    current_price: float = 0.0

    @property
    def unrealized_pnl(self) -> float:
        if self.status != "open" or self.current_price == 0:
            return 0.0
        mult = 1 if self.side == "long" else -1
        return (self.current_price - self.entry_price) * self.qty * mult

    @property
    def unrealized_pnl_pct(self) -> float:
        if self.entry_price == 0:
            return 0.0
        mult = 1 if self.side == "long" else -1
        return mult * (self.current_price - self.entry_price) / self.entry_price * 100

    def to_dict(self) -> dict:
        d = asdict(self)
        d["unrealized_pnl"] = round(self.unrealized_pnl, 4)
        d["unrealized_pnl_pct"] = round(self.unrealized_pnl_pct, 4)
        return d


@dataclass
class SymbolState:
    symbol: str
    candles: deque = field(default_factory=lambda: deque(maxlen=300))
    # MA series
    close_series: deque = field(default_factory=lambda: deque(maxlen=50))
    open_series:  deque = field(default_factory=lambda: deque(maxlen=50))
    htf_close_series: deque = field(default_factory=lambda: deque(maxlen=50))
    htf_open_series:  deque = field(default_factory=lambda: deque(maxlen=50))
    # Signal state
    prev_signal: int = 0        # 1=long, -1=short, 0=flat
    last_signal_bar: int = 0
    # Swing
    swing_highs: deque = field(default_factory=lambda: deque(maxlen=20))
    swing_lows:  deque = field(default_factory=lambda: deque(maxlen=20))
    # Perf
    win_rate: float = 0.0
    total_trades: int = 0
    wins: int = 0
    score: float = 0.0          # composite selection score
    # Cooldown: timestamp after which this symbol may be re-entered
    cooldown_until: float = 0.0
    # Consecutive SL counter — escalates cooldown
    consecutive_sl: int = 0


# ══════════════════════════════════════════════════════════════════════════════
# INDICATOR ENGINE  (Python port of Pine Script logic)
# ══════════════════════════════════════════════════════════════════════════════

class Indicators:

    @staticmethod
    def ema(values: list, period: int) -> list:
        if len(values) < period:
            return [None] * len(values)
        result = [None] * len(values)
        k = 2.0 / (period + 1)
        # seed with SMA
        seed_idx = period - 1
        sma = sum(values[:period]) / period
        result[seed_idx] = sma
        for i in range(seed_idx + 1, len(values)):
            result[i] = values[i] * k + result[i - 1] * (1 - k)
        return result

    @staticmethod
    def alma(values: list, period: int, offset: float, sigma: int) -> list:
        result = [None] * len(values)
        if len(values) < period:
            return result
        m = offset * (period - 1)
        s = period / sigma
        weights = [math.exp(-((i - m) ** 2) / (2 * s * s)) for i in range(period)]
        wsum = sum(weights)
        for i in range(period - 1, len(values)):
            window = values[i - period + 1: i + 1]
            result[i] = sum(w * v for w, v in zip(weights, window)) / wsum
        return result

    @staticmethod
    def hull_ma(values: list, period: int) -> list:
        half = max(1, period // 2)
        sqrt_p = max(1, round(math.sqrt(period)))
        wma1 = Indicators.wma(values, period)
        wma2 = Indicators.wma(values, half)
        diff = [2 * (wma2[i] or 0) - (wma1[i] or 0) if wma2[i] and wma1[i] else None
                for i in range(len(values))]
        return Indicators.wma(diff, sqrt_p)

    @staticmethod
    def wma(values: list, period: int) -> list:
        result = [None] * len(values)
        denom = period * (period + 1) / 2
        for i in range(period - 1, len(values)):
            window = values[i - period + 1: i + 1]
            if None in window:
                continue
            result[i] = sum((j + 1) * v for j, v in enumerate(window)) / denom
        return result

    @staticmethod
    def tema(values: list, period: int) -> list:
        e1 = Indicators.ema(values, period)
        
        e1_valid = [v for v in e1 if v is not None]
        e2_temp = Indicators.ema(e1_valid, period)
        e2 = [None] * (len(e1) - len(e2_temp)) + e2_temp
        
        e2_valid = [v for v in e2 if v is not None]
        e3_temp = Indicators.ema(e2_valid, period)
        e3 = [None] * (len(e2) - len(e3_temp)) + e3_temp
        
        result = []
        for i in range(len(values)):
            if e1[i] is not None and e2[i] is not None and e3[i] is not None:
                result.append(3 * e1[i] - 3 * e2[i] + e3[i])
            else:
                result.append(None)
        return result

    @staticmethod
    def compute_ma(ma_type: str, values: list, period: int,
                   offset_sigma: int, offset_alma: float) -> list:
        if ma_type == "ALMA":
            return Indicators.alma(values, period, offset_alma, offset_sigma)
        elif ma_type == "HullMA":
            return Indicators.hull_ma(values, period)
        elif ma_type == "TEMA":
            return Indicators.tema(values, period)
        else:
            return Indicators.ema(values, period)

    @staticmethod
    def atr(candles: list, period: int = 50) -> float:
        if len(candles) < 2:
            return 0.0
        trs = []
        for i in range(1, len(candles)):
            c = candles[i]
            pc = candles[i - 1].close
            tr = max(c.high - c.low, abs(c.high - pc), abs(c.low - pc))
            trs.append(tr)
        trs = trs[-period:]
        return sum(trs) / len(trs) if trs else 0.0

    @staticmethod
    def rsi(closes: list, period: int = 28) -> Optional[float]:
        if len(closes) < period + 1:
            return None
        gains, losses = [], []
        for i in range(1, len(closes)):
            d = closes[i] - closes[i - 1]
            gains.append(max(d, 0))
            losses.append(max(-d, 0))
        gains = gains[-period:]
        losses = losses[-period:]
        ag = sum(gains) / period
        al = sum(losses) / period
        if al == 0:
            return 100.0
        rs = ag / al
        return 100 - 100 / (1 + rs)

    @staticmethod
    def pivot_high(highs: list, left: int, right: int) -> Optional[float]:
        if len(highs) < left + right + 1:
            return None
        idx = left  # pivot candidate index from right end
        candidate_idx = len(highs) - 1 - right
        if candidate_idx < left:
            return None
        candidate = highs[candidate_idx]
        for i in range(candidate_idx - left, candidate_idx + right + 1):
            if i == candidate_idx:
                continue
            if highs[i] >= candidate:
                return None
        return candidate

    @staticmethod
    def pivot_low(lows: list, left: int, right: int) -> Optional[float]:
        if len(lows) < left + right + 1:
            return None
        candidate_idx = len(lows) - 1 - right
        if candidate_idx < left:
            return None
        candidate = lows[candidate_idx]
        for i in range(candidate_idx - left, candidate_idx + right + 1):
            if i == candidate_idx:
                continue
            if lows[i] <= candidate:
                return None
        return candidate

    @staticmethod
    def detect_regime(candles: list) -> str:
        """Market regime detection using trend + volatility analysis"""
        if len(candles) < 50:
            return "DETECTING"
        closes = [c.close for c in candles[-100:]]
        highs  = [c.high  for c in candles[-100:]]
        lows   = [c.low   for c in candles[-100:]]

        # Short vs long EMA trend
        ema20 = Indicators.ema(closes, 20)
        ema50 = Indicators.ema(closes, 50)
        last_e20 = next((v for v in reversed(ema20) if v), None)
        last_e50 = next((v for v in reversed(ema50) if v), None)

        # ADX proxy: directional movement
        if not last_e20 or not last_e50:
            return "DETECTING"

        trend_pct = (last_e20 - last_e50) / last_e50 * 100

        # Volatility: ATR vs price range
        recent_range = max(highs[-20:]) - min(lows[-20:])
        mid_price = closes[-1]
        
        # Guard against single-wick distortions by capping the range using Average True Range
        atr20 = Indicators.atr(candles, 20)
        effective_range = min(recent_range, atr20 * 4.5) if atr20 else recent_range
        vol_pct = (effective_range / mid_price) * 100 if mid_price > 0 else 0.0

        # Consolidation filter: Count price crosses over EMA50 in the last 20 bars
        # A high cross count mathematically invalidates trending regimes
        crosses = 0
        for i in range(len(closes) - 20, len(closes)):
            if ema50[i] is not None and ema50[i-1] is not None:
                if (closes[i] > ema50[i]) != (closes[i-1] > ema50[i-1]):
                    crosses += 1

        # Slope consistency
        slopes = [closes[i] - closes[i - 5] for i in range(5, len(closes))]
        positive_slopes = sum(1 for s in slopes[-20:] if s > 0)
        slope_ratio = positive_slopes / 20

        # Whipsaw / Consolidation override
        if crosses >= 3:
            if vol_pct > 5.0:
                return "HIGH_VOLATILITY"
            elif vol_pct < 1.5:
                return "MEAN_REVERSION"
            else:
                return "SIDEWAYS"

        if trend_pct > 1.5 and slope_ratio > 0.65:
            return "TRENDING_BULLISH"
        elif trend_pct < -1.5 and slope_ratio < 0.35:
            return "TRENDING_BEARISH"
        elif vol_pct > 5.0:
            return "HIGH_VOLATILITY"
        elif vol_pct < 1.5:
            return "MEAN_REVERSION"
        else:
            return "SIDEWAYS"


# ══════════════════════════════════════════════════════════════════════════════
# PAPER EXCHANGE (simulates Binance)
# ══════════════════════════════════════════════════════════════════════════════

class PaperExchange:
    """Simulates order execution with realistic paper trading"""

    def __init__(self):
        self.balance_usdt = 10_000.0
        self.initial_balance = 10_000.0
        self._lock = threading.Lock()

    # Realistic slippage model: 0.04% taker fee + 0.02% spread
    TAKER_FEE  = 0.0004
    SLIP_PCT   = 0.0002

    def get_balance(self) -> float:
        return self.balance_usdt

    def place_order(self, symbol: str, side: str, usdt_amount: float,
                    current_price: float) -> Optional[dict]:
        with self._lock:
            if usdt_amount > self.balance_usdt:
                usdt_amount = self.balance_usdt
            if usdt_amount < 5:
                return None
            # Slippage: buys fill slightly higher, sells slightly lower
            slip = self.SLIP_PCT + self.TAKER_FEE
            fill_price = current_price * (1 + slip) if side == "long" \
                         else current_price * (1 - slip)
            qty = usdt_amount / fill_price
            self.balance_usdt -= usdt_amount
            return {"qty": qty, "avg_price": fill_price}

    def close_order(self, symbol: str, qty: float, entry_price: float,
                    exit_price: float, side: str) -> float:
        with self._lock:
            if side == "long":
                pnl = (exit_price - entry_price) * qty
            else:
                pnl = (entry_price - exit_price) * qty
            # Return original capital plus profit or loss
            self.balance_usdt += (qty * entry_price) + pnl
            return pnl


class LiveExchange:
    """Real Binance exchange via CCXT (lazy-imported)"""

    def __init__(self, api_key: str, api_secret: str):
        self._cached_balance = 0.0
        self._last_balance_fetch = 0.0
        try:
            import ccxt
            self.exchange = ccxt.binance({
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True,
                "options": {"defaultType": "future"},
            })
            self.exchange.load_markets()
            log.info("Live exchange connected")
        except Exception as e:
            log.error(f"Exchange init failed: {e}")
            self.exchange = None

    def get_balance(self) -> float:
        import time
        now = time.time()
        # Fetch actual balance at most once every 10 seconds
        if now - self._last_balance_fetch < 10.0 and self._last_balance_fetch > 0:
            return self._cached_balance
        try:
            bal = self.exchange.fetch_balance()
            self._cached_balance = bal["USDT"]["free"]
            self._last_balance_fetch = now
            return self._cached_balance
        except Exception:
            return self._cached_balance

    def place_order(self, symbol: str, side: str, usdt_amount: float,
                    current_price: float) -> Optional[dict]:
        try:
            raw_qty = usdt_amount / current_price
            formatted_qty = float(self.exchange.amount_to_precision(symbol, raw_qty))
            
            if formatted_qty <= 0:
                log.error(f"Order failed {symbol}: Quantity too small after formatting")
                return None
                
            order = self.exchange.create_market_order(
                symbol, "buy" if side == "long" else "sell", formatted_qty
            )
            return {"qty": float(order["filled"]), "avg_price": float(order["average"])}
        except Exception as e:
            log.error(f"Order failed {symbol}: {e}")
            return None

    def close_order(self, symbol: str, qty: float, entry_price: float,
                    exit_price: float, side: str) -> float:
        try:
            close_side = "sell" if side == "long" else "buy"
            formatted_qty = float(self.exchange.amount_to_precision(symbol, qty))
            
            if formatted_qty <= 0:
                log.warning(f"Skipping close for {symbol}: formatted quantity is zero")
                return 0.0
                
            order = self.exchange.create_market_order(
                symbol, close_side, formatted_qty, params={"reduceOnly": True}
            )
            
            actual_exit = order.get("average")
            if actual_exit is None or actual_exit == 0:
                actual_exit = exit_price
                
            executed_qty = float(order.get("filled", formatted_qty))
            pnl = (actual_exit - entry_price) * executed_qty if side == "long" \
                  else (entry_price - actual_exit) * executed_qty
            return pnl
        except Exception as e:
            log.error(f"Failed to close order {symbol}: {e}")
            return (exit_price - entry_price) * qty if side == "long" else (entry_price - exit_price) * qty


# ══════════════════════════════════════════════════════════════════════════════
# MARKET DATA PROVIDER (simulated with realistic price movement)
# ══════════════════════════════════════════════════════════════════════════════

class MarketDataProvider:
    """
    Fully dynamic market data — no hardcoded symbols or prices.
    On startup:
      1. Fetches ALL USDT spot pairs from Binance /exchangeInfo
      2. Fetches 24h ticker for all of them — filters by volume
      3. Bootstraps real 3-min kline history for the top N candidates
      4. Scores and selects the best pairs for this strategy
    """

    # BTCUSDT is always included as the regime indicator — never dropped
    REGIME_ANCHOR = "BTCUSDT"

    def __init__(self, config: BotConfig):
        self.config = config
        self._prices: dict = {}
        self._candle_history: dict = {}
        self._candles_are_real: dict = {}
        self._lock = threading.Lock()

        # Populated dynamically — replaces hardcoded UNIVERSE
        self.UNIVERSE: list[tuple[str, float]] = []

        # Kick off async universe discovery immediately
        threading.Thread(target=self._discover_universe, daemon=True).start()

    # ── Step 1: Discover all valid USDT pairs from Binance ────────────────────
    def _discover_universe(self):
        """
        Full dynamic pipeline:
          1. GET /exchangeInfo  → all active USDT spot pairs
          2. GET /ticker/24hr   → real volume + price for each
          3. Filter: min volume, exclude stablecoins/leveraged tokens
          4. Take top 200 by volume as candidates
          5. Bootstrap kline history for candidates
          6. Score candidates by strategy fit → final UNIVERSE
        """
        log.info("Discovering live USDT universe from Binance...")
        try:
            candidates = self._fetch_usdt_candidates()
            if not candidates:
                log.error("Universe discovery failed — no candidates returned")
                return
            log.info(f"Found {len(candidates)} USDT candidates after volume filter")

            # Seed price dict immediately so get_price() works during bootstrap
            with self._lock:
                for sym, price in candidates:
                    self._prices[sym] = price
                    if sym not in self._candle_history:
                        self._candle_history[sym] = deque(maxlen=500)
                    self._candles_are_real[sym] = False
                self.UNIVERSE = candidates

            # Bootstrap klines for all candidates
            self._bootstrap_all_candles()

        except Exception as e:
            log.error(f"Universe discovery error: {e}", exc_info=True)

    def _fetch_usdt_candidates(self) -> list[tuple[str, float]]:
        """
        Returns list of (symbol, price) for all active USDT spot pairs
        sorted by 24h quote volume descending, after applying filters.
        """
        import urllib.request, json as _json

        # ── 1. Get all active USDT spot symbols ──────────────────────────────
        url = "https://api.binance.com/api/v3/exchangeInfo"
        req = urllib.request.Request(url, headers={"User-Agent": "SaiyanBot/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            info = _json.loads(resp.read().decode())

        valid_symbols = set()
        for s in info["symbols"]:
            if (s["quoteAsset"] == "USDT"
                    and s["status"] == "TRADING"
                    and s["isSpotTradingAllowed"]):
                valid_symbols.add(s["symbol"])

        # ── 2. Get 24h tickers for volume + price ────────────────────────────
        url2 = "https://api.binance.com/api/v3/ticker/24hr"
        req2 = urllib.request.Request(url2, headers={"User-Agent": "SaiyanBot/1.0"})
        with urllib.request.urlopen(req2, timeout=10) as resp2:
            tickers = _json.loads(resp2.read().decode())

        # ── 3. Filter and rank ───────────────────────────────────────────────
        # Tokens to exclude: stablecoins, fiat pairs, leveraged tokens
        EXCLUDE_CONTAINS = {"USD", "EUR", "GBP", "AUD", "BRL", "TRY",
                            "BUSD", "USDC", "TUSD", "DAI", "FDUSD",
                            "UP", "DOWN", "BULL", "BEAR", "3L", "3S",
                            "2L", "2S", "BVOL", "IBVOL"}

        def is_clean(sym: str) -> bool:
            base = sym.replace("USDT", "")
            # Skip if base asset name contains any exclusion keyword
            return not any(ex in base for ex in EXCLUDE_CONTAINS)

        ranked = []
        for t in tickers:
            sym = t["symbol"]
            if sym not in valid_symbols:
                continue
            if not is_clean(sym):
                continue
            try:
                price  = float(t["lastPrice"])
                volume = float(t["quoteVolume"])   # 24h volume in USDT
            except (ValueError, KeyError):
                continue
            if price <= 0:
                continue
            if volume < self.config.min_volume_usdt:
                continue
            ranked.append((sym, price, volume))

        # Sort by 24h USDT volume — highest liquidity first
        ranked.sort(key=lambda x: x[2], reverse=True)

        # Always keep BTCUSDT as regime anchor regardless of position
        symbols_out = [(sym, price) for sym, price, _ in ranked[:200]]
        syms_only = [s for s, _ in symbols_out]
        if self.REGIME_ANCHOR not in syms_only:
            # Find BTC in ranked and prepend it
            btc = next(((s, p) for s, p, _ in ranked if s == self.REGIME_ANCHOR), None)
            if btc:
                symbols_out = [btc] + symbols_out[:199]

        log.info(f"Top candidate: {symbols_out[0][0]} | "
                 f"Bottom candidate: {symbols_out[-1][0]} | "
                 f"Total: {len(symbols_out)}")
        return symbols_out

    def _bootstrap_all_candles(self):
        """
        Fetch real 3-min OHLCV from Binance for every symbol in UNIVERSE.
        Runs sequentially with rate-limit spacing.
        """
        log.info(f"Bootstrapping klines for {len(self.UNIVERSE)} symbols...")
        fetched = 0
        for sym, _ in self.UNIVERSE:
            try:
                candles = self._fetch_klines(sym, interval="3m", limit=300)
                if candles:
                    with self._lock:
                        self._candle_history[sym] = deque(candles, maxlen=500)
                        self._prices[sym] = candles[-1].close
                        self._candles_are_real[sym] = True
                    fetched += 1
                    log.debug(f"Bootstrapped {sym} ({len(candles)} candles, "
                              f"last close={candles[-1].close:.6f})")
            except Exception as e:
                log.warning(f"Bootstrap failed {sym}: {e}")
            time.sleep(0.08)   # ~12 req/s — safe under Binance 1200/min limit

        log.info(f"Bootstrap complete: {fetched}/{len(self.UNIVERSE)} symbols ready")

    def _fetch_klines(self, symbol: str, interval: str = "3m",
                      limit: int = 300) -> list:
        """
        Fetch OHLCV klines from Binance public REST — no auth needed.
        Returns list[Candle] sorted oldest→newest.
        """
        import urllib.request, json as _json, urllib.parse
        params = urllib.parse.urlencode({
            "symbol": symbol, "interval": interval, "limit": limit
        })
        url = f"https://api.binance.com/api/v3/klines?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": "SaiyanBot/1.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = _json.loads(resp.read().decode())
        # Binance kline format:
        # [openTime, open, high, low, close, volume, closeTime, ...]
        candles = []
        for k in data:
            candles.append(Candle(
                timestamp = k[0] / 1000.0,
                open      = float(k[1]),
                high      = float(k[2]),
                low       = float(k[3]),
                close     = float(k[4]),
                volume    = float(k[5]),
            ))
        return candles

    # ── Real price fetch (Binance public ticker, no auth needed) ─────────────
    _last_price_fetch: float = 0
    _last_candle_fetch: float = 0
    _price_fetch_interval: float  = 4.0    # spot price refresh
    _candle_fetch_interval: float = 180.0  # new 3-min bar every 180s

    def _fetch_real_prices(self) -> dict:
        """Bulk spot price fetch — Binance public, no auth."""
        try:
            import urllib.request, json as _json
            url = "https://api.binance.com/api/v3/ticker/price"
            req = urllib.request.Request(url, headers={"User-Agent": "SaiyanBot/1.0"})
            with urllib.request.urlopen(req, timeout=4) as resp:
                data = _json.loads(resp.read().decode())
            return {d["symbol"]: float(d["price"]) for d in data}
        except Exception as e:
            log.debug(f"Price fetch failed: {e}")
            return {}

    def _refresh_latest_candles(self):
        """
        Fetch the last 2 closed klines for every bootstrapped symbol.
        Called every 180s to append real closed bars to history.
        Fetching only 2 bars keeps it fast (one REST call per symbol).
        """
        log.debug("Refreshing latest candles...")
        for sym, _ in self.UNIVERSE:
            if not self._candles_are_real.get(sym):
                continue
            try:
                new_candles = self._fetch_klines(sym, interval="3m", limit=3)
                if not new_candles:
                    continue
                with self._lock:
                    existing = self._candle_history[sym]
                    last_ts = existing[-1].timestamp if existing else 0
                    for c in new_candles:
                        if c.timestamp > last_ts:
                            existing.append(c)
                            last_ts = c.timestamp
            except Exception as e:
                log.debug(f"Candle refresh {sym}: {e}")
            time.sleep(0.05)

    def tick(self):
        """
        1. Every 4s  — refresh spot prices (for live P&L / SL-TP checks)
        2. Every 180s — append real closed 3-min candle to history
        Falls back to synthetic price drift if Binance unreachable.
        """
        now = time.time()
        real_prices = {}

        # ── spot price refresh ────────────────────────────────────────────────
        if now - self._last_price_fetch >= self._price_fetch_interval:
            real_prices = self._fetch_real_prices()
            if real_prices:
                self._last_price_fetch = now

        # ── periodic candle bar refresh ───────────────────────────────────────
        if now - self._last_candle_fetch >= self._candle_fetch_interval:
            threading.Thread(
                target=self._refresh_latest_candles, daemon=True
            ).start()
            self._last_candle_fetch = now

        with self._lock:
            for sym, _ in self.UNIVERSE:
                p_prev = self._prices.get(sym, 1.0)

                if real_prices:
                    p_new = real_prices.get(sym, p_prev)
                else:
                    # Strictly hold the last known real price if Binance is unreachable
                    p_new = p_prev

                self._prices[sym] = p_new

    def get_candles(self, symbol: str, limit: int = 300) -> list:
        with self._lock:
            if not self._candles_are_real.get(symbol, False):
                return []   # refuse to serve fake history to signal engine
            hist = self._candle_history.get(symbol, deque())
            return list(hist)[-limit:]

    def is_ready(self, symbol: str) -> bool:
        """True once real Binance history has been loaded for this symbol."""
        return self._candles_are_real.get(symbol, False)

    def universe_ready(self) -> int:
        """Returns count of symbols with real history — 0 if discovery not yet done."""
        return sum(1 for v in self._candles_are_real.values() if v)

    def get_price(self, symbol: str) -> float:
        with self._lock:
            return self._prices.get(symbol, 0.0)

    def get_all_symbols(self) -> list:
        with self._lock:
            return [s for s, _ in self.UNIVERSE]

    def get_top_pairs(self, n: int = 100) -> list:
        """
        Score and rank pairs by suitability for this strategy.
        Only considers symbols with real bootstrapped history.
        Falls back to full UNIVERSE list if bootstrap not yet complete.
        """
        ranked = []
        for sym, base_price in self.UNIVERSE:
            if not self._candles_are_real.get(sym, False):
                continue   # skip symbols still on fake/no history
            candles = self.get_candles(sym, 100)
            if len(candles) < 50:
                continue
            closes = [c.close for c in candles]
            highs  = [c.high  for c in candles]
            lows   = [c.low   for c in candles]

            # Volatility score (want moderate volatility)
            atr_val = Indicators.atr(candles, 20)
            atr_pct = atr_val / closes[-1] * 100 if closes[-1] > 0 else 0
            vol_score = max(0, 1 - abs(atr_pct - 1.5) / 3)  # optimal ~1.5%

            # Trend score (want some trend, not choppy)
            ema_fast = Indicators.ema(closes, 5)
            ema_slow = Indicators.ema(closes, 20)
            ef = [v for v in ema_fast if v]
            es = [v for v in ema_slow if v]
            trend_score = 0.5
            if ef and es:
                cross_count = sum(
                    1 for i in range(1, min(len(ef), len(es)))
                    if (ef[i] > es[i]) != (ef[i-1] > es[i-1])
                )
                # Fewer crosses = more trending
                trend_score = max(0, 1 - cross_count / 20)

            score = vol_score * 0.5 + trend_score * 0.5
            ranked.append((sym, score))

        ranked.sort(key=lambda x: x[1], reverse=True)
        return [sym for sym, _ in ranked[:n]]


# ══════════════════════════════════════════════════════════════════════════════
# SIGNAL ENGINE
# ══════════════════════════════════════════════════════════════════════════════

class SignalEngine:
    """Ports the Pine Script SAIYAN OCC logic to Python"""

    def __init__(self, config: BotConfig):
        self.config = config

    def compute_ma_series(self, values: list) -> list:
        return Indicators.compute_ma(
            self.config.basis_type,
            values,
            self.config.basis_len,
            self.config.offset_sigma,
            self.config.offset_alma,
        )

    def generate_signal(self, candles: list) -> int:
        """Returns: 1=long entry, -1=short entry, 0=no signal"""
        if len(candles) < max(30, self.config.basis_len + 5):
            return 0

        closes = [c.close for c in candles]
        opens  = [c.open  for c in candles]

        # HTF series (multiplier × TF)
        htf_mult = self.config.int_res
        # Simulate HTF by resampling
        htf_closes = self._resample(closes, htf_mult, mode="close")
        htf_opens  = self._resample(opens,  htf_mult, mode="open")

        close_ma = self.compute_ma_series(closes)
        open_ma  = self.compute_ma_series(opens)
        htf_cma  = self.compute_ma_series(htf_closes)
        htf_oma  = self.compute_ma_series(htf_opens)

        # Use HTF MA if use_res, else use raw TF
        if self.config.use_res and len(htf_cma) >= 2:
            c_now  = htf_cma[-1]
            c_prev = htf_cma[-2]
            o_now  = htf_oma[-1]
            o_prev = htf_oma[-2]
        else:
            c_now  = close_ma[-1]
            c_prev = close_ma[-2] if len(close_ma) >= 2 else None
            o_now  = open_ma[-1]
            o_prev = open_ma[-2] if len(open_ma) >= 2 else None

        if None in (c_now, c_prev, o_now, o_prev):
            return 0

        # Crossover / Crossunder
        long_signal  = c_prev <= o_prev and c_now > o_now
        short_signal = c_prev >= o_prev and c_now < o_now

        if long_signal:
            return 1
        if short_signal:
            return -1
        return 0

    def _resample(self, series: list, mult: int, mode: str = "close") -> list:
        """Simulate HTF resolution without averaging"""
        if mult <= 1:
            return series
        result = []
        for i in range(0, len(series) - mult + 1, mult):
            chunk = series[i:i + mult]
            if mode == "close":
                result.append(chunk[-1])
            else:
                result.append(chunk[0])
        # Pad to same length
        if not result:
            return series
        while len(result) < len(series):
            result.insert(0, result[0])
        return result[-len(series):]


# ══════════════════════════════════════════════════════════════════════════════
# TRADE MANAGER
# ══════════════════════════════════════════════════════════════════════════════

class TradeManager:
    def __init__(self, exchange, config: BotConfig):
        self.exchange = exchange
        self.config = config
        self.open_trades: dict[str, Trade] = {}      # id → Trade
        self.closed_trades: list[Trade] = []
        self._lock = threading.Lock()
        self._trade_counter = 0
        # DB init happens once at bot startup via db_init()

    def _new_id(self) -> str:
        self._trade_counter += 1
        return f"T{self._trade_counter:04d}"

    def can_open(self, symbol: str) -> bool:
        with self._lock:
            open_count = sum(1 for t in self.open_trades.values()
                             if t.status == "open")
            already = any(t.symbol == symbol for t in self.open_trades.values())
            return open_count < self.config.max_open_trades and not already

    def open_trade(self, symbol: str, side: str, current_price: float,
                   regime: str) -> Optional[Trade]:
        balance = self.exchange.get_balance()
        usdt_amt = balance * self.config.equity_pct / 100

        result = self.exchange.place_order(symbol, side, usdt_amt, current_price)
        if not result:
            return None

        qty   = result["qty"]
        price = result["avg_price"]
        mult  = 1 if side == "long" else -1

        tp1 = price * (1 + mult * self.config.tp1_pct / 100)
        tp2 = price * (1 + mult * self.config.tp2_pct / 100)
        tp3 = price * (1 + mult * self.config.tp3_pct / 100)
        sl  = price * (1 - mult * self.config.sl_pct  / 100)

        trade = Trade(
            id=self._new_id(),
            symbol=symbol,
            side=side,
            entry_price=price,
            entry_time=datetime.utcnow().isoformat(),
            qty=qty,
            tp1=tp1, tp2=tp2, tp3=tp3, sl=sl,
            status="open",
            regime_at_entry=regime,
            current_price=price,
        )
        with self._lock:
            self.open_trades[trade.id] = trade
        db_upsert_trade(trade)
        log.info(f"OPENED {side.upper()} {symbol} @ {price:.6f} qty={qty:.4f}")
        return trade

    def update_trade(self, trade: Trade, current_price: float):
        """Check TP/SL hits and partial exits"""
        if trade.status != "open":
            return

        trade.current_price = current_price
        side = trade.side

        def hit_tp(level: float) -> bool:
            return (side == "long"  and current_price >= level) or \
                   (side == "short" and current_price <= level)

        def hit_sl(level: float) -> bool:
            return (side == "long"  and current_price <= level) or \
                   (side == "short" and current_price >= level)

        # TP1
        if not trade.tp1_hit and hit_tp(trade.tp1):
            trade.tp1_hit = True
            close_qty = trade.qty * (self.config.tp1_qty / 100.0)
            pnl = self.exchange.close_order(trade.symbol, close_qty, trade.entry_price,
                                            current_price, side)
            trade.realized_pnl += pnl
            trade.qty -= close_qty
            trade.sl = trade.entry_price
            db_upsert_trade(trade)
            log.info(f"TP1 hit {trade.symbol} pnl={pnl:.4f} | SL moved to BE")

        # TP2
        elif trade.tp1_hit and not trade.tp2_hit and hit_tp(trade.tp2):
            trade.tp2_hit = True
            
            tp1_pct = self.config.tp1_qty
            tp2_pct = self.config.tp2_qty
            fraction_of_remaining = tp2_pct / (100.0 - tp1_pct) if tp1_pct < 100 else 1.0
            close_qty = trade.qty * min(fraction_of_remaining, 1.0)
            
            pnl = self.exchange.close_order(trade.symbol, close_qty, trade.entry_price,
                                            current_price, side)
            trade.realized_pnl += pnl
            trade.qty -= close_qty
            db_upsert_trade(trade)
            log.info(f"TP2 hit {trade.symbol} pnl={pnl:.4f}")

        # TP3 / full exit
        elif trade.tp2_hit and not trade.tp3_hit and hit_tp(trade.tp3):
            trade.tp3_hit = True
            pnl = self.exchange.close_order(trade.symbol, trade.qty, trade.entry_price,
                                            current_price, side)
            trade.realized_pnl += pnl
            self._close_trade(trade, current_price, "TP3")

        # SL
        elif hit_sl(trade.sl):
            pnl = self.exchange.close_order(trade.symbol, trade.qty, trade.entry_price,
                                            current_price, side)
            trade.realized_pnl += pnl
            self._close_trade(trade, current_price, "SL")
            # Apply cooldown to the symbol — escalating with consecutive SLs
            sym_state = None
            # TradeManager doesn't hold symbol_states; cooldown applied via bot ref
            # We store it in a shared dict keyed by symbol
            _BOT_COOLDOWNS[trade.symbol] = _BOT_COOLDOWNS.get(trade.symbol, 0) + 1
            consec = _BOT_COOLDOWNS[trade.symbol]
            
            # Align cooldown with the strategy timeframe (3 minutes = 180s)
            base_cooldown = self.config.tf_minutes * 60
            cooldown_secs = min(base_cooldown * consec, 1800)   # 3m, 6m, 9m … up to 30m
            _BOT_COOLDOWN_UNTIL[trade.symbol] = time.time() + cooldown_secs
            log.info(f"Cooldown {trade.symbol} for {cooldown_secs}s (SL #{consec})")

    def _close_trade(self, trade: Trade, exit_price: float, reason: str):
        trade.status = "closed"
        trade.exit_price = exit_price
        trade.exit_time = datetime.utcnow().isoformat()
        trade.exit_reason = reason
        with self._lock:
            self.open_trades.pop(trade.id, None)
            self.closed_trades.append(trade)
        db_upsert_trade(trade)
        # Reset consecutive SL counter on a profitable exit
        if reason not in ("SL",) and trade.realized_pnl > 0:
            _BOT_COOLDOWNS[trade.symbol] = 0
            _BOT_COOLDOWN_UNTIL[trade.symbol] = 0
        log.info(f"CLOSED {trade.symbol} {reason} total_pnl={trade.realized_pnl:.4f}")

    def force_close(self, trade_id: str, current_price: float):
        with self._lock:
            trade = self.open_trades.get(trade_id)
        if trade:
            pnl = self.exchange.close_order(trade.symbol, trade.qty, trade.entry_price,
                                            current_price, trade.side)
            trade.realized_pnl += pnl
            self._close_trade(trade, current_price, "MANUAL")

    def get_stats(self) -> dict:
        closed = self.closed_trades
        if not closed:
            return {
                "total_trades": 0, "wins": 0, "losses": 0,
                "win_rate": 0, "total_pnl": 0, "avg_pnl": 0,
                "profit_factor": 0, "max_drawdown": 0,
                "avg_win": 0, "avg_loss": 0, "best_trade": 0,
                "worst_trade": 0, "open_count": len(self.open_trades),
            }
        wins   = [t for t in closed if t.realized_pnl > 0]
        losses = [t for t in closed if t.realized_pnl <= 0]
        total_pnl  = sum(t.realized_pnl for t in closed)
        gross_win  = sum(t.realized_pnl for t in wins)
        gross_loss = abs(sum(t.realized_pnl for t in losses))
        pnls = [t.realized_pnl for t in closed]
        # Max drawdown
        peak = 0; dd = 0; running = 0
        for p in pnls:
            running += p
            if running > peak: peak = running
            if (peak - running) > dd: dd = peak - running
        return {
            "total_trades": len(closed),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(len(wins) / len(closed) * 100, 1),
            "total_pnl": round(total_pnl, 2),
            "avg_pnl": round(total_pnl / len(closed), 4),
            "profit_factor": round(gross_win / gross_loss, 2) if gross_loss else 999,
            "max_drawdown": round(dd, 2),
            "avg_win": round(gross_win / len(wins), 4) if wins else 0,
            "avg_loss": round(-gross_loss / len(losses), 4) if losses else 0,
            "best_trade": round(max(pnls), 4),
            "worst_trade": round(min(pnls), 4),
            "open_count": len(self.open_trades),
        }


# ══════════════════════════════════════════════════════════════════════════════
# REGIME → PARAMETERS AUTO-ASSIGNMENT
# ══════════════════════════════════════════════════════════════════════════════

REGIME_PARAMS = {
    "TRENDING_BULLISH": {
        "trade_direction": "LONG",
        "tp1_pct": 1.2, "tp2_pct": 2.0, "tp3_pct": 3.0,
        "sl_pct": 1.5, "tp1_qty": 25.0, "tp2_qty": 25.0, "tp3_qty": 50.0,
        "description": "Strong uptrend — run profits, tight SL, longs only.",
        "fits": ["Trend following", "Breakout buying", "Momentum long"],
    },
    "TRENDING_BEARISH": {
        "trade_direction": "SHORT",
        "tp1_pct": 1.2, "tp2_pct": 2.0, "tp3_pct": 3.0,
        "sl_pct": 1.5, "tp1_qty": 25.0, "tp2_qty": 25.0, "tp3_qty": 50.0,
        "description": "Strong downtrend — run profits, tight SL, shorts only.",
        "fits": ["Trend following short", "Breakdown selling", "Momentum short"],
    },
    "SIDEWAYS": {
        "trade_direction": "BOTH",
        "tp1_pct": 0.7, "tp2_pct": 1.0, "tp3_pct": 1.3,
        "sl_pct": 1.2, "tp1_qty": 60.0, "tp2_qty": 30.0, "tp3_qty": 10.0,
        "description": "Range-bound market — take quick profits, both sides.",
        "fits": ["Range trading", "Mean reversion", "Scalping"],
    },
    "MEAN_REVERSION": {
        "trade_direction": "BOTH",
        "tp1_pct": 1.92, "tp2_pct": 3.12, "tp3_pct": 4.32,
        "sl_pct": 1.2, "tp1_qty": 85.0, "tp2_qty": 10.0, "tp3_qty": 05.0,
        "description": "Moderate volatility — 1.6:1 RR at TP1, wider SL, mean-reversion entries.",
        "fits": ["Mean reversion scalping", "Fading extremes"],
    },
    "HIGH_VOLATILITY": {
        "trade_direction": "BOTH",
        "tp1_pct": 1.5, "tp2_pct": 2.5, "tp3_pct": 4.0,
        "sl_pct": 2.5, "tp1_qty": 50.0, "tp2_qty": 30.0, "tp3_qty": 20.0,
        "description": "High vol — wider targets and SL, quick partial exits.",
        "fits": ["Breakout trading", "Momentum scalping", "News plays"],
    },
    "DETECTING": {
        "trade_direction": "BOTH",
        "tp1_pct": 0.8, "tp2_pct": 1.2, "tp3_pct": 1.6,
        "sl_pct": 0.8, "tp1_qty": 75.0, "tp2_qty": 20.0, "tp3_qty": 5.0,
        "description": "Analyzing market conditions — using optimized default parameters.",
        "fits": ["Default balanced parameters"],
    },
}


# ── Shared cooldown state (symbol → timestamp / counter) ─────────────────────
_BOT_COOLDOWNS:     dict[str, int]   = {}   # symbol → consecutive SL count
_BOT_COOLDOWN_UNTIL: dict[str, float] = {}  # symbol → epoch time to resume

# ══════════════════════════════════════════════════════════════════════════════
# MAIN BOT ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

class SaiyanBot:
    def __init__(self, config: BotConfig = None):
        self.config = config or BotConfig()
        self._running = False
        self._lock = threading.Lock()

        # Exchange
        if self.config.paper_mode:
            self.exchange = PaperExchange()
        else:
            self.exchange = LiveExchange(self.config.api_key, self.config.api_secret)

        self.data_provider = MarketDataProvider(self.config)
        self.signal_engine  = SignalEngine(self.config)
        self.trade_manager  = TradeManager(self.exchange, self.config)

        self.active_symbols: list = []
        self.symbol_states:  dict = {}
        self.regime: str = "DETECTING"
        self.regime_info: dict = REGIME_PARAMS["DETECTING"]

        self.tick_count: int = 0
        self.start_time: str = datetime.utcnow().isoformat()
        self.ai_recommendations: list = []

        # Equity curve (timestamp, equity)
        self.equity_curve: list = []
        self._last_regime_update = 0

    def _tick_loop_wrapper(self):
        """Wrapper so tick loop is always defined on the class."""
        while self._running:
            try:
                self.data_provider.tick()
            except Exception as e:
                log.error(f"Tick loop error: {e}")
            time.sleep(3)

    def start(self):
        self._running = True
        log.info("🚀 SaiyanBot starting...")

        # Init DB and restore prior session
        db_init()
        open_t, closed_t = db_load_trades()
        self.trade_manager.open_trades  = open_t
        self.trade_manager.closed_trades = closed_t
        
        # Reconstruct the exact paper balance to prevent double-funding on restart
        if self.config.paper_mode:
            total_realized = sum(t.realized_pnl for t in closed_t)
            deployed = sum(t.qty * t.entry_price for t in open_t.values())
            self.exchange.balance_usdt = self.exchange.initial_balance + total_realized - deployed
            
        # Restore trade counter to avoid ID collisions
        all_ids = [t.id for t in closed_t] + [t.id for t in open_t.values()]
        if all_ids:
            max_num = max(int(i[1:]) for i in all_ids if i[1:].isdigit())
            self.trade_manager._trade_counter = max_num
        # Restore equity curve
        self.equity_curve = db_load_equity()

        # Start data feeds immediately — bootstrap runs in background
        threading.Thread(target=self._tick_loop_wrapper, daemon=True).start()

        # Wait up to 90s for universe discovery + at least 10 kline bootstraps
        log.info("Waiting for live universe discovery + kline bootstrap...")
        deadline = time.time() + 90
        ready = 0
        while time.time() < deadline:
            universe_size = len(self.data_provider.UNIVERSE)
            ready = self.data_provider.universe_ready()
            if universe_size > 0 and ready >= 10:
                log.info(f"Bootstrap sufficient: {ready}/{universe_size} symbols ready")
                break
            time.sleep(2)

        if ready == 0:
            log.critical("No real market data available after 90s — check network/Binance access")

        # Select top pairs — scored on real bootstrapped data
        self.active_symbols = self.data_provider.get_top_pairs(self.config.top_n_pairs)
        if len(self.active_symbols) < 5:
            # Last resort: take whatever bootstrapped, unsorted
            self.active_symbols = self.data_provider.get_all_symbols()
            log.warning(f"Scoring insufficient — using all {len(self.active_symbols)} ready symbols")

        log.info(f"Selected {len(self.active_symbols)} pairs")
        for sym in self.active_symbols:
            self.symbol_states[sym] = SymbolState(symbol=sym)

        threading.Thread(target=self._main_loop, daemon=True).start()
        threading.Thread(target=self._ai_loop, daemon=True).start()

    def stop(self):
        self._running = False
        log.info("Bot stopped")

    def _main_loop(self):
        """Main strategy loop: runs every ~5s"""
        while self._running:
            try:
                self._process_cycle()
                self.tick_count += 1
                time.sleep(5)
            except Exception as e:
                log.error(f"Main loop error: {e}", exc_info=True)
                time.sleep(5)

    def _process_cycle(self):
        # 1. Update regime (every 60s)
        if time.time() - self._last_regime_update > 60:
            self._update_regime()
            self._last_regime_update = time.time()

        # 2. Update open trades (TP/SL checks)
        for trade in list(self.trade_manager.open_trades.values()):
            price = self.data_provider.get_price(trade.symbol)
            if price > 0:
                self.trade_manager.update_trade(trade, price)

        # 3. Scan for new signals
        effective_direction = self._effective_direction()
        for sym in self.active_symbols:
            try:
                self._process_symbol(sym, effective_direction)
            except Exception as e:
                log.debug(f"Symbol error {sym}: {e}")

        # 4. Record equity
        balance = self.exchange.get_balance()
        unrealized = sum(t.unrealized_pnl
                         for t in self.trade_manager.open_trades.values())
        deployed = sum(t.entry_price * t.qty 
                       for t in self.trade_manager.open_trades.values())
        total_equity = balance + deployed + unrealized
        ts = datetime.utcnow().isoformat()
        point = {"time": ts, "equity": round(total_equity, 2), "balance": round(balance, 2)}
        self.equity_curve.append(point)
        if len(self.equity_curve) > 500:
            self.equity_curve.pop(0)
        # Persist every 10th tick to avoid excessive writes
        if self.tick_count % 10 == 0:
            db_save_equity(ts, total_equity, balance)

    def _process_symbol(self, symbol: str, direction: str):
        if not self.trade_manager.can_open(symbol):
            return

        # Cooldown gate — prevents re-entering a churning pair
        state = self.symbol_states.get(symbol)
        if time.time() < _BOT_COOLDOWN_UNTIL.get(symbol, 0):
            return

        candles = self.data_provider.get_candles(symbol, 150)
        if len(candles) < 50:
            return

        signal = self.signal_engine.generate_signal(candles)
        if signal == 0:
            return
        # Deduplicate: don't re-enter on the same crossover that already triggered
        if state and signal == state.prev_signal:
            return

        # ── RSI confirmation filter ───────────────────────────────────────────
        closes = [c.close for c in candles]
        rsi = Indicators.rsi(closes, 14)
        if rsi is None:
            return
        # Long only when RSI is not already overbought; short only not oversold
        # Also reject signals when RSI is extreme (likely mean-reversion spike)
        if signal == 1  and rsi > 72:   # don't chase already-overbought for a long
            return
        if signal == -1 and rsi < 28:   # don't short already-oversold
            return

        # ── EMA trend alignment ───────────────────────────────────────────────
        ema50 = Indicators.ema(closes, 50)
        ema_val = next((v for v in reversed(ema50) if v), None)
        if ema_val:
            if signal == 1  and closes[-1] > ema_val * 1.005:  # don't long when price is already extended above EMA
                return
            if signal == -1 and closes[-1] < ema_val * 0.995:  # don't short when price is already extended below EMA
                return

        price = self.data_provider.get_price(symbol)
        if price <= 0:
            return

        side = "long" if signal == 1 else "short"
        if direction == "LONG"  and side != "long":
            return
        if direction == "SHORT" and side != "short":
            return

        trade = self.trade_manager.open_trade(symbol, side, price, self.regime)
        if trade is None:
            return

        # Track signal to prevent re-entry on the same HTF crossover
        if state:
            state.prev_signal = signal
            state.last_signal_bar = self.tick_count

    def _update_regime(self):
        # Use BTC as primary regime indicator — only if real data available
        candles = self.data_provider.get_candles("BTCUSDT", 150)
        if len(candles) < 50:
            # BTC not ready yet — try ETH, then SOL
            for fallback in ("ETHUSDT", "SOLUSDT", "BNBUSDT"):
                candles = self.data_provider.get_candles(fallback, 150)
                if len(candles) >= 50:
                    break
        if len(candles) < 50:
            log.debug("Regime detection skipped — no real candle data yet")
            return   # keep previous regime, don't overwrite with DETECTING noise
        new_regime = Indicators.detect_regime(candles)
        if new_regime != self.regime:
            log.info(f"Regime changed: {self.regime} → {new_regime}")
            self.regime = new_regime
        self.regime_info = REGIME_PARAMS.get(new_regime, REGIME_PARAMS["DETECTING"])

        # Auto-assign parameters if enabled — only when user has NOT pinned them manually
        if self.config.market_regime_auto and not self.config.params_locked:
            params = REGIME_PARAMS.get(new_regime, {})
            self.config.tp1_pct = params.get("tp1_pct", self.config.tp1_pct)
            self.config.tp2_pct = params.get("tp2_pct", self.config.tp2_pct)
            self.config.tp3_pct = params.get("tp3_pct", self.config.tp3_pct)
            self.config.sl_pct  = params.get("sl_pct",  self.config.sl_pct)
            self.config.tp1_qty = params.get("tp1_qty", self.config.tp1_qty)
            self.config.tp2_qty = params.get("tp2_qty", self.config.tp2_qty)
            self.config.tp3_qty = params.get("tp3_qty", self.config.tp3_qty)
        self.config.active_regime = new_regime
        # Direction override only when auto-mode is on and params not locked
        if self.config.market_regime_auto and not self.config.params_locked:
            pass  # direction applied via _effective_direction() dynamically

    def _effective_direction(self) -> str:
        if self.config.market_regime_auto:
            return REGIME_PARAMS.get(self.regime, {}).get(
                "trade_direction", self.config.trade_direction)
        return self.config.trade_direction

    def _ai_loop(self):
        """Generate AI-like recommendations every 2 minutes"""
        while self._running:
            time.sleep(120)
            self._generate_recommendations()

    def _generate_recommendations(self):
        stats = self.trade_manager.get_stats()
        recs = []
        wr = stats.get("win_rate", 0)
        pf = stats.get("profit_factor", 0)

        if wr < 40:
            recs.append({
                "type": "warning",
                "title": "Low Win Rate Alert",
                "message": f"Win rate at {wr}% — consider tightening entry filters or increasing MA period. Current regime: {self.regime}.",
                "action": "Review signal parameters",
                "time": datetime.utcnow().isoformat(),
            })
        if wr > 65:
            recs.append({
                "type": "positive",
                "title": "Strong Performance",
                "message": f"Win rate {wr}% is excellent. Consider increasing position size from {self.config.equity_pct}% to maximize returns.",
                "action": "Scale up positions",
                "time": datetime.utcnow().isoformat(),
            })
        if pf < 1.0 and stats["total_trades"] > 5:
            recs.append({
                "type": "critical",
                "title": "Negative Profit Factor",
                "message": f"PF={pf} — losses exceed wins. Regime is {self.regime}. Consider switching to {REGIME_PARAMS[self.regime]['trade_direction']} only.",
                "action": "Adjust trade direction",
                "time": datetime.utcnow().isoformat(),
            })

        regime_advice = {
            "TRENDING_BULLISH": "Market trending up — longs performing well. Extend TP targets.",
            "TRENDING_BEARISH": "Bear market — shorts outperforming. Consider reducing long exposure.",
            "SIDEWAYS": "Choppy market — quick scalps recommended. Reduce TP targets.",
            "MEAN_REVERSION": "Low volatility detected — mean reversion plays favored.",
            "HIGH_VOLATILITY": "High volatility — wider stops recommended to avoid whipsaws.",
        }
        if self.regime in regime_advice:
            recs.append({
                "type": "info",
                "title": f"Regime Update: {self.regime.replace('_', ' ')}",
                "message": regime_advice[self.regime],
                "action": "Auto-parameters applied" if self.config.market_regime_auto else "Manual review recommended",
                "time": datetime.utcnow().isoformat(),
            })

        if recs:
            self.ai_recommendations = recs + self.ai_recommendations[:20]

    def get_state(self) -> dict:
        """Full state snapshot for dashboard"""
        stats = self.trade_manager.get_stats()
        balance = self.exchange.get_balance()
        unrealized = sum(t.unrealized_pnl
                         for t in self.trade_manager.open_trades.values())
        deployed = sum(t.entry_price * t.qty 
                       for t in self.trade_manager.open_trades.values())
        open_trades = [t.to_dict() for t in self.trade_manager.open_trades.values()]
        closed_trades = [t.to_dict() for t in reversed(self.trade_manager.closed_trades[-100:])]

        # Pair performance table
        pair_perf = {}
        for t in self.trade_manager.closed_trades:
            if t.symbol not in pair_perf:
                pair_perf[t.symbol] = {"trades": 0, "pnl": 0, "wins": 0}
            pair_perf[t.symbol]["trades"] += 1
            pair_perf[t.symbol]["pnl"] += t.realized_pnl
            if t.realized_pnl > 0:
                pair_perf[t.symbol]["wins"] += 1

        pair_stats = [
            {
                "symbol": sym,
                "trades": v["trades"],
                "pnl": round(v["pnl"], 4),
                "win_rate": round(v["wins"] / v["trades"] * 100, 1) if v["trades"] else 0,
            }
            for sym, v in sorted(pair_perf.items(), key=lambda x: x[1]["pnl"], reverse=True)
        ]

        return {
            "running": self._running,
            "paper_mode": self.config.paper_mode,
            "regime": self.regime,
            "regime_info": self.regime_info,
            "tick_count": self.tick_count,
            "start_time": self.start_time,
            "uptime_minutes": round((datetime.utcnow() - datetime.fromisoformat(self.start_time)).total_seconds() / 60, 1),
            "balance": round(balance, 2),
            "unrealized_pnl": round(unrealized, 4),
            "total_equity": round(balance + deployed + unrealized, 2),
            "initial_balance": self.exchange.initial_balance if hasattr(self.exchange, "initial_balance") else 10000,
            "stats": stats,
            "open_trades": open_trades,
            "closed_trades": closed_trades,
            "equity_curve": self.equity_curve[-100:],
            "pair_stats": pair_stats[:20],
            "ai_recommendations": self.ai_recommendations[:10],
            "config": asdict(self.config),
            "active_symbols_count": len(self.active_symbols),
            "effective_direction": self._effective_direction(),
            "regime_params": REGIME_PARAMS,
        }

    # Keys that count as manual risk overrides — lock params when any of these arrive
    _RISK_KEYS = {"tp1_pct","tp2_pct","tp3_pct","sl_pct",
                  "tp1_qty","tp2_qty","tp3_qty"}

    def update_config(self, new_config: dict):
        with self._lock:
            for k, v in new_config.items():
                if hasattr(self.config, k):
                    setattr(self.config, k, v)
            # If user explicitly touched any risk param → lock so regime won't revert it
            if self._RISK_KEYS & set(new_config.keys()):
                self.config.params_locked = True
                log.info("Risk params manually set — regime auto-override disabled")
            # Explicitly unlocking: user toggled regime_auto ON again → release lock
            if new_config.get("market_regime_auto") is True:
                self.config.params_locked = False
                log.info("Regime auto-mode re-enabled — params_locked released")
            # Re-init exchange if mode changed
        if "paper_mode" in new_config:
            if self.config.paper_mode:
                self.exchange = PaperExchange()
                self.trade_manager.exchange = self.exchange
                total_realized = sum(t.realized_pnl for t in self.trade_manager.closed_trades)
                deployed = sum(t.qty * t.entry_price for t in self.trade_manager.open_trades.values())
                self.exchange.balance_usdt = self.exchange.initial_balance + total_realized - deployed
            log.info(f"Config updated: {new_config}")

    def close_trade(self, trade_id: str):
        price = 0.0
        trade = self.trade_manager.open_trades.get(trade_id)
        if trade:
            price = self.data_provider.get_price(trade.symbol)
        self.trade_manager.force_close(trade_id, price)

    def reset_data(self):
        with self._lock:
            self.trade_manager.open_trades.clear()
            self.trade_manager.closed_trades.clear()
            self.equity_curve.clear()
            self.trade_manager._trade_counter = 0
            
            if self.config.paper_mode:
                self.exchange.balance_usdt = self.exchange.initial_balance
            
            try:
                import sqlite3
                con = sqlite3.connect("saiyan_bot.db", timeout=10)
                cur = con.cursor()
                cur.execute("DELETE FROM trades")
                cur.execute("DELETE FROM equity_curve")
                con.commit()
                con.close()
                log.info("Database and in-memory data have been completely reset.")
            except Exception as e:
                log.error(f"Failed to reset database: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# GLOBAL BOT INSTANCE
# ══════════════════════════════════════════════════════════════════════════════

bot = SaiyanBot()
