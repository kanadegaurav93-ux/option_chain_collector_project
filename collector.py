#!/usr/bin/env python3
"""
NSE Option Chain Collector — Standalone (No Streamlit / No Browser)
Runs on Termux (Android) or any plain Python environment.

Dependencies (all lightweight):
    pip install requests pandas schedule

Usage:
    python collector.py                        # uses config.json if present
    python collector.py --symbol ANGELONE --expiry 28-Apr-2026
    python collector.py --once                 # fetch once and exit
    python collector.py --mock                 # use local OptionChainResponse.json

Data is stored in:
    ./option_chain_data/<SYMBOL>/<YYYY-MM-DD>/<SYMBOL>_<YYYY-MM-DD>.db  (SQLite)
    ./option_chain_data/<SYMBOL>/<YYYY-MM-DD>/<SYMBOL>_<YYYY-MM-DD>.csv (CSV export)

Log file:
    ./option_chain_data/logs/collector_<YYYY-MM-DD>.log
"""

import argparse
import csv
import json
import logging
import signal
import sqlite3
import sys
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import requests
import schedule

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).parent
DATA_DIR  = BASE_DIR / "option_chain_data"
LOG_DIR   = DATA_DIR / "logs"
MOCK_FILE = BASE_DIR / "OptionChainResponse.json"
CFG_FILE  = BASE_DIR / "config.json"

DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── Config defaults (overridden by config.json or CLI args) ───────────────────
DEFAULT_CONFIG = {
    "symbols": [
        {"symbol": "ANGELONE", "type": "Equity", "expiry": "28-Apr-2026"}
    ],
    "interval_mins": 5,
    "market_open":   "09:15",
    "market_close":  "15:30",
    "mock_mode":     False,
}

BASE_URL = "https://www.nseindia.com"

# ── Logging ───────────────────────────────────────────────────────────────────
def setup_logger() -> logging.Logger:
    log_file = LOG_DIR / f"collector_{date.today().isoformat()}.log"
    fmt      = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger("collector")

log = setup_logger()

# ── Config loader ─────────────────────────────────────────────────────────────
def load_config() -> dict:
    cfg = DEFAULT_CONFIG.copy()
    if CFG_FILE.exists():
        try:
            with open(CFG_FILE, "r", encoding="utf-8") as f:
                user_cfg = json.load(f)
            cfg.update(user_cfg)
            log.info(f"Config loaded from {CFG_FILE.name}")
        except Exception as e:
            log.warning(f"Could not read config.json: {e} — using defaults")
    return cfg

# ── DB helpers ────────────────────────────────────────────────────────────────
def get_db_path(symbol: str, trade_date: date) -> Path:
    d = DATA_DIR / symbol / trade_date.isoformat()
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{symbol}_{trade_date.isoformat()}.db"


def init_db(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS option_chain_snapshots (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_ts    TEXT    NOT NULL,
            trade_date     TEXT    NOT NULL,
            symbol         TEXT    NOT NULL,
            expiry         TEXT    NOT NULL,
            underlying_value REAL,
            strike_price   REAL    NOT NULL,
            ce_oi          INTEGER,
            ce_price       REAL,
            ce_iv          REAL,
            pe_oi          INTEGER,
            pe_price       REAL,
            pe_iv          REAL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ts_strike
        ON option_chain_snapshots (snapshot_ts, strike_price)
    """)
    conn.commit()


def insert_rows(conn: sqlite3.Connection, rows: list):
    conn.executemany("""
        INSERT INTO option_chain_snapshots
            (snapshot_ts, trade_date, symbol, expiry, underlying_value,
             strike_price, ce_oi, ce_price, ce_iv, pe_oi, pe_price, pe_iv)
        VALUES
            (:snapshot_ts, :trade_date, :symbol, :expiry, :underlying_value,
             :strike_price, :ce_oi, :ce_price, :ce_iv, :pe_oi, :pe_price, :pe_iv)
    """, rows)
    conn.commit()


def export_csv(symbol: str, trade_date: date):
    """Append today's latest snapshot to a daily CSV file."""
    db_path  = get_db_path(symbol, trade_date)
    csv_path = db_path.parent / f"{symbol}_{trade_date.isoformat()}.csv"
    if not db_path.exists():
        return
    try:
        conn = sqlite3.connect(db_path)
        cur  = conn.execute(
            "SELECT MAX(snapshot_ts) FROM option_chain_snapshots"
        )
        latest_ts = cur.fetchone()[0]
        if not latest_ts:
            conn.close()
            return
        cur = conn.execute(
            "SELECT * FROM option_chain_snapshots WHERE snapshot_ts=? ORDER BY strike_price",
            (latest_ts,)
        )
        rows    = cur.fetchall()
        headers = [d[0] for d in cur.description]
        conn.close()
        write_header = not csv_path.exists()
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(headers)
            writer.writerows(rows)
        log.info(f"CSV updated → {csv_path.name}")
    except Exception as e:
        log.warning(f"CSV export failed: {e}")

# ── NSE Session ───────────────────────────────────────────────────────────────
_session_cache: dict = {"session": None, "ts": 0.0}
SESSION_TTL = 300  # seconds — reuse session for 5 minutes


def _make_session() -> requests.Session:
    """Create a warmed-up requests.Session with NSE-compatible headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer":         "https://www.nseindia.com/",
        "Connection":      "keep-alive",
    })
    try:
        log.info("Warming up NSE session (homepage) ...")
        session.get(BASE_URL, timeout=15)
        time.sleep(1.5)
        log.info("Warming up NSE session (option-chain page) ...")
        session.get(f"{BASE_URL}/option-chain", timeout=15)
        time.sleep(1.0)
        log.info("NSE session ready ✓")
    except Exception as e:
        log.warning(f"Warm-up failed (will still try API): {e}")
    return session


def get_session() -> requests.Session:
    """Return cached session; recreate if expired."""
    now = time.time()
    if (
        _session_cache["session"] is None
        or (now - _session_cache["ts"]) > SESSION_TTL
    ):
        _session_cache["session"] = _make_session()
        _session_cache["ts"]      = now
    return _session_cache["session"]

# ── Fetch ─────────────────────────────────────────────────────────────────────
def fetch_live(symbol: str, otype: str, expiry: str) -> Optional[dict]:
    """Fetch option chain from NSE API — no browser required."""
    api_url = (
        f"{BASE_URL}/api/option-chain-v3"
        f"?type={otype}&symbol={symbol}&expiry={expiry}"
    )
    log.info(f"[LIVE] {api_url}")
    for attempt in range(1, 3):
        try:
            session = get_session()
            session.headers.update({
                "Accept":  "application/json, text/plain, */*",
                "Referer": f"{BASE_URL}/option-chain",
            })
            resp = session.get(api_url, timeout=20)
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    log.info(f"  ✓ JSON received for {symbol} (attempt {attempt})")
                    return data
                log.warning(f"  Empty response for {symbol}")
                return None
            elif resp.status_code in (401, 403):
                log.warning(f"  HTTP {resp.status_code} — refreshing session ...")
                _session_cache["session"] = None  # force recreate
            else:
                log.warning(f"  HTTP {resp.status_code} for {symbol}")
                return None
        except requests.exceptions.Timeout:
            log.warning(f"  Timeout on attempt {attempt} for {symbol}")
        except Exception as e:
            log.error(f"  Fetch error [{symbol}]: {e}")
            return None
    log.error(f"Failed to fetch {symbol} after 2 attempts.")
    return None


def fetch_mock(symbol: str) -> Optional[dict]:
    """Load data from local OptionChainResponse.json (offline testing)."""
    if not MOCK_FILE.exists():
        log.error(f"Mock file not found: {MOCK_FILE.resolve()}")
        return None
    try:
        with open(MOCK_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        log.info(f"[MOCK] Loaded {MOCK_FILE.name} ({MOCK_FILE.stat().st_size // 1024} KB)")
        return data
    except Exception as e:
        log.error(f"Failed to read mock file: {e}")
        return None


def fetch_option_chain(symbol: str, otype: str, expiry: str, mock: bool) -> Optional[dict]:
    return fetch_mock(symbol) if mock else fetch_live(symbol, otype, expiry)

# ── Parse ─────────────────────────────────────────────────────────────────────
def parse_response(raw: dict, symbol: str, expiry: str,
                   snapshot_ts: str, trade_date_str: str) -> list:
    rows = []
    try:
        data_list = raw.get("filtered", {}).get("data", raw.get("data", []))
        if not data_list:
            log.warning(f"No data rows found. Keys: {list(raw.keys())}")
            return rows

        first_ce = next((d["CE"] for d in data_list if "CE" in d), {})
        first_pe = next((d["PE"] for d in data_list if "PE" in d), {})
        spot = (
            first_ce.get("underlyingValue")
            or first_pe.get("underlyingValue")
            or raw.get("records", {}).get("underlyingValue", 0)
        )

        for item in data_list:
            strike = item.get("strikePrice")
            if strike is None:
                continue
            ce = item.get("CE") or {}
            pe = item.get("PE") or {}
            rows.append({
                "snapshot_ts":      snapshot_ts,
                "trade_date":       trade_date_str,
                "symbol":           symbol,
                "expiry":           expiry,
                "underlying_value": spot,
                "strike_price":     float(strike),
                "ce_oi":            ce.get("openInterest"),
                "ce_price":         ce.get("lastPrice"),
                "ce_iv":            ce.get("impliedVolatility"),
                "pe_oi":            pe.get("openInterest"),
                "pe_price":         pe.get("lastPrice"),
                "pe_iv":            pe.get("impliedVolatility"),
            })
        log.info(f"  Parsed {len(rows)} strikes for {symbol} | Spot: {spot}")
    except Exception as e:
        log.error(f"Parse error: {e}")
    return rows

# ── Collect one snapshot ───────────────────────────────────────────────────────
def collect_snapshot(cfg: dict, mock: bool) -> dict:
    symbol = cfg["symbol"]
    now    = datetime.now()
    snap_ts   = now.isoformat(timespec="seconds")
    trade_date = now.date()

    result = {"symbol": symbol, "success": False, "rows": 0, "spot": None, "error": None}

    raw = fetch_option_chain(symbol, cfg.get("type", "Equity"), cfg["expiry"], mock)
    if not raw:
        result["error"] = "No data returned from API"
        return result

    rows = parse_response(raw, symbol, cfg["expiry"], snap_ts, trade_date.isoformat())
    if not rows:
        result["error"] = "0 rows parsed from response"
        return result

    db_path = get_db_path(symbol, trade_date)
    conn    = sqlite3.connect(db_path)
    init_db(conn)
    insert_rows(conn, rows)
    conn.close()

    try:
        export_csv(symbol, trade_date)
    except Exception as e:
        log.warning(f"CSV export skipped: {e}")

    result.update(success=True, rows=len(rows), spot=rows[0]["underlying_value"])
    log.info(f"  ✅ {symbol}: {len(rows)} strikes saved | Spot ₹{rows[0]['underlying_value']:,.2f}")
    return result

# ── Scheduler job ──────────────────────────────────────────────────────────────
def run_job(cfg: dict, mock: bool, market_open: str, market_close: str):
    now = datetime.now()

    # Skip weekends
    if now.weekday() >= 5:
        log.debug(f"Weekend — skipping ({now.strftime('%A')})")
        return

    # Skip outside market hours
    open_h,  open_m  = map(int, market_open.split(":"))
    close_h, close_m = map(int, market_close.split(":"))
    open_dt  = now.replace(hour=open_h,  minute=open_m,  second=0, microsecond=0)
    close_dt = now.replace(hour=close_h, minute=close_m, second=0, microsecond=0)
    if not (open_dt <= now <= close_dt):
        log.debug(f"Outside market hours ({now.strftime('%H:%M')}) — skipping")
        return

    log.info(f"=== Collecting snapshot @ {now.strftime('%H:%M:%S')} ===")
    result = collect_snapshot(cfg, mock)
    if not result["success"]:
        log.warning(f"  ❌ {result['symbol']}: {result['error']}")


def start_scheduler(symbols: list, interval_mins: int,
                    market_open: str, market_close: str, mock: bool):
    log.info(f"Scheduler started | interval={interval_mins}m | "
             f"market={market_open}–{market_close} | mock={mock}")
    log.info(f"Symbols: {[s['symbol'] for s in symbols]}")

    for sym_cfg in symbols:
        schedule.every(interval_mins).minutes.do(
            run_job, cfg=sym_cfg, mock=mock,
            market_open=market_open, market_close=market_close
        )

    # Graceful shutdown on Ctrl+C / SIGTERM
    running = {"flag": True}
    def _stop(sig, frame):
        log.info("Shutdown signal received — stopping ...")
        running["flag"] = False
    signal.signal(signal.SIGINT,  _stop)
    signal.signal(signal.SIGTERM, _stop)

    # Run once immediately at startup
    log.info("Running initial fetch ...")
    for sym_cfg in symbols:
        run_job(sym_cfg, mock, market_open, market_close)

    while running["flag"]:
        schedule.run_pending()
        time.sleep(10)

    log.info("Collector stopped.")

# ── CLI ────────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="NSE Option Chain Standalone Collector")
    p.add_argument("--symbol",   help="Symbol name e.g. ANGELONE")
    p.add_argument("--expiry",   help="Expiry date e.g. 28-Apr-2026")
    p.add_argument("--type",     default="Equity", help="Equity or Index")
    p.add_argument("--interval", type=int, help="Fetch interval in minutes (3 or 5)")
    p.add_argument("--open",     default=None, help="Market open time HH:MM")
    p.add_argument("--close",    default=None, help="Market close time HH:MM")
    p.add_argument("--once",     action="store_true", help="Fetch once and exit")
    p.add_argument("--mock",     action="store_true", help="Use local mock JSON file")
    return p.parse_args()


def main():
    args = parse_args()
    cfg  = load_config()

    # CLI args override config.json
    mock         = args.mock or cfg.get("mock_mode", False)
    interval     = args.interval or cfg.get("interval_mins", 5)
    market_open  = args.open  or cfg.get("market_open",  "09:15")
    market_close = args.close or cfg.get("market_close", "15:30")

    # Build symbols list — CLI takes priority
    if args.symbol and args.expiry:
        symbols = [{"symbol": args.symbol.upper(),
                    "type":   args.type,
                    "expiry": args.expiry}]
    else:
        symbols = cfg.get("symbols", DEFAULT_CONFIG["symbols"])

    if not symbols:
        log.error("No symbols configured. Use --symbol / --expiry or edit config.json")
        sys.exit(1)

    log.info("=" * 60)
    log.info("NSE Option Chain Collector — Standalone")
    log.info(f"  Symbols  : {[s['symbol'] for s in symbols]}")
    log.info(f"  Interval : {interval} min")
    log.info(f"  Market   : {market_open} – {market_close}")
    log.info(f"  Mock     : {mock}")
    log.info(f"  Data dir : {DATA_DIR.resolve()}")
    log.info("=" * 60)

    if args.once:
        # Single fetch and exit
        log.info("--once flag: fetching once and exiting")
        for sym_cfg in symbols:
            collect_snapshot(sym_cfg, mock)
        log.info("Done.")
        return

    start_scheduler(symbols, interval, market_open, market_close, mock)


if __name__ == "__main__":
    main()
