#!/usr/bin/env python3
"""
NSE Option Chain Collector
--------------------------
• Fetches option chain data from NSE at configurable intervals (3 or 5 min)
• Only runs on weekdays during market hours (9:15 – 15:30 IST)
• Stores every snapshot in SQLite (one DB per symbol per day)
• Exports a Parquet file after every insert for easy pandas/polars analysis
• Built to scale: just add more entries to CONFIG["symbols"]

Dependencies:  DrissionPage  schedule  pandas  pyarrow
Install:       pip install DrissionPage schedule pandas pyarrow
"""

import os
import time
import sqlite3
import logging
import schedule
import threading
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import pandas as pd
from DrissionPage import SessionPage

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  ←  edit this block to customise behaviour
# ──────────────────────────────────────────────────────────────────────────────
CONFIG = {
    # ── Symbols to collect ────────────────────────────────────────────────────
    # Add more dicts here to track multiple symbols.
    # "type" is "Equity" for stocks, "Index" for indices.
    # "expiry" format must match NSE: "28-Apr-2026"
    "symbols": [
        {"symbol": "ANGELONE", "type": "Equity", "expiry": "28-Apr-2026"},
        # {"symbol": "CDSL",      "type": "Equity", "expiry": "28-Apr-2026"},
        # {"symbol": "RELIANCE",  "type": "Equity", "expiry": "28-Apr-2026"},
        # {"symbol": "NIFTY",     "type": "Index",  "expiry": "24-Apr-2026"},
    ],

    # ── Scheduler ─────────────────────────────────────────────────────────────
    "interval_minutes": 3,          # 3 or 5 recommended

    # ── Market hours (IST, 24-hr) ─────────────────────────────────────────────
    "market_open":  {"hour": 9,  "minute": 15},
    "market_close": {"hour": 15, "minute": 30},

    # ── Storage ───────────────────────────────────────────────────────────────
    # Layout: <data_dir>/<SYMBOL>/<YYYY-MM-DD>/<SYMBOL>_<YYYY-MM-DD>.db
    #                                          <SYMBOL>_<YYYY-MM-DD>.parquet
    "data_dir": "./option_chain_data",

    # Set False to skip Parquet export (saves a bit of CPU per cycle)
    "export_parquet": True,

    # ── Logging ───────────────────────────────────────────────────────────────
    "log_level": "INFO",
}
# ──────────────────────────────────────────────────────────────────────────────


# ── Logging setup ─────────────────────────────────────────────────────────────
_log_dir = Path(CONFIG["data_dir"]) / "logs"
_log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=getattr(logging, CONFIG["log_level"]),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(_log_dir / f"collector_{date.today().isoformat()}.log"),
    ],
)
log = logging.getLogger("nse_collector")

BASE_URL = "https://www.nseindia.com"


# ── Database helpers ──────────────────────────────────────────────────────────

def get_db_path(symbol: str, trade_date: date) -> Path:
    """Returns path to the per-symbol per-day SQLite database."""
    d = Path(CONFIG["data_dir"]) / symbol / trade_date.isoformat()
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{symbol}_{trade_date.isoformat()}.db"


def init_db(conn: sqlite3.Connection) -> None:
    """
    Creates the snapshots table if it doesn't exist.

    Schema
    ──────
    snapshot_ts      TEXT   – ISO-8601 timestamp of the fetch (IST)
    trade_date       TEXT   – calendar date (YYYY-MM-DD)
    symbol           TEXT   – e.g. ANGELONE
    expiry           TEXT   – option expiry e.g. 28-Apr-2026
    underlying_value REAL   – spot price at snapshot time
    strike_price     REAL   – option strike
    ce_oi            INT    – Call open interest
    ce_price         REAL   – Call last traded price
    ce_iv            REAL   – Call implied volatility (%)
    pe_oi            INT    – Put open interest
    pe_price         REAL   – Put last traded price
    pe_iv            REAL   – Put implied volatility (%)
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS option_chain_snapshots (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_ts      TEXT    NOT NULL,
            trade_date       TEXT    NOT NULL,
            symbol           TEXT    NOT NULL,
            expiry           TEXT    NOT NULL,
            underlying_value REAL,
            strike_price     REAL    NOT NULL,
            ce_oi            INTEGER,
            ce_price         REAL,
            ce_iv            REAL,
            pe_oi            INTEGER,
            pe_price         REAL,
            pe_iv            REAL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ts_strike
        ON option_chain_snapshots (snapshot_ts, strike_price)
    """)
    conn.commit()


def insert_rows(conn: sqlite3.Connection, rows: list) -> None:
    conn.executemany("""
        INSERT INTO option_chain_snapshots
            (snapshot_ts, trade_date, symbol, expiry, underlying_value,
             strike_price, ce_oi, ce_price, ce_iv, pe_oi, pe_price, pe_iv)
        VALUES
            (:snapshot_ts, :trade_date, :symbol, :expiry, :underlying_value,
             :strike_price, :ce_oi, :ce_price, :ce_iv, :pe_oi, :pe_price, :pe_iv)
    """, rows)
    conn.commit()


# ── NSE fetch ─────────────────────────────────────────────────────────────────

def fetch_option_chain(symbol: str, otype: str, expiry: str) -> Optional[dict]:
    """
    Uses DrissionPage (SessionPage) to mimic a browser session.
    Step 1: Hit the equity page to warm up NSE cookies.
    Step 2: Call the option-chain-v3 API with those cookies in place.
    """
    page = SessionPage()
    try:
        # Cookie warm-up – required for NSE APIs to return data
        page.get(f"{BASE_URL}/get-quotes/equity?symbol={symbol}")
        time.sleep(1.5)

        api_url = (
            f"{BASE_URL}/api/option-chain-v3"
            f"?type={otype}&symbol={symbol}&expiry={expiry}"
        )
        log.debug(f"GET {api_url}")
        page.get(api_url)

        if page.json:
            return page.json
        log.warning(f"Empty response for {symbol}")
        return None

    except Exception as exc:
        log.error(f"Fetch error [{symbol}]: {exc}")
        return None
    finally:
        page.close()


# ── Response parser ───────────────────────────────────────────────────────────

def parse_response(
    raw: dict, symbol: str, expiry: str,
    snapshot_ts: str, trade_date: str,
) -> list:
    """
    Transforms the NSE option-chain-v3 JSON into a flat list of dicts,
    one row per strike price.

    Response structure (relevant parts):
      raw["filtered"]["data"]           – list of strike objects
        item["strikePrice"]             – strike price (float)
        item["CE"]["openInterest"]      – call OI
        item["CE"]["lastPrice"]         – call LTP
        item["CE"]["impliedVolatility"] – call IV
        item["CE"]["underlyingValue"]   – spot (same across all strikes)
        item["PE"][...]                 – same keys for puts
    """
    rows = []
    try:
        data_list = raw.get("filtered", {}).get("data", [])
        if not data_list:
            log.warning("No strike data in response.")
            return rows

        # underlyingValue is identical across all strikes in one snapshot
        first_ce = next((d["CE"] for d in data_list if "CE" in d), {})
        first_pe = next((d["PE"] for d in data_list if "PE" in d), {})
        underlying_value = (
            first_ce.get("underlyingValue") or first_pe.get("underlyingValue")
        )

        for item in data_list:
            strike = item.get("strikePrice")
            if strike is None:
                continue

            ce = item.get("CE", {})
            pe = item.get("PE", {})

            rows.append({
                "snapshot_ts":      snapshot_ts,
                "trade_date":       trade_date,
                "symbol":           symbol,
                "expiry":           expiry,
                "underlying_value": underlying_value,
                "strike_price":     float(strike),
                "ce_oi":            ce.get("openInterest"),
                "ce_price":         ce.get("lastPrice"),
                "ce_iv":            ce.get("impliedVolatility"),
                "pe_oi":            pe.get("openInterest"),
                "pe_price":         pe.get("lastPrice"),
                "pe_iv":            pe.get("impliedVolatility"),
            })

    except Exception as exc:
        log.error(f"Parse error: {exc}")

    return rows


# ── Parquet export ────────────────────────────────────────────────────────────

def export_parquet(symbol: str, trade_date: date) -> None:
    """
    Reads the day's SQLite table and writes a Parquet file (Snappy compressed).
    Called after every insert – the file is always current.
    Parquet is ideal for later analysis with pandas / polars.
    """
    db_path      = get_db_path(symbol, trade_date)
    parquet_path = db_path.parent / f"{symbol}_{trade_date.isoformat()}.parquet"
    try:
        conn = sqlite3.connect(db_path)
        df   = pd.read_sql(
            "SELECT * FROM option_chain_snapshots ORDER BY snapshot_ts, strike_price",
            conn,
        )
        conn.close()
        df.to_parquet(parquet_path, index=False, compression="snappy")
        log.debug(f"Parquet → {parquet_path.name}  ({len(df):,} rows)")
    except Exception as exc:
        log.warning(f"Parquet export failed [{symbol}]: {exc}")


# ── Core job ──────────────────────────────────────────────────────────────────

def collect_snapshot(symbol: str, otype: str, expiry: str) -> None:
    """Fetch → parse → store one snapshot for one symbol."""
    now          = datetime.now()
    snapshot_ts  = now.isoformat(timespec="seconds")
    trade_date   = now.date()

    log.info(f"▶ {symbol} [{expiry}] @ {now.strftime('%H:%M:%S')}")

    raw = fetch_option_chain(symbol, otype, expiry)
    if not raw:
        return

    rows = parse_response(raw, symbol, expiry, snapshot_ts, trade_date.isoformat())
    if not rows:
        log.warning(f"  No rows parsed for {symbol}.")
        return

    db_path = get_db_path(symbol, trade_date)
    conn    = sqlite3.connect(db_path)
    init_db(conn)
    insert_rows(conn, rows)
    conn.close()

    log.info(f"  ✓ {len(rows)} strikes stored  →  {db_path.name}")

    if CONFIG["export_parquet"]:
        export_parquet(symbol, trade_date)


def run_all_symbols() -> None:
    """
    Scheduler callback – runs every N minutes.
    Guards:
      • Skip weekends (Saturday=5, Sunday=6)
      • Skip if outside 09:15 – 15:30 IST
    Each symbol is fetched in its own thread for parallel execution.
    """
    now = datetime.now()

    if now.weekday() >= 5:
        log.debug("Weekend – skipping collection.")
        return

    open_time  = now.replace(**CONFIG["market_open"],  second=0, microsecond=0)
    close_time = now.replace(**CONFIG["market_close"], second=0, microsecond=0)

    if not (open_time <= now <= close_time):
        log.debug(f"Outside market hours ({now.strftime('%H:%M')}) – skipping.")
        return

    threads = [
        threading.Thread(
            target=collect_snapshot,
            args=(c["symbol"], c["type"], c["expiry"]),
            daemon=True,
        )
        for c in CONFIG["symbols"]
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=" * 60)
    log.info("NSE Option Chain Collector  –  starting")
    log.info(f"  Symbols  : {[c['symbol'] for c in CONFIG['symbols']]}")
    log.info(f"  Interval : every {CONFIG['interval_minutes']} minute(s)")
    log.info(f"  Data dir : {Path(CONFIG['data_dir']).resolve()}")
    log.info("=" * 60)

    # Fire immediately (respects market-hour guard)
    run_all_symbols()

    schedule.every(CONFIG["interval_minutes"]).minutes.do(run_all_symbols)

    log.info("Scheduler running.  Press Ctrl+C to stop.\n")
    try:
        while True:
            schedule.run_pending()
            time.sleep(10)
    except KeyboardInterrupt:
        log.info("\nCollector stopped.")


if __name__ == "__main__":
    main()
