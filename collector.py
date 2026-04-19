#!/usr/bin/env python3
"""
NSE Option Chain Collector — Standalone (No Streamlit / No Browser)
Runs on Termux (Android) or any plain Python environment.

Dependencies:
    pip install requests pandas schedule

Usage:
    python collector.py                  # uses config.json
    python collector.py --once           # fetch once and exit
    python collector.py --mock           # use local OptionChainResponse.json
    python collector.py --debug          # print full HTTP details for diagnosis
    python collector.py --skip-weekend-check  # collect even on weekends
"""

import argparse
import csv
import json
import logging
import os
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

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_CONFIG = {
    "symbols": [
        {"symbol": "ANGELONE", "type": "Equity", "expiry": "28-Apr-2026"}
    ],
    "interval_mins": 5,
    "market_open":   "09:15",
    "market_close":  "15:30",
    "mock_mode":     False,
}

BASE_URL    = "https://www.nseindia.com"
SESSION_TTL = 300   # reuse session for 5 minutes

# ── Logger ────────────────────────────────────────────────────────────────────
def setup_logger(debug: bool = False) -> logging.Logger:
    log_file = LOG_DIR / f"collector_{date.today().isoformat()}.log"
    level    = logging.DEBUG if debug else logging.INFO
    fmt      = "%(asctime)s [%(levelname)s] %(message)s"
    logger   = logging.getLogger("collector")
    logger.setLevel(level)
    # avoid duplicate handlers on re-runs
    if not logger.handlers:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(logging.Formatter(fmt))
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter(fmt))
        logger.addHandler(fh)
        logger.addHandler(sh)
    return logger

log = setup_logger()

# ── Config ────────────────────────────────────────────────────────────────────
def load_config() -> dict:
    cfg = DEFAULT_CONFIG.copy()
    if CFG_FILE.exists():
        try:
            with open(CFG_FILE, "r", encoding="utf-8") as f:
                cfg.update(json.load(f))
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
    db_path  = get_db_path(symbol, trade_date)
    csv_path = db_path.parent / f"{symbol}_{trade_date.isoformat()}.csv"
    if not db_path.exists():
        return
    try:
        conn      = sqlite3.connect(db_path)
        cur       = conn.execute("SELECT MAX(snapshot_ts) FROM option_chain_snapshots")
        latest_ts = cur.fetchone()[0]
        if not latest_ts:
            conn.close()
            return
        cur     = conn.execute(
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

HEADERS_BASE = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection":      "keep-alive",
}

WARMUP_URLS = [
    (BASE_URL,                          "text/html,*/*;q=0.8"),
    (f"{BASE_URL}/option-chain",        "text/html,*/*;q=0.8"),
    (f"{BASE_URL}/market-data-pre-open-market?series=[%22EQ%22]&",
                                         "application/json,*/*"),
]


def _make_session(debug: bool = False) -> requests.Session:
    """Create a warmed-up requests.Session that passes NSE cookie checks."""
    session = requests.Session()
    session.headers.update(HEADERS_BASE)

    for url, accept in WARMUP_URLS:
        try:
            session.headers.update({
                "Accept":  accept,
                "Referer": BASE_URL + "/",
            })
            r = session.get(url, timeout=15)
            log.debug(f"Warm-up {url[:60]} → HTTP {r.status_code} | "
                      f"cookies: {list(r.cookies.keys())}")
            if debug:
                log.debug(f"  Body preview: {r.text[:200]}")
            time.sleep(1.5)
        except Exception as e:
            log.warning(f"Warm-up hit failed ({url[:50]}): {e}")

    cookies_got = [c.name for c in session.cookies]
    if cookies_got:
        log.info(f"NSE session ready ✓  cookies: {cookies_got}")
    else:
        log.warning(
            "NSE session created but NO cookies received.\n"
            "  ► Check your internet connection and that nseindia.com is reachable.\n"
            "  ► If on mobile data, try toggling Airplane mode once."
        )
    return session


def get_session(debug: bool = False) -> requests.Session:
    now = time.time()
    if (
        _session_cache["session"] is None
        or (now - _session_cache["ts"]) > SESSION_TTL
    ):
        log.info("Creating new NSE session ...")
        _session_cache["session"] = _make_session(debug)
        _session_cache["ts"]      = now
    return _session_cache["session"]


def _decode_response_body(resp: requests.Response) -> str:
    content_encoding = resp.headers.get("Content-Encoding", "").lower()
    if "br" in content_encoding:
        try:
            import brotli
            decompressed = brotli.decompress(resp.content)
            return decompressed.decode(resp.encoding or "utf-8", errors="replace")
        except ImportError:
            log.warning(
                "  Received Brotli-compressed response but 'brotli' is not installed."
            )
            return resp.content.decode(resp.encoding or "utf-8", errors="replace")
        except Exception as e:
            log.warning(f"  Brotli decode failed: {e}")
            return resp.content.decode(resp.encoding or "utf-8", errors="replace")
    return resp.text

# ── Fetch ─────────────────────────────────────────────────────────────────────
def fetch_live(symbol: str, otype: str, expiry: str,
               debug: bool = False) -> Optional[dict]:
    api_url = (
        f"{BASE_URL}/api/option-chain-v3"
        f"?type={otype}&symbol={symbol}&expiry={expiry}"
    )
    log.info(f"[LIVE] Fetching → {api_url}")

    for attempt in range(1, 4):   # up to 3 attempts
        try:
            session = get_session(debug)
            session.headers.update({
                "Accept":           "application/json, text/plain, */*",
                "Referer":          f"{BASE_URL}/option-chain",
                "X-Requested-With": "XMLHttpRequest",
            })
            resp = session.get(api_url, timeout=20)

            content_encoding = resp.headers.get("Content-Encoding", "").lower()
            body_text = _decode_response_body(resp)

            log.debug(
                f"  Attempt {attempt}: HTTP {resp.status_code} | "
                f"Content-Type: {resp.headers.get('Content-Type','?')} | "
                f"Content-Encoding: {content_encoding or 'none'} | "
                f"Body length: {len(body_text)}"
            )
            if debug:
                log.debug(f"  Raw response preview: {body_text[:400]}")

            # ── HTTP error handling ──────────────────────────────────────────
            if resp.status_code in (401, 403):
                log.warning(f"  Session rejected (HTTP {resp.status_code}) "
                            f"— forcing session refresh (attempt {attempt})")
                _session_cache["session"] = None
                time.sleep(2)
                continue

            if resp.status_code != 200:
                log.warning(f"  Unexpected HTTP {resp.status_code} — "
                            f"body: {body_text[:200]}")
                return None

            # ── Empty body ───────────────────────────────────────────────────
            if not body_text.strip():
                log.warning(f"  Empty response body on attempt {attempt} — "
                            "refreshing session and retrying ...")
                _session_cache["session"] = None
                time.sleep(3)
                continue

            # ── Non-JSON body (HTML / Cloudflare challenge page) ─────────────
            content_type = resp.headers.get("Content-Type", "")
            if "html" in content_type.lower():
                log.warning(
                    f"  NSE returned HTML instead of JSON (attempt {attempt}).\n"
                    f"  This usually means NSE's bot-check blocked the request.\n"
                    f"  Preview: {body_text[:300]}"
                )
                _session_cache["session"] = None
                time.sleep(5)
                continue

            # ── Parse JSON ───────────────────────────────────────────────────
            try:
                data = json.loads(body_text)
            except Exception as parse_err:
                log.warning(
                    f"  JSON parse failed (attempt {attempt}): {parse_err}\n"
                    f"  Raw body: {body_text[:400]}"
                )
                _session_cache["session"] = None
                time.sleep(3)
                continue

            if not data:
                log.warning(f"  JSON parsed but empty dict/list for {symbol}")
                return None

            log.info(f"  ✓ Data received for {symbol} on attempt {attempt}")
            return data

        except requests.exceptions.ConnectionError as e:
            log.error(f"  Network error (attempt {attempt}): {e}\n"
                      "  ► Check internet connection.")
            time.sleep(5)
        except requests.exceptions.Timeout:
            log.warning(f"  Request timed out (attempt {attempt})")
            time.sleep(3)
        except Exception as e:
            log.error(f"  Unexpected error (attempt {attempt}): {e}")
            return None

    log.error(
        f"Failed to fetch {symbol} after 3 attempts.\n"
        "  ► Run: python collector.py --debug --once  for full diagnostics."
    )
    return None


def fetch_mock(symbol: str) -> Optional[dict]:
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


def fetch_option_chain(symbol: str, otype: str, expiry: str,
                       mock: bool, debug: bool = False) -> Optional[dict]:
    return fetch_mock(symbol) if mock else fetch_live(symbol, otype, expiry, debug)

# ── Parse ─────────────────────────────────────────────────────────────────────
def parse_response(raw: dict, symbol: str, expiry: str,
                   snapshot_ts: str, trade_date_str: str) -> list:
    rows = []
    try:
        data_list = (
            raw.get("filtered", {}).get("data")
            or raw.get("records", {}).get("data")
            or raw.get("data")
            or []
        )
        if not data_list:
            log.warning(f"No data rows in response. Top-level keys: {list(raw.keys())}")
            return rows

        first_ce = next((d["CE"] for d in data_list if "CE" in d), {})
        first_pe = next((d["PE"] for d in data_list if "PE" in d), {})
        spot = (
            first_ce.get("underlyingValue")
            or first_pe.get("underlyingValue")
            or raw.get("records", {}).get("underlyingValue", 0)
            or 0
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
        log.info(f"  Parsed {len(rows)} strikes | Spot: ₹{spot:,.2f}")
    except Exception as e:
        log.error(f"Parse error: {e}")
    return rows

# ── Collect one snapshot ───────────────────────────────────────────────────────
def collect_snapshot(cfg: dict, mock: bool, debug: bool = False) -> dict:
    symbol     = cfg["symbol"]
    now        = datetime.now()
    snap_ts    = now.isoformat(timespec="seconds")
    trade_date = now.date()
    result     = {"symbol": symbol, "success": False, "rows": 0,
                  "spot": None, "error": None}

    raw = fetch_option_chain(symbol, cfg.get("type", "Equity"),
                             cfg["expiry"], mock, debug)
    if not raw:
        result["error"] = "No data returned from API"
        return result

    rows = parse_response(raw, symbol, cfg["expiry"], snap_ts,
                          trade_date.isoformat())
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

    result.update(success=True, rows=len(rows),
                  spot=rows[0]["underlying_value"])
    log.info(f"✅ {symbol}: {len(rows)} strikes saved | "
             f"Spot ₹{rows[0]['underlying_value']:,.2f} | DB: {db_path.name}")
    return result

# ── Scheduler ─────────────────────────────────────────────────────────────────
def run_job(cfg: dict, mock: bool, debug: bool,
            market_open: str, market_close: str, skip_weekend: bool):
    now = datetime.now()
    if not skip_weekend and now.weekday() >= 5:
        log.debug(f"Weekend — skipping ({now.strftime('%A')})")
        return
    oh, om = map(int, market_open.split(":"))
    ch, cm = map(int, market_close.split(":"))
    open_dt  = now.replace(hour=oh, minute=om,  second=0, microsecond=0)
    close_dt = now.replace(hour=ch, minute=cm, second=0, microsecond=0)
    if not (open_dt <= now <= close_dt):
        log.debug(f"Outside market hours ({now.strftime('%H:%M')}) — skipping")
        return
    log.info(f"=== Snapshot @ {now.strftime('%H:%M:%S')} ===")
    result = collect_snapshot(cfg, mock, debug)
    if not result["success"]:
        log.warning(f"❌ {result['symbol']}: {result['error']}")


def start_scheduler(symbols: list, interval_mins: int,
                    market_open: str, market_close: str,
                    mock: bool, debug: bool, skip_weekend: bool):
    log.info("=" * 60)
    log.info(f"Scheduler started | interval={interval_mins}m | "
             f"market={market_open}–{market_close} | mock={mock}")
    log.info(f"Symbols : {[s['symbol'] for s in symbols]}")
    log.info(f"Data dir: {DATA_DIR.resolve()}")
    log.info("=" * 60)

    for sym_cfg in symbols:
        schedule.every(interval_mins).minutes.do(
            run_job, cfg=sym_cfg, mock=mock, debug=debug,
            market_open=market_open, market_close=market_close,
            skip_weekend=skip_weekend,
        )

    running = {"flag": True}
    def _stop(sig, frame):
        log.info("Shutdown signal received — stopping ...")
        running["flag"] = False
    signal.signal(signal.SIGINT,  _stop)
    signal.signal(signal.SIGTERM, _stop)

    log.info("Running initial fetch at startup ...")
    for sym_cfg in symbols:
        run_job(sym_cfg, mock, debug, market_open, market_close, skip_weekend)

    while running["flag"]:
        schedule.run_pending()
        time.sleep(10)

    log.info("Collector stopped cleanly.")

# ── CLI ────────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(
        description="NSE Option Chain Standalone Collector",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
Examples:
  python collector.py                        # auto-collect using config.json
  python collector.py --once                 # fetch once and exit
  python collector.py --debug --once         # fetch once with full HTTP logs
  python collector.py --mock --once          # test with local JSON file
  python collector.py --symbol RELIANCE --expiry 28-Apr-2026 --once
        """,
    )
    p.add_argument("--symbol",   help="Symbol e.g. ANGELONE")
    p.add_argument("--expiry",   help="Expiry e.g. 28-Apr-2026")
    p.add_argument("--type",     default="Equity", help="Equity or Index")
    p.add_argument("--interval", type=int, help="Interval minutes (3 or 5)")
    p.add_argument("--open",     help="Market open  HH:MM  (default 09:15)")
    p.add_argument("--close",    help="Market close HH:MM  (default 15:30)")
    p.add_argument("--once",     action="store_true",
                   help="Fetch once and exit (ignores market hours)")
    p.add_argument("--mock",     action="store_true",
                   help="Use OptionChainResponse.json instead of live API")
    p.add_argument("--debug",    action="store_true",
                   help="Print full HTTP request/response details")
    p.add_argument("--skip-weekend-check", action="store_true",
                   help="Skip weekend check (collect data even on Sat/Sun)")
    return p.parse_args()


def main():
    args = parse_args()

    # Re-init logger with debug level if requested
    global log
    log = setup_logger(debug=args.debug)

    cfg          = load_config()
    mock         = args.mock     or cfg.get("mock_mode",    False)
    interval     = args.interval or cfg.get("interval_mins", 5)
    market_open  = args.open     or cfg.get("market_open",  "09:15")
    market_close = args.close    or cfg.get("market_close", "15:30")
    skip_weekend = args.skip_weekend_check

    if args.symbol and args.expiry:
        symbols = [{"symbol": args.symbol.upper(),
                    "type":   args.type,
                    "expiry": args.expiry}]
    else:
        symbols = cfg.get("symbols", DEFAULT_CONFIG["symbols"])

    if not symbols:
        log.error("No symbols configured. Use --symbol/--expiry or edit config.json")
        sys.exit(1)

    log.info("=" * 60)
    log.info("NSE Option Chain Collector — Standalone")
    log.info(f"  Python   : {sys.version.split()[0]}")
    log.info(f"  Symbols  : {[s['symbol'] for s in symbols]}")
    log.info(f"  Interval : {interval} min")
    log.info(f"  Market   : {market_open} – {market_close}")
    log.info(f"  Mock     : {mock}")
    log.info(f"  Debug    : {args.debug}")
    log.info(f"  Skip Weekend: {skip_weekend}")
    log.info(f"  Data dir : {DATA_DIR.resolve()}")
    log.info("=" * 60)

    if args.once:
        log.info("--once mode: fetching once (market hours ignored)")
        for sym_cfg in symbols:
            result = collect_snapshot(sym_cfg, mock, args.debug)
            if result["success"]:
                log.info(f"✅ Done: {result['symbol']} | "
                         f"{result['rows']} strikes | Spot ₹{result['spot']:,.2f}")
            else:
                log.error(f"❌ Failed: {result['symbol']} — {result['error']}")
                log.error(
                    "\nTROUBLESHOOTING:\n"
                    "  1. Run with --debug flag to see full HTTP response:\n"
                    "       python collector.py --debug --once\n"
                    "  2. Make sure you have internet access (try: curl https://www.nseindia.com)\n"
                    "  3. If NSE blocks the IP, try toggling mobile data off/on\n"
                    "  4. Check logs in: " + str(LOG_DIR.resolve())
                )
        return

    start_scheduler(symbols, interval, market_open, market_close, mock, args.debug, skip_weekend)


if __name__ == "__main__":
    main()
