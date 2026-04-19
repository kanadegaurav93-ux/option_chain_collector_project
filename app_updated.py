#!/usr/bin/env python3
"""
NSE Option Chain Collector — Streamlit Web UI (v7 - with Live Chain Viewer)
Run: streamlit run app.py
"""

import json
import sqlite3
import threading
import time
import schedule
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from DrissionPage import SessionPage

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NSE Option Chain Collector",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Constants ─────────────────────────────────────────────────────────────────
BASE_URL  = "https://www.nseindia.com"
DATA_DIR  = Path("./option_chain_data")
MOCK_FILE = Path("./OptionChainResponse.json")
DATA_DIR.mkdir(parents=True, exist_ok=True)
(DATA_DIR / "logs").mkdir(exist_ok=True)

POPULAR_SYMBOLS = [
    "ANGELONE","CDSL","CAMS","HAVELLS","TCS","INFY","RELIANCE",
    "LT","BAJFINANCE","HDFCBANK","ICICIBANK","SBIN","ITC",
    "WIPRO","TATAMOTORS","MARUTI","HINDALCO","JIOFIN","EXIDEIND",
]

# ── Session state ─────────────────────────────────────────────────────────────
_defaults = {
    "collector_running": False,
    "log_lines":         [],
    "last_fetch_ts":     None,
    "total_rows_stored": 0,
    "fetch_count":       0,
    "last_error":        None,
    "mock_mode":         MOCK_FILE.exists(),
    "symbols_config": [
        {"symbol": "ANGELONE", "type": "Equity", "expiry": "28-Apr-2026"}
    ],
    "oc_snap":          None,
    "oc_error":         None,
    "viewer_snaps":     [],
    "viewer_last_sym":  None,
    "viewer_last_date": None,
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Logging ───────────────────────────────────────────────────────────────────
_log_lock = threading.Lock()

def _log(msg: str, level: str = "INFO"):
    ts   = datetime.now().strftime("%H:%M:%S")
    line = f"{ts} [{level}] {msg}"
    with _log_lock:
        st.session_state["log_lines"].append(line)
        if len(st.session_state["log_lines"]) > 300:
            st.session_state["log_lines"] = st.session_state["log_lines"][-300:]
    try:
        log_file = DATA_DIR / "logs" / f"collector_{date.today().isoformat()}.log"
        with open(log_file, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass

# ── DB / storage helpers ──────────────────────────────────────────────────────
def get_db_path(symbol: str, trade_date: date) -> Path:
    d = DATA_DIR / symbol / trade_date.isoformat()
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{symbol}_{trade_date.isoformat()}.db"

def init_db(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS option_chain_snapshots (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_ts      TEXT NOT NULL,
            trade_date       TEXT NOT NULL,
            symbol           TEXT NOT NULL,
            expiry           TEXT NOT NULL,
            underlying_value REAL,
            strike_price     REAL NOT NULL,
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

def insert_rows(conn, rows):
    conn.executemany("""
        INSERT INTO option_chain_snapshots
        (snapshot_ts,trade_date,symbol,expiry,underlying_value,
         strike_price,ce_oi,ce_price,ce_iv,pe_oi,pe_price,pe_iv)
        VALUES
        (:snapshot_ts,:trade_date,:symbol,:expiry,:underlying_value,
         :strike_price,:ce_oi,:ce_price,:ce_iv,:pe_oi,:pe_price,:pe_iv)
    """, rows)
    conn.commit()

def load_latest_snapshot(symbol: str) -> pd.DataFrame:
    db = get_db_path(symbol, date.today())
    if not db.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(db)
    ts = conn.execute("SELECT MAX(snapshot_ts) FROM option_chain_snapshots").fetchone()[0]
    if not ts:
        conn.close()
        return pd.DataFrame()
    df = pd.read_sql(
        "SELECT * FROM option_chain_snapshots WHERE snapshot_ts=? ORDER BY strike_price",
        conn, params=(ts,)
    )
    conn.close()
    return df

def load_day(symbol: str, trade_date: date) -> pd.DataFrame:
    """Load all snapshots for a symbol on a given day."""
    pq = DATA_DIR / symbol / trade_date.isoformat() / f"{symbol}_{trade_date.isoformat()}.parquet"
    db = get_db_path(symbol, trade_date)
    try:
        if pq.exists():
            return pd.read_parquet(pq)
        if db.exists():
            conn = sqlite3.connect(db)
            df = pd.read_sql(
                "SELECT * FROM option_chain_snapshots ORDER BY snapshot_ts, strike_price", conn
            )
            conn.close()
            return df
    except Exception as e:
        _log(f"load_day error [{symbol} {trade_date}]: {e}", "ERROR")
    return pd.DataFrame()

def load_date_range(symbol: str, start: date, end: date) -> pd.DataFrame:
    """Load and concatenate data across a date range."""
    frames  = []
    current = start
    while current <= end:
        df = load_day(symbol, current)
        if not df.empty:
            frames.append(df)
        current += timedelta(days=1)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df["snapshot_ts"] = pd.to_datetime(df["snapshot_ts"])
    return df

def get_available_dates(symbol: str) -> list:
    """Return sorted list of dates that have data for a symbol."""
    sym_dir = DATA_DIR / symbol
    if not sym_dir.exists():
        return []
    dates = []
    for d in sorted(sym_dir.iterdir()):
        if d.is_dir():
            try:
                dates.append(date.fromisoformat(d.name))
            except ValueError:
                pass
    return sorted(dates)

def load_pcr_series(symbol: str, trade_date: date = None) -> pd.DataFrame:
    df = load_day(symbol, trade_date or date.today())
    if df.empty:
        return pd.DataFrame()
    df["snapshot_ts"] = pd.to_datetime(df["snapshot_ts"])
    grp = df.groupby("snapshot_ts").agg(
        total_pe_oi=("pe_oi", "sum"),
        total_ce_oi=("ce_oi", "sum")
    ).reset_index()
    grp["PCR"] = grp["total_pe_oi"] / grp["total_ce_oi"].replace(0, float("nan"))
    return grp

def load_oi_series(symbol: str, strike: float, trade_date: date = None) -> pd.DataFrame:
    db = get_db_path(symbol, trade_date or date.today())
    if not db.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(db)
    df = pd.read_sql("""
        SELECT snapshot_ts, ce_oi, pe_oi, underlying_value
        FROM option_chain_snapshots WHERE strike_price=?
        ORDER BY snapshot_ts
    """, conn, params=(strike,))
    conn.close()
    df["snapshot_ts"] = pd.to_datetime(df["snapshot_ts"])
    return df

def count_snapshots_today(symbol: str) -> int:
    db = get_db_path(symbol, date.today())
    if not db.exists():
        return 0
    conn = sqlite3.connect(db)
    n = conn.execute("SELECT COUNT(DISTINCT snapshot_ts) FROM option_chain_snapshots").fetchone()[0]
    conn.close()
    return n

def export_parquet(symbol: str, trade_date: date):
    db = get_db_path(symbol, trade_date)
    pq = db.parent / f"{symbol}_{trade_date.isoformat()}.parquet"
    conn = sqlite3.connect(db)
    df = pd.read_sql("SELECT * FROM option_chain_snapshots ORDER BY snapshot_ts, strike_price", conn)
    conn.close()
    df.to_parquet(pq, index=False, compression="snappy")
    _log(f"Parquet written: {pq.name}")

# ── NSE fetch ─────────────────────────────────────────────────────────────────
def fetch_live(symbol: str, otype: str, expiry: str) -> Optional[dict]:
    _log(f"[LIVE] NSE session for {symbol} ...")
    page = SessionPage()
    try:
        page.get(f"{BASE_URL}/get-quotes/equity?symbol={symbol}")
        time.sleep(2)
        api_url = f"{BASE_URL}/api/option-chain-v3?type={otype}&symbol={symbol}&expiry={expiry}"
        _log(f"  API → {api_url}")
        page.get(api_url)
        if page.json:
            _log(f"  ✓ JSON received for {symbol}")
            return page.json
        raw_text = page.html or ""
        msg = f"No JSON for {symbol}. Preview: {raw_text[:300]}"
        _log(msg, "WARN")
        st.session_state["last_error"] = f"No JSON from NSE for {symbol}. Try Mock Mode for offline testing."
        return None
    except Exception as e:
        msg = f"Fetch error [{symbol}]: {e}"
        _log(msg, "ERROR")
        st.session_state["last_error"] = msg
        return None
    finally:
        page.close()

def fetch_mock(symbol: str) -> Optional[dict]:
    if not MOCK_FILE.exists():
        msg = f"Mock file not found: {MOCK_FILE.resolve()}"
        _log(msg, "ERROR")
        st.session_state["last_error"] = msg
        return None
    _log(f"[MOCK] Loading {MOCK_FILE.name} for {symbol}")
    try:
        with open(MOCK_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        _log(f"  ✓ Mock loaded ({MOCK_FILE.stat().st_size // 1024} KB)")
        return data
    except Exception as e:
        msg = f"Failed to read mock file: {e}"
        _log(msg, "ERROR")
        st.session_state["last_error"] = msg
        return None

def fetch_option_chain(symbol: str, otype: str, expiry: str) -> Optional[dict]:
    return fetch_mock(symbol) if st.session_state.get("mock_mode") else fetch_live(symbol, otype, expiry)

def parse_response(raw, symbol, expiry, snapshot_ts, trade_date_str) -> list:
    rows = []
    try:
        data_list = raw.get("filtered", {}).get("data", [])
        if not data_list:
            msg = f"'filtered.data' empty. Keys: {list(raw.keys())}"
            _log(msg, "WARN")
            st.session_state["last_error"] = msg
            return rows
        first_ce = next((d["CE"] for d in data_list if "CE" in d), {})
        first_pe = next((d["PE"] for d in data_list if "PE" in d), {})
        spot = first_ce.get("underlyingValue") or first_pe.get("underlyingValue")
        _log(f"  Spot=₹{spot} Strikes={len(data_list)}")
        for item in data_list:
            strike = item.get("strikePrice")
            if strike is None:
                continue
            ce = item.get("CE", {})
            pe = item.get("PE", {})
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
        _log(f"  Parsed {len(rows)} rows for {symbol}")
    except Exception as e:
        _log(f"Parse error: {e}", "ERROR")
        st.session_state["last_error"] = str(e)
    return rows

def collect_snapshot_sync(cfg: dict) -> dict:
    symbol = cfg["symbol"]
    now    = datetime.now()
    snap_ts    = now.isoformat(timespec="seconds")
    tdate      = now.date()
    result = {"symbol": symbol, "success": False, "rows": 0, "spot": None, "error": None}

    raw = fetch_option_chain(symbol, cfg["type"], cfg["expiry"])
    if not raw:
        result["error"] = st.session_state.get("last_error", "No data")
        return result

    rows = parse_response(raw, symbol, cfg["expiry"], snap_ts, tdate.isoformat())
    if not rows:
        result["error"] = st.session_state.get("last_error", "0 rows parsed")
        return result

    db   = get_db_path(symbol, tdate)
    conn = sqlite3.connect(db)
    init_db(conn)
    insert_rows(conn, rows)
    conn.close()

    try:
        export_parquet(symbol, tdate)
    except Exception as e:
        _log(f"  Parquet skipped: {e}", "WARN")

    st.session_state["last_fetch_ts"]    = snap_ts
    st.session_state["total_rows_stored"] += len(rows)
    st.session_state["fetch_count"]      += 1
    result.update({"success": True, "rows": len(rows), "spot": rows[0]["underlying_value"]})
    _log(f"✓ {symbol}: {len(rows)} rows. Spot=₹{rows[0]['underlying_value']}")
    return result

def _bg_scheduler(interval_mins, market_open, market_close, skip_weekend):
    def job():
        if not st.session_state.get("collector_running"):
            return schedule.CancelJob
        now = datetime.now()
        if not skip_weekend and now.weekday() >= 5:
            _log("Weekend – skipping.", "DEBUG"); return
        open_dt  = now.replace(hour=market_open[0],  minute=market_open[1],  second=0)
        close_dt = now.replace(hour=market_close[0], minute=market_close[1], second=0)
        if not (open_dt <= now <= close_dt):
            _log(f"Outside market hours ({now.strftime('%H:%M')}) – skipping.", "DEBUG"); return
        for cfg in list(st.session_state.get("symbols_config", [])):
            threading.Thread(target=collect_snapshot_sync, args=(cfg,), daemon=True).start()

    schedule.clear("collector")
    schedule.every(interval_mins).minutes.do(job).tag("collector")
    job()
    while st.session_state.get("collector_running"):
        schedule.run_pending()
        time.sleep(10)
    schedule.clear("collector")
    _log("Scheduler stopped.")

# ── Option Chain Viewer helpers ──────────────────────────────────────────────

_VIEWER_STYLE = '\n:root,[data-theme="light"]{\n  --bg:#f7f6f2;--surface:#f9f8f5;--surface-2:#ffffff;--surface-off:#edeae5;--surface-dyn:#e6e4df;\n  --border:rgba(0,0,0,0.09);--divider:#dcd9d5;\n  --text:#28251d;--muted:#7a7974;--faint:#bab9b4;\n  --primary:#01696f;--primary-h:#0c4e54;--primary-hl:#d4e8e7;\n  --ce:#437a22;--ce-hl:rgba(67,122,34,0.13);\n  --pe:#a12c7b;--pe-hl:rgba(161,44,123,0.12);\n  --warn:#da7101;\n  --r-sm:0.375rem;--r-md:0.5rem;--r-lg:0.75rem;--r-xl:1rem;--r-full:9999px;\n  --sh-sm:0 1px 3px rgba(0,0,0,0.06);--sh-md:0 4px 14px rgba(0,0,0,0.09);\n  --tr:180ms cubic-bezier(0.16,1,0.3,1);\n  --font:\'Satoshi\',\'Inter\',sans-serif;\n  --xs:clamp(0.72rem,0.68rem + 0.2vw,0.8rem);\n  --sm:clamp(0.8rem,0.75rem + 0.25vw,0.875rem);\n  --base:clamp(0.9rem,0.85rem + 0.25vw,1rem);\n  --lg:clamp(1.1rem,1rem + 0.5vw,1.35rem);\n}\n[data-theme="dark"]{\n  --bg:#111110;--surface:#1a1918;--surface-2:#222120;--surface-off:#252422;--surface-dyn:#2e2c2a;\n  --border:rgba(255,255,255,0.07);--divider:#2a2927;\n  --text:#cccac7;--muted:#737270;--faint:#474543;\n  --primary:#4f98a3;--primary-h:#227f8b;--primary-hl:rgba(79,152,163,0.15);\n  --ce:#6daa45;--ce-hl:rgba(109,170,69,0.13);\n  --pe:#d163a7;--pe-hl:rgba(209,99,167,0.12);\n  --warn:#fdab43;\n  --sh-sm:0 1px 4px rgba(0,0,0,0.35);--sh-md:0 4px 18px rgba(0,0,0,0.45);\n}\n*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}\nhtml{-webkit-font-smoothing:antialiased;scroll-behavior:smooth}\nbody{font-family:var(--font);font-size:var(--sm);color:var(--text);background:var(--bg);min-height:100dvh;line-height:1.5}\nbutton{cursor:pointer;background:none;border:none;font:inherit;color:inherit;transition:all var(--tr)}\ntable{border-collapse:collapse;width:100%}\n\n/* ── App shell ── */\n.app{display:flex;flex-direction:column;height:100dvh;overflow:hidden}\n\n/* ── Header ── */\n.hdr{\n  background:var(--surface);border-bottom:1px solid var(--divider);\n  padding:0 1.25rem;height:52px;display:flex;align-items:center;gap:0.875rem;\n  flex-shrink:0;box-shadow:var(--sh-sm);z-index:50;\n}\n.logo{display:flex;align-items:center;gap:0.45rem;font-weight:700;font-size:var(--base);white-space:nowrap}\n.logo-icon{color:var(--primary)}\n.chip{padding:0.18rem 0.55rem;border-radius:var(--r-full);font-size:var(--xs);font-weight:600;white-space:nowrap}\n.chip-sym{background:var(--primary-hl);color:var(--primary);border:1px solid var(--primary)}\n.chip-exp{background:var(--surface-off);color:var(--muted);border:1px solid var(--border)}\n.hdr-right{margin-left:auto;display:flex;align-items:center;gap:0.875rem}\n.spot-block{text-align:right}\n.spot-val{font-size:var(--lg);font-weight:700;font-variant-numeric:tabular-nums;line-height:1.1}\n.spot-val.up{color:var(--ce)} .spot-val.dn{color:var(--pe)}\n.spot-sub{font-size:var(--xs);color:var(--muted)}\n.spot-chg{font-size:var(--xs);font-weight:600;padding:0.18rem 0.45rem;border-radius:var(--r-sm)}\n.spot-chg.up{background:var(--ce-hl);color:var(--ce)} .spot-chg.dn{background:var(--pe-hl);color:var(--pe)}\n.icon-btn{padding:0.4rem;border-radius:var(--r-md);color:var(--muted);display:flex;align-items:center;justify-content:center}\n.icon-btn:hover{background:var(--surface-off);color:var(--text)}\n\n/* ── Controls bar ── */\n.ctrl-bar{\n  background:var(--surface);border-bottom:1px solid var(--divider);\n  padding:0 1.25rem;height:46px;display:flex;align-items:center;gap:1rem;flex-shrink:0;\n}\n.day-tabs{display:flex;gap:0.2rem;background:var(--surface-off);padding:0.18rem;border-radius:var(--r-md)}\n.day-tab{padding:0.25rem 0.75rem;border-radius:calc(var(--r-md) - 0.1rem);font-size:var(--xs);font-weight:600;color:var(--muted);transition:all var(--tr)}\n.day-tab.active{background:var(--surface-2);color:var(--text);box-shadow:var(--sh-sm)}\n.day-tab:hover:not(.active){color:var(--text)}\n.slider-row{display:flex;align-items:center;gap:0.6rem;flex:1;min-width:180px}\n.slider-row label{font-size:var(--xs);color:var(--muted);white-space:nowrap}\n#timeSlider{flex:1;accent-color:var(--primary);height:3px}\n.t-disp{font-size:var(--xs);font-weight:700;font-variant-numeric:tabular-nums;min-width:44px}\n.play-btn{display:flex;align-items:center;gap:0.35rem;padding:0.28rem 0.75rem;\n  border-radius:var(--r-md);background:var(--primary);color:#fff;font-size:var(--xs);font-weight:700;white-space:nowrap}\n.play-btn:hover{background:var(--primary-h)}\n.ctrl-meta{font-size:var(--xs);color:var(--faint);white-space:nowrap}\n\n/* ── Main grid ── */\n.main{display:grid;grid-template-columns:1fr 360px;flex:1;overflow:hidden;min-height:0}\n@media(max-width:960px){.main{grid-template-columns:1fr}}\n\n/* ── Chain section ── */\n.chain-wrap{display:flex;flex-direction:column;overflow:hidden;border-right:1px solid var(--divider)}\n.sec-title{\n  font-size:var(--xs);font-weight:700;text-transform:uppercase;letter-spacing:0.08em;color:var(--muted);\n  padding:0.5rem 1.25rem;background:var(--surface);border-bottom:1px solid var(--divider);\n  display:flex;align-items:center;gap:0.5rem;flex-shrink:0;\n}\n.legend{display:flex;gap:0.875rem;margin-left:auto}\n.leg-item{display:flex;align-items:center;gap:0.3rem;font-size:var(--xs);color:var(--muted)}\n.leg-dot{width:7px;height:7px;border-radius:50%;flex-shrink:0}\n.chain-scroll{overflow-y:auto;overflow-x:auto;flex:1}\n\n/* ── Chain table ── */\ntable.chain{font-size:var(--xs);table-layout:fixed;min-width:620px}\ntable.chain thead th{\n  position:sticky;top:0;z-index:10;\n  padding:0.4rem 0.6rem;background:var(--surface);border-bottom:2px solid var(--divider);\n  font-weight:700;text-transform:uppercase;letter-spacing:0.06em;color:var(--muted);font-size:0.66rem;\n  white-space:nowrap;\n}\nth.ce{text-align:right;color:var(--ce)} th.pe{text-align:left;color:var(--pe)} th.str{text-align:center}\ntable.chain tbody tr{border-bottom:1px solid var(--divider);transition:background var(--tr)}\ntable.chain tbody tr:hover td{background:var(--surface-dyn) !important}\ntable.chain tbody tr.atm td{background:var(--primary-hl) !important;font-weight:600}\ntd.ce-col{background:var(--ce-hl);text-align:right;padding:0.38rem 0.6rem;vertical-align:middle}\ntd.pe-col{background:var(--pe-hl);text-align:left;padding:0.38rem 0.6rem;vertical-align:middle}\ntd.str-col{text-align:center;padding:0.38rem 0.5rem;background:var(--surface-off);font-weight:700;font-size:var(--xs);white-space:nowrap}\n.atm-tag{font-size:0.58rem;font-weight:700;text-transform:uppercase;color:var(--primary);\n  background:rgba(79,152,163,0.2);border-radius:2px;padding:0 3px;margin-left:3px}\n.price-n{font-weight:600;font-variant-numeric:tabular-nums}\n.iv-n{font-size:0.65rem;color:var(--muted);font-variant-numeric:tabular-nums}\n.oi-cell{display:flex;flex-direction:column;gap:2px}\n.oi-n{font-variant-numeric:tabular-nums;font-size:0.68rem}\n.oi-bar-bg{height:5px;border-radius:2px;background:var(--surface-dyn);overflow:hidden;width:80px}\n.oi-bar-fill{height:100%;border-radius:2px;transition:width 0.2s}\n.ce-bar{background:var(--ce)} .pe-bar{background:var(--pe)}\n.ce-bar-bg{display:flex;justify-content:flex-end}\n.ce-bar-bg .oi-bar-bg{display:flex;justify-content:flex-end}\n\n/* ── Right panel ── */\n.right-panel{display:flex;flex-direction:column;overflow:hidden}\n.panel-scroll{overflow-y:auto;flex:1}\n\n/* Stats */\n.stats-row{display:grid;grid-template-columns:repeat(3,1fr);border-bottom:1px solid var(--divider)}\n.stat-cell{padding:0.6rem 0.875rem;border-right:1px solid var(--divider);text-align:center}\n.stat-cell:last-child{border-right:none}\n.stat-lbl{font-size:0.65rem;text-transform:uppercase;letter-spacing:0.07em;color:var(--muted);margin-bottom:0.2rem}\n.stat-v{font-size:var(--base);font-weight:700;font-variant-numeric:tabular-nums}\n.v-ce{color:var(--ce)} .v-pe{color:var(--pe)} .v-n{color:var(--text)}\n\n/* PCR */\n.pcr-box{padding:0.75rem 1.1rem;border-bottom:1px solid var(--divider)}\n.pcr-hdr{display:flex;justify-content:space-between;font-size:0.65rem;color:var(--muted);margin-bottom:0.4rem}\n.pcr-track{height:8px;border-radius:4px;background:var(--surface-off);overflow:hidden;display:flex}\n.pcr-ce{height:100%;background:var(--ce);transition:width var(--tr)}\n.pcr-pe{height:100%;background:var(--pe);transition:width var(--tr)}\n.pcr-val{font-size:var(--xs);font-weight:700;text-align:center;margin-top:0.35rem}\n\n/* Charts */\n.chart-box{padding:0.875rem 1.1rem;border-bottom:1px solid var(--divider)}\n.chart-hdr{display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem}\n.chart-title{font-size:0.68rem;font-weight:700;text-transform:uppercase;letter-spacing:0.07em;color:var(--muted)}\n.chart-sub{font-size:0.62rem;color:var(--faint);font-weight:400}\ncanvas{max-height:145px !important}\n\n/* Integration box */\n.int-box{padding:0.875rem 1.1rem;background:var(--surface-off);border-top:1px solid var(--divider)}\n.int-box p{font-size:var(--xs);color:var(--muted);line-height:1.65}\n.int-box strong{color:var(--primary)}\ncode{font-family:monospace;background:var(--surface);border:1px solid var(--border);\n  border-radius:var(--r-sm);padding:0.08rem 0.32rem;font-size:0.7rem}\n\n::-webkit-scrollbar{width:5px;height:5px}\n::-webkit-scrollbar-track{background:transparent}\n::-webkit-scrollbar-thumb{background:var(--divider);border-radius:3px}\n'
_VIEWER_BODY  = '<div class="app">\n\n<!-- ── Header ── -->\n<header class="hdr">\n  <div class="logo">\n    <svg class="logo-icon" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2">\n      <polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/><polyline points="16 7 22 7 22 13"/>\n    </svg>\n    Option Chain\n  </div>\n  <span class="chip chip-sym">ANGELONE</span>\n  <span class="chip chip-exp">Expiry: 28-Apr-2026</span>\n  <div class="hdr-right">\n    <div class="spot-block">\n      <div class="spot-val" id="spotVal">—</div>\n      <div class="spot-sub" id="lastTs">—</div>\n    </div>\n    <span class="spot-chg" id="spotChg">—</span>\n    <button class="icon-btn" data-theme-toggle title="Toggle theme">\n      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">\n        <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>\n      </svg>\n    </button>\n  </div>\n</header>\n\n<!-- ── Controls ── -->\n<div class="ctrl-bar">\n  <div class="day-tabs">\n    <button class="day-tab active" data-day="0">17 Apr 2026</button>\n    <button class="day-tab" data-day="1">22 Apr 2026</button>\n  </div>\n  <div class="slider-row">\n    <label>Time</label>\n    <input type="range" id="timeSlider" min="0" max="74" value="0" step="1">\n    <span class="t-disp" id="tDisp">09:15</span>\n  </div>\n  <button class="play-btn" id="playBtn">\n    <svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg>\n    Play\n  </button>\n  <span class="ctrl-meta">152 snapshots · 5-min · 2 days</span>\n</div>\n\n<!-- ── Main ── -->\n<div class="main">\n\n  <!-- Chain table -->\n  <div class="chain-wrap">\n    <div class="sec-title">\n      Option Chain\n      <div class="legend">\n        <span class="leg-item"><span class="leg-dot" style="background:var(--ce)"></span>CE (Call)</span>\n        <span class="leg-item"><span class="leg-dot" style="background:var(--pe)"></span>PE (Put)</span>\n        <span class="leg-item"><span class="leg-dot" style="background:var(--primary)"></span>ATM</span>\n      </div>\n    </div>\n    <div class="chain-scroll">\n      <table class="chain">\n        <thead>\n          <tr>\n            <th class="ce" style="width:20%">OI</th>\n            <th class="ce" style="width:9%">IV</th>\n            <th class="ce" style="width:11%">Price</th>\n            <th class="str" style="width:14%">Strike</th>\n            <th class="pe" style="width:11%">Price</th>\n            <th class="pe" style="width:9%">IV</th>\n            <th class="pe" style="width:20%">OI</th>\n          </tr>\n        </thead>\n        <tbody id="chainBody"></tbody>\n      </table>\n    </div>\n  </div>\n\n  <!-- Right panel -->\n  <div class="right-panel">\n    <div class="panel-scroll">\n\n      <!-- Stats -->\n      <div class="stats-row">\n        <div class="stat-cell">\n          <div class="stat-lbl">CE OI</div>\n          <div class="stat-v v-ce" id="sCeOi">—</div>\n        </div>\n        <div class="stat-cell">\n          <div class="stat-lbl">PCR</div>\n          <div class="stat-v v-n" id="sPcr">—</div>\n        </div>\n        <div class="stat-cell">\n          <div class="stat-lbl">PE OI</div>\n          <div class="stat-v v-pe" id="sPeOi">—</div>\n        </div>\n      </div>\n\n      <!-- PCR bar -->\n      <div class="pcr-box">\n        <div class="pcr-hdr"><span style="color:var(--ce)">CE OI</span><span>Put-Call Ratio</span><span style="color:var(--pe)">PE OI</span></div>\n        <div class="pcr-track">\n          <div class="pcr-ce" id="pcrCe" style="width:50%"></div>\n          <div class="pcr-pe" id="pcrPe" style="width:50%"></div>\n        </div>\n        <div class="pcr-val" id="pcrVal">PCR: —</div>\n      </div>\n\n      <!-- Price chart -->\n      <div class="chart-box">\n        <div class="chart-hdr">\n          <span class="chart-title">Spot Price</span>\n          <span class="chart-sub">ANGELONE underlying</span>\n        </div>\n        <canvas id="priceChart"></canvas>\n      </div>\n\n      <!-- OI chart -->\n      <div class="chart-box">\n        <div class="chart-hdr">\n          <span class="chart-title">OI by Strike</span>\n          <span class="chart-sub">ATM ± 3 strikes</span>\n        </div>\n        <canvas id="oiChart"></canvas>\n      </div>\n\n      <!-- IV smile -->\n      <div class="chart-box">\n        <div class="chart-hdr">\n          <span class="chart-title">IV Smile</span>\n          <span class="chart-sub">Implied Volatility curve</span>\n        </div>\n        <canvas id="ivChart"></canvas>\n      </div>\n\n      <!-- Integration note -->\n      <div class="int-box">\n        <p><strong>🔌 Flask Integration:</strong> Replace <code>SAMPLE_DATA</code> with\n        <code>fetch(\'/api/snapshots?symbol=ANGELONE&amp;date=...\')</code> from your\n        <code>app.py</code>. Your scheduler writes to SQLite → Flask serves JSON →\n        this UI reads it live. No breaking changes to existing logic.</p>\n      </div>\n\n    </div>\n  </div>\n</div>\n</div>'
_VIEWER_LOGIC = '\n\nlet daySnaps = [[], []], curDay = 0, curIdx = 0, playing = false, timer = null;\nlet pChart, oChart, iChart;\n\nfunction init() {\n  daySnaps[0] = SAMPLE_DATA.filter(s => s.timestamp.startsWith("17-Apr-2026"));\n  daySnaps[1] = SAMPLE_DATA.filter(s => s.timestamp.startsWith("22-Apr-2026"));\n  initCharts();\n  setDay(0);\n}\n\n/* ── Formatters ── */\nconst fN = n => n >= 1e5 ? (n/1e5).toFixed(1)+"L" : n >= 1e3 ? (n/1e3).toFixed(1)+"K" : String(n);\nconst fP = n => n == null ? "—" : n.toFixed(2);\nconst fI = n => n == null ? "—" : n.toFixed(1)+"%";\n\n/* ── Render header ── */\nfunction renderHeader(snap) {\n  const spot = snap.underlyingValue;\n  const first = daySnaps[curDay][0].underlyingValue;\n  const chg = spot - first, pct = (chg/first*100).toFixed(2), up = chg >= 0;\n  document.getElementById("spotVal").textContent = "₹" + spot.toFixed(2);\n  document.getElementById("spotVal").className = "spot-val " + (up ? "up" : "dn");\n  document.getElementById("lastTs").textContent = snap.timestamp;\n  const chgEl = document.getElementById("spotChg");\n  chgEl.textContent = (up?"+":"") + chg.toFixed(2) + " (" + (up?"+":"") + pct + "%)";\n  chgEl.className = "spot-chg " + (up ? "up" : "dn");\n}\n\n/* ── Render chain table ── */\nfunction renderChain(snap) {\n  const spot = snap.underlyingValue;\n  const atm = snap.data.reduce((b, r) => Math.abs(r.strikePrice - spot) < Math.abs(b - spot) ? r.strikePrice : b, snap.data[0].strikePrice);\n  const maxOI = Math.max(...snap.data.flatMap(r => [r.CE?.openInterest||0, r.PE?.openInterest||0]), 1);\n\n  document.getElementById("chainBody").innerHTML = snap.data.map(row => {\n    const K = row.strikePrice, ce = row.CE, pe = row.PE;\n    const isAtm = K === atm;\n    const cePct = Math.round((ce?.openInterest||0) / maxOI * 100);\n    const pePct = Math.round((pe?.openInterest||0) / maxOI * 100);\n    const itmCe = K < spot, itmPe = K > spot;\n    return `<tr class="${isAtm ? \'atm\' : \'\'}">\n      <td class="ce-col">\n        <div class="oi-cell" style="align-items:flex-end">\n          <span class="oi-n">${fN(ce?.openInterest||0)}</span>\n          <div class="oi-bar-bg" style="display:flex;justify-content:flex-end">\n            <div class="oi-bar-fill ce-bar" style="width:${cePct}%"></div>\n          </div>\n        </div>\n      </td>\n      <td class="ce-col"><span class="iv-n">${fI(ce?.impliedVolatility)}</span></td>\n      <td class="ce-col"><span class="price-n" style="color:${itmCe?\'var(--ce)\':\'inherit\'}">${fP(ce?.lastPrice)}</span></td>\n      <td class="str-col">${K}${isAtm ? \'<span class="atm-tag">ATM</span>\' : \'\'}</td>\n      <td class="pe-col"><span class="price-n" style="color:${itmPe?\'var(--pe)\':\'inherit\'}">${fP(pe?.lastPrice)}</span></td>\n      <td class="pe-col"><span class="iv-n">${fI(pe?.impliedVolatility)}</span></td>\n      <td class="pe-col">\n        <div class="oi-cell">\n          <span class="oi-n">${fN(pe?.openInterest||0)}</span>\n          <div class="oi-bar-bg">\n            <div class="oi-bar-fill pe-bar" style="width:${pePct}%"></div>\n          </div>\n        </div>\n      </td>\n    </tr>`;\n  }).join("");\n\n  const atmRow = document.querySelector("tr.atm");\n  if (atmRow) atmRow.scrollIntoView({block:"nearest",behavior:"smooth"});\n}\n\n/* ── Render stats ── */\nfunction renderStats(snap) {\n  const ceOI = snap.data.reduce((s,r) => s+(r.CE?.openInterest||0), 0);\n  const peOI = snap.data.reduce((s,r) => s+(r.PE?.openInterest||0), 0);\n  const pcr = ceOI > 0 ? (peOI/ceOI).toFixed(2) : "—";\n  document.getElementById("sCeOi").textContent = fN(ceOI);\n  document.getElementById("sPeOi").textContent = fN(peOI);\n  document.getElementById("sPcr").textContent = pcr;\n  document.getElementById("pcrVal").textContent = "PCR: " + pcr;\n  const tot = ceOI + peOI || 1;\n  document.getElementById("pcrCe").style.width = (ceOI/tot*100)+"%";\n  document.getElementById("pcrPe").style.width = (peOI/tot*100)+"%";\n}\n\n/* ── Charts ── */\nfunction gridColor() { return getComputedStyle(document.documentElement).getPropertyValue(\'--divider\').trim(); }\nfunction tickColor() { return getComputedStyle(document.documentElement).getPropertyValue(\'--muted\').trim(); }\n\nfunction initCharts() {\n  const baseOpts = (yFmt, xFmt) => ({\n    responsive: true, maintainAspectRatio: true,\n    plugins: { legend: { display: false } },\n    scales: {\n      x: { ticks: { maxTicksLimit: 6, color: tickColor(), font:{size:9}, callback: xFmt||undefined }, grid: { color: gridColor() } },\n      y: { ticks: { color: tickColor(), font:{size:9}, callback: yFmt||undefined }, grid: { color: gridColor() } }\n    }\n  });\n\n  pChart = new Chart(document.getElementById("priceChart").getContext("2d"), {\n    type: "line",\n    data: { labels:[], datasets:[\n      { data:[], borderColor:"var(--primary)", backgroundColor:"transparent", borderWidth:1.5, pointRadius:0, tension:0.3 },\n      { data:[], borderColor:"var(--warn)", backgroundColor:"transparent", borderWidth:0, pointRadius:5, pointBackgroundColor:"var(--warn)", showLine:false }\n    ]},\n    options: { ...baseOpts(v => "₹"+v.toFixed(0)), plugins:{ legend:{display:false},\n      tooltip:{ callbacks:{ label: c => "₹"+Number(c.raw).toFixed(2) } } } }\n  });\n\n  oChart = new Chart(document.getElementById("oiChart").getContext("2d"), {\n    type: "bar",\n    data: { labels:[], datasets:[\n      { label:"CE OI", data:[], backgroundColor:"rgba(67,122,34,0.72)", borderRadius:3, borderSkipped:false },\n      { label:"PE OI", data:[], backgroundColor:"rgba(161,44,123,0.72)", borderRadius:3, borderSkipped:false }\n    ]},\n    options: { ...baseOpts(v => fN(v)), plugins:{ legend:{ display:true, position:"bottom",\n      labels:{ color:tickColor(), font:{size:9}, boxWidth:9, padding:6 } } } }\n  });\n\n  iChart = new Chart(document.getElementById("ivChart").getContext("2d"), {\n    type: "line",\n    data: { labels:[], datasets:[\n      { label:"CE IV", data:[], borderColor:"rgba(67,122,34,0.85)", backgroundColor:"rgba(67,122,34,0.08)", borderWidth:1.5, pointRadius:2, tension:0.4, fill:true },\n      { label:"PE IV", data:[], borderColor:"rgba(161,44,123,0.85)", backgroundColor:"rgba(161,44,123,0.07)", borderWidth:1.5, pointRadius:2, tension:0.4, fill:true }\n    ]},\n    options: { ...baseOpts(v => v+"%"), plugins:{ legend:{ display:true, position:"bottom",\n      labels:{ color:tickColor(), font:{size:9}, boxWidth:9, padding:6 } } } }\n  });\n}\n\nfunction updatePriceChart(snaps, i) {\n  pChart.data.labels = snaps.map(s => s.timestamp.slice(12,17));\n  pChart.data.datasets[0].data = snaps.map(s => s.underlyingValue);\n  pChart.data.datasets[1].data = snaps.map((s,j) => j===i ? s.underlyingValue : null);\n  pChart.update("none");\n}\n\nfunction updateOIChart(snap) {\n  const spot = snap.underlyingValue;\n  let ai = snap.data.reduce((bi,r,i) => Math.abs(r.strikePrice-spot) < Math.abs(snap.data[bi].strikePrice-spot) ? i : bi, 0);\n  const lo = Math.max(0,ai-3), hi = Math.min(snap.data.length-1,ai+3);\n  const rows = snap.data.slice(lo, hi+1);\n  oChart.data.labels = rows.map(r => r.strikePrice);\n  oChart.data.datasets[0].data = rows.map(r => r.CE?.openInterest||0);\n  oChart.data.datasets[1].data = rows.map(r => r.PE?.openInterest||0);\n  oChart.update("none");\n}\n\nfunction updateIVChart(snap) {\n  iChart.data.labels = snap.data.map(r => r.strikePrice);\n  iChart.data.datasets[0].data = snap.data.map(r => r.CE?.impliedVolatility||0);\n  iChart.data.datasets[1].data = snap.data.map(r => r.PE?.impliedVolatility||0);\n  iChart.update("none");\n}\n\n/* ── Render all ── */\nfunction renderAll() {\n  const snaps = daySnaps[curDay];\n  const snap = snaps[curIdx];\n  if (!snap) return;\n  renderHeader(snap);\n  renderChain(snap);\n  renderStats(snap);\n  updatePriceChart(snaps, curIdx);\n  updateOIChart(snap);\n  updateIVChart(snap);\n  document.getElementById("tDisp").textContent = snap.timestamp.slice(12,17);\n}\n\n/* ── Controls ── */\nfunction setDay(d) {\n  curDay = d; curIdx = 0;\n  const slider = document.getElementById("timeSlider");\n  slider.max = daySnaps[d].length - 1;\n  slider.value = 0;\n  document.querySelectorAll(".day-tab").forEach((b,i) => b.classList.toggle("active", i===d));\n  renderAll();\n}\n\ndocument.querySelectorAll(".day-tab").forEach(b => b.addEventListener("click", () => setDay(+b.dataset.day)));\n\ndocument.getElementById("timeSlider").addEventListener("input", function() {\n  curIdx = +this.value; renderAll();\n});\n\ndocument.getElementById("playBtn").addEventListener("click", function() {\n  if (playing) {\n    clearInterval(timer); playing = false;\n    this.innerHTML = \'<svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg> Play\';\n  } else {\n    playing = true;\n    this.innerHTML = \'<svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/></svg> Pause\';\n    const slider = document.getElementById("timeSlider");\n    timer = setInterval(() => {\n      if (curIdx >= daySnaps[curDay].length - 1) {\n        clearInterval(timer); playing = false;\n        document.getElementById("playBtn").innerHTML = \'<svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg> Play\';\n        return;\n      }\n      curIdx++; slider.value = curIdx; renderAll();\n    }, 180);\n  }\n});\n\n/* ── Theme toggle ── */\n(function(){\n  const btn = document.querySelector("[data-theme-toggle]");\n  const html = document.documentElement;\n  btn.addEventListener("click", () => {\n    const dark = html.getAttribute("data-theme") === "dark";\n    html.setAttribute("data-theme", dark ? "light" : "dark");\n    btn.innerHTML = dark\n      ? \'<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>\'\n      : \'<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="5"/><path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/></svg>\';\n    setTimeout(() => { pChart.update(); oChart.update(); iChart.update(); }, 60);\n  });\n})();\n\ninit();\n'


def load_viewer_snapshots(symbol: str, trade_date: date) -> list:
    """Load all DB snapshots for a day and convert to viewer-ready list."""
    db = get_db_path(symbol, trade_date)
    if not db.exists():
        return []
    conn = sqlite3.connect(db)
    df = pd.read_sql(
        "SELECT * FROM option_chain_snapshots ORDER BY snapshot_ts, strike_price", conn
    )
    conn.close()
    if df.empty:
        return []
    snapshots = []
    for ts, grp in df.groupby("snapshot_ts", sort=True):
        spot = float(grp["underlying_value"].iloc[0])
        rows = []
        for _, r in grp.iterrows():
            rows.append({
                "strikePrice": float(r["strike_price"]),
                "CE": {
                    "openInterest":      int(r["ce_oi"] or 0),
                    "lastPrice":         float(r["ce_price"] or 0),
                    "impliedVolatility": float(r["ce_iv"] or 0),
                },
                "PE": {
                    "openInterest":      int(r["pe_oi"] or 0),
                    "lastPrice":         float(r["pe_price"] or 0),
                    "impliedVolatility": float(r["pe_iv"] or 0),
                },
            })
        rows.sort(key=lambda x: x["strikePrice"])
        snapshots.append({
            "timestamp":       str(ts),
            "underlyingValue": spot,
            "symbol":          str(grp["symbol"].iloc[0]),
            "expiry":          str(grp["expiry"].iloc[0]),
            "data":            rows,
        })
    return snapshots


def transform_api_for_viewer(api_response: dict, symbol: str, expiry: str) -> dict:
    """Convert raw option-chain-v3 API response to a single viewer-ready snapshot."""
    records          = api_response.get("records", {}) or {}
    timestamp        = (records.get("timestamp")
                        or api_response.get("timestamp")
                        or datetime.now().strftime("%d-%b-%Y %H:%M:%S"))
    underlying_value = (records.get("underlyingValue")
                        or api_response.get("underlyingValue") or 0)
    data_rows = api_response.get("data", [])
    rows = []
    for row in data_rows:
        ce = row.get("CE") or {}
        pe = row.get("PE") or {}
        rows.append({
            "strikePrice": row.get("strikePrice"),
            "CE": {
                "openInterest":      ce.get("openInterest", 0),
                "lastPrice":         ce.get("lastPrice", 0),
                "impliedVolatility": ce.get("impliedVolatility", 0),
            },
            "PE": {
                "openInterest":      pe.get("openInterest", 0),
                "lastPrice":         pe.get("lastPrice", 0),
                "impliedVolatility": pe.get("impliedVolatility", 0),
            },
        })
    rows.sort(key=lambda x: x["strikePrice"] if x["strikePrice"] is not None else 0)
    return {
        "timestamp":       timestamp,
        "underlyingValue": underlying_value,
        "symbol":          symbol,
        "expiry":          expiry,
        "data":            rows,
    }


def build_option_chain_viewer_html(snapshots: list, sym: str, expiry: str) -> str:
    """Build full interactive viewer HTML identical to option-chain-viewer.html.

    Accepts a LIST of snapshots (one per fetch interval).
    Supports: day-tabs, time-slider, Play/Pause, spot chart, OI chart, IV smile.
    """
    if not snapshots:
        return (
            "<html><body style=\"background:#111;color:#aaa;"
            "font-family:sans-serif;padding:2rem\">"
            "<p>No snapshot data available. Run a fetch first.</p>"
            "</body></html>"
        )

    # Group by date prefix for day-tabs
    from collections import OrderedDict
    day_map: dict = OrderedDict()
    for s in snapshots:
        ts = s["timestamp"]
        date_part = ts[:11].strip()
        if date_part not in day_map:
            day_map[date_part] = []
        day_map[date_part].append(s)

    sorted_days  = list(day_map.keys())
    data_json    = json.dumps(snapshots)
    snap_count   = len(snapshots)
    num_days     = len(sorted_days)

    # Build day-tab buttons HTML
    day_tabs_html = ""
    for idx, d in enumerate(sorted_days):
        active = "active" if idx == 0 else ""
        day_tabs_html += f'<button class="day-tab {active}" data-day="{idx}">{d}</button>\n      '

    # Patch body: replace day-tabs content
    import re as _re
    patched_body = _re.sub(
        r'<div class="day-tabs">[\s\S]*?</div>',
        f'<div class="day-tabs">\n      {day_tabs_html}</div>',
        _VIEWER_BODY,
        count=1,
    )
    # Patch snapshot count in ctrl-meta span
    patched_body = _re.sub(
        r'<span class="ctrl-meta">[^<]*</span>',
        f'<span class="ctrl-meta">{snap_count} snapshots · {num_days} day(s)</span>',
        patched_body,
    )
    # Patch symbol chip
    patched_body = patched_body.replace(
        '<span class="chip chip-sym">ANGELONE</span>',
        f'<span class="chip chip-sym">{sym}</span>'
    )
    # Patch expiry chip
    patched_body = patched_body.replace(
        '<span class="chip chip-exp">Expiry: 28-Apr-2026</span>',
        f'<span class="chip chip-exp">Expiry: {expiry}</span>'
    )

    # Build dynamic daySnaps init JS
    day_snaps_lines = []
    for idx, d in enumerate(sorted_days):
        day_snaps_lines.append(
            f'  daySnaps[{idx}] = _allData.filter(s => s.timestamp.startsWith("{d}"));' 
        )
    day_snaps_js = "\n".join(day_snaps_lines)

    # Patch logic: replace hardcoded SAMPLE_DATA init
    OLD_INIT = (
        'let daySnaps = [[], []], curDay = 0, curIdx = 0, playing = false, timer = null;\n'
        'let pChart, oChart, iChart;\n\n'
        'function init() {\n'
        '  daySnaps[0] = SAMPLE_DATA.filter(s => s.timestamp.startsWith("17-Apr-2026"));\n'
        '  daySnaps[1] = SAMPLE_DATA.filter(s => s.timestamp.startsWith("22-Apr-2026"));'
    )
    NEW_INIT = (
        f'let daySnaps = Array.from({{length: {num_days}}}, () => []);\n'
        'let curDay = 0, curIdx = 0, playing = false, timer = null;\n'
        'let pChart, oChart, iChart;\n\n'
        'function init() {\n'
        f'  const _allData = DATA;\n'
        f'{day_snaps_js}'
    )
    patched_logic = _VIEWER_LOGIC.replace(OLD_INIT, NEW_INIT)

    return f"""<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<link href="https://api.fontshare.com/v2/css?f[]=satoshi@400,500,700&display=swap" rel="stylesheet">
<style>{_VIEWER_STYLE}</style>
</head>
<body>
{patched_body}
<script>
const DATA = {data_json};
{patched_logic}
</script>
</body></html>"""



# ──────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Configuration")

    with st.expander("🧪 Mock Mode", expanded=True):
        mock_on = st.toggle("Enable Mock Mode", value=st.session_state["mock_mode"],
                            help="Use local OptionChainResponse.json instead of live NSE")
        st.session_state["mock_mode"] = mock_on
        if mock_on:
            if MOCK_FILE.exists():
                st.success(f"✅ `{MOCK_FILE.name}` found ({MOCK_FILE.stat().st_size//1024} KB)")
            else:
                st.error("❌ Place `OptionChainResponse.json` next to `app.py`")
        else:
            st.info("Live NSE mode")

    with st.expander("📋 Symbols", expanded=True):
        sel_syms   = st.multiselect("Select symbols", options=POPULAR_SYMBOLS,
                                    default=[c["symbol"] for c in st.session_state["symbols_config"]])
        custom_sym = st.text_input("Custom symbol", placeholder="e.g. BAJAJFINSV").strip().upper()
        expiry_in  = st.text_input("Expiry", value="28-Apr-2026")
        otype_in   = st.selectbox("Type", ["Equity", "Index"])
        all_syms   = list(dict.fromkeys(sel_syms + ([custom_sym] if custom_sym else [])))
        if all_syms:
            st.session_state["symbols_config"] = [
                {"symbol": s, "type": otype_in, "expiry": expiry_in} for s in all_syms
            ]

    with st.expander("⏱ Schedule", expanded=False):
        interval    = st.radio("Interval (min)", [3, 5], index=0, horizontal=True)
        skip_weekend = st.checkbox("Skip weekend check", value=False,
                                   help="Collect data even on Saturdays and Sundays")
        c1, c2      = st.columns(2)
        mkt_open_h  = c1.number_input("Open H",  value=9,  min_value=0, max_value=23)
        mkt_open_m  = c2.number_input("Open M",  value=15, min_value=0, max_value=59)
        mkt_close_h = c1.number_input("Close H", value=15, min_value=0, max_value=23)
        mkt_close_m = c2.number_input("Close M", value=30, min_value=0, max_value=59)

    # ── NEW: Live Chain Viewer config ─────────────────────────────────────────
    with st.expander("🔍 Live Chain Viewer", expanded=True):
        vc_sym    = st.selectbox("Viewer Symbol", options=POPULAR_SYMBOLS,
                                 index=POPULAR_SYMBOLS.index("ANGELONE"))
        vc_expiry = st.text_input("Viewer Expiry", value="28-Apr-2026",
                                  help="Format: DD-MMM-YYYY", key="vc_expiry")
        vc_type   = st.selectbox("Viewer Type", ["Equity", "Index"], key="vc_type")
        if st.button("📡 Fetch Viewer Snapshot", use_container_width=True):
            with st.spinner("Fetching option-chain-v3 ..."):
                raw = fetch_option_chain(vc_sym, vc_type, vc_expiry)
                if raw is None:
                    st.session_state["oc_error"] = st.session_state.get("last_error", "No data returned.")
                    st.session_state["oc_snap"]  = None
                else:
                    st.session_state["oc_snap"]  = transform_api_for_viewer(raw, vc_sym, vc_expiry)
                    st.session_state["oc_error"] = None
            st.rerun()

    st.markdown("---")
    st.markdown("**Manual Fetch**")
    if st.button("🔄 Fetch Once Now", use_container_width=True, type="primary"):
        if not st.session_state["symbols_config"]:
            st.error("No symbols configured.")
        else:
            results = []
            with st.spinner("Fetching …"):
                for cfg in st.session_state["symbols_config"]:
                    results.append(collect_snapshot_sync(cfg))
            for r in results:
                if r["success"]:
                    st.success(f"✅ {r['symbol']}: {r['rows']} strikes | Spot ₹{r['spot']:,.2f}")
                else:
                    st.error(f"❌ {r['symbol']}: {r['error']}")
            st.rerun()

    st.markdown("---")
    if not st.session_state["collector_running"]:
        if st.button("▶ Start Auto-Collector", use_container_width=True):
            st.session_state["collector_running"] = True
            threading.Thread(
                target=_bg_scheduler,
                args=(interval, (mkt_open_h, mkt_open_m), (mkt_close_h, mkt_close_m), skip_weekend),
                daemon=True,
            ).start()
            st.rerun()
    else:
        st.success("🟢 Collector is running")
        if st.button("⏹ Stop Collector", use_container_width=True):
            st.session_state["collector_running"] = False
            st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# MAIN AREA
# ──────────────────────────────────────────────────────────────────────────────
st.title("📈 NSE Option Chain Collector")

if st.session_state["mock_mode"]:
    st.info("🧪 **Mock Mode ON** — using `OptionChainResponse.json`")
else:
    st.info("🌐 **Live Mode** — fetching from NSE")

if st.session_state["last_error"]:
    st.error(f"⚠️ {st.session_state['last_error']}")
    if st.button("Dismiss"):
        st.session_state["last_error"] = None
        st.rerun()

sym_list = [c["symbol"] for c in st.session_state["symbols_config"]]
c1, c2, c3, c4 = st.columns(4)
c1.metric("Collector",         "🟢 Running" if st.session_state["collector_running"] else "🔴 Stopped")
c2.metric("Fetch Cycles",       st.session_state["fetch_count"])
c3.metric("Rows Stored Today",  f"{st.session_state['total_rows_stored']:,}")
c4.metric("Last Fetch",         st.session_state["last_fetch_ts"] or "—")
if sym_list:
    cols = st.columns(len(sym_list))
    for i, s in enumerate(sym_list):
        cols[i].metric(f"{s} snapshots today", count_snapshots_today(s))

st.markdown("---")

# ── Tabs — added "🔍 Chain Viewer" as the second tab ─────────────────────────
tab_live, tab_viewer, tab_today, tab_history, tab_logs = st.tabs([
    "📊 Live Snapshot",
    "🔍 Chain Viewer",
    "📉 Today's Analysis",
    "🗓 Historical Analysis",
    "🗒 Activity Log",
])

# ── helpers ───────────────────────────────────────────────────────────────────
def render_option_table(df_snap: pd.DataFrame):
    spot      = float(df_snap["underlying_value"].iloc[0])
    snap_time = str(df_snap["snapshot_ts"].iloc[0])[11:19]
    total_ce  = df_snap["ce_oi"].sum()
    total_pe  = df_snap["pe_oi"].sum()
    pcr       = round(total_pe / total_ce, 2) if total_ce else 0.0
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Spot Price",     f"₹{spot:,.2f}")
    m2.metric("Snapshot Time",  snap_time)
    m3.metric("PCR",            f"{pcr:.2f}")
    m4.metric("Strikes loaded", len(df_snap))
    disp = df_snap[["strike_price","ce_oi","ce_price","ce_iv","pe_oi","pe_price","pe_iv"]].copy()
    disp.insert(0, "Type", disp["strike_price"].apply(
        lambda x: "🟡 ATM" if abs(x-spot)/max(spot,1)<0.001
                  else ("🟢 ITM-CE" if x < spot else "🔵 OTM-CE")
    ))
    disp.columns = ["Type","Strike","CE OI","CE Price","CE IV","PE OI","PE Price","PE IV"]
    st.dataframe(disp, use_container_width=True, hide_index=True)

# ─── Tab 1: Live Snapshot ────────────────────────────────────────────────────
with tab_live:
    st.subheader("📊 Latest Stored Snapshot")
    if not sym_list:
        st.info("No symbols configured.")
    else:
        sel = st.selectbox("Symbol", sym_list, key="live_sym")
        df_snap = load_latest_snapshot(sel)
        if df_snap.empty:
            st.warning(f"No data stored yet for {sel}. Use 'Fetch Once Now' in the sidebar.")
        else:
            render_option_table(df_snap)

# ─── Tab 2: Chain Viewer ────────────────────────────────────────────────────
with tab_viewer:
    st.subheader("🔍 Option Chain Viewer")
    st.caption("Full interactive viewer — time-slider, ▶ Play/Pause, spot chart, OI chart, IV smile. Powered by stored DB snapshots.")

    if st.session_state["oc_error"]:
        st.error(st.session_state["oc_error"])

    if not sym_list:
        st.info("No symbols configured.")
    else:
        vc1, vc2 = st.columns([2, 1])
        viewer_sym   = vc1.selectbox("Symbol", sym_list, key="viewer_sym_sel")
        avail_vdates = get_available_dates(viewer_sym)

        if avail_vdates:
            viewer_date = vc2.selectbox(
                "Date", options=avail_vdates,
                index=len(avail_vdates) - 1,
                format_func=lambda d: d.strftime("%d-%b-%Y"),
                key="viewer_date_sel",
            )
        else:
            viewer_date = date.today()

        btn_col, meta_col = st.columns([1, 3])
        load_btn = btn_col.button("🔄 Load Viewer", use_container_width=True, type="primary")

        if load_btn or (
            st.session_state.get("viewer_last_sym") != viewer_sym
            or st.session_state.get("viewer_last_date") != str(viewer_date)
        ):
            with st.spinner("Loading snapshots from DB …"):
                snaps = load_viewer_snapshots(viewer_sym, viewer_date)
            st.session_state["viewer_snaps"]     = snaps
            st.session_state["viewer_last_sym"]  = viewer_sym
            st.session_state["viewer_last_date"] = str(viewer_date)

        snaps = st.session_state.get("viewer_snaps", [])

        # Fallback: sidebar fetch snap
        if not snaps and st.session_state["oc_snap"] is not None:
            snaps = [st.session_state["oc_snap"]]
            meta_col.info("Showing sidebar fetch snapshot (1 snapshot). Run Fetch Once Now to build history.")

        if not snaps:
            st.info(
                "No stored data found for this symbol/date.\n\n"
                "**To populate the viewer:**\n"
                "1. Configure symbol in sidebar\n"
                "2. Click **🔄 Fetch Once Now** (or start auto-collector)\n"
                "3. Return here and click **🔄 Load Viewer**"
            )
        else:
            sym_label    = snaps[0]["symbol"]
            expiry_label = snaps[0]["expiry"]
            n_snaps      = len(snaps)
            last_ts      = snaps[-1]["timestamp"]
            last_spot    = snaps[-1]["underlyingValue"]
            meta_col.caption(
                f"**{sym_label}** | {n_snaps} snapshots | "
                f"Last: {last_ts} | Spot: ₹{float(last_spot):,.2f}"
            )
            viewer_html = build_option_chain_viewer_html(snaps, sym_label, expiry_label)
            components.html(viewer_html, height=980, scrolling=False)


# ─── Tab 3: Today's Analysis ─────────────────────────────────────────────────
with tab_today:
    st.subheader("📉 Today's Intraday Analysis")
    if not sym_list:
        st.info("No symbols configured.")
    else:
        sel_t  = st.selectbox("Symbol", sym_list, key="today_sym")
        df_day = load_day(sel_t, date.today())
        if df_day.empty:
            st.warning(f"No data for {sel_t} today.")
        else:
            df_day["snapshot_ts"] = pd.to_datetime(df_day["snapshot_ts"])
            st.caption(f"{df_day['snapshot_ts'].nunique()} snapshots, {len(df_day):,} rows")

            st.markdown("#### PCR over time")
            pcr_df = load_pcr_series(sel_t)
            if not pcr_df.empty:
                st.line_chart(pcr_df.set_index("snapshot_ts")["PCR"], height=220)

            st.markdown("#### OI buildup for a Strike")
            strikes = sorted(df_day["strike_price"].unique())
            last_spot = float(df_day.sort_values("snapshot_ts")["underlying_value"].iloc[-1])
            atm_strike = min(strikes, key=lambda x: abs(x - last_spot))
            sel_st = st.select_slider("Strike", options=strikes, value=atm_strike)
            oi_df  = load_oi_series(sel_t, sel_st)
            if not oi_df.empty:
                st.line_chart(oi_df.set_index("snapshot_ts")[["ce_oi","pe_oi"]], height=220)

            st.markdown("#### Max Pain (latest snapshot)")
            latest_ts  = df_day["snapshot_ts"].max()
            df_latest  = df_day[df_day["snapshot_ts"] == latest_ts]
            strike_list = df_latest["strike_price"].unique()
            losses = {}
            for s in strike_list:
                ce_loss = ((s - df_latest[df_latest["strike_price"] < s]["strike_price"])
                           * df_latest[df_latest["strike_price"] < s]["ce_oi"].fillna(0)).sum()
                pe_loss = ((df_latest[df_latest["strike_price"] > s]["strike_price"] - s)
                           * df_latest[df_latest["strike_price"] > s]["pe_oi"].fillna(0)).sum()
                losses[s] = ce_loss + pe_loss
            if losses:
                mp = min(losses, key=losses.get)
                st.metric("Max Pain Strike", f"₹{mp:,.0f}")

            st.download_button(
                f"⬇ Download {sel_t} CSV (today)",
                data=df_day.to_csv(index=False).encode(),
                file_name=f"{sel_t}_{date.today().isoformat()}.csv",
                mime="text/csv",
                use_container_width=True,
            )

# ─── Tab 4: Historical Analysis ──────────────────────────────────────────────
with tab_history:
    st.subheader("🗓 Historical Analysis")
    if not sym_list:
        st.info("No symbols configured.")
    else:
        sel_h = st.selectbox("Symbol", sym_list, key="hist_sym")
        avail_dates = get_available_dates(sel_h)
        if not avail_dates:
            st.warning(f"No historical data found for {sel_h}.")
        else:
            st.caption(f"Data available: {avail_dates[0]} → {avail_dates[-1]}")
            hc1, hc2 = st.columns(2)
            start_date = hc1.date_input("From", value=avail_dates[0],  min_value=avail_dates[0],  max_value=avail_dates[-1])
            end_date   = hc2.date_input("To",   value=avail_dates[-1], min_value=avail_dates[0],  max_value=avail_dates[-1])

            if start_date > end_date:
                st.error("'From' date must be before or equal to 'To' date.")
            else:
                df_range = load_date_range(sel_h, start_date, end_date)
                if df_range.empty:
                    st.warning("No data in this range.")
                else:
                    st.caption(f"Loaded {len(df_range):,} rows across {df_range['trade_date'].nunique()} trading day(s)")

                    st.markdown("#### PCR Trend — closing snapshot per day")
                    last_snaps = (df_range.sort_values("snapshot_ts")
                                  .groupby("trade_date").last().reset_index())
                    daily = last_snaps.groupby("trade_date").apply(
                        lambda g: pd.Series({
                            "PCR":  g["pe_oi"].sum() / max(g["ce_oi"].sum(), 1),
                            "spot": g["underlying_value"].iloc[-1],
                        })
                    ).reset_index()
                    daily["trade_date"] = pd.to_datetime(daily["trade_date"])
                    st.line_chart(daily.set_index("trade_date")[["PCR","spot"]], height=250)

                    st.markdown("#### OI per day for a Strike (closing snapshot)")
                    strikes_r  = sorted(df_range["strike_price"].unique())
                    last_spot  = float(df_range.sort_values("snapshot_ts")["underlying_value"].iloc[-1])
                    atm_r      = min(strikes_r, key=lambda x: abs(x - last_spot))
                    sel_st_r   = st.select_slider("Strike", options=strikes_r, value=atm_r, key="range_st")
                    oi_range   = (df_range[df_range["strike_price"] == sel_st_r]
                                  .sort_values("snapshot_ts")
                                  .groupby("trade_date").last()
                                  .reset_index()[["trade_date","ce_oi","pe_oi"]])
                    oi_range["trade_date"] = pd.to_datetime(oi_range["trade_date"])
                    st.bar_chart(oi_range.set_index("trade_date")[["ce_oi","pe_oi"]], height=250)

                    st.markdown("#### Max Pain per day")
                    def max_pain_for_group(g):
                        strks  = g["strike_price"].unique()
                        losses = {}
                        for s in strks:
                            ce_loss = ((s - g[g["strike_price"] < s]["strike_price"])
                                       * g[g["strike_price"] < s]["ce_oi"].fillna(0)).sum()
                            pe_loss = ((g[g["strike_price"] > s]["strike_price"] - s)
                                       * g[g["strike_price"] > s]["pe_oi"].fillna(0)).sum()
                            losses[s] = ce_loss + pe_loss
                        return min(losses, key=losses.get) if losses else None

                    closing  = (df_range.sort_values("snapshot_ts")
                                .groupby(["trade_date","strike_price"]).last().reset_index())
                    mp_daily = closing.groupby("trade_date").apply(max_pain_for_group).reset_index()
                    mp_daily.columns = ["trade_date","max_pain"]
                    mp_daily["trade_date"] = pd.to_datetime(mp_daily["trade_date"])
                    st.line_chart(mp_daily.set_index("trade_date")["max_pain"], height=220)

                    st.download_button(
                        f"⬇ Download {sel_h} CSV ({start_date} to {end_date})",
                        data=df_range.to_csv(index=False).encode(),
                        file_name=f"{sel_h}_{start_date}_{end_date}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )

# ─── Tab 5: Logs ─────────────────────────────────────────────────────────────
with tab_logs:
    st.subheader("Activity Log")
    lines = st.session_state["log_lines"]
    if lines:
        st.code("\n".join(reversed(lines[-150:])), language="text")
    else:
        st.info("No activity yet.")
    if st.button("🗑 Clear log"):
        st.session_state["log_lines"] = []
        st.rerun()

if st.session_state["collector_running"]:
    time.sleep(3)
    st.rerun()
