#!/usr/bin/env python3
"""
Option Chain Query Helper
─────────────────────────
Ready-to-use functions for loading and analysing the collected data.

Usage examples at the bottom of this file (run as a script or import in notebooks).
"""

import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd

DATA_DIR = Path("./option_chain_data")


# ── Loaders ──────────────────────────────────────────────────────────────────

def load_day(symbol: str, trade_date: date) -> pd.DataFrame:
    """Load all snapshots for a symbol on a given day from SQLite."""
    db_path = DATA_DIR / symbol / trade_date.isoformat() / f"{symbol}_{trade_date.isoformat()}.db"
    if not db_path.exists():
        raise FileNotFoundError(f"No DB found: {db_path}")
    conn = sqlite3.connect(db_path)
    df   = pd.read_sql(
        "SELECT * FROM option_chain_snapshots ORDER BY snapshot_ts, strike_price",
        conn, parse_dates=["snapshot_ts"],
    )
    conn.close()
    return df


def load_day_parquet(symbol: str, trade_date: date) -> pd.DataFrame:
    """Faster load via Parquet (preferred for large datasets)."""
    pq_path = DATA_DIR / symbol / trade_date.isoformat() / f"{symbol}_{trade_date.isoformat()}.parquet"
    if not pq_path.exists():
        raise FileNotFoundError(f"No Parquet file: {pq_path}")
    return pd.read_parquet(pq_path)


def load_date_range(symbol: str, start: date, end: date) -> pd.DataFrame:
    """Load and concatenate data across multiple days (uses Parquet when available)."""
    frames = []
    current = start
    while current <= end:
        pq = DATA_DIR / symbol / current.isoformat() / f"{symbol}_{current.isoformat()}.parquet"
        db = DATA_DIR / symbol / current.isoformat() / f"{symbol}_{current.isoformat()}.db"
        try:
            if pq.exists():
                frames.append(pd.read_parquet(pq))
            elif db.exists():
                frames.append(load_day(symbol, current))
        except Exception:
            pass
        current += __import__("datetime").timedelta(days=1)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ── Analysis helpers ──────────────────────────────────────────────────────────

def oi_buildup(df: pd.DataFrame, strike: float) -> pd.DataFrame:
    """
    OI build-up over time for a specific strike.
    Returns a time-indexed DataFrame with CE/PE OI columns.
    """
    sub = df[df["strike_price"] == strike].copy()
    sub = sub.set_index("snapshot_ts")[["ce_oi", "pe_oi", "underlying_value"]]
    return sub


def pcr_by_snapshot(df: pd.DataFrame) -> pd.Series:
    """
    Put-Call Ratio (by OI) for every snapshot timestamp.
    PCR = total PE OI / total CE OI
    """
    grp = df.groupby("snapshot_ts")
    return (grp["pe_oi"].sum() / grp["ce_oi"].sum()).rename("PCR")


def max_pain(df: pd.DataFrame, snapshot_ts: str) -> float:
    """
    Max pain strike for a given snapshot.
    The strike where total option writers' loss (CE + PE) is minimised.
    """
    snap = df[df["snapshot_ts"] == snapshot_ts].copy()
    strikes = sorted(snap["strike_price"].unique())

    losses = {}
    for s in strikes:
        ce_loss = snap[snap["strike_price"] < s]["ce_oi"].sum() * (
            s - snap[snap["strike_price"] < s]["strike_price"]
        ).sum()
        pe_loss = snap[snap["strike_price"] > s]["pe_oi"].sum() * (
            snap[snap["strike_price"] > s]["strike_price"] - s
        ).sum()
        losses[s] = ce_loss + pe_loss

    return min(losses, key=losses.get)


def iv_surface(df: pd.DataFrame, snapshot_ts: str) -> pd.DataFrame:
    """
    IV surface for a snapshot: index=strike, columns=[ce_iv, pe_iv].
    """
    snap = df[df["snapshot_ts"] == snapshot_ts]
    return snap.set_index("strike_price")[["ce_iv", "pe_iv"]].sort_index()


# ── Example usage (run this file directly to test) ────────────────────────────

if __name__ == "__main__":
    import sys

    symbol     = "ANGELONE"
    today      = date.today()

    print(f"\nLoading data for {symbol} on {today} ...")
    try:
        df = load_day_parquet(symbol, today)
    except FileNotFoundError:
        df = load_day(symbol, today)

    if df.empty:
        print("No data collected yet. Run collector.py first.")
        sys.exit(0)

    print(f"\nTotal rows: {len(df):,}")
    print(f"Snapshots : {df['snapshot_ts'].nunique()}")
    print(f"Strikes   : {df['strike_price'].nunique()}")
    print(f"\nFirst snapshot underlying: {df.iloc[0]['underlying_value']}")
    print(f"\nPCR by snapshot:\n{pcr_by_snapshot(df).to_string()}")

    latest_ts = df["snapshot_ts"].max()
    mp        = max_pain(df, latest_ts)
    print(f"\nMax Pain (latest snapshot): {mp}")
    print(f"\nIV Surface (top 5 strikes):\n{iv_surface(df, latest_ts).head()}")
