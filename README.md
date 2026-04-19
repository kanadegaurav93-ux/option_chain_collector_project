# NSE Option Chain Collector

Collects NSE option chain snapshots every 3–5 minutes on working days and stores them in SQLite + Parquet.

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the collector (leave terminal open during market hours)
python collector.py
python collector.py --skip-weekend-check --interval 1
```

## Configuration (`collector.py` → `CONFIG` dict)

| Key | Default | Description |
|---|---|---|
| `symbols` | `[ANGELONE 28-Apr-2026]` | List of symbols to track. Add more dicts to scale. |
| `interval_minutes` | `3` | Fetch frequency (3 or 5 minutes recommended) |
| `market_open` | `09:15` | Collection start time (IST) |
| `market_close` | `15:30` | Collection end time (IST) |
| `data_dir` | `./option_chain_data` | Root folder for all stored data |
| `export_parquet` | `True` | Also write a Parquet file after each insert |

### Adding more symbols

```python
"symbols": [
    {"symbol": "ANGELONE", "type": "Equity", "expiry": "28-Apr-2026"},
    {"symbol": "CDSL",     "type": "Equity", "expiry": "28-Apr-2026"},
    {"symbol": "NIFTY",    "type": "Index",  "expiry": "24-Apr-2026"},
],
```

## Storage Layout

```
option_chain_data/
├── logs/
│   └── collector_2026-04-18.log
└── ANGELONE/
    └── 2026-04-18/
        ├── ANGELONE_2026-04-18.db        ← SQLite (primary store)
        └── ANGELONE_2026-04-18.parquet   ← Parquet (analysis-ready)
```

## Data Schema

| Column | Type | Description |
|---|---|---|
| `snapshot_ts` | TEXT | ISO-8601 timestamp of fetch |
| `trade_date` | TEXT | YYYY-MM-DD |
| `symbol` | TEXT | Underlying symbol |
| `expiry` | TEXT | Option expiry |
| `underlying_value` | REAL | Spot price at snapshot time |
| `strike_price` | REAL | Option strike |
| `ce_oi` | INT | Call Open Interest |
| `ce_price` | REAL | Call LTP |
| `ce_iv` | REAL | Call Implied Volatility (%) |
| `pe_oi` | INT | Put Open Interest |
| `pe_price` | REAL | Put LTP |
| `pe_iv` | REAL | Put Implied Volatility (%) |

## Analysis

```python
from query_helper import load_day_parquet, pcr_by_snapshot, max_pain, iv_surface
from datetime import date

df = load_day_parquet("ANGELONE", date(2026, 4, 18))

# Put-Call Ratio over the day
print(pcr_by_snapshot(df))

# Max Pain for the latest snapshot
latest = df["snapshot_ts"].max()
print("Max Pain:", max_pain(df, latest))

# IV Surface
print(iv_surface(df, latest))
```

## Automate on Linux (crontab)

To start the collector automatically at boot (it self-guards for market hours):

```bash
# crontab -e
@reboot cd /path/to/project && python collector.py >> option_chain_data/logs/cron.log 2>&1 &
```

## Automate on Windows (Task Scheduler)

Create a task that runs `python collector.py` at logon / daily.
The script will skip weekends and non-market hours automatically.

## Running the Web UI

```bash
pip install -r requirements.txt
streamlit run app.py
```

Open http://localhost:8501 in your browser.
