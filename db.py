# forexlab/db.py
import sqlite3
from pathlib import Path

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS bars (
  symbol      TEXT NOT NULL,
  timeframe   TEXT NOT NULL,
  ts_utc      TEXT NOT NULL,   -- ISO "YYYY-MM-DD HH:MM:SS" in UTC
  open        REAL NOT NULL,
  high        REAL NOT NULL,
  low         REAL NOT NULL,
  close       REAL NOT NULL,
  tickvol     REAL,
  vol         REAL,
  spread      REAL,
  source_file TEXT,
  PRIMARY KEY (symbol, timeframe, ts_utc)
);

CREATE INDEX IF NOT EXISTS idx_bars_symbol_tf_ts
ON bars(symbol, timeframe, ts_utc);

CREATE TABLE IF NOT EXISTS imports (
  import_id       INTEGER PRIMARY KEY AUTOINCREMENT,
  source_file     TEXT NOT NULL,
  symbol          TEXT NOT NULL,
  timeframe       TEXT NOT NULL,
  file_range_start TEXT,
  file_range_end   TEXT,
  rows_inserted   INTEGER NOT NULL,
  min_ts_utc      TEXT,
  max_ts_utc      TEXT,
  imported_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS strategy_files (
  strategy_file_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name             TEXT NOT NULL,        -- user-friendly title
  filename         TEXT NOT NULL,
  file_type        TEXT NOT NULL,        -- pdf/docx
  content_bytes    BLOB NOT NULL,
  extracted_text   TEXT,
  uploaded_at_utc  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS signals (
  signal_id    INTEGER PRIMARY KEY AUTOINCREMENT,
  strategy_name TEXT NOT NULL,
  symbol       TEXT NOT NULL,
  entry_tf     TEXT NOT NULL,
  entry_ts_utc TEXT NOT NULL,    -- ISO UTC timestamp
  notes        TEXT
);

CREATE INDEX IF NOT EXISTS idx_signals_lookup
ON signals(strategy_name, symbol, entry_tf, entry_ts_utc);

CREATE TABLE IF NOT EXISTS signals_v2 (
  signal_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  strategy_name TEXT NOT NULL,
  symbol        TEXT NOT NULL,
  entry_tf      TEXT NOT NULL,
  entry_ts_utc  TEXT NOT NULL,
  entry_price   REAL NOT NULL,
  stop_price    REAL NOT NULL,
  notes         TEXT
);

CREATE INDEX IF NOT EXISTS idx_signals_v2_lookup
ON signals_v2(strategy_name, symbol, entry_tf, entry_ts_utc);

CREATE TABLE IF NOT EXISTS strategy_specs (
  spec_id           INTEGER PRIMARY KEY AUTOINCREMENT,
  strategy_file_id  INTEGER NOT NULL,
  spec_name         TEXT NOT NULL,          -- e.g., "Bear Rally v1"
  spec_json         TEXT NOT NULL,          -- machine-readable rules
  created_at_utc    TEXT NOT NULL,
  FOREIGN KEY(strategy_file_id) REFERENCES strategy_files(strategy_file_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_strategy_specs_file
ON strategy_specs(strategy_file_id);

CREATE TABLE IF NOT EXISTS backtest_runs (
  run_id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_utc TEXT NOT NULL,

  strategy_file_id INTEGER,
  spec_id INTEGER,
  spec_name TEXT,

  symbol TEXT NOT NULL,
  entry_tf TEXT,
  trail_tf TEXT,

  max_setups INTEGER,
  starting_equity REAL,
  risk_pct REAL,

  results_json TEXT NOT NULL,      -- full grid dataframe rows as json
  setups_json TEXT,                -- setups dataframe rows as json (optional)
  winner_json TEXT                 -- best row as json (optional)
);

CREATE INDEX IF NOT EXISTS idx_backtest_runs_symbol
ON backtest_runs(symbol);

CREATE INDEX IF NOT EXISTS idx_backtest_runs_spec
ON backtest_runs(spec_id);

CREATE INDEX IF NOT EXISTS idx_backtest_runs_symbol_spec
ON backtest_runs(symbol, spec_id);

CREATE INDEX IF NOT EXISTS idx_backtest_runs_created
ON backtest_runs(created_at_utc);

"""

def connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()

def backup_db(db_path: str, backup_path: str) -> None:
    import sqlite3
    src = sqlite3.connect(db_path)
    dst = sqlite3.connect(backup_path)
    with dst:
        src.backup(dst)
    dst.close()
    src.close()