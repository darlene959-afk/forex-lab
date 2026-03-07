# forexlab/ingest_mt5.py
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import pandas as pd

from tz import broker_to_utc

MT5_TF_MAP = {
    "M1": "1m", "M5": "5m", "M15": "15m", "M30": "30m",
    "H1": "1H", "H2": "2H", "H3": "3H", "H4": "4H",
    "D1": "1D", "W1": "1W", "MN1": "1M",
    "DAILY": "1D", "WEEKLY": "1W", "MONTHLY": "1M",
}

@dataclass
class FileMeta:
    symbol: str
    timeframe: str
    range_start: str | None
    range_end: str | None

def infer_from_filename(filename: str) -> FileMeta:
    name = Path(filename).stem
    # SYMBOL_TF_YYYYMMDDHHMM_YYYYMMDDHHMM
    m = re.match(r"^([A-Z]{6})_([A-Za-z0-9]+)_(\d{12})_(\d{12})$", name)
    if not m:
        raise ValueError(f"Cannot parse filename: {filename}")

    sym = m.group(1)
    tf_raw = m.group(2).upper()
    tf = MT5_TF_MAP.get(tf_raw, tf_raw)

    rs = m.group(3)
    re_ = m.group(4)
    return FileMeta(sym, tf, rs, re_)

def parse_mt5_csv(path: Path) -> pd.DataFrame:
    # MT5 exports: tab + quote
    raw = pd.read_csv(path, sep="\t", quotechar='"')

    cols = {c.lower(): c for c in raw.columns}

    # datetime
    if "<date>" in cols and "<time>" in cols:
        dt = pd.to_datetime(
            raw[cols["<date>"]].astype(str) + " " + raw[cols["<time>"]].astype(str),
            errors="coerce"
        )
    elif "<date>" in cols:
        dt = pd.to_datetime(raw[cols["<date>"]].astype(str) + " 00:00:00", errors="coerce")
    else:
        raise ValueError("MT5 file missing <DATE> column")

    def colpick(*names):
        for n in names:
            if n.lower() in cols:
                return cols[n.lower()]
        return None

    o = colpick("<open>", "open")
    h = colpick("<high>", "high")
    l = colpick("<low>", "low")
    c = colpick("<close>", "close")

    if not all([o, h, l, c]):
        raise ValueError(f"Missing OHLC columns. Found: {list(raw.columns)}")

    out = pd.DataFrame({
        "ts": dt,
        "open": pd.to_numeric(raw[o], errors="coerce"),
        "high": pd.to_numeric(raw[h], errors="coerce"),
        "low": pd.to_numeric(raw[l], errors="coerce"),
        "close": pd.to_numeric(raw[c], errors="coerce"),
    })

    # optional
    tv = colpick("<tickvol>", "tickvol")
    vv = colpick("<vol>", "vol", "volume")
    sp = colpick("<spread>", "spread")
    out["tickvol"] = pd.to_numeric(raw[tv], errors="coerce") if tv else pd.NA
    out["vol"] = pd.to_numeric(raw[vv], errors="coerce") if vv else pd.NA
    out["spread"] = pd.to_numeric(raw[sp], errors="coerce") if sp else pd.NA

    out = out.dropna(subset=["ts", "open", "high", "low", "close"]).copy()
    out = out.sort_values("ts").drop_duplicates(subset=["ts"], keep="last").reset_index(drop=True)

    # broker/server time -> UTC naive
    out["ts_utc"] = broker_to_utc(out["ts"])
    out = out.drop(columns=["ts"])
    return out

def insert_bars(conn, meta: FileMeta, df: pd.DataFrame, source_file: str) -> int:
    df = df.copy()

    # ✅ Convert pandas Timestamp -> SQLite-friendly string
    df["ts_utc"] = pd.to_datetime(df["ts_utc"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    rows = list(df[["ts_utc","open","high","low","close","tickvol","vol","spread"]].itertuples(index=False, name=None))

    conn.executemany(
        """
        INSERT OR REPLACE INTO bars(symbol,timeframe,ts_utc,open,high,low,close,tickvol,vol,spread,source_file)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        [(meta.symbol, meta.timeframe, ts, o, h, l, c, tv, vv, sp, source_file)
         for (ts, o, h, l, c, tv, vv, sp) in rows]
    )
    conn.commit()
    return len(rows)

def log_import(conn, meta: FileMeta, source_file: str, rows_inserted: int, min_ts_utc: str, max_ts_utc: str) -> None:
    now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """
        INSERT INTO imports(source_file,symbol,timeframe,file_range_start,file_range_end,rows_inserted,min_ts_utc,max_ts_utc,imported_at_utc)
        VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (source_file, meta.symbol, meta.timeframe, meta.range_start, meta.range_end, rows_inserted, min_ts_utc, max_ts_utc, now_utc)
    )
    conn.commit()