import math
import pandas as pd
import numpy as np

# ---------- Indicator math (reproducible) ----------

def sma(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n, min_periods=n).mean()

def psar(high: pd.Series, low: pd.Series, step=0.02, max_af=0.2) -> pd.Series:
    """
    Parabolic SAR (TradingView-style defaults).
    Returns SAR values aligned to input index.
    """
    h = high.to_numpy(float)
    l = low.to_numpy(float)
    n = len(h)
    sar = np.full(n, np.nan)

    # initialize trend based on first two bars
    bull = True
    af = step
    ep = h[0]
    sar[0] = l[0]

    for i in range(1, n):
        prev_sar = sar[i-1]
        if np.isnan(prev_sar):
            prev_sar = l[i-1]

        # calculate new SAR
        sar_i = prev_sar + af * (ep - prev_sar)

        # clamp SAR using prior lows/highs (classic PSAR rules)
        if bull:
            sar_i = min(sar_i, l[i-1], l[i-2] if i >= 2 else l[i-1])
            # reversal?
            if l[i] < sar_i:
                bull = False
                sar_i = ep
                ep = l[i]
                af = step
            else:
                if h[i] > ep:
                    ep = h[i]
                    af = min(af + step, max_af)
        else:
            sar_i = max(sar_i, h[i-1], h[i-2] if i >= 2 else h[i-1])
            # reversal?
            if h[i] > sar_i:
                bull = True
                sar_i = ep
                ep = h[i]
                af = step
            else:
                if l[i] < ep:
                    ep = l[i]
                    af = min(af + step, max_af)

        sar[i] = sar_i

    return pd.Series(sar, index=high.index)

# ---------- SQLite helpers ----------

def load_bars(conn, symbol: str, tf: str, start_utc: str, end_utc: str) -> pd.DataFrame:
    q = """
    SELECT ts_utc, open, high, low, close
    FROM bars
    WHERE symbol=? AND timeframe=? AND ts_utc>=? AND ts_utc<=?
    ORDER BY ts_utc ASC
    """
    df = pd.read_sql_query(q, conn, params=(symbol, tf, start_utc, end_utc))
    df["ts_utc"] = pd.to_datetime(df["ts_utc"])
    return df

def get_bar_window(conn, symbol: str, tf: str) -> tuple[str, str]:
    q = """
    SELECT MIN(ts_utc) AS s, MAX(ts_utc) AS e
    FROM bars
    WHERE symbol=? AND timeframe=?
    """
    row = conn.execute(q, (symbol, tf)).fetchone()
    return row[0], row[1]

# ---------- Execution / resolution logic ----------

def resolve_both_touched(conn, symbol: str, stop: float, target: float, bar_start_utc: pd.Timestamp, bar_end_utc: pd.Timestamp) -> tuple[str, str]:
    """
    Determine whether STOP or TARGET was touched first inside a higher-TF bar.
    Uses 1m if available, else 5m, else unresolved => 'stop' (conservative).
    Returns (resolved_first, used_tf): resolved_first in {'stop','target'}, used_tf in {'1m','5m','unresolved_stop_first'}
    """
    for tf in ["1m", "5m"]:
        s, e = get_bar_window(conn, symbol, tf)
        if s is None:
            continue

        # Only attempt if we have coverage for this window
        if bar_start_utc.strftime("%Y-%m-%d %H:%M:%S") < s or bar_end_utc.strftime("%Y-%m-%d %H:%M:%S") > e:
            continue

        df = load_bars(conn, symbol, tf,
                       bar_start_utc.strftime("%Y-%m-%d %H:%M:%S"),
                       bar_end_utc.strftime("%Y-%m-%d %H:%M:%S"))
        if df.empty:
            continue

        # Iterate chronologically: short trade assumption (Bear Rally is bearish)
        # Stop hit if high >= stop; target hit if low <= target
        for _, r in df.iterrows():
            hit_stop = r["high"] >= stop
            hit_target = r["low"] <= target
            if hit_stop and hit_target:
                # still ambiguous at 1m/5m: conservative stop-first
                return "stop", f"{tf}_both_stop_first"
            if hit_stop:
                return "stop", tf
            if hit_target:
                return "target", tf

    return "stop", "unresolved_stop_first"

# ---------- Backtest core (exits + management) ----------

def run_exit_grid(
    conn,
    symbol: str,
    entry_tf: str,
    trail_tf: str,
    entry_ts_list_utc: list[pd.Timestamp],
    psar_step=0.02,
    psar_max=0.2,
    be_touch_levels=(0.5, 1.0),
    target_r_list=(0.5, 1.0, 1.5, 2.0, 3.0, 5.0),
    horizon_bars_list=(None, 24, 48, 72, 120, 168, 240, 336),
    starting_equity=4000.0,
    risk_pct=0.01
) -> pd.DataFrame:
    """
    Phase 1: Entries are provided (no hard-coded detector).
    We simulate:
      - touch-based execution
      - BE on touch at +0.5R/+1.0R (tested)
      - after BE: trail above PSAR computed on trail_tf (15m)
      - ambiguous same-bar resolution using lower TF (1m then 5m), else stop-first
    NOTE: Initial stop/entry price must come from your signal notes later; for now we assume you will include them in a future signal schema.
    """
    # Placeholder: in Phase 1 we require entry price + stop price per signal.
    # We enforce it to avoid inventing values.
    raise NotImplementedError(
        "Phase 1 requires entry price and initial stop per signal. "
        "Next step: store them with the signals (entry_price, stop_price) so exits can be simulated with real math."
    )

def winrate_moe_95(p: float, n: int) -> float:
    if n <= 0:
        return float("nan")
    return 1.96 * math.sqrt((p * (1 - p)) / n)