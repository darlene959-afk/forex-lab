import math
import pandas as pd
import numpy as np
from streamlit import progress
def attach_indicators_basic(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["sma20"] = d["close"].rolling(20, min_periods=20).mean()
    d["sma50"] = d["close"].rolling(50, min_periods=50).mean()
    return d

def pivot_high_flags(high: pd.Series, left: int = 2, right: int = 2) -> pd.Series:
    h = high.to_numpy(float)
    n = len(h)
    piv = np.zeros(n, dtype=bool)
    for i in range(left, n - right):
        center = h[i]
        if center > h[i-left:i].max() and center > h[i+1:i+right+1].max():
            piv[i] = True
    return pd.Series(piv, index=high.index)

def last_row_at_or_before(df: pd.DataFrame, t: pd.Timestamp) -> pd.Series | None:
    # df must be sorted by ts_utc ascending
    idx = df["ts_utc"].searchsorted(t, side="right") - 1
    if idx < 0 or idx >= len(df):
        return None
    return df.iloc[int(idx)]

def find_bear_rally_v2_setups(
    df1: pd.DataFrame,
    df15: pd.DataFrame,
    dfD: pd.DataFrame,
    spec: dict,
    max_setups: int,
    progress_cb=None
) -> pd.DataFrame:
    """
    Bear Rally v2:
      Trend filters (D/1H/15m): close < sma50 AND sma20 < sma50
      Rally: >=3 bullish candles (close>open) in last lookback bars BEFORE confirmation candle
      Confirmation candle: bearish candle (close<open) immediately BEFORE trigger bar
      Trigger: prev_close > prev_psar AND curr_close <= curr_psar  (close-break below PSAR)
      Entry: trigger_close (after trigger close)
      Stop: most recent pivot high (2L/2R) prior to trigger
    """
    sf = spec["strategy_fields"]
    lookback = int(sf["rally_definition"]["lookback_bars"])
    min_bull = int(sf["rally_definition"]["min_bullish_candles"])

    d = df1.copy().reset_index(drop=True)

    # Trigger: close-break below PSAR
    d["prev_close"] = d["close"].shift(1)
    d["prev_psar"]  = d["psar"].shift(1)
    trigger_mask = (d["prev_close"] > d["prev_psar"]) & (d["close"] <= d["psar"])

    setups = []
    trigger_idxs = np.where(trigger_mask.to_numpy(bool))[0]

    for i in trigger_idxs:
        t = d.loc[i, "ts_utc"]

        # Need confirmation candle immediately before trigger
        if i - 1 < 0:
            continue
        confirm_bear = d.loc[i - 1, "close"] < d.loc[i - 1, "open"]
        if not confirm_bear:
            continue

        # Rally condition: >= min_bull bullish candles in the window before confirmation
        w_start = max(0, (i - 1) - lookback)
        rally_window = d.iloc[w_start:(i - 1)]
        bull_count = int((rally_window["close"] > rally_window["open"]).sum())
        if bull_count < min_bull:
            continue

        # Trend filters at trigger time
        rowD = last_row_at_or_before(dfD, t)
        row15 = last_row_at_or_before(df15, t)
        if rowD is None or row15 is None:
            continue

        # Daily downtrend
        if not (rowD["close"] < rowD["sma50"] and rowD["sma20"] < rowD["sma50"]):
            continue
        # 1H downtrend (at trigger bar)
        if not (d.loc[i, "close"] < d.loc[i, "sma50"] and d.loc[i, "sma20"] < d.loc[i, "sma50"]):
            continue
        # 15m downtrend + trading below 50MA
        if not (row15["close"] < row15["sma50"] and row15["sma20"] < row15["sma50"]):
            continue

        setups.append({
            "trigger_ts_utc": t,
            "trigger_close": float(d.loc[i, "close"]),
            "trigger_high": float(d.loc[i, "high"]),
            "trigger_low": float(d.loc[i, "low"]),
            "psar_1h": float(d.loc[i, "psar"]),
            "bull_count_in_window": bull_count,
            "trigger_index": int(i)
        })

        # Progress update: show setups found so far (not spammy)
        if progress_cb and (len(setups) == 1 or len(setups) % 25 == 0):
            progress_cb({"msg": f"Scanning setups… found {len(setups)} so far"})

        if len(setups) >= max_setups:
            break

    return pd.DataFrame(setups)

# ---------------- Indicators (reproducible math) ----------------

def sma(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n, min_periods=n).mean()

def psar(high: pd.Series, low: pd.Series, step=0.02, max_af=0.2) -> pd.Series:
    """
    Parabolic SAR with TradingView-style defaults.
    Returns SAR values aligned to input index.
    """
    h = high.to_numpy(float)
    l = low.to_numpy(float)
    n = len(h)
    sar = np.full(n, np.nan)

    bull = True
    af = step
    ep = h[0]
    sar[0] = l[0]

    for i in range(1, n):
        prev_sar = sar[i-1]
        if np.isnan(prev_sar):
            prev_sar = l[i-1]

        sar_i = prev_sar + af * (ep - prev_sar)

        if bull:
            sar_i = min(sar_i, l[i-1], l[i-2] if i >= 2 else l[i-1])
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

# ---------------- SQLite helpers ----------------

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

def get_bar_window(conn, symbol: str, tf: str) -> tuple[str | None, str | None]:
    q = """
    SELECT MIN(ts_utc) AS s, MAX(ts_utc) AS e
    FROM bars
    WHERE symbol=? AND timeframe=?
    """
    row = conn.execute(q, (symbol, tf)).fetchone()
    return row[0], row[1]

# ---------------- Ambiguous bar resolution ----------------

def resolve_both_touched(conn, symbol: str, stop: float, target: float,
                         bar_start_utc: pd.Timestamp, bar_end_utc: pd.Timestamp) -> tuple[str, str]:
    """
    If a higher-TF bar hits stop and target, drill down to 1m, else 5m.
    If still ambiguous or no coverage -> stop-first (conservative).
    Returns (first_hit, used_tf).
    """
    for tf in ["1m", "5m"]:
        s, e = get_bar_window(conn, symbol, tf)
        if s is None:
            continue

        s_dt = pd.to_datetime(s)
        e_dt = pd.to_datetime(e)
        if bar_start_utc < s_dt or bar_end_utc > e_dt:
            continue

        df = load_bars(conn, symbol, tf,
                       bar_start_utc.strftime("%Y-%m-%d %H:%M:%S"),
                       bar_end_utc.strftime("%Y-%m-%d %H:%M:%S"))
        if df.empty:
            continue

        # short trade: stop hit if high >= stop; target hit if low <= target
        for _, r in df.iterrows():
            hit_stop = r["high"] >= stop
            hit_target = r["low"] <= target
            if hit_stop and hit_target:
                return "stop", f"{tf}_both_stop_first"
            if hit_stop:
                return "stop", tf
            if hit_target:
                return "target", tf

    return "stop", "unresolved_stop_first"

# ---------------- Utilities ----------------

def winrate_moe_95(p: float, n: int) -> float:
    if n <= 0:
        return float("nan")
    return 1.96 * math.sqrt((p * (1 - p)) / n)


# ---------------- Backtest core ----------------
def run_bear_rally_backtest(
    conn,
    symbol: str,
    spec: dict,
    max_setups: int = 20,
    starting_equity: float = 4000.0,
    risk_pct: float = 0.01,
    progress_cb=None
) -> tuple[pd.DataFrame, pd.DataFrame]: 
    """
    Returns:
      - grid results dataframe
      - setups dataframe (the detected setups used)
    """
    def _progress(msg: str, done: int | None = None, total: int | None = None):
        if progress_cb:
            payload = {"msg": msg}
            if done is not None and total is not None and total > 0:
                payload["done"] = int(done)
                payload["total"] = int(total)
            progress_cb(payload)

    # Pull TFs from spec
    entry_tf = spec["strategy_fields"]["entry_timeframe"]   # "1H"
    trail_tf = spec["strategy_fields"]["trail_timeframe"]   # "15m"
    daily_tf = "1D"

    # Determine overlap window across required TFs
    s1, e1 = get_bar_window(conn, symbol, entry_tf)
    s15, e15 = get_bar_window(conn, symbol, trail_tf)
    sD, eD = get_bar_window(conn, symbol, daily_tf)

    if None in (s1, e1, s15, e15, sD, eD):
        raise ValueError(f"Missing required data for {symbol}: need {entry_tf}, {trail_tf}, and 1D.")

    start = max(pd.to_datetime(s1), pd.to_datetime(s15), pd.to_datetime(sD))
    end   = min(pd.to_datetime(e1), pd.to_datetime(e15), pd.to_datetime(eD))

    df1 = load_bars(conn, symbol, entry_tf, start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S"))
    df15 = load_bars(conn, symbol, trail_tf, start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S"))
    dfD = load_bars(conn, symbol, daily_tf, start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S"))

    if df1.empty or df15.empty or dfD.empty:
        raise ValueError("Not enough overlapping data across timeframes.")

    # Indicators (SMA20/SMA50 + PSAR)
    df1 = attach_indicators_basic(df1)
    df15 = attach_indicators_basic(df15)
    dfD = attach_indicators_basic(dfD)

    psar_step = float(spec["indicator_defaults"]["psar"]["step"])
    psar_max  = float(spec["indicator_defaults"]["psar"]["max"])

    df1["psar"] = psar(df1["high"], df1["low"], step=psar_step, max_af=psar_max)
    df15["psar"] = psar(df15["high"], df15["low"], step=psar_step, max_af=psar_max)

    # Pivot highs for initial stop (2L/2R)
    df1["pivH"] = pivot_high_flags(df1["high"], left=2, right=2)

    # Find Bear Rally v2 setups
    setups = find_bear_rally_v2_setups(
    df1=df1,
    df15=df15,
    dfD=dfD,
    spec=spec,
    max_setups=max_setups,
    progress_cb=progress_cb
)
    if setups.empty:
        raise ValueError("No Bear Rally v2 setups found with current filters in this data window.")

    _progress(f"Setup scan complete. Setups found: {len(setups)}. Preparing trades…")
    # Build a lookup for 15m PSAR by timestamp
    df15x = df15.copy()
    df15x = df15x.dropna(subset=["psar"]).reset_index(drop=True)

    # Function to get 15m rows between two UTC times
    def get_15m_slice(t0: pd.Timestamp, t1: pd.Timestamp) -> pd.DataFrame:
        m = (df15x["ts_utc"] >= t0) & (df15x["ts_utc"] <= t1)
        return df15x.loc[m, ["ts_utc", "high", "low", "close", "psar"]].copy()

    # Prepare entry/stop per setup
    # Build per-setup entry + initial stop using "most recent pivot high before trigger"
    trades = []
    for r in setups.itertuples(index=False):
        entry_ts = pd.to_datetime(r.trigger_ts_utc)
        entry_price = float(r.trigger_close)   # entry after close of trigger candle

        # most recent pivot high strictly before trigger candle
        piv = df1.loc[df1["ts_utc"] < entry_ts]
        piv = piv.loc[piv["pivH"] == True]
        if piv.empty:
            continue
        stop0 = float(piv.iloc[-1]["high"])  # most recent pivot high

        # Must be a valid short risk
        if stop0 <= entry_price:
            continue

        trades.append({
            "entry_ts": entry_ts,
            "entry": entry_price,
            "stop0": stop0
        })

    trades_df = pd.DataFrame(trades)
    if trades_df.empty:
        raise ValueError("Setups found, but none had a valid pivot-high stop (stop <= entry).")
    
    # Spec-driven grids
    be_levels = [None] + [float(x) for x in spec["strategy_fields"]["be_move"]["candidates_r"]]
    targets = [float(x) for x in spec["strategy_fields"]["targets"]["tested_target_r"]]

    max_holds_raw = spec["strategy_fields"]["time_exits"]["entry_tf_bars"]
    max_holds = []
    for x in max_holds_raw:
        if x == "Hold":
            max_holds.append(None)
        else:
            max_holds.append(int(x))
        
    rows = []

    # For trailing on 15m: we tighten stop downward for shorts
    def get_15m_slice(t0: pd.Timestamp, t1: pd.Timestamp) -> pd.DataFrame:
        m = (df15x["ts_utc"] >= t0) & (df15x["ts_utc"] <= t1)
        return df15x.loc[m, ["ts_utc", "high", "low", "close", "psar"]].copy()
    
    total_cfg = len(be_levels) * len(targets) * len(max_holds)
    done_cfg = 0
    _progress(f"Starting simulation of {total_cfg} configurations…", 0, total_cfg)
    
    for be_r in be_levels:
        for targetR in targets:
            for horizon in max_holds:

                eq = starting_equity
                eq_curve = []
                bars_held_list = []
                wins = 0
                n_trades = 0

                stop_hits = 0
                target_hits = 0
                time_exits = 0
                
                both_hit = 0
                both_1m = 0
                both_5m = 0
                both_unres = 0

                for tr in trades_df.itertuples(index=False):
                    entry_ts = tr.entry_ts
                    entry = float(tr.entry)
                    stop0 = float(tr.stop0)

                    R = stop0 - entry
                    if R <= 0:
                        continue

                    # Baseline RR>=2 filter: require that 2R target is below entry (always true) AND meaningful.
                    # (This is inherently meaningful if R>0 and price series exists, so we don't exclude here.)

                    target = entry - targetR * R  # short target

                    # Locate entry bar index in df1
                    idx = df1.index[df1["ts_utc"] == entry_ts]
                    if len(idx) == 0:
                        continue
                    start_i = int(idx[0])
                    end_i = (len(df1) - 1) if horizon is None else min(start_i + horizon, len(df1) - 1)

                    stop_current = stop0
                    be_active = False
                    exited = False

                    for i in range(start_i, end_i + 1):
                        bar = df1.iloc[i]
                        hi = float(bar["high"])
                        lo = float(bar["low"])

                        # BE activation (touch)
                        if (be_r is not None) and (not be_active):
                            be_price = entry - float(be_r) * R
                            if lo <= be_price:
                                stop_current = min(stop_current, entry)  # move to BE
                                be_active = True

                        # After BE: trail stop above PSAR on 15m
                        if be_active:
                            t0 = pd.to_datetime(bar["ts_utc"])
                            t1 = t0 + pd.Timedelta(hours=1) - pd.Timedelta(seconds=1)
                            sl = get_15m_slice(t0, t1)
                            for _, rr in sl.iterrows():
                                ps = float(rr["psar"])
                                stop_current = min(stop_current, ps)

                        hit_stop = hi >= stop_current
                        hit_target = lo <= target

                        if hit_stop or hit_target:
                            if hit_stop and hit_target:
                                both_hit += 1
                                t0 = pd.to_datetime(bar["ts_utc"])
                                t1 = t0 + pd.Timedelta(hours=1) - pd.Timedelta(seconds=1)
                                first, used = resolve_both_touched(conn, symbol, stop_current, target, t0, t1)
                                if used == "1m":
                                    both_1m += 1
                                elif used == "5m":
                                    both_5m += 1
                                else:
                                    both_unres += 1
                                exit_price = stop_current if first == "stop" else target

                                if first == "stop":
                                    stop_hits += 1
                                else:
                                    target_hits += 1

                            elif hit_stop:
                                exit_price = stop_current
                                stop_hits += 1
                            else:
                                exit_price = target
                                target_hits += 1

                            bars_held = i - start_i + 1
                            bars_held_list.append(bars_held)

                            r_mult = (entry - float(exit_price)) / R
                            if r_mult > 0:
                                wins += 1

                            eq += (risk_pct * eq) * r_mult
                            eq_curve.append(eq)
                            n_trades += 1
                            exited = True
                            break

                    if not exited:
                        # time exit at close of last bar
                        exit_price = float(df1.iloc[end_i]["close"])
                        time_exits += 1
                        bars_held = end_i - start_i + 1
                        bars_held_list.append(bars_held)
                        r_mult = (entry - exit_price) / R
                        if r_mult > 0:
                            wins += 1
                        eq += (risk_pct * eq) * r_mult
                        eq_curve.append(eq)
                        n_trades += 1

                if n_trades == 0:
                    continue

                eq_arr = np.array(eq_curve, dtype=float)
                peak = np.maximum.accumulate(eq_arr)
                dd = (eq_arr - peak) / peak

                profit_pct = (eq_arr[-1] / starting_equity - 1.0) * 100.0
                win_rate = wins / n_trades
                moe = winrate_moe_95(win_rate, n_trades) * 100.0
                avg_bars = float(np.mean(bars_held_list)) if bars_held_list else float("nan")
                max_dd = float(dd.min() * 100.0) if len(dd) else 0.0

                rows.append({
                    "symbol": symbol,
                    "stop_hit_count": int(stop_hits),
                    "stop_hit_%": float((stop_hits / n_trades) * 100.0),
                    "target_hit_count": int(target_hits),
                    "target_hit_%": float((target_hits / n_trades) * 100.0),
                    "time_exit_count": int(time_exits),
                    "time_exit_%": float((time_exits / n_trades) * 100.0),
                    "strategy": spec.get("_spec_name", spec.get("strategy_name", "Bear Rally")),
                    "spec_id": int(spec.get("_spec_id", -1)),
                    "spec_label": spec.get("spec_label", ""),
                    "entry_tf": entry_tf,
                    "trail_tf": trail_tf,
                    "be_touch_R": ("None" if be_r is None else float(be_r)),
                    "target_R": float(targetR),
                    "max_hold_bars": ("Hold" if horizon is None else int(horizon)),
                    "setups": int(n_trades),
                    "profit_%": float(profit_pct),
                    "win_rate_%": float(win_rate * 100.0),
                    "win_rate_MOE95_%": float(moe),
                    "avg_bars_held": float(avg_bars),
                    "max_DD_%": float(max_dd),
                    "both_hit_cases": int(both_hit),
                    "both_hit_resolved_1m": int(both_1m),
                    "both_hit_resolved_5m": int(both_5m),
                    "both_hit_unresolved": int(both_unres)
                })

                done_cfg += 1
                if done_cfg == 1 or done_cfg % 10 == 0 or done_cfg == total_cfg:
                    _progress(f"Simulating configs… {done_cfg}/{total_cfg}", done_cfg, total_cfg)

    grid = pd.DataFrame(rows)
    if grid.empty:
        raise ValueError("Backtest produced no results after applying all Bear Rally v2 filters and stop logic.")

    grid = grid.sort_values("profit_%", ascending=False).reset_index(drop=True)

    _progress("Backtest complete. Rendering results…", total_cfg, total_cfg)
    return grid, setups

def count_bear_rally_setups_all(conn, symbol: str, spec: dict) -> tuple[int, str, str]:
    """
    Count all Bear Rally v2 setups over the full overlapping window (1H + 15m + 1D).
    Returns (count, start_utc_str, end_utc_str).
    """
    entry_tf = spec["strategy_fields"]["entry_timeframe"]
    trail_tf = spec["strategy_fields"]["trail_timeframe"]
    daily_tf = "1D"

    s1, e1 = get_bar_window(conn, symbol, entry_tf)
    s15, e15 = get_bar_window(conn, symbol, trail_tf)
    sD, eD = get_bar_window(conn, symbol, daily_tf)
    if None in (s1, e1, s15, e15, sD, eD):
        raise ValueError(f"Missing required data for {symbol}: need {entry_tf}, {trail_tf}, and 1D.")

    start = max(pd.to_datetime(s1), pd.to_datetime(s15), pd.to_datetime(sD))
    end   = min(pd.to_datetime(e1), pd.to_datetime(e15), pd.to_datetime(eD))

    df1 = load_bars(conn, symbol, entry_tf, start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S"))
    df15 = load_bars(conn, symbol, trail_tf, start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S"))
    dfD = load_bars(conn, symbol, daily_tf, start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S"))

    df1 = attach_indicators_basic(df1)
    df15 = attach_indicators_basic(df15)
    dfD = attach_indicators_basic(dfD)

    psar_step = float(spec["indicator_defaults"]["psar"]["step"])
    psar_max  = float(spec["indicator_defaults"]["psar"]["max"])

    df1["psar"] = psar(df1["high"], df1["low"], step=psar_step, max_af=psar_max)
    df15["psar"] = psar(df15["high"], df15["low"], step=psar_step, max_af=psar_max)

    setups = find_bear_rally_v2_setups(df1, df15, dfD, spec, max_setups=10**12)
    return int(len(setups)), start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")