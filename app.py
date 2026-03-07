# app.py
import streamlit as st
from pathlib import Path
import pandas as pd

def strategy_name_exists(conn, name: str) -> bool:
    cur = conn.execute("SELECT 1 FROM strategy_files WHERE LOWER(name)=LOWER(?) LIMIT 1", (name.strip(),))
    return cur.fetchone() is not None

def list_strategies(conn) -> list[tuple]:
    """
    Returns list of (strategy_file_id, name, filename, uploaded_at_utc)
    """
    cur = conn.execute("""
        SELECT strategy_file_id, name, filename, uploaded_at_utc
        FROM strategy_files
        ORDER BY strategy_file_id DESC
    """)
    return cur.fetchall()

def delete_strategy(conn, strategy_file_id: int) -> None:
    conn.execute("DELETE FROM strategy_files WHERE strategy_file_id=?", (strategy_file_id,))
    conn.commit()


from db import connect, init_db
from ingest_mt5 import infer_from_filename, parse_mt5_csv, insert_bars, log_import
from strategy_store import save_strategy_file
from tz import utc_to_display

DB_PATH = "forex.db"

st.set_page_config(page_title="Forex Lab", layout="wide")
st.title("Forex Lab — MT5 Data + Strategy Library")

conn = connect(DB_PATH)
init_db(conn)

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Upload Market Data",
    "Upload Strategy PDF/DOCX",
    "Database Overview",
    "Strategy Viewer",
    "Backtest"
])

with tab1:
    st.subheader("Upload MT5 CSV files")
    st.caption("Filenames like EURUSD_M5_YYYYMMDDHHMM_YYYYMMDDHHMM.csv are auto-detected. Timestamps stored in UTC, displayed in EST/EDT.")

    files = st.file_uploader("Upload one or more MT5 CSV files", type=["csv"], accept_multiple_files=True)

    if files:
        rows_out = []
        for f in files:
            meta = infer_from_filename(f.name)
            df = parse_mt5_csv(Path(f.name)) if False else None  # placeholder to satisfy linter

            # Streamlit upload gives bytes; parse_mt5_csv expects a Path.
            # So we parse via pandas directly here:
            import pandas as pd
            from io import StringIO, BytesIO

            # MT5 is tab-delimited + quotes
            raw = pd.read_csv(BytesIO(f.getvalue()), sep="\t", quotechar='"')
            # reuse the same parser logic by calling parse_mt5_csv on a temp file is overkill;
            # simplest: write to a temp path (safe locally).
            tmp_path = Path(".tmp_upload.csv")
            tmp_path.write_bytes(f.getvalue())

            df = parse_mt5_csv(tmp_path)
            tmp_path.unlink(missing_ok=True)

            inserted = insert_bars(conn, meta, df, f.name)
            min_ts = df["ts_utc"].min().strftime("%Y-%m-%d %H:%M:%S")
            max_ts = df["ts_utc"].max().strftime("%Y-%m-%d %H:%M:%S")
            log_import(conn, meta, f.name, inserted, min_ts, max_ts)

            rows_out.append({
                "file": f.name,
                "symbol": meta.symbol,
                "timeframe": meta.timeframe,
                "rows_inserted": inserted,
                "utc_start": min_ts,
                "utc_end": max_ts
            })

        st.success("Import complete.")
        st.dataframe(pd.DataFrame(rows_out), use_container_width=True)

with tab2:
    st.subheader("Upload Strategy Sheet (PDF or DOCX)")
    st.caption("We store the original file + extracted text in SQLite for later analysis.")

    # reset counter to clear widgets after save
    if "strategy_reset" not in st.session_state:
        st.session_state["strategy_reset"] = 0

    name_key = f"strategy_name_{st.session_state['strategy_reset']}"
    file_key = f"strategy_file_{st.session_state['strategy_reset']}"

    strat_name = st.text_input(
        "Strategy name (e.g., Bearish Power Reversal)",
        key=name_key
    )

    # ✅ warn if name already exists (case-insensitive)
    if strat_name.strip() and strategy_name_exists(conn, strat_name):
        st.warning("A strategy with this name already exists. Consider using a unique name (e.g., add v2).")

    strat_file = st.file_uploader(
        "Upload a strategy PDF/DOCX",
        type=["pdf", "docx"],
        accept_multiple_files=False,
        key=file_key
    )

    # Optional: require unique name before saving (toggle)
    enforce_unique = st.checkbox("Prevent saving duplicate strategy names", value=True)

    if st.button("Save strategy"):
        if not strat_file:
            st.error("Please choose a PDF or DOCX file first.")
        elif not strat_name.strip():
            st.error("Please enter a strategy name first.")
        else:
            if enforce_unique and strategy_name_exists(conn, strat_name):
                st.error("That strategy name already exists. Rename it (e.g., add v2) or uncheck the duplicate protection.")
            else:
                try:
                    strategy_id = save_strategy_file(conn, strat_name.strip(), strat_file.name, strat_file.getvalue())
                    st.success(f"Saved strategy file. ID = {strategy_id}")

                    # ✅ clear fields by changing widget keys
                    st.session_state["strategy_reset"] += 1
                    st.rerun()

                except Exception as e:
                    st.error(f"Failed to save strategy: {e}")

    st.divider()
    st.subheader("Delete a strategy")

    strategies = list_strategies(conn)
    if not strategies:
        st.info("No strategies saved yet.")
    else:
        # Build display labels
        options = [
            (sid, f"ID {sid} — {name} — {filename} — uploaded {uploaded_at}")
            for (sid, name, filename, uploaded_at) in strategies
        ]

        selected = st.selectbox(
            "Select a strategy to delete",
            options=options,
            format_func=lambda x: x[1]
        )

        confirm = st.checkbox("I understand this will permanently delete the selected strategy.", value=False)

        if st.button("Delete selected strategy"):
            if not confirm:
                st.error("Please check the confirmation box before deleting.")
            else:
                delete_strategy(conn, int(selected[0]))
                st.success("Strategy deleted.")
                st.rerun()
with tab3:
    import datetime
    from db import backup_db
    import shutil

    st.subheader("Backup database")

    backup_name = f"forex_backup_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.db"
    backup_path = backup_name

    if st.button("Create backup now"):
        try:
            # Make sure DB is flushed/closed before backup call
            conn.close()
            backup_db(DB_PATH, backup_path)
            # Re-open for rest of app
            conn = connect(DB_PATH)
            init_db(conn)

            with open(backup_path, "rb") as f:
                st.download_button("Download backup file", data=f, file_name=backup_name)
            st.success("Backup created.")
        except Exception as e:
            st.error(f"Backup failed: {e}")

    st.subheader("What’s in the database?")

    # Coverage
    cov = pd.read_sql_query("""
        SELECT symbol, timeframe, MIN(ts_utc) AS utc_start, MAX(ts_utc) AS utc_end, COUNT(*) AS bars
        FROM bars
        GROUP BY symbol, timeframe
        ORDER BY symbol, timeframe
    """, conn)

    if not cov.empty:
        # Show display-time conversions
        cov["start_est"] = utc_to_display(pd.to_datetime(cov["utc_start"])) .dt.strftime("%Y-%m-%d %H:%M")
        cov["end_est"] = utc_to_display(pd.to_datetime(cov["utc_end"])) .dt.strftime("%Y-%m-%d %H:%M")
        st.dataframe(cov, use_container_width=True)
    else:
        st.info("No bars imported yet.")

    st.subheader("Recent imports")
    imp = pd.read_sql_query("""
        SELECT import_id, source_file, symbol, timeframe, rows_inserted, min_ts_utc, max_ts_utc, imported_at_utc
        FROM imports
        ORDER BY import_id DESC
        LIMIT 50
    """, conn)
    st.dataframe(imp, use_container_width=True)

    st.subheader("Strategy files")
    sf = pd.read_sql_query("""
        SELECT strategy_file_id, name, filename, file_type, uploaded_at_utc,
               CASE WHEN extracted_text IS NULL THEN 0 ELSE LENGTH(extracted_text) END AS extracted_chars
        FROM strategy_files
        ORDER BY strategy_file_id DESC
        LIMIT 50
    """, conn)
    st.dataframe(sf, use_container_width=True)

with tab4:
    st.subheader("Strategy Viewer")
    st.caption("View the extracted text stored for each uploaded strategy (PDF/DOCX).")

    # Load strategies from DB
    strat_rows = conn.execute("""
        SELECT strategy_file_id, name, filename, file_type, uploaded_at_utc,
               COALESCE(LENGTH(extracted_text), 0) AS extracted_chars
        FROM strategy_files
        ORDER BY strategy_file_id DESC
    """).fetchall()

    if not strat_rows:
        st.info("No strategies found yet. Upload one in the 'Upload Strategy PDF/DOCX' tab.")
    else:
        options = [
            (r[0], f"ID {r[0]} — {r[1]} — {r[2]} ({r[3]}) — uploaded {r[4]} — {r[5]} chars")
            for r in strat_rows
        ]

        selected = st.selectbox(
            "Select a strategy to view",
            options=options,
            format_func=lambda x: x[1]
        )

        strategy_id = int(selected[0])

        row = conn.execute("""
            SELECT strategy_file_id, name, filename, file_type, uploaded_at_utc, extracted_text
            FROM strategy_files
            WHERE strategy_file_id = ?
        """, (strategy_id,)).fetchone()

        st.markdown(f"**Name:** {row[1]}")
        st.markdown(f"**File:** {row[2]} ({row[3]})")
        st.markdown(f"**Uploaded (UTC):** {row[4]}")

        text = row[5] or ""
        if not text.strip():
            st.warning("No extracted text found for this file (PDF may be image-only).")
        else:
            st.markdown("### Extracted text")
            st.text_area("",
                         value=text,
                         height=500)

            # Download extracted text
            st.download_button(
                label="Download extracted text (.txt)",
                data=text.encode("utf-8"),
                file_name=f"strategy_{strategy_id}_{row[1].replace(' ','_')}.txt",
                mime="text/plain"
            )

with tab5:
    st.subheader("Backtest (Phase 1: signal-driven, no hard-coded detector)")
    st.caption("Upload a signals CSV with entry timestamp + entry price + initial stop. All results are computed from SQLite bar data.")

    st.markdown("### Upload signals CSV")
    st.caption("CSV must include: strategy_name, symbol, entry_tf, entry_price, stop_price, and either entry_ts_est (preferred) or entry_ts_utc.")
    sig_file = st.file_uploader("Signals CSV", type=["csv"], accept_multiple_files=False, key="signals_upload")

    if sig_file is not None:
        import pandas as pd
        from io import BytesIO
        df = pd.read_csv(BytesIO(sig_file.getvalue()))
        
        import pytz

        times_entered_in_est = st.checkbox("My timestamps are entered in EST/EDT (America/New_York). Convert to UTC automatically.",
        value=True)

        def est_to_utc_str(ts_str: str) -> str:
            """
            Convert 'YYYY-MM-DD HH:MM:SS' entered in America/New_York to UTC string.
            Handles DST using tz rules.
            """
            ny = pytz.timezone("America/New_York")
            utc = pytz.UTC

            dt = pd.to_datetime(ts_str, errors="coerce")
            if pd.isna(dt):
                raise ValueError(f"Bad timestamp: {ts_str}")

            # localize to NY time, then convert to UTC
            dt_local = ny.localize(dt.to_pydatetime(), is_dst=None)  # raises if ambiguous
            dt_utc = dt_local.astimezone(utc).replace(tzinfo=None)

            return dt_utc.strftime("%Y-%m-%d %H:%M:%S")

        #required = {"strategy_name","symbol","entry_tf","entry_ts_utc","entry_price","stop_price"}
        required_base = {"strategy_name","symbol","entry_tf","entry_price","stop_price"}
        has_est = "entry_ts_est" in df.columns
        has_utc = "entry_ts_utc" in df.columns
        missing_base = required_base - set(df.columns)

        if missing_base:
            st.error(f"Missing required columns: {sorted(list(missing_base))}")
        elif not (has_est or has_utc):
            st.error("Missing timestamp column. Provide either 'entry_ts_est' (preferred) or 'entry_ts_utc'.")
        else:
            pass  # continue

        missing = required_base - set(df.columns)
        if missing:
            st.error(f"Missing required columns: {sorted(list(missing))}")
        else:
            # Insert into signals_v2 (dedupe by exact timestamp per strategy/symbol/tf)
            rows = []
            for _, r in df.iterrows():
                ts_input = str(r["entry_ts_est"]) if "entry_ts_est" in df.columns else str(r["entry_ts_utc"])
                ts_utc = est_to_utc_str(ts_input) if times_entered_in_est else ts_input

                rows.append((
                    str(r["strategy_name"]),
                    str(r["symbol"]),
                    str(r["entry_tf"]),
                    ts_utc,
                    float(r["entry_price"]),
                    float(r["stop_price"]),
                    str(r.get("notes",""))
                ))
            
            conn.executemany("""
                INSERT INTO signals_v2(strategy_name,symbol,entry_tf,entry_ts_utc,entry_price,stop_price,notes)
                VALUES (?,?,?,?,?,?,?)
            """, rows)
            conn.commit()
            st.success(f"Inserted {len(rows)} signals into signals_v2.")

    st.markdown("### Next")
    st.info("Next step: wire the Bear Rally exit grid (BE on touch + PSAR trail on 15m + targets + time exits), using these stored signals.")

conn.close()