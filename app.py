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

tab1, tab2, tab3 = st.tabs(["Upload Market Data", "Upload Strategy PDF/DOCX", "Database Overview"])

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
        cov["start_est"] = utc_to_display(pd.to_datetime(cov["utc_start"])).dt.strftime("%Y-%m-%d %H:%M")
        cov["end_est"] = utc_to_display(pd.to_datetime(cov["utc_end"])).dt.strftime("%Y-%m-%d %H:%M")
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

conn.close()