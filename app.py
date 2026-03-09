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

@st.cache_resource
def get_connection():
    """Creates a single persisted connection to the SQLite database
     Streamlite will reuse this object across all reruns and sessions.
     """
    c = connect(DB_PATH)
    init_db(c) #initialize tables if not exist

    return c

#Now use the cached function to get the connection
conn = get_connection()



tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Upload Market Data",
    "Upload Strategy PDF/DOCX",
    "Database Overview",
    "Strategy Viewer",
    "Backtest",
    "Help"
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
            
            backup_db(DB_PATH, backup_path)
            

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
        import json
        import datetime

        st.divider()
        st.subheader("Strategy Spec (machine-readable)")

        st.caption("This turns the uploaded strategy text into a structured spec. Review/edit before saving. No hard-coded logic—this spec becomes the source of truth.")

        default_spec_name = f"{row[1]} v1"
        # Apply pending load before widgets render
        pending = st.session_state.get(f"pending_spec_load_{strategy_id}")
        if pending:
            st.session_state[f"spec_json_{strategy_id}"] = pending["spec_json"]
            st.session_state[f"spec_name_{strategy_id}"] = pending["spec_name"]
            st.session_state[f"spec_id_{strategy_id}"] = pending["spec_id"]
            del st.session_state[f"pending_spec_load_{strategy_id}"]
      
        spec_name_key = f"spec_name_{strategy_id}"
        if spec_name_key not in st.session_state:
            st.session_state[spec_name_key] = default_spec_name

        spec_name = st.text_input("Spec name", key=spec_name_key)

        # Draft spec generated from extracted text (lightweight, transparent)
        # We store the extracted text reference in the spec so it's auditable.
        draft = {
            "strategy_name": row[1],
            "source_strategy_file_id": strategy_id,
            "source_filename": row[2],
            "created_from_extracted_text": True,
            "assumptions": {
                "execution": "touch",
                "both_hit_resolution": "use 1m then 5m; else stop-first",
                "display_timezone": "America/New_York"
            },
            "indicator_defaults": {
                "psar": {"step": 0.02, "max": 0.2, "source": "TradingView defaults (per uploaded settings)"},
                "cci": {"length": 20, "smoothing": "raw"},
                "atr": {"length": 14, "ma": "RMA"}
            },
            "rules_text_snippets": {
                "full_extracted_text": (row[5] or "")[:20000]  # store first 20k chars for traceability
            },
            "strategy_fields": {
                "entry_timeframe": "1H",
                "trail_timeframe": "15m",
                "be_move": {
                    "trigger_type": "touch",
                    "candidates_r": [0.5, 1.0]
                },
                "trailing_stop": {
                    "type": "psar",
                    "timeframe": "15m",
                    "side": "above_price"
                },
                "entry_trigger_definition": "FROM_PDF_TEXT",
                "initial_stop_definition": "FROM_PDF_TEXT"
            }
        }

        st.caption("Edit the JSON below as needed. For now, entry/stop definitions will be filled next after we tag the exact lines in the PDF text.")
        st.caption("Tip: Loaded specs are auto-formatted for readability. You can edit and re-save as a new version.")
        # ---- Load an existing saved spec into the JSON editor ----
        saved_specs = conn.execute("""
            SELECT spec_id, spec_name, created_at_utc
            FROM strategy_specs
            WHERE strategy_file_id=?
            ORDER BY spec_id DESC
        """, (strategy_id,)).fetchall()

        if saved_specs:
            st.markdown("### Load an existing spec")
            opt = [(r[0], f"spec_id {r[0]} — {r[1]} ({r[2]} UTC)") for r in saved_specs]
            chosen_spec = st.selectbox("Choose saved spec to load", opt, format_func=lambda x: x[1], key=f"load_spec_{strategy_id}")
            if st.button("Load selected spec into editor", key=f"btn_load_spec_{strategy_id}"):
                spec_id_to_load = int(chosen_spec[0])
                row_spec = conn.execute(
                    "SELECT spec_json, spec_name FROM strategy_specs WHERE spec_id=?",
                    (spec_id_to_load,)
                ).fetchone()

                if row_spec:
                    st.session_state[f"pending_spec_load_{strategy_id}"] = {
                        "spec_id": spec_id_to_load, 
                        "spec_json": json.dumps(json.loads(row_spec[0]), indent=2),
                        "spec_name": row_spec[1]
                    }
                    st.rerun()
                else:
                    st.error("Could not load that spec_id.")


        else:
            st.info("No saved specs yet for this strategy file.")


        spec_json_key = f"spec_json_{strategy_id}"

        # Seed once (only if nothing is loaded yet)
        if spec_json_key not in st.session_state:
            st.session_state[spec_json_key] = json.dumps(draft, indent=2)

        spec_json_str = st.text_area(
            "Spec JSON",
            height=400,
            key=spec_json_key
        )


        # Save spec
        if st.button("Save spec", key=f"save_spec_{strategy_id}"):
            try:
                parsed = json.loads(spec_json_str)  # validate JSON
                now_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

                conn.execute("""
                    INSERT INTO strategy_specs(strategy_file_id, spec_name, spec_json, created_at_utc)
                    VALUES (?,?,?,?)
                """, (strategy_id, spec_name.strip(), json.dumps(parsed), now_utc))
                conn.commit()

                st.success("Spec saved.")
            except Exception as e:
                st.error(f"Could not save spec: {e}")

        # Show existing specs for this strategy file
        specs = conn.execute("""
            SELECT spec_id, spec_name, created_at_utc
            FROM strategy_specs
            WHERE strategy_file_id=?
            ORDER BY spec_id DESC
        """, (strategy_id,)).fetchall()

        if specs:
            st.markdown("### Saved specs")
            st.dataframe(pd.DataFrame(specs, columns=["spec_id","spec_name","created_at_utc"]), use_container_width=True)
        else:
            st.info("No specs saved yet for this strategy.")

with tab5:
    import pandas as pd
    from backtest_engine import run_backtest

  
    # Select spec (for now we use it as the record of settings/version)
    specs = conn.execute("""
        SELECT s.spec_id, s.spec_name, f.name, f.filename
        FROM strategy_specs s
        JOIN strategy_files f ON f.strategy_file_id = s.strategy_file_id
        ORDER BY s.spec_id DESC
    """).fetchall()

    if not specs:
        st.warning("No strategy specs found. Save a spec in Strategy Viewer first.")
    else:
        spec_opt = [(r[0], f"spec_id {r[0]} — {r[1]} (file: {r[3]})") for r in specs]
        chosen = st.selectbox("Choose strategy spec", spec_opt, format_func=lambda x: x[1])
        spec_id = int(chosen[0])

        import json
        spec_row = conn.execute(
            "SELECT spec_json FROM strategy_specs WHERE spec_id=?",
            (spec_id,)
        ).fetchone()

        if spec_row is None:
            st.error("Selected spec_id not found in strategy_specs.")
            st.stop()

        spec = json.loads(spec_row[0])
        
        st.subheader("Backtest — Bear Rally (from selected spec)")

        entry_tf = spec["strategy_fields"]["entry_timeframe"]
        trail_tf = spec["strategy_fields"]["trail_timeframe"]
        trigger_tf = spec["strategy_fields"]["trigger"].get("timeframe", "CURRENT")
        be_levels = spec["strategy_fields"]["be_move"]["candidates_r"]
        targets = spec["strategy_fields"]["targets"]["tested_target_r"]
        time_exits = spec["strategy_fields"]["time_exits"]["entry_tf_bars"]

        st.caption(
            f"Entry: after trigger close ({entry_tf}). "
            f"Trigger: PSAR close-break short on {trigger_tf}. "
            f"Stop: above most recent pivot high before trigger. "
            f"After BE (touch @ {be_levels}R), trail above PSAR on {trail_tf}. "
            f"Targets tested: {targets}R. "
            f"Max hold (bars): {time_exits}. "
            f"Stops/targets execute on touch; same-bar resolved via 1m→5m→stop-first."
        )
        # Pair selection
        symbol = st.selectbox("Symbol", ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD"], index=0)

        with st.expander("What is being tested? (configs grid breakdown)", expanded=False):
            be_levels = spec["strategy_fields"]["be_move"]["candidates_r"]
            targets = spec["strategy_fields"]["targets"]["tested_target_r"]
            holds = spec["strategy_fields"]["time_exits"]["entry_tf_bars"]

            # Count holds excluding the "Hold" label if present
            num_holds = len(holds)
            num_be = len(be_levels) + 1  # +1 for "None" (no BE move)
            num_targets = len(targets)

            total_cfg = num_be * num_targets * num_holds

            st.markdown(f"""
        **A configuration (config)** is one combination of:
        - **BE rule**: None + {be_levels}R  → **{num_be} options**
        - **Target_R**: {targets} → **{num_targets} options**
        - **Max Hold (bars)**: {holds} → **{num_holds} options**

        **Total configs simulated** = {num_be} × {num_targets} × {num_holds} = **{total_cfg}**
        """)
            st.caption("Each config is tested on the same set of detected setups (first N found).")



        from backtest_engine import count_strategy_setups_all

        # ---- Count button ----
        if st.button("Count all strategy setups in full dataset window"):
            try:
                cnt, s_utc, e_utc = count_strategy_setups_all(conn, symbol, spec)
                st.session_state["last_count"] = {
                    "cnt": cnt, "s": s_utc, "e": e_utc,
                    "symbol": symbol, "spec_id": spec_id
                }
            except Exception as e:
                st.error(f"Count failed: {e}")

        # ---- Persisted count display (always) ----
        lc = st.session_state.get("last_count")
        if lc and lc["symbol"] == symbol and lc["spec_id"] == spec_id:
            st.success(f"Total strategy setups found: {lc['cnt']}")
            st.caption("Setup count is based on entry criteria only (no trade management). Requires a valid initial stop per spec.")
            st.write(f"Scanned window (UTC): {lc['s']} → {lc['e']}")

        # Slider should ALWAYS exist
        max_setups = st.slider(
            "How many setups to scan (first N found)",
            min_value=5,
            max_value=500,
            value=50,
            step=5
        )

        # ---- Run button (robust disable + persistent progress) ----
        if "is_backtest_running" not in st.session_state:
            st.session_state["is_backtest_running"] = False
        if "progress_msg" not in st.session_state:
            st.session_state["progress_msg"] = ""
        if "progress_done" not in st.session_state:
            st.session_state["progress_done"] = 0
        if "progress_total" not in st.session_state:
            st.session_state["progress_total"] = 0
        if "run_nonce" not in st.session_state:
            st.session_state["run_nonce"] = 0
        if "run_nonce_completed" not in st.session_state:
            st.session_state["run_nonce_completed"] = -1

       
        if st.button("Run backtest now"):
            st.session_state["is_backtest_running"] = True
            progress_bar = st.progress(0)
            status_text = st.empty()

            try:
                #Pass a callback to update progress from the backtest engine
                def update_ui(p):
                    status_text.text(p.get("msg", "Running backtest..."))
                    if "done" in p and "total" in p: 
                        pct = int(100 * p["done"] / p["total"])
                        progress_bar.progress(min(max(pct, 0), 100))
             
                        grid, setups = run_backtest(conn, symbol, spec, progress_cb=update_ui)
                        st.session_state["results"] = (grid)
                        st.success("Backtest Completed!")

           

            except Exception as e:
                st.error(f"Backtest failed: {str(e)}")
            finally:
                #This always runs even if there is an error, ensuring the UI resets properly
                st.session_state["is_backtest_running"] = False
                progress_bar.empty() #Clear the progress bar
                status_text.empty()
"""        # Show progress if running
        progress_bar = None
        if st.session_state["is_backtest_running"]:
            st.info(st.session_state.get("progress_msg", "Running backtest..."))
            progress_bar = st.progress(0)
            done = st.session_state.get("progress_done", 0)
            total = st.session_state.get("progress_total", 0)
            if total and total > 0:
                pct = int(100 * done / total)
                progress_bar.progress(min(max(pct, 0), 100))

        # Run the backtest only once per nonce
        if (st.session_state["is_backtest_running"] and st.session_state["run_nonce_completed"] != st.session_state["run_nonce"]
        ):

            def progress_cb(p):
                msg = p.get("msg", "")
                st.session_state["progress_msg"] = msg
                if "done" in p and "total" in p and p["total"] > 0:
                    st.session_state["progress_done"] = int(p["done"])
                    st.session_state["progress_total"] = int(p["total"])

            try:
                with st.spinner("Running backtest..."):
                    grid, setups = run_backtest(
                        conn=conn,
                        symbol=symbol,
                        spec=spec,
                        max_setups=max_setups,
                        progress_cb=progress_cb
                    )

                st.session_state["last_grid"] = grid
                st.session_state["last_setups"] = setups
                st.session_state["run_nonce_completed"] = st.session_state["run_nonce"]

                #st.success(f"Backtest complete. Setups used: {len(setups)}")
                st.session_state["last_run_success"] = f"Backtest complete. Setups used: {len(setups)}"
"""

"""
            except Exception as e:
                #st.error(f"Backtest failed: {e}") 
                st.session_state["last_run_error"] = f"Backtest failed: {e}"

            finally:
                st.session_state["is_backtest_running"] = False
                #st.session_state["progress_msg"] = ""
                #st.session_state["progress_done"] = 0
                #st.session_state["progress_total"] = 0
                st.session_state["run_nonce"] += 1
   """                 
"""
            # Move “identical columns” into the title (don’t change DB)
            # We’ll detect columns where all rows have the same value (e.g., symbol, strategy, entry_tf, trail_tf, maybe setups) and:
            # show them as a header block
            # drop them from the displayed table only
            import pandas as pd
            import numpy as np

            def tf_to_minutes(tf: str) -> int | None:
                ert '1m','5m','15m','30m','1H','2H','4H','1D','1W','1M' to minutes."""
"""          if tf is None:
                    return None
                tf = str(tf).strip()
                m = tf.lower()
                if m.endswith("m") and m[:-1].isdigit():
                    return int(m[:-1])
                if m.endswith("h") and m[:-1].isdigit():
                    return int(m[:-1]) * 60
                if m == "1d":
                    return 1440
                if m == "1w":
                    return 10080
                if m == "1m":  # month is ambiguous; you likely mean Monthly timeframe
                    # If you store Monthly as "1M" already, handle below:
                    return 43200  # 30 days approx (only for display; not used for intraday)
                if tf == "1M":  # Monthly
                    return 43200
                return None

            def split_constant_columns(df: pd.DataFrame, exclude: set[str] | None = None):
                Return (constants_dict, df_without_constant_cols).
                exclude = exclude or set()
                constants = {}
                keep_cols = []
                for c in df.columns:
                    if c in exclude:
                        keep_cols.append(c)
                        continue
                    s = df[c]
                    # treat NaN-only as non-constant (keep it)
                    non_na = s.dropna()
                    if len(non_na) > 0 and non_na.nunique() == 1:
                        constants[c] = non_na.iloc[0]
                    else:
                        keep_cols.append(c)
                return constants, df[keep_cols].copy()

            def add_derived_time_columns(df: pd.DataFrame) -> pd.DataFrame:
                d = df.copy()

                # Entry TF minutes for conversions
                entry_tf = None
                if "entry_tf" in d.columns:
                    # after constant split, entry_tf might be gone; handle both cases
                    if d["entry_tf"].dropna().nunique() == 1:
                        entry_tf = str(d["entry_tf"].dropna().iloc[0])
                # We'll also accept entry_tf passed in via constants externally.
                return d
        
        if st.session_state.get("last_run_error"):
            st.error(st.session_state["last_run_error"])
        elif st.session_state.get("last_run_success"):
            st.success(st.session_state["last_run_success"])


        # ---- Persisted results display (always) ----
        if "last_setups" in st.session_state and "last_grid" in st.session_state:
            st.markdown("### Setups found (first N)")
            st.dataframe(st.session_state["last_setups"], use_container_width=True)


            # cleaner title
            # row numbering starts at 1 for user-friendliness (not 0)


            grid = st.session_state["last_grid"].copy()

            # Detect constant columns (same value for every row)
            constant_cols = [c for c in grid.columns if grid[c].nunique() == 1]

            constants = {c: grid[c].iloc[0] for c in constant_cols}

            # Remove constant columns from display table
            display_df = grid.drop(columns=constant_cols)

            # Create user-friendly header
            header_parts = []
            for k, v in constants.items():
                header_parts.append(f"{k}: {v}")

            header_text = " | ".join(header_parts)

            st.markdown("### Results (sorted by Profit%)")
            st.caption(header_text)

            # Add ranking column starting at 1
            display_df = display_df.reset_index(drop=True)
            display_df.insert(0, "Rank", display_df.index + 1)

            # headers will look like normal English and hovering over them
            # will explain exactly what they mean (no more guessing R% vs Win Rate% vs Target Hit% etc). We want to show the most important metrics in the table and hide less important ones in the tooltip to avoid overwhelming users with info. The tooltip also allows us to explain complex metrics without cluttering the display.

            import numpy as np
            import streamlit as st

            # ---------------- Derived time columns ----------------
            def tf_to_minutes(tf: str) -> int | None:
                tf = str(tf).strip()
                t = tf.lower()
                if t.endswith("m") and t[:-1].isdigit():
                    return int(t[:-1])
                if t.endswith("h") and t[:-1].isdigit():
                    return int(t[:-1]) * 60
                if t == "1d":
                    return 1440
                if t == "1w":
                    return 10080
                if tf == "1M":  # monthly (approx)
                    return 43200
                return None

            entry_tf = str(constants.get("entry_tf", "")).strip()
            tf_min = tf_to_minutes(entry_tf) if entry_tf else None
            bars_per_day = (1440 / tf_min) if tf_min and tf_min > 0 else None

            # Add Max Hold Days (for numeric bars)
            if bars_per_day and "max_hold_bars" in display_df.columns:
                def _max_hold_days(v):
                    if isinstance(v, str) and v.lower() == "hold":
                        return np.nan
                    try:
                        return float(v) / bars_per_day
                    except:
                        return np.nan
                display_df["max_hold_days"] = display_df["max_hold_bars"].apply(_max_hold_days)

            # Add Avg Days Held
            if bars_per_day and "avg_bars_held" in display_df.columns:
                display_df["avg_days_held"] = display_df["avg_bars_held"].apply(lambda x: (float(x) / bars_per_day) if float(x) > 0 else np.nan)

            # Profit% per Bar
            if "profit_%" in display_df.columns and "avg_bars_held" in display_df.columns:
                display_df["profit_%_per_bar"] = display_df.apply(
                    lambda r: (float(r["profit_%"]) / float(r["avg_bars_held"])) if float(r["avg_bars_held"]) > 0 else np.nan,
                    axis=1
                )

            # Profit% per Day
            if "profit_%" in display_df.columns and "avg_days_held" in display_df.columns:
                display_df["profit_%_per_day"] = display_df.apply(
                    lambda r: (float(r["profit_%"]) / float(r["avg_days_held"])) if float(r["avg_days_held"]) > 0 else np.nan,
                    axis=1
                )

            # ---------------- Friendly names + tooltips ----------------
            LABELS = {
                "stop_hit_count": "Stops Hit (#)",
                "stop_hit_%": "Stops Hit (%)",
                "target_hit_count": "Targets Hit (#)",
                "target_hit_%": "Targets Hit (%)",
                "time_exit_count": "Time Exits (#)",
                "time_exit_%": "Time Exits (%)",
                "be_touch_R": "BE Move (R)",
                "target_R": "Target (R)",
                "max_hold_bars": "Max Hold (Bars)",
                "max_hold_days": "Max Hold (Days)",
                "setups": "Trades Tested (#)",
                "profit_%": "Profit (%)",
                "win_rate_%": "Win Rate (%)",
                "win_rate_MOE95_%": "Win Rate MOE (95%)",
                "avg_bars_held": "Avg Hold (Bars)",
                "avg_days_held": "Avg Hold (Days)",
                "profit_%_per_bar": "Profit% / Bar",
                "profit_%_per_day": "Profit% / Day",
                "max_DD_%": "Max Drawdown (%)",
            }

            HELP = {
                "Stops Hit (#)": "Number of trades that exited via stop-loss (touch execution).",
                "Stops Hit (%)": "Percent of trades that exited via stop-loss (touch execution).",
                "Targets Hit (#)": "Number of trades that hit the profit target (touch execution).",
                "Targets Hit (%)": "Percent of trades that hit the profit target (touch execution).",
                "Time Exits (#)": "Trades closed because the max-hold limit was reached (neither stop nor target hit first).",
                "Time Exits (%)": "Percent of trades closed by max-hold time exit.",
                "BE Move (R)": "Break-even move threshold. When MFE touches this R level, stop is moved to entry. 'None' means no BE move.",
                "Target (R)": "Profit target measured in R (initial risk units). Example: 2R = profit = 2 × initial risk.",
                "Max Hold (Bars)": "Maximum holding time in entry-timeframe bars. 'Hold' means no forced time exit.",
                "Max Hold (Days)": "Max hold converted from bars to days using the entry timeframe (e.g., 1H bars ÷ 24).",
                "Trades Tested (#)": "How many trades (setups) were included in this run for this config.",
                "Profit (%)": "Total compounded percent return on equity for this configuration across all tested trades.",
                "Win Rate (%)": "Percent of trades that ended positive (target hit or profitable time exit).",
                "Win Rate MOE (95%)": "95% margin of error for Win Rate. Smaller is better; improves with more trades tested.",
                "Avg Hold (Bars)": "Average number of entry-timeframe bars the trade was held before exit.",
                "Avg Hold (Days)": "Avg Hold converted to days based on entry timeframe.",
                "Profit% / Bar": "Profit (%) divided by Avg Hold (Bars). Higher = more profit per unit time (bars).",
                "Profit% / Day": "Profit (%) divided by Avg Hold (Days). Higher = more profit per unit time (days).",
                "Max Drawdown (%)": "Worst peak-to-trough equity decline during the run (equity curve drawdown).",
            }

            # Rename columns for display only
            display_df_ui = display_df.rename(columns=LABELS)

            # Build column_config tooltips
            colcfg = {}
            for col in display_df_ui.columns:
                if col in HELP:
                    if col.endswith("(%)") or col.endswith("Profit (%)") or col.endswith("Win Rate (%)") or col.endswith("Max Drawdown (%)"):
                        colcfg[col] = st.column_config.NumberColumn(help=HELP[col], format="%.2f")
                    elif col.endswith("(#)"):
                        colcfg[col] = st.column_config.NumberColumn(help=HELP[col], format="%.0f")
                    else:
                        colcfg[col] = st.column_config.NumberColumn(help=HELP[col])

            # Show table

            # ---------------- Filters (fast decision-making) ----------------
            st.markdown("### Filters")

            fcol1, fcol2, fcol3 = st.columns(3)

            with fcol1:
                min_trades = st.number_input(
                    "Min trades tested",
                    min_value=1,
                    value=50,
                    step=10,
                    help="Hide configs tested on too few trades. Higher = more reliable."
                )

            with fcol2:
                max_dd_cap = st.number_input(
                    "Max drawdown cap (%)",
                    min_value=0.0,
                    value=50.0,
                    step=1.0,
                    help="Only show configs with Max Drawdown <= this value."
                )

            with fcol3:
                min_profit_day = st.number_input(
                    "Min Profit% / Day",
                    min_value=-1000.0,
                    value=0.0,
                    step=0.1,
                    help="Only show configs with Profit%/Day >= this value."
                )

            filtered = display_df_ui.copy()

            # Apply filters safely (only if columns exist)
            if "Trades Tested (#)" in filtered.columns:
                filtered = filtered[filtered["Trades Tested (#)"] >= float(min_trades)]

            if "Max Drawdown (%)" in filtered.columns:
                filtered = filtered[filtered["Max Drawdown (%)"] <= float(max_dd_cap)]

            if "Profit% / Day" in filtered.columns:
                filtered = filtered[filtered["Profit% / Day"] >= float(min_profit_day)]

            st.caption(f"Rows after filters: {len(filtered):,} of {len(display_df_ui):,}")


            # ---------------- Winner summary (Rank 1 after filters) ----------------
            st.markdown("### Winner summary (Rank 1)")

            if len(filtered) == 0:
                st.warning("No rows match the current filters.")
            else:
                winner = filtered.iloc[0].to_dict()

                w1, w2, w3, w4 = st.columns(4)

                w1.metric("Profit (%)", f"{winner.get('Profit (%)', '—'):.2f}" if isinstance(winner.get("Profit (%)"), (int,float)) else str(winner.get("Profit (%)")))
                w2.metric("Profit% / Day", f"{winner.get('Profit% / Day', '—'):.4f}" if isinstance(winner.get("Profit% / Day"), (int,float)) else str(winner.get("Profit% / Day")))
                w3.metric("Win Rate (%)", f"{winner.get('Win Rate (%)', '—'):.2f}" if isinstance(winner.get("Win Rate (%)"), (int,float)) else str(winner.get("Win Rate (%)")))
                w4.metric("Max DD (%)", f"{winner.get('Max Drawdown (%)', '—'):.2f}" if isinstance(winner.get("Max Drawdown (%)"), (int,float)) else str(winner.get("Max Drawdown (%)")))

                st.caption(
                    f"Target: {winner.get('Target (R)', '—')}R | "
                    f"BE: {winner.get('BE Move (R)', '—')} | "
                    f"Max Hold: {winner.get('Max Hold (Bars)', '—')} bars"
                    + (f" ({winner.get('Max Hold (Days)', '—'):.2f} days)" if isinstance(winner.get("Max Hold (Days)"), (int,float)) else "")
                )

            # Now the table respects your filters and the “winner” is the filtered Rank 1.
            st.dataframe(filtered.head(50), use_container_width=True, column_config=colcfg)




            st.download_button(
                "Download filtered results as CSV",
                data=filtered.to_csv(index=False).encode("utf-8"),
                file_name=f"{symbol}_BearRally_results_spec{spec_id}_filtered.csv",
                mime="text/csv"
        )

            st.markdown("## Backtest History")

            # Filters
            col1, col2 = st.columns(2)
            with col1:
                hist_symbol = st.selectbox("Filter by symbol", ["(All)", "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD"], index=0)
            with col2:
                hist_spec = st.selectbox("Filter by spec", ["(All)"] + [f"{r[0]} — {r[1]}" for r in specs], index=0)

            where = []
            params = []

            if hist_symbol != "(All)":
                where.append("symbol=?")
                params.append(hist_symbol)

            if hist_spec != "(All)":
                hist_spec_id = int(hist_spec.split("—")[0].strip())
                where.append("spec_id=?")
                params.append(hist_spec_id)

            where_sql = ("WHERE " + " AND ".join(where)) if where else ""

            rows = conn.execute(f
                SELECT run_id, created_at_utc, spec_name, symbol, entry_tf, trail_tf, max_setups
                FROM backtest_runs
                {where_sql}
                ORDER BY run_id DESC
                LIMIT 200
            , params).fetchall()

            import pandas as pd
            if rows:
                df_runs = pd.DataFrame(rows, columns=["run_id","created_at_utc","spec_name","symbol","entry_tf","trail_tf","max_setups"])
                st.dataframe(df_runs, use_container_width=True)

                run_id = st.selectbox("View run_id", df_runs["run_id"].tolist())
                one = conn.execute(, (run_id,)).fetchone()

                if one:
                    results_df = pd.read_json(one[0])
                    setups_df = pd.read_json(one[1]) if one[1] else pd.DataFrame()
                    st.markdown("### Saved Results (top 50)")
                    st.dataframe(results_df.head(50), use_container_width=True)
                    st.markdown("### Saved Setups (top 50)")
                    st.dataframe(setups_df.head(50), use_container_width=True)

                    st.download_button(
                        "Download this run (results) as CSV",
                        data=results_df.to_csv(index=False).encode("utf-8"),
                        file_name=f"backtest_run_{run_id}_results.csv",
                        mime="text/csv"
                    )

                    if st.button("Delete this run"):
                        conn.execute("DELETE FROM backtest_runs WHERE run_id=?", (run_id,))
                        conn.commit()
                        st.success("Deleted run.")
                        st.rerun()
            else:
                st.info("No backtest runs saved yet.")
"""
                
                
with tab6:
    st.title("ForexLab Help")

    #This will render the screenshots you already placed in forexlab/assets/help/.
    import os

    st.markdown("### Built-in walkthrough screenshots (saved in repo)")
    st.caption("These images are loaded from: assets/help/ (relative to app.py).")

    help_dir = os.path.join("assets", "help")
    st.code(f"Looking for screenshots in: {os.path.abspath(help_dir)}")

    walkthrough = [
        ("1) Upload Market Data", "upload_market_data.png",
        "Upload MT5 CSV files and import them into SQLite."),

        ("2) Database Overview — Recent Imports + Strategy Files", "Database-Recent-imports-and-strategy-files.png",
        "Confirm what data is in the database (symbols, timeframes, ranges) and review strategy uploads."),

        ("3) Database Overview — Data File Uploads", "Database-Data-file-uploads.png",
        "See the upload/import log and confirm row counts and date ranges."),

        ("4) Strategy Viewer", "Strategy-Viewer.png",
        "Load an existing spec, edit the JSON, and save a new version (v1/v2/v3…)."),

        ("5) Upload Strategy Sheet", "upload_strategy_sheet.png",
        "Upload a strategy PDF/DOCX to extract text and store it."),

        ("6) Backtesting Tab", "Backtesting-tab.png",
        "Select a spec + symbol, count setups, run the backtest, and review results."),

        ("7) Help Tab Screenshot", "help-screenshot.png",
        "Optional: the help tab screenshot you captured."),
    ]

    for title, filename, caption in walkthrough:
        st.markdown(f"#### {title}")
        path = os.path.join(help_dir, filename)
        if os.path.exists(path):
            st.image(path, use_container_width=True)
            st.caption(caption)
        else:
            st.warning(f"Missing screenshot file: {path}")




    # This gives you an immediate “drop screenshots here” experience.
    # How you use it
    #Take screenshots (Win+Shift+S)
    #Upload them once
    #They show in the Help tab

    #This is a temporary quick-fix to get image in through the app and can be
    # deleted later once we have the screenshots in the codebase. It’s not ideal but it’s a nice-to-have for now to avoid needing to hardcode image files while we iterate on the Help content.
    #     
    st.markdown("## Screenshots (optional)")
    st.caption("Upload screenshots to display them in this Help page. These are stored only for this session unless you save them as files in your project.")

    help_imgs = st.file_uploader(
        "Upload Help screenshots (PNG/JPG)",
        type=["png", "jpg", "jpeg"],
        accept_multiple_files=True,
        key="help_screenshots_uploader"
    )

    if help_imgs:
        st.session_state["help_screenshots"] = help_imgs

    imgs = st.session_state.get("help_screenshots", [])
    if imgs:
        for i, img in enumerate(imgs, start=1):
            st.markdown(f"### Screenshot {i}")
            st.image(img, use_container_width=True)


# This is for storing static screenshots in the codebase (e.g., assets/help/01_upload_data.png) and displaying them in the Help tab without needing to upload each time. You can replace these with your actual screenshots.
# This is for rendering static screenshots stored in the codebase (e.g., assets/help/01_upload_data.png) in the Help tab without needing to upload each time. You can replace these with your actual screenshots.
# This will render screenshots from your local project folder and also work in GitHub.
import os

st.markdown("## Help walkthrough (with screenshots)")

help_dir = os.path.join("assets", "help")
files = [
    ("1) Upload MT5 Data", "upload_market_data.png", "Go to Upload Data and import your MT5 CSV files into SQLite."),
    ("2) Strategy Viewer", "Strategy-Viewer.png", "Upload a strategy PDF/DOCX, extract text, and save a versioned spec (v1/v2/v3...)."),
    ("3) Backtest", "03_backtest.png", "Choose spec + symbol, count setups, run backtest, review winner + download results."),
    ("4) Backtest History", "Backtesting-tab.png", "View saved backtest runs, filter by symbol/spec, and download past results.")
]

for title, filename, caption in files:
    st.markdown(f"### {title}")
    path = os.path.join(help_dir, filename)
    if os.path.exists(path):
        st.image(path, use_container_width=True)
        st.caption(caption)
    else:
        st.warning(f"Missing screenshot file: {path}")

# This section is for me personally so that I can refer to it and share it with others as a quick-start guide. It’s not meant to be a polished user manual, but more of a “here’s how to get going and what to expect” overview. I can expand it over time as needed.

    st.markdown("""
### What this app does
ForexLab lets you:
- Import MT5 historical bar data into SQLite (multiple symbols + timeframes)
- Store strategy PDFs/DOCX and versioned **strategy specs**
- Detect strategy **setups** from bar data (entry criteria only)
- Backtest **position management configurations** on those setups and compare results

---

## Typical workflow
1) **Upload Data**: Import MT5 CSV files (EURUSD_M15..., EURUSD_H1..., etc.)
2) **Strategy Viewer**: Upload a strategy PDF/DOCX → extract text → create/save a spec (v1, v2, v3...)
3) **Backtest**: Choose a spec + symbol → count setups → run backtest → review winner + download results

---

## Key definitions
### Setup
A **setup** is a valid entry opportunity found by the strategy’s entry criteria.
It does **not** depend on trade outcome.

> Setup count is based on entry criteria only (no trade management).  
> It also requires a valid initial stop per the spec.

### R (Reward-to-Risk unit)
**R** = (Stop − Entry) for shorts (or Entry − Stop for longs).  
Targets like **2R** mean profit equal to 2 × the initial risk.

### Execution assumptions
- **Stops/targets execute on touch** (wick counts)
- If stop and target are both touched inside the same higher-timeframe candle:
  - resolve using **1m**, then **5m**, else **stop-first** (conservative)

---

## Backtest results: what “Simulating configs …” means
A **config** is one combination of trade management settings applied to the same set of setups:

- **BE rule**: when to move stop to breakeven (in R)
  - example: None, 0.25R, 0.5R, 0.75R, 1.0R
- **Target_R**: profit target multiple
  - example: 0.5R, 1R, 1.5R, 2R, 3R, 5R
- **Max Hold (bars)**: time exit horizon (force exit if stop/target not hit)
  - example: Hold, 24, 48, 72, ...

Total configs = **(#BE levels) × (#targets) × (#hold options)**

Example:
- BE(5) × Targets(6) × Holds(8) = 240 configs

When you see “Simulating configs 70/240”, it means:
> We’ve completed 70 different management configurations out of 240 total.

---

## Common metrics in the results grid
- **profit_%**: percent change in account equity over the run (compounded)
- **win_rate_%**: percent of trades with positive R (includes time-exit winners)
- **win_rate_MOE95_%**: 95% margin-of-error for win rate (small samples = wider MOE)
- **avg_bars_held**: average duration in bars
- **max_DD_%**: maximum peak-to-trough drawdown of equity curve
- **stop_hit_% / target_hit_% / time_exit_%**: explicit exit breakdown (if enabled)

---

## Tips for better statistical confidence
- 20 setups is small → results can vary a lot.
- 100–200 setups is better.
- 500+ setups is strong when available.

---

### Troubleshooting
- If results don’t match the selected spec name, ensure spec_id/spec_name is passed into the engine and displayed from the spec label.
- If you change symbol/spec and see old results, clear cached session results on selection change.
""")
conn.close()