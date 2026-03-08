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
    from backtest_engine import run_bear_rally_backtest

  
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

        from backtest_engine import count_bear_rally_setups_all

        # ---- Count button ----
        if st.button("Count all Bear Rally setups in full dataset window"):
            try:
                cnt, s_utc, e_utc = count_bear_rally_setups_all(conn, symbol, spec)
                st.session_state["last_count"] = {
                    "cnt": cnt, "s": s_utc, "e": e_utc,
                    "symbol": symbol, "spec_id": spec_id
                }
            except Exception as e:
                st.error(f"Count failed: {e}")

        # ---- Persisted count display (always) ----
        lc = st.session_state.get("last_count")
        if lc and lc["symbol"] == symbol and lc["spec_id"] == spec_id:
            st.success(f"Total Bear Rally setups found: {lc['cnt']}")
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

        # ---- Run button ----
        if "is_backtest_running" not in st.session_state:
            st.session_state["is_backtest_running"] = False
        if st.button("Run Bear Rally backtest now", disabled=st.session_state["is_backtest_running"]):
            st.session_state["is_backtest_running"] = True
            st.session_state["last_run_status"] = "Starting backtest..."
            
            status = st.empty()
            progress_bar = st.progress(0)
            status_text = st.empty()

            def progress_cb(p):
                msg = p.get("msg", "")
                status_text.info(msg)
                if "done" in p and "total" in p and p["total"] > 0:
                    pct = int(100 * p["done"] / p["total"])
                    progress_bar.progress(min(max(pct, 0), 100))

            try:
                with st.spinner("Running backtest..."):
                    status.info(st.session_state["last_run_status"])
            
                    grid, setups = run_bear_rally_backtest(
                        conn=conn,
                        symbol=symbol,
                        spec=spec,
                        max_setups=max_setups,
                        progress_cb=progress_cb
                    )

                st.session_state["last_grid"] = grid
                st.session_state["last_setups"] = setups
                st.success(f"Backtest complete. Setups used: {len(setups)}")

            except Exception as e:
                st.error(f"Backtest failed: {e}")

            finally:
                st.session_state["is_backtest_running"] = False
                st.status.empty()
                status_text.empty()
                progress_bar.empty()
                
            import datetime, json

            created_at = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            winner = grid.iloc[0].to_dict() if len(grid) else {}

            conn.execute(
                """
                INSERT INTO backtest_runs(
                created_at_utc,
                strategy_file_id,
                spec_id,
                spec_name,
                symbol,
                entry_tf,
                trail_tf,
                max_setups,
                starting_equity,
                risk_pct,
                results_json,
                setups_json,
                winner_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                created_at,
                spec.get("source_strategy_file_id"),
                spec.get("_spec_id", None),
                spec.get("_spec_name", ""),
                symbol,
                spec["strategy_fields"]["entry_timeframe"],
                spec["strategy_fields"]["trail_timeframe"],
                int(max_setups),
                float(4000.0),
                float(0.01),
                grid.to_json(orient="records"),
                setups.to_json(orient="records"),
                json.dumps(winner),
                )
            )
            conn.commit()
            st.success("Backtest saved to history.")
            progress_bar.empty()
            status_text.empty()
            

        # ---- Persisted results display (always) ----
        if "last_setups" in st.session_state and "last_grid" in st.session_state:
            st.markdown("### Setups found (first N)")
            st.dataframe(st.session_state["last_setups"], use_container_width=True)

            st.markdown("### Results (sorted by Profit%) — winner is row 1")
            st.dataframe(st.session_state["last_grid"].head(50), use_container_width=True)

            st.download_button(
                "Download full results as CSV",
                data=st.session_state["last_grid"].to_csv(index=False).encode("utf-8"),
                file_name=f"{symbol}_BearRally_results_spec{spec_id}.csv",
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

            rows = conn.execute(f"""
                SELECT run_id, created_at_utc, spec_name, symbol, entry_tf, trail_tf, max_setups
                FROM backtest_runs
                {where_sql}
                ORDER BY run_id DESC
                LIMIT 200
            """, params).fetchall()

            import pandas as pd
            if rows:
                df_runs = pd.DataFrame(rows, columns=["run_id","created_at_utc","spec_name","symbol","entry_tf","trail_tf","max_setups"])
                st.dataframe(df_runs, use_container_width=True)

                run_id = st.selectbox("View run_id", df_runs["run_id"].tolist())
                one = conn.execute("""
                    SELECT results_json, setups_json, winner_json
                    FROM backtest_runs
                    WHERE run_id=?
                """, (run_id,)).fetchone()

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
conn.close()