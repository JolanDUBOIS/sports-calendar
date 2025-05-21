from pathlib import Path

import json

import pandas as pd
import streamlit as st # type: ignore
from st_aggrid import AgGrid, GridOptionsBuilder # type: ignore


DEBUG_LOGS_DIR = Path(__file__).parent.parent / "logs"

def read_log_file(file_path: Path) -> pd.DataFrame:
    with file_path.open(mode='r') as f:
        logs = [json.loads(line) for line in f]
    return pd.DataFrame(logs)

def main():
    st.set_page_config(layout="wide")

    if 'refresh' not in st.session_state:
        st.session_state['refresh'] = False
    
    log_files = list(DEBUG_LOGS_DIR.glob("*.json"))

    if not log_files:
        st.error("No log files found.")
    else:
        tabs = st.tabs([file.stem for file in log_files])
        
        for i, file in enumerate(log_files):
            with tabs[i]:
                try:
                    df = read_log_file(file)
                    df = df.reset_index()
                    df = df.sort_values("index", ascending=False)

                    gb = GridOptionsBuilder.from_dataframe(df)
                    gb.configure_default_column(resizable=True, filter=True, sortable=True)
                    gb.configure_grid_options(enableCellTextSelection=True)
                    go = gb.build()

                    st.subheader(f"Logs from: {file.name}")
                    AgGrid(
                        df,
                        gridOptions=go,
                        height=600,
                        fit_columns_on_grid_load=True,
                        enable_enterprise_modules=False,
                    )

                    if st.button("Refresh logs"):
                        st.session_state['refresh'] = not st.session_state['refresh']

                except Exception as e:
                    st.error(f"Error reading {file.name}: {e}")

if __name__ == "__main__":
    main()
