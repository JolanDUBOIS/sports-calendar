#!/bin/bash

cd "$(dirname "$0")"
poetry run streamlit run log_viewer.py &
sleep 1
cmd.exe /C "start chrome --new-window http://localhost:8501"
