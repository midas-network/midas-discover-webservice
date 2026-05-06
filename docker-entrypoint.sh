#!/usr/bin/env sh
set -eu

DB_PATH="${MIDAS_DB_PATH:-midasDB}"
BOOTSTRAP_DIR="${MIDAS_BOOTSTRAP_INPUT_DIR:-midas-api-data}"

has_required_tables() {
python - "$DB_PATH" <<'PY'
import sqlite3
import sys

db_path = sys.argv[1]
try:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='pdetails'")
    row = cur.fetchone()
    conn.close()
    raise SystemExit(0 if row else 1)
except Exception:
    raise SystemExit(1)
PY
}

if [ ! -f "$DB_PATH" ] || ! has_required_tables; then
  if [ ! -f "$BOOTSTRAP_DIR/PAPERS_ENDPOINT.json" ]; then
    echo "Error: missing bootstrap data at $BOOTSTRAP_DIR. Expected PAPERS_ENDPOINT.json." >&2
    exit 1
  fi
  echo "Bootstrapping webservice database from $BOOTSTRAP_DIR..."
  python data_updates/update_database.py
fi

exec flask run --host=0.0.0.0 --port=8080
