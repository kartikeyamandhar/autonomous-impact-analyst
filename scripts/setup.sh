#!/usr/bin/env bash
# scripts/setup.sh
# One-shot environment bootstrap. Idempotent.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# ---- Helpers ---------------------------------------------------------------

log()  { printf '\033[1;36m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!!\033[0m %s\n' "$*" >&2; }
fail() { printf '\033[1;31mxx\033[0m %s\n' "$*" >&2; exit 1; }

# ---- 1. Python version ------------------------------------------------------

log "Checking Python version (>= 3.11)"
if ! command -v python3 >/dev/null 2>&1; then
  fail "python3 not found on PATH"
fi
PY_OK=$(python3 -c 'import sys; print(1 if sys.version_info >= (3, 11) else 0)')
if [ "$PY_OK" != "1" ]; then
  fail "Python 3.11+ required, found $(python3 --version)"
fi
log "Python OK: $(python3 --version)"

# ---- 2. Virtual environment -------------------------------------------------

if [ ! -d "venv" ]; then
  log "Creating virtual environment at ./venv"
  python3 -m venv venv
else
  log "Reusing existing ./venv"
fi

# shellcheck disable=SC1091
source venv/bin/activate

log "Upgrading pip"
python -m pip install --upgrade pip >/dev/null

# ---- 3. Install dev dependencies -------------------------------------------

log "Installing requirements/dev.txt"
pip install -r requirements/dev.txt

# ---- 4. .env validation -----------------------------------------------------

if [ ! -f ".env" ]; then
  warn ".env not found. Copy .env.example to .env and fill in values before continuing."
else
  log "Checking .env for empty values"
  EMPTY=$(grep -E '^[A-Z_]+=$' .env || true)
  if [ -n "$EMPTY" ]; then
    warn "The following .env variables are empty:"
    printf '   %s\n' $EMPTY
  else
    log ".env populated"
  fi
fi

# ---- 5. Neo4j connectivity --------------------------------------------------

log "Testing Neo4j connectivity"
python - <<'PY' || warn "Neo4j connectivity check failed (see error above)"
import os
from dotenv import load_dotenv
load_dotenv()
from neo4j import GraphDatabase

uri = os.environ.get("NEO4J_URI")
user = os.environ.get("NEO4J_USER")
pw = os.environ.get("NEO4J_PASSWORD")
if not (uri and user and pw):
    raise SystemExit("Neo4j env vars missing")
d = GraphDatabase.driver(uri, auth=(user, pw))
d.verify_connectivity()
print("Neo4j OK")
d.close()
PY

# ---- 6. Databricks connectivity --------------------------------------------

log "Testing Databricks connectivity"
python - <<'PY' || warn "Databricks connectivity check failed (see error above)"
import os
from dotenv import load_dotenv
load_dotenv()
from databricks import sql

host = os.environ.get("DATABRICKS_HOST")
path = os.environ.get("DATABRICKS_HTTP_PATH")
tok  = os.environ.get("DATABRICKS_TOKEN")
if not (host and path and tok):
    raise SystemExit("Databricks env vars missing")
conn = sql.connect(server_hostname=host, http_path=path, access_token=tok)
cur = conn.cursor()
cur.execute("SELECT 1")
print("Databricks OK:", cur.fetchone())
cur.close()
conn.close()
PY

# ---- 7. dbt deps ------------------------------------------------------------

log "Running dbt deps"
(
  cd src/dbt_project
  DBT_PROFILES_DIR="$PWD" dbt deps
) || warn "dbt deps failed (continuing)"

log "Setup complete."
echo
echo "Next steps:"
echo "  source venv/bin/activate"
echo "  make test-phase-0"
