#!/usr/bin/env bash
#
# scripts/demo.sh — the end-to-end portfolio demo.
#
# Shows the full loop: build the warehouse + graph, post a healthy Slack
# summary, inject an anomaly, let the agent trace impact and alert, show the
# exposure bot degrade, then roll back and recover. Watch the Slack channel.
#
set -euo pipefail

cd "$(dirname "$0")/.."

# venv + secrets
# shellcheck disable=SC1091
source venv/bin/activate
set -a
# shellcheck disable=SC1091
source .env
set +a

PY=python
hr() { printf '\n\033[1;36m=== %s ===\033[0m\n' "$*"; }

hr "Autonomous Impact Analyst Demo"

# 1. Verify services -----------------------------------------------------------
hr "Step 1: Checking services (Neo4j + Databricks)"
$PY - <<'PY'
import os
from neo4j import GraphDatabase
from databricks import sql
d = GraphDatabase.driver(os.environ["NEO4J_URI"],
                         auth=(os.environ["NEO4J_USER"], os.environ["NEO4J_PASSWORD"]))
d.verify_connectivity(); d.close(); print("  Neo4j OK")
c = sql.connect(server_hostname=os.environ["DATABRICKS_HOST"],
                http_path=os.environ["DATABRICKS_HTTP_PATH"],
                access_token=os.environ["DATABRICKS_TOKEN"])
cur = c.cursor(); cur.execute("SELECT 1"); cur.fetchone(); c.close(); print("  Databricks OK")
PY

# 2. Baseline build ------------------------------------------------------------
hr "Step 2: Baseline dbt build + artifacts"
make dbt-run
make dbt-docs

# 3. Load graph ----------------------------------------------------------------
hr "Step 3: Loading knowledge graph into Neo4j"
make graph-load

# 4. Healthy exposure summary --------------------------------------------------
hr "Step 4: Posting HEALTHY market summary to Slack"
$PY -m src.exposure_bot.slack_bot --once

# 5. Inject anomaly ------------------------------------------------------------
hr "Step 5: Injecting anomaly (null spike on current_price)"
$PY scripts/simulate_anomalies.py --action null_injection \
    --table raw.coingecko_coins_markets --column current_price --pct 0.5

# 6. Run agent + act -----------------------------------------------------------
hr "Step 6: Running impact agent (traces lineage, scores risk, alerts Slack)"
$PY -m src.agent.graph_agent --execute

# 7. Degrade the exposure bot --------------------------------------------------
hr "Step 7: Breaking a mart column the bot reads -> DEGRADED summary"
$PY scripts/simulate_anomalies.py --action column_drop \
    --table marts.fct_daily_token_metrics --column current_price_usd
$PY -m src.exposure_bot.slack_bot --once

# 8. Rollback ------------------------------------------------------------------
hr "Step 8: Rolling back (re-seed raw tables)"
$PY scripts/simulate_anomalies.py --action rollback

# 9. Recover -------------------------------------------------------------------
hr "Step 9: Rebuilding + verifying recovery"
make dbt-run
$PY -m src.exposure_bot.slack_bot --once

hr "Demo complete — check #all-impact-analysis for: healthy → alert → degraded → recovered"
