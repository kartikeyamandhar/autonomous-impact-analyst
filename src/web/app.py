"""FastAPI backend for the interactive demo.

Serves the lineage graph and drives the live loop:
  break  -> mutate the warehouse, run the agent, post Slack + open a PR, light
            up the blast radius
  approve-> merge the PR, rebuild the marts, flip the graph back to healthy

Single-user local demo: steps stream over SSE so the UI shows progress.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

load_dotenv()

import yaml  # type: ignore[import-untyped]  # noqa: E402

from src.actions.dbt_runner import DbtRunner  # noqa: E402
from src.actions.github_pr import GitHubPRCreator  # noqa: E402
from src.actions.slack_notifier import SlackNotifier  # noqa: E402
from src.agent.graph_agent import run_agent  # noqa: E402
from src.anomaly_detection.anomaly_events import (  # noqa: E402
    AnomalyEvent,
    AnomalyType,
    Severity,
)

_STATIC = Path(__file__).parent / "static"
DBT_DIR = "src/dbt_project"

# Source column -> (mart table, mart column) so a "break" degrades the bot too.
MART_COLUMN_MAP = {
    "current_price": ("fct_daily_token_metrics", "current_price_usd"),
    "total_volume": ("fct_daily_token_metrics", "total_volume_usd"),
    "market_cap": ("fct_daily_token_metrics", "market_cap_usd"),
}

app = FastAPI(title="Autonomous Impact Analyst")

# --- lazily-built singletons -------------------------------------------------
_state: dict[str, Any] = {"driver": None, "client": None, "config": None}
INCIDENT: dict[str, Any] = {
    "active": False, "root": None, "column": None, "broken": [],
    "pr_url": None, "pr_number": None, "risk": None, "summary": None,
}


def _config() -> dict:
    if _state["config"] is None:
        with open("config/settings.yml") as f:
            _state["config"] = yaml.safe_load(f)
    return _state["config"]


def _driver() -> Any:
    if _state["driver"] is None:
        from neo4j import GraphDatabase

        _state["driver"] = GraphDatabase.driver(
            os.environ["NEO4J_URI"],
            auth=(os.environ["NEO4J_USER"], os.environ["NEO4J_PASSWORD"]),
        )
    return _state["driver"]


def _client() -> Any:
    if _state["client"] is None:
        from anthropic import Anthropic

        _state["client"] = Anthropic()
    return _state["client"]


def _databricks() -> Any:
    from databricks import sql

    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def _sse(payload: dict) -> str:
    return f"data: {json.dumps(payload)}\n\n"


# --- graph -------------------------------------------------------------------


@app.get("/api/graph")
def graph() -> dict:
    cypher_nodes = (
        "MATCH (n) WHERE n:Model OR n:Source OR n:Exposure "
        "RETURN n.unique_id AS id, labels(n)[0] AS label, n.name AS name, "
        "n.layer AS layer, n.priority AS priority"
    )
    cypher_dep = (
        "MATCH (a)-[:DEPENDS_ON]->(b) "
        "WHERE (a:Model OR a:Source) AND (b:Model OR b:Source) "
        "RETURN a.unique_id AS downstream, b.unique_id AS upstream"
    )
    cypher_consumes = (
        "MATCH (e:Exposure)-[:CONSUMES]->(m) "
        "RETURN m.unique_id AS mart, e.unique_id AS exposure"
    )
    broken = set(INCIDENT["broken"])
    nodes, edges = [], []
    with _driver().session() as s:
        for r in s.run(cypher_nodes):
            nid = r["id"]
            status = "root" if nid == INCIDENT["root"] else (
                "broken" if nid in broken else "healthy"
            )
            nodes.append({
                "id": nid, "label": r["label"], "name": r["name"],
                "layer": r["layer"] or r["label"].lower(),
                "priority": r["priority"], "status": status,
            })
        for r in s.run(cypher_dep):
            edges.append({"source": r["upstream"], "target": r["downstream"]})
        for r in s.run(cypher_consumes):
            edges.append({"source": r["mart"], "target": r["exposure"]})
    return {"nodes": nodes, "edges": edges, "incident": _incident_public()}


def _incident_public() -> dict:
    return {k: INCIDENT[k] for k in ("active", "root", "column", "risk", "summary",
                                     "pr_url", "broken")}


# --- break -------------------------------------------------------------------


@app.get("/api/break")
def break_(node_id: str, column: str = "current_price") -> StreamingResponse:
    return StreamingResponse(_break_stream(node_id, column), media_type="text/event-stream")


def _break_stream(node_id: str, column: str) -> Iterator[str]:
    cfg = _config()
    try:
        # 1. Real warehouse mutation: drop the mapped mart column so the
        #    exposure bot genuinely degrades.
        if column in MART_COLUMN_MAP:
            table, mcol = MART_COLUMN_MAP[column]
            yield _sse({"type": "step", "name": "inject",
                        "msg": f"Dropping {mcol} from marts.{table} (warehouse mutation)"})
            from scripts.simulate_anomalies import simulate_column_drop

            conn = _databricks()
            try:
                simulate_column_drop(conn, f"marts.{table}", mcol)
            finally:
                conn.close()
        else:
            yield _sse({"type": "step", "name": "inject",
                        "msg": f"Simulating schema change on {column}"})

        # 2. Run the agent on a COLUMN_REMOVED event for the source column.
        yield _sse({"type": "step", "name": "analyze",
                    "msg": "Tracing lineage + scoring risk (LangGraph agent)…"})
        event = AnomalyEvent(
            anomaly_type=AnomalyType.COLUMN_REMOVED,
            severity=Severity.CRITICAL,
            source_node_id=node_id,
            source_column=column,
            description=f"Column '{column}' removed from {node_id.split('.')[-1]}",
            previous_value="present",
            current_value=None,
            detected_at=datetime.utcnow(),
            metadata={},
        )
        state = run_agent(event, _driver(), _client(), cfg)
        affected = sorted({n for path in state.affected_paths for n in path})
        exposures = [e["unique_id"] for e in state.affected_exposures]

        # 3. Open a (mergeable) PR with the agent's fix.
        pr_url = pr_number = None
        gh_action = next(
            (a for a in state.recommended_actions if a.action_type == "github_pr"), None
        )
        if state.fix_suggestion and gh_action:
            yield _sse({"type": "step", "name": "pr", "msg": "Opening GitHub fix PR…"})
            creator = GitHubPRCreator(os.environ["GITHUB_TOKEN"], os.environ["GITHUB_REPO"])
            pr_url = creator.create_fix_pr(
                event=event,
                fix_sql=state.fix_suggestion,
                model_file_path=gh_action.payload.get(
                    "model_path", "src/dbt_project/models/staging/"
                    "stg_coingecko__coins_markets.sql"),
                impact_summary=state.impact_summary,
                risk_level=state.overall_risk,
                draft=False,  # mergeable for the demo
            )
            pr_number = GitHubPRCreator.pr_number_from_url(pr_url)

        # 4. Post the Slack alert (linking the PR).
        yield _sse({"type": "step", "name": "alert", "msg": "Posting Slack impact alert…"})
        slack_ok = SlackNotifier(os.environ["SLACK_WEBHOOK_URL"]).send_impact_alert(
            state, pr_url=pr_url
        )

        # 5. Record incident → drives the graph colouring.
        INCIDENT.update({
            "active": True, "root": node_id, "column": column,
            "broken": sorted(set(affected) | set(exposures) | {node_id}),
            "pr_url": pr_url, "pr_number": pr_number,
            "risk": state.overall_risk, "summary": state.impact_summary,
        })
        yield _sse({
            "type": "done",
            "risk": state.overall_risk,
            "summary": state.impact_summary,
            "affected": INCIDENT["broken"],
            "exposures": [e["name"] for e in state.affected_exposures],
            "pruned": len(state.pruned_paths),
            "slack_sent": slack_ok,
            "pr_url": pr_url,
        })
    except Exception as e:  # noqa: BLE001
        yield _sse({"type": "error", "msg": str(e)})


# --- approve -----------------------------------------------------------------


@app.get("/api/approve")
def approve() -> StreamingResponse:
    return StreamingResponse(_approve_stream(), media_type="text/event-stream")


def _approve_stream() -> Iterator[str]:
    try:
        if not INCIDENT["active"]:
            yield _sse({"type": "error", "msg": "No active incident to approve."})
            return

        # Approve = accept the fix and heal, WITHOUT merging to main (no
        # auto-generated SQL is ever committed to the default branch).
        approved = None
        if INCIDENT["pr_number"]:
            yield _sse({"type": "step", "name": "approve",
                        "msg": f"Approving PR #{INCIDENT['pr_number']} (resolving, not merging)…"})
            creator = GitHubPRCreator(os.environ["GITHUB_TOKEN"], os.environ["GITHUB_REPO"])
            creator.close_pr(
                INCIDENT["pr_number"],
                comment="✅ Approved via Impact Analyst demo — resolving incident.",
            )
            approved = True

        yield _sse({"type": "step", "name": "rebuild",
                    "msg": "Rebuilding marts (dbt build) to restore the data…"})
        DbtRunner(DBT_DIR).trigger_build("autonomous_impact_analyst")

        yield _sse({"type": "step", "name": "resolve", "msg": "Resolving incident…"})
        INCIDENT.update({
            "active": False, "root": None, "column": None, "broken": [],
            "pr_url": None, "pr_number": None, "risk": None, "summary": None,
        })
        time.sleep(0.2)
        yield _sse({"type": "done", "approved": approved})
    except Exception as e:  # noqa: BLE001
        yield _sse({"type": "error", "msg": str(e)})


# --- reset (undo a break without approving) ----------------------------------


@app.get("/api/reset")
def reset() -> StreamingResponse:
    return StreamingResponse(_reset_stream(), media_type="text/event-stream")


def _reset_stream() -> Iterator[str]:
    try:
        if INCIDENT["pr_number"]:
            yield _sse({"type": "step", "name": "close",
                        "msg": f"Closing PR #{INCIDENT['pr_number']}…"})
            creator = GitHubPRCreator(os.environ["GITHUB_TOKEN"], os.environ["GITHUB_REPO"])
            creator.close_pr(INCIDENT["pr_number"], comment="Demo reset — closing.")
        yield _sse({"type": "step", "name": "rebuild",
                    "msg": "Rebuilding marts (dbt build)…"})
        DbtRunner(DBT_DIR).trigger_build("autonomous_impact_analyst")
        INCIDENT.update({
            "active": False, "root": None, "column": None, "broken": [],
            "pr_url": None, "pr_number": None, "risk": None, "summary": None,
        })
        yield _sse({"type": "done"})
    except Exception as e:  # noqa: BLE001
        yield _sse({"type": "error", "msg": str(e)})


# --- static ------------------------------------------------------------------


@app.get("/")
def index() -> FileResponse:
    return FileResponse(_STATIC / "index.html")


app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")
