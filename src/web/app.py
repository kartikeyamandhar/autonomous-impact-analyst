"""FastAPI backend for the stakeholder-facing demo.

Presents the system as a three-lane "data journey" — Sources → Data Products →
Dashboards — and drives the live loop:
  break  -> (optionally) mutate the warehouse, run the agent, post Slack + open
            a PR, and light up the affected products/dashboards
  approve-> resolve + close the PR, rebuild the marts, heal back to green
Steps stream over SSE so the UI shows progress. Approve never merges to main.
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
from src.common.identifiers import source_node_id  # noqa: E402

_STATIC = Path(__file__).parent / "static"
DBT_DIR = "src/dbt_project"
PKG = "autonomous_impact_analyst"

# Source column -> (mart table, mart column): a real warehouse mutation that
# also degrades the exposure bot. Other breaks are agent-only (still real Slack
# + PR), and recovery is always a marts rebuild.
MART_COLUMN_MAP = {
    "current_price": ("fct_daily_token_metrics", "current_price_usd"),
    "total_volume": ("fct_daily_token_metrics", "total_volume_usd"),
    "market_cap": ("fct_daily_token_metrics", "market_cap_usd"),
}

_SEVERITY = {
    "column_removed": Severity.CRITICAL,
    "type_changed": Severity.ERROR,
    "null_ratio_spike": Severity.WARNING,
    "row_count_drop": Severity.ERROR,
    "freshness_violation": Severity.WARNING,
}

# Stakeholder-friendly names.
SOURCE_NAMES = {
    "coingecko_coins_markets": "Coin Market Prices",
    "coingecko_coins_detail": "Coin Profiles",
    "coingecko_exchanges": "Exchanges",
    "defi_llama_protocols": "DeFi Protocols",
    "defi_llama_yields_pools": "Yield Pools",
    "etherscan_eth_transactions": "ETH Transactions",
    "etherscan_token_transfers": "Token Transfers",
}
PRODUCT_NAMES = {
    "fct_daily_token_metrics": "Daily Token Metrics",
    "fct_protocol_health": "Protocol Health",
    "fct_whale_movements": "Whale Movements",
    "dim_tokens": "Token Directory",
    "dim_protocols": "Protocol Directory",
    "dim_exchanges": "Exchange Directory",
}
DASHBOARD_NAMES = {
    "defi_market_slack_bot": "Market Summary Bot",
    "whale_alert_pipeline": "Whale Alert Pipeline",
}

# Per-source schema preview + plain-language "what can break here" options.
_C = lambda label, column, anomaly: {"label": label, "column": column, "anomaly": anomaly}  # noqa: E731
SOURCE_BREAKS: dict[str, dict] = {
    "coingecko_coins_markets": {
        "columns": [("current_price", "decimal"), ("market_cap", "decimal"),
                    ("total_volume", "decimal")],
        "options": [
            _C("Remove the price column", "current_price", "column_removed"),
            _C("Prices turn null / invalid", "current_price", "null_ratio_spike"),
            _C("Market-cap values turn invalid", "market_cap", "null_ratio_spike"),
            _C("Half the rows disappear", None, "row_count_drop"),
            _C("Data stops refreshing", None, "freshness_violation"),
        ],
    },
    "coingecko_coins_detail": {
        "columns": [("description", "string"), ("categories", "json"),
                    ("genesis_date", "date")],
        "options": [
            _C("Remove the categories field", "categories", "column_removed"),
            _C("Descriptions go missing", "description", "null_ratio_spike"),
            _C("Data stops refreshing", None, "freshness_violation"),
        ],
    },
    "coingecko_exchanges": {
        "columns": [("trust_score", "int"), ("trade_volume_24h_btc", "decimal")],
        "options": [
            _C("Remove the trust-score column", "trust_score", "column_removed"),
            _C("Volume values turn invalid", "trade_volume_24h_btc", "null_ratio_spike"),
        ],
    },
    "defi_llama_protocols": {
        "columns": [("tvl", "decimal"), ("category", "string"), ("chains", "json")],
        "options": [
            _C("Remove the TVL column", "tvl", "column_removed"),
            _C("Category labels go missing", "category", "null_ratio_spike"),
            _C("Half the rows disappear", None, "row_count_drop"),
        ],
    },
    "defi_llama_yields_pools": {
        "columns": [("apy", "decimal"), ("tvlUsd", "decimal")],
        "options": [
            _C("Remove the APY column", "apy", "column_removed"),
            _C("Yield values turn invalid", "apy", "null_ratio_spike"),
        ],
    },
    "etherscan_eth_transactions": {
        "columns": [("value", "string"), ("from", "string"), ("to", "string")],
        "options": [
            _C("Remove the value column", "value", "column_removed"),
            _C("Data stops refreshing", None, "freshness_violation"),
        ],
    },
    "etherscan_token_transfers": {
        "columns": [("value", "string"), ("tokenSymbol", "string"),
                    ("contractAddress", "string")],
        "options": [
            _C("Remove the token-symbol column", "tokenSymbol", "column_removed"),
            _C("Transfer values turn invalid", "value", "null_ratio_spike"),
        ],
    },
}

app = FastAPI(title="Autonomous Impact Analyst")

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


def _status(node_id: str) -> str:
    if node_id == INCIDENT["root"]:
        return "root"
    return "broken" if node_id in set(INCIDENT["broken"]) else "healthy"


# --- lanes -------------------------------------------------------------------


@app.get("/api/lanes")
def lanes() -> dict:
    sources = []
    for table, meta in SOURCE_BREAKS.items():
        nid = source_node_id(table)
        sources.append({
            "id": nid, "table": table, "name": SOURCE_NAMES[table],
            "columns": [{"name": c, "type": t} for c, t in meta["columns"]],
            "options": meta["options"], "status": _status(nid),
        })
    products = [
        {"id": f"model.{PKG}.{m}", "name": name, "status": _status(f"model.{PKG}.{m}")}
        for m, name in PRODUCT_NAMES.items()
    ]
    dashboards = [
        {"id": f"exposure.{PKG}.{e}", "name": name,
         "priority": "high" if e == "defi_market_slack_bot" else "medium",
         "status": _status(f"exposure.{PKG}.{e}")}
        for e, name in DASHBOARD_NAMES.items()
    ]
    return {"sources": sources, "products": products, "dashboards": dashboards,
            "incident": {k: INCIDENT[k] for k in
                         ("active", "root", "column", "risk", "summary", "pr_url", "broken")}}


# --- break -------------------------------------------------------------------


@app.get("/api/break")
def break_(node_id: str, column: str | None = None,
           anomaly: str = "column_removed") -> StreamingResponse:
    return StreamingResponse(_break_stream(node_id, column, anomaly),
                             media_type="text/event-stream")


def _break_stream(node_id: str, column: str | None, anomaly: str) -> Iterator[str]:
    cfg = _config()
    try:
        # 1. Real warehouse mutation when the column maps to a mart column.
        if anomaly == "column_removed" and column in MART_COLUMN_MAP:
            table, mcol = MART_COLUMN_MAP[column]
            yield _sse({"type": "step", "name": "inject",
                        "msg": f"Dropping {mcol} from marts.{table} (warehouse mutation)…"})
            from scripts.simulate_anomalies import simulate_column_drop

            conn = _databricks()
            try:
                simulate_column_drop(conn, f"marts.{table}", mcol)
            finally:
                conn.close()
        else:
            yield _sse({"type": "step", "name": "inject",
                        "msg": f"Simulating '{anomaly}' on {column or 'table'}…"})

        # 2. Run the agent.
        yield _sse({"type": "step", "name": "analyze",
                    "msg": "Tracing lineage + scoring risk (LangGraph agent)…"})
        atype = AnomalyType(anomaly)
        event = AnomalyEvent(
            anomaly_type=atype,
            severity=_SEVERITY.get(anomaly, Severity.ERROR),
            source_node_id=node_id,
            source_column=column,
            description=_describe(atype, column, node_id),
            previous_value="present" if anomaly == "column_removed" else None,
            current_value=None,
            detected_at=datetime.utcnow(),
            metadata={},
        )
        state = run_agent(event, _driver(), _client(), cfg)
        affected = sorted({n for path in state.affected_paths for n in path})
        exposures = [e["unique_id"] for e in state.affected_exposures]

        # 3. Open a (mergeable) PR with the agent's fix, if any.
        pr_url = pr_number = None
        gh_action = next(
            (a for a in state.recommended_actions if a.action_type == "github_pr"), None
        )
        if state.fix_suggestion and gh_action:
            yield _sse({"type": "step", "name": "pr", "msg": "Opening GitHub fix PR…"})
            creator = GitHubPRCreator(os.environ["GITHUB_TOKEN"], os.environ["GITHUB_REPO"])
            pr_url = creator.create_fix_pr(
                event=event, fix_sql=state.fix_suggestion,
                model_file_path=gh_action.payload.get(
                    "model_path", f"{DBT_DIR}/models/staging/stg_coingecko__coins_markets.sql"),
                impact_summary=state.impact_summary, risk_level=state.overall_risk, draft=False,
            )
            pr_number = GitHubPRCreator.pr_number_from_url(pr_url)

        # 4. Slack alert.
        yield _sse({"type": "step", "name": "alert", "msg": "Posting Slack impact alert…"})
        slack_ok = SlackNotifier(os.environ["SLACK_WEBHOOK_URL"]).send_impact_alert(
            state, pr_url=pr_url)

        # 5. Record incident → drives the lane colouring.
        INCIDENT.update({
            "active": True, "root": node_id, "column": column,
            "broken": sorted(set(affected) | set(exposures) | {node_id}),
            "pr_url": pr_url, "pr_number": pr_number,
            "risk": state.overall_risk, "summary": state.impact_summary,
        })
        yield _sse({
            "type": "done", "risk": state.overall_risk, "summary": state.impact_summary,
            "affected": INCIDENT["broken"],
            "exposures": [e["name"] for e in state.affected_exposures],
            "pruned": len(state.pruned_paths), "slack_sent": slack_ok, "pr_url": pr_url,
        })
    except Exception as e:  # noqa: BLE001
        yield _sse({"type": "error", "msg": str(e)})


def _describe(atype: AnomalyType, column: str | None, node_id: str) -> str:
    table = node_id.split(".")[-1]
    col = column or ""
    return {
        AnomalyType.COLUMN_REMOVED: f"Column '{col}' removed from {table}",
        AnomalyType.NULL_RATIO_SPIKE: f"Null ratio on '{col}' spiked in {table}",
        AnomalyType.ROW_COUNT_DROP: f"Row count dropped sharply in {table}",
        AnomalyType.FRESHNESS_VIOLATION: f"Source {table} is stale (stopped refreshing)",
        AnomalyType.TYPE_CHANGED: f"Column '{col}' changed type in {table}",
    }.get(atype, f"Anomaly on {table}")


# --- approve (resolve, never merge to main) ----------------------------------


@app.get("/api/approve")
def approve() -> StreamingResponse:
    return StreamingResponse(_resolve_stream(approve=True), media_type="text/event-stream")


@app.get("/api/reset")
def reset() -> StreamingResponse:
    return StreamingResponse(_resolve_stream(approve=False), media_type="text/event-stream")


def _resolve_stream(approve: bool) -> Iterator[str]:
    try:
        verb = "Approving" if approve else "Resetting"
        if INCIDENT["pr_number"]:
            yield _sse({"type": "step", "name": "pr",
                        "msg": f"{verb}: closing PR #{INCIDENT['pr_number']} "
                               f"(never merged to main)…"})
            creator = GitHubPRCreator(os.environ["GITHUB_TOKEN"], os.environ["GITHUB_REPO"])
            note = "✅ Approved via demo — resolving." if approve else "↩︎ Demo reset — closing."
            creator.close_pr(INCIDENT["pr_number"], comment=note)
        yield _sse({"type": "step", "name": "rebuild",
                    "msg": "Rebuilding data products (dbt build) to restore the data…"})
        DbtRunner(DBT_DIR).trigger_build("autonomous_impact_analyst")
        yield _sse({"type": "step", "name": "resolve", "msg": "Healing the data journey…"})
        INCIDENT.update({
            "active": False, "root": None, "column": None, "broken": [],
            "pr_url": None, "pr_number": None, "risk": None, "summary": None,
        })
        time.sleep(0.2)
        yield _sse({"type": "done"})
    except Exception as e:  # noqa: BLE001
        yield _sse({"type": "error", "msg": str(e)})


# --- static ------------------------------------------------------------------


@app.get("/")
def index() -> FileResponse:
    return FileResponse(_STATIC / "index.html")


app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")
