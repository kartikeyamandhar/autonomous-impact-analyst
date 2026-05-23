"""Dagster Software-Defined Assets wrapping each phase component into one
observable, scheduled pipeline.

Error policy (per spec): every asset try/excepts, logs via the Dagster context,
and returns a *degraded* result instead of raising — so a failure in one stage
lets downstream stages decide whether to proceed rather than crashing the run.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from dagster import asset
from dotenv import load_dotenv

load_dotenv()

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

DBT_DIR = "src/dbt_project"
MANIFEST = f"{DBT_DIR}/target/manifest.json"
CATALOG = f"{DBT_DIR}/target/catalog.json"
RUN_RESULTS = f"{DBT_DIR}/target/run_results.json"
WATCH_TABLES = [
    "coingecko_coins_markets",
    "coingecko_coins_detail",
    "coingecko_exchanges",
    "defi_llama_protocols",
    "defi_llama_yields_pools",
    "etherscan_eth_transactions",
    "etherscan_token_transfers",
]


def _config() -> dict:
    with open("config/settings.yml") as f:
        return yaml.safe_load(f)


def _databricks():
    from databricks import sql

    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def _neo4j():
    from neo4j import GraphDatabase

    return GraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=(os.environ["NEO4J_USER"], os.environ["NEO4J_PASSWORD"]),
    )


def _serialize_event(ev: Any) -> dict:
    return {
        "anomaly_type": getattr(ev.anomaly_type, "value", ev.anomaly_type),
        "severity": getattr(ev.severity, "value", ev.severity),
        "source_node_id": ev.source_node_id,
        "source_column": ev.source_column,
        "description": ev.description,
        "previous_value": ev.previous_value,
        "current_value": ev.current_value,
        "detected_at": ev.detected_at.isoformat(),
        "metadata": ev.metadata,
    }


def _deserialize_event(d: dict) -> Any:
    from src.anomaly_detection.anomaly_events import AnomalyEvent, AnomalyType, Severity

    return AnomalyEvent(
        anomaly_type=AnomalyType(d["anomaly_type"]),
        severity=Severity(d["severity"]),
        source_node_id=d["source_node_id"],
        source_column=d.get("source_column"),
        description=d["description"],
        previous_value=d.get("previous_value"),
        current_value=d.get("current_value"),
        detected_at=datetime.fromisoformat(d["detected_at"]),
        metadata=d.get("metadata", {}),
    )


def _serialize_state(state: Any) -> dict:
    return {
        "event": _serialize_event(state.event),
        "overall_risk": state.overall_risk,
        "affected_paths": state.affected_paths,
        "pruned_paths": state.pruned_paths,
        "affected_exposures": state.affected_exposures,
        "test_coverage_per_node": state.test_coverage_per_node,
        "risk_scores": state.risk_scores,
        "recommended_actions": [
            {"action_type": a.action_type, "payload": a.payload, "priority": a.priority}
            for a in state.recommended_actions
        ],
        "impact_summary": state.impact_summary,
        "fix_suggestion": state.fix_suggestion,
        "requires_approval": state.requires_approval,
        "prior_occurrences": state.prior_occurrences,
        "incident_key": state.incident_key,
        "run_id": state.run_id,
        "errors": state.errors,
    }


def _deserialize_state(d: dict) -> Any:
    from src.agent.types import AgentState, PlannedAction

    state = AgentState(event=_deserialize_event(d["event"]))
    state.overall_risk = d["overall_risk"]
    state.affected_paths = d["affected_paths"]
    state.pruned_paths = d["pruned_paths"]
    state.affected_exposures = d["affected_exposures"]
    state.test_coverage_per_node = d["test_coverage_per_node"]
    state.risk_scores = d["risk_scores"]
    state.recommended_actions = [
        PlannedAction(a["action_type"], a["payload"], a["priority"])
        for a in d["recommended_actions"]
    ]
    state.impact_summary = d["impact_summary"]
    state.fix_suggestion = d.get("fix_suggestion")
    state.requires_approval = d.get("requires_approval", False)
    state.prior_occurrences = d.get("prior_occurrences", 0)
    state.incident_key = d.get("incident_key", "")
    state.run_id = d.get("run_id", "")
    state.errors = d.get("errors", [])
    return state


# ---------------------------------------------------------------------------
# Assets
# ---------------------------------------------------------------------------


@asset(group_name="ingestion")
def raw_data_sync(context) -> dict:
    try:
        from scripts.seed_databricks import run_seed

        result = run_seed()
        context.log.info(f"raw_data_sync: {result}")
        return result
    except Exception as e:  # noqa: BLE001
        context.log.error(f"raw_data_sync failed: {e}")
        return {"status": "failed", "error": str(e)}


@asset(group_name="transformation")
def dbt_build(context, raw_data_sync: dict) -> dict:
    from src.actions.dbt_runner import DbtRunner

    runner = DbtRunner(DBT_DIR)
    if runner.is_paused():
        context.log.warning("dbt paused — skipping build")
        return {"status": "paused", "models_run": 0, "tests_passed": 0}
    try:
        results = runner.trigger_build("autonomous_impact_analyst")
        nodes = results.get("results", [])
        models = sum(
            1 for r in nodes
            if r.get("unique_id", "").startswith("model.") and r.get("status") == "success"
        )
        tests = sum(
            1 for r in nodes
            if r.get("unique_id", "").startswith("test.") and r.get("status") == "pass"
        )
        failures = [r["unique_id"] for r in nodes if r.get("status") in ("fail", "error")]
        status = "failed" if failures else "success"
        context.log.info(f"dbt_build: {status} models={models} tests={tests}")
        return {"status": status, "models_run": models, "tests_passed": tests,
                "failures": failures}
    except Exception as e:  # noqa: BLE001
        context.log.error(f"dbt_build failed: {e}")
        return {"status": "failed", "error": str(e), "models_run": 0, "tests_passed": 0}


@asset(group_name="transformation")
def dbt_artifacts(context, dbt_build: dict) -> dict:
    try:
        from src.actions.dbt_runner import DbtRunner

        manifest, catalog = DbtRunner(DBT_DIR).generate_artifacts()
        context.log.info("dbt_artifacts generated")
        return {"manifest": manifest, "catalog": catalog}
    except Exception as e:  # noqa: BLE001
        context.log.error(f"dbt_artifacts failed: {e}")
        return {"status": "failed", "error": str(e),
                "manifest": MANIFEST, "catalog": CATALOG}


@asset(group_name="graph")
def neo4j_graph(context, dbt_artifacts: dict) -> dict:
    try:
        from src.graph_engine.lineage_builder import extract_column_lineage
        from src.graph_engine.manifest_parser import parse_dbt_artifacts
        from src.graph_engine.neo4j_loader import load_graph

        graph = parse_dbt_artifacts(
            dbt_artifacts.get("manifest", MANIFEST),
            dbt_artifacts.get("catalog", CATALOG),
        )
        graph.column_lineage = extract_column_lineage(graph)
        result = load_graph(
            graph, os.environ["NEO4J_URI"],
            os.environ["NEO4J_USER"], os.environ["NEO4J_PASSWORD"],
        )
        context.log.info(f"neo4j_graph: {result}")
        return {"nodes": result["nodes"], "edges": result["relationships"]}
    except Exception as e:  # noqa: BLE001
        context.log.error(f"neo4j_graph failed: {e}")
        return {"status": "failed", "error": str(e), "nodes": 0, "edges": 0}


@asset(group_name="detection")
def anomaly_events(context, dbt_build: dict, neo4j_graph: dict) -> list[dict]:
    if dbt_build.get("status") in ("paused", "failed"):
        context.log.warning(f"skipping detection — dbt_build status={dbt_build.get('status')}")
        return []
    try:
        from src.anomaly_detection.data_quality_monitor import DataQualityMonitor
        from src.anomaly_detection.freshness_monitor import FreshnessMonitor
        from src.anomaly_detection.schema_monitor import SchemaMonitor
        from src.anomaly_detection.snapshot_store import SnapshotStore
        from src.anomaly_detection.test_failure_monitor import TestFailureMonitor

        config = _config()
        store = SnapshotStore()
        events: list[Any] = []
        conn = _databricks()
        try:
            schema_mon = SchemaMonitor(conn, WATCH_TABLES)
            events += schema_mon.detect(store.get_latest_schema())
            store.save_schema(schema_mon.get_current_schema())

            dq = DataQualityMonitor(conn, config["data_quality"])
            events += dq.detect(WATCH_TABLES, store.get_latest_quality())
            store.save_quality(dq.compute_baseline(WATCH_TABLES))
        finally:
            conn.close()

        events += FreshnessMonitor(DBT_DIR).detect()
        events += TestFailureMonitor().detect(RUN_RESULTS)

        context.log.info(f"anomaly_events: {len(events)} detected")
        return [_serialize_event(e) for e in events]
    except Exception as e:  # noqa: BLE001
        context.log.error(f"anomaly_events failed: {e}")
        return []


@asset(group_name="agent")
def impact_reports(context, anomaly_events: list[dict]) -> list[dict]:
    if not anomaly_events:
        context.log.info("no anomalies — no impact reports")
        return []
    try:
        import yaml as _yaml  # noqa: F401
        from anthropic import Anthropic

        from src.agent.graph_agent import run_agent

        config = _config()
        client = Anthropic()
        driver = _neo4j()
        reports: list[dict] = []
        try:
            for ev_dict in anomaly_events:
                event = _deserialize_event(ev_dict)
                state = run_agent(event, driver, client, config)
                reports.append(_serialize_state(state))
        finally:
            driver.close()
        context.log.info(f"impact_reports: {len(reports)} generated")
        return reports
    except Exception as e:  # noqa: BLE001
        context.log.error(f"impact_reports failed: {e}")
        return []


@asset(group_name="actions")
def executed_actions(context, impact_reports: list[dict]) -> list[dict]:
    if not impact_reports:
        context.log.info("no impact reports — no actions")
        return []
    try:
        from src.actions.executor import execute_actions

        config = _config()
        results: list[dict] = []
        for report in impact_reports:
            state = _deserialize_state(report)
            outcome = execute_actions(state, config)
            results.append({"run_id": state.run_id, "risk": state.overall_risk,
                            "outcome": outcome})
        context.log.info(f"executed_actions: {results}")
        return results
    except Exception as e:  # noqa: BLE001
        context.log.error(f"executed_actions failed: {e}")
        return [{"status": "failed", "error": str(e)}]
