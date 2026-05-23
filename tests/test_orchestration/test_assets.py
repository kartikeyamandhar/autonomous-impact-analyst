"""Phase 7 orchestration tests: asset control-flow (pause/skip), serializers,
and that the Dagster Definitions load with the right wiring. No live services —
the short-circuit paths are exercised so no asset reaches Databricks/Neo4j."""

from datetime import datetime

import pytest
from dagster import build_asset_context

from src.orchestration import assets as A
from src.orchestration.dagster_definitions import defs

pytestmark = pytest.mark.phase_7


# -- serializers round-trip ---------------------------------------------------


def _event():
    from src.anomaly_detection.anomaly_events import AnomalyEvent, AnomalyType, Severity

    return AnomalyEvent(
        AnomalyType.TYPE_CHANGED, Severity.ERROR,
        "source.autonomous_impact_analyst.coingecko.coingecko_coins_markets",
        "current_price", "desc", "DECIMAL", "STRING", datetime.utcnow(), {"k": "v"},
    )


def test_event_serde_roundtrip():
    ev = _event()
    back = A._deserialize_event(A._serialize_event(ev))
    assert back.source_node_id == ev.source_node_id
    assert back.anomaly_type == ev.anomaly_type
    assert back.severity == ev.severity
    assert back.metadata == {"k": "v"}


def test_state_serde_roundtrip():
    from src.agent.types import AgentState, PlannedAction

    state = AgentState(event=_event(), overall_risk="high",
                       recommended_actions=[PlannedAction("slack_alert", {"x": 1}, 1)],
                       impact_summary="s", run_id="r1")
    back = A._deserialize_state(A._serialize_state(state))
    assert back.overall_risk == "high"
    assert back.recommended_actions[0].action_type == "slack_alert"
    assert back.recommended_actions[0].payload == {"x": 1}
    assert back.event.source_column == "current_price"


# -- asset control-flow (no external calls) -----------------------------------


def test_dbt_build_respects_pause_lock(tmp_path, monkeypatch):
    import src.actions.dbt_runner as runner_mod

    lock = tmp_path / "dbt_pause.lock"
    lock.write_text("{}")
    monkeypatch.setattr(runner_mod, "_LOCK_PATH", lock)
    out = A.dbt_build(build_asset_context(), {"tables_synced": 7})
    assert out["status"] == "paused"


def test_anomaly_events_skipped_when_dbt_failed():
    out = A.anomaly_events(build_asset_context(), {"status": "failed"}, {"nodes": 1})
    assert out == []


def test_anomaly_events_skipped_when_paused():
    out = A.anomaly_events(build_asset_context(), {"status": "paused"}, {"nodes": 1})
    assert out == []


def test_impact_reports_empty_passthrough():
    assert A.impact_reports(build_asset_context(), []) == []


def test_executed_actions_empty_passthrough():
    assert A.executed_actions(build_asset_context(), []) == []


# -- definitions wiring -------------------------------------------------------


def test_definitions_load_jobs_and_schedule():
    assert defs.resolve_job_def("full_monitoring_cycle")
    assert defs.resolve_job_def("graph_refresh")
    assert defs.resolve_job_def("detection_only")
    assert defs.resolve_schedule_def("monitoring_schedule").cron_schedule == "*/15 * * * *"


def test_all_seven_assets_present():
    keys = {k.to_user_string() for k in defs.resolve_asset_graph().get_all_asset_keys()}
    expected = {
        "raw_data_sync", "dbt_build", "dbt_artifacts", "neo4j_graph",
        "anomaly_events", "impact_reports", "executed_actions",
    }
    assert expected <= keys
