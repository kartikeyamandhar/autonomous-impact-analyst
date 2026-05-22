"""Pure-function tests for risk scoring and action planning (no services)."""

from datetime import datetime

import pytest

from src.agent.action_planner import plan_actions
from src.agent.risk_scorer import aggregate_risk, score_node
from src.anomaly_detection.anomaly_events import AnomalyEvent, AnomalyType, Severity

pytestmark = pytest.mark.phase_5

WEIGHTS = {"test_coverage": 0.3, "fan_out": 0.25, "exposure_distance": 0.25, "materialization": 0.2}
THRESHOLDS = {"low": 0.30, "medium": 0.60, "high": 0.85}


def _event(atype: AnomalyType, column: str | None = None) -> AnomalyEvent:
    return AnomalyEvent(
        anomaly_type=atype,
        severity=Severity.ERROR,
        source_node_id="source.autonomous_impact_analyst.coingecko.coingecko_coins_markets",
        source_column=column,
        description="test",
        previous_value=None,
        current_value=None,
        detected_at=datetime.utcnow(),
        metadata={},
    )


def test_score_node_matches_spec_example() -> None:
    score = score_node(0.0, 4, 1, "table", "high", "type_changed", WEIGHTS)
    assert abs(score - 0.7315) < 1e-9


def test_score_clamped_to_one() -> None:
    score = score_node(0.0, 100, 0, "view", None, "column_removed", WEIGHTS)
    assert score == 1.0


def test_column_added_is_low_risk() -> None:
    score = score_node(1.0, 0, None, "table", None, "column_added", WEIGHTS)
    assert score < 0.1


def test_aggregate_buckets() -> None:
    assert aggregate_risk({}, THRESHOLDS) == "low"
    assert aggregate_risk({"a": 0.1}, THRESHOLDS) == "low"
    assert aggregate_risk({"a": 0.45}, THRESHOLDS) == "medium"
    assert aggregate_risk({"a": 0.7}, THRESHOLDS) == "high"
    assert aggregate_risk({"a": 0.9}, THRESHOLDS) == "critical"


def test_plan_actions_tiers(settings: dict) -> None:
    ev = _event(AnomalyType.TYPE_CHANGED, "current_price")
    assert [a.action_type for a in plan_actions("low", ev, [], [], settings)] == ["log"]
    assert [a.action_type for a in plan_actions("medium", ev, [], [], settings)] == [
        "log",
        "slack_alert",
    ]
    high = [a.action_type for a in plan_actions("high", ev, [], [], settings)]
    assert high == ["log", "slack_alert", "github_pr"]  # schema change + enabled


def test_plan_actions_no_pr_for_non_schema(settings: dict) -> None:
    ev = _event(AnomalyType.ROW_COUNT_DROP)
    high = [a.action_type for a in plan_actions("high", ev, [], [], settings)]
    assert "github_pr" not in high


def test_plan_actions_priority_order(settings: dict) -> None:
    ev = _event(AnomalyType.TYPE_CHANGED, "current_price")
    actions = plan_actions("high", ev, [], [], settings)
    assert [a.priority for a in actions] == sorted(a.priority for a in actions)
