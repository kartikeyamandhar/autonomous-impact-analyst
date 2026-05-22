"""Tests for the Phase 5 enhancements: modifiers, incident memory, correlation,
approval gate, and a deterministic eval harness over the structured summary."""

from datetime import datetime, timedelta

import pytest

from src.agent.action_planner import plan_actions
from src.agent.correlator import correlate_events, primary_event
from src.agent.graph_agent import run_agent
from src.agent.incident_store import IncidentStore, incident_key
from src.agent.risk_scorer import apply_modifiers
from src.anomaly_detection.anomaly_events import AnomalyEvent, AnomalyType, Severity

from .test_graph_agent import SRC, FakeClient, FakeQueries, _event

pytestmark = pytest.mark.phase_5


@pytest.fixture
def store(tmp_path) -> IncidentStore:
    return IncidentStore(str(tmp_path / "incidents"))


# -- risk modifiers (#3/#4/#8) ------------------------------------------------


def test_severity_scales_score() -> None:
    base = 0.5
    crit = apply_modifiers(base, severity="critical")
    info = apply_modifiers(base, severity="info")
    assert crit > base > info


def test_low_confidence_dampens_gently_not_halves() -> None:
    base = 0.8
    full = apply_modifiers(base, confidence=1.0)
    low = apply_modifiers(base, confidence=0.5)
    assert low < full
    assert low / full > 0.8  # gentle, not a 50% cut


def test_distance_decay_reduces_far_nodes() -> None:
    near = apply_modifiers(0.6, distance_from_source=0)
    far = apply_modifiers(0.6, distance_from_source=5)
    assert far < near


def test_high_priority_exposure_boosts() -> None:
    plain = apply_modifiers(0.5)
    boosted = apply_modifiers(0.5, reaches_high_priority_exposure=True)
    assert boosted > plain


def test_modifier_clamped_to_one() -> None:
    assert apply_modifiers(0.95, severity="critical", reaches_high_priority_exposure=True) <= 1.0


# -- incident memory + dedup (#7/#13) -----------------------------------------


def test_incident_key_stable() -> None:
    e1 = _event(AnomalyType.TYPE_CHANGED)
    e2 = _event(AnomalyType.TYPE_CHANGED)
    assert incident_key(e1) == incident_key(e2)
    assert incident_key(_event(AnomalyType.COLUMN_REMOVED)) != incident_key(e1)


def test_incident_store_counts_and_dedups(store: IncidentStore) -> None:
    ev = _event(AnomalyType.TYPE_CHANGED)
    key = incident_key(ev)
    assert store.prior_occurrences(key) == 0
    assert store.is_duplicate(key, 60) is False
    store.record(key, ev, "high")
    assert store.prior_occurrences(key) == 1
    assert store.is_duplicate(key, 60) is True


def test_agent_records_incident(settings, store: IncidentStore) -> None:
    run_agent(_event(AnomalyType.NULL_RATIO_SPIKE), None, FakeClient("s"),
              settings, queries=FakeQueries(), incident_store=store)
    key = incident_key(_event(AnomalyType.NULL_RATIO_SPIKE))
    assert store.prior_occurrences(key) == 1


# -- correlation (#11) --------------------------------------------------------


def _ev_at(atype, sev, dt, column=None):
    return AnomalyEvent(
        anomaly_type=atype, severity=sev, source_node_id=SRC, source_column=column,
        description="d", previous_value=None, current_value=None,
        detected_at=dt, metadata={},
    )


def test_correlate_groups_same_source_in_window() -> None:
    now = datetime.utcnow()
    events = [
        _ev_at(AnomalyType.TYPE_CHANGED, Severity.ERROR, now, "current_price"),
        _ev_at(AnomalyType.NULL_RATIO_SPIKE, Severity.WARNING, now + timedelta(minutes=2)),
        _ev_at(AnomalyType.COLUMN_REMOVED, Severity.CRITICAL, now + timedelta(minutes=1), "roi"),
    ]
    incidents = correlate_events(events, window_minutes=15)
    assert len(incidents) == 1
    # primary = highest severity (critical)
    assert primary_event(incidents[0]).severity == Severity.CRITICAL


def test_correlate_splits_outside_window() -> None:
    now = datetime.utcnow()
    events = [
        _ev_at(AnomalyType.TYPE_CHANGED, Severity.ERROR, now, "current_price"),
        _ev_at(AnomalyType.NULL_RATIO_SPIKE, Severity.WARNING, now + timedelta(hours=2)),
    ]
    assert len(correlate_events(events, window_minutes=15)) == 2


# -- approval gate (#10) ------------------------------------------------------


def test_approval_gate_marks_pending(settings) -> None:
    cfg = {**settings, "agent": {**settings["agent"], "require_approval": True}}
    ev = _event(AnomalyType.TYPE_CHANGED)
    actions = plan_actions("critical", ev, [], [], cfg)
    pr = [a for a in actions if a.action_type == "github_pr"]
    assert pr and pr[0].payload.get("status") == "pending_approval"


# -- eval harness (#16) -------------------------------------------------------


def test_eval_blast_radius_and_actions(settings, store: IncidentStore) -> None:
    """Golden expectations on the deterministic structured summary."""
    state = run_agent(
        _event(AnomalyType.TYPE_CHANGED),
        None,
        FakeClient("SELECT id AS coin_id, try_cast(current_price AS decimal(38,8)) FROM raw.t"),
        settings, queries=FakeQueries(), incident_store=store,
    )
    p = state.summary_payload
    assert p["event"]["type"] == "type_changed"
    assert p["affected_path_count"] == 1
    assert set(p["recommended_actions"]) >= {"log", "slack_alert"}
    assert p["overall_risk"] in ("high", "critical")
    assert [t["node"] for t in p["trace"]][:2] == ["receive_event", "traverse_forward"]
    # every affected node with <0.5 coverage is reported as a gap
    assert all(v < 0.5 for v in p["coverage_gaps"].values())
