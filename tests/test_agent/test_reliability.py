"""Tests for the staff-eng hardening pass: idempotency, loud errors, fix
provenance, sigma detection, and atomic incident counting."""


import pytest

from src.agent.graph_agent import run_agent
from src.agent.incident_store import IncidentStore, incident_key
from src.anomaly_detection.anomaly_events import AnomalyType
from src.anomaly_detection.data_quality_monitor import DataQualityMonitor
from src.anomaly_detection.snapshot_store import QualityBaseline

from .test_graph_agent import FakeClient, FakeQueries, _event

pytestmark = pytest.mark.phase_5

KEY = "raw.coingecko_coins_markets"


@pytest.fixture
def store(tmp_path) -> IncidentStore:
    return IncidentStore(str(tmp_path / "incidents"))


# -- idempotency + run_id (#1, observability) ---------------------------------


def test_actions_carry_idempotency_keys(settings, store):
    state = run_agent(_event(AnomalyType.NULL_RATIO_SPIKE), None, FakeClient("s"),
                      settings, queries=FakeQueries(), incident_store=store)
    assert state.run_id
    for a in state.recommended_actions:
        assert a.payload["idempotency_key"].endswith(a.action_type)


# -- loud node-not-found (#3) -------------------------------------------------


def test_node_not_found_is_loud(settings, store):
    class Empty(FakeQueries):
        def node_metadata(self, node_id):
            return None

    state = run_agent(_event(AnomalyType.NULL_RATIO_SPIKE), None, FakeClient("s"),
                      settings, queries=Empty(), incident_store=store)
    assert state.errors and "node not found" in state.errors[0]


# -- LLM fix safety (#8) ------------------------------------------------------


def test_fix_requires_human_review(settings, store):
    state = run_agent(
        _event(AnomalyType.TYPE_CHANGED), None,
        FakeClient("SELECT id AS coin_id, try_cast(current_price AS decimal(38,8)) FROM raw.t"),
        settings, queries=FakeQueries(), incident_store=store,
    )
    pr = [a for a in state.recommended_actions if a.action_type == "github_pr"][0]
    assert pr.payload["requires_human_review"] is True
    assert pr.payload["fix_provenance"]["generated_by"] == "llm"
    assert pr.payload["fix_provenance"]["validated"] == "sqlglot_parse"


# -- atomic incident counting (#1) --------------------------------------------


def test_incident_count_is_monotonic(store):
    ev = _event(AnomalyType.TYPE_CHANGED)
    key = incident_key(ev)
    counts = [store.record(key, ev, "high") for _ in range(5)]
    assert counts == [1, 2, 3, 4, 5]


def test_outcome_feedback_and_summary(store):
    ev = _event(AnomalyType.TYPE_CHANGED)
    key = incident_key(ev)
    store.record(key, ev, "high")
    store.record_outcome(key, actionable=True)
    store.record_outcome(key, actionable=False)
    s = store.summary()
    assert s["incidents"] == 1
    assert s["feedback_count"] == 2
    assert s["actionable_rate"] == 0.5


# -- sigma value-range detection (#2) -----------------------------------------


def _baseline_with_stats(mean, std, count, cur_range) -> QualityBaseline:
    return QualityBaseline(
        row_counts={KEY: 100},
        null_ratios={KEY: {}},
        value_ranges={KEY: {"current_price": cur_range}},
        value_stats={KEY: {"current_price": {"mean": mean, "stddev": std, "count": count}}},
    )


def test_sigma_breach_detected(settings):
    cfg = {"value_range_breach_stddev": 3.0, "min_baseline_samples": 5}
    m = DataQualityMonitor(databricks_conn=None, config=cfg)
    # current max way outside the prior 3-sigma band [mean-3std, mean+3std]
    current = _baseline_with_stats(100.0, 10.0, 100, (95.0, 500.0))
    previous = _baseline_with_stats(100.0, 10.0, 100, (95.0, 105.0))
    m.compute_baseline = lambda tables: current  # type: ignore[method-assign]
    events = m.detect(["coingecko_coins_markets"], previous)
    breach = [e for e in events if e.anomaly_type == AnomalyType.VALUE_RANGE_BREACH]
    assert breach and breach[0].metadata["method"] == "sigma"


def test_sigma_skipped_below_min_samples(settings):
    cfg = {"value_range_breach_stddev": 3.0, "min_baseline_samples": 50}
    m = DataQualityMonitor(databricks_conn=None, config=cfg)
    current = _baseline_with_stats(100.0, 10.0, 10, (95.0, 500.0))   # count<min
    previous = _baseline_with_stats(100.0, 10.0, 10, (95.0, 105.0))
    m.compute_baseline = lambda tables: current  # type: ignore[method-assign]
    events = m.detect(["coingecko_coins_markets"], previous)
    assert not [e for e in events if e.anomaly_type == AnomalyType.VALUE_RANGE_BREACH]
