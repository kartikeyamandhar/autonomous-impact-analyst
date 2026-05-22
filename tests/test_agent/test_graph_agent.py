"""Agent pipeline tests with mocked GraphQueries and a mocked Claude client."""

from datetime import datetime

import pytest

from src.agent.graph_agent import run_agent
from src.agent.incident_store import IncidentStore
from src.anomaly_detection.anomaly_events import AnomalyEvent, AnomalyType, Severity

pytestmark = pytest.mark.phase_5


@pytest.fixture
def store(tmp_path) -> IncidentStore:
    return IncidentStore(str(tmp_path / "incidents"))

PKG = "autonomous_impact_analyst"
SRC = f"source.{PKG}.coingecko.coingecko_coins_markets"
STG = f"model.{PKG}.stg_coingecko__coins_markets"
INT = f"model.{PKG}.int_token_profiles"
FCT = f"model.{PKG}.fct_daily_token_metrics"
EXP = f"exposure.{PKG}.defi_market_slack_bot"


class _TextBlock:
    def __init__(self, text: str) -> None:
        self.type = "text"
        self.text = text


class _Resp:
    def __init__(self, text: str) -> None:
        self.content = [_TextBlock(text)]


class FakeClient:
    def __init__(self, text: str) -> None:
        self._text = text
        self.messages = self

    def create(self, **kwargs):
        return _Resp(self._text)


class FakeQueries:
    """Mimics GraphQueries for a current_price anomaly on coins_markets."""

    driver = None

    _META = {
        SRC: {"unique_id": SRC, "name": "coingecko_coins_markets", "layer": None,
              "materialization": None, "resource_type": "source", "compiled_sql": None},
        STG: {"unique_id": STG, "name": "stg_coingecko__coins_markets", "layer": "staging",
              "materialization": "view", "resource_type": "model",
              "compiled_sql": "select id as coin_id, current_price from raw.t"},
        INT: {"unique_id": INT, "name": "int_token_profiles", "layer": "intermediate",
              "materialization": "view", "resource_type": "model", "compiled_sql": "select 1"},
        FCT: {"unique_id": FCT, "name": "fct_daily_token_metrics", "layer": "marts",
              "materialization": "table", "resource_type": "model", "compiled_sql": "select 1"},
    }

    def node_metadata(self, node_id):
        return self._META.get(node_id)

    def column_lineage_forward(self, model_id, column):
        return [
            {"path": [{"model": SRC, "column": "current_price"},
                      {"model": STG, "column": "current_price_usd"},
                      {"model": INT, "column": "current_price_usd"},
                      {"model": FCT, "column": "current_price_usd"}]},
        ]

    def downstream_models(self, model_id):
        return [{"unique_id": STG, "name": "x", "layer": "staging"}]

    def paths_to_exposures(self, node_id):
        return [[{"unique_id": FCT, "name": "fct_daily_token_metrics"},
                 {"unique_id": EXP, "name": "defi_market_slack_bot"}]]

    def test_coverage(self, node_id):
        return {"total_columns": 10, "tested_columns": 1, "coverage_ratio": 0.1, "tests": []}

    def fan_out(self, node_id):
        return 2

    def distance_to_nearest_exposure(self, node_id):
        return 1


def _event(atype, column="current_price"):
    return AnomalyEvent(
        anomaly_type=atype, severity=Severity.ERROR, source_node_id=SRC,
        source_column=column, description="desc", previous_value="DECIMAL",
        current_value="STRING", detected_at=datetime.utcnow(), metadata={},
    )


def test_agent_traverses_and_scores(settings, store):
    state = run_agent(
        _event(AnomalyType.NULL_RATIO_SPIKE), None, FakeClient("a summary"),
        settings, queries=FakeQueries(), incident_store=store,
    )
    assert len(state.affected_paths) == 1
    assert state.affected_paths[0] == [SRC, STG, INT, FCT]
    assert {e["name"] for e in state.affected_exposures} == {"defi_market_slack_bot"}
    assert state.overall_risk in ("medium", "high", "critical")
    assert state.risk_scores
    assert state.impact_summary == "a summary"


def test_agent_slack_payload_filled(settings, store):
    state = run_agent(
        _event(AnomalyType.NULL_RATIO_SPIKE), None, FakeClient("the summary"),
        settings, queries=FakeQueries(), incident_store=store,
    )
    slack = [a for a in state.recommended_actions if a.action_type == "slack_alert"]
    assert slack and slack[0].payload["summary"] == "the summary"


def test_agent_schema_change_generates_fix(settings, store):
    state = run_agent(
        _event(AnomalyType.TYPE_CHANGED),
        None,
        FakeClient("SELECT id AS coin_id, try_cast(current_price AS decimal(38,8)) FROM raw.t"),
        settings,
        queries=FakeQueries(), incident_store=store,
    )
    action_types = [a.action_type for a in state.recommended_actions]
    assert "github_pr" in action_types
    assert state.fix_suggestion is not None
    pr = [a for a in state.recommended_actions if a.action_type == "github_pr"][0]
    assert pr.payload["fix_sql"] == state.fix_suggestion


def test_agent_invalid_fix_drops_pr(settings, store):
    state = run_agent(
        _event(AnomalyType.TYPE_CHANGED),
        None,
        FakeClient("this is not valid sql !!!"),
        settings,
        queries=FakeQueries(), incident_store=store,
    )
    assert state.fix_suggestion is None
    assert "github_pr" not in [a.action_type for a in state.recommended_actions]


def test_agent_aborts_on_unknown_node(settings, store):
    class Empty(FakeQueries):
        def node_metadata(self, node_id):
            return None

    state = run_agent(
        _event(AnomalyType.NULL_RATIO_SPIKE), None, FakeClient("x"),
        settings, queries=Empty(), incident_store=store,
    )
    assert state.affected_paths == []
    assert state.overall_risk == "low"
