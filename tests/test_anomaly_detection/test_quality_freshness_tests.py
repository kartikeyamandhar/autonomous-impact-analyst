"""Data-quality diff, freshness parse, test-failure parse, snapshot round-trip."""

from datetime import datetime

import pytest

from src.anomaly_detection.anomaly_events import AnomalyType, Severity
from src.anomaly_detection.data_quality_monitor import DataQualityMonitor
from src.anomaly_detection.freshness_monitor import FreshnessMonitor
from src.anomaly_detection.snapshot_store import (
    ColumnSnapshot,
    QualityBaseline,
    SchemaSnapshot,
    SnapshotStore,
)
from src.anomaly_detection.test_failure_monitor import TestFailureMonitor

pytestmark = pytest.mark.phase_4

CONFIG = {"row_count_drop_ratio": 0.20, "null_ratio_spike_delta": 0.10}
KEY = "raw.coingecko_coins_markets"


def _baseline(rows: int, null_ratio: float, vmin: float) -> QualityBaseline:
    return QualityBaseline(
        row_counts={KEY: rows},
        null_ratios={KEY: {"current_price": null_ratio}},
        value_ranges={KEY: {"current_price": (vmin, 100.0)}},
        captured_at=datetime.utcnow(),
    )


def _monitor(current: QualityBaseline) -> DataQualityMonitor:
    m = DataQualityMonitor(databricks_conn=None, config=CONFIG)
    m.compute_baseline = lambda tables: current  # type: ignore[method-assign]
    return m


def test_quality_first_run_empty() -> None:
    m = _monitor(_baseline(100, 0.0, 1.0))
    assert m.detect([KEY], None) == []


def test_row_count_drop_detected() -> None:
    m = _monitor(_baseline(50, 0.0, 1.0))
    events = m.detect(["coingecko_coins_markets"], _baseline(100, 0.0, 1.0))
    assert any(e.anomaly_type == AnomalyType.ROW_COUNT_DROP for e in events)


def test_null_spike_detected() -> None:
    m = _monitor(_baseline(100, 0.40, 1.0))
    events = m.detect(["coingecko_coins_markets"], _baseline(100, 0.05, 1.0))
    spike = [e for e in events if e.anomaly_type == AnomalyType.NULL_RATIO_SPIKE]
    assert spike and spike[0].source_column == "current_price"


def test_value_range_breach_detected() -> None:
    m = _monitor(_baseline(100, 0.0, -5.0))  # min now negative
    events = m.detect(["coingecko_coins_markets"], _baseline(100, 0.0, 1.0))
    assert any(e.anomaly_type == AnomalyType.VALUE_RANGE_BREACH for e in events)


def test_no_event_when_stable() -> None:
    m = _monitor(_baseline(100, 0.05, 1.0))
    assert m.detect(["coingecko_coins_markets"], _baseline(100, 0.05, 1.0)) == []


def test_freshness_parse() -> None:
    sources = {
        "results": [
            {"unique_id": "source.x.a", "status": "pass", "max_loaded_at": "t"},
            {"unique_id": "source.x.b", "status": "warn", "max_loaded_at": "t",
             "criteria": {}},
            {"unique_id": "source.x.c", "status": "error", "max_loaded_at": "t",
             "criteria": {}},
        ]
    }
    events = FreshnessMonitor("src/dbt_project").parse(sources)
    assert len(events) == 2
    sev = {e.source_node_id: e.severity for e in events}
    assert sev["source.x.b"] == Severity.WARNING
    assert sev["source.x.c"] == Severity.ERROR


def test_test_failure_parse() -> None:
    run_results = {
        "results": [
            {"unique_id": "test.p.unique_m_c.abc", "status": "pass"},
            {"unique_id": "test.p.not_null_m_c.def", "status": "fail", "failures": 3},
            {"unique_id": "test.p.relationships_m_c.ghi", "status": "error"},
        ]
    }
    events = TestFailureMonitor().parse(run_results)
    assert len(events) == 2
    assert all(e.anomaly_type == AnomalyType.TEST_FAILURE for e in events)
    types = {e.metadata["test_type"] for e in events}
    assert {"not_null", "relationships"} == types


def test_snapshot_store_roundtrip(tmp_path) -> None:
    store = SnapshotStore(str(tmp_path))
    schema = SchemaSnapshot(
        tables={"raw.t": [ColumnSnapshot("c", "raw", "t", "col", "STRING", 1, True)]}
    )
    store.save_schema(schema)
    assert store.get_latest_schema().tables["raw.t"][0].column_name == "col"

    baseline = QualityBaseline(
        row_counts={"raw.t": 10},
        null_ratios={"raw.t": {"col": 0.1}},
        value_ranges={"raw.t": {"col": (0.0, 9.0)}},
    )
    store.save_quality(baseline)
    assert store.get_latest_quality().value_ranges["raw.t"]["col"] == (0.0, 9.0)
