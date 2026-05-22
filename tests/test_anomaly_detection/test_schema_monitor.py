"""Schema monitor diff + catalog.json fallback tests (no live Databricks)."""

from datetime import datetime
from pathlib import Path

import pytest

from src.anomaly_detection.anomaly_events import AnomalyType, Severity
from src.anomaly_detection.schema_monitor import SchemaMonitor, source_node_id
from src.anomaly_detection.snapshot_store import ColumnSnapshot, SchemaSnapshot

pytestmark = pytest.mark.phase_4


def _col(table: str, name: str, dtype: str = "string", nullable: bool = True) -> ColumnSnapshot:
    return ColumnSnapshot("workspace", "raw", table, name, dtype, 1, nullable)


def _snap(table: str, cols: list[ColumnSnapshot]) -> SchemaSnapshot:
    return SchemaSnapshot(tables={f"raw.{table}": cols}, captured_at=datetime.utcnow())


class _RaisingConn:
    def cursor(self):
        raise RuntimeError("information_schema disabled")


def test_source_node_id_mapping() -> None:
    assert source_node_id("coingecko_coins_markets") == (
        "source.autonomous_impact_analyst.coingecko.coingecko_coins_markets"
    )
    assert source_node_id("defi_llama_protocols").endswith("defi_llama.defi_llama_protocols")


def test_first_run_returns_empty() -> None:
    m = SchemaMonitor(_RaisingConn(), ["coingecko_coins_markets"])
    assert m.detect(None) == []


def test_detect_column_removed_and_type_change() -> None:
    table = "coingecko_coins_markets"
    prev = _snap(table, [_col(table, "current_price"), _col(table, "roi")])
    cur = _snap(table, [_col(table, "current_price", dtype="double")])

    m = SchemaMonitor(_RaisingConn(), [table])
    m.get_current_schema = lambda: cur  # type: ignore[method-assign]
    events = m.detect(prev)

    by_type = {e.anomaly_type: e for e in events}
    assert by_type[AnomalyType.COLUMN_REMOVED].source_column == "roi"
    assert by_type[AnomalyType.COLUMN_REMOVED].severity == Severity.CRITICAL
    assert by_type[AnomalyType.TYPE_CHANGED].source_column == "current_price"
    assert by_type[AnomalyType.TYPE_CHANGED].previous_value == "string"
    assert by_type[AnomalyType.TYPE_CHANGED].current_value == "double"


def test_detect_column_added_and_nullability() -> None:
    table = "coingecko_exchanges"
    prev = _snap(table, [_col(table, "trust_score", nullable=True)])
    cur = _snap(
        table,
        [_col(table, "trust_score", nullable=False), _col(table, "new_col")],
    )
    m = SchemaMonitor(_RaisingConn(), [table])
    m.get_current_schema = lambda: cur  # type: ignore[method-assign]
    events = {e.anomaly_type for e in m.detect(prev)}
    assert AnomalyType.COLUMN_ADDED in events
    assert AnomalyType.NULLABILITY_CHANGED in events


def test_catalog_json_fallback(fixtures_dir: Path) -> None:
    # information_schema raises -> falls back to committed catalog.json fixture.
    m = SchemaMonitor(
        _RaisingConn(),
        ["coingecko_coins_markets"],
        catalog_json_path=str(fixtures_dir / "catalog.json"),
    )
    snap = m.get_current_schema()
    assert "raw.coingecko_coins_markets" in snap.tables
    cols = {c.column_name for c in snap.tables["raw.coingecko_coins_markets"]}
    assert "current_price" in cols
