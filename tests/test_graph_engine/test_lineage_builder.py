"""Column lineage tests against committed dbt artifact fixtures."""

from pathlib import Path

import pytest

from src.graph_engine.lineage_builder import extract_column_lineage
from src.graph_engine.manifest_parser import parse_dbt_artifacts
from src.graph_engine.types import DbtGraph

pytestmark = pytest.mark.phase_3

PKG = "autonomous_impact_analyst"


@pytest.fixture(scope="module")
def graph(fixtures_dir: Path) -> DbtGraph:
    return parse_dbt_artifacts(
        str(fixtures_dir / "manifest.json"),
        str(fixtures_dir / "catalog.json"),
    )


def test_lineage_non_empty(graph: DbtGraph) -> None:
    edges = extract_column_lineage(graph)
    assert len(edges) > 0


def test_confidence_values_valid(graph: DbtGraph) -> None:
    edges = extract_column_lineage(graph)
    assert all(e.confidence in (1.0, 0.8, 0.5) for e in edges)


def test_resolution_rate_above_threshold(graph: DbtGraph) -> None:
    edges = extract_column_lineage(graph)
    models_with_sql = [
        n for n in graph.nodes.values() if n.resource_type == "model" and n.compiled_sql
    ]
    total_cols = sum(len(n.columns) for n in models_with_sql)
    resolved_downstream = {
        (e.downstream_model, e.downstream_column) for e in edges
    }
    assert len(resolved_downstream) / total_cols >= 0.8


def test_lineage_endpoints_are_known_nodes(graph: DbtGraph) -> None:
    edges = extract_column_lineage(graph)
    for e in edges:
        assert e.upstream_model in graph.nodes
        assert e.downstream_model in graph.nodes


def test_aggregate_confidence_present(graph: DbtGraph) -> None:
    # int_protocol_metrics.avg_apy / total_pool_tvl come through aggregates.
    edges = extract_column_lineage(graph)
    agg_edges = [e for e in edges if e.confidence == 0.8]
    assert len(agg_edges) > 0
