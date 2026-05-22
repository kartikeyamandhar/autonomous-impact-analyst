"""Parser tests against committed dbt artifact fixtures (no live Neo4j)."""

from pathlib import Path

import pytest

from src.graph_engine.manifest_parser import parse_dbt_artifacts
from src.graph_engine.types import DbtGraph

pytestmark = pytest.mark.phase_3

PKG = "autonomous_impact_analyst"
STG_MARKETS = f"model.{PKG}.stg_coingecko__coins_markets"
INT_PROFILES = f"model.{PKG}.int_token_profiles"
SRC_MARKETS = f"source.{PKG}.coingecko.coingecko_coins_markets"


@pytest.fixture(scope="module")
def graph(fixtures_dir: Path) -> DbtGraph:
    return parse_dbt_artifacts(
        str(fixtures_dir / "manifest.json"),
        str(fixtures_dir / "catalog.json"),
    )


def test_node_and_edge_counts(graph: DbtGraph) -> None:
    assert len(graph.nodes) > 20
    assert len(graph.edges) > 15


def test_has_each_resource_type(graph: DbtGraph) -> None:
    types = {n.resource_type for n in graph.nodes.values()}
    assert {"model", "source", "exposure"} <= types


def test_model_node_fields(graph: DbtGraph) -> None:
    node = graph.nodes[STG_MARKETS]
    assert node.resource_type == "model"
    assert node.layer == "staging"
    assert node.materialization == "view"
    assert node.compiled_sql is not None
    assert SRC_MARKETS in node.depends_on


def test_layer_from_directory_not_schema(graph: DbtGraph) -> None:
    # intermediate models are routed into the 'staging' schema; layer must
    # still resolve to 'intermediate' from the model directory.
    node = graph.nodes[INT_PROFILES]
    assert node.schema == "staging"
    assert node.layer == "intermediate"


def test_catalog_types_merged(graph: DbtGraph) -> None:
    node = graph.nodes[STG_MARKETS]
    col = node.columns["current_price_usd"]
    assert col.data_type is not None
    assert "decimal" in col.data_type.lower()


def test_source_dependency_edges_exist(graph: DbtGraph) -> None:
    src_edges = [e for e in graph.edges if e.edge_type == "source"]
    assert any(
        e.source_id == SRC_MARKETS and e.target_id == STG_MARKETS for e in src_edges
    )


def test_consumes_edges_for_exposures(graph: DbtGraph) -> None:
    consumes = [e for e in graph.edges if e.edge_type == "consumes"]
    assert len(consumes) >= 2
    assert all(e.source_id.startswith("exposure.") for e in consumes)


def test_test_edges_reference_models_or_columns(graph: DbtGraph) -> None:
    tests = [e for e in graph.edges if e.edge_type == "tests"]
    assert len(tests) > 10
    assert all(e.source_id.startswith("test.") for e in tests)
