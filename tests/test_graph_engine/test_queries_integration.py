"""Integration tests against a live Neo4j (the loaded graph).

These exercise the real Cypher — which the mocked agent tests cannot — so a
relationship-direction bug or a query regression is caught. Skipped unless
NEO4J_URI is set; run with `pytest -m integration` after `make graph-load`.
"""

import os

import pytest

pytestmark = [pytest.mark.phase_3, pytest.mark.integration]

PKG = "autonomous_impact_analyst"
SRC = f"source.{PKG}.coingecko.coingecko_coins_markets"
STG = f"model.{PKG}.stg_coingecko__coins_markets"

_HAVE_NEO4J = all(os.environ.get(k) for k in ("NEO4J_URI", "NEO4J_USER", "NEO4J_PASSWORD"))
pytest.importorskip("neo4j")
if not _HAVE_NEO4J:
    pytest.skip("NEO4J_* env not set", allow_module_level=True)


@pytest.fixture(scope="module")
def queries():
    from neo4j import GraphDatabase

    from src.graph_engine.queries import GraphQueries

    driver = GraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=(os.environ["NEO4J_USER"], os.environ["NEO4J_PASSWORD"]),
    )
    yield GraphQueries(driver)
    driver.close()


def test_node_metadata_resolves_source(queries):
    meta = queries.node_metadata(SRC)
    assert meta and meta["name"] == "coingecko_coins_markets"


def test_downstream_and_fan_out(queries):
    assert len(queries.downstream_models(STG)) > 0
    assert queries.fan_out(STG) > 0


def test_paths_reach_exposure(queries):
    paths = queries.paths_to_exposures(SRC)
    assert paths
    assert any(p[-1]["unique_id"].startswith("exposure.") for p in paths)


def test_exposure_priority_loaded(queries):
    # the slack bot exposure should carry priority=high (enhancement #2)
    for path in queries.paths_to_exposures(SRC):
        exp = path[-1]
        meta = queries.node_metadata(exp["unique_id"])
        if exp["name"] == "defi_market_slack_bot":
            assert meta["priority"] == "high"
            return
    pytest.fail("defi_market_slack_bot not reachable from source")


def test_column_lineage_has_confidence(queries):
    rows = queries.column_lineage_forward(SRC, "current_price")
    if rows:  # current_price flows downstream
        assert all("confidence" in r for r in rows)
