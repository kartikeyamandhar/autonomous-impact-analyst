"""Load a DbtGraph into Neo4j.

Node labels: :Model :Source :Exposure :Test :Column
Relationships:
  (downstream)-[:DEPENDS_ON]->(upstream)      from ref/source edges
  (model)-[:HAS_COLUMN]->(column)
  (downstream_col)-[:DERIVES_FROM]->(upstream_col)   from column lineage
  (test)-[:TESTS]->(column|model)
  (exposure)-[:CONSUMES]->(mart)

No APOC (AuraDB Free). Batches via UNWIND + parameters.
"""

from __future__ import annotations

import os
from typing import Any

from neo4j import GraphDatabase

from src.graph_engine.types import DbtGraph

_RESOURCE_LABEL = {
    "model": "Model",
    "source": "Source",
    "exposure": "Exposure",
    "seed": "Model",
    "snapshot": "Model",
}

# dbt generic test types, longest-prefix first so "not_null" wins over "not".
_TEST_TYPES = ("not_null", "accepted_values", "relationships", "unique")


def _test_type(test_uid: str) -> str:
    """Infer the generic test type from a test unique_id."""
    parts = test_uid.split(".")
    name = parts[2] if len(parts) > 2 else test_uid
    for t in _TEST_TYPES:
        if name.startswith(t):
            return t
    return "custom"


def _node_props(node: Any) -> dict[str, Any]:
    """Non-null scalar/list properties for a resource node."""
    props: dict[str, Any] = {
        "name": node.name,
        "resource_type": node.resource_type,
        "tags": node.tags,
        "description": node.description,
    }
    for key in ("schema", "database", "materialization", "layer", "owner", "row_count"):
        val = getattr(node, key)
        if val is not None and val != "":
            props[key] = val
    if node.compiled_sql:
        props["compiled_sql"] = node.compiled_sql
    return props


def _create_constraints(session: Any) -> None:
    for label in ("Model", "Source", "Exposure", "Test", "Column"):
        session.run(
            f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) "
            f"REQUIRE n.unique_id IS UNIQUE"
        )


def load_graph(
    graph: DbtGraph, neo4j_uri: str, neo4j_user: str, neo4j_password: str
) -> dict:
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    try:
        with driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            _create_constraints(session)

            # --- Resource nodes, grouped by label -----------------------------
            by_label: dict[str, list[dict]] = {}
            for uid, node in graph.nodes.items():
                label = _RESOURCE_LABEL.get(node.resource_type, "Model")
                by_label.setdefault(label, []).append(
                    {"unique_id": uid, "props": _node_props(node)}
                )
            for label, rows in by_label.items():
                session.run(
                    f"UNWIND $rows AS row "
                    f"CREATE (n:{label} {{unique_id: row.unique_id}}) SET n += row.props",
                    rows=rows,
                )

            # --- Column nodes + HAS_COLUMN ------------------------------------
            col_rows: list[dict] = []
            has_col_rows: list[dict] = []
            for uid, node in graph.nodes.items():
                for cname, cinfo in node.columns.items():
                    col_uid = f"{uid}.{cname}"
                    cprops: dict[str, Any] = {
                        "name": cname,
                        "model_unique_id": uid,
                        "description": cinfo.description,
                    }
                    if cinfo.data_type:
                        cprops["data_type"] = cinfo.data_type
                    col_rows.append({"unique_id": col_uid, "props": cprops})
                    has_col_rows.append({"model": uid, "col": col_uid})
            if col_rows:
                session.run(
                    "UNWIND $rows AS row "
                    "CREATE (c:Column {unique_id: row.unique_id}) SET c += row.props",
                    rows=col_rows,
                )
                session.run(
                    "UNWIND $rows AS row "
                    "MATCH (m {unique_id: row.model}), (c:Column {unique_id: row.col}) "
                    "CREATE (m)-[:HAS_COLUMN]->(c)",
                    rows=has_col_rows,
                )

            # --- Test nodes ---------------------------------------------------
            test_uids = sorted(
                {e.source_id for e in graph.edges if e.edge_type == "tests"}
            )
            if test_uids:
                session.run(
                    "UNWIND $rows AS row "
                    "CREATE (t:Test {unique_id: row.unique_id}) SET t.test_type = row.test_type",
                    rows=[{"unique_id": u, "test_type": _test_type(u)} for u in test_uids],
                )

            # --- DEPENDS_ON (ref/source) --------------------------------------
            dep_rows = [
                {"downstream": e.target_id, "upstream": e.source_id}
                for e in graph.edges
                if e.edge_type in ("ref", "source")
            ]
            if dep_rows:
                session.run(
                    "UNWIND $rows AS row "
                    "MATCH (a {unique_id: row.downstream}), (b {unique_id: row.upstream}) "
                    "CREATE (a)-[:DEPENDS_ON]->(b)",
                    rows=dep_rows,
                )

            # --- TESTS --------------------------------------------------------
            test_rows = [
                {"test": e.source_id, "target": e.target_id}
                for e in graph.edges
                if e.edge_type == "tests"
            ]
            if test_rows:
                session.run(
                    "UNWIND $rows AS row "
                    "MATCH (t:Test {unique_id: row.test}), (tgt {unique_id: row.target}) "
                    "CREATE (t)-[:TESTS]->(tgt)",
                    rows=test_rows,
                )

            # --- CONSUMES -----------------------------------------------------
            consume_rows = [
                {"exposure": e.source_id, "mart": e.target_id}
                for e in graph.edges
                if e.edge_type == "consumes"
            ]
            if consume_rows:
                session.run(
                    "UNWIND $rows AS row "
                    "MATCH (ex:Exposure {unique_id: row.exposure}), (m {unique_id: row.mart}) "
                    "CREATE (ex)-[:CONSUMES]->(m)",
                    rows=consume_rows,
                )

            # --- DERIVES_FROM (column lineage) --------------------------------
            lineage_rows = [
                {
                    "down": f"{e.downstream_model}.{e.downstream_column}",
                    "up": f"{e.upstream_model}.{e.upstream_column}",
                    "confidence": e.confidence,
                }
                for e in graph.column_lineage
            ]
            if lineage_rows:
                session.run(
                    "UNWIND $rows AS row "
                    "MATCH (d:Column {unique_id: row.down}), (u:Column {unique_id: row.up}) "
                    "CREATE (d)-[r:DERIVES_FROM]->(u) SET r.confidence = row.confidence",
                    rows=lineage_rows,
                )

            node_rec = session.run("MATCH (n) RETURN count(n) AS c").single()
            rel_rec = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()
            node_count = node_rec["c"] if node_rec else 0
            rel_count = rel_rec["c"] if rel_rec else 0
        return {"nodes": node_count, "relationships": rel_count}
    finally:
        driver.close()


def main() -> None:
    from dotenv import load_dotenv

    from src.graph_engine.lineage_builder import extract_column_lineage
    from src.graph_engine.manifest_parser import parse_dbt_artifacts

    load_dotenv()
    manifest = "src/dbt_project/target/manifest.json"
    catalog = "src/dbt_project/target/catalog.json"
    graph = parse_dbt_artifacts(manifest, catalog)
    graph.column_lineage = extract_column_lineage(graph)
    result = load_graph(
        graph,
        os.environ["NEO4J_URI"],
        os.environ["NEO4J_USER"],
        os.environ["NEO4J_PASSWORD"],
    )
    print(f"Loaded into Neo4j: {result}")


if __name__ == "__main__":
    main()
