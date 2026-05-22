"""Cypher query helpers over the loaded dbt knowledge graph.

Direction convention (set by neo4j_loader):
  (downstream)-[:DEPENDS_ON]->(upstream)
  (downstream_col)-[:DERIVES_FROM]->(upstream_col)
  (exposure)-[:CONSUMES]->(mart)
  (test)-[:TESTS]->(column|model)

So "forward / downstream" traversal walks DEPENDS_ON edges *backwards*.
Returns plain dicts/lists to keep the Neo4j layer decoupled (see
specs/interfaces.md "Neo4j Query Return Types").
"""

from __future__ import annotations

from typing import Any


class GraphQueries:
    def __init__(self, driver: Any) -> None:
        self.driver = driver

    # -- node resolution ------------------------------------------------------

    def node_metadata(self, node_id: str) -> dict | None:
        """Resolve a unique_id to its node properties, or None if absent."""
        cypher = (
            "MATCH (n {unique_id: $id}) "
            "RETURN n.unique_id AS unique_id, n.name AS name, n.layer AS layer, "
            "n.materialization AS materialization, n.resource_type AS resource_type, "
            "n.compiled_sql AS compiled_sql, n.priority AS priority, "
            "n.exposure_type AS exposure_type, labels(n)[0] AS label LIMIT 1"
        )
        with self.driver.session() as session:
            rec = session.run(cypher, id=node_id).single()
            return dict(rec) if rec else None

    # -- model-level traversal ------------------------------------------------

    def downstream_models(self, model_id: str) -> list[dict]:
        cypher = (
            "MATCH (s {unique_id: $id})<-[:DEPENDS_ON*]-(d) "
            "WHERE d:Model OR d:Source "
            "RETURN DISTINCT d.unique_id AS unique_id, d.name AS name, d.layer AS layer"
        )
        with self.driver.session() as session:
            return [dict(r) for r in session.run(cypher, id=model_id)]

    def upstream_models(self, model_id: str) -> list[dict]:
        cypher = (
            "MATCH (s {unique_id: $id})-[:DEPENDS_ON*]->(u) "
            "RETURN DISTINCT u.unique_id AS unique_id, u.name AS name, u.layer AS layer"
        )
        with self.driver.session() as session:
            return [dict(r) for r in session.run(cypher, id=model_id)]

    # -- column-level lineage -------------------------------------------------

    def column_lineage_forward(self, model_id: str, column: str) -> list[dict]:
        col_id = f"{model_id}.{column}"
        cypher = (
            "MATCH path = (c:Column {unique_id: $col})<-[rels:DERIVES_FROM*]-(d:Column) "
            "RETURN nodes(path) AS ns, [r IN rels | r.confidence] AS confs"
        )
        return self._paths_as_columns(cypher, col=col_id)

    def column_lineage_backward(self, model_id: str, column: str) -> list[dict]:
        col_id = f"{model_id}.{column}"
        cypher = (
            "MATCH path = (c:Column {unique_id: $col})-[rels:DERIVES_FROM*]->(u:Column) "
            "RETURN nodes(path) AS ns, [r IN rels | r.confidence] AS confs"
        )
        return self._paths_as_columns(cypher, col=col_id)

    def _paths_as_columns(self, cypher: str, **params: Any) -> list[dict]:
        out: list[dict] = []
        with self.driver.session() as session:
            for record in session.run(cypher, **params):
                path = [
                    {"model": n.get("model_unique_id"), "column": n.get("name")}
                    for n in record["ns"]
                ]
                confs = [c for c in (record["confs"] or []) if c is not None]
                out.append({"path": path, "confidence": min(confs) if confs else 1.0})
        return out

    # -- test coverage --------------------------------------------------------

    def test_coverage(self, model_id: str) -> dict:
        cypher = (
            "MATCH (m {unique_id: $id})-[:HAS_COLUMN]->(c:Column) "
            "OPTIONAL MATCH (t:Test)-[:TESTS]->(c) "
            "RETURN c.name AS column, "
            "collect(DISTINCT {unique_id: t.unique_id, test_type: t.test_type}) AS tests"
        )
        total = 0
        tested = 0
        test_list: list[dict] = []
        with self.driver.session() as session:
            for record in session.run(cypher, id=model_id):
                total += 1
                column = record["column"]
                col_tests = [t for t in record["tests"] if t.get("unique_id")]
                if col_tests:
                    tested += 1
                for t in col_tests:
                    test_list.append(
                        {
                            "unique_id": t["unique_id"],
                            "column": column,
                            "test_type": t.get("test_type"),
                        }
                    )
        ratio = (tested / total) if total else 0.0
        return {
            "total_columns": total,
            "tested_columns": tested,
            "coverage_ratio": ratio,
            "tests": test_list,
        }

    # -- exposure reachability ------------------------------------------------

    def paths_to_exposures(self, model_id: str) -> list[list[dict]]:
        cypher = (
            "MATCH path = (m {unique_id: $id})<-[:DEPENDS_ON|CONSUMES*]-(e:Exposure) "
            "RETURN nodes(path) AS ns"
        )
        out: list[list[dict]] = []
        with self.driver.session() as session:
            for record in session.run(cypher, id=model_id):
                out.append(
                    [
                        {"unique_id": n.get("unique_id"), "name": n.get("name")}
                        for n in record["ns"]
                    ]
                )
        return out

    def fan_out(self, model_id: str) -> int:
        cypher = (
            "MATCH (m {unique_id: $id})<-[:DEPENDS_ON]-(d) RETURN count(DISTINCT d) AS c"
        )
        with self.driver.session() as session:
            rec = session.run(cypher, id=model_id).single()
            return int(rec["c"]) if rec else 0

    def distance_to_nearest_exposure(self, model_id: str) -> int | None:
        cypher = (
            "MATCH path = shortestPath("
            "(m {unique_id: $id})<-[:DEPENDS_ON|CONSUMES*]-(e:Exposure)) "
            "RETURN length(path) AS d ORDER BY d ASC LIMIT 1"
        )
        with self.driver.session() as session:
            rec = session.run(cypher, id=model_id).single()
            return int(rec["d"]) if rec else None
