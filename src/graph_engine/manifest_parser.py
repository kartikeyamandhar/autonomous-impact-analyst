"""Parse dbt artifacts (manifest.json + catalog.json) into a DbtGraph.

The manifest holds the dependency DAG, compiled SQL, columns, and metadata.
The catalog holds real column types and (for tables) row counts. We merge both
into typed GraphNode / GraphEdge objects.
"""

from __future__ import annotations

import json
from typing import Any

from src.graph_engine.types import ColumnInfo, DbtGraph, GraphEdge, GraphNode

# resource_types from manifest["nodes"] that become first-class graph nodes.
_MODEL_LIKE = {"model", "seed", "snapshot"}


def _load(path: str) -> dict[str, Any]:
    with open(path) as f:
        return json.load(f)


def _layer_from_fqn(node: dict[str, Any]) -> str | None:
    """Derive the dbt layer from the model's directory.

    We can't use the schema: this project routes intermediate models into the
    'staging' schema, so the fqn directory (staging/intermediate/marts) is the
    reliable signal.
    """
    fqn = node.get("fqn") or []
    for part in fqn:
        if part in ("staging", "intermediate", "marts"):
            return part
    name = node.get("name", "")
    if name.startswith("stg_"):
        return "staging"
    if name.startswith("int_"):
        return "intermediate"
    if name.startswith(("fct_", "dim_")):
        return "marts"
    return None


def _row_count(catalog_node: dict[str, Any] | None) -> int | None:
    if not catalog_node:
        return None
    stats = catalog_node.get("stats", {})
    rc = stats.get("row_count") or stats.get("num_rows")
    if isinstance(rc, dict) and rc.get("include") and rc.get("value") is not None:
        try:
            return int(rc["value"])
        except (TypeError, ValueError):
            return None
    return None


def _columns(
    manifest_cols: dict[str, Any],
    catalog_node: dict[str, Any] | None,
    column_tests: dict[str, list[str]],
) -> dict[str, ColumnInfo]:
    catalog_cols = (catalog_node or {}).get("columns", {})
    out: dict[str, ColumnInfo] = {}
    # Union of column names from manifest and catalog.
    names = list(manifest_cols.keys())
    for cname in catalog_cols:
        if cname not in manifest_cols:
            names.append(cname)
    for cname in names:
        mcol = manifest_cols.get(cname, {})
        ccol = catalog_cols.get(cname, {})
        data_type = ccol.get("type") or mcol.get("data_type")
        out[cname] = ColumnInfo(
            name=cname,
            description=mcol.get("description", "") or "",
            data_type=data_type,
            tests=column_tests.get(cname, []),
        )
    return out


def _collect_test_targets(
    manifest: dict[str, Any],
) -> tuple[dict[str, dict[str, list[str]]], list[GraphEdge]]:
    """Walk test nodes once.

    Returns:
      - tests_by_model: model_uid -> {column_name -> [test_uid]}
      - edges: GraphEdge(test -> tested column/model, edge_type="tests")
    """
    tests_by_model: dict[str, dict[str, list[str]]] = {}
    edges: list[GraphEdge] = []
    for uid, node in manifest["nodes"].items():
        if node.get("resource_type") != "test":
            continue
        attached = node.get("attached_node")
        dep_nodes = node.get("depends_on", {}).get("nodes", [])
        model_uid = attached or (dep_nodes[0] if dep_nodes else None)
        if not model_uid:
            continue
        column_name = node.get("column_name") or (
            node.get("test_metadata", {}).get("kwargs", {}).get("column_name")
        )
        tests_by_model.setdefault(model_uid, {}).setdefault(column_name or "", []).append(uid)
        target = f"{model_uid}.{column_name}" if column_name else model_uid
        edges.append(GraphEdge(source_id=uid, target_id=target, edge_type="tests"))
    return tests_by_model, edges


def parse_dbt_artifacts(manifest_path: str, catalog_path: str) -> DbtGraph:
    manifest = _load(manifest_path)
    catalog = _load(catalog_path)
    catalog_nodes = catalog.get("nodes", {})
    catalog_sources = catalog.get("sources", {})

    nodes: dict[str, GraphNode] = {}
    edges: list[GraphEdge] = []

    tests_by_model, test_edges = _collect_test_targets(manifest)
    edges.extend(test_edges)

    def column_tests_for(model_uid: str) -> dict[str, list[str]]:
        return tests_by_model.get(model_uid, {})

    # --- Models / seeds / snapshots ------------------------------------------
    for uid, node in manifest["nodes"].items():
        rtype = node.get("resource_type")
        if rtype not in _MODEL_LIKE:
            continue
        catalog_node = catalog_nodes.get(uid)
        depends_on = list(node.get("depends_on", {}).get("nodes", []))
        nodes[uid] = GraphNode(
            unique_id=uid,
            resource_type=rtype,
            name=node["name"],
            schema=node.get("schema", ""),
            database=node.get("database", ""),
            materialization=node.get("config", {}).get("materialized"),
            layer=_layer_from_fqn(node),
            tags=list(node.get("tags", [])),
            owner=(node.get("config", {}).get("meta", {}) or {}).get("owner"),
            description=node.get("description", "") or "",
            compiled_sql=node.get("compiled_code"),
            columns=_columns(node.get("columns", {}), catalog_node, column_tests_for(uid)),
            row_count=_row_count(catalog_node),
            depends_on=depends_on,
        )
        for upstream in depends_on:
            edge_type = "source" if upstream.startswith("source.") else "ref"
            edges.append(GraphEdge(source_id=upstream, target_id=uid, edge_type=edge_type))

    # --- Sources -------------------------------------------------------------
    for uid, node in manifest.get("sources", {}).items():
        catalog_node = catalog_sources.get(uid)
        nodes[uid] = GraphNode(
            unique_id=uid,
            resource_type="source",
            name=node["name"],
            schema=node.get("schema", ""),
            database=node.get("database", ""),
            materialization=None,
            layer=None,
            tags=list(node.get("tags", [])),
            owner=(node.get("meta", {}) or {}).get("owner"),
            description=node.get("description", "") or "",
            compiled_sql=None,
            columns=_columns(node.get("columns", {}), catalog_node, {}),
            row_count=_row_count(catalog_node),
            depends_on=[],
        )

    # --- Exposures -----------------------------------------------------------
    for uid, node in manifest.get("exposures", {}).items():
        depends_on = list(node.get("depends_on", {}).get("nodes", []))
        owner = node.get("owner", {})
        exposure_meta = node.get("meta", {}) or {}
        nodes[uid] = GraphNode(
            unique_id=uid,
            resource_type="exposure",
            name=node["name"],
            schema="",
            database="",
            materialization=None,
            layer=None,
            tags=list(node.get("tags", [])),
            owner=owner.get("name") if isinstance(owner, dict) else owner,
            description=node.get("description", "") or "",
            compiled_sql=None,
            columns={},
            row_count=None,
            depends_on=depends_on,
            meta={
                "type": node.get("type"),
                "priority": exposure_meta.get("priority"),
                "maturity": node.get("maturity"),
            },
        )
        for mart_uid in depends_on:
            edges.append(GraphEdge(source_id=uid, target_id=mart_uid, edge_type="consumes"))

    return DbtGraph(nodes=nodes, edges=edges, column_lineage=[])
