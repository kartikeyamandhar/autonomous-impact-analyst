"""Column-level lineage via sqlglot.

For each model column, trace which upstream model/source columns it derives
from. sqlglot resolves through CTEs, joins, and SELECT * (given a schema map).
Failures are non-fatal: we log unresolved columns and fall back to a coarse
name-match for staging models that use JSON-extraction functions sqlglot can't
trace through.
"""

from __future__ import annotations

import logging

import sqlglot
from sqlglot import exp
from sqlglot.lineage import Node, lineage

from src.graph_engine.types import ColumnLineageEdge, DbtGraph

logger = logging.getLogger(__name__)

CONFIDENCE_DIRECT = 1.0
CONFIDENCE_AGGREGATE = 0.8
CONFIDENCE_EXPRESSION = 0.5


def _build_schema_map(graph: DbtGraph) -> dict:
    """sqlglot schema: {database: {schema: {table: {column: type}}}}."""
    schema: dict = {}
    for node in graph.nodes.values():
        if not node.columns:
            continue
        db = node.database or "default"
        schema.setdefault(db, {}).setdefault(node.schema, {})[node.name] = {
            cname: (ci.data_type or "STRING") for cname, ci in node.columns.items()
        }
    return schema


def _name_index(graph: DbtGraph) -> dict[str, str]:
    """Lowercased model/source table name -> unique_id."""
    index: dict[str, str] = {}
    for uid, node in graph.nodes.items():
        if node.resource_type in ("model", "source", "seed", "snapshot"):
            index[node.name.lower()] = uid
    return index


def _classify_confidence(node: Node) -> float:
    """Confidence from the output column's projection expression."""
    expr = node.expression
    if expr is None:
        return CONFIDENCE_EXPRESSION
    inner = expr.this if isinstance(expr, exp.Alias) else expr
    if isinstance(inner, exp.Column):
        return CONFIDENCE_DIRECT
    if inner.find(exp.AggFunc):
        return CONFIDENCE_AGGREGATE
    return CONFIDENCE_EXPRESSION


def _walk_leaves(node: Node) -> list[Node]:
    leaves: list[Node] = []

    def _walk(n: Node) -> None:
        if not n.downstream:
            leaves.append(n)
            return
        for d in n.downstream:
            _walk(d)

    _walk(node)
    return leaves


def _resolve_column(
    model_uid: str,
    column: str,
    sql: str,
    schema: dict,
    name_index: dict[str, str],
    dialect: str,
) -> list[ColumnLineageEdge]:
    """Resolve a single output column via sqlglot. Empty list if unresolved."""
    root = lineage(column, sql, schema=schema, dialect=dialect)
    confidence = _classify_confidence(root)
    edges: list[ColumnLineageEdge] = []
    seen: set[tuple[str, str]] = set()
    for leaf in _walk_leaves(root):
        src = leaf.source
        if not isinstance(src, exp.Table):
            continue
        upstream_uid = name_index.get(src.name.lower())
        if not upstream_uid or upstream_uid == model_uid:
            continue
        upstream_col = leaf.name.rsplit(".", 1)[-1].strip('`"')
        key = (upstream_uid, upstream_col)
        if key in seen:
            continue
        seen.add(key)
        edges.append(
            ColumnLineageEdge(
                upstream_model=upstream_uid,
                upstream_column=upstream_col,
                downstream_model=model_uid,
                downstream_column=column,
                confidence=confidence,
            )
        )
    return edges


def _fallback_staging(
    model_uid: str, column: str, depends_on: list[str]
) -> list[ColumnLineageEdge]:
    """Coarse fallback for JSON-extraction columns sqlglot can't trace.

    Staging models read exactly one upstream source; assume the output column
    derives from it with low confidence.
    """
    sources = [d for d in depends_on if d.startswith("source.")]
    upstreams = sources or depends_on
    if len(upstreams) != 1:
        return []
    return [
        ColumnLineageEdge(
            upstream_model=upstreams[0],
            upstream_column=column,
            downstream_model=model_uid,
            downstream_column=column,
            confidence=CONFIDENCE_EXPRESSION,
        )
    ]


def extract_column_lineage(
    graph: DbtGraph, dialect: str = "databricks"
) -> list[ColumnLineageEdge]:
    schema = _build_schema_map(graph)
    name_index = _name_index(graph)

    edges: list[ColumnLineageEdge] = []
    total_columns = 0
    resolved_columns = 0

    for uid, node in graph.nodes.items():
        if node.resource_type != "model" or not node.compiled_sql:
            continue
        for column in node.columns:
            total_columns += 1
            col_edges: list[ColumnLineageEdge] = []
            try:
                col_edges = _resolve_column(
                    uid, column, node.compiled_sql, schema, name_index, dialect
                )
            except (sqlglot.errors.SqlglotError, KeyError, ValueError, RecursionError) as e:
                logger.debug("lineage failed for %s.%s: %s", node.name, column, e)
            if not col_edges and node.layer == "staging":
                col_edges = _fallback_staging(uid, column, node.depends_on)
            if col_edges:
                resolved_columns += 1
                edges.extend(col_edges)

    pct = (resolved_columns / total_columns * 100) if total_columns else 0.0
    logger.info(
        "column lineage: resolved %d/%d columns (%.1f%%), %d edges",
        resolved_columns,
        total_columns,
        pct,
        len(edges),
    )
    print(
        f"column lineage: resolved {resolved_columns}/{total_columns} "
        f"columns ({pct:.1f}%), {len(edges)} edges"
    )
    return edges
