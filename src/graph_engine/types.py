"""Shared graph-engine dataclasses.

Mirrors specs/interfaces.md. Phase 4/5 consume these types, so field names
and semantics must match the spec exactly.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ColumnInfo:
    name: str
    description: str
    data_type: str | None       # From catalog.json, e.g. "STRING", "DECIMAL(18,8)"
    tests: list[str]            # test unique_ids covering this column


@dataclass
class GraphNode:
    unique_id: str
    resource_type: str          # "source", "model", "test", "exposure", "seed", "snapshot"
    name: str
    schema: str
    database: str
    materialization: str | None  # "table", "view", "incremental", None
    layer: str | None            # "staging", "intermediate", "marts", None
    tags: list[str]
    owner: str | None
    description: str
    compiled_sql: str | None     # Only for models. manifest compiled_code.
    columns: dict[str, ColumnInfo]
    row_count: int | None        # From catalog.json
    depends_on: list[str]        # unique_ids this node depends on


@dataclass
class GraphEdge:
    source_id: str              # upstream node unique_id
    target_id: str              # downstream node unique_id
    edge_type: str              # "ref", "source", "tests", "consumes"


@dataclass
class ColumnLineageEdge:
    upstream_model: str
    upstream_column: str
    downstream_model: str
    downstream_column: str
    confidence: float           # 1.0 direct ref, 0.8 aggregation, 0.5 expression


@dataclass
class DbtGraph:
    nodes: dict[str, GraphNode]            # keyed by unique_id
    edges: list[GraphEdge] = field(default_factory=list)
    column_lineage: list[ColumnLineageEdge] = field(default_factory=list)
