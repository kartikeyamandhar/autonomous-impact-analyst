"""Anomaly event types produced by the detectors (Phase 4) and consumed by the
agent (Phase 5). Mirrors specs/interfaces.md exactly."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class AnomalyType(str, Enum):
    COLUMN_ADDED = "column_added"
    COLUMN_REMOVED = "column_removed"
    TYPE_CHANGED = "type_changed"
    NULLABILITY_CHANGED = "nullability_changed"
    ROW_COUNT_DROP = "row_count_drop"
    NULL_RATIO_SPIKE = "null_ratio_spike"
    VALUE_RANGE_BREACH = "value_range_breach"
    FRESHNESS_VIOLATION = "freshness_violation"
    TEST_FAILURE = "test_failure"


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class AnomalyEvent:
    anomaly_type: AnomalyType
    severity: Severity
    source_node_id: str           # dbt unique_id of the affected source/model
    source_column: str | None     # column name if applicable, else None
    description: str
    previous_value: str | None
    current_value: str | None
    detected_at: datetime
    metadata: dict = field(default_factory=dict)
