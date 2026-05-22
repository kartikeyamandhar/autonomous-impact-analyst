"""Persist schema snapshots and quality baselines between detection runs.

Snapshots are JSON files in data/snapshots/. The detectors compare the current
warehouse state against the most recent snapshot to find drift.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path

from src.common.atomic_json import read_json, write_json_atomic


@dataclass
class ColumnSnapshot:
    table_catalog: str
    table_schema: str
    table_name: str
    column_name: str
    data_type: str
    ordinal_position: int
    is_nullable: bool


@dataclass
class SchemaSnapshot:
    tables: dict[str, list[ColumnSnapshot]]  # keyed by "schema.table_name"
    captured_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class QualityBaseline:
    row_counts: dict[str, int]
    null_ratios: dict[str, dict[str, float]]
    value_ranges: dict[str, dict[str, tuple[float, float]]]
    # table -> column -> {"mean", "stddev", "count"} for sigma-based detection.
    value_stats: dict[str, dict[str, dict[str, float]]] = field(default_factory=dict)
    captured_at: datetime = field(default_factory=datetime.utcnow)


def _ts() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")


class SnapshotStore:
    def __init__(self, snapshot_dir: str = "data/snapshots") -> None:
        self.snapshot_dir = Path(snapshot_dir)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

    # -- schema ---------------------------------------------------------------

    def save_schema(self, snapshot: SchemaSnapshot) -> Path:
        payload = {
            "tables": {
                key: [asdict(col) for col in cols]
                for key, cols in snapshot.tables.items()
            },
            "captured_at": snapshot.captured_at.isoformat(),
        }
        path = self.snapshot_dir / f"schema_{_ts()}.json"
        write_json_atomic(path, payload)
        return path

    def get_latest_schema(self) -> SchemaSnapshot | None:
        files = sorted(self.snapshot_dir.glob("schema_*.json"))
        if not files:
            return None
        data = read_json(files[-1], {})
        tables = {
            key: [ColumnSnapshot(**col) for col in cols]
            for key, cols in data["tables"].items()
        }
        return SchemaSnapshot(
            tables=tables,
            captured_at=datetime.fromisoformat(data["captured_at"]),
        )

    # -- quality --------------------------------------------------------------

    def save_quality(self, baseline: QualityBaseline) -> Path:
        payload = {
            "row_counts": baseline.row_counts,
            "null_ratios": baseline.null_ratios,
            "value_ranges": {
                table: {col: list(rng) for col, rng in cols.items()}
                for table, cols in baseline.value_ranges.items()
            },
            "value_stats": baseline.value_stats,
            "captured_at": baseline.captured_at.isoformat(),
        }
        path = self.snapshot_dir / f"quality_{_ts()}.json"
        write_json_atomic(path, payload)
        return path

    def get_latest_quality(self) -> QualityBaseline | None:
        files = sorted(self.snapshot_dir.glob("quality_*.json"))
        if not files:
            return None
        data = read_json(files[-1], {})
        value_ranges = {
            table: {col: (rng[0], rng[1]) for col, rng in cols.items()}
            for table, cols in data["value_ranges"].items()
        }
        return QualityBaseline(
            row_counts=data["row_counts"],
            null_ratios=data["null_ratios"],
            value_ranges=value_ranges,
            value_stats=data.get("value_stats", {}),
            captured_at=datetime.fromisoformat(data["captured_at"]),
        )
