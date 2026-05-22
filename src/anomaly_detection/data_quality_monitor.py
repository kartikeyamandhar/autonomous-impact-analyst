"""Data-quality drift detection.

Computes per-table row counts, per-column null ratios, and per-column numeric
ranges, then compares against the previous baseline to emit ROW_COUNT_DROP,
NULL_RATIO_SPIKE, and VALUE_RANGE_BREACH events. Raw tables are all STRING, so
numeric ranges use try_cast(col AS DOUBLE) (non-numeric columns yield NULL and
are ignored).
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from src.anomaly_detection.anomaly_events import AnomalyEvent, AnomalyType, Severity
from src.anomaly_detection.schema_monitor import source_node_id
from src.anomaly_detection.snapshot_store import QualityBaseline

_IGNORE_COLS = {"_extracted_at"}


def _bt(col: str) -> str:
    return "`" + col.replace("`", "``") + "`"


class DataQualityMonitor:
    def __init__(
        self,
        databricks_conn: Any,
        config: dict,
        catalog: str | None = None,
        raw_schema: str | None = None,
    ) -> None:
        self.conn = databricks_conn
        self.config = config
        self.catalog = catalog or os.environ.get("DATABRICKS_CATALOG", "workspace")
        self.raw_schema = raw_schema or os.environ.get("DATABRICKS_SCHEMA_RAW", "raw")

    def _fq(self, table: str) -> str:
        return f"{self.catalog}.{self.raw_schema}.{table}"

    def _columns(self, table: str) -> list[str]:
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT * FROM {self._fq(table)} LIMIT 0")
            cols = [d[0] for d in cursor.description]
        finally:
            cursor.close()
        return [c for c in cols if c not in _IGNORE_COLS]

    def compute_baseline(self, source_tables: list[str]) -> QualityBaseline:
        row_counts: dict[str, int] = {}
        null_ratios: dict[str, dict[str, float]] = {}
        value_ranges: dict[str, dict[str, tuple[float, float]]] = {}
        value_stats: dict[str, dict[str, dict[str, float]]] = {}

        for table in source_tables:
            key = f"{self.raw_schema}.{table}"
            cols = self._columns(table)
            selects = ["count(*) AS __rows"]
            for c in cols:
                selects.append(f"sum(case when {_bt(c)} is null then 1 else 0 end) AS null__{c}")
                selects.append(f"min(try_cast({_bt(c)} as double)) AS min__{c}")
                selects.append(f"max(try_cast({_bt(c)} as double)) AS max__{c}")
                selects.append(f"avg(try_cast({_bt(c)} as double)) AS avg__{c}")
                selects.append(f"stddev(try_cast({_bt(c)} as double)) AS std__{c}")
                selects.append(f"count(try_cast({_bt(c)} as double)) AS cnt__{c}")
            query = f"SELECT {', '.join(selects)} FROM {self._fq(table)}"

            cursor = self.conn.cursor()
            try:
                cursor.execute(query)
                names = [d[0] for d in cursor.description]
                row = dict(zip(names, cursor.fetchone()))
            finally:
                cursor.close()

            total = int(row["__rows"] or 0)
            row_counts[key] = total
            null_ratios[key] = {}
            value_ranges[key] = {}
            value_stats[key] = {}
            for c in cols:
                nulls = int(row.get(f"null__{c}") or 0)
                null_ratios[key][c] = (nulls / total) if total else 0.0
                cmin, cmax = row.get(f"min__{c}"), row.get(f"max__{c}")
                if cmin is not None and cmax is not None:
                    value_ranges[key][c] = (float(cmin), float(cmax))
                    value_stats[key][c] = {
                        "mean": float(row.get(f"avg__{c}") or 0.0),
                        "stddev": float(row.get(f"std__{c}") or 0.0),
                        "count": float(row.get(f"cnt__{c}") or 0.0),
                    }
        return QualityBaseline(
            row_counts=row_counts,
            null_ratios=null_ratios,
            value_ranges=value_ranges,
            value_stats=value_stats,
            captured_at=datetime.utcnow(),
        )

    def detect(
        self, tables: list[str], previous: QualityBaseline | None
    ) -> list[AnomalyEvent]:
        if previous is None:
            return []
        current = self.compute_baseline(tables)
        events: list[AnomalyEvent] = []
        now = datetime.utcnow()
        drop_ratio = float(self.config.get("row_count_drop_ratio", 0.20))
        null_delta = float(self.config.get("null_ratio_spike_delta", 0.10))
        n_sigma = float(self.config.get("value_range_breach_stddev", 3.0))
        min_samples = int(self.config.get("min_baseline_samples", 5))

        for table in tables:
            key = f"{self.raw_schema}.{table}"
            node_id = source_node_id(table)

            prev_rows = previous.row_counts.get(key)
            cur_rows = current.row_counts.get(key)
            if prev_rows and cur_rows is not None and prev_rows > 0:
                drop = (prev_rows - cur_rows) / prev_rows
                if drop > drop_ratio:
                    events.append(
                        AnomalyEvent(
                            anomaly_type=AnomalyType.ROW_COUNT_DROP,
                            severity=Severity.ERROR,
                            source_node_id=node_id,
                            source_column=None,
                            description=(
                                f"Row count for {key} dropped {drop:.0%} "
                                f"({prev_rows} -> {cur_rows})"
                            ),
                            previous_value=f"{prev_rows} rows",
                            current_value=f"{cur_rows} rows",
                            detected_at=now,
                            metadata={"table": key, "drop_ratio": drop},
                        )
                    )

            prev_nulls = previous.null_ratios.get(key, {})
            cur_nulls = current.null_ratios.get(key, {})
            for col, cur_ratio in cur_nulls.items():
                prev_ratio = prev_nulls.get(col)
                if prev_ratio is not None and (cur_ratio - prev_ratio) > null_delta:
                    events.append(
                        AnomalyEvent(
                            anomaly_type=AnomalyType.NULL_RATIO_SPIKE,
                            severity=Severity.WARNING,
                            source_node_id=node_id,
                            source_column=col,
                            description=(
                                f"Null ratio for {key}.{col} rose from "
                                f"{prev_ratio:.2f} to {cur_ratio:.2f}"
                            ),
                            previous_value=f"{prev_ratio:.2f} null ratio",
                            current_value=f"{cur_ratio:.2f} null ratio",
                            detected_at=now,
                            metadata={"table": key},
                        )
                    )

            prev_stats = previous.value_stats.get(key, {})
            prev_ranges = previous.value_ranges.get(key, {})
            cur_ranges = current.value_ranges.get(key, {})
            for col, (cur_min, cur_max) in cur_ranges.items():
                stats = prev_stats.get(col)
                event = None
                # Primary: sigma-based detection against the prior distribution.
                if stats and stats.get("count", 0) >= min_samples and stats.get("stddev", 0) > 0:
                    mean, std = stats["mean"], stats["stddev"]
                    lo, hi = mean - n_sigma * std, mean + n_sigma * std
                    if cur_min < lo or cur_max > hi:
                        event = AnomalyEvent(
                            anomaly_type=AnomalyType.VALUE_RANGE_BREACH,
                            severity=Severity.ERROR,
                            source_node_id=node_id,
                            source_column=col,
                            description=(
                                f"Column {key}.{col} range [{cur_min:.4g}, {cur_max:.4g}] "
                                f"breaches {n_sigma:g}σ band "
                                f"[{lo:.4g}, {hi:.4g}] (mean {mean:.4g}, σ {std:.4g})"
                            ),
                            previous_value=f"{n_sigma:g}σ band [{lo:.4g}, {hi:.4g}]",
                            current_value=f"[{cur_min:.4g}, {cur_max:.4g}]",
                            detected_at=now,
                            metadata={"table": key, "method": "sigma", "n_sigma": n_sigma},
                        )
                # Fallback: when no usable distribution, flag a sign flip to negative.
                elif (prev_range := prev_ranges.get(col)) and prev_range[0] >= 0 and cur_min < 0:
                    event = AnomalyEvent(
                        anomaly_type=AnomalyType.VALUE_RANGE_BREACH,
                        severity=Severity.ERROR,
                        source_node_id=node_id,
                        source_column=col,
                        description=(
                            f"Column {key}.{col} now has negative values "
                            f"(min {cur_min}); was previously non-negative"
                        ),
                        previous_value=f"min {prev_range[0]}",
                        current_value=f"min {cur_min}",
                        detected_at=now,
                        metadata={"table": key, "method": "sign_flip"},
                    )
                if event:
                    events.append(event)
        return events
