"""Schema drift detection.

Compares the current source-table schema against the previous snapshot and
emits AnomalyEvents for added/removed columns, type changes, and nullability
changes. Reads information_schema where available; falls back to dbt's
catalog.json (some Databricks tiers restrict information_schema).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any

from src.anomaly_detection.anomaly_events import AnomalyEvent, AnomalyType, Severity
from src.anomaly_detection.snapshot_store import ColumnSnapshot, SchemaSnapshot

logger = logging.getLogger(__name__)

PROJECT = "autonomous_impact_analyst"
DEFAULT_CATALOG_PATH = "src/dbt_project/target/catalog.json"


def _source_name(table: str) -> str:
    for prefix in ("coingecko", "defi_llama", "etherscan"):
        if table.startswith(prefix + "_"):
            return prefix
    return table.split("_")[0]


def source_node_id(table: str) -> str:
    return f"source.{PROJECT}.{_source_name(table)}.{table}"


class SchemaMonitor:
    def __init__(
        self,
        databricks_conn: Any,
        source_tables: list[str],
        catalog: str | None = None,
        raw_schema: str | None = None,
        catalog_json_path: str = DEFAULT_CATALOG_PATH,
    ) -> None:
        self.conn = databricks_conn
        self.source_tables = source_tables
        self.catalog = catalog or os.environ.get("DATABRICKS_CATALOG", "workspace")
        self.raw_schema = raw_schema or os.environ.get("DATABRICKS_SCHEMA_RAW", "raw")
        self.catalog_json_path = catalog_json_path

    # -- current state --------------------------------------------------------

    def get_current_schema(self) -> SchemaSnapshot:
        try:
            return self._from_information_schema()
        except Exception as e:  # noqa: BLE001 - intentional broad fallback
            logger.warning("information_schema unavailable (%s); using catalog.json", e)
            return self._from_catalog_json()

    def _from_information_schema(self) -> SchemaSnapshot:
        placeholders = ", ".join("?" for _ in self.source_tables)
        query = (
            "SELECT table_name, column_name, data_type, ordinal_position, is_nullable "
            f"FROM {self.catalog}.information_schema.columns "
            f"WHERE table_schema = ? AND table_name IN ({placeholders}) "
            "ORDER BY table_name, ordinal_position"
        )
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, [self.raw_schema, *self.source_tables])
            rows = cursor.fetchall()
        finally:
            cursor.close()
        if not rows:
            raise RuntimeError("information_schema returned no rows")
        tables: dict[str, list[ColumnSnapshot]] = {}
        for table_name, col, dtype, pos, nullable in rows:
            key = f"{self.raw_schema}.{table_name}"
            tables.setdefault(key, []).append(
                ColumnSnapshot(
                    table_catalog=self.catalog,
                    table_schema=self.raw_schema,
                    table_name=table_name,
                    column_name=col,
                    data_type=str(dtype),
                    ordinal_position=int(pos),
                    is_nullable=str(nullable).upper() in ("YES", "TRUE", "1"),
                )
            )
        return SchemaSnapshot(tables=tables, captured_at=datetime.utcnow())

    def _from_catalog_json(self) -> SchemaSnapshot:
        with open(self.catalog_json_path) as f:
            catalog = json.load(f)
        wanted = set(self.source_tables)
        tables: dict[str, list[ColumnSnapshot]] = {}
        for node in catalog.get("sources", {}).values():
            meta = node.get("metadata", {})
            table_name = meta.get("name")
            if table_name not in wanted:
                continue
            key = f"{self.raw_schema}.{table_name}"
            cols: list[ColumnSnapshot] = []
            for cname, cinfo in node.get("columns", {}).items():
                cols.append(
                    ColumnSnapshot(
                        table_catalog=meta.get("database", self.catalog),
                        table_schema=meta.get("schema", self.raw_schema),
                        table_name=table_name,
                        column_name=cname,
                        data_type=str(cinfo.get("type", "")),
                        ordinal_position=int(cinfo.get("index", 0)),
                        is_nullable=True,
                    )
                )
            tables[key] = cols
        return SchemaSnapshot(tables=tables, captured_at=datetime.utcnow())

    # -- diff -----------------------------------------------------------------

    def detect(self, previous: SchemaSnapshot | None) -> list[AnomalyEvent]:
        if previous is None:
            return []
        current = self.get_current_schema()
        events: list[AnomalyEvent] = []
        now = datetime.utcnow()

        for table_key in set(current.tables) | set(previous.tables):
            cur_cols = {c.column_name: c for c in current.tables.get(table_key, [])}
            prev_cols = {c.column_name: c for c in previous.tables.get(table_key, [])}
            table_name = table_key.split(".", 1)[-1]
            node_id = source_node_id(table_name)

            for col in cur_cols.keys() - prev_cols.keys():
                events.append(
                    AnomalyEvent(
                        anomaly_type=AnomalyType.COLUMN_ADDED,
                        severity=Severity.INFO,
                        source_node_id=node_id,
                        source_column=col,
                        description=f"Column '{col}' added to {table_key}",
                        previous_value=None,
                        current_value=cur_cols[col].data_type,
                        detected_at=now,
                        metadata={"table": table_key},
                    )
                )
            for col in prev_cols.keys() - cur_cols.keys():
                events.append(
                    AnomalyEvent(
                        anomaly_type=AnomalyType.COLUMN_REMOVED,
                        severity=Severity.CRITICAL,
                        source_node_id=node_id,
                        source_column=col,
                        description=f"Column '{col}' removed from {table_key}",
                        previous_value=prev_cols[col].data_type,
                        current_value=None,
                        detected_at=now,
                        metadata={"table": table_key},
                    )
                )
            for col in cur_cols.keys() & prev_cols.keys():
                cur, prev = cur_cols[col], prev_cols[col]
                if cur.data_type != prev.data_type:
                    events.append(
                        AnomalyEvent(
                            anomaly_type=AnomalyType.TYPE_CHANGED,
                            severity=Severity.ERROR,
                            source_node_id=node_id,
                            source_column=col,
                            description=(
                                f"Column '{col}' type changed from {prev.data_type} "
                                f"to {cur.data_type} in {table_key}"
                            ),
                            previous_value=prev.data_type,
                            current_value=cur.data_type,
                            detected_at=now,
                            metadata={"table": table_key},
                        )
                    )
                if cur.is_nullable != prev.is_nullable:
                    events.append(
                        AnomalyEvent(
                            anomaly_type=AnomalyType.NULLABILITY_CHANGED,
                            severity=Severity.WARNING,
                            source_node_id=node_id,
                            source_column=col,
                            description=(
                                f"Column '{col}' nullability changed from "
                                f"{prev.is_nullable} to {cur.is_nullable} in {table_key}"
                            ),
                            previous_value=str(prev.is_nullable),
                            current_value=str(cur.is_nullable),
                            detected_at=now,
                            metadata={"table": table_key},
                        )
                    )
        return events
