"""Source freshness detection.

Runs `dbt source freshness` and parses target/sources.json, emitting a
FRESHNESS_VIOLATION for each source whose status is warn or error.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path

from src.anomaly_detection.anomaly_events import AnomalyEvent, AnomalyType, Severity

logger = logging.getLogger(__name__)

_STATUS_SEVERITY = {
    "warn": Severity.WARNING,
    "error": Severity.ERROR,
    "runtime error": Severity.ERROR,
}


class FreshnessMonitor:
    def __init__(self, dbt_project_dir: str) -> None:
        self.dbt_project_dir = Path(dbt_project_dir)

    def _run_freshness(self) -> None:
        env = {**os.environ, "DBT_PROFILES_DIR": str(self.dbt_project_dir)}
        try:
            subprocess.run(
                ["dbt", "source", "freshness"],
                cwd=self.dbt_project_dir,
                env=env,
                capture_output=True,
                timeout=300,
                check=False,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired) as e:
            logger.warning("dbt source freshness could not run: %s", e)

    def detect(self, run_dbt: bool = True) -> list[AnomalyEvent]:
        if run_dbt:
            self._run_freshness()
        sources_path = self.dbt_project_dir / "target" / "sources.json"
        if not sources_path.exists():
            logger.warning("sources.json not found at %s", sources_path)
            return []
        return self.parse(json.loads(sources_path.read_text()))

    def parse(self, sources: dict) -> list[AnomalyEvent]:
        events: list[AnomalyEvent] = []
        now = datetime.utcnow()
        for result in sources.get("results", []):
            status = str(result.get("status", "")).lower()
            severity = _STATUS_SEVERITY.get(status)
            if severity is None:
                continue
            unique_id = result.get("unique_id", "")
            crit = result.get("criteria", {})
            events.append(
                AnomalyEvent(
                    anomaly_type=AnomalyType.FRESHNESS_VIOLATION,
                    severity=severity,
                    source_node_id=unique_id,
                    source_column=None,
                    description=(
                        f"Source {unique_id} freshness {status}: last loaded "
                        f"{result.get('max_loaded_at')}"
                    ),
                    previous_value=None,
                    current_value=result.get("max_loaded_at"),
                    detected_at=now,
                    metadata={
                        "snapshotted_at": result.get("snapshotted_at"),
                        "criteria": crit,
                        "status": status,
                    },
                )
            )
        return events
