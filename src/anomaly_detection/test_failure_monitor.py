"""dbt test-failure detection.

Parses run_results.json and emits a TEST_FAILURE event for each test whose
status is fail or error.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from src.anomaly_detection.anomaly_events import AnomalyEvent, AnomalyType, Severity

_FAIL_STATUSES = {"fail", "error"}
_GENERIC_TESTS = ("not_null", "accepted_values", "relationships", "unique")


def _test_type(test_uid: str) -> str:
    parts = test_uid.split(".")
    name = parts[2] if len(parts) > 2 else test_uid
    for t in _GENERIC_TESTS:
        if name.startswith(t):
            return t
    return "custom"


class TestFailureMonitor:
    def detect(self, run_results_path: str) -> list[AnomalyEvent]:
        path = Path(run_results_path)
        if not path.exists():
            return []
        data = json.loads(path.read_text())
        return self.parse(data)

    def parse(self, run_results: dict) -> list[AnomalyEvent]:
        events: list[AnomalyEvent] = []
        now = datetime.utcnow()
        for result in run_results.get("results", []):
            status = str(result.get("status", "")).lower()
            if status not in _FAIL_STATUSES:
                continue
            unique_id = result.get("unique_id", "")
            failures = result.get("failures")
            severity = Severity.ERROR if status == "error" else Severity.WARNING
            events.append(
                AnomalyEvent(
                    anomaly_type=AnomalyType.TEST_FAILURE,
                    severity=severity,
                    source_node_id=unique_id,
                    source_column=None,
                    description=(
                        f"dbt test {unique_id} {status}"
                        + (f" ({failures} failing rows)" if failures else "")
                    ),
                    previous_value=None,
                    current_value=status,
                    detected_at=now,
                    metadata={
                        "test_type": _test_type(unique_id),
                        "failures": failures,
                        "status": status,
                    },
                )
            )
        return events
