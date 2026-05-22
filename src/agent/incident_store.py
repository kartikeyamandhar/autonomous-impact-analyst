"""Incident memory + dedup.

Persists each distinct anomaly (keyed by source + type + column) to
data/incidents/ so the agent can report recurrence ("3rd time this week") and
suppress duplicate actions within a window. Keeps the system from spamming
Slack/GitHub when the same anomaly fires every detection cycle.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from src.common.atomic_json import locked_update, read_json


def incident_key(event: Any) -> str:
    atype = getattr(event.anomaly_type, "value", event.anomaly_type)
    raw = f"{event.source_node_id}|{atype}|{event.source_column or ''}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class IncidentStore:
    def __init__(self, incident_dir: str = "data/incidents") -> None:
        self.path = Path(incident_dir) / "incidents.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> dict[str, dict]:
        return read_json(self.path, {})

    def prior_occurrences(self, key: str) -> int:
        return int(self._load().get(key, {}).get("count", 0))

    def last_seen(self, key: str) -> datetime | None:
        ts = self._load().get(key, {}).get("last_seen")
        return datetime.fromisoformat(ts) if ts else None

    def is_duplicate(self, key: str, window_minutes: int) -> bool:
        last = self.last_seen(key)
        if last is None:
            return False
        return datetime.utcnow() - last < timedelta(minutes=window_minutes)

    def record(self, key: str, event: Any, risk_level: str) -> int:
        """Record an occurrence; return the new total count.

        Lock-guarded read-modify-write so overlapping scheduled runs can't lose
        updates or double-count.
        """
        now = datetime.utcnow().isoformat()
        with locked_update(self.path, {}) as box:
            data = box[0]
            entry = data.get(key, {"count": 0, "first_seen": now})
            entry["count"] = int(entry.get("count", 0)) + 1
            entry["last_seen"] = now
            entry["last_risk"] = risk_level
            entry["source_node_id"] = event.source_node_id
            entry["anomaly_type"] = getattr(event.anomaly_type, "value", event.anomaly_type)
            entry["source_column"] = event.source_column
            entry.setdefault("outcomes", [])
            data[key] = entry
            box[0] = data
            return int(entry["count"])

    def record_outcome(self, key: str, actionable: bool, note: str = "") -> None:
        """Capture human feedback on an alert for later calibration (#5)."""
        with locked_update(self.path, {}) as box:
            data = box[0]
            entry = data.get(key)
            if entry is None:
                return
            entry.setdefault("outcomes", []).append(
                {"actionable": actionable, "note": note,
                 "at": datetime.utcnow().isoformat()}
            )
            box[0] = data

    def summary(self) -> dict:
        """Aggregate stats for calibration: counts by risk, actionable rate."""
        data = self._load()
        by_risk: dict[str, int] = {}
        actionable = total_feedback = 0
        for entry in data.values():
            by_risk[entry.get("last_risk", "?")] = by_risk.get(entry.get("last_risk", "?"), 0) + 1
            for o in entry.get("outcomes", []):
                total_feedback += 1
                if o.get("actionable"):
                    actionable += 1
        return {
            "incidents": len(data),
            "by_risk": by_risk,
            "feedback_count": total_feedback,
            "actionable_rate": (actionable / total_feedback) if total_feedback else None,
        }
