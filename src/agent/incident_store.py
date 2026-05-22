"""Incident memory + dedup.

Persists each distinct anomaly (keyed by source + type + column) to
data/incidents/ so the agent can report recurrence ("3rd time this week") and
suppress duplicate actions within a window. Keeps the system from spamming
Slack/GitHub when the same anomaly fires every detection cycle.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


def incident_key(event: Any) -> str:
    atype = getattr(event.anomaly_type, "value", event.anomaly_type)
    raw = f"{event.source_node_id}|{atype}|{event.source_column or ''}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class IncidentStore:
    def __init__(self, incident_dir: str = "data/incidents") -> None:
        self.path = Path(incident_dir) / "incidents.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> dict[str, dict]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text())
        except json.JSONDecodeError:
            return {}

    def _save(self, data: dict[str, dict]) -> None:
        self.path.write_text(json.dumps(data, indent=2))

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
        """Record an occurrence; return the new total count."""
        data = self._load()
        now = datetime.utcnow().isoformat()
        entry = data.get(key, {"count": 0, "first_seen": now})
        entry["count"] = int(entry.get("count", 0)) + 1
        entry["last_seen"] = now
        entry["last_risk"] = risk_level
        entry["source_node_id"] = event.source_node_id
        entry["anomaly_type"] = getattr(event.anomaly_type, "value", event.anomaly_type)
        entry["source_column"] = event.source_column
        data[key] = entry
        self._save(data)
        return int(entry["count"])
