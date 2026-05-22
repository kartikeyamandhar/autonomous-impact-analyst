"""Correlate simultaneous anomalies into incidents.

A single root cause (e.g. a dropped column) typically trips several detectors
at once: a schema event, downstream test failures, a null spike. Treating them
as independent runs spams alerts. correlate_events groups events that share a
root table (and fall within a time window) into one incident, so the agent can
reason about and report them together.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

# Severity ordering for choosing an incident's primary event.
_SEVERITY_RANK = {"info": 0, "warning": 1, "error": 2, "critical": 3}


def _root_table(event: Any) -> str:
    """Group key: the source/table the event pertains to.

    For source events the node id is source.<pkg>.<source>.<table>; for test
    failures it's the test id. We group on the source node id when present,
    else the raw node id.
    """
    nid = event.source_node_id
    if nid.startswith("source."):
        return nid
    return nid


def _severity_value(event: Any) -> str:
    return getattr(event.severity, "value", event.severity)


def correlate_events(
    events: list[Any], window_minutes: int = 15
) -> list[list[Any]]:
    """Group events into incidents by root table within a time window.

    Returns a list of incident groups, each a list of events sorted with the
    highest-severity event first (the primary event the agent runs on).
    """
    window = timedelta(minutes=window_minutes)
    groups: list[list[Any]] = []
    for event in sorted(events, key=lambda e: e.detected_at):
        key = _root_table(event)
        placed = False
        for grp in groups:
            if _root_table(grp[0]) == key and (
                event.detected_at - grp[-1].detected_at
            ) <= window:
                grp.append(event)
                placed = True
                break
        if not placed:
            groups.append([event])

    return [
        sorted(
            group,
            key=lambda e: (_SEVERITY_RANK.get(_severity_value(e), 0), e.detected_at),
            reverse=True,
        )
        for group in groups
    ]


def primary_event(incident: list[Any]) -> Any:
    """The highest-severity event in an incident (already first after sort)."""
    return incident[0]
