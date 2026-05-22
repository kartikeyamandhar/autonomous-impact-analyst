"""Agent state types. Mirrors specs/interfaces.md (Phase 5 produces, Phase 6
consumes)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from src.anomaly_detection.anomaly_events import AnomalyEvent


@dataclass
class PlannedAction:
    action_type: Literal["log", "slack_alert", "github_pr", "pause_dbt_run"]
    payload: dict
    priority: int  # lower = first. log=0, slack=1, pr=2, pause=3


@dataclass
class AgentState:
    # Input
    event: AnomalyEvent

    # Traversal results
    affected_paths: list[list[str]] = field(default_factory=list)
    pruned_paths: list[list[str]] = field(default_factory=list)
    affected_exposures: list[dict] = field(default_factory=list)

    # Analysis
    test_coverage_per_node: dict[str, float] = field(default_factory=dict)
    risk_scores: dict[str, float] = field(default_factory=dict)
    overall_risk: Literal["low", "medium", "high", "critical"] = "low"

    # Decisions
    recommended_actions: list[PlannedAction] = field(default_factory=list)

    # Output
    impact_summary: str = ""
    fix_suggestion: str | None = None

    # Enhancements
    summary_payload: dict = field(default_factory=dict)  # structured summary (#15)
    incident_key: str = ""           # stable identity for dedup/memory (#7/#13)
    prior_occurrences: int = 0       # times this incident was seen before this run (#13)
    requires_approval: bool = False  # high-impact actions held for human approval (#10)
    correlated_events: list = field(default_factory=list)  # other events in the incident (#11)
    errors: list = field(default_factory=list)  # loud failures (e.g. node not found)
    run_id: str = ""                 # correlation id for this agent run
