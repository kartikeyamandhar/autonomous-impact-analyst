"""Deterministic action selection from risk level. No LLM."""

from __future__ import annotations

from src.agent.types import PlannedAction
from src.anomaly_detection.anomaly_events import AnomalyEvent

_SCHEMA_CHANGES = {
    "column_added",
    "column_removed",
    "type_changed",
    "nullability_changed",
}

_PRIORITY = {"log": 0, "slack_alert": 1, "github_pr": 2, "pause_dbt_run": 3}


def _is_schema_change(event: AnomalyEvent) -> bool:
    return getattr(event.anomaly_type, "value", event.anomaly_type) in _SCHEMA_CHANGES


def plan_actions(
    risk_level: str,
    event: AnomalyEvent,
    affected_paths: list[list[str]],
    affected_exposures: list[dict],
    config: dict,
) -> list[PlannedAction]:
    actions_cfg = config.get("actions", {})
    actions: list[PlannedAction] = []

    # Always log.
    actions.append(
        PlannedAction(
            action_type="log",
            payload={"message": f"{event.anomaly_type} on {event.source_node_id} "
                     f"-> risk={risk_level}"},
            priority=_PRIORITY["log"],
        )
    )

    if risk_level in ("medium", "high", "critical"):
        actions.append(
            PlannedAction(
                action_type="slack_alert",
                payload={
                    "summary": "",  # filled by generate_summary
                    "risk_level": risk_level,
                    "affected_count": len(affected_paths),
                    "affected_exposures": affected_exposures,
                },
                priority=_PRIORITY["slack_alert"],
            )
        )

    if risk_level in ("high", "critical"):
        if actions_cfg.get("enable_github_pr", False) and _is_schema_change(event):
            branch_prefix = actions_cfg.get("github_pr_branch_prefix", "impact-analyst/fix")
            actions.append(
                PlannedAction(
                    action_type="github_pr",
                    payload={
                        "fix_sql": "",  # filled by generate_fix
                        "model_path": "",
                        "branch_name": f"{branch_prefix}/{event.source_column or 'schema'}",
                    },
                    priority=_PRIORITY["github_pr"],
                )
            )

    if risk_level == "critical":
        if actions_cfg.get("enable_pause_dbt_run", False):
            actions.append(
                PlannedAction(
                    action_type="pause_dbt_run",
                    payload={"reason": f"Critical risk from {event.anomaly_type} "
                             f"on {event.source_node_id}"},
                    priority=_PRIORITY["pause_dbt_run"],
                )
            )

    return sorted(actions, key=lambda a: a.priority)
