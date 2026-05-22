"""Dispatch an agent's PlannedActions to the real executors.

Honors the safety flags the agent set: suppressed_duplicate (dedup),
requires_approval (hold high-impact actions), requires_human_review (PRs as
drafts). Creates the PR before the Slack alert so the alert can link it.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from src.actions.dbt_runner import DbtRunner
from src.actions.github_pr import GitHubPRCreator
from src.actions.slack_notifier import SlackNotifier

logger = logging.getLogger(__name__)


def _action(state: Any, action_type: str) -> Any:
    for a in state.recommended_actions:
        if a.action_type == action_type:
            return a
    return None


def execute_actions(
    state: Any,
    config: dict,
    *,
    notifier: SlackNotifier | None = None,
    pr_creator: GitHubPRCreator | None = None,
    dbt_runner: DbtRunner | None = None,
    approved: bool = False,
) -> dict:
    """Execute planned actions; return {action_type: outcome}."""
    results: dict[str, Any] = {}
    pr_url: str | None = None

    held = state.requires_approval and not approved

    # 1. log (always)
    if _action(state, "log"):
        logger.info("impact: risk=%s node=%s run=%s",
                    state.overall_risk, state.event.source_node_id, state.run_id)
        results["log"] = True

    # 2. github_pr (before slack, so the alert can link it)
    pr = _action(state, "github_pr")
    if pr:
        if pr.payload.get("suppressed_duplicate"):
            results["github_pr"] = "skipped_duplicate"
        elif held:
            results["github_pr"] = "pending_approval"
        elif pr.payload.get("fix_sql"):
            creator = pr_creator or GitHubPRCreator(
                os.environ["GITHUB_TOKEN"], os.environ["GITHUB_REPO"]
            )
            pr_url = creator.create_fix_pr(
                event=state.event,
                fix_sql=pr.payload["fix_sql"],
                model_file_path=pr.payload.get("model_path", ""),
                impact_summary=state.impact_summary,
                risk_level=state.overall_risk,
                draft=pr.payload.get("requires_human_review", True),
            )
            results["github_pr"] = pr_url
        else:
            results["github_pr"] = "no_fix_sql"

    # 3. slack_alert
    slack = _action(state, "slack_alert")
    if slack:
        if slack.payload.get("suppressed_duplicate"):
            results["slack_alert"] = "skipped_duplicate"
        else:
            sender = notifier or SlackNotifier(os.environ["SLACK_WEBHOOK_URL"])
            results["slack_alert"] = sender.send_impact_alert(state, pr_url=pr_url)

    # 4. pause_dbt_run
    pause = _action(state, "pause_dbt_run")
    if pause:
        if held:
            results["pause_dbt_run"] = "pending_approval"
        else:
            runner = dbt_runner or DbtRunner("src/dbt_project")
            runner.create_pause_lock(pause.payload.get("reason", "critical risk"))
            results["pause_dbt_run"] = "paused"

    logger.info("executed actions: %s", results)
    return results
