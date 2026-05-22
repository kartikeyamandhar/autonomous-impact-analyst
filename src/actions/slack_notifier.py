"""Post agent impact alerts to Slack as Block Kit messages."""

from __future__ import annotations

import logging
from typing import Any

from slack_sdk.webhook import WebhookClient

logger = logging.getLogger(__name__)

# Slack text fields cap at 3000 chars; leave headroom.
_TEXT_LIMIT = 2900

_RISK_COLOR = {
    "low": "#36a64f",       # green
    "medium": "#daa038",    # yellow
    "high": "#e8912d",      # orange
    "critical": "#d00000",  # red
}
_RISK_EMOJI = {"low": "🟢", "medium": "🟡", "high": "🟠", "critical": "🔴"}


def _truncate(text: str, limit: int = _TEXT_LIMIT) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


class SlackNotifier:
    def __init__(self, webhook_url: str) -> None:
        self.client = WebhookClient(webhook_url)

    def build_blocks(self, state: Any, pr_url: str | None = None) -> list[dict]:
        ev = state.event
        atype = getattr(ev.anomaly_type, "value", ev.anomaly_type)
        severity = getattr(ev.severity, "value", ev.severity)
        risk = state.overall_risk
        emoji = _RISK_EMOJI.get(risk, "⚪")

        exposures = ", ".join(e.get("name", "?") for e in state.affected_exposures) or "none"
        gaps = [
            f"`{node.split('.')[-1]}` ({ratio:.0%})"
            for node, ratio in state.test_coverage_per_node.items()
            if ratio < 0.5
        ]

        blocks: list[dict] = [
            {
                "type": "header",
                "text": {"type": "plain_text",
                         "text": _truncate(f"{emoji} {atype} — {severity}", 150)},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn",
                         "text": _truncate(state.impact_summary or ev.description)},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Risk:*\n{risk}"},
                    {"type": "mrkdwn", "text": f"*Affected paths:*\n{len(state.affected_paths)}"},
                    {"type": "mrkdwn", "text": f"*Exposures:*\n{_truncate(exposures, 500)}"},
                    {"type": "mrkdwn",
                     "text": f"*Recurrence:*\n#{state.prior_occurrences + 1}"},
                ],
            },
        ]
        if gaps:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn",
                         "text": _truncate("*Test coverage gaps:* " + ", ".join(gaps))},
            })
        if pr_url:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Proposed fix PR:* <{pr_url}>"},
            })
        footer = f"detected_at {ev.detected_at.isoformat()} • run {state.run_id}"
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": footer}],
        })
        return blocks

    def send_impact_alert(self, state: Any, pr_url: str | None = None) -> bool:
        risk = state.overall_risk
        try:
            resp = self.client.send(
                text=f"Impact alert: {state.overall_risk} risk",
                attachments=[{
                    "color": _RISK_COLOR.get(risk, "#cccccc"),
                    "blocks": self.build_blocks(state, pr_url),
                }],
            )
            ok = resp.status_code == 200
            if not ok:
                logger.warning("slack send failed: %s %s", resp.status_code, resp.body)
            return ok
        except Exception as e:  # noqa: BLE001 - never crash the action layer
            logger.error("slack send error: %s", e)
            return False
