"""Post agent impact alerts to Slack as Block Kit messages."""

from __future__ import annotations

import logging
from typing import Any

from slack_sdk.webhook import WebhookClient

from src.utils.retry import retry

logger = logging.getLogger(__name__)

# Slack text fields cap at 3000 chars; leave headroom.
_TEXT_LIMIT = 2900

_RISK_COLOR = {
    "low": "#36a64f",       # green
    "medium": "#daa038",    # yellow
    "high": "#e8912d",      # orange
    "critical": "#d00000",  # red
}
def _truncate(text: str, limit: int = _TEXT_LIMIT) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


class SlackNotifier:
    def __init__(self, webhook_url: str) -> None:
        self.client = WebhookClient(webhook_url)

    def build_blocks(
        self, state: Any, pr_url: str | None = None,
        affected_products: list[str] | None = None,
        affected_dashboards: list[str] | None = None,
    ) -> list[dict]:
        ev = state.event
        atype = getattr(ev.anomaly_type, "value", ev.anomaly_type)
        severity = getattr(ev.severity, "value", ev.severity)
        risk = state.overall_risk

        dashboards = affected_dashboards or [
            e.get("name", "?") for e in state.affected_exposures
        ]
        products = affected_products or []
        gaps = [
            f"`{node.split('.')[-1]}` ({ratio:.0%})"
            for node, ratio in state.test_coverage_per_node.items()
            if ratio < 0.5
        ]

        blocks: list[dict] = [
            {
                "type": "header",
                "text": {"type": "plain_text",
                         "text": _truncate(f"{atype} — {severity} (risk: {risk})", 150)},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn",
                         "text": _truncate(state.impact_summary or ev.description)},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Overall risk:*\n{risk}"},
                    {"type": "mrkdwn", "text": f"*Affected nodes:*\n{len(state.affected_paths)}"},
                    {"type": "mrkdwn",
                     "text": "*Data products affected:*\n"
                             + (_truncate(", ".join(products), 500) or "none")},
                    {"type": "mrkdwn",
                     "text": "*Dashboards at risk:*\n"
                             + (_truncate(", ".join(dashboards), 500) or "none")},
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

    @retry(max_attempts=3, exceptions=(Exception,))
    def _send(self, text: str, attachments: list[dict]) -> Any:
        return self.client.send(text=text, attachments=attachments)

    def send_impact_alert(
        self, state: Any, pr_url: str | None = None,
        affected_products: list[str] | None = None,
        affected_dashboards: list[str] | None = None,
    ) -> bool:
        risk = state.overall_risk
        try:
            resp = self._send(
                f"Impact alert: {state.overall_risk} risk",
                [{
                    "color": _RISK_COLOR.get(risk, "#cccccc"),
                    "blocks": self.build_blocks(
                        state, pr_url, affected_products, affected_dashboards),
                }],
            )
            ok = resp.status_code == 200
            if not ok:
                logger.warning("slack send failed: %s %s", resp.status_code, resp.body)
            return ok
        except Exception as e:  # noqa: BLE001 - never crash the action layer
            logger.error("slack send error: %s", e)
            return False
