"""The real DeFi market Slack bot (the defi_market_slack_bot exposure).

Posts periodic market summaries built from the mart tables. When an upstream
model is broken, the relevant section returns no rows and the bot renders a
visible "data unavailable" warning — the concrete, demonstrable degradation the
agent predicts when it traces impact to this exposure.
"""

from __future__ import annotations

import logging
import signal
import time
from datetime import datetime
from typing import Any

from slack_sdk import WebClient

from src.exposure_bot.mart_queries import MartQueries

logger = logging.getLogger(__name__)


def _table(rows: list[dict], cols: list[tuple[str, str]]) -> str:
    """Render rows as a monospace text table for a Slack code block."""
    header = " | ".join(label for label, _ in cols)
    lines = [header, "-" * len(header)]
    for r in rows:
        cells = []
        for _, key in cols:
            val = r.get(key)
            if isinstance(val, float):
                val = f"{val:,.4g}"
            cells.append(str(val))
        lines.append(" | ".join(cells))
    return "```\n" + "\n".join(lines) + "\n```"


def _warn_block(section: str) -> dict:
    return {
        "type": "section",
        "text": {"type": "mrkdwn",
                 "text": f":warning: *Data unavailable: {section}.* "
                         f"Upstream models may be broken."},
    }


class ExposureBot:
    def __init__(self, bot_token: str, channel: str, mart_queries: MartQueries) -> None:
        self.client = WebClient(token=bot_token)
        self.channel = channel
        self.mq = mart_queries

    def format_market_summary(self) -> list[dict]:
        tokens = self.mq.top_tokens_summary(10)
        protocols = self.mq.protocol_health_summary(10)
        whales = self.mq.recent_whale_movements(5)

        blocks: list[dict] = [
            {"type": "header",
             "text": {"type": "plain_text", "text": "DeFi Market Summary"}},
        ]

        if tokens:
            blocks.append({"type": "section", "text": {"type": "mrkdwn",
                "text": "*Top tokens by market cap*\n" + _table(tokens, [
                    ("token", "token"), ("price", "price"),
                    ("mcap", "market_cap"), ("chg%", "price_change_pct")])}})
        else:
            blocks.append(_warn_block("top tokens"))

        if protocols:
            blocks.append({"type": "section", "text": {"type": "mrkdwn",
                "text": "*Protocol health*\n" + _table(protocols, [
                    ("protocol", "protocol"), ("tvl", "tvl"),
                    ("avg_yield", "avg_yield"), ("chains", "chain_count")])}})
        else:
            blocks.append(_warn_block("protocol health"))

        if whales:
            blocks.append({"type": "section", "text": {"type": "mrkdwn",
                "text": "*Recent whale movements*\n" + _table(whales, [
                    ("token", "token"), ("amount", "amount"),
                    ("from", "from_addr"), ("to", "to_addr")])}})
        else:
            blocks.append(_warn_block("whale movements"))

        degraded = not (tokens and protocols and whales)
        footer = (
            f"{'⚠️ DEGRADED • ' if degraded else ''}"
            f"generated {datetime.utcnow().isoformat(timespec='seconds')}Z"
        )
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": footer}]})
        return blocks

    def post_market_summary(self) -> bool:
        try:
            blocks = self.format_market_summary()
            resp = self.client.chat_postMessage(
                channel=self.channel, blocks=blocks, text="DeFi Market Summary"
            )
            return bool(resp.get("ok"))
        except Exception as e:  # noqa: BLE001 - the bot must not crash on a post failure
            logger.error("post_market_summary failed: %s", e)
            return False

    def start_scheduled(self, interval_minutes: int = 60) -> None:
        stop = {"flag": False}

        def _handle(signum: Any, frame: Any) -> None:
            logger.info("ExposureBot stopping (signal %s)", signum)
            stop["flag"] = True

        signal.signal(signal.SIGINT, _handle)
        signal.signal(signal.SIGTERM, _handle)
        while not stop["flag"]:
            self.post_market_summary()
            for _ in range(int(interval_minutes * 60)):
                if stop["flag"]:
                    break
                time.sleep(1)


def main() -> None:
    import os

    from databricks import sql
    from dotenv import load_dotenv

    load_dotenv()
    conn = sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    bot = ExposureBot(
        os.environ["SLACK_BOT_TOKEN"], os.environ["SLACK_BOT_CHANNEL"], MartQueries(conn)
    )
    print("Posted:", bot.post_market_summary())
    conn.close()


if __name__ == "__main__":
    main()
