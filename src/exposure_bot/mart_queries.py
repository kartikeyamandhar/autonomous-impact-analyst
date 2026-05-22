"""Read-side queries for the DeFi market Slack bot (the defi_market_slack_bot
exposure). Every method swallows errors and returns [] so a broken upstream
model degrades the bot rather than crashing it — that degraded state is the
visible proof of impact the agent predicts."""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


class MartQueries:
    def __init__(self, databricks_conn: Any, catalog: str | None = None,
                 marts_schema: str | None = None) -> None:
        self.conn = databricks_conn
        self.catalog = catalog or os.environ.get("DATABRICKS_CATALOG", "workspace")
        self.marts = marts_schema or os.environ.get("DATABRICKS_SCHEMA_MARTS", "marts")

    def _fq(self, table: str) -> str:
        return f"{self.catalog}.{self.marts}.{table}"

    def _query(self, sql: str) -> list[dict]:
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql)
            cols = [d[0] for d in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def top_tokens_summary(self, limit: int = 10) -> list[dict]:
        try:
            return self._query(
                f"SELECT coin_name AS token, current_price_usd AS price, "
                f"market_cap_usd AS market_cap, total_volume_usd AS volume_24h, "
                f"price_change_pct_24h AS price_change_pct "
                f"FROM {self._fq('fct_daily_token_metrics')} "
                f"WHERE market_cap_usd IS NOT NULL "
                f"ORDER BY market_cap_usd DESC LIMIT {int(limit)}"
            )
        except Exception as e:  # noqa: BLE001
            logger.error("top_tokens_summary failed: %s", e)
            return []

    def protocol_health_summary(self, limit: int = 10) -> list[dict]:
        try:
            return self._query(
                f"SELECT p.protocol_name AS protocol, p.tvl_usd AS tvl, "
                f"p.avg_apy AS avg_yield, "
                f"size(from_json(d.chains, 'array<string>')) AS chain_count "
                f"FROM {self._fq('fct_protocol_health')} p "
                f"LEFT JOIN {self._fq('dim_protocols')} d ON p.protocol_id = d.protocol_id "
                f"WHERE p.tvl_usd IS NOT NULL "
                f"ORDER BY p.tvl_usd DESC LIMIT {int(limit)}"
            )
        except Exception as e:  # noqa: BLE001
            logger.error("protocol_health_summary failed: %s", e)
            return []

    def recent_whale_movements(self, limit: int = 5) -> list[dict]:
        # fct_whale_movements is per-address aggregate; present the largest-value
        # actors with their dominant token and counterparty count.
        try:
            return self._query(
                f"SELECT coalesce(token_symbol, token_name, '?') AS token, "
                f"total_value AS amount, address AS from_addr, "
                f"concat('+', cast(unique_counterparties AS string), ' cps') AS to_addr, "
                f"tx_count AS timestamp "
                f"FROM {self._fq('fct_whale_movements')} "
                f"WHERE total_value IS NOT NULL "
                f"ORDER BY total_value DESC LIMIT {int(limit)}"
            )
        except Exception as e:  # noqa: BLE001
            logger.error("recent_whale_movements failed: %s", e)
            return []
