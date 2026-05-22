"""Phase 6b exposure-bot tests: mart query mapping, degraded mode, posting."""

from unittest.mock import MagicMock

import pytest

from src.exposure_bot.mart_queries import MartQueries
from src.exposure_bot.slack_bot import ExposureBot

pytestmark = pytest.mark.phase_6


class _Cursor:
    def __init__(self, rows, cols, raise_exc=False):
        self._rows, self._cols, self._raise = rows, cols, raise_exc
        self.description = [(c,) for c in cols]

    def execute(self, sql):
        if self._raise:
            raise RuntimeError("table not found")

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class _Conn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def test_mart_query_maps_rows():
    cur = _Cursor([("Bitcoin", 76755.0, 1.5e12, 2.8e10, -0.4)],
                  ["token", "price", "market_cap", "volume_24h", "price_change_pct"])
    mq = MartQueries(_Conn(cur))
    rows = mq.top_tokens_summary(5)
    assert rows == [{"token": "Bitcoin", "price": 76755.0, "market_cap": 1.5e12,
                     "volume_24h": 2.8e10, "price_change_pct": -0.4}]


def test_mart_query_never_crashes():
    mq = MartQueries(_Conn(_Cursor([], [], raise_exc=True)))
    assert mq.top_tokens_summary() == []
    assert mq.protocol_health_summary() == []
    assert mq.recent_whale_movements() == []


def _bot_with(tokens, protocols, whales) -> ExposureBot:
    bot = ExposureBot.__new__(ExposureBot)  # skip __init__ (no real Slack client)
    bot.client = MagicMock()
    bot.channel = "C123"
    bot.mq = MagicMock()
    bot.mq.top_tokens_summary.return_value = tokens
    bot.mq.protocol_health_summary.return_value = protocols
    bot.mq.recent_whale_movements.return_value = whales
    return bot


def test_summary_healthy_has_no_warning():
    bot = _bot_with(
        [{"token": "Bitcoin", "price": 1.0, "market_cap": 1.0, "price_change_pct": 0.1}],
        [{"protocol": "Lido", "tvl": 1.0, "avg_yield": 2.0, "chain_count": 3}],
        [{"token": "USDT", "amount": 1.0, "from_addr": "0x", "to_addr": "+1 cps"}],
    )
    blocks = bot.format_market_summary()
    text = str(blocks)
    assert "Data unavailable" not in text
    assert "DEGRADED" not in text
    assert "Top tokens" in text and "Protocol health" in text


def test_summary_degrades_when_section_empty():
    # tokens query returned [] (e.g. upstream staging model broken)
    bot = _bot_with([], [{"protocol": "Lido", "tvl": 1.0, "avg_yield": 2.0, "chain_count": 3}],
                    [{"token": "USDT", "amount": 1.0, "from_addr": "0x", "to_addr": "+1 cps"}])
    blocks = bot.format_market_summary()
    text = str(blocks)
    assert "Data unavailable: top tokens" in text
    assert "DEGRADED" in text


def test_post_returns_bool():
    bot = _bot_with([{"token": "B", "price": 1, "market_cap": 1, "price_change_pct": 0}], [], [])
    bot.client.chat_postMessage.return_value = {"ok": True}
    assert bot.post_market_summary() is True
    bot.client.chat_postMessage.side_effect = RuntimeError("slack down")
    assert bot.post_market_summary() is False
