"""
scripts/seed_databricks.py

Fetch live data from CoinGecko, DeFi Llama, and Etherscan and load it as
raw Delta tables in Databricks (schema: `raw`).

Rules (per phase-1 spec):
- All columns stored as STRING. Type casting happens in dbt staging.
- Nested dicts/lists are serialized as JSON strings (not flattened).
- Every table carries an `_extracted_at` UTC ISO8601 STRING column.
- Truncate-and-reload semantics: each run drops and recreates each table.
- Rate limits: CoinGecko 2.5s between calls, Etherscan 0.25s between calls.

Tables written:
    raw.coingecko_coins_markets        (~100 rows)
    raw.coingecko_coins_detail         (~20 rows)
    raw.coingecko_exchanges            (~50 rows)
    raw.defi_llama_protocols           (~200 rows)
    raw.defi_llama_yields_pools        (~500 rows)
    raw.etherscan_eth_transactions     (>0 rows)
    raw.etherscan_token_transfers      (>0 rows)
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
CATALOG = os.environ.get("DATABRICKS_CATALOG", "hive_metastore")
RAW_SCHEMA = os.environ.get("DATABRICKS_SCHEMA_RAW", "raw")

COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")
ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY", "")

COINGECKO_SLEEP = 2.5
ETHERSCAN_SLEEP = 0.25
INSERT_CHUNK = 25

ETHERSCAN_ADDRESSES = [
    "0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8",  # Binance
    "0xDA9dfA130Df4dE4673b89022EE50ff26f6EA73Cf",  # Kraken
    "0x1B3cB81E51011b549d78bf720b0d924ac763A7C2",  # Lido
]
ETHERSCAN_TOKENS = [
    "0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT
    "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
]


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------


def _sql_literal(v: Any) -> str:
    """Render a Python value as a SQL string literal or NULL.

    Dicts and lists are JSON-serialized. Booleans become 'true'/'false'.
    Backslashes are doubled so Spark SQL treats them literally; single
    quotes are doubled per SQL standard.
    """
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        s = "true" if v else "false"
    elif isinstance(v, (dict, list)):
        s = json.dumps(v, default=str, separators=(",", ":"), ensure_ascii=False)
    else:
        s = str(v)
    s = s.replace("\\", "\\\\").replace("'", "''")
    return f"'{s}'"


def _ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _union_columns(records: list[dict]) -> list[str]:
    """Union of keys across records, preserving first-seen order."""
    seen: dict[str, None] = {}
    for r in records:
        for k in r:
            seen.setdefault(k, None)
    return list(seen.keys())


def write_raw_table(cur: Any, table: str, records: list[dict]) -> int:
    """Drop, recreate, and load a STRING-typed Delta table."""
    if not records:
        print(f"  {table}: 0 records — skipping")
        return 0

    extracted_at = now_iso()
    data_cols = _union_columns(records)
    all_cols = data_cols + ["_extracted_at"]
    fq = f"{CATALOG}.{RAW_SCHEMA}.{table}"

    cur.execute(f"DROP TABLE IF EXISTS {fq}")
    col_defs = ", ".join(f"{_ident(c)} STRING" for c in all_cols)
    cur.execute(f"CREATE TABLE {fq} ({col_defs}) USING DELTA")

    col_list = ", ".join(_ident(c) for c in all_cols)
    inserted = 0
    for i in range(0, len(records), INSERT_CHUNK):
        chunk = records[i : i + INSERT_CHUNK]
        rows_sql = []
        for r in chunk:
            vals = [_sql_literal(r.get(c)) for c in data_cols] + [_sql_literal(extracted_at)]
            rows_sql.append("(" + ", ".join(vals) + ")")
        cur.execute(f"INSERT INTO {fq} ({col_list}) VALUES " + ", ".join(rows_sql))
        inserted += len(chunk)

    print(f"  {table}: {inserted} rows, {len(all_cols)} cols")
    return inserted


# ---------------------------------------------------------------------------
# API fetchers
# ---------------------------------------------------------------------------


def _coingecko_get(path: str, params: dict) -> Any:
    full = {**params, "x_cg_demo_api_key": COINGECKO_API_KEY}
    r = requests.get(f"https://api.coingecko.com/api/v3{path}", params=full, timeout=30)
    if r.status_code == 429:
        print("  CoinGecko 429 — sleeping 30s and retrying once")
        time.sleep(30)
        r = requests.get(f"https://api.coingecko.com/api/v3{path}", params=full, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_coingecko_coins_markets() -> list[dict]:
    print("Fetching CoinGecko /coins/markets (per_page=100)...")
    data = _coingecko_get("/coins/markets", {"vs_currency": "usd", "per_page": 100, "page": 1})
    time.sleep(COINGECKO_SLEEP)
    return data


def fetch_coingecko_coins_detail(coin_ids: list[str]) -> list[dict]:
    print(f"Fetching CoinGecko /coins/{{id}} for {len(coin_ids)} coins...")
    out: list[dict] = []
    for cid in coin_ids:
        try:
            d = _coingecko_get(
                f"/coins/{cid}",
                {
                    "localization": "false",
                    "tickers": "false",
                    "market_data": "true",
                    "community_data": "true",
                    "developer_data": "true",
                },
            )
            out.append(d)
        except requests.HTTPError as e:
            print(f"  skip {cid}: {e}")
        time.sleep(COINGECKO_SLEEP)
    return out


def fetch_coingecko_exchanges() -> list[dict]:
    print("Fetching CoinGecko /exchanges (per_page=50)...")
    data = _coingecko_get("/exchanges", {"per_page": 50, "page": 1})
    time.sleep(COINGECKO_SLEEP)
    return data


def fetch_defi_llama_protocols() -> list[dict]:
    print("Fetching DeFi Llama /protocols (first 200)...")
    r = requests.get("https://api.llama.fi/protocols", timeout=60)
    r.raise_for_status()
    return r.json()[:200]


def fetch_defi_llama_yields() -> list[dict]:
    print("Fetching DeFi Llama /yields/pools (first 500)...")
    r = requests.get("https://yields.llama.fi/pools", timeout=60)
    r.raise_for_status()
    return r.json().get("data", [])[:500]


def _etherscan_get(params: dict) -> list[dict]:
    full = {"chainid": "1", "apikey": ETHERSCAN_API_KEY, **params}
    r = requests.get("https://api.etherscan.io/v2/api", params=full, timeout=30)
    r.raise_for_status()
    j = r.json()
    result = j.get("result")
    if isinstance(result, list):
        return result
    if isinstance(result, str):
        if "No transactions found" in result or "No records found" in result:
            return []
        raise RuntimeError(
            f"Etherscan error: status={j.get('status')} "
            f"message={j.get('message')} result={result}"
        )
    return []


def fetch_etherscan_eth_transactions() -> list[dict]:
    print(f"Fetching Etherscan txlist for {len(ETHERSCAN_ADDRESSES)} addresses...")
    out: list[dict] = []
    for addr in ETHERSCAN_ADDRESSES:
        rows = _etherscan_get(
            {
                "module": "account",
                "action": "txlist",
                "address": addr,
                "page": 1,
                "offset": 50,
                "sort": "desc",
            }
        )
        out.extend(rows)
        time.sleep(ETHERSCAN_SLEEP)
    return out


def fetch_etherscan_token_transfers() -> list[dict]:
    print(f"Fetching Etherscan tokentx for {len(ETHERSCAN_TOKENS)} contracts...")
    out: list[dict] = []
    for contract in ETHERSCAN_TOKENS:
        rows = _etherscan_get(
            {
                "module": "account",
                "action": "tokentx",
                "contractaddress": contract,
                "page": 1,
                "offset": 50,
                "sort": "desc",
            }
        )
        out.extend(rows)
        time.sleep(ETHERSCAN_SLEEP)
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    print(f"Connecting to Databricks ({DATABRICKS_HOST})...")
    conn = sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )
    try:
        with conn.cursor() as cur:
            print(f"Ensuring schema {CATALOG}.{RAW_SCHEMA} exists...")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}")

        # Fetch first, then write — fail fast if APIs are down before we
        # destroy the existing tables.
        markets = fetch_coingecko_coins_markets()
        top_ids = [r["id"] for r in markets[:20] if r.get("id")]
        detail = fetch_coingecko_coins_detail(top_ids)
        exchanges = fetch_coingecko_exchanges()
        protocols = fetch_defi_llama_protocols()
        yields = fetch_defi_llama_yields()
        txs = fetch_etherscan_eth_transactions()
        transfers = fetch_etherscan_token_transfers()

        with conn.cursor() as cur:
            print("Writing raw tables...")
            write_raw_table(cur, "coingecko_coins_markets", markets)
            write_raw_table(cur, "coingecko_coins_detail", detail)
            write_raw_table(cur, "coingecko_exchanges", exchanges)
            write_raw_table(cur, "defi_llama_protocols", protocols)
            write_raw_table(cur, "defi_llama_yields_pools", yields)
            write_raw_table(cur, "etherscan_eth_transactions", txs)
            write_raw_table(cur, "etherscan_token_transfers", transfers)
        print("Seed complete.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
