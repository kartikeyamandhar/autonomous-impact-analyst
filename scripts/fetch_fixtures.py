"""
Fetch real API responses and save as fixture JSON files.
Run once to populate specs/fixtures/ with ground truth data shapes.

Usage:
    pip install requests
    export COINGECKO_API_KEY=your_key
    export ETHERSCAN_API_KEY=your_key
    python scripts/fetch_fixtures.py
"""
import os
import json
import time
import requests

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "specs", "fixtures")
os.makedirs(FIXTURES_DIR, exist_ok=True)

COINGECKO_KEY = os.environ.get("COINGECKO_API_KEY", "")
ETHERSCAN_KEY = os.environ.get("ETHERSCAN_API_KEY", "")


def save(name: str, data):
    path = os.path.join(FIXTURES_DIR, name)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved {path} ({len(json.dumps(data))} bytes)")


def fetch_coingecko_coins_markets():
    print("\n--- CoinGecko /coins/markets ---")
    r = requests.get(
        "https://api.coingecko.com/api/v3/coins/markets",
        params={"vs_currency": "usd", "per_page": 5, "page": 1,
                "x_cg_demo_api_key": COINGECKO_KEY},
        timeout=30,
    )
    r.raise_for_status()
    save("coingecko_coins_markets.json", r.json())


def fetch_coingecko_coins_detail():
    print("\n--- CoinGecko /coins/bitcoin ---")
    time.sleep(3)
    r = requests.get(
        "https://api.coingecko.com/api/v3/coins/bitcoin",
        params={"localization": "false", "tickers": "false",
                "market_data": "true", "community_data": "true",
                "developer_data": "true", "x_cg_demo_api_key": COINGECKO_KEY},
        timeout=30,
    )
    r.raise_for_status()
    save("coingecko_coins_detail.json", r.json())


def fetch_coingecko_exchanges():
    print("\n--- CoinGecko /exchanges ---")
    time.sleep(3)
    r = requests.get(
        "https://api.coingecko.com/api/v3/exchanges",
        params={"per_page": 5, "page": 1, "x_cg_demo_api_key": COINGECKO_KEY},
        timeout=30,
    )
    r.raise_for_status()
    save("coingecko_exchanges.json", r.json())


def fetch_defi_llama_protocols():
    print("\n--- DeFi Llama /protocols ---")
    r = requests.get("https://api.llama.fi/protocols", timeout=30)
    r.raise_for_status()
    save("defi_llama_protocols.json", r.json()[:5])


def fetch_defi_llama_yields():
    print("\n--- DeFi Llama /yields/pools ---")
    r = requests.get("https://yields.llama.fi/pools", timeout=60)
    r.raise_for_status()
    data = r.json().get("data", [])[:5]
    save("defi_llama_yields_pools.json", data)


def fetch_etherscan_transactions():
    print("\n--- Etherscan txlist ---")
    r = requests.get(
        "https://api.etherscan.io/v2/api",
        params={"chainid": "1", "module": "account", "action": "txlist",
                "address": "0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8",
                "page": 1, "offset": 3, "sort": "desc",
                "apikey": ETHERSCAN_KEY},
        timeout=30,
    )
    r.raise_for_status()
    save("etherscan_transactions.json", r.json())


def fetch_etherscan_token_transfers():
    print("\n--- Etherscan tokentx ---")
    time.sleep(0.3)
    r = requests.get(
        "https://api.etherscan.io/v2/api",
        params={"chainid": "1", "module": "account", "action": "tokentx",
                "contractaddress": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                "page": 1, "offset": 3, "sort": "desc",
                "apikey": ETHERSCAN_KEY},
        timeout=30,
    )
    r.raise_for_status()
    save("etherscan_token_transfers.json", r.json())


if __name__ == "__main__":
    print("Fetching API fixtures...")
    fetch_coingecko_coins_markets()
    fetch_coingecko_coins_detail()
    fetch_coingecko_exchanges()
    fetch_defi_llama_protocols()
    fetch_defi_llama_yields()
    fetch_etherscan_transactions()
    fetch_etherscan_token_transfers()
    print("\nDone. Fixtures saved to specs/fixtures/")
    print("Commit these files to the repo.")
