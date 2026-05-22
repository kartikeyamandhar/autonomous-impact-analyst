"""Canonical dbt node-id construction and parsing.

`source_node_id` is the join key tying detectors -> graph -> agent together by
string equality, so it must be built in exactly one place. A mismatch here
causes silent false negatives (an event resolves to no graph node), so we also
expose validation the agent uses to fail loudly instead of quietly.
"""

from __future__ import annotations

PROJECT = "autonomous_impact_analyst"

_SOURCE_PREFIXES = ("coingecko", "defi_llama", "etherscan")


def source_name(table: str) -> str:
    for prefix in _SOURCE_PREFIXES:
        if table.startswith(prefix + "_"):
            return prefix
    return table.split("_")[0]


def source_node_id(table: str) -> str:
    return f"source.{PROJECT}.{source_name(table)}.{table}"


def model_node_id(name: str) -> str:
    return f"model.{PROJECT}.{name}"


def is_source(node_id: str) -> bool:
    return node_id.startswith("source.")


def is_test(node_id: str) -> bool:
    return node_id.startswith("test.")
