"""Deterministic risk scoring. No LLM — pure weighted arithmetic over graph
features so risk levels are reproducible and auditable."""

from __future__ import annotations

from typing import Literal

_MATERIALIZATION_RISK = {"view": 1.0, "table": 0.7, "incremental": 0.5}

# Multipliers reflecting how dangerous each anomaly kind is to downstream data.
_ANOMALY_MODIFIER = {
    "column_removed": 1.3,
    "type_changed": 1.1,
    "row_count_drop": 0.9,
    "null_ratio_spike": 0.8,
    "freshness_violation": 0.7,
    "test_failure": 1.0,
    "column_added": 0.3,
    "value_range_breach": 0.85,
    "nullability_changed": 0.6,
}


def score_node(
    test_coverage: float,
    fan_out: int,
    distance_to_exposure: int | None,
    materialization: str | None,
    exposure_priority: str | None,
    anomaly_type: str,
    weights: dict,
) -> float:
    """Per-node risk in [0, 1].

    exposure_priority is accepted for interface compatibility; the deterministic
    formula does not use it (priority informs action routing, not the score).
    """
    test_coverage_score = (1.0 - test_coverage) * weights["test_coverage"]
    fan_out_score = min(fan_out / 10.0, 1.0) * weights["fan_out"]
    if distance_to_exposure is not None:
        exposure_score = (1.0 / (distance_to_exposure + 1)) * weights["exposure_distance"]
    else:
        exposure_score = 0.0
    materialization_score = (
        _MATERIALIZATION_RISK.get(materialization or "", 0.5) * weights["materialization"]
    )

    raw_score = test_coverage_score + fan_out_score + exposure_score + materialization_score
    modifier = _ANOMALY_MODIFIER.get(_normalize(anomaly_type), 1.0)
    return min(raw_score * modifier, 1.0)


_SEVERITY_MODIFIER = {
    "info": 0.85,
    "warning": 0.95,
    "error": 1.1,
    "critical": 1.25,
}


def apply_modifiers(
    base_score: float,
    severity: str | None = None,
    confidence: float = 1.0,
    distance_from_source: int = 0,
    reaches_high_priority_exposure: bool = False,
    distance_decay: float = 0.15,
    exposure_priority_boost: float = 1.15,
    severity_modifiers: dict | None = None,
) -> float:
    """Refine a base node score with contextual factors (deterministic).

    - severity: scale by how severe the triggering anomaly is.
    - confidence: column-lineage certainty (0.5-1.0); low confidence => the
      column may not really flow here, so dampen the score.
    - distance_from_source: mild decay the further a node is from the anomaly.
    - reaches_high_priority_exposure: escalate if a high-priority consumer is hit.
    """
    sev_table = severity_modifiers or _SEVERITY_MODIFIER
    sev = sev_table.get(_normalize(severity) if severity else "", 1.0)
    decay = 1.0 / (1.0 + distance_decay * max(distance_from_source, 0))
    # Confidence nudges, not halves: lineage-tracing uncertainty shouldn't
    # erase a real impact. 0.5 conf -> 0.85 factor, 1.0 conf -> 1.0 factor.
    conf = max(min(confidence, 1.0), 0.0)
    conf_factor = 0.7 + 0.3 * conf
    score = base_score * sev * conf_factor * decay
    if reaches_high_priority_exposure:
        score *= exposure_priority_boost
    return min(score, 1.0)


def aggregate_risk(
    node_scores: dict[str, float], thresholds: dict
) -> Literal["low", "medium", "high", "critical"]:
    """Bucket the max node score into a level. >=high -> critical so the
    pause_dbt_run action tier is reachable."""
    if not node_scores:
        return "low"
    top = max(node_scores.values())
    if top >= thresholds["high"]:
        return "critical"
    if top >= thresholds["medium"]:
        return "high"
    if top >= thresholds["low"]:
        return "medium"
    return "low"


def _normalize(anomaly_type: str) -> str:
    # Accept either an AnomalyType enum or its .value string.
    return getattr(anomaly_type, "value", anomaly_type)
