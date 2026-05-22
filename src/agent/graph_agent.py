"""LangGraph agent: AnomalyEvent -> graph traversal -> deterministic risk +
actions -> Claude-generated summary/fix.

All reasoning (impact, risk, action selection) is deterministic graph work.
Claude is used only to (a) phrase the summary and (b) draft a SQL fix — never
to decide risk or actions.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import sqlglot
import structlog
from langgraph.graph import END, START, StateGraph

from src.agent.action_planner import _is_schema_change, plan_actions
from src.agent.correlator import correlate_events, primary_event
from src.agent.incident_store import IncidentStore, incident_key
from src.agent.risk_scorer import aggregate_risk, apply_modifiers, score_node
from src.agent.types import AgentState
from src.graph_engine.queries import GraphQueries

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.KeyValueRenderer(key_order=["run_id", "event"]),
    ],
)
log = structlog.get_logger("impact_agent")

_PROMPT_DIR = Path(__file__).parent / "prompts"
DEFAULT_MODEL = os.environ.get("AGENT_MODEL", "claude-haiku-4-5-20251001")
MAX_SUMMARY_TOKENS = 600
MAX_FIX_TOKENS = 1500


def _prompt(name: str) -> str:
    return (_PROMPT_DIR / name).read_text()


def _claude_text(
    client: Any, system: str, user: str, max_tokens: int, agent_cfg: dict,
    logger: Any = log, purpose: str = "llm",
) -> tuple[str, str]:
    """Call Claude with retry/backoff, model fallback, prompt caching.

    Returns (text, model_used). Logs token usage for cost observability.
    """
    models = [agent_cfg.get("model", DEFAULT_MODEL)]
    fallback = agent_cfg.get("fallback_model")
    if fallback and fallback not in models:
        models.append(fallback)
    max_retries = max(int(agent_cfg.get("max_retries", 3)), 1)
    backoff = float(agent_cfg.get("retry_backoff_seconds", 2))
    if agent_cfg.get("prompt_caching", True):
        system_param: Any = [
            {"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}
        ]
    else:
        system_param = system

    last_err: Exception | None = None
    for attempt in range(max_retries):
        model = models[min(attempt, len(models) - 1)]
        try:
            resp = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=system_param,
                messages=[{"role": "user", "content": user}],
            )
            usage = getattr(resp, "usage", None)
            logger.info(
                "llm_call",
                purpose=purpose,
                model=model,
                attempt=attempt,
                input_tokens=getattr(usage, "input_tokens", None),
                output_tokens=getattr(usage, "output_tokens", None),
                cache_read_tokens=getattr(usage, "cache_read_input_tokens", None),
            )
            parts = [
                getattr(b, "text", "") for b in resp.content
                if getattr(b, "type", "") == "text"
            ]
            return (("".join(parts) or getattr(resp.content[0], "text", "")).strip(), model)
        except Exception as e:  # noqa: BLE001 - retried below, surfaced if all fail
            last_err = e
            logger.warning("llm_call_failed", purpose=purpose, attempt=attempt,
                           model=model, error=str(e))
            if attempt < max_retries - 1:
                time.sleep(backoff * (2 ** attempt))
    raise last_err if last_err else RuntimeError("claude call failed")


def _collect_exposures(queries: GraphQueries, node_id: str) -> list[dict]:
    seen: dict[str, dict] = {}
    for path in queries.paths_to_exposures(node_id):
        if not path:
            continue
        exp = path[-1]
        uid = exp.get("unique_id")
        if uid and uid not in seen:
            meta = queries.node_metadata(uid) or {}
            seen[uid] = {
                "unique_id": uid,
                "name": exp.get("name"),
                "type": meta.get("exposure_type"),
                "priority": meta.get("priority"),
            }
    return list(seen.values())


def run_agent(
    event: Any,
    neo4j_driver: Any,
    anthropic_client: Any,
    config: dict,
    queries: GraphQueries | None = None,
    incident_store: IncidentStore | None = None,
) -> AgentState:
    queries = queries or GraphQueries(neo4j_driver)
    agent_cfg = config.get("agent", {})
    if incident_store is None:
        incident_store = IncidentStore(agent_cfg.get("incident_dir", "data/incidents"))
    weights = config["risk_scoring"]["weights"]
    thresholds = config["risk_scoring"]["thresholds"]
    decay = float(config["risk_scoring"].get("distance_decay", 0.15))
    exp_boost = float(config["risk_scoring"].get("exposure_priority_boost", 1.15))
    severity_mods = config.get("severity_modifiers")
    run_id = uuid.uuid4().hex[:12]
    rlog = log.bind(run_id=run_id, source_node_id=event.source_node_id)
    ctx: dict[str, Any] = {"base_nodes": set(), "mat": {}, "conf": {}, "dist": {}, "trace": []}

    def _trace(node: str, **fields: Any) -> None:
        elapsed = time.perf_counter() - ctx.get("_t0", time.perf_counter())
        ctx["trace"].append({"node": node, "elapsed_s": round(elapsed, 4), **fields})
        ctx["_t0"] = time.perf_counter()
        rlog.info(node, **fields)

    # -- nodes ----------------------------------------------------------------

    def receive_event(state: AgentState) -> dict:
        ctx["_t0"] = time.perf_counter()
        meta = queries.node_metadata(state.event.source_node_id)
        ctx["root_meta"] = meta
        ctx["abort"] = meta is None
        key = incident_key(state.event)
        prior = incident_store.prior_occurrences(key)
        ctx["incident_key"] = key
        errors: list = []
        if meta is None:
            # Loud failure: a referenced node missing from the graph is a
            # silent-false-negative risk, so surface it rather than scoring low.
            msg = (
                f"node not found in graph: {state.event.source_node_id} "
                f"(stale lineage or id mismatch?)"
            )
            rlog.error("node_not_found", node_id=state.event.source_node_id)
            errors.append(msg)
        _trace("receive_event", node_id=state.event.source_node_id,
               found=meta is not None, prior_occurrences=prior)
        return {"incident_key": key, "prior_occurrences": prior,
                "run_id": run_id, "errors": errors}

    def traverse_forward(state: AgentState) -> dict:
        ev = state.event
        atype = getattr(ev.anomaly_type, "value", ev.anomaly_type)
        paths: list[list[str]] = []
        pruned: list[list[str]] = []
        base: set[str] = set()
        conf: dict[str, float] = {}
        dist: dict[str, int] = {}

        if atype == "test_failure":
            model_id = queries.tested_model(ev.source_node_id)
            if model_id:
                downstream = queries.downstream_models(model_id)
                paths = [[model_id, d["unique_id"]] for d in downstream] or [[model_id]]
                base = {model_id} | {d["unique_id"] for d in downstream}
                for d in downstream:
                    dist[d["unique_id"]] = 1
                dist[model_id] = 0
        elif ev.source_column:
            for item in queries.column_lineage_forward(ev.source_node_id, ev.source_column):
                node_path = [p["model"] for p in item["path"] if p.get("model")]
                if not node_path:
                    continue
                paths.append(node_path)
                base.update(node_path)
                c = float(item.get("confidence", 1.0))
                for idx, node in enumerate(node_path):
                    conf[node] = max(conf.get(node, 0.0), c)
                    dist[node] = min(dist.get(node, idx), idx)
            # Real pruning (#1): downstream models that the column does NOT flow
            # into are recorded as pruned rather than scored.
            flow_nodes = set(base)
            for d in queries.downstream_models(ev.source_node_id):
                if d["unique_id"] not in flow_nodes:
                    pruned.append([ev.source_node_id, d["unique_id"]])
            if not paths:
                downstream = queries.downstream_models(ev.source_node_id)
                paths = [[ev.source_node_id, d["unique_id"]] for d in downstream]
                base = {ev.source_node_id} | {d["unique_id"] for d in downstream}
        else:
            downstream = queries.downstream_models(ev.source_node_id)
            paths = [[ev.source_node_id, d["unique_id"]] for d in downstream] or [
                [ev.source_node_id]
            ]
            base = {ev.source_node_id} | {d["unique_id"] for d in downstream}
            for d in downstream:
                dist[d["unique_id"]] = 1

        base.add(ev.source_node_id)
        dist.setdefault(ev.source_node_id, 0)
        ctx["base_nodes"] = base
        ctx["conf"] = conf
        ctx["dist"] = dist
        exposures = _collect_exposures(queries, ev.source_node_id)
        ctx["high_priority_exposure"] = any(
            e.get("priority") == "high" for e in exposures
        )
        _trace("traverse_forward", affected=len(paths), pruned=len(pruned),
               exposures=[e["name"] for e in exposures])
        return {
            "affected_paths": paths,
            "pruned_paths": pruned,
            "affected_exposures": exposures,
        }

    def prune_irrelevant(state: AgentState) -> dict:
        # Pruning already computed during column-lineage traversal; this node
        # exists to honor the pipeline shape and log the outcome.
        _trace("prune_irrelevant", kept=len(state.affected_paths),
               pruned=len(state.pruned_paths))
        return {}

    def assess_coverage(state: AgentState) -> dict:
        coverage: dict[str, float] = {}
        mats: dict[str, str | None] = {}
        for node in ctx["base_nodes"]:
            meta = queries.node_metadata(node)
            if not meta:
                continue
            mats[node] = meta.get("materialization")
            coverage[node] = queries.test_coverage(node)["coverage_ratio"]
        ctx["mat"] = mats
        _trace("assess_coverage", scored_nodes=len(coverage))
        return {"test_coverage_per_node": coverage}

    def score_risk(state: AgentState) -> dict:
        ev = state.event
        atype = getattr(ev.anomaly_type, "value", ev.anomaly_type)
        severity = getattr(ev.severity, "value", ev.severity)
        scores: dict[str, float] = {}
        for node in ctx["base_nodes"]:
            cov = state.test_coverage_per_node.get(node, 0.0)
            fo = queries.fan_out(node)
            dist_to_exp = queries.distance_to_nearest_exposure(node)
            base = score_node(
                cov, fo, dist_to_exp, ctx["mat"].get(node), None, atype, weights
            )
            scores[node] = apply_modifiers(
                base,
                severity=severity,
                confidence=ctx["conf"].get(node, 1.0),
                distance_from_source=ctx["dist"].get(node, 0),
                reaches_high_priority_exposure=ctx.get("high_priority_exposure", False),
                distance_decay=decay,
                exposure_priority_boost=exp_boost,
                severity_modifiers=severity_mods,
            )
        overall = aggregate_risk(scores, thresholds)
        _trace("score_risk", overall=overall,
               top=sorted(scores.items(), key=lambda kv: -kv[1])[:3])
        return {"risk_scores": scores, "overall_risk": overall}

    def select_action(state: AgentState) -> dict:
        actions = plan_actions(
            state.overall_risk, state.event, state.affected_paths,
            state.affected_exposures, config,
        )
        # Dedup (#7): suppress duplicate actions seen within the window.
        key = ctx["incident_key"]
        window = int(agent_cfg.get("dedup_window_minutes", 60))
        is_dup = incident_store.is_duplicate(key, window)
        day = datetime.utcnow().strftime("%Y%m%d")
        for a in actions:
            # Deterministic idempotency key so Phase 6 executors can dedupe at
            # the Slack/GitHub boundary (exactly-once side effects).
            a.payload["idempotency_key"] = f"{key}:{day}:{a.action_type}"
            if is_dup and a.action_type in ("slack_alert", "github_pr"):
                a.payload["suppressed_duplicate"] = True
        count = incident_store.record(key, state.event, state.overall_risk)
        requires_approval = bool(agent_cfg.get("require_approval", False)) and any(
            a.action_type in ("github_pr", "pause_dbt_run") for a in actions
        )
        _trace("select_action", actions=[a.action_type for a in actions],
               duplicate=is_dup, occurrence=count, requires_approval=requires_approval)
        return {
            "recommended_actions": actions,
            "prior_occurrences": count - 1,
            "requires_approval": requires_approval,
        }

    def generate_summary(state: AgentState) -> dict:
        payload = _summary_payload(state, ctx)
        try:
            summary, _ = _claude_text(
                anthropic_client, _prompt("impact_summary.txt"),
                json.dumps(payload, indent=2), MAX_SUMMARY_TOKENS, agent_cfg,
                logger=rlog, purpose="summary",
            )
        except Exception as e:  # noqa: BLE001 - never fail the pipeline on LLM error
            summary = (
                f"{payload['event']['description']} Overall risk: "
                f"{state.overall_risk}. (summary generation failed: {e})"
            )
        for action in state.recommended_actions:
            if action.action_type == "slack_alert":
                action.payload["summary"] = summary
        _trace("generate_summary", chars=len(summary))
        return {
            "impact_summary": summary,
            "summary_payload": payload,
            "recommended_actions": state.recommended_actions,
        }

    def generate_fix(state: AgentState) -> dict:
        staging_sql = _affected_staging_sql(queries, ctx, state)
        actions = state.recommended_actions
        if not staging_sql:
            return {"recommended_actions": [a for a in actions if a.action_type != "github_pr"]}
        user = (
            f"Anomaly: {state.event.description}\n\n"
            f"Compiled SQL of the affected staging model:\n{staging_sql}"
        )
        try:
            fix, fix_model = _claude_text(
                anthropic_client, _prompt("fix_generation.txt"), user,
                MAX_FIX_TOKENS, agent_cfg, logger=rlog, purpose="fix",
            )
        except Exception:  # noqa: BLE001
            fix, fix_model = "", ""
        if fix and _valid_sql(fix):
            for action in actions:
                if action.action_type == "github_pr":
                    action.payload["fix_sql"] = fix
                    action.payload["model_path"] = ctx.get("staging_path", "")
                    # Provenance + mandatory review: LLM-authored SQL must never
                    # be auto-merged (untrusted-data -> LLM -> committed code).
                    action.payload["fix_provenance"] = {
                        "generated_by": "llm",
                        "model": fix_model,
                        "generated_at": datetime.utcnow().isoformat(),
                        "validated": "sqlglot_parse",
                    }
                    action.payload["requires_human_review"] = True
            _trace("generate_fix", valid=True, model=fix_model)
            return {"fix_suggestion": fix, "recommended_actions": actions}
        _trace("generate_fix", valid=False)
        return {
            "fix_suggestion": None,
            "recommended_actions": [a for a in actions if a.action_type != "github_pr"],
        }

    # -- routing --------------------------------------------------------------

    def after_receive(state: AgentState) -> str:
        return END if ctx.get("abort") else "traverse_forward"

    def after_prune(state: AgentState) -> str:
        return "assess_coverage" if state.affected_paths else "select_action"

    def after_summary(state: AgentState) -> str:
        has_pr = any(a.action_type == "github_pr" for a in state.recommended_actions)
        return "generate_fix" if has_pr and _is_schema_change(state.event) else END

    # -- graph ----------------------------------------------------------------

    builder = StateGraph(AgentState)
    for name, fn in [
        ("receive_event", receive_event),
        ("traverse_forward", traverse_forward),
        ("prune_irrelevant", prune_irrelevant),
        ("assess_coverage", assess_coverage),
        ("score_risk", score_risk),
        ("select_action", select_action),
        ("generate_summary", generate_summary),
        ("generate_fix", generate_fix),
    ]:
        builder.add_node(name, fn)

    builder.add_edge(START, "receive_event")
    builder.add_conditional_edges(
        "receive_event", after_receive, {"traverse_forward": "traverse_forward", END: END}
    )
    builder.add_edge("traverse_forward", "prune_irrelevant")
    builder.add_conditional_edges(
        "prune_irrelevant", after_prune,
        {"assess_coverage": "assess_coverage", "select_action": "select_action"},
    )
    builder.add_edge("assess_coverage", "score_risk")
    builder.add_edge("score_risk", "select_action")
    builder.add_edge("select_action", "generate_summary")
    builder.add_conditional_edges(
        "generate_summary", after_summary, {"generate_fix": "generate_fix", END: END}
    )
    builder.add_edge("generate_fix", END)

    app = builder.compile()
    result = app.invoke(AgentState(event=event))  # type: ignore[arg-type]
    state = result if isinstance(result, AgentState) else AgentState(**result)
    return state


def run_agent_batch(
    events: list[Any],
    neo4j_driver: Any,
    anthropic_client: Any,
    config: dict,
    queries: GraphQueries | None = None,
    incident_store: IncidentStore | None = None,
) -> list[AgentState]:
    """Correlate simultaneous anomalies into incidents and run the agent once
    per incident (on the highest-severity event), attaching the others (#11)."""
    window = int(config.get("agent", {}).get("correlation_window_minutes", 15))
    results: list[AgentState] = []
    for incident in correlate_events(events, window):
        primary = primary_event(incident)
        state = run_agent(
            primary, neo4j_driver, anthropic_client, config, queries, incident_store
        )
        state.correlated_events = [
            {
                "anomaly_type": getattr(e.anomaly_type, "value", e.anomaly_type),
                "severity": getattr(e.severity, "value", e.severity),
                "source_node_id": e.source_node_id,
                "source_column": e.source_column,
            }
            for e in incident
            if e is not primary
        ]
        results.append(state)
    return results


# -- helpers ------------------------------------------------------------------


def _summary_payload(state: AgentState, ctx: dict) -> dict:
    ev = state.event
    top_nodes = sorted(state.risk_scores.items(), key=lambda kv: kv[1], reverse=True)[:5]
    coverage_gaps = {
        node: round(ratio, 2)
        for node, ratio in state.test_coverage_per_node.items()
        if ratio < 0.5
    }
    return {
        "event": {
            "type": getattr(ev.anomaly_type, "value", ev.anomaly_type),
            "severity": getattr(ev.severity, "value", ev.severity),
            "source_node_id": ev.source_node_id,
            "source_column": ev.source_column,
            "description": ev.description,
            "previous_value": ev.previous_value,
            "current_value": ev.current_value,
        },
        "affected_path_count": len(state.affected_paths),
        "pruned_path_count": len(state.pruned_paths),
        "coverage_gaps": coverage_gaps,
        "overall_risk": state.overall_risk,
        "top_risk_nodes": [{"node": n, "score": round(s, 3)} for n, s in top_nodes],
        "affected_exposures": state.affected_exposures,
        "recommended_actions": [a.action_type for a in state.recommended_actions],
        "prior_occurrences": state.prior_occurrences,
        "requires_approval": state.requires_approval,
        "trace": ctx.get("trace", []),
    }


def _affected_staging_sql(queries: GraphQueries, ctx: dict, state: AgentState) -> str | None:
    """Pick the staging model on the affected column path (#9), not just any
    staging node."""
    path_nodes = [n for path in state.affected_paths for n in path]
    ordered = path_nodes + list(ctx["base_nodes"])
    for node in ordered:
        meta = queries.node_metadata(node)
        if meta and meta.get("layer") == "staging" and meta.get("compiled_sql"):
            ctx["staging_path"] = f"models/staging/{meta.get('name')}.sql"
            return str(meta["compiled_sql"])
    return None


def _valid_sql(sql: str) -> bool:
    try:
        parsed = sqlglot.parse(sql, dialect="databricks")
        return bool(parsed and parsed[0] is not None)
    except Exception:  # noqa: BLE001
        return False


def main() -> None:
    from datetime import datetime

    import yaml  # type: ignore[import-untyped]
    from anthropic import Anthropic
    from dotenv import load_dotenv
    from neo4j import GraphDatabase

    from src.anomaly_detection.anomaly_events import AnomalyEvent, AnomalyType, Severity

    load_dotenv()
    event = AnomalyEvent(
        anomaly_type=AnomalyType.NULL_RATIO_SPIKE,
        severity=Severity.WARNING,
        source_node_id="source.autonomous_impact_analyst.coingecko.coingecko_coins_markets",
        source_column="current_price",
        description="Null ratio on current_price spiked from 0.0 to 0.5",
        previous_value="0.0",
        current_value="0.5",
        detected_at=datetime.utcnow(),
        metadata={},
    )
    driver = GraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=(os.environ["NEO4J_USER"], os.environ["NEO4J_PASSWORD"]),
    )
    config = yaml.safe_load(open("config/settings.yml"))
    state = run_agent(event, driver, Anthropic(), config)
    print("Risk:", state.overall_risk)
    print("Actions:", [a.action_type for a in state.recommended_actions])
    print("Prior occurrences:", state.prior_occurrences)
    print("Summary:", state.impact_summary[:300])
    driver.close()


if __name__ == "__main__":
    main()
