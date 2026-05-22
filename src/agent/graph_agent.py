"""LangGraph agent: AnomalyEvent -> graph traversal -> deterministic risk +
actions -> Claude-generated summary/fix.

All reasoning (impact, risk, action selection) is deterministic graph work.
Claude is used only to (a) phrase the summary and (b) draft a SQL fix — never
to decide risk or actions.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import sqlglot
from langgraph.graph import END, START, StateGraph

from src.agent.action_planner import _is_schema_change, plan_actions
from src.agent.risk_scorer import aggregate_risk, score_node
from src.agent.types import AgentState
from src.graph_engine.queries import GraphQueries

_PROMPT_DIR = Path(__file__).parent / "prompts"
DEFAULT_MODEL = os.environ.get("AGENT_MODEL", "claude-haiku-4-5-20251001")
MAX_SUMMARY_TOKENS = 600
MAX_FIX_TOKENS = 1500


def _prompt(name: str) -> str:
    return (_PROMPT_DIR / name).read_text()


def _claude_text(client: Any, system: str, user: str, max_tokens: int) -> str:
    resp = client.messages.create(
        model=DEFAULT_MODEL,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    parts = [getattr(b, "text", "") for b in resp.content if getattr(b, "type", "") == "text"]
    return ("".join(parts) or getattr(resp.content[0], "text", "")).strip()


def _resolve_tested_model(queries: GraphQueries, test_id: str) -> str | None:
    cypher = (
        "MATCH (t {unique_id: $id})-[:TESTS]->(x) "
        "RETURN x.unique_id AS uid, x.model_unique_id AS model, labels(x)[0] AS label LIMIT 1"
    )
    with queries.driver.session() as session:
        rec = session.run(cypher, id=test_id).single()
    if not rec:
        return None
    if rec["label"] == "Column" and rec["model"]:
        return str(rec["model"])
    return str(rec["uid"])


def _collect_exposures(queries: GraphQueries, node_id: str) -> list[dict]:
    seen: dict[str, dict] = {}
    for path in queries.paths_to_exposures(node_id):
        if not path:
            continue
        exp = path[-1]
        uid = exp.get("unique_id")
        if uid and uid not in seen:
            seen[uid] = {
                "unique_id": uid,
                "name": exp.get("name"),
                "type": None,
                "priority": None,
            }
    return list(seen.values())


def run_agent(
    event: Any,
    neo4j_driver: Any,
    anthropic_client: Any,
    config: dict,
    queries: GraphQueries | None = None,
) -> AgentState:
    queries = queries or GraphQueries(neo4j_driver)
    weights = config["risk_scoring"]["weights"]
    thresholds = config["risk_scoring"]["thresholds"]
    ctx: dict[str, Any] = {"base_nodes": set(), "mat": {}}

    # -- nodes ----------------------------------------------------------------

    def receive_event(state: AgentState) -> dict:
        meta = queries.node_metadata(state.event.source_node_id)
        ctx["root_meta"] = meta
        ctx["abort"] = meta is None
        return {}

    def traverse_forward(state: AgentState) -> dict:
        ev = state.event
        atype = getattr(ev.anomaly_type, "value", ev.anomaly_type)
        paths: list[list[str]] = []
        base: set[str] = set()

        if atype == "test_failure":
            model_id = _resolve_tested_model(queries, ev.source_node_id)
            if model_id:
                downstream = queries.downstream_models(model_id)
                paths = [[model_id, d["unique_id"]] for d in downstream] or [[model_id]]
                base = {model_id} | {d["unique_id"] for d in downstream}
        elif ev.source_column:
            for item in queries.column_lineage_forward(ev.source_node_id, ev.source_column):
                node_path = [p["model"] for p in item["path"] if p.get("model")]
                if node_path:
                    paths.append(node_path)
                    base.update(node_path)
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

        base.add(ev.source_node_id)
        ctx["base_nodes"] = base
        exposures = _collect_exposures(queries, ev.source_node_id)
        return {"affected_paths": paths, "affected_exposures": exposures}

    def prune_irrelevant(state: AgentState) -> dict:
        # Column-level paths already came from DERIVES_FROM (only columns that
        # actually flow), so they are pre-pruned. Model-level paths have no
        # column to prune against. Pass through.
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
        return {"test_coverage_per_node": coverage}

    def score_risk(state: AgentState) -> dict:
        ev = state.event
        atype = getattr(ev.anomaly_type, "value", ev.anomaly_type)
        scores: dict[str, float] = {}
        for node in ctx["base_nodes"]:
            cov = state.test_coverage_per_node.get(node, 0.0)
            fo = queries.fan_out(node)
            dist = queries.distance_to_nearest_exposure(node)
            scores[node] = score_node(
                cov, fo, dist, ctx["mat"].get(node), None, atype, weights
            )
        overall = aggregate_risk(scores, thresholds)
        return {"risk_scores": scores, "overall_risk": overall}

    def select_action(state: AgentState) -> dict:
        actions = plan_actions(
            state.overall_risk,
            state.event,
            state.affected_paths,
            state.affected_exposures,
            config,
        )
        return {"recommended_actions": actions}

    def generate_summary(state: AgentState) -> dict:
        payload = _summary_payload(state)
        try:
            summary = _claude_text(
                anthropic_client, _prompt("impact_summary.txt"),
                json.dumps(payload, indent=2), MAX_SUMMARY_TOKENS,
            )
        except Exception as e:  # noqa: BLE001 - never fail the pipeline on LLM error
            summary = (
                f"{payload['event']['description']} "
                f"Overall risk: {state.overall_risk}. "
                f"(summary generation failed: {e})"
            )
        for action in state.recommended_actions:
            if action.action_type == "slack_alert":
                action.payload["summary"] = summary
        return {"impact_summary": summary, "recommended_actions": state.recommended_actions}

    def generate_fix(state: AgentState) -> dict:
        staging_sql = _affected_staging_sql(queries, ctx)
        actions = state.recommended_actions
        if not staging_sql:
            return {"recommended_actions": [a for a in actions if a.action_type != "github_pr"]}
        user = (
            f"Anomaly: {state.event.description}\n\n"
            f"Compiled SQL of the affected staging model:\n{staging_sql}"
        )
        try:
            fix = _claude_text(
                anthropic_client, _prompt("fix_generation.txt"), user, MAX_FIX_TOKENS
            )
        except Exception:  # noqa: BLE001
            fix = ""
        if fix and _valid_sql(fix):
            for action in actions:
                if action.action_type == "github_pr":
                    action.payload["fix_sql"] = fix
                    action.payload["model_path"] = ctx.get("staging_path", "")
            return {"fix_suggestion": fix, "recommended_actions": actions}
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
        "prune_irrelevant",
        after_prune,
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
    if isinstance(result, AgentState):
        return result
    return AgentState(**result)


# -- helpers ------------------------------------------------------------------


def _summary_payload(state: AgentState) -> dict:
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
    }


def _affected_staging_sql(queries: GraphQueries, ctx: dict) -> str | None:
    for node in ctx["base_nodes"]:
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
    import yaml  # type: ignore[import-untyped]
    from anthropic import Anthropic
    from dotenv import load_dotenv
    from neo4j import GraphDatabase

    from src.anomaly_detection.anomaly_events import AnomalyEvent, AnomalyType, Severity

    load_dotenv()
    from datetime import datetime

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
    print("Summary:", state.impact_summary[:300])
    driver.close()


if __name__ == "__main__":
    main()
