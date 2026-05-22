"""Phase 6a action-execution tests with mocked Slack/GitHub/dbt."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.actions.dbt_runner import DbtRunner
from src.actions.executor import execute_actions
from src.actions.github_pr import GitHubPRCreator
from src.actions.slack_notifier import SlackNotifier
from src.agent.types import AgentState, PlannedAction
from src.anomaly_detection.anomaly_events import AnomalyEvent, AnomalyType, Severity

pytestmark = pytest.mark.phase_6


def _state(actions=None, **kw) -> AgentState:
    event = AnomalyEvent(
        AnomalyType.TYPE_CHANGED, Severity.ERROR,
        "source.autonomous_impact_analyst.coingecko.coingecko_coins_markets",
        "current_price", "type changed", "DECIMAL", "STRING", datetime.utcnow(), {},
    )
    return AgentState(
        event=event, overall_risk=kw.get("risk", "high"),
        impact_summary=kw.get("summary", "summary text"),
        affected_paths=[["a", "b"]],
        affected_exposures=[{"name": "defi_market_slack_bot"}],
        test_coverage_per_node={"model.x.stg_y": 0.2},
        recommended_actions=actions or [],
        run_id="run123",
        requires_approval=kw.get("requires_approval", False),
    )


# -- slack notifier -----------------------------------------------------------


def test_slack_blocks_structure():
    n = SlackNotifier("https://hooks.slack.test/x")
    blocks = n.build_blocks(_state(), pr_url="https://github.test/pr/1")
    types = [b["type"] for b in blocks]
    assert types[0] == "header"
    assert "section" in types and "context" in types
    # PR link section present when pr_url supplied
    assert any("Proposed fix PR" in str(b) for b in blocks)


def test_slack_truncates_long_summary():
    n = SlackNotifier("https://hooks.slack.test/x")
    blocks = n.build_blocks(_state(summary="x" * 5000))
    summary_block = blocks[1]["text"]["text"]
    assert len(summary_block) <= 2900


def test_slack_send_success_and_failure():
    n = SlackNotifier("https://hooks.slack.test/x")
    n.client = MagicMock()
    n.client.send.return_value = MagicMock(status_code=200, body="ok")
    assert n.send_impact_alert(_state()) is True
    n.client.send.return_value = MagicMock(status_code=500, body="err")
    assert n.send_impact_alert(_state()) is False
    n.client.send.side_effect = RuntimeError("network")
    assert n.send_impact_alert(_state()) is False


# -- github pr ----------------------------------------------------------------


def _fake_repo():
    repo = MagicMock()
    repo.owner.login = "owner"
    repo.default_branch = "main"
    repo.get_branch.return_value.commit.sha = "abc123"
    repo.get_contents.return_value.sha = "filesha"
    repo.create_pull.return_value.html_url = "https://github.test/owner/repo/pull/7"
    repo.get_pulls.return_value = []  # no existing PR
    return repo


def test_create_fix_pr_returns_url():
    with patch("src.actions.github_pr.Github") as gh:
        repo = _fake_repo()
        gh.return_value.get_repo.return_value = repo
        creator = GitHubPRCreator("tok", "owner/repo")
        url = creator.create_fix_pr(
            event=_state().event, fix_sql="SELECT 1", model_file_path="m.sql",
            impact_summary="s", risk_level="high",
        )
        assert url.endswith("/pull/7")
        repo.create_pull.assert_called_once()
        assert repo.create_pull.call_args.kwargs["draft"] is True


def test_create_fix_pr_idempotent_reuses_open_pr():
    with patch("src.actions.github_pr.Github") as gh:
        repo = _fake_repo()
        existing = MagicMock()
        existing.html_url = "https://github.test/owner/repo/pull/3"
        repo.get_pulls.return_value = [existing]
        gh.return_value.get_repo.return_value = repo
        creator = GitHubPRCreator("tok", "owner/repo")
        url = creator.create_fix_pr(
            event=_state().event, fix_sql="SELECT 1", model_file_path="m.sql",
            impact_summary="s", branch_name="fix/dup",
        )
        assert url.endswith("/pull/3")
        repo.create_pull.assert_not_called()  # reused, not recreated


# -- dbt runner pause lock ----------------------------------------------------


def test_pause_lock_lifecycle(tmp_path, monkeypatch):
    import src.actions.dbt_runner as mod
    monkeypatch.setattr(mod, "_LOCK_PATH", tmp_path / "dbt_pause.lock")
    r = DbtRunner("src/dbt_project")
    assert r.is_paused() is False
    r.create_pause_lock("critical type change")
    assert r.is_paused() is True
    r.remove_pause_lock()
    assert r.is_paused() is False


# -- executor / dispatcher ----------------------------------------------------


def test_executor_dispatches_and_links_pr():
    actions = [
        PlannedAction("log", {}, 0),
        PlannedAction("slack_alert", {"summary": "s"}, 1),
        PlannedAction("github_pr", {"fix_sql": "SELECT 1", "model_path": "m.sql",
                                     "requires_human_review": True}, 2),
    ]
    notifier = MagicMock()
    notifier.send_impact_alert.return_value = True
    pr_creator = MagicMock()
    pr_creator.create_fix_pr.return_value = "https://github.test/pull/9"

    results = execute_actions(_state(actions), {}, notifier=notifier, pr_creator=pr_creator)
    assert results["github_pr"].endswith("/pull/9")
    assert results["slack_alert"] is True
    # PR url threaded into the slack call
    assert notifier.send_impact_alert.call_args.kwargs["pr_url"].endswith("/pull/9")


def test_executor_skips_duplicates():
    actions = [
        PlannedAction("slack_alert", {"suppressed_duplicate": True}, 1),
        PlannedAction("github_pr", {"fix_sql": "x", "suppressed_duplicate": True}, 2),
    ]
    notifier, pr_creator = MagicMock(), MagicMock()
    results = execute_actions(_state(actions), {}, notifier=notifier, pr_creator=pr_creator)
    assert results["slack_alert"] == "skipped_duplicate"
    assert results["github_pr"] == "skipped_duplicate"
    notifier.send_impact_alert.assert_not_called()
    pr_creator.create_fix_pr.assert_not_called()


def test_executor_holds_for_approval():
    actions = [PlannedAction("github_pr", {"fix_sql": "x"}, 2)]
    pr_creator = MagicMock()
    results = execute_actions(_state(actions, requires_approval=True), {},
                              pr_creator=pr_creator, notifier=MagicMock())
    assert results["github_pr"] == "pending_approval"
    pr_creator.create_fix_pr.assert_not_called()
