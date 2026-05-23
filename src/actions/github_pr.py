"""Open a GitHub PR carrying an agent-proposed SQL fix.

LLM-authored SQL is never auto-merged: PRs are opened as drafts and labelled
needs-review (the agent sets requires_human_review on the action). Branch names
are idempotency-keyed so a re-fired action reuses the existing PR rather than
spamming duplicates.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from github import Github, GithubException

logger = logging.getLogger(__name__)


class GitHubPRCreator:
    def __init__(self, token: str, repo: str) -> None:
        self._gh = Github(token)
        self.repo = self._gh.get_repo(repo)

    def _existing_pr_url(self, branch_name: str) -> str | None:
        """Return the URL of an open PR for this branch, if one exists."""
        owner = self.repo.owner.login
        for pr in self.repo.get_pulls(state="open", head=f"{owner}:{branch_name}"):
            return str(pr.html_url)
        return None

    def create_fix_pr(
        self,
        event: Any,
        fix_sql: str,
        model_file_path: str,
        impact_summary: str,
        risk_level: str = "medium",
        branch_name: str | None = None,
        draft: bool = True,
        affected_products: list[str] | None = None,
        affected_dashboards: list[str] | None = None,
    ) -> str:
        atype = getattr(event.anomaly_type, "value", event.anomaly_type)
        if branch_name is None:
            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            branch_name = f"fix/impact-{atype}-{ts}"

        # Idempotency: reuse an existing open PR for this branch.
        existing = self._existing_pr_url(branch_name)
        if existing:
            logger.info("PR already open for %s: %s", branch_name, existing)
            return existing

        default_branch = self.repo.default_branch
        base_sha = self.repo.get_branch(default_branch).commit.sha
        try:
            self.repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=base_sha)
        except GithubException as e:
            if e.status != 422:  # 422 = ref already exists
                raise

        # Update the model file on the branch; capture the original for a diff.
        contents = self.repo.get_contents(model_file_path, ref=branch_name)
        try:
            original_sql = contents.decoded_content.decode()  # type: ignore[union-attr]
        except Exception:  # noqa: BLE001
            original_sql = ""
        self.repo.update_file(
            path=model_file_path,
            message=f"fix({atype}): defensive update to {model_file_path}",
            content=fix_sql,
            sha=contents.sha,  # type: ignore[union-attr]
            branch=branch_name,
        )

        body = self._pr_body(
            event, impact_summary, risk_level, model_file_path,
            original_sql, fix_sql, affected_products or [], affected_dashboards or [],
        )
        pr = self.repo.create_pull(
            title=f"[auto] Fix {atype} on {event.source_node_id}",
            body=body,
            head=branch_name,
            base=default_branch,
            draft=draft,
        )
        try:
            pr.add_to_labels("auto-generated", risk_level, "needs-review")
        except GithubException as e:
            logger.warning("could not add labels: %s", e)
        return str(pr.html_url)

    def merge_pr(self, number: int, method: str = "squash") -> dict:
        """Merge a PR by number; close it as a fallback if merge is blocked."""
        pr = self.repo.get_pull(number)
        try:
            result = pr.merge(merge_method=method)
            return {"merged": bool(result.merged), "message": result.message}
        except GithubException as e:
            logger.warning("merge failed for #%d: %s; closing instead", number, e)
            pr.edit(state="closed")
            return {"merged": False, "message": f"merge failed; PR #{number} closed"}

    def close_pr(self, number: int, comment: str | None = None) -> None:
        pr = self.repo.get_pull(number)
        if comment:
            pr.create_issue_comment(comment)
        pr.edit(state="closed")

    @staticmethod
    def pr_number_from_url(url: str) -> int:
        return int(url.rstrip("/").split("/")[-1])

    @staticmethod
    def _pr_body(
        event: Any, impact_summary: str, risk_level: str, model_file_path: str,
        original_sql: str, fix_sql: str, products: list[str], dashboards: list[str],
    ) -> str:
        atype = getattr(event.anomaly_type, "value", event.anomaly_type)
        col = f" (column `{event.source_column}`)" if event.source_column else ""
        prod = ", ".join(f"`{p}`" for p in products) or "_none detected_"
        dash = ", ".join(f"`{d}`" for d in dashboards) or "_none detected_"
        return (
            f"## Autonomous Impact Analyst — proposed fix\n\n"
            f"**Anomaly:** `{atype}` on `{event.source_node_id}`{col}\n"
            f"**Risk level:** **{risk_level.upper()}**\n\n"
            f"### Impact summary\n{impact_summary}\n\n"
            f"### Affected data products\n{prod}\n\n"
            f"### Dashboards at risk\n{dash}\n\n"
            f"### SQL logic change\n"
            f"`{model_file_path}`\n\n"
            f"<details><summary>Before</summary>\n\n```sql\n{original_sql.strip()}\n```\n"
            f"</details>\n\n"
            f"<details open><summary>After (proposed)</summary>\n\n```sql\n"
            f"{fix_sql.strip()}\n```\n</details>\n\n"
            f"---\n"
            f"This SQL was generated by an LLM and validated only for parse "
            f"correctness. **Human review required before merge.**\n"
        )
