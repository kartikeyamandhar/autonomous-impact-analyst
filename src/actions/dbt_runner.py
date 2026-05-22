"""Programmatic dbt control: build/test/docs/freshness + a pause lockfile.

The pause lock lets a critical-risk decision halt scheduled dbt runs (the
pause_dbt_run action). Orchestration (Phase 8) checks is_paused() before
building.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_LOCK_PATH = Path("data/locks/dbt_pause.lock")


class DbtRunner:
    def __init__(self, project_dir: str) -> None:
        self.project_dir = Path(project_dir)

    def _run(self, args: list[str]) -> subprocess.CompletedProcess:
        env = {**os.environ, "DBT_PROFILES_DIR": str(self.project_dir)}
        return subprocess.run(
            ["dbt", *args],
            cwd=self.project_dir,
            env=env,
            capture_output=True,
            text=True,
            timeout=900,
            check=False,
        )

    def _read_target(self, name: str) -> dict:
        path = self.project_dir / "target" / name
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text())
        except json.JSONDecodeError:
            return {}

    def trigger_build(self, select: str) -> dict:
        self._run(["build", "--select", select])
        return self._read_target("run_results.json")

    def trigger_test(self, select: str) -> dict:
        self._run(["test", "--select", select])
        return self._read_target("run_results.json")

    def generate_artifacts(self) -> tuple[str, str]:
        self._run(["docs", "generate"])
        target = self.project_dir / "target"
        return str(target / "manifest.json"), str(target / "catalog.json")

    def check_source_freshness(self) -> dict:
        self._run(["source", "freshness"])
        return self._read_target("sources.json")

    # -- pause control --------------------------------------------------------

    def create_pause_lock(self, reason: str) -> Path:
        _LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
        _LOCK_PATH.write_text(
            json.dumps({"reason": reason, "created_at": datetime.utcnow().isoformat()}, indent=2)
        )
        logger.warning("dbt runs PAUSED: %s", reason)
        return _LOCK_PATH

    def remove_pause_lock(self) -> None:
        _LOCK_PATH.unlink(missing_ok=True)

    def is_paused(self) -> bool:
        return _LOCK_PATH.exists()
