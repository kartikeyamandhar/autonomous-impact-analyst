"""Phase 0 scaffolding tests."""

from math import isclose
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.phase_0


REQUIRED_ENV_VARS = [
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "COINGECKO_API_KEY",
    "ETHERSCAN_API_KEY",
    "NEO4J_URI",
    "ANTHROPIC_API_KEY",
    "SLACK_WEBHOOK_URL",
    "GITHUB_TOKEN",
]

SRC_PACKAGES = [
    "src",
    "src/ingestion",
    "src/graph_engine",
    "src/anomaly_detection",
    "src/agent",
    "src/actions",
    "src/exposure_bot",
    "src/orchestration",
]


def test_settings_loads(settings: dict) -> None:
    assert isinstance(settings, dict)
    assert "risk_scoring" in settings


def test_risk_weights_sum_to_one(settings: dict) -> None:
    weights = settings["risk_scoring"]["weights"]
    total = sum(weights.values())
    assert isclose(total, 1.0, abs_tol=1e-9), f"weights sum to {total}, not 1.0"


def test_risk_thresholds_ascending(settings: dict) -> None:
    t = settings["risk_scoring"]["thresholds"]
    assert t["low"] < t["medium"] < t["high"], (
        f"thresholds must be strictly ascending: {t}"
    )


def test_dbt_project_yml(dbt_project_dir: Path) -> None:
    p = dbt_project_dir / "dbt_project.yml"
    assert p.exists(), "src/dbt_project/dbt_project.yml missing"
    cfg = yaml.safe_load(p.read_text())
    assert cfg["name"] == "autonomous_impact_analyst"


def test_profiles_yml_exists(dbt_project_dir: Path) -> None:
    assert (dbt_project_dir / "profiles.yml").exists()


def test_src_init_files_exist(project_root: Path) -> None:
    for pkg in SRC_PACKAGES:
        init = project_root / pkg / "__init__.py"
        assert init.exists(), f"missing {pkg}/__init__.py"


def test_env_example_has_required_vars(project_root: Path) -> None:
    env_example = project_root / ".env.example"
    assert env_example.exists(), ".env.example missing"
    content = env_example.read_text()
    for var in REQUIRED_ENV_VARS:
        assert var in content, f".env.example missing {var}"
