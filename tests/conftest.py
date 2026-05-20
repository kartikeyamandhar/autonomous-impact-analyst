from pathlib import Path

import pytest
import yaml


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def fixtures_dir(project_root: Path) -> Path:
    return project_root / "tests" / "fixtures"


@pytest.fixture(scope="session")
def dbt_project_dir(project_root: Path) -> Path:
    return project_root / "src" / "dbt_project"


@pytest.fixture(scope="session")
def settings(project_root: Path) -> dict:
    with open(project_root / "config" / "settings.yml") as f:
        return yaml.safe_load(f)
