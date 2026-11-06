"""Pytest fixtures for reconciliation test suite."""
from pathlib import Path
import pytest


@pytest.fixture
def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


@pytest.fixture
def sample_data_dir(repo_root: Path) -> Path:
    return repo_root / "data" / "sample"


@pytest.fixture
def demo_config_path(repo_root: Path) -> Path:
    return repo_root / "config" / "demo.toml"
