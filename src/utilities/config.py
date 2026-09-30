"""
Shared config loader. Falls back to config.example.yaml if config.yaml
hasn't been created yet, so scripts still run out of the box with
sensible defaults, but prefer a real config.yaml once one exists.
"""

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def load_config() -> dict:
    config_path = REPO_ROOT / "config" / "config.yaml"
    example_path = REPO_ROOT / "config" / "config.example.yaml"
    path = config_path if config_path.exists() else example_path

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)