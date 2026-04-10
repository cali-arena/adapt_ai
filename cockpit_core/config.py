"""Configuration loader — reads from centralized env bootstrap."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from cockpit_core.env_bootstrap import bootstrap


@dataclass
class CockpitConfig:
    jira_base_url: str
    jira_email: str
    jira_api_token: str
    project_key: str
    data_dir: Path
    ai_enabled: bool = False
    anthropic_api_key: str = ""
    sprint_field_id: str = ""
    story_points_field_id: str = ""

    @property
    def db_path(self) -> Path:
        return self.data_dir / "cockpit.db"

    @property
    def snapshots_dir(self) -> Path:
        return self.data_dir / "snapshots"


def load_config(data_dir: Path | None = None) -> CockpitConfig:
    # Ensure .env files are loaded before reading os.environ.
    # bootstrap() is idempotent — safe to call multiple times.
    bootstrap()

    base_url = os.environ.get("JIRA_BASE_URL", "").rstrip("/")
    email = os.environ.get("JIRA_EMAIL", "")
    token = os.environ.get("JIRA_API_TOKEN", "")
    project_key = os.environ.get("JIRA_PROJECT_KEY", "NAI")

    if data_dir is None:
        from cockpit_core.env_bootstrap import _REPO_ROOT
        data_dir = _REPO_ROOT / os.environ.get("COCKPIT_DATA_DIR", "data")

    ai_enabled = os.environ.get("COCKPIT_AI_ENABLED", "false").lower() in ("1", "true", "yes")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")

    return CockpitConfig(
        jira_base_url=base_url,
        jira_email=email,
        jira_api_token=token,
        project_key=project_key,
        data_dir=data_dir,
        ai_enabled=ai_enabled,
        anthropic_api_key=anthropic_key,
    )
