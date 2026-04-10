"""Centralized environment bootstrap for the ADAPT Engineering Cockpit.

Priority order (highest to lowest):
  1. Already-exported process environment (os.environ) — never overridden
  2. Streamlit secrets (st.secrets)                    — Streamlit Cloud / .streamlit/secrets.toml
  3. Local .env (repo root)                            — local development only

Resolution is anchor-based from THIS FILE'S location — never from CWD —
so every entry point (Streamlit, CLI, verify_jira.py) resolves identically
regardless of what directory the user launched from.

Streamlit Cloud deployment:
  Set JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN, JIRA_PROJECT_KEY (and optionally
  ANTHROPIC_API_KEY, COCKPIT_AI_ENABLED) in the Streamlit Cloud dashboard under
  "Secrets", or in .streamlit/secrets.toml for local testing.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv as _load_dotenv
    _HAS_DOTENV = True
except ImportError:  # pragma: no cover
    _HAS_DOTENV = False
    def _load_dotenv(*args, **kwargs) -> None:  # type: ignore[misc]
        pass


# ── Anchor: this file's directory is always cockpit_core/ at the repo root ───

_COCKPIT_CORE_DIR = Path(__file__).resolve().parent          # .../cockpit_core
_REPO_ROOT        = _COCKPIT_CORE_DIR.parent                 # repo root


# ── .env candidate list (highest priority last — load_dotenv skips vars already set) ─

def _candidate_env_files() -> list[Path]:
    """Return candidate .env paths, lowest-priority first (dotenv loads in order)."""
    return [
        _REPO_ROOT / ".env",   # repo-root .env (gitignored, local dev only)
    ]


# ── Module-level sentinel so bootstrap() runs only once ──────────────────────

_bootstrapped = False


def _inject_streamlit_secrets() -> None:
    """Copy Streamlit secrets into os.environ (skip vars already set).

    Called during bootstrap() so that Streamlit Cloud secrets are available
    to the entire app — including CLI helpers and cockpit_core modules — as
    plain os.environ vars. This is safe because:
      - We only inject; we never overwrite existing env vars (override=False logic).
      - st.secrets is only accessible when running under `streamlit run`.
      - Any exception (CLI context, secrets.toml absent, etc.) is silently swallowed.
    """
    try:
        import streamlit as st  # noqa: PLC0415 — intentional lazy import
        secrets = dict(st.secrets)  # raises outside Streamlit runtime; caught below
        for key, value in secrets.items():
            if key not in os.environ and isinstance(value, str):
                os.environ[key] = value
    except Exception:  # noqa: BLE001
        pass  # Not in Streamlit runtime, or secrets not configured — dotenv takes over


def bootstrap() -> None:
    """Load credentials into os.environ. Safe to call multiple times.

    Priority (highest wins, lower sources never overwrite):
      1. os.environ already set (e.g. CI / shell export / Streamlit Cloud Secrets)
      2. st.secrets  (Streamlit Cloud dashboard or .streamlit/secrets.toml)
      3. Local .env file (dotenv, for local development)
    """
    global _bootstrapped
    if _bootstrapped:
        return
    _bootstrapped = True

    # 2. Streamlit secrets — only active inside `streamlit run` context
    _inject_streamlit_secrets()

    # 3. Local .env files — dotenv fallback for local dev
    if not _HAS_DOTENV:
        return

    for p in _candidate_env_files():
        if p.is_file():
            # override=False: never clobber vars already in the process environment
            _load_dotenv(p, override=False)


# ── Diagnostics ───────────────────────────────────────────────────────────────

_JIRA_VARS = ("JIRA_BASE_URL", "JIRA_EMAIL", "JIRA_API_TOKEN", "JIRA_PROJECT_KEY")
_COCKPIT_VARS = ("COCKPIT_AI_ENABLED", "ANTHROPIC_API_KEY", "COCKPIT_DATA_DIR")


def _mask(value: str) -> str:
    if not value:
        return "(not set)"
    if len(value) <= 8:
        return "***MASKED***"
    return value[:4] + "***" + value[-2:]


def _find_source(key: str) -> str:
    """Determine where a var came from (process env vs which file)."""
    current_val = os.environ.get(key, "")
    if not current_val:
        return "(not set anywhere)"

    for p in reversed(_candidate_env_files()):  # highest priority first for display
        if not p.is_file():
            continue
        for line in p.read_text(errors="replace").splitlines():
            line = line.strip()
            if line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            if k.strip() == key and v.strip():
                return f"file: {p}"
    return "process environment (shell export, Streamlit secrets, or CI)"


@dataclass
class EnvDiagnostics:
    cwd: str
    cockpit_root: str
    repo_root: str
    candidate_files: list[tuple[str, bool]]          # (path, exists)
    loaded_files: list[str]                          # files that were actually loaded
    var_sources: dict[str, tuple[str, str]]          # key → (masked_value, source)
    all_jira_vars_set: bool


def get_diagnostics() -> EnvDiagnostics:
    """Return safe diagnostics about env resolution. Never exposes raw secrets."""
    bootstrap()  # ensure env is loaded

    candidate_files = [
        (str(p), p.is_file()) for p in _candidate_env_files()
    ]

    var_sources: dict[str, tuple[str, str]] = {}
    for key in (*_JIRA_VARS, *_COCKPIT_VARS):
        val = os.environ.get(key, "")
        source = _find_source(key) if val else "(not set anywhere)"
        var_sources[key] = (_mask(val), source)

    all_jira = all(os.environ.get(k, "").strip() for k in _JIRA_VARS)

    return EnvDiagnostics(
        cwd=os.getcwd(),
        cockpit_root=str(_COCKPIT_CORE_DIR),
        repo_root=str(_REPO_ROOT),
        candidate_files=candidate_files,
        loaded_files=[str(p) for p in _candidate_env_files() if p.is_file()],
        var_sources=var_sources,
        all_jira_vars_set=all_jira,
    )


def has_jira_credentials() -> bool:
    """Return True only when all 4 required Jira env vars are non-empty."""
    bootstrap()
    return all(os.environ.get(k, "").strip() for k in _JIRA_VARS)


def has_ai_credentials() -> bool:
    bootstrap()
    enabled = os.environ.get("COCKPIT_AI_ENABLED", "false").lower() in ("1", "true", "yes")
    key_set = bool(os.environ.get("ANTHROPIC_API_KEY", "").strip())
    return enabled and key_set
