#!/usr/bin/env python3
"""Standalone Jira integration verifier for the Engineering Cockpit.

Usage:
    python verify_jira.py                   # full end-to-end check
    python verify_jira.py --date 2026-04-09 # check specific date
    python verify_jira.py --skip-ingest     # credentials + fetch only, no write

Exit codes: 0 = LIVE OK, 1 = PARTIALLY LIVE, 2 = SNAPSHOT ONLY, 3 = BROKEN
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

# ── path setup ────────────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

# Use the centralized bootstrap — it resolves paths from its own file location,
# not from CWD, so it works regardless of where this script is launched from.
from cockpit_core.env_bootstrap import bootstrap, get_diagnostics, has_jira_credentials
bootstrap()

# ── colour helpers ─────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg: str)   -> None: print(f"  {GREEN}✓{RESET}  {msg}")
def warn(msg: str) -> None: print(f"  {YELLOW}⚠{RESET}  {msg}")
def fail(msg: str) -> None: print(f"  {RED}✗{RESET}  {msg}")
def head(msg: str) -> None: print(f"\n{BOLD}{CYAN}{msg}{RESET}")
def info(msg: str) -> None: print(f"     {msg}")


# ── checks ────────────────────────────────────────────────────────────────────

def check_env() -> bool:
    """A — Environment variables present (with source diagnostics)."""
    head("A. Environment variables + resolution diagnostics")
    diag = get_diagnostics()

    print(f"  {'Working directory':20s}: {diag.cwd}")
    print(f"  {'Cockpit core':20s}: {diag.cockpit_root}")
    print(f"  {'Repo root':20s}: {diag.repo_root}")
    print()
    print("  .env candidate files (lowest → highest priority):")
    for path, exists in diag.candidate_files:
        status = f"{GREEN}EXISTS{RESET}" if exists else f"{YELLOW}missing{RESET}"
        print(f"    [{status}] {path}")
    print()

    _REQUIRED = ("JIRA_BASE_URL", "JIRA_EMAIL", "JIRA_API_TOKEN", "JIRA_PROJECT_KEY")
    _OPTIONAL = ("COCKPIT_AI_ENABLED", "ANTHROPIC_API_KEY", "COCKPIT_DATA_DIR")

    print("  Required Jira vars:")
    all_ok = True
    for key in _REQUIRED:
        masked, source = diag.var_sources.get(key, ("(not set)", "(not set anywhere)"))
        if "(not set" in masked:
            fail(f"{key:30s} {masked:15s}  ← {source}")
            all_ok = False
        else:
            ok(f"{key:30s} {masked:15s}  ← {source}")

    print()
    print("  Optional vars:")
    for key in _OPTIONAL:
        masked, source = diag.var_sources.get(key, ("(not set)", ""))
        info(f"{key:30s} {masked:15s}  ← {source}")

    return all_ok


def check_credentials() -> tuple[bool, str]:
    """B — Jira authentication."""
    head("B. Jira authentication")
    try:
        from cockpit_core.config import load_config
        from cockpit_core.jira.client import ReadOnlyJiraClient
        config = load_config()
        client = ReadOnlyJiraClient(
            config.jira_base_url, config.jira_email, config.jira_api_token
        )
        user = client.get_current_user()
        name = user.get("displayName", user.get("emailAddress", "?"))
        ok(f"Authenticated as: {name}")
        return True, name
    except Exception as exc:
        fail(f"Authentication failed: {exc}")
        return False, str(exc)


def check_field_discovery() -> tuple[str | None, str | None]:
    """C — Custom field auto-detection."""
    head("C. Custom field discovery")
    try:
        from cockpit_core.config import load_config
        from cockpit_core.jira.client import ReadOnlyJiraClient
        from cockpit_core.jira.fetchers import auto_detect_custom_fields
        config = load_config()
        client = ReadOnlyJiraClient(
            config.jira_base_url, config.jira_email, config.jira_api_token
        )
        sprint_fid, sp_fid = auto_detect_custom_fields(client)
        if sprint_fid:
            ok(f"Sprint field: {sprint_fid}")
        else:
            warn("Sprint field not found — sprint_factor will return 0 for all issues")
        if sp_fid:
            ok(f"Story points field: {sp_fid}")
        else:
            warn("Story points field not found")
        return sprint_fid, sp_fid
    except Exception as exc:
        fail(f"Field discovery failed: {exc}")
        return None, None


def check_issue_fetch(sprint_fid: str | None, sp_fid: str | None, lookback: int = 3) -> int:
    """D — Issue fetch (lightweight, lookback_days=3)."""
    head(f"D. Issue fetch (lookback={lookback}d)")
    try:
        from cockpit_core.config import load_config
        from cockpit_core.jira.client import ReadOnlyJiraClient
        from cockpit_core.jira.fetchers import fetch_issues_for_project
        config = load_config()
        client = ReadOnlyJiraClient(
            config.jira_base_url, config.jira_email, config.jira_api_token
        )
        issues = fetch_issues_for_project(
            client, config.project_key,
            lookback_days=lookback,
            sprint_field_id=sprint_fid,
            sp_field_id=sp_fid,
        )
        ok(f"{len(issues)} issues retrieved for project {config.project_key}")
        if issues:
            sample = issues[0]
            info(f"Sample: {sample.key} | {sample.status} | assignee={sample.assignee}")
        return len(issues)
    except Exception as exc:
        fail(f"Issue fetch failed: {exc}")
        return -1


def check_worklog_fetch(sprint_fid: str | None, sp_fid: str | None, lookback: int = 3) -> int:
    """E — Worklog fetch."""
    head(f"E. Worklog fetch (lookback={lookback}d)")
    try:
        from datetime import timedelta
        from cockpit_core.config import load_config
        from cockpit_core.jira.client import ReadOnlyJiraClient
        from cockpit_core.jira.fetchers import fetch_issues_for_project, fetch_worklogs_for_issues
        config = load_config()
        client = ReadOnlyJiraClient(
            config.jira_base_url, config.jira_email, config.jira_api_token
        )
        issues = fetch_issues_for_project(
            client, config.project_key,
            lookback_days=lookback,
            sprint_field_id=sprint_fid,
            sp_field_id=sp_fid,
        )
        today = date.today()
        since_ms = int(
            (datetime.combine(today - timedelta(days=lookback), datetime.min.time())
             .replace(tzinfo=timezone.utc)).timestamp() * 1000
        )
        issue_keys = [i.key for i in issues[:50]]  # cap at 50 for the verify script
        worklogs = fetch_worklogs_for_issues(client, issue_keys, since_epoch_ms=since_ms)
        ok(f"{len(worklogs)} worklog entries retrieved (sampled {len(issue_keys)} issues)")
        return len(worklogs)
    except Exception as exc:
        fail(f"Worklog fetch failed: {exc}")
        return -1


def check_full_ingest(target_date: date, skip: bool = False) -> bool:
    """F — Full end-to-end ingest (writes Parquet)."""
    head(f"F. Full ingest → {target_date.isoformat()}")
    if skip:
        warn("Skipped (--skip-ingest)")
        return False
    try:
        from cockpit_core.config import load_config
        from cockpit_core.ingest.runner import IngestRunner
        config = load_config()
        runner = IngestRunner(config)

        steps: list[str] = []
        def _progress(msg: str) -> None:
            steps.append(msg)
            # Print only === markers to keep output readable
            if "===" in msg:
                info(msg)

        result = runner.run(target_date=target_date, force=True, progress=_progress)

        if result.status == "ok":
            ok(f"Ingest OK in {result.duration_seconds:.1f}s")
            ok(f"issues={result.issues_seen}  worklogs={result.worklogs_seen}  "
               f"transitions={result.transitions_seen}  prod_rows={result.prod_rows}")
            return True
        else:
            fail(f"Ingest status={result.status}: {result.error_message}")
            return False
    except Exception as exc:
        fail(f"Ingest raised: {exc}")
        return False


def check_snapshot_readable(target_date: date) -> bool:
    """G — Snapshot files readable after ingest."""
    head(f"G. Snapshot readability → {target_date.isoformat()}")
    try:
        from cockpit_core.config import load_config
        from cockpit_core.storage.snapshots import (
            snapshot_exists, read_issues, read_worklogs,
            read_transitions, read_productivity,
        )
        config = load_config()
        root = config.snapshots_dir

        if not snapshot_exists(root, target_date):
            fail("Snapshot does not exist (or is incomplete)")
            return False

        issues_df      = read_issues(root, target_date)
        worklogs_df    = read_worklogs(root, target_date)
        transitions_df = read_transitions(root, target_date)
        prod_df        = read_productivity(root, target_date)

        ok(f"issues.parquet      — {len(issues_df)} rows")
        ok(f"worklogs.parquet    — {len(worklogs_df)} rows")
        ok(f"transitions.parquet — {len(transitions_df)} rows")
        ok(f"productivity.parquet— {len(prod_df)} rows")
        return True
    except Exception as exc:
        fail(f"Snapshot read failed: {exc}")
        return False


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Jira live integration")
    parser.add_argument("--date", default=date.today().isoformat(),
                        help="Target date (ISO format, default: today)")
    parser.add_argument("--skip-ingest", action="store_true",
                        help="Skip full ingest — only test credentials and fetch")
    parser.add_argument("--lookback", type=int, default=3,
                        help="Lookback days for fetch checks (default: 3)")
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date)

    print(f"\n{BOLD}ADAPT Engineering Cockpit — Jira Integration Verifier{RESET}")
    print(f"Target date : {target_date.isoformat()}")
    print(f"Run at      : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    env_ok   = check_env()
    if not env_ok:
        print(f"\n{BOLD}{RED}VERDICT: SNAPSHOT ONLY{RESET}")
        print()
        print("Jira credentials were not found in any of these locations:")
        diag = get_diagnostics()
        for path, exists in diag.candidate_files:
            print(f"  {'✓' if exists else '·'} {path}  ({'exists, but JIRA_ vars missing or commented' if exists else 'does not exist'})")
        print()
        print("To fix — choose one option:")
        print(f"  Option A: Create {diag.repo_root}/.env with your JIRA_* credentials")
        print(f"            (copy from {diag.repo_root}/.env.example and fill in values)")
        print(f"  Option B: Export vars in your shell: export JIRA_BASE_URL=... etc.")
        print(f"  Option C: On Streamlit Cloud, set them under App Settings → Secrets.")
        return 2

    cred_ok, _ = check_credentials()
    if not cred_ok:
        print(f"\n{BOLD}{RED}VERDICT: BROKEN{RESET}")
        print("Credentials are set but authentication failed — check JIRA_EMAIL / JIRA_API_TOKEN.")
        return 3

    sprint_fid, sp_fid = check_field_discovery()

    issues_count   = check_issue_fetch(sprint_fid, sp_fid, lookback=args.lookback)
    worklogs_count = check_worklog_fetch(sprint_fid, sp_fid, lookback=args.lookback)

    fetch_ok = issues_count >= 0 and worklogs_count >= 0

    ingest_ok   = check_full_ingest(target_date, skip=args.skip_ingest)
    snapshot_ok = check_snapshot_readable(target_date) if (ingest_ok or args.skip_ingest) else False

    # ── Verdict ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    head("VERDICT")

    if env_ok and cred_ok and fetch_ok and ingest_ok and snapshot_ok:
        print(f"\n{BOLD}{GREEN}LIVE OK{RESET}")
        print("All checks passed. The dashboard will show live Jira data on next refresh.")
        return 0
    elif env_ok and cred_ok and fetch_ok:
        print(f"\n{BOLD}{YELLOW}PARTIALLY LIVE{RESET}")
        print("Credentials and fetches work, but ingest or snapshot write failed.")
        if args.skip_ingest:
            print("Re-run without --skip-ingest to complete the end-to-end check.")
        return 1
    elif env_ok and cred_ok:
        print(f"\n{BOLD}{RED}BROKEN{RESET}")
        print("Authentication works but issue/worklog fetch failed — check Jira permissions.")
        return 3
    else:
        print(f"\n{BOLD}{YELLOW}SNAPSHOT ONLY{RESET}")
        print("No live Jira access. The dashboard shows cached snapshots only.")
        return 2


if __name__ == "__main__":
    sys.exit(main())
