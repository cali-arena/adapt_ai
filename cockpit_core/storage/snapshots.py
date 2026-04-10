"""Parquet-based append-only daily snapshots."""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from cockpit_core.models import IssueSnapshot, StatusTransition, WorklogEntry

logger = logging.getLogger(__name__)

_LIST_SEP = "|"


def _snapshot_dir(snapshots_root: Path, d: date) -> Path:
    return snapshots_root / d.isoformat()


def snapshot_exists(snapshots_root: Path, d: date) -> bool:
    """Return True only when all 4 required parquet files are present and non-empty."""
    sd = _snapshot_dir(snapshots_root, d)
    required = ("issues.parquet", "worklogs.parquet", "transitions.parquet", "productivity.parquet")
    return all((sd / f).exists() and (sd / f).stat().st_size > 0 for f in required)


def list_snapshot_dates(snapshots_root: Path) -> list[date]:
    if not snapshots_root.exists():
        return []
    dates: list[date] = []
    for child in sorted(snapshots_root.iterdir(), reverse=True):
        if child.is_dir() and (child / "issues.parquet").exists():
            try:
                dates.append(date.fromisoformat(child.name))
            except ValueError:
                pass
    return dates


def write_snapshot(
    snapshots_root: Path,
    target_date: date,
    issues: list[IssueSnapshot],
    worklogs: list[WorklogEntry],
    transitions: list[StatusTransition],
) -> None:
    sd = _snapshot_dir(snapshots_root, target_date)
    sd.mkdir(parents=True, exist_ok=True)

    # Issues
    issue_rows = []
    for iss in issues:
        issue_rows.append({
            "key": iss.key,
            "project_key": iss.project_key,
            "summary": iss.summary,
            "status": iss.status,
            "status_category": iss.status_category,
            "issue_type": iss.issue_type,
            "priority": iss.priority,
            "resolution": iss.resolution,
            "assignee": iss.assignee,
            "assignee_account_id": iss.assignee_account_id,
            "sprint_id": iss.sprint_id,
            "sprint_name": iss.sprint_name,
            "sprint_state": iss.sprint_state,
            "story_points": iss.story_points,
            "original_estimate_seconds": iss.original_estimate_seconds,
            "time_spent_seconds": iss.time_spent_seconds,
            "remaining_estimate_seconds": iss.remaining_estimate_seconds,
            "created_at": iss.created_at,
            "updated_at": iss.updated_at,
            "resolved_at": iss.resolved_at,
            "due_date": str(iss.due_date) if iss.due_date else None,
            "labels": _LIST_SEP.join(iss.labels),
            "components": _LIST_SEP.join(iss.components),
            "is_blocked": iss.is_blocked,
            "blocker_keys": _LIST_SEP.join(iss.blocker_keys),
            "blocking_keys": _LIST_SEP.join(iss.blocking_keys),
            "dependency_keys": _LIST_SEP.join(iss.dependency_keys),
            "age_days": iss.age_days,
            "days_in_current_status": iss.days_in_current_status,
            "raw_url": iss.raw_url,
        })
    pd.DataFrame(issue_rows).to_parquet(sd / "issues.parquet", index=False)

    # Worklogs
    wl_rows = []
    for wl in worklogs:
        wl_rows.append({
            "issue_key": wl.issue_key,
            "worklog_id": wl.worklog_id,
            "author": wl.author,
            "author_account_id": wl.author_account_id,
            "started_at": wl.started_at,
            "time_spent_seconds": wl.time_spent_seconds,
            "comment": wl.comment,
            "created_at": wl.created_at,
            "updated_at": wl.updated_at,
        })
    pd.DataFrame(wl_rows).to_parquet(sd / "worklogs.parquet", index=False)

    # Transitions
    tr_rows = []
    for tr in transitions:
        tr_rows.append({
            "issue_key": tr.issue_key,
            "author": tr.author,
            "author_account_id": tr.author_account_id,
            "occurred_at": tr.occurred_at,
            "from_status": tr.from_status,
            "to_status": tr.to_status,
            "from_category": tr.from_category,
            "to_category": tr.to_category,
            "is_progress": tr.is_progress,
            "is_completion": tr.is_completion,
        })
    pd.DataFrame(tr_rows).to_parquet(sd / "transitions.parquet", index=False)

    logger.info(
        "Snapshot written: %s — %d issues, %d worklogs, %d transitions",
        target_date, len(issues), len(worklogs), len(transitions),
    )


def read_issues(snapshots_root: Path, d: date) -> pd.DataFrame:
    p = _snapshot_dir(snapshots_root, d) / "issues.parquet"
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)


def read_worklogs(snapshots_root: Path, d: date) -> pd.DataFrame:
    p = _snapshot_dir(snapshots_root, d) / "worklogs.parquet"
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)


def read_transitions(snapshots_root: Path, d: date) -> pd.DataFrame:
    p = _snapshot_dir(snapshots_root, d) / "transitions.parquet"
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)


def read_productivity(snapshots_root: Path, d: date) -> pd.DataFrame:
    p = _snapshot_dir(snapshots_root, d) / "productivity.parquet"
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)


# ── Cumulative transition history ─────────────────────────────────────────────
# Lives at <data_root>/transitions_history.parquet (NOT inside a date-scoped
# folder).  Accumulates ALL known status transitions across every refresh,
# deduped by (issue_key, occurred_at, from_status, to_status).
# This is the authoritative source for duration calculations.

_HISTORY_DEDUP_COLS = ["issue_key", "occurred_at", "from_status", "to_status"]


def _history_path(data_root: Path) -> Path:
    return data_root / "transitions_history.parquet"


def read_transitions_history(data_root: Path, snapshots_root: Path | None = None) -> pd.DataFrame:
    """Read the cumulative transition history.

    If the history file does not yet exist and ``snapshots_root`` is provided,
    it is built automatically from all available daily ``transitions.parquet``
    files before returning.  This makes the function self-healing so callers
    never need to call ``build_transitions_history`` separately.
    """
    p = _history_path(data_root)
    if not p.exists() and snapshots_root is not None:
        build_transitions_history(data_root, snapshots_root)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)


def append_transitions_history(data_root: Path, new_df: pd.DataFrame) -> int:
    """Merge new_df into cumulative history, dedup, persist. Returns new rows added."""
    if new_df is None or new_df.empty:
        return 0
    p = _history_path(data_root)
    existing = pd.read_parquet(p) if p.exists() else pd.DataFrame()
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined["occurred_at"] = pd.to_datetime(combined["occurred_at"], utc=True, errors="coerce")
    combined = combined.dropna(subset=["occurred_at"])
    before = len(existing)
    combined = combined.drop_duplicates(subset=_HISTORY_DEDUP_COLS, keep="first")
    combined = combined.sort_values("occurred_at").reset_index(drop=True)
    combined.to_parquet(p, index=False)
    added = max(0, len(combined) - before)
    logger.info("transitions_history: %d total rows (+%d new)", len(combined), added)
    return added


def build_transitions_history(data_root: Path, snapshots_root: Path) -> int:
    """Bootstrap the history file from all existing daily snapshots.

    Idempotent — safe to call on every startup. Returns total rows after build.
    """
    frames: list[pd.DataFrame] = []
    if snapshots_root.exists():
        for child in sorted(snapshots_root.iterdir()):
            if not child.is_dir():
                continue
            tr_path = child / "transitions.parquet"
            if tr_path.exists():
                try:
                    df = pd.read_parquet(tr_path)
                    if not df.empty:
                        frames.append(df)
                except Exception as exc:
                    logger.warning("Could not read %s: %s", tr_path, exc)
    if not frames:
        return 0
    combined = pd.concat(frames, ignore_index=True)
    combined["occurred_at"] = pd.to_datetime(combined["occurred_at"], utc=True, errors="coerce")
    combined = combined.dropna(subset=["occurred_at"])
    combined = combined.drop_duplicates(subset=_HISTORY_DEDUP_COLS, keep="first")
    combined = combined.sort_values("occurred_at").reset_index(drop=True)
    p = _history_path(data_root)
    combined.to_parquet(p, index=False)
    logger.info(
        "transitions_history bootstrapped: %d rows from %d snapshot(s)",
        len(combined), len(frames),
    )
    return len(combined)
