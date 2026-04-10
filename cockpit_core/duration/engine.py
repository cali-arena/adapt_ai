"""Duration model — computes cycle time, lead time, and in-progress duration.

All durations are derived from transitions.parquet (status transition history).
The 7-day lookback window in ingestion means `started_at` may be unavailable
for tasks that entered In Progress more than 7 days ago — these are flagged
transparently with `has_start_transition=False` rather than silently omitted.

Output schema (one row per issue key):
  key                         str
  started_at                  datetime | NaT   — first transition INTO an active status
  completed_at                datetime | NaT   — last transition INTO a done status
  cycle_time_hours            float | NaN      — started_at → completed_at (if both present)
  lead_time_hours             float | NaN      — created_at → completed_at (if issues_df provided)
  time_to_start_hours         float | NaN      — created_at → started_at
  currently_in_progress_hours float | NaN      — started_at → now (if in-progress and not done)
  reopened_count              int              — done → active transitions (0 if none)
  has_start_transition        bool             — False if no start transition in window
  has_completion_transition   bool             — False if no completion transition in window
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import FrozenSet

import pandas as pd

# ── Status buckets (case-insensitive, lowercase-normalised at match time) ─────

ACTIVE_STATUSES: FrozenSet[str] = frozenset({
    "in progress",
    "em andamento",
    "doing",
    "in development",
    "desenvolvimento",
    "implementing",
    "in review",
    "em revisão",
    "em revisao",
    "code review",
    "testing",
    "in test",
    "in qa",
    "qa",
    "working",
    "indeterminate",  # Jira's own category label used in some configs
})

DONE_STATUSES: FrozenSet[str] = frozenset({
    "done",
    "concluído",
    "concluido",
    "completed",
    "resolved",
    "closed",
    "released",
    "delivered",
    "finalizado",
    "pronto",
    "ready for release",
    "ready to release",
})


def _is_active(status: str) -> bool:
    return status.strip().lower() in ACTIVE_STATUSES


def _is_done(status: str) -> bool:
    return status.strip().lower() in DONE_STATUSES


def format_hours(hours: float) -> str:
    """Human-readable duration: '2h 30m', '3d 4h', etc."""
    if pd.isna(hours) or hours < 0:
        return "—"
    total_minutes = int(round(hours * 60))
    days, rem = divmod(total_minutes, 1440)
    h, m = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if h:
        parts.append(f"{h}h")
    if m and not days:
        parts.append(f"{m}m")
    return " ".join(parts) if parts else "< 1m"


def compute_issue_durations(
    transitions_df: pd.DataFrame,
    issues_df: pd.DataFrame | None = None,
    now: datetime | None = None,
    active_statuses: FrozenSet[str] | None = None,
    done_statuses: FrozenSet[str] | None = None,
) -> pd.DataFrame:
    """Compute per-issue duration metrics from transition history.

    Parameters
    ----------
    transitions_df:
        Output of ``read_transitions()`` — must have columns:
        ``issue_key``, ``to_status``, ``from_status``, ``occurred_at``
    issues_df:
        Optional issues snapshot for ``created_at`` (used in lead/start time).
        Must have columns ``key``, ``created_at``.
    now:
        Reference timestamp for in-progress duration. Defaults to UTC now.
    active_statuses / done_statuses:
        Override the default status buckets. Useful for non-English Jira configs.

    Returns
    -------
    pd.DataFrame with one row per issue_key and the columns listed in the module
    docstring.
    """
    _active = active_statuses or ACTIVE_STATUSES
    _done = done_statuses or DONE_STATUSES

    if now is None:
        now = datetime.now(timezone.utc)

    # ── Guard: empty input ────────────────────────────────────────────────────
    if transitions_df is None or transitions_df.empty:
        return pd.DataFrame(columns=[
            "key", "started_at", "completed_at",
            "cycle_time_hours", "lead_time_hours", "time_to_start_hours",
            "currently_in_progress_hours", "reopened_count",
            "has_start_transition", "has_completion_transition",
        ])

    # ── Normalise column names (transitions_df may come from dataclass or parquet) ─
    df = transitions_df.copy()
    # Rename 'issue_key' → 'key' internally if needed
    if "issue_key" in df.columns and "key" not in df.columns:
        df = df.rename(columns={"issue_key": "key"})

    required = {"key", "to_status", "occurred_at"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"transitions_df missing columns: {missing}")

    # ── Parse timestamps ──────────────────────────────────────────────────────
    df["occurred_at"] = pd.to_datetime(df["occurred_at"], utc=True, errors="coerce")
    df = df.dropna(subset=["occurred_at"])
    df = df.sort_values("occurred_at")

    df["_to_lower"] = df["to_status"].fillna("").str.strip().str.lower()
    df["_from_lower"] = (
        df["from_status"].fillna("").str.strip().str.lower()
        if "from_status" in df.columns
        else ""
    )
    df["_is_active_transition"] = df["_to_lower"].isin(_active)
    df["_is_done_transition"] = df["_to_lower"].isin(_done)
    df["_is_reopen"] = df["_from_lower"].isin(_done) & df["_is_active_transition"]

    # ── Build created_at lookup from issues_df ────────────────────────────────
    created_lookup: dict[str, datetime] = {}
    if issues_df is not None and not issues_df.empty and "key" in issues_df.columns and "created_at" in issues_df.columns:
        ts = pd.to_datetime(issues_df["created_at"], utc=True, errors="coerce")
        for k, t in zip(issues_df["key"], ts):
            if pd.notna(t):
                created_lookup[str(k)] = t

    # ── Per-issue aggregation ─────────────────────────────────────────────────
    records = []
    for key, grp in df.groupby("key"):
        key = str(key)
        grp = grp.sort_values("occurred_at")

        active_rows = grp[grp["_is_active_transition"]]
        done_rows = grp[grp["_is_done_transition"]]
        reopen_rows = grp[grp["_is_reopen"]]

        started_at = active_rows["occurred_at"].min() if not active_rows.empty else pd.NaT
        completed_at = done_rows["occurred_at"].max() if not done_rows.empty else pd.NaT
        reopened_count = int(len(reopen_rows))

        has_start = pd.notna(started_at)
        has_completion = pd.notna(completed_at)

        # Cycle time: started_at → completed_at
        cycle_time_hours: float = float("nan")
        if has_start and has_completion and completed_at >= started_at:
            cycle_time_hours = (completed_at - started_at).total_seconds() / 3600.0

        # Lead time: created_at → completed_at
        lead_time_hours: float = float("nan")
        created_at = created_lookup.get(key)
        if created_at and has_completion:
            lead_time_hours = (completed_at - created_at).total_seconds() / 3600.0

        # Time to start: created_at → started_at
        time_to_start_hours: float = float("nan")
        if created_at and has_start:
            time_to_start_hours = max(0.0, (started_at - created_at).total_seconds() / 3600.0)

        # Currently in-progress: started_at → now (only if not completed)
        currently_hours: float = float("nan")
        if has_start and not has_completion:
            now_aware = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
            if now_aware >= started_at:
                currently_hours = (now_aware - started_at).total_seconds() / 3600.0

        records.append({
            "key": key,
            "started_at": started_at,
            "completed_at": completed_at,
            "cycle_time_hours": cycle_time_hours,
            "lead_time_hours": lead_time_hours,
            "time_to_start_hours": time_to_start_hours,
            "currently_in_progress_hours": currently_hours,
            "reopened_count": reopened_count,
            "has_start_transition": has_start,
            "has_completion_transition": has_completion,
        })

    if not records:
        return pd.DataFrame(columns=[
            "key", "started_at", "completed_at",
            "cycle_time_hours", "lead_time_hours", "time_to_start_hours",
            "currently_in_progress_hours", "reopened_count",
            "has_start_transition", "has_completion_transition",
        ])

    return pd.DataFrame(records)
