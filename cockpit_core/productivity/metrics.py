"""Productivity metrics engine.

Signal hierarchy (strictly enforced):
  1. EFFORT signal  — worklogs (time actually logged) + is_completion transitions
  2. ACTIVITY signal — issues with updated_at on target_date (auxiliary hint ONLY)

The activity signal is NEVER surfaced as a productivity number. It is kept as
a secondary column with explicit labelling in the UI.
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from cockpit_core.storage.snapshots import (
    _snapshot_dir,
    read_productivity,
)

logger = logging.getLogger(__name__)

PRODUCTIVITY_COLS = [
    "user",
    "date",
    # ── Effort signal (primary) ──
    "effort_hours",
    "effort_tasks_done",
    "effort_story_points",
    "effort_transitions",
    # ── Activity signal (auxiliary) ──
    "activity_updates",
    # ── Composite ──
    "issues_in_progress",
    "log_coverage",
    "has_real_activity",
    "score_warnings",
]


def _coerce_dt(series: pd.Series) -> pd.Series:
    """Ensure a datetime series is timezone-aware (UTC)."""
    if series.empty:
        return series
    if hasattr(series.dtype, "tz") and series.dtype.tz is not None:
        return series
    try:
        return pd.to_datetime(series, utc=True)
    except Exception:
        return pd.to_datetime(series, errors="coerce", utc=True)


def compute_daily_productivity(
    worklogs_df: pd.DataFrame,
    transitions_df: pd.DataFrame,
    issues_df: pd.DataFrame,
    target_date: date,
) -> pd.DataFrame:
    """Compute per-user productivity for target_date.

    Returns a DataFrame with PRODUCTIVITY_COLS schema.
    All users who appear in any signal are included (even zero-effort ones).
    """
    rows: dict[str, dict] = {}

    def _get(user: str) -> dict:
        if user not in rows:
            rows[user] = {
                "user": user,
                "date": target_date.isoformat(),
                "effort_hours": 0.0,
                "effort_tasks_done": 0,
                "effort_story_points": 0.0,
                "effort_transitions": 0,
                "activity_updates": 0,
                "issues_in_progress": 0,
                "log_coverage": 0.0,
                "has_real_activity": False,
                "score_warnings": [],
            }
        return rows[user]

    # 1. Effort signal: worklogs started on target_date
    if not worklogs_df.empty and "started_at" in worklogs_df.columns:
        wl = worklogs_df.copy()
        wl["started_at"] = _coerce_dt(wl["started_at"])
        wl_today = wl[wl["started_at"].dt.date == target_date]
        for author, grp in wl_today.groupby("author"):
            r = _get(str(author))
            r["effort_hours"] += grp["time_spent_seconds"].sum() / 3600.0

    # 2. Effort signal: is_completion transitions on target_date
    if not transitions_df.empty and "occurred_at" in transitions_df.columns:
        tr = transitions_df.copy()
        tr["occurred_at"] = _coerce_dt(tr["occurred_at"])
        tr_today = tr[tr["occurred_at"].dt.date == target_date]

        completions = tr_today[tr_today["is_completion"].astype(bool)]
        all_tr_today = tr_today[tr_today["is_progress"].astype(bool)]

        for author, grp in completions.groupby("author"):
            r = _get(str(author))
            r["effort_tasks_done"] += len(grp)
            r["effort_transitions"] += len(grp)

            # Story points for completed tasks
            if not issues_df.empty and "key" in issues_df.columns and "story_points" in issues_df.columns:
                keys = grp["issue_key"].tolist()
                sp = issues_df[issues_df["key"].isin(keys)]["story_points"].fillna(0).sum()
                r["effort_story_points"] += float(sp)

        for author, grp in all_tr_today.groupby("author"):
            r = _get(str(author))
            r["effort_transitions"] = max(r["effort_transitions"], len(grp))

    # 3. Activity signal: issues updated on target_date (auxiliary ONLY)
    if not issues_df.empty and "updated_at" in issues_df.columns and "assignee" in issues_df.columns:
        iss = issues_df.copy()
        iss["updated_at"] = _coerce_dt(iss["updated_at"])
        iss_today = iss[iss["updated_at"].dt.date == target_date]
        for assignee, grp in iss_today.groupby("assignee"):
            if pd.isna(assignee) or not assignee:
                continue
            r = _get(str(assignee))
            r["activity_updates"] = len(grp)

    # 4. In-progress count
    if not issues_df.empty and "assignee" in issues_df.columns and "status_category" in issues_df.columns:
        in_prog = issues_df[
            issues_df["status_category"].str.lower().isin({
                "indeterminate",   # canonical statusCategory.key (new fetcher)
                "in progress",     # English status name (changelog / legacy)
                "em andamento",    # Portuguese (legacy snapshots before fix)
            })
        ]
        for assignee, grp in in_prog.groupby("assignee"):
            if pd.isna(assignee) or not assignee:
                continue
            if str(assignee) in rows:
                rows[str(assignee)]["issues_in_progress"] = len(grp)

    # 5. Compute derived fields
    for user, r in rows.items():
        has_real = r["effort_hours"] > 0 or r["effort_tasks_done"] > 0
        r["has_real_activity"] = has_real

        denominator = r["effort_tasks_done"] + r["issues_in_progress"]
        r["log_coverage"] = min(1.0, r["effort_hours"] / 8.0) if has_real and denominator == 0 else (
            min(1.0, r["effort_hours"] / max(1, denominator)) if denominator > 0 else 0.0
        )

        warnings: list[str] = []
        if not has_real and r["activity_updates"] > 0:
            warnings.append("no_real_activity")
        if r["effort_hours"] == 0 and has_real:
            warnings.append("no_worklogs")
        if r["effort_tasks_done"] > 0 and r["effort_hours"] == 0:
            warnings.append("completed_without_logging")
        if r["effort_hours"] > 10:
            warnings.append("possible_overload")
        r["score_warnings"] = "|".join(warnings)

    if not rows:
        return pd.DataFrame(columns=PRODUCTIVITY_COLS)

    df = pd.DataFrame(list(rows.values()))
    return df[PRODUCTIVITY_COLS]


def compute_weekly_productivity(
    snapshots_root: Path,
    end_date: date,
    project_key: str,
    lookback_days: int = 7,
) -> pd.DataFrame:
    """Aggregate productivity.parquet files over the last N days."""
    from datetime import timedelta

    frames: list[pd.DataFrame] = []
    for i in range(lookback_days):
        d = end_date - timedelta(days=i)
        df = read_productivity(snapshots_root, d)
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    def _agg(grp: pd.DataFrame) -> pd.Series:
        days_with = int((grp["effort_hours"] > 0).sum())
        return pd.Series({
            "total_effort_hours": grp["effort_hours"].sum(),
            "total_tasks_done": grp["effort_tasks_done"].sum(),
            "total_story_points": grp["effort_story_points"].sum(),
            "avg_daily_hours": grp["effort_hours"].mean(),
            "days_with_activity": days_with,
            "consistency_pct": int(days_with / lookback_days * 100),
            "period_start": (end_date - __import__("datetime").timedelta(days=lookback_days - 1)).isoformat(),
            "period_end": end_date.isoformat(),
        })

    return combined.groupby("user").apply(_agg).reset_index()


def build_done_today(
    issues_df: pd.DataFrame,
    transitions_df: pd.DataFrame,
    target_date: date,
) -> pd.DataFrame:
    """Return issues that were moved to Done on target_date."""
    if transitions_df.empty or "occurred_at" not in transitions_df.columns:
        return pd.DataFrame()

    tr = transitions_df.copy()
    tr["occurred_at"] = pd.to_datetime(tr["occurred_at"], utc=True, errors="coerce")
    completions = tr[
        (tr["occurred_at"].dt.date == target_date) &
        (tr["is_completion"].astype(bool))
    ].drop_duplicates("issue_key")

    if completions.empty:
        return pd.DataFrame()

    if issues_df.empty:
        return completions[["issue_key", "author", "occurred_at"]].rename(
            columns={"issue_key": "key", "author": "assignee"}
        )

    merged = completions.merge(issues_df, left_on="issue_key", right_on="key", how="left")

    keep_cols = [c for c in [
        "key", "summary", "issue_type", "priority", "assignee", "story_points",
        "sprint_name", "labels", "original_estimate_seconds", "time_spent_seconds",
        "occurred_at",
    ] if c in merged.columns]

    result = merged[keep_cols].copy()

    if "original_estimate_seconds" in result.columns:
        result["estimate_hours"] = result["original_estimate_seconds"].fillna(0) / 3600
    if "time_spent_seconds" in result.columns:
        result["logged_hours"] = result["time_spent_seconds"].fillna(0) / 3600

    return result.reset_index(drop=True)


def write_productivity_parquet(
    snapshots_root: Path,
    target_date: date,
    prod_df: pd.DataFrame,
) -> None:
    sd = _snapshot_dir(snapshots_root, target_date)
    sd.mkdir(parents=True, exist_ok=True)
    prod_df.to_parquet(sd / "productivity.parquet", index=False)
    logger.info("Wrote productivity.parquet for %s (%d rows)", target_date, len(prod_df))
