"""Plan assembler — merges deterministic scores with TL overrides.

Priority order after merge:
  1. Pinned issues (pinned=1) — sorted by their rank_override value (ascending)
  2. Issues with explicit rank_override (not pinned) — inserted at that position
  3. Remaining issues — in deterministic score order (descending)

The original deterministic rank is always preserved in `original_rank` so the TL
can see exactly what they overrode.
"""
from __future__ import annotations

import sqlite3
from datetime import date
from typing import Any

import pandas as pd


def build_plan(
    scored_df: pd.DataFrame,
    overrides: list[sqlite3.Row],
    notes: list[sqlite3.Row],
) -> pd.DataFrame:
    """Return a plan_df with merged ranking and override metadata.

    Columns added:
        plan_rank       — final position in the plan (1-based)
        original_rank   — where the deterministic score placed this issue
        is_pinned       — True if TL explicitly pinned it
        rank_override   — integer rank the TL requested, or None
        override_reason — TL's written reason
        override_by     — who made the override (default 'tl')
    """
    if scored_df.empty:
        return pd.DataFrame()

    df = scored_df.reset_index(drop=True).copy()
    df["original_rank"] = range(1, len(df) + 1)
    df["is_pinned"] = False
    df["rank_override"] = pd.NA
    df["override_reason"] = None
    df["override_by"] = None

    # Index overrides by key
    pinned: list[tuple[int, str, str | None, str]] = []   # (order_val, key, reason, by)
    ranked: dict[str, tuple[int, str | None, str]] = {}    # key → (rank, reason, by)

    for ov in overrides:
        key = ov["issue_key"]
        if key not in df["key"].values:
            continue
        reason = ov["reason"] or ""
        by = ov["created_by"] or "tl"
        if ov["pinned"]:
            order_val = ov["rank_override"] if ov["rank_override"] is not None else 9999
            pinned.append((order_val, key, reason, by))
        elif ov["rank_override"] is not None:
            ranked[key] = (int(ov["rank_override"]), reason, by)

    pinned.sort(key=lambda x: x[0])
    pinned_keys = [x[1] for x in pinned]

    # Separate pinned vs non-pinned
    pinned_df = df[df["key"].isin(pinned_keys)].copy()
    non_pinned_df = df[~df["key"].isin(pinned_keys)].copy()

    # Reorder pinned to match pinned order
    pin_order = {k: i for i, k in enumerate(pinned_keys)}
    pinned_df["_po"] = pinned_df["key"].map(pin_order)
    pinned_df = pinned_df.sort_values("_po").drop(columns=["_po"])

    # Apply rank_override to non-pinned: issue with rank_override=N sits at position N
    # Strategy: give them a sort key in the non-pinned pool
    def _sort_key(row: pd.Series) -> float:
        k = row["key"]
        if k in ranked:
            return float(ranked[k][0])                  # requested rank (relative)
        return float(row["original_rank"]) + 10_000     # after all explicitly ranked

    non_pinned_df["_sk"] = non_pinned_df.apply(_sort_key, axis=1)
    non_pinned_df = non_pinned_df.sort_values("_sk").drop(columns=["_sk"])

    # Combine
    plan_df = pd.concat([pinned_df, non_pinned_df], ignore_index=True)
    plan_df["plan_rank"] = range(1, len(plan_df) + 1)

    # Write override metadata back
    for _, key, reason, by in pinned:
        m = plan_df["key"] == key
        plan_df.loc[m, "is_pinned"] = True
        plan_df.loc[m, "override_reason"] = reason
        plan_df.loc[m, "override_by"] = by

    for key, (rank, reason, by) in ranked.items():
        m = plan_df["key"] == key
        plan_df.loc[m, "rank_override"] = rank
        plan_df.loc[m, "override_reason"] = reason
        plan_df.loc[m, "override_by"] = by

    return plan_df


def get_day_notes(notes: list[sqlite3.Row]) -> str:
    """Extract the day-scoped TL note (scope='day')."""
    for n in notes:
        if n["scope"] == "day":
            return n["body"]
    return ""


def get_issue_notes(notes: list[sqlite3.Row]) -> dict[str, str]:
    """Return {issue_key: note_body} for issue-scoped notes."""
    result: dict[str, str] = {}
    for n in notes:
        scope = n["scope"]
        if scope.startswith("issue:"):
            key = scope.split(":", 1)[1]
            result[key] = n["body"]
    return result
