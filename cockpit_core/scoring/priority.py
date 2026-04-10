"""Deterministic priority scoring engine.

Score formula (additive, bounded [0, 100]):

    score = clamp(
        W_PRIORITY * priority_factor(issue)     # 0..1
      + W_DUE      * due_factor(issue)          # 0..1
      - W_BLOCKED  * |blocked_factor(issue)|    # penalty: 0 or -W_BLOCKED
      + W_BLOCKING * blocking_factor(issue)     # 0..1
      + W_AGE      * age_factor(issue)          # 0..1
      + W_SPRINT   * sprint_factor(issue)       # 0..1
      + W_STALL    * stall_factor(issue)        # 0..1
    , 0, 100)

Every factor returns a float in [0.0, 1.0].
The blocked factor is applied as a SUBTRACTION (penalty).
All weights are integer points; the max attainable score for a non-blocked,
non-blocking issue with all factors maxed is 70 (25+20+10+10+5); a blocking
issue adds up to 15 more (max 85). Scores are clipped to [0, 100].

Warnings are generated for any missing fields that would have contributed
to the score — the issue is scored conservatively (missing field → 0
contribution) and the warning is surfaced in the UI.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import pandas as pd

# ── Default weights (integer points, tunable) ─────────────────────────────────
WEIGHTS: dict[str, int] = {
    "priority": 25,
    "due":      20,
    "blocked":  15,   # applied as penalty (subtraction)
    "blocking": 15,
    "age":      10,
    "sprint":   10,
    "stall":    5,
}

_PRIORITY_MAP: dict[str, float] = {
    "highest": 1.00,
    "high":    0.75,
    "medium":  0.50,
    "low":     0.25,
    "lowest":  0.00,
}


# ── Shared null-safe coercions ────────────────────────────────────────────────

def _is_null(val) -> bool:
    """Return True for None, NaN (float or pandas), empty string."""
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    try:
        if pd.isna(val):
            return True
    except (TypeError, ValueError):
        pass
    return False


def _safe_str(val) -> str | None:
    """Coerce val to a non-empty stripped string, or None if null-like."""
    if _is_null(val):
        return None
    if not isinstance(val, str):
        # e.g. numeric values that slipped through
        val = str(val)
    val = val.strip()
    return val if val else None


def _safe_bool(val, default: bool = False) -> bool:
    if _is_null(val):
        return default
    if isinstance(val, (bool, int, float)):
        return bool(val)
    s = str(val).strip().lower()
    return s in ("true", "1", "yes")


def _safe_int(val, default: int = 0) -> int:
    if _is_null(val):
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


# ── Factor functions (pure, deterministic) ────────────────────────────────────

def priority_factor(priority: str | None) -> tuple[float, list[str]]:
    """Map Jira priority string to [0, 1]. Returns 0.5 for any null-like or unrecognised value."""
    s = _safe_str(priority)
    if s is None:
        return 0.5, ["missing_priority"]
    v = _PRIORITY_MAP.get(s.lower())
    if v is None:
        return 0.5, [f"unknown_priority:{s}"]
    return v, []


def due_factor(due_date: Optional[date], today: date) -> tuple[float, list[str]]:
    """Linear ramp: 1.0 if overdue, 0.0 if >14 days away. Returns 0.0 if missing."""
    if due_date is None:
        return 0.0, ["missing_due_date"]
    days_remaining = (due_date - today).days
    if days_remaining <= 0:
        return 1.0, []
    if days_remaining >= 14:
        return 0.0, []
    return round(1.0 - days_remaining / 14.0, 4), []


def blocked_factor(is_blocked: bool) -> tuple[float, list[str]]:
    """Returns 1.0 if blocked (applied as a PENALTY, not an addition)."""
    return (1.0, ["is_blocked"]) if is_blocked else (0.0, [])


def blocking_factor(blocking_keys: list[str]) -> tuple[float, list[str]]:
    """Each blocked dependency adds 0.25 (capped at 1.0)."""
    n = len(blocking_keys)
    return min(1.0, 0.25 * n), []


def age_factor(age_days: int | None) -> tuple[float, list[str]]:
    """Log-scaled: 0 at 0d, ~1 at 30d, capped at 1."""
    if age_days is None or age_days < 0:
        return 0.0, ["missing_age"]
    if age_days == 0:
        return 0.0, []
    return round(min(1.0, math.log(1 + age_days) / math.log(1 + 30)), 4), []


def sprint_factor(sprint_state: str | None) -> tuple[float, list[str]]:
    """Active sprint → 1.0, future/next → 0.3, backlog/none/null → 0.0.

    Accepts None, NaN, empty string, or non-string without crashing.
    """
    s = _safe_str(sprint_state)
    if s is None:
        return 0.0, ["not_in_sprint"]
    sl = s.lower()
    if sl == "active":
        return 1.0, []
    if sl in ("future", "next"):
        return 0.3, []
    return 0.0, []   # closed, archived, backlog, unknown


def stall_factor(days_in_status: int | None) -> tuple[float, list[str]]:
    """Ramp from 0 at 1d → 1 at ≥5d in the same status."""
    if days_in_status is None:
        return 0.0, ["missing_stall"]
    if days_in_status <= 1:
        return 0.0, []
    if days_in_status >= 5:
        return 1.0, []
    return round((days_in_status - 1) / 4.0, 4), []


# ── Scored issue result ───────────────────────────────────────────────────────

@dataclass
class FactorBreakdown:
    name: str
    weight: int
    raw: float          # 0..1 (or 1.0 for penalty)
    contribution: float # ±weight * raw (signed)
    warnings: list[str] = field(default_factory=list)
    is_penalty: bool = False


@dataclass
class ScoredIssue:
    key: str
    total: float                        # clamped [0, 100]
    raw_total: float                    # before clamping
    factors: list[FactorBreakdown]
    all_warnings: list[str] = field(default_factory=list)


# ── Core scoring function ─────────────────────────────────────────────────────

def score_issue(
    key: str,
    priority: str | None,
    due_date: Optional[date],
    is_blocked: bool,
    blocking_keys: list[str],
    age_days: int | None,
    sprint_state: str | None,
    days_in_status: int | None,
    today: date | None = None,
    weights: dict[str, int] | None = None,
) -> ScoredIssue:
    """Compute priority score for a single issue. Fully deterministic."""
    if today is None:
        today = date.today()
    w = weights or WEIGHTS

    pf, pw = priority_factor(priority)
    df, dw = due_factor(due_date, today)
    bf, bw = blocked_factor(is_blocked)
    blf, blw = blocking_factor(blocking_keys)
    af, aw = age_factor(age_days)
    sf, sw = sprint_factor(sprint_state)
    stf, stw = stall_factor(days_in_status)

    factors = [
        FactorBreakdown("priority", w["priority"], pf, round(w["priority"] * pf, 2), pw, False),
        FactorBreakdown("due",      w["due"],      df, round(w["due"] * df, 2),      dw, False),
        FactorBreakdown("blocked",  w["blocked"],  bf, round(-w["blocked"] * bf, 2), bw, True),
        FactorBreakdown("blocking", w["blocking"], blf, round(w["blocking"] * blf, 2), blw, False),
        FactorBreakdown("age",      w["age"],      af, round(w["age"] * af, 2),      aw, False),
        FactorBreakdown("sprint",   w["sprint"],   sf, round(w["sprint"] * sf, 2),   sw, False),
        FactorBreakdown("stall",    w["stall"],    stf, round(w["stall"] * stf, 2),  stw, False),
    ]

    raw_total = sum(f.contribution for f in factors)
    clamped = max(0.0, min(100.0, raw_total))

    all_warnings = pw + dw + bw + blw + aw + sw + stw

    return ScoredIssue(
        key=key,
        total=round(clamped, 1),
        raw_total=round(raw_total, 2),
        factors=factors,
        all_warnings=all_warnings,
    )


# ── DataFrame-level scorer ────────────────────────────────────────────────────

def score_dataframe(
    issues_df: pd.DataFrame,
    today: date | None = None,
    weights: dict[str, int] | None = None,
    exclude_done: bool = True,
) -> tuple[pd.DataFrame, dict[str, ScoredIssue]]:
    """Score every issue in issues_df.

    Returns:
        scored_df: issues_df with added columns (priority_score, score_warnings,
                   and one column per factor: score_f_priority, score_f_due, etc.)
        scores: mapping key → ScoredIssue for breakdown rendering
    """
    if today is None:
        today = date.today()

    df = issues_df.copy()

    if exclude_done and "status_category" in df.columns:
        # fillna("") prevents NaN from breaking .str.lower()
        cat = df["status_category"].fillna("").astype(str).str.lower()
        df = df[~cat.isin({"done", "complete", "closed", "resolved"})]

    if df.empty:
        return df, {}

    def _parse_due(val) -> Optional[date]:
        if _is_null(val):
            return None
        if isinstance(val, date):
            return val
        try:
            return date.fromisoformat(str(val)[:10])
        except Exception:
            return None

    def _get_list(val) -> list[str]:
        if _is_null(val):
            return []
        s = str(val).strip()
        if not s:
            return []
        return [x.strip() for x in s.split("|") if x.strip()]

    scores: dict[str, ScoredIssue] = {}
    score_rows: list[dict] = []

    for _, row in df.iterrows():
        key = str(row.get("key", ""))
        due = _parse_due(row.get("due_date"))
        blocking = _get_list(row.get("blocking_keys", ""))
        age = _safe_int(row.get("age_days"))
        days_in_status = _safe_int(row.get("days_in_current_status"))
        is_blocked = _safe_bool(row.get("is_blocked", False))
        # _safe_str guards against NaN/float/None — the root cause of the crash
        sprint_state = _safe_str(row.get("sprint_state"))
        priority = _safe_str(row.get("priority"))

        s = score_issue(
            key=key,
            priority=priority,
            due_date=due,
            is_blocked=is_blocked,
            blocking_keys=blocking,
            age_days=age,
            sprint_state=sprint_state,
            days_in_status=days_in_status,
            today=today,
            weights=weights,
        )
        scores[key] = s

        row_data = {
            "priority_score": s.total,
            "score_warnings": "|".join(s.all_warnings),
        }
        for f in s.factors:
            row_data[f"score_f_{f.name}"] = f.contribution  # signed contribution

        score_rows.append({"key": key, **row_data})

    score_meta = pd.DataFrame(score_rows)
    result = df.merge(score_meta, on="key", how="left")
    result = result.sort_values("priority_score", ascending=False).reset_index(drop=True)

    return result, scores
