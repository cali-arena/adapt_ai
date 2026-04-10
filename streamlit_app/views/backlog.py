"""Backlog view — scored issue list with per-factor breakdown."""
from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from cockpit_core.duration.engine import format_hours
from cockpit_core.scoring.priority import ScoredIssue, score_dataframe
from streamlit_app.theme.adapt_theme import section_header

_FACTOR_META: dict[str, dict] = {
    "priority": {"icon": "🎯", "label": "Priority",  "tip": "Jira priority level (Highest=25 pts, Lowest=0)"},
    "due":      {"icon": "📅", "label": "Due date",  "tip": "1.0 if overdue, linear ramp to 0 at ≥14d away"},
    "blocked":  {"icon": "🚫", "label": "Blocked",   "tip": "Penalty −15 pts if blocked by another issue"},
    "blocking": {"icon": "⛓",  "label": "Blocking",  "tip": "Each blocked dependency adds up to 15 pts total"},
    "age":      {"icon": "📆", "label": "Age",       "tip": "Log-scaled: 0 at day 0 → 1.0 at day 30"},
    "sprint":   {"icon": "🏃", "label": "Sprint",    "tip": "Active sprint=1.0 · Future=0.3 · Backlog=0"},
    "stall":    {"icon": "🐌", "label": "Stall",     "tip": "Days in same status: 0 at 1d → 1.0 at ≥5d"},
}

_PRIO_EMOJI: dict[str, str] = {
    "highest": "🔴", "high": "🟠", "medium": "🟡", "low": "🟢", "lowest": "⚪",
}
_TYPE_ICON: dict[str, str] = {
    "Bug": "🐛", "Story": "📖", "Task": "✔", "Epic": "⚡", "Sub-task": "↳",
}
_WARN_LABELS: dict[str, str] = {
    "missing_due_date":          "⚠ no due date",
    "missing_priority":          "⚠ no priority",
    "missing_age":               "⚠ no age",
    "not_in_sprint":             "backlog",
    "is_blocked":                "🚫 blocked",
    "missing_stall":             "⚠ no stall",
    "completed_without_logging": "done/unlogged",
    "no_real_activity":          "⚠ no effort",
    "no_worklogs":               "⚠ no worklogs",
    "possible_overload":         "🔥 overload",
}


def _score_band(score: float) -> str:
    if score >= 80:
        return "🔴 Critical"
    if score >= 60:
        return "🟠 High"
    if score >= 40:
        return "🟡 Medium"
    return "🟢 Low"


def _fmt_warnings(raw: str) -> str:
    if not raw:
        return ""
    return "  ·  ".join(
        _WARN_LABELS.get(w.strip(), w.strip())
        for w in raw.split("|")
        if w.strip()
    )


def render_backlog(
    issues_df: pd.DataFrame,
    target_date: date,
    user_filter: list[str],
    duration_df: pd.DataFrame | None = None,
) -> None:
    section_header("Backlog — Priority Score")

    if issues_df.empty:
        st.info("No backlog data for this date.")
        return

    scored_df, scores = score_dataframe(issues_df, today=target_date, exclude_done=True)

    if scored_df.empty:
        st.info("All issues are resolved — nothing in the backlog.")
        return

    if user_filter and "assignee" in scored_df.columns:
        scored_df = scored_df[scored_df["assignee"].isin(user_filter)]

    if scored_df.empty:
        st.info("No backlog items match the current filter.")
        return

    # ── Controls ──────────────────────────────────────────────────────────────
    ctrl1, ctrl2, ctrl3 = st.columns([2, 2, 2])
    with ctrl1:
        type_opts = (
            ["All types"] + sorted(scored_df["issue_type"].dropna().unique().tolist())
            if "issue_type" in scored_df.columns else ["All types"]
        )
        type_filter = st.selectbox("Type", type_opts, key="bl_type")
    with ctrl2:
        status_opts = (
            ["All statuses"] + sorted(scored_df["status_category"].dropna().unique().tolist())
            if "status_category" in scored_df.columns else ["All statuses"]
        )
        status_filter = st.selectbox("Status", status_opts, key="bl_status")
    with ctrl3:
        show_n = st.slider(
            "Show top N",
            min_value=5,
            max_value=min(50, len(scored_df)),
            value=min(20, len(scored_df)),
            key="bl_n",
        )

    display_df = scored_df.copy()
    if type_filter != "All types" and "issue_type" in display_df.columns:
        display_df = display_df[display_df["issue_type"] == type_filter]
    if status_filter != "All statuses" and "status_category" in display_df.columns:
        display_df = display_df[display_df["status_category"] == status_filter]
    display_df = display_df.head(show_n)

    # ── Table ─────────────────────────────────────────────────────────────────
    _render_backlog_table(display_df, scores, duration_df)

    st.divider()

    # ── Breakdown ─────────────────────────────────────────────────────────────
    _render_breakdown_panel(display_df, scores)


def _render_backlog_table(
    df: pd.DataFrame,
    scores: dict[str, ScoredIssue],
    duration_df: pd.DataFrame | None = None,
) -> None:
    # Build in-progress duration lookup
    dur_lookup: dict[str, float] = {}
    if duration_df is not None and not duration_df.empty and "key" in duration_df.columns:
        for _, dr in duration_df.iterrows():
            ct = dr.get("currently_in_progress_hours")
            if ct and not pd.isna(ct):
                dur_lookup[str(dr["key"])] = float(ct)

    rows = []
    for rank, (_, row) in enumerate(df.iterrows(), 1):
        key = str(row.get("key", ""))
        issue_type = str(row.get("issue_type") or "Task")
        priority = str(row.get("priority") or "Medium")
        prio_icon = _PRIO_EMOJI.get(priority.lower().replace(" ", ""), "🔵")
        type_icon = _TYPE_ICON.get(issue_type, "●")
        sp = row.get("story_points")
        sp_str = f" · {sp:.0f} SP" if sp and not pd.isna(sp) else ""
        score = float(row.get("priority_score", 0))
        summary = str(row.get("summary", "—"))
        sprint = str(row.get("sprint_name") or "")
        is_blocked = bool(row.get("is_blocked", False))
        blocked_flag = " 🚫" if is_blocked else ""
        in_prog_h = dur_lookup.get(key)
        in_prog_str = f"⏳ {format_hours(in_prog_h)}" if in_prog_h is not None else "—"

        rows.append({
            "#": rank,
            "Key": f"{type_icon} {key}{sp_str}{blocked_flag}",
            "Priority": f"{prio_icon} {priority}",
            "Summary": summary[:80] + ("…" if len(summary) > 80 else ""),
            "Sprint": sprint[:30] if sprint else "—",
            "Assignee": str(row.get("assignee") or "—"),
            "Status": str(row.get("status_category") or "—"),
            "In Progress": in_prog_str,
            "Score": score,
            "Band": _score_band(score),
            "Warnings": _fmt_warnings(str(row.get("score_warnings") or "")),
        })

    tbl = pd.DataFrame(rows)
    st.dataframe(
        tbl,
        column_config={
            "#": st.column_config.NumberColumn(width="small"),
            "In Progress": st.column_config.TextColumn("⏳ In Progress", width="small"),
            "Score": st.column_config.ProgressColumn(
                "Score ▼",
                format="%.0f",
                min_value=0,
                max_value=100,
                width="medium",
            ),
            "Band": st.column_config.TextColumn("Band", width="small"),
            "Warnings": st.column_config.TextColumn("Warnings", width="medium"),
        },
        use_container_width=True,
        hide_index=True,
    )


def _render_breakdown_panel(df: pd.DataFrame, scores: dict[str, ScoredIssue]) -> None:
    st.subheader("Score Breakdown", divider=False)

    available = [
        str(r.get("key", ""))
        for _, r in df.iterrows()
        if str(r.get("key", "")) in scores
    ]
    if not available:
        return

    selected_key = st.selectbox(
        "Select issue for breakdown",
        options=available,
        format_func=lambda k: f"{k}  (score: {scores[k].total:.0f})",
        key="bl_breakdown_key",
        label_visibility="collapsed",
    )

    if selected_key not in scores:
        return

    scored = scores[selected_key]
    issue_row = df[df["key"] == selected_key]
    summary = (
        str(issue_row["summary"].iloc[0])
        if not issue_row.empty and "summary" in issue_row.columns
        else ""
    )
    priority = (
        str(issue_row["priority"].iloc[0])
        if not issue_row.empty and "priority" in issue_row.columns
        else "—"
    )

    # Header row
    hdr_info, hdr_score = st.columns([4, 1])
    with hdr_info:
        st.markdown(f"**{selected_key}** — {summary}")
        st.caption(f"Priority: {priority}")
    with hdr_score:
        st.metric("Score", f"{scored.total:.0f} / 100")

    # Factor breakdown as dataframe
    rows = []
    for f in scored.factors:
        meta = _FACTOR_META.get(f.name, {})
        icon = meta.get("icon", "?")
        label = meta.get("label", f.name)
        penalty_note = " (penalty)" if f.is_penalty else ""
        warn_text = "  ·  ".join(
            _WARN_LABELS.get(w, w) for w in f.warnings
        ) if f.warnings else ""
        rows.append({
            "Factor": f"{icon} {label}",
            "Max weight": f"{f.weight}{penalty_note}",
            "Raw (0–1)": round(f.raw, 3),
            "Contribution": f"{f.contribution:+.1f} pts",
            "Notes": warn_text,
        })

    # Total row
    rows.append({
        "Factor": "─" * 10,
        "Max weight": "",
        "Raw (0–1)": None,
        "Contribution": f"{scored.raw_total:+.1f} raw → {scored.total:.0f} final",
        "Notes": "clamped to [0, 100]",
    })

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    if scored.all_warnings:
        st.caption(
            "⚠ Score warnings: "
            + "  ·  ".join(_WARN_LABELS.get(w, w) for w in scored.all_warnings)
        )
