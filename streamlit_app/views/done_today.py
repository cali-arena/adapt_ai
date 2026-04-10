"""Done Today view — issues resolved on the selected date."""
from __future__ import annotations

from datetime import date, timezone
import pandas as pd
import streamlit as st

from cockpit_core.duration.engine import format_hours
from streamlit_app.theme.adapt_theme import COLORS, section_header

_PRIORITY_EMOJI = {
    "highest": "🔴",
    "high":    "🟠",
    "medium":  "🟡",
    "low":     "🟢",
    "lowest":  "⚪",
}

_TYPE_ICON = {
    "Bug":      "🐛",
    "Story":    "📖",
    "Task":     "✔",
    "Epic":     "⚡",
    "Sub-task": "↳",
}


def _prio_label(priority: str) -> str:
    key = priority.lower().replace(" ", "")
    icon = _PRIORITY_EMOJI.get(key, "🔵")
    return f"{icon} {priority}"


def _fmt_ts(ts) -> str:
    """Format a timestamp to human-readable local string."""
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return "—"
    try:
        import pandas as pd
        ts = pd.Timestamp(ts)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts.strftime("%b %d  %H:%M")
    except Exception:
        return str(ts)[:16]


def render_done_today(
    done_df: pd.DataFrame,
    target_date: date,
    user_filter: list[str],
    duration_df: pd.DataFrame | None = None,
    view_mode: str = "single",
) -> None:
    title = "Completed in Period" if view_mode == "range" else "Completed Today"
    section_header(title)

    if done_df.empty:
        if view_mode == "range":
            st.info("No tasks completed in the selected date range.")
        else:
            st.info(f"No tasks completed on {target_date.strftime('%B %d')}.")
        return

    df = done_df.copy()
    if user_filter:
        col = "assignee" if "assignee" in df.columns else "completed_by"
        if col in df.columns:
            df = df[df[col].isin(user_filter)]

    if df.empty:
        st.info("No completed tasks match the current filter.")
        return

    # Build duration lookup keyed by issue key
    dur_lookup: dict[str, dict] = {}
    if duration_df is not None and not duration_df.empty and "key" in duration_df.columns:
        for _, dr in duration_df.iterrows():
            dur_lookup[str(dr["key"])] = dr.to_dict()

    # ── Summary KPIs ──────────────────────────────────────────────────────────
    count = len(df)
    sp_total = float(df["story_points"].fillna(0).sum()) if "story_points" in df.columns else 0
    hours_total = float(df["logged_hours"].fillna(0).sum()) if "logged_hours" in df.columns else 0

    # Avg cycle time across completed issues that have it
    cycle_times = [
        float(dur_lookup[str(row.get("key", ""))].get("cycle_time_hours", float("nan")))
        for _, row in df.iterrows()
        if str(row.get("key", "")) in dur_lookup
        and not pd.isna(dur_lookup[str(row.get("key", ""))].get("cycle_time_hours", float("nan")))
    ]
    avg_cycle = sum(cycle_times) / len(cycle_times) if cycle_times else None

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("✅ Completed", str(count))
    with m2:
        st.metric("📦 Story Pts", f"{sp_total:.0f}" if sp_total else "—")
    with m3:
        st.metric("⏱ Hours Logged", f"{hours_total:.1f}h" if hours_total else "—")
    with m4:
        ct_label = format_hours(avg_cycle) if avg_cycle is not None else "—"
        ct_help = f"Average cycle time across {len(cycle_times)} issue(s) with full transition history"
        st.metric("🔄 Avg Cycle", ct_label, help=ct_help)

    st.write("")

    for _, row in df.iterrows():
        _render_issue_card(row, dur_lookup.get(str(row.get("key", ""))))


def _render_issue_card(row: pd.Series, dur: dict | None = None) -> None:
    key = str(row.get("key", ""))
    summary = str(row.get("summary", "—"))
    assignee = str(row.get("assignee") or row.get("completed_by") or "—")
    priority = str(row.get("priority") or "Medium")
    issue_type = str(row.get("issue_type") or "Task")
    sp = row.get("story_points")
    sprint = str(row.get("sprint_name") or "")
    est_h = row.get("estimate_hours")
    logged_h = row.get("logged_hours")
    labels_raw = str(row.get("labels") or "")
    labels = [lbl.strip() for lbl in labels_raw.split("|") if lbl.strip()]

    type_icon = _TYPE_ICON.get(issue_type, "●")

    # Duration fields
    started_at = dur.get("started_at") if dur else None
    completed_at = dur.get("completed_at") if dur else None
    cycle_h = dur.get("cycle_time_hours") if dur else None
    lead_h = dur.get("lead_time_hours") if dur else None
    reopened = int(dur.get("reopened_count", 0) or 0) if dur else 0
    has_start = bool(dur.get("has_start_transition", False)) if dur else False

    with st.container(border=True):
        # Row 1: key + type + priority + SP
        top_left, top_right = st.columns([3, 1])
        with top_left:
            sp_part = f"  `{sp:.0f} SP`" if sp and not pd.isna(sp) else ""
            st.markdown(f"{type_icon} **{key}**  ·  {_prio_label(priority)}{sp_part}")
        with top_right:
            if logged_h is not None and not pd.isna(logged_h) and float(logged_h) > 0:
                over = (
                    est_h is not None
                    and not pd.isna(est_h)
                    and float(est_h) > 0
                    and float(logged_h) > float(est_h) * 1.2
                )
                est_str = f" / {float(est_h):.1f}h est" if est_h and not pd.isna(est_h) else ""
                flag = " ⚠" if over else ""
                st.markdown(f"⏱ {float(logged_h):.1f}h{est_str}{flag}")

        # Row 2: summary
        st.markdown(summary)

        # Row 3: LIFECYCLE (the operational core)
        if dur:
            if has_start and cycle_h and not pd.isna(cycle_h):
                # Full lifecycle known
                reopen_note = f"  ·  ↩ reopened {reopened}×" if reopened else ""
                lead_note = f"  ·  lead {format_hours(lead_h)}" if lead_h and not pd.isna(lead_h) else ""
                lifecycle_str = (
                    f"▶ **{_fmt_ts(started_at)}** → **{_fmt_ts(completed_at)}** "
                    f"= **{format_hours(float(cycle_h))} cycle**{lead_note}{reopen_note}"
                )
                st.markdown(lifecycle_str)
            elif has_start and not (cycle_h and not pd.isna(cycle_h)):
                # Started but no completion in history (shouldn't happen for done tasks, flag it)
                st.caption(f"▶ Started {_fmt_ts(started_at)}  ·  completion not in history")
            else:
                # Missing start — was In Progress before the history window
                st.caption(
                    f"🔄 Completed {_fmt_ts(completed_at)}  ·  start not in history "
                    "(run a fresh Jira refresh to backfill)"
                )

        # Row 4: meta — assignee, sprint, labels
        meta_parts = [f"👤 {assignee}"]
        if sprint:
            meta_parts.append(f"📍 {sprint}")
        for lbl in labels[:3]:
            meta_parts.append(f"`{lbl}`")
        st.caption("  ·  ".join(meta_parts))
