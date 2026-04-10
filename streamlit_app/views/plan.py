"""Plan view — TL decision layer on top of deterministic scores.

Layout:
  ┌─────────────────────────────────────────────────────┐
  │ TL Notes (text area, auto-saved)                    │
  │                                                     │
  │ ── Ranked plan table ─────────────────────────────  │
  │  #  Key   Summary   Assignee  Score  Override       │
  │  📌 BE-85  …         Lucas     50    📌 TL reason   │
  │  2  BE-84  …         Lucas     47                   │
  │  …                                                  │
  │                                                     │
  │ ── Override controls ─────────────────────────────  │
  │  Issue selector · Action · Reason · Apply           │
  │                                                     │
  │ [Export to Markdown]                                │
  └─────────────────────────────────────────────────────┘
"""
from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from cockpit_core.plan.assembler import build_plan, get_day_notes, get_issue_notes
from cockpit_core.storage.repo import CockpitRepository
from streamlit_app.exporters.markdown import export_plan_markdown
from streamlit_app.theme.adapt_theme import COLORS, section_header

_PRIO_EMOJI: dict[str, str] = {
    "highest": "🔴", "high": "🟠", "medium": "🟡", "low": "🟢", "lowest": "⚪",
}
_TYPE_ICON: dict[str, str] = {
    "Bug": "🐛", "Story": "📖", "Task": "✔", "Epic": "⚡", "Sub-task": "↳",
}
_WARN_LABELS: dict[str, str] = {
    "missing_due_date": "⚠ no due date",
    "missing_priority": "⚠ no priority",
    "missing_age": "⚠ no age",
    "not_in_sprint": "backlog",
    "is_blocked": "🚫 blocked",
    "missing_stall": "⚠ no stall",
}


def _fmt_warnings(raw: str) -> str:
    if not raw:
        return ""
    return "  ·  ".join(
        _WARN_LABELS.get(w.strip(), w.strip()) for w in raw.split("|") if w.strip()
    )


def render_plan(
    scored_df: pd.DataFrame,
    issues_df: pd.DataFrame,
    target_date: date,
    user_filter: list[str],
    project_key: str,
    repo: CockpitRepository,
) -> None:
    section_header("Daily Plan")

    if scored_df.empty:
        st.info("No scored issues available for this date.")
        return

    date_str = target_date.isoformat()

    # ── Load state from SQLite ─────────────────────────────────────────────
    overrides = repo.get_overrides_for_date(date_str)
    notes = _load_notes(repo, date_str)
    day_note = get_day_notes(notes)
    issue_notes = get_issue_notes(notes)

    # ── Apply user filter before building plan ─────────────────────────────
    df = scored_df.copy()
    if user_filter and "assignee" in df.columns:
        df = df[df["assignee"].isin(user_filter)]

    plan_df = build_plan(df, overrides, notes)

    if plan_df.empty:
        st.info("No backlog items match the current filter.")
        return

    # ── TL day note ────────────────────────────────────────────────────────
    _render_day_note_editor(repo, date_str, day_note)

    st.write("")  # vertical spacing

    # ── Export button ──────────────────────────────────────────────────────
    exp_col, _ = st.columns([1, 3])
    with exp_col:
        if st.button("📤 Export to Markdown", use_container_width=True, key="plan_export_btn"):
            _do_export(plan_df, target_date, project_key, day_note, issue_notes, repo, date_str)

    if st.session_state.get("plan_export_md"):
        st.success("✓ Plan exported — copy the text below")
        st.text_area(
            "Markdown (copy & paste into Slack/Notion)",
            value=st.session_state["plan_export_md"],
            height=300,
            key="plan_export_ta",
        )
        if st.button("Clear", key="plan_clear_export"):
            st.session_state["plan_export_md"] = ""
            st.rerun()

    # ── Plan table ─────────────────────────────────────────────────────────
    _render_plan_table(plan_df, issue_notes)

    st.write("")

    # ── Override controls ──────────────────────────────────────────────────
    _render_override_controls(plan_df, overrides, repo, date_str, project_key)


# ── TL day note ──────────────────────────────────────────────────────────────

def _load_notes(repo: CockpitRepository, date_str: str) -> list:
    return repo._conn.execute(
        "SELECT * FROM tl_note WHERE target_date=? ORDER BY id DESC",
        (date_str,),
    ).fetchall()


def _render_day_note_editor(repo: CockpitRepository, date_str: str, current: str) -> None:
    with st.expander("🗒 TL Day Note (included in export)", expanded=bool(current)):
        new_note = st.text_area(
            "Write today's context, focus, or decisions here:",
            value=current,
            height=100,
            key=f"day_note_{date_str}",
            placeholder="e.g. Sprint demo tomorrow — prioritise BE-84 and BE-85. BE-83 unblocked at 10am.",
        )
        if st.button("Save note", key=f"save_note_{date_str}"):
            _upsert_day_note(repo, date_str, new_note)
            repo.audit("tl", "note.add", f"date:{date_str}", {"scope": "day", "length": len(new_note)})
            st.success("Note saved.")
            st.rerun()


def _upsert_day_note(repo: CockpitRepository, date_str: str, body: str) -> None:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    repo._conn.execute("DELETE FROM tl_note WHERE target_date=? AND scope='day'", (date_str,))
    repo._conn.execute(
        "INSERT INTO tl_note (target_date, scope, body, created_at) VALUES (?, 'day', ?, ?)",
        (date_str, body, now),
    )
    repo._conn.commit()


# ── Plan table ───────────────────────────────────────────────────────────────

def _render_plan_table(plan_df: pd.DataFrame, issue_notes: dict[str, str]) -> None:
    rows = []
    for _, row in plan_df.iterrows():
        plan_rank = int(row["plan_rank"])
        orig_rank = int(row.get("original_rank", plan_rank))
        key = str(row.get("key", ""))
        summary = str(row.get("summary", "—"))
        assignee = str(row.get("assignee") or "—")
        priority = str(row.get("priority") or "Medium")
        issue_type = str(row.get("issue_type") or "Task")
        score = float(row.get("priority_score", 0))
        is_pinned = bool(row.get("is_pinned", False))
        rank_ov = row.get("rank_override")
        reason = str(row.get("override_reason") or "")
        sp = row.get("story_points")
        score_warnings = str(row.get("score_warnings") or "")
        is_blocked = bool(row.get("is_blocked", False))

        type_icon = _TYPE_ICON.get(issue_type, "●")
        prio_icon = _PRIO_EMOJI.get(priority.lower().replace(" ", ""), "🔵")
        sp_str = f" · {sp:.0f} SP" if sp and not pd.isna(sp) else ""
        blocked_flag = " 🚫" if is_blocked else ""

        # Rank column
        if is_pinned:
            rank_str = "📌"
        elif orig_rank != plan_rank:
            rank_str = f"{plan_rank} (was #{orig_rank})"
        else:
            rank_str = str(plan_rank)

        # Override / notes column
        override_parts = []
        if is_pinned:
            override_parts.append(f"📌 TL: {reason}" if reason else "📌 TL pinned")
        elif pd.notna(rank_ov):
            override_parts.append(f"🔀 moved: {reason}" if reason else "🔀 moved")
        if key in issue_notes:
            note_text = issue_notes[key][:50] + ("…" if len(issue_notes[key]) > 50 else "")
            override_parts.append(f"🗒 {note_text}")
        override_str = "  ·  ".join(override_parts)

        rows.append({
            "#": rank_str,
            "Key": f"{type_icon} {key}{blocked_flag}{sp_str}",
            "Priority": f"{prio_icon} {priority}",
            "Summary": summary[:70] + ("…" if len(summary) > 70 else ""),
            "Assignee": assignee,
            "Score": score,
            "Override / Notes": override_str,
            "Warnings": _fmt_warnings(score_warnings),
        })

    plan_tbl = pd.DataFrame(rows)
    st.dataframe(
        plan_tbl,
        column_config={
            "#": st.column_config.TextColumn("#", width="small"),
            "Score": st.column_config.ProgressColumn(
                "Score",
                format="%.0f",
                min_value=0,
                max_value=100,
                width="medium",
            ),
            "Override / Notes": st.column_config.TextColumn("Override / Notes", width="large"),
            "Warnings": st.column_config.TextColumn("Warnings", width="medium"),
        },
        use_container_width=True,
        hide_index=True,
    )


# ── Override controls ─────────────────────────────────────────────────────────

def _render_override_controls(
    plan_df: pd.DataFrame,
    overrides: list,
    repo: CockpitRepository,
    date_str: str,
    project_key: str,
) -> None:
    with st.expander("⚙ Override Controls — Pin / Reorder / Note", expanded=False):
        all_keys = plan_df["key"].tolist()
        if not all_keys:
            st.info("No issues to override.")
            return

        ov_col1, ov_col2 = st.columns([2, 3])

        with ov_col1:
            selected_key = st.selectbox(
                "Select issue",
                options=all_keys,
                format_func=lambda k: (
                    f"{k}  (#{int(plan_df[plan_df['key']==k]['plan_rank'].iloc[0])} · "
                    f"score {plan_df[plan_df['key']==k]['priority_score'].iloc[0]:.0f})"
                ),
                key="ov_issue",
            )
            action = st.radio(
                "Action",
                ["📌 Pin to top", "🔢 Set rank", "🗒 Issue note", "🗑 Remove override"],
                key="ov_action",
                horizontal=False,
            )

        with ov_col2:
            reason = ""
            rank_val = None

            if action == "📌 Pin to top":
                reason = st.text_input(
                    "Reason (required for audit trail)",
                    key="ov_reason",
                    placeholder="e.g. demo tomorrow, unblock team",
                )
                st.caption("Issue will appear at the top of the plan, above all scored items.")

            elif action == "🔢 Set rank":
                rank_val = st.number_input(
                    "Move to position #",
                    min_value=1,
                    max_value=len(all_keys),
                    value=1,
                    step=1,
                    key="ov_rank",
                )
                reason = st.text_input(
                    "Reason",
                    key="ov_reason_rank",
                    placeholder="e.g. reassigned, dependency resolved",
                )

            elif action == "🗒 Issue note":
                existing = _load_notes(repo, date_str)
                issue_notes = get_issue_notes(existing)
                default_note = issue_notes.get(selected_key, "")
                note_body = st.text_area(
                    f"Note for {selected_key}",
                    value=default_note,
                    key="ov_note_body",
                    placeholder="e.g. waiting for design approval, pair with Pablo",
                    height=80,
                )

            elif action == "🗑 Remove override":
                st.warning(
                    f"This will remove any pin/rank override for **{selected_key}** on {date_str}."
                )

            st.write("")

            if st.button("✅ Apply", key="ov_apply", use_container_width=True, type="primary"):
                if action == "📌 Pin to top":
                    if not reason.strip():
                        st.error("Please enter a reason for the pin.")
                    else:
                        repo.upsert_override(selected_key, date_str, rank_override=1, pinned=True, reason=reason)
                        repo.audit("tl", "override.pin", selected_key, {"date": date_str, "reason": reason})
                        st.success(f"📌 {selected_key} pinned to top.")
                        st.rerun()

                elif action == "🔢 Set rank":
                    repo.upsert_override(selected_key, date_str, rank_override=int(rank_val), pinned=False, reason=reason)
                    repo.audit("tl", "override.rank", selected_key, {"date": date_str, "rank": int(rank_val), "reason": reason})
                    st.success(f"🔢 {selected_key} moved to #{rank_val}.")
                    st.rerun()

                elif action == "🗒 Issue note":
                    _upsert_issue_note(repo, date_str, selected_key, note_body)
                    repo.audit("tl", "note.issue", selected_key, {"date": date_str, "length": len(note_body)})
                    st.success(f"🗒 Note saved for {selected_key}.")
                    st.rerun()

                elif action == "🗑 Remove override":
                    repo._conn.execute(
                        "DELETE FROM priority_override WHERE issue_key=? AND target_date=?",
                        (selected_key, date_str),
                    )
                    repo._conn.commit()
                    repo.audit("tl", "override.remove", selected_key, {"date": date_str})
                    st.success(f"Override removed for {selected_key}.")
                    st.rerun()

    # ── Active overrides summary ───────────────────────────────────────────
    if overrides:
        _render_overrides_summary(overrides, date_str)


def _upsert_issue_note(repo: CockpitRepository, date_str: str, key: str, body: str) -> None:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    scope = f"issue:{key}"
    repo._conn.execute("DELETE FROM tl_note WHERE target_date=? AND scope=?", (date_str, scope))
    if body.strip():
        repo._conn.execute(
            "INSERT INTO tl_note (target_date, scope, body, created_at) VALUES (?, ?, ?, ?)",
            (date_str, scope, body, now),
        )
    repo._conn.commit()


def _render_overrides_summary(overrides: list, date_str: str) -> None:
    st.caption(f"Active overrides for {date_str}")
    rows = []
    for ov in overrides:
        kind = "📌 pinned" if ov["pinned"] else f"🔢 rank #{ov['rank_override']}"
        rows.append({
            "Issue": ov["issue_key"],
            "Type": kind,
            "Reason": ov["reason"] or "—",
        })
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# ── Export ────────────────────────────────────────────────────────────────────

def _do_export(
    plan_df: pd.DataFrame,
    target_date: date,
    project_key: str,
    day_note: str,
    issue_notes: dict[str, str],
    repo: CockpitRepository,
    date_str: str,
) -> None:
    md = export_plan_markdown(
        plan_df=plan_df,
        target_date=target_date,
        project_key=project_key,
        day_note=day_note,
        issue_notes=issue_notes,
    )
    st.session_state["plan_export_md"] = md
    repo.audit("tl", "plan.export", f"date:{date_str}", {
        "issues": len(plan_df),
        "overrides": int((plan_df["is_pinned"] | plan_df["rank_override"].notna()).sum()),
    })
    st.rerun()
