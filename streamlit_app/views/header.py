"""Branded header with native Streamlit KPI metrics."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

from streamlit_app.theme.adapt_theme import COLORS  # noqa: F401


def render_header(
    prod_df: pd.DataFrame,
    issues_df: pd.DataFrame,
    done_df: pd.DataFrame,
    target_date: date,
    logo_path: Path | None,
    project_key: str,
    view_mode: str = "single",
    date_start: date | None = None,
    date_end: date | None = None,
) -> None:
    # ── KPI aggregation ───────────────────────────────────────────────────────
    hours = (
        float(prod_df["effort_hours"].sum())
        if not prod_df.empty and "effort_hours" in prod_df.columns
        else 0.0
    )
    tasks_done = int(done_df.shape[0]) if not done_df.empty else 0
    active_devs = (
        int(prod_df["has_real_activity"].astype(bool).sum())
        if not prod_df.empty and "has_real_activity" in prod_df.columns
        else 0
    )
    total_devs = int(prod_df["user"].nunique()) if not prod_df.empty and "user" in prod_df.columns else 0
    in_progress = 0
    blocked = 0
    if not issues_df.empty and "status_category" in issues_df.columns:
        cat = issues_df["status_category"].fillna("").astype(str).str.lower()
        in_progress = int(cat.isin({
            "indeterminate",
            "in progress",
            "em andamento",
        }).sum())
    if not issues_df.empty and "is_blocked" in issues_df.columns:
        blocked = int(issues_df["is_blocked"].astype(bool).sum())

    # ── Viewing period label ──────────────────────────────────────────────────
    if view_mode == "range" and date_start and date_end and date_start != date_end:
        if date_start.year == date_end.year:
            period_str = f"{date_start.strftime('%b %d')} → {date_end.strftime('%b %d, %Y')}"
        else:
            period_str = f"{date_start.strftime('%b %d, %Y')} → {date_end.strftime('%b %d, %Y')}"
    else:
        period_str = target_date.strftime("%A, %B %d, %Y")

    # ── Header — native Streamlit columns (no st.markdown flex hacks) ─────────
    # st.image() and st.columns() are immune to markdown container clipping.
    has_logo = logo_path is not None and Path(logo_path).exists()

    if has_logo:
        col_logo, col_title, col_badge = st.columns([1, 6, 1], gap="small")
    else:
        col_title, col_badge = st.columns([7, 1], gap="small")
        col_logo = None

    if col_logo is not None:
        with col_logo:
            st.image(str(logo_path), width=90)

    with col_title:
        st.markdown(
            f'<p style="'
            f'font-family: Space\\ Grotesk, Inter, sans-serif; '
            f'font-size: 22px; '
            f'font-weight: 700; '
            f'color: #1E1B4B; '
            f'line-height: 1.4; '
            f'margin: 6px 0 0 0; '
            f'padding: 0 0 0 14px; '
            f'border-left: 3px solid #9D4EDD; '
            f'">ADAPT Engineering Cockpit</p>'
            f'<p style="'
            f'font-family: Inter, sans-serif; '
            f'font-size: 12px; '
            f'color: #6B7280; '
            f'line-height: 1.5; '
            f'margin: 4px 0 6px 17px; '
            f'padding: 0; '
            f'">📅 {period_str}</p>',
            unsafe_allow_html=True,
        )

    with col_badge:
        st.markdown(
            f'<div style="padding-top: 10px; text-align: right;">'
            f'<span style="'
            f'background: #EDE9FE; '
            f'color: #7C3AED; '
            f'border: 1px solid #D8D0F5; '
            f'font-family: Space\\ Grotesk, Inter, sans-serif; '
            f'font-size: 11px; '
            f'font-weight: 700; '
            f'padding: 5px 14px; '
            f'border-radius: 20px; '
            f'letter-spacing: 0.8px; '
            f'white-space: nowrap; '
            f'">{project_key}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    # Thin purple-tinted divider under the header
    st.markdown(
        '<hr style="border:none; border-top:2px solid #EDE9FE; margin:2px 0 16px 0;">',
        unsafe_allow_html=True,
    )

    # ── KPI strip ────────────────────────────────────────────────────────────
    k1, k2, k3, k4, k5 = st.columns(5)

    hours_help = "Total hours logged via Jira worklogs"
    if view_mode == "range" and date_start and date_end and date_start != date_end:
        hours_help += " across the selected period"

    with k1:
        st.metric(label="🟢 Hours Logged", value=f"{hours:.1f}h", help=hours_help)
    with k2:
        st.metric(label="✅ Tasks Done", value=str(tasks_done),
                  help="Issues moved to Done status in the selected period")
    with k3:
        st.metric(label="👥 Active Engineers", value=f"{active_devs} / {total_devs}",
                  help="Engineers with logged effort vs total team")
    with k4:
        st.metric(label="🔄 In Progress", value=str(in_progress),
                  help="Open issues currently in-progress (snapshot at end of period)")
    with k5:
        st.metric(label="🚫 Blocked", value=str(blocked),
                  help="Issues currently blocked by dependencies")
