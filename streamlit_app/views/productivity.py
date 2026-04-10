"""Productivity view — enterprise-grade interactive charts.

Data sources (all real, no mock):
  prod_df       — today's per-user productivity (from productivity.parquet)
  weekly_df     — 7-day aggregate per user (computed from N snapshots)
  daily_trend_df — raw per-day-per-user rows for trend line chart
"""
from __future__ import annotations

from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from cockpit_core.duration.engine import format_hours
from streamlit_app.theme.adapt_theme import CHART_PALETTE, COLORS, chart_layout, section_header

_SUCCESS  = COLORS["success"]
_PRIMARY  = COLORS["primary"]
_ACCENT   = COLORS["accent"]
_WARNING  = COLORS["warning"]
_DANGER   = COLORS["danger"]
_INFO     = COLORS["info"]
_MUTED    = COLORS["text_muted"]
_BORDER   = COLORS["border"]
_SURFACE  = COLORS["surface"]
_TEXT     = COLORS["text_primary"]


# ── Chart config helper ───────────────────────────────────────────────────────

def _plotly_cfg() -> dict:
    return {"displayModeBar": False, "responsive": True}


def _bar_colors(values: list[float], positive_color: str, zero_color: str = _BORDER) -> list[str]:
    return [positive_color if v > 0 else zero_color for v in values]


# ── Main render ───────────────────────────────────────────────────────────────

def render_productivity(
    prod_df: pd.DataFrame,
    weekly_df: pd.DataFrame,
    daily_trend_df: pd.DataFrame,
    target_date: date,
    user_filter: list[str],
    duration_df: pd.DataFrame | None = None,
    done_df: pd.DataFrame | None = None,
) -> None:
    section_header("Productivity")

    if prod_df.empty:
        st.info("No productivity data for this date. Run a Jira refresh to populate snapshots.")
        return

    df = prod_df.copy()
    if user_filter:
        df = df[df["user"].isin(user_filter)]

    # Pre-compute per-user duration stats (requires done_df to map issue→user)
    user_dur = _build_user_duration_stats(duration_df, done_df)

    tab_daily, tab_weekly = st.tabs(["📅 Daily", "📈 7-Day Rolling"])

    with tab_daily:
        _render_daily(df, target_date, user_dur)

    with tab_weekly:
        _render_weekly(weekly_df, daily_trend_df, user_filter)


# ── Duration helpers ──────────────────────────────────────────────────────────

def _build_user_duration_stats(
    duration_df: pd.DataFrame | None,
    done_df: pd.DataFrame | None,
) -> dict[str, dict]:
    """Build per-user duration stats by joining duration_df with done_df (key→assignee).

    Returns dict: user → {avg_cycle_h, max_cycle_h, avg_lead_h, count_with_cycle}
    """
    if duration_df is None or duration_df.empty:
        return {}
    if done_df is None or done_df.empty:
        return {}
    if "key" not in duration_df.columns or "key" not in done_df.columns:
        return {}

    # Join issue→user via done_df
    merged = duration_df.merge(
        done_df[["key", "assignee"]].rename(columns={"assignee": "user"}),
        on="key",
        how="inner",
    )
    if merged.empty:
        return {}

    result: dict[str, dict] = {}
    for user, grp in merged.groupby("user"):
        ct = grp["cycle_time_hours"].dropna()
        lt = grp["lead_time_hours"].dropna()
        result[str(user)] = {
            "avg_cycle_h": float(ct.mean()) if not ct.empty else None,
            "max_cycle_h": float(ct.max()) if not ct.empty else None,
            "avg_lead_h":  float(lt.mean()) if not lt.empty else None,
            "count_with_cycle": int(len(ct)),
            "total_issues": int(len(grp)),
        }
    return result


# ── Daily tab ─────────────────────────────────────────────────────────────────

def _render_daily(df: pd.DataFrame, target_date: date, user_dur: dict | None = None) -> None:
    if df.empty:
        st.info("No data for the current filter.")
        return

    df = df.sort_values("effort_hours", ascending=False)

    # ── Compute which metrics have real data ──────────────────────────────────
    df_hours = df[df["effort_hours"].fillna(0) > 0]
    df_tasks = (
        df[df["effort_tasks_done"].fillna(0).astype(float) > 0]
        if "effort_tasks_done" in df.columns else pd.DataFrame()
    )
    df_ip = (
        df[df["issues_in_progress"].fillna(0).astype(float) > 0]
        if "issues_in_progress" in df.columns else pd.DataFrame()
    )
    _hours_mask = pd.to_numeric(df.get("effort_hours", pd.Series(0, index=df.index)), errors="coerce").fillna(0) > 0
    _ip_mask    = pd.to_numeric(df.get("issues_in_progress", pd.Series(0, index=df.index)), errors="coerce").fillna(0) > 0
    _done_mask  = pd.to_numeric(df.get("effort_tasks_done", pd.Series(0, index=df.index)), errors="coerce").fillna(0) > 0
    has_activity_mask = _hours_mask | _ip_mask | _done_mask
    df_cov = df[has_activity_mask] if "log_coverage" in df.columns else pd.DataFrame()

    # cycle time per user (from user_dur dict)
    cycle_users = {u: s for u, s in (user_dur or {}).items() if s.get("avg_cycle_h") is not None}

    # ── KPI strip (duration metrics — always first if available) ──────────────
    all_ct  = [s["avg_cycle_h"] for s in (user_dur or {}).values() if s.get("avg_cycle_h") is not None]
    all_lt  = [s["avg_lead_h"]  for s in (user_dur or {}).values() if s.get("avg_lead_h")  is not None]
    all_max = [s["max_cycle_h"] for s in (user_dur or {}).values() if s.get("max_cycle_h") is not None]

    if all_ct or all_max:
        longest_user = max((user_dur or {}).items(), key=lambda kv: kv[1].get("max_cycle_h") or 0)[0] if all_max else None
        d1, d2, d3, d4 = st.columns(4)
        with d1:
            st.metric("🔄 Avg cycle",  format_hours(sum(all_ct) / len(all_ct)) if all_ct else "—",
                      help="Average cycle time (In Progress → Done) across completed issues")
        with d2:
            st.metric("📬 Avg lead",   format_hours(sum(all_lt) / len(all_lt)) if all_lt else "—",
                      help="Average lead time (created → Done) across completed issues")
        with d3:
            st.metric("🐢 Max cycle",  format_hours(max(all_max)) if all_max else "—",
                      help="Longest single-issue cycle time today")
        with d4:
            st.metric("👤 Slowest eng", longest_user or "—",
                      help="Engineer with the longest max cycle time today")

    # ── Signal guide (compact) ────────────────────────────────────────────────
    has_hours = not df_hours.empty
    hours_note = "" if has_hours else " · <b style='color:#C49A4A;'>No worklogs today</b> — showing execution metrics"
    st.markdown(
        f'<div style="background:{_SURFACE}; border:1px solid {_BORDER}; '
        f'border-left:3px solid {_SUCCESS}; border-radius:8px; '
        f'padding:6px 14px; margin-bottom:12px; font-size:11px; color:{COLORS["text_secondary"]};">'
        f'🟢 <b>Effort</b> = worklogs + completions — primary signal.&nbsp;&nbsp;'
        f'🔘 <b>Activity</b> = issues touched — auxiliary only{hours_note}.'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Adaptive panel builder ────────────────────────────────────────────────
    # Build an ordered list of panels that actually have real data.
    # Render them in a 2-column grid — no empty cards, no wasted space.
    panels: list[tuple[str, callable]] = []

    if not df_hours.empty:
        panels.append(("hours", _panel_hours))

    if not df_tasks.empty:
        panels.append(("tasks", _panel_tasks))

    if cycle_users:
        panels.append(("cycle", _panel_cycle))

    if not df_ip.empty:
        panels.append(("ip", _panel_in_progress))

    if not df_cov.empty and "log_coverage" in df_cov.columns:
        panels.append(("cov", _panel_coverage))

    if not panels:
        st.caption("⚪ No operational metrics available for this date. Run a Jira refresh to populate data.")
    else:
        # Render first two panels in a top row, remaining in a second row
        _render_panel_grid(panels, df_hours, df_tasks, cycle_users, df_ip, df_cov)

    # Full detail always available in expander
    with st.expander("📋 Full daily detail", expanded=False):
        _render_daily_table(df, user_dur)


def _render_panel_grid(
    panels: list[tuple[str, callable]],
    df_hours: pd.DataFrame,
    df_tasks: pd.DataFrame,
    cycle_users: dict,
    df_ip: pd.DataFrame,
    df_cov: pd.DataFrame,
) -> None:
    """Lay panels out in a 2-column responsive grid, top two then remaining."""
    data = {
        "hours": df_hours,
        "tasks": df_tasks,
        "cycle": cycle_users,
        "ip": df_ip,
        "cov": df_cov,
    }
    funcs = {
        "hours": _panel_hours,
        "tasks": _panel_tasks,
        "cycle": _panel_cycle,
        "ip":    _panel_in_progress,
        "cov":   _panel_coverage,
    }
    for i in range(0, len(panels), 2):
        left_key, _ = panels[i]
        if i + 1 < len(panels):
            right_key, _ = panels[i + 1]
            col_l, col_r = st.columns(2)
            with col_l:
                funcs[left_key](data[left_key])
            with col_r:
                funcs[right_key](data[right_key])
        else:
            # Odd panel — render half-width on the left
            col_l, _ = st.columns([1, 1])
            with col_l:
                funcs[left_key](data[left_key])


def _panel_hours(df: pd.DataFrame) -> None:
    users = df["user"].astype(str).tolist()
    hours = [float(r.get("effort_hours", 0)) for _, r in df.iterrows()]
    layout = chart_layout("🟢 Hours logged", height=260, show_legend=False)
    layout["yaxis"]["ticksuffix"] = "h"
    fig = go.Figure(layout=layout)
    fig.add_trace(go.Bar(
        x=users, y=hours,
        marker=dict(color=[_SUCCESS] * len(hours), line=dict(width=0)),
        text=[f"{h:.1f}h" for h in hours],
        textposition="outside",
        textfont=dict(size=11, color=_TEXT),
        hovertemplate="<b>%{x}</b><br>%{y:.1f}h logged<extra></extra>",
    ))
    st.plotly_chart(fig, use_container_width=True, config=_plotly_cfg())


def _panel_tasks(df: pd.DataFrame) -> None:
    df_s = df.sort_values("effort_tasks_done", ascending=False)
    users = df_s["user"].astype(str).tolist()
    tasks = [int(float(r.get("effort_tasks_done", 0))) for _, r in df_s.iterrows()]
    layout = chart_layout("✅ Tasks completed", height=260, show_legend=False)
    layout["yaxis"]["dtick"] = 1
    fig = go.Figure(layout=layout)
    fig.add_trace(go.Bar(
        x=users, y=tasks,
        marker=dict(color=[_PRIMARY] * len(tasks), line=dict(width=0)),
        text=[str(t) for t in tasks],
        textposition="outside",
        textfont=dict(size=11, color=_TEXT),
        hovertemplate="<b>%{x}</b><br>%{y} tasks done<extra></extra>",
    ))
    st.plotly_chart(fig, use_container_width=True, config=_plotly_cfg())


def _panel_cycle(cycle_users: dict) -> None:
    """Avg cycle time per engineer — horizontal bar sorted fastest first."""
    items = sorted(
        [(u, s["avg_cycle_h"]) for u, s in cycle_users.items() if s.get("avg_cycle_h") is not None],
        key=lambda x: x[1],
    )
    if not items:
        return
    users  = [i[0] for i in items]
    values = [i[1] for i in items]
    labels = [format_hours(v) for v in values]

    # Colour by cycle speed: green < 8h, yellow < 24h, red >= 24h
    colors = [_SUCCESS if v < 8 else (_WARNING if v < 24 else _DANGER) for v in values]

    layout = chart_layout("🔄 Avg cycle time by engineer", height=260, show_legend=False)
    layout["xaxis"]["ticksuffix"] = "h"
    layout["xaxis"]["title"] = "Hours"
    layout["yaxis"]["title"] = ""
    fig = go.Figure(layout=layout)
    fig.add_trace(go.Bar(
        x=values, y=users,
        orientation="h",
        marker=dict(color=colors, line=dict(width=0)),
        text=labels,
        textposition="outside",
        textfont=dict(size=11),
        hovertemplate="<b>%{y}</b><br>avg cycle: %{text}<extra></extra>",
    ))
    # Reference line at 8h
    fig.add_vline(x=8, line_dash="dot", line_color=_MUTED, line_width=1,
                  annotation_text="8h", annotation_font_size=9, annotation_font_color=_MUTED)
    st.plotly_chart(fig, use_container_width=True, config=_plotly_cfg())


def _panel_in_progress(df: pd.DataFrame) -> None:
    df_s = df.sort_values("issues_in_progress", ascending=False)
    users  = df_s["user"].astype(str).tolist()
    in_prog = [int(float(r.get("issues_in_progress", 0))) for _, r in df_s.iterrows()]
    layout = chart_layout("🔄 Issues in progress", height=220, show_legend=False)
    layout["yaxis"]["dtick"] = 1
    fig = go.Figure(layout=layout)
    fig.add_trace(go.Bar(
        x=users, y=in_prog,
        marker=dict(color=[_INFO] * len(in_prog), line=dict(width=0)),
        text=[str(v) for v in in_prog],
        textposition="outside",
        textfont=dict(size=11),
        hovertemplate="<b>%{x}</b><br>%{y} in progress<extra></extra>",
    ))
    st.plotly_chart(fig, use_container_width=True, config=_plotly_cfg())


def _panel_coverage(df: pd.DataFrame) -> None:
    users_c = df["user"].astype(str).tolist()
    log_cov = [float(r.get("log_coverage", 0)) * 100 for _, r in df.iterrows()]
    cov_colors = [_SUCCESS if v >= 50 else (_WARNING if v > 0 else _DANGER) for v in log_cov]
    layout = chart_layout("📋 Worklog coverage", height=220, show_legend=False)
    layout["yaxis"]["ticksuffix"] = "%"
    layout["yaxis"]["range"] = [0, 110]
    fig = go.Figure(layout=layout)
    fig.add_trace(go.Bar(
        x=users_c, y=log_cov,
        marker=dict(color=cov_colors, line=dict(width=0)),
        text=[f"{v:.0f}%" for v in log_cov],
        textposition="outside",
        textfont=dict(size=11),
        hovertemplate="<b>%{x}</b><br>%{y:.0f}% coverage<extra></extra>",
    ))
    fig.add_hline(y=50, line_dash="dot", line_color=_MUTED, line_width=1,
                  annotation_text="50%", annotation_font_size=10, annotation_font_color=_MUTED)
    st.plotly_chart(fig, use_container_width=True, config=_plotly_cfg())


def _render_daily_table(df: pd.DataFrame, user_dur: dict | None = None) -> None:
    warn_labels = {
        "no_real_activity": "⚠ no effort",
        "no_worklogs": "⚠ no worklogs",
        "completed_without_logging": "⚠ done/unlogged",
        "possible_overload": "🔥 overload",
    }
    rows = []
    for _, row in df.iterrows():
        effort_h = float(row.get("effort_hours", 0))
        tasks_done = int(row.get("effort_tasks_done", 0))
        sp = float(row.get("effort_story_points", 0))
        transitions = int(row.get("effort_transitions", 0))
        activity = int(row.get("activity_updates", 0))
        in_prog = int(row.get("issues_in_progress", 0))
        cov = float(row.get("log_coverage", 0))
        warnings = str(row.get("score_warnings", ""))
        warn_str = "  ".join(
            warn_labels.get(w.strip(), w.strip())
            for w in warnings.split("|") if w.strip()
        )
        user = str(row.get("user", ""))
        udur = (user_dur or {}).get(user, {})
        avg_ct = udur.get("avg_cycle_h")
        max_ct = udur.get("max_cycle_h")
        avg_lt = udur.get("avg_lead_h")
        ct_count = udur.get("count_with_cycle", 0)
        rows.append({
            "Engineer": user,
            "Hours (effort)": f"{effort_h:.1f}h",
            "Done": tasks_done,
            "Story pts": f"{sp:.0f}",
            "Avg cycle": format_hours(avg_ct) if avg_ct is not None else "—",
            "Max cycle": format_hours(max_ct) if max_ct is not None else "—",
            "Avg lead": format_hours(avg_lt) if avg_lt is not None else "—",
            "Cycle n": ct_count if ct_count else "—",
            "In Progress": in_prog,
            "Log coverage": f"{int(cov * 100)}%",
            "Flags": warn_str,
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# ── Weekly tab ────────────────────────────────────────────────────────────────

def _render_weekly(
    weekly_df: pd.DataFrame,
    daily_trend_df: pd.DataFrame,
    user_filter: list[str],
) -> None:
    if weekly_df.empty:
        st.info(
            "Weekly data requires snapshots from multiple days. "
            "Run Jira refresh on consecutive days to build the 7-day view."
        )
        return

    df = weekly_df.copy()
    if user_filter:
        df = df[df["user"].isin(user_filter)]

    if df.empty:
        st.info("No weekly data for the current filter.")
        return

    # Data coverage indicator
    period_start = df["period_start"].iloc[0] if "period_start" in df.columns else "—"
    period_end   = df["period_end"].iloc[0]   if "period_end"   in df.columns else "—"
    max_days = df["days_with_activity"].max() if "days_with_activity" in df.columns else 0
    n_users = len(df)

    st.markdown(
        f'<div style="background:{_SURFACE}; border:1px solid {_BORDER}; border-radius:8px; '
        f'padding:8px 16px; margin-bottom:14px; display:flex; gap:24px; font-size:12px; '
        f'color:{COLORS["text_secondary"]};">'
        f'<span>📆 Period: <b>{period_start}</b> → <b>{period_end}</b></span>'
        f'<span>👥 <b>{n_users}</b> engineers</span>'
        f'<span>📊 Up to <b>{max_days}</b>/7 active days in window</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # Filter to users with real effort before any chart
    df = df[df["total_effort_hours"].fillna(0).astype(float) > 0]
    df = df.sort_values("total_effort_hours", ascending=False)

    if df.empty:
        st.info("No real worklog data in the 7-day window.")
        return

    users = df["user"].astype(str).tolist()
    total_h   = [float(r.get("total_effort_hours", 0)) for _, r in df.iterrows()]
    avg_h     = [float(r.get("avg_daily_hours", 0))    for _, r in df.iterrows()]
    tasks     = [int(r.get("total_tasks_done", 0))     for _, r in df.iterrows()]
    consist   = [int(r.get("consistency_pct", 0))      for _, r in df.iterrows()]
    days_act  = [int(r.get("days_with_activity", 0))   for _, r in df.iterrows()]

    # Row 1: Daily trend chart (if multi-day data available)
    if not daily_trend_df.empty and "date" in daily_trend_df.columns:
        _render_trend_chart(daily_trend_df, user_filter)

    # Row 2: aggregate charts
    col_l, col_r = st.columns(2)

    with col_l:
        layout = chart_layout("📊 Total vs avg daily hours", height=280)
        fig = go.Figure(layout=layout)
        fig.add_trace(go.Bar(
            name="Total hours",
            x=users,
            y=total_h,
            marker=dict(color=_PRIMARY, opacity=0.9, line=dict(width=0)),
            text=[f"{h:.1f}h" for h in total_h],
            textposition="outside",
            textfont=dict(size=10),
            hovertemplate="<b>%{x}</b><br>%{y:.1f}h total<extra></extra>",
        ))
        fig.add_trace(go.Bar(
            name="Avg/day",
            x=users,
            y=avg_h,
            marker=dict(color=_INFO, opacity=0.85, line=dict(width=0)),
            text=[f"{h:.1f}h" for h in avg_h],
            textposition="outside",
            textfont=dict(size=10),
            hovertemplate="<b>%{x}</b><br>%{y:.1f}h avg/day<extra></extra>",
        ))
        fig.update_layout(bargroupgap=0.08, bargap=0.22, showlegend=True)
        st.plotly_chart(fig, use_container_width=True, config=_plotly_cfg())

    with col_r:
        layout = chart_layout("📈 Consistency & active days", height=280)
        consist_colors = [
            _SUCCESS if c >= 70 else (_WARNING if c >= 40 else _DANGER)
            for c in consist
        ]
        fig = go.Figure(layout=layout)
        fig.add_trace(go.Bar(
            name="Consistency %",
            x=users,
            y=consist,
            marker=dict(color=consist_colors, line=dict(width=0)),
            text=[f"{c}%" for c in consist],
            textposition="outside",
            textfont=dict(size=11),
            hovertemplate="<b>%{x}</b><br>%{y}% consistency<extra></extra>",
            yaxis="y",
        ))
        fig.add_trace(go.Scatter(
            name="Active days",
            x=users,
            y=days_act,
            mode="markers+text",
            marker=dict(size=12, color=_ACCENT, symbol="diamond"),
            text=[f"{d}d" for d in days_act],
            textposition="top center",
            textfont=dict(size=10),
            hovertemplate="<b>%{x}</b><br>%{y} active days<extra></extra>",
            yaxis="y2",
        ))
        fig.update_layout(
            yaxis=dict(ticksuffix="%", title="Consistency %", range=[0, 120]),
            yaxis2=dict(
                title="Active days",
                overlaying="y",
                side="right",
                showgrid=False,
                range=[0, 10],
                dtick=1,
            ),
            showlegend=True,
        )
        st.plotly_chart(fig, use_container_width=True, config=_plotly_cfg())

    # Row 3: tasks done — only users with at least 1 completed task
    df_tasks_w = df[df["total_tasks_done"].fillna(0).astype(float) > 0]
    if df_tasks_w.empty:
        st.info("No completed tasks in selected 7-day window.")
    else:
        users_t = df_tasks_w["user"].astype(str).tolist()
        tasks_t = [int(r.get("total_tasks_done", 0)) for _, r in df_tasks_w.iterrows()]
        layout = chart_layout("✅ Tasks completed (7-day total)", height=220, show_legend=False)
        layout["yaxis"]["dtick"] = 1
        fig = go.Figure(layout=layout)
        fig.add_trace(go.Bar(
            x=users_t,
            y=tasks_t,
            marker=dict(color=[_SUCCESS] * len(tasks_t), line=dict(width=0)),
            text=[str(t) for t in tasks_t],
            textposition="outside",
            textfont=dict(size=11),
            hovertemplate="<b>%{x}</b><br>%{y} tasks done<extra></extra>",
        ))
        st.plotly_chart(fig, use_container_width=True, config=_plotly_cfg())

    # Detail table
    with st.expander("📋 Full weekly detail", expanded=False):
        _render_weekly_table(df)


def _render_trend_chart(daily_trend_df: pd.DataFrame, user_filter: list[str]) -> None:
    """Render a per-engineer daily effort trend line chart."""
    df = daily_trend_df.copy()
    if user_filter:
        df = df[df["user"].isin(user_filter)]
    if df.empty:
        return

    # Only include users who have at least one day with real hours
    df["effort_hours"] = pd.to_numeric(df["effort_hours"], errors="coerce").fillna(0)
    users_with_hours = df.groupby("user")["effort_hours"].sum()
    users_with_hours = users_with_hours[users_with_hours > 0].index
    df = df[df["user"].isin(users_with_hours)]
    if df.empty:
        return

    df["date"] = pd.to_datetime(df["date"]).dt.date
    users_in_data = sorted(df["user"].dropna().unique().tolist())
    n_days = df["date"].nunique()

    if n_days < 2:
        # Only 1 day — trend chart not useful, skip
        st.caption(f"📊 Trend chart requires ≥2 days of data — currently {n_days} day available.")
        return

    layout = chart_layout(
        f"📉 Daily effort trend — last {n_days} days",
        height=300,
        show_legend=True,
    )
    layout["hovermode"] = "x unified"
    layout["xaxis"]["tickformat"] = "%b %d"
    layout["yaxis"]["ticksuffix"] = "h"
    layout["yaxis"]["title"] = "Hours logged"

    fig = go.Figure(layout=layout)

    for i, user in enumerate(users_in_data):
        user_data = df[df["user"] == user].sort_values("date")
        color = CHART_PALETTE[i % len(CHART_PALETTE)]
        fig.add_trace(go.Scatter(
            x=user_data["date"].tolist(),
            y=user_data["effort_hours"].tolist(),
            name=user,
            mode="lines+markers",
            line=dict(color=color, width=2),
            marker=dict(size=6, color=color, symbol="circle"),
            hovertemplate=f"<b>{user}</b><br>%{{x|%b %d}}: %{{y:.1f}}h<extra></extra>",
            connectgaps=False,
        ))

    # Reference line at 8h/day
    fig.add_hline(
        y=8, line_dash="dot", line_color=_MUTED, line_width=1,
        annotation_text="8h target", annotation_position="right",
        annotation_font_size=10, annotation_font_color=_MUTED,
    )

    st.plotly_chart(fig, use_container_width=True, config=_plotly_cfg())


def _render_weekly_table(df: pd.DataFrame) -> None:
    rows = []
    for _, row in df.iterrows():
        total_h  = float(row.get("total_effort_hours", 0))
        avg_h    = float(row.get("avg_daily_hours", 0))
        tasks    = int(row.get("total_tasks_done", 0))
        sp       = float(row.get("total_story_points", 0))
        days_act = int(row.get("days_with_activity", 0))
        consist  = int(row.get("consistency_pct", 0))
        rows.append({
            "Engineer":       str(row.get("user", "")),
            "Total hours":    f"{total_h:.1f}h",
            "Avg / active day": f"{avg_h:.1f}h",
            "Tasks done":     tasks,
            "Story pts":      f"{sp:.0f}",
            "Active days":    f"{days_act} / 7",
            "Consistency":    f"{consist}%",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
