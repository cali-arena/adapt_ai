"""ADAPT Engineering Daily Cockpit — Streamlit entry point.

Run locally:
    streamlit run streamlit_app/main.py

Streamlit Community Cloud:
    Set the entry point to ``streamlit_app/main.py`` and configure the Jira
    secrets in the dashboard (see ``.streamlit/secrets.toml.example``).
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

_root = Path(__file__).parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

# Load env vars from canonical locations before anything else reads os.environ
from cockpit_core.env_bootstrap import bootstrap as _bootstrap
_bootstrap()

import streamlit as st

st.set_page_config(
    page_title="ADAPT Engineering Cockpit",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={"About": "ADAPT Engineering Daily Cockpit — Phase 4"},
)

from cockpit_core.duration.engine import compute_issue_durations
from cockpit_core.productivity.metrics import (
    build_done_today,
    compute_weekly_productivity,
)
from cockpit_core.scoring.priority import score_dataframe
from cockpit_core.storage.repo import CockpitRepository
from cockpit_core.storage.snapshots import (
    list_snapshot_dates,
    read_issues,
    read_productivity,
    read_transitions,
    read_transitions_history,
    read_worklogs,
    snapshot_exists,
)
from streamlit_app import state
from streamlit_app.theme.adapt_theme import inject_css
from streamlit_app.views.ai_panel import render_ai_panel
from streamlit_app.views.backlog import render_backlog
from streamlit_app.views.done_today import render_done_today
from streamlit_app.views.header import render_header
from streamlit_app.views.plan import render_plan
from streamlit_app.views.productivity import render_productivity
from streamlit_app.views.sidebar import render_sidebar

APP_ROOT = Path(__file__).parent.parent
DATA_DIR = APP_ROOT / os.environ.get("COCKPIT_DATA_DIR", "data")
SNAPSHOTS_DIR = DATA_DIR / "snapshots"


def _find_logo(root: Path) -> Path | None:
    candidates = [
        root / "streamlit_app" / "theme" / "assets" / "logo_Adapt.svg",
        root / "streamlit_app" / "theme" / "assets" / "logo.png",
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


LOGO_PATH = _find_logo(APP_ROOT)


def _get_project_key() -> str:
    from dotenv import load_dotenv
    load_dotenv(APP_ROOT / ".env", override=False)
    load_dotenv(override=False)
    return os.environ.get("JIRA_PROJECT_KEY", "NAI")


@st.cache_resource
def _get_repo(db_path: str) -> CockpitRepository:
    return CockpitRepository(Path(db_path))


# ── Single-date loaders ───────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def _load_snapshot(snapshots_dir: str, target_date_str: str, refresh_token: int):
    root = Path(snapshots_dir)
    d = date.fromisoformat(target_date_str)
    return (
        read_issues(root, d),
        read_worklogs(root, d),
        read_transitions(root, d),
        read_productivity(root, d),
    )


@st.cache_data(ttl=300)
def _load_weekly(snapshots_dir: str, end_date_str: str, project_key: str, refresh_token: int):
    root = Path(snapshots_dir)
    d = date.fromisoformat(end_date_str)
    return compute_weekly_productivity(root, d, project_key, lookback_days=7)


@st.cache_data(ttl=300)
def _load_durations(data_dir: str, snapshots_dir: str, target_date_str: str, refresh_token: int):
    from datetime import datetime, timezone
    import pandas as pd
    data_root = Path(data_dir)
    snap_root = Path(snapshots_dir)
    d = date.fromisoformat(target_date_str)

    history_df = read_transitions_history(data_root, snap_root)
    if history_df.empty:
        return pd.DataFrame()

    issues_df = read_issues(snap_root, d)
    return compute_issue_durations(
        transitions_df=history_df,
        issues_df=issues_df if not issues_df.empty else None,
        now=datetime.now(timezone.utc),
    )


@st.cache_data(ttl=300)
def _load_daily_trend(snapshots_dir: str, end_date_str: str, refresh_token: int):
    import pandas as pd
    root = Path(snapshots_dir)
    d = date.fromisoformat(end_date_str)
    frames = []
    for i in range(7):
        day = d - timedelta(days=i)
        df = read_productivity(root, day)
        if not df.empty:
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else __import__("pandas").DataFrame()


# ── Range loaders ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def _load_range_productivity(
    snapshots_dir: str, start_date_str: str, end_date_str: str, refresh_token: int
):
    """Aggregate productivity rows for all snapshots in [start, end].

    Uses the real PRODUCTIVITY_COLS schema: effort_hours, effort_tasks_done,
    effort_story_points, effort_transitions, has_real_activity.
    """
    import pandas as pd
    root = Path(snapshots_dir)
    start = date.fromisoformat(start_date_str)
    end = date.fromisoformat(end_date_str)
    if start > end:
        start, end = end, start

    frames = []
    current = start
    while current <= end:
        df = read_productivity(root, current)
        if not df.empty:
            df = df.copy()
            df["snapshot_date"] = current
            frames.append(df)
        current += timedelta(days=1)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    if "user" not in combined.columns:
        return pd.DataFrame()

    # Aggregate per user — sum numeric effort columns, any-activity boolean
    num_cols = [c for c in [
        "effort_hours", "effort_tasks_done", "effort_story_points",
        "effort_transitions", "activity_updates", "issues_in_progress",
    ] if c in combined.columns]

    agg: dict = {c: "sum" for c in num_cols}
    if "has_real_activity" in combined.columns:
        agg["has_real_activity"] = "any"
    if "log_coverage" in combined.columns:
        agg["log_coverage"] = "mean"

    aggregated = combined.groupby("user", as_index=False).agg(agg)

    # Active days count (days where the user had any real effort)
    if "snapshot_date" in combined.columns and "effort_hours" in combined.columns:
        active = combined[combined["effort_hours"].fillna(0) > 0]
        day_counts = active.groupby("user")["snapshot_date"].nunique().rename("active_days")
        aggregated = aggregated.merge(day_counts, on="user", how="left")
        aggregated["active_days"] = aggregated["active_days"].fillna(0).astype(int)

    return aggregated


@st.cache_data(ttl=300)
def _load_range_done(
    snapshots_dir: str, start_date_str: str, end_date_str: str, refresh_token: int
):
    """Collect all tasks completed across [start, end] from daily snapshots."""
    import pandas as pd
    root = Path(snapshots_dir)
    start = date.fromisoformat(start_date_str)
    end = date.fromisoformat(end_date_str)
    if start > end:
        start, end = end, start

    frames = []
    current = start
    while current <= end:
        if snapshot_exists(root, current):
            issues_df = read_issues(root, current)
            tr_df = read_transitions(root, current)
            if not issues_df.empty:
                daily_done = build_done_today(issues_df, tr_df, current)
                if not daily_done.empty:
                    daily_done = daily_done.copy()
                    daily_done["completed_date"] = current
                    frames.append(daily_done)
        current += timedelta(days=1)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    # Deduplicate by issue key — keep the latest completion date
    if "key" in combined.columns and "completed_date" in combined.columns:
        combined = combined.sort_values("completed_date", ascending=False)
        combined = combined.drop_duplicates(subset=["key"], keep="first")
    return combined


@st.cache_data(ttl=300)
def _load_range_issues(snapshots_dir: str, end_date_str: str, refresh_token: int):
    """Load issues snapshot at end_date (most recent state of backlog)."""
    root = Path(snapshots_dir)
    d = date.fromisoformat(end_date_str)
    return read_issues(root, d)


@st.cache_data(ttl=300)
def _load_range_daily_trend(
    snapshots_dir: str, start_date_str: str, end_date_str: str, refresh_token: int
):
    """Load all daily productivity rows in the range for trend charts."""
    import pandas as pd
    root = Path(snapshots_dir)
    start = date.fromisoformat(start_date_str)
    end = date.fromisoformat(end_date_str)
    if start > end:
        start, end = end, start

    frames = []
    current = start
    while current <= end:
        df = read_productivity(root, current)
        if not df.empty:
            df = df.copy()
            df["snapshot_date"] = current
            frames.append(df)
        current += timedelta(days=1)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main() -> None:
    inject_css()

    project_key = _get_project_key()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)

    repo = _get_repo(str(DATA_DIR / "cockpit.db"))

    state.init_state(SNAPSHOTS_DIR, project_key)
    render_sidebar(SNAPSHOTS_DIR)

    view_mode = state.get_view_mode()
    user_filter = state.get_users()
    refresh = state.get_refresh_token()

    import pandas as pd

    if view_mode == "range":
        date_start, date_end = state.get_date_range()
        target_date = date_end  # use end date as reference for backlog/scoring

        # Range data loading
        prod_df = _load_range_productivity(
            str(SNAPSHOTS_DIR), date_start.isoformat(), date_end.isoformat(), refresh
        )
        done_df = _load_range_done(
            str(SNAPSHOTS_DIR), date_start.isoformat(), date_end.isoformat(), refresh
        )
        issues_df = _load_range_issues(str(SNAPSHOTS_DIR), date_end.isoformat(), refresh)
        daily_trend_df = _load_range_daily_trend(
            str(SNAPSHOTS_DIR), date_start.isoformat(), date_end.isoformat(), refresh
        )
        weekly_df = _load_weekly(str(SNAPSHOTS_DIR), date_end.isoformat(), project_key, refresh)
        duration_df = _load_durations(
            str(DATA_DIR), str(SNAPSHOTS_DIR), date_end.isoformat(), refresh
        )
        # Single date dummies not needed; wl_df and tr_df unused in range mode
        wl_df = tr_df = pd.DataFrame()

    else:
        target_date = state.get_date()
        date_start = date_end = target_date

        if snapshot_exists(SNAPSHOTS_DIR, target_date):
            issues_df, wl_df, tr_df, prod_df = _load_snapshot(
                str(SNAPSHOTS_DIR), target_date.isoformat(), refresh
            )
            done_df = build_done_today(issues_df, tr_df, target_date)
            weekly_df = _load_weekly(str(SNAPSHOTS_DIR), target_date.isoformat(), project_key, refresh)
            daily_trend_df = _load_daily_trend(str(SNAPSHOTS_DIR), target_date.isoformat(), refresh)
        else:
            issues_df = wl_df = tr_df = prod_df = done_df = weekly_df = daily_trend_df = pd.DataFrame()

        duration_df = _load_durations(
            str(DATA_DIR), str(SNAPSHOTS_DIR), target_date.isoformat(), refresh
        )

    # Score open issues
    scored_df, scores = score_dataframe(issues_df, today=target_date) if not issues_df.empty else ({}, {})
    if not isinstance(scored_df, pd.DataFrame):
        scored_df = pd.DataFrame()

    # ── Header ───────────────────────────────────────────────────────────────
    render_header(
        prod_df=prod_df,
        issues_df=issues_df,
        done_df=done_df,
        target_date=target_date,
        logo_path=LOGO_PATH,
        project_key=project_key,
        view_mode=view_mode,
        date_start=date_start,
        date_end=date_end,
    )

    left, right = st.columns([3, 2], gap="large")
    with left:
        render_productivity(prod_df, weekly_df, daily_trend_df, target_date, user_filter, duration_df, done_df)
    with right:
        render_done_today(done_df, target_date, user_filter, duration_df, view_mode=view_mode)

    # ── Bottom section ────────────────────────────────────────────────────────
    st.divider()
    tab_backlog, tab_plan, tab_ai = st.tabs(["📋 Backlog Score", "📝 Daily Plan", "🤖 AI Advisor"])

    with tab_backlog:
        render_backlog(issues_df, target_date, user_filter, duration_df)

    with tab_plan:
        render_plan(scored_df, issues_df, target_date, user_filter, project_key, repo)

    with tab_ai:
        render_ai_panel(
            scored_df=scored_df,
            scores=scores,
            prod_df=prod_df,
            done_df=done_df,
            issues_df=issues_df,
            weekly_df=weekly_df,
            target_date=target_date,
            repo=repo,
        )


if __name__ == "__main__":
    main()
