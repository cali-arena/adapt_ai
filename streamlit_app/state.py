"""Session state contracts — typed accessors, no st.session_state sprawl."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import streamlit as st

from cockpit_core.storage.snapshots import list_snapshot_dates


def init_state(snapshots_dir: Path, project_key: str) -> None:
    dates = list_snapshot_dates(snapshots_dir)
    latest = dates[0] if dates else date.today()
    oldest = dates[-1] if dates else date.today()

    if "selected_date" not in st.session_state:
        st.session_state["selected_date"] = latest
    if "selected_users" not in st.session_state:
        st.session_state["selected_users"] = []
    if "selected_sprint" not in st.session_state:
        st.session_state["selected_sprint"] = None
    if "refresh_token" not in st.session_state:
        st.session_state["refresh_token"] = 0
    if "project_key" not in st.session_state:
        st.session_state["project_key"] = project_key
    # View mode: "single" | "range"
    if "view_mode" not in st.session_state:
        st.session_state["view_mode"] = "single"
    if "date_start" not in st.session_state:
        st.session_state["date_start"] = oldest
    if "date_end" not in st.session_state:
        st.session_state["date_end"] = latest


# ── Single date ───────────────────────────────────────────────────────────────

def get_date() -> date:
    return st.session_state.get("selected_date", date.today())


def set_date(d: date) -> None:
    st.session_state["selected_date"] = d


# ── View mode ─────────────────────────────────────────────────────────────────

def get_view_mode() -> str:
    """Return 'single' or 'range'."""
    return st.session_state.get("view_mode", "single")


def set_view_mode(mode: str) -> None:
    st.session_state["view_mode"] = mode


# ── Date range ────────────────────────────────────────────────────────────────

def get_date_range() -> tuple[date, date]:
    """Return (start, end) inclusive."""
    start = st.session_state.get("date_start", date.today())
    end = st.session_state.get("date_end", date.today())
    # Ensure start <= end
    if start > end:
        start, end = end, start
    return start, end


def set_date_range(start: date, end: date) -> None:
    if start > end:
        start, end = end, start
    st.session_state["date_start"] = start
    st.session_state["date_end"] = end


# ── Shared ────────────────────────────────────────────────────────────────────

def get_users() -> list[str]:
    return st.session_state.get("selected_users", [])


def get_sprint() -> str | None:
    return st.session_state.get("selected_sprint")


def get_refresh_token() -> int:
    return st.session_state.get("refresh_token", 0)


def bump_refresh() -> None:
    st.session_state["refresh_token"] = st.session_state.get("refresh_token", 0) + 1
