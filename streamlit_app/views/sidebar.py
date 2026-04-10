"""Sidebar — date selector (single day or range), user filters, refresh button.

Auto-update strategy:
  - On every render, check whether today's snapshot exists.
  - If missing AND Jira credentials are present, show a prominent banner
    so the user can sync with one click.
  - No silent background pulls (Streamlit has no background threads),
    but the banner makes the required action obvious.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import streamlit as st

from cockpit_core.env_bootstrap import bootstrap, has_jira_credentials
from cockpit_core.storage.snapshots import list_snapshot_dates, read_issues, read_productivity
from streamlit_app import state

bootstrap()   # idempotent — loads .env once per process


# ── Internal helpers ──────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def _list_users(snapshots_dir: str, date_str: str) -> list[str]:
    root = Path(snapshots_dir)
    d = date.fromisoformat(date_str)
    prod_df = read_productivity(root, d)
    if not prod_df.empty and "user" in prod_df.columns:
        return sorted(prod_df["user"].dropna().unique().tolist())
    issues_df = read_issues(root, d)
    if not issues_df.empty and "assignee" in issues_df.columns:
        return sorted(issues_df["assignee"].dropna().unique().tolist())
    return []


def _has_jira_credentials() -> bool:
    return has_jira_credentials()


def _get_coverage_info(snapshots_root: Path) -> dict:
    """Return metadata about the local snapshot coverage."""
    available = list_snapshot_dates(snapshots_root)
    today = date.today()
    today_synced = today in available
    yesterday = today - timedelta(days=1)
    yesterday_synced = yesterday in available

    # Detect last sync time from file mtime of today's productivity parquet
    last_sync_ts: str | None = None
    if today_synced:
        p = snapshots_root / today.isoformat() / "productivity.parquet"
        if p.exists():
            mtime = datetime.fromtimestamp(p.stat().st_mtime)
            last_sync_ts = mtime.strftime("%H:%M")

    return {
        "available": available,
        "count": len(available),
        "earliest": min(available) if available else None,
        "latest": max(available) if available else None,
        "today": today,
        "today_synced": today_synced,
        "yesterday_synced": yesterday_synced,
        "last_sync_ts": last_sync_ts,
    }


# ── Jira operations ───────────────────────────────────────────────────────────

def _run_jira_refresh(snapshots_root: Path, target_date: date) -> None:
    from cockpit_core.config import load_config
    from cockpit_core.ingest.runner import IngestRunner

    try:
        config = load_config()
    except Exception as exc:
        st.error(f"Config error: {exc}")
        return

    with st.status("Refreshing from Jira…", expanded=True) as status_widget:
        def _progress(msg: str) -> None:
            status_widget.write(msg)

        runner = IngestRunner(config)
        result = runner.run(
            target_date=target_date,
            force=True,
            lookback_days=7,
            progress=_progress,
        )

        if result.status == "ok":
            status_widget.update(
                label=(
                    f"Refresh complete — {result.issues_seen} issues, "
                    f"{result.worklogs_seen} worklogs, "
                    f"{result.transitions_seen} transitions "
                    f"({result.duration_seconds:.1f}s)"
                ),
                state="complete",
                expanded=False,
            )
            st.session_state["last_ingest_result"] = result
        elif result.status == "skipped":
            status_widget.update(
                label="Snapshot already up-to-date (pass force=True to override)",
                state="complete",
                expanded=False,
            )
        else:
            status_widget.update(
                label=f"Refresh failed: {result.error_message}",
                state="error",
                expanded=True,
            )
            st.session_state["last_ingest_result"] = result

    state.bump_refresh()
    st.rerun()


def _run_backfill(snapshots_root: Path, from_date: date, to_date: date, force: bool) -> None:
    from cockpit_core.config import load_config
    from cockpit_core.ingest.backfill import BackfillRunner

    if from_date > to_date:
        st.error("From date must be before To date.")
        return

    try:
        config = load_config()
    except Exception as exc:
        st.error(f"Config error: {exc}")
        return

    with st.status("Running historical backfill…", expanded=True) as status_widget:
        def _progress(msg: str) -> None:
            status_widget.write(msg)

        runner = BackfillRunner(config)
        result = runner.run(from_date, to_date, force=force, progress=_progress)

        if result.status == "ok":
            status_widget.update(
                label=(
                    f"Backfill complete — {result.days_written} days written, "
                    f"{result.days_skipped} skipped. "
                    f"{result.issues_fetched} issues · "
                    f"{result.worklogs_fetched} worklogs · "
                    f"{result.transitions_fetched} transitions"
                ),
                state="complete",
                expanded=False,
            )
        else:
            status_widget.update(
                label=f"Backfill failed: {result.error_message}",
                state="error",
                expanded=True,
            )

    state.bump_refresh()
    st.rerun()


# ── Main render ───────────────────────────────────────────────────────────────

def render_sidebar(snapshots_root: Path) -> None:
    cov = _get_coverage_info(snapshots_root)
    available_dates = cov["available"]

    with st.sidebar:
        # ── Header ───────────────────────────────────────────────────────────
        st.markdown(
            '<div style="font-family:\'Space Grotesk\',sans-serif; font-size:16px; '
            'font-weight:700; color:#F0E8FF; padding-bottom:12px; '
            'border-bottom:1px solid #3D2B6B; margin-bottom:16px;">⚙ Cockpit Controls</div>',
            unsafe_allow_html=True,
        )

        # ── Mode badge ────────────────────────────────────────────────────────
        if _has_jira_credentials():
            _render_mode_badge("live")
        else:
            _render_mode_badge("snapshot")

        # ── History coverage indicator ─────────────────────────────────────────
        _render_coverage_info(cov)

        # ── Today-freshness banner (most important UX signal) ─────────────────
        if _has_jira_credentials():
            _render_today_banner(snapshots_root, cov)

        if not available_dates:
            st.warning("No snapshots found. Use the backfill or refresh button below.")

        # ── View mode toggle ──────────────────────────────────────────────────
        current_mode = state.get_view_mode()
        chosen_mode_label = st.radio(
            "📅 View mode",
            options=["Single Day", "Date Range"],
            index=1 if current_mode == "range" else 0,
            horizontal=True,
            key="sb_view_mode",
        )
        new_mode = "range" if chosen_mode_label == "Date Range" else "single"
        if new_mode != current_mode:
            state.set_view_mode(new_mode)

        st.write("")

        # ── Date picker(s) ────────────────────────────────────────────────────
        if new_mode == "single":
            _render_single_date(available_dates)
            selected_for_users = state.get_date()
        else:
            _render_date_range(available_dates)
            _, end = state.get_date_range()
            selected_for_users = end if end in available_dates else (available_dates[0] if available_dates else date.today())

        # ── User filter ───────────────────────────────────────────────────────
        if available_dates and selected_for_users in available_dates:
            users = _list_users(str(snapshots_root), selected_for_users.isoformat())
            if users:
                chosen = st.multiselect(
                    "👤 Filter engineers",
                    options=users,
                    default=st.session_state.get("selected_users", []),
                    key="sb_users",
                )
                st.session_state["selected_users"] = chosen

        st.divider()

        # ── Primary refresh controls ──────────────────────────────────────────
        refresh_date = state.get_date() if new_mode == "single" else state.get_date_range()[1]
        _render_refresh_controls(snapshots_root, refresh_date, cov)

        # ── Historical backfill (live mode only) ──────────────────────────────
        if _has_jira_credentials():
            st.divider()
            _render_backfill_panel(snapshots_root, cov)


# ── Sub-renderers ─────────────────────────────────────────────────────────────

def _render_coverage_info(cov: dict) -> None:
    """Show a compact coverage badge: date range + snapshot count."""
    if not cov["count"]:
        return

    earliest = cov["earliest"].strftime("%b %d") if cov["earliest"] else "?"
    latest   = cov["latest"].strftime("%b %d, %Y") if cov["latest"] else "?"
    count    = cov["count"]
    sync_str = f" · last sync {cov['last_sync_ts']}" if cov["last_sync_ts"] else ""

    st.markdown(
        f'<div style="background:#1A1330; border:1px solid #3D2B6B; border-radius:6px; '
        f'padding:5px 10px; font-size:10px; color:#9B8EC4; margin-bottom:10px; line-height:1.5;">'
        f'📅 <b style="color:#C8B8F0;">History:</b> {earliest} → {latest} '
        f'&nbsp;·&nbsp; <b style="color:#C8B8F0;">{count}</b> snapshot(s){sync_str}'
        f'</div>',
        unsafe_allow_html=True,
    )


def _render_today_banner(snapshots_root: Path, cov: dict) -> None:
    """Show a yellow banner when today's data is not yet synced."""
    today = cov["today"]
    if cov["today_synced"]:
        return  # today is already in the store — no banner needed

    last_sync_ts = cov["last_sync_ts"] or "—"
    st.markdown(
        f'<div style="background:#3D2A00; border:1px solid #7A5500; border-radius:6px; '
        f'padding:8px 12px; font-size:11px; color:#FFD060; margin-bottom:10px; line-height:1.5;">'
        f'⚠ <b>Today ({today.strftime("%b %d")}) not yet synced.</b><br>'
        f'<span style="color:#C49A4A;">Click below to pull today\'s Jira data.</span>'
        f'</div>',
        unsafe_allow_html=True,
    )
    if st.button(
        f"⚡ Sync Today ({today.strftime('%b %d')})",
        use_container_width=True,
        type="primary",
        key="sb_sync_today",
    ):
        _run_jira_refresh(snapshots_root, today)


def _render_refresh_controls(snapshots_root: Path, refresh_date: date, cov: dict) -> None:
    """Primary refresh button area."""
    if _has_jira_credentials():
        if st.button(
            f"🔄 Refresh from Jira ({refresh_date.strftime('%b %d')})",
            use_container_width=True,
            key="sb_refresh_live",
            type="primary" if cov["today_synced"] else "secondary",
        ):
            _run_jira_refresh(snapshots_root, refresh_date)

        last = st.session_state.get("last_ingest_result")
        if last and last.status == "ok":
            st.markdown(
                f'<div style="font-size:10px; color:#7B6BA8; margin-top:4px; line-height:1.4;">'
                f'Last pull: {last.issues_seen} issues · '
                f'{last.worklogs_seen} worklogs · '
                f'{last.transitions_seen} transitions<br>'
                f'Auth: {last.authenticated_as or "—"} · '
                f'{last.duration_seconds:.1f}s</div>',
                unsafe_allow_html=True,
            )
        elif last and last.status == "failed":
            st.markdown(
                f'<div style="font-size:10px; color:#FF6B6B; margin-top:4px;">'
                f'Last pull failed: {last.error_message[:80]}</div>',
                unsafe_allow_html=True,
            )
        return

    # Snapshot-only mode (no credentials)
    if st.button("🔄 Refresh UI", use_container_width=True, key="sb_refresh_snap"):
        state.bump_refresh()
        st.rerun()
    st.markdown(
        '<div style="font-size:10px; color:#7B6BA8; margin-top:4px; line-height:1.4;">'
        'No Jira credentials — showing cached snapshots.<br>'
        'Add <code>JIRA_BASE_URL</code>, <code>JIRA_EMAIL</code>,<br>'
        '<code>JIRA_API_TOKEN</code>, <code>JIRA_PROJECT_KEY</code><br>'
        'to your <code>.env</code> to enable live pulls.</div>',
        unsafe_allow_html=True,
    )


def _render_backfill_panel(snapshots_root: Path, cov: dict) -> None:
    """Historical backfill expander."""
    # Suggest a sensible default from-date: earliest existing snapshot or 30 days ago
    default_from = (
        cov["earliest"] - timedelta(days=30)
        if cov["earliest"]
        else date.today() - timedelta(days=30)
    )

    with st.expander("📚 Historical Backfill", expanded=not cov["count"]):
        st.markdown(
            '<div style="font-size:11px; color:#B0A0D0; margin-bottom:8px;">'
            'Pulls <b>full Jira history</b> — all issues, changelogs, worklogs '
            '(no date limit) — and reconstructs per-day snapshots. '
            'Use this to populate history before the current rolling window.'
            '</div>',
            unsafe_allow_html=True,
        )

        col_f, col_t = st.columns(2)
        with col_f:
            bf_from = st.date_input("From", value=default_from, key="sb_bf_from")
        with col_t:
            bf_to = st.date_input("To", value=date.today(), key="sb_bf_to")

        bf_force = st.checkbox("Overwrite existing snapshots", value=False, key="sb_bf_force")

        # Show how many days in the range already have snapshots
        if bf_from and bf_to:
            start, end = (bf_from, bf_to) if bf_from <= bf_to else (bf_to, bf_from)
            total_days = (end - start).days + 1
            existing = sum(1 for d in cov["available"] if start <= d <= end)
            missing = total_days - existing
            st.caption(
                f"{total_days} calendar days · {existing} already have snapshots · "
                f"{missing} will be reconstructed"
                + (" (overwrite mode)" if bf_force else "")
            )

        if st.button("🕐 Run Historical Backfill", use_container_width=True, key="sb_backfill"):
            _run_backfill(snapshots_root, bf_from, bf_to, bf_force)

        st.caption(
            "CLI equivalent: `python -m cockpit_core.ingest backfill "
            "--from YYYY-MM-DD --to YYYY-MM-DD`"
        )


def _render_single_date(available_dates: list[date]) -> None:
    if not available_dates:
        return
    current = state.get_date()
    if current not in available_dates:
        current = available_dates[0]

    selected = st.selectbox(
        "Date",
        options=available_dates,
        index=available_dates.index(current),
        format_func=lambda d: d.strftime("%a, %b %d, %Y"),
        key="sb_date_single",
    )
    state.set_date(selected)


def _render_date_range(available_dates: list[date]) -> None:
    """Date-range picker.

    Single day: constrained to available snapshot dates (makes no sense to
    "select" a day with no data).
    Range: allows any date — if some days in the range lack snapshots, the
    aggregate just uses the days that do exist, and coverage is reported.
    """
    today = date.today()

    # Default boundaries: earliest snapshot to today
    abs_min = min(available_dates) if available_dates else date(2025, 1, 1)
    abs_max = today

    current_start, current_end = state.get_date_range()
    # Clamp only the start to abs_min so we don't allow pre-Jira-project dates
    current_start = max(abs_min, current_start)
    current_end   = min(abs_max, current_end)

    col_s, col_e = st.columns(2)
    with col_s:
        new_start = st.date_input(
            "From",
            value=current_start,
            min_value=abs_min,
            max_value=abs_max,
            key="sb_date_range_start",
        )
    with col_e:
        new_end = st.date_input(
            "To",
            value=current_end,
            min_value=abs_min,
            max_value=abs_max,
            key="sb_date_range_end",
        )

    if new_start and new_end:
        state.set_date_range(new_start, new_end)
        state.set_date(new_end)

    # Coverage annotation
    if new_start and new_end and available_dates:
        start, end = (new_start, new_end) if new_start <= new_end else (new_end, new_start)
        total_days    = (end - start).days + 1
        snaps_in_range = sum(1 for d in available_dates if start <= d <= end)
        missing_days   = total_days - snaps_in_range
        if missing_days > 0:
            st.caption(
                f"{total_days} calendar day(s) · {snaps_in_range} with real data · "
                f"⚠ {missing_days} missing (run backfill to fill gaps)"
            )
        else:
            st.caption(f"{total_days} day(s) · all {snaps_in_range} have real data ✓")


def _render_mode_badge(mode: str) -> None:
    if mode == "live":
        bg, border, color, label = "#1A4D2222", "#2D7A3344", "#4CAF82", "🟢 Live — Jira connected"
    else:
        bg, border, color, label = "#4D3A1A22", "#7A5A2A44", "#C49A4A", "🟡 Snapshot only"

    st.markdown(
        f'<div style="background:{bg}; border:1px solid {border}; '
        f'border-radius:6px; padding:6px 10px; font-size:11px; '
        f'color:{color}; margin-bottom:12px; text-align:center; '
        f'font-weight:600;">{label}</div>',
        unsafe_allow_html=True,
    )
