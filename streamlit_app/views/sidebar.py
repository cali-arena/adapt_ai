"""Sidebar — date selector (single day or range), user filters, refresh button.

Date selection model:
  - The calendar is NOT bounded by existing snapshot dates. Users can pick any
    date in [project_history_floor, today]. project_history_floor is
    PROJECT_HISTORY_FLOOR_DAYS days ago, OR earlier if existing snapshots
    extend further back.
  - When the user lands on a date that has no local snapshot, a prominent
    "Fetch from Jira" banner is rendered. Clicking it runs an idempotent
    on-demand ingest for that exact date and reruns the app.
  - The same hydrate path is exposed for date ranges via a "Backfill missing
    days" button on the range picker.

Refresh strategy:
  - On every render, check whether today's snapshot exists. Banner if not.
  - The primary "Refresh" button is context-aware: in single-day mode it
    re-fetches the SELECTED day; in range mode it backfills missing days
    inside the selected range.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import streamlit as st

from cockpit_core.env_bootstrap import bootstrap, has_jira_credentials
from cockpit_core.storage.snapshots import list_snapshot_dates, read_issues, read_productivity
from streamlit_app import state

bootstrap()   # idempotent — loads .env once per process

# How far back the calendar lets the user go by default. The picker also
# extends to whatever existing snapshots reach below this floor, so older
# manually-backfilled history is never hidden.
PROJECT_HISTORY_FLOOR_DAYS = 730


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
            st.warning("No snapshots found. Use the fetch / backfill button below.")

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
            selected_single = state.get_date()
            # Hydrate-on-demand banner (only when there's no snapshot yet)
            _render_missing_snapshot_banner(snapshots_root, selected_single, available_dates)
            # User filter source: prefer the selected date, but fall back to
            # the latest snapshot for the multiselect options (so the dropdown
            # is never empty when the user lands on an un-hydrated day).
            selected_for_users = (
                selected_single
                if selected_single in available_dates
                else (max(available_dates) if available_dates else selected_single)
            )
        else:
            _render_date_range(available_dates)
            range_start, range_end = state.get_date_range()
            _render_missing_range_banner(snapshots_root, range_start, range_end, available_dates)
            selected_for_users = (
                range_end
                if range_end in available_dates
                else (max(available_dates) if available_dates else range_end)
            )

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

        # ── Primary refresh controls (context-aware) ──────────────────────────
        _render_refresh_controls(snapshots_root, new_mode, cov, available_dates)

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


def _render_refresh_controls(
    snapshots_root: Path,
    view_mode: str,
    cov: dict,
    available_dates: list[date],
) -> None:
    """Context-aware refresh button.

    Behavior:
      - Single-day mode: re-pulls the SELECTED day from Jira (force=True) so
        the user can update an old date as easily as today.
      - Range mode: backfills any missing day in the selected range.
      - Snapshot-only (no credentials): just bumps the refresh token to
        invalidate the Streamlit cache.
    """
    if not _has_jira_credentials():
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
        return

    if view_mode == "single":
        target = state.get_date()
        if st.button(
            f"🔄 Refresh from Jira ({target.strftime('%b %d')})",
            use_container_width=True,
            key="sb_refresh_live_single",
            type="primary" if cov["today_synced"] else "secondary",
        ):
            _run_jira_refresh(snapshots_root, target)
    else:
        range_start, range_end = state.get_date_range()
        missing = [
            range_start + timedelta(days=i)
            for i in range((range_end - range_start).days + 1)
            if (range_start + timedelta(days=i)) not in available_dates
        ]
        if missing:
            label = f"🔄 Refresh range — fetch {len(missing)} missing day(s)"
        else:
            label = (
                f"🔄 Re-fetch latest day in range "
                f"({range_end.strftime('%b %d')})"
            )
        if st.button(
            label,
            use_container_width=True,
            key="sb_refresh_live_range",
            type="primary",
        ):
            if missing:
                if ensure_snapshots_for_range(
                    snapshots_root, range_start, range_end, force=False
                ):
                    state.bump_refresh()
                    st.rerun()
            else:
                _run_jira_refresh(snapshots_root, range_end)

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


def ensure_snapshot_for_date(snapshots_root: Path, target_date: date) -> bool:
    """Idempotently materialize a snapshot for ``target_date`` from Jira.

    Returns True iff a snapshot exists at the end of the call (either it
    already existed, or this call successfully created it). Streamlit UI
    feedback is rendered via ``st.status``. Caller must ``st.rerun()`` after
    a successful hydrate to refresh cached data.
    """
    from cockpit_core.config import load_config
    from cockpit_core.ingest.runner import IngestRunner
    from cockpit_core.storage.snapshots import snapshot_exists

    if snapshot_exists(snapshots_root, target_date):
        return True

    if not _has_jira_credentials():
        st.error(
            "Cannot fetch from Jira — credentials missing. "
            "Set JIRA_BASE_URL / JIRA_EMAIL / JIRA_API_TOKEN / JIRA_PROJECT_KEY."
        )
        return False

    try:
        config = load_config()
    except Exception as exc:
        st.error(f"Config error: {exc}")
        return False

    with st.status(f"Fetching {target_date.isoformat()} from Jira…", expanded=True) as sw:
        def _progress(msg: str) -> None:
            sw.write(msg)

        runner = IngestRunner(config)
        result = runner.run(
            target_date=target_date,
            force=False,        # idempotent — skip if already there
            lookback_days=7,
            progress=_progress,
        )

        if result.status in ("ok", "skipped"):
            sw.update(
                label=(
                    f"✓ {target_date.isoformat()} hydrated — "
                    f"{result.issues_seen} issues, {result.worklogs_seen} worklogs, "
                    f"{result.transitions_seen} transitions"
                ),
                state="complete",
                expanded=False,
            )
            st.session_state["last_ingest_result"] = result
            return True

        sw.update(
            label=f"Hydrate failed: {result.error_message}",
            state="error",
            expanded=True,
        )
        st.session_state["last_ingest_result"] = result
        return False


def ensure_snapshots_for_range(
    snapshots_root: Path,
    start: date,
    end: date,
    force: bool = False,
) -> bool:
    """Idempotently materialize snapshots for every day in [start, end].

    Uses BackfillRunner so the API calls are batched (one historical pull,
    then per-day reconstruction). Returns True iff the run completed.
    """
    from cockpit_core.config import load_config
    from cockpit_core.ingest.backfill import BackfillRunner

    if start > end:
        start, end = end, start

    if not _has_jira_credentials():
        st.error("Cannot fetch from Jira — credentials missing.")
        return False

    try:
        config = load_config()
    except Exception as exc:
        st.error(f"Config error: {exc}")
        return False

    with st.status(
        f"Backfilling {start.isoformat()} → {end.isoformat()} from Jira…",
        expanded=True,
    ) as sw:
        def _progress(msg: str) -> None:
            sw.write(msg)

        runner = BackfillRunner(config)
        result = runner.run(start, end, force=force, progress=_progress)

        if result.status == "ok":
            sw.update(
                label=(
                    f"✓ Backfill complete — {result.days_written} new, "
                    f"{result.days_skipped} skipped"
                ),
                state="complete",
                expanded=False,
            )
            return True

        sw.update(
            label=f"Backfill failed: {result.error_message}",
            state="error",
            expanded=True,
        )
        return False


def _render_missing_snapshot_banner(
    snapshots_root: Path,
    selected_date: date,
    available_dates: list[date],
) -> None:
    """Prominent prompt rendered when the user has selected a single date
    that has no local snapshot. One click hydrates that exact date from
    Jira (or backfills the gap relative to the latest snapshot)."""
    if selected_date in available_dates:
        return  # already hydrated, nothing to do

    if not _has_jira_credentials():
        st.markdown(
            f'<div style="background:#3D2A00; border:1px solid #7A5500; border-radius:6px; '
            f'padding:8px 12px; font-size:11px; color:#FFD060; margin-top:8px; line-height:1.5;">'
            f'⚠ <b>No snapshot for {selected_date.strftime("%b %d, %Y")}.</b><br>'
            f'<span style="color:#C49A4A;">Add Jira credentials to fetch it on demand.</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
        return

    st.markdown(
        f'<div style="background:#1A2D4D; border:1px solid #2D5C8A; border-radius:6px; '
        f'padding:8px 12px; font-size:11px; color:#7FB8FF; margin-top:8px; line-height:1.5;">'
        f'ℹ <b>No local snapshot for {selected_date.strftime("%b %d, %Y")}.</b><br>'
        f'<span style="color:#5B9CD8;">One click pulls that day from Jira.</span>'
        f'</div>',
        unsafe_allow_html=True,
    )
    if st.button(
        f"⬇ Fetch {selected_date.strftime('%b %d')} from Jira",
        use_container_width=True,
        type="primary",
        key="sb_hydrate_single",
    ):
        if ensure_snapshot_for_date(snapshots_root, selected_date):
            state.bump_refresh()
            st.rerun()


def _render_missing_range_banner(
    snapshots_root: Path,
    start: date,
    end: date,
    available_dates: list[date],
) -> None:
    """Prompt rendered when the selected range has missing days."""
    if start > end:
        start, end = end, start
    missing = [
        start + timedelta(days=i)
        for i in range((end - start).days + 1)
        if (start + timedelta(days=i)) not in available_dates
    ]
    if not missing:
        return

    if not _has_jira_credentials():
        st.markdown(
            f'<div style="background:#3D2A00; border:1px solid #7A5500; border-radius:6px; '
            f'padding:8px 12px; font-size:11px; color:#FFD060; margin-top:8px; line-height:1.5;">'
            f'⚠ <b>{len(missing)} day(s) missing in this range.</b> '
            f'Add Jira credentials to fetch them.'
            f'</div>',
            unsafe_allow_html=True,
        )
        return

    st.markdown(
        f'<div style="background:#1A2D4D; border:1px solid #2D5C8A; border-radius:6px; '
        f'padding:8px 12px; font-size:11px; color:#7FB8FF; margin-top:8px; line-height:1.5;">'
        f'ℹ <b>{len(missing)} day(s) missing in this range.</b><br>'
        f'<span style="color:#5B9CD8;">One click backfills them from Jira.</span>'
        f'</div>',
        unsafe_allow_html=True,
    )
    if st.button(
        f"⬇ Backfill {len(missing)} missing day(s)",
        use_container_width=True,
        type="primary",
        key="sb_hydrate_range",
    ):
        if ensure_snapshots_for_range(snapshots_root, start, end, force=False):
            state.bump_refresh()
            st.rerun()


def _project_date_bounds(available_dates: list[date]) -> tuple[date, date]:
    """Compute the calendar bounds for the date picker.

    Returns (min_selectable, today). The minimum is whichever is older:
    PROJECT_HISTORY_FLOOR_DAYS ago, or the earliest existing snapshot.
    The picker is INTENTIONALLY NOT constrained to the set of existing
    snapshot dates — that was the bug that locked users to the latest
    snapshot. Any date in [min_selectable, today] is selectable, and dates
    without a snapshot are hydrated on demand via the banner / refresh button.
    """
    today = date.today()
    floor = today - timedelta(days=PROJECT_HISTORY_FLOOR_DAYS)
    if available_dates:
        floor = min(floor, min(available_dates))
    return floor, today


def _render_single_date(available_dates: list[date]) -> None:
    """Free single-date picker — any date in [project_floor, today]."""
    abs_min, abs_max = _project_date_bounds(available_dates)

    current = state.get_date()
    # Clamp the persisted state into the bounds, but DO NOT snap to an
    # existing snapshot date — the user must be free to navigate.
    if current < abs_min:
        current = abs_min
    if current > abs_max:
        current = abs_max

    selected = st.date_input(
        "📅 Date",
        value=current,
        min_value=abs_min,
        max_value=abs_max,
        format="YYYY-MM-DD",
        key="sb_date_single",
    )
    if selected:
        state.set_date(selected)

    # Inline hint about whether the picked date is already snapshotted
    if selected and available_dates:
        if selected in available_dates:
            st.caption(f"✓ Snapshot ready for {selected.strftime('%a, %b %d, %Y')}")
        else:
            st.caption(
                f"⚠ No local snapshot for {selected.strftime('%a, %b %d, %Y')} — "
                f"use the fetch button below."
            )


def _render_date_range(available_dates: list[date]) -> None:
    """Free date-range picker.

    Bounds: [project_floor, today]. The picker is intentionally NOT clamped
    to the earliest existing snapshot — users can pick a range that extends
    into un-snapshotted history, and the missing days are hydrated on demand
    via the "Backfill missing days" button below.
    """
    abs_min, abs_max = _project_date_bounds(available_dates)

    current_start, current_end = state.get_date_range()
    current_start = max(abs_min, min(current_start, abs_max))
    current_end   = max(abs_min, min(current_end,   abs_max))

    col_s, col_e = st.columns(2)
    with col_s:
        new_start = st.date_input(
            "From",
            value=current_start,
            min_value=abs_min,
            max_value=abs_max,
            format="YYYY-MM-DD",
            key="sb_date_range_start",
        )
    with col_e:
        new_end = st.date_input(
            "To",
            value=current_end,
            min_value=abs_min,
            max_value=abs_max,
            format="YYYY-MM-DD",
            key="sb_date_range_end",
        )

    if new_start and new_end:
        state.set_date_range(new_start, new_end)
        state.set_date(new_end)

    # Coverage annotation — counts ALL calendar days, not just available_dates
    if new_start and new_end:
        start, end = (new_start, new_end) if new_start <= new_end else (new_end, new_start)
        total_days     = (end - start).days + 1
        snaps_in_range = sum(1 for d in available_dates if start <= d <= end)
        missing_days   = total_days - snaps_in_range
        if missing_days > 0:
            st.caption(
                f"{total_days} day(s) · {snaps_in_range} with snapshot · "
                f"⚠ {missing_days} missing — use the fetch button below"
            )
        else:
            st.caption(f"{total_days} day(s) · all {snaps_in_range} ready ✓")


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
