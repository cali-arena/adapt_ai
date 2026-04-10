"""Historical backfill — reconstructs per-day snapshots from full Jira history.

Fetches ALL issues (no date filter, including historically-done ones),
their complete changelogs, and all worklogs, then builds per-day snapshots
via point-in-time reconstruction.

This produces real, Jira-backed history for any date range, not just the
7-day rolling window that the regular ingest runner uses.

Usage (CLI):
    python -m cockpit_core.ingest.backfill --from 2026-01-01 --to 2026-04-09

Usage (Python):
    from cockpit_core.ingest.backfill import BackfillRunner
    from cockpit_core.config import load_config
    runner = BackfillRunner(load_config())
    runner.run(from_date, to_date, progress=print)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from cockpit_core.config import CockpitConfig
from cockpit_core.jira.client import ReadOnlyJiraClient
from cockpit_core.jira.fetchers import (
    auto_detect_custom_fields,
    normalise_changelog_to_transitions,
    normalise_issue,
    normalise_worklog,
)
from cockpit_core.models import IssueSnapshot, StatusTransition, WorklogEntry
from cockpit_core.productivity.metrics import (
    compute_daily_productivity,
    write_productivity_parquet,
)
from cockpit_core.storage.snapshots import (
    append_transitions_history,
    read_transitions,
    snapshot_exists,
    write_snapshot,
)

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[str], None]


def _noop(msg: str) -> None:
    pass


@dataclass
class BackfillResult:
    from_date: date
    to_date: date
    days_written: int = 0
    days_skipped: int = 0
    issues_fetched: int = 0
    worklogs_fetched: int = 0
    transitions_fetched: int = 0
    error_message: str = ""
    status: str = "ok"           # "ok" | "failed"
    log_lines: list[str] = field(default_factory=list)


def _reconstruct_status(
    transitions: list[StatusTransition],
    target_date: date,
    current_status: str,
    current_category: str,
) -> tuple[str, str]:
    """Return (status, status_category) for an issue as of target_date.

    Uses the full changelog: the last transition that occurred on or before
    target_date determines the status. If no transitions exist before the date
    (i.e. the issue was created after the date or had no transitions yet),
    returns the current status as a fallback.
    """
    before = [t for t in transitions if t.occurred_at.date() <= target_date]
    if not before:
        return current_status, current_category
    last = max(before, key=lambda t: t.occurred_at)
    return last.to_status, last.to_category


def _is_done_on_date(transitions: list[StatusTransition], target_date: date) -> bool:
    """Return True if the issue reached a Done status on or before target_date."""
    done_keywords = {"done", "resolved", "closed", "completed", "finalizado", "concluído"}
    before = [t for t in transitions if t.occurred_at.date() <= target_date]
    if not before:
        return False
    last = max(before, key=lambda t: t.occurred_at)
    return last.to_status.lower() in done_keywords or "done" in last.to_status.lower()


class BackfillRunner:
    def __init__(self, config: CockpitConfig) -> None:
        self.config = config
        self._client: ReadOnlyJiraClient | None = None

    def _get_client(self) -> ReadOnlyJiraClient:
        if self._client is None:
            self._client = ReadOnlyJiraClient(
                self.config.jira_base_url,
                self.config.jira_email,
                self.config.jira_api_token,
            )
        return self._client

    def _fetch_all_issues(
        self,
        project_key: str,
        sprint_field_id: str,
        sp_field_id: str,
        log: Callable[[str], None],
    ) -> list[dict]:
        """Fetch ALL issues in the project — open and done, no date filter."""
        client = self._get_client()
        today = date.today()

        # One JQL that gets everything, sorted oldest first
        jql = f'project = "{project_key}" ORDER BY created ASC'
        log(f"[BACKFILL] JQL: {jql}")
        try:
            raw = client.search_with_fields(jql, max_results=10000)
            log(f"[BACKFILL] Fetched {len(raw)} total issues from Jira")
            return raw
        except Exception as exc:
            log(f"[BACKFILL] ERROR fetching issues: {exc}")
            raise

    def _fetch_all_worklogs(
        self,
        issue_key: str,
    ) -> list[dict]:
        """Fetch ALL worklogs for an issue — no date filter."""
        client = self._get_client()
        return client.get_worklogs(issue_key, started_after=None)

    def _fetch_full_changelog(self, issue_key: str) -> list[dict]:
        """Fetch the complete changelog for an issue."""
        client = self._get_client()
        return client.get_changelog(issue_key, since_epoch_ms=None)

    def run(
        self,
        from_date: date,
        to_date: date,
        force: bool = False,
        progress: ProgressCallback = _noop,
    ) -> BackfillResult:
        """Reconstruct per-day snapshots for [from_date, to_date] from full Jira history.

        Args:
            from_date: First date to reconstruct (inclusive).
            to_date:   Last date to reconstruct (inclusive).
            force:     Overwrite existing snapshots.
            progress:  Optional callback for UI progress messages.
        """
        result = BackfillResult(from_date=from_date, to_date=to_date)
        logs: list[str] = []

        def _log(msg: str) -> None:
            logger.info(msg)
            logs.append(msg)
            progress(msg)
            result.log_lines.append(msg)

        config = self.config
        config.snapshots_dir.mkdir(parents=True, exist_ok=True)
        config.data_dir.mkdir(parents=True, exist_ok=True)

        _log(f"[BACKFILL] === START {from_date} → {to_date} force={force} ===")

        try:
            client = self._get_client()

            # ── Detect custom fields ─────────────────────────────────────────
            sprint_fid = config.sprint_field_id or ""
            sp_fid = config.story_points_field_id or ""
            if not sprint_fid or not sp_fid:
                _log("[BACKFILL] Auto-detecting custom fields…")
                sprint_fid, sp_fid = auto_detect_custom_fields(client)
                _log(f"[BACKFILL] sprint_field={sprint_fid or 'not found'} sp_field={sp_fid or 'not found'}")

            # ── Fetch ALL issues (no date filter) ────────────────────────────
            _log(f"[BACKFILL] Fetching ALL issues for project={config.project_key}…")
            raw_issues = self._fetch_all_issues(config.project_key, sprint_fid, sp_fid, _log)
            result.issues_fetched = len(raw_issues)

            # Normalise to IssueSnapshot (as of today — we reconstruct status per day below)
            today = date.today()
            issue_map: dict[str, IssueSnapshot] = {}
            for raw in raw_issues:
                snap = normalise_issue(raw, config.project_key, today, sprint_fid, sp_fid)
                issue_map[snap.key] = snap

            _log(f"[BACKFILL] Normalised {len(issue_map)} issues")

            # ── Fetch full changelogs for all issues ──────────────────────────
            _log(f"[BACKFILL] Fetching full changelogs for {len(issue_map)} issues…")
            all_transitions: dict[str, list[StatusTransition]] = {}
            for i, key in enumerate(issue_map):
                if i % 20 == 0 and i > 0:
                    _log(f"[BACKFILL] Changelogs: {i}/{len(issue_map)}…")
                try:
                    entries = self._fetch_full_changelog(key)
                    all_transitions[key] = normalise_changelog_to_transitions(entries, key)
                except Exception as exc:
                    logger.warning("Changelog failed for %s: %s", key, exc)
                    all_transitions[key] = []

            total_tr = sum(len(v) for v in all_transitions.values())
            result.transitions_fetched = total_tr
            _log(f"[BACKFILL] Got {total_tr} total transitions across all issues")

            # ── Fetch all worklogs for all issues ────────────────────────────
            _log(f"[BACKFILL] Fetching all worklogs for {len(issue_map)} issues…")
            all_worklogs: dict[str, list[WorklogEntry]] = {}
            for i, key in enumerate(issue_map):
                if i % 20 == 0 and i > 0:
                    _log(f"[BACKFILL] Worklogs: {i}/{len(issue_map)}…")
                try:
                    raw_wl = self._fetch_all_worklogs(key)
                    all_worklogs[key] = [normalise_worklog(w, key) for w in raw_wl]
                except Exception as exc:
                    logger.warning("Worklog fetch failed for %s: %s", key, exc)
                    all_worklogs[key] = []

            total_wl = sum(len(v) for v in all_worklogs.values())
            result.worklogs_fetched = total_wl
            _log(f"[BACKFILL] Got {total_wl} total worklog entries across all issues")

            # ── Reconstruct per-day snapshots ─────────────────────────────────
            days_written = 0
            days_skipped = 0
            current = from_date
            while current <= to_date:
                if snapshot_exists(config.snapshots_dir, current) and not force:
                    _log(f"[BACKFILL] {current}: snapshot exists, skipping (pass force=True to overwrite)")
                    days_skipped += 1
                    current += timedelta(days=1)
                    continue

                _log(f"[BACKFILL] Reconstructing snapshot for {current}…")

                # Issues that existed on this date
                day_issues: list[IssueSnapshot] = []
                for key, snap in issue_map.items():
                    if snap.created_at.date() > current:
                        continue  # not created yet on this date

                    transitions_for_issue = all_transitions.get(key, [])
                    status_on_day, cat_on_day = _reconstruct_status(
                        transitions_for_issue,
                        current,
                        snap.status,
                        snap.status_category,
                    )

                    # Reconstruct a point-in-time snapshot
                    import dataclasses
                    day_snap = dataclasses.replace(
                        snap,
                        status=status_on_day,
                        status_category=cat_on_day,
                        age_days=max(0, (current - snap.created_at.date()).days),
                    )
                    day_issues.append(day_snap)

                # Worklogs for this day
                day_worklogs: list[WorklogEntry] = []
                for wl_list in all_worklogs.values():
                    for wl in wl_list:
                        if wl.started_at.date() == current:
                            day_worklogs.append(wl)

                # Transitions for this day
                day_transitions: list[StatusTransition] = []
                for tr_list in all_transitions.values():
                    for tr in tr_list:
                        if tr.occurred_at.date() == current:
                            day_transitions.append(tr)

                # Write the snapshot
                write_snapshot(
                    config.snapshots_dir,
                    current,
                    day_issues,
                    day_worklogs,
                    day_transitions,
                )

                # Append transitions to cumulative history
                tr_df = read_transitions(config.snapshots_dir, current)
                append_transitions_history(config.data_dir, tr_df)

                # Compute productivity
                import pandas as pd
                issues_df_day = pd.DataFrame([{
                    "key": s.key,
                    "summary": s.summary,
                    "status": s.status,
                    "status_category": s.status_category,
                    "issue_type": s.issue_type,
                    "priority": s.priority,
                    "assignee": s.assignee,
                    "story_points": s.story_points,
                    "sprint_name": s.sprint_name,
                    "updated_at": s.updated_at,
                } for s in day_issues])

                wl_df_day = pd.DataFrame([{
                    "issue_key": w.issue_key,
                    "author": w.author,
                    "author_account_id": w.author_account_id,
                    "started_at": w.started_at,
                    "time_spent_seconds": w.time_spent_seconds,
                } for w in day_worklogs])

                tr_df_day = pd.DataFrame([{
                    "issue_key": t.issue_key,
                    "author": t.author,
                    "occurred_at": t.occurred_at,
                    "from_status": t.from_status,
                    "to_status": t.to_status,
                    "is_completion": t.is_completion,
                    "is_progress": t.is_progress,
                } for t in day_transitions])

                prod_df = compute_daily_productivity(wl_df_day, tr_df_day, issues_df_day, current)
                write_productivity_parquet(config.snapshots_dir, current, prod_df)

                _log(
                    f"[BACKFILL] {current}: {len(day_issues)} issues, "
                    f"{len(day_worklogs)} worklogs, {len(day_transitions)} transitions, "
                    f"{len(prod_df)} prod rows"
                )
                days_written += 1
                current += timedelta(days=1)

            result.days_written = days_written
            result.days_skipped = days_skipped
            _log(
                f"[BACKFILL] === COMPLETE: {days_written} days written, "
                f"{days_skipped} skipped ==="
            )
            return result

        except Exception as exc:
            _log(f"[BACKFILL] === FAILED: {exc} ===")
            logger.error("[BACKFILL] Backfill failed: %s", exc, exc_info=True)
            result.status = "failed"
            result.error_message = str(exc)
            return result


def main() -> None:
    """CLI entry point: python -m cockpit_core.ingest.backfill --from YYYY-MM-DD --to YYYY-MM-DD"""
    import argparse, sys
    from pathlib import Path

    parser = argparse.ArgumentParser(description="Backfill historical Jira snapshots")
    parser.add_argument("--from", dest="from_date", required=True,
                        help="Start date (YYYY-MM-DD, inclusive)")
    parser.add_argument("--to", dest="to_date", required=True,
                        help="End date (YYYY-MM-DD, inclusive)")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing snapshots")
    args = parser.parse_args()

    try:
        from_date = date.fromisoformat(args.from_date)
        to_date   = date.fromisoformat(args.to_date)
    except ValueError as e:
        print(f"ERROR: Invalid date format — {e}")
        sys.exit(1)

    # Bootstrap env vars
    from cockpit_core.env_bootstrap import bootstrap
    bootstrap()
    from cockpit_core.config import load_config
    config = load_config()

    runner = BackfillRunner(config)
    result = runner.run(from_date, to_date, force=args.force, progress=print)

    if result.status == "ok":
        print(
            f"\nBackfill complete: {result.days_written} days written, "
            f"{result.days_skipped} skipped."
        )
        print(f"Issues: {result.issues_fetched}, Worklogs: {result.worklogs_fetched}, "
              f"Transitions: {result.transitions_fetched}")
        sys.exit(0)
    else:
        print(f"\nBackfill FAILED: {result.error_message}")
        sys.exit(1)


if __name__ == "__main__":
    main()
