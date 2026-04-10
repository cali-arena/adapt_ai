"""Ingestion runner — pulls from Jira and writes snapshots.

Architecture: near-real-time pull (on-demand, not streaming).
Every run is idempotent; force=True overwrites an existing snapshot.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Callable

from cockpit_core.config import CockpitConfig
from cockpit_core.jira.client import ReadOnlyJiraClient
from cockpit_core.jira.fetchers import (
    auto_detect_custom_fields,
    fetch_issues_for_project,
    fetch_transitions_for_issues,
    fetch_worklogs_for_issues,
)
from cockpit_core.productivity.metrics import (
    compute_daily_productivity,
    write_productivity_parquet,
)
from cockpit_core.storage.repo import CockpitRepository
from cockpit_core.storage.snapshots import (
    append_transitions_history,
    read_issues, read_transitions, read_worklogs,
    snapshot_exists, write_snapshot,
)

logger = logging.getLogger(__name__)

# ── Progress callback type (used by Streamlit UI to show spinner steps) ───────
ProgressCallback = Callable[[str], None]


def _noop(msg: str) -> None:
    pass


@dataclass
class IngestResult:
    target_date: date
    status: str                         # "ok" | "skipped" | "failed"
    issues_seen: int = 0
    worklogs_seen: int = 0
    transitions_seen: int = 0
    prod_rows: int = 0
    error_message: str = ""
    skipped: bool = False
    authenticated_as: str = ""
    duration_seconds: float = 0.0
    log_lines: list[str] = field(default_factory=list)


class IngestRunner:
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

    def verify_credentials(self) -> tuple[bool, str]:
        """Returns (ok, display_name_or_error_message)."""
        try:
            user = self._get_client().get_current_user()
            name = user.get("displayName", user.get("emailAddress", "?"))
            logger.info("[INGEST] Authenticated as: %s", name)
            return True, name
        except Exception as exc:
            logger.error("[INGEST] Credential verification failed: %s", exc)
            return False, str(exc)

    def run(
        self,
        target_date: date,
        force: bool = False,
        lookback_days: int = 7,
        progress: ProgressCallback = _noop,
    ) -> IngestResult:
        """Pull Jira data and write a complete snapshot for target_date.

        Args:
            target_date:   The calendar date to snapshot.
            force:         Overwrite existing snapshot if present.
            lookback_days: How far back to pull worklogs/transitions.
            progress:      Optional callback for UI progress messages.
        """
        t_start = datetime.now(timezone.utc)
        logs: list[str] = []

        def _log(msg: str) -> None:
            logger.info(msg)
            logs.append(msg)
            progress(msg)

        config = self.config
        config.snapshots_dir.mkdir(parents=True, exist_ok=True)
        config.data_dir.mkdir(parents=True, exist_ok=True)

        # ── Idempotency guard ─────────────────────────────────────────────────
        if snapshot_exists(config.snapshots_dir, target_date) and not force:
            msg = f"[INGEST] Snapshot already exists for {target_date} — skipping (pass force=True to override)"
            _log(msg)
            return IngestResult(target_date=target_date, status="skipped", skipped=True, log_lines=logs)

        _log(f"[INGEST] === REFRESH STARTED: {target_date} project={config.project_key} force={force} ===")

        repo = CockpitRepository(config.db_path)
        run_id = repo.start_ingest_run(target_date.isoformat(), config.project_key)

        try:
            client = self._get_client()

            # ── Step 0: Authenticate ──────────────────────────────────────────
            _log("[INGEST] Verifying Jira credentials…")
            ok, who = self.verify_credentials()
            if not ok:
                raise RuntimeError(f"Jira authentication failed: {who}")
            _log(f"[INGEST] Authenticated as: {who}")

            # ── Step 1: Detect custom fields ──────────────────────────────────
            sprint_fid = config.sprint_field_id
            sp_fid = config.story_points_field_id
            if not sprint_fid or not sp_fid:
                _log("[INGEST] Auto-detecting custom field IDs (sprint, story points)…")
                sprint_fid, sp_fid = auto_detect_custom_fields(client)
                _log(f"[INGEST] sprint_field={sprint_fid or 'not found'}  sp_field={sp_fid or 'not found'}")

            # ── Step 2: Fetch issues ──────────────────────────────────────────
            _log(f"[INGEST] Fetching issues for project={config.project_key} lookback={lookback_days}d…")
            issues = fetch_issues_for_project(
                client, config.project_key,
                lookback_days=lookback_days,
                sprint_field_id=sprint_fid,
                sp_field_id=sp_fid,
            )
            _log(f"[INGEST] JIRA FETCH: {len(issues)} issues retrieved")

            # ── Step 3: Fetch worklogs ────────────────────────────────────────
            since_ms = int(
                (datetime.combine(
                    target_date - timedelta(days=lookback_days),
                    datetime.min.time(),
                ).replace(tzinfo=timezone.utc)).timestamp() * 1000
            )
            issue_keys = [i.key for i in issues]
            _log(f"[INGEST] Fetching worklogs for {len(issue_keys)} issues (since {target_date - timedelta(days=lookback_days)})…")
            worklogs = fetch_worklogs_for_issues(client, issue_keys, since_epoch_ms=since_ms)
            _log(f"[INGEST] JIRA FETCH: {len(worklogs)} worklog entries retrieved")

            # ── Step 4: Fetch transitions ─────────────────────────────────────
            _log(f"[INGEST] Fetching status transitions for {len(issue_keys)} issues…")
            transitions = fetch_transitions_for_issues(client, issue_keys, since_epoch_ms=since_ms)
            _log(f"[INGEST] JIRA FETCH: {len(transitions)} transitions retrieved")

            _log(f"[INGEST] === JIRA FETCH COMPLETE: {len(issues)} issues / {len(worklogs)} worklogs / {len(transitions)} transitions ===")

            # ── Step 5: Write snapshot ────────────────────────────────────────
            _log(f"[INGEST] Writing snapshot to {config.snapshots_dir / target_date.isoformat()}…")
            write_snapshot(config.snapshots_dir, target_date, issues, worklogs, transitions)
            _log(f"[INGEST] === SNAPSHOT UPDATED: {target_date} ({len(issues)} issues, {len(worklogs)} worklogs, {len(transitions)} transitions) ===")

            # ── Step 5b: Append to cumulative transition history ───────────────
            _log("[INGEST] Appending transitions to cumulative history…")
            tr_df = read_transitions(config.snapshots_dir, target_date)
            added = append_transitions_history(config.data_dir, tr_df)
            _log(f"[INGEST] transitions_history updated (+{added} new rows)")

            # ── Step 6: Recompute productivity ────────────────────────────────
            _log("[INGEST] Recomputing productivity metrics…")
            issues_df = read_issues(config.snapshots_dir, target_date)
            wl_df     = read_worklogs(config.snapshots_dir, target_date)
            tr_df     = read_transitions(config.snapshots_dir, target_date)
            prod_df   = compute_daily_productivity(wl_df, tr_df, issues_df, target_date)
            write_productivity_parquet(config.snapshots_dir, target_date, prod_df)
            _log(f"[INGEST] === METRICS RECOMPUTED: {len(prod_df)} user rows for {target_date} ===")

            # ── Finish run log ────────────────────────────────────────────────
            duration = (datetime.now(timezone.utc) - t_start).total_seconds()
            repo.finish_ingest_run(
                run_id, "ok",
                issues_seen=len(issues),
                worklogs_seen=len(worklogs),
                transitions_seen=len(transitions),
            )
            _log(f"[INGEST] === INGEST COMPLETE in {duration:.1f}s — run_id={run_id} ===")

            return IngestResult(
                target_date=target_date,
                status="ok",
                issues_seen=len(issues),
                worklogs_seen=len(worklogs),
                transitions_seen=len(transitions),
                prod_rows=len(prod_df),
                authenticated_as=who,
                duration_seconds=duration,
                log_lines=logs,
            )

        except Exception as exc:
            duration = (datetime.now(timezone.utc) - t_start).total_seconds()
            _log(f"[INGEST] === INGEST FAILED after {duration:.1f}s: {exc} ===")
            logger.error("[INGEST] Ingestion failed: %s", exc, exc_info=True)
            repo.finish_ingest_run(run_id, "failed", error_message=str(exc))
            return IngestResult(
                target_date=target_date,
                status="failed",
                error_message=str(exc),
                duration_seconds=duration,
                log_lines=logs,
            )
