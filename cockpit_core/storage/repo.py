"""SQLite repository — transactional state (overrides, audit, run log)."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


_SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS ingest_run (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    target_date     TEXT NOT NULL,
    project_key     TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'running',
    issues_seen     INTEGER,
    worklogs_seen   INTEGER,
    transitions_seen INTEGER,
    error_message   TEXT,
    meta_json       TEXT
);

CREATE TABLE IF NOT EXISTS priority_override (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_key       TEXT NOT NULL,
    target_date     TEXT NOT NULL,
    rank_override   INTEGER,
    pinned          INTEGER DEFAULT 0,
    reason          TEXT,
    created_by      TEXT NOT NULL DEFAULT 'tl',
    created_at      TEXT NOT NULL,
    UNIQUE(issue_key, target_date)
);

CREATE TABLE IF NOT EXISTS tl_note (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    target_date     TEXT NOT NULL,
    scope           TEXT NOT NULL,
    body            TEXT NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at     TEXT NOT NULL,
    actor           TEXT NOT NULL,
    action          TEXT NOT NULL,
    target          TEXT,
    payload_json    TEXT
);

CREATE TABLE IF NOT EXISTS ai_explanation (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    target_date     TEXT NOT NULL,
    kind            TEXT NOT NULL,
    target          TEXT NOT NULL,
    model           TEXT NOT NULL,
    prompt_hash     TEXT NOT NULL,
    body            TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    UNIQUE(target_date, kind, target, prompt_hash)
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class CockpitRepository:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    # ── Ingestion run log ──────────────────────────────────────────────────

    def start_ingest_run(self, target_date: str, project_key: str) -> int:
        cur = self._conn.execute(
            "INSERT INTO ingest_run (started_at, target_date, project_key, status) VALUES (?, ?, ?, 'running')",
            (_now(), target_date, project_key),
        )
        self._conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    def finish_ingest_run(
        self,
        run_id: int,
        status: str,
        issues_seen: int = 0,
        worklogs_seen: int = 0,
        transitions_seen: int = 0,
        error_message: str = "",
        meta: dict | None = None,
    ) -> None:
        self._conn.execute(
            """UPDATE ingest_run
               SET finished_at=?, status=?, issues_seen=?, worklogs_seen=?,
                   transitions_seen=?, error_message=?, meta_json=?
               WHERE id=?""",
            (
                _now(), status, issues_seen, worklogs_seen,
                transitions_seen, error_message or None,
                json.dumps(meta) if meta else None,
                run_id,
            ),
        )
        self._conn.commit()

    def list_recent_runs(self, limit: int = 20) -> list[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM ingest_run ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()

    def get_runs_for_date(self, target_date: str) -> list[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM ingest_run WHERE target_date=? ORDER BY id DESC",
            (target_date,),
        ).fetchall()

    # ── Priority overrides ─────────────────────────────────────────────────

    def upsert_override(
        self,
        issue_key: str,
        target_date: str,
        rank_override: Optional[int] = None,
        pinned: bool = False,
        reason: str = "",
        created_by: str = "tl",
    ) -> None:
        self._conn.execute(
            """INSERT INTO priority_override
               (issue_key, target_date, rank_override, pinned, reason, created_by, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(issue_key, target_date) DO UPDATE SET
                   rank_override=excluded.rank_override,
                   pinned=excluded.pinned,
                   reason=excluded.reason,
                   created_by=excluded.created_by,
                   created_at=excluded.created_at""",
            (issue_key, target_date, rank_override, int(pinned), reason, created_by, _now()),
        )
        self._conn.commit()

    def get_overrides_for_date(self, target_date: str) -> list[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM priority_override WHERE target_date=?", (target_date,)
        ).fetchall()

    # ── Audit log ──────────────────────────────────────────────────────────

    def audit(self, actor: str, action: str, target: str = "", payload: Any = None) -> None:
        self._conn.execute(
            "INSERT INTO audit_log (occurred_at, actor, action, target, payload_json) VALUES (?, ?, ?, ?, ?)",
            (_now(), actor, action, target or None, json.dumps(payload) if payload else None),
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
