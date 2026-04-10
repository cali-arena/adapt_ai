"""Canonical data models — the only shapes the UI ever sees.

Jira-specific API responses stop at the fetcher layer; everything inside
cockpit_core (scoring, productivity, AI, storage) works with these types.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional


@dataclass
class IssueSnapshot:
    key: str
    project_key: str
    summary: str
    status: str
    status_category: str          # 'To Do' | 'In Progress' | 'Done'
    issue_type: str
    priority: str                 # Highest | High | Medium | Low | Lowest
    resolution: Optional[str]
    assignee: Optional[str]
    assignee_account_id: Optional[str]
    sprint_id: Optional[int]
    sprint_name: Optional[str]
    sprint_state: Optional[str]   # 'active' | 'closed' | 'future'
    story_points: Optional[float]
    original_estimate_seconds: Optional[int]
    time_spent_seconds: Optional[int]
    remaining_estimate_seconds: Optional[int]
    created_at: datetime
    updated_at: datetime
    resolved_at: Optional[datetime]
    due_date: Optional[date]
    labels: list[str] = field(default_factory=list)
    components: list[str] = field(default_factory=list)
    is_blocked: bool = False
    blocker_keys: list[str] = field(default_factory=list)
    blocking_keys: list[str] = field(default_factory=list)
    dependency_keys: list[str] = field(default_factory=list)
    age_days: int = 0
    days_in_current_status: int = 0
    raw_url: str = ""


@dataclass
class WorklogEntry:
    issue_key: str
    worklog_id: str
    author: str
    author_account_id: str
    started_at: datetime
    time_spent_seconds: int
    comment: Optional[str]
    created_at: datetime
    updated_at: datetime


@dataclass
class StatusTransition:
    issue_key: str
    author: str
    author_account_id: str
    occurred_at: datetime
    from_status: str
    to_status: str
    from_category: str
    to_category: str
    is_progress: bool    # any forward transition
    is_completion: bool  # transition to Done
