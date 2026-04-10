"""High-level Jira fetchers that produce canonical dataclasses."""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from dateutil import parser as _dp

from cockpit_core.jira.client import ReadOnlyJiraClient
from cockpit_core.models import IssueSnapshot, StatusTransition, WorklogEntry

logger = logging.getLogger(__name__)

_DONE_CATEGORIES = {"done", "complete", "completed", "closed", "resolved"}


def _parse_dt(s: str | None) -> Optional[datetime]:
    if not s:
        return None
    try:
        return _dp.parse(s).astimezone(timezone.utc)
    except Exception:
        return None


def _parse_date(s: str | None) -> Optional[date]:
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except Exception:
        return None


def _adf_to_plain(node: dict | str | None) -> str:
    if node is None:
        return ""
    if isinstance(node, str):
        return node
    if node.get("type") == "text":
        return node.get("text", "")
    parts = []
    for child in node.get("content", []):
        parts.append(_adf_to_plain(child))
    return " ".join(p for p in parts if p)


def auto_detect_custom_fields(client: ReadOnlyJiraClient) -> tuple[str, str]:
    """Return (sprint_field_id, story_points_field_id) by inspecting /field."""
    try:
        mapping = client.discover_custom_fields()
    except Exception as exc:
        logger.warning("Could not discover custom fields: %s", exc)
        return "", ""

    sprint_id = ""
    sp_id = ""
    for name, fid in mapping.items():
        if "sprint" in name and not sprint_id:
            sprint_id = fid
        if name in ("story points", "story point estimate", "sp", "story_points") and not sp_id:
            sp_id = fid

    logger.debug("Auto-detected sprint_field=%s sp_field=%s", sprint_id, sp_id)
    return sprint_id, sp_id


def _extract_sprint(fields: dict, sprint_field_id: str) -> tuple[Optional[int], Optional[str], Optional[str]]:
    """Extract (sprint_id, sprint_name, sprint_state) from an issue's fields."""
    candidates: list[str] = []
    if sprint_field_id:
        candidates.append(sprint_field_id)
    for k in fields:
        if k.startswith("customfield_") and k not in candidates:
            val = fields[k]
            if isinstance(val, list) and val and isinstance(val[0], dict) and "sprintId" in val[0]:
                candidates.insert(0, k)
            elif isinstance(val, dict) and "sprintId" in val:
                candidates.insert(0, k)

    for fid in candidates:
        raw = fields.get(fid)
        if not raw:
            continue
        if isinstance(raw, list):
            raw = raw[-1]  # last sprint = current
        if isinstance(raw, dict):
            sid = raw.get("sprintId") or raw.get("id")
            name = raw.get("name") or raw.get("sprintName") or ""
            state = (raw.get("state") or raw.get("sprintState") or "").lower()
            if sid:
                return int(sid), name, state
    return None, None, None


def _extract_sp(fields: dict, sp_field_id: str) -> Optional[float]:
    candidates = [sp_field_id] if sp_field_id else []
    for k in fields:
        if k.startswith("customfield_") and k not in candidates:
            v = fields[k]
            if isinstance(v, (int, float)):
                # heuristic: story points are typically 1-100
                if 0 < v <= 100:
                    candidates.append(k)

    for fid in candidates:
        val = fields.get(fid)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    return None


def normalise_issue(
    raw: dict,
    project_key: str,
    as_of: date,
    sprint_field_id: str = "",
    sp_field_id: str = "",
) -> IssueSnapshot:
    key = raw["key"]
    fields = raw.get("fields", {})

    assignee_raw = fields.get("assignee") or {}
    assignee_name = assignee_raw.get("displayName") or assignee_raw.get("name") or None
    assignee_id = assignee_raw.get("accountId") or None

    created_at = _parse_dt(fields.get("created")) or datetime.now(timezone.utc)
    updated_at = _parse_dt(fields.get("updated")) or datetime.now(timezone.utc)
    resolved_at = _parse_dt(fields.get("resolutiondate"))

    status_raw = fields.get("status") or {}
    status_name = status_raw.get("name", "Unknown")
    # Use statusCategory.key (language-neutral: "new", "indeterminate", "done")
    # instead of .name which is localized (e.g. "Em andamento" in Portuguese Jira)
    status_cat_raw = (status_raw.get("statusCategory") or {}).get("key", "")
    status_cat = status_cat_raw if status_cat_raw else status_name

    priority_raw = (fields.get("priority") or {}).get("name", "Medium")
    issue_type = (fields.get("issuetype") or {}).get("name", "Task")
    resolution = (fields.get("resolution") or {}).get("name")

    sprint_id, sprint_name, sprint_state = _extract_sprint(fields, sprint_field_id)
    sp = _extract_sp(fields, sp_field_id)

    orig_est = fields.get("timeoriginalestimate")
    time_spent = fields.get("timespent")
    remaining = fields.get("timeestimate")

    labels = fields.get("labels") or []
    components = [c.get("name", "") for c in (fields.get("components") or [])]

    due_date = _parse_date(fields.get("duedate"))

    age_days = (as_of - created_at.date()).days
    days_in_status = 0  # filled in by transition analysis when available

    is_blocked = False
    blocker_keys: list[str] = []
    blocking_keys: list[str] = []
    dep_keys: list[str] = []
    for link in fields.get("issuelinks") or []:
        link_type = (link.get("type") or {}).get("name", "")
        if "blocks" in link_type.lower():
            if "inwardIssue" in link:
                blocker_keys.append(link["inwardIssue"]["key"])
                is_blocked = True
            if "outwardIssue" in link:
                blocking_keys.append(link["outwardIssue"]["key"])
        elif "depend" in link_type.lower():
            target = link.get("inwardIssue") or link.get("outwardIssue")
            if target:
                dep_keys.append(target["key"])

    jira_url = f"{raw.get('self', '').split('/rest/')[0]}/browse/{key}"

    return IssueSnapshot(
        key=key,
        project_key=project_key,
        summary=fields.get("summary", ""),
        status=status_name,
        status_category=status_cat,
        issue_type=issue_type,
        priority=priority_raw,
        resolution=resolution,
        assignee=assignee_name,
        assignee_account_id=assignee_id,
        sprint_id=sprint_id,
        sprint_name=sprint_name,
        sprint_state=sprint_state,
        story_points=sp,
        original_estimate_seconds=orig_est,
        time_spent_seconds=time_spent,
        remaining_estimate_seconds=remaining,
        created_at=created_at,
        updated_at=updated_at,
        resolved_at=resolved_at,
        due_date=due_date,
        labels=labels,
        components=components,
        is_blocked=is_blocked,
        blocker_keys=blocker_keys,
        blocking_keys=blocking_keys,
        dependency_keys=dep_keys,
        age_days=max(0, age_days),
        days_in_current_status=days_in_status,
        raw_url=jira_url,
    )


def normalise_worklog(raw: dict, issue_key: str) -> WorklogEntry:
    author = raw.get("author") or {}
    comment_raw = raw.get("comment")
    comment_text = _adf_to_plain(comment_raw) if isinstance(comment_raw, dict) else (comment_raw or "")

    return WorklogEntry(
        issue_key=issue_key,
        worklog_id=str(raw.get("id", "")),
        author=author.get("displayName") or author.get("name") or "",
        author_account_id=author.get("accountId") or "",
        started_at=_parse_dt(raw.get("started")) or datetime.now(timezone.utc),
        time_spent_seconds=raw.get("timeSpentSeconds", 0),
        comment=comment_text or None,
        created_at=_parse_dt(raw.get("created")) or datetime.now(timezone.utc),
        updated_at=_parse_dt(raw.get("updated")) or datetime.now(timezone.utc),
    )


def normalise_changelog_to_transitions(
    changelog_entries: list[dict],
    issue_key: str,
) -> list[StatusTransition]:
    transitions: list[StatusTransition] = []
    for entry in changelog_entries:
        occurred = _parse_dt(entry.get("created"))
        if not occurred:
            continue
        author_raw = entry.get("author") or {}
        author = author_raw.get("displayName") or author_raw.get("name") or ""
        author_id = author_raw.get("accountId") or ""

        for item in entry.get("items", []):
            if item.get("field") != "status":
                continue
            from_s = item.get("fromString") or ""
            to_s = item.get("toString") or ""
            # Use status name strings (English in Jira changelog) not numeric IDs
            from_cat = from_s
            to_cat = to_s

            is_completion = to_s.lower() in _DONE_CATEGORIES or "done" in to_s.lower()
            is_progress = to_s.lower() not in {"to do", "backlog", "open"}

            transitions.append(StatusTransition(
                issue_key=issue_key,
                author=author,
                author_account_id=author_id,
                occurred_at=occurred,
                from_status=from_s,
                to_status=to_s,
                from_category=from_cat,
                to_category=to_cat,
                is_progress=is_progress,
                is_completion=is_completion,
            ))
    return transitions


def fetch_issues_for_project(
    client: ReadOnlyJiraClient,
    project_key: str,
    lookback_days: int = 7,
    sprint_field_id: str = "",
    sp_field_id: str = "",
) -> list[IssueSnapshot]:
    today = date.today()
    lookback_str = (today - timedelta(days=lookback_days)).isoformat()

    jql_open = f'project = "{project_key}" AND statusCategory != Done ORDER BY updated DESC'
    jql_done = (
        f'project = "{project_key}" AND statusCategory = Done '
        f'AND resolutiondate >= "{lookback_str}" ORDER BY resolutiondate DESC'
    )

    seen: dict[str, IssueSnapshot] = {}
    for jql in (jql_open, jql_done):
        try:
            raw_issues = client.search_with_fields(jql, max_results=2000)
        except Exception as exc:
            logger.error("JQL failed (%s): %s", jql[:60], exc)
            continue
        for raw in raw_issues:
            issue = normalise_issue(raw, project_key, today, sprint_field_id, sp_field_id)
            seen[issue.key] = issue

    return list(seen.values())


def fetch_worklogs_for_issues(
    client: ReadOnlyJiraClient,
    issue_keys: list[str],
    since_epoch_ms: Optional[int] = None,
) -> list[WorklogEntry]:
    entries: list[WorklogEntry] = []
    for key in issue_keys:
        try:
            raw_list = client.get_worklogs(key, started_after=since_epoch_ms)
            for raw in raw_list:
                entries.append(normalise_worklog(raw, key))
        except Exception as exc:
            logger.warning("Worklog fetch failed for %s: %s", key, exc)
    return entries


def fetch_transitions_for_issues(
    client: ReadOnlyJiraClient,
    issue_keys: list[str],
    since_epoch_ms: Optional[int] = None,
) -> list[StatusTransition]:
    all_transitions: list[StatusTransition] = []
    for key in issue_keys:
        try:
            entries = client.get_changelog(key, since_epoch_ms=since_epoch_ms)
            all_transitions.extend(normalise_changelog_to_transitions(entries, key))
        except Exception as exc:
            logger.warning("Changelog fetch failed for %s: %s", key, exc)
    return all_transitions
