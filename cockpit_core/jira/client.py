"""Jira Cloud REST API v3 client (read-only cockpit variant).

Copied from scripts/jira_sprint18_output_engine_import.py and extended with
worklog, changelog and full-field search methods. The ReadOnlyJiraClient
subclass blocks all write operations for safety.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

_RETRY_POLICY = Retry(
    total=4,
    backoff_factor=1.0,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods={"GET", "POST"},
    raise_on_status=False,
)


class JiraClient:
    """Thin Jira Cloud API client with retry and Basic auth."""

    def __init__(self, base_url: str, email: str, api_token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._session = requests.Session()
        self._session.auth = (email, api_token)
        self._session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
        adapter = HTTPAdapter(max_retries=_RETRY_POLICY)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def get(self, path: str, params: dict | None = None) -> requests.Response:
        url = f"{self.base_url}{path}"
        resp = self._session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp

    def post(self, path: str, json_body: dict) -> requests.Response:
        url = f"{self.base_url}{path}"
        resp = self._session.post(url, json=json_body, timeout=30)
        resp.raise_for_status()
        return resp

    def put(self, path: str, json_body: dict) -> requests.Response:
        url = f"{self.base_url}{path}"
        resp = self._session.put(url, json=json_body, timeout=30)
        resp.raise_for_status()
        return resp

    # ── Extended methods ────────────────────────────────────────────────────

    def get_worklogs(self, issue_key: str, started_after: Optional[int] = None) -> list[dict]:
        """Fetch all worklog entries for an issue.

        Args:
            issue_key: e.g. "NAI-42"
            started_after: epoch-milliseconds filter (inclusive)
        Returns:
            list of raw worklog dicts from the API
        """
        path = f"/rest/api/3/issue/{issue_key}/worklog"
        params: dict[str, Any] = {"maxResults": 5000}
        if started_after is not None:
            params["startedAfter"] = started_after

        worklogs: list[dict] = []
        start_at = 0
        while True:
            params["startAt"] = start_at
            data = self.get(path, params=params).json()
            page = data.get("worklogs", [])
            worklogs.extend(page)
            total = data.get("total", 0)
            start_at += len(page)
            if start_at >= total or not page:
                break
        return worklogs

    def get_changelog(self, issue_key: str, since_epoch_ms: Optional[int] = None) -> list[dict]:
        """Fetch the FULL changelog for an issue (all pages, all history).

        The ``since_epoch_ms`` parameter is kept for API compatibility but is
        intentionally NOT applied as a filter.  The Jira changelog REST API has
        no server-side date filter, so the previous client-side filter was
        silently discarding valid historical transitions (e.g. the first
        "In Progress" transition for a task started weeks ago).  Fetching the
        full changelog costs the same number of API calls as the windowed
        version because pagination always walks every page regardless.
        """
        path = f"/rest/api/3/issue/{issue_key}/changelog"
        entries: list[dict] = []
        start = 0
        while True:
            data = self.get(path, params={"startAt": start, "maxResults": 100}).json()
            page = data.get("values", [])
            entries.extend(page)
            total = data.get("total", 0)
            start += len(page)
            if start >= total or not page:
                break
        return entries

    def search_with_fields(
        self,
        jql: str,
        fields: list[str] | None = None,
        expand: list[str] | None = None,
        max_results: int = 5000,
    ) -> list[dict]:
        """Search issues via JQL using the v3 /search/jql endpoint.

        The v3 endpoint uses cursor-based pagination (nextPageToken) and
        does NOT accept startAt.  Sending startAt → 400 Bad Request.
        """
        if fields is None:
            fields = ["*all"]

        page_size = min(max_results, 100)
        body: dict[str, Any] = {
            "jql": jql,
            "fields": fields,
            "maxResults": page_size,
        }
        if expand:
            body["expand"] = expand

        issues: list[dict] = []
        while True:
            data = self.post("/rest/api/3/search/jql", body).json()
            page = data.get("issues", [])
            issues.extend(page)
            # Stop when: no more pages, no results, or hit the caller's limit
            if data.get("isLast", True) or not page or len(issues) >= max_results:
                break
            next_token = data.get("nextPageToken")
            if not next_token:
                break
            body["nextPageToken"] = next_token
        return issues[:max_results]

    def get_current_user(self) -> dict:
        return self.get("/rest/api/3/myself").json()

    def discover_custom_fields(self) -> dict[str, str]:
        """Return a mapping of field name (lowercase) → field id."""
        fields = self.get("/rest/api/3/field").json()
        return {f["name"].lower(): f["id"] for f in fields if "id" in f}


class ReadOnlyJiraClient(JiraClient):
    """Cockpit-safe Jira client that refuses all write operations."""

    def post(self, path: str, json_body: dict) -> requests.Response:
        if "/search" in path:
            return super().post(path, json_body)
        raise PermissionError(
            f"ReadOnlyJiraClient: write attempt via POST to '{path}' blocked. "
            "The cockpit must never modify Jira data."
        )

    def put(self, path: str, json_body: dict) -> requests.Response:
        raise PermissionError(
            f"ReadOnlyJiraClient: write attempt via PUT to '{path}' blocked."
        )
