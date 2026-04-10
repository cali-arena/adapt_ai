# ADAPT Engineering Cockpit

A Streamlit app that turns Jira into a daily decision surface for engineering tech leads:
real productivity metrics (worklogs + transitions, never `updated`), a deterministic
priority score for the backlog, a daily plan exporter, and an optional Claude advisory
layer.

> **No mocks. No demo data.** The dashboard renders live Jira data via the bundled
> `cockpit_core` package, with append-only Parquet snapshots for history.

---

## Run locally

Requires Python 3.11.

```bash
# 1. Install dependencies
python -m pip install --upgrade pip
pip install -r requirements.txt

# 2. Configure secrets
cp .env.example .env
# edit .env and fill in JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN, JIRA_PROJECT_KEY

# 3. Verify the Jira integration is reachable
python verify_jira.py

# 4. Pull historical snapshots (optional, recommended for first run)
python -m cockpit_core.ingest backfill --from 2026-03-09 --to 2026-04-09

# 5. Launch the cockpit
streamlit run streamlit_app/main.py
```

---

## Deploy on Streamlit Community Cloud

1. Push this repo to GitHub.
2. Go to [share.streamlit.io](https://share.streamlit.io), connect the repo.
3. Set the entry point to **`streamlit_app/main.py`**.
4. Under **App settings → Secrets**, paste:

   ```toml
   JIRA_BASE_URL    = "https://your-org.atlassian.net"
   JIRA_EMAIL       = "your-email@example.com"
   JIRA_API_TOKEN   = "your-atlassian-api-token"
   JIRA_PROJECT_KEY = "YOUR_PROJECT_KEY"

   # Optional
   # COCKPIT_AI_ENABLED = "true"
   # ANTHROPIC_API_KEY  = "sk-ant-..."
   ```

5. Deploy. The app reads secrets via the priority chain
   `os.environ → st.secrets → .env`, so the same code path works in every environment.

`runtime.txt` pins Python 3.11. `.streamlit/config.toml` ships without a localhost
address lock (Streamlit Cloud requires the default `0.0.0.0` binding).

---

## Repository layout

```
.
├── cockpit_core/             # Pure-Python package: Jira client, ingest, scoring, AI
│   ├── ai/                   # Anthropic advisor (advisory only, never decides)
│   ├── duration/             # Issue duration / cycle-time engine
│   ├── ingest/               # Daily ingest + historical backfill
│   ├── jira/                 # ReadOnlyJiraClient + fetchers
│   ├── plan/                 # Daily plan assembler
│   ├── productivity/         # Effort vs activity metrics (worklog-first)
│   ├── scoring/              # Deterministic priority score (W_PRIORITY, W_DUE, …)
│   ├── snapshot/             # Snapshot orchestration
│   ├── storage/              # SQLite repo + Parquet snapshots
│   ├── config.py             # CockpitConfig dataclass
│   ├── env_bootstrap.py      # os.environ → st.secrets → .env loader
│   └── models.py             # Canonical dataclasses (IssueSnapshot, …)
├── streamlit_app/            # UI layer
│   ├── main.py               # Entry point
│   ├── state.py              # Typed st.session_state accessors
│   ├── theme/
│   │   ├── adapt_theme.py    # ADAPT purple theme + CSS injection
│   │   └── assets/           # logo.png, logo_Adapt.svg
│   ├── views/                # header, sidebar, productivity, backlog, plan, ai_panel
│   ├── components/
│   └── exporters/            # Markdown export
├── .streamlit/
│   ├── config.toml           # Streamlit Cloud-compatible
│   └── secrets.toml.example  # Local secrets template
├── .github/workflows/ci.yml  # Lint + import + deployment-readiness checks
├── requirements.txt
├── runtime.txt               # python-3.11
├── verify_jira.py            # Standalone integration verifier
└── .env.example
```

---

## Architecture in one breath

- **Productivity = effort signal** (worklogs + status transitions to `done`),
  never `updated`. The schema separates `effort_signal` from `activity_signal`
  so they cannot be confused.
- **Priority score** is additive and bounded `[0, 100]`. Every factor
  (`W_PRIORITY`, `W_DUE`, `W_BLOCKED`, `W_BLOCKING`, `W_AGE`, `W_SPRINT`,
  `W_STALL`) is visible per row. No ML, no opacity.
- **AI is an explainer**, not a decider. `COCKPIT_AI_ENABLED=false` disables
  the advisor without removing any functionality.
- **Storage** is local: SQLite for hot mutable state (overrides, audit),
  Parquet for daily history. Both file-based, both gitignored.
- **Read-only Jira**: `ReadOnlyJiraClient` raises on any write method, so the
  cockpit cannot accidentally mutate Jira state.

---

## Required environment variables

| Variable | Required | Description |
|---|---|---|
| `JIRA_BASE_URL` | ✅ | e.g. `https://your-org.atlassian.net` |
| `JIRA_EMAIL` | ✅ | Atlassian account email |
| `JIRA_API_TOKEN` | ✅ | Atlassian API token (id.atlassian.com → Security → API tokens) |
| `JIRA_PROJECT_KEY` | ✅ | The Jira project key, e.g. `NAI` |
| `ANTHROPIC_API_KEY` | optional | Required only when `COCKPIT_AI_ENABLED=true` |
| `COCKPIT_AI_ENABLED` | optional | `"true"` to turn on the advisor (default off) |
| `SPRINT_FIELD_ID` | optional | Override custom field ID auto-detection |
| `STORY_POINTS_FIELD_ID` | optional | Override custom field ID auto-detection |
| `COCKPIT_DATA_DIR` | optional | Override the local data directory (default `./data`) |

---

## CLI commands

```bash
# Daily ingest for a single date
python -m cockpit_core.ingest --date 2026-04-09 --project-key NAI

# Historical backfill
python -m cockpit_core.ingest backfill --from 2026-03-09 --to 2026-04-09

# End-to-end Jira integration check (credentials → fetch → ingest → snapshot read)
python verify_jira.py
```
