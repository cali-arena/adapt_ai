"""AI Advisory Panel — explanatory layer on top of deterministic scores.

Strict UI rules:
  - Every AI output shows "🤖 AI advisory · review before acting" badge.
  - Kill-switch check before any API call — panel collapses gracefully if disabled.
  - AI outputs are rendered in a distinct visual container (not mixed with score data).
  - Cached results shown with a "(cached)" indicator.
  - No AI output can trigger an override or change any score.
"""
from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from cockpit_core.ai.advisor import AdvisorDisabledError, CockpitAdvisor
from cockpit_core.storage.repo import CockpitRepository
from streamlit_app.theme.adapt_theme import COLORS, section_header


# ── Visual constants ──────────────────────────────────────────────────────────

_AI_BADGE = (
    f'<span style="background:#9D4EDD22; color:#B97FFF; font-size:10px; '
    f'padding:2px 8px; border-radius:10px; border:1px solid #9D4EDD44; '
    f'font-weight:600;">🤖 AI advisory · review before acting</span>'
)

_CACHED_BADGE = (
    f'<span style="background:{COLORS["surface2"]}; color:{COLORS["text_muted"]}; '
    f'font-size:10px; padding:2px 6px; border-radius:8px; margin-left:6px;">'
    f'cached</span>'
)


def _ai_box(content: str, cached: bool = False) -> None:
    """Render an AI response inside a distinct advisory box."""
    cache_html = _CACHED_BADGE if cached else ""
    st.markdown(
        f'<div style="background:{COLORS["deep"]}; border:1px solid #9D4EDD44; '
        f'border-left:3px solid #9D4EDD; border-radius:8px; padding:14px 18px; '
        f'margin:8px 0 12px 0;">'
        f'<div style="margin-bottom:8px;">{_AI_BADGE}{cache_html}</div>'
        f'<div style="font-size:13px; color:{COLORS["text_secondary"]}; line-height:1.6;">'
        f'{content.replace(chr(10), "<br>")}'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


def _make_advisor(repo: CockpitRepository) -> CockpitAdvisor | None:
    """Build advisor from environment. Returns None if disabled."""
    import os
    enabled = os.environ.get("COCKPIT_AI_ENABLED", "false").lower() in ("1", "true", "yes")
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not enabled:
        return None
    if not api_key:
        st.warning("COCKPIT_AI_ENABLED=true but ANTHROPIC_API_KEY is not set.")
        return None
    return CockpitAdvisor(api_key=api_key, repo=repo, enabled=True)


def _cache_hit(repo: CockpitRepository, date_str: str, kind: str, target: str, prompt: str) -> bool:
    """Quick check whether a cache entry exists for this prompt."""
    import hashlib
    ph = hashlib.sha256(prompt.encode()).hexdigest()[:16]
    row = repo._conn.execute(
        "SELECT 1 FROM ai_explanation WHERE target_date=? AND kind=? AND target=? AND prompt_hash=?",
        (date_str, kind, target, ph),
    ).fetchone()
    return row is not None


# ── Main render ───────────────────────────────────────────────────────────────

def render_ai_panel(
    scored_df: pd.DataFrame,
    scores: dict,                    # key → ScoredIssue
    prod_df: pd.DataFrame,
    done_df: pd.DataFrame,
    issues_df: pd.DataFrame,
    weekly_df: pd.DataFrame,
    target_date: date,
    repo: CockpitRepository,
) -> None:
    section_header("AI Advisory")

    import os
    ai_enabled = os.environ.get("COCKPIT_AI_ENABLED", "false").lower() in ("1", "true", "yes")

    if not ai_enabled:
        st.markdown(
            f'<div style="background:{COLORS["surface"]}; border:1px solid {COLORS["border"]}; '
            f'border-radius:8px; padding:20px; text-align:center; '
            f'color:{COLORS["text_muted"]}; font-size:13px;">'
            f'🤖 AI advisory is disabled.<br>'
            f'<span style="font-size:11px;">Set <code>COCKPIT_AI_ENABLED=true</code> and '
            f'<code>ANTHROPIC_API_KEY=...</code> to enable.</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
        return

    advisor = _make_advisor(repo)
    if advisor is None:
        return

    date_str = target_date.isoformat()

    # ── Section 1: Day Summary ────────────────────────────────────────────────
    _render_day_summary(advisor, prod_df, done_df, issues_df, target_date, date_str)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Section 2: Bottleneck Detection ──────────────────────────────────────
    _render_bottlenecks(advisor, weekly_df, issues_df, target_date, date_str)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Section 3: Priority Explainer ────────────────────────────────────────
    _render_priority_explainer(advisor, scored_df, scores, target_date, date_str)


# ── Section renderers ─────────────────────────────────────────────────────────

def _render_day_summary(
    advisor: CockpitAdvisor,
    prod_df: pd.DataFrame,
    done_df: pd.DataFrame,
    issues_df: pd.DataFrame,
    target_date: date,
    date_str: str,
) -> None:
    st.markdown(
        f'<div style="font-size:14px; font-weight:600; color:{COLORS["text_primary"]}; '
        f'margin-bottom:10px;">📊 Day Summary</div>',
        unsafe_allow_html=True,
    )

    # Check cache state for button label
    kind = "day_summary"
    target_id = f"team:{date_str}"

    col_btn, col_info = st.columns([2, 3])
    with col_btn:
        if st.button("Generate day summary", key="ai_day_sum_btn", use_container_width=True):
            st.session_state["ai_day_sum_loading"] = True

    with col_info:
        st.markdown(
            f'<div style="font-size:11px; color:{COLORS["text_muted"]}; padding-top:8px;">'
            f'Uses sonnet-4-6 · aggregated metrics only · no raw Jira content</div>',
            unsafe_allow_html=True,
        )

    # Show existing cached result or trigger new call
    cached_body = _get_cached(advisor._repo, date_str, kind, target_id)
    if cached_body:
        _ai_box(cached_body, cached=True)
    elif st.session_state.get("ai_day_sum_loading"):
        with st.spinner("Generating…"):
            try:
                body = advisor.summarize_day(prod_df, done_df, issues_df, target_date)
                st.session_state["ai_day_sum_loading"] = False
                _ai_box(body, cached=False)
                st.rerun()
            except AdvisorDisabledError as e:
                st.error(str(e))
            except Exception as e:
                st.error(f"AI call failed: {e}")
                st.session_state["ai_day_sum_loading"] = False


def _render_bottlenecks(
    advisor: CockpitAdvisor,
    weekly_df: pd.DataFrame,
    issues_df: pd.DataFrame,
    target_date: date,
    date_str: str,
) -> None:
    st.markdown(
        f'<div style="font-size:14px; font-weight:600; color:{COLORS["text_primary"]}; '
        f'margin-bottom:10px;">⚠️ Bottleneck Detection</div>',
        unsafe_allow_html=True,
    )

    kind = "bottlenecks"
    target_id = f"team:{date_str}"

    if weekly_df.empty:
        st.markdown(
            f'<div style="color:{COLORS["text_muted"]}; font-size:12px;">'
            f'Weekly data not available — run ingestion for at least 2 days.</div>',
            unsafe_allow_html=True,
        )
        return

    col_btn, col_info = st.columns([2, 3])
    with col_btn:
        if st.button("Detect bottlenecks", key="ai_btl_btn", use_container_width=True):
            st.session_state["ai_btl_loading"] = True
    with col_info:
        st.markdown(
            f'<div style="font-size:11px; color:{COLORS["text_muted"]}; padding-top:8px;">'
            f'Uses opus-4-6 · 7-day rolling data · advisory suggestions only</div>',
            unsafe_allow_html=True,
        )

    cached_body = _get_cached(advisor._repo, date_str, kind, target_id)
    if cached_body:
        _ai_box(cached_body, cached=True)
    elif st.session_state.get("ai_btl_loading"):
        with st.spinner("Analysing 7-day metrics…"):
            try:
                body = advisor.detect_bottlenecks(weekly_df, issues_df, target_date)
                st.session_state["ai_btl_loading"] = False
                _ai_box(body, cached=False)
                st.rerun()
            except AdvisorDisabledError as e:
                st.error(str(e))
            except Exception as e:
                st.error(f"AI call failed: {e}")
                st.session_state["ai_btl_loading"] = False


def _render_priority_explainer(
    advisor: CockpitAdvisor,
    scored_df: pd.DataFrame,
    scores: dict,
    target_date: date,
    date_str: str,
) -> None:
    st.markdown(
        f'<div style="font-size:14px; font-weight:600; color:{COLORS["text_primary"]}; '
        f'margin-bottom:10px;">🔍 Priority Explainer</div>',
        unsafe_allow_html=True,
    )

    if scored_df.empty or not scores:
        st.markdown(
            f'<div style="color:{COLORS["text_muted"]}; font-size:12px;">'
            f'No scored issues available.</div>',
            unsafe_allow_html=True,
        )
        return

    available_keys = [str(k) for k in scored_df["key"].tolist() if k in scores]
    if not available_keys:
        return

    col_sel, col_btn = st.columns([3, 1])
    with col_sel:
        selected_key = st.selectbox(
            "Select issue to explain",
            options=available_keys,
            format_func=lambda k: f"{k}  (score: {scores[k].total:.0f})",
            key="ai_exp_key",
            label_visibility="collapsed",
        )
    with col_btn:
        explain_clicked = st.button("Explain", key="ai_exp_btn", use_container_width=True)

    if explain_clicked:
        st.session_state["ai_exp_target"] = selected_key
        st.session_state["ai_exp_loading"] = True

    explain_target = st.session_state.get("ai_exp_target", selected_key)

    if explain_target not in scores:
        return

    scored_issue = scores[explain_target]
    issue_row = scored_df[scored_df["key"] == explain_target]
    summary = str(issue_row["summary"].iloc[0]) if not issue_row.empty and "summary" in issue_row.columns else ""
    priority = str(issue_row["priority"].iloc[0]) if not issue_row.empty and "priority" in issue_row.columns else "Medium"

    kind = "priority"
    cached_body = _get_cached(advisor._repo, date_str, kind, explain_target)
    if cached_body and not st.session_state.get("ai_exp_loading"):
        st.markdown(
            f'<div style="font-size:11px; color:{COLORS["text_muted"]}; margin-bottom:4px;">'
            f'Explaining <b>{explain_target}</b></div>',
            unsafe_allow_html=True,
        )
        _ai_box(cached_body, cached=True)
    elif st.session_state.get("ai_exp_loading") and st.session_state.get("ai_exp_target") == explain_target:
        with st.spinner(f"Explaining {explain_target}…"):
            try:
                body = advisor.explain_priority(
                    key=explain_target,
                    summary=summary,
                    priority=priority,
                    score=scored_issue.total,
                    factors=scored_issue.factors,
                    target_date=target_date,
                )
                st.session_state["ai_exp_loading"] = False
                _ai_box(body, cached=False)
                st.rerun()
            except AdvisorDisabledError as e:
                st.error(str(e))
                st.session_state["ai_exp_loading"] = False
            except Exception as e:
                st.error(f"AI call failed: {e}")
                st.session_state["ai_exp_loading"] = False

    # Cache clear utility (de-scoped to selected issue)
    if _get_cached(advisor._repo, date_str, kind, explain_target):
        if st.button(f"Clear cached explanation for {explain_target}", key="ai_exp_clear"):
            advisor._repo._conn.execute(
                "DELETE FROM ai_explanation WHERE target_date=? AND kind=? AND target=?",
                (date_str, kind, explain_target),
            )
            advisor._repo._conn.commit()
            st.rerun()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_cached(repo: CockpitRepository, date_str: str, kind: str, target: str) -> str | None:
    """Return any cached body for this (date, kind, target) — any prompt_hash."""
    row = repo._conn.execute(
        "SELECT body FROM ai_explanation WHERE target_date=? AND kind=? AND target=? ORDER BY id DESC LIMIT 1",
        (date_str, kind, target),
    ).fetchone()
    return row["body"] if row else None
