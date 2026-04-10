"""ADAPT brand tokens and CSS injection for Streamlit."""
from __future__ import annotations

from pathlib import Path
import streamlit as st

# ── Brand color tokens ────────────────────────────────────────────────────────
COLORS: dict[str, str] = {
    # Brand purple
    "primary":          "#9D4EDD",
    "primary_light":    "#7C3AED",
    "accent":           "#B97FFF",
    "deep":             "#3B1F6E",
    # Page canvas
    "bg":               "#FFFFFF",
    "surface":          "#F9F7FF",
    "surface2":         "#EDE9FE",
    "border":           "#E5E0F5",
    # Text
    "text_primary":     "#1E1B4B",
    "text_secondary":   "#374151",
    "text_muted":       "#6B7280",
    # Status
    "success":          "#059669",
    "warning":          "#D97706",
    "danger":           "#DC2626",
    "info":             "#2563EB",
    # Status aliases
    "done_green":       "#059669",
    "in_progress_blue": "#2563EB",
    "todo_grey":        "#9CA3AF",
    # Priority
    "highest":          "#DC2626",
    "high":             "#EA580C",
    "medium":           "#D97706",
    "low":              "#16A34A",
    "lowest":           "#6B7280",
}

# ── Engineer palette for multi-series charts ──────────────────────────────────
CHART_PALETTE = [
    "#9D4EDD",  # purple  (primary)
    "#2563EB",  # blue
    "#059669",  # green
    "#D97706",  # amber
    "#DC2626",  # red
    "#0891B2",  # cyan
    "#7C3AED",  # violet
    "#EA580C",  # orange
    "#0D9488",  # teal
    "#DB2777",  # pink
]

# ── Chart layout defaults (import and spread into go.Figure layout) ───────────
def chart_layout(title: str = "", height: int = 300, show_legend: bool = True) -> dict:
    return dict(
        title=dict(text=title, font=dict(size=13, color=COLORS["text_primary"], family="Inter, sans-serif"), x=0, pad=dict(b=8)),
        height=height,
        margin=dict(l=4, r=4, t=36 if title else 8, b=40),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", size=11, color=COLORS["text_secondary"]),
        legend=dict(
            orientation="h",
            y=1.12,
            x=0,
            font=dict(size=11),
            bgcolor="rgba(0,0,0,0)",
        ) if show_legend else dict(visible=False),
        showlegend=show_legend,
        xaxis=dict(
            showgrid=False,
            zeroline=False,
            tickfont=dict(size=11, color=COLORS["text_muted"]),
            linecolor=COLORS["border"],
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor=COLORS["border"],
            gridwidth=1,
            zeroline=False,
            tickfont=dict(size=11, color=COLORS["text_muted"]),
            linecolor=COLORS["border"],
        ),
        hovermode="x unified",
        bargap=0.30,
    )

# ── CSS ───────────────────────────────────────────────────────────────────────
_CSS = """
<style>
/* ── Layout ───────────────────────────────────────────────────────────────── */
.block-container {
  padding-top: 1rem !important;
  padding-bottom: 2rem !important;
  max-width: 1440px;
}

/* ── Markdown container overflow fix ──────────────────────────────────────── */
/* Streamlit wraps st.markdown() content in stMarkdownContainer which can
   clip child elements. Ensure it never hides content we inject. */
[data-testid="stMarkdownContainer"] {
  overflow: visible !important;
}
[data-testid="stMarkdownContainer"] p {
  overflow: visible !important;
}

/* ── Section headers ──────────────────────────────────────────────────────── */
.section-header {
  font-family: 'Space Grotesk', 'Inter', sans-serif;
  font-size: 14px;
  font-weight: 700;
  color: {text_primary};
  padding: 6px 0 8px;
  border-bottom: 2px solid {primary};
  margin-bottom: 14px;
  letter-spacing: -0.1px;
}

/* ── Sidebar ──────────────────────────────────────────────────────────────── */
[data-testid="stSidebar"] {
  background: {surface} !important;
  border-right: 1px solid {border} !important;
}

/* ── st.metric improvements ───────────────────────────────────────────────── */
[data-testid="metric-container"] {
  background: {surface};
  border: 1px solid {border};
  border-radius: 12px;
  padding: 14px 18px !important;
  box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}
[data-testid="stMetricLabel"] {
  font-size: 11px !important;
  font-weight: 600 !important;
  text-transform: uppercase;
  letter-spacing: 0.6px;
  color: {text_muted} !important;
}
[data-testid="stMetricValue"] {
  font-family: 'Space Grotesk', monospace !important;
  font-size: 26px !important;
  font-weight: 700 !important;
  color: {text_primary} !important;
  line-height: 1.1 !important;
}
[data-testid="stMetricDelta"] {
  font-size: 11px !important;
}

/* ── Tabs ─────────────────────────────────────────────────────────────────── */
[data-testid="stTabs"] [data-baseweb="tab-list"] {
  gap: 2px;
  border-bottom: 2px solid {border};
}
[data-testid="stTabs"] [data-baseweb="tab"] {
  font-size: 13px !important;
  font-weight: 500;
  color: {text_muted} !important;
  padding: 8px 16px !important;
  border-radius: 6px 6px 0 0;
}
[data-testid="stTabs"] [aria-selected="true"] {
  color: {primary} !important;
  font-weight: 700 !important;
  border-bottom: 2px solid {primary} !important;
}

/* ── st.container border ──────────────────────────────────────────────────── */
[data-testid="stVerticalBlockBorderWrapper"] {
  border-radius: 10px !important;
  border-color: {border} !important;
  box-shadow: 0 1px 3px rgba(0,0,0,0.04) !important;
}

/* ── Dataframe ────────────────────────────────────────────────────────────── */
[data-testid="stDataFrame"] {
  border-radius: 10px;
  overflow: hidden;
}

/* ── Buttons ──────────────────────────────────────────────────────────────── */
[data-testid="stButton"] button[kind="primary"] {
  background: linear-gradient(135deg, {primary_light} 0%, {primary} 100%) !important;
  border: none !important;
  box-shadow: 0 2px 8px rgba(157,78,221,0.30) !important;
  font-weight: 600 !important;
}
[data-testid="stButton"] button[kind="primary"]:hover {
  box-shadow: 0 4px 12px rgba(157,78,221,0.40) !important;
  transform: translateY(-1px);
}
</style>
"""


def inject_css() -> None:
    css = _CSS
    for key, val in COLORS.items():
        css = css.replace(f"{{{key}}}", val)
    st.markdown(css, unsafe_allow_html=True)


# ── Component helpers ─────────────────────────────────────────────────────────

def section_header(title: str) -> None:
    st.markdown(f'<div class="section-header">{title}</div>', unsafe_allow_html=True)


def priority_badge(priority: str) -> str:
    """Returns an HTML badge string (use only inside safe HTML blocks)."""
    p = priority.lower().replace(" ", "")
    cls_map = {
        "highest": "prio-highest",
        "high":    "prio-high",
        "medium":  "prio-medium",
        "low":     "prio-low",
        "lowest":  "prio-lowest",
    }
    cls = cls_map.get(p, "prio-medium")
    color_map = {
        "highest": COLORS["highest"],
        "high":    COLORS["high"],
        "medium":  COLORS["medium"],
        "low":     COLORS["low"],
        "lowest":  COLORS["lowest"],
    }
    color = color_map.get(p, COLORS["medium"])
    return (
        f'<span style="background:{color}18; color:{color}; border:1px solid {color}40; '
        f'font-size:10px; padding:1px 8px; border-radius:10px; font-weight:600; '
        f'white-space:nowrap;">{priority}</span>'
    )


def warning_chips(warnings_str: str) -> str:
    """Returns HTML chips for warning strings (use only inside safe HTML blocks)."""
    if not warnings_str:
        return ""
    chips = []
    labels = {
        "no_real_activity":          ("⚠ no effort logged", "warn"),
        "no_worklogs":               ("⚠ no worklogs",       "warn"),
        "completed_without_logging": ("⚠ done, not logged",  "warn"),
        "possible_overload":         ("🔥 overload",          "danger"),
    }
    for w in warnings_str.split("|"):
        w = w.strip()
        if not w:
            continue
        label, kind = labels.get(w, (w, "warn"))
        color = COLORS["danger"] if kind == "danger" else COLORS["warning"]
        chips.append(
            f'<span style="background:{color}18; color:{color}; font-size:10px; '
            f'padding:2px 7px; border-radius:10px; border:1px solid {color}40; '
            f'white-space:nowrap;">{label}</span>'
        )
    return " ".join(chips)


def user_initials(name: str) -> str:
    parts = name.strip().split()
    if len(parts) >= 2:
        return (parts[0][0] + parts[-1][0]).upper()
    return name[:2].upper() if name else "??"


def logo_html(logo_path, height: int = 36) -> str:
    if logo_path is None:
        return ""
    import base64
    try:
        data = Path(logo_path).read_bytes()
        ext = Path(logo_path).suffix.lstrip(".")
        mime = "image/svg+xml" if ext == "svg" else f"image/{ext}"
        b64 = base64.b64encode(data).decode()
        return (
            f'<img src="data:{mime};base64,{b64}" height="{height}" '
            f'style="vertical-align:middle; flex-shrink:0;">'
        )
    except Exception:
        return ""
