"""
pages/4_📈_Metrics.py
Full experiment metrics dashboard with Go/No-Go decision.
Implements all 8 metrics from Section 5.6 of the spec.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from utils.metrics_calc import compute_metrics

st.set_page_config(page_title="Metrics Dashboard", page_icon="📈", layout="wide")
st.title("📈 Metrics Dashboard")
st.caption("Experiment results against Go/No-Go thresholds from Section 5.6")

# ─── Data ─────────────────────────────────────────────────────────────────────
results = st.session_state.get("experiment_results", {})
if not results:
    st.info("No results yet. Run the experiment first.")
    st.stop()

funder_stats = list(results.values())
metrics = compute_metrics(funder_stats)
totals = metrics["totals"]
decision = metrics["decision"]

# ─── Go/No-Go Banner ─────────────────────────────────────────────────────────
if decision["decision"] == "GO":
    st.success(f"## {decision['label']}\n{decision['rationale']}")
elif decision["decision"] == "CONDITIONAL":
    st.warning(f"## {decision['label']}\n{decision['rationale']}")
else:
    st.error(f"## {decision['label']}\n{decision['rationale']}")

st.divider()

# ─── Core 8 Metrics ──────────────────────────────────────────────────────────
st.subheader("Core Experiment Metrics")

def metric_with_threshold(col, label, value, unit, threshold_go, threshold_warn=None, invert=False):
    """Display a metric with a color-coded status indicator."""
    if invert:
        # Lower is better (e.g. false positive rate, cost)
        if value <= threshold_go:
            status = "🟢"
        elif threshold_warn and value <= threshold_warn:
            status = "🟡"
        else:
            status = "🔴"
    else:
        # Higher is better
        if value >= threshold_go:
            status = "🟢"
        elif threshold_warn and value >= threshold_warn:
            status = "🟡"
        else:
            status = "🔴"
    col.metric(f"{status} {label}", f"{value:.1f}{unit}")


m1, m2, m3, m4 = st.columns(4)
metric_with_threshold(m1, "IRS Match Rate", metrics["irs_match_rate"], "%", 60, 40)
metric_with_threshold(m2, "Confirmation Rate", metrics["confirmation_rate"], "%", 70)
metric_with_threshold(m3, "New Discovery Rate", metrics["new_discovery_rate"], "%", 20)
metric_with_threshold(m4, "Grant-Relevant Found", metrics["grant_relevant_rate"], "%", 10)

m5, m6, m7, m8 = st.columns(4)
metric_with_threshold(m5, "False Positive Rate", metrics["false_positive_rate"], "%", 15, 25, invert=True)
metric_with_threshold(m6, "Stale IRS Detection", metrics["stale_detection_rate"], "%", 50)
metric_with_threshold(m7, "Cost per Funder", metrics["cost_per_funder"] * 100, "¢", 50, 200, invert=True)
m8.metric("Total API Cost", f"${metrics['total_api_cost']:.3f}")

st.divider()

# ─── Totals ───────────────────────────────────────────────────────────────────
st.subheader("Pipeline Totals")
t1, t2, t3, t4, t5, t6 = st.columns(6)
t1.metric("Funders Processed", totals["funders"])
t2.metric("IRS People (ground truth)", totals["irs_people"])
t3.metric("MATCHED", totals["matched"])
t4.metric("MOVED", totals["moved"])
t5.metric("DISCOVERED", totals["discovered"])
t6.metric("IRS Only (not found)", totals["irs_only"])

st.divider()

# ─── Charts ───────────────────────────────────────────────────────────────────
chart1, chart2 = st.columns(2)

with chart1:
    st.subheader("Status Distribution")
    status_data = {
        "MATCHED": totals["matched"],
        "MOVED": totals["moved"],
        "IRS_ONLY": totals["irs_only"],
        "DISCOVERED": totals["discovered"],
    }
    fig_pie = px.pie(
        values=list(status_data.values()),
        names=list(status_data.keys()),
        color=list(status_data.keys()),
        color_discrete_map={
            "MATCHED": "#22c55e",
            "MOVED": "#f59e0b",
            "IRS_ONLY": "#3b82f6",
            "DISCOVERED": "#8b5cf6",
        },
        hole=0.4,
    )
    fig_pie.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=320,
    )
    st.plotly_chart(fig_pie, use_container_width=True)

with chart2:
    st.subheader("Match Rate by Segment")
    seg = metrics["segment_breakdown"]
    if seg:
        seg_df = pd.DataFrame([
            {"Segment": k.title(), "Match Rate %": v["match_rate"], "Funders": v["count"]}
            for k, v in seg.items()
        ])
        fig_bar = px.bar(
            seg_df, x="Segment", y="Match Rate %",
            text="Match Rate %",
            color="Segment",
            color_discrete_sequence=["#4F46E5", "#7C3AED", "#A78BFA", "#6B7280"],
        )
        fig_bar.add_hline(y=60, line_dash="dash", line_color="#22c55e",
                          annotation_text="GO threshold (60%)")
        fig_bar.add_hline(y=40, line_dash="dot", line_color="#f59e0b",
                          annotation_text="Conditional (40%)")
        fig_bar.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_bar.update_layout(
            showlegend=False, paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)", height=320,
        )
        st.plotly_chart(fig_bar, use_container_width=True)

# ─── Per-funder match rate scatter ────────────────────────────────────────────
st.subheader("Match Rate per Funder")
scatter_data = []
for r in funder_stats:
    irs = r.get("irs_people_count", 0)
    found = r.get("matched_count", 0) + r.get("moved_count", 0)
    rate = round((found / irs * 100) if irs > 0 else 0, 1)
    scatter_data.append({
        "Organization": r.get("org_name", "")[:40],
        "Match Rate %": rate,
        "Segment": r.get("segment", "unknown"),
        "IRS People": irs,
        "Discovered": r.get("discovered_count", 0),
        "Errors": len(r.get("api_errors") or []),
    })

scatter_df = pd.DataFrame(scatter_data).sort_values("Match Rate %", ascending=True)
fig_scatter = px.bar(
    scatter_df,
    x="Match Rate %",
    y="Organization",
    color="Segment",
    orientation="h",
    hover_data=["IRS People", "Discovered", "Errors"],
    color_discrete_map={
        "large": "#4F46E5", "mid": "#7C3AED",
        "small": "#A78BFA", "unknown": "#6B7280"
    },
    height=max(400, len(scatter_df) * 22),
)
fig_scatter.add_vline(x=60, line_dash="dash", line_color="#22c55e")
fig_scatter.add_vline(x=40, line_dash="dot", line_color="#f59e0b")
fig_scatter.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    yaxis=dict(tickfont=dict(size=10)),
)
st.plotly_chart(fig_scatter, use_container_width=True)

# ─── Threshold reference table ────────────────────────────────────────────────
st.divider()
st.subheader("Threshold Reference")
threshold_rows = [
    {"Metric": "IRS Person Match Rate", "GO Threshold": "≥ 60%", "Conditional": "40–59%",
     "No-Go": "< 40%", "Your Result": f"{metrics['irs_match_rate']:.1f}%"},
    {"Metric": "Current Employee Confirmation Rate", "GO Threshold": "≥ 70%", "Conditional": "—",
     "No-Go": "—", "Your Result": f"{metrics['confirmation_rate']:.1f}%"},
    {"Metric": "New Staff Discovery Rate", "GO Threshold": "> 20%", "Conditional": "—",
     "No-Go": "—", "Your Result": f"{metrics['new_discovery_rate']:.1f}%"},
    {"Metric": "Grant-Relevant Staff Found", "GO Threshold": "> 10% of discovered", "Conditional": "—",
     "No-Go": "—", "Your Result": f"{metrics['grant_relevant_rate']:.1f}%"},
    {"Metric": "False Positive Rate", "GO Threshold": "< 15%", "Conditional": "15–25%",
     "No-Go": "≥ 25%", "Your Result": f"{metrics['false_positive_rate']:.1f}%"},
    {"Metric": "Stale IRS Detection Rate", "GO Threshold": "> 50%", "Conditional": "—",
     "No-Go": "—", "Your Result": f"{metrics['stale_detection_rate']:.1f}%"},
    {"Metric": "Cost per Funder", "GO Threshold": "< $0.50", "Conditional": "$0.50–$2.00",
     "No-Go": "> $2.00", "Your Result": f"${metrics['cost_per_funder']:.4f}"},
]
st.dataframe(pd.DataFrame(threshold_rows), use_container_width=True, hide_index=True)

# ─── CSV export ───────────────────────────────────────────────────────────────
metrics_export = {
    "metric": [r["Metric"] for r in threshold_rows],
    "go_threshold": [r["GO Threshold"] for r in threshold_rows],
    "your_result": [r["Your Result"] for r in threshold_rows],
    "decision": [decision["decision"]] + [""] * (len(threshold_rows) - 1),
}
csv = pd.DataFrame(metrics_export).to_csv(index=False)
st.download_button("📥 Download experiment_metrics.csv", data=csv,
                   file_name="experiment_metrics.csv", mime="text/csv")
