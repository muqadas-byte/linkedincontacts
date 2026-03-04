"""
pages/1_📊_Overview.py
Explore the 100 funder sample before running the experiment.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Sample Overview", page_icon="📊", layout="wide")
st.title("📊 Sample Overview")
st.caption("Explore the 100 funders before running the experiment")

if not st.session_state.get("funders_loaded"):
    st.warning("No funders loaded. Go to the **Home** page and upload 100randomFunders.json first.")
    st.stop()

funders = st.session_state["funders"]
df = pd.DataFrame(funders)

# ─── Summary cards ────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    st.metric("Total Funders", len(df))
with c2:
    st.metric("With Leadership Data", int((df["leadership_count"] > 0).sum()))
with c3:
    st.metric("With Website", int((df["website_domain"] != "").sum()))
with c4:
    total_people = df["leadership_count"].sum()
    st.metric("Total IRS People", int(total_people))
with c5:
    avg_people = df["leadership_count"].mean()
    st.metric("Avg People / Funder", f"{avg_people:.1f}")

st.divider()

# ─── Charts ───────────────────────────────────────────────────────────────────
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("Asset Segment Distribution")
    seg_counts = df["segment"].value_counts().reset_index()
    seg_counts.columns = ["Segment", "Count"]
    seg_order = ["large", "mid", "small", "unknown"]
    seg_counts["Segment"] = pd.Categorical(seg_counts["Segment"], categories=seg_order, ordered=True)
    seg_counts = seg_counts.sort_values("Segment")
    fig = px.bar(
        seg_counts, x="Segment", y="Count",
        color="Segment",
        color_discrete_map={
            "large": "#4F46E5", "mid": "#7C3AED",
            "small": "#A78BFA", "unknown": "#6B7280"
        },
        text="Count",
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(showlegend=False, plot_bgcolor="rgba(0,0,0,0)",
                      paper_bgcolor="rgba(0,0,0,0)", height=300)
    st.plotly_chart(fig, use_container_width=True)

with chart_col2:
    st.subheader("Leadership Count Distribution")
    fig2 = px.histogram(
        df, x="leadership_count", nbins=20,
        labels={"leadership_count": "# of IRS People Listed"},
        color_discrete_sequence=["#4F46E5"],
    )
    fig2.update_layout(plot_bgcolor="rgba(0,0,0,0)",
                       paper_bgcolor="rgba(0,0,0,0)", height=300)
    st.plotly_chart(fig2, use_container_width=True)

# ─── State coverage ───────────────────────────────────────────────────────────
st.subheader("Geographic Coverage")
state_counts = df[df["state"] != ""]["state"].value_counts().reset_index()
state_counts.columns = ["state", "count"]
if not state_counts.empty:
    fig3 = px.choropleth(
        state_counts,
        locations="state",
        locationmode="USA-states",
        color="count",
        scope="usa",
        color_continuous_scale="Purples",
        labels={"count": "Funders"},
    )
    fig3.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        geo_bgcolor="rgba(0,0,0,0)",
        height=380,
        margin=dict(t=0, b=0, l=0, r=0),
    )
    st.plotly_chart(fig3, use_container_width=True)

# ─── Funder table ─────────────────────────────────────────────────────────────
st.subheader("Funder Details")

# Filters
filter_col1, filter_col2, filter_col3 = st.columns(3)
with filter_col1:
    seg_filter = st.multiselect(
        "Filter by Segment",
        options=["large", "mid", "small", "unknown"],
        default=["large", "mid", "small", "unknown"],
    )
with filter_col2:
    has_leadership = st.checkbox("Only funders with leadership data", value=False)
with filter_col3:
    search_term = st.text_input("Search by org name or EIN", placeholder="Maine Education...")

filtered = df[df["segment"].isin(seg_filter)]
if has_leadership:
    filtered = filtered[filtered["leadership_count"] > 0]
if search_term:
    mask = (
        filtered["org_name"].str.contains(search_term, case=False, na=False) |
        filtered["ein"].str.contains(search_term, case=False, na=False)
    )
    filtered = filtered[mask]

# Display table
display_df = filtered[[
    "ein", "org_name", "city", "state", "segment",
    "leadership_count", "website_domain", "total_assets"
]].copy()
display_df["total_assets"] = display_df["total_assets"].apply(
    lambda x: f"${x:,.0f}" if x else "—"
)
display_df.columns = [
    "EIN", "Organization", "City", "State", "Segment",
    "IRS People", "Website", "Total Assets"
]
st.dataframe(display_df, use_container_width=True, height=450)
st.caption(f"Showing {len(filtered)} of {len(df)} funders")

# ─── Ground truth preview ─────────────────────────────────────────────────────
st.divider()
st.subheader("Leadership Ground Truth Preview")
st.caption("IRS leadership data that will be cross-referenced during the experiment")

selected_org = st.selectbox(
    "Select a funder to view leadership",
    options=[f["org_name"] for f in funders if f.get("leadership")],
    index=0,
)
if selected_org:
    funder_data = next((f for f in funders if f["org_name"] == selected_org), None)
    if funder_data and funder_data.get("leadership"):
        leadership_df = pd.DataFrame(funder_data["leadership"])
        st.dataframe(leadership_df, use_container_width=True)
        st.caption(f"EIN: {funder_data['ein']} | {funder_data['city']}, {funder_data['state']}")
