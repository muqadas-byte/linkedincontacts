"""
pages/3_📋_Results.py
Per-funder results explorer with staff profile drill-down.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import pandas as pd
import json
import io
from utils.supabase_client import get_or_create_client, auto_restore_session

st.set_page_config(page_title="Results", page_icon="📋", layout="wide")
st.title("📋 Results Explorer")
st.caption("Per-funder discovery and matching results")

# ─── Data source: session state or Supabase ───────────────────────────────────
results = st.session_state.get("experiment_results", {})

if not results:
    # Auto-connect and try loading the most recent session
    sb, _ = get_or_create_client()
    if sb:
        try:
            with st.spinner("Loading results from Supabase..."):
                session_id, restored = auto_restore_session(sb)
                if session_id and restored:
                    st.session_state["active_session_id"] = session_id
                    st.session_state["experiment_results"] = restored
                    results = restored
        except Exception as e:
            st.error(f"Failed to load from Supabase: {e}")

if not results:
    st.info("No results yet. Run the experiment first.")
    st.stop()

# ─── Status badge colors ──────────────────────────────────────────────────────
STATUS_COLORS = {
    "MATCHED": "🟢",
    "MOVED": "🟡",
    "IRS_ONLY": "🔵",
    "DISCOVERED": "🟣",
}
STATUS_LABELS = {
    "MATCHED": "Verified (still at org)",
    "MOVED": "Left the org",
    "IRS_ONLY": "IRS only (not found online)",
    "DISCOVERED": "New discovery (not in IRS)",
}

# ─── Summary table ────────────────────────────────────────────────────────────
st.subheader("All Funders Summary")

summary_rows = []
for ein, r in results.items():
    total_irs = r.get("irs_people_count", 0)
    matched = r.get("matched_count", 0)
    moved = r.get("moved_count", 0)
    match_rate = round(((matched + moved) / total_irs * 100) if total_irs > 0 else 0, 1)
    has_errors = len(r.get("api_errors") or []) > 0
    summary_rows.append({
        "EIN": ein,
        "Organization": r.get("org_name", "")[:45],
        "Segment": r.get("segment", ""),
        "IRS People": total_irs,
        "Matched": matched,
        "Moved": moved,
        "IRS Only": r.get("irs_only_count", 0),
        "Discovered": r.get("discovered_count", 0),
        "Match Rate %": match_rate,
        "Grant Relevant": r.get("grant_relevant_count", 0),
        "Queries Run": r.get("serper_queries_run", 0),
        "Errors": "⚠️" if has_errors else "",
    })

summary_df = pd.DataFrame(summary_rows)

# Filters
filter1, filter2, filter3 = st.columns(3)
with filter1:
    seg_opts = summary_df["Segment"].dropna().unique().tolist()
    seg_filter = st.multiselect("Segment", seg_opts, default=seg_opts)
with filter2:
    show_errors_only = st.checkbox("Only show funders with errors")
with filter3:
    sort_col = st.selectbox("Sort by", ["Match Rate %", "IRS People", "Discovered", "Organization"])

filtered_summary = summary_df[summary_df["Segment"].isin(seg_filter)]
if show_errors_only:
    filtered_summary = filtered_summary[filtered_summary["Errors"] != ""]
filtered_summary = filtered_summary.sort_values(sort_col, ascending=False)

st.dataframe(
    filtered_summary,
    use_container_width=True,
    height=400,
    column_config={
        "Match Rate %": st.column_config.ProgressColumn(
            "Match Rate %", min_value=0, max_value=100, format="%.1f%%"
        ),
    }
)

# ─── Export buttons ───────────────────────────────────────────────────────────
exp_col1, exp_col2, exp_col3 = st.columns(3)

with exp_col1:
    csv_summary = summary_df.to_csv(index=False)
    st.download_button(
        "📥 Download funders_summary.csv",
        data=csv_summary,
        file_name="funders_summary.csv",
        mime="text/csv",
    )

with exp_col2:
    # All merged staff
    all_staff = []
    for ein, r in results.items():
        for person in (r.get("merged_staff") or []):
            all_staff.append({
                "ein": ein,
                "org_name": r.get("org_name", ""),
                **{k: v for k, v in person.items() if k != "photo_url"},
            })
    if all_staff:
        staff_df = pd.DataFrame(all_staff)
        csv_staff = staff_df.to_csv(index=False)
        st.download_button(
            "📥 Download merged_staff.csv",
            data=csv_staff,
            file_name="merged_staff.csv",
            mime="text/csv",
        )

with exp_col3:
    # Errors export
    all_errors = []
    for ein, r in results.items():
        for err in (r.get("api_errors") or []):
            all_errors.append({
                "ein": ein,
                "org_name": r.get("org_name", ""),
                **err
            })
    if all_errors:
        errors_df = pd.DataFrame(all_errors)
        csv_errors = errors_df.to_csv(index=False)
        st.download_button(
            "📥 Download api_errors.csv",
            data=csv_errors,
            file_name="api_errors.csv",
            mime="text/csv",
        )

st.divider()

# ─── Per-funder drill-down ─────────────────────────────────────────────────────
st.subheader("🔍 Funder Drill-Down")

funder_names = [r.get("org_name", ein) for ein, r in results.items()]
selected_name = st.selectbox("Select a funder", funder_names)
selected_ein = next(
    (ein for ein, r in results.items() if r.get("org_name") == selected_name), None
)

if selected_ein:
    r = results[selected_ein]
    merged_staff = r.get("merged_staff") or []

    # Header metrics
    h1, h2, h3, h4, h5, h6 = st.columns(6)
    h1.metric("IRS People", r.get("irs_people_count", 0))
    h2.metric("✅ Matched", r.get("matched_count", 0))
    h3.metric("🔄 Moved", r.get("moved_count", 0))
    h4.metric("🔵 IRS Only", r.get("irs_only_count", 0))
    h5.metric("🆕 Discovered", r.get("discovered_count", 0))
    h6.metric("⚠️ Errors", len(r.get("api_errors") or []))

    # Staff profiles
    if merged_staff:
        st.subheader("Staff Profiles")
        for person in sorted(merged_staff, key=lambda x: x.get("status", "")):
            status = person.get("status", "IRS_ONLY")
            icon = STATUS_COLORS.get(status, "⚪")
            label = STATUS_LABELS.get(status, status)
            name = person.get("person_name") or person.get("irs_name") or "Unknown"
            title = person.get("current_title") or person.get("irs_title") or ""
            company = person.get("current_company") or ""
            linkedin = person.get("linkedin_url") or ""
            score = person.get("match_score") or 0
            enriched = person.get("enriched", False)
            grant_rel = person.get("is_grant_relevant", False)
            photo_url = person.get("photo_url") or ""

            with st.container():
                col_photo, col_a, col_b, col_c, col_d = st.columns([1, 3, 3, 2, 2])
                with col_photo:
                    if photo_url:
                        st.image(photo_url, width=56)
                    else:
                        st.markdown("👤")
                with col_a:
                    st.markdown(f"**{icon} {name}**")
                    if person.get("irs_title"):
                        st.caption(f"IRS: {person['irs_title']}")
                with col_b:
                    if title:
                        st.markdown(f"*{title}*")
                    if company:
                        st.caption(f"@ {company}")
                    if grant_rel:
                        st.caption("🎯 Grant-relevant role")
                with col_c:
                    st.caption(f"Status: **{label}**")
                    if score > 0:
                        st.caption(f"Match score: {score:.0f}%")
                    if enriched:
                        st.caption("✓ PDL enriched")
                with col_d:
                    if linkedin:
                        st.markdown(f"[LinkedIn Profile]({linkedin})")
                    else:
                        st.caption("No LinkedIn found")
            st.markdown("---")
    else:
        st.info("No staff data for this funder.")

    # API errors
    api_errors = r.get("api_errors") or []
    if api_errors:
        with st.expander(f"⚠️ API Errors ({len(api_errors)})", expanded=False):
            for err in api_errors:
                st.error(f"**{err.get('step', 'unknown')}**: {err.get('error', '')}")

    # Serper query detail
    if r.get("merged_staff"):
        with st.expander("🔍 SerpApi Query Detail", expanded=False):
            st.markdown(f"""
            - Queries run: **{r.get('serper_queries_run', 0)}**
            - LinkedIn URLs found: **{r.get('serper_urls_found', 0)}**
            - PDL profiles found: **{r.get('pdl_profiles_found', 0)}**
            - PDL enrichments done: **{r.get('enrichments_done', 0)}**
            """)
