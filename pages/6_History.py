"""
pages/6_History.py
Browse all past experiment results across every session saved to Supabase.
One flat table of every org run — pick any to see full drill-down details.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import json
import streamlit as st
import pandas as pd

from utils.supabase_client import get_or_create_client

st.set_page_config(page_title="Session History", page_icon="🗂️", layout="wide")
st.title("🗂️ Experiment History")
st.caption("All orgs run across every past experiment — pick one to view full results")

STATUS_COLORS = {
    "MATCHED":    "🟢",
    "MOVED":      "🟡",
    "IRS_ONLY":   "🔵",
    "DISCOVERED": "🟣",
}
STATUS_LABELS = {
    "MATCHED":    "Verified (still at org)",
    "MOVED":      "Left the org",
    "IRS_ONLY":   "IRS only (not found online)",
    "DISCOVERED": "New discovery (not in IRS)",
}

# ─── Supabase connection ──────────────────────────────────────────────────────
with st.spinner("Connecting to Supabase..."):
    sb, conn_err = get_or_create_client()

if not sb:
    st.error(f"Could not connect to Supabase: {conn_err}")
    st.stop()

# ─── Load all sessions + all funder results ───────────────────────────────────
@st.cache_data(ttl=60, show_spinner=False)
def load_all_experiments(_sb):
    """
    Returns a flat list of dicts — one per (session, funder) pair.
    Cached for 60 s so navigating within the page doesn't re-fetch.
    """
    try:
        sessions = _sb.list_sessions()
    except Exception as e:
        return [], str(e)

    rows = []
    for session in sessions:
        sid        = session["id"]
        run_date   = (session.get("started_at") or "")[:19].replace("T", " ")
        status     = session.get("status", "")

        try:
            funders = _sb.get_funder_results(sid)
        except Exception:
            funders = []

        for f in funders:
            total_irs  = f.get("irs_people_count", 0)
            matched    = f.get("matched_count", 0)
            moved      = f.get("moved_count", 0)
            match_rate = round(((matched + moved) / total_irs * 100) if total_irs > 0 else 0, 1)

            api_errors = f.get("api_errors") or []
            if isinstance(api_errors, str):
                try:
                    api_errors = json.loads(api_errors)
                except Exception:
                    api_errors = []

            rows.append({
                # lookup keys (not displayed in table)
                "_session_id": sid,
                "_ein":        f.get("ein", ""),
                "_run_date":   run_date,
                # display columns
                "Run Date":    run_date,
                "Session":     status.title(),
                "Organization": (f.get("org_name") or f.get("ein", ""))[:55],
                "Segment":     f.get("segment", ""),
                "IRS People":  total_irs,
                "Matched":     matched,
                "Moved":       moved,
                "IRS Only":    f.get("irs_only_count", 0),
                "Discovered":  f.get("discovered_count", 0),
                "Match Rate %": match_rate,
                "Grant Relevant": f.get("grant_relevant_count", 0),
                "Errors":      "⚠️" if api_errors else "",
            })

    return rows, None


with st.spinner("Loading all experiments from Supabase..."):
    all_rows, load_error = load_all_experiments(sb)

if load_error:
    st.error(f"Failed to load experiments: {load_error}")
    st.stop()

if not all_rows:
    st.info("No experiment results found in Supabase yet. Run an experiment first.")
    st.stop()

# ─── Section 1: Flat experiments table ───────────────────────────────────────
st.subheader(f"📋 All Experiments ({len(all_rows)} orgs across {len(set(r['_session_id'] for r in all_rows))} runs)")

display_df = pd.DataFrame([
    {k: v for k, v in r.items() if not k.startswith("_")}
    for r in all_rows
])

# Filters
f1, f2, f3 = st.columns(3)
with f1:
    seg_opts = sorted(display_df["Segment"].dropna().unique().tolist())
    seg_filter = st.multiselect("Filter by Segment", seg_opts, default=seg_opts)
with f2:
    show_errors_only = st.checkbox("Only orgs with errors")
with f3:
    sort_col = st.selectbox("Sort by", ["Run Date", "Match Rate %", "IRS People", "Discovered", "Organization"])

filtered_df = display_df[display_df["Segment"].isin(seg_filter)] if seg_filter else display_df
if show_errors_only:
    filtered_df = filtered_df[filtered_df["Errors"] != ""]
filtered_df = filtered_df.sort_values(sort_col, ascending=(sort_col not in ["Match Rate %", "IRS People", "Discovered"]))

st.dataframe(
    filtered_df,
    use_container_width=True,
    height=min(80 + len(filtered_df) * 35, 500),
    column_config={
        "Match Rate %": st.column_config.ProgressColumn(
            "Match Rate %", min_value=0, max_value=100, format="%.1f%%"
        ),
    },
)

st.divider()

# ─── Section 2: Org selector ─────────────────────────────────────────────────
st.subheader("🔍 Org Drill-Down")

# Build unique selectbox labels: "Org Name  (Run Date)"
# Using index position ensures no collision even if org+date are identical
select_options = [
    f"{r['Organization']}  —  {r['_run_date']}"
    for r in all_rows
]
# De-duplicate labels if needed by appending a counter
seen: dict = {}
unique_options = []
for opt in select_options:
    if opt in seen:
        seen[opt] += 1
        unique_options.append(f"{opt}  #{seen[opt]}")
    else:
        seen[opt] = 1
        unique_options.append(opt)

selected_label = st.selectbox(
    "Select an organisation to view its full results",
    unique_options,
    help="Includes org name and the date it was run. Same org run twice will appear twice."
)

selected_idx    = unique_options.index(selected_label)
selected_row    = all_rows[selected_idx]
selected_sid    = selected_row["_session_id"]
selected_ein    = selected_row["_ein"]
selected_org    = selected_row["Organization"]
selected_date   = selected_row["_run_date"]

st.caption(f"Showing results for **{selected_org}** from run on **{selected_date}**")

st.divider()

# ─── Funder metrics ───────────────────────────────────────────────────────────
# Re-fetch the full funder row to get api_errors etc.
with st.spinner(f"Loading details for {selected_org}..."):
    try:
        funder_rows = sb.get_funder_results(selected_sid)
        funder_data = next(
            (f for f in funder_rows if f.get("ein") == selected_ein),
            None
        )
    except Exception as e:
        st.error(f"Failed to load funder data: {e}")
        st.stop()

if not funder_data:
    st.warning("Could not find saved data for this org.")
    st.stop()

raw_errors = funder_data.get("api_errors") or []
if isinstance(raw_errors, str):
    try:
        raw_errors = json.loads(raw_errors)
    except Exception:
        raw_errors = []

h1, h2, h3, h4, h5, h6 = st.columns(6)
h1.metric("IRS People",    funder_data.get("irs_people_count", 0))
h2.metric("✅ Matched",    funder_data.get("matched_count", 0))
h3.metric("🔄 Moved",      funder_data.get("moved_count", 0))
h4.metric("🔵 IRS Only",   funder_data.get("irs_only_count", 0))
h5.metric("🆕 Discovered", funder_data.get("discovered_count", 0))
h6.metric("⚠️ Errors",     len(raw_errors))

m1, m2, m3, m4 = st.columns(4)
m1.caption(f"**EIN:** {selected_ein}")
m2.caption(f"**City/State:** {funder_data.get('city', '')} {funder_data.get('state', '')}")
m3.caption(f"**Segment:** {funder_data.get('segment', '')}")
m4.caption(f"**Processing time:** {funder_data.get('processing_ms', 0):,} ms")

st.divider()

# ─── Staff profiles ───────────────────────────────────────────────────────────
with st.spinner(f"Loading staff profiles..."):
    try:
        staff_profiles = sb.get_staff_profiles(selected_sid, selected_ein)
    except Exception as e:
        st.error(f"Failed to load staff profiles: {e}")
        staff_profiles = []

if staff_profiles:
    st.subheader(f"👥 Staff Profiles ({len(staff_profiles)})")

    status_opts = sorted(set(p.get("status", "IRS_ONLY") for p in staff_profiles))
    status_filter = st.multiselect(
        "Filter by status", status_opts, default=status_opts,
        format_func=lambda s: f"{STATUS_COLORS.get(s, '⚪')} {STATUS_LABELS.get(s, s)}"
    )

    filtered_staff = [p for p in staff_profiles if p.get("status") in status_filter]

    for person in sorted(filtered_staff, key=lambda x: x.get("status", "")):
        status    = person.get("status", "IRS_ONLY")
        icon      = STATUS_COLORS.get(status, "⚪")
        label     = STATUS_LABELS.get(status, status)
        name      = person.get("person_name") or person.get("irs_name") or "Unknown"
        irs_title = person.get("irs_title") or ""
        cur_title = person.get("current_title") or ""
        company   = person.get("current_company") or ""
        linkedin  = person.get("linkedin_url") or ""
        score     = person.get("match_score") or 0
        enriched  = person.get("enriched", False)
        grant_rel = person.get("is_grant_relevant", False)
        near_miss = person.get("near_miss", False)

        with st.container():
            col_a, col_b, col_c, col_d = st.columns([3, 3, 2, 2])
            with col_a:
                st.markdown(f"**{icon} {name}**")
                if irs_title:
                    st.caption(f"IRS: {irs_title}")
            with col_b:
                if cur_title:
                    st.markdown(f"*{cur_title}*")
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
                if near_miss:
                    st.caption("⚡ Near miss")
            with col_d:
                if linkedin:
                    st.markdown(f"[LinkedIn Profile]({linkedin})")
                else:
                    st.caption("No LinkedIn found")
        st.markdown("---")

    # CSV export
    export_cols = [
        "person_name", "irs_name", "irs_title", "current_title",
        "current_company", "status", "match_score", "linkedin_url",
        "enriched", "is_grant_relevant", "near_miss", "source",
    ]
    staff_export = pd.DataFrame([{c: p.get(c, "") for c in export_cols} for p in staff_profiles])
    st.download_button(
        f"📥 Download {selected_org[:30]}_staff.csv",
        data=staff_export.to_csv(index=False),
        file_name=f"{selected_ein}_staff.csv",
        mime="text/csv",
    )

else:
    st.info("No staff profiles saved for this organisation.")

# ─── API errors ───────────────────────────────────────────────────────────────
if raw_errors:
    with st.expander(f"⚠️ API Errors ({len(raw_errors)})", expanded=False):
        for err in raw_errors:
            st.error(f"**{err.get('step', 'unknown')}**: {err.get('error', '')}")

# ─── Query detail ─────────────────────────────────────────────────────────────
with st.expander("🔍 API Query Detail", expanded=False):
    st.markdown(f"""
- Serper queries run: **{funder_data.get('serper_queries_run', 0)}**
- LinkedIn URLs found: **{funder_data.get('serper_urls_found', 0)}**
- PDL profiles found: **{funder_data.get('pdl_profiles_found', 0)}**
- PDL enrichments done: **{funder_data.get('enrichments_done', 0)}**
- Past IRS people: **{funder_data.get('past_people_count', 0)}**
- Past detected as moved: **{funder_data.get('past_detected_as_moved', 0)}**
    """)
