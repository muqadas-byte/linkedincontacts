"""
pages/5_⚠️_Edge_Cases.py
Edge case documentation, failure analysis, and experiment recommendation.
Implements Task 7 from Section 5.3 of the spec.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import pandas as pd
import json

from utils.metrics_calc import compute_metrics

st.set_page_config(page_title="Edge Cases", page_icon="⚠️", layout="wide")
st.title("⚠️ Edge Cases & Failure Analysis")
st.caption("Task 7: Document failures, categorize them, propose mitigations")

# ─── Data ─────────────────────────────────────────────────────────────────────
results = st.session_state.get("experiment_results", {})
if not results:
    st.info("No results yet. Run the experiment first.")
    st.stop()

funder_stats = list(results.values())
metrics = compute_metrics(funder_stats)

# ─── Failure categorization ───────────────────────────────────────────────────
failures = {
    "no_results":         [],  # Org not found at all on Google
    "zero_match":         [],  # Org found but 0% IRS match rate
    "high_false_positive": [], # Many MOVED vs expected
    "pdl_auth_error":     [],
    "serper_auth_error":  [],
    "pdl_credits_exhausted": [],
    "connection_errors":  [],
    "no_leadership":      [],  # Funders with 0 IRS people to match
    "common_name_risk":   [],  # Org names likely to have false positives
}

COMMON_NAME_FRAGMENTS = [
    "united way", "community foundation", "family foundation",
    "health foundation", "education foundation", "arts council",
]

for r in funder_stats:
    ein = r.get("ein")
    org = r.get("org_name", "")
    errors = r.get("api_errors") or []
    irs_count = r.get("irs_people_count", 0)
    serper_urls = r.get("serper_urls_found", 0)
    matched = r.get("matched_count", 0)
    moved = r.get("moved_count", 0)

    # No leadership in IRS
    if irs_count == 0:
        failures["no_leadership"].append({"ein": ein, "org": org})

    # No URLs found at all
    if serper_urls == 0 and irs_count > 0:
        failures["no_results"].append({"ein": ein, "org": org, "irs_people": irs_count})

    # Zero match rate despite having IRS people
    if irs_count > 0 and matched == 0 and moved == 0 and serper_urls > 0:
        failures["zero_match"].append({
            "ein": ein, "org": org,
            "irs_people": irs_count, "urls_found": serper_urls,
        })

    # Many moved (potential false positives)
    if moved > 0 and (matched + moved) > 0:
        fp_rate = moved / (matched + moved)
        if fp_rate > 0.5 and (matched + moved) >= 2:
            failures["high_false_positive"].append({
                "ein": ein, "org": org, "moved": moved, "matched": matched,
            })

    # API-specific errors
    for err in errors:
        err_str = err.get("error", "")
        step = err.get("step", "")
        if "AUTH_ERROR" in err_str and step == "apollo_enrich":
            failures["pdl_auth_error"].append({"ein": ein, "org": org, "error": err_str})
        elif "AUTH_ERROR" in err_str and step == "serper":
            failures["serper_auth_error"].append({"ein": ein, "org": org, "error": err_str})
        elif "CREDITS_EXHAUSTED" in err_str:
            failures["pdl_credits_exhausted"].append({"ein": ein, "org": org})
        elif "CONNECTION_ERROR" in err_str or "TIMEOUT" in err_str:
            failures["connection_errors"].append({"ein": ein, "org": org, "error": err_str})

    # Common name risk
    org_lower = org.lower()
    if any(frag in org_lower for frag in COMMON_NAME_FRAGMENTS):
        failures["common_name_risk"].append({"ein": ein, "org": org})

# ─── Display ─────────────────────────────────────────────────────────────────
total_failures = sum(len(v) for v in failures.values())
st.metric("Total Edge Cases Identified", total_failures)
st.divider()

# API Authentication Errors (most critical)
if failures["serper_auth_error"]:
    st.subheader("🔴 Serper Authentication Errors")
    st.error(
        f"{len(failures['serper_auth_error'])} auth failures. "
        "This stops discovery entirely — verify your Serper API key."
    )
    st.markdown("**Mitigation:** Check API key is correct and account is active at serper.dev/dashboard")

if failures["pdl_auth_error"]:
    st.subheader("🔴 PDL Authentication Errors")
    st.error(f"{len(failures['pdl_auth_error'])} PDL auth failures.")
    st.markdown("**Mitigation:** Verify PDL API key at peopledatalabs.com. Enrichment auth errors don't affect discovery.")

if failures["pdl_credits_exhausted"]:
    st.subheader("🟡 PDL Credits Exhausted")
    st.warning(
        f"Credits ran out after {len(failures['pdl_credits_exhausted'])} enrichment calls. "
        f"{len(funder_stats) - len(failures['pdl_credits_exhausted'])} funders had no enrichment."
    )
    st.markdown("**Mitigation:** Upgrade PDL plan or reduce `Max Enrichment Credits` per run. "
                "Discovery (Serper + PDL Search) still works without credits.")

if failures["connection_errors"]:
    st.subheader("🟡 Connection / Timeout Errors")
    st.warning(f"{len(failures['connection_errors'])} connection or timeout errors.")
    with st.expander("See affected funders"):
        st.dataframe(pd.DataFrame(failures["connection_errors"]), use_container_width=True)
    st.markdown("**Mitigation:** Add retry logic with exponential backoff. Check your network connection.")

st.divider()

# Discovery Failures
if failures["no_leadership"]:
    st.subheader("🔵 No IRS Leadership Data")
    st.info(f"{len(failures['no_leadership'])} funders have no IRS keyPeople to match against.")
    with st.expander("See funders"):
        st.dataframe(pd.DataFrame(failures["no_leadership"]), use_container_width=True)
    st.markdown("**Mitigation:** These are discovery-only funders. Any PDL/Serper finds count as DISCOVERED.")

if failures["no_results"]:
    st.subheader("🔵 No LinkedIn Results Found")
    st.info(f"{len(failures['no_results'])} funders returned zero LinkedIn URLs from Serper.")
    with st.expander("See affected funders"):
        st.dataframe(pd.DataFrame(failures["no_results"]), use_container_width=True)
    st.markdown("""
    **Possible causes:**
    - Small orgs with no LinkedIn presence
    - Very local/rural organizations  
    - Unusual organization names not in Google's index

    **Mitigation:**
    - Try adding EIN to disambiguation queries
    - Fall back to org website scraping for staff pages
    - Accept these as IRS_ONLY — label badge: *"From IRS Filing"*
    """)

if failures["zero_match"]:
    st.subheader("🟡 URLs Found But Zero IRS Matches")
    st.warning(
        f"{len(failures['zero_match'])} funders had LinkedIn URLs in SERP results "
        "but couldn't match any to IRS people."
    )
    with st.expander("See affected funders"):
        st.dataframe(pd.DataFrame(failures["zero_match"]), use_container_width=True)
    st.markdown("""
    **Possible causes:**
    - Common org names returning unrelated profiles
    - IRS people have very common names (John Smith)
    - Name formatting differences (ALL CAPS in IRS vs mixed case on LinkedIn)
    - People changed names (marriage/divorce)
    
    **Mitigation:**
    - Lower match threshold to 80% for these specific cases
    - Add location-based disambiguation
    - Try inverse queries: search person name + EIN
    """)

if failures["high_false_positive"]:
    st.subheader("🟡 High False Positive Risk")
    st.warning(
        f"{len(failures['high_false_positive'])} funders have >50% of matched profiles "
        "categorized as MOVED (potential false positives)."
    )
    with st.expander("See affected funders"):
        st.dataframe(pd.DataFrame(failures["high_false_positive"]), use_container_width=True)
    st.markdown("""
    **Mitigation:**
    - Use more specific query types (person + org name) rather than broad search
    - Require enrichment confirmation before showing as MATCHED
    - Raise fuzzy match threshold to 90% for common-name orgs
    """)

if failures["common_name_risk"]:
    st.subheader("ℹ️ Common Name Organizations")
    st.info(
        f"{len(failures['common_name_risk'])} funders have generic names "
        "(e.g. 'United Way', 'Community Foundation') that may produce false positives."
    )
    with st.expander("See affected funders"):
        st.dataframe(pd.DataFrame(failures["common_name_risk"]), use_container_width=True)
    st.markdown("""
    **Mitigation:**
    - Always include city/state in queries for these orgs
    - Use website domain query (Query E) as primary signal
    - Require match_score ≥ 90% for common-name orgs
    """)

st.divider()

# ─── Final Recommendation ─────────────────────────────────────────────────────
st.subheader("📝 Experiment Recommendation")

decision = metrics["decision"]
ir = metrics["irs_match_rate"]
fp = metrics["false_positive_rate"]
cost = metrics["cost_per_funder"]

if decision["decision"] == "GO":
    st.success(f"""
**Recommendation: GO → Proceed to full implementation**

The experiment meets all go criteria:
- IRS match rate: **{ir:.1f}%** (threshold: ≥60%)
- False positive rate: **{fp:.1f}%** (threshold: <15%)
- Cost per funder: **${cost:.4f}** (threshold: <$0.50)

**Next steps:**
1. Proceed to Sections 6–10 of the implementation spec
2. Build the Funder Pages UI with the three-layer hybrid system
3. Set up 30-day cache with on-demand re-enrichment
4. Plan v2: user-submitted corrections, batch processing for high-traffic funders
    """)
elif decision["decision"] == "CONDITIONAL":
    st.warning(f"""
**Recommendation: CONDITIONAL GO → 1-week refinement sprint**

Results are in the conditional range:
- IRS match rate: **{ir:.1f}%** (target: ≥60%, current threshold: 40–59%)
- False positive rate: **{fp:.1f}%** (target: <15%)

**Refinement steps:**
1. Tune fuzzy match threshold — try 80% instead of {st.session_state.get('match_threshold', 85)}%
2. Add EIN-based disambiguation for funders with 0 match
3. Improve Query D role keywords for grant-relevant discovery
4. Re-test on failing segments only (saves API cost)
5. Re-evaluate after refinement sprint
    """)
else:
    st.error(f"""
**Recommendation: NO-GO → Pivot to alternative provider**

Current approach does not meet minimum thresholds:
- IRS match rate: **{ir:.1f}%** (threshold: ≥40%)
- False positive rate: **{fp:.1f}%** (threshold: <25%)
- Cost per funder: **${cost:.4f}** (threshold: <$2.00)

**Pivot options:**
1. **People Data Labs direct** — try PDL Person Search as primary discovery (not just enrichment)
2. **Crustdata** — YC-backed, weekly refresh, from ~$95/mo
3. **Hybrid fallback** — org website scraping for staff pages (no API cost)
    """)

# ─── Export edge cases ────────────────────────────────────────────────────────
export_rows = []
for category, items in failures.items():
    for item in items:
        export_rows.append({"category": category, **item})

if export_rows:
    csv = pd.DataFrame(export_rows).to_csv(index=False)
    st.download_button(
        "📥 Download edge_cases.csv",
        data=csv,
        file_name="edge_cases.csv",
        mime="text/csv",
    )
