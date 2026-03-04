"""
pages/2_🔬_Run_Experiment.py
Core experiment execution pipeline with live progress tracking.
Implements all tasks from Section 5.3 of the spec.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import time
import streamlit as st
import pandas as pd
from datetime import datetime

from utils.serper_client import run_discovery, SerperAuthError
from utils.apollo_client import search_people_by_company, match_person, enrich_person
from utils.matching import merge_staff_for_funder, is_past_role
from utils.metrics_calc import compute_metrics

st.set_page_config(page_title="Run Experiment", page_icon="🔬", layout="wide")
st.title("🔬 Run Experiment")
st.caption("Execute the full discovery + enrichment + matching pipeline")

# ─── Prerequisites check ──────────────────────────────────────────────────────
errors = []
if not st.session_state.get("funders_loaded"):
    errors.append("No funders loaded — upload 100randomFunders.json on the Home page")
if not st.session_state.get("serpapi_key"):
    errors.append("SerpApi key missing — configure on Home page")
if not st.session_state.get("apollo_match_key"):
    errors.append("Apollo Match key missing — configure on Home page")

if errors:
    for e in errors:
        st.error(f"⛔ {e}")
    st.stop()

funders = st.session_state["funders"]
serpapi_key = st.session_state["serpapi_key"]
apollo_search_key = st.session_state.get("apollo_search_key", "")
apollo_match_key = st.session_state["apollo_match_key"]
match_threshold = st.session_state.get("match_threshold", 85)
enrich_enabled = st.session_state.get("enrich_enabled", True)
max_funders = st.session_state.get("max_funders", 100)
enrich_budget = st.session_state.get("enrich_budget", 100)

# ─── Current settings summary ─────────────────────────────────────────────────
with st.expander("⚙️ Current Settings", expanded=False):
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Funders to Process", min(max_funders, len(funders)))
    s2.metric("Match Threshold", f"{match_threshold}%")
    s3.metric("Enrichment", "Enabled" if enrich_enabled else "Disabled")
    s4.metric("Enrichment Budget", enrich_budget if enrich_enabled else "—")

# ─── Resume or start ─────────────────────────────────────────────────────────
already_done = st.session_state.get("experiment_done", False)
already_running = st.session_state.get("experiment_running", False)
results_so_far = st.session_state.get("experiment_results", {})

if already_done and results_so_far:
    st.success(f"Experiment completed — {len(results_so_far)} funders processed.")
    if st.button("🔄 Re-run Experiment (clears previous results)"):
        st.session_state["experiment_results"] = {}
        st.session_state["experiment_done"] = False
        st.session_state["active_session_id"] = None
        st.rerun()
    st.info("Go to **📋 Results** or **📈 Metrics** to explore the results.")
    st.stop()

# ─── Funder selection ────────────────────────────────────────────────────────
st.subheader("🎯 Funder Selection")
selection_mode = st.radio(
    "How do you want to select funders?",
    ["Run first N funders", "Pick specific funders by name"],
    horizontal=True,
)
if selection_mode == "Pick specific funders by name":
    all_names = [f["org_name"] for f in funders]
    selected_names = st.multiselect(
        "Search and select funders to run",
        options=all_names,
        placeholder="Type a foundation name...",
    )
    funders_to_run = [f for f in funders if f["org_name"] in selected_names]
    if not funders_to_run:
        st.info("Select at least one funder above to continue.")
        st.stop()
else:
    funders_to_run = funders[:max_funders]

# ─── Pre-flight summary ───────────────────────────────────────────────────────
total_with_leadership = sum(1 for f in funders_to_run if f.get("leadership"))
total_queries_estimate = len(funders_to_run) * 7
cost_estimate = total_queries_estimate * 0.001

st.subheader("📋 Pre-flight Summary")
pf1, pf2, pf3, pf4 = st.columns(4)
pf1.metric("Funders", len(funders_to_run))
pf2.metric("With Leadership", total_with_leadership)
pf3.metric("Est. SerpApi Queries", f"~{total_queries_estimate:,}")
pf4.metric("Est. Cost", f"~${cost_estimate:.2f}")

st.divider()

# ─── Start button ─────────────────────────────────────────────────────────────
if st.button("🚀 Start Experiment", type="primary", use_container_width=True,
             disabled=already_running):
    st.session_state["experiment_running"] = True
    st.session_state["experiment_done"] = False
    st.session_state["experiment_results"] = {}

    # Create Supabase session if connected
    session_id = None
    sb = st.session_state.get("supabase_client")
    if sb and st.session_state.get("supabase_ok"):
        try:
            session_id = sb.create_session(
                total_funders=len(funders_to_run),
                match_threshold=match_threshold,
                enrich_enabled=enrich_enabled,
                notes=f"Experiment run at {datetime.utcnow().isoformat()}",
            )
            st.session_state["active_session_id"] = session_id
        except Exception as e:
            st.warning(f"Could not create Supabase session: {e} — continuing without persistence")

    # ── Pipeline ───────────────────────────────────────────────────────────────
    overall_start = time.time()
    credits_used = 0
    all_funder_stats = []

    progress_bar = st.progress(0, text="Starting experiment...")
    status_placeholder = st.empty()
    live_table_placeholder = st.empty()
    error_log_placeholder = st.empty()

    serper_auth_failed = False

    for idx, funder in enumerate(funders_to_run):
        ein = funder["ein"]
        org_name = funder["org_name"]
        city = funder["city"]
        state = funder["state"]
        website_domain = funder["website_domain"]
        leadership = funder.get("leadership") or []

        progress = (idx + 1) / len(funders_to_run)
        progress_bar.progress(progress, text=f"[{idx+1}/{len(funders_to_run)}] {org_name[:50]}...")

        funder_start = time.time()
        api_errors = []

        # ── Task 2: Serper Discovery ───────────────────────────────────────
        serper_result = {"profiles": [], "queries_run": 0, "queries_detail": [], "error": None}

        if not serper_auth_failed:
            with status_placeholder.container():
                st.caption(f"🔍 [{idx+1}/{len(funders_to_run)}] SerpApi discovery: {org_name}")
            try:
                serper_result = run_discovery(
                    api_key=serpapi_key,
                    org_name=org_name,
                    city=city,
                    state=state,
                    website_domain=website_domain,
                    leadership=leadership,
                )
                if serper_result.get("error"):
                    err = serper_result["error"]
                    api_errors.append({"step": "serper", "error": err})
                    if "AUTH_ERROR" in err:
                        serper_auth_failed = True
                        error_log_placeholder.error(
                            f"⛔ SerpApi authentication failed — check your API key. Stopping discovery for remaining funders."
                        )
            except Exception as e:
                api_errors.append({"step": "serper", "error": f"UNEXPECTED: {str(e)}"})

        # ── Task 3: Apollo People Search ──────────────────────────────────────
        # Uses the dedicated search key on api/v1/mixed_people/api_search.
        # Finds additional staff Apollo knows about (supplements SerpApi).
        with status_placeholder.container():
            st.caption(f"👥 [{idx+1}/{len(funders_to_run)}] Apollo search: {org_name}")

        pdl_search_result = {"profiles": [], "total_found": 0, "error": None}
        if apollo_search_key:
            try:
                pdl_search_result = search_people_by_company(
                    search_key=apollo_search_key,
                    org_name=org_name,
                    website_domain=website_domain,
                    size=10,
                )
                if pdl_search_result.get("error"):
                    api_errors.append({"step": "apollo_search", "error": pdl_search_result["error"]})
            except Exception as e:
                api_errors.append({"step": "apollo_search", "error": f"UNEXPECTED: {str(e)}"})

        # ── Task 4: Apollo Match → Enrich ─────────────────────────────────────
        # Step 6a: People Match  — LinkedIn URL → apollo_person_id (match key)
        # Step 6b: People Enrich — apollo_person_id  → full profile  (match key)
        enrichment_results = {}
        enrichments_done = 0

        if enrich_enabled and credits_used < enrich_budget:
            # Collect unique LinkedIn URLs from both discovery sources
            enrich_candidates = []
            seen_urls = set()

            # Priority: SerpApi finds first (query-type C/D/E ranked higher),
            # then Apollo Search supplements.
            all_discovered = serper_result.get("profiles", []) + pdl_search_result.get("profiles", [])
            for p in all_discovered:
                url = p.get("linkedin_url") or ""
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    enrich_candidates.append(url)

            # Cap at 2 per funder to conserve credits
            for url in enrich_candidates[:2]:
                if credits_used >= enrich_budget:
                    break

                # Step 6a — People Match
                with status_placeholder.container():
                    st.caption(f"🔗 [{idx+1}/{len(funders_to_run)}] Matching: {url[-40:]}")

                match_res = match_person(apollo_match_key, url)
                if match_res.get("error"):
                    err_msg = match_res["error"]
                    api_errors.append({"step": "apollo_match", "url": url, "error": err_msg})
                    if "AUTH_ERROR" in err_msg or "CREDITS_EXHAUSTED" in err_msg:
                        break
                    continue  # skip enrichment if match failed

                if not match_res.get("found"):
                    continue  # no match — nothing to enrich

                apollo_id  = match_res.get("apollo_person_id", "")
                matched_url = match_res.get("linkedin_url") or url

                # Step 6b — People Enrichment
                with status_placeholder.container():
                    st.caption(f"⚡ [{idx+1}/{len(funders_to_run)}] Enriching: {matched_url[-40:]}")

                enrich_res = enrich_person(
                    apollo_match_key,
                    apollo_person_id=apollo_id,
                    linkedin_url=matched_url,
                )

                if enrich_res.get("error"):
                    err_msg = enrich_res["error"]
                    api_errors.append({"step": "apollo_enrich", "url": matched_url, "error": err_msg})
                    if "CREDITS_EXHAUSTED" in err_msg or "AUTH_ERROR" in err_msg:
                        break

                if enrich_res.get("found") and enrich_res.get("profile"):
                    # Store under the original SerpApi URL so matching.py lookup works
                    enrichment_results[url] = enrich_res["profile"]
                    enrichments_done += 1
                    credits_used += 1

                # Low-credits warning
                remaining = enrich_res.get("credits_remaining")
                if remaining is not None and remaining < 10:
                    st.warning(f"⚠️ Apollo enrichment credits low: {remaining} remaining")

        # ── Task 5: Cross-Reference & Matching ────────────────────────────
        merged_staff, match_stats = merge_staff_for_funder(
            org_name=org_name,
            irs_leadership=leadership,
            serper_profiles=serper_result.get("profiles", []),
            pdl_search_profiles=pdl_search_result.get("profiles", []),
            enrichment_results=enrichment_results,
            match_threshold=match_threshold,
        )

        # Count past people
        past_count = sum(1 for p in leadership if is_past_role(p.get("title") or ""))
        past_moved = sum(
            1 for m in merged_staff
            if m.get("status") == "MOVED"
            and any(is_past_role(p.get("title") or "") and p.get("name") == m.get("irs_name")
                    for p in leadership)
        )

        processing_ms = int((time.time() - funder_start) * 1000)

        funder_stat = {
            "ein": ein,
            "org_name": org_name,
            "segment": funder.get("segment"),
            "city": city,
            "state": state,
            "irs_people_count": len(leadership),
            "matched_count": match_stats.get("MATCHED", 0),
            "moved_count": match_stats.get("MOVED", 0),
            "irs_only_count": match_stats.get("IRS_ONLY", 0),
            "discovered_count": match_stats.get("DISCOVERED", 0),
            "grant_relevant_count": match_stats.get("grant_relevant_discovered", 0),
            "serper_queries_run": serper_result.get("queries_run", 0),
            "serper_urls_found": len(serper_result.get("profiles", [])),
            "pdl_profiles_found": len(pdl_search_result.get("profiles", [])),
            "enrichments_done": enrichments_done,
            "past_people_count": past_count,
            "past_detected_as_moved": past_moved,
            "api_errors": api_errors,
            "merged_staff": merged_staff,
            "processing_ms": processing_ms,
        }

        st.session_state["experiment_results"][ein] = funder_stat
        all_funder_stats.append(funder_stat)

        # ── Persist to Supabase ────────────────────────────────────────────
        if session_id and sb:
            try:
                sb.upsert_funder_result(session_id, ein, {
                    k: v for k, v in funder_stat.items() if k != "merged_staff"
                })
                sb.insert_staff_profiles(session_id, ein, org_name, merged_staff)
            except Exception as e:
                err_msg = str(e)
                api_errors.append({"step": "supabase_write", "error": err_msg})
                st.warning(f"⚠️ Supabase write failed for {org_name}: {err_msg[:120]}")

        # ── Live results table update ──────────────────────────────────────
        if all_funder_stats:
            preview_df = pd.DataFrame([
                {
                    "Org": r["org_name"][:35],
                    "Segment": r["segment"],
                    "IRS": r["irs_people_count"],
                    "Matched": r["matched_count"],
                    "Moved": r["moved_count"],
                    "Discovered": r["discovered_count"],
                    "Errors": len(r["api_errors"]),
                }
                for r in all_funder_stats[-15:]  # Show last 15
            ])
            live_table_placeholder.dataframe(preview_df, use_container_width=True, height=350)

    # ── Experiment complete ────────────────────────────────────────────────
    progress_bar.progress(1.0, text="✅ Experiment complete!")
    st.session_state["experiment_running"] = False
    st.session_state["experiment_done"] = True

    if session_id and sb:
        try:
            sb.complete_session(session_id, len(all_funder_stats))
        except Exception:
            pass

    total_elapsed = time.time() - overall_start
    status_placeholder.empty()

    # Final summary
    metrics = compute_metrics(all_funder_stats)
    st.divider()
    st.subheader("🏁 Experiment Complete")

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Funders Processed", len(all_funder_stats))
    m2.metric("IRS Match Rate", f"{metrics['irs_match_rate']:.1f}%")
    m3.metric("New Discovered", metrics["totals"]["discovered"])
    m4.metric("SerpApi Cost", f"${metrics['total_serper_cost']:.3f}")
    m5.metric("Time", f"{total_elapsed:.0f}s")

    decision = metrics["decision"]
    if decision["decision"] == "GO":
        st.success(f"**{decision['label']}** — {decision['rationale']}")
    elif decision["decision"] == "CONDITIONAL":
        st.warning(f"**{decision['label']}** — {decision['rationale']}")
    else:
        st.error(f"**{decision['label']}** — {decision['rationale']}")

    st.info("Go to **📋 Results** to explore per-funder details or **📈 Metrics** for the full dashboard.")
