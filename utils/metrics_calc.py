"""
metrics_calc.py
Calculates the 8 experiment metrics from Section 5.6 of the spec.
Works on both live session data and exported DataFrames.
"""
from typing import List, Dict, Any, Optional
import pandas as pd


# ── Go/No-Go thresholds ──────────────────────────────────────────────────────
THRESHOLDS = {
    "irs_match_rate":          {"go": 60, "conditional": 40},
    "confirmation_rate":       {"go": 70},
    "new_discovery_rate":      {"signal": 20},
    "grant_relevant_rate":     {"signal": 10},
    "false_positive_rate":     {"ok": 15, "conditional": 25},
    "stale_detection_rate":    {"signal": 50},
    "cost_per_funder":         {"ok": 0.50, "max": 2.00},
}


def compute_metrics(funder_results: List[Dict]) -> Dict:
    """
    Compute all 8 experiment metrics from a list of funder_results dicts.

    Each dict must have (at minimum):
      matched_count, moved_count, irs_only_count, discovered_count,
      irs_people_count, grant_relevant_count, past_people_count,
      past_detected_as_moved, enrichments_done
    """
    if not funder_results:
        return _empty_metrics()

    total_irs_people = sum(r.get("irs_people_count", 0) for r in funder_results)
    total_matched = sum(r.get("matched_count", 0) for r in funder_results)
    total_moved = sum(r.get("moved_count", 0) for r in funder_results)
    total_irs_only = sum(r.get("irs_only_count", 0) for r in funder_results)
    total_discovered = sum(r.get("discovered_count", 0) for r in funder_results)
    total_unique_found = total_matched + total_moved + total_discovered
    total_enrichments = sum(r.get("enrichments_done", 0) for r in funder_results)
    total_grant_relevant = sum(r.get("grant_relevant_count", 0) for r in funder_results)
    total_past = sum(r.get("past_people_count", 0) for r in funder_results)
    total_past_moved = sum(r.get("past_detected_as_moved", 0) for r in funder_results)

    # Metric 1: IRS Person Match Rate
    irs_match_rate = (
        ((total_matched + total_moved) / total_irs_people * 100)
        if total_irs_people > 0 else 0
    )

    # Metric 2: Current Employee Confirmation Rate
    confirmation_rate = (
        (total_matched / (total_matched + total_moved) * 100)
        if (total_matched + total_moved) > 0 else 0
    )

    # Metric 3: New Staff Discovery Rate
    new_discovery_rate = (
        (total_discovered / total_unique_found * 100)
        if total_unique_found > 0 else 0
    )

    # Metric 4: Grant-Relevant Staff Found
    grant_relevant_rate = (
        (total_grant_relevant / total_discovered * 100)
        if total_discovered > 0 else 0
    )

    # Metric 5: False Positive Rate
    # Enriched profiles where company doesn't match = MOVED
    # Approximation: MOVED / (MATCHED + MOVED + DISCOVERED enriched)
    false_positive_rate = (
        (total_moved / total_enrichments * 100)
        if total_enrichments > 0 else 0
    )

    # Metric 6: Stale IRS Detection Rate
    stale_detection_rate = (
        (total_past_moved / total_past * 100)
        if total_past > 0 else 0
    )

    # Metric 7: Cost per Funder (SerpApi only — PDL search is free)
    # SerpApi: $0.015/query × ~7 queries avg = ~$0.105/funder (Developer plan)
    total_queries = sum(r.get("serper_queries_run", 0) for r in funder_results)
    total_serper_cost = total_queries * 0.015
    cost_per_funder = (
        total_serper_cost / len(funder_results)
        if funder_results else 0
    )

    # Metric 8: Segment Coverage Variance
    segment_metrics = _segment_breakdown(funder_results)

    # Go/No-Go decision
    decision = _go_nogo(irs_match_rate, false_positive_rate, cost_per_funder)

    return {
        "irs_match_rate": round(irs_match_rate, 1),
        "confirmation_rate": round(confirmation_rate, 1),
        "new_discovery_rate": round(new_discovery_rate, 1),
        "grant_relevant_rate": round(grant_relevant_rate, 1),
        "false_positive_rate": round(false_positive_rate, 1),
        "stale_detection_rate": round(stale_detection_rate, 1),
        "cost_per_funder": round(cost_per_funder, 4),
        "total_serper_cost": round(total_serper_cost, 3),
        "total_api_cost": round(total_serper_cost, 3),  # PDL search is free
        "segment_breakdown": segment_metrics,
        "totals": {
            "funders": len(funder_results),
            "irs_people": total_irs_people,
            "matched": total_matched,
            "moved": total_moved,
            "irs_only": total_irs_only,
            "discovered": total_discovered,
            "grant_relevant": total_grant_relevant,
            "enrichments": total_enrichments,
            "past_people": total_past,
            "past_detected": total_past_moved,
            "total_queries": total_queries,
        },
        "decision": decision,
    }


def _segment_breakdown(funder_results: List[Dict]) -> Dict:
    """Match rate by asset segment (large/mid/small/unknown)."""
    segments = {}
    for segment in ("large", "mid", "small", "unknown"):
        seg_results = [r for r in funder_results if r.get("segment") == segment]
        if not seg_results:
            continue
        irs = sum(r.get("irs_people_count", 0) for r in seg_results)
        found = sum((r.get("matched_count", 0) + r.get("moved_count", 0)) for r in seg_results)
        rate = round((found / irs * 100) if irs > 0 else 0, 1)
        segments[segment] = {
            "count": len(seg_results),
            "irs_people": irs,
            "found": found,
            "match_rate": rate,
        }
    return segments


def _go_nogo(irs_match_rate: float, false_positive_rate: float, cost_per_funder: float) -> Dict:
    """Return a Go/No-Go decision with rationale."""
    t = THRESHOLDS

    if (irs_match_rate >= t["irs_match_rate"]["go"]
            and false_positive_rate < t["false_positive_rate"]["ok"]
            and cost_per_funder <= t["cost_per_funder"]["ok"]):
        return {
            "decision": "GO",
            "color": "green",
            "label": "✅ GO",
            "rationale": (
                f"Match rate {irs_match_rate:.1f}% ≥ 60%, "
                f"false positive {false_positive_rate:.1f}% < 15%, "
                f"cost ${cost_per_funder:.4f}/funder ≤ $0.50."
            ),
        }
    elif (t["irs_match_rate"]["conditional"] <= irs_match_rate < t["irs_match_rate"]["go"]
          or t["false_positive_rate"]["ok"] <= false_positive_rate < t["false_positive_rate"]["conditional"]):
        return {
            "decision": "CONDITIONAL",
            "color": "orange",
            "label": "⚠️ CONDITIONAL GO",
            "rationale": (
                f"Match rate {irs_match_rate:.1f}% is in conditional range (40–59%) "
                f"or false positives {false_positive_rate:.1f}% are elevated (15–25%). "
                "Run 1-week refinement sprint before full implementation."
            ),
        }
    else:
        return {
            "decision": "NO_GO",
            "color": "red",
            "label": "🚫 NO-GO",
            "rationale": (
                f"Match rate {irs_match_rate:.1f}% < 40% or "
                f"false positive rate {false_positive_rate:.1f}% > 25% or "
                f"cost ${cost_per_funder:.4f}/funder > $2.00. "
                "Pivot to alternative provider (People Data Labs direct or Crustdata)."
            ),
        }


def _empty_metrics() -> Dict:
    return {
        "irs_match_rate": 0, "confirmation_rate": 0,
        "new_discovery_rate": 0, "grant_relevant_rate": 0,
        "false_positive_rate": 0, "stale_detection_rate": 0,
        "cost_per_funder": 0, "total_serper_cost": 0, "total_api_cost": 0,
        "segment_breakdown": {},
        "totals": {k: 0 for k in ("funders", "irs_people", "matched", "moved",
                                   "irs_only", "discovered", "grant_relevant",
                                   "enrichments", "past_people", "past_detected", "total_queries")},
        "decision": {"decision": "NO_DATA", "color": "gray",
                     "label": "⏳ No Data", "rationale": "Run the experiment first."},
    }


def funder_results_to_df(funder_results: List[Dict]) -> pd.DataFrame:
    if not funder_results:
        return pd.DataFrame()
    return pd.DataFrame(funder_results)


def staff_profiles_to_df(staff_profiles: List[Dict]) -> pd.DataFrame:
    if not staff_profiles:
        return pd.DataFrame()
    return pd.DataFrame(staff_profiles)
