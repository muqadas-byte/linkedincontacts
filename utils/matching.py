"""
matching.py
Fuzzy name matching and IRS cross-reference logic.
Uses RapidFuzz (Jaro-Winkler) for name similarity.

Match categories (per experiment spec Section 5.3 Task 5):
  MATCHED   — IRS person found on LinkedIn, enrichment confirms same org
  MOVED     — IRS person found on LinkedIn, now at different org
  IRS_ONLY  — IRS person not found on LinkedIn
  DISCOVERED — LinkedIn person NOT in IRS data (new discovery)
"""
from typing import List, Dict, Optional, Tuple
import re

from rapidfuzz import fuzz, process

# Match thresholds (configurable)
DEFAULT_MATCH_THRESHOLD = 85    # % similarity to count as a match
NEAR_MISS_THRESHOLD = 70        # % similarity to log as "near miss"

# Keywords that identify grant-relevant roles
GRANT_RELEVANT_KEYWORDS = [
    "program officer", "grants manager", "program director",
    "program manager", "grants officer", "grant manager",
    "grant officer", "foundation officer", "program associate",
    "grants associate", "program staff", "giving officer",
    "philanthropy officer",
]

# Keywords that indicate a "past" role in IRS data
PAST_KEYWORDS = ["past ", "former ", "ex-"]


def normalize_name(name: str) -> str:
    """Lowercase, strip punctuation, collapse spaces."""
    name = name.lower().strip()
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name)
    return name


def is_past_role(title: str) -> bool:
    """Return True if IRS title indicates a former employee."""
    t = title.lower().strip()
    return any(t.startswith(k) for k in PAST_KEYWORDS)


def is_grant_relevant(title: str) -> bool:
    """Return True if role title is grant/program relevant."""
    t = title.lower()
    return any(kw in t for kw in GRANT_RELEVANT_KEYWORDS)


def name_similarity(name_a: str, name_b: str) -> float:
    """
    Return 0–100 similarity score between two names.
    Uses token_sort_ratio to handle name order differences (e.g. "Smith John" vs "John Smith").
    """
    if not name_a or not name_b:
        return 0.0
    a = normalize_name(name_a)
    b = normalize_name(name_b)
    return fuzz.token_sort_ratio(a, b)


def company_matches(company_from_pdl: str, org_name: str) -> bool:
    """
    Return True if the PDL current_company is likely the same as the funder's org.
    Uses 80% similarity threshold (looser than person names — company names vary more).
    """
    if not company_from_pdl or not org_name:
        return False
    score = fuzz.token_sort_ratio(
        normalize_name(company_from_pdl),
        normalize_name(org_name)
    )
    return score >= 80


def find_best_irs_match(
    discovered_name: str,
    irs_people: List[Dict],
    threshold: int = DEFAULT_MATCH_THRESHOLD,
) -> Tuple[Optional[Dict], float]:
    """
    Find the best matching IRS person for a discovered LinkedIn name.
    Returns (matched_irs_person_dict_or_None, similarity_score).
    """
    if not discovered_name or not irs_people:
        return None, 0.0

    best_score = 0.0
    best_match = None

    for irs_person in irs_people:
        irs_name = irs_person.get("name") or ""
        score = name_similarity(discovered_name, irs_name)
        if score > best_score:
            best_score = score
            best_match = irs_person

    if best_score >= threshold:
        return best_match, best_score
    return None, best_score


def categorize_profile(
    profile: Dict,
    org_name: str,
    matched_irs_person: Optional[Dict],
) -> str:
    """
    Determine the status of a discovered profile.

    profile: enriched PDL profile or discovery-only profile
    org_name: the funder's organization name
    matched_irs_person: the IRS person this was matched to (or None)
    """
    current_company = profile.get("current_company") or ""

    if matched_irs_person:
        # We matched this to an IRS person
        if current_company and company_matches(current_company, org_name):
            return "MATCHED"
        elif current_company:
            return "MOVED"
        else:
            # Can't confirm company from PDL — call it matched
            return "MATCHED"
    else:
        # Not in IRS data — new discovery
        return "DISCOVERED"


def merge_staff_for_funder(
    org_name: str,
    irs_leadership: List[Dict],
    serper_profiles: List[Dict],
    pdl_search_profiles: List[Dict],
    enrichment_results: Dict,  # {linkedin_url: enriched_profile_dict}
    match_threshold: int = DEFAULT_MATCH_THRESHOLD,
) -> Tuple[List[Dict], Dict]:
    """
    Cross-reference IRS leadership against discovered profiles and produce
    a merged staff list with status categorization.

    Returns:
        (merged_staff_list, match_stats_dict)
    """
    merged: List[Dict] = []
    irs_matched_indices = set()  # Track which IRS people were found

    # Combine all discovered LinkedIn profiles (deduplicate by URL)
    all_discovered: Dict[str, Dict] = {}  # linkedin_url -> profile

    for p in serper_profiles:
        url = p.get("linkedin_url") or ""
        if url and url not in all_discovered:
            all_discovered[url] = {**p, "source": "serper"}

    for p in pdl_search_profiles:
        url = p.get("linkedin_url") or ""
        if url and url not in all_discovered:
            all_discovered[url] = {**p, "source": "pdl_search"}
        elif url in all_discovered:
            # Merge: PDL search may have better name/title data
            existing = all_discovered[url]
            if not existing.get("full_name") and p.get("full_name"):
                existing["full_name"] = p["full_name"]
            if not existing.get("current_title") and p.get("current_title"):
                existing["current_title"] = p["current_title"]
            if not existing.get("photo_url") and p.get("photo_url"):
                existing["photo_url"] = p["photo_url"]

    # Enrich profiles where we have enrichment data
    for url, profile in all_discovered.items():
        enriched = enrichment_results.get(url)
        if enriched:
            # Enrichment takes priority for title and company
            profile["current_title"] = enriched.get("current_title") or profile.get("current_title") or ""
            profile["current_company"] = enriched.get("current_company") or profile.get("current_company") or ""
            profile["full_name"] = enriched.get("full_name") or profile.get("full_name") or profile.get("name_hint") or ""
            profile["photo_url"] = enriched.get("photo_url") or ""
            profile["enriched"] = True
        else:
            profile["enriched"] = False

    # Match discovered profiles against IRS people
    for url, profile in all_discovered.items():
        display_name = (
            profile.get("full_name")
            or profile.get("name_hint")
            or ""
        ).strip()

        matched_irs, match_score = find_best_irs_match(
            display_name, irs_leadership, match_threshold
        )

        if matched_irs:
            idx = next(
                (i for i, p in enumerate(irs_leadership) if p.get("name") == matched_irs.get("name")),
                None
            )
            if idx is not None:
                irs_matched_indices.add(idx)

        status = categorize_profile(profile, org_name, matched_irs)
        near_miss = (
            NEAR_MISS_THRESHOLD <= match_score < match_threshold
            and matched_irs is None
        )

        merged.append({
            "person_name": display_name or profile.get("name_hint") or "",
            "irs_name": matched_irs.get("name") if matched_irs else "",
            "irs_title": matched_irs.get("title") if matched_irs else "",
            "current_title": profile.get("current_title") or profile.get("title_hint") or "",
            "current_company": profile.get("current_company") or "",
            "linkedin_url": url,
            "photo_url": profile.get("photo_url") or "",
            "status": status,
            "match_score": round(match_score, 1),
            "near_miss": near_miss,
            "enriched": profile.get("enriched", False),
            "source": profile.get("source") or "serper",
            "is_grant_relevant": is_grant_relevant(profile.get("current_title") or ""),
        })

    # Add IRS_ONLY entries for people not found anywhere
    for i, irs_person in enumerate(irs_leadership):
        if i not in irs_matched_indices:
            merged.append({
                "person_name": irs_person.get("name") or "",
                "irs_name": irs_person.get("name") or "",
                "irs_title": irs_person.get("title") or "",
                "current_title": "",
                "current_company": "",
                "linkedin_url": "",
                "photo_url": "",
                "status": "IRS_ONLY",
                "match_score": 0.0,
                "near_miss": False,
                "enriched": False,
                "source": "irs",
                "is_grant_relevant": False,
                "is_past_role": is_past_role(irs_person.get("title") or ""),
            })

    # Build match stats
    status_counts = {"MATCHED": 0, "MOVED": 0, "IRS_ONLY": 0, "DISCOVERED": 0}
    for entry in merged:
        s = entry.get("status", "IRS_ONLY")
        if s in status_counts:
            status_counts[s] += 1

    past_people = [p for p in irs_leadership if is_past_role(p.get("title") or "")]
    past_detected_as_moved = sum(
        1 for m in merged
        if m.get("status") == "MOVED"
        and any(p.get("name") == m.get("irs_name") for p in past_people)
    )

    stats = {
        **status_counts,
        "irs_total": len(irs_leadership),
        "discovered_total": len(all_discovered),
        "past_people_count": len(past_people),
        "past_detected_as_moved": past_detected_as_moved,
        "grant_relevant_discovered": sum(
            1 for m in merged
            if m.get("status") == "DISCOVERED" and m.get("is_grant_relevant")
        ),
    }

    return merged, stats
