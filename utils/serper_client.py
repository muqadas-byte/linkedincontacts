"""
serper_client.py
Handles all Google Search discovery via Serper.dev.
Implements the 5 query types from Section 5.3 of the experiment spec.
"""
import re
import time
import requests
from typing import List, Dict, Optional, Tuple

SERPER_ENDPOINT = "https://google.serper.dev/search"
LINKEDIN_PROFILE_RE = re.compile(r"linkedin\.com/in/([\w\-]+)", re.IGNORECASE)

# How many organic results to request per query
RESULTS_PER_QUERY = 10
# Delay between queries to stay within rate limits (seconds)
QUERY_DELAY = 0.2


class SerperAPIError(Exception):
    """Raised when Serper API returns a non-200 response."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"Serper API error {status_code}: {message}")


class SerperRateLimitError(SerperAPIError):
    """Raised on 429 Too Many Requests."""
    pass


class SerperAuthError(SerperAPIError):
    """Raised on 401 Unauthorized (bad API key)."""
    pass


def _call_serper(api_key: str, query: str, num: int = RESULTS_PER_QUERY) -> Dict:
    """
    Make a single search request to Serper.dev.
    Returns the raw JSON response dict.
    Raises SerperAPIError subclasses on failure.
    """
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json",
    }
    payload = {"q": query, "num": num}

    try:
        resp = requests.post(
            SERPER_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=15,
        )
    except requests.exceptions.Timeout:
        raise SerperAPIError(0, "Request timed out after 15 seconds")
    except requests.exceptions.ConnectionError as e:
        raise SerperAPIError(0, f"Connection error: {str(e)}")

    if resp.status_code == 401:
        raise SerperAuthError(401, "Invalid or missing Serper API key")
    if resp.status_code == 429:
        raise SerperRateLimitError(429, "Serper rate limit exceeded — slow down requests")
    if resp.status_code == 403:
        raise SerperAPIError(403, "Serper API quota exhausted or plan limit reached")
    if not resp.ok:
        try:
            detail = resp.json().get("message", resp.text[:200])
        except Exception:
            detail = resp.text[:200]
        raise SerperAPIError(resp.status_code, detail)

    return resp.json()


def _extract_linkedin_profiles(serp_result: Dict, query_type: str) -> List[Dict]:
    """
    Parse organic results from a Serper response.
    Returns a list of dicts: {linkedin_url, name_hint, title_hint, query_type, snippet}
    """
    profiles = []
    organic = serp_result.get("organic") or []

    for item in organic:
        link = item.get("link") or ""
        title = item.get("title") or ""
        snippet = item.get("snippet") or ""

        # Only care about linkedin.com/in/ profile pages
        if "linkedin.com/in/" not in link.lower():
            continue

        match = LINKEDIN_PROFILE_RE.search(link)
        if not match:
            continue

        profile_slug = match.group(1)
        # Normalize URL to canonical form
        linkedin_url = f"https://www.linkedin.com/in/{profile_slug}"

        # Try to extract a name from the title
        # LinkedIn titles look like: "FirstName LastName - Title at Company | LinkedIn"
        name_hint = ""
        title_hint = ""
        if " - " in title:
            parts = title.split(" - ", 1)
            name_hint = parts[0].strip()
            role_part = parts[1].split("|")[0].strip()
            title_hint = role_part

        profiles.append({
            "linkedin_url": linkedin_url,
            "profile_slug": profile_slug,
            "name_hint": name_hint,
            "title_hint": title_hint,
            "snippet": snippet[:300],
            "query_type": query_type,
        })

    return profiles


def build_queries(org_name: str, city: str, state: str,
                  website_domain: str, top_people: List[str]) -> List[Tuple[str, str]]:
    """
    Build the 5 query types for a funder.
    Returns list of (query_string, query_type_label).
    """
    queries = []

    # Query A: Broad org search
    q_a = f'site:linkedin.com/in "{org_name}"'
    queries.append((q_a, "A_org_broad"))

    # Query B: Location-scoped
    if city and state:
        q_b = f'site:linkedin.com/in "{org_name}" {city} {state}'
    elif state:
        q_b = f'site:linkedin.com/in "{org_name}" {state}'
    else:
        q_b = q_a  # fallback to broad if no location
    queries.append((q_b, "B_org_location"))

    # Query C: Per-person validation (top 5 IRS people by name)
    for person_name in top_people[:5]:
        if person_name:
            q_c = f'site:linkedin.com/in "{person_name}" "{org_name}"'
            queries.append((q_c, "C_person_validate"))

    # Query D: Grant-relevant role discovery
    q_d = f'site:linkedin.com/in "{org_name}" "program officer" OR "grants manager" OR "program director"'
    queries.append((q_d, "D_role_discovery"))

    # Query E: Domain-based (if website domain available)
    if website_domain:
        q_e = f'site:linkedin.com/in "@{website_domain}"'
        queries.append((q_e, "E_domain_based"))

    return queries


def run_discovery(
    api_key: str,
    org_name: str,
    city: str,
    state: str,
    website_domain: str,
    leadership: List[Dict],
    query_delay: float = QUERY_DELAY,
) -> Dict:
    """
    Run all 5 query types for a single funder.

    Returns:
        {
            "profiles": list of deduplicated profile dicts,
            "queries_run": int,
            "queries_detail": list of {query, query_type, results_count, error},
            "total_raw_hits": int,
            "error": str or None  (fatal error that stopped all queries)
        }
    """
    person_names = [p["name"] for p in (leadership or []) if p.get("name")]
    queries = build_queries(org_name, city, state, website_domain, person_names)

    all_profiles: List[Dict] = []
    seen_slugs: set = set()
    queries_detail = []
    queries_run = 0
    total_raw_hits = 0

    for query_str, query_type in queries:
        detail = {"query": query_str, "query_type": query_type,
                  "results_count": 0, "error": None}
        try:
            time.sleep(query_delay)
            result = _call_serper(api_key, query_str)
            profiles = _extract_linkedin_profiles(result, query_type)
            detail["results_count"] = len(profiles)
            total_raw_hits += len(profiles)
            queries_run += 1

            # Deduplicate by profile_slug
            for p in profiles:
                slug = p["profile_slug"]
                if slug not in seen_slugs:
                    seen_slugs.add(slug)
                    all_profiles.append(p)

        except SerperAuthError as e:
            detail["error"] = f"AUTH_ERROR: {e.message}"
            queries_detail.append(detail)
            # Auth errors are fatal — stop processing this funder
            return {
                "profiles": all_profiles,
                "queries_run": queries_run,
                "queries_detail": queries_detail,
                "total_raw_hits": total_raw_hits,
                "error": f"AUTH_ERROR: {e.message}",
            }
        except SerperRateLimitError as e:
            detail["error"] = f"RATE_LIMIT: {e.message}"
            queries_detail.append(detail)
            # Back off and continue
            time.sleep(5)
            continue
        except SerperAPIError as e:
            detail["error"] = f"API_ERROR_{e.status_code}: {e.message}"
        except Exception as e:
            detail["error"] = f"UNEXPECTED: {str(e)}"

        queries_detail.append(detail)

    return {
        "profiles": all_profiles,
        "queries_run": queries_run,
        "queries_detail": queries_detail,
        "total_raw_hits": total_raw_hits,
        "error": None,
    }
