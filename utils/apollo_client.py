"""
apollo_client.py
Apollo.io integration with three separate steps:

  Step 4 — People Search  (search_key)   api/v1/mixed_people/api_search
           Find people at a company — supplements SerpApi discovery.

  Step 6a — People Match  (match_key)    api/v1/people/match
           Resolve a LinkedIn URL → apollo_person_id + basic profile.

  Step 6b — People Enrichment (match_key) api/v1/people/enrichment
           Resolve apollo_person_id / linkedin_url → full profile
           (name, title, company, photo, headline).
"""
import time
import requests
from typing import List, Dict, Optional

APOLLO_SEARCH_ENDPOINT = "https://api.apollo.io/api/v1/mixed_people/api_search"
APOLLO_MATCH_ENDPOINT  = "https://api.apollo.io/api/v1/people/match"
APOLLO_ENRICH_ENDPOINT = "https://api.apollo.io/api/v1/people/enrichment"

REQUEST_TIMEOUT = 20


# ── Typed errors ──────────────────────────────────────────────────────────────

class ApolloAPIError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"Apollo API error {status_code}: {message}")

class ApolloAuthError(ApolloAPIError):
    pass

class ApolloRateLimitError(ApolloAPIError):
    pass

class ApolloCreditsExhaustedError(ApolloAPIError):
    pass


# ── Internal helpers ──────────────────────────────────────────────────────────

def _headers(api_key: str) -> Dict:
    return {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": api_key,
    }


def _handle_response(resp: requests.Response) -> Dict:
    if resp.status_code == 401:
        raise ApolloAuthError(401, "Invalid Apollo API key")
    if resp.status_code == 403:
        raise ApolloCreditsExhaustedError(403, "Apollo credits exhausted or plan limit reached")
    if resp.status_code == 429:
        raise ApolloRateLimitError(429, "Apollo rate limit exceeded")
    if not resp.ok:
        try:
            detail = resp.json()
            msg = detail.get("error", detail.get("message", resp.text[:200]))
        except Exception:
            msg = resp.text[:200]
        raise ApolloAPIError(resp.status_code, str(msg))
    return resp.json()


def _normalize_person(raw: Dict) -> Dict:
    """Extract the fields we care about from any Apollo person record."""
    employment_history = raw.get("employment_history") or []
    org = raw.get("organization") or (employment_history[0] if employment_history else {})
    if isinstance(org, list):
        org = org[0] if org else {}

    current_company = (
        raw.get("organization_name")
        or (org.get("name") if isinstance(org, dict) else "")
        or ""
    )

    linkedin_url = raw.get("linkedin_url") or ""
    if linkedin_url and not linkedin_url.startswith("http"):
        linkedin_url = "https://www." + linkedin_url

    return {
        "full_name":       (raw.get("name") or "").strip(),
        "first_name":      raw.get("first_name") or "",
        "last_name":       raw.get("last_name") or "",
        "current_title":   raw.get("title") or "",
        "current_company": current_company,
        "linkedin_url":    linkedin_url,
        "photo_url":       raw.get("photo_url") or "",
        "headline":        raw.get("headline") or "",
        "location_name":   raw.get("city") or raw.get("state") or "",
        "email":           raw.get("email") or "",
        "apollo_person_id": raw.get("id") or "",
    }


def _normalize_linkedin(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    if not url.startswith("http"):
        url = "https://www." + url
    return url


# ── Step 4: People Search ─────────────────────────────────────────────────────

def search_people_by_company(
    search_key: str,
    org_name: str,
    website_domain: str = "",
    size: int = 10,
) -> Dict:
    """
    Apollo People Search — find people who work at a given organization.
    Uses api/v1/mixed_people/api_search with the dedicated search key.
    Supplements SerpApi — finds staff Apollo knows about via their DB.

    Returns:
        {
            "profiles": list of normalized person dicts,
            "total_found": int,
            "error": str or None
        }
    """
    payload = {
        "page": 1,
        "per_page": size,
        "person_titles": [],
    }
    if website_domain:
        payload["q_organization_domains"] = [website_domain.lower()]
    else:
        payload["organization_names"] = [org_name]

    try:
        resp = requests.post(
            APOLLO_SEARCH_ENDPOINT,
            headers=_headers(search_key),
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        data = _handle_response(resp)

        raw_people = data.get("people") or []
        profiles = [_normalize_person(p) for p in raw_people]
        profiles = [p for p in profiles if p["full_name"] or p["linkedin_url"]]

        return {
            "profiles": profiles,
            "total_found": data.get("pagination", {}).get("total_entries", len(profiles)),
            "error": None,
        }

    except ApolloAuthError as e:
        return {"profiles": [], "total_found": 0, "error": f"AUTH_ERROR: {e.message}"}
    except ApolloRateLimitError as e:
        time.sleep(3)
        return {"profiles": [], "total_found": 0, "error": f"RATE_LIMIT: {e.message}"}
    except ApolloCreditsExhaustedError as e:
        return {"profiles": [], "total_found": 0, "error": f"CREDITS_EXHAUSTED: {e.message}"}
    except ApolloAPIError as e:
        return {"profiles": [], "total_found": 0, "error": f"APOLLO_ERROR_{e.status_code}: {e.message}"}
    except requests.exceptions.Timeout:
        return {"profiles": [], "total_found": 0, "error": "TIMEOUT: Apollo search timed out"}
    except requests.exceptions.ConnectionError as e:
        return {"profiles": [], "total_found": 0, "error": f"CONNECTION_ERROR: {str(e)}"}
    except Exception as e:
        return {"profiles": [], "total_found": 0, "error": f"UNEXPECTED: {str(e)}"}


# ── Step 6a: People Match ─────────────────────────────────────────────────────

def match_person(
    match_key: str,
    linkedin_url: str,
) -> Dict:
    """
    Apollo People Match — resolve a LinkedIn URL to an apollo_person_id
    and a basic confirmed profile.
    Uses the match key on api/v1/people/match.

    Returns:
        {
            "apollo_person_id": str or "",
            "linkedin_url": str,
            "profile": normalized person dict or None,
            "found": bool,
            "error": str or None
        }
    """
    url_clean = _normalize_linkedin(linkedin_url)

    payload = {
        "linkedin_url": url_clean,
        "reveal_personal_emails": False,
        "reveal_phone_number": False,
    }

    try:
        resp = requests.post(
            APOLLO_MATCH_ENDPOINT,
            headers=_headers(match_key),
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )

        if resp.status_code == 404:
            return {"apollo_person_id": "", "linkedin_url": url_clean,
                    "profile": None, "found": False, "error": None}

        data = _handle_response(resp)
        raw_person = data.get("person")

        if not raw_person:
            return {"apollo_person_id": "", "linkedin_url": url_clean,
                    "profile": None, "found": False, "error": None}

        profile = _normalize_person(raw_person)
        return {
            "apollo_person_id": profile.get("apollo_person_id") or "",
            "linkedin_url":     profile.get("linkedin_url") or url_clean,
            "profile":          profile,
            "found":            True,
            "error":            None,
        }

    except ApolloCreditsExhaustedError as e:
        return {"apollo_person_id": "", "linkedin_url": url_clean,
                "profile": None, "found": False, "error": f"CREDITS_EXHAUSTED: {e.message}"}
    except ApolloAuthError as e:
        return {"apollo_person_id": "", "linkedin_url": url_clean,
                "profile": None, "found": False, "error": f"AUTH_ERROR: {e.message}"}
    except ApolloRateLimitError as e:
        time.sleep(3)
        return {"apollo_person_id": "", "linkedin_url": url_clean,
                "profile": None, "found": False, "error": f"RATE_LIMIT: {e.message}"}
    except ApolloAPIError as e:
        return {"apollo_person_id": "", "linkedin_url": url_clean,
                "profile": None, "found": False, "error": f"APOLLO_ERROR_{e.status_code}: {e.message}"}
    except requests.exceptions.Timeout:
        return {"apollo_person_id": "", "linkedin_url": url_clean,
                "profile": None, "found": False, "error": "TIMEOUT: Apollo match timed out"}
    except requests.exceptions.ConnectionError as e:
        return {"apollo_person_id": "", "linkedin_url": url_clean,
                "profile": None, "found": False, "error": f"CONNECTION_ERROR: {str(e)}"}
    except Exception as e:
        return {"apollo_person_id": "", "linkedin_url": url_clean,
                "profile": None, "found": False, "error": f"UNEXPECTED: {str(e)}"}


# ── Step 6b: People Enrichment ────────────────────────────────────────────────

def enrich_person(
    match_key: str,
    apollo_person_id: str = "",
    linkedin_url: str = "",
) -> Dict:
    """
    Apollo People Enrichment — fetch full profile using apollo_person_id
    (preferred) or linkedin_url as fallback. Uses the match key.
    Returns name, title, company, photo, headline, linkedin.

    Returns:
        {
            "profile": normalized person dict or None,
            "found": bool,
            "credits_remaining": int or None,
            "error": str or None
        }
    """
    params = {}
    if apollo_person_id:
        params["id"] = apollo_person_id
    elif linkedin_url:
        params["linkedin_url"] = _normalize_linkedin(linkedin_url)
    else:
        return {"profile": None, "found": False, "credits_remaining": None,
                "error": "No apollo_person_id or linkedin_url provided"}

    try:
        resp = requests.get(
            APOLLO_ENRICH_ENDPOINT,
            headers=_headers(match_key),
            params=params,
            timeout=REQUEST_TIMEOUT,
        )

        if resp.status_code == 404:
            return {"profile": None, "found": False, "credits_remaining": None, "error": None}

        data = _handle_response(resp)
        raw_person = data.get("person")
        credits_remaining = data.get("credits_remaining")

        if not raw_person:
            return {"profile": None, "found": False,
                    "credits_remaining": credits_remaining, "error": None}

        profile = _normalize_person(raw_person)
        return {
            "profile":           profile,
            "found":             True,
            "credits_remaining": credits_remaining,
            "error":             None,
        }

    except ApolloCreditsExhaustedError as e:
        return {"profile": None, "found": False, "credits_remaining": 0,
                "error": f"CREDITS_EXHAUSTED: {e.message}"}
    except ApolloAuthError as e:
        return {"profile": None, "found": False, "credits_remaining": None,
                "error": f"AUTH_ERROR: {e.message}"}
    except ApolloRateLimitError as e:
        time.sleep(3)
        return {"profile": None, "found": False, "credits_remaining": None,
                "error": f"RATE_LIMIT: {e.message}"}
    except ApolloAPIError as e:
        return {"profile": None, "found": False, "credits_remaining": None,
                "error": f"APOLLO_ERROR_{e.status_code}: {e.message}"}
    except requests.exceptions.Timeout:
        return {"profile": None, "found": False, "credits_remaining": None,
                "error": "TIMEOUT: Apollo enrichment timed out"}
    except requests.exceptions.ConnectionError as e:
        return {"profile": None, "found": False, "credits_remaining": None,
                "error": f"CONNECTION_ERROR: {str(e)}"}
    except Exception as e:
        return {"profile": None, "found": False, "credits_remaining": None,
                "error": f"UNEXPECTED: {str(e)}"}


# ── Legacy alias (kept so any other imports don't break) ──────────────────────
def enrich_person_by_linkedin(api_key: str, linkedin_url: str) -> Dict:
    """Backward-compat wrapper — routes through match → enrich."""
    match_res = match_person(api_key, linkedin_url)
    if not match_res["found"]:
        return {"profile": None, "found": False, "error": match_res["error"]}
    return enrich_person(
        api_key,
        apollo_person_id=match_res["apollo_person_id"],
        linkedin_url=match_res["linkedin_url"],
    )
