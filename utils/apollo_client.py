"""
apollo_client.py
Apollo.io integration for person search and enrichment.

People Search: find people by company name/domain (FREE — no credits)
People Enrichment: retrieve full profile by LinkedIn URL (1 credit each)

Docs:
  Search:     https://docs.apollo.io/reference/people-api-search
  Enrichment: https://docs.apollo.io/reference/people-enrichment
"""
import time
import requests
from typing import List, Dict, Optional

APOLLO_SEARCH_ENDPOINT   = "https://api.apollo.io/v1/mixed_people/search"
APOLLO_ENRICH_ENDPOINT   = "https://api.apollo.io/v1/people/match"

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
    """Extract the fields we care about from an Apollo person record."""
    # Handle both search results and enrichment results
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
        "full_name": (raw.get("name") or "").strip(),
        "first_name": raw.get("first_name") or "",
        "last_name": raw.get("last_name") or "",
        "current_title": raw.get("title") or "",
        "current_company": current_company,
        "linkedin_url": linkedin_url,
        "photo_url": raw.get("photo_url") or "",
        "location_name": raw.get("city") or raw.get("state") or "",
        "email": raw.get("email") or "",
    }


def _normalize_linkedin(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    if not url.startswith("http"):
        url = "https://www." + url
    return url


# ── Public API ────────────────────────────────────────────────────────────────

def search_people_by_company(
    api_key: str,
    org_name: str,
    website_domain: str = "",
    size: int = 10,
) -> Dict:
    """
    Apollo People Search — find people who work at a given organization.
    Uses company name as primary filter, domain as secondary.
    Does NOT consume credits.

    Returns:
        {
            "profiles": list of normalized person dicts,
            "total_found": int,
            "error": str or None
        }
    """
    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": api_key,
    }

    # Build payload — try domain first for precision, fall back to org name
    payload = {
        "page": 1,
        "per_page": size,
        "person_titles": [],   # no title filter — we want all staff
    }

    if website_domain:
        payload["q_organization_domains"] = [website_domain.lower()]
    else:
        payload["organization_names"] = [org_name]

    try:
        resp = requests.post(
            APOLLO_SEARCH_ENDPOINT,
            headers=headers,
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
        return {"profiles": [], "total_found": 0, "error": "TIMEOUT: Apollo request timed out"}
    except requests.exceptions.ConnectionError as e:
        return {"profiles": [], "total_found": 0, "error": f"CONNECTION_ERROR: {str(e)}"}
    except Exception as e:
        return {"profiles": [], "total_found": 0, "error": f"UNEXPECTED: {str(e)}"}


def enrich_person_by_linkedin(
    api_key: str,
    linkedin_url: str,
) -> Dict:
    """
    Apollo People Enrichment — fetch full profile for a LinkedIn URL.
    Consumes 1 credit per successful match.

    Returns:
        {
            "profile": normalized person dict or None,
            "found": bool,
            "error": str or None
        }
    """
    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": api_key,
    }

    # Apollo expects the full linkedin URL
    url_clean = _normalize_linkedin(linkedin_url)

    payload = {
        "linkedin_url": url_clean,
        "reveal_personal_emails": False,
        "reveal_phone_number": False,
    }

    try:
        resp = requests.post(
            APOLLO_ENRICH_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )

        # Apollo returns 200 even when not found — check the person field
        if resp.status_code == 404:
            return {"profile": None, "found": False, "error": None}

        data = _handle_response(resp)

        raw_person = data.get("person")
        if not raw_person:
            return {"profile": None, "found": False, "error": None}

        profile = _normalize_person(raw_person)
        return {
            "profile": profile,
            "found": True,
            "error": None,
        }

    except ApolloCreditsExhaustedError as e:
        return {"profile": None, "found": False, "error": f"CREDITS_EXHAUSTED: {e.message}"}
    except ApolloAuthError as e:
        return {"profile": None, "found": False, "error": f"AUTH_ERROR: {e.message}"}
    except ApolloRateLimitError as e:
        time.sleep(3)
        return {"profile": None, "found": False, "error": f"RATE_LIMIT: {e.message}"}
    except ApolloAPIError as e:
        return {"profile": None, "found": False, "error": f"APOLLO_ERROR_{e.status_code}: {e.message}"}
    except requests.exceptions.Timeout:
        return {"profile": None, "found": False, "error": "TIMEOUT: Apollo enrichment timed out"}
    except requests.exceptions.ConnectionError as e:
        return {"profile": None, "found": False, "error": f"CONNECTION_ERROR: {str(e)}"}
    except Exception as e:
        return {"profile": None, "found": False, "error": f"UNEXPECTED: {str(e)}"}
