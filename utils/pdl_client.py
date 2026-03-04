"""
pdl_client.py
People Data Labs (PDL) integration for person search and enrichment.

Person Search: finds people by company name/domain (free, no credits)
Person Enrichment: retrieves full profile by LinkedIn URL (1 credit each)
"""
import time
import requests
from typing import List, Dict, Optional

PDL_BASE = "https://api.peopledatalabs.com/v5"
PDL_SEARCH_ENDPOINT = f"{PDL_BASE}/person/search"
PDL_ENRICH_ENDPOINT = f"{PDL_BASE}/person/enrich"

REQUEST_TIMEOUT = 20


class PDLAPIError(Exception):
    """Base error for PDL API failures."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"PDL API error {status_code}: {message}")


class PDLAuthError(PDLAPIError):
    """Invalid or missing API key."""
    pass


class PDLCreditsExhaustedError(PDLAPIError):
    """402 — ran out of enrichment credits."""
    pass


class PDLRateLimitError(PDLAPIError):
    """429 — too many requests."""
    pass


class PDLNotFoundError(PDLAPIError):
    """404 — person/profile not found in PDL database."""
    pass


def _handle_pdl_response(resp: requests.Response) -> Dict:
    """Parse response and raise typed errors for non-200 statuses."""
    if resp.status_code == 401:
        raise PDLAuthError(401, "Invalid PDL API key")
    if resp.status_code == 402:
        raise PDLCreditsExhaustedError(402, "PDL enrichment credits exhausted for this billing period")
    if resp.status_code == 404:
        raise PDLNotFoundError(404, "Profile not found in PDL database")
    if resp.status_code == 429:
        raise PDLRateLimitError(429, "PDL rate limit exceeded")
    if resp.status_code == 422:
        try:
            detail = resp.json()
        except Exception:
            detail = {}
        raise PDLAPIError(422, f"Unprocessable request: {detail.get('error', {}).get('message', resp.text[:200])}")
    if not resp.ok:
        try:
            detail = resp.json().get("error", {}).get("message", resp.text[:200])
        except Exception:
            detail = resp.text[:200]
        raise PDLAPIError(resp.status_code, str(detail))
    return resp.json()


def _normalize_person(raw: Dict) -> Dict:
    """Extract the fields we care about from a PDL person record."""
    # PDL person search returns nested under the record itself
    # Enrichment returns the data at the top level
    return {
        "full_name": raw.get("full_name") or raw.get("name") or "",
        "first_name": raw.get("first_name") or "",
        "last_name": raw.get("last_name") or "",
        "current_title": raw.get("job_title") or "",
        "current_company": raw.get("job_company_name") or "",
        "linkedin_url": _normalize_linkedin(raw.get("linkedin_url") or raw.get("linkedin") or ""),
        "linkedin_username": raw.get("linkedin_username") or "",
        "photo_url": raw.get("profile_pic") or raw.get("photo_url") or "",
        "location_name": raw.get("location_name") or raw.get("location") or "",
        "work_email": raw.get("work_email") or "",
        "industry": raw.get("industry") or "",
    }


def _normalize_linkedin(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    if not url.startswith("http"):
        url = "https://www." + url
    return url


def search_people_by_company(
    api_key: str,
    org_name: str,
    website_domain: str = "",
    size: int = 10,
) -> Dict:
    """
    PDL Person Search — find people who work at a given organization.
    Uses company name as primary filter, domain as secondary.
    Does NOT consume credits.

    Returns:
        {
            "profiles": list of normalized person dicts,
            "total_found": int,
            "error": str or None
        }
    """
    headers = {"X-Api-Key": api_key, "Content-Type": "application/json"}

    # Build Elasticsearch query
    must_clauses = [{"term": {"job_company_name": org_name.lower()}}]

    if website_domain:
        # OR match by domain for better recall
        query = {
            "bool": {
                "should": [
                    {"term": {"job_company_name": org_name.lower()}},
                    {"term": {"job_company_website": website_domain.lower()}},
                ],
                "minimum_should_match": 1
            }
        }
    else:
        query = {"bool": {"must": must_clauses}}

    payload = {
        "query": query,
        "size": size,
        "pretty": False,
        "dataset": "all",
    }

    try:
        resp = requests.post(
            PDL_SEARCH_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        data = _handle_pdl_response(resp)

        raw_people = data.get("data") or []
        profiles = [_normalize_person(p) for p in raw_people]
        # Filter out profiles with no name or linkedin
        profiles = [p for p in profiles if p["full_name"] or p["linkedin_url"]]

        return {
            "profiles": profiles,
            "total_found": data.get("total", len(profiles)),
            "error": None,
        }

    except PDLAuthError as e:
        return {"profiles": [], "total_found": 0, "error": f"AUTH_ERROR: {e.message}"}
    except PDLRateLimitError as e:
        time.sleep(3)
        return {"profiles": [], "total_found": 0, "error": f"RATE_LIMIT: {e.message}"}
    except PDLAPIError as e:
        return {"profiles": [], "total_found": 0, "error": f"PDL_ERROR_{e.status_code}: {e.message}"}
    except requests.exceptions.Timeout:
        return {"profiles": [], "total_found": 0, "error": "TIMEOUT: PDL request timed out"}
    except requests.exceptions.ConnectionError as e:
        return {"profiles": [], "total_found": 0, "error": f"CONNECTION_ERROR: {str(e)}"}
    except Exception as e:
        return {"profiles": [], "total_found": 0, "error": f"UNEXPECTED: {str(e)}"}


def enrich_person_by_linkedin(
    api_key: str,
    linkedin_url: str,
) -> Dict:
    """
    PDL Person Enrichment — fetch full profile for a LinkedIn URL.
    Consumes 1 credit per successful call.

    Returns:
        {
            "profile": normalized person dict or None,
            "found": bool,
            "credits_remaining": int or None,
            "error": str or None
        }
    """
    headers = {"X-Api-Key": api_key}

    # Normalize URL for PDL
    # PDL accepts: "linkedin.com/in/username" (without https://www.)
    url_for_pdl = linkedin_url.replace("https://www.", "").replace("https://", "")

    params = {
        "profile": url_for_pdl,
        "required": "profiles",  # only return if LinkedIn profile confirmed
        "pretty": "false",
    }

    try:
        resp = requests.get(
            PDL_ENRICH_ENDPOINT,
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        # Credits remaining is in the header
        credits_remaining = None
        try:
            credits_remaining = int(resp.headers.get("x-credits-remaining", -1))
        except (ValueError, TypeError):
            pass

        if resp.status_code == 404:
            return {
                "profile": None,
                "found": False,
                "credits_remaining": credits_remaining,
                "error": None,  # 404 is a valid "not found" — not an error
            }

        data = _handle_pdl_response(resp)
        profile = _normalize_person(data) if data.get("status") == 200 else None

        return {
            "profile": profile,
            "found": profile is not None,
            "credits_remaining": credits_remaining,
            "error": None,
        }

    except PDLCreditsExhaustedError as e:
        return {"profile": None, "found": False, "credits_remaining": 0,
                "error": f"CREDITS_EXHAUSTED: {e.message}"}
    except PDLAuthError as e:
        return {"profile": None, "found": False, "credits_remaining": None,
                "error": f"AUTH_ERROR: {e.message}"}
    except PDLRateLimitError as e:
        time.sleep(3)
        return {"profile": None, "found": False, "credits_remaining": None,
                "error": f"RATE_LIMIT: {e.message}"}
    except PDLNotFoundError:
        return {"profile": None, "found": False, "credits_remaining": None, "error": None}
    except PDLAPIError as e:
        return {"profile": None, "found": False, "credits_remaining": None,
                "error": f"PDL_ERROR_{e.status_code}: {e.message}"}
    except requests.exceptions.Timeout:
        return {"profile": None, "found": False, "credits_remaining": None,
                "error": "TIMEOUT: PDL enrichment timed out"}
    except requests.exceptions.ConnectionError as e:
        return {"profile": None, "found": False, "credits_remaining": None,
                "error": f"CONNECTION_ERROR: {str(e)}"}
    except Exception as e:
        return {"profile": None, "found": False, "credits_remaining": None,
                "error": f"UNEXPECTED: {str(e)}"}
