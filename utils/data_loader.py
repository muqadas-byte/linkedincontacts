"""
data_loader.py
Parses 100randomFunders.json into a normalized structure.
Leadership is inside funderOverviewN8NOutput.leadership.
"""
import json
from pathlib import Path
from typing import List, Dict, Any, Optional


def load_funders(filepath: str) -> List[Dict]:
    """Load and parse funders JSON. Returns raw list."""
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def get_n8n(funder: Dict) -> Dict:
    """Safely extract funderOverviewN8NOutput as a dict."""
    n8n = funder.get("funderOverviewN8NOutput") or {}
    if isinstance(n8n, str):
        try:
            n8n = json.loads(n8n)
        except Exception:
            n8n = {}
    return n8n if isinstance(n8n, dict) else {}


def get_latest_financials(funder: Dict) -> Dict:
    """Return the most recent year's financialBreakdown entry."""
    financial = funder.get("financialBreakdown") or {}
    if not financial:
        return {}
    latest_year = max(financial.keys())
    return financial[latest_year]


def get_total_assets(funder: Dict) -> int:
    fin = get_latest_financials(funder)
    try:
        return int(fin.get("totalAssets") or 0)
    except (ValueError, TypeError):
        return 0


def get_segment(total_assets: int) -> str:
    if total_assets >= 10_000_000:
        return "large"
    elif total_assets >= 1_000_000:
        return "mid"
    elif total_assets > 0:
        return "small"
    return "unknown"


def clean_website(website: Optional[str]) -> str:
    """Normalize website string to a plain domain."""
    if not website or website.strip().upper() in ("N/A", "NONE", ""):
        return ""
    w = website.strip().lower()
    w = w.replace("https://", "").replace("http://", "").replace("www.", "")
    # Remove trailing slashes and paths
    w = w.split("/")[0]
    return w


def extract_funder(funder: Dict) -> Dict:
    """Return a clean, normalized funder dict ready for the experiment."""
    n8n = get_n8n(funder)
    hq = funder.get("headquartersAddress") or {}
    total_assets = get_total_assets(funder)
    website_raw = n8n.get("website") or funder.get("website") or funder.get("sourceLink") or ""
    website_domain = clean_website(website_raw)

    leadership = n8n.get("leadership") or []
    # Normalize leadership entries
    clean_leadership = []
    for person in leadership:
        if not person or not person.get("name"):
            continue
        clean_leadership.append({
            "name": (person.get("name") or "").strip().title(),
            "title": (person.get("title") or "").strip().title(),
        })

    city = (hq.get("city") or "").strip()
    state = (hq.get("state") or "").strip()

    return {
        "ein": str(funder.get("ein") or n8n.get("ein") or "").strip(),
        "org_name": (funder.get("name") or n8n.get("organizationName") or "").strip(),
        "city": city,
        "state": state,
        "website_domain": website_domain,
        "leadership": clean_leadership,
        "leadership_count": len(clean_leadership),
        "total_assets": total_assets,
        "segment": get_segment(total_assets),
        "org_type": funder.get("type") or "",
    }


def extract_all_funders(raw_list: List[Dict]) -> List[Dict]:
    """Extract and normalize all funders from raw JSON list."""
    result = []
    for f in raw_list:
        try:
            result.append(extract_funder(f))
        except Exception as e:
            # Skip malformed entries, log the EIN
            ein = f.get("ein", "unknown")
            result.append({
                "ein": str(ein),
                "org_name": f.get("name", "Unknown"),
                "city": "", "state": "", "website_domain": "",
                "leadership": [], "leadership_count": 0,
                "total_assets": 0, "segment": "unknown",
                "org_type": "", "_parse_error": str(e),
            })
    return result
