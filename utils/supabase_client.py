"""
supabase_client.py
Supabase integration for persisting experiment results.

Tables:
  experiment_sessions  — tracks each run of the experiment
  funder_results       — per-funder aggregated outcomes
  staff_profiles       — individual staff entries per funder
"""
import json
from typing import Optional, List, Dict, Any
from datetime import datetime

try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False


# ─── SQL for creating the required tables ────────────────────────────────────
SCHEMA_SQL = """
-- Run this in your Supabase SQL editor to create the experiment tables.

CREATE TABLE IF NOT EXISTS experiment_sessions (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  started_at      TIMESTAMPTZ DEFAULT NOW(),
  completed_at    TIMESTAMPTZ,
  status          TEXT DEFAULT 'running',   -- running | completed | failed | paused
  total_funders   INTEGER DEFAULT 0,
  funders_done    INTEGER DEFAULT 0,
  match_threshold INTEGER DEFAULT 85,
  enrich_enabled  BOOLEAN DEFAULT TRUE,
  notes           TEXT,
  created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS funder_results (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id            UUID REFERENCES experiment_sessions(id) ON DELETE CASCADE,
  ein                   TEXT NOT NULL,
  org_name              TEXT,
  segment               TEXT,
  city                  TEXT,
  state                 TEXT,
  irs_people_count      INTEGER DEFAULT 0,
  matched_count         INTEGER DEFAULT 0,
  moved_count           INTEGER DEFAULT 0,
  irs_only_count        INTEGER DEFAULT 0,
  discovered_count      INTEGER DEFAULT 0,
  grant_relevant_count  INTEGER DEFAULT 0,
  serper_queries_run    INTEGER DEFAULT 0,
  serper_urls_found     INTEGER DEFAULT 0,
  pdl_profiles_found    INTEGER DEFAULT 0,
  enrichments_done      INTEGER DEFAULT 0,
  api_errors            JSONB DEFAULT '[]',
  processing_ms         INTEGER,
  created_at            TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staff_profiles (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id      UUID REFERENCES experiment_sessions(id) ON DELETE CASCADE,
  ein             TEXT NOT NULL,
  org_name        TEXT,
  person_name     TEXT,
  irs_name        TEXT,
  irs_title       TEXT,
  current_title   TEXT,
  current_company TEXT,
  linkedin_url    TEXT,
  photo_url       TEXT,
  status          TEXT,   -- MATCHED | MOVED | IRS_ONLY | DISCOVERED
  match_score     FLOAT,
  enriched        BOOLEAN DEFAULT FALSE,
  source          TEXT,   -- serper | pdl_search | irs
  is_grant_relevant BOOLEAN DEFAULT FALSE,
  near_miss       BOOLEAN DEFAULT FALSE,
  created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_funder_results_session ON funder_results(session_id);
CREATE INDEX IF NOT EXISTS idx_funder_results_ein ON funder_results(ein);
CREATE INDEX IF NOT EXISTS idx_staff_session ON staff_profiles(session_id);
CREATE INDEX IF NOT EXISTS idx_staff_ein ON staff_profiles(ein);
CREATE INDEX IF NOT EXISTS idx_staff_status ON staff_profiles(status);
"""


class SupabaseClient:
    """Thin wrapper around supabase-py with experiment-specific helpers."""

    def __init__(self, url: str, key: str):
        if not SUPABASE_AVAILABLE:
            raise RuntimeError("supabase package not installed. Run: pip install supabase")
        if not url or not key:
            raise ValueError("Supabase URL and anon key are required")
        self._client: Client = create_client(url, key)

    # ── Session management ───────────────────────────────────────────────────

    def create_session(self, total_funders: int, match_threshold: int,
                       enrich_enabled: bool, notes: str = "") -> str:
        """Insert a new experiment_session row. Returns the session UUID."""
        result = (
            self._client.table("experiment_sessions")
            .insert({
                "total_funders": total_funders,
                "match_threshold": match_threshold,
                "enrich_enabled": enrich_enabled,
                "notes": notes,
                "status": "running",
            })
            .execute()
        )
        return result.data[0]["id"]

    def update_session(self, session_id: str, **kwargs) -> None:
        """Update fields on an experiment_session row."""
        self._client.table("experiment_sessions").update(kwargs).eq("id", session_id).execute()

    def complete_session(self, session_id: str, funders_done: int) -> None:
        self.update_session(
            session_id,
            status="completed",
            completed_at=datetime.utcnow().isoformat(),
            funders_done=funders_done,
        )

    def fail_session(self, session_id: str, funders_done: int) -> None:
        self.update_session(
            session_id,
            status="failed",
            completed_at=datetime.utcnow().isoformat(),
            funders_done=funders_done,
        )

    def list_sessions(self) -> List[Dict]:
        result = (
            self._client.table("experiment_sessions")
            .select("*")
            .order("created_at", desc=True)
            .limit(20)
            .execute()
        )
        return result.data or []

    def get_session(self, session_id: str) -> Optional[Dict]:
        result = (
            self._client.table("experiment_sessions")
            .select("*")
            .eq("id", session_id)
            .single()
            .execute()
        )
        return result.data

    # ── Funder results ───────────────────────────────────────────────────────

    def upsert_funder_result(self, session_id: str, ein: str, data: Dict) -> None:
        """Insert or update a funder_results row."""
        payload = {
            "session_id": session_id,
            "ein": ein,
            **data,
            "api_errors": json.dumps(data.get("api_errors") or []),
        }
        self._client.table("funder_results").upsert(payload).execute()

    def get_funder_results(self, session_id: str) -> List[Dict]:
        result = (
            self._client.table("funder_results")
            .select("*")
            .eq("session_id", session_id)
            .order("created_at")
            .execute()
        )
        return result.data or []

    # ── Staff profiles ───────────────────────────────────────────────────────

    def insert_staff_profiles(self, session_id: str, ein: str,
                               org_name: str, profiles: List[Dict]) -> None:
        """Batch insert staff profile rows for a funder."""
        if not profiles:
            return
        rows = []
        for p in profiles:
            rows.append({
                "session_id": session_id,
                "ein": ein,
                "org_name": org_name,
                "person_name": p.get("person_name") or "",
                "irs_name": p.get("irs_name") or "",
                "irs_title": p.get("irs_title") or "",
                "current_title": p.get("current_title") or "",
                "current_company": p.get("current_company") or "",
                "linkedin_url": p.get("linkedin_url") or "",
                "photo_url": p.get("photo_url") or "",
                "status": p.get("status") or "IRS_ONLY",
                "match_score": p.get("match_score") or 0.0,
                "enriched": bool(p.get("enriched")),
                "source": p.get("source") or "irs",
                "is_grant_relevant": bool(p.get("is_grant_relevant")),
                "near_miss": bool(p.get("near_miss")),
            })
        self._client.table("staff_profiles").insert(rows).execute()

    def get_staff_profiles(self, session_id: str, ein: Optional[str] = None) -> List[Dict]:
        q = (
            self._client.table("staff_profiles")
            .select("*")
            .eq("session_id", session_id)
        )
        if ein:
            q = q.eq("ein", ein)
        result = q.order("status").execute()
        return result.data or []

    def get_all_staff(self, session_id: str) -> List[Dict]:
        return self.get_staff_profiles(session_id)


def get_schema_sql() -> str:
    """Return the SQL needed to set up the Supabase tables."""
    return SCHEMA_SQL


def try_connect(url: str, key: str) -> tuple:
    """
    Try to connect to Supabase. Returns (client_or_None, error_message_or_None).
    """
    if not SUPABASE_AVAILABLE:
        return None, "supabase package not installed"
    if not url or not key:
        return None, "Supabase URL and key not configured"
    try:
        client = SupabaseClient(url, key)
        # Lightweight connectivity check
        client._client.table("experiment_sessions").select("id").limit(1).execute()
        return client, None
    except Exception as e:
        msg = str(e)
        if "relation" in msg and "does not exist" in msg:
            return None, "Tables not created yet — run the schema SQL in your Supabase SQL editor"
        return None, msg
