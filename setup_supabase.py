"""
setup_supabase.py
Run this ONCE to create all required tables in your Supabase project.
Usage: python setup_supabase.py
"""
import sys

SUPABASE_URL = "https://gsqfpiupidahqxjhycrm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdzcWZwaXVwaWRhaHF4amh5Y3JtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI1MjQyNzQsImV4cCI6MjA4ODEwMDI3NH0.IXTkGFxsRk3_85QKau9OTVWUEiRk1pR_htVp_hobZlU"

# Each statement is run separately (Supabase anon key can't run multi-statement SQL via RPC)
STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS experiment_sessions (
      id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      started_at      TIMESTAMPTZ DEFAULT NOW(),
      completed_at    TIMESTAMPTZ,
      status          TEXT DEFAULT 'running',
      total_funders   INTEGER DEFAULT 0,
      funders_done    INTEGER DEFAULT 0,
      match_threshold INTEGER DEFAULT 85,
      enrich_enabled  BOOLEAN DEFAULT TRUE,
      notes           TEXT,
      created_at      TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    """
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
      past_people_count     INTEGER DEFAULT 0,
      past_detected_as_moved INTEGER DEFAULT 0,
      api_errors            JSONB DEFAULT '[]',
      processing_ms         INTEGER,
      created_at            TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    """
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
      status          TEXT,
      match_score     FLOAT,
      enriched        BOOLEAN DEFAULT FALSE,
      source          TEXT,
      is_grant_relevant BOOLEAN DEFAULT FALSE,
      near_miss       BOOLEAN DEFAULT FALSE,
      created_at      TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_funder_results_session ON funder_results(session_id)",
    "CREATE INDEX IF NOT EXISTS idx_funder_results_ein ON funder_results(ein)",
    "CREATE INDEX IF NOT EXISTS idx_staff_session ON staff_profiles(session_id)",
    "CREATE INDEX IF NOT EXISTS idx_staff_ein ON staff_profiles(ein)",
    "CREATE INDEX IF NOT EXISTS idx_staff_status ON staff_profiles(status)",
]

def run_setup():
    try:
        from supabase import create_client
    except ImportError:
        print("ERROR: supabase package not installed. Run: pip install supabase")
        sys.exit(1)

    print(f"Connecting to {SUPABASE_URL}...")
    try:
        client = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"ERROR: Could not create Supabase client: {e}")
        sys.exit(1)

    # Use the Postgres REST endpoint via rpc to run raw SQL
    # Supabase anon key can run DDL if RLS is not blocking it
    import requests

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }

    # Try using the SQL endpoint (Supabase Management API path)
    sql_url = f"{SUPABASE_URL}/rest/v1/rpc/exec_sql"

    errors = []
    success = 0

    for i, stmt in enumerate(STATEMENTS):
        stmt = stmt.strip()
        if not stmt:
            continue

        # Try via postgrest rpc
        resp = requests.post(
            sql_url,
            headers=headers,
            json={"query": stmt},
            timeout=15,
        )

        if resp.status_code in (200, 201, 204):
            success += 1
            print(f"  ✅ Statement {i+1}/{len(STATEMENTS)} OK")
        else:
            # Fall back: try as a direct table create via supabase-py workaround
            # Some Supabase projects have exec_sql disabled — print the SQL for manual run
            errors.append((i+1, stmt[:60], resp.status_code, resp.text[:100]))
            print(f"  ⚠️  Statement {i+1}/{len(STATEMENTS)} — status {resp.status_code}")

    print()
    if not errors:
        print("✅ All tables created successfully!")
        print("You can now run: streamlit run app.py")
    else:
        print(f"⚠️  {len(errors)} statement(s) could not run automatically.")
        print("This usually means the `exec_sql` RPC function is not enabled.")
        print()
        print("Please run the following SQL manually in your Supabase SQL Editor:")
        print("  https://supabase.com/dashboard/project/gsqfpiupidahqxjhycrm/editor")
        print()
        print("=" * 60)
        for stmt in STATEMENTS:
            print(stmt.strip())
            print(";")
            print()
        print("=" * 60)

if __name__ == "__main__":
    run_setup()
