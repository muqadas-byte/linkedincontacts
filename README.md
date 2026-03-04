# Staff Intelligence R&D Experiment
### 100-Funder Validation — Google Search Discovery + PDL Enrichment

---

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure API keys
Edit `.streamlit/secrets.toml`:
```toml
SERPAPI_KEY    = "your-serpapi-key"
PDL_API_KEY    = "your-pdl-key"

# Optional — for persisting results
SUPABASE_URL      = "https://xxxx.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGci..."
```

### 3. Set up Supabase (optional but recommended)
1. Go to your Supabase project → SQL Editor
2. Copy and run the SQL from the **Home page → Supabase Schema Setup** expander
3. Enter your Supabase URL and anon key on the Home page and click **Test Connection**

### 4. Run the app
```bash
streamlit run app.py
```

---

## App Pages

| Page | Description |
|------|-------------|
| 🏠 Home | Load funders JSON, configure API keys, Supabase setup |
| 📊 Overview | Explore the 100 funder sample before running |
| 🔬 Run Experiment | Execute the full pipeline with live progress |
| 📋 Results | Per-funder drill-down, staff profiles, CSV exports |
| 📈 Metrics | All 8 experiment metrics + Go/No-Go decision |
| ⚠️ Edge Cases | Failure analysis + final recommendation |

---

## Pipeline (per funder)

1. **SerpApi Discovery** — 5 query types (broad, location, per-person, role, domain)
2. **PDL People Search** — company search (free, no credits)
3. **PDL Enrichment** — LinkedIn URL enrichment (1 credit/profile, configurable budget)
4. **Fuzzy Matching** — Jaro-Winkler cross-reference against IRS leadership
5. **Categorize** — MATCHED / MOVED / IRS_ONLY / DISCOVERED
6. **Persist** — Supabase (if configured) + in-memory + CSV export

---

## Cost Estimate (100 funders)

| Service | Usage | Cost |
|---------|-------|------|
| SerpApi | ~700 queries | ~$10.50 |
| PDL Person Search | 100 company searches | $0 (free) |
| PDL Enrichment | 100–200 profiles | $0 (free tier: 100/mo) |
| **Total** | | **~$10.50** |

---

## Data Structure

`100randomFunders.json` — Leadership data is inside `funderOverviewN8NOutput.leadership`:
```json
{
  "funderOverviewN8NOutput": {
    "leadership": [
      {"name": "RICH HILLMAN", "title": "DIRECTOR"},
      ...
    ],
    "website": "https://...",
    "hqAddress": "..."
  }
}
```
