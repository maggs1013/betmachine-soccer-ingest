# betmachine-soccer-ingest

Soccer-only data pulls for ML betting research. This repo:
- Connects to multiple **soccer** sources (The Odds API, Football-Data.co.uk, StatsBomb Open, Understat, FBref, OpenLigaDB)
- Saves **raw** snapshots to `data/raw/<source>/<YYYY-MM-DD>/...`
- Prints **fields/headers** and a few **sanity observations** (to confirm we’re getting real values)
- Generates a quick **schema report** (columns seen per source)
- Includes a **starter normalizer** to a tiny canonical schema

## Sources (soccer only)
- **The Odds API** (freemium; needs key): upcoming/live soccer odds
- **Football-Data.co.uk** (free CSV): historical results + bookmaker odds
- **StatsBomb Open** (free JSON): event data (selected comps) incl. xG per shot
- **Understat** (free community wrapper): xG/xA for top leagues
- **FBref** (free tables): team/player aggregates incl. xG/xA
- **OpenLigaDB** (free JSON): Bundesliga fixtures/results/lineups

## Quick start (local)
```bash
git clone https://github.com/<your-username>/betmachine-soccer-ingest.git
cd betmachine-soccer-ingest

python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate

pip install -r requirements.txt

cp .env.example .env
# put your The Odds API key into .env (ODDS_API_KEY=...)

# run pulls (will save raw and print fields + sample values)
python src/odds_api_pull.py
python src/football_data_pull.py
python src/statsbomb_open_pull.py
python src/understat_pull.py
python src/fbref_pull.py
python src/openligadb_pull.py

# optional: see unified example rows after basic normalization
python src/normalize_soccer.py

# get a columns summary of what you fetched today
python src/schema_report.py
