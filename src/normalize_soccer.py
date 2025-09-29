#!/usr/bin/env python3
"""
normalize_soccer.py
Takes today's Odds API snapshot (if present) and normalizes a few fields
into a canonical schema. Just a demo, safe to expand later.
"""
import sys, json
import pandas as pd
from pathlib import Path
from utils import today_dir

RAW_DIR = Path("data/raw/odds_api")

def load_today_events():
    if not RAW_DIR.exists():
        return []
    # find latest dated folder
    dated = sorted([p for p in RAW_DIR.iterdir() if p.is_dir()])
    if not dated:
        return []
    latest = dated[-1]
    rows = []
    for p in latest.glob("odds_*.json"):
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(obj, list):
                rows.extend(obj)
        except Exception:
            continue
    return rows

def to_canonical(events):
    out = []
    for ev in events:
        row = {
            "provider": "odds_api",
            "provider_event_id": ev.get("id"),
            "competition": ev.get("sport_title"),
            "match_date_utc": ev.get("commence_time"),
            "home_team": ev.get("home_team"),
            "away_team": ev.get("away_team"),
            "status": ev.get("status"),
        }
        out.append(row)
    return out

if __name__ == "__main__":
    events = load_today_events()
    if not events:
        print("No Odds API events found for today. Run odds_api_pull.py first.")
        sys.exit(0)
    df = pd.DataFrame(to_canonical(events))
    print("\nCanonical sample rows:")
    print(df.head().to_string(index=False))
    outdir = today_dir("canonical")
    outpath = outdir / "odds_api_canonical.csv"
    df.to_csv(outpath, index=False)
    print(f"\nSaved canonical CSV → {outpath}")
