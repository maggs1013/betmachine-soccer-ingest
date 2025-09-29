#!/usr/bin/env python3
"""
Very small example normalizer that:
- Reads today's Odds API soccer events (if present)
- Produces a few canonical rows (provider=odds_api)
- Prints them and saves a CSV

Extend this to join with Football-Data/Understat later.
"""
import sys, json
from pathlib import Path
import pandas as pd
from utils import today_dir

RAW_DIR = Path("data/raw/odds_api")

def load_any_today_json(prefix="odds_soccer"):
    # find any odds_* file from today
    today = sorted(RAW_DIR.iterdir())[-1] if RAW_DIR.exists() and list(RAW_DIR.iterdir()) else None
    if not today:
        return []
    files = [p for p in today.glob("*.json") if p.name.startswith("odds_")]
    rows = []
    for p in files:
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
        bm = (ev.get("bookmakers") or [])
        best = None
        # pick first market set as "best" for demonstration
        if bm and (bm[0].get("markets")):
            m = bm[0]["markets"]
            # extract simple h2h
            h2h = next((x for x in m if x.get("key")=="h2h"), None)
            odds_home = odds_draw = odds_away = None
            if h2h:
                for o in (h2h.get("outcomes") or []):
                    if o.get("name") == ev.get("home_team"):
                        odds_home = o.get("price")
                    elif o.get("name") == ev.get("away_team"):
                        odds_away = o.get("price")
                    elif o.get("name") in ("Draw","draw","X"):
                        odds_draw = o.get("price")
            # extract totals if present
            totals = next((x for x in m if x.get("key")=="totals"), None)
            total_line = over_price = under_price = None
            if totals and totals.get("outcomes"):
                # outcomes often include Over/Under with "point"
                total_line = totals["outcomes"][0].get("point")
                for o in totals["outcomes"]:
                    if str(o.get("name","")).lower().startswith("over"):
                        over_price = o.get("price")
                    if str(o.get("name","")).lower().startswith("under"):
                        under_price = o.get("price")
            # spreads if present
            spreads = next((x for x in m if x.get("key")=="spreads"), None)
            spread_home_line = spread_home_price = spread_away_line = spread_away_price = None
            if spreads and spreads.get("outcomes"):
                for o in spreads["outcomes"]:
                    if o.get("name") == ev.get("home_team"):
                        spread_home_line = o.get("point"); spread_home_price = o.get("price")
                    if o.get("name") == ev.get("away_team"):
                        spread_away_line = o.get("point"); spread_away_price = o.get("price")
        row = {
            "provider": "odds_api",
            "provider_event_id": ev.get("id"),
            "competition": ev.get("sport_title"),
            "season": None,
            "match_date_utc": ev.get("commence_time"),
            "home_team": ev.get("home_team"),
            "away_team": ev.get("away_team"),
            "status": ev.get("status"),
            "score_home": None,
            "score_away": None,
            "odds_home": odds_home,
            "odds_draw": odds_draw,
            "odds_away": odds_away,
            "total_goals_line": total_line,
            "total_goals_over_price": over_price,
            "total_goals_under_price": under_price,
            "spread_home_line": spread_home_line,
            "spread_home_price": spread_home_price,
            "spread_away_line": spread_away_line,
            "spread_away_price": spread_away_price,
            "xg_home": None,
            "xg_away": None,
            "lineup_home_available": None,
            "lineup_away_available": None,
            "source_last_update": (bm[0].get("last_update") if bm else None)
        }
        out.append(row)
    return out

if __name__ == "__main__":
    events = load_any_today_json()
    if not events:
        print("no Odds API soccer events found today — run odds_api_pull.py first")
        sys.exit(0)
    rows = to_canonical(events)
    df = pd.DataFrame(rows)
    print("\ncanonical sample rows (first 8):")
    print(df.head(8).to_string(index=False))
    outdir = today_dir("canonical")
    outpath = outdir / "odds_api_canonical.csv"
    df.to_csv(outpath, index=False)
    print(f"\nsaved canonical csv → {outpath}")
