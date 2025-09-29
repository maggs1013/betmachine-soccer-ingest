#!/usr/bin/env python3
"""
API-Football (API-Sports) — Stage 6 connector

What this script does:
- Pulls fixtures for the next 14 days (league + season from .env)
- Attempts lineups for the first fixture (may be empty until close to kickoff)
- Pulls injuries for the last 14 days
- Prints fields and sample values for quick verification
- Saves raw JSON snapshots under data/raw/api_football/YYYY-MM-DD/

Env required (in .env or GitHub Actions env):
  APIFOOTBALL_BASE=https://v3.football.api-sports.io
  APIFOOTBALL_KEY=<your key>
  APIFOOTBALL_LEAGUE_ID=39
  APIFOOTBALL_SEASON=2024
"""

import sys, os, json, requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# import your shared utils (provides dump_json -> data/raw/<source>/<YYYY-MM-DD>/...)
from utils import dump_json

load_dotenv()

BASE   = os.getenv("APIFOOTBALL_BASE", "https://v3.football.api-sports.io")
KEY    = os.getenv("APIFOOTBALL_KEY", "")
LEAGUE = os.getenv("APIFOOTBALL_LEAGUE_ID", "39")  # EPL
SEASON = os.getenv("APIFOOTBALL_SEASON", "2024")

def headers():
    if not KEY:
        raise RuntimeError("Missing APIFOOTBALL_KEY in environment")
    return {
        "x-apisports-key": KEY,
        "User-Agent": "betmachine-stage6/api-football"
    }

def get(path, params=None):
    url = f"{BASE}{path}"
    r = requests.get(url, headers=headers(), params=params or {}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict) or "response" not in data:
        raise RuntimeError(f"Unexpected payload at {path}: {str(data)[:300]}")
    return data["response"], data  # (response list/dict, full payload)

def print_fields(title, items):
    print("\n" + "="*72)
    print(title)
    print("="*72)
    if isinstance(items, list) and items:
        sample = items[0]
        if isinstance(sample, dict):
            print("fields:", sorted(sample.keys()))
        print("sample:", json.dumps(sample, ensure_ascii=False)[:800], "...")
    elif isinstance(items, dict):
        print("fields:", sorted(items.keys()))
        print("sample:", json.dumps(items, ensure_ascii=False)[:800], "...")
    else:
        print("no items")

def fixtures_window():
    today = datetime.now(timezone.utc).date()
    params = {"league": LEAGUE, "season": SEASON, "from": str(today), "to": str(today + timedelta(days=14))}
    resp, full = get("/fixtures", params)
    # export raw snapshot
    dump_json("api_football", f"fixtures_future_{LEAGUE}_{SEASON}.json", full)
    print_fields("API-Football fixtures (future 14d)", resp)
    if resp:
        f0 = resp[0]
        fid = f0["fixture"]["id"]
        kickoff = f0["fixture"]["date"]
        home = f0["teams"]["home"]["name"]
        away = f0["teams"]["away"]["name"]
        print(f"\nFixture sample: id={fid} kickoff={kickoff}  {home} vs {away}")
        return fid
    return None

def lineups_for_fixture(fid: int):
    resp, full = get("/fixtures/lineups", {"fixture": fid})
    dump_json("api_football", f"lineups_fixture_{fid}.json", full)
    print_fields("API-Football lineups", resp)
    if resp:
        team = resp[0]["team"]["name"]
        formation = resp[0].get("formation")
        starts = len(resp[0].get("startXI", []))
        print(f"\nLineups sample: team={team} formation={formation} startXI={starts}")

def injuries_window():
    today = datetime.now(timezone.utc).date()
    resp, full = get("/injuries", {
        "league": LEAGUE,
        "season": SEASON,
        "from": str(today - timedelta(days=14)),
        "to":   str(today)
    })
    dump_json("api_football", f"injuries_{LEAGUE}_{SEASON}_last14d.json", full)
    print_fields("API-Football injuries (last 14d)", resp)
    if resp:
        p = resp[0]["player"]["name"]
        t = resp[0]["team"]["name"]
        typ = resp[0].get("type")
        print(f"\nInjury sample: player={p} team={t} type={typ}")

if __name__ == "__main__":
    try:
        first_fid = fixtures_window()
        if first_fid:
            try:
                lineups_for_fixture(first_fid)
            except Exception as e:
                # Often empty until close to kickoff; do not fail the job
                print("Lineups note (may be empty until close to kickoff):", e)
        injuries_window()
        print("\n✅ API-Football smoke test + exports complete")
    except Exception as e:
        print("❌", repr(e))
        sys.exit(1)
