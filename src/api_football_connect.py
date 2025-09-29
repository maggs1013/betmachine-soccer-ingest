#!/usr/bin/env python3
import sys, os, json, requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

BASE = os.getenv("APIFOOTBALL_BASE", "https://v3.football.api-sports.io")
KEY  = os.getenv("APIFOOTBALL_KEY", "")
LEAGUE = os.getenv("APIFOOTBALL_LEAGUE_ID", "39")   # EPL default
SEASON = os.getenv("APIFOOTBALL_SEASON", "2024")

def headers():
    if not KEY:
        raise RuntimeError("Missing APIFOOTBALL_KEY in .env")
    return {"x-apisports-key": KEY, "User-Agent":"betmachine-stage6"}

def get(path, params=None):
    url = f"{BASE}{path}"
    r = requests.get(url, headers=headers(), params=params or {}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "response" not in data:
        raise RuntimeError(f"Unexpected payload: {data}")
    return data["response"]

def print_fields(title, items):
    print("\n" + "="*72)
    print(title)
    print("="*72)
    if isinstance(items, list) and items:
        sample = items[0]
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
    fx = get("/fixtures", params)
    print_fields("API-Football fixtures (future 14d)", fx)
    if fx:
        f0 = fx[0]
        fid = f0["fixture"]["id"]
        kickoff = f0["fixture"]["date"]
        home = f0["teams"]["home"]["name"]
        away = f0["teams"]["away"]["name"]
        print(f"\nFixture sample: id={fid} kickoff={kickoff} {home} vs {away}")
    return fx

def lineups_for_fixture(fid):
    ln = get("/fixtures/lineups", {"fixture": fid})
    print_fields("API-Football lineups", ln)
    if ln:
        team = ln[0]["team"]["name"]
        formation = ln[0].get("formation")
        starts = len(ln[0].get("startXI", []))
        print(f"\nLineups sample: team={team} formation={formation} startXI={starts}")
    return ln

def injuries_window():
    today = datetime.now(timezone.utc).date()
    inj = get("/injuries", {"league": LEAGUE, "season": SEASON, "from": str(today - timedelta(days=14)), "to": str(today)})
    print_fields("API-Football injuries (last 14d)", inj)
    if inj:
        p = inj[0]["player"]["name"]; t = inj[0]["team"]["name"]; typ = inj[0].get("type")
        print(f"\nInjury sample: player={p} team={t} type={typ}")
    return inj

if __name__ == "__main__":
    try:
        fx = fixtures_window()
        if fx:
            fid = fx[0]["fixture"]["id"]
            try:
                lineups_for_fixture(fid)
            except Exception as e:
                print("Lineups fetch note (may be empty until close to kickoff):", e)
        injuries_window()
        print("\n✅ API-Football smoke test complete")
    except Exception as e:
        print("❌", repr(e))
        sys.exit(1)
