#!/usr/bin/env python3
"""
API-Football (API-Sports) probe:
- Lists leagues/coverage
- Pulls fixtures (past + future window)
- Pulls lineups/injuries if available
- Pulls odds if your plan supports it
- Prints field names AND example values
- Saves raw JSON to data/raw/api_football/YYYY-MM-DD/

Auth: either:
  Direct API-Sports -> header: x-apisports-key
  RapidAPI          -> headers: X-RapidAPI-Key + X-RapidAPI-Host
"""

import sys, os, json, time, requests
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from dotenv import load_dotenv
from utils import env, UA, dump_json, print_fields, short_obs

load_dotenv()

BASE     = env("APIFOOTBALL_BASE", "https://v3.football.api-sports.io")
API_KEY  = env("APIFOOTBALL_KEY", "")
RKEY     = env("APIFOOTBALL_RAPIDAPI_KEY", "")
RHOST    = env("APIFOOTBALL_RAPIDAPI_HOST", "")
LEAGUE   = env("APIFOOTBALL_LEAGUE_ID", "39")  # EPL example
SEASON   = env("APIFOOTBALL_SEASON", "2024")
LIMIT    = int(env("PAGINATION_LIMIT", "3"))

def headers():
    h = dict(UA)
    if API_KEY:
        h["x-apisports-key"] = API_KEY
    elif RKEY and RHOST:
        h["X-RapidAPI-Key"] = RKEY
        h["X-RapidAPI-Host"] = RHOST
    else:
        raise RuntimeError("No API-Football credentials found. Set APIFOOTBALL_KEY or RapidAPI keys.")
    return h

def get(path, params=None):
    url = f"{BASE}{path}"
    r = requests.get(url, params=params or {}, headers=headers(), timeout=30)
    r.raise_for_status()
    obj = r.json()
    if not isinstance(obj, dict) or "response" not in obj:
        raise RuntimeError(f"Unexpected API-Football payload at {path}")
    return obj

def list_leagues():
    data = get("/leagues", {"season": SEASON})
    dump_json("api_football", f"leagues_{SEASON}.json", data)
    resp = data.get("response", [])
    print_fields("API-Football /leagues fields", resp)
    # show a few leagues
    sample = []
    for x in resp[:10]:
        lg = x.get("league", {})
        cn = (x.get("country") or {}).get("name")
        sample.append(f"{lg.get('id')} | {lg.get('name')} | {cn} | {lg.get('type')} | seasons={len(x.get('seasons',[]))}")
    short_obs("sample leagues", sample)

def fixtures_window():
    # FUTURE: next 14 days
    today = datetime.now(timezone.utc).date()
    end   = today + timedelta(days=14)
    params_future = {"league": LEAGUE, "season": SEASON, "from": str(today), "to": str(end)}
    fut = get("/fixtures", params_future)
    dump_json("api_football", f"fixtures_future_{LEAGUE}_{SEASON}.json", fut)
    resp = fut.get("response", [])
    print_fields("fixtures (future 14d) fields", resp)
    if resp:
        f0 = resp[0]
        fix = f0.get("fixture", {})
        tm  = f0.get("teams", {})
        short_obs("fixture sample (future)", [
            f"id={fix.get('id')} date={fix.get('date')} status={fix.get('status',{}).get('short')}",
            f"home={tm.get('home',{}).get('name')} away={tm.get('away',{}).get('name')}"
        ])
    else:
        short_obs("fixtures (future)", ["no fixtures returned (offseason or league id/season mismatch)"])

    # PAST: last 30 days
    start = today - timedelta(days=30)
    params_past = {"league": LEAGUE, "season": SEASON, "from": str(start), "to": str(today)}
    pst = get("/fixtures", params_past)
    dump_json("api_football", f"fixtures_past_{LEAGUE}_{SEASON}.json", pst)
    resp2 = pst.get("response", [])
    print_fields("fixtures (past 30d) fields", resp2)
    if resp2:
        f0 = resp2[0]
        fix = f0.get("fixture", {})
        goals = f0.get("goals",{})
        short_obs("fixture sample (past)", [
            f"id={fix.get('id')} date={fix.get('date')} score={goals.get('home')}-{goals.get('away')}"
        ])

def lineups_for_first_future_fixture():
    # need a fixture id
    fut = json.loads(open(next(iter(sorted((os.path.join("data","raw","api_football",d,f)) 
                 for d in os.listdir("data/raw/api_football") if os.path.isdir(f"data/raw/api_football/{d}")
                 for f in os.listdir(f"data/raw/api_football/{d}") if f.startswith("fixtures_future")), None)),"r",encoding="utf-8").read())
    fixtures = fut.get("response", [])
    if not fixtures:
        short_obs("lineups", ["no future fixtures cached locally; skipping lineups"])
        return
    fid = fixtures[0].get("fixture",{}).get("id")
    if not fid:
        short_obs("lineups", ["no fixture id found"])
        return
    data = get("/fixtures/lineups", {"fixture": fid})
    dump_json("api_football", f"lineups_fixture_{fid}.json", data)
    resp = data.get("response", [])
    print_fields("lineups fields", resp)
    if resp:
        t = resp[0]
        short_obs("lineups sample", [
            f"team={t.get('team',{}).get('name')}",
            f"formation={t.get('formation')}",
            f"startXI_count={len(t.get('startXI',[]))}",
            f"subs_count={len(t.get('substitutes',[]))}"
        ])

def injuries_recent_window():
    # injuries endpoint often limited by plan; try last 14 days by date filter if supported
    try:
        today = datetime.now(timezone.utc).date()
        start = today - timedelta(days=14)
        data = get("/injuries", {"league": LEAGUE, "season": SEASON, "from": str(start), "to": str(today)})
        dump_json("api_football", f"injuries_{LEAGUE}_{SEASON}.json", data)
        resp = data.get("response", [])
        print_fields("injuries fields", resp)
        if resp:
            r0 = resp[0]
            ply = r0.get("player", {})
            t   = r0.get("team", {})
            short_obs("injury sample", [
                f"player={ply.get('name')} | team={t.get('name')} | type={r0.get('type')} | reason={r0.get('reason')}"
            ])
        else:
            short_obs("injuries", ["no injuries returned in last 14d (or endpoint restricted)"])
    except requests.HTTPError as e:
        short_obs("injuries", [f"not available on your plan / {e}"])

def odds_demo_if_enabled():
    # Try main odds endpoint (plan-dependent)
    try:
        data = get("/odds", {"league": LEAGUE, "season": SEASON})
        dump_json("api_football", f"odds_{LEAGUE}_{SEASON}.json", data)
        resp = data.get("response", [])
        print_fields("odds fields (API-Football)", resp)
        if resp:
            r0 = resp[0]
            bks = r0.get("bookmakers", [])
            short_obs("odds sample", [
                f"fixture={r0.get('fixture',{}).get('id')} bookmakers={len(bks)}",
                f"first_bookmaker={bks[0].get('name') if bks else None}"
            ])
        else:
            short_obs("odds", ["no odds returned (league/season/plan)"])
    except requests.HTTPError as e:
        short_obs("odds", [f"not available on your plan / {e}"])

if __name__ == "__main__":
    try:
        list_leagues()
        fixtures_window()
        # Optional deep endpoints:
        try:
            lineups_for_first_future_fixture()
        except Exception:
            short_obs("lineups", ["skipped (no cached future fixture or endpoint not available)"])
        injuries_recent_window()
        odds_demo_if_enabled()
        print("\n✅ api_football_pull complete")
    except Exception as e:
        print("❌", repr(e))
        sys.exit(1)
