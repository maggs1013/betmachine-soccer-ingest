#!/usr/bin/env python3
"""
Football-Data.org official REST (v4):
- Lists competitions
- Pulls matches by competition & date window (past/future)
- Prints fields + example values
- Saves raw JSON to data/raw/footballdata/YYYY-MM-DD/

Auth: header X-Auth-Token
Note: Odds are limited; this API focuses on fixtures, results, standings, scorers, etc.
"""

import sys, os, json, requests
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from dotenv import load_dotenv
from utils import env, UA, dump_json, print_fields, short_obs

load_dotenv()

BASE   = env("FOOTBALLDATA_BASE", "https://api.football-data.org/v4")
TOKEN  = env("FOOTBALLDATA_TOKEN", required=True)
LIMIT  = int(env("PAGINATION_LIMIT", "3"))

def headers():
    h = dict(UA)
    h["X-Auth-Token"] = TOKEN
    return h

def get(path, params=None):
    url = f"{BASE}{path}"
    r = requests.get(url, params=params or {}, headers=headers(), timeout=30)
    r.raise_for_status()
    return r.json()

def list_competitions():
    data = get("/competitions")
    dump_json("footballdata", "competitions.json", data)
    comps = data.get("competitions", [])
    print_fields("FD.org /competitions fields", comps)
    sample = [f"{c.get('code')} | {c.get('name')} | {c.get('area',{}).get('name')} | type={c.get('type')}" for c in comps[:10]]
    short_obs("sample competitions", sample)
    # choose some common ones (EPL=PL, CL=CL, Bundesliga=BL1, LaLiga=PD, Serie A=SA)
    return [x for x in comps if x.get("code") in ("PL","BL1","PD","SA","CL")]

def matches_window(code="PL"):
    today = datetime.now(timezone.utc).date()
    # future next 14d
    data_fut = get(f"/competitions/{code}/matches", {"dateFrom": str(today), "dateTo": str(today + timedelta(days=14))})
    dump_json("footballdata", f"matches_future_{code}.json", data_fut)
    matches = data_fut.get("matches", [])
    print_fields(f"FD.org matches future (14d) {code}", matches)
    if matches:
        m = matches[0]
        short_obs("match sample (future)", [
            f"id={m.get('id')} | utcDate={m.get('utcDate')} | status={m.get('status')}",
            f"home={m.get('homeTeam',{}).get('name')} | away={m.get('awayTeam',{}).get('name')}"
        ])
    else:
        short_obs("matches future", [f"{code}: none returned (offseason?)"])

    # past last 30d
    data_pst = get(f"/competitions/{code}/matches", {"dateFrom": str(today - timedelta(days=30)), "dateTo": str(today)})
    dump_json("footballdata", f"matches_past_{code}.json", data_pst)
    matches2 = data_pst.get("matches", [])
    print_fields(f"FD.org matches past (30d) {code}", matches2)
    if matches2:
        m = matches2[0]
        full = m.get("score",{}).get("fullTime",{})
        short_obs("match sample (past)", [
            f"id={m.get('id')} | utcDate={m.get('utcDate')} | FT={full.get('home')}-{full.get('away')}"
        ])

def standings_and_scorers(code="PL"):
    try:
        st = get(f"/competitions/{code}/standings")
        dump_json("footballdata", f"standings_{code}.json", st)
        print_fields(f"FD.org standings {code}", st.get("standings", []))
    except requests.HTTPError as e:
        short_obs("standings", [f"{code}: {e}"])

    try:
        sc = get(f"/competitions/{code}/scorers", {"limit": 20})
        dump_json("footballdata", f"scorers_{code}.json", sc)
        print_fields(f"FD.org scorers {code}", sc.get("scorers", []))
        scs = sc.get("scorers", [])
        if scs:
            s0 = scs[0]
            short_obs("top scorer sample", [
                f"player={s0.get('player',{}).get('name')} | team={s0.get('team',{}).get('name')} | goals={s0.get('goals')}"
            ])
    except requests.HTTPError as e:
        short_obs("scorers", [f"{code}: {e}"])

if __name__ == "__main__":
    try:
        comps = list_competitions()
        for c in comps[:3]:  # probe a few popular comps
            code = c.get("code")
            matches_window(code)
            standings_and_scorers(code)
        print("\n✅ football_data_org_pull complete")
    except Exception as e:
        print("❌", repr(e))
        sys.exit(1)
