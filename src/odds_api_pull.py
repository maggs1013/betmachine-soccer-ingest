#!/usr/bin/env python3
import sys, requests
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode
from dotenv import load_dotenv
from utils import env, UA, dump_json, print_fields, short_obs

load_dotenv()

BASE = "https://api.the-odds-api.com/v4"
API_KEY = env("ODDS_API_KEY", required=True)
REGIONS = env("ODDS_REGIONS", "us,uk,eu")
ODDS_FORMAT = env("ODDS_FORMAT", "decimal")
SPORT_KEYS = [s.strip() for s in env("ODDS_SPORT_KEYS", "soccer_epl").split(",") if s.strip()]

def get(path, params=None):
    params = params.copy() if params else {}
    params["apiKey"] = API_KEY
    url = f"{BASE}{path}?{urlencode(params)}"
    r = requests.get(url, headers=UA, timeout=25)
    r.raise_for_status()
    return r.json()

def list_sports():
    data = get("/sports")
    # filter to soccer only
    data = [s for s in data if str(s.get("key","")).startswith("soccer_")]
    dump_json("odds_api", "sports_soccer.json", data)
    print_fields("The Odds API /sports (soccer only)", data)
    sample = [f"{x.get('key')} | {x.get('group')} | {x.get('title')}" for x in data[:12]]
    short_obs("sample soccer sports", sample)
    return data

def fetch_odds(sport_key, markets="h2h,spreads,totals"):
    data = get(f"/sports/{sport_key}/odds", {
        "regions": REGIONS,
        "oddsFormat": ODDS_FORMAT,
        "markets": markets
    })
    dump_json("odds_api", f"odds_{sport_key}.json", data)
    if not data:
        print(f"no events returned for {sport_key}")
        return
    print_fields(f"{sport_key} event top-level fields", data)

    # demo values from first event
    ev = data[0]
    lines = [
        f"id={ev.get('id')}",
        f"commence_time={ev.get('commence_time')}",
        f"home={ev.get('home_team')}",
        f"away={ev.get('away_team')}",
        f"bookmakers={len(ev.get('bookmakers', []))}"
    ]
    short_obs("event sanity", lines)

    # bookmaker/market/outcome sample values
    if ev.get("bookmakers"):
        bm = ev["bookmakers"][0]
        short_obs("bookmaker sample", [
            f"title={bm.get('title')}",
            f"last_update={bm.get('last_update')}",
            f"markets={len(bm.get('markets', []))}"
        ])
        if bm.get("markets"):
            m = bm["markets"][0]
            out_lines = [
                f"market.key={m.get('key')} last_update={m.get('last_update')}",
                f"outcomes={len(m.get('outcomes', []))}"
            ]
            if m.get("outcomes"):
                o = m["outcomes"][0]
                out_lines.append(f"outcome: name={o.get('name')} price={o.get('price')} point={o.get('point')}")
            short_obs("market sample", out_lines)

def try_historical_if_enabled(sport_key):
    # If your plan includes historical snapshots this succeeds; otherwise prints a friendly note.
    try:
        events = get(f"/sports/{sport_key}/odds", {
            "regions": REGIONS, "oddsFormat": ODDS_FORMAT, "markets": "h2h"
        })
        if not events:
            print("no current events to demo historical snapshot")
            return
        event_id = events[0]["id"]
        # request “yesterday” snapshot
        date_param = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        data = get(f"/historical/sports/{sport_key}/events/{event_id}/odds", {
            "date": date_param, "regions": REGIONS, "oddsFormat": ODDS_FORMAT, "markets": "h2h"
        })
        dump_json("odds_api", f"historical_{sport_key}_{event_id}.json", data)
        short_obs("historical snapshot (if enabled)", [
            f"bookmakers={len(data.get('bookmakers', []))}",
            f"date={date_param}"
        ])
    except requests.HTTPError as e:
        short_obs("historical not enabled/available", [str(e)])

if __name__ == "__main__":
    try:
        list_sports()
        for sk in SPORT_KEYS:
            fetch_odds(sk)
            try_historical_if_enabled(sk)
        print("\n✅ odds_api_pull complete (soccer only)")
    except Exception as e:
        print("❌", repr(e))
        sys.exit(1)
