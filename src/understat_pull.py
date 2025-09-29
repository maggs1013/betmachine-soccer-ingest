#!/usr/bin/env python3
"""
Understat (no official API) — robust HTML scraper:
- Fetches league page (e.g., EPL 2024)
- Extracts embedded JSON (works for both legacy playersData and __NUXT__ payloads)
- Prints fields and real values (xG, xA, etc.)
- Saves raw JSON to data/raw/understat/YYYY-MM-DD/

Change LEAGUE / SEASON if you want a different league.
Common league slugs: EPL, La_Liga, Bundesliga, Serie_A, Ligue_1, RFPL (RPL)
"""

import sys, json, re, html
from pathlib import Path
import requests
import pandas as pd
from bs4 import BeautifulSoup
from utils import UA, dump_json, today_dir, short_obs, print_fields

LEAGUE = "EPL"     # change if desired
SEASON = "2024"    # year-like season label on Understat

BASE = "https://understat.com"

def fetch_league_html(league: str, season: str) -> str:
    url = f"{BASE}/league/{league}/{season}"
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    return r.text

def extract_json_payloads(html_text: str):
    """
    Understat has used a few formats over time:
    1) Legacy: window.__NUXT__ = {...}
    2) Legacy script with JSON.parse('...') for variables like "playersData"
    We attempt both and return a dict with any found keys.
    """
    soup = BeautifulSoup(html_text, "lxml")
    scripts = soup.find_all("script")

    payload = {}

    # Try __NUXT__ first (newer Nuxt payload)
    nuxt_pat = re.compile(r"window\.__NUXT__\s*=\s*(\{.*?\});", re.DOTALL)
    for sc in scripts:
        m = nuxt_pat.search(sc.text or "")
        if m:
            try:
                payload["__NUXT__"] = json.loads(m.group(1))
            except Exception:
                pass

    # Try legacy JSON.parse('...') blocks (playersData / teamsData / matchesData)
    parse_pat = re.compile(r"JSON\.parse\('([^']+)'\)")
    for sc in scripts:
        txt = sc.text or ""
        if "playersData" in txt or "teamsData" in txt or "matchesData" in txt:
            # Extract key name by a loose heuristic
            varname = None
            if "playersData" in txt: varname = "playersData"
            elif "teamsData" in txt: varname = "teamsData"
            elif "matchesData" in txt: varname = "matchesData"

            m = parse_pat.search(txt)
            if m and varname:
                try:
                    decoded = html.unescape(m.group(1))
                    payload[varname] = json.loads(decoded)
                except Exception:
                    # sometimes arrays/objects are outside JSON.parse; try a broader fallback
                    pass

    return payload

def pick_players_table(payload: dict) -> pd.DataFrame:
    """
    Try to locate a players array with xG/xA stats. We probe known spots.
    Returns empty DataFrame if nothing found.
    """
    # 1) Direct playersData (legacy)
    if "playersData" in payload and isinstance(payload["playersData"], list) and payload["playersData"]:
        return pd.DataFrame(payload["playersData"])

    # 2) Nuxt payload likely nests under something like:
    # payload["__NUXT__"]["data"][0]["playersData"]  OR
    # payload["__NUXT__"]["data"][0]["league"]["players"] ...
    nuxt = payload.get("__NUXT__")
    if isinstance(nuxt, dict):
        # Walk common paths
        data = nuxt.get("data")
        if isinstance(data, list):
            for block in data:
                # direct playersData
                if isinstance(block, dict) and "playersData" in block and isinstance(block["playersData"], list):
                    return pd.DataFrame(block["playersData"])
                # nested under league/players
                if isinstance(block, dict):
                    league_obj = block.get("league") or block.get("state") or {}
                    # brute-force scan for any list with dicts containing 'xG'
                    stack = [league_obj, block]
                    seen = set()
                    while stack:
                        cur = stack.pop()
                        if id(cur) in seen:
                            continue
                        seen.add(id(cur))
                        if isinstance(cur, dict):
                            for v in cur.values():
                                stack.append(v)
                        elif isinstance(cur, list) and cur and isinstance(cur[0], dict):
                            # if a list of dicts has key 'xG' or 'xA' or 'player_name', assume it's players
                            keys = set(cur[0].keys())
                            if {"xG", "xA"} & keys or {"player_name", "player"} & keys:
                                try:
                                    return pd.DataFrame(cur)
                                except Exception:
                                    pass
    # nothing found
    return pd.DataFrame()

if __name__ == "__main__":
    try:
        html_text = fetch_league_html(LEAGUE, SEASON)
        payloads = extract_json_payloads(html_text)

        # Save raw payloads we found (for audit)
        outdir = today_dir("understat")
        if payloads:
            dump_json("understat", f"understat_{LEAGUE}_{SEASON}_payload.json", payloads)
        else:
            print("No recognizable JSON payload found on the page; site structure may have changed.")

        # Try to form a players table
        df = pick_players_table(payloads)
        if df.empty:
            print("\nNo players table found. Consider trying another league/season, or updating the parser.")
            sys.exit(0)

        # Print fields and a few real values (xG/xA etc.)
        print(f"\nunderstat {LEAGUE} {SEASON}: rows={len(df)}, cols={len(df.columns)}")
        # show columns
        if not df.empty:
            print_fields("understat columns", [df.head(1).to_dict(orient="records")[0]])

        # Friendly display of common fields if present
        candidates = [c for c in ["player_name","team_title","games","xG","npxG","xA","xGChain","xGBuildup","shots","time"] if c in df.columns]
        if candidates:
            print("\nexample values:")
            print(df[candidates].head(10).to_string(index=False))

        # Tiny observation: top-5 by xG if available
        if "xG" in df.columns:
            top = df.sort_values("xG", ascending=False).head(5)
            short_obs("top-5 by xG (sample)", [f"{row.get('player_name','?')} xG={row['xG']:.2f}" for _, row in top.iterrows()])

        print("\n✅ understat_pull complete (scrape mode)")
    except Exception as e:
        print("❌", repr(e))
        sys.exit(1)
