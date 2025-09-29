#!/usr/bin/env python3
"""
Football-Data.org v4 — Stage 6 connector

What this script does:
- Lists competitions (v4)
- Pulls matches (future 14 days and past 30 days) for a competition code (e.g., PL)
- Pulls standings and scorers
- Prints fields and sample values for verification
- Saves raw JSON snapshots under data/raw/footballdata_org/YYYY-MM-DD/

Env required (in .env or GitHub Actions env):
  FOOTBALLDATA_BASE=https://api.football-data.org/v4
  FOOTBALLDATA_TOKEN=<your token>
"""

import sys, os, json, requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

from utils import dump_json

load_dotenv()

BASE  = os.getenv("FOOTBALLDATA_BASE", "https://api.football-data.org/v4")
TOKEN = os.getenv("FOOTBALLDATA_TOKEN", "")

HDR = {
    "X-Auth-Token": TOKEN,
    "User-Agent": "betmachine-stage6/football-data.org"
}

def get(path, params=None):
    if not TOKEN:
        raise RuntimeError("Missing FOOTBALLDATA_TOKEN in environment")
    url = f"{BASE}{path}"
    r = requests.get(url, headers=HDR, params=params or {}, timeout=30)
    r.raise_for_status()
    return r.json()

def print_fields(title, items):
    print("\n" + "="*72)
    print(title)
    print("="*72)
    if isinstance(items, list) and items:
        sample = items[0]
        if isinstance(sample, dict):
            print("fields:", sorted(sample.keys()))
        print("sample:", json.dumps(sample, ensure_ascii=False)[:600], "...")
    elif isinstance(items, dict):
        print("fields:", sorted(items.keys()))
        print("sample:", json.dumps(items, ensure_ascii=False)[:600], "...")
    else:
        print("no items")

def comps():
    data = get("/competitions")
    dump_json("footballdata_org", "competitions.json", data)
    comps = data.get("competitions", [])
    print_fields("FD.org competitions", comps)
    return comps

def matches_window(code="PL"):
    today = datetime.now(timezone.utc).date()

    fut = get(f"/competitions/{code}/matches", {"dateFrom": str(today), "dateTo": str(today + timedelta(days=14))})
    dump_json("footballdata_org", f"matches_future_{code}.json", fut)
    print_fields(f"FD.org matches future 14d ({code})", fut.get("matches", []))

    pst = get(f"/competitions/{code}/matches", {"dateFrom": str(today - timedelta(days=30)), "dateTo": str(today)})
    dump_json("footballdata_org", f"matches_past_{code}.json", pst)
    print_fields(f"FD.org matches past 30d ({code})", pst.get("matches", []))

def standings(code="PL"):
    st = get(f"/competitions/{code}/standings")
    dump_json("footballdata_org", f"standings_{code}.json", st)
    print_fields(f"FD.org standings ({code})", st.get("standings", []))

def scorers(code="PL"):
    sc = get(f"/competitions/{code}/scorers", {"limit": 20})
    dump_json("footballdata_org", f"scorers_{code}.json", sc)
    print_fields(f"FD.org scorers ({code})", sc.get("scorers", []))

if __name__ == "__main__":
    try:
        cs = comps()
        # Premier League (PL) as default probe; adjust/add codes if you want more
        matches_window("PL")
        standings("PL")
        scorers("PL")
        print("\n✅ Football-Data.org smoke test + exports complete")
    except Exception as e:
        print("❌", repr(e))
        sys.exit(1)
