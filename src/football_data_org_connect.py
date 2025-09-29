#!/usr/bin/env python3
import sys, os, json, requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()
BASE = os.getenv("FOOTBALLDATA_BASE", "https://api.football-data.org/v4")
TOKEN = os.getenv("FOOTBALLDATA_TOKEN", "")

HDR = {"X-Auth-Token": TOKEN, "User-Agent":"betmachine-stage6"}

def get(path, params=None):
    if not TOKEN:
        raise RuntimeError("Missing FOOTBALLDATA_TOKEN")
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
        print("fields:", sorted(sample.keys()))
        print("sample:", json.dumps(sample, ensure_ascii=False)[:600], "...")
    elif isinstance(items, dict):
        print("fields:", sorted(items.keys()))
        print("sample:", json.dumps(items, ensure_ascii=False)[:600], "...")
    else:
        print("no items")

def comps():
    data = get("/competitions")
    comps = data.get("competitions", [])
    print_fields("FD.org competitions", comps)
    return comps

def matches_window(code="PL"):
    today = datetime.now(timezone.utc).date()
    fut = get(f"/competitions/{code}/matches", {"dateFrom": str(today), "dateTo": str(today + timedelta(days=14))})
    print_fields(f"FD.org matches future 14d ({code})", fut.get("matches", []))
    pst = get(f"/competitions/{code}/matches", {"dateFrom": str(today - timedelta(days=30)), "dateTo": str(today)})
    print_fields(f"FD.org matches past 30d ({code})", pst.get("matches", []))

def standings(code="PL"):
    st = get(f"/competitions/{code}/standings")
    print_fields(f"FD.org standings ({code})", st.get("standings", []))

def scorers(code="PL"):
    sc = get(f"/competitions/{code}/scorers", {"limit": 20})
    print_fields(f"FD.org scorers ({code})", sc.get("scorers", []))

if __name__ == "__main__":
    try:
        cs = comps()
        matches_window("PL")
        standings("PL")
        scorers("PL")
        print("\n✅ Football-Data.org smoke test complete")
    except Exception as e:
        print("❌", repr(e))
        sys.exit(1)
