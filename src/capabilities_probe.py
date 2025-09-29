#!/usr/bin/env python3
"""
Summarize today's capabilities across sources:
- counts of items pulled
- whether certain key fields exist (odds, xG, lineups, injuries)
"""

import json
from pathlib import Path
from utils import short_obs

ROOT = Path("data/raw")

def count_json_items(dirpath: Path, name_contains=None):
    if not dirpath.exists():
        return 0
    dated_dirs = sorted([p for p in dirpath.iterdir() if p.is_dir()])
    if not dated_dirs:
        return 0
    latest = dated_dirs[-1]
    total = 0
    for p in latest.glob("*.json"):
        if name_contains and name_contains not in p.name:
            continue
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(obj, list): total += len(obj)
            elif isinstance(obj, dict):
                # heuristic: common list field names
                for k in ("response","matches","competitions","standings","scorers"):
                    if isinstance(obj.get(k), list):
                        total += len(obj[k])
                        break
        except Exception:
            pass
    return total

if __name__ == "__main__":
    rows = []
    rows.append(f"odds_api events: {count_json_items(ROOT/'odds_api')}")
    rows.append(f"football_data rows: {count_json_items(ROOT/'football_data')}")
    rows.append(f"statsbomb_open objects: {count_json_items(ROOT/'statsbomb_open')}")
    rows.append(f"understat rows: {count_json_items(ROOT/'understat')}")
    rows.append(f"fbref files: {len(list((ROOT/'fbref').glob('*/*.*'))) if (ROOT/'fbref').exists() else 0}")
    rows.append(f"openligadb matches: {count_json_items(ROOT/'openligadb')}")
    rows.append(f"api_football fixtures: {count_json_items(ROOT/'api_football','fixtures')}")
    rows.append(f"api_football injuries: {count_json_items(ROOT/'api_football','injuries')}")
    rows.append(f"api_football odds: {count_json_items(ROOT/'api_football','odds')}")
    rows.append(f"footballdata matches: {count_json_items(ROOT/'footballdata','matches')}")
    rows.append(f"footballdata standings: {count_json_items(ROOT/'footballdata','standings')}")
    rows.append(f"footballdata scorers: {count_json_items(ROOT/'footballdata','scorers')}")
    short_obs("capabilities summary (today)", rows)
