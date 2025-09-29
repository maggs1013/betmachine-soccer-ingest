#!/usr/bin/env python3
import sys, requests, pandas as pd
from collections import Counter
from utils import UA, dump_json, print_fields, short_obs

URL = "https://api.openligadb.de/getmatchdata/bl1/2024"

def last_result(m):
    # Handle both 'matchResults' and 'MatchResults'
    results = m.get("matchResults") or m.get("MatchResults") or []
    if isinstance(results, list) and results:
        return results[-1]
    return {}

def get_val(obj, *keys, default=None):
    # Try several casings/paths
    for k in keys:
        if obj and isinstance(obj, dict) and k in obj:
            return obj[k]
    return default

if __name__ == "__main__":
    try:
        r = requests.get(URL, headers=UA, timeout=30)
        r.raise_for_status()
        data = r.json()
        dump_json("openligadb", "bl1_2024.json", data)
        if not data:
            print("no matches returned")
            sys.exit(0)

        # Show fields on the first item
        print_fields("openligadb match item fields", [data[0]])

        # Build a clean sample table with proper casing
        rows = []
        for m in data[:5]:
            res = last_result(m)
            rows.append({
                "MatchID": get_val(m, "matchID", "MatchID"),
                "DateUTC": get_val(m, "matchDateTimeUTC", "MatchDateTimeUTC"),
                "Team1": get_val(get_val(m, "team1", "Team1", default={}), "teamName", "TeamName"),
                "Team2": get_val(get_val(m, "team2", "Team2", default={}), "teamName", "TeamName"),
                "Score": f"{get_val(res, 'pointsTeam1', 'PointsTeam1', default=0)}-"
                         f"{get_val(res, 'pointsTeam2', 'PointsTeam2', default=0)}"
            })
        print("\nsample normalized values:")
        print(pd.DataFrame(rows).to_string(index=False))

        # Scoreline distribution (first 50)
        dist = Counter()
        for m in data[:50]:
            res = last_result(m)
            h = get_val(res, 'pointsTeam1', 'PointsTeam1', default=0)
            a = get_val(res, 'pointsTeam2', 'PointsTeam2', default=0)
            dist[f"{h}-{a}"] += 1
        short_obs("scoreline counts (first 50)", [f"{k}: {v}" for k,v in dist.most_common(5)])

        print("\n✅ openligadb_pull complete")
    except Exception as e:
        print("❌", repr(e))
        sys.exit(1)
