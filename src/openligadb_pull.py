#!/usr/bin/env python3
import sys, requests, pandas as pd
from collections import Counter
from utils import UA, dump_json, print_fields, short_obs

URL = "https://api.openligadb.de/getmatchdata/bl1/2024"

if __name__ == "__main__":
    try:
        r = requests.get(URL, headers=UA, timeout=30)
        r.raise_for_status()
        data = r.json()
        dump_json("openligadb", "bl1_2024.json", data)
        if not data:
            print("no matches returned")
            sys.exit(0)

        print_fields("openligadb match item fields", [data[0]])

        # show some real values
        sample = []
        for m in data[:5]:
            res = (m.get("MatchResults") or [{}])[-1]
            sample.append({
                "MatchID": m.get("MatchID"),
                "DateUTC": m.get("MatchDateTimeUTC"),
                "Team1": (m.get("Team1") or {}).get("TeamName"),
                "Team2": (m.get("Team2") or {}).get("TeamName"),
                "Score": f"{res.get('PointsTeam1', 0)}-{res.get('PointsTeam2', 0)}"
            })
        print("\nsample normalized values:")
        print(pd.DataFrame(sample).to_string(index=False))

        dist = Counter()
        for m in data[:50]:
            res = (m.get("MatchResults") or [{}])[-1]
            dist[f"{res.get('PointsTeam1',0)}-{res.get('PointsTeam2',0)}"] += 1
        short_obs("scoreline counts (first 50)", [f"{k}: {v}" for k,v in dist.most_common(5)])

        print("\n✅ openligadb_pull complete")
    except Exception as e:
        print("❌", repr(e))
        sys.exit(1)
