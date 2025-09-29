#!/usr/bin/env python3
import sys, requests, pandas as pd
from utils import UA, dump_json, print_fields

BASE = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"

def get(url):
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    return r

if __name__ == "__main__":
    try:
        comps = get(f"{BASE}/competitions.json").json()
        dump_json("statsbomb_open", "competitions.json", comps)
        print_fields("statsbomb competitions fields", comps)

        # pick a competition/season with events available
        comp = comps[0]
        cid, sid = comp["competition_id"], comp["season_id"]
        matches = get(f"{BASE}/matches/{cid}/{sid}.json").json()
        dump_json("statsbomb_open", f"matches_{cid}_{sid}.json", matches)
        print_fields("statsbomb matches fields", matches)

        if not matches:
            print("no matches in sample season")
            sys.exit(0)

        mid = matches[0]["match_id"]
        events = get(f"{BASE}/events/{mid}.json").json()
        dump_json("statsbomb_open", f"events_{mid}.json", events)

        df = pd.json_normalize(events)
        print(f"\nrows(events)={len(df)}, cols={len(df.columns)}")
        # show real values for a few key columns
        show = ["type.name","team.name","player.name","shot.statsbomb_xg","pass.length","pass.height","minute","second"]
        present = [c for c in show if c in df.columns]
        print("\nexample values:")
        print(df[present].head(8).to_string(index=False))

        print("\n✅ statsbomb_open_pull complete")
    except Exception as e:
        print("❌", repr(e))
        sys.exit(1)
