#!/usr/bin/env python3
import sys, requests, pandas as pd
from utils import env, UA, dump_text, print_fields

URL = env("FBREF_LEAGUE_URL", "https://fbref.com/en/comps/9/stats/Premier-League-Stats")

if __name__ == "__main__":
    try:
        r = requests.get(URL, headers=UA, timeout=30)
        r.raise_for_status()
        html = r.text
        dump_text("fbref", "page.html", html)

        tables = pd.read_html(html, header=1)
        df = max(tables, key=lambda t: t.shape[0] * t.shape[1])
        print(f"fbref tables found={len(tables)}, chosen rows={len(df)}, cols={len(df.columns)}")
        # print columns and a few real values
        print_fields("fbref chosen table columns", [df.head(1).to_dict(orient="records")[0]])
        print("\nexample values:")
        print(df.head(5).to_string(index=False))

        print("\n✅ fbref_pull complete")
    except Exception as e:
        print("❌", repr(e))
        sys.exit(1)
