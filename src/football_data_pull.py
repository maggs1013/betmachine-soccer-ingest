#!/usr/bin/env python3
import sys, io, requests, pandas as pd
from utils import UA, dump_text, print_fields, short_obs

# Example: Premier League 2024/25 = E0. Change for other leagues/years as needed.
URL = "https://www.football-data.co.uk/mmz4281/2425/E0.csv"

if __name__ == "__main__":
    try:
        r = requests.get(URL, headers=UA, timeout=25)
        r.raise_for_status()
        dump_text("football_data", "E0_2425.csv", r.text)
        df = pd.read_csv(io.StringIO(r.text))
        print(f"\nfootball-data rows={len(df)}, cols={len(df.columns)}")
        print_fields("football-data columns (soccer)", [df.head(1).to_dict(orient="records")[0]])
        for c in ["PSCH","PSCD","PSCA","B365H","B365D","B365A","WHH","WHD","WHA"]:
            if c in df.columns:
                short_obs("odds column sample", [f"{c} non-null={df[c].notna().sum()}"])
        print("\n✅ football_data_pull complete")
    except Exception as e:
        print("❌", repr(e))
        sys.exit(1)
