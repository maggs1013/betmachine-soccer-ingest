#!/usr/bin/env python3
import sys, asyncio, pandas as pd
from utils import dump_json, print_fields

async def main():
    try:
        import aiohttp
        from understat import Understat
    except Exception as e:
        print("Install aiohttp and understat:", e)
        sys.exit(1)

    async with aiohttp.ClientSession() as session:
        u = Understat(session)
        data = await u.get_league_players("epl", 2024)  # EPL sample
        dump_json("understat", "epl_players_2024.json", data)
        print_fields("understat player fields", data)
        df = pd.DataFrame(data)
        if df.empty:
            print("no data returned")
            return

        # show real values for key xG fields
        cols = [c for c in ["player_name","team_title","games","xG","npxG","xA","xGChain","xGBuildup"] if c in df.columns]
        print("\nexample values:")
        print(df[cols].head(10).to_string(index=False))

if __name__ == "__main__":
    try:
        asyncio.run(main())
        print("\n✅ understat_pull complete")
    except Exception as e:
        print("❌", repr(e))
        sys.exit(1)
