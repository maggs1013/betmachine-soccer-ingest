#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

NORM = Path("data/normalized")
RAW  = Path("data/raw")
OUTJ = Path("data/joined"); OUTJ.mkdir(parents=True, exist_ok=True)

# Inputs
fx  = pd.read_parquet(NORM/"api_football_fixtures.parquet")            # fixtures (kickoff + teams canonical)
fdm = pd.read_parquet(NORM/"fdorg_matches.parquet")                    # future/past matches (kickoff + FT scores)
inj = (pd.read_parquet(NORM/"api_football_injuries.parquet")
       if (NORM/"api_football_injuries.parquet").exists() else pd.DataFrame())

# Canonical odds (Stage 5)
# Use the latest dated folder for canonical joined odds if you roll them by day;
# here we assume normalize_soccer wrote: data/raw/canonical/<date>/odds_api_canonical.csv
cand_dirs = sorted((RAW/"canonical").glob("*"))
odds = pd.read_csv(next(cand_dirs[-1].glob("odds_api_canonical.csv"))) if cand_dirs else pd.read_csv("data/raw/canonical/odds_api_canonical.csv")

# Prepare keys
def norm_name(s): 
    return (s or "").strip().lower()

fx = fx.rename(columns={"home_team_canonical":"home_canon","away_team_canonical":"away_canon"})
fdm = fdm.rename(columns={"home_team_canonical":"home_canon","away_team_canonical":"away_canon"})
odds["match_date_utc"] = pd.to_datetime(odds.get("match_date_utc") or odds.get("date_utc"), utc=True, errors="coerce")
fx["kickoff_utc"] = pd.to_datetime(fx["kickoff_utc"], utc=True, errors="coerce")
fdm["kickoff_utc"] = pd.to_datetime(fdm["kickoff_utc"], utc=True, errors="coerce")

fx["home_key"] = fx["home_canon"].map(norm_name); fx["away_key"] = fx["away_canon"].map(norm_name)
fdm["home_key"] = fdm["home_canon"].map(norm_name); fdm["away_key"] = fdm["away_canon"].map(norm_name)
odds["home_key"] = odds["home_team"].map(norm_name); odds["away_key"] = odds["away_team"].map(norm_name)

# Time-window join (± 4 hours)
def join_time(a, a_time, b, b_time, keys, hours=4):
    a2 = a.copy()
    b2 = b.copy()
    a2["_left"]  = a2[a_time] - pd.Timedelta(hours=hours)
    a2["_right"] = a2[a_time] + pd.Timedelta(hours=hours)
    # cartesian limited by time window
    m = a2.merge(b2, on=keys, suffixes=("_fx","_b"))
    m = m[(m[b_time].between(m["_left"], m["_right"]))]
    # choose closest by absolute time diff
    m["_dt"] = (m[b_time] - m[a_time]).abs()
    m = m.sort_values(["_dt"]).drop_duplicates(subset=["fixture_id"], keep="first")
    return m.drop(columns=["_left","_right","_dt"])

# 1) fixtures ↔ odds
fx_odds = join_time(
    a=fx, a_time="kickoff_utc",
    b=odds, b_time="match_date_utc",
    keys=["home_key","away_key"]
)

# 2) bring in FD.org results (if past matches)
fx_odds_res = join_time(
    a=fx_odds, a_time="kickoff_utc",
    b=fdm[["kickoff_utc","home_key","away_key","ft_home_goals","ft_away_goals","status"]],
    b_time="kickoff_utc",
    keys=["home_key","away_key"]
)

# 3) Injuries → match-level flags
if not inj.empty:
    inj["team_key"] = inj["team_canonical"].map(lambda s: (s or "").strip().lower())
    home_inj = inj.groupby("team_key").size().rename("inj_count_team").reset_index()
    fx_odds_res = fx_odds_res.merge(home_inj.rename(columns={"team_key":"home_key"}), how="left", on="home_key")
    fx_odds_res = fx_odds_res.merge(home_inj.rename(columns={"team_key":"away_key"}), how="left", on="away_key", suffixes=("_home","_away"))
else:
    fx_odds_res["inj_count_team_home"] = 0
    fx_odds_res["inj_count_team_away"] = 0

# Basic QC report
total = len(fx)
with_odds = fx_odds["fixture_id"].nunique() if "fixture_id" in fx_odds else 0
with_res  = fx_odds_res["fixture_id"].nunique() if "fixture_id" in fx_odds_res else 0

qc_lines = [
    f"Fixtures total: {total}",
    f"Fixtures joined with odds (±4h): {with_odds}",
    f"Fixtures joined with results (±4h): {with_res}",
    f"Null odds_home %: {fx_odds_res['odds_home'].isna().mean():.2%}" if "odds_home" in fx_odds_res else "odds_home absent",
    f"Null ft goals %: {fx_odds_res[['ft_home_goals','ft_away_goals']].isna().any(axis=1).mean():.2%}" if {'ft_home_goals','ft_away_goals'}.issubset(fx_odds_res.columns) else "results absent"
]

# Save
fx_odds_res.to_parquet(OUTJ/"stage7_master_training_table.parquet", index=False)
(Path("data/joined")/"stage7_join_report.txt").write_text("\n".join(qc_lines), encoding="utf-8")
print("✅ Stage 7 master table → data/joined/stage7_master_training_table.parquet")
print("\n".join(qc_lines))
