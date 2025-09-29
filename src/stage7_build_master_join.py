#!/usr/bin/env python3
import os
import pandas as pd
from pathlib import Path

NORM = Path("data/normalized")
RAW  = Path("data/raw")
OUTJ = Path("data/joined"); OUTJ.mkdir(parents=True, exist_ok=True)

# ---------- Helpers ----------
def norm_name(s):
    if s is None:
        return ""
    return str(s).strip().lower()

def safe_to_datetime(s, utc=True):
    return pd.to_datetime(s, utc=utc, errors="coerce")

def ensure_col(df, new_col, from_cols):
    """
    Ensure df[new_col] exists:
    - If already present, keep it.
    - Else, copy from the first existing name in from_cols.
    - Else, create empty.
    """
    if new_col in df.columns:
        return df
    for c in from_cols:
        if c in df.columns:
            df[new_col] = df[c]
            return df
    df[new_col] = pd.Series([None] * len(df))
    return df

def latest_canonical_odds_csv():
    """Pick the latest odds_api_canonical.csv under data/raw/canonical/<date>/."""
    dated_dirs = sorted((RAW / "canonical").glob("*"))
    if dated_dirs:
        latest = dated_dirs[-1]
        cand = list(latest.glob("odds_api_canonical.csv"))
        if cand:
            return cand[0]
    # fallback to a flat path if you store it directly
    fallback = RAW / "canonical" / "odds_api_canonical.csv"
    return fallback if fallback.exists() else None

def join_time(a, a_time, b, b_time, keys, hours=4, left_id_col=None):
    """
    Window join on time with exact keys:
    - Restrict candidate matches to +/- hours around a_time
    - Keep closest by absolute time difference
    """
    if a.empty or b.empty:
        return pd.DataFrame()
    a2 = a.copy()
    b2 = b.copy()
    a2["_left"]  = a2[a_time] - pd.Timedelta(hours=hours)
    a2["_right"] = a2[a_time] + pd.Timedelta(hours=hours)
    # exact key match first
    m = a2.merge(b2, on=keys, suffixes=("_fx","_b"))
    if m.empty:
        return pd.DataFrame()
    # within time window
    m = m[(m[b_time].between(m["_left"], m["_right"])) | (m[a_time].isna()) | (m[b_time].isna())]
    if m.empty:
        return pd.DataFrame()
    # choose closest by absolute difference
    m["_dt"] = (m[b_time] - m[a_time]).abs()
    sort_cols = ["_dt"]
    m = m.sort_values(sort_cols)
    if left_id_col and left_id_col in m.columns:
        m = m.drop_duplicates(subset=[left_id_col], keep="first")
    else:
        # fallback dedup by keys + time if fixture_id missing
        m = m.drop_duplicates(subset=keys + [a_time], keep="first")
    return m.drop(columns=["_left","_right","_dt"], errors="ignore")

# ---------- Load inputs (robustly) ----------
# Fixtures (API-Football)
fx_path  = NORM / "api_football_fixtures.parquet"
fx = pd.read_parquet(fx_path) if fx_path.exists() else pd.DataFrame()

# FD.org matches (future/past with FT results where available)
fdm_path = NORM / "fdorg_matches.parquet"
fdm = pd.read_parquet(fdm_path) if fdm_path.exists() else pd.DataFrame()

# Injuries (API-Football)
inj_path = NORM / "api_football_injuries.parquet"
inj = pd.read_parquet(inj_path) if inj_path.exists() else pd.DataFrame()

# Canonical odds (Stage 5)
odds_csv = latest_canonical_odds_csv()
odds = pd.read_csv(odds_csv) if odds_csv else pd.DataFrame()

# ---------- Normalize columns / create fallbacks ----------
# Fixtures: ensure canonical team columns exist
fx = ensure_col(fx, "home_canon", ["home_team_canonical", "home_team"])
fx = ensure_col(fx, "away_canon", ["away_team_canonical", "away_team"])
fx = ensure_col(fx, "kickoff_utc", ["kickoff_utc", "match_date_utc", "date_utc"])
fx["kickoff_utc"] = safe_to_datetime(fx["kickoff_utc"])
# Prefer having a fixture_id; if missing, synthesize a stable id
fx = ensure_col(fx, "fixture_id", ["fixture_id"])
if fx["fixture_id"].isna().all():
    fx["fixture_id"] = (fx["home_canon"].fillna("") + "_" +
                        fx["away_canon"].fillna("") + "_" +
                        fx["kickoff_utc"].astype(str))

# FD.org matches: ensure canonical team columns + kickoff
fdm = ensure_col(fdm, "home_canon", ["home_team_canonical", "home_team"])
fdm = ensure_col(fdm, "away_canon", ["away_team_canonical", "away_team"])
fdm = ensure_col(fdm, "kickoff_utc", ["kickoff_utc", "utcDate", "match_date_utc", "date_utc"])
fdm["kickoff_utc"] = safe_to_datetime(fdm["kickoff_utc"])
# Some files might not have FT goals yet (future); ensure present
fdm = ensure_col(fdm, "ft_home_goals", ["ft_home_goals"])
fdm = ensure_col(fdm, "ft_away_goals", ["ft_away_goals"])
fdm = ensure_col(fdm, "status", ["status"])

# Odds: ensure match datetime column exists as match_date_utc
if not odds.empty:
    if "match_date_utc" in odds.columns:
        odds["match_date_utc"] = safe_to_datetime(odds["match_date_utc"])
    elif "date_utc" in odds.columns:
        odds["match_date_utc"] = safe_to_datetime(odds["date_utc"])
    else:
        odds["match_date_utc"] = pd.NaT
    odds = ensure_col(odds, "home_team", ["home_team"])
    odds = ensure_col(odds, "away_team", ["away_team"])

# Keys for joining
for c in ["home_canon","away_canon"]:
    if c not in fx.columns:  fx[c]  = None
    if c not in fdm.columns: fdm[c] = None

fx["home_key"]  = fx["home_canon"].map(norm_name)
fx["away_key"]  = fx["away_canon"].map(norm_name)
fdm["home_key"] = fdm["home_canon"].map(norm_name)
fdm["away_key"] = fdm["away_canon"].map(norm_name)

if not odds.empty:
    odds["home_key"] = odds["home_team"].map(norm_name)
    odds["away_key"] = odds["away_team"].map(norm_name)

# ---------- Perform joins (robust to empties) ----------
# 1) fixtures ↔ odds (± hours). Allow override via env (default 4)
hours = float(os.getenv("STAGE7_JOIN_HOURS", "4"))
if not fx.empty and not odds.empty:
    fx_odds = join_time(
        a=fx, a_time="kickoff_utc",
        b=odds, b_time="match_date_utc",
        keys=["home_key","away_key"],
        hours=hours,
        left_id_col="fixture_id"
    )
else:
    fx_odds = pd.DataFrame()

# 2) add FD.org results (± hours)
if not fx_odds.empty and not fdm.empty:
    add_cols = ["kickoff_utc","home_key","away_key","ft_home_goals","ft_away_goals","status"]
    for c in add_cols:
        if c not in fdm.columns:
            fdm[c] = None
    fx_odds_res = join_time(
        a=fx_odds, a_time="kickoff_utc",
        b=fdm[add_cols],
        b_time="kickoff_utc",
        keys=["home_key","away_key"],
        hours=hours,
        left_id_col="fixture_id"
    )
else:
    fx_odds_res = fx_odds.copy()

# 3) Injuries → simple match-level counts
if not fx_odds_res.empty:
    if not inj.empty:
        # team_canonical may not exist; fallback to team_name
        if "team_canonical" not in inj.columns:
            inj["team_canonical"] = inj.get("team_name", pd.Series([None]*len(inj)))
        inj["team_key"] = inj["team_canonical"].map(norm_name)
        counts = inj.groupby("team_key").size().rename("inj_count_team").reset_index()
        fx_odds_res = fx_odds_res.merge(
            counts.rename(columns={"team_key":"home_key"}), how="left", on="home_key"
        ).rename(columns={"inj_count_team":"inj_count_team_home"})
        fx_odds_res = fx_odds_res.merge(
            counts.rename(columns={"team_key":"away_key"}), how="left", on="away_key"
        ).rename(columns={"inj_count_team":"inj_count_team_away"})
    else:
        fx_odds_res["inj_count_team_home"] = 0
        fx_odds_res["inj_count_team_away"] = 0

# ---------- QC report ----------
def safe_nunique(df, col):
    return df[col].nunique() if (not df.empty and col in df.columns) else 0

total_fixtures = len(fx)
with_odds = safe_nunique(fx_odds, "fixture_id")
with_res  = safe_nunique(fx_odds_res, "fixture_id")

qc_lines = [
    f"Fixtures total: {total_fixtures}",
    f"Fixtures joined with odds (±{hours}h): {with_odds}",
    f"Fixtures joined with results (±{hours}h): {with_res}",
]

if not fx_odds_res.empty and {"odds_home","odds_draw","odds_away"}.issubset(fx_odds_res.columns):
    qc_lines.append(f"Null odds_home %: {fx_odds_res['odds_home'].isna().mean():.2%}")
else:
    qc_lines.append("odds columns absent after join")

if not fx_odds_res.empty and {"ft_home_goals","ft_away_goals"}.issubset(fx_odds_res.columns):
    qc_lines.append(f"Null FT goals %: {fx_odds_res[['ft_home_goals','ft_away_goals']].isna().any(axis=1).mean():.2%}")
else:
    qc_lines.append("results columns absent after join")

# ---------- Save outputs (always) ----------
# If parquet engine not available, you can switch to CSV by uncommenting:
# fx_odds_res.to_csv(OUTJ/"stage7_master_training_table.csv", index=False)
try:
    fx_odds_res.to_parquet(OUTJ/"stage7_master_training_table.parquet", index=False)
except Exception as e:
    # fallback to CSV so the pipeline never fails
    fx_odds_res.to_csv(OUTJ/"stage7_master_training_table.csv", index=False)
    qc_lines.append(f"NOTE: Parquet engine missing, wrote CSV instead. ({e})")

(Path("data/joined")/"stage7_join_report.txt").write_text("\n".join(qc_lines), encoding="utf-8")
print("✅ Stage 7 master table → data/joined/")
print("\n".join(qc_lines))
