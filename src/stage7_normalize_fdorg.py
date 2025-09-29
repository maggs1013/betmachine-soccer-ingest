#!/usr/bin/env python3
import json
from pathlib import Path
import pandas as pd
from utils import today_dir

RAW = Path("data/raw/footballdata_org")
OUT = Path("data/normalized"); OUT.mkdir(parents=True, exist_ok=True)
MAP = pd.read_csv("mappings/team_dictionary.csv")

def canon(df, src_col):
    m = MAP[MAP["source"]=="footballdata_org"][["source_team","canonical_team"]].drop_duplicates()
    return df.merge(m, left_on=src_col, right_on="source_team", how="left").assign(
        **{f"{src_col}_canonical": lambda d: d["canonical_team"].fillna(d[src_col])}
    ).drop(columns=["source_team","canonical_team"])

def latest_dir(base): return sorted([p for p in base.glob("*") if p.is_dir()])[-1]

def load_json(path): return json.loads(Path(path).read_text(encoding="utf-8")).get("json", json.loads(Path(path).read_text()))

def flatten_matches(payload):
    rows=[]
    for m in payload.get("matches", []):
        rows.append({
            "provider":"footballdata_org",
            "comp_code": (m.get("competition") or {}).get("code"),
            "match_id": m.get("id"),
            "kickoff_utc": m.get("utcDate"),
            "status": m.get("status"),
            "home_team": (m.get("homeTeam") or {}).get("name"),
            "away_team": (m.get("awayTeam") or {}).get("name"),
            "ft_home_goals": ((m.get("score") or {}).get("fullTime") or {}).get("home"),
            "ft_away_goals": ((m.get("score") or {}).get("fullTime") or {}).get("away"),
        })
    return pd.DataFrame(rows)

if __name__ == "__main__":
    d = latest_dir(RAW)
    f_future = next(d.glob("matches_future_*.json"), None)
    f_past   = next(d.glob("matches_past_*.json"), None)

    frames=[]
    for p in [f_future, f_past]:
        if p and p.exists():
            payload = load_json(p)
            frames.append(flatten_matches(payload))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if not df.empty:
        df = canon(df, "home_team")
        df = canon(df, "away_team")
        df["kickoff_utc"] = pd.to_datetime(df["kickoff_utc"], utc=True, errors="coerce")

    df.to_parquet(OUT/"fdorg_matches.parquet", index=False)
    print("✅ Stage 7: normalized FD.org → data/normalized/fdorg_matches.parquet")
