#!/usr/bin/env python3
import json
from pathlib import Path
import pandas as pd

RAW = Path("data/raw/footballdata_org")
OUT = Path("data/normalized"); OUT.mkdir(parents=True, exist_ok=True)
MAP_PATH = Path("mappings/team_dictionary.csv")

def load_map():
    return pd.read_csv(MAP_PATH) if MAP_PATH.exists() else pd.DataFrame(columns=["source","source_team","canonical_team"])

MAP = load_map()

def canon(df, src_col):
    if df.empty or src_col not in df.columns: return df
    m = MAP[MAP["source"]=="footballdata_org"][["source_team","canonical_team"]].drop_duplicates()
    if m.empty:
        df[f"{src_col}_canonical"] = df[src_col]
        return df
    out = df.merge(m, left_on=src_col, right_on="source_team", how="left")
    out[f"{src_col}_canonical"] = out["canonical_team"].fillna(out[src_col])
    return out.drop(columns=["source_team","canonical_team"])

def latest_dir(base):
    dirs = [p for p in base.glob("*") if p.is_dir()]
    return sorted(dirs)[-1] if dirs else None

def load_json(path):
    txt = Path(path).read_text(encoding="utf-8")
    obj = json.loads(txt)
    return obj.get("json", obj)

def flatten_matches(payload):
    rows=[]
    for m in (payload.get("matches") or []):
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
    return pd.DataFrame(rows, columns=[
        "provider","comp_code","match_id","kickoff_utc","status",
        "home_team","away_team","ft_home_goals","ft_away_goals"
    ])

if __name__ == "__main__":
    d = latest_dir(RAW)
    frames=[]
    if d:
        for p in [*d.glob("matches_future_*.json"), *d.glob("matches_past_*.json")]:
            payload = load_json(p)
            frames.append(flatten_matches(payload))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if not df.empty:
        df = canon(df, "home_team")
        df = canon(df, "away_team")
        df["kickoff_utc"] = pd.to_datetime(df["kickoff_utc"], utc=True, errors="coerce")

    df.to_parquet(OUT/"fdorg_matches.parquet", index=False)
    print("✅ Stage 7: normalized FD.org → data/normalized/fdorg_matches.parquet")