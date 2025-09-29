#!/usr/bin/env python3
import json, zipfile, os
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

from utils import today_dir

RAW = Path("data/raw/api_football")
OUT = Path("data/normalized"); OUT.mkdir(parents=True, exist_ok=True)
MAP = pd.read_csv("mappings/team_dictionary.csv")

def canon(df, src_col):
    m = MAP[MAP["source"]=="api_football"][["source_team","canonical_team"]].drop_duplicates()
    return df.merge(m, left_on=src_col, right_on="source_team", how="left").assign(
        **{f"{src_col}_canonical": lambda d: d["canonical_team"].fillna(d[src_col])}
    ).drop(columns=["source_team","canonical_team"])

def latest_dir(base):
    return sorted([p for p in base.glob("*") if p.is_dir()])[-1]

def load_json(path):
    obj = json.loads(Path(path).read_text(encoding="utf-8"))
    return obj.get("json", obj)

def flatten_fixtures(payload):
    resp = payload.get("response", [])
    rows = []
    for r in resp:
        fx = r.get("fixture", {})
        lg = r.get("league", {})
        tm = r.get("teams", {})
        rows.append({
            "provider":"api_football",
            "fixture_id": fx.get("id"),
            "kickoff_utc": fx.get("date"),
            "league_id": lg.get("id"),
            "league_name": lg.get("name"),
            "season": lg.get("season"),
            "home_team": tm.get("home",{}).get("name"),
            "away_team": tm.get("away",{}).get("name"),
            "status": (fx.get("status") or {}).get("short"),
            "venue": (fx.get("venue") or {}).get("name")
        })
    return pd.DataFrame(rows)

def flatten_injuries(payload):
    resp = payload.get("response", [])
    rows = []
    for r in resp:
        ply = r.get("player", {})
        t = r.get("team", {})
        rows.append({
            "provider":"api_football",
            "player_name": ply.get("name"),
            "player_id": ply.get("id"),
            "team_name": t.get("name"),
            "team_id": t.get("id"),
            "type": r.get("type"),
            "reason": r.get("reason"),
        })
    return pd.DataFrame(rows)

if __name__ == "__main__":
    d = latest_dir(RAW)
    fx_path = next(d.glob("fixtures_future_*.json"), None)
    inj_path = next(d.glob("injuries_*_last14d.json"), None)

    # Fixtures
    if fx_path and fx_path.exists():
        fx_json = load_json(fx_path)
        fx = flatten_fixtures(fx_json)
        if not fx.empty:
            fx = canon(fx, "home_team")
            fx = canon(fx, "away_team")
        fx["kickoff_utc"] = pd.to_datetime(fx["kickoff_utc"], utc=True, errors="coerce")
        fx.to_parquet(OUT/"api_football_fixtures.parquet", index=False)

    # Injuries
    if inj_path and inj_path.exists():
        inj_json = load_json(inj_path)
        inj = flatten_injuries(inj_json)
        if not inj.empty:
            inj = inj.merge(
                MAP[MAP["source"]=="api_football"][["source_team","canonical_team"]].drop_duplicates(),
                left_on="team_name", right_on="source_team", how="left"
            ).assign(team_canonical=lambda d: d["canonical_team"].fillna(d["team_name"]))\
             .drop(columns=["source_team","canonical_team"])
        inj.to_parquet(OUT/"api_football_injuries.parquet", index=False)

    print("✅ Stage 7: normalized API-Football → data/normalized/*.parquet")
