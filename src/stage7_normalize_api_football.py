cat > src/stage7_normalize_api_football.py <<'PY'
#!/usr/bin/env python3
import json
from pathlib import Path
import pandas as pd
from utils import today_dir

RAW = Path("data/raw/api_football")
OUT = Path("data/normalized"); OUT.mkdir(parents=True, exist_ok=True)
MAP_PATH = Path("mappings/team_dictionary.csv")

def load_map():
    if MAP_PATH.exists():
        m = pd.read_csv(MAP_PATH)
        need = {"source","source_team","canonical_team"}
        if not need.issubset(m.columns): raise RuntimeError("team_dictionary.csv missing required columns")
        return m
    return pd.DataFrame(columns=["source","source_team","canonical_team"])

MAP = load_map()

def canon(df, src_col, source_name):
    if df.empty or src_col not in df.columns: return df
    m = MAP[MAP["source"]==source_name][["source_team","canonical_team"]].drop_duplicates()
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
    obj = json.loads(Path(path).read_text(encoding="utf-8"))
    return obj.get("json", obj)  # unwrap fail-safe wrapper

def flatten_fixtures(payload):
    resp = payload.get("response", [])
    rows = []
    for r in resp:
        fx = (r.get("fixture") or {})
        lg = (r.get("league") or {})
        tm = (r.get("teams") or {})
        rows.append({
            "provider":"api_football",
            "fixture_id": fx.get("id"),
            "kickoff_utc": fx.get("date"),
            "league_id": lg.get("id"),
            "league_name": lg.get("name"),
            "season": lg.get("season"),
            "home_team": (tm.get("home") or {}).get("name"),
            "away_team": (tm.get("away") or {}).get("name"),
            "status": (fx.get("status") or {}).get("short"),
            "venue": (fx.get("venue") or {}).get("name"),
        })
    return pd.DataFrame(rows, columns=[
        "provider","fixture_id","kickoff_utc","league_id","league_name","season",
        "home_team","away_team","status","venue"
    ])

def flatten_injuries(payload):
    resp = payload.get("response", [])
    rows = []
    for r in resp:
        ply = (r.get("player") or {})
        t = (r.get("team") or {})
        rows.append({
            "provider":"api_football",
            "player_name": ply.get("name"),
            "player_id": ply.get("id"),
            "team_name": t.get("name"),
            "team_id": t.get("id"),
            "type": r.get("type"),
            "reason": r.get("reason"),
        })
    return pd.DataFrame(rows, columns=[
        "provider","player_name","player_id","team_name","team_id","type","reason"
    ])

if __name__ == "__main__":
    d = latest_dir(RAW)
    OUT.mkdir(parents=True, exist_ok=True)

    fx = pd.DataFrame()
    inj = pd.DataFrame()

    if d:
        fx_path  = next(d.glob("fixtures_future_*.json"), None)
        inj_path = next(d.glob("injuries_*_last14d.json"), None)

        if fx_path and fx_path.exists():
            try:
                fx_json = load_json(fx_path)
                fx = flatten_fixtures(fx_json)
                if not fx.empty:
                    fx = canon(fx, "home_team", "api_football")
                    fx = canon(fx, "away_team", "api_football")
                    if "kickoff_utc" in fx.columns:
                        fx["kickoff_utc"] = pd.to_datetime(fx["kickoff_utc"], utc=True, errors="coerce")
            except Exception as e:
                print("Fixture normalize error:", e)

        if inj_path and inj_path.exists():
            try:
                inj_json = load_json(inj_path)
                inj = flatten_injuries(inj_json)
                if not inj.empty:
                    m = MAP[MAP["source"]=="api_football"][["source_team","canonical_team"]].drop_duplicates()
                    if not m.empty:
                        inj = inj.merge(m, left_on="team_name", right_on="source_team", how="left")\
                                 .assign(team_canonical=lambda d: d["canonical_team"].fillna(d["team_name"]))\
                                 .drop(columns=["source_team","canonical_team"])
                    else:
                        inj["team_canonical"] = inj["team_name"]
            except Exception as e:
                print("Injuries normalize error:", e)

    # Always write outputs (even if empty)
    fx.to_parquet(OUT/"api_football_fixtures.parquet", index=False)
    inj.to_parquet(OUT/"api_football_injuries.parquet", index=False)
    print("✅ Stage 7: normalized API-Football → data/normalized/*.parquet")
PY

chmod +x src/stage7_normalize_api_football.py
git add src/stage7_normalize_api_football.py
git commit -m "Stage 7: hardened API-Football normalizer"
