#!/usr/bin/env python3
import sys, json
from pathlib import Path
import pandas as pd
from utils import today_dir

def summarize_json_records(path: Path, max_files=8):
    rows = []
    for i, p in enumerate(sorted(path.glob("*.json"))):
        if i >= max_files: break
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(obj, list) and obj and isinstance(obj[0], dict):
                df = pd.DataFrame(obj)
                rows.append((p.name, len(df), len(df.columns), list(df.columns)))
            elif isinstance(obj, dict):
                df = pd.json_normalize(obj)
                rows.append((p.name, len(df), len(df.columns), list(df.columns)))
            else:
                rows.append((p.name, 0, 0, []))
        except Exception:
            rows.append((p.name, -1, -1, []))
    return rows

if __name__ == "__main__":
    root = Path("data/raw")
    if not root.exists():
        print("no data/raw yet — run pulls first")
        sys.exit(0)

    for source_dir in root.iterdir():
        dated = sorted(source_dir.iterdir())[-1] if list(source_dir.iterdir()) else None
        if not dated:
            continue
        print("\n" + "="*80)
        print(f"schema report — source: {source_dir.name}, date: {dated.name}")
        print("="*80)
        rows = summarize_json_records(dated)
        for name, nrows, ncols, cols in rows:
            print(f"\n{name}: rows={nrows}, cols={ncols}")
            if cols:
                print(f"columns: {cols[:30]}{' ...' if len(cols)>30 else ''}")

    print("\n✅ schema_report complete")
