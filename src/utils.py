import os, json
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path("data") / "raw"
UA = {"User-Agent": "betmachine-soccer-ingest/1.0 (+github)"}

def today_dir(source: str) -> Path:
    d = DATA_DIR / source / datetime.now(timezone.utc).strftime("%Y-%m-%d")
    d.mkdir(parents=True, exist_ok=True)
    return d

def dump_json(source: str, name: str, obj):
    p = today_dir(source) / name
    with open(p, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    print(f"saved: {p}")

def dump_text(source: str, name: str, text: str):
    p = today_dir(source) / name
    with open(p, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"saved: {p}")

def env(key: str, default: str = None, required: bool = False) -> str:
    v = os.getenv(key, default)
    if required and (v is None or v.strip() == ""):
        raise RuntimeError(f"Missing required env var: {key}")
    return v

def union_keys(list_of_dicts):
    s = set()
    for d in list_of_dicts:
        if isinstance(d, dict):
            s.update(d.keys())
    return sorted(s)

def print_fields(title: str, items):
    print("\n" + "-"*72)
    print(title)
    print("-"*72)
    if isinstance(items, list) and items and isinstance(items[0], dict):
        keys = union_keys(items)
        print(f"fields ({len(keys)}): {keys}")
        # show an example row with values
        ex = items[0]
        print("example first row values:")
        for k in list(keys)[:20]:
            print(f"  {k}: {ex.get(k)}")
    elif isinstance(items, dict):
        print(f"fields: {sorted(items.keys())}")
    else:
        print("no dict-like items to summarize")

def short_obs(label: str, lines):
    print(f"\n{label}")
    for ln in lines:
        print("  • " + str(ln))
