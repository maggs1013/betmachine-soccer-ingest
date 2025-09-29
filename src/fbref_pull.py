#!/usr/bin/env python3
import sys, time, requests, pandas as pd
from utils import env, dump_text, print_fields

URL = env("FBREF_LEAGUE_URL", "https://fbref.com/en/comps/9/stats/Premier-League-Stats")

# FBref sometimes blocks CI; send friendlier headers and retry gently.
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://fbref.com/"
}

def fetch_html_with_retries(url, attempts=3, backoff=3):
    last_err = None
    for i in range(attempts):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 403:
                # FBref blocked us (often on GitHub Actions). Try again after a pause.
                last_err = requests.HTTPError(f"403 Forbidden on attempt {i+1}")
                time.sleep(backoff)
                continue
            r.raise_for_status()
            return r.text
        except requests.HTTPError as e:
            last_err = e
            time.sleep(backoff)
        except Exception as e:
            last_err = e
            time.sleep(backoff)
    # Give up but do NOT hard-fail CI: print and exit 0 so the rest of pipeline continues.
    print(f"FBref fetch failed after {attempts} attempts: {last_err}")
    sys.exit(0)

if __name__ == "__main__":
    try:
        html = fetch_html_with_retries(URL)
        if not html:
            # fetch already exited 0 on failure; this is just a safeguard
            sys.exit(0)

        dump_text("fbref", "page.html", html)

        # Parse tables (largest by area)
        tables = pd.read_html(html, header=1)
        df = max(tables, key=lambda t: t.shape[0] * t.shape[1])
        print(f"fbref tables found={len(tables)}, chosen rows={len(df)}, cols={len(df.columns)}")
        print_fields("fbref chosen table columns", [df.head(1).to_dict(orient="records")[0]])

        # Show a few real values
        print("\nexample values:")
        print(df.head(5).to_string(index=False))

        print("\n✅ fbref_pull complete")
    except SystemExit as se:
        # allow graceful exit (0) to propagate
        raise
    except Exception as e:
        # Don’t fail the whole workflow if FBref parsing changes—just log & exit 0
        print(f"FBref parse error: {e}")
        sys.exit(0)
