"""Refresh board tokens from Common Crawl using cdx_toolkit.

Only fetches index data (URLs) — does not download any page content.
"""
import cdx_toolkit
import pandas as pd

ATS_CONFIG = {
    "greenhouse": {
        "prefix": "boards.greenhouse.io/*",
        "start": "20250101",
    },
    "lever": {
        "prefix": "jobs.lever.co/*",
        "start": "20250101",
    },
    "lever_eu": {
        "prefix": "jobs.eu.lever.co/*",
        "start": "20250101",
    },
    "ashby": {
        "prefix": "jobs.ashbyhq.com/*",
        "start": "20250101",
    },
    "jobvite": {
        "prefix": "jobs.jobvite.com/*",
        "start": "20250101",
    },
}

SKIP_TOKENS = {"", "api", "embed", "search", "jobs", "static", "assets", "cdn", "www", "non-user-graphql", "robots.txt"}


def get_board_token(url):
    token = url.split("/")[3]
    token = token.split("?")[0]
    return token


def refresh_platform(ats_name, config):
    print(f"\n{'='*50}")
    print(f"  {ats_name}")
    print(f"{'='*50}")

    cdx = cdx_toolkit.CDXFetcher(source="cc")
    objs = list(cdx.iter(config["prefix"], from_ts=config["start"], limit=100000))
    print(f"  CC records: {len(objs)}")

    if not objs:
        print("  No records found, skipping")
        return

    df = pd.DataFrame(objs)
    df["board_token"] = df.url.apply(get_board_token)

    # Filter out junk tokens
    df = df[~df.board_token.str.lower().isin(SKIP_TOKENS)]
    df = df[df.board_token.str.len() < 100]
    df = df[df.board_token.str.len() > 0]

    tokens = sorted(df.board_token.str.lower().unique().tolist())
    print(f"  Unique tokens: {len(tokens)}")

    # Compare with old list
    old_path = f"data/board_tokens/{ats_name}.txt"
    try:
        with open(old_path) as f:
            old_tokens = set(line.strip().lower() for line in f if line.strip())
        new = set(tokens) - old_tokens
        lost = old_tokens - set(tokens)
        print(f"  Previously known: {len(old_tokens)}")
        print(f"  New: {len(new)}")
        print(f"  Not in recent CC: {len(lost)}")
    except FileNotFoundError:
        pass

    # Save
    out_path = f"data/board_tokens/{ats_name}_2026.txt"
    with open(out_path, "w") as f:
        for token in tokens:
            f.write(token + "\n")
    print(f"  Saved to {out_path}")


if __name__ == "__main__":
    for ats_name, config in ATS_CONFIG.items():
        refresh_platform(ats_name, config)
    print("\nDone!")
