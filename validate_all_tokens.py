"""Validate all board tokens and insert into pipeline_companies table.
Checks if each board is active (has jobs) and stores the result."""

import time
import sys
from dotenv import load_dotenv
load_dotenv()

from scrapers.greenhouse_scraper import GreenhouseScraper
from scrapers.lever_scraper import LeverScraper
from scrapers.ashby_scraper import AshbyScraper
from scrapers.jobvite_scraper import JobviteScraper
from db import get_connection, init_schema, upsert_company


ATS_SCRAPERS = {
    "greenhouse": GreenhouseScraper,
    "lever": LeverScraper,
    "lever_eu": LeverScraper,
    "ashby": AshbyScraper,
    "jobvite": JobviteScraper,
}

ATS_NAMES = {
    "lever_eu": "lever",  # normalize to "lever" in DB
}


def load_tokens():
    """Load all board tokens from files."""
    tokens = []
    for platform in ["greenhouse", "lever", "lever_eu", "ashby", "jobvite"]:
        path = f"data/board_tokens/{platform}_2026.txt"
        try:
            with open(path) as f:
                for line in f:
                    token = line.strip()
                    if token:
                        ats = ATS_NAMES.get(platform, platform)
                        tokens.append((ats, token, platform))
        except FileNotFoundError:
            print(f"  {path} not found, skipping")
    return tokens


def check_board(ats: str, token: str, platform: str) -> tuple[bool, int, str | None]:
    """Check if a board is active. Returns (active, job_count, company_name)."""
    scraper_cls = ATS_SCRAPERS.get(platform)
    if not scraper_cls:
        return False, 0, None

    try:
        if platform == "lever_eu":
            scraper = scraper_cls(token, eu=True)
        else:
            scraper = scraper_cls(token)

        # Just check if it exists and has jobs — don't fetch all jobs
        if platform in ("greenhouse",):
            import requests
            r = requests.get(
                f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs",
                timeout=10,
            )
            if not r.ok:
                return False, 0, None
            data = r.json()
            jobs = data.get("jobs", [])
            name = None
            # Get name from board endpoint
            r2 = requests.get(f"https://boards-api.greenhouse.io/v1/boards/{token}", timeout=5)
            if r2.ok:
                name = r2.json().get("name")
            return len(jobs) > 0, len(jobs), name

        elif platform in ("lever", "lever_eu"):
            import requests
            base = "https://api.eu.lever.co" if platform == "lever_eu" else "https://api.lever.co"
            r = requests.get(f"{base}/v0/postings/{token}?limit=1", timeout=10)
            if not r.ok:
                return False, 0, None
            jobs = r.json()
            return len(jobs) > 0, len(jobs), None

        elif platform == "ashby":
            import requests
            r = requests.post("https://jobs.ashbyhq.com/api/non-user-graphql", json={
                "operationName": "ApiJobBoardWithTeams",
                "variables": {"organizationHostedJobsPageName": token},
                "query": """query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {
                    jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) {
                        jobPostings { id }
                    }
                }"""
            }, timeout=10)
            if not r.ok:
                return False, 0, None
            board = r.json().get("data", {}).get("jobBoard")
            if not board:
                return False, 0, None
            jobs = board.get("jobPostings", [])
            return len(jobs) > 0, len(jobs), None

        elif platform == "jobvite":
            import requests
            r = requests.get(f"https://jobs.jobvite.com/{token}", timeout=10)
            return r.ok, 1 if r.ok else 0, None  # Can't count jobs, use 1 as active signal

        return False, 0, None

    except Exception:
        return False, 0, None


def main():
    conn = get_connection()
    init_schema(conn)

    tokens = load_tokens()
    print(f"Loaded {len(tokens)} tokens to validate\n")

    # Check what's already in DB
    with conn.cursor() as cur:
        cur.execute("SELECT ats, board_token FROM pipeline_companies")
        existing = {(r[0], r[1]) for r in cur.fetchall()}
    print(f"{len(existing)} already in DB\n")

    active_count = 0
    inactive_count = 0
    skipped = 0
    errors = 0
    t0 = time.time()

    for i, (ats, token, platform) in enumerate(tokens):
        # Skip if already in DB
        if (ats, token) in existing:
            skipped += 1
            continue

        active, job_count, name = check_board(ats, token, platform)

        if active:
            active_count += 1
        else:
            inactive_count += 1

        upsert_company(conn, ats, token, company_name=name, job_count=job_count)

        # Progress every 100
        if (i + 1) % 100 == 0:
            elapsed = time.time() - t0
            rate = (i + 1 - skipped) / elapsed if elapsed > 0 else 0
            checked = active_count + inactive_count
            eta = (len(tokens) - i - 1) / rate / 60 if rate > 0 else 0
            print(
                f"  {i+1}/{len(tokens)} | {checked} checked | "
                f"{active_count} active | {inactive_count} inactive | "
                f"{skipped} skipped | {rate:.1f}/s | ETA {eta:.0f}m",
                flush=True,
            )

        time.sleep(0.15)  # gentle rate limiting

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed/60:.1f}m")
    print(f"  Active: {active_count}")
    print(f"  Inactive: {inactive_count}")
    print(f"  Skipped (already in DB): {skipped}")

    # Show DB totals
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pipeline_companies")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM pipeline_companies WHERE active = TRUE")
        active_total = cur.fetchone()[0]
    print(f"\nDB: {total} companies, {active_total} active")

    conn.close()


if __name__ == "__main__":
    main()
