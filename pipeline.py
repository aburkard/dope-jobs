"""
End-to-end pipeline: scrape → detect changes → parse new/changed → upsert MeiliSearch.

Uses Neon Postgres for state tracking (content hashes, parse status).
Only re-parses jobs whose content actually changed.

Usage:
  uv run python pipeline.py --companies companies.txt
  uv run python pipeline.py --companies companies.txt --skip-scrape   # re-parse pending + reload
  uv run python pipeline.py --companies companies.txt --skip-parse    # just reload from DB
  uv run python pipeline.py --companies companies.txt --parse-pending # parse only jobs with needs_parse=True
"""

import argparse
import json
import time
import sys
from collections import defaultdict

from dotenv import load_dotenv
load_dotenv()

from scrapers.greenhouse_scraper import GreenhouseScraper
from scrapers.lever_scraper import LeverScraper
from scrapers.ashby_scraper import AshbyScraper
from scrapers.jobvite_scraper import JobviteScraper
from db import (
    get_connection, init_schema, upsert_scraped_jobs, mark_removed,
    job_id, upsert_company, get_jobs_needing_parse, save_parsed_result,
    get_parsed_jobs, get_removed_job_ids,
)


ATS_SCRAPERS = {
    "greenhouse": GreenhouseScraper,
    "lever": LeverScraper,
    "ashby": AshbyScraper,
    "jobvite": JobviteScraper,
}


def parse_companies_file(path: str) -> list[tuple[str, str]]:
    """Parse companies.txt → list of (ats, board_token)."""
    companies = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" in line:
                ats, token = line.split(":", 1)
            else:
                ats, token = "greenhouse", line
            companies.append((ats.strip(), token.strip()))
    return companies


def step_scrape(conn, companies: list[tuple[str, str]], max_per_company: int = 50):
    """Scrape all companies and detect changes via content hashing."""
    print(f"\n--- SCRAPE ({len(companies)} companies) ---")

    total_new = 0
    total_changed = 0
    total_unchanged = 0
    total_removed = 0
    errors = 0

    for ats, token in companies:
        scraper_cls = ATS_SCRAPERS.get(ats)
        if not scraper_cls:
            print(f"  Unknown ATS '{ats}' for {token}, skipping")
            continue

        try:
            scraper = scraper_cls(token)
            all_jobs = list(scraper.fetch_jobs())
            jobs = all_jobs[:max_per_company]

            # Detect changes against DB
            result = upsert_scraped_jobs(conn, jobs)
            n_new = len(result["new"])
            n_changed = len(result["changed"])
            n_unchanged = result["unchanged"]

            # Mark jobs not seen in this scrape as removed
            seen_ids = {job_id(j) for j in jobs}
            n_removed = mark_removed(conn, ats, token, seen_ids)

            # Update company record
            company_name = None
            try:
                company_name = scraper.get_company_name()
            except Exception:
                pass
            upsert_company(conn, ats, token, company_name=company_name, job_count=len(all_jobs))

            status_parts = []
            if n_new: status_parts.append(f"{n_new} new")
            if n_changed: status_parts.append(f"{n_changed} changed")
            if n_unchanged: status_parts.append(f"{n_unchanged} unchanged")
            if n_removed: status_parts.append(f"{n_removed} removed")
            status = ", ".join(status_parts) or "empty"
            print(f"  {ats}:{token} — {len(all_jobs)} total → {status}")

            total_new += n_new
            total_changed += n_changed
            total_unchanged += n_unchanged
            total_removed += n_removed

        except Exception as e:
            errors += 1
            print(f"  {ats}:{token} — ERROR: {e}")

        time.sleep(0.3)

    print(f"\nScrape complete: {total_new} new, {total_changed} changed, "
          f"{total_unchanged} unchanged, {total_removed} removed, {errors} errors")
    return total_new + total_changed


def step_parse(conn, base_url: str, model: str, api_key: str | None = None,
               limit: int | None = None):
    """Parse jobs that need extraction (needs_parse=True)."""
    import os
    from parse import OpenAIBackend, prepare_job_text

    pending = get_jobs_needing_parse(conn, limit=limit)
    if not pending:
        print("\n--- PARSE (0 jobs pending) ---")
        return 0

    print(f"\n--- PARSE ({len(pending)} jobs pending) ---")
    key = api_key or os.environ.get("OPENAI_API_KEY", "not-needed")
    backend = OpenAIBackend(base_url, model, api_key=key)

    successes = 0
    t0 = time.time()

    for i, job_row in enumerate(pending):
        jid = job_row["id"]
        raw = job_row.get("raw_json")

        if not raw:
            print(f"  Skipping {jid}: no raw_json in DB", file=sys.stderr)
            continue

        try:
            text = prepare_job_text(raw)
            results = backend.extract_batch([text])
            result = results[0]

            if result is not None:
                parsed = result.model_dump(mode="json")
                save_parsed_result(conn, jid, parsed)
                successes += 1
        except Exception as e:
            print(f"  Error parsing {jid}: {e}", file=sys.stderr)

        if (i + 1) % 10 == 0 or i == len(pending) - 1:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (len(pending) - i - 1) / rate if rate > 0 else 0
            print(f"  {i+1}/{len(pending)} | {successes} ok | {rate:.2f} jobs/s | ETA {eta/60:.0f}m")

    print(f"Parsed {successes}/{len(pending)}")
    return successes


def step_load(conn, meili_host: str = "http://localhost:7700"):
    """Load parsed jobs from DB into MeiliSearch (upsert, not full-replace)."""
    import meilisearch
    from utils.html_utils import remove_html_markup

    parsed_rows = get_parsed_jobs(conn)
    removed_ids = get_removed_job_ids(conn)

    if not parsed_rows and not removed_ids:
        print("\n--- LOAD (nothing to update) ---")
        return

    print(f"\n--- LOAD ({len(parsed_rows)} active, {len(removed_ids)} removed) ---")

    # Build MeiliSearch documents
    docs = []
    for row in parsed_rows:
        m = row["parsed_json"]
        if not m:
            continue

        board = row["board_token"]
        company = board.replace("-", " ").replace("_", " ").title()

        # Location string from parsed metadata
        locs = m.get("locations", [])
        loc_parts = []
        if locs:
            loc = locs[0]
            if loc.get("city"): loc_parts.append(loc["city"])
            if loc.get("state"): loc_parts.append(loc["state"])
            if loc.get("country_code"): loc_parts.append(loc["country_code"])
        location_str = ", ".join(loc_parts)

        # Salary
        sal = m.get("salary")

        # Geo
        geo = None
        if locs and locs[0].get("lat") and locs[0].get("lng"):
            geo = {"lat": locs[0]["lat"], "lng": locs[0]["lng"]}

        docs.append({
            "id": row["id"],
            "title": row["title"],
            "tagline": m.get("tagline", ""),
            "company": company,
            "company_slug": board,
            "description": "",  # Could fetch from R2 later
            "location": location_str,
            "_geo": geo,
            "office_type": m.get("office_type", ""),
            "job_type": m.get("job_type", ""),
            "experience_level": m.get("experience_level", ""),
            "is_manager": m.get("is_manager", False),
            "industry": m.get("industry", ""),
            "salary_min": sal["min"] if sal else None,
            "salary_max": sal["max"] if sal else None,
            "salary_currency": sal["currency"] if sal else None,
            "salary_period": sal["period"] if sal else None,
            "salary_transparency": m.get("salary_transparency", "not_disclosed"),
            "hard_skills": m.get("hard_skills", []),
            "soft_skills": m.get("soft_skills", []),
            "cool_factor": m.get("cool_factor", "standard"),
            "vibe_tags": [v for v in m.get("vibe_tags", [])],
            "visa_sponsorship": m.get("visa_sponsorship", "unknown"),
            "equity_offered": m.get("equity", {}).get("offered", False),
            "company_stage": m.get("company_stage"),
            "benefits_categories": [b for b in m.get("benefits_categories", [])],
            "benefits_highlights": m.get("benefits_highlights", []),
            "reports_to": m.get("reports_to"),
            "ats_type": row["ats"],
        })

    client = meilisearch.Client(meili_host)
    index = client.index("jobs")

    # Configure index settings (idempotent)
    index.update_filterable_attributes([
        "office_type", "job_type", "experience_level", "is_manager",
        "industry", "company_slug", "ats_type",
        "cool_factor", "vibe_tags", "visa_sponsorship", "equity_offered",
        "company_stage", "benefits_categories", "salary_transparency",
    ])
    index.update_searchable_attributes([
        "title", "tagline", "company", "description", "location",
        "hard_skills", "soft_skills", "benefits_highlights",
    ])
    index.update_sortable_attributes(["salary_min", "salary_max"])

    # Upsert documents (not full replace)
    if docs:
        task = index.add_documents(docs, primary_key="id")
        print(f"  Upserting {len(docs)} documents... (task {task.task_uid})")
        try:
            client.wait_for_task(task.task_uid, timeout_in_ms=30000)
        except Exception:
            print("  (waiting for index timed out, but task is queued)")

    # Delete removed jobs
    if removed_ids:
        task = index.delete_documents(ids=removed_ids)
        print(f"  Deleting {len(removed_ids)} removed jobs...")
        try:
            client.wait_for_task(task.task_uid, timeout_in_ms=10000)
        except Exception:
            pass

    stats = index.get_stats()
    print(f"  Index: {stats.number_of_documents} documents")


def main():
    parser = argparse.ArgumentParser(description="dopejobs pipeline")
    parser.add_argument("--companies", required=True, help="Companies file (ats:token per line)")
    parser.add_argument("--skip-scrape", action="store_true")
    parser.add_argument("--skip-parse", action="store_true")
    parser.add_argument("--skip-load", action="store_true")
    parser.add_argument("--parse-pending", action="store_true", help="Only parse jobs with needs_parse=True")
    parser.add_argument("--base-url", default="https://api.openai.com/v1", help="LLM API base URL")
    parser.add_argument("--model", default="gpt-5.4-nano-2026-03-17", help="LLM model name")
    parser.add_argument("--max-per-company", type=int, default=50, help="Max jobs per company")
    parser.add_argument("--parse-limit", type=int, default=None, help="Max jobs to parse per run")
    parser.add_argument("--meili-host", default="http://localhost:7700")
    args = parser.parse_args()

    conn = get_connection()
    init_schema(conn)

    companies = parse_companies_file(args.companies)

    # Step 1: Scrape + detect changes
    if not args.skip_scrape:
        needs_parse = step_scrape(conn, companies, max_per_company=args.max_per_company)
    else:
        print("Skipping scrape")

    # Step 2: Parse new/changed jobs
    if not args.skip_parse:
        step_parse(conn, args.base_url, args.model, limit=args.parse_limit)
    else:
        print("Skipping parse")

    # Step 3: Load to MeiliSearch
    if not args.skip_load:
        step_load(conn, meili_host=args.meili_host)
    else:
        print("Skipping load")

    conn.close()
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
