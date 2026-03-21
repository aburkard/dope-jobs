"""
End-to-end pipeline: scrape → parse → load into MeiliSearch.

Usage:
  uv run python pipeline.py --companies companies.txt
  uv run python pipeline.py --companies companies.txt --skip-scrape  # re-parse + reload
  uv run python pipeline.py --companies companies.txt --skip-parse   # just reload
"""

import argparse
import json
import time
import sys
from pathlib import Path

from scrapers.greenhouse_scraper import GreenhouseScraper
from scrapers.lever_scraper import LeverScraper
from scrapers.ashby_scraper import AshbyScraper
from scrapers.jobvite_scraper import JobviteScraper


ATS_SCRAPERS = {
    "greenhouse": GreenhouseScraper,
    "lever": LeverScraper,
    "ashby": AshbyScraper,
    "jobvite": JobviteScraper,
}


def scrape(companies_file: str, output_path: str, max_per_company: int = 50):
    """Scrape jobs from a companies file. Format: one 'ats:token' per line."""
    companies = []
    with open(companies_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" in line:
                ats, token = line.split(":", 1)
            else:
                # Default to greenhouse
                ats, token = "greenhouse", line
            companies.append((ats.strip(), token.strip()))

    print(f"Scraping {len(companies)} companies...")
    all_jobs = []
    errors = 0

    for ats, token in companies:
        scraper_cls = ATS_SCRAPERS.get(ats)
        if not scraper_cls:
            print(f"  Unknown ATS '{ats}' for {token}, skipping")
            continue
        try:
            scraper = scraper_cls(token)
            jobs = list(scraper.fetch_jobs())
            taken = jobs[:max_per_company]
            all_jobs.extend(taken)
            print(f"  {ats}:{token} — {len(jobs)} jobs (took {len(taken)})")
        except Exception as e:
            errors += 1
            print(f"  {ats}:{token} — ERROR: {e}")
        time.sleep(0.3)

    with open(output_path, "w") as f:
        for job in all_jobs:
            f.write(json.dumps(job) + "\n")

    print(f"Scraped {len(all_jobs)} jobs ({errors} errors) → {output_path}")
    return len(all_jobs)


def parse(raw_path: str, parsed_path: str, base_url: str, model: str):
    """Parse raw jobs using LLM."""
    from parse import OpenAIBackend, prepare_job_text, load_raw_jobs

    print(f"Parsing {raw_path}...")
    raw_jobs = load_raw_jobs(raw_path)
    print(f"  {len(raw_jobs)} jobs to parse")

    backend = OpenAIBackend(base_url, model)
    texts = [prepare_job_text(j) for j in raw_jobs]

    out = open(parsed_path, "w")
    successes = 0
    t0 = time.time()

    for i, (raw, text) in enumerate(zip(raw_jobs, texts)):
        results = backend.extract_batch([text])
        result = results[0]
        if result is not None:
            record = {
                "id": raw.get("id") or raw.get("absolute_url", ""),
                "title": raw.get("title", ""),
                "metadata": result.model_dump(mode="json"),
            }
            out.write(json.dumps(record) + "\n")
            successes += 1

        if (i + 1) % 10 == 0 or i == len(raw_jobs) - 1:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            eta = (len(raw_jobs) - i - 1) / rate if rate > 0 else 0
            print(f"  {i+1}/{len(raw_jobs)} | {successes} ok | {rate:.1f} jobs/s | ETA {eta/60:.0f}m")

    out.close()
    print(f"Parsed {successes}/{len(raw_jobs)} → {parsed_path}")
    return successes


def load(parsed_path: str, raw_path: str, clear: bool = True):
    """Load parsed jobs into MeiliSearch."""
    from load_to_meili import load as meili_load
    meili_load(parsed_path, raw_path, clear=clear)


def main():
    parser = argparse.ArgumentParser(description="dopejobs pipeline")
    parser.add_argument("--companies", required=True, help="Companies file (ats:token per line)")
    parser.add_argument("--skip-scrape", action="store_true", help="Skip scraping, reuse existing raw data")
    parser.add_argument("--skip-parse", action="store_true", help="Skip parsing, reuse existing parsed data")
    parser.add_argument("--base-url", default="http://10.0.0.158:1234/v1", help="LLM API base URL")
    parser.add_argument("--model", default="qwen/qwen3.5-35b-a3b", help="LLM model name")
    parser.add_argument("--max-per-company", type=int, default=50, help="Max jobs per company")
    parser.add_argument("--output-dir", default="data/pipeline", help="Output directory")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_path = str(out_dir / "raw.jsonl")
    parsed_path = str(out_dir / "parsed.jsonl")

    # Step 1: Scrape
    if not args.skip_scrape:
        n = scrape(args.companies, raw_path, max_per_company=args.max_per_company)
        if n == 0:
            print("No jobs scraped. Exiting.")
            return
    else:
        print(f"Skipping scrape, using {raw_path}")

    # Step 2: Parse
    if not args.skip_parse:
        parse(raw_path, parsed_path, args.base_url, args.model)
    else:
        print(f"Skipping parse, using {parsed_path}")

    # Step 3: Load
    load(parsed_path, raw_path, clear=True)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
