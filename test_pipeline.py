"""Quick test of the incremental pipeline."""
import os
from dotenv import load_dotenv
load_dotenv()

from db import get_connection, init_schema

conn = get_connection()

# Clear tables for fresh test
with conn.cursor() as cur:
    cur.execute("DELETE FROM pipeline_jobs")
    cur.execute("DELETE FROM pipeline_companies")
conn.commit()
print("Cleared pipeline tables")

# Run scrape on just 3 companies
from pipeline import parse_companies_file, step_scrape, step_parse, step_load

companies = [
    ("greenhouse", "stripe"),
    ("greenhouse", "duolingo"),
    ("greenhouse", "watershed"),
]

# Step 1: Scrape
step_scrape(conn, companies, max_per_company=3)

# Check DB state
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM pipeline_jobs")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM pipeline_jobs WHERE needs_parse = TRUE")
    pending = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM pipeline_jobs WHERE raw_json IS NOT NULL")
    has_raw = cur.fetchone()[0]
print(f"\nDB state: {total} jobs, {pending} need parse, {has_raw} have raw_json")

# Step 2: Parse (using OpenAI API for speed)
step_parse(conn, "https://api.openai.com/v1", "gpt-5.4-nano-2026-03-17",
           api_key=os.environ.get("OPENAI_API_KEY"))

# Check parse results
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM pipeline_jobs WHERE parsed_json IS NOT NULL")
    parsed = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM pipeline_jobs WHERE needs_parse = TRUE")
    still_pending = cur.fetchone()[0]
print(f"\nAfter parse: {parsed} parsed, {still_pending} still pending")

# Step 3: Load to MeiliSearch
step_load(conn)

# Step 4: Re-scrape — should be all unchanged
print("\n\n=== RE-SCRAPE (should be all unchanged) ===")
step_scrape(conn, companies, max_per_company=3)

with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM pipeline_jobs WHERE needs_parse = TRUE")
    pending2 = cur.fetchone()[0]
print(f"After re-scrape: {pending2} need parse (should be 0)")

conn.close()
print("\nTest complete!")
