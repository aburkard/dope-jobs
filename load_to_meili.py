"""Load parsed job data into MeiliSearch."""
import argparse
import json
import meilisearch
from utils.html_utils import remove_html_markup

MEILI_HOST = "http://localhost:7700"
INDEX_NAME = "jobs"


def build_doc(record: dict, raw: dict) -> dict:
    """Convert a parsed record + raw job into a MeiliSearch document."""
    m = record["metadata"]

    # Location string
    locs = m.get("locations", [])
    loc_parts = []
    if locs:
        loc = locs[0]
        if loc.get("city"):
            loc_parts.append(loc["city"])
        if loc.get("state"):
            loc_parts.append(loc["state"])
        if loc.get("country_code"):
            loc_parts.append(loc["country_code"])
    location_str = ", ".join(loc_parts) if loc_parts else ""
    if not location_str:
        raw_loc = raw.get("location", {})
        if isinstance(raw_loc, dict):
            location_str = raw_loc.get("name", "")
        elif isinstance(raw_loc, str):
            location_str = raw_loc

    # Description
    description = (
        raw.get("content", "")
        or raw.get("description", "")
        or raw.get("descriptionHtml", "")
        or ""
    )
    if description:
        description = remove_html_markup(description, double_unescape=True)

    # Company
    board = raw.get("board_token", "")
    ats = raw.get("ats_name", "")
    company = board.replace("-", " ").replace("_", " ").title()

    # Salary
    sal = m.get("salary")
    salary_min = sal["min"] if sal else None
    salary_max = sal["max"] if sal else None
    salary_currency = sal["currency"] if sal else None
    salary_period = sal["period"] if sal else None

    # Geo
    geo = None
    if locs and locs[0].get("lat") and locs[0].get("lng"):
        geo = {"lat": locs[0]["lat"], "lng": locs[0]["lng"]}

    return {
        "id": f"{ats}__{board}__{record['id']}",
        "title": record["title"],
        "tagline": m.get("tagline", ""),
        "company": company,
        "company_slug": board,
        "description": description[:3000],
        "url": raw.get("absolute_url", raw.get("url", raw.get("hostedUrl", ""))),
        "location": location_str,
        "_geo": geo,
        "office_type": m["office_type"],
        "job_type": m["job_type"],
        "experience_level": m["experience_level"],
        "is_manager": m["is_manager"],
        "industry": m["industry"],
        "salary_min": salary_min,
        "salary_max": salary_max,
        "salary_currency": salary_currency,
        "salary_period": salary_period,
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
        "ats_type": ats,
    }


def load(parsed_path: str, raw_path: str, clear: bool = False):
    # Load raw jobs for enrichment
    raw_lookup = {}
    with open(raw_path) as f:
        for line in f:
            job = json.loads(line)
            raw_lookup[job.get("id", job.get("absolute_url", ""))] = job

    # Build documents
    docs = []
    with open(parsed_path) as f:
        for line in f:
            record = json.loads(line)
            raw = raw_lookup.get(record["id"], {})
            docs.append(build_doc(record, raw))

    print(f"Prepared {len(docs)} documents")

    # Index
    client = meilisearch.Client(MEILI_HOST)
    index = client.index(INDEX_NAME)

    if clear:
        task = index.delete_all_documents()
        client.wait_for_task(task.task_uid)
        print("Cleared existing documents")

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
    index.update_sortable_attributes([
        "salary_min", "salary_max",
    ])

    task = index.add_documents(docs, primary_key="id")
    print(f"Indexing... task uid: {task.task_uid}")
    client.wait_for_task(task.task_uid)

    stats = index.get_stats()
    print(f"Done! {stats.number_of_documents} documents in index")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("parsed", help="Parsed JSONL file")
    parser.add_argument("raw", help="Raw scraped JSONL file")
    parser.add_argument("--clear", action="store_true", help="Clear index first")
    args = parser.parse_args()
    load(args.parsed, args.raw, args.clear)
