"""Load a sample of parsed + raw job data into local MeiliSearch."""
import bz2
import json
import meilisearch
from utils.html_utils import remove_html_markup

MEILI_HOST = "http://localhost:7700"
SAMPLE_SIZE = 500

def load_raw_jobs():
    """Load raw jobs into a dict keyed by id."""
    raw = {}
    for ats in ["greenhouse", "lever", "ashby", "jobvite"]:
        path = f"data/raw/{ats}.jsonl.bz2"
        try:
            with bz2.open(path, "rt") as f:
                for line in f:
                    job = json.loads(line)
                    # Build the same ID format used in parsed data
                    board = job.get("board_token", "")
                    ats_name = job.get("ats_name", ats)
                    job_id = job.get("id", job.get("hash_id", ""))
                    key = f"{ats_name}__{board}__{job_id}"
                    raw[key] = job
        except FileNotFoundError:
            print(f"Skipping {path} (not found)")
    return raw


def load_parsed_jobs(raw_lookup, limit=SAMPLE_SIZE):
    """Merge parsed metadata with raw job data."""
    docs = []
    with bz2.open("data/parsed_data.jsonl.bz2", "rt") as f:
        for i, line in enumerate(f):
            if i >= limit:
                break
            parsed = json.loads(line)
            job_id = parsed.get("id", "")
            raw = raw_lookup.get(job_id, {})

            # Extract company from board token
            parts = job_id.split("__")
            ats_type = parts[0] if len(parts) > 0 else ""
            company_slug = parts[1] if len(parts) > 1 else ""

            # Clean description
            description = raw.get("content", "") or raw.get("description", "")
            if description:
                description = remove_html_markup(description, double_unescape=True)

            # Build location string
            locations = parsed.get("locations", [])
            location_str = ""
            if locations and isinstance(locations, list) and len(locations) > 0:
                loc = locations[0]
                parts_loc = []
                if loc.get("city"):
                    parts_loc.append(loc["city"])
                if loc.get("state"):
                    parts_loc.append(loc["state"])
                if loc.get("country"):
                    parts_loc.append(loc["country"])
                location_str = ", ".join(parts_loc)
            if not location_str:
                location_str = raw.get("location", {}).get("name", "") if isinstance(raw.get("location"), dict) else str(raw.get("location", ""))

            # Office type
            ot = parsed.get("office_type", {})
            if isinstance(ot, dict):
                if ot.get("remote"):
                    office_type = "remote"
                elif ot.get("hybrid"):
                    office_type = "hybrid"
                elif ot.get("onsite"):
                    office_type = "onsite"
                else:
                    office_type = "unknown"
            else:
                office_type = str(ot) if ot else "unknown"

            # Salary
            salary = parsed.get("salary", {})
            salary_min = salary.get("min") if isinstance(salary, dict) else None
            salary_max = salary.get("max") if isinstance(salary, dict) else None
            salary_currency = salary.get("currency", "USD") if isinstance(salary, dict) else "USD"

            doc = {
                "id": job_id,
                "title": raw.get("title", parsed.get("tagline", "")),
                "company": company_slug.replace("-", " ").replace("_", " ").title(),
                "company_slug": company_slug,
                "company_logo": raw.get("company_logo", None),
                "description": description[:2000] if description else "",
                "url": raw.get("absolute_url", raw.get("url", "")),
                "location": location_str,
                "office_type": office_type,
                "job_type": parsed.get("job_type", "unknown") or "unknown",
                "experience_level": parsed.get("experience_level", "unknown") or "unknown",
                "is_manager": parsed.get("is_manager", False),
                "salary_min": salary_min,
                "salary_max": salary_max,
                "salary_currency": salary_currency,
                "hard_skills": parsed.get("hard_skills", []),
                "soft_skills": parsed.get("soft_skills", []),
                "benefits": parsed.get("benefits", []),
                "tags": parsed.get("tags", []),
                "ats_type": ats_type,
                "industry": parsed.get("industry", ""),
            }
            docs.append(doc)

    return docs


def index_to_meilisearch(docs):
    client = meilisearch.Client(MEILI_HOST)
    index = client.index("jobs")

    # Configure index settings
    index.update_filterable_attributes([
        "office_type", "job_type", "experience_level", "ats_type",
        "is_manager", "industry", "company_slug",
    ])
    index.update_searchable_attributes([
        "title", "company", "description", "location", "tags", "hard_skills",
    ])
    index.update_sortable_attributes([
        "salary_min", "salary_max",
    ])

    # Add documents
    task = index.add_documents(docs, primary_key="id")
    print(f"Indexing {len(docs)} documents... task uid: {task.task_uid}")

    # Wait for completion
    client.wait_for_task(task.task_uid)
    print("Done!")

    # Verify
    stats = index.get_stats()
    print(f"Index stats: {stats.number_of_documents} documents")


if __name__ == "__main__":
    print("Loading raw jobs...")
    raw = load_raw_jobs()
    print(f"Loaded {len(raw)} raw jobs")

    print("Loading and merging parsed jobs...")
    docs = load_parsed_jobs(raw)
    print(f"Prepared {len(docs)} documents")

    print("Indexing to MeiliSearch...")
    index_to_meilisearch(docs)
