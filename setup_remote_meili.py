"""Load data and configure embedder on remote MeiliSearch."""
import json
import os
import time
import requests
from dotenv import load_dotenv
load_dotenv()

REMOTE_HOST = "http://178.156.164.231"
MASTER_KEY = "b5ec361a9058eea40af00d05c2ef76e1cc9ba7be"
GEMINI_KEY = os.environ["GEMINI_API_KEY"]
HEADERS = {"Authorization": f"Bearer {MASTER_KEY}", "Content-Type": "application/json"}

# Step 1: Load documents from local parsed data
print("=== Loading documents ===")

# Build docs from curated data (same as load_to_meili.py)
from load_to_meili import build_doc

raw_lookup = {}
with open("data/curated/raw.jsonl") as f:
    for line in f:
        job = json.loads(line)
        raw_lookup[job.get("id", job.get("absolute_url", ""))] = job

docs = []
with open("data/curated/parsed.jsonl") as f:
    for line in f:
        record = json.loads(line)
        raw = raw_lookup.get(record["id"], {})
        docs.append(build_doc(record, raw))

print(f"  {len(docs)} documents prepared")

# Configure index settings
print("=== Configuring index settings ===")
settings = {
    "filterableAttributes": [
        "office_type", "job_type", "experience_level", "is_manager",
        "industry", "company_slug", "ats_type",
        "cool_factor", "vibe_tags", "visa_sponsorship", "equity_offered",
        "company_stage", "benefits_categories", "salary_transparency",
    ],
    "searchableAttributes": [
        "title", "tagline", "company", "description", "location",
        "hard_skills", "soft_skills", "benefits_highlights",
    ],
    "sortableAttributes": ["salary_min", "salary_max"],
    "embedders": {
        "default": {
            "source": "rest",
            "url": f"https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent?key={GEMINI_KEY}",
            "dimensions": 3072,
            "request": {
                "content": {
                    "parts": [{"text": "{{text}}"}]
                }
            },
            "response": {
                "embedding": {
                    "values": "{{embedding}}"
                }
            },
            "documentTemplate": "{{doc.title}} at {{doc.company}}. {{doc.tagline}}. Skills: {{doc.hard_skills}}. {{doc.industry}}. {{doc.office_type}}.",
            "documentTemplateMaxBytes": 1000,
        }
    },
}

resp = requests.patch(f"{REMOTE_HOST}/indexes/jobs/settings", headers=HEADERS, json=settings)
print(f"  Settings: {resp.status_code}")
if resp.ok:
    task_uid = resp.json()["taskUid"]
    # Wait for settings to apply
    for _ in range(10):
        task = requests.get(f"{REMOTE_HOST}/tasks/{task_uid}", headers=HEADERS).json()
        if task["status"] in ("succeeded", "failed"):
            print(f"  Settings task: {task['status']}")
            if task["status"] == "failed":
                print(f"  Error: {task.get('error', {}).get('message', '')[:200]}")
            break
        time.sleep(1)

# Add documents
print("=== Adding documents ===")
resp = requests.post(
    f"{REMOTE_HOST}/indexes/jobs/documents",
    headers=HEADERS,
    json=docs,
    params={"primaryKey": "id"},
)
print(f"  Add docs: {resp.status_code}")
if resp.ok:
    task_uid = resp.json()["taskUid"]
    # Wait for indexing (including embedding generation)
    print("  Waiting for indexing + embedding generation...")
    for i in range(120):  # up to 4 min
        task = requests.get(f"{REMOTE_HOST}/tasks/{task_uid}", headers=HEADERS).json()
        status = task["status"]
        if status in ("succeeded", "failed"):
            print(f"  Indexing task: {status}")
            if status == "failed":
                print(f"  Error: {task.get('error', {}).get('message', '')[:300]}")
            break
        if i % 10 == 0:
            print(f"  ... still processing ({i*2}s)")
        time.sleep(2)

# Verify
print("\n=== Verification ===")
stats = requests.get(f"{REMOTE_HOST}/indexes/jobs/stats", headers=HEADERS).json()
print(f"  Documents: {stats['numberOfDocuments']}")

# Test search
search = requests.post(f"{REMOTE_HOST}/indexes/jobs/search", headers=HEADERS, json={
    "q": "machine learning",
    "hybrid": {"semanticRatio": 0.5, "embedder": "default"},
    "limit": 3,
}).json()
print(f"  Hybrid search test: {search.get('estimatedTotalHits', 0)} hits")
for hit in search.get("hits", []):
    print(f"    {hit['title'][:50]} @ {hit['company']}")

print("\n=== DONE ===")
print(f"Host: {REMOTE_HOST}")
print(f"Master key: {MASTER_KEY}")
print(f"Search key: 0518d4c697915fe71c5fc6534f53a57a20b86ef4ef205e0a66546deef6cc1a2f")
