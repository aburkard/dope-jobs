"""Test Gemini embedding API and configure MeiliSearch embedder."""
import os
import json
import requests
from dotenv import load_dotenv
load_dotenv()

GEMINI_KEY = os.environ.get("GEMINI_API_KEY")
MEILI_HOST = "http://localhost:7700"

# Step 1: Test Gemini embedding API
print("=== Testing Gemini Embedding API ===")
resp = requests.post(
    f"https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent",
    headers={"Content-Type": "application/json", "x-goog-api-key": GEMINI_KEY},
    json={"content": {"parts": [{"text": "Senior React developer at a climate tech startup"}]}},
    timeout=10,
)
print(f"Status: {resp.status_code}")
if resp.ok:
    data = resp.json()
    embedding = data["embedding"]["values"]
    print(f"Dimensions: {len(embedding)}")
    print(f"First 5 values: {embedding[:5]}")
else:
    print(f"Error: {resp.text[:200]}")
    exit(1)

# Step 2: Configure MeiliSearch rest embedder
# The Gemini API format:
#   Request: {"content": {"parts": [{"text": "..."}]}}
#   Response: {"embedding": {"values": [...]}}
print("\n=== Configuring MeiliSearch embedder ===")

embedder_config = {
    "embedders": {
        "default": {
            "source": "rest",
            "url": f"https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent?key={GEMINI_KEY}",
            "dimensions": len(embedding),
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
    }
}

resp = requests.patch(
    f"{MEILI_HOST}/indexes/jobs/settings",
    headers={"Content-Type": "application/json"},
    json=embedder_config,
    timeout=10,
)
print(f"Settings update status: {resp.status_code}")
print(f"Response: {resp.text[:200]}")

if resp.ok:
    task_uid = resp.json().get("taskUid")
    print(f"Task UID: {task_uid}")

    # Wait for task
    import time
    for _ in range(30):
        task_resp = requests.get(f"{MEILI_HOST}/tasks/{task_uid}")
        task = task_resp.json()
        status = task.get("status")
        print(f"  Task status: {status}")
        if status in ("succeeded", "failed"):
            if status == "failed":
                print(f"  Error: {task.get('error')}")
            break
        time.sleep(2)

# Step 3: Test hybrid search
print("\n=== Testing hybrid search ===")
search_resp = requests.post(
    f"{MEILI_HOST}/indexes/jobs/search",
    json={
        "q": "machine learning engineer",
        "hybrid": {
            "semanticRatio": 0.5,
            "embedder": "default",
        },
        "limit": 3,
    },
    timeout=30,
)
if search_resp.ok:
    results = search_resp.json()
    print(f"Hits: {results.get('estimatedTotalHits', 0)}")
    for hit in results.get("hits", []):
        print(f"  {hit['title']} @ {hit['company']} — {hit.get('location', '')}")
else:
    print(f"Search error: {search_resp.status_code} {search_resp.text[:200]}")
