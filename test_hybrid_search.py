"""Test hybrid search quality across different query types."""
import requests

MEILI = "http://localhost:7700"

queries = [
    ("machine learning engineer", 0.5),
    ("remote work climate change", 0.8),       # semantic-heavy
    ("sales", 0.0),                             # pure keyword
    ("sales", 0.5),                             # hybrid
    ("sales", 1.0),                             # pure semantic
    ("I want to build rockets", 0.9),           # natural language
    ("entry level no experience needed", 0.7),  # intent-based
    ("good work life balance startup", 0.8),    # vibes-based
]

for query, ratio in queries:
    resp = requests.post(f"{MEILI}/indexes/jobs/search", json={
        "q": query,
        "hybrid": {"semanticRatio": ratio, "embedder": "default"},
        "limit": 3,
    })
    results = resp.json()
    hits = results.get("hits", [])
    print(f"\n\"{query}\" (semantic={ratio})")
    for hit in hits:
        print(f"  {hit['title'][:55]:55s} @ {hit['company']:15s} {hit.get('location','')}")
