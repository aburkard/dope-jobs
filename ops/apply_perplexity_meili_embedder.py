import json
import os
import sys

import requests


MEILI_HOST = os.environ.get("MEILI_HOST", "http://127.0.0.1:7700").rstrip("/")
MEILI_KEY = os.environ.get("MEILI_MASTER_KEY", os.environ.get("MEILISEARCH_MASTER_KEY", ""))
SEARCH_EMBEDDER_URL = os.environ.get(
    "PPLX_SEARCH_EMBEDDER_URL", "http://178.156.164.231:8087/openai-search/v1/embeddings"
)
INDEX_EMBEDDER_URL = os.environ.get(
    "PPLX_INDEX_EMBEDDER_URL", "http://178.156.164.231:8087/openai-index/v1/embeddings"
)
MODEL_ID = os.environ.get("PPLX_MODEL_ID", "perplexity/pplx-embed-v1-0.6b")
EMBED_DIM = int(os.environ.get("PPLX_EMBED_DIM", "512"))
INDEX_UID = os.environ.get("MEILI_INDEX_UID", "jobs")
RESET_FIRST = os.environ.get("PPLX_RESET_EMBEDDERS_FIRST", "true").lower() not in {"0", "false", "no"}
ENABLE_COMPOSITE = os.environ.get("PPLX_ENABLE_COMPOSITE", "true").lower() not in {"0", "false", "no"}

DOCUMENT_TEMPLATE = """
{% if doc.title %}Job title: {{ doc.title }}.{% endif %}
{% if doc.company %} Company: {{ doc.company }}.{% endif %}
{% if doc.office_type %} Work setup: {{ doc.office_type }}.{% endif %}
{% if doc.tagline %} Summary: {{ doc.tagline | truncatewords: 16 }}.{% endif %}
{% if doc.description %} Description: {{ doc.description | truncatewords: 90 }}{% endif %}
""".strip()


def main() -> int:
    headers = {
        "Content-Type": "application/json",
        **({"Authorization": f"Bearer {MEILI_KEY}"} if MEILI_KEY else {}),
    }
    if ENABLE_COMPOSITE:
        features = requests.patch(
            f"{MEILI_HOST}/experimental-features",
            headers=headers,
            data=json.dumps({"compositeEmbedders": True}),
            timeout=120,
        )
        print(features.status_code)
        print(features.text)
        if not features.ok:
            return 1

    if RESET_FIRST:
        reset = requests.delete(
            f"{MEILI_HOST}/indexes/{INDEX_UID}/settings/embedders",
            headers=headers,
            timeout=120,
        )
        print(reset.status_code)
        print(reset.text)
        if not reset.ok:
            return 1

    body = {
        "default": {
            "source": "composite",
            "indexingEmbedder": {
                "source": "openAi",
                "model": MODEL_ID,
                "url": INDEX_EMBEDDER_URL,
                "dimensions": EMBED_DIM,
                "documentTemplate": DOCUMENT_TEMPLATE,
                "documentTemplateMaxBytes": 8000,
            },
            "searchEmbedder": {
                "source": "openAi",
                "model": MODEL_ID,
                "url": SEARCH_EMBEDDER_URL,
                "dimensions": EMBED_DIM,
            },
        }
    }

    resp = requests.patch(
        f"{MEILI_HOST}/indexes/{INDEX_UID}/settings/embedders",
        headers=headers,
        data=json.dumps(body),
        timeout=120,
    )
    print(resp.status_code)
    print(resp.text)
    return 0 if resp.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
