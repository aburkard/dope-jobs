from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


DOPEJOBS_ROOT = Path("/Users/aburkard/fun/dopejobs")
PIPELINE_ROOT = Path("/Users/aburkard/fun/dope-jobs-pipeline")
ENV_PATH = DOPEJOBS_ROOT / ".env"


@dataclass
class DbStatus:
    active_raw: int
    pending_load: int
    loaded: int
    parsed: int
    unparsed: int
    parsed_loaded: int
    unparsed_loaded: int
    percent_loaded: float


def parse_timestamp(raw: str | None) -> datetime | None:
    if not raw:
        return None
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC)


def estimate_throughput(recent_tasks: list[dict[str, Any]]) -> dict[str, float | int | None]:
    succeeded = [task for task in recent_tasks if task.get("status") == "succeeded"]
    if not succeeded:
        return {
            "recent_batches": 0,
            "docs_per_second": None,
            "seconds_remaining": None,
            "hours_remaining": None,
        }

    total_docs = 0
    total_seconds = 0.0
    counted_batches = 0
    for task in succeeded:
        started_at = parse_timestamp(task.get("startedAt"))
        finished_at = parse_timestamp(task.get("finishedAt"))
        docs = (task.get("details") or {}).get("indexedDocuments") or (task.get("details") or {}).get("receivedDocuments") or 0
        if not started_at or not finished_at or not docs:
            continue
        elapsed = (finished_at - started_at).total_seconds()
        if elapsed <= 0:
            continue
        total_docs += int(docs)
        total_seconds += elapsed
        counted_batches += 1

    if total_docs <= 0 or total_seconds <= 0:
        return {
            "recent_batches": counted_batches,
            "docs_per_second": None,
            "seconds_remaining": None,
            "hours_remaining": None,
        }

    docs_per_second = total_docs / total_seconds
    return {
        "recent_batches": counted_batches,
        "docs_per_second": docs_per_second,
        "seconds_remaining": None,
        "hours_remaining": None,
    }


def load_pipeline_modules():
    load_dotenv(ENV_PATH)
    sys.path.insert(0, str(PIPELINE_ROOT))
    from db import MEILI_DOC_SCHEMA_VERSION, get_connection

    return MEILI_DOC_SCHEMA_VERSION, get_connection


def get_db_status() -> DbStatus:
    meili_doc_schema_version, get_connection = load_pipeline_modules()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM pipeline_jobs
                WHERE removed_at IS NULL
                  AND raw_json IS NOT NULL
                """
            )
            active_raw = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM pipeline_jobs
                WHERE removed_at IS NULL
                  AND raw_json IS NOT NULL
                  AND (
                      (
                          meili_loaded_doc_version IS NULL
                          AND (
                              meili_loaded_content_hash IS DISTINCT FROM content_hash
                              OR meili_loaded_last_parsed_at IS DISTINCT FROM last_parsed_at
                          )
                      )
                      OR (
                          meili_loaded_doc_version IS NOT NULL
                          AND meili_loaded_doc_version IS DISTINCT FROM md5(
                              concat_ws(
                                  '|',
                                  COALESCE(%s, ''),
                                  COALESCE(content_hash, ''),
                                  COALESCE(last_parsed_at::text, ''),
                                  COALESCE(job_group, id)
                              )
                          )
                      )
                  )
                """,
                (meili_doc_schema_version,),
            )
            pending_load = cur.fetchone()[0]

            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE last_parsed_at IS NOT NULL) AS parsed,
                    COUNT(*) FILTER (WHERE last_parsed_at IS NULL) AS unparsed,
                    COUNT(*) FILTER (
                        WHERE last_parsed_at IS NOT NULL
                          AND meili_loaded_doc_version = md5(
                              concat_ws(
                                  '|',
                                  COALESCE(%s, ''),
                                  COALESCE(content_hash, ''),
                                  COALESCE(last_parsed_at::text, ''),
                                  COALESCE(job_group, id)
                              )
                          )
                    ) AS parsed_loaded,
                    COUNT(*) FILTER (
                        WHERE last_parsed_at IS NULL
                          AND meili_loaded_doc_version = md5(
                              concat_ws(
                                  '|',
                                  COALESCE(%s, ''),
                                  COALESCE(content_hash, ''),
                                  COALESCE(last_parsed_at::text, ''),
                                  COALESCE(job_group, id)
                              )
                          )
                    ) AS unparsed_loaded
                FROM pipeline_jobs
                WHERE removed_at IS NULL
                  AND raw_json IS NOT NULL
                """,
                (meili_doc_schema_version, meili_doc_schema_version),
            )
            parsed, unparsed, parsed_loaded, unparsed_loaded = cur.fetchone()

        loaded = active_raw - pending_load
        percent_loaded = (loaded / active_raw * 100.0) if active_raw else 100.0
        return DbStatus(
            active_raw=active_raw,
            pending_load=pending_load,
            loaded=loaded,
            parsed=parsed,
            unparsed=unparsed,
            parsed_loaded=parsed_loaded,
            unparsed_loaded=unparsed_loaded,
            percent_loaded=percent_loaded,
        )
    finally:
        conn.close()


def meili_candidates() -> list[str]:
    raw = [
        os.environ.get("MEILI_HOST"),
        "http://127.0.0.1:17701",
        os.environ.get("MEILISEARCH_HOST"),
    ]
    seen: set[str] = set()
    ordered: list[str] = []
    for host in raw:
        if not host:
            continue
        host = host.rstrip("/")
        if host in seen:
            continue
        seen.add(host)
        ordered.append(host)
    return ordered


def get_meili_status() -> dict[str, Any]:
    key = os.environ.get("MEILI_MASTER_KEY") or os.environ.get("MEILISEARCH_MASTER_KEY", "")
    headers = {"Authorization": f"Bearer {key}"} if key else {}
    errors: list[dict[str, str]] = []

    for host in meili_candidates():
        try:
            stats_response = requests.get(
                f"{host}/indexes/jobs/stats",
                headers=headers,
                timeout=5,
            )
            stats_response.raise_for_status()
            tasks_response = requests.get(
                f"{host}/tasks",
                params={"indexUids": "jobs", "limit": 10},
                headers=headers,
                timeout=5,
            )
            tasks_response.raise_for_status()
            stats = stats_response.json()
            tasks = tasks_response.json().get("results", [])
            return {
                "host": host,
                "stats": {
                    "numberOfDocuments": stats.get("numberOfDocuments"),
                    "numberOfEmbeddings": stats.get("numberOfEmbeddings"),
                    "isIndexing": stats.get("isIndexing"),
                    "fieldDistribution": stats.get("fieldDistribution"),
                },
                "recent_tasks": [
                    {
                        "uid": task.get("uid"),
                        "type": task.get("type"),
                        "status": task.get("status"),
                        "canceledBy": task.get("canceledBy"),
                        "details": task.get("details"),
                        "error": task.get("error"),
                        "enqueuedAt": task.get("enqueuedAt"),
                        "startedAt": task.get("startedAt"),
                        "finishedAt": task.get("finishedAt"),
                    }
                    for task in tasks
                ],
            }
        except requests.RequestException as exc:
            errors.append({"host": host, "error": repr(exc)})

    return {"errors": errors}


def main() -> int:
    db_status = get_db_status()
    meili_status = get_meili_status()
    rate = estimate_throughput(meili_status.get("recent_tasks", []))
    if rate["docs_per_second"]:
        seconds_remaining = db_status.pending_load / float(rate["docs_per_second"])
        rate["seconds_remaining"] = seconds_remaining
        rate["hours_remaining"] = seconds_remaining / 3600.0

    summary = {
        "db": asdict(db_status),
        "meili": meili_status,
        "throughput": rate,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
