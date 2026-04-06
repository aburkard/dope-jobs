from __future__ import annotations

import os
import sys
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv


DOPEJOBS_ROOT = Path("/Users/aburkard/fun/dopejobs")
PIPELINE_ROOT = Path("/Users/aburkard/fun/dope-jobs-pipeline")
ENV_PATH = DOPEJOBS_ROOT / ".env"
MEILI_HOST = "http://127.0.0.1:17701"
MEILI_KEY_FALLBACK = "b5ec361a9058eea40af00d05c2ef76e1cc9ba7be"
BATCH_SIZE = 200
RETRY_SLEEP_SECONDS = 5
JOB_GROUP_PROGRESS_EVERY = 25
DB_CONN_RESET_EVERY = 100


def log(*parts: object) -> None:
    print(datetime.now(UTC).isoformat(), *parts, flush=True)


def with_retries(label: str, fn, retries: int = 5):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as exc:
            last_error = exc
            log(f"{label}:retry", attempt, "error", repr(exc))
            if attempt == retries:
                break
            time.sleep(RETRY_SLEEP_SECONDS * attempt)
    raise last_error  # type: ignore[misc]


def close_quietly(conn) -> None:
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass


def main() -> int:
    load_dotenv(ENV_PATH)
    sys.path.insert(0, str(PIPELINE_ROOT))

    from db import get_connection, get_job_ids_pending_meili_load, get_removed_job_ids
    from job_groups import recompute_job_groups_for_boards
    from pipeline import step_load

    meili_key = os.environ.get("MEILISEARCH_MASTER_KEY") or MEILI_KEY_FALLBACK

    def get_boards_requiring_job_group_pass() -> list[tuple[str, str, int]]:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH boards_with_dupes AS (
                        SELECT ats, board_token
                        FROM pipeline_jobs
                        WHERE removed_at IS NULL AND raw_json IS NOT NULL
                        GROUP BY ats, board_token, title
                        HAVING COUNT(*) > 1
                    ),
                    boards_with_groups AS (
                        SELECT DISTINCT ats, board_token
                        FROM pipeline_jobs
                        WHERE removed_at IS NULL AND raw_json IS NOT NULL AND job_group IS NOT NULL
                    ),
                    relevant_boards AS (
                        SELECT ats, board_token FROM boards_with_dupes
                        UNION
                        SELECT ats, board_token FROM boards_with_groups
                    )
                    SELECT rb.ats, rb.board_token, COUNT(*)
                    FROM relevant_boards rb
                    JOIN pipeline_jobs pj
                      ON pj.ats = rb.ats
                     AND pj.board_token = rb.board_token
                    WHERE pj.removed_at IS NULL AND pj.raw_json IS NOT NULL
                    GROUP BY rb.ats, rb.board_token
                    ORDER BY COUNT(*) DESC, rb.ats, rb.board_token
                    """
                )
                return [(ats, board_token, count) for ats, board_token, count in cur.fetchall()]
        finally:
            conn.close()

    boards = with_retries("job_groups:board_list", get_boards_requiring_job_group_pass)
    log("job_groups:start", "boards", len(boards))

    total_changed = 0
    total_groups = 0
    total_grouped_jobs = 0
    total_singletons = 0
    conn = None
    for index, (ats, board_token, job_count) in enumerate(boards, start=1):
        if conn is None:
            conn = with_retries("job_groups:connect", get_connection)

        attempt = 0
        while True:
            attempt += 1
            try:
                changed_ids, stats = recompute_job_groups_for_boards(conn, [(ats, board_token)])
                break
            except Exception as exc:
                log(f"job_groups:{ats}/{board_token}:retry", attempt, "error", repr(exc))
                close_quietly(conn)
                conn = None
                if attempt >= 5:
                    raise
                time.sleep(RETRY_SLEEP_SECONDS * attempt)
                conn = with_retries("job_groups:reconnect", get_connection)

        total_changed += len(changed_ids)
        total_groups += stats["groups"]
        total_grouped_jobs += stats["grouped_jobs"]
        total_singletons += stats["singletons"]

        if (
            index == 1
            or index % JOB_GROUP_PROGRESS_EVERY == 0
            or len(changed_ids) > 0
            or index == len(boards)
        ):
            log(
                "job_groups:progress",
                f"{index}/{len(boards)}",
                f"{ats}/{board_token}",
                "jobs",
                job_count,
                "changed",
                len(changed_ids),
                "groups",
                stats["groups"],
                "grouped_jobs",
                stats["grouped_jobs"],
                "singletons",
                stats["singletons"],
            )
        if index % DB_CONN_RESET_EVERY == 0:
            close_quietly(conn)
            conn = None

    close_quietly(conn)

    log(
        "job_groups:done",
        {
            "changed_ids": total_changed,
            "groups": total_groups,
            "grouped_jobs": total_grouped_jobs,
            "singletons": total_singletons,
        },
    )

    def delete_removed():
        conn = get_connection()
        try:
            removed_ids = get_removed_job_ids(conn)
            if not removed_ids:
                return 0
            log("meili:delete_removed", len(removed_ids))
            step_load(
                conn,
                meili_host=MEILI_HOST,
                meili_key=meili_key,
                parsed_job_ids=[],
                removed_job_ids=removed_ids,
                meili_batch_size=BATCH_SIZE,
            )
            return len(removed_ids)
        finally:
            conn.close()

    with_retries("meili:delete_removed", delete_removed)

    batch_num = 0
    total_loaded = 0
    conn = None
    while True:
        if conn is None:
            conn = with_retries("meili:connect", get_connection)

        try:
            pending_ids = get_job_ids_pending_meili_load(conn, limit=BATCH_SIZE)
        except Exception as exc:
            log("meili:pending:retry", batch_num + 1, "error", repr(exc))
            close_quietly(conn)
            conn = None
            time.sleep(RETRY_SLEEP_SECONDS)
            continue
        if not pending_ids:
            log("meili:done_no_pending", "total_loaded", total_loaded)
            break

        batch_num += 1
        total_loaded += len(pending_ids)
        log(
            "meili:batch",
            batch_num,
            "size",
            len(pending_ids),
            "first",
            pending_ids[0],
            "last",
            pending_ids[-1],
            "total_loaded",
            total_loaded,
        )

        attempt = 0
        while True:
            attempt += 1
            try:
                step_load(
                    conn,
                    meili_host=MEILI_HOST,
                    meili_key=meili_key,
                    parsed_job_ids=pending_ids,
                    removed_job_ids=[],
                    meili_batch_size=BATCH_SIZE,
                )
                break
            except Exception as exc:
                log(f"meili:batch:{batch_num}:retry", attempt, "error", repr(exc))
                close_quietly(conn)
                conn = None
                if attempt >= 5:
                    raise
                time.sleep(RETRY_SLEEP_SECONDS * attempt)
                conn = with_retries("meili:reconnect", get_connection)
        if batch_num % DB_CONN_RESET_EVERY == 0:
            close_quietly(conn)
            conn = None

    close_quietly(conn)
    log("all_done")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        raise
