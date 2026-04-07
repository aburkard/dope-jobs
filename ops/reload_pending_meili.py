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

    from db import get_connection, get_job_ids_pending_meili_load
    from pipeline import step_load

    meili_key = os.environ.get("MEILISEARCH_MASTER_KEY") or MEILI_KEY_FALLBACK

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
