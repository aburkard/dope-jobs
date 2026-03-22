"""
Pipeline state database (Neon Postgres).

Tracks companies, jobs, content hashes, and parse state.
Used for incremental pipeline runs — only re-parse jobs that actually changed.
"""

import hashlib
import json
import os
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values, Json

from utils.html_utils import remove_html_markup


def get_connection():
    """Get a Postgres connection using DATABASE_URL from environment."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set. Add it to .env")
    return psycopg2.connect(url)


def init_schema(conn):
    """Create tables if they don't exist."""
    statements = [
        """CREATE TABLE IF NOT EXISTS pipeline_companies (
            ats TEXT NOT NULL,
            board_token TEXT NOT NULL,
            company_name TEXT,
            domain TEXT,
            last_scraped_at TIMESTAMPTZ,
            job_count INTEGER DEFAULT 0,
            active BOOLEAN DEFAULT TRUE,
            PRIMARY KEY (ats, board_token)
        )""",
        """CREATE TABLE IF NOT EXISTS pipeline_jobs (
            id TEXT PRIMARY KEY,
            ats TEXT NOT NULL,
            board_token TEXT NOT NULL,
            title TEXT,
            content_hash TEXT,
            first_seen_at TIMESTAMPTZ DEFAULT NOW(),
            last_seen_at TIMESTAMPTZ DEFAULT NOW(),
            last_parsed_at TIMESTAMPTZ,
            removed_at TIMESTAMPTZ,
            needs_parse BOOLEAN DEFAULT TRUE,
            raw_json JSONB,
            parsed_json JSONB
        )""",
        "CREATE INDEX IF NOT EXISTS idx_jobs_needs_parse ON pipeline_jobs (needs_parse) WHERE needs_parse = TRUE",
        "CREATE INDEX IF NOT EXISTS idx_jobs_board ON pipeline_jobs (ats, board_token)",
    ]
    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)
    conn.commit()


def content_hash(raw_job: dict) -> str:
    """Compute SHA256 of cleaned job text (title + description)."""
    title = raw_job.get("title", "") or ""
    content = (
        raw_job.get("content", "")
        or raw_job.get("description", "")
        or raw_job.get("descriptionHtml", "")
        or ""
    )
    if content:
        content = remove_html_markup(content, double_unescape=True)
    text = f"{title}\n{content}".strip()
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def job_id(raw_job: dict) -> str:
    """Get the compound job ID. The scraper's normalize_job already builds
    the format ats__board_token__job_id in the 'id' field."""
    jid = raw_job.get("id", "")
    if jid and "__" in jid:
        return jid
    # Fallback: build it ourselves
    ats = raw_job.get("ats_name", "")
    board = raw_job.get("board_token", "")
    return f"{ats}__{board}__{jid}"


def upsert_scraped_jobs(conn, scraped_jobs: list[dict]) -> dict:
    """
    Compare scraped jobs against DB state. Returns dict with:
      - new: list of raw jobs that are new
      - changed: list of raw jobs whose content changed
      - unchanged: count of jobs that didn't change
      - removed: count of jobs marked as removed
    """
    if not scraped_jobs:
        return {"new": [], "changed": [], "unchanged": 0, "removed": 0}

    now = datetime.now(timezone.utc)

    # Get all job IDs and hashes for this batch
    job_data = []
    for raw in scraped_jobs:
        jid = job_id(raw)
        h = content_hash(raw)
        ats = raw.get("ats_name", "")
        board = raw.get("board_token", "")
        title = raw.get("title", "")
        job_data.append((jid, ats, board, title, h, raw))

    # Fetch existing hashes from DB
    ids = [jd[0] for jd in job_data]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, content_hash FROM pipeline_jobs WHERE id = ANY(%s)",
            (ids,)
        )
        existing = {row[0]: row[1] for row in cur.fetchall()}

    new_jobs = []
    changed_jobs = []
    unchanged = 0

    for jid, ats, board, title, h, raw in job_data:
        if jid not in existing:
            # New job
            new_jobs.append(raw)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pipeline_jobs (id, ats, board_token, title, content_hash, first_seen_at, last_seen_at, needs_parse, raw_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        last_seen_at = EXCLUDED.last_seen_at,
                        content_hash = EXCLUDED.content_hash,
                        needs_parse = TRUE,
                        removed_at = NULL,
                        raw_json = EXCLUDED.raw_json
                """, (jid, ats, board, title, h, now, now, Json(raw)))
        elif existing[jid] != h:
            # Content changed
            changed_jobs.append(raw)
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE pipeline_jobs SET
                        content_hash = %s,
                        title = %s,
                        last_seen_at = %s,
                        needs_parse = TRUE,
                        removed_at = NULL,
                        raw_json = %s
                    WHERE id = %s
                """, (h, title, now, Json(raw), jid))
        else:
            # Unchanged
            unchanged += 1
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE pipeline_jobs SET last_seen_at = %s, removed_at = NULL WHERE id = %s",
                    (now, jid)
                )

    conn.commit()
    return {"new": new_jobs, "changed": changed_jobs, "unchanged": unchanged, "removed": 0}


def mark_removed(conn, ats: str, board_token: str, seen_ids: set[str]) -> int:
    """Mark jobs as removed if they weren't seen in the latest scrape."""
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_jobs SET removed_at = %s
            WHERE ats = %s AND board_token = %s
                AND removed_at IS NULL
                AND id != ALL(%s)
        """, (now, ats, board_token, list(seen_ids)))
        removed = cur.rowcount
    conn.commit()
    return removed


def get_jobs_needing_parse(conn, limit: int | None = None) -> list[dict]:
    """Get jobs that need LLM parsing, including raw_json for text preparation."""
    query = "SELECT id, ats, board_token, title, raw_json FROM pipeline_jobs WHERE needs_parse = TRUE AND removed_at IS NULL"
    if limit:
        query += f" LIMIT {limit}"
    with conn.cursor() as cur:
        cur.execute(query)
        return [{"id": r[0], "ats": r[1], "board_token": r[2], "title": r[3], "raw_json": r[4]} for r in cur.fetchall()]


def save_parsed_result(conn, job_id: str, parsed_json: dict):
    """Save LLM extraction result and clear needs_parse flag."""
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_jobs SET
                parsed_json = %s,
                last_parsed_at = %s,
                needs_parse = FALSE
            WHERE id = %s
        """, (Json(parsed_json), now, job_id))
    conn.commit()


def get_parsed_jobs(conn, include_removed: bool = False) -> list[dict]:
    """Get all parsed jobs for loading into MeiliSearch."""
    query = "SELECT id, ats, board_token, title, parsed_json FROM pipeline_jobs WHERE parsed_json IS NOT NULL"
    if not include_removed:
        query += " AND removed_at IS NULL"
    with conn.cursor() as cur:
        cur.execute(query)
        return [
            {"id": r[0], "ats": r[1], "board_token": r[2], "title": r[3], "parsed_json": r[4]}
            for r in cur.fetchall()
        ]


def get_removed_job_ids(conn) -> list[str]:
    """Get IDs of jobs that have been removed (for MeiliSearch deletion)."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM pipeline_jobs WHERE removed_at IS NOT NULL")
        return [r[0] for r in cur.fetchall()]


def upsert_company(conn, ats: str, board_token: str, company_name: str | None = None,
                    domain: str | None = None, job_count: int = 0):
    """Upsert a company record."""
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_companies (ats, board_token, company_name, domain, last_scraped_at, job_count, active)
            VALUES (%s, %s, %s, %s, %s, %s, %s > 0)
            ON CONFLICT (ats, board_token) DO UPDATE SET
                company_name = COALESCE(EXCLUDED.company_name, pipeline_companies.company_name),
                domain = COALESCE(EXCLUDED.domain, pipeline_companies.domain),
                last_scraped_at = EXCLUDED.last_scraped_at,
                job_count = EXCLUDED.job_count,
                active = EXCLUDED.job_count > 0
        """, (ats, board_token, company_name, domain, now, job_count, job_count))
    conn.commit()


if __name__ == "__main__":
    """Initialize the schema."""
    from dotenv import load_dotenv
    load_dotenv()

    conn = get_connection()
    init_schema(conn)
    print("Schema initialized.")

    # Show counts
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pipeline_companies")
        print(f"Companies: {cur.fetchone()[0]}")
        cur.execute("SELECT COUNT(*) FROM pipeline_jobs")
        print(f"Jobs: {cur.fetchone()[0]}")
        cur.execute("SELECT COUNT(*) FROM pipeline_jobs WHERE needs_parse = TRUE")
        print(f"Needs parse: {cur.fetchone()[0]}")

    conn.close()
