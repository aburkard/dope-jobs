"""Detect and assign job groups for multi-location deduplication.

Groups jobs that are the same role posted in different locations.
Uses exact title match + content similarity (>95%) to avoid false positives.
"""
import hashlib
import difflib
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv()

from db import get_connection
from parse import prepare_job_text


SIMILARITY_THRESHOLD = 0.95


def strip_metadata_lines(text: str, n: int = 5) -> str:
    """Strip first N lines which are typically location/department metadata."""
    lines = text.split("\n")[n:]
    return "\n".join(lines)


def content_similarity(text_a: str, text_b: str) -> float:
    """Compute similarity ratio between two job descriptions,
    ignoring the first few lines of metadata."""
    a = strip_metadata_lines(text_a)
    b = strip_metadata_lines(text_b)
    return difflib.SequenceMatcher(None, a, b).ratio()


def compute_job_groups(conn) -> dict:
    """Compute job_group assignments for all parsed jobs.

    Returns dict of {job_id: job_group_hash} for jobs that belong to a group.
    Jobs with no group (unique postings) are not included.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, board_token, title, raw_json
            FROM pipeline_jobs
            WHERE parsed_json IS NOT NULL AND removed_at IS NULL
            ORDER BY board_token, title
        """)
        rows = cur.fetchall()

    # Group candidates by company + exact title
    by_key = defaultdict(list)
    for job_id, board, title, raw_json in rows:
        by_key[(board, title)].append((job_id, raw_json))

    groups = {}
    group_stats = {"groups": 0, "grouped_jobs": 0, "singletons": 0}

    for (board, title), candidates in by_key.items():
        if len(candidates) == 1:
            group_stats["singletons"] += 1
            continue

        # Prepare texts
        texts = {}
        for job_id, raw_json in candidates:
            if raw_json:
                texts[job_id] = prepare_job_text(raw_json)

        if len(texts) < 2:
            continue

        # Cluster by similarity
        # Simple approach: compare each to the first, group if similar
        # (For most cases, all variants of a role are similar to each other)
        job_ids = list(texts.keys())
        reference_id = job_ids[0]
        reference_text = texts[reference_id]

        cluster = [reference_id]
        for jid in job_ids[1:]:
            sim = content_similarity(reference_text, texts[jid])
            if sim >= SIMILARITY_THRESHOLD:
                cluster.append(jid)

        if len(cluster) > 1:
            # Generate group hash from company + title
            group_hash = hashlib.sha256(f"{board}__{title}".encode()).hexdigest()[:16]
            for jid in cluster:
                groups[jid] = group_hash
            group_stats["groups"] += 1
            group_stats["grouped_jobs"] += len(cluster)

        # Handle remaining jobs that didn't match the reference
        # (e.g., same title but different team)
        remaining = [jid for jid in job_ids if jid not in cluster]
        if len(remaining) > 1:
            # These could form their own subgroup — check pairwise
            ref2 = remaining[0]
            cluster2 = [ref2]
            for jid in remaining[1:]:
                sim = content_similarity(texts[ref2], texts[jid])
                if sim >= SIMILARITY_THRESHOLD:
                    cluster2.append(jid)
            if len(cluster2) > 1:
                group_hash2 = hashlib.sha256(f"{board}__{title}__alt".encode()).hexdigest()[:16]
                for jid in cluster2:
                    groups[jid] = group_hash2
                group_stats["groups"] += 1
                group_stats["grouped_jobs"] += len(cluster2)

    return groups, group_stats


def save_job_groups(conn, groups: dict):
    """Save job_group assignments to DB and update MeiliSearch documents."""
    # Add job_group column if not exists
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE pipeline_jobs ADD COLUMN IF NOT EXISTS job_group TEXT")
    conn.commit()

    # Clear existing groups
    with conn.cursor() as cur:
        cur.execute("UPDATE pipeline_jobs SET job_group = NULL")

    # Set new groups
    for job_id, group_hash in groups.items():
        with conn.cursor() as cur:
            cur.execute("UPDATE pipeline_jobs SET job_group = %s WHERE id = %s", (group_hash, job_id))
    conn.commit()


def get_group_summary(conn) -> list[dict]:
    """Get summary of all job groups for display."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT job_group, board_token, title, COUNT(*) as cnt,
                   array_agg(parsed_json->'locations'->0->>'city') as cities
            FROM pipeline_jobs
            WHERE job_group IS NOT NULL AND removed_at IS NULL
            GROUP BY job_group, board_token, title
            ORDER BY cnt DESC
        """)
        return [
            {"group": r[0], "company": r[1], "title": r[2], "count": r[3], "cities": r[4]}
            for r in cur.fetchall()
        ]


if __name__ == "__main__":
    conn = get_connection()

    print("Computing job groups...")
    groups, stats = compute_job_groups(conn)
    print(f"  Groups: {stats['groups']}")
    print(f"  Grouped jobs: {stats['grouped_jobs']}")
    print(f"  Singleton jobs: {stats['singletons']}")

    print("\nSaving to DB...")
    save_job_groups(conn, groups)

    print("\nTop groups:")
    summary = get_group_summary(conn)
    for g in summary[:15]:
        cities = [c for c in g["cities"] if c][:5]
        print(f"  {g['count']:3d}x  {g['company']:20s}  {g['title'][:40]:40s}  {', '.join(cities)}")

    conn.close()
    print("\nDone!")
