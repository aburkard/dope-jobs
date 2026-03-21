"""Validate a sample of board tokens from each platform to see how many are still active."""
import random
import requests
import time

SAMPLE_SIZE = 20

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
})


def load_tokens(path):
    with open(path) as f:
        tokens = [line.strip() for line in f if line.strip() and not line.startswith("%")]
    return tokens


def check_greenhouse(token):
    r = session.get(f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs",
                    params={"content": "false"}, timeout=10)
    if r.status_code == 200:
        jobs = r.json().get("jobs", [])
        return len(jobs) > 0, len(jobs)
    return False, 0


def check_lever(token):
    r = session.get(f"https://api.lever.co/v0/postings/{token}",
                    params={"mode": "json", "limit": 1}, timeout=10)
    if r.status_code == 200 and isinstance(r.json(), list):
        return len(r.json()) > 0, len(r.json())
    return False, 0


def check_ashby(token):
    r = session.post("https://jobs.ashbyhq.com/api/non-user-graphql", json={
        "operationName": "ApiJobBoardWithTeams",
        "variables": {"organizationHostedJobsPageName": token},
        "query": """query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {
            jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) {
                jobPostings { id }
            }
        }"""
    }, timeout=10)
    if r.status_code == 200:
        postings = (r.json().get("data", {}).get("jobBoard") or {}).get("jobPostings", [])
        return len(postings) > 0, len(postings)
    return False, 0


def check_jobvite(token):
    r = session.get(f"https://jobs.jobvite.com/{token}/search", timeout=10)
    if r.status_code == 200 and len(r.text) > 5000:
        return True, -1  # Can't easily count without parsing
    return False, 0


def test_platform(name, path, check_fn):
    print(f"\n{'='*50}")
    print(f"  {name}")
    print(f"{'='*50}")
    tokens = load_tokens(path)
    print(f"  Total tokens: {len(tokens)}")
    sample = random.sample(tokens, min(SAMPLE_SIZE, len(tokens)))

    active = 0
    for token in sample:
        try:
            is_active, count = check_fn(token)
            status = f"ACTIVE ({count} jobs)" if is_active else "DEAD"
            print(f"  {token:30s} {status}")
            if is_active:
                active += 1
            time.sleep(0.3)  # Be nice
        except Exception as e:
            print(f"  {token:30s} ERROR: {e}")

    pct = (active / len(sample)) * 100
    print(f"\n  Result: {active}/{len(sample)} active ({pct:.0f}%)")
    print(f"  Estimated active from full list: ~{int(len(tokens) * pct / 100)}")


if __name__ == "__main__":
    random.seed(42)
    test_platform("Greenhouse", "data/board_tokens/greenhouse.txt", check_greenhouse)
    test_platform("Lever", "data/board_tokens/lever.txt", check_lever)
    test_platform("Ashby", "data/board_tokens/ashby.txt", check_ashby)
    test_platform("Jobvite", "data/board_tokens/jobvite.txt", check_jobvite)
