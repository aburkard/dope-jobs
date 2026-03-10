"""Quick smoke test: one request per scraper to verify APIs still work."""

from factory import ScraperFactory


def test_greenhouse():
    print("=== Greenhouse ===")
    scraper = ScraperFactory.get_scraper("greenhouse", "openai")
    exists = scraper.check_exists()
    print(f"  Board exists: {exists}")
    if exists:
        jobs = scraper.fetch_jobs(content=False, normalize=False)
        print(f"  Jobs found: {len(jobs)}")
        if jobs:
            print(f"  Sample: {jobs[0].get('title')} — {jobs[0].get('location', {}).get('name')}")


def test_lever():
    print("\n=== Lever ===")
    scraper = ScraperFactory.get_scraper("lever", "netflix")
    exists = scraper.check_exists()
    print(f"  Board exists: {exists}")
    if exists:
        jobs = scraper.fetch_jobs(normalize=False)
        print(f"  Jobs found: {len(jobs)}")
        if jobs:
            print(f"  Sample: {jobs[0].get('text')} — {(jobs[0].get('categories') or {}).get('location')}")


def test_ashby():
    print("\n=== Ashby ===")
    scraper = ScraperFactory.get_scraper("ashby", "anthropic")
    exists = scraper.check_exists()
    print(f"  Board exists: {exists}")
    if exists:
        # fetch_jobs is a generator that also fetches descriptions, just get the board listing
        board = scraper.fetch_job_board()
        org = board.get("data", {}).get("organization", {})
        print(f"  Company: {org.get('name')}")
        # Fetch just the job listing without individual descriptions
        import requests
        data = {
            "operationName": "ApiJobBoardWithTeams",
            "variables": {"organizationHostedJobsPageName": "anthropic"},
            "query": """query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {
                jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) {
                    jobPostings { id title locationName }
                }
            }"""
        }
        resp = scraper.session.post(scraper.base_url, json=data, timeout=5)
        postings = (resp.json()["data"]["jobBoard"] or {}).get("jobPostings", [])
        print(f"  Jobs found: {len(postings)}")
        if postings:
            print(f"  Sample: {postings[0]['title']} — {postings[0].get('locationName')}")


def test_jobvite():
    print("\n=== Jobvite ===")
    scraper = ScraperFactory.get_scraper("jobvite", "logitech")
    exists = scraper.check_exists()
    print(f"  Board exists: {exists}")
    if exists:
        # Just fetch page 0 without job content to avoid many requests
        jobs = list(scraper._fetch_jobs(page=0, content=False))
        print(f"  Jobs on page 0: {len(jobs)}")
        if jobs:
            print(f"  Sample: {jobs[0].get('title')} — {jobs[0].get('location')}")


if __name__ == "__main__":
    test_greenhouse()
    test_lever()
    test_ashby()
    test_jobvite()
    print("\n✓ Done")
