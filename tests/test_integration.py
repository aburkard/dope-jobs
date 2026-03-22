"""Integration tests using real ATS data.

These hit live APIs — run with: uv run pytest tests/test_integration.py -v
Skip with: uv run pytest tests/ -v --ignore=tests/test_integration.py
"""
import pytest
from parse import merge_api_data, prepare_job_text
from db import content_hash


# --- Scrape real jobs from each ATS ---

@pytest.fixture(scope="module")
def greenhouse_jobs():
    from scrapers.greenhouse_scraper import GreenhouseScraper
    scraper = GreenhouseScraper("anthropic")
    jobs = list(scraper.fetch_jobs())
    assert len(jobs) > 0, "Anthropic Greenhouse board returned no jobs"
    return jobs


@pytest.fixture(scope="module")
def greenhouse_pay(greenhouse_jobs):
    """Fetch pay data for first 3 jobs."""
    from scrapers.greenhouse_scraper import GreenhouseScraper
    scraper = GreenhouseScraper("anthropic")
    results = []
    for job in greenhouse_jobs[:3]:
        raw_id = str(job["id"]).split("__")[-1]
        pay = scraper.fetch_job_pay(raw_id)
        results.append({"job": job, "pay": pay})
    return results


@pytest.fixture(scope="module")
def lever_jobs():
    from scrapers.lever_scraper import LeverScraper
    scraper = LeverScraper("spotify")
    jobs = list(scraper.fetch_jobs())
    assert len(jobs) > 0, "Spotify Lever board returned no jobs"
    return jobs


@pytest.fixture(scope="module")
def ashby_jobs():
    from scrapers.ashby_scraper import AshbyScraper
    scraper = AshbyScraper("ramp")
    jobs = list(scraper.fetch_jobs())
    assert len(jobs) > 0, "Ramp Ashby board returned no jobs"
    return jobs


# --- Test scraper output structure ---

class TestGreenhouseOutput:
    def test_has_required_fields(self, greenhouse_jobs):
        for job in greenhouse_jobs[:5]:
            assert job.get("id"), f"Missing id: {job.get('title')}"
            assert "__" in job["id"], f"ID not compound: {job['id']}"
            assert job.get("title"), f"Missing title"
            assert job.get("url"), f"Missing url"
            assert job.get("board_token") == "anthropic"
            assert job.get("ats_name") == "greenhouse"

    def test_has_content(self, greenhouse_jobs):
        for job in greenhouse_jobs[:5]:
            assert job.get("content") or job.get("description"), f"No content: {job['title']}"

    def test_has_structured_fields(self, greenhouse_jobs):
        """departments and offices should be present from content=true."""
        has_departments = any(job.get("departments") for job in greenhouse_jobs[:10])
        has_offices = any(job.get("offices") for job in greenhouse_jobs[:10])
        assert has_departments, "No jobs had departments"
        assert has_offices, "No jobs had offices"

    def test_departments_are_strings(self, greenhouse_jobs):
        for job in greenhouse_jobs[:10]:
            for dept in job.get("departments", []):
                assert isinstance(dept, str), f"Department not string: {dept}"

    def test_offices_have_location(self, greenhouse_jobs):
        for job in greenhouse_jobs[:10]:
            for office in job.get("offices", []):
                assert "name" in office or "location" in office

    def test_pay_transparency(self, greenhouse_pay):
        """At least some Anthropic jobs should have pay data."""
        has_pay = any(r["pay"] for r in greenhouse_pay)
        assert has_pay, "No jobs had pay_input_ranges"

    def test_pay_structure(self, greenhouse_pay):
        for r in greenhouse_pay:
            for pay in r["pay"]:
                assert "min_cents" in pay
                assert "max_cents" in pay
                assert "currency_type" in pay
                assert pay["currency_type"] == "USD"
                if pay["min_cents"]:
                    assert pay["min_cents"] > 0
                    assert pay["min_cents"] < 100000000  # < $1M in cents


class TestLeverOutput:
    def test_has_required_fields(self, lever_jobs):
        for job in lever_jobs[:5]:
            assert job.get("id"), f"Missing id"
            assert "__" in job["id"]
            assert job.get("title"), f"Missing title"
            assert job.get("url"), f"Missing url"
            assert job.get("board_token") == "spotify"

    def test_has_description(self, lever_jobs):
        for job in lever_jobs[:5]:
            assert job.get("description"), f"No description: {job['title']}"
            assert len(job["description"]) > 50, f"Description too short: {job['title']}"

    def test_has_workplace_type(self, lever_jobs):
        """Lever should provide workplaceType."""
        has_workplace = any(job.get("workplaceType") for job in lever_jobs[:10])
        assert has_workplace, "No jobs had workplaceType"

    def test_workplace_values(self, lever_jobs):
        valid = {"onsite", "remote", "hybrid", "unspecified", ""}
        for job in lever_jobs[:10]:
            wt = job.get("workplaceType", "")
            assert wt.lower() in valid, f"Unexpected workplaceType: {wt}"

    def test_has_commitment(self, lever_jobs):
        has_commitment = any(job.get("commitment") for job in lever_jobs[:10])
        assert has_commitment, "No jobs had commitment"

    def test_has_department(self, lever_jobs):
        has_dept = any(job.get("department") for job in lever_jobs[:10])
        assert has_dept, "No jobs had department"


class TestAshbyOutput:
    def test_has_required_fields(self, ashby_jobs):
        for job in ashby_jobs[:5]:
            assert job.get("id"), f"Missing id"
            assert "__" in job["id"]
            assert job.get("title"), f"Missing title"
            assert job.get("url"), f"Missing url"
            assert job.get("board_token") == "ramp"

    def test_has_description(self, ashby_jobs):
        for job in ashby_jobs[:5]:
            assert job.get("description") or job.get("descriptionPlain"), f"No description: {job['title']}"

    def test_has_workplace_type(self, ashby_jobs):
        has_wt = any(job.get("workplaceType") for job in ashby_jobs[:10])
        assert has_wt, "No jobs had workplaceType"

    def test_has_employment_type(self, ashby_jobs):
        has_et = any(job.get("employmentType") for job in ashby_jobs[:10])
        assert has_et, "No jobs had employmentType"

    def test_employment_type_values(self, ashby_jobs):
        valid = {"FullTime", "PartTime", "Intern", "Contract", "Temporary", ""}
        for job in ashby_jobs[:10]:
            et = job.get("employmentType", "")
            assert et in valid, f"Unexpected employmentType: {et}"

    def test_has_compensation(self, ashby_jobs):
        """Ramp should have compensation for most roles."""
        has_comp = any(job.get("compensationTierSummary") for job in ashby_jobs[:10])
        assert has_comp, "No jobs had compensationTierSummary"

    def test_has_structured_location(self, ashby_jobs):
        has_city = any(job.get("locationCity") for job in ashby_jobs[:10])
        assert has_city, "No jobs had locationCity"

    def test_has_department(self, ashby_jobs):
        has_dept = any(job.get("department") for job in ashby_jobs[:10])
        assert has_dept, "No jobs had department"


# --- Test merge with real data ---

class TestMergeWithRealData:
    def test_greenhouse_pay_merge(self, greenhouse_pay):
        """Merge real Greenhouse pay data with mock LLM output."""
        for r in greenhouse_pay:
            if not r["pay"]:
                continue
            raw = {**r["job"], "pay_input_ranges": r["pay"]}
            llm = {"salary": None, "salary_transparency": "not_disclosed", "office_type": "onsite", "job_type": "full-time"}
            merged = merge_api_data(raw, llm)
            assert merged["salary"] is not None, f"Salary not merged for {r['job']['title']}"
            assert merged["salary"]["min"] > 0
            assert merged["salary"]["currency"] == "USD"
            assert merged["salary_transparency"] in ("full_range", "minimum_only")

    def test_ashby_merge(self, ashby_jobs):
        """Merge real Ashby structured data with mock LLM output."""
        for job in ashby_jobs[:5]:
            llm = {
                "salary": None, "salary_transparency": "not_disclosed",
                "office_type": "onsite", "job_type": "full-time",
                "locations": [], "equity": {"offered": False, "min_pct": None, "max_pct": None},
            }
            merged = merge_api_data(job, llm)

            # Workplace type should be set from API
            if job.get("workplaceType"):
                assert merged["office_type"] in ("remote", "hybrid", "onsite"), \
                    f"Bad office_type: {merged['office_type']} from {job['workplaceType']}"

            # Employment type should be set from API
            if job.get("employmentType"):
                assert merged["job_type"] in ("full-time", "part-time", "contract", "internship", "temporary"), \
                    f"Bad job_type: {merged['job_type']} from {job['employmentType']}"

            # Compensation should be parsed if present
            if job.get("compensationSalarySummary"):
                assert merged["salary"] is not None, \
                    f"Salary not parsed from: {job['compensationSalarySummary']}"

            # Equity detection
            if job.get("compensationTierSummary") and "equity" in job["compensationTierSummary"].lower():
                assert merged["equity"]["offered"] is True

    def test_lever_merge(self, lever_jobs):
        """Merge real Lever structured data with mock LLM output."""
        for job in lever_jobs[:5]:
            llm = {"office_type": "onsite", "job_type": "full-time"}
            merged = merge_api_data(job, llm)

            if job.get("workplaceType") and job["workplaceType"] != "unspecified":
                assert merged["office_type"] in ("remote", "hybrid", "onsite")

            if job.get("commitment"):
                assert merged["job_type"] in ("full-time", "part-time", "contract", "internship", "temporary", "freelance")


# --- Test prepare_job_text with real data ---

class TestPrepareJobText:
    def test_greenhouse_text(self, greenhouse_jobs):
        for job in greenhouse_jobs[:3]:
            text = prepare_job_text(job)
            assert len(text) > 100, f"Text too short for {job['title']}"
            assert job["title"] in text, "Title not in prepared text"

    def test_lever_text(self, lever_jobs):
        for job in lever_jobs[:3]:
            text = prepare_job_text(job)
            assert len(text) > 100
            assert job["title"] in text

    def test_ashby_text(self, ashby_jobs):
        for job in ashby_jobs[:3]:
            text = prepare_job_text(job)
            assert len(text) > 100
            assert job["title"] in text
            # Location context should be included
            if job.get("locationName"):
                assert job["locationName"] in text or "Location" in text

    def test_text_not_too_long(self, greenhouse_jobs, lever_jobs, ashby_jobs):
        """Text should be capped at max_chars."""
        all_jobs = greenhouse_jobs[:2] + lever_jobs[:2] + ashby_jobs[:2]
        for job in all_jobs:
            text = prepare_job_text(job, max_chars=5000)
            assert len(text) <= 5000


# --- Test content hash consistency ---

class TestContentHashReal:
    def test_same_job_same_hash(self, greenhouse_jobs):
        """Scraping same job twice should produce same hash."""
        from db import content_hash
        job = greenhouse_jobs[0]
        h1 = content_hash(job)
        h2 = content_hash(job)
        assert h1 == h2

    def test_different_jobs_different_hash(self, greenhouse_jobs):
        from db import content_hash
        if len(greenhouse_jobs) < 2:
            pytest.skip("Need at least 2 jobs")
        h1 = content_hash(greenhouse_jobs[0])
        h2 = content_hash(greenhouse_jobs[1])
        assert h1 != h2
