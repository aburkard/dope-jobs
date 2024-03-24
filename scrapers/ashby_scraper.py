import datetime
import html2text
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper
import utils


class AshbyScraper(BaseScraper):

    ats_name = 'ashby'

    def __init__(self, board_token):
        super().__init__(board_token)
        self.ats_name = 'ashby'
        self.base_url = 'https://jobs.ashbyhq.com/api/non-user-graphql'

        h = html2text.HTML2Text()
        h.body_width = 0
        h.ignore_links = True
        h.ignore_images = True
        h.ignore_emphasis = True
        self.html2text = h

    # TODO: Find a more robust way to check if an Ashby job board exists
    def check_exists(self):
        return len(
            self.session.get(
                f"https://jobs.ashbyhq.com/{self.board_token}").text) > 7000

    def fetch_job_board(self, force=False):
        data = {
            "operationName":
            "ApiOrganizationFromHostedJobsPageName",
            "variables": {
                "organizationHostedJobsPageName": self.board_token
            },
            "query":
            """query ApiOrganizationFromHostedJobsPageName($organizationHostedJobsPageName: String!) {
                            organization: organizationFromHostedJobsPageName(
                                organizationHostedJobsPageName: $organizationHostedJobsPageName
                            ) {
                                ...OrganizationParts
                            }
                            }

                            fragment OrganizationParts on Organization {
                            name
                            publicWebsite
                            customJobsPageUrl
                            allowJobPostIndexing
                            theme {
                                colors
                                logoWordmarkImageUrl
                                logoSquareImageUrl
                            }
                            activeFeatureFlags
                            }"""
        }

        if not hasattr(self, '_cached_job_board') or force:
            response = self.session.post(self.base_url, json=data, timeout=5)
            self._cached_job_board = response.json()
        return self._cached_job_board

    def fetch_jobs(self, fetch_job_descriptions=True, normalize=True):
        if normalize and not fetch_job_descriptions:
            raise ValueError(
                "normalize=True requires fetch_job_descriptions=True")
        data = {
            "operationName":
            "ApiJobBoardWithTeams",
            "variables": {
                "organizationHostedJobsPageName": self.board_token
            },
            "query":
            """query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {
                    jobBoard: jobBoardWithTeams(
                        organizationHostedJobsPageName: $organizationHostedJobsPageName
                    ) {
                        jobPostings {
                            id
                            title
                            teamId
                            locationId
                            locationName
                            employmentType
                            secondaryLocations {
                                locationId
                                locationName
                            }
                            compensationTierSummary
                        }
                    }
                }"""
        }
        response = self.session.post(self.base_url, json=data, timeout=5)
        jobs = (response.json()['data']['jobBoard']
                or {}).get('jobPostings', [])
        for job in jobs:
            if fetch_job_descriptions:
                job_data = self.fetch_job(job['id'])
                job = {**job, **job_data}
            if normalize:
                job = self.normalize_job(job)
            jobs = self.add_default_fields(job)
            yield job

    def fetch_job(self, job_id):
        data = {
            "operationName":
            "ApiJobPosting",
            "variables": {
                "organizationHostedJobsPageName": self.board_token,
                "jobPostingId": job_id
            },
            "query":
            """query ApiJobPosting($organizationHostedJobsPageName: String!, $jobPostingId: String!) {
                            jobPosting(
                                organizationHostedJobsPageName: $organizationHostedJobsPageName
                                jobPostingId: $jobPostingId
                            ) {
                                id
                                title
                                departmentName
                                locationName
                                employmentType
                                descriptionHtml
                                isListed
                                isConfidential
                                teamNames
                                secondaryLocationNames
                                compensationTierSummary
                                compensationTiers {
                                id
                                title
                                tierSummary
                                }
                                compensationTierGuideUrl
                                scrapeableCompensationSalarySummary
                                compensationPhilosophyHtml
                            }
                        }"""
        }
        response = self.session.post(self.base_url, json=data, timeout=5)
        job = response.json()['data']['jobPosting']
        return job

    def normalize_job(self, job):
        return {
            "id": f"{self.ats_name}__{self.board_token}__{job.get('id')}",
            "board_token": self.board_token,
            "company": utils.get_company_name(self.board_token),
            "title": job.get('title'),
            "description": self.clean_description(job['descriptionHtml']),
            "location": job.get('locationName'),
            "url": f"https://jobs.ashbyhq.com/{self.board_token}/{job['id']}",
            # "updated_at": None,
        }

    def clean_description(self, text):
        return self.html2text.handle(text).strip()

    # def _fetch_html(self, force=False):
    #     if not hasattr(self, '_cached_html') or force:
    #         headers = self.session.headers.copy()
    #         headers['Accept'] = None
    #         response = self.session.get(
    #             f"https://jobs.ashbyhq.com/{self.board_token}",
    #             timeout=5,
    #             headers=headers)
    #         self._cached_html = response.text
    #     return self._cached_html

    def get_company_name(self):
        data = self.fetch_job_board()
        return data['data']['organization']['name']

    def get_company_domain(self):
        data = self.fetch_job_board()
        return data['data']['organization']['publicWebsite']

    def get_company_logo_url(self):
        data = self.fetch_job_board()
        return data['data']['organization']['theme']['logoSquareImageUrl']
