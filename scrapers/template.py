from .base_scraper import BaseScraper
import utils


class Template(BaseScraper):

    def __init__(self, board_token):
        super().__init__(board_token)
        self.ats_name = 'template'
        self.base_url = ''

    def check_exists(self):
        raise NotImplementedError

    def fetch_job_board(self):
        url = f'{self.base_url}/{self.board_token}/'
        response = self.session.get(url, timeout=5)
        job_board = response.json()
        return job_board

    def fetch_jobs(self, normalize=True):
        url = f"{self.base_url}/{self.board_token}/jobs"
        response = self.session.get(url, )
        jobs = response.json()
        if normalize:
            jobs = [self.normalize_job(job) for job in jobs]
        return jobs

    def fetch_job(self, job_id):
        url = f"{self.base_url}/{self.board_token}/jobs/{job_id}"
        response = self.session.get(url)
        return response.json()

    def normalize_job(self, job):
        return {
            "id": f"{self.ats_name}__{self.board_token}__{job.get('id')}",
            "board_token": self.board_token,
            "company": utils.get_company_name(self.board_token),
            "title": "",
            "description": self.clean_description(job),
            "location": "",
            "url": "",
            "updated_at": "",
        }

    def clean_description(self, job):
        raise NotImplementedError