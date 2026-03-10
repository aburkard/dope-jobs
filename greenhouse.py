import requests
import utils


def fetch_jobs(board_token, content=True, normalize=True):
    url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs"
    content = "true" if content else "false"
    response = requests.get(url, params={"content": content})
    jobs = response.json().get("jobs", [])
    if normalize:
        jobs = [normalize_job(job, board_token) for job in jobs]
    return jobs


def fetch_job(board_token, job_id):
    url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs/{job_id}"
    response = requests.get(url)
    return response.json()


def normalize_job(job, board_token):
    description = utils.remove_html_markup(job.get('content', ''),
                                           double_unescape=True)
    return {
        "id": f"greenhouse__{board_token}__{job.get('id')}",
        "board_token": board_token,
        "company": utils.get_company_name(board_token),
        "title": job.get('title'),
        "description": description,
        "location": job.get('location', {}).get('name'),
        "url": job.get('absolute_url'),
        "updated_at": job.get('updated_at'),
    }