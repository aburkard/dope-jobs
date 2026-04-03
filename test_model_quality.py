"""Deep quality comparison — full output for review."""
import os
import json
import time
import requests
from dotenv import load_dotenv
load_dotenv()

from parse import SYSTEM_PROMPT, COMPACT_SCHEMA, prepare_job_text, _parse_response, merge_api_data
from scrapers.greenhouse_scraper import GreenhouseScraper
from scrapers.ashby_scraper import AshbyScraper

OPENAI_KEY = os.environ["OPENAI_API_KEY"]
GEMINI_KEY = os.environ["GEMINI_API_KEY"]

# Get 3 diverse jobs
jobs = []
for cls, token in [(GreenhouseScraper, "discord"), (AshbyScraper, "ramp"), (GreenhouseScraper, "duolingo")]:
    scraper = cls(token)
    scraped = list(scraper.fetch_jobs())
    # Pick a non-AE role for variety
    for j in scraped:
        if "Account Executive" not in j.get("title", "") and "Manager" not in j.get("title", ""):
            jobs.append(j)
            break
    else:
        jobs.append(scraped[0])

def call_openai(text):
    r = requests.post("https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_KEY}"},
        json={
            "model": "gpt-5.4-nano-2026-03-17",
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"{COMPACT_SCHEMA}\n\nJob posting:\n{text}"},
            ],
            "max_completion_tokens": 2000, "temperature": 0.1,
        }, timeout=60)
    return r.json()["choices"][0]["message"]["content"]

def call_gemini(text):
    r = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-lite-preview:generateContent?key={GEMINI_KEY}",
        json={
            "contents": [{"parts": [{"text": f"{SYSTEM_PROMPT}\n\n{COMPACT_SCHEMA}\n\nJob posting:\n{text}"}]}],
            "generationConfig": {"temperature": 0.1, "maxOutputTokens": 2000},
        }, timeout=60)
    return r.json()["candidates"][0]["content"]["parts"][0]["text"]

import random
random.seed(42)

for i, job in enumerate(jobs):
    title = job.get("title", "?")
    company = job.get("board_token", "?")
    text = prepare_job_text(job)

    content_a = call_openai(text)
    content_b = call_gemini(text)
    result_a = _parse_response(content_a, use_flat=True)
    result_b = _parse_response(content_b, use_flat=True)

    # Apply API merge to both
    result_a_dict = None
    result_b_dict = None
    if result_a:
        result_a_dict = merge_api_data(job, result_a.model_dump(mode="json"))
    if result_b:
        result_b_dict = merge_api_data(job, result_b.model_dump(mode="json"))

    # Randomize which is Model A vs B
    if random.random() > 0.5:
        r1, r2, labels = result_a_dict, result_b_dict, ("Model A", "Model B")
        # Track: A=openai, B=gemini
    else:
        r1, r2, labels = result_b_dict, result_a_dict, ("Model A", "Model B")
        # Track: A=gemini, B=openai

    print(f"\n{'='*70}")
    print(f"JOB {i+1}: {title} at {company}")
    print(f"{'='*70}")

    if not r1 or not r2:
        print(f"  PARSE FAILED: r1={'ok' if r1 else 'FAIL'} r2={'ok' if r2 else 'FAIL'}")
        continue

    # Show fields that differ
    for field in ["tagline", "cool_factor", "office_type", "job_type", "experience_level",
                   "is_manager", "industry", "visa_sponsorship"]:
        v1 = r1.get(field)
        v2 = r2.get(field)
        marker = " <<<" if v1 != v2 else ""
        print(f"  {field:20s}  A: {str(v1)[:50]:50s}  B: {str(v2)[:50]}{marker}")

    # Skills comparison
    hs1 = r1.get("hard_skills", [])
    hs2 = r2.get("hard_skills", [])
    ss1 = r1.get("soft_skills", [])
    ss2 = r2.get("soft_skills", [])
    print(f"\n  hard_skills         A ({len(hs1)}): {hs1[:6]}")
    print(f"                      B ({len(hs2)}): {hs2[:6]}")
    print(f"  soft_skills         A ({len(ss1)}): {ss1[:5]}")
    print(f"                      B ({len(ss2)}): {ss2[:5]}")

    # Benefits
    bc1 = r1.get("benefits_categories", [])
    bc2 = r2.get("benefits_categories", [])
    bh1 = r1.get("benefits_highlights", [])
    bh2 = r2.get("benefits_highlights", [])
    print(f"\n  benefits_cats       A ({len(bc1)}): {bc1[:5]}")
    print(f"                      B ({len(bc2)}): {bc2[:5]}")
    print(f"  benefits_highlights A: {bh1[:2]}")
    print(f"                      B: {bh2[:2]}")

    # Vibe tags
    vt1 = r1.get("vibe_tags", [])
    vt2 = r2.get("vibe_tags", [])
    print(f"\n  vibe_tags           A: {vt1}")
    print(f"                      B: {vt2}")

    # Salary (should be same if API merge worked)
    sal1 = r1.get("salary")
    sal2 = r2.get("salary")
    print(f"\n  salary              A: {sal1}")
    print(f"                      B: {sal2}")

# Reveal the mapping
print(f"\n{'='*70}")
print("REVEAL: Check test_model_quality.py source to see which model is A vs B")
print("(Randomized per job with seed=42)")
