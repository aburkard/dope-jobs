"""Evaluate prompt variations on the eval set. Shows blind A/B results."""
import os
import json
import random
import requests
import sys
from dotenv import load_dotenv
load_dotenv()

from parse import FLAT_JSON_SCHEMA, _parse_response

OPENAI_KEY = os.environ["OPENAI_API_KEY"]
GEMINI_KEY = os.environ["GEMINI_API_KEY"]

# Load eval set
with open("data/eval_set.jsonl") as f:
    eval_set = [json.loads(line) for line in f]

# --- Prompt variations ---

V2_SYSTEM = """You are extracting metadata from a job posting for dopejobs, a job board that helps people find roles they'll actually be excited about.

Your extractions should help job seekers quickly decide: "Is this worth my time?"

TAGLINE: One sentence, under 120 characters. Specific to THIS role — mention the actual product, technology, or mission. Not generic. Examples of good taglines:
- "Build the geometry engine that powers 3D creation for 80M daily Roblox users"
- "Own revenue forecasting for Duolingo's 500M learners"
Bad: "Join a fast-growing company as a software engineer"

COOL FACTOR: Calibrate carefully.
- boring: generic back-office role at unremarkable company
- standard: decent job, decent company, nothing stands out (MOST jobs are this)
- interesting: notable company OR unique role OR good compensation
- compelling: notable company AND unique/impactful role AND strong compensation/culture
- exceptional: once-in-a-career opportunity (extremely rare)
An SDR role is usually "standard". A rocket engineer at a space startup is "compelling". A temp program manager is "standard" or "interesting" at most.

INDUSTRY: Choose the most SPECIFIC match for what the company's PRIMARY product/service is:
- A gaming platform → gaming (not saas_software)
- A language learning app → education (not saas_software)
- A fintech/expense management company → financial_services (not saas_software)
- A security camera company → cybersecurity (not saas_software)
- Only use saas_software for companies whose primary product is B2B software tools

VIBE TAGS: Only include tags where you can point to specific text evidence. A boilerplate "we value diversity" statement is NOT evidence for diverse_inclusive. Look for concrete programs, policies, actions.

BENEFITS HIGHLIGHTS: List EXACTLY 0-3 perks. Only genuinely unusual ones that would surprise a job seeker. Standard health/dental/401k are NOT highlights. Examples of highlights: "$10K annual learning budget", "6-month paid parental leave", "4-day work weeks", "company-funded sabbatical".

For non-English postings: extract all metadata in English."""

V5_SYSTEM = """You are extracting metadata from a job posting for dopejobs, a job board that helps people find roles they'll actually be excited about.

Your extractions should help job seekers quickly decide: "Is this worth my time?"

TAGLINE: One sentence, under 120 characters. Write like a friend telling you about a cool job — specific, vivid, human. Mention what you'd actually WORK ON and why it matters. Include the company name.
Good: "You'll 3D-print rocket engines at Relativity Space"
Good: "Own Duolingo's Gen-Z marketing blitz in Beijing"
Good: "Build the AI safety evals that decide if frontier models ship at Anthropic"
OK but boring: "Develop advanced additive manufacturing processes for Terran R"
Bad: "Join a fast-growing company as a software engineer"

COOL FACTOR: Calibrate carefully. ~10% boring, ~40% standard, ~30% interesting, ~15% compelling, ~5% exceptional.
- boring: generic back-office role at unremarkable company (data entry, temp admin)
- standard: decent job but nothing particularly stands out. This is the DEFAULT. SDR/BDR, account exec, coordinator, associate, ops/logistics, PM/TPM, sourcing, junior analyst roles are almost always "standard" even at good companies. Ask: "would my friend who works in a different industry think this is cool?" If no → standard.
- interesting: notable company OR genuinely unique role OR clearly above-average compensation.
- compelling: notable company AND unique/impactful role AND strong signals. Examples: AI safety researcher at a frontier lab, rocket engineer at a space startup, lead designer at a top consumer product.
- exceptional: once-in-a-career. EXTREMELY rare, maybe 1 in 200 jobs.
A temp/contract role is almost never above "interesting".

INDUSTRY: Classify by what the company SELLS to end users, not the function of this specific role:
- AI labs, ML platforms, AI safety orgs → ai_ml
- Design/collaboration tools (Figma, Canva) → saas_software
- Gaming platforms → gaming
- Language learning apps → education
- Fintech/expense management → financial_services
- Security/cybersecurity companies → cybersecurity
- Lodging/travel platforms (Airbnb, Booking.com) → hospitality_tourism
- Music/video streaming → entertainment_media
- Do NOT classify by the job function. An accountant at a gaming company is "gaming". A sourcing manager at a travel company is "hospitality_tourism".
- Do NOT use biotechnology for AI companies.

EXPERIENCE LEVEL:
- entry: intern, new grad, associate, coordinator, 0-2 years
- mid: 2-5 years, no "senior" in title
- senior: "Senior" in title, or 5+ years required
- staff: "Staff" in title
- principal: "Principal" or "Distinguished" in title
- executive: VP, Director, C-suite, Head of

IS MANAGER: true ONLY if the role manages people (Director of X, Engineering Manager, Team Lead, Head of). Individual contributors are false, even if senior/staff/principal.

VIBE TAGS: Only include tags where you can point to specific text evidence. "We value diversity" is NOT evidence for diverse_inclusive — look for concrete programs, ERGs, specific policies. Each tag needs a real signal in the text.

BENEFITS HIGHLIGHTS: EXACTLY 0-3 perks that would make someone say "wow, really?"
NOT highlights (never list): health/dental/vision, 401k/pension, PTO/vacation (even if "generous"/"unlimited"), standard parental leave, remote/hybrid, equity/stock options.
ARE highlights: "$10K learning budget", "6-month parental leave", "4-day work weeks", "sabbatical", "fertility benefits $10K+", pro-bono programs, on-site childcare, pet insurance. If nothing unusual → empty array [].

VISA SPONSORSHIP: "yes" if mentions sponsorship/visa support. "no" if "must be authorized to work" or "no sponsorship". "unknown" if not mentioned.

LANGUAGE: ALL output MUST be in English, regardless of posting language. Translate everything."""

PROMPTS = {
    "v5_openai": {
        "model_type": "openai",
        "model": "gpt-5.4-nano-2026-03-17",
        "system": V5_SYSTEM,
        "structured": True,
    },
    "v5_gemini": {
        "model_type": "gemini",
        "model": "gemini-3.1-flash-lite-preview",
        "system": V5_SYSTEM,
        "structured": True,
    },
}

# Gemini schema (same as before)
GEMINI_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "tagline": {"type": "STRING", "description": "One sentence that makes a job seeker stop scrolling"},
        "location_city": {"type": "STRING"}, "location_state": {"type": "STRING"},
        "location_country": {"type": "STRING"}, "location_lat": {"type": "NUMBER"}, "location_lng": {"type": "NUMBER"},
        "salary_min": {"type": "NUMBER"}, "salary_max": {"type": "NUMBER"},
        "salary_currency": {"type": "STRING"},
        "salary_period": {"type": "STRING", "enum": ["hourly", "weekly", "monthly", "annually"]},
        "salary_transparency": {"type": "STRING", "enum": ["full_range", "minimum_only", "not_disclosed"]},
        "office_type": {"type": "STRING", "enum": ["remote", "hybrid", "onsite"]},
        "hybrid_days": {"type": "INTEGER"},
        "job_type": {"type": "STRING", "enum": ["full-time", "part-time", "contract", "internship", "temporary", "freelance"]},
        "experience_level": {"type": "STRING", "enum": ["entry", "mid", "senior", "staff", "principal", "executive"]},
        "is_manager": {"type": "BOOLEAN"},
        "industry": {"type": "STRING", "enum": [
            "agriculture", "aerospace_defense", "ai_ml", "automotive", "biotechnology", "construction",
            "consulting", "consumer_goods", "cryptocurrency_web3", "cybersecurity", "education",
            "energy_utilities", "entertainment_media", "fashion_apparel", "financial_services",
            "food_beverage", "gaming", "government", "healthcare", "hospitality_tourism",
            "insurance", "legal", "logistics_supply_chain", "manufacturing", "marketing_advertising",
            "nonprofit", "pharmaceuticals", "real_estate", "retail_ecommerce", "robotics",
            "saas_software", "semiconductors", "telecommunications", "transportation", "other"]},
        "hard_skills": {"type": "ARRAY", "items": {"type": "STRING"}},
        "soft_skills": {"type": "ARRAY", "items": {"type": "STRING"}},
        "cool_factor": {"type": "STRING", "enum": ["boring", "standard", "interesting", "compelling", "exceptional"]},
        "vibe_tags": {"type": "ARRAY", "items": {"type": "STRING", "enum": [
            "mission_driven", "high_growth", "small_team", "cutting_edge_tech",
            "strong_culture", "high_autonomy", "work_life_balance", "well_funded",
            "public_benefit", "creative_role", "data_intensive", "global_team",
            "diverse_inclusive", "fast_paced", "customer_facing", "research_focused"]}},
        "visa_sponsorship": {"type": "STRING", "enum": ["yes", "no", "unknown"]},
        "equity_offered": {"type": "BOOLEAN"},
        "company_stage": {"type": "STRING", "enum": [
            "pre-seed", "seed", "series-a", "series-b", "series-c-plus",
            "public", "bootstrapped", "government", "nonprofit", "unknown"]},
        "company_size_min": {"type": "INTEGER"}, "company_size_max": {"type": "INTEGER"},
        "team_size_min": {"type": "INTEGER"}, "team_size_max": {"type": "INTEGER"},
        "reports_to": {"type": "STRING"},
        "benefits_categories": {"type": "ARRAY", "items": {"type": "STRING", "enum": [
            "health", "dental", "vision", "life_insurance", "disability", "401k",
            "pension", "equity_comp", "bonus", "unlimited_pto", "generous_pto",
            "parental_leave", "remote_stipend", "home_office", "relocation",
            "learning_budget", "tuition_reimbursement", "gym_fitness", "wellness",
            "meals", "commuter", "mental_health", "childcare", "pet_friendly",
            "sabbatical", "stock_purchase"]}},
        "benefits_highlights": {"type": "ARRAY", "items": {"type": "STRING"}},
        "education_level": {"type": "STRING", "enum": ["none", "high-school", "bachelors", "masters", "phd", "not_specified"]},
        "years_experience_min": {"type": "INTEGER"}, "years_experience_max": {"type": "INTEGER"},
    },
    "required": ["tagline", "office_type", "job_type", "experience_level", "is_manager",
                  "industry", "hard_skills", "soft_skills", "cool_factor", "vibe_tags",
                  "visa_sponsorship", "benefits_categories", "salary_transparency"],
}


def call_model(config, text):
    if config["model_type"] == "openai":
        payload = {
            "model": config["model"],
            "messages": [
                {"role": "system", "content": config["system"]},
                {"role": "user", "content": f"Extract metadata:\n\n{text}"},
            ],
            "max_completion_tokens": 2000, "temperature": 0.1,
        }
        if config.get("structured"):
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {"name": "j", "schema": FLAT_JSON_SCHEMA},
            }
        r = requests.post("https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_KEY}"},
            json=payload, timeout=60)
        return r.json()["choices"][0]["message"]["content"]
    else:
        payload = {
            "contents": [{"parts": [{"text": f"{config['system']}\n\nExtract metadata:\n\n{text}"}]}],
            "generationConfig": {"temperature": 0.1, "maxOutputTokens": 2000},
        }
        if config.get("structured"):
            payload["generationConfig"]["responseMimeType"] = "application/json"
            payload["generationConfig"]["responseSchema"] = GEMINI_SCHEMA
        r = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{config['model']}:generateContent?key={GEMINI_KEY}",
            json=payload, timeout=60)
        return r.json()["candidates"][0]["content"]["parts"][0]["text"]


# Run evaluation
random.seed(42)
prompt_keys = list(PROMPTS.keys())

for i, item in enumerate(eval_set):
    text = item["text"]
    title = item["title"][:50]
    company = item["company"]

    results = {}
    for key in prompt_keys:
        try:
            content = call_model(PROMPTS[key], text)
            parsed = _parse_response(content, use_flat=True)
            results[key] = parsed
        except Exception as e:
            results[key] = None
            print(f"  {key} ERROR: {e}", file=sys.stderr)

    # Randomize A/B
    if random.random() > 0.5:
        a_key, b_key = prompt_keys[0], prompt_keys[1]
    else:
        a_key, b_key = prompt_keys[1], prompt_keys[0]

    a = results[a_key]
    b = results[b_key]

    print(f"\n{'='*70}")
    print(f"JOB {i+1}: {title} at {company} ({item['description']})")
    print(f"Location: {item['location']}")
    print(f"{'='*70}")

    if not a or not b:
        print(f"  PARSE FAILED: A={'ok' if a else 'FAIL'} B={'ok' if b else 'FAIL'}")
        continue

    a_d = a.model_dump(mode="json")
    b_d = b.model_dump(mode="json")

    print(f"\n  TAGLINE:")
    print(f"    A: {a_d['tagline']}")
    print(f"    B: {b_d['tagline']}")

    print(f"\n  COOL:     A={a_d['cool_factor']:12s}  B={b_d['cool_factor']}")
    print(f"  INDUSTRY: A={a_d['industry']:20s}  B={b_d['industry']}")
    print(f"  LEVEL:    A={a_d['experience_level']:12s}  B={b_d['experience_level']}")
    print(f"  OFFICE:   A={a_d['office_type']:12s}  B={b_d['office_type']}")
    print(f"  TYPE:     A={a_d['job_type']:12s}  B={b_d['job_type']}")
    print(f"  VISA:     A={a_d['visa_sponsorship']:12s}  B={b_d['visa_sponsorship']}")

    sal_a = f"${a_d['salary']['min']:,.0f}-${a_d['salary']['max']:,.0f}" if a_d.get('salary') else "—"
    sal_b = f"${b_d['salary']['min']:,.0f}-${b_d['salary']['max']:,.0f}" if b_d.get('salary') else "—"
    print(f"  SALARY:   A={sal_a:20s}  B={sal_b}")

    print(f"\n  VIBES:    A={a_d['vibe_tags']}")
    print(f"            B={b_d['vibe_tags']}")

    print(f"\n  BENEFITS HIGHLIGHTS:")
    print(f"    A: {a_d['benefits_highlights']}")
    print(f"    B: {b_d['benefits_highlights']}")

    print(f"\n  HARD SKILLS ({len(a_d['hard_skills'])} vs {len(b_d['hard_skills'])}):")
    print(f"    A: {a_d['hard_skills'][:6]}")
    print(f"    B: {b_d['hard_skills'][:6]}")

# Reveal
print(f"\n\n{'='*70}")
print("REVEAL:")
random.seed(42)
for i in range(len(eval_set)):
    r = random.random()
    a_model = prompt_keys[0] if r > 0.5 else prompt_keys[1]
    b_model = prompt_keys[1] if r > 0.5 else prompt_keys[0]
    print(f"  Job {i+1}: A={a_model}, B={b_model}")
