"""Run a cheap model tournament for job metadata extraction.

Uses the current production parsing prompt/schema via the local parse wrapper,
which re-exports the canonical pipeline parse module.

Examples:
  uv run python model_tournament.py --preset affordable --limit 12
  uv run python model_tournament.py --models gpt-5-nano gpt-4.1-nano gemini-2.5-flash-lite
  uv run python model_tournament.py --preset affordable --openrouter-mode cheap
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from legacy_pipeline_bridge import load_pipeline_module


PARSE_MODULE = load_pipeline_module("parse.py")
FLAT_JSON_SCHEMA = PARSE_MODULE.FLAT_JSON_SCHEMA
SYSTEM_PROMPT = PARSE_MODULE.SYSTEM_PROMPT
build_user_prompt = PARSE_MODULE.build_user_prompt
parse_response = PARSE_MODULE._parse_response

for model_name in [
    "Location",
    "ApplicantLocationRequirement",
    "Salary",
    "Equity",
    "MinMax",
    "TimezoneRange",
    "JobMetadata",
]:
    model_cls = getattr(PARSE_MODULE, model_name, None)
    if model_cls is not None and hasattr(model_cls, "model_rebuild"):
        model_cls.model_rebuild(_types_namespace=vars(PARSE_MODULE))


ROOT = Path(__file__).resolve().parent
DEFAULT_EVAL_SET = ROOT / "data" / "eval_set.jsonl"
DEFAULT_OUTPUT_ROOT = ROOT / "tmp" / "model_tournament"


@dataclass(frozen=True)
class ModelSpec:
    key: str
    provider: str
    model: str
    label: str


MODEL_SPECS: dict[str, ModelSpec] = {
    "gpt-5-nano": ModelSpec("gpt-5-nano", "openai", "gpt-5-nano", "gpt-5-nano"),
    "gpt-4.1-nano": ModelSpec("gpt-4.1-nano", "openai", "gpt-4.1-nano", "gpt-4.1-nano"),
    "gpt-5.4-nano": ModelSpec("gpt-5.4-nano", "openai", "gpt-5.4-nano", "gpt-5.4-nano"),
    "gemini-2.5-flash-lite": ModelSpec(
        "gemini-2.5-flash-lite", "gemini", "gemini-2.5-flash-lite", "gemini-2.5-flash-lite"
    ),
    "gemini-3.1-flash-lite-preview": ModelSpec(
        "gemini-3.1-flash-lite-preview",
        "gemini",
        "gemini-3.1-flash-lite-preview",
        "gemini-3.1-flash-lite-preview",
    ),
    "mistral-small-2506": ModelSpec(
        "mistral-small-2506",
        "openrouter",
        "mistralai/mistral-small-3.2-24b-instruct-2506",
        "mistral-small-2506",
    ),
    "gpt-oss-120b": ModelSpec(
        "gpt-oss-120b", "openrouter", "openai/gpt-oss-120b", "gpt-oss-120b"
    ),
    "qwen3-30b-a3b-instruct-2507": ModelSpec(
        "qwen3-30b-a3b-instruct-2507",
        "openrouter",
        "qwen/qwen3-30b-a3b-instruct-2507",
        "qwen3-30b-a3b-instruct-2507",
    ),
    "step-3.5-flash": ModelSpec(
        "step-3.5-flash", "openrouter", "stepfun/step-3.5-flash", "step-3.5-flash"
    ),
    "mimo-v2-flash": ModelSpec(
        "mimo-v2-flash", "openrouter", "xiaomi/mimo-v2-flash", "mimo-v2-flash"
    ),
    "glm-4.7-flash": ModelSpec(
        "glm-4.7-flash", "openrouter", "z-ai/glm-4.7-flash-20260119", "glm-4.7-flash"
    ),
}

PRESETS: dict[str, list[str]] = {
    "affordable": [
        "gpt-5-nano",
        "gpt-4.1-nano",
        "gpt-5.4-nano",
        "gemini-2.5-flash-lite",
        "gemini-3.1-flash-lite-preview",
        "mistral-small-2506",
        "gpt-oss-120b",
        "qwen3-30b-a3b-instruct-2507",
        "step-3.5-flash",
        "mimo-v2-flash",
        "glm-4.7-flash",
    ],
    "direct": [
        "gpt-5-nano",
        "gpt-4.1-nano",
        "gpt-5.4-nano",
        "gemini-2.5-flash-lite",
        "gemini-3.1-flash-lite-preview",
    ],
}

OPENROUTER_MODES: dict[str, dict[str, Any]] = {
    "strict": {
        "allow_fallbacks": False,
        "require_parameters": True,
        "data_collection": "deny",
    },
    "cheap": {
        "allow_fallbacks": True,
        "require_parameters": True,
        "data_collection": "deny",
        "sort": "price",
    },
    "fast": {
        "allow_fallbacks": True,
        "require_parameters": True,
        "data_collection": "deny",
        "sort": "throughput",
    },
    "compatible": {
        "allow_fallbacks": True,
        "require_parameters": False,
        "data_collection": "deny",
        "sort": "throughput",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs-file", type=Path, default=DEFAULT_EVAL_SET)
    parser.add_argument("--limit", type=int, default=0, help="Limit jobs from the eval file (0 = all)")
    parser.add_argument("--models", nargs="*", default=None, help="Model keys to run")
    parser.add_argument("--preset", choices=sorted(PRESETS), default="affordable")
    parser.add_argument(
        "--openrouter-mode",
        choices=sorted(OPENROUTER_MODES),
        default="strict",
        help="How OpenRouter should route requests",
    )
    parser.add_argument("--max-output-tokens", type=int, default=2000)
    parser.add_argument("--temperature", type=float, default=0.1)
    parser.add_argument("--sleep-seconds", type=float, default=0.0, help="Pause between requests")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--run-name", default="", help="Optional suffix for the run directory name")
    return parser.parse_args()


def load_eval_items(path: Path, limit: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            items.append(json.loads(line))
    if limit > 0:
        return items[:limit]
    return items


def summarize_salary(parsed: dict[str, Any]) -> str:
    salary = parsed.get("salary") or {}
    lower = salary.get("min")
    upper = salary.get("max")
    currency = salary.get("currency") or "USD"
    period = salary.get("period") or "annually"
    if lower is None and upper is None:
        return "—"
    if lower is not None and upper is not None:
        return f"{currency} {lower:,.0f}-{upper:,.0f} / {period}"
    if lower is not None:
        return f"{currency} {lower:,.0f}+ / {period}"
    return f"{currency} ≤{upper:,.0f} / {period}"


def flatten_location_label(parsed: dict[str, Any]) -> str:
    locations = parsed.get("locations") or []
    labels = [loc.get("label") for loc in locations if isinstance(loc, dict) and loc.get("label")]
    return " | ".join(labels[:3]) if labels else "—"


def call_openai_compatible(
    *,
    base_url: str,
    api_key: str,
    model: str,
    job_text: str,
    max_output_tokens: int,
    temperature: float,
    extra_headers: dict[str, str] | None = None,
    extra_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": build_user_prompt(job_text)},
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": {"name": "job_metadata", "schema": FLAT_JSON_SCHEMA},
        },
    }
    # Some reasoning-heavy models reject or behave poorly with explicit temperature here.
    if model not in {"gpt-5-nano", "stepfun/step-3.5-flash"}:
        payload["temperature"] = temperature
    if model in {"gpt-5-nano", "gpt-5.4-nano"}:
        payload["reasoning_effort"] = "minimal"
    if "api.openai.com" in base_url:
        payload["max_completion_tokens"] = max_output_tokens
    else:
        payload["max_tokens"] = max_output_tokens
    if extra_body:
        payload.update(extra_body)
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    if extra_headers:
        headers.update(extra_headers)
    response = requests.post(
        f"{base_url.rstrip('/')}/chat/completions",
        headers=headers,
        json=payload,
        timeout=300,
    )
    if response.status_code >= 400:
        detail = response.text.strip()
        raise RuntimeError(f"{response.status_code} {response.reason}: {detail[:1200]}")
    return response.json()


def call_gemini(
    *,
    api_key: str,
    model: str,
    job_text: str,
    max_output_tokens: int,
    temperature: float,
) -> dict[str, Any]:
    schema = {
        "type": "OBJECT",
        "properties": {
            "tagline": {"type": "STRING"},
            "location_city": {"type": "STRING"},
            "location_state": {"type": "STRING"},
            "location_country": {"type": "STRING"},
            "location_lat": {"type": "NUMBER"},
            "location_lng": {"type": "NUMBER"},
            "applicant_location_requirements": FLAT_JSON_SCHEMA["properties"]["applicant_location_requirements"],
            "salary_min": {"type": "NUMBER"},
            "salary_max": {"type": "NUMBER"},
            "salary_currency": {"type": "STRING"},
            "salary_period": {"type": "STRING", "enum": ["hourly", "weekly", "monthly", "annually"]},
            "salary_transparency": {"type": "STRING", "enum": ["full_range", "minimum_only", "not_disclosed"]},
            "office_type": {"type": "STRING", "enum": ["remote", "hybrid", "onsite"]},
            "hybrid_days": {"type": "INTEGER"},
            "job_type": {"type": "STRING", "enum": ["full-time", "part-time", "contract", "internship", "temporary", "freelance"]},
            "experience_level": {"type": "STRING", "enum": ["entry", "mid", "senior", "staff", "principal", "executive"]},
            "is_manager": {"type": "BOOLEAN"},
            "industry_primary": FLAT_JSON_SCHEMA["properties"]["industry_primary"],
            "industry_tags": FLAT_JSON_SCHEMA["properties"]["industry_tags"],
            "industry_other_hint": {"type": "STRING"},
            "hard_skills": {"type": "ARRAY", "items": {"type": "STRING"}},
            "soft_skills": {"type": "ARRAY", "items": {"type": "STRING"}},
            "cool_factor": {"type": "STRING", "enum": ["boring", "standard", "interesting", "compelling", "exceptional"]},
            "vibe_tags": FLAT_JSON_SCHEMA["properties"]["vibe_tags"],
            "visa_sponsorship": {"type": "STRING", "enum": ["yes", "no", "unknown"]},
            "visa_sponsorship_types": FLAT_JSON_SCHEMA["properties"]["visa_sponsorship_types"],
            "equity_offered": {"type": "BOOLEAN"},
            "equity_min_pct": {"type": "NUMBER"},
            "equity_max_pct": {"type": "NUMBER"},
            "company_stage": {"type": "STRING", "enum": ["pre-seed", "seed", "series-a", "series-b", "series-c-plus", "public", "bootstrapped", "government", "nonprofit", "unknown"]},
            "company_size_min": {"type": "INTEGER"},
            "company_size_max": {"type": "INTEGER"},
            "team_size_min": {"type": "INTEGER"},
            "team_size_max": {"type": "INTEGER"},
            "reports_to": {"type": "STRING"},
            "benefits_categories": FLAT_JSON_SCHEMA["properties"]["benefits_categories"],
            "benefits_highlights": {"type": "ARRAY", "items": {"type": "STRING"}},
            "remote_timezone_earliest": {"type": "STRING"},
            "remote_timezone_latest": {"type": "STRING"},
            "education_level": {"type": "STRING", "enum": ["none", "high-school", "bachelors", "masters", "phd", "not_specified"]},
            "years_experience_min": {"type": "INTEGER"},
            "years_experience_max": {"type": "INTEGER"},
            "certifications": {"type": "ARRAY", "items": {"type": "STRING"}},
            "languages": {"type": "ARRAY", "items": {"type": "STRING"}},
            "travel_percent": {"type": "INTEGER"},
            "interview_stages": {"type": "INTEGER"},
            "posting_language": {"type": "STRING"},
        },
        "required": list(FLAT_JSON_SCHEMA["required"]),
    }
    prompt = f"{SYSTEM_PROMPT}\n\n{build_user_prompt(job_text)}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": temperature,
            "maxOutputTokens": max_output_tokens,
            "responseMimeType": "application/json",
            "responseSchema": schema,
        },
    }
    response = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}",
        json=payload,
        timeout=300,
    )
    if response.status_code >= 400:
        detail = response.text.strip()
        raise RuntimeError(f"{response.status_code} {response.reason}: {detail[:1200]}")
    return response.json()


def extract_openai_content(payload: dict[str, Any]) -> str | None:
    try:
        message = payload["choices"][0]["message"]
        content = message["content"]
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            texts = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text" and item.get("text"):
                    texts.append(item["text"])
            if texts:
                return "".join(texts)
        reasoning = message.get("reasoning")
        if isinstance(reasoning, str):
            stripped = reasoning.strip()
            if stripped.startswith("{") or stripped.startswith("["):
                return stripped
        return None
    except Exception:
        return None


def extract_gemini_content(payload: dict[str, Any]) -> str | None:
    candidates = payload.get("candidates") or []
    if not candidates:
        return None
    content = candidates[0].get("content") or {}
    parts = content.get("parts") or []
    texts = [part.get("text", "") for part in parts if isinstance(part, dict) and part.get("text")]
    return "".join(texts) if texts else None


def get_usage(payload: dict[str, Any], provider: str) -> dict[str, Any] | None:
    if provider in {"openai", "openrouter"}:
        return payload.get("usage")
    if provider == "gemini":
        return payload.get("usageMetadata")
    return None


def openrouter_request_overrides(spec: ModelSpec, openrouter_mode: str) -> dict[str, Any]:
    body: dict[str, Any] = {"provider": OPENROUTER_MODES[openrouter_mode]}
    # Most cheap OpenRouter models are better for this task when we suppress visible reasoning.
    if spec.key == "step-3.5-flash":
        body["reasoning"] = {"max_tokens": 256, "exclude": True}
    else:
        body["reasoning"] = {"effort": "none", "exclude": True}
    return body


def effective_max_output_tokens(spec: ModelSpec, requested: int) -> int:
    if spec.key == "step-3.5-flash":
        return max(requested, 4000)
    return requested


def run_one(
    spec: ModelSpec,
    item: dict[str, Any],
    max_output_tokens: int,
    temperature: float,
    openrouter_mode: str,
) -> dict[str, Any]:
    started = time.time()
    raw_text = item["text"]
    effective_max_tokens = effective_max_output_tokens(spec, max_output_tokens)
    error = None
    raw_content = None
    payload: dict[str, Any] | None = None
    parsed = None
    try:
        if spec.provider == "openai":
            api_key = os.environ.get("OPENAI_API_KEY")
            if api_key:
                payload = call_openai_compatible(
                    base_url="https://api.openai.com/v1",
                    api_key=api_key,
                    model=spec.model,
                    job_text=raw_text,
                    max_output_tokens=effective_max_tokens,
                    temperature=temperature,
                )
            else:
                fallback_api_key = os.environ["OPENROUTER_API_KEY"]
                payload = call_openai_compatible(
                    base_url="https://openrouter.ai/api/v1",
                    api_key=fallback_api_key,
                    model=f"openai/{spec.model}",
                    job_text=raw_text,
                    max_output_tokens=effective_max_tokens,
                    temperature=temperature,
                    extra_headers={
                        "HTTP-Referer": "https://dopejobs.xyz",
                        "X-Title": "dopejobs model tournament",
                    },
                    extra_body=openrouter_request_overrides(spec, openrouter_mode),
                )
            raw_content = extract_openai_content(payload)
        elif spec.provider == "gemini":
            api_key = os.environ["GEMINI_API_KEY"]
            payload = call_gemini(
                api_key=api_key,
                model=spec.model,
                job_text=raw_text,
                max_output_tokens=effective_max_tokens,
                temperature=temperature,
            )
            raw_content = extract_gemini_content(payload)
        elif spec.provider == "openrouter":
            api_key = os.environ["OPENROUTER_API_KEY"]
            extra_headers = {
                "HTTP-Referer": "https://dopejobs.xyz",
                "X-Title": "dopejobs model tournament",
            }
            extra_body = openrouter_request_overrides(spec, openrouter_mode)
            payload = call_openai_compatible(
                base_url="https://openrouter.ai/api/v1",
                api_key=api_key,
                model=spec.model,
                job_text=raw_text,
                max_output_tokens=effective_max_tokens,
                temperature=temperature,
                extra_headers=extra_headers,
                extra_body=extra_body,
            )
            raw_content = extract_openai_content(payload)
        else:
            raise ValueError(f"Unknown provider {spec.provider}")

        if raw_content:
            parsed_obj = parse_response(raw_content, use_flat=True)
            parsed = parsed_obj.model_dump(mode="json") if parsed_obj else None
        else:
            error = "empty_response_content"
    except Exception as exc:
        error = str(exc)

    duration_s = round(time.time() - started, 3)
    usage = get_usage(payload or {}, spec.provider)
    return {
        "job_title": item.get("title"),
        "job_company": item.get("company"),
        "job_location": item.get("location"),
        "job_description": item.get("description"),
        "model_key": spec.key,
        "model_label": spec.label,
        "provider": spec.provider,
        "model_id": spec.model,
        "duration_s": duration_s,
        "success": parsed is not None,
        "error": error,
        "usage": usage,
        "payload": payload,
        "raw_content": raw_content,
        "parsed": parsed,
    }


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def render_summary(
    results: list[dict[str, Any]],
    selected_models: list[ModelSpec],
    eval_items: list[dict[str, Any]],
) -> str:
    lines: list[str] = []
    lines.append("# Model Tournament")
    lines.append("")
    lines.append("## Aggregate")
    lines.append("")
    lines.append("| Model | Success | Avg sec | Prompt tokens | Output tokens |")
    lines.append("| --- | ---: | ---: | ---: | ---: |")

    for spec in selected_models:
        model_rows = [row for row in results if row["model_key"] == spec.key]
        success_count = sum(1 for row in model_rows if row["success"])
        avg_s = sum(row["duration_s"] for row in model_rows) / len(model_rows) if model_rows else 0.0
        prompt_tokens = 0
        output_tokens = 0
        for row in model_rows:
            usage = row.get("usage") or {}
            if spec.provider in {"openai", "openrouter"}:
                prompt_tokens += usage.get("prompt_tokens", 0) or 0
                output_tokens += usage.get("completion_tokens", 0) or 0
            elif spec.provider == "gemini":
                prompt_tokens += usage.get("promptTokenCount", 0) or 0
                output_tokens += usage.get("candidatesTokenCount", 0) or 0
        lines.append(
            f"| {spec.label} | {success_count}/{len(model_rows)} | {avg_s:.2f} | {prompt_tokens:,} | {output_tokens:,} |"
        )

    for item in eval_items:
        lines.append("")
        lines.append(f"## {item['title']} at {item['company']}")
        lines.append("")
        lines.append(f"- Description: {item.get('description', '—')}")
        lines.append(f"- Location: {item.get('location', '—')}")
        lines.append("")
        lines.append("| Model | Office | Type | Level | Industry | Visa | Salary | Locations | Tagline |")
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
        item_rows = [row for row in results if row["job_title"] == item["title"] and row["job_company"] == item["company"]]
        by_key = {row["model_key"]: row for row in item_rows}
        for spec in selected_models:
            row = by_key.get(spec.key)
            if not row or not row["success"]:
                err = row["error"] if row else "not_run"
                lines.append(f"| {spec.label} | FAIL | FAIL | FAIL | FAIL | FAIL | FAIL | FAIL | `{err}` |")
                continue
            parsed = row["parsed"] or {}
            lines.append(
                "| "
                + " | ".join(
                    [
                        spec.label,
                        str(parsed.get("office_type") or "—"),
                        str(parsed.get("job_type") or "—"),
                        str(parsed.get("experience_level") or "—"),
                        str(parsed.get("industry_primary") or "—"),
                        str(parsed.get("visa_sponsorship") or "—"),
                        summarize_salary(parsed),
                        flatten_location_label(parsed),
                        str(parsed.get("tagline") or "—").replace("|", "/"),
                    ]
                )
                + " |"
            )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    load_dotenv()
    args = parse_args()
    model_keys = args.models or PRESETS[args.preset]
    unknown = [key for key in model_keys if key not in MODEL_SPECS]
    if unknown:
        raise SystemExit(f"Unknown model keys: {', '.join(sorted(unknown))}")

    eval_items = load_eval_items(args.jobs_file, args.limit)
    if not eval_items:
        raise SystemExit("No eval items loaded")

    selected_models = [MODEL_SPECS[key] for key in model_keys]
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    suffix = f"-{args.run_name}" if args.run_name else ""
    run_dir = args.output_root / f"{timestamp}{suffix}"
    run_dir.mkdir(parents=True, exist_ok=False)

    manifest = {
        "created_at": datetime.now().isoformat(),
        "jobs_file": str(args.jobs_file),
        "job_count": len(eval_items),
        "models": [spec.__dict__ for spec in selected_models],
        "openrouter_mode": args.openrouter_mode,
        "max_output_tokens": args.max_output_tokens,
        "temperature": args.temperature,
        "sleep_seconds": args.sleep_seconds,
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    results: list[dict[str, Any]] = []
    total = len(eval_items) * len(selected_models)
    index = 0
    for item in eval_items:
        print(f"\nJOB: {item['title']} at {item['company']}", file=sys.stderr)
        for spec in selected_models:
            index += 1
            print(f"  [{index}/{total}] {spec.label}", file=sys.stderr)
            row = run_one(
                spec,
                item,
                max_output_tokens=args.max_output_tokens,
                temperature=args.temperature,
                openrouter_mode=args.openrouter_mode,
            )
            results.append(row)
            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)

    write_jsonl(run_dir / "results.jsonl", results)
    summary = render_summary(results, selected_models, eval_items)
    (run_dir / "summary.md").write_text(summary)

    success_count = sum(1 for row in results if row["success"])
    print(f"\nWrote run to {run_dir}", file=sys.stderr)
    print(f"Successful parses: {success_count}/{len(results)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
