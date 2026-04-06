import json
import os
import statistics
import sys
import time
from dataclasses import dataclass

import requests


API_URL = "https://openrouter.ai/api/v1/embeddings"
INPUT_TEXTS = [
    "senior product designer remote",
    "staff backend engineer distributed systems",
    "machine learning engineer computer vision",
    "growth marketing manager fintech",
    "head of product healthcare startup",
]


@dataclass
class TrialResult:
    ok: bool
    latency_s: float
    dims: int | None
    error: str | None


def trial(model: str, text: str, timeout_s: int = 90) -> TrialResult:
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is not set")
    started = time.perf_counter()
    try:
        resp = requests.post(
            API_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={"model": model, "input": text},
            timeout=timeout_s,
        )
        latency_s = time.perf_counter() - started
        if resp.status_code >= 400:
            return TrialResult(
                ok=False,
                latency_s=latency_s,
                dims=None,
                error=f"{resp.status_code}: {resp.text[:400]}",
            )
        payload = resp.json()
        data = payload.get("data") or []
        if not data:
            return TrialResult(
                ok=False,
                latency_s=latency_s,
                dims=None,
                error=f"no data field: {json.dumps(payload)[:400]}",
            )
        emb = data[0].get("embedding")
        if not isinstance(emb, list):
            return TrialResult(
                ok=False,
                latency_s=latency_s,
                dims=None,
                error=f"unexpected embedding payload: {json.dumps(data[0])[:400]}",
            )
        return TrialResult(ok=True, latency_s=latency_s, dims=len(emb), error=None)
    except Exception as exc:
        latency_s = time.perf_counter() - started
        return TrialResult(ok=False, latency_s=latency_s, dims=None, error=repr(exc))


def summarize(model: str, results: list[TrialResult]) -> dict:
    oks = [r for r in results if r.ok]
    errs = [r for r in results if not r.ok]
    latencies = [r.latency_s for r in oks]
    dims = sorted({r.dims for r in oks if r.dims is not None})
    return {
        "model": model,
        "attempts": len(results),
        "successes": len(oks),
        "failures": len(errs),
        "avg_latency_s": round(statistics.mean(latencies), 3) if latencies else None,
        "p50_latency_s": round(statistics.median(latencies), 3) if latencies else None,
        "min_latency_s": round(min(latencies), 3) if latencies else None,
        "max_latency_s": round(max(latencies), 3) if latencies else None,
        "dimensions": dims,
        "errors": [e.error for e in errs[:3]],
    }


def main(argv: list[str]) -> int:
    models = argv[1:]
    if not models:
        print("usage: python benchmark_openrouter_embeddings.py <model> [<model> ...]", file=sys.stderr)
        return 2
    all_results: list[dict] = []
    for model in models:
        results: list[TrialResult] = []
        for text in INPUT_TEXTS:
            results.append(trial(model, text))
        all_results.append(summarize(model, results))
    print(json.dumps(all_results, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
