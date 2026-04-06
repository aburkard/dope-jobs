import json
import sys
import time
from statistics import mean, median


INPUT_TEXTS = [
    "senior product designer remote",
    "staff backend engineer distributed systems",
    "machine learning engineer computer vision",
    "growth marketing manager fintech",
    "head of product healthcare startup",
]


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(
            "usage: python benchmark_local_embedding_model.py <model_id> [<instruction>]",
            file=sys.stderr,
        )
        return 2

    model_id = argv[1]
    instruction = argv[2] if len(argv) > 2 else None

    load_started = time.perf_counter()
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(model_id, trust_remote_code=True)
    load_s = time.perf_counter() - load_started

    # Warmup
    warmup_started = time.perf_counter()
    if instruction:
        warm = model.encode([INPUT_TEXTS[0]], prompt=instruction, convert_to_numpy=True)
    else:
        warm = model.encode([INPUT_TEXTS[0]], convert_to_numpy=True)
    warmup_s = time.perf_counter() - warmup_started

    dims = int(warm.shape[-1])

    latencies = []
    for text in INPUT_TEXTS:
        started = time.perf_counter()
        if instruction:
            vec = model.encode([text], prompt=instruction, convert_to_numpy=True)
        else:
            vec = model.encode([text], convert_to_numpy=True)
        latencies.append(time.perf_counter() - started)
        dims = int(vec.shape[-1])

    print(
        json.dumps(
            {
                "model": model_id,
                "load_s": round(load_s, 3),
                "warmup_s": round(warmup_s, 3),
                "avg_latency_s": round(mean(latencies), 3),
                "p50_latency_s": round(median(latencies), 3),
                "min_latency_s": round(min(latencies), 3),
                "max_latency_s": round(max(latencies), 3),
                "dimensions": dims,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
