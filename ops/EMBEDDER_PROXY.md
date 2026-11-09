# Embedding proxy on the Meili box (perplexity-embedder, :8087)

As deployed on 2026-11-09. The code lives in `ops/perplexity_embedder_service.py`; the tiny routes come from PR #1, branch `tiny-query-encoder`.

## Routes
| Route | Backend | Used by |
|---|---|---|
| `/openai-search/v1/embeddings` | pplx TEI on 127.0.0.1:8088, 512-d | live `jobs` search embedder |
| `/openai-index/v1/embeddings` | OpenRouter `perplexity/pplx-embed-v1-0.6b` | live `jobs` indexing embedder |
| `/openai-search-tiny/v1/embeddings` | `dopejobs-tiny-q256` ONNX in-process (ORT), 256-d, L2 | new 256-d index, search |
| `/openai-index-tiny/v1/embeddings` | OpenRouter `mongodb/voyage-4-lite`, input_type document, 1024-d, then L2, × P, L2 | new index fallback |
| `/health`, `/health-tiny` | | |

- **Request:** OpenAI style, `{"input": str or [str]}`. `model`, `dimensions` and `encoding_format` are ignored.
- **Response:** `{"object":"list","data":[{"object":"embedding","index":i,"embedding":[...]}],...}`.
- **Query preprocessing:** raw string, no prefix, 32-token truncation, `""` becomes `"."`.
- **Meili embedder for the 256-d index:** one `source: openAi` embedder, `model: text-embedding-3-small`, `dimensions: 256`, `url` = the tiny search route. A composite embedder is rejected: the search and indexing spaces are deliberately asymmetric.

## Files on the box
- Model files in `/opt/dopejobs-tiny/`:
  - `dopejobs-tiny-q256.onnx`: sha256 `255851232a57cc430b67baac0fcd3cf3fc071f916ced39b796d35f937f7d93f1`
  - `dopejobs-tiny-q256/tokenizer.json`: sha256 `b4492dc2ba7774b98ed3cd59e5b1fbd4d9b12845acc9b1dbe8776adf358a2e1a`
  - `proj_clean_voy4_1024_to_256.npy`: sha256 `3788076b4bf3d0a9b323878d0fa528d48948a48227482cb13ee66d30b8b1fe5f`
  - `reference_eval340_*` and `tiny_query_encoder.py`, used for the parity self-test.
- onnxruntime 1.31.1 is installed in `/opt/dopejobs-embedder/.venv`.
- Unit: `perplexity-embedder.service`, plus the drop-in `perplexity-embedder.service.d/memory.conf` (MemoryMax 1200M, MemorySwapMax 0).

## Network
- The proxy listens on `0.0.0.0:8087`, because Meili calls it via the box's own IP (178.156.164.231). That traffic goes over `lo`.
- `rules.v4` has `-A INPUT ! -i lo -p tcp --dport 8087 -j DROP`. IPv6 inbound is default-deny.
- Port 8087 was checked closed from outside on v4 and v6.
- The proxy holds the OpenRouter key, so keep both the drop rule and the IPv6 default-deny.

## Checks (2026-11-09)
- **Parity:** min cos 1.000000 on 340/340 against `reference_eval340_vectors.npy`, single and batched, over HTTP.
- **Restart:** healthy in 1.4–2.3 s. The lifespan warms TEI, then loads the tiny model and embeds a warm-up string, all before uvicorn binds.
  - If the tiny model files fail to load, only the tiny routes return 503 (`/health-tiny` shows the error).
- **Memory:** RSS ~480 MB after the restart, 0 swap.
- **Latency:** the tiny route takes 6–8 ms per call over HTTP after the restart; pplx `/openai-search` takes ~80 ms.
  - Measured while a 325k-doc index build was running.
  - Under that load, 2 ORT threads were faster than the default 4 (in-process p50/p95 4.4/5.6 ms vs 7.3/12.9 ms). Tune with env `TINY_THREADS`.

## Self-test on the box
```
cd /opt/dopejobs-tiny && /opt/dopejobs-embedder/.venv/bin/python tiny_query_encoder.py
```

## Rollback
- Proxy code: `cp /opt/dopejobs/ops/perplexity_embedder_service.py.bak-20261109-pre-tiny /opt/dopejobs/ops/perplexity_embedder_service.py && systemctl restart perplexity-embedder`
- Memory cap: remove the drop-in, run `systemctl daemon-reload`, then restart.
