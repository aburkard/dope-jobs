import json
import os
from contextlib import asynccontextmanager
from typing import Any

import httpx
import numpy as np
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool


MODEL_ID = os.environ.get("PPLX_MODEL_ID", "perplexity/pplx-embed-v1-0.6b")
SEARCH_UPSTREAM_URL = os.environ.get("PPLX_SEARCH_UPSTREAM_URL", "http://127.0.0.1:8088/embed")
OPENROUTER_URL = os.environ.get("PPLX_OPENROUTER_URL", "https://openrouter.ai/api/v1/embeddings")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
EMBED_DIM = int(os.environ.get("PPLX_EMBED_DIM", "512"))
NORMALIZE = os.environ.get("PPLX_NORMALIZE", "true").lower() not in {"0", "false", "no"}
HTTP_TIMEOUT = float(os.environ.get("PPLX_TIMEOUT_SECONDS", "30"))
STARTUP_RETRIES = int(os.environ.get("PPLX_STARTUP_RETRIES", "20"))
STARTUP_RETRY_SECONDS = float(os.environ.get("PPLX_STARTUP_RETRY_SECONDS", "1"))
SEARCH_UPSTREAM_BATCH_SIZE = max(1, int(os.environ.get("PPLX_SEARCH_UPSTREAM_BATCH_SIZE", "1")))
INDEX_UPSTREAM_BATCH_SIZE = max(1, min(10, int(os.environ.get("PPLX_INDEX_UPSTREAM_BATCH_SIZE", "10"))))
OPENROUTER_ENCODING_FORMAT = os.environ.get("PPLX_OPENROUTER_ENCODING_FORMAT", "float")
OPENROUTER_HTTP_REFERER = os.environ.get("PPLX_OPENROUTER_HTTP_REFERER", "")
OPENROUTER_X_TITLE = os.environ.get("PPLX_OPENROUTER_X_TITLE", "")

CLIENT: httpx.AsyncClient | None = None


class EmbedRequest(BaseModel):
    inputs: str | list[str] = Field(default_factory=list)


class EmbedItem(BaseModel):
    values: list[float]


class EmbedResponse(BaseModel):
    embeddings: list[EmbedItem]
    dimensions: int
    model: str


class OpenAIEmbedRequest(BaseModel):
    input: str | list[str] = Field(default_factory=list)
    model: str | None = None
    dimensions: int | None = None
    encoding_format: str | None = None


def _post_process_embeddings(raw: Any) -> np.ndarray:
    vectors = np.asarray(raw, dtype=np.float32)
    if vectors.ndim == 1:
        vectors = vectors.reshape(1, -1)
    if vectors.ndim != 2:
        raise ValueError(f"unexpected embedding payload shape: {vectors.shape}")
    if EMBED_DIM > 0:
        if vectors.shape[1] < EMBED_DIM:
            raise ValueError(
                f"upstream returned {vectors.shape[1]} dims, smaller than requested {EMBED_DIM}"
            )
        vectors = vectors[:, :EMBED_DIM]
    if NORMALIZE and vectors.size:
        # Vectorize normalization across the batch to keep post-processing overhead low.
        norms = np.linalg.norm(vectors, axis=1)
        np.maximum(norms, 1e-12, out=norms)
        vectors /= norms[:, None]
    return vectors


async def _warm_up_upstream(client: httpx.AsyncClient) -> None:
    last_error: Exception | None = None
    for _ in range(STARTUP_RETRIES):
        try:
            warmup = await client.post(SEARCH_UPSTREAM_URL, json={"inputs": ["warmup"]})
            warmup.raise_for_status()
            _post_process_embeddings(warmup.json())
            return
        except (httpx.HTTPError, ValueError) as exc:
            last_error = exc
            import asyncio

            await asyncio.sleep(STARTUP_RETRY_SECONDS)
    raise RuntimeError(f"failed to warm upstream embedder after retries: {last_error}") from last_error


async def _fetch_search_embeddings(client: httpx.AsyncClient, inputs: list[str]) -> np.ndarray:
    chunks: list[np.ndarray] = []
    for start in range(0, len(inputs), SEARCH_UPSTREAM_BATCH_SIZE):
        batch = inputs[start:start + SEARCH_UPSTREAM_BATCH_SIZE]
        resp = await client.post(SEARCH_UPSTREAM_URL, json={"inputs": batch})
        resp.raise_for_status()
        chunks.append(_post_process_embeddings(resp.json()))
    if not chunks:
        return np.empty((0, EMBED_DIM), dtype=np.float32)
    if len(chunks) == 1:
        return chunks[0]
    return np.vstack(chunks)


def _openrouter_headers() -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if OPENROUTER_API_KEY:
        headers["Authorization"] = f"Bearer {OPENROUTER_API_KEY}"
    if OPENROUTER_HTTP_REFERER:
        headers["HTTP-Referer"] = OPENROUTER_HTTP_REFERER
    if OPENROUTER_X_TITLE:
        headers["X-Title"] = OPENROUTER_X_TITLE
    return headers


async def _fetch_index_embeddings(client: httpx.AsyncClient, inputs: list[str]) -> np.ndarray:
    chunks: list[np.ndarray] = []
    headers = _openrouter_headers()
    for start in range(0, len(inputs), INDEX_UPSTREAM_BATCH_SIZE):
        batch = inputs[start:start + INDEX_UPSTREAM_BATCH_SIZE]
        payload: dict[str, Any] = {
            "model": MODEL_ID,
            "input": batch if len(batch) > 1 else batch[0],
            "encoding_format": OPENROUTER_ENCODING_FORMAT,
            "dimensions": EMBED_DIM,
        }
        resp = await client.post(OPENROUTER_URL, headers=headers, json=payload)
        resp.raise_for_status()
        body = resp.json()
        data = body.get("data")
        if not isinstance(data, list):
            raise ValueError("unexpected OpenRouter response shape: missing data array")
        chunk = [item.get("embedding") for item in data]
        chunks.append(_post_process_embeddings(chunk))
    if not chunks:
        return np.empty((0, EMBED_DIM), dtype=np.float32)
    if len(chunks) == 1:
        return chunks[0]
    return np.vstack(chunks)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global CLIENT
    CLIENT = httpx.AsyncClient(timeout=HTTP_TIMEOUT)
    await _warm_up_upstream(CLIENT)
    _load_tiny()
    yield
    await CLIENT.aclose()
    CLIENT = None


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "ok": CLIENT is not None,
        "model": MODEL_ID,
        "search_upstream_url": SEARCH_UPSTREAM_URL,
        "search_upstream_batch_size": SEARCH_UPSTREAM_BATCH_SIZE,
        "index_upstream_url": OPENROUTER_URL,
        "index_upstream_batch_size": INDEX_UPSTREAM_BATCH_SIZE,
        "dimensions": EMBED_DIM,
        "normalize": NORMALIZE,
        "timeout_seconds": HTTP_TIMEOUT,
    }


def _embed_response(vectors: np.ndarray) -> EmbedResponse:
    embeddings = [EmbedItem(values=row.tolist()) for row in vectors]
    return EmbedResponse(embeddings=embeddings, dimensions=int(vectors.shape[1]), model=MODEL_ID)


def _coerce_inputs(value: str | list[str]) -> list[str]:
    if isinstance(value, str):
        return [value]
    return value


def _openai_response(vectors: np.ndarray) -> dict[str, Any]:
    return {
        "object": "list",
        "data": [
            {
                "object": "embedding",
                "index": idx,
                "embedding": row.tolist(),
            }
            for idx, row in enumerate(vectors)
        ],
        "model": MODEL_ID,
        "usage": {
            "prompt_tokens": 0,
            "total_tokens": 0,
        },
    }


def _standard_response(vectors: np.ndarray) -> dict[str, Any]:
    return {
        "embeddings": [{"values": row.tolist()} for row in vectors],
        "dimensions": int(vectors.shape[1]) if vectors.ndim == 2 else EMBED_DIM,
        "model": MODEL_ID,
    }


def _json_response(payload: dict[str, Any]) -> Response:
    return Response(
        content=json.dumps(payload, separators=(",", ":")),
        media_type="application/json",
    )


async def _parse_request_json(request: Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"invalid json: {exc}") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="request body must be a JSON object")
    return payload


@app.post("/search-embed")
async def search_embed(request: Request) -> Response:
    if CLIENT is None:
        raise HTTPException(status_code=503, detail="proxy not ready")
    payload = await _parse_request_json(request)
    inputs = _coerce_inputs(payload.get("inputs", []))
    if not inputs:
        return _json_response(_standard_response(np.empty((0, EMBED_DIM), dtype=np.float32)))

    try:
        vectors = await _fetch_search_embeddings(CLIENT, inputs)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"upstream error: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return _json_response(_standard_response(vectors))


@app.post("/index-embed")
async def index_embed(request: Request) -> Response:
    if CLIENT is None:
        raise HTTPException(status_code=503, detail="proxy not ready")
    if not OPENROUTER_API_KEY:
        raise HTTPException(status_code=503, detail="OpenRouter API key not configured")
    payload = await _parse_request_json(request)
    inputs = _coerce_inputs(payload.get("inputs", []))
    if not inputs:
        return _json_response(_standard_response(np.empty((0, EMBED_DIM), dtype=np.float32)))

    try:
        vectors = await _fetch_index_embeddings(CLIENT, inputs)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"upstream error: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return _json_response(_standard_response(vectors))


@app.post("/embed")
async def embed(request: Request) -> Response:
    # Backward-compatible alias for existing search-time configs.
    return await search_embed(request)


@app.post("/openai-search/v1/embeddings")
async def openai_search_embed(request: Request) -> Response:
    if CLIENT is None:
        raise HTTPException(status_code=503, detail="proxy not ready")
    payload = await _parse_request_json(request)
    inputs = _coerce_inputs(payload.get("input", []))
    if not inputs:
        return _json_response(_openai_response(np.empty((0, EMBED_DIM), dtype=np.float32)))

    try:
        vectors = await _fetch_search_embeddings(CLIENT, inputs)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"upstream error: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return _json_response(_openai_response(vectors))


@app.post("/openai-index/v1/embeddings")
async def openai_index_embed(request: Request) -> Response:
    if CLIENT is None:
        raise HTTPException(status_code=503, detail="proxy not ready")
    if not OPENROUTER_API_KEY:
        raise HTTPException(status_code=503, detail="OpenRouter API key not configured")
    payload = await _parse_request_json(request)
    inputs = _coerce_inputs(payload.get("input", []))
    if not inputs:
        return _json_response(_openai_response(np.empty((0, EMBED_DIM), dtype=np.float32)))

    try:
        vectors = await _fetch_index_embeddings(CLIENT, inputs)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"upstream error: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return _json_response(_openai_response(vectors))


# ---------------------------------------------------------------------------
# Tiny 256-d query encoder in the voyage-4 x P space (dopejobs-tiny-q256).
# Search queries are embedded in-process with onnxruntime (no TEI hop); documents
# are voyage-4-lite (input_type "document", native 1024-d) x P, L2-normalised.
# Everything here is optional and isolated: if the model files or onnxruntime are
# missing, only the /openai-*-tiny routes return 503; the pplx routes are unaffected.
# ---------------------------------------------------------------------------
TINY_DIR = os.environ.get("TINY_DIR", "/opt/dopejobs-tiny")
TINY_DOC_MODEL = os.environ.get("TINY_DOC_MODEL", "mongodb/voyage-4-lite")
TINY_THREADS = int(os.environ.get("TINY_THREADS", "4"))
TINY_MAX_LEN = 32
TINY_DIM = 256
TINY: dict[str, Any] = {"error": "not loaded"}


def _load_tiny() -> None:
    try:
        import onnxruntime as ort
        from tokenizers import Tokenizer

        tok = Tokenizer.from_file(f"{TINY_DIR}/dopejobs-tiny-q256/tokenizer.json")
        tok.enable_truncation(TINY_MAX_LEN)
        so = ort.SessionOptions()
        so.intra_op_num_threads, so.inter_op_num_threads = TINY_THREADS, 1
        so.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        sess = ort.InferenceSession(f"{TINY_DIR}/dopejobs-tiny-q256.onnx", so, providers=["CPUExecutionProvider"])
        proj = np.load(f"{TINY_DIR}/proj_clean_voy4_1024_to_256.npy").astype(np.float32)
        if proj.shape != (1024, TINY_DIM):
            raise ValueError(f"unexpected projection shape {proj.shape}")
        TINY.clear()
        TINY.update(tok=tok, sess=sess, proj=proj)
        _tiny_query_embeddings(["warmup"])
    except Exception as exc:  # never take the proxy down because of the optional encoder
        TINY.clear()
        TINY["error"] = repr(exc)


def _tiny_query_embeddings(inputs: list[str]) -> np.ndarray:
    encs = TINY["tok"].encode_batch([t if t.strip() else "." for t in inputs])
    n = max(len(e.ids) for e in encs)
    ids = np.full((len(encs), n), 1, dtype=np.int64)  # <pad> = 1
    mask = np.zeros_like(ids)
    for i, e in enumerate(encs):
        ids[i, : len(e.ids)] = e.ids
        mask[i, : len(e.ids)] = 1
    return TINY["sess"].run(None, {"input_ids": ids, "attention_mask": mask})[0].astype(np.float32)


def _project_docs(raw: Any) -> np.ndarray:
    x = np.asarray(raw, dtype=np.float32)
    if x.ndim != 2 or x.shape[1] != 1024:
        raise ValueError(f"expected 1024-d voyage vectors, got {x.shape}")
    x /= np.maximum(np.linalg.norm(x, axis=1, keepdims=True), 1e-12)
    y = x @ TINY["proj"]
    y /= np.maximum(np.linalg.norm(y, axis=1, keepdims=True), 1e-12)
    return y


async def _fetch_tiny_index_embeddings(client: httpx.AsyncClient, inputs: list[str]) -> np.ndarray:
    chunks: list[np.ndarray] = []
    headers = _openrouter_headers()
    for start in range(0, len(inputs), INDEX_UPSTREAM_BATCH_SIZE):
        batch = inputs[start:start + INDEX_UPSTREAM_BATCH_SIZE]
        payload = {"model": TINY_DOC_MODEL, "input": batch, "input_type": "document", "encoding_format": "float"}
        resp = await client.post(OPENROUTER_URL, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json().get("data")
        if not isinstance(data, list) or len(data) != len(batch):
            raise ValueError("unexpected OpenRouter response shape")
        data = sorted(data, key=lambda item: item.get("index", 0))
        chunks.append(_project_docs([item.get("embedding") for item in data]))
    return np.vstack(chunks) if chunks else np.empty((0, TINY_DIM), dtype=np.float32)


@app.get("/health-tiny")
def health_tiny() -> dict[str, Any]:
    return {"ok": "sess" in TINY, "error": TINY.get("error"), "dir": TINY_DIR, "doc_model": TINY_DOC_MODEL,
            "dimensions": TINY_DIM, "threads": TINY_THREADS}


@app.post("/openai-search-tiny/v1/embeddings")
async def openai_search_tiny(request: Request) -> Response:
    if "sess" not in TINY:
        raise HTTPException(status_code=503, detail=f"tiny encoder unavailable: {TINY.get('error')}")
    payload = await _parse_request_json(request)
    inputs = _coerce_inputs(payload.get("input", []))
    if not inputs:
        return _json_response(_openai_response(np.empty((0, TINY_DIM), dtype=np.float32)))
    vectors = await run_in_threadpool(_tiny_query_embeddings, inputs)
    return _json_response(_openai_response(vectors))


@app.post("/openai-index-tiny/v1/embeddings")
async def openai_index_tiny(request: Request) -> Response:
    if CLIENT is None or "proj" not in TINY:
        raise HTTPException(status_code=503, detail="proxy or tiny projection not ready")
    if not OPENROUTER_API_KEY:
        raise HTTPException(status_code=503, detail="OpenRouter API key not configured")
    payload = await _parse_request_json(request)
    inputs = _coerce_inputs(payload.get("input", []))
    if not inputs:
        return _json_response(_openai_response(np.empty((0, TINY_DIM), dtype=np.float32)))
    try:
        vectors = await _fetch_tiny_index_embeddings(CLIENT, inputs)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"upstream error: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return _json_response(_openai_response(vectors))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=os.environ.get("PPLX_HOST", "127.0.0.1"),
        port=int(os.environ.get("PPLX_PORT", "8087")),
        log_level=os.environ.get("PPLX_LOG_LEVEL", "info"),
        access_log=False,
    )
