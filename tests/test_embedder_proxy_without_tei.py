"""The embedding proxy must start and serve the tiny routes while TEI is down.

Needs the proxy's deps (fastapi, httpx, numpy); skipped otherwise. By default the tiny
encoder is stubbed. To use the real artifacts, point TINY_DIR at a folder laid out like
/opt/dopejobs-tiny:
    TINY_DIR=/path/to/release pytest tests/test_embedder_proxy_without_tei.py -v
"""
import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
np = pytest.importorskip("numpy")
from fastapi.testclient import TestClient  # noqa: E402

QUERY = {"model": "text-embedding-3-small", "input": ["registered nurse night shift"], "dimensions": 256}


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _FakeTEI(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        inputs = json.loads(self.rfile.read(n) or b"{}").get("inputs", [])
        body = json.dumps([[1.0] * 1024 for _ in inputs]).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def svc(monkeypatch):
    from ops import perplexity_embedder_service as svc

    port = _free_port()  # nothing listens here: TEI is "stopped"
    monkeypatch.setattr(svc, "SEARCH_UPSTREAM_URL", f"http://127.0.0.1:{port}/embed")
    monkeypatch.setattr(svc, "STARTUP_RETRIES", 30)  # the unit's value: must not delay startup
    monkeypatch.setattr(svc, "STARTUP_RETRY_SECONDS", 0.05)
    monkeypatch.setitem(svc.SEARCH_UPSTREAM, "ok", None)
    tiny_dir = os.environ.get("TINY_DIR")
    if tiny_dir and os.path.exists(f"{tiny_dir}/dopejobs-tiny-q256.onnx"):
        monkeypatch.setattr(svc, "TINY_DIR", tiny_dir)
    else:
        def fake_load():
            svc.TINY.clear()
            svc.TINY.update(sess=object(), tok=None, proj=np.eye(1024, 256, dtype=np.float32))

        def fake_embed(inputs):
            v = np.zeros((len(inputs), 256), dtype=np.float32)
            v[:, 0] = 1.0
            return v

        monkeypatch.setattr(svc, "_load_tiny", fake_load)
        monkeypatch.setattr(svc, "_tiny_query_embeddings", fake_embed)
    svc._test_tei_port = port
    return svc


def test_starts_and_serves_tiny_with_tei_down(svc):
    t0 = time.monotonic()
    with TestClient(svc.app) as c:
        assert time.monotonic() - t0 < 1.0, "startup waited for TEI"
        assert c.get("/health").json()["ok"] is True
        assert c.get("/health-tiny").json()["ok"] is True

        r = c.post("/openai-search-tiny/v1/embeddings", json=QUERY)
        assert r.status_code == 200
        vec = np.asarray(r.json()["data"][0]["embedding"])
        assert vec.shape == (256,) and abs(np.linalg.norm(vec) - 1) < 1e-4

        for path, body in [
            ("/openai-search/v1/embeddings", {"input": "nurse"}),
            ("/search-embed", {"inputs": "nurse"}),
            ("/embed", {"inputs": "nurse"}),
        ]:
            r = c.post(path, json=body)
            assert r.status_code == 503, (path, r.text)
            assert "TEI" in r.json()["detail"]
        assert c.get("/health").json()["search_upstream_ok"] is False


def test_pplx_routes_recover_when_tei_returns(svc):
    with TestClient(svc.app) as c:
        assert c.post("/openai-search/v1/embeddings", json={"input": "nurse"}).status_code == 503

        server = ThreadingHTTPServer(("127.0.0.1", svc._test_tei_port), _FakeTEI)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            r = c.post("/openai-search/v1/embeddings", json={"input": "nurse"})
            assert r.status_code == 200, r.text
            assert len(r.json()["data"][0]["embedding"]) == svc.EMBED_DIM
            assert c.get("/health").json()["search_upstream_ok"] is True
            assert c.post("/openai-search-tiny/v1/embeddings", json=QUERY).status_code == 200
        finally:
            server.shutdown()
            server.server_close()
