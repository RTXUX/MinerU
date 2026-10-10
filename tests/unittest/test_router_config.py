from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient

from mineru.kit.router import RouterSettings, create_app
from mineru.kit.router.config import load_upstreams
from mineru.kit.router.proxy import request_upstream
from mineru.kit.router.workers import WorkerPool


def write_config(path: Path, upstreams: list[dict]) -> None:
    path.write_text(yaml.safe_dump({"upstreams": upstreams}))


def test_reload_retains_owners_and_rejects_invalid_changes(tmp_path: Path) -> None:
    path = tmp_path / "router.yaml"
    write_config(path, [{"id": "a", "url": "http://a", "api_key": "old"}])
    seen: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.host, request.headers.get("authorization")))
        return httpx.Response(200, json={"status": "ok", "data": [], "features": {}})

    async def exercise() -> None:
        pool = WorkerPool(
            RouterSettings(config_file=str(path), local_gpus="none", worker_refresh_interval_seconds=0),
            transport=httpx.MockTransport(handler),
        )
        try:
            await pool.start()
            old_owner = pool.get("a")
            assert seen == [("a", "Bearer old")] * 3
            write_config(path, [{"id": "b", "url": "http://b", "api_key": "new", "max_concurrent_jobs": 4}])
            await pool.reload_upstreams()
            assert pool.get("a") is old_owner
            assert old_owner.draining
            assert [worker.worker_id for worker in pool.healthy_workers()] == ["b"]
            assert pool.get("b").max_concurrent_jobs == 4
            await request_upstream(pool, old_owner, "GET", "/v1/parse/jobs/old")
            assert seen[-1] == ("a", "Bearer old")
            write_config(path, [{"id": "b", "url": "http://different", "api_key": "secret"}])
            with pytest.raises(ValueError, match="new id"):
                await pool.reload_upstreams()
            assert pool.get("b").base_url == "http://b"
            assert not pool.get("b").draining
            path.write_text("upstreams: [broken")
            with pytest.raises(ValueError, match="parse"):
                await pool.reload_upstreams()
            assert not pool.get("b").draining
            write_config(path, [{"id": "a", "url": "http://a", "api_key": "rotated"}])
            await pool.reload_upstreams()
            assert pool.get("a") is old_owner
            assert not old_owner.draining
            await request_upstream(pool, old_owner, "GET", "/v1/files/file", headers={"authorization": "Bearer caller"})
            assert seen[-1] == ("a", "Bearer rotated")
        finally:
            await pool.close()

    asyncio.run(exercise())


def test_upstream_identity_survives_reordering(tmp_path: Path) -> None:
    path = tmp_path / "router.yaml"
    write_config(path, [{"url": "http://a/"}, {"url": "http://b"}])
    first = {upstream.url: upstream.id for upstream in load_upstreams(str(path))}
    write_config(path, [{"url": "http://b"}, {"url": "http://a"}])
    assert {upstream.url: upstream.id for upstream in load_upstreams(str(path))} == first


@pytest.mark.parametrize(
    "entry",
    [
        {"url": "ftp://a"},
        {"url": "http://user:secret@a"},
        {"url": "http://a", "max_concurrent_jobs": True},
        {"url": "http://a", "headers": {"Host": "b"}},
        {"url": "http://a", "api_key": "secret", "headers": {"Authorization": "other"}},
        {"url": "http://a", "api_key": "secret\r\ninjected: yes"},
        {"url": "http://a", "typo": 1},
    ],
)
def test_invalid_config_is_rejected(tmp_path: Path, entry: dict) -> None:
    path = tmp_path / "router.yaml"
    write_config(path, [entry])
    with pytest.raises(ValueError):
        load_upstreams(str(path))


def test_config_credentials_cover_discovery_and_proxy(tmp_path: Path) -> None:
    path = tmp_path / "router.yaml"
    write_config(path, [{"url": "http://a", "api_key": "backend-key"}])
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer backend-key"
        seen.append(request.url.path)
        if request.url.path == "/v1/health":
            return httpx.Response(200, json={"status": "ok", "features": {"sources": ["file_id"]}})
        if request.url.path == "/v1/uploads":
            return httpx.Response(200, json={"id": "upload_upstream", "status": "pending"})
        return httpx.Response(200, json={"data": []})

    app = create_app(
        RouterSettings(config_file=str(path), local_gpus="none", worker_refresh_interval_seconds=0),
        transport=httpx.MockTransport(handler),
    )
    with TestClient(app) as client:
        response = client.post("/v1/uploads", json={"filename": "a.pdf"}, headers={"Authorization": "Bearer caller"})
        assert response.status_code == 200
        assert seen == ["/v1/health", "/v1/tiers", "/v1/models", "/v1/uploads"]


def test_sighup_reloads_on_main_event_loop(tmp_path: Path) -> None:
    import os
    import signal

    if not hasattr(signal, "SIGHUP"):
        pytest.skip("SIGHUP is unavailable")
    path = tmp_path / "router.yaml"
    write_config(path, [{"id": "before", "url": "http://before"}])
    previous = signal.getsignal(signal.SIGHUP)
    app = create_app(
        RouterSettings(config_file=str(path), local_gpus="none", worker_refresh_interval_seconds=0),
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"status": "ok", "data": []})),
    )

    async def exercise() -> None:
        async with app.router.lifespan_context(app):
            write_config(path, [{"id": "after", "url": "http://after"}])
            os.kill(os.getpid(), signal.SIGHUP)
            async with asyncio.timeout(5):
                while "after" not in {worker.worker_id for worker in app.state.worker_pool.healthy_workers()}:
                    await asyncio.sleep(0.01)
            assert app.state.worker_pool.get("before").draining

    asyncio.run(exercise())
    assert signal.getsignal(signal.SIGHUP) == previous
