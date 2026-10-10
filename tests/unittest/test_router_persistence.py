from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

redis = pytest.importorskip("redis")
from mineru.kit.router import RouterSettings, create_app  # noqa: E402
from mineru.kit.router.persistence import RedisStateStore  # noqa: E402
from mineru.kit.router.proxy import RouterProxyError  # noqa: E402
from test_v1_router import _FakeV1Upstream, _upload_file, _upload_files_on_distinct_workers  # noqa: E402


@pytest.fixture
def redis_target() -> Iterator[tuple[str, str]]:
    url = os.getenv("MINERU_TEST_REDIS_URL")
    if not url:
        pytest.skip("Set MINERU_TEST_REDIS_URL to run real Redis integration tests")
    namespace = "test-" + uuid.uuid4().hex
    client = redis.Redis.from_url(url)
    client.ping()
    try:
        yield url, namespace
    finally:
        keys = list(client.scan_iter(f"mineru-router:{{{namespace}}}:*"))
        if keys:
            client.delete(*keys)
        client.close()


def router(redis_target: tuple[str, str], tmp_path: Path, *upstreams: _FakeV1Upstream) -> Any:
    url, namespace = redis_target
    by_host = {upstream.name: upstream for upstream in upstreams}

    async def handle(request: httpx.Request) -> httpx.Response:
        return await by_host[request.url.host].handle(request)

    return create_app(
        RouterSettings(
            upstream_urls=tuple(f"http://{upstream.name}" for upstream in upstreams),
            local_gpus="none",
            redis_url=url,
            redis_namespace=namespace,
            source_dir=str(tmp_path / "sources"),
            worker_refresh_interval_seconds=0,
        ),
        transport=httpx.MockTransport(handle),
    )


def test_restart_recovers_jobs_files_scopes_and_counters(redis_target: tuple[str, str], tmp_path: Path) -> None:
    upstream = _FakeV1Upstream("worker-a", ("standard",))
    app = router(redis_target, tmp_path, upstream)
    headers = {"authorization": "Bearer caller"}
    with TestClient(app) as client:
        file_id = _upload_file(client, token="caller", content=b"pdf")
        response = client.post(
            "/v1/parse/jobs",
            headers=headers,
            json={
                "tier": "standard",
                "files": [{"source": {"type": "file_id", "file_id": file_id}}],
            },
        )
        assert response.status_code == 202
        job_id = response.json()["job_id"]
        # The mapping is durable before the response, not just at shutdown.
        with redis.Redis.from_url(redis_target[0]) as inspect:
            state = json.loads(inspect.get(app.state.persistence.state_key))
        saved = next(route for route in state["registry"]["routes"] if route["public_id"] == job_id)
        assert saved["metadata"]["active_counted"]
        assert saved["metadata"]["upstream_headers"] == headers

    restored = router(redis_target, tmp_path, upstream)
    with TestClient(restored) as client:
        assert sum(worker.active_jobs for worker in restored.state.worker_pool.workers) == 1
        assert client.get(f"/v1/parse/jobs/{job_id}").status_code == 404
        assert client.get(f"/v1/files/{file_id}", headers=headers).status_code == 200
        result = client.get(f"/v1/parse/jobs/{job_id}", headers=headers)
        assert result.status_code == 200
        assert result.json()["status"] == "completed"
        output_id = result.json()["files"][0]["output_files"]["markdown"]["file_id"]
        assert client.get(f"/v1/files/{output_id}/content", headers=headers).content == b"result"
        assert sum(worker.active_jobs for worker in restored.state.worker_pool.workers) == 0
        assert client.get("/v1/parse/jobs", headers=headers).json()["data"][0]["job_id"] == job_id
        assert client.delete(f"/v1/files/{output_id}", headers=headers).status_code == 200
    with TestClient(router(redis_target, tmp_path, upstream)) as client:
        assert client.get(f"/v1/files/{output_id}", headers=headers).status_code == 404


def test_restart_recovers_pending_upload_and_cross_worker_copy(redis_target: tuple[str, str], tmp_path: Path) -> None:
    first = _FakeV1Upstream("worker-a", ("standard",))
    second = _FakeV1Upstream("worker-b", ("standard",))
    app = router(redis_target, tmp_path, first, second)
    headers = {"authorization": "Bearer caller"}
    with TestClient(app) as client:
        created = client.post(
            "/v1/uploads",
            headers=headers,
            json={
                "filename": "pending.pdf",
                "bytes": 3,
                "mime_type": "application/pdf",
                "purpose": "parse",
            },
        )
        upload_id = created.json()["id"]
        assert client.put(f"/v1/uploads/{upload_id}/content", headers=headers, content=b"pdf").status_code == 200
    restored = router(redis_target, tmp_path, first, second)
    with TestClient(restored) as client:
        assert client.post(f"/v1/uploads/{upload_id}/complete", headers=headers, json={}).status_code == 200
        files = _upload_files_on_distinct_workers(client, restored, token="caller")
    restored = router(redis_target, tmp_path, first, second)
    with TestClient(restored) as client:
        result = client.post(
            "/v1/parse/jobs",
            headers=headers,
            json={
                "tier": "standard",
                "files": [{"source": {"type": "file_id", "file_id": file_id}} for file_id in files],
            },
        )
        assert result.status_code == 202, result.text
        job_id = result.json()["job_id"]
        route = restored.state.registry.get("job", job_id)
        assert route.metadata["copied_inputs"]
    restored = router(redis_target, tmp_path, first, second)
    with TestClient(restored) as client:
        # Reconciliation must deserialize copied-input dataclasses and retain auth.
        client.portal.call(restored.state.reconcile_jobs_once)
        route = restored.state.registry.get("job", job_id)
        assert "copied_inputs" not in route.metadata
        assert "upstream_headers" not in route.metadata
        assert set(files).issubset({item["id"] for item in client.get("/v1/files", headers=headers).json()["data"]})
        result = client.get(f"/v1/parse/jobs/{job_id}", headers=headers)
        assert {file["file_id"] for file in result.json()["files"]} == set(files)


def test_removed_upstream_mapping_survives_restart(redis_target: tuple[str, str], tmp_path: Path) -> None:
    upstream = _FakeV1Upstream("worker-a", ("standard",))
    app = router(redis_target, tmp_path, upstream)
    headers = {"authorization": "Bearer caller"}
    with TestClient(app) as client:
        file_id = _upload_file(client, token="caller", content=b"pdf")
        result = client.post(
            "/v1/parse/jobs",
            headers=headers,
            json={
                "tier": "standard",
                "files": [{"source": {"type": "file_id", "file_id": file_id}}],
            },
        )
        job_id = result.json()["job_id"]
    app = create_app(
        RouterSettings(
            local_gpus="none", redis_url=redis_target[0], redis_namespace=redis_target[1], worker_refresh_interval_seconds=0
        ),
        transport=httpx.MockTransport(upstream.handle),
    )
    with TestClient(app) as client:
        assert app.state.worker_pool.healthy_workers() == []
        assert client.get(f"/v1/parse/jobs/{job_id}", headers=headers).status_code == 200
        assert client.get("/v1/health").status_code == 503


def test_single_owner_renewal_and_stale_writer_fencing(redis_target: tuple[str, str]) -> None:
    async def exercise() -> None:
        first = RedisStateStore(*redis_target, lease_seconds=1)
        second = RedisStateStore(*redis_target, lease_seconds=1)
        try:
            assert await first.start() is None
            with pytest.raises(RuntimeError, match="Another router"):
                await second.start()
            await asyncio.sleep(1.2)
            await first.assert_owner()
            await first.save(lambda: {"version": 1, "value": "original"})
            # Simulate expiration/takeover while the old process remains alive.
            await first.client.set(first.lease_key, "new-owner", px=1000)
            with pytest.raises(RouterProxyError):
                await first.save(lambda: {"version": 1, "value": "stale"})
            assert json.loads(await first.client.get(first.state_key))["value"] == "original"
            await first.close()
            assert await second.client.get(first.lease_key) == b"new-owner"
        finally:
            await first.close()
            await second.close()

    asyncio.run(exercise())


def test_lost_lease_fails_closed_before_proxy(redis_target: tuple[str, str], tmp_path: Path) -> None:
    upstream = _FakeV1Upstream("worker-a", ("standard",))
    app = router(redis_target, tmp_path, upstream)
    with TestClient(app) as client:
        with redis.Redis.from_url(redis_target[0]) as external:
            external.set(app.state.persistence.lease_key, "new-owner", ex=60)
        result = client.post("/v1/uploads", json={"filename": "a.pdf"})
        assert result.status_code == 503
        assert result.json()["error"]["code"] == "router_state_unavailable"
        assert upstream.upload_counter == 0


def test_corrupt_snapshot_is_not_overwritten(redis_target: tuple[str, str], tmp_path: Path) -> None:
    app = router(redis_target, tmp_path, _FakeV1Upstream("worker-a", ("standard",)))
    with redis.Redis.from_url(redis_target[0]) as external:
        external.set(app.state.persistence.state_key, '{"version":1,"registry":{}}')
        with pytest.raises(RuntimeError, match="Invalid router Redis snapshot"):
            with TestClient(app):
                pass
        assert external.get(app.state.persistence.state_key) == b'{"version":1,"registry":{}}'
        assert external.get(app.state.persistence.lease_key) is None


def test_checkpoint_failure_does_not_acknowledge_success(
    redis_target: tuple[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from redis.exceptions import ConnectionError

    app = router(redis_target, tmp_path, _FakeV1Upstream("worker-a", ("standard",)))
    with TestClient(app) as client:

        async def fail_save(*args: Any, **kwargs: Any) -> None:
            raise ConnectionError("simulated unavailable Redis")

        monkeypatch.setattr(app.state.persistence.client, "eval", fail_save)
        result = client.post(
            "/v1/uploads",
            json={
                "filename": "a.pdf",
                "bytes": 3,
                "mime_type": "application/pdf",
                "purpose": "parse",
            },
        )
        assert result.status_code == 503
        assert result.json()["error"]["code"] == "router_state_unavailable"
        monkeypatch.undo()


def test_sigkill_restart_uses_acknowledged_mapping(redis_target: tuple[str, str], tmp_path: Path) -> None:
    import socket
    import subprocess
    import sys
    import time

    if os.name != "posix":
        pytest.skip("Process crash test requires POSIX file descriptor inheritance")
    script = """
import os
import httpx
import uvicorn
from mineru.kit.router import RouterSettings, create_app

def backend(request):
    path = request.url.path
    if path == "/v1/health":
        data = {"status": "ok", "features": {"sources": ["url"]}}
    elif path == "/v1/tiers":
        data = {"data": [{"id": "standard"}]}
    elif path == "/v1/models":
        data = {"data": []}
    elif path == "/v1/parse/jobs":
        return httpx.Response(202, json={"job_id": "backend-job", "status": "queued", "files": []})
    elif path == "/v1/parse/jobs/backend-job":
        data = {"job_id": "backend-job", "status": "completed", "files": [
            {"status": "completed", "output_files": {"markdown": {"file_id": "backend-output"}}}
        ]}
    elif path == "/v1/files/backend-output":
        data = {"id": "backend-output", "purpose": "parse_output", "created_at": 1}
    elif path == "/v1/files/backend-output/content":
        return httpx.Response(200, content=b"recovered-result")
    else:
        return httpx.Response(404)
    return httpx.Response(200, json=data)

app = create_app(RouterSettings(
    upstream_urls=("http://backend",), local_gpus="none",
    redis_url=os.environ["MINERU_TEST_REDIS_URL"], redis_namespace=os.environ["TEST_ROUTER_NAMESPACE"],
    worker_refresh_interval_seconds=0,
), transport=httpx.MockTransport(backend))
app.state.persistence.lease_seconds = 1
uvicorn.run(app, fd=int(os.environ["TEST_ROUTER_FD"]), log_level="warning")
"""
    with socket.socket() as listener, (tmp_path / "router-process.log").open("w+") as log:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        env = {
            **os.environ,
            "MINERU_TEST_REDIS_URL": redis_target[0],
            "TEST_ROUTER_NAMESPACE": redis_target[1],
            "TEST_ROUTER_FD": str(listener.fileno()),
        }
        processes = []

        def start() -> subprocess.Popen:
            process = subprocess.Popen(
                [sys.executable, "-c", script], env=env, pass_fds=(listener.fileno(),), stdout=log, stderr=log
            )
            processes.append(process)
            deadline = time.monotonic() + 20
            with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=0.3) as probe:
                while time.monotonic() < deadline and process.poll() is None:
                    try:
                        if probe.get("/v1/health").status_code == 200:
                            return process
                    except httpx.HTTPError:
                        pass
                    time.sleep(0.05)
            log.seek(0)
            pytest.fail("Router process did not start: " + log.read())

        try:
            first = start()
            headers = {"authorization": "Bearer crash-test"}
            with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=5, headers=headers) as client:
                result = client.post(
                    "/v1/parse/jobs",
                    json={
                        "tier": "standard",
                        "files": [{"source": {"type": "url", "url": "https://example.invalid/input.pdf"}}],
                    },
                )
                assert result.status_code == 202, result.text
                job_id = result.json()["job_id"]
            first.kill()
            first.wait(timeout=5)
            time.sleep(1.1)
            start()
            with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=5, headers=headers) as client:
                result = client.get(f"/v1/parse/jobs/{job_id}")
                assert result.status_code == 200, result.text
                assert result.json()["status"] == "completed"
                output_id = result.json()["files"][0]["output_files"]["markdown"]["file_id"]
                assert client.get(f"/v1/files/{output_id}/content").content == b"recovered-result"
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)


def test_reload_credentials_and_drained_owner_persist(redis_target: tuple[str, str], tmp_path: Path) -> None:
    import yaml

    config = tmp_path / "router.yaml"
    config.write_text(yaml.safe_dump({"upstreams": [{"id": "a", "url": "http://worker-a", "api_key": "old"}]}))
    upstream = _FakeV1Upstream("worker-a", ("standard",))
    seen: list[str] = []

    async def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("authorization", ""))
        return await upstream.handle(request)

    def make_app() -> Any:
        return create_app(
            RouterSettings(
                config_file=str(config),
                local_gpus="none",
                redis_url=redis_target[0],
                redis_namespace=redis_target[1],
                worker_refresh_interval_seconds=0,
            ),
            transport=httpx.MockTransport(handle),
        )

    headers = {"authorization": "Bearer caller"}
    app = make_app()
    with TestClient(app) as client:
        response = client.post(
            "/v1/parse/jobs",
            headers=headers,
            json={"tier": "standard", "files": [{"source": {"type": "url", "url": "https://example.invalid/input.pdf"}}]},
        )
        job_id = response.json()["job_id"]
        config.write_text(yaml.safe_dump({"upstreams": [{"id": "a", "url": "http://worker-a", "api_key": "rotated"}]}))
        client.portal.call(app.state.reload_upstreams)
        config.write_text("upstreams: []\n")
        client.portal.call(app.state.reload_upstreams)
        assert app.state.worker_pool.get("a").draining
    seen.clear()
    with TestClient(make_app()) as client:
        assert client.get(f"/v1/parse/jobs/{job_id}", headers=headers).status_code == 200
        assert seen and set(seen) == {"Bearer rotated"}
