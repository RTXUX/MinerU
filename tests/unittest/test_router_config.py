# Copyright (c) Opendatalab. All rights reserved.
"""验证 Router YAML upstream 配置、认证覆盖、SIGHUP 重载及动态管理共存边界。"""

from __future__ import annotations

import asyncio
import os
import signal
from pathlib import Path

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient

from mineru.kit.router import RouterSettings, create_app
from mineru.kit.router.config import load_upstreams
from mineru.kit.router.proxy import request_upstream
from mineru.kit.router.workers import WorkerPool


def write_config(path: Path, upstreams: list[dict[str, object]]) -> None:
    """将测试 upstream 列表写入标准 YAML 配置文件。"""
    path.write_text(yaml.safe_dump({"upstreams": upstreams}), encoding="utf-8")


def test_reload_retains_owners_and_rejects_invalid_changes(tmp_path: Path) -> None:
    """SIGHUP/reload 保留已移除节点的凭证和引用，拒绝同 ID 换地址及非法配置。"""
    path = tmp_path / "router.yaml"
    write_config(path, [{"id": "a", "url": "http://a", "api_key": "old"}])
    seen: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.host, request.headers.get("authorization")))
        return httpx.Response(200, json={"status": "ok", "data": [], "features": {}}, request=request)

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
            assert old_owner.draining and old_owner.state == "draining"
            assert [worker.worker_id for worker in pool.healthy_workers()] == ["b"]
            assert pool.get("b").max_concurrent_jobs == 4
            await request_upstream(pool, old_owner, "GET", "/v1/tasks/job_old")
            assert seen[-1] == ("a", "Bearer old")
            write_config(path, [{"id": "b", "url": "http://different", "api_key": "secret"}])
            with pytest.raises(ValueError, match="new id"):
                await pool.reload_upstreams()
            assert pool.get("b").base_url == "http://b"
            assert not pool.get("b").draining
            path.write_text("upstreams: [broken", encoding="utf-8")
            with pytest.raises(ValueError, match="parse"):
                await pool.reload_upstreams()
            assert not pool.get("b").draining
            write_config(path, [{"id": "a", "url": "http://a", "api_key": "rotated"}])
            await pool.reload_upstreams()
            assert pool.get("a") is old_owner
            assert not old_owner.draining
            await request_upstream(pool, old_owner, "GET", "/v1/files/file-1", headers={"Authorization": "Bearer caller"})
            assert seen[-1] == ("a", "Bearer rotated")
        finally:
            await pool.close()

    asyncio.run(exercise())


def test_upstream_identity_survives_reordering(tmp_path: Path) -> None:
    """省略显式 id 时，默认标识仅取决于规范化 URL，不受文件顺序影响。"""
    path = tmp_path / "router.yaml"
    write_config(path, [{"url": "http://a/"}, {"url": "http://b"}])
    first = {upstream.url: upstream.id for upstream in load_upstreams(str(path))}
    write_config(path, [{"url": "http://b"}, {"url": "HTTP://A:80"}])
    assert {upstream.url: upstream.id for upstream in load_upstreams(str(path))} == first


@pytest.mark.parametrize(
    "entry",
    [
        {"url": "ftp://a"},
        {"url": "http://a:invalid"},
        {"url": "http://[broken"},
        {"url": "http://a", "headers": {"Invalid Header": "value"}},
        {"url": "http://user:secret@a"},
        {"url": "http://a", "max_concurrent_jobs": True},
        {"url": "http://a", "headers": {"Host": "b"}},
        {"url": "http://a", "api_key": "secret", "headers": {"Authorization": "other"}},
        {"url": "http://a", "api_key": "secret\r\ninjected: yes"},
        {"url": "http://a", "id": "local-spoof"},
        {"url": "http://a", "id": "bad/slash"},
        {"url": "http://a", "typo": 1},
    ],
)
def test_invalid_config_is_rejected(tmp_path: Path, entry: dict[str, object]) -> None:
    """非法协议、内嵌凭证、保留请求头与注入字符均在配置加载阶段拒绝。"""
    path = tmp_path / "router.yaml"
    write_config(path, [entry])
    with pytest.raises(ValueError):
        load_upstreams(str(path))


def test_config_credentials_cover_discovery_tasks_and_streaming(tmp_path: Path) -> None:
    """配置的后端凭证覆盖探测、Tasks 创建与流式下载，同时保持调用方 scope 隔离。"""
    path = tmp_path / "router.yaml"
    write_config(path, [{"url": "http://a", "api_key": "backend-key", "max_concurrent_jobs": 3}])
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer backend-key"
        seen.append(request.url.path)
        if request.url.path == "/v1/health":
            return httpx.Response(
                200,
                json={"status": "ok", "features": {"sources": ["upload", "url"], "task_output_formats": ["default"]}},
                request=request,
            )
        if request.url.path == "/v1/tiers":
            return httpx.Response(200, json={"data": [{"id": "standard"}]}, request=request)
        if request.url.path == "/v1/models":
            return httpx.Response(200, json={"data": []}, request=request)
        if request.url.path == "/v1/usage":
            return httpx.Response(200, json={"object": "usage", "current": {}}, request=request)
        if request.url.path == "/v1/files/upstream-out/content":
            return httpx.Response(200, content=b"artifact-bytes", headers={"content-type": "application/zip"}, request=request)
        return httpx.Response(404, json={"error": {"code": "not_found", "message": "missing"}}, request=request)

    app = create_app(
        RouterSettings(config_file=str(path), local_gpus="none", worker_refresh_interval_seconds=0),
        transport=httpx.MockTransport(handler),
    )
    with TestClient(app) as client:
        response = client.post(
            "/v1/tasks",
            json={"tier": "standard", "files": [{"file_name": "a.pdf", "bytes": 3}]},
            headers={"Authorization": "Bearer caller-a"},
        )
        assert response.status_code == 202, response.text
        task_id = response.json()["task_id"]
        assert client.get(f"/v1/tasks/{task_id}", headers={"Authorization": "Bearer caller-b"}).status_code == 404
        assert client.get(f"/v1/tasks/{task_id}", headers={"Authorization": "Bearer caller-a"}).status_code == 200
        usage = client.get("/v1/usage", headers={"Authorization": "Bearer caller-a"}).json()
        assert usage["limits"]["max_concurrent_jobs"] == 3
        worker = app.state.worker_pool.workers[0]
        route = app.state.registry.register_file(
            owner_scope="auth:" + __import__("hashlib").sha256(b"Bearer caller-a").hexdigest(),
            worker_id=worker.worker_id,
            upstream_id="upstream-out",
        )
        downloaded = client.get(f"/v1/files/{route.public_id}/content", headers={"Authorization": "Bearer caller-a"})
        assert downloaded.status_code == 200
        assert downloaded.content == b"artifact-bytes"
        assert seen[:4] == ["/v1/health", "/v1/tiers", "/v1/models", "/v1/usage"]
        assert seen[-1] == "/v1/files/upstream-out/content"


def test_sighup_preserves_cli_and_admin_workers(tmp_path: Path) -> None:
    """SIGHUP 仅对 YAML 管理的节点做增删，保留 CLI 参数与管理 API 动态注册的节点。"""
    path = tmp_path / "router.yaml"
    write_config(path, [{"id": "yaml-1", "url": "http://yaml-1", "api_key": "k1"}])

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/health":
            return httpx.Response(200, json={"status": "ok", "features": {"sources": ["upload"]}}, request=request)
        if request.url.path == "/v1/tiers":
            return httpx.Response(200, json={"data": [{"id": "flash"}]}, request=request)
        return httpx.Response(200, json={"data": []}, request=request)

    app = create_app(
        RouterSettings(
            upstream_urls=("http://cli-1",),
            config_file=str(path),
            local_gpus="none",
            worker_refresh_interval_seconds=0,
            admin_api_key="admin-secret",
        ),
        transport=httpx.MockTransport(handler),
    )
    admin_headers = {"Authorization": "Bearer admin-secret"}
    with TestClient(app) as client:
        registered = client.post("/v1/router/workers", json={"url": "http://admin-1"}, headers=admin_headers)
        assert registered.status_code == 201
        assert {w.base_url for w in app.state.worker_pool.healthy_workers()} == {
            "http://cli-1",
            "http://yaml-1",
            "http://admin-1",
        }
        write_config(path, [])
        client.portal.call(app.state.reload_upstreams)
        assert app.state.worker_pool.get("yaml-1").draining
        assert {w.base_url for w in app.state.worker_pool.healthy_workers()} == {"http://cli-1", "http://admin-1"}

        # 与现有 CLI 或动态节点 URL 冲突的 YAML 变更必须整批拒绝。
        write_config(path, [{"id": "conflict", "url": "http://admin-1"}])
        with pytest.raises(ValueError, match="unique"):
            client.portal.call(app.state.reload_upstreams)
        assert {w.base_url for w in app.state.worker_pool.healthy_workers()} == {"http://cli-1", "http://admin-1"}


def test_sighup_reloads_on_main_event_loop(tmp_path: Path) -> None:
    """主事件循环收到真实 SIGHUP 时应用新配置，并在退出后恢复原信号处理器。"""
    if not hasattr(signal, "SIGHUP"):
        pytest.skip("SIGHUP is unavailable")
    path = tmp_path / "router.yaml"
    write_config(path, [{"id": "before", "url": "http://before"}])
    previous = signal.getsignal(signal.SIGHUP)
    app = create_app(
        RouterSettings(config_file=str(path), local_gpus="none", worker_refresh_interval_seconds=0),
        transport=httpx.MockTransport(lambda req: httpx.Response(200, json={"status": "ok", "data": []}, request=req)),
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
