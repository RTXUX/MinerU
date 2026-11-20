# Copyright (c) Opendatalab. All rights reserved.
"""验证 Router 基于 Redis 的单活租约、崩溃/重启恢复与失败闭锁契约。"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import os
import sys
import threading
import time
import uuid
import zipfile
from collections.abc import Iterator
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient
from task_test_utils import pending_files, task_url
from test_task_api import HTML, _submit, _terminal, _upload
from test_v1_router import _FakeV1Upstream

from mineru.kit.router import RouterSettings, create_app
from mineru.kit.router.persistence import (
    _RELEASE,
    _RENEW,
    _SAVE,
    RedisStateStore,
    RedisTransportError,
)
from mineru.kit.router.proxy import RouterProxyError
from mineru.parser import api_server


class _InMemoryRedisServer:
    """实现单活租约与快照 Lua 原语的进程内存储，使 CI 无外部 Redis 时仍执行完整契约测试。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._values: dict[str, bytes] = {}
        self._deadlines: dict[str, float] = {}

    def _purge_expired(self) -> None:
        now = time.monotonic()
        for key, deadline in list(self._deadlines.items()):
            if deadline <= now:
                self._deadlines.pop(key, None)
                self._values.pop(key, None)

    def get_sync(self, key: str) -> bytes | None:
        with self._lock:
            self._purge_expired()
            return self._values.get(key)

    def set_sync(
        self,
        key: str,
        value: str | bytes,
        *,
        nx: bool = False,
        px: int | None = None,
        ex: int | None = None,
    ) -> bool:
        encoded = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        with self._lock:
            self._purge_expired()
            if nx and key in self._values:
                return False
            self._values[key] = encoded
            if px is not None:
                self._deadlines[key] = time.monotonic() + (px / 1000.0)
            elif ex is not None:
                self._deadlines[key] = time.monotonic() + float(ex)
            else:
                self._deadlines.pop(key, None)
            return True

    def eval_sync(self, script: str, numkeys: int, *keys_and_args: Any) -> int:
        keys = [str(item) for item in keys_and_args[:numkeys]]
        args = list(keys_and_args[numkeys:])
        with self._lock:
            self._purge_expired()
            expected = args[0].encode("utf-8") if isinstance(args[0], str) else bytes(args[0])
            if self._values.get(keys[0]) != expected:
                return 0
            if script == _RENEW:
                self._deadlines[keys[0]] = time.monotonic() + (int(args[1]) / 1000.0)
                return 1
            if script == _SAVE:
                payload = args[1].encode("utf-8") if isinstance(args[1], str) else bytes(args[1])
                self._values[keys[1]] = payload
                self._deadlines.pop(keys[1], None)
                return 1
            if script == _RELEASE:
                self._values.pop(keys[0], None)
                self._deadlines.pop(keys[0], None)
                return 1
            raise ValueError("Unsupported Lua script in fake Redis")


class _FakeAsyncRedisClient:
    """包装共享进程内 Redis 存储，模拟每个 Router 进程独立的异步客户端连接。"""

    def __init__(self, server: _InMemoryRedisServer) -> None:
        self._server = server
        self.closed = False

    async def get(self, key: str) -> bytes | None:
        return self._server.get_sync(key)

    async def set(
        self,
        key: str,
        value: str | bytes,
        *,
        nx: bool = False,
        px: int | None = None,
        ex: int | None = None,
    ) -> bool:
        return self._server.set_sync(key, value, nx=nx, px=px, ex=ex)

    async def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> int:
        return self._server.eval_sync(script, numkeys, *keys_and_args)

    async def aclose(self) -> None:
        self.closed = True


@dataclass
class _RedisHarness:
    """统一封装内存替身与真实外部 Redis 的连接工厂及状态检查辅助方法。"""

    url: str
    namespace: str
    server: _InMemoryRedisServer | None = None

    def new_client(self) -> Any:
        if self.server is not None:
            return _FakeAsyncRedisClient(self.server)
        return None

    def get_raw(self, key: str) -> bytes | None:
        if self.server is not None:
            return self.server.get_sync(key)
        import redis

        with redis.Redis.from_url(self.url) as external:
            return external.get(key)

    def set_raw(self, key: str, value: str | bytes, *, ex: int | None = None) -> None:
        if self.server is not None:
            self.server.set_sync(key, value, ex=ex)
            return
        import redis

        with redis.Redis.from_url(self.url) as external:
            external.set(key, value, ex=ex)


@pytest.fixture
def redis_harness() -> Iterator[_RedisHarness]:
    """设置 MINERU_TEST_REDIS_URL 时连接真实 Redis，否则使用高保真进程内实现。"""
    url = os.getenv("MINERU_TEST_REDIS_URL")
    namespace = "test-" + uuid.uuid4().hex
    if not url:
        yield _RedisHarness(url="redis://in-memory/0", namespace=namespace, server=_InMemoryRedisServer())
        return
    import redis

    client = redis.Redis.from_url(url)
    client.ping()
    try:
        yield _RedisHarness(url=url, namespace=namespace, server=None)
    finally:
        keys = list(client.scan_iter(f"mineru-router:{{{namespace}}}:*"))
        if keys:
            client.delete(*keys)
        client.close()


class _IndependentWorkerCluster(httpx.AsyncBaseTransport):
    """在独立事件循环线程托管远端 worker，使其生命周期跨越多次 Router 重启。"""

    def __init__(self) -> None:
        self._stack = ExitStack()
        self._clients: dict[str, TestClient] = {}

    def register(self, apps: dict[str, FastAPI]) -> _IndependentWorkerCluster:
        for host, app in apps.items():
            if host not in self._clients:
                self._clients[host] = self._stack.enter_context(TestClient(app, base_url=f"http://{host}"))
        return self

    def _send_sync(
        self,
        host: str,
        method: str,
        raw_path: str,
        headers: list[tuple[str, str]],
        body: bytes,
    ) -> tuple[int, list[tuple[str, str]], bytes]:
        client = self._clients[host]
        resp = client.request(method, raw_path, headers=headers, content=body)
        return resp.status_code, list(resp.headers.multi_items()), resp.content

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        body = await request.aread()
        headers = [
            (k.decode("latin-1"), v.decode("latin-1"))
            for k, v in request.headers.raw
            if k.lower() not in {b"host", b"content-length"}
        ]
        status_code, resp_headers, content = await asyncio.to_thread(
            self._send_sync,
            request.url.host,
            request.method,
            request.url.raw_path.decode("ascii"),
            headers,
            body,
        )
        return httpx.Response(status_code, headers=resp_headers, content=content, request=request)

    async def aclose(self) -> None:
        """Router 关闭自身 HTTP 客户端时不应停止独立维护的远端 worker。"""
        return

    def shutdown(self) -> None:
        self._stack.close()


@pytest.fixture
def worker_cluster() -> Iterator[_IndependentWorkerCluster]:
    """确保测试中创建的全部独立远端 worker 在测试结束时统一退出。"""
    cluster = _IndependentWorkerCluster()
    try:
        yield cluster
    finally:
        cluster.shutdown()


def _build_router(
    harness: _RedisHarness,
    tmp_path: Path,
    workers: dict[str, FastAPI],
    *,
    cluster: _IndependentWorkerCluster | None = None,
    upstream_urls: tuple[str, ...] | None = None,
    config_file: str | None = None,
    use_source_dir: bool = True,
    admin_api_key: str | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    urls = (
        tuple(f"http://{host}" for host in workers) if upstream_urls is None and config_file is None else (upstream_urls or ())
    )
    active_transport = transport
    if active_transport is None:
        assert cluster is not None
        active_transport = cluster.register(workers)
    return create_app(
        RouterSettings(
            upstream_urls=urls,
            config_file=config_file,
            local_gpus="none",
            redis_url=harness.url,
            redis_namespace=harness.namespace,
            source_dir=str(tmp_path / "sources") if use_source_dir else None,
            worker_refresh_interval_seconds=0,
            admin_api_key=admin_api_key,
        ),
        transport=active_transport,
        persistence_client=harness.new_client(),
    )


def test_restart_recovers_tasks_files_scopes_usage_and_counters(
    redis_harness: _RedisHarness,
    worker_cluster: _IndependentWorkerCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """在途任务跨重启保留调用方隔离、活跃计数、产物下载及累计用量。"""
    release = threading.Event()
    original_parse = api_server.parse_async

    async def gated_parse(*args: Any, **kwargs: Any) -> Any:
        while not release.is_set():
            await asyncio.sleep(0.01)
        return await original_parse(*args, **kwargs)

    monkeypatch.setattr(api_server, "parse_async", gated_parse)
    worker = api_server.create_app(upload_dir=str(tmp_path / "worker-a"), tiers=("flash",))
    headers = {"Authorization": "Bearer caller-one"}

    app1 = _build_router(redis_harness, tmp_path, {"worker-a": worker}, cluster=worker_cluster)
    with TestClient(app1) as client:
        task = _submit(client, [("report.html", HTML)], headers=headers)
        _upload(client, task, [("report.html", HTML)], headers=headers)
        task_id = task["task_id"]
        deadline = time.monotonic() + 5
        while not app1.state.registry.list("job"):
            assert time.monotonic() < deadline
            time.sleep(0.01)
        # 响应返回与后台建单后，快照已实时落盘。
        raw = redis_harness.get_raw(app1.state.persistence.state_key)
        assert raw is not None
        saved = json.loads(raw)
        assert saved["registry"]["routes"][0]["state"]["reservation_active"] is True
        assert saved["tasks"][0]["id"] == task_id

    # Router 下线期间放行 worker 完成实际解析。
    release.set()
    app2 = _build_router(redis_harness, tmp_path, {"worker-a": worker}, cluster=worker_cluster)
    with TestClient(app2) as client:
        assert client.get(f"/v1/tasks/{task_id}").status_code == 404
        assert client.get(f"/v1/tasks/{task_id}", headers={"Authorization": "Bearer stranger"}).status_code == 404
        done = _terminal(client, task, headers=headers)
        assert done["status"] == "completed"
        result_url = done["files"][0]["result_url"]
        file_id = done["files"][0]["file_id"]
        assert client.get(f"/v1/files/{file_id}").status_code == 404
        meta = client.get(f"/v1/files/{file_id}", headers=headers)
        assert meta.status_code == 200
        assert meta.json()["id"] == file_id
        downloaded = client.get(result_url, headers=headers)
        assert downloaded.status_code == 200
        with zipfile.ZipFile(io.BytesIO(downloaded.content)) as archive:
            assert b"Hello" in archive.read("markdown.md") or archive.namelist()
        assert all(w.active_jobs == 0 for w in app2.state.worker_pool.workers)
        usage = client.get("/v1/usage", headers=headers).json()["current"]
        assert usage["jobs_created"] == 1
        assert usage["files_processed"] == 1


def test_restart_recovers_pending_upload_and_source_cache_hit(
    redis_harness: _RedisHarness,
    worker_cluster: _IndependentWorkerCluster,
    tmp_path: Path,
) -> None:
    """待上传任务在重启后仍可完成 PUT，且持久化源目录支持跨重启哈希免传复用。"""
    worker = api_server.create_app(upload_dir=str(tmp_path / "worker-a"), tiers=("flash",))
    headers = {"Authorization": "Bearer uploader"}

    app1 = _build_router(redis_harness, tmp_path, {"worker-a": worker}, cluster=worker_cluster)
    with TestClient(app1) as client:
        created = client.post(
            "/v1/tasks",
            headers=headers,
            json={
                "tier": "flash",
                "files": [
                    {
                        "file_name": "pending.html",
                        "bytes": len(HTML),
                        "sha256sum": hashlib.sha256(HTML).hexdigest(),
                    }
                ],
            },
        )
        assert created.status_code == 202, created.text
        task = created.json()
        assert len(pending_files(task)) == 1

    # 重启后再执行 PUT 上传，验证未上传任务跨重启可继续投递。
    app2 = _build_router(redis_harness, tmp_path, {"worker-a": worker}, cluster=worker_cluster)
    with TestClient(app2) as client:
        _upload(client, task, [("pending.html", HTML)], headers=headers)
        assert _terminal(client, task, headers=headers)["status"] == "completed"

    # 再次重启后提交相同 SHA256，验证 --source-dir 恢复了调用方隔离的源缓存。
    app3 = _build_router(redis_harness, tmp_path, {"worker-a": worker}, cluster=worker_cluster)
    with TestClient(app3) as client:
        cached = client.post(
            "/v1/tasks",
            headers=headers,
            json={
                "tier": "flash",
                "files": [
                    {
                        "file_name": "reused.html",
                        "bytes": len(HTML),
                        "sha256sum": hashlib.sha256(HTML).hexdigest(),
                    }
                ],
            },
        )
        assert cached.status_code == 202, cached.text
        assert pending_files(cached.json()) == []
        assert _terminal(client, cached.json(), headers=headers)["status"] == "completed"
        # 其他调用方不能命中 uploader 的源缓存。
        stranger_task = client.post(
            "/v1/tasks",
            headers={"Authorization": "Bearer other"},
            json={
                "tier": "flash",
                "files": [
                    {
                        "file_name": "reused.html",
                        "bytes": len(HTML),
                        "sha256sum": hashlib.sha256(HTML).hexdigest(),
                    }
                ],
            },
        )
        assert len(pending_files(stranger_task.json())) == 1


@pytest.mark.parametrize("use_source_dir", [True, False])
def test_restart_handles_staged_unsubmitted_upload_boundary(
    redis_harness: _RedisHarness,
    worker_cluster: _IndependentWorkerCluster,
    tmp_path: Path,
    use_source_dir: bool,
) -> None:
    """PUT 已受理但尚未向 worker 建单即重启：有 --source-dir 自动补交，无则明确报告 source_file_unavailable。"""
    worker = api_server.create_app(upload_dir=str(tmp_path / "worker-a"), tiers=("flash",))
    base_transport = worker_cluster.register({"worker-a": worker})
    block_worker_post = True

    class _BlockingTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            if block_worker_post and request.method == "POST" and request.url.path == "/v1/tasks":
                await asyncio.sleep(60)
            return await base_transport.handle_async_request(request)

    app1 = _build_router(
        redis_harness,
        tmp_path,
        {"worker-a": worker},
        use_source_dir=use_source_dir,
        transport=_BlockingTransport(),
    )
    with TestClient(app1) as client:
        task = _submit(client, [("staged.html", HTML)])
        _upload(client, task, [("staged.html", HTML)])
        assert not app1.state.registry.list("job")

    block_worker_post = False
    app2 = _build_router(
        redis_harness,
        tmp_path,
        {"worker-a": worker},
        use_source_dir=use_source_dir,
        transport=base_transport,
    )
    with TestClient(app2) as client:
        done = _terminal(client, task)
        if use_source_dir:
            assert done["status"] == "completed", done
        else:
            assert done["status"] == "failed", done
            assert done["files"][0]["error"]["code"] == "source_file_unavailable"


def test_restart_preserves_inflight_task_cancellation(
    redis_harness: _RedisHarness,
    worker_cluster: _IndependentWorkerCluster,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """运行中任务在 Router 重启后仍可通过 /cancel 取消下游作业并归还负载。"""
    started = threading.Event()
    release = threading.Event()
    original_parse = api_server.parse_async

    async def slow_parse(*args: Any, **kwargs: Any) -> Any:
        started.set()
        while not release.is_set():
            await asyncio.sleep(0.02)
        return await original_parse(*args, **kwargs)

    monkeypatch.setattr(api_server, "parse_async", slow_parse)
    worker = api_server.create_app(upload_dir=str(tmp_path / "worker-a"), tiers=("flash",))
    app1 = _build_router(redis_harness, tmp_path, {"worker-a": worker}, cluster=worker_cluster)
    with TestClient(app1) as client:
        task = _submit(client, [("cancel-me.html", HTML)])
        _upload(client, task, [("cancel-me.html", HTML)])
        assert started.wait(timeout=5)

    try:
        app2 = _build_router(redis_harness, tmp_path, {"worker-a": worker}, cluster=worker_cluster)
        with TestClient(app2) as client:
            assert sum(w.active_jobs for w in app2.state.worker_pool.workers) == 1
            canceled = client.post(task_url(client, task) + "/cancel")
            assert canceled.status_code == 200, canceled.text
            assert canceled.json()["status"] == "canceled"
            release.set()
            deadline = time.monotonic() + 5
            while sum(w.active_jobs for w in app2.state.worker_pool.workers) != 0:
                assert time.monotonic() < deadline
                time.sleep(0.01)
            file_id = task["files"][0]["file_id"]
            assert client.get(f"/v1/files/{file_id}").status_code == 404
    finally:
        release.set()


def test_admin_dynamic_upstreams_and_drained_owners_persist_across_restart(
    redis_harness: _RedisHarness,
    worker_cluster: _IndependentWorkerCluster,
    tmp_path: Path,
) -> None:
    """动态注册节点、YAML 轮换凭证与已移除 draining 节点均跨重启保留各自语义。"""
    config = tmp_path / "router.yaml"
    config.write_text(
        yaml.safe_dump({"upstreams": [{"id": "yaml-a", "url": "http://yaml-a", "api_key": "old-secret"}]}),
        encoding="utf-8",
    )
    workers = {
        name: api_server.create_app(
            upload_dir=str(tmp_path / name),
            tiers=("flash",),
            api_key="rotated-secret" if name == "yaml-a" else None,
        )
        for name in ("yaml-a", "dyn-active", "dyn-drained")
    }
    base_transport = worker_cluster.register(workers)
    seen_yaml_auth: list[str] = []
    accept_old_key = True

    class _AuthTrackingTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            if request.url.host == "yaml-a":
                auth = request.headers.get("authorization", "")
                seen_yaml_auth.append(auth)
                if accept_old_key and auth == "Bearer old-secret":
                    patched_headers = [
                        (k, b"Bearer rotated-secret" if k.lower() == b"authorization" else v) for k, v in request.headers.raw
                    ]
                    request = httpx.Request(
                        request.method,
                        request.url,
                        headers=patched_headers,
                        stream=request.stream,
                        extensions=request.extensions,
                    )
            return await base_transport.handle_async_request(request)

    transport = _AuthTrackingTransport()
    admin_headers = {"Authorization": "Bearer admin-secret"}
    caller_headers = {"Authorization": "Bearer caller"}

    def make_app() -> FastAPI:
        return _build_router(
            redis_harness,
            tmp_path,
            workers,
            upstream_urls=(),
            config_file=str(config),
            admin_api_key="admin-secret",
            transport=transport,
        )

    with TestClient(make_app()) as client:
        task = _submit(client, [("doc.html", HTML)], headers=caller_headers)
        _upload(client, task, [("doc.html", HTML)], headers=caller_headers)
        done = _terminal(client, task, headers=caller_headers)
        assert done["status"] == "completed"

        # 注册两个动态节点，并将其中一个置为 draining（先挂一个文件引用以保持 draining）。
        r1 = client.post("/v1/router/workers", json={"url": "http://dyn-active"}, headers=admin_headers)
        r2 = client.post("/v1/router/workers", json={"url": "http://dyn-drained"}, headers=admin_headers)
        assert r1.status_code == 201 and r2.status_code == 201
        drained_id = r2.json()["worker_id"]
        client.app.state.registry.register_file(  # type: ignore[union-attr]
            owner_scope="anonymous",
            worker_id=drained_id,
            upstream_id="held-file",
        )
        assert client.delete(f"/v1/router/workers/{drained_id}", headers=admin_headers).status_code == 202

        # 轮换 YAML 凭证，随后清空 YAML 使 yaml-a 进入 draining。
        accept_old_key = False
        config.write_text(
            yaml.safe_dump({"upstreams": [{"id": "yaml-a", "url": "http://yaml-a", "api_key": "rotated-secret"}]}),
            encoding="utf-8",
        )
        client.portal.call(client.app.state.reload_upstreams)  # type: ignore[union-attr]
        config.write_text("upstreams: []\n", encoding="utf-8")
        client.portal.call(client.app.state.reload_upstreams)  # type: ignore[union-attr]

    seen_yaml_auth.clear()
    restored = make_app()
    with TestClient(restored) as client:
        # 动态注册的 active 节点在重启后仍保持 active，draining 节点保持 draining。
        listed = {item["url"]: item for item in client.get("/v1/router/workers", headers=admin_headers).json()["data"]}
        assert listed["http://dyn-active"]["state"] == "active"
        assert listed["http://dyn-active"]["healthy"] is True
        assert listed["http://dyn-drained"]["state"] == "draining"
        assert listed["http://yaml-a"]["state"] == "draining"

        # 已从 YAML 移除但仍保留轮换后凭证的 yaml-a 可正常提供旧结果下载。
        file_id = done["files"][0]["file_id"]
        downloaded = client.get(f"/v1/files/{file_id}/content", headers=caller_headers)
        assert downloaded.status_code == 200
        assert seen_yaml_auth and set(seen_yaml_auth) == {"Bearer rotated-secret"}


def test_single_owner_renewal_and_stale_writer_fencing(redis_harness: _RedisHarness) -> None:
    """单活租约阻止并发实例启动，自动续期，并在租约被接管后禁止旧实例覆盖状态或误删新租约。"""

    async def exercise() -> None:
        first = RedisStateStore(
            redis_harness.url,
            redis_harness.namespace,
            lease_seconds=1,
            client=redis_harness.new_client(),
        )
        second = RedisStateStore(
            redis_harness.url,
            redis_harness.namespace,
            lease_seconds=1,
            client=redis_harness.new_client(),
        )
        try:
            assert await first.start() is None
            with pytest.raises(RuntimeError, match="Another router"):
                await second.start()
            await asyncio.sleep(1.2)
            await first.assert_owner()
            await first.save(lambda: {"version": 1, "value": "original"})
            # 模拟租约过期后被新实例接管。
            redis_harness.set_raw(first.lease_key, "new-owner", ex=60)
            with pytest.raises(RouterProxyError) as caught:
                await first.save(lambda: {"version": 1, "value": "stale"})
            assert caught.value.status_code == 503
            assert caught.value.code == "router_state_unavailable"
            raw = redis_harness.get_raw(first.state_key)
            assert raw is not None and json.loads(raw)["value"] == "original"
            await first.close()
            assert redis_harness.get_raw(first.lease_key) == b"new-owner"
        finally:
            await first.close()
            await second.close()

    asyncio.run(exercise())


def test_lost_lease_fails_closed_before_proxy(redis_harness: _RedisHarness, tmp_path: Path) -> None:
    """租约丢失后请求在转发到 upstream 前即返回 503 router_state_unavailable。"""
    calls: list[str] = []
    upstream = _FakeV1Upstream("worker-a", ("standard",))

    async def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return await upstream.handle(request)

    app = _build_router(
        redis_harness,
        tmp_path,
        {},
        upstream_urls=("http://worker-a",),
        transport=httpx.MockTransport(handle),
    )
    with TestClient(app) as client:
        calls.clear()
        redis_harness.set_raw(app.state.persistence.lease_key, "new-owner", ex=60)
        result = client.post("/v1/tasks", json={"tier": "standard", "files": [{"file_name": "a.pdf", "bytes": 3}]})
        assert result.status_code == 503
        assert result.json()["error"]["code"] == "router_state_unavailable"
        assert calls == []


def test_corrupt_snapshot_is_not_overwritten_and_releases_lease(
    redis_harness: _RedisHarness,
    tmp_path: Path,
) -> None:
    """损坏快照拒绝启动，不覆盖原始 Redis 数据，并在退出时释放租约。"""
    upstream = _FakeV1Upstream("worker-a", ("standard",))
    app = _build_router(
        redis_harness,
        tmp_path,
        {},
        upstream_urls=("http://worker-a",),
        transport=httpx.MockTransport(upstream.handle),
    )
    corrupt = b'{"version":1,"registry":{}}'
    redis_harness.set_raw(app.state.persistence.state_key, corrupt)
    with pytest.raises(RuntimeError, match="Invalid router Redis snapshot"):
        with TestClient(app):
            pass
    assert redis_harness.get_raw(app.state.persistence.state_key) == corrupt
    assert redis_harness.get_raw(app.state.persistence.lease_key) is None


def test_checkpoint_failure_does_not_acknowledge_success_or_leak_streams(
    redis_harness: _RedisHarness,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """快照写入失败时不向客户端返回 2xx，且流式下载中断仍归还节点请求引用。"""
    upstream = _FakeV1Upstream("worker-a", ("standard",))

    async def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/files/out-1/content":
            return httpx.Response(200, stream=httpx.ByteStream(b"zip-bytes"), request=request)
        return await upstream.handle(request)

    app = _build_router(
        redis_harness,
        tmp_path,
        {},
        upstream_urls=("http://worker-a",),
        transport=httpx.MockTransport(handle),
    )
    with TestClient(app) as client:
        worker = app.state.worker_pool.workers[0]
        file_route = app.state.registry.register_file(
            owner_scope="anonymous",
            worker_id=worker.worker_id,
            upstream_id="out-1",
        )

        async def fail_eval(*args: Any, **kwargs: Any) -> int:
            raise RedisTransportError("simulated unavailable Redis")

        monkeypatch.setattr(app.state.persistence.client, "eval", fail_eval)
        task_resp = client.post("/v1/tasks", json={"tier": "standard", "files": [{"file_name": "a.pdf", "bytes": 3}]})
        assert task_resp.status_code == 503
        assert task_resp.json()["error"]["code"] == "router_state_unavailable"

        stream_resp = client.get(f"/v1/files/{file_route.public_id}/content")
        assert stream_resp.status_code == 503
        assert stream_resp.json()["error"]["code"] == "router_state_unavailable"
        assert worker.active_requests == 0
        monkeypatch.undo()


def test_missing_redis_dependency_reports_clear_install_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    """未安装可选 redis 依赖时提示安装 mineru[router]。"""
    monkeypatch.setitem(sys.modules, "redis", None)
    monkeypatch.setitem(sys.modules, "redis.asyncio", None)
    monkeypatch.setitem(sys.modules, "redis.exceptions", None)
    with pytest.raises(RuntimeError, match=r"mineru\[router\]"):
        create_app(RouterSettings(redis_url="redis://localhost:6379/0", local_gpus="none"))


def test_sigkill_restart_uses_acknowledged_mapping(tmp_path: Path) -> None:
    """真实多进程 SIGKILL 强杀与租约过期后，新进程从已确认快照恢复任务与结果下载。"""
    import socket
    import subprocess

    url = os.getenv("MINERU_TEST_REDIS_URL")
    if not url:
        pytest.skip("Set MINERU_TEST_REDIS_URL to run real multi-process Redis SIGKILL recovery test")
    if os.name != "posix":
        pytest.skip("Process crash test requires POSIX file descriptor inheritance")
    import redis

    namespace = "sigkill-" + uuid.uuid4().hex
    script = """
import os
import httpx
import uvicorn
from mineru.kit.router import RouterSettings, create_app

def backend(request):
    path = request.url.path
    if path == "/v1/health":
        data = {"status": "ok", "version": "test", "features": {"sources": ["url"], "task_output_formats": ["default"]}}
    elif path == "/v1/tiers":
        data = {"object": "list", "data": [{"id": "standard", "description": "std"}]}
    elif path == "/v1/models":
        data = {"object": "list", "data": []}
    elif path == "/v1/usage":
        data = {"object": "usage", "current": {}}
    elif path == "/v1/tasks" and request.method == "POST":
        return httpx.Response(202, json={
            "task_id": "backend-task",
            "status": "waiting-file",
            "files": [{"file_id": "backend-output", "file_name": "input.pdf", "status": "waiting-file"}],
        }, request=request)
    elif path == "/v1/tasks/backend-task":
        data = {
            "task_id": "backend-task",
            "status": "completed",
            "created_at": "2026-10-10T00:00:00Z",
            "finished_at": "2026-10-10T00:00:01Z",
            "output_formats": ["default"],
            "progress": {"completed": 1, "failed": 0, "total": 1},
            "files": [{
                "file_id": "backend-output",
                "file_name": "input.pdf",
                "status": "completed",
                "bytes": 16,
                "result_url": "http://backend/v1/files/backend-output/content",
            }],
        }
    elif path == "/v1/files/backend-output":
        data = {
            "id": "backend-output",
            "object": "file",
            "bytes": 16,
            "created_at": 1,
            "file_name": "input.zip",
            "purpose": "parse_output",
            "sha256sum": "a" * 64,
        }
    elif path == "/v1/files/backend-output/content":
        return httpx.Response(200, content=b"recovered-result", request=request)
    else:
        return httpx.Response(404, json={"error": {"code": "not_found", "message": "missing"}}, request=request)
    return httpx.Response(200, json=data, request=request)

app = create_app(RouterSettings(
    upstream_urls=("http://backend",), local_gpus="none",
    redis_url=os.environ["MINERU_TEST_REDIS_URL"], redis_namespace=os.environ["TEST_ROUTER_NAMESPACE"],
    worker_refresh_interval_seconds=0,
), transport=httpx.MockTransport(backend))
app.state.persistence.lease_seconds = 1
uvicorn.run(app, fd=int(os.environ["TEST_ROUTER_FD"]), log_level="warning")
"""
    ext = redis.Redis.from_url(url)
    try:
        with socket.socket() as listener, (tmp_path / "router-process.log").open("w+") as log:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
            env = {
                **os.environ,
                "PYTHONPATH": str(Path.cwd()),
                "MINERU_TEST_REDIS_URL": url,
                "TEST_ROUTER_NAMESPACE": namespace,
                "TEST_ROUTER_FD": str(listener.fileno()),
            }
            processes: list[subprocess.Popen[str]] = []

            def start() -> subprocess.Popen[str]:
                process = subprocess.Popen(
                    [sys.executable, "-c", script],
                    env=env,
                    pass_fds=(listener.fileno(),),
                    stdout=log,
                    stderr=log,
                    text=True,
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
                        "/v1/tasks",
                        json={
                            "tier": "standard",
                            "files": [{"file_name": "input.pdf", "url": "https://example.invalid/input.pdf"}],
                        },
                    )
                    assert result.status_code == 202, result.text
                    task_id = result.json()["task_id"]
                first.kill()
                first.wait(timeout=5)
                time.sleep(1.2)
                start()
                with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=5, headers=headers) as client:
                    deadline = time.monotonic() + 5
                    while True:
                        polled = client.get(f"/v1/tasks/{task_id}")
                        assert polled.status_code == 200, polled.text
                        body = polled.json()
                        if body["status"] == "completed":
                            break
                        assert time.monotonic() < deadline
                        time.sleep(0.05)
                    result_url = body["files"][0]["result_url"]
                    assert client.get(result_url).content == b"recovered-result"
            finally:
                for process in processes:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=5)
    finally:
        keys = list(ext.scan_iter(f"mineru-router:{{{namespace}}}:*"))
        if keys:
            ext.delete(*keys)
        ext.close()


def test_legacy_file_parse_with_redis_keeps_state_empty_and_fences_lease(
    redis_harness: _RedisHarness,
    worker_cluster: _IndependentWorkerCluster,
    tmp_path: Path,
) -> None:
    """同步 `/file_parse` 受单活租约保护，且不向 Redis 快照写入临时任务或文件路由。"""
    worker = api_server.create_app(upload_dir=str(tmp_path / "worker-a"), tiers=("flash",))
    app = _build_router(redis_harness, tmp_path, {"worker-a": worker}, cluster=worker_cluster)
    with TestClient(app) as client:
        response = client.post("/file_parse", files={"files": ("document.html", HTML, "text/html")})
        assert response.status_code == 200, response.text
        assert "Shared tasks" in response.json()["results"]["document"]["md_content"]
        raw = redis_harness.get_raw(app.state.persistence.state_key)
        assert raw is not None
        snapshot = json.loads(raw)
        assert snapshot["registry"]["routes"] == []
        assert snapshot["tasks"] == []
        assert all(w.active_jobs == 0 for w in app.state.worker_pool.workers)

        redis_harness.set_raw(app.state.persistence.lease_key, "another-owner", ex=30)
        fenced = client.post("/file_parse", files={"files": ("document.html", HTML, "text/html")})
        assert fenced.status_code == 503
        assert fenced.json()["error"]["code"] == "router_state_unavailable"
