# Copyright (c) Opendatalab. All rights reserved.
"""验证 Router 旧版同步 `/file_parse` 兼容接口对下游 V1 `/v1/tasks` 的适配、负载调度与清理边界。"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import zipfile
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import httpx
import pytest
from fastapi.testclient import TestClient

from mineru.kit.router import RouterSettings, create_app, legacy
from mineru.parser import api_server
from mineru.parser.base import ParseResult
from mineru.types import MiddleJson, PageInfo
from mineru.version import __version__


def _sample_middle_json() -> MiddleJson:
    """构造最小合法 2.0 MiddleJson 供归档转换与 Content List 渲染测试复用。"""
    return MiddleJson(
        pages=[PageInfo(page_idx=0, blocks=[])],
        is_full_document=True,
        metadata={"file_suffix": "pdf", "producer": {"name": "mineru", "version": __version__}},
    )


def _result_archive(*, markdown: str = "# Router Tasks adapter\n\nConverted from V1 task output.") -> bytes:
    """构造符合 V1 `default` 契约的内存 ZIP 归档。"""
    exported = ParseResult(_sample_middle_json())
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("markdown.md", markdown.encode("utf-8"))
        archive.writestr("middle_json.json", exported.to_json().encode("utf-8"))
        archive.writestr("structured_content.json", json.dumps(exported.structured_content()).encode("utf-8"))
        archive.writestr("images/", b"")
        archive.writestr("images/figure.png", b"png-bytes")
    return buffer.getvalue()


@dataclass
class _TasksOnlyUpstream:
    """仅暴露 V1 `/v1/tasks` 与结果下载的受控测试上游；若收到 `/file_parse` 立即报错。"""

    name: str
    tiers: tuple[str, ...]
    features: dict[str, Any] = field(
        default_factory=lambda: {
            "sources": ["upload", "url"],
            "task_output_formats": ["default", "docx", "html", "latex"],
        }
    )
    tasks: dict[str, dict[str, Any]] = field(default_factory=dict)
    uploads: dict[str, bytes] = field(default_factory=dict)
    cache_by_hash: set[str] = field(default_factory=set)
    task_counter: int = 0
    upload_counter: int = 0
    leases: dict[str, int] = field(default_factory=dict)

    async def handle(self, request: httpx.Request) -> httpx.Response:
        """实现最小高保真 V1 Tasks 状态机，验证创建、上传、轮询、取消与租约释放。"""
        path = request.url.path
        assert path != "/file_parse", "Router must adapt /file_parse over /v1/tasks"
        assert not path.startswith(("/v1/uploads", "/v1/parse/jobs")), "Removed 4.0 endpoints must not be used"

        lease_header = request.headers.get("x-mineru-task-lease-seconds")
        base = f"{request.url.scheme}://{request.url.netloc.decode('ascii')}"

        if path == "/v1/health":
            return httpx.Response(
                200,
                json={"status": "ok", "version": __version__, "features": self.features},
                request=request,
            )
        if path == "/v1/tiers":
            return httpx.Response(
                200,
                json={
                    "object": "list",
                    "data": [{"id": tier, "description": f"{self.name}-{tier}", "current_model": tier} for tier in self.tiers],
                },
                request=request,
            )
        if path == "/v1/models":
            return httpx.Response(
                200,
                json={"object": "list", "data": [{"id": f"{self.name}-model", "object": "model", "created": 1}]},
                request=request,
            )
        if path == "/v1/tasks" and request.method == "POST":
            body = json.loads(request.content)
            self.task_counter += 1
            task_id = f"job_{self.name}_{self.task_counter}"
            file_id = f"file-{self.name}-{self.task_counter}"
            spec = body["files"][0]
            sha = spec.get("sha256sum")
            cached = bool(sha and sha in self.cache_by_hash)
            status = "pending" if cached else "waiting-file"
            record: dict[str, Any] = {
                "task_id": task_id,
                "file_id": file_id,
                "file_name": spec["file_name"],
                "bytes": spec["bytes"],
                "sha256sum": sha,
                "body": body,
                "status": status,
                "archive": _result_archive(),
                "error": None,
            }
            self.tasks[task_id] = record
            if lease_header is not None:
                self.leases[task_id] = int(lease_header)
            created_file: dict[str, Any] = {
                "file_id": file_id,
                "file_name": spec["file_name"],
                "status": status,
            }
            if not cached:
                created_file["upload"] = {
                    "url": f"{base}/v1/tasks/{task_id}/uploads/{file_id}",
                    "headers": {"Content-Type": "application/octet-stream"},
                    "expires_at": 4102444800,
                }
            return httpx.Response(
                202,
                json={"task_id": task_id, "status": status, "files": [created_file]},
                request=request,
            )

        parts = [unquote(part) for part in path.strip("/").split("/")]
        if len(parts) == 5 and parts[:2] == ["v1", "tasks"] and parts[3] == "uploads" and request.method == "PUT":
            task_id, file_id = parts[2], parts[4]
            record = self.tasks[task_id]
            assert record["file_id"] == file_id
            payload = await request.aread()
            assert len(payload) == record["bytes"]
            if record["sha256sum"] is not None:
                assert hashlib.sha256(payload).hexdigest() == record["sha256sum"]
                self.cache_by_hash.add(record["sha256sum"])
            self.upload_counter += 1
            self.uploads[file_id] = payload
            record["status"] = "pending"
            if lease_header is not None:
                self.leases[task_id] = int(lease_header)
            return httpx.Response(
                200,
                json={"task_id": task_id, "file_id": file_id, "status": "pending"},
                request=request,
            )

        if len(parts) == 3 and parts[:2] == ["v1", "tasks"] and request.method == "GET":
            task_id = parts[2]
            if task_id not in self.tasks:
                return httpx.Response(
                    404,
                    json={"error": {"code": "task_not_found", "message": "Task not found"}},
                    request=request,
                )
            record = self.tasks[task_id]
            if lease_header is not None:
                self.leases[task_id] = int(lease_header)
            if record["status"] == "pending" and lease_header != "0":
                record["status"] = "completed"
            file_detail: dict[str, Any] = {
                "file_id": record["file_id"],
                "file_name": record["file_name"],
                "bytes": record["bytes"],
                "status": record["status"],
            }
            if record["status"] == "waiting-file":
                file_detail["upload"] = {
                    "url": f"{base}/v1/tasks/{task_id}/uploads/{record['file_id']}",
                    "headers": {"Content-Type": "application/octet-stream"},
                    "expires_at": 4102444800,
                }
            elif record["status"] == "completed":
                file_detail["parse"] = {
                    "tier": record["body"].get("tier", "flash"),
                    "duration_ms": 5,
                    "parser_version": __version__,
                }
                file_detail["result_url"] = f"{base}/v1/files/{record['file_id']}/content"
            elif record["error"] is not None:
                file_detail["error"] = record["error"]
            return httpx.Response(
                200,
                json={
                    "task_id": task_id,
                    "status": record["status"],
                    "created_at": "2026-11-20T00:00:00Z",
                    "finished_at": "2026-11-20T00:00:01Z" if record["status"] in {"completed", "failed", "canceled"} else None,
                    "output_formats": record["body"]["output_formats"],
                    "progress": {
                        "completed": int(record["status"] == "completed"),
                        "failed": int(record["status"] == "failed"),
                        "total": 1,
                    },
                    "files": [file_detail],
                },
                request=request,
            )

        if len(parts) == 4 and parts[:2] == ["v1", "tasks"] and parts[3] == "cancel" and request.method == "POST":
            task_id = parts[2]
            if task_id not in self.tasks:
                return httpx.Response(
                    404,
                    json={"error": {"code": "task_not_found", "message": "Task not found"}},
                    request=request,
                )
            record = self.tasks[task_id]
            if record["status"] in {"completed", "failed", "canceled"}:
                return httpx.Response(
                    409,
                    json={"error": {"code": "task_already_terminal", "message": "Already terminal"}},
                    request=request,
                )
            record["status"] = "canceled"
            return httpx.Response(
                200,
                json={
                    "task_id": task_id,
                    "status": "canceled",
                    "finished_at": "2026-11-20T00:00:01Z",
                    "files": [{"file_id": record["file_id"], "file_name": record["file_name"], "status": "canceled"}],
                },
                request=request,
            )

        if len(parts) == 4 and parts[:2] == ["v1", "files"] and parts[3] == "content" and request.method == "GET":
            file_id = parts[2]
            record = next((item for item in self.tasks.values() if item["file_id"] == file_id), None)
            if record is None or record["status"] != "completed":
                return httpx.Response(
                    404,
                    json={"error": {"code": "file_not_found", "message": "Result not found"}},
                    request=request,
                )
            return httpx.Response(
                200,
                content=record["archive"],
                headers={"content-type": "application/zip"},
                request=request,
            )

        return httpx.Response(
            404,
            json={"error": {"code": "not_found", "message": f"Unhandled {request.method} {path}"}},
            request=request,
        )


def _make_router(*upstreams: _TasksOnlyUpstream) -> Any:
    """按 hostname 分发到多个仅支持 `/v1/tasks` 的测试上游。"""
    by_host = {upstream.name: upstream for upstream in upstreams}

    async def handle(request: httpx.Request) -> httpx.Response:
        return await by_host[request.url.host].handle(request)

    return create_app(
        RouterSettings(
            upstream_urls=tuple(f"http://{upstream.name}" for upstream in upstreams),
            local_gpus="none",
            worker_refresh_interval_seconds=0,
        ),
        transport=httpx.MockTransport(handle),
    )


def test_internal_tasks_workflow_tiers_auth_and_options(tmp_path: Path) -> None:
    """验证混合批次、3.x/4.0 backend 映射、显式 tier 覆盖、认证头隔离与租约释放。"""
    config = tmp_path / "router.yaml"
    config.write_text(
        "upstreams:\n"
        "  - id: standard\n"
        "    url: http://standard\n"
        "    api_key: backend-secret\n"
        "  - id: basic\n"
        "    url: http://basic\n",
        encoding="utf-8",
    )
    upstreams = {name: _TasksOnlyUpstream(name, (name,)) for name in ("standard", "basic")}
    seen: list[tuple[str, str, str, str | None, str | None]] = []

    async def handle(request: httpx.Request) -> httpx.Response:
        seen.append(
            (
                request.url.host,
                request.method,
                request.url.path,
                request.headers.get("authorization"),
                request.headers.get("content-type"),
            )
        )
        if request.method in {"GET", "POST"} and request.url.path != "/v1/tasks":
            assert "multipart/form-data" not in (request.headers.get("content-type") or "")
        return await upstreams[request.url.host].handle(request)

    app = create_app(
        RouterSettings(config_file=str(config), local_gpus="none", worker_refresh_interval_seconds=0),
        transport=httpx.MockTransport(handle),
    )
    with TestClient(app) as client:
        schema = client.get("/openapi.json").json()
        assert "/file_parse" in schema["paths"]
        assert "/v1/file_parse" not in schema["paths"]

        result = client.post(
            "/file_parse",
            headers={"authorization": "Bearer caller"},
            files=[
                ("files", ("one.pdf", b"pdf-bytes", "application/pdf")),
                ("files", ("two.html", b"<h1>two</h1>", "text/html")),
                ("lang_list", (None, "en")),
                ("lang_list", (None, "ch")),
            ],
            data={
                "backend": "pipeline",
                "effort": "high",
                "tier": "standard",
                "parse_method": "txt",
                "start_page_id": "1",
                "end_page_id": "3",
                "formula_enable": "true",
                "table_enable": "true",
                "image_analysis": "false",
                "return_images": "true",
            },
        )
        assert result.status_code == 200, result.text
        payload = result.json()
        assert payload["backend"] == "pipeline"
        assert payload["version"] == __version__
        assert set(payload["results"]) == {"one", "two"}
        assert "Router Tasks adapter" in payload["results"]["one"]["md_content"]
        assert payload["results"]["one"]["images"]["figure.png"].startswith("data:image/png;base64,")

        pdf_task = next(iter(upstreams["standard"].tasks.values()))["body"]
        assert pdf_task["tier"] == "standard"
        assert pdf_task["ocr_mode"] == "txt"
        assert pdf_task["output_formats"] == ["default"]
        assert pdf_task["files"][0]["page_range"] == "2-4"
        html_task = next(
            item["body"]
            for upstream in upstreams.values()
            for item in upstream.tasks.values()
            if item["file_name"] == "two.html"
        )
        assert "tier" not in html_task
        assert "page_range" not in html_task["files"][0]

        assert {auth for host, _, path, auth, _ in seen if host == "standard" and path.startswith("/v1/tasks")} == {
            "Bearer backend-secret"
        }
        assert all(lease == 0 for upstream in upstreams.values() for lease in upstream.leases.values())

        for legacy_backend, expected_host in (
            ("pipeline", "basic"),
            ("hybrid-engine", "standard"),
            ("vlm-engine", "standard"),
            ("hybrid-auto-engine", "standard"),
        ):
            res = client.post(
                "/file_parse",
                data={"backend": legacy_backend},
                files={"files": ("doc.pdf", b"pdf-content")},
                headers={"authorization": "Bearer caller"},
            )
            assert res.status_code == 200, res.text
            assert res.json()["backend"] == legacy_backend
            task_hosts = [host for host, method, path, _, _ in seen if method == "POST" and path == "/v1/tasks"]
            assert task_hosts[-1] == expected_host

        assert sum(worker.active_jobs for worker in app.state.worker_pool.workers) == 0
        assert sum(worker.active_requests for worker in app.state.worker_pool.workers) == 0
        assert app.state.registry.list("job") == []
        assert app.state.registry.list("file") == []
        assert not app.state.task_store._records


@pytest.mark.parametrize(
    ("data", "filename", "expected"),
    [
        ({}, "test.pdf", 200),
        ({"backend": "flash"}, "test.pdf", 200),
        ({"tier": "advanced"}, "native.html", 200),
        ({"tier": "advanced"}, "test.pdf", 503),
        ({"tier": "invalid"}, "test.pdf", 400),
        ({"backend": "invalid"}, "test.pdf", 400),
        ({"parse_method": "invalid"}, "test.pdf", 400),
        ({"return_md": "invalid"}, "test.pdf", 400),
        ({"start_page_id": "-1"}, "test.pdf", 400),
        ({"start_page_id": "3", "end_page_id": "2"}, "test.pdf", 400),
        ({"server_url": "http://invalid.local"}, "test.pdf", 400),
        ({"client_side_output_generation": "true"}, "test.pdf", 400),
        ({}, "unsupported.txt", 400),
        ({}, "noextension", 400),
    ],
)
def test_validation_and_tier_boundaries(data: dict[str, str], filename: str, expected: int) -> None:
    """非法输入与不可用档位在创建任何下游任务或上传前立即拒绝。"""
    upstream = _TasksOnlyUpstream("flash", ("flash",))
    app = _make_router(upstream)
    with TestClient(app) as client:
        result = client.post("/file_parse", data=data, files={"files": (filename, b"content")})
        assert result.status_code == expected, result.text
        assert upstream.task_counter == (1 if expected == 200 else 0)
        if expected != 200:
            assert upstream.upload_counter == 0
        assert client.post("/file_parse").status_code == 400
        assert client.post("/file_parse", json={"files": []}).status_code == 400
        assert (
            client.post("/file_parse", files=[("files", ("a.pdf", b"pdf")), ("tier", ("tier.txt", b"flash"))]).status_code
            == 400
        )


def test_mixed_batch_preflight_rejects_before_creating_partial_tasks() -> None:
    """混合批次若任一文件缺少可用档位，不得提前为原生文件创建下游任务。"""
    upstream = _TasksOnlyUpstream("flash", ("flash",))
    with TestClient(_make_router(upstream)) as client:
        response = client.post(
            "/file_parse",
            data={"tier": "advanced"},
            files=[("files", ("first.html", b"<p>ok</p>")), ("files", ("second.pdf", b"%PDF-1.4"))],
        )
        assert response.status_code == 503
        assert upstream.task_counter == 0
        assert upstream.upload_counter == 0


def test_zip_flags_duplicate_names_colon_sanitization_and_originals() -> None:
    """验证重复文件名去重、特殊字符清洗、原文件打包以及 model_output/content_list 转换。"""
    upstream = _TasksOnlyUpstream("flash", ("flash",))
    with TestClient(_make_router(upstream)) as client:
        uploads = [
            ("files", ("../../same.pdf", b"first")),
            ("files", ("C:\\fakepath\\same.pdf", b"second")),
            ("files", ("report:2026.pdf", b"third")),
        ]
        response = client.post(
            "/file_parse",
            files=uploads,
            data={
                "response_format_zip": "true",
                "return_original_file": "true",
                "return_md": "false",
                "return_middle_json": "true",
                "return_images": "true",
            },
        )
        assert response.status_code == 200, response.text
        assert response.headers["content-type"] == "application/zip"
        assert response.headers["content-disposition"] == 'attachment; filename="results.zip"'
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            assert set(archive.namelist()) == {
                "same/auto/same_middle.json",
                "same/auto/same_origin.pdf",
                "same/auto/images/figure.png",
                "same_2/auto/same_2_middle.json",
                "same_2/auto/same_2_origin.pdf",
                "same_2/auto/images/figure.png",
                "report_2026/auto/report_2026_middle.json",
                "report_2026/auto/report_2026_origin.pdf",
                "report_2026/auto/images/figure.png",
            }
            assert archive.read("same/auto/same_origin.pdf") == b"first"
            assert archive.read("same_2/auto/same_2_origin.pdf") == b"second"
            assert archive.read("report_2026/auto/report_2026_origin.pdf") == b"third"

        # 第二次提交相同字节命中下游 SHA-256 缓存，无需再次 PUT。
        previous_uploads = upstream.upload_counter
        json_resp = client.post(
            "/file_parse",
            files=uploads[:2],
            data={"return_md": "false", "return_model_output": "true", "return_content_list": "true"},
        )
        assert json_resp.status_code == 200, json_resp.text
        assert upstream.upload_counter == previous_uploads
        assert json_resp.json()["results"]["same"] == {"model_output": None, "content_list": "[]"}


@pytest.mark.parametrize(
    ("mode", "status"),
    [
        ("auth", 401),
        ("missing", 404),
        ("busy", 429),
        ("timeout", 504),
        ("connection", 502),
    ],
)
def test_upstream_errors_are_propagated_and_release_reservations(mode: str, status: int) -> None:
    """下游认证、限流、404 与传输超时按 V1 错误契约透传，并立即归还并发名额。"""
    upstream = _TasksOnlyUpstream("standard", ("standard",))
    original = upstream.handle

    async def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/tasks" and request.method == "POST":
            if mode == "timeout":
                raise httpx.WriteTimeout("timeout", request=request)
            if mode == "connection":
                raise httpx.ConnectError("disconnected", request=request)
            return httpx.Response(
                status,
                json={"error": {"code": f"err_{mode}", "message": "backend response"}},
                request=request,
            )
        return await original(request)

    upstream.handle = handle  # type: ignore[method-assign]
    app = _make_router(upstream)
    with TestClient(app) as client:
        response = client.post("/file_parse", files={"files": ("test.pdf", b"pdf")})
        assert response.status_code == status
        if status < 500:
            assert response.json() == {"error": {"code": f"err_{mode}", "message": "backend response"}}
        assert all(worker.active_jobs == 0 for worker in app.state.worker_pool.workers)


@pytest.mark.parametrize("failure", ["put-error", "put-timeout", "poll-lost", "corrupt-archive", "worker-replaced"])
def test_midflight_failures_cancel_and_clean_private_tasks(failure: str) -> None:
    """创建任务后的上传失败、任务丢失、损坏归档或 worker 重建必须释放预占与取消任务。"""
    upstream = _TasksOnlyUpstream("standard", ("standard",))
    original = upstream.handle
    app = _make_router(upstream)

    async def handle(request: httpx.Request) -> httpx.Response:
        if failure == "put-error" and request.method == "PUT":
            return httpx.Response(
                500,
                json={"error": {"code": "storage_failed", "message": "disk full"}},
                request=request,
            )
        if failure == "put-timeout" and request.method == "PUT":
            raise httpx.ReadTimeout("upload timed out", request=request)
        if failure == "poll-lost" and request.method == "GET" and request.url.path.startswith("/v1/tasks/"):
            return httpx.Response(
                404,
                json={"error": {"code": "task_not_found", "message": "gone"}},
                request=request,
            )
        if failure == "corrupt-archive" and request.url.path.endswith("/content"):
            return httpx.Response(200, content=b"not-a-zip", request=request)
        if failure == "worker-replaced" and request.method == "PUT":
            response = await original(request)
            worker = app.state.worker_pool.workers[0]
            worker.generation += 1
            worker.active_jobs = 0
            return response
        return await original(request)

    upstream.handle = handle  # type: ignore[method-assign]
    with TestClient(app) as client:
        response = client.post("/file_parse", files={"files": ("test.pdf", b"pdf")})
        expected = {
            "put-error": 500,
            "put-timeout": 504,
            "poll-lost": 502,
            "corrupt-archive": 502,
            "worker-replaced": 502,
        }[failure]
        assert response.status_code == expected, response.text
        assert all(worker.active_jobs == 0 for worker in app.state.worker_pool.workers)
        if failure in {"put-error", "put-timeout"}:
            assert all(task["status"] == "canceled" for task in upstream.tasks.values())
            assert all(lease == 0 for lease in upstream.leases.values())


def test_cancellation_and_multi_file_load_balancing() -> None:
    """并发与多文件 `/file_parse` 均按文件均衡分配 worker，断连取消下游任务并释放租约。"""

    async def exercise() -> None:
        upstreams = {name: _TasksOnlyUpstream(name, ("standard",)) for name in ("a", "b")}
        entered: list[str] = []
        both = asyncio.Event()
        originals = {name: upstream.handle for name, upstream in upstreams.items()}

        async def handle(request: httpx.Request) -> httpx.Response:
            upstream = upstreams[request.url.host]
            if request.method == "GET" and request.url.path.startswith("/v1/tasks/"):
                if request.headers.get("x-mineru-task-lease-seconds") != "0":
                    entered.append(upstream.name)
                    if len(entered) == 2:
                        both.set()
                    await asyncio.Event().wait()
            return await originals[upstream.name](request)

        app = create_app(
            RouterSettings(upstream_urls=("http://a", "http://b"), local_gpus="none", worker_refresh_interval_seconds=0),
            transport=httpx.MockTransport(handle),
        )
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://router") as client:
                call = asyncio.create_task(
                    client.post(
                        "/file_parse",
                        files=[("files", ("first.pdf", b"one")), ("files", ("second.pdf", b"two"))],
                    )
                )
                try:
                    await asyncio.wait_for(both.wait(), 5)
                    assert set(entered) == {"a", "b"}
                    assert [worker.active_jobs for worker in app.state.worker_pool.workers] == [1, 1]
                finally:
                    call.cancel()
                    await asyncio.gather(call, return_exceptions=True)

                assert all(worker.active_jobs == 0 for worker in app.state.worker_pool.workers)
                assert all(task["status"] == "canceled" for upstream in upstreams.values() for task in upstream.tasks.values())
                assert all(lease == 0 for upstream in upstreams.values() for lease in upstream.leases.values())

    asyncio.run(exercise())


@pytest.mark.parametrize("status", ["failed", "canceled"])
def test_terminal_task_failures_keep_legacy_status_and_release_lease(status: str) -> None:
    """解析失败返回旧版 409 errors 结构，上游取消返回 503。"""
    upstream = _TasksOnlyUpstream("standard", ("standard",))
    original = upstream.handle

    async def handle(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT":
            response = await original(request)
            task = next(iter(upstream.tasks.values()))
            task["status"] = status
            task["error"] = {"code": "parse_failed", "message": "corrupt document"}
            return response
        return await original(request)

    upstream.handle = handle  # type: ignore[method-assign]
    with TestClient(_make_router(upstream)) as client:
        response = client.post("/file_parse", files={"files": ("broken.pdf", b"pdf")})
        assert response.status_code == (503 if status == "canceled" else 409)
        if status == "failed":
            assert response.json() == {
                "message": "Task execution failed",
                "errors": {"broken.pdf": {"code": "parse_failed", "message": "corrupt document"}},
            }
        assert all(lease == 0 for lease in upstream.leases.values())


def test_empty_oversized_and_capability_filtering(monkeypatch: pytest.MonkeyPatch) -> None:
    """空文件、超限文件以及不支持 upload/default 的节点在建单前被过滤或拒绝。"""
    standard = _TasksOnlyUpstream("standard", ("standard",))
    basic = _TasksOnlyUpstream("basic", ("basic",))
    app = _make_router(standard, basic)
    with TestClient(app) as client:
        assert client.post("/file_parse", files={"files": ("a.pdf", b"")}).status_code == 400
        monkeypatch.setattr(legacy, "_MAX_FILE_BYTES", 3)
        assert client.post("/file_parse", files={"files": ("a.pdf", b"four")}).status_code == 413
        monkeypatch.undo()

        # standard 缺少 default 输出格式时，默认选档自动落到具备完整能力的 basic。
        standard_worker = next(w for w in app.state.worker_pool.workers if w.base_url == "http://standard")
        standard_worker.features = {"sources": ["upload"], "task_output_formats": ["html"]}
        ok = client.post("/file_parse", files={"files": ("a.pdf", b"pdf")})
        assert ok.status_code == 200
        assert standard.task_counter == 0
        assert basic.task_counter == 1

        basic_worker = next(w for w in app.state.worker_pool.workers if w.base_url == "http://basic")
        basic_worker.features = {"sources": ["url"], "task_output_formats": ["default"]}
        assert client.post("/file_parse", files={"files": ("a.pdf", b"pdf")}).status_code == 503


def test_real_v1_worker_and_cascaded_router_end_to_end(tmp_path: Path) -> None:
    """真实 `api_server` worker 与双层 Router 级联下完成 JSON、ZIP、鉴权与坏文件 409 验证。"""

    async def exercise() -> None:
        worker_app = api_server.create_app(tiers=("flash",), api_key="secret", upload_dir=str(tmp_path / "worker"))
        worker_paths: list[str] = []

        async def traced_worker(scope: Any, receive: Any, send: Any) -> None:
            if scope["type"] == "http":
                worker_paths.append(scope["path"])
                assert scope["path"] != "/file_parse"
            await worker_app(scope, receive, send)

        inner_router = create_app(
            RouterSettings(upstream_urls=("http://worker",), local_gpus="none", worker_refresh_interval_seconds=0),
            transport=httpx.ASGITransport(traced_worker),
        )
        outer_router = create_app(
            RouterSettings(upstream_urls=("http://inner",), local_gpus="none", worker_refresh_interval_seconds=0),
            transport=httpx.ASGITransport(inner_router),
        )
        html = b"<h1>Real V1 worker</h1><p>Legacy file_parse wrapped over Tasks.</p>"

        async with AsyncExitStack() as stack:
            for service in (worker_app, inner_router, outer_router):
                await stack.enter_async_context(service.router.lifespan_context(service))
            client = await stack.enter_async_context(
                httpx.AsyncClient(transport=httpx.ASGITransport(outer_router), base_url="http://outer")
            )
            files = {"files": ("hello.html", html, "text/html")}
            assert (await client.post("/file_parse", files=files)).status_code == 401

            client.headers["authorization"] = "Bearer secret"
            json_res = await client.post(
                "/file_parse",
                files=files,
                data={"return_middle_json": "true", "return_content_list": "true"},
            )
            assert json_res.status_code == 200, json_res.text
            parsed = json_res.json()["results"]["hello"]
            assert "Real V1 worker" in parsed["md_content"]
            assert json.loads(parsed["middle_json"])["schema"] == "docvortex.middle"
            assert isinstance(json.loads(parsed["content_list"]), list)

            zip_res = await client.post(
                "/file_parse",
                files=files,
                data={
                    "response_format_zip": "true",
                    "return_original_file": "true",
                    "return_middle_json": "true",
                },
            )
            assert zip_res.status_code == 200
            with zipfile.ZipFile(io.BytesIO(zip_res.content)) as archive:
                assert archive.read("hello/auto/hello_origin.html") == html
                assert "Real V1 worker" in archive.read("hello/auto/hello.md").decode("utf-8")

            bad_pdf = await client.post("/file_parse", files={"files": ("corrupt.pdf", b"not-a-valid-pdf")})
            assert bad_pdf.status_code == 409
            assert "corrupt.pdf" in bad_pdf.json()["errors"]

            assert outer_router.state.registry.list("job") == []
            assert outer_router.state.registry.list("file") == []
            assert "/v1/tasks" in worker_paths
            assert "/file_parse" not in worker_paths

    asyncio.run(exercise())
