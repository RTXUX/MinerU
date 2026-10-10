from __future__ import annotations

import asyncio
import io
import json
import zipfile
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from mineru.kit.router import RouterSettings, create_app
from mineru.kit.router import legacy
from mineru.parser.api_server import create_app as create_api_server
from test_legacy_file_parse import _result
from test_v1_router import _FakeV1Upstream


def result_archive() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("markdown.md", "# Router V1 adapter\n\nA blue circle and seven green squares.")
        archive.writestr("middle_json.json", json.dumps(_result().middle_json.to_dict(skip_defaults=True)))
        archive.writestr("images/figure.png", b"image-data")
    return buffer.getvalue()


class V1OnlyUpstream(_FakeV1Upstream):
    """A server that fails the test immediately if its legacy endpoint is used."""

    async def handle(self, request: httpx.Request) -> httpx.Response:
        assert request.url.path != "/file_parse", "Router must adapt internally"
        response = await super().handle(request)
        if request.url.path == "/v1/health":
            data = response.json()
            data["features"]["output_formats"].append("zip")
            return httpx.Response(200, json=data)
        return response

    def _handle_job(self, request: httpx.Request) -> httpx.Response:
        response = super()._handle_job(request)
        for record in self.files.values():
            if record["purpose"] == "parse_output":
                record["content"] = result_archive()
        return response

    def _job_payload(self, job_id: str) -> dict[str, Any]:
        result = super()._job_payload(job_id)
        for item in result["files"]:
            if "output_files" in item:
                item["output_files"] = {"zip": item["output_files"]["markdown"], "markdown": None, "middle_json": None}
        return result


def make_router(*upstreams: V1OnlyUpstream) -> Any:
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


def test_internal_v1_workflow_tiers_auth_and_options(tmp_path: Path) -> None:
    config = tmp_path / "router.yaml"
    config.write_text(
        "upstreams:\n  - id: standard\n    url: http://standard\n    api_key: backend-secret\n"
        "  - id: basic\n    url: http://basic\n"
    )
    upstreams = {name: V1OnlyUpstream(name, (name,)) for name in ("standard", "basic")}
    seen = []

    async def handle(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.host, request.url.path, request.headers.get("authorization")))
        if request.method == "POST":
            assert "application/json" in request.headers["content-type"]
        return await upstreams[request.url.host].handle(request)

    app = create_app(
        RouterSettings(config_file=str(config), local_gpus="none", worker_refresh_interval_seconds=0),
        transport=httpx.MockTransport(handle),
    )
    with TestClient(app) as client:
        result = client.post(
            "/file_parse",
            headers={"authorization": "Bearer caller"},
            files=[
                ("files", ("one.pdf", b"one", "application/pdf")),
                ("files", ("two.html", b"two", "text/html")),
                ("lang_list", (None, "en")),
                ("lang_list", (None, "ch")),
            ],
            data={
                "backend": "pipeline",
                "tier": "standard",
                "parse_method": "txt",
                "start_page_id": "1",
                "end_page_id": "3",
                "return_images": "true",
            },
        )
        assert result.status_code == 200, result.text
        payload = result.json()
        assert payload["backend"] == "pipeline"
        assert set(payload["results"]) == {"one", "two"}
        assert "Router V1 adapter" in payload["results"]["one"]["md_content"]
        assert payload["results"]["one"]["images"]["figure.png"].startswith("data:image/png;base64,")
        job = next(iter(upstreams["standard"].jobs.values()))["body"]
        assert job["tier"] == "standard"
        assert job["ocr_mode"] == "txt"
        assert job["output_formats"] == ["zip"]
        assert job["files"][0]["page_range"] == "2-4"
        assert "page_range" not in job["files"][1]
        assert {auth for host, _, auth in seen if host == "standard"} == {"Bearer backend-secret"}
        assert upstreams["standard"].files == {}
        result = client.post(
            "/file_parse",
            data={"backend": "pipeline"},
            files={"files": ("a.pdf", b"a")},
            headers={"authorization": "Bearer caller"},
        )
        assert result.status_code == 200
        assert seen[-1][0::2] == ("basic", "Bearer caller")
        assert upstreams["basic"].files == {}
        assert sum(worker.active_jobs for worker in app.state.worker_pool.workers) == 0
        assert app.state.registry.list("job") == []
        assert app.state.registry.list("file") == []


@pytest.mark.parametrize(
    "data,expected",
    [
        ({}, 200),
        ({"backend": "flash"}, 200),
        ({"tier": "advanced"}, 503),
        ({"tier": "invalid"}, 400),
        ({"backend": "invalid"}, 400),
        ({"parse_method": "invalid"}, 400),
        ({"return_md": "invalid"}, 400),
        ({"start_page_id": "-1"}, 400),
        ({"start_page_id": "3", "end_page_id": "2"}, 400),
        ({"server_url": "http://invalid.local"}, 400),
        ({"client_side_output_generation": "true"}, 400),
    ],
)
def test_default_flash_and_invalid_options(data: dict[str, str], expected: int) -> None:
    upstream = V1OnlyUpstream("flash", ("flash",))
    app = make_router(upstream)
    with TestClient(app) as client:
        result = client.post("/file_parse", data=data, files={"files": ("test.pdf", b"pdf")})
        assert result.status_code == expected, result.text
        assert upstream.job_counter == (1 if expected == 200 else 0)
        if expected != 200:
            assert upstream.upload_counter == 0
        assert client.post("/file_parse").status_code == 400
        assert (
            client.post("/file_parse", files=[("files", ("a.pdf", b"pdf")), ("tier", ("tier.txt", b"flash"))]).status_code
            == 400
        )


def test_zip_flags_duplicate_names_and_originals() -> None:
    upstream = V1OnlyUpstream("flash", ("flash",))
    with TestClient(make_router(upstream)) as client:
        uploads = [("files", ("../../same.pdf", b"first")), ("files", ("C:\\same.pdf", b"second"))]
        response = client.post(
            "/file_parse",
            files=uploads,
            data={
                "response_format_zip": "true",
                "return_original_file": "true",
                "return_md": "false",
                "return_middle_json": "true",
            },
        )
        assert response.status_code == 200, response.text
        assert response.headers["content-type"] == "application/zip"
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            assert set(archive.namelist()) == {
                "same/auto/same_middle.json",
                "same/auto/same_origin.pdf",
                "same_2/auto/same_2_middle.json",
                "same_2/auto/same_2_origin.pdf",
            }
            assert archive.read("same/auto/same_origin.pdf") == b"first"
            assert archive.read("same_2/auto/same_2_origin.pdf") == b"second"
        response = client.post(
            "/file_parse",
            files=uploads,
            data={"return_md": "false", "return_model_output": "true", "return_content_list": "true"},
        )
        assert response.status_code == 200
        assert response.json()["results"]["same"] == {"model_output": None, "content_list": "[]"}
        assert upstream.files == {}


@pytest.mark.parametrize("mode,status", [("auth", 401), ("missing", 404), ("busy", 429), ("timeout", 504), ("connection", 502)])
def test_upstream_errors_are_propagated(mode: str, status: int) -> None:
    upstream = V1OnlyUpstream("standard", ("standard",))
    original = upstream.handle

    async def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/uploads":
            if mode == "timeout":
                raise httpx.WriteTimeout("timeout")
            if mode == "connection":
                raise httpx.ConnectError("disconnected")
            return httpx.Response(status, json={"error": {"message": "backend response"}})
        return await original(request)

    upstream.handle = handle
    app = make_router(upstream)
    with TestClient(app) as client:
        response = client.post("/file_parse", files={"files": ("test.pdf", b"pdf")})
        assert response.status_code == status
        if status < 500:
            assert response.json() == {"error": {"message": "backend response"}}
        assert all(worker.active_jobs == 0 for worker in app.state.worker_pool.workers)


def test_cancellation_balances_load_and_cleans_v1_resources() -> None:
    async def exercise() -> None:
        upstreams = {name: V1OnlyUpstream(name, ("standard",)) for name in ("a", "b")}
        entered: list[str] = []
        both = asyncio.Event()

        async def handle(request: httpx.Request) -> httpx.Response:
            upstream = upstreams[request.url.host]
            if request.method == "GET" and request.url.path.startswith("/v1/parse/jobs/"):
                job = upstream.jobs[request.url.path.rsplit("/", 1)[-1]]
                if job["status"] == "queued":
                    entered.append(upstream.name)
                    if len(entered) == 2:
                        both.set()
                    await asyncio.Event().wait()
            return await upstream.handle(request)

        app = create_app(
            RouterSettings(upstream_urls=("http://a", "http://b"), local_gpus="none", worker_refresh_interval_seconds=0),
            transport=httpx.MockTransport(handle),
        )
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://router") as client:
                tasks = [asyncio.create_task(client.post("/file_parse", files={"files": ("a.pdf", b"pdf")})) for _ in range(2)]
                try:
                    await asyncio.wait_for(both.wait(), 5)
                    assert set(entered) == {"a", "b"}
                    assert [worker.active_jobs for worker in app.state.worker_pool.workers] == [1, 1]
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                assert all(worker.active_jobs == 0 for worker in app.state.worker_pool.workers)
                assert all(upstream.files == {} for upstream in upstreams.values())
                assert all(job["status"] == "canceled" for upstream in upstreams.values() for job in upstream.jobs.values())

    asyncio.run(exercise())


@pytest.mark.parametrize("failure", ["put", "complete-response", "parse", "archive"])
def test_partial_failure_cleans_private_resources(failure: str) -> None:
    upstream = V1OnlyUpstream("standard", ("standard",))
    original = upstream.handle

    async def handle(request: httpx.Request) -> httpx.Response:
        if failure == "put" and request.method == "PUT":
            return httpx.Response(500)
        if failure == "complete-response" and request.url.path.endswith("/complete"):
            await original(request)
            raise httpx.ReadTimeout("lost completion")
        if failure == "parse" and request.url.path == "/v1/parse/jobs":
            return httpx.Response(400, json={"error": {"message": "rejected"}})
        if failure == "archive" and request.url.path.startswith("/v1/files/") and request.url.path.endswith("/content"):
            return httpx.Response(200, content=b"invalid zip")
        return await original(request)

    upstream.handle = handle
    with TestClient(make_router(upstream)) as client:
        response = client.post("/file_parse", files={"files": ("test.pdf", b"pdf")})
        assert response.status_code == {"put": 500, "complete-response": 504, "parse": 400, "archive": 502}[failure]
        assert upstream.files == {}
        assert all(upload["status"] != "pending" for upload in upstream.uploads.values())


def test_empty_oversized_and_missing_zip_capability(monkeypatch: pytest.MonkeyPatch) -> None:
    upstream = V1OnlyUpstream("standard", ("standard",))
    app = make_router(upstream)
    with TestClient(app) as client:
        assert client.post("/file_parse", files={"files": ("a.pdf", b"")}).status_code == 400
        monkeypatch.setattr(legacy, "_MAX_FILE_BYTES", 3)
        assert client.post("/file_parse", files={"files": ("a.pdf", b"four")}).status_code == 413
        app.state.worker_pool.workers[0].features["output_formats"] = ["markdown"]
        assert client.post("/file_parse", files={"files": ("a.pdf", b"pdf")}).status_code == 503
        assert upstream.upload_counter == 0


def test_real_v1_only_server_json_zip_and_validation(tmp_path: Path) -> None:
    async def exercise() -> None:
        backend = create_api_server(tier="flash", api_key="secret", upload_dir=str(tmp_path))
        calls = []

        async def v1_only(scope: Any, receive: Any, send: Any) -> None:
            calls.append(scope["path"])
            assert scope["path"] != "/file_parse", "Upstream has no compatibility adapter"
            await backend(scope, receive, send)

        router = create_app(
            RouterSettings(upstream_urls=("http://backend",), local_gpus="none", worker_refresh_interval_seconds=0),
            transport=httpx.ASGITransport(v1_only),
        )
        html = b"<h1>Router adapter test</h1><p>Native parsing works through the router.</p>"
        async with backend.router.lifespan_context(backend), router.router.lifespan_context(router):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(router), base_url="http://router") as client:
                files = {"files": ("hello.html", html, "text/html")}
                assert (await client.post("/file_parse", files=files)).status_code == 401
                client.headers["authorization"] = "Bearer secret"
                result = await client.post("/file_parse", files=files, data={"return_middle_json": "true"})
                assert result.status_code == 200, result.text
                parsed = result.json()["results"]["hello"]
                assert "Router adapter test" in parsed["md_content"]
                assert json.loads(parsed["middle_json"])["schema"] == "docvortex.middle"
                result = await client.post(
                    "/file_parse",
                    files=files,
                    data={
                        "response_format_zip": "true",
                        "return_original_file": "true",
                        "return_middle_json": "true",
                    },
                )
                assert result.status_code == 200
                with zipfile.ZipFile(io.BytesIO(result.content)) as archive:
                    assert archive.read("hello/auto/hello_origin.html") == html
                    assert "Router adapter test" in archive.read("hello/auto/hello.md").decode()
                invalid = await client.post("/file_parse", files=files, data={"parse_method": "invalid"})
                assert invalid.status_code == 400
                assert router.state.registry.list("job") == []
                assert (await client.get("/v1/files")).json()["data"] == []
                assert all(job.status == "completed" for job in backend.state.job_store._jobs.values())
                assert any(path == "/v1/parse/jobs" for path in calls)
                assert "/file_parse" not in calls

    asyncio.run(exercise())


@pytest.mark.parametrize("status", ["partial", "failed", "canceled"])
def test_terminal_job_failures_keep_legacy_status_and_cleanup(status: str) -> None:
    upstream = V1OnlyUpstream("standard", ("standard",))
    original = upstream.handle

    async def handle(request: httpx.Request) -> httpx.Response:
        response = await original(request)
        if request.method == "GET" and request.url.path.startswith("/v1/parse/jobs/"):
            payload = response.json()
            payload["status"] = status
            payload["files"][0]["name"] = "test.pdf"
            payload["files"][0]["error"] = {"code": "parse_failed", "message": "bad input"}
            return httpx.Response(200, json=payload)
        return response

    upstream.handle = handle
    with TestClient(make_router(upstream)) as client:
        response = client.post("/file_parse", files={"files": ("test.pdf", b"pdf")})
        assert response.status_code == (503 if status == "canceled" else 409)
        if status != "canceled":
            assert response.json() == {
                "message": "Task execution failed",
                "errors": {
                    "test.pdf": {"code": "parse_failed", "message": "bad input"},
                },
            }
        assert upstream.files == {}


def test_default_tier_considers_v1_upload_and_zip_capabilities() -> None:
    standard = V1OnlyUpstream("standard", ("standard",))
    basic = V1OnlyUpstream("basic", ("basic",))
    app = make_router(standard, basic)
    with TestClient(app) as client:
        worker = next(worker for worker in app.state.worker_pool.workers if worker.base_url == "http://standard")
        worker.features["output_formats"] = ["markdown"]
        response = client.post("/file_parse", files={"files": ("a.pdf", b"pdf")})
        assert response.status_code == 200
        assert standard.job_counter == 0
        assert basic.job_counter == 1
