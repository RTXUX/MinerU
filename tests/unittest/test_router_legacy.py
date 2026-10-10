from __future__ import annotations

import asyncio
import gzip
import io
import json
import zipfile
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi import Request
from fastapi.testclient import TestClient
from starlette.datastructures import UploadFile

from mineru.kit.router import RouterSettings, create_app
from mineru.parser.api_server import create_app as create_api_server
from test_v1_router import _FakeV1Upstream


async def multipart(request: httpx.Request) -> list[tuple[str, Any]]:
    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": await request.aread(), "more_body": False}

    incoming = Request({"type": "http", "headers": [(name.lower(), value) for name, value in request.headers.raw]}, receive)
    async with incoming.form() as form:
        return [
            (key, (value.filename, await value.read()) if isinstance(value, UploadFile) else value)
            for key, value in form.multi_items()
        ]


def test_routes_by_tier_preserves_fields_and_upstream_credentials(tmp_path: Path) -> None:
    config = tmp_path / "router.yaml"
    config.write_text(
        "upstreams:\n  - id: standard\n    url: http://standard\n    api_key: backend-secret\n"
        "  - id: basic\n    url: http://basic\n"
    )
    upstreams = {name: _FakeV1Upstream(name, (tier,)) for name, tier in (("standard", "standard"), ("basic", "basic"))}
    seen = []

    async def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path != "/file_parse":
            return await upstreams[request.url.host].handle(request)
        seen.append((request.url.host, request.headers.get("authorization"), await multipart(request)))
        assert request.extensions["timeout"]["read"] is None
        return httpx.Response(200, json={"backend": "pipeline", "results": {"document": {"md_content": "hello"}}})

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
                ("files", ("two.pdf", b"two", "application/pdf")),
                ("lang_list", (None, "en")),
                ("lang_list", (None, "ch")),
                ("backend", (None, "pipeline")),
                ("tier", (None, "standard")),
                ("return_images", (None, "true")),
            ],
        )
        assert result.status_code == 200
        assert result.json()["results"]["document"]["md_content"] == "hello"
        assert seen[-1] == (
            "standard",
            "Bearer backend-secret",
            [
                ("files", ("one.pdf", b"one")),
                ("files", ("two.pdf", b"two")),
                ("lang_list", "en"),
                ("lang_list", "ch"),
                ("backend", "pipeline"),
                ("return_images", "true"),
                ("tier", "standard"),
            ],
        )
        result = client.post(
            "/file_parse",
            data={"backend": "pipeline"},
            files={"files": ("a.pdf", b"a")},
            headers={"authorization": "Bearer caller"},
        )
        assert result.status_code == 200
        assert seen[-1][0:2] == ("basic", "Bearer caller")
        assert sum(worker.active_jobs for worker in app.state.worker_pool.workers) == 0
        assert app.state.registry.list("job") == []
        assert app.state.registry.list("file") == []
        schema = client.get("/openapi.json").json()["paths"]["/file_parse"]["post"]
        assert "multipart/form-data" in schema["requestBody"]["content"]


@pytest.mark.parametrize(
    "data,expected",
    [
        ({}, 200),
        ({"backend": "flash"}, 200),
        ({"tier": "advanced"}, 503),
        ({"tier": "invalid"}, 400),
        ({"backend": "invalid"}, 400),
    ],
)
def test_default_flash_and_invalid_routing(data: dict[str, str], expected: int) -> None:
    upstream = _FakeV1Upstream("flash", ("flash",))
    calls = []

    async def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/file_parse":
            calls.append(await multipart(request))
            return httpx.Response(200, json={"results": {}})
        return await upstream.handle(request)

    app = create_app(
        RouterSettings(upstream_urls=("http://flash",), local_gpus="none", worker_refresh_interval_seconds=0),
        transport=httpx.MockTransport(handle),
    )
    with TestClient(app) as client:
        result = client.post("/file_parse", data=data, files={"files": ("test.pdf", b"pdf")})
        assert result.status_code == expected
        assert len(calls) == (1 if expected == 200 else 0)
        if calls:
            assert ("tier", "flash") in calls[0]
        assert client.post("/file_parse").status_code == 400
        assert (
            client.post("/file_parse", files=[("files", ("a.pdf", b"pdf")), ("tier", ("tier.txt", b"flash"))]).status_code
            == 400
        )


@pytest.mark.parametrize(
    "mode,status", [("zip", 200), ("gzip", 200), ("error", 401), ("missing", 404), ("timeout", 504), ("connection", 502)]
)
def test_streams_zip_and_preserves_errors(mode: str, status: int) -> None:
    upstream = _FakeV1Upstream("standard", ("standard",))
    payload = b"zip-output" if mode == "zip" else b'{"error":{"message":"backend response"}}'
    responses = []

    async def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path != "/file_parse":
            return await upstream.handle(request)
        if mode == "timeout":
            raise httpx.WriteTimeout("timeout")
        if mode == "connection":
            raise httpx.ConnectError("disconnected")
        headers = {"content-type": "application/zip" if mode == "zip" else "application/json"}
        content = payload
        if mode == "zip":
            headers["content-disposition"] = 'attachment; filename="results.zip"'
        if mode == "gzip":
            headers["content-encoding"] = "gzip"
            content = gzip.compress(payload)
        response = httpx.Response(status, headers=headers, stream=httpx.ByteStream(content))
        responses.append(response)
        return response

    app = create_app(
        RouterSettings(upstream_urls=("http://standard",), local_gpus="none", worker_refresh_interval_seconds=0),
        transport=httpx.MockTransport(handle),
    )
    with TestClient(app) as client:
        result = client.post("/file_parse", files={"files": ("test.pdf", b"pdf")})
        assert result.status_code == status
        if responses:
            assert result.content == payload
            assert responses[0].is_closed
        if mode == "zip":
            assert result.headers["content-disposition"] == 'attachment; filename="results.zip"'
            assert result.headers["content-type"] == "application/zip"
        assert sum(worker.active_jobs for worker in app.state.worker_pool.workers) == 0


def test_concurrent_requests_balance_and_release_after_cancellation() -> None:
    async def exercise() -> None:
        upstreams = {name: _FakeV1Upstream(name, ("standard",)) for name in ("a", "b")}
        entered: list[str] = []
        both = asyncio.Event()

        async def handle(request: httpx.Request) -> httpx.Response:
            if request.url.path != "/file_parse":
                return await upstreams[request.url.host].handle(request)
            entered.append(request.url.host)
            if len(entered) == 2:
                both.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

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

    asyncio.run(exercise())


def test_real_api_adapter_json_zip_and_validation(tmp_path: Path) -> None:
    async def exercise() -> None:
        backend = create_api_server(tier="flash", api_key="secret", upload_dir=str(tmp_path))
        router = create_app(
            RouterSettings(upstream_urls=("http://backend",), local_gpus="none", worker_refresh_interval_seconds=0),
            transport=httpx.ASGITransport(backend),
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
                assert backend.state.job_store._jobs == {}
                assert router.state.registry.list("job") == []

    asyncio.run(exercise())
