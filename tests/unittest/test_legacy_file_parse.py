"""Regression coverage for the synchronous multipart compatibility boundary."""

from __future__ import annotations

import asyncio
import base64
import io
import json
import tempfile
import zipfile
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from mineru.parser import api_server, legacy_api
from mineru.parser.base import ParseResult
from mineru.types import ImageBlock, ImageBodyBlock, MiddleJson, ModelJson, PageInfo
from mineru.version import __version__


def _result() -> ParseResult:
    return ParseResult(
        MiddleJson(
            pages=[PageInfo(page_idx=0, blocks=[])],
            is_full_document=True,
            metadata={"file_suffix": "pdf", "producer": {"name": "mineru", "version": __version__}},
        )
    )


def test_native_html_round_trip(tmp_path: Path) -> None:
    """Exercise real parsing/rendering and the legacy JSON-string convention."""
    app = api_server.create_app(tier="flash", upload_dir=str(tmp_path))
    with TestClient(app) as client:
        response = client.post(
            "/file_parse",
            files={"files": ("hello.html", b"<h1>Hello adapter</h1><p>Native parsing works.</p>", "text/html")},
            data={"return_middle_json": "true", "return_content_list": "true", "return_images": "true"},
        )
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload["version"] == __version__
        assert payload["backend"] == "hybrid-auto-engine"
        result = payload["results"]["hello"]
        assert "Hello adapter" in result["md_content"]
        assert json.loads(result["middle_json"])["schema"] == "docvortex.middle"
        assert isinstance(json.loads(result["content_list"]), list)
        assert result["images"] == {}
        assert client.get("/v1/parse/jobs").json()["data"] == []
        assert client.get("/v1/files").json()["data"] == []
        assert client.post("/tasks").status_code == 404


def test_flags_duplicate_names_and_zip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async def parse(*args: object, **kwargs: object) -> ParseResult:
        return _result()

    monkeypatch.setattr(api_server, "parse_async", parse)
    app = api_server.create_app(tier="flash", upload_dir=str(tmp_path))
    uploads = [("files", (name, b"pdf", "application/pdf")) for name in ("../../same.pdf", "C:\\same.pdf", "same_2.pdf")]
    with TestClient(app) as client:
        response = client.post("/file_parse", files=uploads, data={"return_md": "false", "return_model_output": "true"})
        assert response.status_code == 200, response.text
        assert response.json()["results"] == {
            "same": {"model_output": None},
            "same_2": {"model_output": None},
            "same_2_2": {"model_output": None},
        }
        response = client.post(
            "/file_parse",
            files=uploads[:1],
            data={
                "response_format_zip": "true",
                "return_md": "false",
                "return_middle_json": "true",
                "return_original_file": "true",
            },
        )
        assert response.status_code == 200, response.text
        assert response.headers["content-type"] == "application/zip"
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            assert set(archive.namelist()) == {"same/auto/same_middle.json", "same/auto/same_origin.pdf"}
            assert archive.read("same/auto/same_origin.pdf") == b"pdf"


def test_images_and_model_output(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from PIL import Image

    buffer = io.BytesIO()
    Image.new("RGB", (2, 2), "red").save(buffer, format="PNG")
    image_bytes = buffer.getvalue()
    data_uri = "data:image/png;base64," + base64.b64encode(image_bytes).decode("ascii")
    result = _result()
    result.middle_json.pages[0].blocks = [
        ImageBlock(
            type="image",
            index=0,
            bbox=(0.0, 0.0, 1.0, 1.0),
            content=[ImageBodyBlock(type="image_body", index=0, bbox=(0.0, 0.0, 1.0, 1.0), content="", image_base64=data_uri)],
        )
    ]
    model = ModelJson(
        pages=[[{"raw": "model"}]],
        page_index_map=[],
        metadata={"file_suffix": "pdf", "producer": {"name": "mineru", "version": __version__}},
    )
    result = ParseResult(result.middle_json, _model_output=model)

    async def parse(*args: object, **kwargs: object) -> ParseResult:
        return result

    monkeypatch.setattr(api_server, "parse_async", parse)
    app = api_server.create_app(tier="flash", upload_dir=str(tmp_path))
    with TestClient(app) as client:
        response = client.post(
            "/file_parse",
            files={"files": ("figure.pdf", b"pdf")},
            data={"return_images": "true", "return_model_output": "true", "return_content_list": "true"},
        )
        assert response.status_code == 200, response.text
        document = response.json()["results"]["figure"]
        assert json.loads(document["model_output"]) == model.to_dict(skip_defaults=False)
        assert len(document["images"]) == 1
        name, encoded = next(iter(document["images"].items()))
        assert base64.b64decode(encoded.split(",", 1)[1]) == image_bytes
        assert f"images/{name}" in document["md_content"]
        assert f"images/{name}" in document["content_list"]
        assert result.pages[0].blocks[0].content[0].image_base64 == data_uri


@pytest.mark.parametrize(
    ("data", "status"),
    [
        ({"parse_method": "invalid"}, 400),
        ({"start_page_id": "-1"}, 400),
        ({"start_page_id": "3", "end_page_id": "2"}, 400),
        ({"return_md": "invalid"}, 400),
        ({"backend": "unknown"}, 400),
        ({"backend": "pipeline"}, 400),  # basic is unavailable on a Flash server
        ({"tier": "standard"}, 400),
        ({"server_url": "http://unconfigured.invalid"}, 400),
        ({"client_side_output_generation": "true"}, 400),
    ],
)
def test_invalid_options(data: dict[str, str], status: int, tmp_path: Path) -> None:
    app = api_server.create_app(tier="flash", upload_dir=str(tmp_path))
    with TestClient(app) as client:
        response = client.post("/file_parse", files={"files": ("a.pdf", b"pdf")}, data=data)
        assert response.status_code == status, response.text
        assert app.state.job_store._jobs == {}


def test_auth_empty_and_size_limits(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app = api_server.create_app(tier="flash", upload_dir=str(tmp_path), api_key="test-key")
    monkeypatch.setattr(legacy_api, "_MAX_FILE_SIZE_BYTES_DEFAULT", 4)
    with TestClient(app) as client:
        assert client.post("/file_parse", files={"files": ("a.pdf", b"pdf")}).status_code == 401
        client.headers["Authorization"] = "Bearer test-key"
        assert client.post("/file_parse").status_code == 400
        assert client.post("/file_parse", files={"files": ("a.pdf", b"")}).status_code == 400
        assert client.post("/file_parse", files={"files": ("a.pdf", b"12345")}).status_code == 413


def test_mapping_and_page_selection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    async def parse(*args: object, **kwargs: Any) -> ParseResult:
        calls.append(kwargs)
        return _result()

    monkeypatch.setattr(api_server, "parse_async", parse)
    monkeypatch.setattr(api_server, "ensure_tier_runtime_dependencies", lambda *args, **kwargs: None)
    app = api_server.create_app(tier="standard", upload_dir=str(tmp_path))
    with TestClient(app) as client:
        response = client.post(
            "/file_parse",
            files={"files": ("a.pdf", b"pdf")},
            data={"backend": "pipeline", "parse_method": "txt", "start_page_id": "1", "end_page_id": "3"},
        )
        assert response.status_code == 200, response.text
        assert calls[0]["tier"] == "basic"
        assert calls[0]["ocr_mode"] == "txt"
        assert calls[0]["page_range"] == "2-4"


def test_failure_cleans_transient_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    roots: list[Path] = []
    original_directory = tempfile.TemporaryDirectory

    def temporary_directory(*args: Any, **kwargs: Any) -> tempfile.TemporaryDirectory:
        directory = original_directory(*args, **kwargs)
        roots.append(Path(directory.name))
        return directory

    async def fail(*args: object, **kwargs: object) -> ParseResult:
        raise ValueError("broken input")

    monkeypatch.setattr(legacy_api.tempfile, "TemporaryDirectory", temporary_directory)
    monkeypatch.setattr(api_server, "parse_async", fail)
    app = api_server.create_app(tier="flash", upload_dir=str(tmp_path))
    with TestClient(app) as client:
        response = client.post("/file_parse", files={"files": ("a.pdf", b"pdf")})
        assert response.status_code == 409, response.text
        assert response.json()["errors"]["a.pdf"]["message"] == "broken input"
        assert app.state.job_store._jobs == {}
        assert app.state.job_store._tasks == {}
        assert roots and all(not root.exists() for root in roots)


def test_concurrency_shared_with_v1_and_cancellation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async def exercise() -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def parse(*args: object, **kwargs: object) -> ParseResult:
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return _result()

        monkeypatch.setattr(api_server, "parse_async", parse)
        app = api_server.create_app(tier="flash", upload_dir=str(tmp_path), concurrency=1)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/v1/parse/jobs",
                json={"tier": "flash", "files": [{"source": {"type": "inline", "name": "v1.pdf", "data": "cGRm"}}]},
            )
            assert response.status_code == 202
            await asyncio.wait_for(started.wait(), 5)
            legacy_request = asyncio.create_task(client.post("/file_parse", files={"files": ("old.pdf", b"pdf")}))
            for _ in range(100):
                if len(app.state.job_store._jobs) == 2:
                    break
                await asyncio.sleep(0.01)
            assert len(app.state.job_store._jobs) == 2
            assert calls == 1
            legacy_request.cancel()
            with pytest.raises(asyncio.CancelledError):
                await legacy_request
            assert len(app.state.job_store._jobs) == 1
            release.set()
            await app.state.job_store._tasks[response.json()["job_id"]]
            assert calls == 1

    asyncio.run(exercise())


@pytest.mark.parametrize("shutdown", [False, True])
def test_running_request_cancellation_drains_before_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, shutdown: bool
) -> None:
    async def exercise() -> None:
        started = asyncio.Event()
        cleaned = asyncio.Event()
        source_paths: list[Path] = []

        async def parse(path: str, **kwargs: object) -> ParseResult:
            source_paths.append(Path(path))
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0.01)
                assert source_paths[0].read_bytes() == b"pdf"
                cleaned.set()
            return _result()

        monkeypatch.setattr(api_server, "parse_async", parse)
        app = api_server.create_app(tier="flash", upload_dir=str(tmp_path))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            pending = asyncio.create_task(client.post("/file_parse", files={"files": ("a.pdf", b"pdf")}))
            await asyncio.wait_for(started.wait(), 5)
            if shutdown:
                await app.state.job_store.shutdown()
                response = await pending
                assert response.status_code == 503, response.text
            else:
                pending.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await pending
            assert cleaned.is_set()
            assert all(not path.exists() for path in source_paths)
            assert app.state.job_store._jobs == {}
            assert app.state.job_store._tasks == {}

    asyncio.run(exercise())
