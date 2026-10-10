"""Implement synchronous multipart compatibility using upstream V1 APIs only."""

from __future__ import annotations

import asyncio
import io
import json
import zipfile
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, Response
from loguru import logger
from pydantic import BaseModel, Field, ValidationError
from starlette.datastructures import UploadFile
from starlette.exceptions import HTTPException

from ...parser.legacy_api import _result_files, _upload_name
from ...parser.legacy_options import BACKEND_TIERS
from ...types import Tier, select_default_quality_tier
from ...utils.async_utils import drain_future, run_sync
from ...version import __version__
from .proxy import RouterProxyError, forwarded_headers, json_or_error, passthrough_response
from .workers import WorkerPool, WorkerState

_MAX_FILE_BYTES = 200 * 1024 * 1024
_POLL_INTERVAL = 0.25
_TERMINAL = {"completed", "partial", "failed", "canceled"}


class LegacyOptions(BaseModel):
    backend: str | None = None
    tier: Tier | None = None
    parse_method: Literal["auto", "txt", "ocr"] = "auto"
    start_page_id: int = Field(default=0, ge=0)
    end_page_id: int = Field(default=99999, ge=0)
    return_md: bool = True
    return_middle_json: bool = False
    return_model_output: bool = False
    return_content_list: bool = False
    return_images: bool = False
    response_format_zip: bool = False
    return_original_file: bool = False
    server_url: str | None = None
    client_side_output_generation: bool = False


class UpstreamResponseError(Exception):
    def __init__(self, response: httpx.Response) -> None:
        self.response = response


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RouterProxyError(502, "invalid_upstream_response", f"Invalid upstream {field}")
    return value


def _identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise RouterProxyError(502, "invalid_upstream_response", f"Upstream omitted {field}")
    return value


class LegacySession:
    """Track only this call's private V1 uploads, input/output files, and job."""

    def __init__(self, request: Request, pool: WorkerPool, worker: WorkerState) -> None:
        self.request = request
        self.pool = pool
        self.worker = worker
        self.generation = worker.generation
        self.base_url = worker.base_url
        self.headers = {**forwarded_headers(request), **worker.headers}
        for name in ("content-type", "content-encoding", "accept"):
            self.headers.pop(name, None)
        self.uploads: set[str] = set()
        self.files: set[str] = set()
        self.job_id: str | None = None
        self.terminal = False

    async def call(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        if self.worker.generation != self.generation:
            raise RouterProxyError(502, "upstream_unavailable", "Upstream worker was replaced")
        headers = {**self.headers, **kwargs.pop("headers", {})}
        try:
            response = await self.pool.client.request(method, self.base_url + path, headers=headers, **kwargs)
        except httpx.TimeoutException as exc:
            raise RouterProxyError(504, "upstream_timeout", "Upstream V1 request timed out") from exc
        except httpx.HTTPError as exc:
            raise RouterProxyError(502, "upstream_unavailable", "Upstream V1 request failed") from exc
        if response.status_code >= 400:
            raise UpstreamResponseError(response)
        return response

    async def upload(self, upload: UploadFile, name: str) -> str:
        created = json_or_error(
            await self.call(
                "POST",
                "/v1/uploads",
                json={
                    "filename": name,
                    "bytes": upload.size,
                    "mime_type": upload.content_type or "application/octet-stream",
                    "purpose": "parse",
                },
            )
        )
        upload_id = _identifier(created.get("id"), "upload id")
        self.uploads.add(upload_id)
        # No hash is supplied at creation: each request owns its own File record.
        if created.get("status") != "completed":

            async def chunks() -> AsyncIterator[bytes]:
                await upload.seek(0)
                while chunk := await upload.read(1024 * 1024):
                    yield chunk

            path = f"/v1/uploads/{quote(upload_id, safe='')}"
            await self.call("PUT", path + "/content", content=chunks(), headers={"content-type": "application/octet-stream"})
            created = json_or_error(await self.call("POST", path + "/complete", json={}))
        file_id = _identifier(_object(created.get("file"), "upload file").get("id"), "completed upload file id")
        self.files.add(file_id)
        self.uploads.discard(upload_id)
        return file_id

    def observe_job(self, job: dict[str, Any]) -> None:
        if job.get("status") not in _TERMINAL | {"queued", "running"}:
            raise RouterProxyError(502, "invalid_upstream_response", "Unknown upstream job status")
        self.terminal = job.get("status") in _TERMINAL
        items = job.get("files")
        if not isinstance(items, list):
            raise RouterProxyError(502, "invalid_upstream_response", "Invalid upstream job files")
        for item in items:
            outputs = _object(_object(item, "job file").get("output_files") or {}, "output files")
            for output in outputs.values():
                if output is None:
                    continue
                self.files.add(_identifier(_object(output, "output file").get("file_id"), "output file id"))

    async def parse(self, entries: list[dict[str, Any]], tier: Tier, method: str) -> dict[str, Any]:
        job = json_or_error(
            await self.call(
                "POST",
                "/v1/parse/jobs",
                json={
                    "files": entries,
                    "tier": tier,
                    "ocr_mode": method,
                    "output_formats": ["zip"],
                },
            )
        )
        self.job_id = _identifier(job.get("job_id"), "job id")
        while True:
            self.observe_job(job)
            if self.terminal:
                return job
            if await self.request.is_disconnected():
                raise RouterProxyError(499, "client_disconnected", "Client disconnected during parsing")
            await asyncio.sleep(_POLL_INTERVAL)
            job = json_or_error(await self.call("GET", f"/v1/parse/jobs/{quote(self.job_id, safe='')}"))

    async def cleanup(self) -> None:
        """Bound cleanup time; leave inputs intact if cancellation cannot be confirmed."""
        try:
            async with asyncio.timeout(10):
                if self.job_id is not None and not self.terminal:
                    path = f"/v1/parse/jobs/{quote(self.job_id, safe='')}"
                    try:
                        await self.call("DELETE", path)
                    except UpstreamResponseError as exc:
                        if exc.response.status_code not in {404, 409}:
                            raise
                    try:
                        job = json_or_error(await self.call("GET", path))
                        self.observe_job(job)
                        if not self.terminal:
                            logger.warning("Legacy job cleanup deferred: upstream is still active")
                            return
                    except UpstreamResponseError as exc:
                        if exc.response.status_code != 404:
                            raise
                # Lookup also recovers a File created by a /complete response that
                # was lost before the router learned its file id.
                for upload_id in self.uploads:
                    path = f"/v1/uploads/{quote(upload_id, safe='')}"
                    try:
                        upload = json_or_error(await self.call("GET", path))
                        file_id = _object(upload.get("file") or {}, "upload file").get("id")
                        if isinstance(file_id, str):
                            self.files.add(file_id)
                        elif upload.get("status") == "pending":
                            await self.call("POST", path + "/cancel", json={})
                    except (RouterProxyError, UpstreamResponseError):
                        logger.warning("Legacy upload cleanup failed")
                for file_id in self.files:
                    try:
                        await self.call("DELETE", f"/v1/files/{quote(file_id, safe='')}")
                    except (RouterProxyError, UpstreamResponseError):
                        logger.warning("Legacy file cleanup failed")
        except (TimeoutError, RouterProxyError, UpstreamResponseError):
            logger.warning("Legacy resource cleanup incomplete; upstream retention policy applies")


async def _render_response(
    session: LegacySession,
    job: dict[str, Any],
    names: list[str],
    uploads: list[UploadFile],
    options: LegacyOptions,
) -> Response:
    if job["status"] == "canceled":
        raise RouterProxyError(503, "parsing_canceled", "Parsing was canceled")
    if job["status"] != "completed":
        return JSONResponse(
            {
                "message": "Task execution failed",
                "errors": {
                    item.get("name", names[index] if index < len(names) else "unknown"): item["error"]
                    for index, item in enumerate(job.get("files") or [])
                    if item.get("error")
                },
            },
            status_code=409,
        )
    results: dict[str, Any] = {}
    parsed_files = job.get("files")
    if not isinstance(parsed_files, list) or len(parsed_files) != len(names):
        raise RouterProxyError(502, "invalid_upstream_response", "Upstream returned an unexpected file count")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, uploaded, parsed in zip(names, uploads, parsed_files):
            output_id = _identifier(((parsed.get("output_files") or {}).get("zip") or {}).get("file_id"), "ZIP output id")
            response = await session.call("GET", f"/v1/files/{quote(output_id, safe='')}/content")
            stem = Path(name).stem
            try:
                result, files = await run_sync(
                    _result_files,
                    response.content,
                    stem,
                    **{
                        key: getattr(options, key)
                        for key in (
                            "return_md",
                            "return_middle_json",
                            "return_model_output",
                            "return_content_list",
                            "return_images",
                        )
                    },
                )
            except (ValueError, KeyError, zipfile.BadZipFile):
                raise RouterProxyError(502, "invalid_upstream_response", "Invalid upstream result archive") from None
            results[stem] = result
            if options.response_format_zip:
                if options.return_original_file:
                    await uploaded.seek(0)
                    files[f"{stem}_origin{Path(name).suffix}"] = await uploaded.read()
                for filename, content in files.items():
                    await run_sync(archive.writestr, f"{stem}/{options.parse_method}/{filename}", content)
    if options.response_format_zip:
        return Response(
            buffer.getvalue(),
            media_type="application/zip",
            headers={"Content-Disposition": 'attachment; filename="results.zip"'},
        )
    return Response(
        json.dumps({"backend": options.backend or "hybrid-auto-engine", "version": __version__, "results": results}).encode(),
        media_type="application/json",
    )


async def file_parse(request: Request, pool: WorkerPool) -> Response:
    try:
        form = await request.form(max_files=100)
    except (HTTPException, ValueError):
        raise RouterProxyError(400, "invalid_request", "Invalid multipart form") from None
    session = None
    try:
        uploads = form.getlist("files")
        if not uploads or any(not isinstance(upload, UploadFile) for upload in uploads):
            raise RouterProxyError(400, "invalid_request", "At least one file upload is required", "files")
        try:
            options = LegacyOptions.model_validate(
                {key: value for key, value in form.items() if key != "files" and value != ""}
            )
        except ValidationError as exc:
            field = str(exc.errors()[0]["loc"][0])
            raise RouterProxyError(
                400, "invalid_request", f"Invalid request: {field}: {exc.errors()[0]['msg']}", field
            ) from None
        if options.backend is not None and options.backend not in BACKEND_TIERS:
            raise RouterProxyError(400, "invalid_request", "Unsupported legacy backend", "backend")
        if options.server_url:
            raise RouterProxyError(
                400, "invalid_request", "Configure the VLM URL on the server; per-request server_url is unsupported"
            )
        if options.end_page_id < options.start_page_id:
            raise RouterProxyError(400, "invalid_request", "end_page_id must be greater than or equal to start_page_id")
        if options.client_side_output_generation:
            raise RouterProxyError(
                400, "invalid_request", "client_side_output_generation requires the removed legacy document schema"
            )
        used_stems: set[str] = set()
        names = [_upload_name(upload.filename, used_stems) for upload in uploads]
        for upload in uploads:
            if upload.size is None or upload.size == 0:
                raise RouterProxyError(400, "invalid_request", "Uploaded files must not be empty")
            if upload.size > _MAX_FILE_BYTES:
                raise RouterProxyError(413, "file_too_large", "File exceeds the 200 MiB compatibility upload limit")
        available = {
            tier
            for worker in pool.healthy_workers()
            if "file_id" in (worker.features.get("sources") or []) and "zip" in (worker.features.get("output_formats") or [])
            for tier in worker.tiers
        }
        tier = options.tier or (
            BACKEND_TIERS[options.backend]
            if options.backend
            else select_default_quality_tier(available) or ("flash" if "flash" in available else None)
        )
        if tier is None:
            raise RouterProxyError(503, "quality_tier_unavailable", "No default parsing tier is available")
        worker = pool.select(tier=tier, required_sources={"file_id"}, required_outputs={"zip"})
        if worker is None:
            raise RouterProxyError(
                503, "quality_tier_unavailable", f"No healthy upstream supports tier '{tier}' with uploads and ZIP output"
            )
        session = LegacySession(request, pool, worker)
        pool.mark_job_started(worker.worker_id)
        entries = []
        for upload, name in zip(uploads, names):
            file_id = await session.upload(upload, name)
            entries.append(
                {
                    "source": {"type": "file_id", "file_id": file_id},
                    **(
                        {"page_range": f"{options.start_page_id + 1}-{options.end_page_id + 1}"}
                        if Path(name).suffix.lower() == ".pdf"
                        else {}
                    ),
                }
            )
        job = await session.parse(entries, tier, options.parse_method)
        return await _render_response(session, job, names, uploads, options)
    except UpstreamResponseError as exc:
        return passthrough_response(exc.response)
    finally:
        try:
            if session is not None:
                try:
                    await drain_future(asyncio.create_task(session.cleanup()))
                finally:
                    if session.worker.generation == session.generation:
                        pool.mark_job_finished(session.worker.worker_id)
        finally:
            await form.close()
