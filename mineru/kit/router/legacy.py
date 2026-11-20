# Copyright (c) Opendatalab. All rights reserved.
"""在 Router 内将旧版同步 multipart `/file_parse` 适配到下游 V1 `/v1/tasks` 协议。"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import mimetypes
import unicodedata
import zipfile
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote, unquote

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, Response
from loguru import logger
from pydantic import BaseModel, Field, ValidationError
from starlette.datastructures import UploadFile
from starlette.exceptions import HTTPException
from starlette.requests import ClientDisconnect

from ...filetypes import (
    PARSEABLE_EXTENSIONS,
    is_flash_only_parse_extension,
    normalize_parse_extension,
    validate_parse_filename,
)
from ...tasks.contracts import CreateTaskResponse, TaskDetailResponse, TaskUploadLink, UploadFileResponse
from ...types import MiddleJson, Tier, select_default_task_tier
from ...utils.async_utils import drain_future, run_sync
from ...version import __version__
from .proxy import (
    RouterProxyError,
    forwarded_headers,
    json_or_error,
    passthrough_response,
    request_upstream,
    worker_resource_path,
)
from .workers import JobReservation, WorkerPool

BACKEND_TIERS: dict[str, Tier] = {
    "pipeline": "basic",
    "hybrid-engine": "standard",
    "hybrid-auto-engine": "standard",
    "hybrid-http-client": "standard",
    "vlm-engine": "standard",
    "vlm-auto-engine": "standard",
    "vlm-http-client": "standard",
    "flash": "flash",
}

_MAX_FILE_BYTES = 200 * 1024 * 1024
_POLL_INTERVAL = 0.25
_LEASE_SECONDS = "120"


class LegacyOptions(BaseModel):
    """兼容旧版 `/file_parse` 的表单控制项；无关历史开关按原兼容边界忽略。"""

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


class _UpstreamResponseError(Exception):
    """携带下游原始 HTTP 错误响应，由顶层按统一 V1 错误边界转发。"""

    def __init__(self, response: httpx.Response) -> None:
        super().__init__(f"Upstream returned HTTP {response.status_code}")
        self.response = response


@dataclass(frozen=True)
class _PreparedInput:
    """保存已完成本地校验、去重命名与哈希计算的单个上传文件。"""

    name: str
    upload: UploadFile
    size: int
    sha256sum: str
    lightweight: bool
    page_range: str | None


@dataclass(frozen=True)
class _FileOutcome:
    """保存单个文件在下游 `/v1/tasks` 的终态与可选结果归档字节。"""

    name: str
    upload: UploadFile
    status: Literal["completed", "failed"]
    archive_bytes: bytes = b""
    error: dict[str, Any] | None = None


def _upload_name(filename: str | None, used_stems: set[str]) -> str:
    """剥离客户端目录前缀并去重 stem，确保满足 V1 `file_name` 约束。"""
    raw = (filename or "document.pdf").replace("\\", "/").rsplit("/", 1)[-1]
    cleaned = "".join("_" if char == ":" else char for char in raw if unicodedata.category(char) != "Cc")
    parsed = Path(cleaned)
    suffix = parsed.suffix
    max_stem_len = max(1, 960 - len(suffix))
    stem = (parsed.stem.strip(". ") or "document")[:max_stem_len]
    candidate = stem
    number = 2
    while candidate in used_stems:
        prefix = stem[: max(1, max_stem_len - len(str(number)) - 1)]
        candidate = f"{prefix}_{number}"
        number += 1
    used_stems.add(candidate)
    return validate_parse_filename(f"{candidate}{suffix}")


def _result_files(
    payload: bytes,
    stem: str,
    *,
    return_md: bool,
    return_middle_json: bool,
    return_model_output: bool,
    return_content_list: bool,
    return_images: bool,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    """在内存中将 V1 `default` 任务归档转换为旧版 JSON 字段与 ZIP 条目。"""
    from ...render.content_list import render_content_list

    result: dict[str, Any] = {}
    files: dict[str, bytes] = {}
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        members = set(archive.namelist())
        if "markdown.md" not in members or "middle_json.json" not in members:
            raise ValueError("Missing required members in upstream result archive")
        for enabled, key, source, destination in (
            (return_md, "md_content", "markdown.md", f"{stem}.md"),
            (return_middle_json, "middle_json", "middle_json.json", f"{stem}_middle.json"),
            (return_model_output, "model_output", "model_output.json", f"{stem}_model.json"),
        ):
            if enabled:
                data = archive.read(source) if source in members else None
                result[key] = data.decode("utf-8") if data is not None else None
                if data is not None:
                    files[destination] = data
        if return_content_list:
            middle = MiddleJson.from_dict(json.loads(archive.read("middle_json.json")))
            data = json.dumps(render_content_list(middle), ensure_ascii=False).encode("utf-8")
            result["content_list"] = data.decode("utf-8")
            files[f"{stem}_content_list.json"] = data
        if return_images:
            images: dict[str, str] = {}
            for member in archive.namelist():
                if not member.startswith("images/") or member.endswith("/"):
                    continue
                rel = member[len("images/") :]
                if not rel or rel in {".", ".."} or "/" in rel or "\\" in rel:
                    continue
                data = archive.read(member)
                mime = mimetypes.guess_type(rel)[0] or "application/octet-stream"
                images[rel] = f"data:{mime};base64,{base64.b64encode(data).decode('ascii')}"
                files[f"images/{rel}"] = data
            result["images"] = images
    return result, files


class _PrivateTask:
    """跟踪单个输入文件对应的私有下游 `/v1/tasks` 生命周期、租约与并发预占。"""

    def __init__(
        self,
        pool: WorkerPool,
        reservation: JobReservation,
        base_headers: Mapping[str, str],
    ) -> None:
        self.pool = pool
        self.reservation = reservation
        self.base_headers = dict(base_headers)
        self.task_id: str | None = None
        self.terminal = False
        self.lease_held = False

    async def call(
        self,
        method: str,
        path: str,
        *,
        json_body: Any = None,
        content: bytes | AsyncIterator[bytes] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        """在确认 worker generation 未发生替换的前提下发送一次下游请求。"""
        worker = self.reservation.worker
        if worker.generation != self.reservation.generation or not worker.base_url:
            raise RouterProxyError(502, "upstream_unavailable", "Upstream worker was replaced")
        merged = {**self.base_headers, **(headers or {})}
        response = await request_upstream(
            self.pool,
            worker,
            method,
            path,
            json_body=json_body,
            content=content,
            headers=merged,
        )
        if worker.generation != self.reservation.generation or not worker.base_url:
            raise RouterProxyError(502, "upstream_unavailable", "Upstream worker was replaced")
        if response.status_code >= 400:
            raise _UpstreamResponseError(response)
        return response

    async def upload_file(
        self,
        link: TaskUploadLink,
        upload: UploadFile,
        size: int,
        task_id: str,
        remote_file_id: str,
    ) -> None:
        """向校验通过的下游资源路径流式发送单文件字节。"""
        worker = self.reservation.worker
        resource_path = worker_resource_path(worker, link.url)
        expected_path = f"/v1/tasks/{quote(task_id, safe='')}/uploads/{quote(remote_file_id, safe='')}"
        if resource_path != expected_path:
            raise RouterProxyError(502, "invalid_upstream_response", "Invalid worker upload path")

        async def chunks() -> AsyncIterator[bytes]:
            await upload.seek(0)
            while chunk := await upload.read(1024 * 1024):
                yield chunk

        response = await self.call(
            "PUT",
            resource_path,
            content=chunks(),
            headers={
                **link.headers,
                "content-length": str(size),
                "x-mineru-task-lease-seconds": _LEASE_SECONDS,
            },
        )
        try:
            acknowledged = UploadFileResponse.model_validate(json_or_error(response))
        except ValidationError as exc:
            raise RouterProxyError(502, "invalid_upstream_response", f"Invalid upload acknowledgement: {exc}") from None
        if acknowledged.task_id != task_id or acknowledged.file_id != remote_file_id:
            raise RouterProxyError(502, "invalid_upstream_response", "Upload acknowledgement identity changed")

    async def cleanup(self) -> None:
        """取消未终结的私有任务并释放结果保留租约与 worker 预占。"""
        try:
            if self.task_id is None:
                return
            worker = self.reservation.worker
            if worker.generation != self.reservation.generation or not worker.base_url:
                return
            task_path = f"/v1/tasks/{quote(self.task_id, safe='')}"
            if not self.terminal:
                try:
                    await self.call("POST", f"{task_path}/cancel")
                except _UpstreamResponseError as exc:
                    if exc.response.status_code not in {404, 409}:
                        raise
            if self.lease_held:
                try:
                    await self.call("GET", task_path, headers={"x-mineru-task-lease-seconds": "0"})
                except _UpstreamResponseError as exc:
                    if exc.response.status_code not in {404, 409}:
                        raise
                self.lease_held = False
        except (RouterProxyError, _UpstreamResponseError):
            logger.warning("Legacy /file_parse task cleanup failed for {}", self.task_id)
        finally:
            self.reservation.release()


async def _prepare_inputs(uploads: list[UploadFile], options: LegacyOptions) -> list[_PreparedInput]:
    """校验全部上传文件的非空、体积上限与可解析后缀，并预计算 SHA-256。"""
    used_stems: set[str] = set()
    prepared: list[_PreparedInput] = []
    for upload in uploads:
        try:
            name = _upload_name(upload.filename, used_stems)
        except ValueError as exc:
            raise RouterProxyError(400, "invalid_request", str(exc), "files") from None
        if normalize_parse_extension(name) not in PARSEABLE_EXTENSIONS:
            raise RouterProxyError(400, "invalid_request", f"Unsupported file type: {Path(name).suffix or name}", "files")
        if upload.size is not None:
            if upload.size == 0:
                raise RouterProxyError(400, "invalid_request", "Uploaded files must not be empty", "files")
            if upload.size > _MAX_FILE_BYTES:
                raise RouterProxyError(413, "file_too_large", "File exceeds the 200 MiB compatibility upload limit", "files")
        await upload.seek(0)
        size = 0
        hasher = hashlib.sha256()
        while chunk := await upload.read(1024 * 1024):
            size += len(chunk)
            if size > _MAX_FILE_BYTES:
                raise RouterProxyError(413, "file_too_large", "File exceeds the 200 MiB compatibility upload limit", "files")
            hasher.update(chunk)
        if size == 0:
            raise RouterProxyError(400, "invalid_request", "Uploaded files must not be empty", "files")
        page_range = f"{options.start_page_id + 1}-{options.end_page_id + 1}" if Path(name).suffix.lower() == ".pdf" else None
        prepared.append(
            _PreparedInput(
                name=name,
                upload=upload,
                size=size,
                sha256sum=hasher.hexdigest(),
                lightweight=is_flash_only_parse_extension(name),
                page_range=page_range,
            )
        )
    return prepared


def _resolve_batch_tier(pool: WorkerPool, prepared: list[_PreparedInput], options: LegacyOptions) -> Tier:
    """在创建任何下游资源前预检目标能力，保持与 V1 Tasks 一致的选档规则。"""
    capable_any = pool.capable_workers(
        tier=None,
        required_sources={"upload"},
        required_task_outputs={"default"},
    )
    if all(item.lightweight for item in prepared):
        if not capable_any:
            raise RouterProxyError(
                503, "quality_tier_unavailable", "No healthy upstream supports task uploads and default output"
            )
        return "flash"
    available_tiers = {tier for worker in capable_any for tier in worker.tiers}
    requested = options.tier or (BACKEND_TIERS[options.backend] if options.backend else None)
    effective = requested or select_default_task_tier(available_tiers)
    if effective is None:
        raise RouterProxyError(503, "quality_tier_unavailable", "No default parsing tier is available")
    if not pool.capable_workers(
        tier=effective,
        required_sources={"upload"},
        required_task_outputs={"default"},
    ):
        raise RouterProxyError(
            503,
            "quality_tier_unavailable",
            f"No healthy upstream supports tier '{effective}' with uploads and default output",
        )
    return effective


async def _execute_file(
    request: Request,
    pool: WorkerPool,
    item: _PreparedInput,
    tier: Tier,
    parse_method: Literal["auto", "txt", "ocr"],
    base_headers: Mapping[str, str],
    handles: list[_PrivateTask],
) -> _FileOutcome:
    """为单个文件预占最优 worker，完成创建、按需上传、轮询与归档下载。"""
    assert_owner = getattr(request.app.state, "assert_persistence_owner", None)
    if assert_owner is not None:
        await assert_owner()
    reservation = pool.reserve_job(
        tier=None if item.lightweight else tier,
        required_sources={"upload"},
        required_task_outputs={"default"},
    )
    if reservation is None:
        raise RouterProxyError(
            503,
            "quality_tier_unavailable",
            f"No healthy upstream supports tier '{tier}' with uploads and default output",
        )
    handle = _PrivateTask(pool=pool, reservation=reservation, base_headers=base_headers)
    handles.append(handle)

    manifest_entry: dict[str, Any] = {
        "file_name": item.name,
        "bytes": item.size,
        "sha256sum": item.sha256sum,
    }
    if item.page_range is not None:
        manifest_entry["page_range"] = item.page_range
    create_body: dict[str, Any] = {
        "files": [manifest_entry],
        "ocr_mode": parse_method,
        "output_formats": ["default"],
    }
    if not item.lightweight:
        create_body["tier"] = tier

    created_resp = await handle.call(
        "POST",
        "/v1/tasks",
        json_body=create_body,
        headers={"x-mineru-task-lease-seconds": _LEASE_SECONDS},
    )
    try:
        created = CreateTaskResponse.model_validate(json_or_error(created_resp))
    except ValidationError as exc:
        raise RouterProxyError(502, "invalid_upstream_response", f"Invalid task creation response: {exc}") from None
    if len(created.files) != 1 or created.files[0].file_name != item.name:
        raise RouterProxyError(502, "invalid_upstream_response", "Upstream returned unexpected task files")

    task_id = created.task_id
    remote_file_id = created.files[0].file_id
    handle.task_id = task_id
    handle.lease_held = True

    uploaded = created.files[0].status != "waiting-file"
    if created.files[0].status == "waiting-file" and created.files[0].upload is not None:
        await handle.upload_file(created.files[0].upload, item.upload, item.size, task_id, remote_file_id)
        uploaded = True

    task_path = f"/v1/tasks/{quote(task_id, safe='')}"
    while True:
        try:
            detail_resp = await handle.call("GET", task_path, headers={"x-mineru-task-lease-seconds": _LEASE_SECONDS})
        except _UpstreamResponseError as exc:
            if exc.response.status_code == 404:
                raise RouterProxyError(502, "upstream_task_lost", "Upstream task disappeared while polling") from None
            raise
        try:
            detail = TaskDetailResponse.model_validate(json_or_error(detail_resp))
        except ValidationError as exc:
            raise RouterProxyError(502, "invalid_upstream_response", f"Invalid task status response: {exc}") from None
        if (
            detail.task_id != task_id
            or len(detail.files) != 1
            or detail.files[0].file_id != remote_file_id
            or detail.files[0].file_name != item.name
            or detail.status == "partial"
        ):
            raise RouterProxyError(502, "invalid_upstream_response", "Upstream task identity changed")

        remote_file = detail.files[0]
        if detail.status in {"completed", "failed", "canceled"}:
            handle.terminal = True
            handle.reservation.release()
            break
        if remote_file.status == "waiting-file" and remote_file.upload is not None and not uploaded:
            await handle.upload_file(remote_file.upload, item.upload, item.size, task_id, remote_file_id)
            uploaded = True
            continue
        if await request.is_disconnected():
            raise RouterProxyError(499, "client_disconnected", "Client disconnected during parsing")
        await asyncio.sleep(_POLL_INTERVAL)

    if detail.status == "canceled":
        raise RouterProxyError(503, "parsing_canceled", "Parsing was canceled")
    if detail.status != "completed":
        err = remote_file.error or detail.error
        return _FileOutcome(
            name=item.name,
            upload=item.upload,
            status="failed",
            error=err.model_dump(exclude_none=True)
            if err is not None
            else {"code": "task_failed", "message": "Upstream task failed"},
        )

    if not isinstance(remote_file.result_url, str) or not remote_file.result_url:
        raise RouterProxyError(502, "invalid_upstream_response", "Completed task file omitted result_url")
    result_path = worker_resource_path(handle.reservation.worker, remote_file.result_url)
    parts = result_path.strip("/").split("/")
    if len(parts) != 4 or parts[:2] != ["v1", "files"] or parts[-1] != "content" or unquote(parts[2]) != remote_file_id:
        raise RouterProxyError(502, "invalid_upstream_response", "Invalid worker result File URL")

    try:
        content_resp = await handle.call("GET", result_path)
    except _UpstreamResponseError as exc:
        if exc.response.status_code == 404:
            raise RouterProxyError(502, "upstream_task_lost", "Upstream result file disappeared") from None
        raise

    try:
        await handle.call("GET", task_path, headers={"x-mineru-task-lease-seconds": "0"})
        handle.lease_held = False
    except (RouterProxyError, _UpstreamResponseError):
        pass

    return _FileOutcome(
        name=item.name,
        upload=item.upload,
        status="completed",
        archive_bytes=content_resp.content,
    )


async def _cleanup_all(handles: list[_PrivateTask]) -> None:
    """在有限时间预算内清理本次请求创建的全部私有下游任务和预占。"""
    if not handles:
        return
    try:
        async with asyncio.timeout(10):
            await asyncio.gather(*(handle.cleanup() for handle in handles))
    except TimeoutError:
        for handle in handles:
            handle.reservation.release()
        logger.warning("Legacy /file_parse cleanup timed out; upstream retention policy applies")


async def _render_response(outcomes: list[_FileOutcome], options: LegacyOptions) -> Response:
    """将全部单文件产物组装为旧版 `/file_parse` 的 JSON 或 ZIP 响应。"""
    if any(item.status != "completed" for item in outcomes):
        return JSONResponse(
            {
                "message": "Task execution failed",
                "errors": {item.name: item.error for item in outcomes if item.status != "completed" and item.error},
            },
            status_code=409,
        )

    results: dict[str, Any] = {}
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for item in outcomes:
            stem = Path(item.name).stem
            try:
                result, files = await run_sync(
                    _result_files,
                    item.archive_bytes,
                    stem,
                    return_md=options.return_md,
                    return_middle_json=options.return_middle_json,
                    return_model_output=options.return_model_output,
                    return_content_list=options.return_content_list,
                    return_images=options.return_images,
                )
            except (ValueError, KeyError, zipfile.BadZipFile):
                raise RouterProxyError(502, "invalid_upstream_response", "Invalid upstream result archive") from None
            results[stem] = result
            if options.response_format_zip:
                if options.return_original_file:
                    await item.upload.seek(0)
                    files[f"{stem}_origin{Path(item.name).suffix}"] = await item.upload.read()
                for filename, content in files.items():
                    await run_sync(archive.writestr, f"{stem}/{options.parse_method}/{filename}", content)

    if options.response_format_zip:
        return Response(
            buffer.getvalue(),
            media_type="application/zip",
            headers={"Content-Disposition": 'attachment; filename="results.zip"'},
        )
    return Response(
        json.dumps(
            {
                "backend": options.backend or "hybrid-auto-engine",
                "version": __version__,
                "results": results,
            },
            ensure_ascii=False,
        ).encode("utf-8"),
        media_type="application/json",
    )


async def file_parse(request: Request, pool: WorkerPool) -> Response:
    """接收旧版 multipart `/file_parse` 请求并通过下游 V1 `/v1/tasks` 同步返回结果。"""
    try:
        form = await request.form(max_files=100)
    except ClientDisconnect:
        raise RouterProxyError(400, "upload_interrupted", "Client disconnected before upload completed") from None
    except (HTTPException, ValueError):
        raise RouterProxyError(400, "invalid_request", "Invalid multipart form") from None

    handles: list[_PrivateTask] = []
    try:
        uploads = form.getlist("files")
        if not uploads or any(not isinstance(upload, UploadFile) for upload in uploads):
            raise RouterProxyError(400, "invalid_request", "At least one file upload is required", "files")
        for key, value in form.multi_items():
            if key != "files" and not isinstance(value, str):
                raise RouterProxyError(400, "invalid_request", f"Field '{key}' must be a text value", key)
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
        if options.server_url and options.server_url.strip():
            raise RouterProxyError(
                400,
                "invalid_request",
                "Configure the VLM URL on the server; per-request server_url is unsupported",
                "server_url",
            )
        if options.end_page_id < options.start_page_id:
            raise RouterProxyError(
                400, "invalid_request", "end_page_id must be greater than or equal to start_page_id", "end_page_id"
            )
        if options.client_side_output_generation:
            raise RouterProxyError(
                400,
                "invalid_request",
                "client_side_output_generation requires the removed legacy document schema",
                "client_side_output_generation",
            )

        prepared = await _prepare_inputs(uploads, options)
        tier = _resolve_batch_tier(pool, prepared, options)

        base_headers = forwarded_headers(request)
        for header_name in ("content-type", "content-encoding", "accept"):
            for existing in [k for k in base_headers if k.lower() == header_name]:
                base_headers.pop(existing, None)

        tasks = [
            asyncio.create_task(_execute_file(request, pool, item, tier, options.parse_method, base_headers, handles))
            for item in prepared
        ]
        try:
            outcomes = await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        return await _render_response(outcomes, options)
    except _UpstreamResponseError as exc:
        return passthrough_response(exc.response)
    finally:
        try:
            if handles:
                await drain_future(asyncio.create_task(_cleanup_all(handles)))
        finally:
            await form.close()


__all__ = ["BACKEND_TIERS", "LegacyOptions", "file_parse"]
