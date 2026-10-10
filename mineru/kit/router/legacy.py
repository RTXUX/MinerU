"""Route synchronous /file_parse requests to the upstream compatibility adapter."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx
from fastapi import Request
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
from starlette.datastructures import UploadFile
from starlette.exceptions import HTTPException

from ...parser.legacy_options import BACKEND_TIERS
from ...types import select_default_quality_tier, validate_tier
from .proxy import RouterProxyError, forwarded_headers, response_headers
from .workers import WorkerPool


async def file_parse(request: Request, pool: WorkerPool) -> StreamingResponse:
    """Preserve the backend adapter's fields and output while applying router policy."""
    try:
        form = await request.form(max_files=100)
    except (HTTPException, ValueError):
        raise RouterProxyError(400, "invalid_request", "Invalid multipart form") from None
    try:
        uploads = form.getlist("files")
        if not uploads or any(not isinstance(upload, UploadFile) for upload in uploads):
            raise RouterProxyError(400, "invalid_request", "At least one file upload is required", "files")
        backend = form.get("backend") or None
        if backend is not None and (not isinstance(backend, str) or backend not in BACKEND_TIERS):
            raise RouterProxyError(400, "invalid_request", "Unsupported legacy backend", "backend")
        raw_tier = form.get("tier") or None
        if raw_tier is not None:
            if not isinstance(raw_tier, str):
                raise RouterProxyError(400, "invalid_request", "Tier must be a string", "tier")
            try:
                tier = validate_tier(raw_tier)
            except ValueError:
                raise RouterProxyError(400, "invalid_request", "Unsupported tier", "tier") from None
        elif backend is not None:
            tier = BACKEND_TIERS[backend]
        else:
            available = pool.available_tiers()
            tier = select_default_quality_tier(available) or ("flash" if "flash" in available else None)
        if tier is None:
            raise RouterProxyError(503, "quality_tier_unavailable", "No default parsing tier is available")
        worker = pool.select(tier=tier)
        if worker is None:
            raise RouterProxyError(503, "quality_tier_unavailable", f"No healthy upstream supports tier '{tier}'")

        # Re-encode from spooled uploads: preserve duplicate fields/files, and let
        # HTTPX generate the new multipart boundary and content length.
        parts: list[tuple[str, Any]] = []
        for name, value in form.multi_items():
            if name == "tier":
                continue
            if isinstance(value, UploadFile):
                await value.seek(0)
                parts.append((name, (value.filename, value.file, value.content_type)))
            else:
                parts.append((name, (None, value)))
        parts.append(("tier", (None, tier)))
        headers = {**forwarded_headers(request), **worker.headers}
        headers.pop("content-type", None)
        headers.pop("content-encoding", None)
        upstream_request = pool.client.build_request(
            "POST",
            f"{worker.base_url}/file_parse",
            files=parts,
            headers=headers,
            # A synchronous parse can take longer than the V1 polling timeout.
            timeout=httpx.Timeout(60, connect=10, read=None),
        )
        generation = worker.generation
        pool.mark_job_started(worker.worker_id)
        try:
            upstream = await pool.client.send(upstream_request, stream=True)
        except httpx.TimeoutException as exc:
            raise RouterProxyError(504, "upstream_timeout", f"Upstream {worker.worker_id} timed out") from exc
        except httpx.HTTPError as exc:
            raise RouterProxyError(502, "upstream_unavailable", f"Upstream {worker.worker_id} is unavailable") from exc
        finally:
            if worker.generation == generation:
                pool.mark_job_finished(worker.worker_id)
    finally:
        await form.close()

    async def body() -> AsyncIterator[bytes]:
        try:
            if upstream.is_stream_consumed:
                yield upstream.content
            else:
                async for chunk in upstream.aiter_raw():
                    yield chunk
        finally:
            await upstream.aclose()

    headers = response_headers(upstream.headers)
    if upstream.is_stream_consumed:
        headers.pop("content-encoding", None)
    return StreamingResponse(
        body(),
        status_code=upstream.status_code,
        headers=headers,
        background=BackgroundTask(upstream.aclose),
    )
