"""Multipart /file_parse compatibility over the current V1 job execution path.

Only the transport envelope is legacy: document JSON uses the current schema.
"""

from __future__ import annotations

import base64
import io
import json
import mimetypes
import tempfile
import zipfile
from pathlib import Path
from typing import Annotated, Any, Literal

from fastapi import APIRouter, File, Form, HTTPException, Request, Response, UploadFile

from ..render import render_content_list
from ..types import MiddleJson, Tier
from ..utils.async_utils import run_sync
from ..version import __version__
from .api_server import (
    _MAX_FILE_SIZE_BYTES_DEFAULT,
    _MAX_FILES_PER_JOB_DEFAULT,
    CreateJobRequest,
    CreateUploadRequest,
    FileIdSource,
    FileStore,
    JobFileEntry,
    JobStore,
    _json_utf8_bytes,
    create_job,
)

_BACKEND_TIERS: dict[str, Tier] = {
    "pipeline": "basic",
    "hybrid-auto-engine": "standard",
    "hybrid-http-client": "standard",
    "vlm-auto-engine": "standard",
    "vlm-http-client": "standard",
    "flash": "flash",
}


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
    """Translate a self-contained V1 archive without extracting paths to disk."""
    result: dict[str, Any] = {}
    files: dict[str, bytes] = {}
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        for enabled, key, source, destination in (
            (return_md, "md_content", "markdown.md", f"{stem}.md"),
            (return_middle_json, "middle_json", "middle_json.json", f"{stem}_middle.json"),
            (return_model_output, "model_output", "model_output.json", f"{stem}_model.json"),
        ):
            if enabled:
                data = archive.read(source) if source in archive.namelist() else None
                result[key] = data.decode("utf-8") if data is not None else None
                if data is not None:
                    files[destination] = data
        if return_content_list:
            middle = MiddleJson.from_dict(json.loads(archive.read("middle_json.json")))
            data = _json_utf8_bytes(render_content_list(middle))
            result["content_list"] = data.decode("utf-8")
            files[f"{stem}_content_list.json"] = data
        if return_images:
            images: dict[str, str] = {}
            for name in archive.namelist():
                if name.startswith("images/") and not name.endswith("/"):
                    data = archive.read(name)
                    mime = mimetypes.guess_type(name)[0] or "application/octet-stream"
                    images[Path(name).name] = f"data:{mime};base64,{base64.b64encode(data).decode('ascii')}"
                    files[name] = data
            result["images"] = images
    return result, files


def _upload_name(filename: str | None, used_stems: set[str]) -> str:
    """Keep result keys and ZIP paths safe and unique, including Windows names."""
    name = (filename or "document.pdf").replace("\\", "/").rsplit("/", 1)[-1]
    name = "".join(char for char in name if char >= " " and char != "\x7f")
    path = Path(name)
    stem = path.stem.strip(". ") or "document"
    candidate = stem
    number = 2
    while candidate in used_stems:
        candidate = f"{stem}_{number}"
        number += 1
    used_stems.add(candidate)
    return f"{candidate}{path.suffix}"


async def file_parse(
    request: Request,
    files: Annotated[list[UploadFile], File(min_length=1, max_length=_MAX_FILES_PER_JOB_DEFAULT)],
    backend: Annotated[str | None, Form()] = None,
    tier: Annotated[Tier | None, Form(description="Override the legacy backend-to-tier mapping.")] = None,
    parse_method: Annotated[Literal["auto", "txt", "ocr"], Form()] = "auto",
    start_page_id: Annotated[int, Form(ge=0)] = 0,
    end_page_id: Annotated[int, Form(ge=0)] = 99999,
    return_md: Annotated[bool, Form()] = True,
    return_middle_json: Annotated[bool, Form()] = False,
    return_model_output: Annotated[bool, Form()] = False,
    return_content_list: Annotated[bool, Form()] = False,
    return_images: Annotated[bool, Form()] = False,
    response_format_zip: Annotated[bool, Form()] = False,
    return_original_file: Annotated[bool, Form()] = False,
    server_url: Annotated[str | None, Form()] = None,
    client_side_output_generation: Annotated[bool, Form()] = False,
) -> Response:
    """Parse uploads synchronously with legacy response keys and current schemas.

    Legacy lang_list, formula_enable, table_enable and image_analysis fields are
    accepted but ignored; parsing follows current tier and server configuration.
    Per-request server_url overrides are unsupported. /tasks is not restored.
    """
    if backend is not None and backend not in _BACKEND_TIERS:
        raise HTTPException(400, f"Unsupported legacy backend: {backend}")
    if server_url:
        raise HTTPException(400, "Configure the VLM URL on the server; per-request server_url is unsupported")
    if end_page_id < start_page_id:
        raise HTTPException(400, "end_page_id must be greater than or equal to start_page_id")
    if client_side_output_generation:
        raise HTTPException(400, "client_side_output_generation requires the removed legacy document schema")
    selected_tier = tier or (_BACKEND_TIERS[backend] if backend is not None else request.app.state.tier)
    response_backend = backend or "hybrid-auto-engine"
    job_store: JobStore = request.app.state.job_store
    access_level = "registered" if request.app.state.api_key else "anonymous"
    used_stems: set[str] = set()
    entries: list[JobFileEntry] = []
    originals: dict[str, bytes] = {}

    # Private storage prevents synchronous calls from retaining uploads/artifacts
    # in the V1 resource index. Execution still uses the application's JobStore.
    with tempfile.TemporaryDirectory(prefix="mineru_legacy_") as directory:
        store = FileStore(Path(directory))
        for upload in files:
            name = _upload_name(upload.filename, used_stems)
            if upload.size is not None and upload.size > _MAX_FILE_SIZE_BYTES_DEFAULT:
                raise HTTPException(413, "File exceeds the 200 MiB compatibility upload limit")
            data = await upload.read(_MAX_FILE_SIZE_BYTES_DEFAULT + 1)
            if len(data) > _MAX_FILE_SIZE_BYTES_DEFAULT:
                raise HTTPException(413, "File exceeds the 200 MiB compatibility upload limit")
            if not data:
                raise HTTPException(400, "Uploaded files must not be empty")
            upload_record = store.create_upload(
                CreateUploadRequest(filename=name, bytes=len(data), mime_type=upload.content_type or "application/octet-stream")
            )
            store.store_upload_data(upload_record.id, data)
            completed = store.complete_upload(upload_record.id, None)
            assert completed.file is not None
            # Non-PDF native formats remain whole-document only.
            page_range = f"{start_page_id + 1}-{end_page_id + 1}" if Path(name).suffix.lower() == ".pdf" else None
            entries.append(JobFileEntry(source=FileIdSource(file_id=completed.file.id), page_range=page_range))
            if response_format_zip and return_original_file:
                originals[name] = data

        submitted = await create_job(
            CreateJobRequest(files=entries, tier=selected_tier, ocr_mode=parse_method, output_formats=["zip"]),
            request,
            store,
            job_store,
            access_level,
        )
        job_id = json.loads(submitted.body)["job_id"]
        job = await job_store.wait_and_remove(job_id)
        if job.status == "canceled":
            raise HTTPException(503, "Parsing was canceled")
        if job.status != "completed":
            return Response(
                content=_json_utf8_bytes(
                    {"message": "Task execution failed", "errors": {f.name: f.error.model_dump() for f in job.files if f.error}}
                ),
                status_code=409,
                media_type="application/json",
            )

        results: dict[str, Any] = {}
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as output_zip:
            for parsed_file in job.files:
                assert parsed_file.output_files is not None and parsed_file.output_files.zip is not None
                artifact = store.get_file(parsed_file.output_files.zip.file_id)
                assert artifact.sha256sum is not None
                stem = Path(parsed_file.name).stem
                result, result_files = await run_sync(
                    _result_files,
                    store.read_blob(artifact.sha256sum),
                    stem,
                    return_md=return_md,
                    return_middle_json=return_middle_json,
                    return_model_output=return_model_output,
                    return_content_list=return_content_list,
                    return_images=return_images,
                )
                results[stem] = result
                if response_format_zip:
                    if return_original_file:
                        result_files[f"{stem}_origin{Path(parsed_file.name).suffix}"] = originals[parsed_file.name]
                    for name, data in result_files.items():
                        output_zip.writestr(f"{stem}/{parse_method}/{name}", data)
        if response_format_zip:
            return Response(
                content=zip_buffer.getvalue(),
                media_type="application/zip",
                headers={"Content-Disposition": 'attachment; filename="results.zip"'},
            )
        return Response(
            content=_json_utf8_bytes({"backend": response_backend, "version": __version__, "results": results}),
            media_type="application/json",
        )


def build_legacy_router() -> APIRouter:
    """Build the compatibility route without changing the V1 contract."""
    router = APIRouter()
    router.add_api_route("/file_parse", file_parse, methods=["POST"], tags=["Legacy compatibility"])
    return router


__all__ = ["build_legacy_router"]
