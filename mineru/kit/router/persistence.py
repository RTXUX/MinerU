# Copyright (c) Opendatalab. All rights reserved.
"""Router 的单活租约 Redis 快照持久化、ASGI 写前栅栏与 Tasks 状态恢复。"""

from __future__ import annotations

import asyncio
import json
import re
import secrets
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from ...tasks.contracts import ErrorDetail, FileParseInfo, TaskFileItem, UploadTaskOptions
from ...tasks.execution import TaskExecution, TaskInputMetadata, TaskUpload
from ...tasks.service import TaskFileRecord, TaskRecord, TaskStore
from .proxy import RouterProxyError, router_error_response
from .resources import ResourceRegistry, SourceFileStore

if TYPE_CHECKING:
    from .tasks import FinalizeExecution

_RENEW = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('PEXPIRE', KEYS[1], ARGV[2])
"""
_SAVE = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[2], ARGV[2])
return 1
"""
_RELEASE = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
"""


class RedisTransportError(RuntimeError):
    """允许无真实 Redis 依赖的测试替身模拟底层连接或命令异常。"""


class RedisStateStore:
    """在响应确认前原子写入快照；租约丢失后立即拒绝写入和转发。"""

    def __init__(
        self,
        url: str,
        namespace: str,
        *,
        lease_seconds: float = 30,
        client: Any = None,
    ) -> None:
        """校验命名空间与租约周期，按需惰性导入可选 Redis 客户端。"""
        if not re.fullmatch(r"[A-Za-z0-9:_-]+", namespace):
            raise ValueError("Redis namespace must contain only letters, digits, colon, underscore or hyphen")
        if lease_seconds < 1:
            raise ValueError("Redis lease must be at least one second")
        errors: list[type[BaseException]] = [RedisTransportError]
        if client is None:
            try:
                from redis.asyncio import Redis
                from redis.exceptions import RedisError
            except ImportError:
                raise RuntimeError("Redis persistence requires installing mineru[router]") from None
            client = Redis.from_url(url, socket_connect_timeout=5, socket_timeout=5)
            errors.append(RedisError)
        else:
            with suppress(ImportError):
                from redis.exceptions import RedisError

                errors.append(RedisError)
        self.client = client
        self._redis_errors: tuple[type[BaseException], ...] = tuple(errors)
        prefix = f"mineru-router:{{{namespace}}}"
        self.lease_key = prefix + ":owner"
        self.state_key = prefix + ":state"
        self.token = secrets.token_hex(24)
        self.lease_seconds = lease_seconds
        self._heartbeat: asyncio.Task[None] | None = None
        self._lost = False
        self._acquired = False
        self._save_lock = asyncio.Lock()

    @staticmethod
    def unavailable() -> RouterProxyError:
        """统一构造持久层失联或租约易主时的 503 响应异常。"""
        return RouterProxyError(503, "router_state_unavailable", "Router persistence or ownership is unavailable")

    async def start(self) -> dict[str, Any] | None:
        """获取单活租约、启动续租协程并读取已有快照。"""
        try:
            self._acquired = bool(await self.client.set(self.lease_key, self.token, nx=True, px=int(self.lease_seconds * 1000)))
            if not self._acquired:
                raise RuntimeError("Another router owns this Redis namespace; stop it or wait for its lease to expire")
            self._heartbeat = asyncio.create_task(self._renew_loop(), name="mineru-router-redis-lease")
            raw = await self.client.get(self.state_key)
            if raw is None:
                return None
            state = json.loads(raw)
            if not isinstance(state, dict) or state.get("version") != 1:
                raise ValueError("Unsupported router persistence schema")
            return state
        except self._redis_errors:
            raise RuntimeError("Cannot initialize router Redis persistence") from None
        except (ValueError, TypeError):
            raise RuntimeError("Invalid router Redis snapshot; refusing to overwrite it") from None

    async def assert_owner(self) -> None:
        """在转发或修改状态前确认当前进程仍持有命名空间租约。"""
        if self._lost or not self._acquired:
            raise self.unavailable()
        try:
            owner = await self.client.get(self.lease_key)
        except self._redis_errors:
            raise self.unavailable() from None
        if owner not in {self.token.encode("utf-8"), self.token}:
            self._lost = True
            raise self.unavailable()

    async def _renew_loop(self) -> None:
        """按三分之一租约周期原子续期；任何续期失败均永久锁死本实例写入。"""
        try:
            while True:
                await asyncio.sleep(self.lease_seconds / 3)
                renewed = await self.client.eval(_RENEW, 1, self.lease_key, self.token, int(self.lease_seconds * 1000))
                if not renewed:
                    self._lost = True
                    return
        except self._redis_errors:
            # 网络中断期间租约可能已过期并被新实例接管，要求重启恢复。
            self._lost = True

    async def save(self, snapshot: Callable[[], dict[str, Any]]) -> None:
        """在单写锁内构建快照并校验租约原子落盘，避免旧快照覆盖新状态。"""
        async with self._save_lock:
            if self._lost or not self._acquired:
                raise self.unavailable()
            encoded = json.dumps(snapshot(), separators=(",", ":"), ensure_ascii=True)
            try:
                saved = await self.client.eval(_SAVE, 2, self.lease_key, self.state_key, self.token, encoded)
            except self._redis_errors:
                raise self.unavailable() from None
            if not saved:
                self._lost = True
                raise self.unavailable()

    async def close(self) -> None:
        """停止续租并在仍持有所有权时原子释放租约。"""
        if self._heartbeat is not None:
            self._heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await self._heartbeat
            self._heartbeat = None
        if self._acquired:
            with suppress(*self._redis_errors):
                await self.client.eval(_RELEASE, 1, self.lease_key, self.token)
        self._acquired = False
        await self.client.aclose()


class _PersistenceAbort(Exception):
    """在响应头尚未发往外层 ASGI 连接时中断内层路由栈，避开 Starlette 已启动标志误判。"""

    def __init__(self, error: RouterProxyError) -> None:
        super().__init__(error.message)
        self.error = error


class PersistenceMiddleware:
    """纯 ASGI 持久化栅栏：不缓冲流式请求体，且在发送响应头前完成快照写入。"""

    def __init__(
        self,
        app: ASGIApp,
        persistence: RedisStateStore,
        checkpoint: Callable[[], Awaitable[None]],
    ) -> None:
        """绑定底层 ASGI 应用、租约存储与快照回调。"""
        self.app = app
        self.persistence = persistence
        self.checkpoint = checkpoint

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """请求前核对租约，响应头发出前落盘；失败时返回 503 而不确认未保存变更。"""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        try:
            await self.persistence.assert_owner()
        except RouterProxyError as exc:
            await router_error_response(exc)(scope, receive, send)
            return
        checkpointed = False
        response_started = False

        async def wrapped_send(message: Message) -> None:
            nonlocal checkpointed, response_started
            if message["type"] == "http.response.start" and not checkpointed:
                checkpointed = True
                try:
                    await self.checkpoint()
                except RouterProxyError as exc:
                    raise _PersistenceAbort(exc) from None
                response_started = True
            await send(message)

        try:
            await self.app(scope, receive, wrapped_send)
        except _PersistenceAbort as abort:
            if response_started:
                raise abort.error from None
            await router_error_response(abort.error)(scope, receive, send)
        except RouterProxyError as exc:
            if response_started:
                raise
            await router_error_response(exc)(scope, receive, send)
        finally:
            if not checkpointed:
                with suppress(RouterProxyError):
                    await self.checkpoint()


def _serialize_execution(execution: TaskExecution | None) -> dict[str, Any] | None:
    """把单文件执行快照编码成纯 JSON 字典。"""
    if execution is None:
        return None
    return {
        "execution_id": execution.execution_id,
        "file_id": execution.file_id,
        "file_name": execution.file_name,
        "status": execution.status,
        "created_at": execution.created_at,
        "started_at": execution.started_at,
        "finished_at": execution.finished_at,
        "page_range": execution.page_range,
        "parse": execution.parse.model_dump() if execution.parse is not None else None,
        "result_file_id": execution.result_file_id,
        "error": execution.error.model_dump() if execution.error is not None else None,
        "input_metadata": {"bytes": execution.input_metadata.bytes} if execution.input_metadata is not None else None,
    }


def _deserialize_execution(data: dict[str, Any] | None) -> TaskExecution | None:
    """从持久化 JSON 字典还原有类型 TaskExecution。"""
    if data is None:
        return None
    if not isinstance(data, dict):
        raise ValueError("Invalid persisted execution")
    parse_data = data.get("parse")
    error_data = data.get("error")
    metadata_data = data.get("input_metadata")
    return TaskExecution(
        execution_id=str(data["execution_id"]),
        file_id=str(data["file_id"]),
        file_name=str(data["file_name"]),
        status=data["status"],
        created_at=str(data["created_at"]),
        started_at=data.get("started_at"),
        finished_at=data.get("finished_at"),
        page_range=str(data.get("page_range") or ""),
        parse=FileParseInfo.model_validate(parse_data) if parse_data is not None else None,
        result_file_id=data.get("result_file_id"),
        error=ErrorDetail.model_validate(error_data) if error_data is not None else None,
        input_metadata=TaskInputMetadata(bytes=int(metadata_data["bytes"])) if metadata_data is not None else None,
    )


def export_task_store(task_store: TaskStore) -> list[dict[str, Any]]:
    """导出 Router TaskStore 的父任务、逐文件状态与源缓存引用。"""
    from .tasks import RouterTaskBackend

    records: list[dict[str, Any]] = []
    for record in task_store._records.values():
        if not isinstance(record.backend, RouterTaskBackend):
            raise TypeError("Expected RouterTaskBackend in Router TaskStore")
        files_payload: list[dict[str, Any]] = []
        for file in record.file_states:
            if not isinstance(file.backend, RouterTaskBackend):
                raise TypeError("Expected RouterTaskBackend in Router TaskFileRecord")
            upload_payload = None
            if file.upload is not None:
                upload_payload = {
                    "filename": file.upload.filename,
                    "bytes": file.upload.bytes,
                    "sha256sum": file.upload.sha256sum,
                    "mime_type": file.upload.mime_type,
                    "source_ref": file.upload.file_id,
                }
            files_payload.append(
                {
                    "file_id": file.file_id,
                    "file_name": file.file_name,
                    "execution_id": file.execution_id,
                    "status": file.status,
                    "expired": file.expired,
                    "submitted": file.submitted,
                    "upload": upload_payload,
                    "execution": _serialize_execution(file.execution),
                    "error": file.error.model_dump() if file.error is not None else None,
                    "prepared_sources": list(file.backend._prepared_sources),
                    "task_sources": list(file.backend._task_sources),
                }
            )
        records.append(
            {
                "id": record.id,
                "owner_scope": record.backend.owner_scope,
                "upstream_headers": dict(record.backend.upstream_headers),
                "options": record.options.model_dump(),
                "inputs": [item.model_dump() for item in record.inputs],
                "expires_at": record.expires_at,
                "created_at": record.created_at,
                "status": record.status,
                "finished_at": record.finished_at,
                "finished_epoch": record.finished_epoch,
                "retention_epoch": record.retention_epoch,
                "lease_expires_at": record.lease_expires_at,
                "error": record.error.model_dump() if record.error is not None else None,
                "files": files_payload,
            }
        )
    return records


def restore_task_store(
    state: list[dict[str, Any]],
    *,
    app: FastAPI,
    task_store: TaskStore,
    source_store: SourceFileStore,
    registry: ResourceRegistry,
    finalize: FinalizeExecution,
) -> None:
    """还原 Router TaskStore 并在事件循环内恢复待上传、待投递和在途监控协程。"""
    from .tasks import RouterTaskBackend

    if not isinstance(state, list):
        raise ValueError("Invalid persisted task store")
    for item in state:
        if not isinstance(item, dict):
            raise ValueError("Invalid persisted task record")
        task_id = str(item["id"])
        if task_id in task_store._records:
            raise ValueError("Duplicate persisted task ID")
        owner_scope = str(item["owner_scope"])
        upstream_headers = {str(k): str(v) for k, v in (item.get("upstream_headers") or {}).items()}
        options = UploadTaskOptions.model_validate(item["options"])
        inputs = [TaskFileItem.model_validate(entry) for entry in item["inputs"]]
        raw_files = item["files"]
        if not isinstance(raw_files, list) or len(raw_files) != len(inputs):
            raise ValueError("Persisted task file count mismatch")
        parent_backend = RouterTaskBackend.from_restored(app, owner_scope, upstream_headers, finalize)
        file_states: list[TaskFileRecord] = []
        missing_source_indices: set[int] = set()
        for index, (input_item, raw_file) in enumerate(zip(inputs, raw_files)):
            if not isinstance(raw_file, dict) or raw_file.get("file_id") != input_item.file_id:
                raise ValueError("Persisted task file identity mismatch")
            prepared_sources = [str(ref) for ref in (raw_file.get("prepared_sources") or [])]
            task_sources = [str(ref) for ref in (raw_file.get("task_sources") or [])]
            file_backend = RouterTaskBackend.from_restored(
                app,
                owner_scope,
                upstream_headers,
                finalize,
                prepared_sources=prepared_sources,
                task_sources=task_sources,
            )
            execution_id = str(raw_file["execution_id"])
            submitted = bool(raw_file.get("submitted", False))
            status = raw_file["status"]
            upload_obj: TaskUpload | None = None
            raw_upload = raw_file.get("upload")
            if raw_upload is not None:
                if not isinstance(raw_upload, dict):
                    raise ValueError("Invalid persisted task upload")
                source_ref = raw_upload.get("source_ref")
                if not source_ref and prepared_sources:
                    source_ref = prepared_sources[0]
                stored = source_store.find_file(str(source_ref)) if source_ref else None
                if stored is not None and stored.path.is_file():
                    upload_obj = TaskUpload(
                        path=stored.path,
                        filename=str(raw_upload["filename"]),
                        bytes=int(raw_upload["bytes"]),
                        sha256sum=str(raw_upload["sha256sum"]),
                        mime_type=str(raw_upload.get("mime_type") or "application/octet-stream"),
                        file_id=str(source_ref),
                    )
                elif not submitted and registry.find("job", execution_id) is None:
                    missing_source_indices.add(index)
            file_backend.rebind_restored_sources(source_store)
            error_data = raw_file.get("error")
            file_record = TaskFileRecord(
                backend=file_backend,
                file_id=input_item.file_id,
                file_name=str(raw_file["file_name"]),
                execution_id=execution_id,
                status=status,
                upload=upload_obj,
                expired=bool(raw_file.get("expired", False)),
                execution=_deserialize_execution(raw_file.get("execution")),
                submitted=submitted,
                error=ErrorDetail.model_validate(error_data) if error_data is not None else None,
            )
            file_states.append(file_record)
        parent_error = item.get("error")
        record = TaskRecord(
            id=task_id,
            backend=parent_backend,
            options=options,
            inputs=inputs,
            directory=task_store._root / task_id,
            expires_at=int(item["expires_at"]),
            file_states=file_states,
            created_at=str(item["created_at"]),
            status=item["status"],
            finished_at=item.get("finished_at"),
            finished_epoch=float(item["finished_epoch"]) if item.get("finished_epoch") is not None else None,
            retention_epoch=float(item["retention_epoch"]) if item.get("retention_epoch") is not None else None,
            lease_expires_at=float(item.get("lease_expires_at") or 0),
            error=ErrorDetail.model_validate(parent_error) if parent_error is not None else None,
        )
        task_store.resume_restored_record(record, missing_source_indices=missing_source_indices)


__all__ = [
    "PersistenceMiddleware",
    "RedisStateStore",
    "RedisTransportError",
    "export_task_store",
    "restore_task_store",
]
