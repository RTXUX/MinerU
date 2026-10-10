"""Redis snapshots with a renewable, fenced single-router lease."""

from __future__ import annotations

import asyncio
import json
import re
import secrets
from contextlib import suppress
from collections.abc import Callable
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import RedisError

from .proxy import RouterProxyError

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


class RedisStateStore:
    """Persist before acknowledging requests; never write after losing ownership."""

    def __init__(self, url: str, namespace: str, *, lease_seconds: float = 30) -> None:
        if not re.fullmatch(r"[A-Za-z0-9:_-]+", namespace):
            raise ValueError("Redis namespace must contain only letters, digits, colon, underscore or hyphen")
        if lease_seconds < 1:
            raise ValueError("Redis lease must be at least one second")
        self.client = Redis.from_url(url, socket_connect_timeout=5, socket_timeout=5)
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
        return RouterProxyError(503, "router_state_unavailable", "Router persistence or ownership is unavailable")

    async def start(self) -> dict[str, Any] | None:
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
        except RedisError:
            raise RuntimeError("Cannot initialize router Redis persistence") from None
        except (ValueError, TypeError):
            raise RuntimeError("Invalid router Redis snapshot; refusing to overwrite it") from None

    async def assert_owner(self) -> None:
        if self._lost or not self._acquired:
            raise self.unavailable()
        try:
            owner = await self.client.get(self.lease_key)
        except RedisError:
            raise self.unavailable() from None
        if owner != self.token.encode():
            self._lost = True
            raise self.unavailable()

    async def _renew_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.lease_seconds / 3)
                renewed = await self.client.eval(_RENEW, 1, self.lease_key, self.token, int(self.lease_seconds * 1000))
                if not renewed:
                    self._lost = True
                    return
        except RedisError:
            # Ownership may have expired during a network outage. Require a restart.
            self._lost = True

    async def save(self, snapshot: Callable[[], dict[str, Any]]) -> None:
        # Build snapshots inside the lock so an older request can never overwrite
        # newer state. No upload or upstream HTTP operation runs under this lock.
        async with self._save_lock:
            if self._lost or not self._acquired:
                raise self.unavailable()
            encoded = json.dumps(snapshot(), separators=(",", ":"), ensure_ascii=True)
            try:
                saved = await self.client.eval(_SAVE, 2, self.lease_key, self.state_key, self.token, encoded)
            except RedisError:
                raise self.unavailable() from None
            if not saved:
                self._lost = True
                raise self.unavailable()

    async def close(self) -> None:
        if self._heartbeat is not None:
            self._heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await self._heartbeat
        if self._acquired:
            with suppress(RedisError):
                await self.client.eval(_RELEASE, 1, self.lease_key, self.token)
        self._acquired = False
        await self.client.aclose()
