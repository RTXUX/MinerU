"""Validated, reloadable upstream configuration for the V1 router."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import yaml


@dataclass(frozen=True)
class UpstreamConfig:
    id: str
    url: str
    headers: dict[str, str] = field(default_factory=dict, repr=False)
    max_concurrent_jobs: int = 1


def upstream_config(value: object) -> UpstreamConfig:
    if isinstance(value, str):
        value = {"url": value}
    if not isinstance(value, dict) or set(value) - {"id", "url", "api_key", "headers", "max_concurrent_jobs"}:
        raise ValueError("Each upstream must contain only id, url, api_key, headers, max_concurrent_jobs")
    url = value.get("url")
    if not isinstance(url, str):
        raise ValueError("Upstream url must be a string")
    url = url.strip().rstrip("/")
    try:
        parsed = urlsplit(url)
        httpx.URL(url)
        # urllib validates the numeric range only when the port is accessed.
        parsed.port
    except (ValueError, httpx.InvalidURL):
        raise ValueError("Invalid upstream URL") from None
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("Upstream url must be an HTTP(S) base URL without embedded credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("Upstream url must not contain a query or fragment")
    worker_id = value.get("id", "remote-" + hashlib.sha256(url.encode()).hexdigest()[:24])
    if not isinstance(worker_id, str) or not worker_id or worker_id.startswith("local-"):
        raise ValueError("Upstream id must be non-empty and must not start with local-")
    capacity = value.get("max_concurrent_jobs", 1)
    if type(capacity) is not int or capacity < 1:
        raise ValueError("max_concurrent_jobs must be a positive integer")
    raw_headers = value.get("headers", {})
    if not isinstance(raw_headers, dict) or any(
        not isinstance(k, str) or not isinstance(v, str) for k, v in raw_headers.items()
    ):
        raise ValueError("Upstream headers must be a string mapping")
    headers = {k.lower(): v for k, v in raw_headers.items()}
    forbidden = {"host", "content-length", "connection", "transfer-encoding", "upgrade"}
    if forbidden.intersection(headers):
        raise ValueError("Upstream headers must not override transport headers")
    key = value.get("api_key")
    if key is not None:
        if not isinstance(key, str) or not key.strip() or "authorization" in headers:
            raise ValueError("api_key must be non-empty and cannot be combined with an Authorization header")
        headers["authorization"] = f"Bearer {key}"
    try:
        for name, content in headers.items():
            if not re.fullmatch(r"[!#$%&'*+.^_`|~0-9a-z-]+", name) or any(
                ord(c) < 32 and c != "\t" or ord(c) == 127 for c in content
            ):
                raise ValueError
        httpx.Headers(headers)
    except (ValueError, UnicodeError):
        raise ValueError("Invalid upstream headers") from None
    return UpstreamConfig(worker_id, url, headers, capacity)


def load_upstreams(path: str | None, urls: tuple[str, ...] = ()) -> list[UpstreamConfig]:
    entries: list[object] = list(dict.fromkeys(urls))
    if path:
        try:
            document = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        except (OSError, UnicodeError, yaml.YAMLError):
            # YAML parser errors can include credentials from the input document.
            raise ValueError("Cannot read or parse router configuration") from None
        if not isinstance(document, dict) or set(document) != {"upstreams"}:
            raise ValueError("Router configuration must contain an upstreams list")
        if not isinstance(document["upstreams"], list):
            raise ValueError("upstreams must be a list")
        entries.extend(document["upstreams"])
    upstreams = [upstream_config(entry) for entry in entries]
    if len({item.id for item in upstreams}) != len(upstreams):
        raise ValueError("Upstream ids must be unique")
    if len({item.url for item in upstreams}) != len(upstreams):
        raise ValueError("Upstream URLs must be unique")
    return upstreams
