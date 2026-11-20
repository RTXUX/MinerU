# Copyright (c) Opendatalab. All rights reserved.
"""V1 Router 的可重载远程 upstream 配置解析与校验。"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import yaml

_FORBIDDEN_UPSTREAM_HEADERS = frozenset(
    {
        "host",
        "content-length",
        "connection",
        "transfer-encoding",
        "upgrade",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "expect",
    }
)


def normalize_base_url(url: str) -> str:
    """统一校验并规范化 HTTP base URL，保留路径前缀且禁止凭证、查询和片段。"""
    normalized = url.strip()
    try:
        parts = urlsplit(normalized)
        parsed = httpx.URL(normalized)
        port = parts.port
        valid = (
            parsed.scheme in {"http", "https"}
            and bool(parsed.host)
            and parts.username is None
            and parts.password is None
            and "?" not in normalized
            and "#" not in normalized
            and not any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in normalized)
            and "\\" not in normalized
        )
        if valid and port == (80 if parsed.scheme == "http" else 443):
            parsed = parsed.copy_with(port=None)
    except (ValueError, httpx.InvalidURL):
        valid = False
    if not valid:
        raise ValueError("Upstream must be an HTTP(S) base URL without credentials, query, or fragment")
    return str(parsed).rstrip("/")


@dataclass(frozen=True)
class UpstreamConfig:
    """保存单个校验通过的远程 upstream 连接、认证头与并发权重。"""

    id: str
    url: str
    headers: dict[str, str] = field(default_factory=dict, repr=False)
    max_concurrent_jobs: int = 1


def upstream_config(value: object) -> UpstreamConfig:
    """校验单个 upstream 配置项并规范化其稳定标识、URL 与请求头。"""
    if isinstance(value, str):
        value = {"url": value}
    if not isinstance(value, dict) or set(value) - {"id", "url", "api_key", "headers", "max_concurrent_jobs"}:
        raise ValueError("Each upstream must contain only id, url, api_key, headers, max_concurrent_jobs")
    raw_url = value.get("url")
    if not isinstance(raw_url, str):
        raise ValueError("Upstream url must be a string")
    url = normalize_base_url(raw_url)
    worker_id = value.get("id", "remote-" + hashlib.sha256(url.encode("utf-8")).hexdigest()[:24])
    if (
        not isinstance(worker_id, str)
        or not worker_id
        or worker_id.startswith("local-")
        or any(char in worker_id for char in "/\\")
        or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in worker_id)
    ):
        raise ValueError("Upstream id must be a non-empty identifier and must not start with local-")
    capacity = value.get("max_concurrent_jobs", 1)
    if type(capacity) is not int or capacity < 1:
        raise ValueError("max_concurrent_jobs must be a positive integer")
    raw_headers = value.get("headers", {})
    if not isinstance(raw_headers, dict) or any(
        not isinstance(k, str) or not isinstance(v, str) for k, v in raw_headers.items()
    ):
        raise ValueError("Upstream headers must be a string mapping")
    headers = {k.lower(): v for k, v in raw_headers.items()}
    if _FORBIDDEN_UPSTREAM_HEADERS.intersection(headers):
        raise ValueError("Upstream headers must not override transport headers")
    key = value.get("api_key")
    if key is not None:
        if not isinstance(key, str) or not key.strip() or "authorization" in headers:
            raise ValueError("api_key must be non-empty and cannot be combined with an Authorization header")
        headers["authorization"] = f"Bearer {key}"
    try:
        for name, content in headers.items():
            if not re.fullmatch(r"[!#$%&'*+.^_`|~0-9a-z-]+", name) or any(
                (ord(c) < 32 and c != "\t") or ord(c) == 127 for c in content
            ):
                raise ValueError
        httpx.Headers(headers)
    except (ValueError, UnicodeError):
        raise ValueError("Invalid upstream headers") from None
    return UpstreamConfig(worker_id, url, headers, capacity)


def load_upstreams(path: str | None, urls: tuple[str, ...] = ()) -> list[UpstreamConfig]:
    """完整读取并校验 YAML upstream 清单，任何错误均不触发部分生效。"""
    entries: list[object] = list(dict.fromkeys(urls))
    if path:
        try:
            document = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        except (OSError, UnicodeError, yaml.YAMLError):
            # YAML 解析异常可能携带配置文件中的明文密钥，统一隐去底层异常文本。
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


__all__ = ["UpstreamConfig", "load_upstreams", "normalize_base_url", "upstream_config"]
