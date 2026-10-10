"""Shared backend names for the synchronous multipart compatibility API."""

from ..types import Tier

BACKEND_TIERS: dict[str, Tier] = {
    "pipeline": "basic",
    "hybrid-auto-engine": "standard",
    "hybrid-http-client": "standard",
    "vlm-auto-engine": "standard",
    "vlm-http-client": "standard",
    "flash": "flash",
}
