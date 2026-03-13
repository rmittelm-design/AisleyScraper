from __future__ import annotations

import os


_HF_TOKEN_APPLIED = False


def ensure_hf_token_from_settings() -> None:
    """Populate HF Hub auth token from settings-loaded .env when available.

    Some runtime paths load CLIP models directly and rely on process env vars.
    This helper bridges tokens loaded via pydantic settings into os.environ.
    """

    global _HF_TOKEN_APPLIED
    if _HF_TOKEN_APPLIED:
        return

    existing = (os.environ.get("HF_TOKEN") or "").strip()
    if existing:
        _HF_TOKEN_APPLIED = True
        return

    try:
        from aisley_scraper.config import get_settings

        settings = get_settings()
    except Exception:
        return

    token = (getattr(settings, "hf_token", None) or "").strip()
    if token:
        os.environ["HF_TOKEN"] = token

    _HF_TOKEN_APPLIED = True
