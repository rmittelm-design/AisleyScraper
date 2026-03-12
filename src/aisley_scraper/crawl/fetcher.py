from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any
from urllib.parse import urlparse

import httpx

from aisley_scraper.config import Settings


class Fetcher:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        timeout = httpx.Timeout(
            connect=settings.crawl_connect_timeout_sec,
            read=settings.crawl_request_timeout_sec,
            write=settings.crawl_request_timeout_sec,
            pool=settings.crawl_request_timeout_sec,
        )
        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": settings.user_agent},
        )
        self._domain_semaphores: dict[str, asyncio.Semaphore] = defaultdict(
            lambda: asyncio.Semaphore(self._settings.crawl_per_domain_concurrency)
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def get_text(self, url: str) -> str:
        domain = urlparse(url).netloc
        async with self._domain_semaphores[domain]:
            response = await self._client.get(url)
            response.raise_for_status()
            return response.text

    async def get_json(self, url: str) -> dict[str, Any]:
        domain = urlparse(url).netloc
        async with self._domain_semaphores[domain]:
            response = await self._client.get(url)
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                return payload
            return {}

    async def get_bytes(self, url: str) -> bytes:
        domain = urlparse(url).netloc
        async with self._domain_semaphores[domain]:
            response = await self._client.get(url)
            response.raise_for_status()
            return response.content
