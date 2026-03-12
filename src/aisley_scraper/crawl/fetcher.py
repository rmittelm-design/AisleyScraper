from __future__ import annotations

import asyncio
import json
import logging
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
        self._logger = logging.getLogger(__name__)

    async def close(self) -> None:
        await self._client.aclose()

    async def _curl_fetch(self, url: str) -> bytes:
        # Some targets block python clients while allowing curl/browser traffic.
        proc = await asyncio.create_subprocess_exec(
            "curl",
            "-sSL",
            "--max-time",
            str(max(1, self._settings.crawl_request_timeout_sec)),
            "--connect-timeout",
            str(max(1, self._settings.crawl_connect_timeout_sec)),
            "-A",
            self._settings.user_agent or "Mozilla/5.0",
            url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            err = stderr.decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"curl fallback failed for {url}: {err}")
        return stdout

    @staticmethod
    def _should_use_curl_fallback(exc: Exception) -> bool:
        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code in {403, 429}
        return False

    async def get_text(self, url: str) -> str:
        domain = urlparse(url).netloc
        async with self._domain_semaphores[domain]:
            try:
                response = await self._client.get(url)
                response.raise_for_status()
                return response.text
            except Exception as exc:
                if not self._should_use_curl_fallback(exc):
                    raise
                self._logger.info("Using curl fallback for %s", url)
                return (await self._curl_fetch(url)).decode("utf-8", errors="replace")

    async def get_json(self, url: str) -> dict[str, Any]:
        domain = urlparse(url).netloc
        async with self._domain_semaphores[domain]:
            try:
                response = await self._client.get(url)
                response.raise_for_status()
                payload = response.json()
            except Exception as exc:
                if not self._should_use_curl_fallback(exc):
                    raise
                self._logger.info("Using curl fallback for %s", url)
                payload = json.loads((await self._curl_fetch(url)).decode("utf-8", errors="replace"))

            if isinstance(payload, dict):
                return payload
            return {}

    async def get_bytes(self, url: str) -> bytes:
        domain = urlparse(url).netloc
        async with self._domain_semaphores[domain]:
            try:
                response = await self._client.get(url)
                response.raise_for_status()
                return response.content
            except Exception as exc:
                if not self._should_use_curl_fallback(exc):
                    raise
                self._logger.info("Using curl fallback for %s", url)
                return await self._curl_fetch(url)
