from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import time
from collections import OrderedDict, defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from curl_cffi import requests as curl_cffi_requests

from aisley_scraper.config import Settings


_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)


class Fetcher:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._logger = logging.getLogger(__name__)
        self._request_user_agent = (settings.user_agent or "").strip() or _BROWSER_USER_AGENT
        timeout = httpx.Timeout(
            connect=settings.crawl_connect_timeout_sec,
            read=settings.crawl_request_timeout_sec,
            write=settings.crawl_request_timeout_sec,
            pool=settings.crawl_request_timeout_sec,
        )
        limits = httpx.Limits(
            max_connections=max(1, settings.crawl_http_max_connections),
            max_keepalive_connections=max(1, settings.crawl_http_max_keepalive_connections),
        )
        http2_enabled = bool(settings.crawl_http2_enabled)
        try:
            self._client = httpx.AsyncClient(
                timeout=timeout,
                limits=limits,
                http2=http2_enabled,
                follow_redirects=True,
                headers={"User-Agent": self._request_user_agent},
            )
        except ImportError as exc:
            if not http2_enabled:
                raise
            self._logger.warning(
                "HTTP/2 requested but optional 'h2' dependency is missing; falling back to HTTP/1.1"
            )
            self._client = httpx.AsyncClient(
                timeout=timeout,
                limits=limits,
                http2=False,
                follow_redirects=True,
                headers={"User-Agent": self._request_user_agent},
            )
        # Secondary client with HTTP/1.1 and browser-like headers for image fallback fetches.
        self._image_fallback_client = httpx.AsyncClient(
            timeout=timeout,
            limits=limits,
            http2=False,
            follow_redirects=True,
            headers={"User-Agent": _BROWSER_USER_AGENT},
        )
        self._domain_semaphores: dict[str, asyncio.Semaphore] = defaultdict(
            lambda: asyncio.Semaphore(self._settings.crawl_per_domain_concurrency)
        )
        self._byte_domain_semaphores: dict[str, asyncio.Semaphore] = defaultdict(
            lambda: asyncio.Semaphore(
                max(
                    self._settings.crawl_per_domain_concurrency,
                    self._settings.image_validation_concurrency,
                )
            )
        )
        self._byte_cache: OrderedDict[str, bytes] = OrderedDict()
        self._byte_cache_size = 0
        self._byte_cache_max_bytes = max(0, int(self._settings.fetcher_byte_cache_max_mb)) * 1024 * 1024
        self._disk_cache_enabled = bool(self._settings.fetcher_disk_cache_enabled)
        self._disk_cache_dir = Path(self._settings.fetcher_disk_cache_dir)
        self._disk_cache_max_bytes = max(0, int(self._settings.fetcher_disk_cache_max_mb)) * 1024 * 1024
        self._jitter_lock = asyncio.Lock()
        self._rate_limit_lock = asyncio.Lock()
        self._next_global_request_at = 0.0
        self._long_jitter_request_countdown = self._next_long_jitter_countdown()
        if self._disk_cache_enabled:
            self._disk_cache_dir.mkdir(parents=True, exist_ok=True)

    def _reserve_global_request_delay(self, now: float) -> float:
        qps = max(0, int(self._settings.crawl_global_qps))
        if qps <= 0:
            self._next_global_request_at = now
            return 0.0

        scheduled_at = max(now, self._next_global_request_at)
        self._next_global_request_at = scheduled_at + (1.0 / qps)
        return max(0.0, scheduled_at - now)

    def _next_long_jitter_countdown(self) -> int:
        min_requests = max(0, int(self._settings.crawl_long_jitter_every_min_requests))
        max_requests = max(min_requests, int(self._settings.crawl_long_jitter_every_max_requests))
        if max_requests <= 0:
            return 0
        return random.randint(min_requests, max_requests)

    def _next_long_jitter_seconds(self) -> float:
        min_ms = max(0, int(self._settings.crawl_long_jitter_min_ms))
        max_ms = max(min_ms, int(self._settings.crawl_long_jitter_max_ms))
        if max_ms <= 0:
            return 0.0
        return random.uniform(min_ms / 1000.0, max_ms / 1000.0)

    async def _apply_jitter(self) -> None:
        """Apply global pacing and random jitter before fetch requests."""
        async with self._rate_limit_lock:
            rate_delay_sec = self._reserve_global_request_delay(time.monotonic())

        jitter_ms = max(0, int(self._settings.crawl_jitter_ms))
        delay_sec = rate_delay_sec
        if jitter_ms > 0:
            delay_sec += random.uniform(0, jitter_ms / 1000.0)

        async with self._jitter_lock:
            if self._long_jitter_request_countdown > 0:
                self._long_jitter_request_countdown -= 1
                if self._long_jitter_request_countdown == 0:
                    delay_sec += self._next_long_jitter_seconds()
                    self._long_jitter_request_countdown = self._next_long_jitter_countdown()

        if delay_sec > 0:
            await asyncio.sleep(delay_sec)

    @staticmethod
    def _default_referer(url: str) -> str:
        domain = urlparse(url).netloc
        return f"https://{domain}/" if domain else "https://www.google.com/"

    def _text_request_headers(self, url: str) -> dict[str, str]:
        return {
            "User-Agent": self._request_user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": self._default_referer(url),
        }

    def _json_request_headers(self, url: str) -> dict[str, str]:
        return {
            "User-Agent": self._request_user_agent,
            "Accept": "application/json,text/javascript,*/*;q=0.1",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": self._default_referer(url),
        }

    def _disk_cache_path_for_url(self, url: str) -> Path:
        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return self._disk_cache_dir / f"{digest}.img"

    def _write_disk_cache(self, url: str, content: bytes) -> None:
        if not self._disk_cache_enabled:
            return
        path = self._disk_cache_path_for_url(url)
        tmp = path.with_suffix(".tmp")
        tmp.write_bytes(content)
        tmp.replace(path)
        self._enforce_disk_cache_limit()

    def _enforce_disk_cache_limit(self) -> None:
        if not self._disk_cache_enabled or self._disk_cache_max_bytes <= 0:
            return

        files = [p for p in self._disk_cache_dir.glob("*.img") if p.is_file()]
        total_size = sum(file_path.stat().st_size for file_path in files)
        if total_size <= self._disk_cache_max_bytes:
            return

        files.sort(key=lambda p: p.stat().st_mtime)
        for file_path in files:
            if total_size <= self._disk_cache_max_bytes:
                break
            try:
                file_size = file_path.stat().st_size
                file_path.unlink()
                total_size -= file_size
            except FileNotFoundError:
                continue
            except Exception:
                continue

    def _read_disk_cache(self, url: str) -> bytes | None:
        if not self._disk_cache_enabled:
            return None
        path = self._disk_cache_path_for_url(url)
        if not path.exists():
            return None
        try:
            return path.read_bytes()
        except Exception:
            return None

    def _delete_disk_cache(self, url: str) -> None:
        if not self._disk_cache_enabled:
            return
        path = self._disk_cache_path_for_url(url)
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass

    def _set_cached_bytes(self, url: str, content: bytes) -> None:
        if self._byte_cache_max_bytes <= 0:
            return

        content_size = len(content)
        if content_size > self._byte_cache_max_bytes:
            return

        existing = self._byte_cache.pop(url, None)
        if existing is not None:
            self._byte_cache_size -= len(existing)

        self._byte_cache[url] = content
        self._byte_cache_size += content_size

        while self._byte_cache and self._byte_cache_size > self._byte_cache_max_bytes:
            _, evicted = self._byte_cache.popitem(last=False)
            self._byte_cache_size -= len(evicted)

    async def close(self) -> None:
        self.clear_cached_bytes(clear_disk_cache=False)
        await self._image_fallback_client.aclose()
        await self._client.aclose()

    async def _curl_fetch(
        self,
        url: str,
        *,
        user_agent: str | None = None,
        referer: str | None = None,
        accept: str | None = None,
    ) -> bytes:
        # Some targets block python clients while allowing curl/browser traffic.
        effective_user_agent = user_agent or self._request_user_agent or "Mozilla/5.0"
        args = [
            "curl",
            "-sSL",
            "--max-time",
            str(max(1, self._settings.crawl_request_timeout_sec)),
            "--connect-timeout",
            str(max(1, self._settings.crawl_connect_timeout_sec)),
            "-A",
            effective_user_agent,
        ]
        if referer:
            args.extend(["-e", referer])
        if accept:
            args.extend(["-H", f"Accept: {accept}"])
        args.extend(["-H", "Accept-Language: en-US,en;q=0.9"])
        args.append(url)
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            err = stderr.decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"curl fallback failed for {url}: {err}")
        return stdout

    async def _curl_cffi_get_json(self, url: str) -> dict[str, Any]:
        headers = self._json_request_headers(url)

        def _request() -> dict[str, Any]:
            response = curl_cffi_requests.get(
                url,
                impersonate="chrome124",
                timeout=max(1, self._settings.crawl_request_timeout_sec),
                headers=headers,
                allow_redirects=True,
            )
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                return payload
            return {}

        return await asyncio.to_thread(_request)

    @staticmethod
    def _should_use_curl_fallback(exc: Exception) -> bool:
        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code in {403, 429, 500, 502, 503, 504, 520, 522, 524}
        if isinstance(exc, (httpx.TimeoutException, httpx.TransportError)):
            return True
        return False

    @staticmethod
    def _should_use_image_fallback_client(exc: Exception) -> bool:
        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code in {403, 429, 500, 502, 503, 504, 520, 522, 524}
        return isinstance(exc, (httpx.TimeoutException, httpx.TransportError))

    @staticmethod
    def _image_request_headers(url: str) -> dict[str, str]:
        domain = urlparse(url).netloc
        referer = f"https://{domain}/" if domain else "https://www.google.com/"
        return {
            "User-Agent": _BROWSER_USER_AGENT,
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "Referer": referer,
        }

    async def get_text(self, url: str) -> str:
        domain = urlparse(url).netloc
        async with self._domain_semaphores[domain]:
            await self._apply_jitter()
            try:
                response = await self._client.get(url, headers=self._text_request_headers(url))
                response.raise_for_status()
                return response.text
            except Exception as exc:
                if not self._should_use_curl_fallback(exc):
                    raise
                self._logger.info("Using curl fallback for %s", url)
                return (
                    await self._curl_fetch(
                        url,
                        user_agent=_BROWSER_USER_AGENT,
                        referer=self._default_referer(url),
                        accept=self._text_request_headers(url)["Accept"],
                    )
                ).decode("utf-8", errors="replace")

    async def get_json(self, url: str) -> dict[str, Any]:
        domain = urlparse(url).netloc
        async with self._domain_semaphores[domain]:
            await self._apply_jitter()
            curl_cffi_exc: Exception | None = None
            curl_exc: Exception | None = None
            try:
                response = await self._client.get(url, headers=self._json_request_headers(url))
                response.raise_for_status()
                try:
                    payload = response.json()
                    if isinstance(payload, dict):
                        return payload
                    return {}
                except ValueError:
                    # Server returned 200 but with empty/non-JSON body (bot protection).
                    self._logger.info(
                        "Non-JSON response from %s (status=%s, body_prefix=%r), trying curl fallback",
                        url, response.status_code, response.text[:120],
                    )
            except Exception as exc:
                if not self._should_use_curl_fallback(exc):
                    raise
                self._logger.info("HTTP error for %s: %s, trying curl fallback", url, exc)

            try:
                payload = await self._curl_cffi_get_json(url)
                self._logger.info("curl-cffi JSON fallback succeeded for %s", url)
                return payload
            except Exception as exc:
                curl_cffi_exc = exc
                self._logger.info("curl-cffi JSON fallback failed for %s: %s", url, exc)

            # Curl fallback with browser user-agent.
            try:
                raw = (
                    await self._curl_fetch(
                        url,
                        user_agent=_BROWSER_USER_AGENT,
                        referer=self._default_referer(url),
                        accept=self._json_request_headers(url)["Accept"],
                    )
                ).decode("utf-8", errors="replace")
                payload = json.loads(raw)
                if isinstance(payload, dict):
                    return payload
                return {}
            except (ValueError, json.JSONDecodeError) as exc:
                curl_exc = exc
                self._logger.info(
                    "Curl fallback also returned non-JSON for %s (body_prefix=%r)",
                    url, raw[:120] if "raw" in dir() else "",
                )

            raise ValueError(
                f"Could not retrieve JSON from {url} (bot-blocked or not a Shopify store): "
                f"curl_cffi={curl_cffi_exc}; curl={curl_exc}"
            )

    async def get_bytes(self, url: str) -> bytes:
        cached = self._byte_cache.get(url)
        if cached is not None:
            self._byte_cache.move_to_end(url)
            return cached

        disk_cached = self._read_disk_cache(url)
        if disk_cached is not None:
            self._set_cached_bytes(url, disk_cached)
            return disk_cached

        domain = urlparse(url).netloc
        async with self._byte_domain_semaphores[domain]:
            cached = self._byte_cache.get(url)
            if cached is not None:
                self._byte_cache.move_to_end(url)
                return cached
            await self._apply_jitter()
            try:
                response = await self._client.get(url)
                response.raise_for_status()
                content = response.content
                self._set_cached_bytes(url, content)
                self._write_disk_cache(url, content)
                return content
            except Exception as exc:
                fallback_exc: Exception = exc
                if self._should_use_image_fallback_client(exc):
                    try:
                        fallback_response = await self._image_fallback_client.get(
                            url,
                            headers=self._image_request_headers(url),
                        )
                        fallback_response.raise_for_status()
                        content = fallback_response.content
                        self._set_cached_bytes(url, content)
                        self._write_disk_cache(url, content)
                        self._logger.info("Image browser-profile fallback succeeded for %s", url)
                        return content
                    except Exception as secondary_exc:
                        fallback_exc = secondary_exc

                if not self._should_use_curl_fallback(fallback_exc):
                    raise fallback_exc

                self._logger.info("Using curl fallback for %s", url)
                referer_domain = urlparse(url).netloc
                referer = f"https://{referer_domain}/" if referer_domain else None
                content = await self._curl_fetch(
                    url,
                    user_agent=_BROWSER_USER_AGENT,
                    referer=referer,
                    accept="image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                )
                self._set_cached_bytes(url, content)
                self._write_disk_cache(url, content)
                return content

    def get_cached_bytes(self, url: str) -> bytes | None:
        cached = self._byte_cache.get(url)
        if cached is not None:
            self._byte_cache.move_to_end(url)
            return cached
        disk_cached = self._read_disk_cache(url)
        if disk_cached is not None:
            self._set_cached_bytes(url, disk_cached)
            return disk_cached
        return None

    def clear_cached_bytes(self, urls: list[str] | None = None, *, clear_disk_cache: bool = True) -> None:
        if urls is None:
            self._byte_cache.clear()
            self._byte_cache_size = 0
            if clear_disk_cache and self._disk_cache_enabled and self._disk_cache_dir.exists():
                for file_path in self._disk_cache_dir.glob("*.img"):
                    try:
                        file_path.unlink()
                    except Exception:
                        pass
            return
        for url in urls:
            existing = self._byte_cache.pop(url, None)
            if existing is not None:
                self._byte_cache_size -= len(existing)
            if clear_disk_cache:
                self._delete_disk_cache(url)
