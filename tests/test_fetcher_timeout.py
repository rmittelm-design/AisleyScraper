"""A single http request must be bounded by a hard total wall-clock ceiling.

httpx's read timeout is per-read, so a server that slow-streams one byte at a
time (a Cloudflare "Just a moment" interstitial) resets it forever and the whole
crawl hangs. _get_within_budget wraps every GET in asyncio.wait_for so the hang
becomes a TimeoutError that routes to the curl / curl_cffi fallbacks.
"""
from __future__ import annotations

import asyncio
import time

from aisley_scraper.config import Settings
from aisley_scraper.crawl.fetcher import Fetcher


def _settings(**over) -> Settings:
    base = dict(
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        CRAWL_REQUEST_TOTAL_TIMEOUT_SEC="1",
    )
    base.update(over)
    return Settings(**base)


class _SlowStreamClient:
    """Simulates a server that never finishes sending the body in time."""

    def __init__(self) -> None:
        self.calls = 0

    async def get(self, url: str, **kwargs) -> object:
        self.calls += 1
        await asyncio.sleep(30)  # far beyond the 1s total budget
        return object()


def test_get_within_budget_bounds_a_slow_stream() -> None:
    fetcher = Fetcher(_settings())

    async def _run() -> float:
        client = _SlowStreamClient()
        started = time.monotonic()
        try:
            await fetcher._get_within_budget(client, "https://slow.example.com")
            raise AssertionError("expected the total-timeout ceiling to trip")
        except TimeoutError:
            pass
        finally:
            await fetcher.close()
        return time.monotonic() - started

    elapsed = asyncio.run(_run())
    # Must abandon near the 1s budget, nowhere near the 30s the "server" would take.
    assert elapsed < 5.0, f"request was not bounded (took {elapsed:.1f}s)"


def test_total_timeout_routes_to_curl_fallback() -> None:
    # A tripped total-timeout raises TimeoutError; both fallback gates must accept it
    # so a slow-streamed store retries via curl/curl_cffi instead of failing outright.
    assert Fetcher._should_use_curl_fallback(TimeoutError()) is True
    assert Fetcher._should_use_image_fallback_client(TimeoutError()) is True
    # asyncio.TimeoutError is an alias of TimeoutError since 3.11 — verify explicitly.
    assert Fetcher._should_use_curl_fallback(asyncio.TimeoutError()) is True
