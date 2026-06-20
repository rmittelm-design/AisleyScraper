from __future__ import annotations

import asyncio

import httpx

from aisley_scraper.config import Settings
from aisley_scraper.crawl.fetcher import Fetcher


def _disk_cache_settings(cache_dir) -> Settings:
    return Settings(
        LOG_LEVEL="INFO",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
        PERSISTENCE_TARGET="supabase",
        FETCHER_DISK_CACHE_ENABLED=True,
        FETCHER_DISK_CACHE_DIR=str(cache_dir),
        FETCHER_DISK_CACHE_MAX_MB=10,
        FETCHER_BYTE_CACHE_MAX_MB=10,
    )


def test_fetcher_can_preserve_disk_cache_on_close_when_requested(tmp_path) -> None:
    cache_dir = tmp_path / "cache"
    fetcher = Fetcher(_disk_cache_settings(cache_dir))
    url = "https://cdn.example.com/example.jpg"
    content = b"example-bytes"

    fetcher._set_cached_bytes(url, content)
    fetcher._write_disk_cache(url, content)

    fetcher.clear_cached_bytes([url], clear_disk_cache=False)

    cached = fetcher.get_cached_bytes(url)
    assert cached == content
    assert list(cache_dir.glob("*.img"))

    # Opt-out keeps the disk cache for a subsequent fetcher in the same process.
    asyncio.run(fetcher.close(clear_disk_cache=False))

    assert list(cache_dir.glob("*.img"))


def test_fetcher_clears_disk_cache_on_close_by_default(tmp_path) -> None:
    cache_dir = tmp_path / "cache"
    fetcher = Fetcher(_disk_cache_settings(cache_dir))

    fetcher._write_disk_cache("https://cdn.example.com/a.jpg", b"aaaa")
    fetcher._write_disk_cache("https://cdn.example.com/b.jpg", b"bbbb")
    assert list(cache_dir.glob("*.img"))

    # Default teardown reclaims the disk so the cache doesn't linger between runs.
    asyncio.run(fetcher.close())

    assert not list(cache_dir.glob("*.img"))
    assert fetcher._disk_cache_size == 0


def test_disk_cache_evicts_to_low_water_and_tracks_size(tmp_path) -> None:
    cache_dir = tmp_path / "cache"
    settings = Settings(
        LOG_LEVEL="INFO",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
        PERSISTENCE_TARGET="supabase",
        FETCHER_DISK_CACHE_ENABLED=True,
        FETCHER_DISK_CACHE_DIR=str(cache_dir),
        FETCHER_BYTE_CACHE_MAX_MB=10,
    )

    fetcher = Fetcher(settings)
    # Shrink the cap to byte scale so we can exercise eviction cheaply.
    fetcher._disk_cache_max_bytes = 1000
    payload = b"x" * 200

    for i in range(20):
        fetcher._write_disk_cache(f"https://cdn.example.com/{i}.jpg", payload)

    on_disk = sum(p.stat().st_size for p in cache_dir.glob("*.img"))
    # Never exceeds the cap, and eviction trims down to ~80% (the low-water mark)
    # rather than scanning + deleting on every single write.
    assert on_disk <= 1000
    assert on_disk <= int(1000 * 0.8) + len(payload)
    # The running counter stays in sync with what is actually on disk.
    assert fetcher._disk_cache_size == on_disk


def test_disk_cache_write_survives_full_disk(tmp_path, monkeypatch) -> None:
    cache_dir = tmp_path / "cache"
    settings = Settings(
        LOG_LEVEL="INFO",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
        PERSISTENCE_TARGET="supabase",
        FETCHER_DISK_CACHE_ENABLED=True,
        FETCHER_DISK_CACHE_DIR=str(cache_dir),
        FETCHER_BYTE_CACHE_MAX_MB=10,
    )
    fetcher = Fetcher(settings)

    from pathlib import Path

    def _raise_enospc(self, _data):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(Path, "write_bytes", _raise_enospc)

    # Must not raise — a full disk should degrade to memory-only caching.
    fetcher._write_disk_cache("https://cdn.example.com/a.jpg", b"data")
    assert fetcher._disk_cache_size == 0


def test_fetcher_defaults_to_browser_user_agent_when_user_agent_is_empty() -> None:
    settings = Settings(
        LOG_LEVEL="INFO",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
        PERSISTENCE_TARGET="supabase",
        USER_AGENT="",
    )

    fetcher = Fetcher(settings)
    try:
        assert fetcher._client.headers["User-Agent"]
        assert "Mozilla/5.0" in fetcher._client.headers["User-Agent"]
    finally:
        asyncio.run(fetcher.close())


def test_fetcher_global_qps_reservation_spaces_requests() -> None:
    settings = Settings(
        LOG_LEVEL="INFO",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
        PERSISTENCE_TARGET="supabase",
        CRAWL_GLOBAL_QPS=8,
    )

    fetcher = Fetcher(settings)
    try:
        first_delay = fetcher._reserve_global_request_delay(100.0)
        second_delay = fetcher._reserve_global_request_delay(100.0)
        third_delay = fetcher._reserve_global_request_delay(100.1)

        assert first_delay == 0.0
        assert round(second_delay, 3) == 0.125
        assert round(third_delay, 3) == 0.15
    finally:
        asyncio.run(fetcher.close())


def test_get_json_uses_curl_cffi_fallback_after_http_429(monkeypatch) -> None:
    settings = Settings(
        LOG_LEVEL="INFO",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
        PERSISTENCE_TARGET="supabase",
    )

    fetcher = Fetcher(settings)

    class _TooManyRequestsResponse:
        status_code = 429
        text = "blocked"

        def raise_for_status(self) -> None:
            request = httpx.Request("GET", "https://example.com/products.json")
            response = httpx.Response(429, request=request)
            raise httpx.HTTPStatusError("too many requests", request=request, response=response)

    async def _fake_get(url: str, headers=None):
        _ = (url, headers)
        return _TooManyRequestsResponse()

    async def _fake_curl_cffi(url: str):
        assert url == "https://example.com/products.json"
        return {"products": [{"id": 1}]}

    monkeypatch.setattr(fetcher._client, "get", _fake_get)
    monkeypatch.setattr(fetcher, "_curl_cffi_get_json", _fake_curl_cffi)

    try:
        payload = asyncio.run(fetcher.get_json("https://example.com/products.json"))
        assert payload == {"products": [{"id": 1}]}
    finally:
        asyncio.run(fetcher.close())