from __future__ import annotations

import asyncio

from aisley_scraper.config import Settings
from aisley_scraper.crawl.fetcher import Fetcher


def test_fetcher_preserves_disk_cache_on_memory_clear_and_close(tmp_path) -> None:
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
        FETCHER_DISK_CACHE_MAX_MB=10,
        FETCHER_BYTE_CACHE_MAX_MB=10,
    )

    fetcher = Fetcher(settings)
    url = "https://cdn.example.com/example.jpg"
    content = b"example-bytes"

    fetcher._set_cached_bytes(url, content)
    fetcher._write_disk_cache(url, content)

    fetcher.clear_cached_bytes([url], clear_disk_cache=False)

    cached = fetcher.get_cached_bytes(url)
    assert cached == content
    assert list(cache_dir.glob("*.img"))

    asyncio.run(fetcher.close())

    assert list(cache_dir.glob("*.img"))