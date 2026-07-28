"""_fetch_all_products must report catalog completeness correctly.

The removal-marking feature depends on this: an INCOMPLETE scrape (cap, block,
error, anomalous 200) must return complete=False so callers don't mark the
un-scraped tail unavailable.
"""
from __future__ import annotations

import asyncio
import re

from aisley_scraper.config import Settings
from aisley_scraper.crawl import orchestrator


def _settings(**over) -> Settings:
    base = dict(
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="k",
        SUPABASE_STORAGE_BUCKET="b",
        SUPABASE_STORAGE_PATH="p",
        SHOPIFY_PRODUCTS_PAGE_LIMIT="2",
        SHOPIFY_PRODUCTS_MAX_PAGES="50",
        SHOPIFY_PRODUCTS_MAX_ITEMS_PER_STORE="0",
        MIN_PRODUCT_IMAGES="1",
    )
    base.update(over)
    return Settings(**base)


def _prod(pid: int) -> dict:
    return {
        "id": pid,
        "title": f"Blue Shirt {pid}",
        "handle": f"blue-shirt-{pid}",
        "body_html": "A nice shirt.",
        "vendor": "Brand",
        "product_type": "Shirts",
        "updated_at": "2024-01-01T00:00:00Z",
        "images": [
            {"src": f"https://cdn.example.com/{pid}-a.jpg"},
            {"src": f"https://cdn.example.com/{pid}-b.jpg"},
        ],
        "variants": [{"price": "20.00", "available": True, "sku": f"S{pid}"}],
        "options": [],
    }


class _FakeFetcher:
    """get_json serves a canned payload per page; raises past the last page."""

    def __init__(self, pages: list[dict]) -> None:
        self.pages = pages

    async def get_json(self, url: str) -> dict:
        page = int(re.search(r"page=(\d+)", url).group(1))
        idx = page - 1
        if 0 <= idx < len(self.pages):
            return self.pages[idx]
        raise RuntimeError("404 past last page")  # simulates get_json raising


def _run(pages, settings=None):
    settings = settings or _settings()
    fetcher = _FakeFetcher(pages)
    return asyncio.run(
        orchestrator._fetch_all_products(
            base="https://shop.example.com", settings=settings, fetcher=fetcher
        )
    )


def test_natural_empty_page_is_complete():
    # page1 has products, page2 is a well-formed empty products.json -> complete.
    products, complete = _run([{"products": [_prod(1), _prod(2)]}, {"products": []}])
    assert complete is True
    assert len(products) == 2


def test_repeat_pages_is_complete():
    # store ignores ?page and repeats -> no-new-products break -> complete.
    products, complete = _run([{"products": [_prod(1)]}, {"products": [_prod(1)]}])
    assert complete is True


def test_item_cap_is_incomplete():
    # cap hit before catalog exhausted -> NOT complete (truncated tail).
    _, complete = _run(
        [{"products": [_prod(1), _prod(2)]}],
        settings=_settings(SHOPIFY_PRODUCTS_MAX_ITEMS_PER_STORE="1"),
    )
    assert complete is False


def test_anomalous_200_without_products_key_is_incomplete():
    # page2 is a 200 that lacks a products array (WAF/block) -> NOT complete.
    _, complete = _run([{"products": [_prod(1), _prod(2)]}, {"error": "blocked"}])
    assert complete is False


def test_error_past_first_page_is_incomplete():
    # page1 fills a full page (==limit) so pagination continues; page2 raises
    # (past last page) -> kept but NOT complete.
    _, complete = _run([{"products": [_prod(1), _prod(2)]}])  # only 1 page served
    assert complete is False


def test_first_page_error_raises():
    fetcher = _FakeFetcher([])  # page1 raises
    try:
        asyncio.run(
            orchestrator._fetch_all_products(
                base="https://shop.example.com", settings=_settings(), fetcher=fetcher
            )
        )
        raise AssertionError("expected first-page error to propagate")
    except RuntimeError:
        pass
