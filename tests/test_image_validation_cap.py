"""verify_product_images(max_images_per_product=N) must only download/validate the
first N images per product (the gallery cost / CDN-throttling source), preserve the
untested gallery tail, and drop a product only when its tested image(s) fail."""
from __future__ import annotations

import asyncio

from aisley_scraper.config import Settings
from aisley_scraper.crawl import image_verifier
from aisley_scraper.crawl.image_verifier import (
    ImageVerificationFailure,
    verify_product_images,
)
from aisley_scraper.models import ProductRecord


def _settings(**over) -> Settings:
    base = dict(
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        IMAGE_VALIDATION_CONCURRENCY="2",
        IMAGE_VALIDATION_QUEUE_MAX_RETRIES="0",
        IMAGE_VALIDATION_ATTEMPT_TIMEOUT_SEC="5",
        IMAGE_VALIDATION_CHUNK_TIMEOUT_SEC="30",
    )
    base.update(over)
    return Settings(**base)


def _prod(pid: str, imgs: list[str]) -> ProductRecord:
    return ProductRecord(
        product_id=pid, product_handle=None, item_name="x", description=None, images=imgs
    )


def _install_fake_validator(monkeypatch):
    """Deterministic per-URL validator: a url containing 'bad' fails, else passes."""
    checked: list[str] = []

    async def _fake(image_url, fetcher, max_retries, min_width, min_height):
        _ = (fetcher, max_retries, min_width, min_height)
        checked.append(image_url)
        if "bad" in image_url:
            raise ImageVerificationFailure(reason="not_a_product_photo", detail="nope")
        return True

    monkeypatch.setattr(image_verifier, "_verify_single_image_url", _fake)
    return checked


def test_cap_downloads_only_first_image_and_keeps_gallery(monkeypatch) -> None:
    checked = _install_fake_validator(monkeypatch)
    products = [
        _prod("p1", ["https://cdn/p1-a.jpg", "https://cdn/p1-b.jpg", "https://cdn/p1-c.jpg"]),
        _prod("p2", ["https://cdn/p2-bad.jpg", "https://cdn/p2-b.jpg"]),  # first fails -> drop
    ]
    asyncio.run(
        verify_product_images(
            products=products, fetcher=object(), settings=_settings(), max_images_per_product=1
        )
    )
    # Only the FIRST image of each product was fetched/validated.
    assert set(checked) == {"https://cdn/p1-a.jpg", "https://cdn/p2-bad.jpg"}
    assert len(checked) == 2
    # p1 kept with its FULL gallery (untested images preserved); p2 dropped.
    assert [p.product_id for p in products] == ["p1"]
    assert products[0].images == [
        "https://cdn/p1-a.jpg",
        "https://cdn/p1-b.jpg",
        "https://cdn/p1-c.jpg",
    ]


def test_no_cap_validates_all_images(monkeypatch) -> None:
    checked = _install_fake_validator(monkeypatch)
    products = [_prod("p1", ["https://cdn/p1-a.jpg", "https://cdn/p1-b.jpg", "https://cdn/p1-c.jpg"])]
    asyncio.run(
        verify_product_images(products=products, fetcher=object(), settings=_settings())
    )
    # Default (no cap): every image is validated — original behavior preserved.
    assert len(checked) == 3
