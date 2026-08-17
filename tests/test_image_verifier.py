import asyncio

from aisley_scraper.config import Settings
from aisley_scraper.crawl import image_verifier
from aisley_scraper.models import ProductRecord


def _settings() -> Settings:
    return Settings(
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
        IMAGE_VALIDATION_ENABLED=True,
        IMAGE_VALIDATION_CONCURRENCY=4,
        IMAGE_VALIDATION_MAX_RETRIES=0,
    )


def test_verify_product_images_drops_products_without_valid_images(monkeypatch) -> None:
    async def _fake_verify_single_image_url(image_url: str, fetcher: object, max_retries: int, min_width: int, min_height: int) -> bool:
        _ = (fetcher, max_retries, min_width, min_height)
        return image_url.endswith("keep.jpg")

    monkeypatch.setattr(image_verifier, "_verify_single_image_url", _fake_verify_single_image_url)

    products = [
        ProductRecord(
            product_id="1",
            product_handle="a",
            item_name="A",
            description=None,
            images=["https://cdn.example.com/drop.jpg"],
        ),
        ProductRecord(
            product_id="2",
            product_handle="b",
            item_name="B",
            description=None,
            images=["https://cdn.example.com/keep.jpg", "https://cdn.example.com/drop.jpg"],
        ),
    ]

    asyncio.run(
        image_verifier.verify_product_images(
            products=products,
            fetcher=object(),
            settings=_settings(),
        )
    )

    assert len(products) == 1
    assert products[0].product_id == "2"
    assert products[0].images == ["https://cdn.example.com/keep.jpg"]


def test_verify_product_images_preserves_products_on_network_only_failures(monkeypatch) -> None:
    async def _always_timeout(image_url: str, fetcher: object, max_retries: int, min_width: int, min_height: int) -> bool:
        _ = (image_url, fetcher, max_retries, min_width, min_height)
        raise asyncio.TimeoutError()

    monkeypatch.setattr(image_verifier, "_verify_single_image_url", _always_timeout)

    products = [
        ProductRecord(
            product_id="1",
            product_handle="a",
            item_name="A",
            description=None,
            images=["https://cdn.example.com/a.jpg"],
        )
    ]

    settings = _settings().model_copy(update={"image_validation_queue_max_retries": 0})
    asyncio.run(
        image_verifier.verify_product_images(
            products=products,
            fetcher=object(),
            settings=settings,
        )
    )

    assert len(products) == 1
    assert products[0].images == ["https://cdn.example.com/a.jpg"]


def test_verify_product_images_drops_products_on_hard_validation_failures(monkeypatch) -> None:
    async def _always_blurry(image_url: str, fetcher: object, max_retries: int, min_width: int, min_height: int) -> bool:
        _ = (image_url, fetcher, max_retries, min_width, min_height)
        raise image_verifier.ImageVerificationFailure("image_too_blurry", "too blurry")

    monkeypatch.setattr(image_verifier, "_verify_single_image_url", _always_blurry)

    products = [
        ProductRecord(
            product_id="1",
            product_handle="a",
            item_name="A",
            description=None,
            images=["https://cdn.example.com/a.jpg"],
        )
    ]

    settings = _settings().model_copy(update={"image_validation_queue_max_retries": 0})
    asyncio.run(
        image_verifier.verify_product_images(
            products=products,
            fetcher=object(),
            settings=settings,
        )
    )

    assert products == []


def test_verify_first_image_product_validation_keeps_if_any_sampled_image_passes(monkeypatch) -> None:
    # A non-product lead image must NOT drop the item when a later sampled image
    # is a valid product photo (max-over-images).
    called_urls: list[str] = []

    class _FakeFetcher:
        async def get_bytes(self, image_url: str) -> bytes:
            called_urls.append(image_url)
            return image_url.encode("utf-8")

    def _fake_validate_product_photo_only(
        *, content: bytes, filename: str, min_product_prob: float, **_kwargs
    ):
        _ = (filename, min_product_prob)
        text = content.decode("utf-8")
        if text.endswith("first.jpg"):
            raise image_verifier.ImageValidationFailure("not_a_product_photo", "not product")
        return {"ok": True, "product": {"product_prob": 0.97}}

    monkeypatch.setattr(image_verifier, "validate_product_photo_only", _fake_validate_product_photo_only)

    products = [
        ProductRecord(
            product_id="1",
            product_handle="a",
            item_name="A",
            description=None,
            images=["https://cdn.example.com/first.jpg", "https://cdn.example.com/second.jpg"],
        )
    ]

    asyncio.run(
        image_verifier.verify_first_image_product_validation(
            products=products,
            fetcher=_FakeFetcher(),
            settings=_settings(),
        )
    )

    # Item kept (second image passed); both lead images were sampled.
    assert len(products) == 1
    assert called_urls == [
        "https://cdn.example.com/first.jpg",
        "https://cdn.example.com/second.jpg",
    ]


def test_verify_first_image_product_validation_drops_only_when_all_sampled_fail(monkeypatch) -> None:
    class _FakeFetcher:
        async def get_bytes(self, image_url: str) -> bytes:
            return image_url.encode("utf-8")

    def _fake_validate_product_photo_only(
        *, content: bytes, filename: str, min_product_prob: float, **_kwargs
    ):
        _ = (content, filename, min_product_prob)
        raise image_verifier.ImageValidationFailure(
            "not_a_product_photo", "not product", {"product_prob": 0.2}
        )

    monkeypatch.setattr(image_verifier, "validate_product_photo_only", _fake_validate_product_photo_only)

    products = [
        ProductRecord(
            product_id="1",
            product_handle="a",
            item_name="A",
            description=None,
            images=["https://cdn.example.com/a.jpg", "https://cdn.example.com/b.jpg"],
        )
    ]

    asyncio.run(
        image_verifier.verify_first_image_product_validation(
            products=products,
            fetcher=_FakeFetcher(),
            settings=_settings(),
        )
    )

    assert products == []


def test_verify_first_image_product_validation_preserves_on_timeout(monkeypatch) -> None:
    class _FakeFetcher:
        async def get_bytes(self, image_url: str) -> bytes:
            _ = image_url
            raise asyncio.TimeoutError()

    def _fake_validate_product_photo_only(
        *, content: bytes, filename: str, min_product_prob: float, **_kwargs
    ):
        _ = (content, filename, min_product_prob)
        return {"ok": True}

    monkeypatch.setattr(image_verifier, "validate_product_photo_only", _fake_validate_product_photo_only)

    products = [
        ProductRecord(
            product_id="1",
            product_handle="a",
            item_name="A",
            description=None,
            images=["https://cdn.example.com/first.jpg"],
        )
    ]

    asyncio.run(
        image_verifier.verify_first_image_product_validation(
            products=products,
            fetcher=_FakeFetcher(),
            settings=_settings(),
        )
    )

    assert len(products) == 1
    assert products[0].images == ["https://cdn.example.com/first.jpg"]


def test_verify_first_image_product_validation_chunk_ceiling_caps_cdn_hang(monkeypatch) -> None:
    """A CDN that accepts the socket but never responds makes per-product validation
    hang forever. The chunk wall-clock ceiling must cap the whole chunk and PRESERVE
    every undecided product instead of hanging or dropping them."""
    async def _hang(*_args, **_kwargs):
        await asyncio.sleep(3600)  # never returns — simulates a stalled CDN

    monkeypatch.setattr(image_verifier, "evaluate_first_image_product_validation", _hang)

    settings = Settings(
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
        IMAGE_VALIDATION_ENABLED=True,
        IMAGE_VALIDATION_CONCURRENCY=2,
        IMAGE_VALIDATION_CHUNK_TIMEOUT_SEC=0.3,
    )
    products = [
        ProductRecord(
            product_id=str(i), product_handle=None, item_name="X",
            description=None, images=[f"https://cdn.example.com/x{i}.jpg"],
        )
        for i in range(5)
    ]

    async def _run() -> None:
        # Outer guard: if the chunk ceiling were broken this hangs and the test fails
        # loudly instead of stalling forever.
        await asyncio.wait_for(
            image_verifier.verify_first_image_product_validation(
                products=products, fetcher=object(), settings=settings
            ),
            timeout=5.0,
        )

    asyncio.run(_run())
    # Capped fast, and all 5 preserved (network/transient) — none dropped by the stall.
    assert len(products) == 5
