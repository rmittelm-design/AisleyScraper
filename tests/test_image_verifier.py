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
    async def _fake_verify_single_image_url(image_url: str, fetcher: object, max_retries: int) -> bool:
        _ = (fetcher, max_retries)
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
