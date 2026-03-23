import asyncio
from urllib.parse import parse_qs, urlparse

from aisley_scraper.config import Settings
from aisley_scraper.crawl import orchestrator
from aisley_scraper.models import ScrapeResult, StoreProfile, StoreSeed


class _FakeFetcher:
    def __init__(self, _settings: Settings) -> None:
        _ = _settings

    async def get_text(self, _url: str) -> str:
        return "<html></html>"

    async def get_json(self, url: str):
        page = int(parse_qs(urlparse(url).query).get("page", ["1"])[0])
        if page == 1:
            return {
                "products": [
                    {
                        "id": 101,
                        "handle": "item-101",
                        "title": "Item 101",
                        "images": [{"src": "https://cdn.example.com/101.jpg"}],
                    }
                ]
            }
        if page == 2:
            return {
                "products": [
                    {
                        "id": 202,
                        "handle": "item-202",
                        "title": "Item 202",
                        "images": [{"src": "https://cdn.example.com/202.jpg"}],
                    }
                ]
            }
        return {"products": []}

    async def close(self) -> None:
        return None


def _settings() -> Settings:
    return Settings(
        LOG_LEVEL="INFO",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
        PERSISTENCE_TARGET="supabase",
        SHOPIFY_PRODUCTS_PAGE_LIMIT=1,
        SHOPIFY_PRODUCTS_MAX_PAGES=10,
    )


def test_scrape_store_fetches_multiple_pages(monkeypatch) -> None:
    settings = _settings()
    seed = StoreSeed(store_url="https://example.com")

    def _fake_classify_store(_homepage: str, base: str, _settings: Settings) -> StoreProfile:
        _ = _settings
        return StoreProfile(store_name="Example", website=base, store_type="online")

    async def _fake_verify_product_images(*, products, fetcher, settings):
        _ = (products, fetcher, settings)
        return None

    async def _fake_enrich_gender_probabilities_for_products(*, products, fetcher, concurrency):
        _ = (products, fetcher, concurrency)
        return None

    monkeypatch.setattr(orchestrator, "classify_store", _fake_classify_store)
    monkeypatch.setattr(orchestrator, "verify_product_images", _fake_verify_product_images)
    monkeypatch.setattr(
        orchestrator,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )

    result = asyncio.run(orchestrator.scrape_store(seed, settings, _FakeFetcher(settings)))

    assert [p.product_id for p in result.products] == ["101", "202"]


def test_scrape_many_stream_without_postprocess_fetches_multiple_pages(monkeypatch) -> None:
    settings = _settings()
    seed = StoreSeed(store_url="https://example.com")

    def _fake_classify_store(_homepage: str, base: str, _settings: Settings) -> StoreProfile:
        _ = _settings
        return StoreProfile(store_name="Example", website=base, store_type="online")

    monkeypatch.setattr(orchestrator, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(orchestrator, "classify_store", _fake_classify_store)

    async def _collect() -> tuple[StoreSeed, ScrapeResult | Exception]:
        async for item in orchestrator.scrape_many_stream(
            [seed], settings, include_postprocess=False
        ):
            return item
        raise RuntimeError("no scrape result emitted")

    _seed, outcome = asyncio.run(_collect())

    assert isinstance(outcome, ScrapeResult)
    assert [p.product_id for p in outcome.products] == ["101", "202"]


def test_scrape_store_continues_on_sparse_pages_until_empty(monkeypatch) -> None:
    settings = Settings(
        LOG_LEVEL="INFO",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
        PERSISTENCE_TARGET="supabase",
        SHOPIFY_PRODUCTS_PAGE_LIMIT=250,
        SHOPIFY_PRODUCTS_MAX_PAGES=10,
    )
    seed = StoreSeed(store_url="https://example.com")

    class _SparsePageFetcher:
        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        async def get_text(self, _url: str) -> str:
            return "<html></html>"

        async def get_json(self, url: str):
            page = int(parse_qs(urlparse(url).query).get("page", ["1"])[0])
            if page == 1:
                return {
                    "products": [
                        {
                            "id": 301,
                            "handle": "item-301",
                            "title": "Item 301",
                            "images": [{"src": "https://cdn.example.com/301.jpg"}],
                        }
                    ]
                }
            if page == 2:
                return {
                    "products": [
                        {
                            "id": 302,
                            "handle": "item-302",
                            "title": "Item 302",
                            "images": [{"src": "https://cdn.example.com/302.jpg"}],
                        }
                    ]
                }
            return {"products": []}

        async def close(self) -> None:
            return None

    def _fake_classify_store(_homepage: str, base: str, _settings: Settings) -> StoreProfile:
        _ = _settings
        return StoreProfile(store_name="Example", website=base, store_type="online")

    async def _fake_verify_product_images(*, products, fetcher, settings):
        _ = (products, fetcher, settings)
        return None

    async def _fake_enrich_gender_probabilities_for_products(*, products, fetcher, concurrency):
        _ = (products, fetcher, concurrency)
        return None

    monkeypatch.setattr(orchestrator, "classify_store", _fake_classify_store)
    monkeypatch.setattr(orchestrator, "verify_product_images", _fake_verify_product_images)
    monkeypatch.setattr(
        orchestrator,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )

    result = asyncio.run(orchestrator.scrape_store(seed, settings, _SparsePageFetcher(settings)))

    assert [p.product_id for p in result.products] == ["301", "302"]


def test_scrape_many_stream_surfaces_postprocess_errors(monkeypatch) -> None:
    settings = _settings()
    seed = StoreSeed(store_url="https://example.com")

    def _fake_classify_store(_homepage: str, base: str, _settings: Settings) -> StoreProfile:
        _ = _settings
        return StoreProfile(store_name="Example", website=base, store_type="online")

    async def _failing_verify_product_images(*, products, fetcher, settings):
        _ = (products, fetcher, settings)
        raise RuntimeError("synthetic verification failure")

    monkeypatch.setattr(orchestrator, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(orchestrator, "classify_store", _fake_classify_store)
    monkeypatch.setattr(orchestrator, "verify_product_images", _failing_verify_product_images)

    async def _collect() -> tuple[StoreSeed, ScrapeResult | Exception]:
        async for item in orchestrator.scrape_many_stream(
            [seed], settings, include_postprocess=True
        ):
            return item
        raise RuntimeError("no scrape result emitted")

    _seed, outcome = asyncio.run(_collect())

    assert isinstance(outcome, Exception)


def test_scrape_store_uses_seed_name_and_address_when_present(monkeypatch) -> None:
    settings = _settings()
    seed = StoreSeed(
        store_url="https://example.com",
        store_name="Seeded Name",
        address="Seeded Address",
    )

    def _fake_classify_store(_homepage: str, base: str, _settings: Settings) -> StoreProfile:
        _ = _settings
        return StoreProfile(
            store_name="Extracted Name",
            website=base,
            store_type="online",
            address="Extracted Address",
        )

    async def _fake_verify_product_images(*, products, fetcher, settings):
        _ = (products, fetcher, settings)
        return None

    async def _fake_enrich_gender_probabilities_for_products(*, products, fetcher, concurrency):
        _ = (products, fetcher, concurrency)
        return None

    monkeypatch.setattr(orchestrator, "classify_store", _fake_classify_store)
    monkeypatch.setattr(orchestrator, "verify_product_images", _fake_verify_product_images)
    monkeypatch.setattr(
        orchestrator,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )

    result = asyncio.run(orchestrator.scrape_store(seed, settings, _FakeFetcher(settings)))

    assert result.store.store_name == "Seeded Name"
    assert result.store.address == "Seeded Address"
