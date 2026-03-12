from aisley_scraper import cli
from aisley_scraper.config import Settings
from aisley_scraper.models import ProductRecord, ScrapeResult, StoreProfile, StoreSeed


def test_dedupe_seeds_by_domain_keeps_first_per_domain() -> None:
    seeds = [
        StoreSeed(store_url="https://example.com", source_id="a"),
        StoreSeed(store_url="https://example.com", source_id="b"),
        StoreSeed(store_url="https://shop.example.org", source_id="c"),
        StoreSeed(store_url="https://SHOP.EXAMPLE.ORG", source_id="d"),
        StoreSeed(store_url="https://another.com", source_id="e"),
    ]

    deduped = cli._dedupe_seeds_by_domain(seeds)

    assert [s.store_url for s in deduped] == [
        "https://example.com",
        "https://shop.example.org",
        "https://another.com",
    ]
    assert [s.source_id for s in deduped] == ["a", "c", "e"]


def test_run_crawl_skips_new_unavailable_products(monkeypatch) -> None:
    settings = Settings(
        LOG_LEVEL="INFO",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
        PERSISTENCE_TARGET="supabase",
    )

    seed = StoreSeed(store_url="https://example.com")
    outcome = ScrapeResult(
        store=StoreProfile(
            store_name="Example",
            website="https://example.com",
            store_type="online",
        ),
        products=[
            ProductRecord(
                product_id="p-1",
                product_handle="p-1",
                item_name="Unavailable Item",
                description=None,
                images=["https://cdn.example.com/1.jpg"],
                unavailable=True,
            ),
            ProductRecord(
                product_id="p-2",
                product_handle="p-2",
                item_name="Available Item",
                description=None,
                images=["https://cdn.example.com/2.jpg"],
                unavailable=False,
            ),
        ],
    )

    class _FakeRestRepo:
        inserted_products: list[str] = []

        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        def ensure_schema(self) -> None:
            return None

        def upsert_store(self, store: StoreProfile) -> int:
            _ = store
            return 1

        def get_product_image_state(self, store_id: int, product_id: str):
            _ = store_id
            _ = product_id
            return None

        def upsert_product(self, store_id: int, product: ProductRecord) -> None:
            _ = store_id
            self.inserted_products.append(product.product_id)

    class _FakeUploader:
        uploaded_for: list[str] = []

        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        def upload_product_images(self, image_urls: list[str], store_id: int, product_id: str) -> list[str]:
            _ = image_urls
            _ = store_id
            self.uploaded_for.append(product_id)
            return [f"https://x.supabase.co/storage/v1/object/public/product-images/{product_id}.jpg"]

        def sync_product_images(
            self,
            current_source_urls: list[str],
            existing_source_urls: list[str],
            existing_supabase_urls: list[str],
            store_id: int,
            product_id: str,
        ) -> list[str]:
            _ = (
                current_source_urls,
                existing_source_urls,
                existing_supabase_urls,
                store_id,
                product_id,
            )
            return []

    class _FakeFetcher:
        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        async def close(self) -> None:
            return None

    async def _fake_enrich_gender_probabilities_for_products(
        *,
        products: list[ProductRecord],
        fetcher: object,
        concurrency: int,
    ) -> None:
        _ = (fetcher, concurrency)
        for product in products:
            if product.gender_label is None and not product.gender_probs_csv:
                product.gender_probs_csv = "0.2,0.5,0.3"

    async def _fake_scrape_many(seeds: list[StoreSeed], _settings: Settings):
        _ = seeds
        _ = _settings
        return [(seed, outcome)]

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "load_store_seeds", lambda path, _settings: [seed])
    monkeypatch.setattr(cli, "scrape_many", _fake_scrape_many)
    monkeypatch.setattr(cli, "SupabaseRestRepository", _FakeRestRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(
        cli,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )

    exit_code = cli.run_crawl(limit=None)

    assert exit_code == 0
    assert _FakeRestRepo.inserted_products == ["p-2"]
    assert _FakeUploader.uploaded_for == ["p-2"]


def test_run_crawl_falls_back_to_rest_without_db_credentials(monkeypatch) -> None:
    settings = Settings(
        LOG_LEVEL="INFO",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
        PERSISTENCE_TARGET="supabase",
    )

    seed = StoreSeed(store_url="https://example.com")
    outcome = ScrapeResult(
        store=StoreProfile(
            store_name="Example",
            website="https://example.com",
            store_type="online",
        ),
        products=[],
    )

    class _FakeRestRepo:
        used = False

        def __init__(self, _settings: Settings) -> None:
            _ = _settings
            _FakeRestRepo.used = True

        def ensure_schema(self) -> None:
            return None

        def upsert_store(self, store: StoreProfile) -> int:
            _ = store
            return 1

        def upsert_product(self, store_id: int, product: ProductRecord) -> None:
            _ = (store_id, product)
            return None

        def get_product_image_state(self, store_id: int, product_id: str):
            _ = (store_id, product_id)
            return None

    class _FakeUploader:
        def __init__(self, _settings: Settings) -> None:
            _ = _settings

    class _FakeFetcher:
        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        async def close(self) -> None:
            return None

    async def _fake_enrich_gender_probabilities_for_products(
        *,
        products: list[ProductRecord],
        fetcher: object,
        concurrency: int,
    ) -> None:
        _ = (products, fetcher, concurrency)
        return None

    async def _fake_scrape_many(seeds: list[StoreSeed], _settings: Settings):
        _ = (seeds, _settings)
        return [(seed, outcome)]

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "load_store_seeds", lambda path, _settings: [seed])
    monkeypatch.setattr(cli, "scrape_many", _fake_scrape_many)
    monkeypatch.setattr(cli, "SupabaseRestRepository", _FakeRestRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(
        cli,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )

    exit_code = cli.run_crawl(limit=1)

    assert exit_code == 0
    assert _FakeRestRepo.used is True


def test_run_crawl_backfills_missing_gender_probs_for_existing_product(monkeypatch) -> None:
    settings = Settings(
        LOG_LEVEL="INFO",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
        PERSISTENCE_TARGET="supabase",
    )

    seed = StoreSeed(store_url="https://example.com")
    outcome = ScrapeResult(
        store=StoreProfile(
            store_name="Example",
            website="https://example.com",
            store_type="online",
        ),
        products=[
            ProductRecord(
                product_id="p-10",
                product_handle="p-10",
                item_name="Existing Item",
                description=None,
                images=["https://cdn.example.com/10.jpg"],
                gender_label=None,
                gender_probs_csv=None,
            )
        ],
    )

    class _FakeRestRepo:
        upserted_gender_probs: str | None = None

        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        def ensure_schema(self) -> None:
            return None

        def upsert_store(self, store: StoreProfile) -> int:
            _ = store
            return 1

        def get_product_image_state(self, store_id: int, product_id: str):
            _ = (store_id, product_id)
            return (["https://cdn.example.com/10.jpg"], ["https://x.supabase.co/p10.jpg"])

        def upsert_product(self, store_id: int, product: ProductRecord) -> None:
            _ = store_id
            _FakeRestRepo.upserted_gender_probs = product.gender_probs_csv

    class _FakeUploader:
        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        def sync_product_images(
            self,
            current_source_urls: list[str],
            existing_source_urls: list[str],
            existing_supabase_urls: list[str],
            store_id: int,
            product_id: str,
        ) -> list[str]:
            _ = (
                current_source_urls,
                existing_source_urls,
                existing_supabase_urls,
                store_id,
                product_id,
            )
            return ["https://x.supabase.co/p10.jpg"]

    class _FakeFetcher:
        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        async def close(self) -> None:
            return None

    async def _fake_enrich_gender_probabilities_for_products(
        *,
        products: list[ProductRecord],
        fetcher: object,
        concurrency: int,
    ) -> None:
        _ = (fetcher, concurrency)
        for product in products:
            product.gender_probs_csv = "0.11,0.22,0.67"

    async def _fake_scrape_many(seeds: list[StoreSeed], _settings: Settings):
        _ = (seeds, _settings)
        return [(seed, outcome)]

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "load_store_seeds", lambda path, _settings: [seed])
    monkeypatch.setattr(cli, "scrape_many", _fake_scrape_many)
    monkeypatch.setattr(cli, "SupabaseRestRepository", _FakeRestRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(
        cli,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )

    exit_code = cli.run_crawl(limit=1)

    assert exit_code == 0
    assert _FakeRestRepo.upserted_gender_probs == "0.11,0.22,0.67"
