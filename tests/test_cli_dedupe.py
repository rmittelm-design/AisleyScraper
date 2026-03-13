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

    async def _fake_verify_product_images(*, products: list[ProductRecord], fetcher: object, settings: Settings):
        _ = (products, fetcher, settings)
        return None

    async def _fake_scrape_many_stream(
        seeds: list[StoreSeed],
        _settings: Settings,
        *,
        include_postprocess: bool = True,
    ):
        _ = (seeds, _settings, include_postprocess)
        yield (seed, outcome)

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "load_store_seeds", lambda path, _settings: [seed])
    monkeypatch.setattr(cli, "scrape_many_stream", _fake_scrape_many_stream)
    monkeypatch.setattr(cli, "SupabaseRestRepository", _FakeRestRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "verify_product_images", _fake_verify_product_images)
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

    async def _fake_verify_product_images(*, products: list[ProductRecord], fetcher: object, settings: Settings):
        _ = (products, fetcher, settings)
        return None

    async def _fake_scrape_many_stream(
        seeds: list[StoreSeed],
        _settings: Settings,
        *,
        include_postprocess: bool = True,
    ):
        _ = (seeds, _settings, include_postprocess)
        yield (seed, outcome)

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "load_store_seeds", lambda path, _settings: [seed])
    monkeypatch.setattr(cli, "scrape_many_stream", _fake_scrape_many_stream)
    monkeypatch.setattr(cli, "SupabaseRestRepository", _FakeRestRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "verify_product_images", _fake_verify_product_images)
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

    async def _fake_verify_product_images(*, products: list[ProductRecord], fetcher: object, settings: Settings):
        _ = (products, fetcher, settings)
        return None

    async def _fake_scrape_many_stream(
        seeds: list[StoreSeed],
        _settings: Settings,
        *,
        include_postprocess: bool = True,
    ):
        _ = (seeds, _settings, include_postprocess)
        yield (seed, outcome)

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "load_store_seeds", lambda path, _settings: [seed])
    monkeypatch.setattr(cli, "scrape_many_stream", _fake_scrape_many_stream)
    monkeypatch.setattr(cli, "SupabaseRestRepository", _FakeRestRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "verify_product_images", _fake_verify_product_images)
    monkeypatch.setattr(
        cli,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )

    exit_code = cli.run_crawl(limit=1)

    assert exit_code == 0
    assert _FakeRestRepo.upserted_gender_probs == "0.11,0.22,0.67"


def test_run_crawl_recovers_after_transient_enrich_failure_for_existing_product(monkeypatch) -> None:
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
                product_id="p-20",
                product_handle="p-20",
                item_name="Existing Item 1",
                description=None,
                images=["https://cdn.example.com/20.jpg"],
                gender_label=None,
                gender_probs_csv=None,
            ),
            ProductRecord(
                product_id="p-21",
                product_handle="p-21",
                item_name="Existing Item 2",
                description=None,
                images=["https://cdn.example.com/21.jpg"],
                gender_label=None,
                gender_probs_csv=None,
            ),
        ],
    )

    class _FakeRestRepo:
        final_upserts: list[str] = []

        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        def ensure_schema(self) -> None:
            return None

        def upsert_store(self, store: StoreProfile) -> int:
            _ = store
            return 1

        def get_product_image_state(self, store_id: int, product_id: str):
            _ = store_id
            return ([f"https://cdn.example.com/{product_id[-2:]}.jpg"], [])

        def upsert_product(self, store_id: int, product: ProductRecord) -> None:
            _ = store_id
            self.final_upserts.append(product.product_id)

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
            *,
            delete_stale: bool = True,
        ) -> list[str]:
            _ = (
                current_source_urls,
                existing_source_urls,
                existing_supabase_urls,
                store_id,
                product_id,
                delete_stale,
            )
            return [f"https://x.supabase.co/{product_id}.jpg"]

    class _FakeFetcher:
        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        async def close(self) -> None:
            return None

    enrich_calls = {"n": 0}

    async def _fake_enrich_gender_probabilities_for_products(
        *,
        products: list[ProductRecord],
        fetcher: object,
        concurrency: int,
    ) -> None:
        _ = (fetcher, concurrency)
        enrich_calls["n"] += 1
        if enrich_calls["n"] == 1:
            raise RuntimeError("synthetic transient enrich failure")
        for product in products:
            product.gender_probs_csv = "0.10,0.80,0.10"

    async def _fake_verify_product_images(*, products: list[ProductRecord], fetcher: object, settings: Settings):
        _ = (fetcher, settings)
        # Trigger fallback branch for all products.
        for product in products:
            product.images = []

    async def _fake_scrape_many_stream(
        seeds: list[StoreSeed],
        _settings: Settings,
        *,
        include_postprocess: bool = True,
    ):
        _ = (seeds, _settings, include_postprocess)
        yield (seed, outcome)

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "load_store_seeds", lambda path, _settings: [seed])
    monkeypatch.setattr(cli, "scrape_many_stream", _fake_scrape_many_stream)
    monkeypatch.setattr(cli, "SupabaseRestRepository", _FakeRestRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "verify_product_images", _fake_verify_product_images)
    monkeypatch.setattr(
        cli,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )

    exit_code = cli.run_crawl(limit=1)

    assert exit_code == 0
    # Existing rows are no longer placeholder-upserted early; finalize should still persist both.
    assert _FakeRestRepo.final_upserts.count("p-20") >= 1
    assert _FakeRestRepo.final_upserts.count("p-21") >= 1


def test_run_crawl_marks_store_failed_when_final_upsert_never_succeeds(monkeypatch, capsys) -> None:
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
                product_id="p-30",
                product_handle="p-30",
                item_name="Item",
                description=None,
                images=["https://cdn.example.com/30.jpg"],
                gender_label="female",
            )
        ],
    )

    class _FakeRestRepo:
        call_count = 0

        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        def ensure_schema(self) -> None:
            return None

        def upsert_store(self, store: StoreProfile) -> int:
            _ = store
            return 1

        def get_product_image_state(self, store_id: int, product_id: str):
            _ = (store_id, product_id)
            return None

        def upsert_product(self, store_id: int, product: ProductRecord) -> None:
            _ = (store_id, product)
            _FakeRestRepo.call_count += 1
            # Fail all finalize retries.
            if _FakeRestRepo.call_count >= 1:
                raise RuntimeError("synthetic final upsert failure")

    class _FakeUploader:
        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        def upload_product_images(self, image_urls: list[str], store_id: int, product_id: str) -> list[str]:
            _ = (image_urls, store_id, product_id)
            return ["https://x.supabase.co/storage/v1/object/public/product-images/p-30.jpg"]

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
            product.gender_probs_csv = "0,1.0,0"

    async def _fake_verify_product_images(*, products: list[ProductRecord], fetcher: object, settings: Settings):
        _ = (products, fetcher, settings)
        return None

    async def _fake_scrape_many_stream(
        seeds: list[StoreSeed],
        _settings: Settings,
        *,
        include_postprocess: bool = True,
    ):
        _ = (seeds, _settings, include_postprocess)
        yield (seed, outcome)

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "load_store_seeds", lambda path, _settings: [seed])
    monkeypatch.setattr(cli, "scrape_many_stream", _fake_scrape_many_stream)
    monkeypatch.setattr(cli, "SupabaseRestRepository", _FakeRestRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "verify_product_images", _fake_verify_product_images)
    monkeypatch.setattr(
        cli,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )

    exit_code = cli.run_crawl(limit=1)
    output = capsys.readouterr().out

    # Store persistence should be marked failed, not silently reported as success.
    assert exit_code == 0
    assert "Crawled 0/1 stores successfully" in output


def test_run_crawl_fallback_restored_images_get_gender_probs(monkeypatch) -> None:
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
                product_id="fallback-1",
                product_handle="fallback-1",
                item_name="Fallback Item",
                description=None,
                images=["https://cdn.example.com/fallback.jpg"],
                gender_label=None,
            )
        ],
    )

    class _FakeRestRepo:
        last_gender_probs: str | None = None

        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        def ensure_schema(self) -> None:
            return None

        def upsert_store(self, store: StoreProfile) -> int:
            _ = store
            return 1

        def get_product_image_state(self, store_id: int, product_id: str):
            _ = (store_id, product_id)
            return None

        def upsert_product(self, store_id: int, product: ProductRecord) -> None:
            _ = store_id
            if product.product_id == "fallback-1":
                _FakeRestRepo.last_gender_probs = product.gender_probs_csv

    class _FakeUploader:
        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        def upload_product_images(self, image_urls: list[str], store_id: int, product_id: str) -> list[str]:
            _ = (store_id, product_id)
            return [f"https://x.supabase.co/storage/{idx}.jpg" for idx, _ in enumerate(image_urls, start=1)]

    class _FakeFetcher:
        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        async def close(self) -> None:
            return None

    async def _fake_verify_product_images(*, products: list[ProductRecord], fetcher: object, settings: Settings):
        _ = (fetcher, settings)
        products[:] = []

    async def _fake_enrich_gender_probabilities_for_products(
        *,
        products: list[ProductRecord],
        fetcher: object,
        concurrency: int,
    ) -> None:
        _ = (fetcher, concurrency)
        for product in products:
            if product.images:
                product.gender_probs_csv = "0.3,0.4,0.3"

    async def _fake_scrape_many_stream(
        seeds: list[StoreSeed],
        _settings: Settings,
        *,
        include_postprocess: bool = True,
    ):
        _ = (seeds, _settings, include_postprocess)
        yield (seed, outcome)

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "load_store_seeds", lambda path, _settings: [seed])
    monkeypatch.setattr(cli, "scrape_many_stream", _fake_scrape_many_stream)
    monkeypatch.setattr(cli, "SupabaseRestRepository", _FakeRestRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "verify_product_images", _fake_verify_product_images)
    monkeypatch.setattr(
        cli,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )

    exit_code = cli.run_crawl(limit=1)

    assert exit_code == 0
    assert _FakeRestRepo.last_gender_probs == "0.3,0.4,0.3"


def test_run_crawl_fallback_restored_images_get_gender_probs_when_verifier_empties_images(
    monkeypatch,
) -> None:
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
                product_id="fallback-2",
                product_handle="fallback-2",
                item_name="Fallback Item",
                description=None,
                images=["https://cdn.example.com/fallback2.jpg"],
                gender_label=None,
            )
        ],
    )

    class _FakeRestRepo:
        last_gender_probs: str | None = None

        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        def ensure_schema(self) -> None:
            return None

        def upsert_store(self, store: StoreProfile) -> int:
            _ = store
            return 1

        def get_product_image_state(self, store_id: int, product_id: str):
            _ = (store_id, product_id)
            return None

        def upsert_product(self, store_id: int, product: ProductRecord) -> None:
            _ = store_id
            if product.product_id == "fallback-2":
                _FakeRestRepo.last_gender_probs = product.gender_probs_csv

    class _FakeUploader:
        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        def upload_product_images(self, image_urls: list[str], store_id: int, product_id: str) -> list[str]:
            _ = (store_id, product_id)
            return [f"https://x.supabase.co/storage/{idx}.jpg" for idx, _ in enumerate(image_urls, start=1)]

    class _FakeFetcher:
        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        async def close(self) -> None:
            return None

    async def _fake_verify_product_images(*, products: list[ProductRecord], fetcher: object, settings: Settings):
        _ = (fetcher, settings)
        # Simulate verifier clearing product images in-place.
        for product in products:
            product.images = []

    async def _fake_enrich_gender_probabilities_for_products(
        *,
        products: list[ProductRecord],
        fetcher: object,
        concurrency: int,
    ) -> None:
        _ = (fetcher, concurrency)
        for product in products:
            if product.images:
                product.gender_probs_csv = "0.25,0.5,0.25"

    async def _fake_scrape_many_stream(
        seeds: list[StoreSeed],
        _settings: Settings,
        *,
        include_postprocess: bool = True,
    ):
        _ = (seeds, _settings, include_postprocess)
        yield (seed, outcome)

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "load_store_seeds", lambda path, _settings: [seed])
    monkeypatch.setattr(cli, "scrape_many_stream", _fake_scrape_many_stream)
    monkeypatch.setattr(cli, "SupabaseRestRepository", _FakeRestRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "verify_product_images", _fake_verify_product_images)
    monkeypatch.setattr(
        cli,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )

    exit_code = cli.run_crawl(limit=1)

    assert exit_code == 0
    assert _FakeRestRepo.last_gender_probs == "0.25,0.5,0.25"


def test_run_crawl_repairs_transient_upload_failure_in_finalize(monkeypatch) -> None:
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
                product_id="repair-1",
                product_handle="repair-1",
                item_name="Repair Item",
                description=None,
                images=["https://cdn.example.com/repair.jpg"],
                gender_label=None,
            )
        ],
    )

    class _FakeRestRepo:
        last_supa_len = 0

        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        def ensure_schema(self) -> None:
            return None

        def upsert_store(self, store: StoreProfile) -> int:
            _ = store
            return 1

        def get_product_image_state(self, store_id: int, product_id: str):
            _ = (store_id, product_id)
            return None

        def upsert_product(self, store_id: int, product: ProductRecord) -> None:
            _ = store_id
            if product.product_id == "repair-1":
                _FakeRestRepo.last_supa_len = len(product.supabase_images)

    class _FakeUploader:
        calls = 0

        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        def upload_product_images(self, image_urls: list[str], store_id: int, product_id: str) -> list[str]:
            _ = (image_urls, store_id, product_id)
            _FakeUploader.calls += 1
            if _FakeUploader.calls == 1:
                return []
            return ["https://x.supabase.co/storage/repair-1.jpg"]

    class _FakeFetcher:
        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        async def close(self) -> None:
            return None

    async def _fake_verify_product_images(*, products: list[ProductRecord], fetcher: object, settings: Settings):
        _ = (products, fetcher, settings)
        return None

    async def _fake_enrich_gender_probabilities_for_products(
        *,
        products: list[ProductRecord],
        fetcher: object,
        concurrency: int,
    ) -> None:
        _ = (fetcher, concurrency)
        for product in products:
            product.gender_probs_csv = "0.2,0.6,0.2"

    async def _fake_scrape_many_stream(
        seeds: list[StoreSeed],
        _settings: Settings,
        *,
        include_postprocess: bool = True,
    ):
        _ = (seeds, _settings, include_postprocess)
        yield (seed, outcome)

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "load_store_seeds", lambda path, _settings: [seed])
    monkeypatch.setattr(cli, "scrape_many_stream", _fake_scrape_many_stream)
    monkeypatch.setattr(cli, "SupabaseRestRepository", _FakeRestRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "verify_product_images", _fake_verify_product_images)
    monkeypatch.setattr(
        cli,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )

    exit_code = cli.run_crawl(limit=1)

    assert exit_code == 0
    assert _FakeUploader.calls >= 2
    assert _FakeRestRepo.last_supa_len == 1


def test_run_crawl_cleans_orphan_uploads_when_final_upsert_fails(monkeypatch) -> None:
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
                product_id="cleanup-1",
                product_handle="cleanup-1",
                item_name="Cleanup Item",
                description=None,
                images=["https://cdn.example.com/c1.jpg"],
            )
        ],
    )

    class _FakeRestRepo:
        call_count = 0

        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        def ensure_schema(self) -> None:
            return None

        def upsert_store(self, store: StoreProfile) -> int:
            _ = store
            return 1

        def get_product_image_state(self, store_id: int, product_id: str):
            _ = (store_id, product_id)
            return None

        def upsert_product(self, store_id: int, product: ProductRecord) -> None:
            _ = (store_id, product)
            _FakeRestRepo.call_count += 1
            # Finalize upserts fail.
            if _FakeRestRepo.call_count >= 1:
                raise RuntimeError("synthetic finalize failure")

    class _FakeUploader:
        deleted: list[str] = []

        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        def upload_product_images(self, image_urls: list[str], store_id: int, product_id: str) -> list[str]:
            _ = (image_urls, store_id, product_id)
            return ["https://x.supabase.co/storage/new-cleanup-1.jpg"]

        def delete_images(self, public_urls: list[str]) -> None:
            self.deleted.extend(public_urls)

    class _FakeFetcher:
        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        async def close(self) -> None:
            return None

    async def _fake_verify_product_images(*, products: list[ProductRecord], fetcher: object, settings: Settings):
        _ = (products, fetcher, settings)
        return None

    async def _fake_enrich_gender_probabilities_for_products(
        *,
        products: list[ProductRecord],
        fetcher: object,
        concurrency: int,
    ) -> None:
        _ = (fetcher, concurrency)
        for product in products:
            product.gender_probs_csv = "0.4,0.5,0.1"

    async def _fake_scrape_many_stream(
        seeds: list[StoreSeed],
        _settings: Settings,
        *,
        include_postprocess: bool = True,
    ):
        _ = (seeds, _settings, include_postprocess)
        yield (seed, outcome)

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "load_store_seeds", lambda path, _settings: [seed])
    monkeypatch.setattr(cli, "scrape_many_stream", _fake_scrape_many_stream)
    monkeypatch.setattr(cli, "SupabaseRestRepository", _FakeRestRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "verify_product_images", _fake_verify_product_images)
    monkeypatch.setattr(
        cli,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )

    exit_code = cli.run_crawl(limit=1)

    assert exit_code == 0
    assert "https://x.supabase.co/storage/new-cleanup-1.jpg" in _FakeUploader.deleted


def test_run_crawl_does_not_upsert_incomplete_finalize_rows(monkeypatch, capsys) -> None:
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
                product_id="incomplete-1",
                product_handle="incomplete-1",
                item_name="Incomplete Item",
                description=None,
                images=["https://cdn.example.com/incomplete.jpg"],
                gender_label=None,
            )
        ],
    )

    class _FakeRestRepo:
        upserts: list[tuple[int, int, str, int, int, bool]] = []

        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        def ensure_schema(self) -> None:
            return None

        def upsert_store(self, store: StoreProfile) -> int:
            _ = store
            return 1

        def get_product_image_state(self, store_id: int, product_id: str):
            _ = (store_id, product_id)
            return None

        def upsert_product(self, store_id: int, product: ProductRecord) -> None:
            _FakeRestRepo.upserts.append(
                (
                    store_id,
                    len(product.images),
                    product.product_id,
                    len(product.supabase_images),
                    0 if not product.gender_probs_csv else 1,
                    product.gender_probs_csv is None,
                )
            )

    class _FakeUploader:
        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        def upload_product_images(self, image_urls: list[str], store_id: int, product_id: str) -> list[str]:
            _ = (image_urls, store_id, product_id)
            # Simulate persistent upload failure.
            return []

    class _FakeFetcher:
        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        async def close(self) -> None:
            return None

    async def _fake_verify_product_images(*, products: list[ProductRecord], fetcher: object, settings: Settings):
        _ = (products, fetcher, settings)
        return None

    async def _fake_enrich_gender_probabilities_for_products(
        *,
        products: list[ProductRecord],
        fetcher: object,
        concurrency: int,
    ) -> None:
        _ = (fetcher, concurrency)
        for product in products:
            product.gender_probs_csv = "0.2,0.6,0.2"

    async def _fake_scrape_many_stream(
        seeds: list[StoreSeed],
        _settings: Settings,
        *,
        include_postprocess: bool = True,
    ):
        _ = (seeds, _settings, include_postprocess)
        yield (seed, outcome)

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "load_store_seeds", lambda path, _settings: [seed])
    monkeypatch.setattr(cli, "scrape_many_stream", _fake_scrape_many_stream)
    monkeypatch.setattr(cli, "SupabaseRestRepository", _FakeRestRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "verify_product_images", _fake_verify_product_images)
    monkeypatch.setattr(
        cli,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )

    exit_code = cli.run_crawl(limit=1)
    output = capsys.readouterr().out

    assert exit_code == 0
    # No placeholder upsert should happen; incomplete finalize payloads are blocked.
    assert len(_FakeRestRepo.upserts) == 0
    assert "Crawled 0/1 stores successfully" in output


def test_run_crawl_skips_upsert_for_products_without_images(monkeypatch) -> None:
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
                product_id="no-img-1",
                product_handle="no-img-1",
                item_name="No Image Product",
                description=None,
                images=[],
                unavailable=False,
            )
        ],
    )

    class _FakeRestRepo:
        upsert_count = 0

        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        def ensure_schema(self) -> None:
            return None

        def upsert_store(self, store: StoreProfile) -> int:
            _ = store
            return 1

        def get_product_image_state(self, store_id: int, product_id: str):
            _ = (store_id, product_id)
            return (["https://cdn.example.com/original.jpg"], ["https://x.supabase.co/original.jpg"])

        def upsert_product(self, store_id: int, product: ProductRecord) -> None:
            _ = (store_id, product)
            _FakeRestRepo.upsert_count += 1

    class _FakeUploader:
        def __init__(self, _settings: Settings) -> None:
            _ = _settings

    class _FakeFetcher:
        def __init__(self, _settings: Settings) -> None:
            _ = _settings

        async def close(self) -> None:
            return None

    async def _fake_verify_product_images(*, products: list[ProductRecord], fetcher: object, settings: Settings):
        _ = (products, fetcher, settings)
        return None

    async def _fake_enrich_gender_probabilities_for_products(
        *,
        products: list[ProductRecord],
        fetcher: object,
        concurrency: int,
    ) -> None:
        _ = (products, fetcher, concurrency)
        return None

    async def _fake_scrape_many_stream(
        seeds: list[StoreSeed],
        _settings: Settings,
        *,
        include_postprocess: bool = True,
    ):
        _ = (seeds, _settings, include_postprocess)
        yield (seed, outcome)

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "load_store_seeds", lambda path, _settings: [seed])
    monkeypatch.setattr(cli, "scrape_many_stream", _fake_scrape_many_stream)
    monkeypatch.setattr(cli, "SupabaseRestRepository", _FakeRestRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "verify_product_images", _fake_verify_product_images)
    monkeypatch.setattr(
        cli,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )

    exit_code = cli.run_crawl(limit=1)

    assert exit_code == 0
    assert _FakeRestRepo.upsert_count == 0
