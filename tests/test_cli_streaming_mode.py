from __future__ import annotations

from urllib.parse import parse_qs, urlparse

from aisley_scraper import cli
from aisley_scraper.config import Settings
from aisley_scraper.crawl import orchestrator
from aisley_scraper.models import ProductRecord, StoreProfile, StoreSeed


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
                        "id": 1001,
                        "handle": "item-1001",
                        "title": "Item 1001",
                        "images": [{"src": "https://cdn.example.com/1001-0.jpg"}, {"src": "https://cdn.example.com/1001-1.jpg"}, {"src": "https://cdn.example.com/1001-2.jpg"}, {"src": "https://cdn.example.com/1001-3.jpg"}],
                    },
                    {
                        "id": 1002,
                        "handle": "item-1002",
                        "title": "Item 1002",
                        "images": [{"src": "https://cdn.example.com/1002-0.jpg"}, {"src": "https://cdn.example.com/1002-1.jpg"}, {"src": "https://cdn.example.com/1002-2.jpg"}, {"src": "https://cdn.example.com/1002-3.jpg"}],
                    },
                ]
            }
        if page == 2:
            return {
                "products": [
                    {
                        "id": 1003,
                        "handle": "item-1003",
                        "title": "Item 1003",
                        "images": [{"src": "https://cdn.example.com/1003-0.jpg"}, {"src": "https://cdn.example.com/1003-1.jpg"}, {"src": "https://cdn.example.com/1003-2.jpg"}, {"src": "https://cdn.example.com/1003-3.jpg"}],
                    }
                ]
            }
        return {"products": []}

    async def get_bytes(self, _url: str) -> bytes:
        return b"bytes"

    async def close(self) -> None:
        return None


class _FailIfUsedFetcher:
    def __init__(self, _settings: Settings) -> None:
        raise RuntimeError("orchestrator Fetcher should not be used in streaming mode")


class _FakeRestRepo:
    inserted_product_ids: list[str] = []

    def __init__(self, _settings: Settings) -> None:
        _ = _settings

    def ensure_schema(self) -> None:
        return None

    def list_all_store_websites(self) -> list[str]:
        return []

    def upsert_store(self, store: StoreProfile) -> int:
        _ = store
        return 1

    def get_product_image_states(self, store_id: int, product_ids: list[str]):
        _ = (store_id, product_ids)
        return {}

    def get_product_image_state(self, store_id: int, product_id: str):
        _ = (store_id, product_id)
        return None

    def upsert_product(self, store_id: int, product: ProductRecord) -> None:
        _ = store_id
        self.__class__.inserted_product_ids.append(product.product_id)


class _FakeUploader:
    def __init__(self, _settings: Settings) -> None:
        _ = _settings

    def upload_product_images(self, image_urls: list[str], *, store_id: int, product_id: str) -> list[str]:
        _ = store_id
        return [f"https://supabase.example/{product_id}/{idx}" for idx, _url in enumerate(image_urls)]

    def sync_product_images(
        self,
        current_source_urls: list[str],
        existing_source_urls: list[str],
        existing_supabase_urls: list[str],
        store_id: int,
        product_id: str,
        delete_stale: bool = False,
    ) -> list[str]:
        _ = (
            current_source_urls,
            existing_source_urls,
            existing_supabase_urls,
            store_id,
            product_id,
            delete_stale,
        )
        return existing_supabase_urls


def test_run_crawl_streaming_mode_persists_page_by_page_without_orchestrator_fetcher(
    monkeypatch,
    tmp_path,
) -> None:
    settings = Settings(
        LOG_LEVEL="INFO",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
        PERSISTENCE_TARGET="supabase",
        STORE_PAGE_STREAMING_ENABLED=True,
        SHOPIFY_PRODUCTS_PAGE_LIMIT=2,
        SHOPIFY_PRODUCTS_MAX_PAGES=10,
        CRAWL_RUN_STATE_PATH=str(tmp_path / "run_id.txt"),
    )

    seed = StoreSeed(store_url="https://example.com")

    def _fake_classify_store(_homepage: str, base: str, _settings: Settings) -> StoreProfile:
        _ = _settings
        return StoreProfile(store_name="Example", website=base, store_type="online")

    async def _fake_verify_product_images(*, products, fetcher, settings):
        _ = (products, fetcher, settings)
        return None


    _FakeRestRepo.inserted_product_ids = []

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "_run_orphan_preflight", lambda _settings, batch_size=200: None)
    monkeypatch.setattr(cli, "_build_db_first_seeds", lambda _settings, _repo: [seed])
    monkeypatch.setattr(cli, "Repository", _FakeRestRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "classify_store", _fake_classify_store)
    monkeypatch.setattr(cli, "verify_product_images", _fake_verify_product_images)
    monkeypatch.setattr(orchestrator, "Fetcher", _FailIfUsedFetcher)

    exit_code = cli.run_crawl(limit=1, fresh=True)

    assert exit_code == 0
    assert _FakeRestRepo.inserted_product_ids == ["1001", "1002", "1003"]


class _ReconcileFetcher:
    """One catalog page: a normal apparel item, an image-poor apparel item that
    we won't persist (<2 images), and a non-apparel item that the filter drops."""

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
                        "id": 2001,
                        "handle": "blue-dress",
                        "title": "Blue Dress",
                        "product_type": "Dresses",
                        "images": [
                            {"src": "https://cdn.example.com/2001-0.jpg"},
                            {"src": "https://cdn.example.com/2001-1.jpg"},
                        ],
                    },
                    {
                        "id": 2002,
                        "handle": "red-skirt",
                        "title": "Red Skirt",
                        "product_type": "Skirts",
                        # Only 1 image -> excluded from persistence by MIN_PRODUCT_IMAGES,
                        # but it is STILL LISTED on the store, not delisted.
                        "images": [{"src": "https://cdn.example.com/2002-0.jpg"}],
                    },
                    {
                        "id": 2003,
                        "handle": "scented-candle",
                        "title": "Scented Candle",
                        "product_type": "Home",
                        "images": [
                            {"src": "https://cdn.example.com/2003-0.jpg"},
                            {"src": "https://cdn.example.com/2003-1.jpg"},
                        ],
                    },
                ]
            }
        return {"products": []}

    async def get_bytes(self, _url: str) -> bytes:
        return b"bytes"

    async def close(self) -> None:
        return None


class _ReconcileRepo(_FakeRestRepo):
    reconcile_product_ids: set | None = None

    def mark_removed_products_unavailable(
        self, website: str, product_ids, *, min_coverage: float = 0.5, dry_run: bool = False
    ):
        _ = (website, min_coverage, dry_run)
        self.__class__.reconcile_product_ids = {str(p) for p in product_ids}
        return {"marked_unavailable": 0}


def test_mark_removed_counts_image_poor_listed_products_as_present(monkeypatch, tmp_path) -> None:
    """Regression: a product still in the store's catalog but skipped for the
    <2-images rule must NOT be treated as delisted by --mark-removed. Only
    non-apparel (and genuinely-absent) ids may be reconciled away."""
    settings = Settings(
        LOG_LEVEL="INFO",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
        PERSISTENCE_TARGET="supabase",
        STORE_PAGE_STREAMING_ENABLED=True,
        SHOPIFY_PRODUCTS_PAGE_LIMIT=50,
        SHOPIFY_PRODUCTS_MAX_PAGES=10,
        CRAWL_RUN_STATE_PATH=str(tmp_path / "run_id.txt"),
    )
    seed = StoreSeed(store_url="https://example.com")

    def _fake_classify_store(_homepage: str, base: str, _settings: Settings) -> StoreProfile:
        _ = _settings
        return StoreProfile(store_name="Example", website=base, store_type="online")

    async def _fake_verify_product_images(*, products, fetcher, settings):
        _ = (products, fetcher, settings)
        return None

    _ReconcileRepo.inserted_product_ids = []
    _ReconcileRepo.reconcile_product_ids = None

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "_run_orphan_preflight", lambda _settings, batch_size=200: None)
    monkeypatch.setattr(cli, "_build_db_first_seeds", lambda _settings, _repo: [seed])
    monkeypatch.setattr(cli, "Repository", _ReconcileRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _ReconcileFetcher)
    monkeypatch.setattr(cli, "classify_store", _fake_classify_store)
    monkeypatch.setattr(cli, "verify_product_images", _fake_verify_product_images)
    monkeypatch.setattr(orchestrator, "Fetcher", _FailIfUsedFetcher)

    exit_code = cli.run_crawl(limit=1, fresh=True, mark_removed=True)

    assert exit_code == 0
    # Only the multi-image apparel item is persisted.
    assert _ReconcileRepo.inserted_product_ids == ["2001"]
    # The reconcile must have run and been handed the full listed-apparel catalog.
    ids = _ReconcileRepo.reconcile_product_ids
    assert ids is not None, "reconcile was not invoked"
    assert "2001" in ids
    assert "2002" in ids, "image-poor but still-listed apparel was wrongly treated as delisted"
    assert "2003" not in ids, "non-apparel must be excluded so it is reconciled away"


class _SkipFetcher:
    """Two products: 3001 (already in DB with identical images) and 3002 (new)."""

    _IMGS = {
        3001: ["https://cdn.example.com/3001-a.jpg", "https://cdn.example.com/3001-b.jpg"],
        3002: ["https://cdn.example.com/3002-a.jpg", "https://cdn.example.com/3002-b.jpg"],
    }

    def __init__(self, _settings: Settings) -> None:
        _ = _settings

    async def get_text(self, _url: str) -> str:
        return "<html></html>"

    async def get_json(self, url: str):
        page = int(parse_qs(urlparse(url).query).get("page", ["1"])[0])
        if page == 1:
            return {
                "products": [
                    {"id": 3001, "handle": "unchanged", "title": "Unchanged Dress",
                     "product_type": "Dresses",
                     "images": [{"src": u} for u in self._IMGS[3001]]},
                    {"id": 3002, "handle": "newprod", "title": "New Top",
                     "product_type": "Tops",
                     "images": [{"src": u} for u in self._IMGS[3002]]},
                ]
            }
        return {"products": []}

    async def get_bytes(self, _url: str) -> bytes:
        return b"bytes"

    async def close(self) -> None:
        return None


class _SkipRepo(_FakeRestRepo):
    validated_ids: list | None = None
    # 3001 already validated last run with the SAME images -> should be skipped.
    _existing = {"3001": (list(_SkipFetcher._IMGS[3001]), [], None)}

    def get_product_image_states(self, store_id: int, product_ids: list[str]):
        _ = store_id
        return {pid: st for pid, st in self._existing.items() if pid in set(product_ids)}

    def get_product_image_state(self, store_id: int, product_id: str):
        _ = store_id
        return self._existing.get(product_id)


def test_unchanged_products_skip_revalidation(monkeypatch, tmp_path) -> None:
    """Regression: on a re-crawl, a product whose image URLs are unchanged must NOT
    be re-downloaded/re-validated (the skip-validation optimisation). Before the fix,
    the missing allow_null_gender_probs short-circuit re-validated every product."""
    settings = Settings(
        LOG_LEVEL="INFO",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
        PERSISTENCE_TARGET="supabase",
        STORE_PAGE_STREAMING_ENABLED=True,
        SHOPIFY_PRODUCTS_PAGE_LIMIT=50,
        SHOPIFY_PRODUCTS_MAX_PAGES=10,
        CRAWL_RUN_STATE_PATH=str(tmp_path / "run_id.txt"),
    )
    seed = StoreSeed(store_url="https://example.com")

    def _fake_classify_store(_homepage: str, base: str, _settings: Settings) -> StoreProfile:
        _ = _settings
        return StoreProfile(store_name="Example", website=base, store_type="online")

    async def _recording_verify(*, products, fetcher, settings, max_images_per_product=None):
        _ = (fetcher, settings, max_images_per_product)
        _SkipRepo.validated_ids = sorted(p.product_id for p in products)

    _SkipRepo.inserted_product_ids = []
    _SkipRepo.validated_ids = None

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "_run_orphan_preflight", lambda _settings, batch_size=200: None)
    monkeypatch.setattr(cli, "_build_db_first_seeds", lambda _settings, _repo: [seed])
    monkeypatch.setattr(cli, "Repository", _SkipRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _SkipFetcher)
    monkeypatch.setattr(cli, "classify_store", _fake_classify_store)
    monkeypatch.setattr(cli, "verify_product_images", _recording_verify)
    monkeypatch.setattr(orchestrator, "Fetcher", _FailIfUsedFetcher)

    exit_code = cli.run_crawl(limit=1, fresh=True)

    assert exit_code == 0
    assert _SkipRepo.validated_ids is not None, "verify_product_images was never called"
    # 3001 (unchanged) must be skipped; only 3002 (new) is validated.
    assert "3001" not in _SkipRepo.validated_ids, "unchanged product was needlessly re-validated"
    assert _SkipRepo.validated_ids == ["3002"]


# ── Two-lane concurrency: partition + parallel batch ───────────────────────────
class _SizedRepo:
    """Minimal repo exposing _connect() for _partition_seeds_by_size."""

    def __init__(self, rows):
        self._rows = rows

    def _connect(self):
        rows = self._rows

        class _Cur:
            def execute(self, *a, **k):
                return None

            def fetchall(self):
                return rows

            def close(self):
                return None

        class _Conn:
            autocommit = False

            def cursor(self):
                return _Cur()

            def close(self):
                return None

        return _Conn()


def test_partition_seeds_by_size_splits_large_and_small() -> None:
    rows = [("https://big.com", 5000), ("https://small.com", 10), ("https://empty.com", 0)]
    seeds = [StoreSeed("https://big.com"), StoreSeed("https://small.com"), StoreSeed("https://new.com")]
    small, large = cli._partition_seeds_by_size(seeds, _SizedRepo(rows), 3000)
    assert [s.store_url for s in large] == ["https://big.com"]
    assert sorted(s.store_url for s in small) == ["https://new.com", "https://small.com"]


def test_partition_falls_back_to_small_on_db_error() -> None:
    class _Broken:
        def _connect(self):
            raise RuntimeError("no db")

    seeds = [StoreSeed("https://a.com"), StoreSeed("https://b.com")]
    small, large = cli._partition_seeds_by_size(seeds, _Broken(), 3000)
    assert len(small) == 2 and len(large) == 0


class _TwoStoreFetcher:
    """Each store returns its own single product (id derived from the host)."""

    def __init__(self, _settings: Settings) -> None:
        _ = _settings

    async def get_text(self, _url: str) -> str:
        return "<html></html>"

    async def get_json(self, url: str):
        page = int(parse_qs(urlparse(url).query).get("page", ["1"])[0])
        host = urlparse(url).netloc
        if page == 1:
            pid = 100 if "storea" in host else 200
            return {"products": [{
                "id": pid, "handle": f"h{pid}", "title": f"Item {pid}", "product_type": "Dresses",
                "images": [{"src": f"https://cdn.example.com/{pid}-a.jpg"},
                           {"src": f"https://cdn.example.com/{pid}-b.jpg"}],
            }]}
        return {"products": []}

    async def get_bytes(self, _url: str) -> bytes:
        return b"bytes"

    async def close(self) -> None:
        return None


class _TwoStoreRepo(_FakeRestRepo):
    completed: list = []
    reconciled: list = []

    def mark_run_store_status(self, *, run_id, website, status, error_message=None):
        _ = (run_id, error_message)
        if status == "completed":
            self.__class__.completed.append(website)

    def mark_removed_products_unavailable(self, website, product_ids, *, min_coverage=0.5, dry_run=False):
        _ = (min_coverage, dry_run)
        self.__class__.reconciled.append((website, {str(p) for p in product_ids}))
        return {"marked_unavailable": 0}


def test_two_stores_processed_concurrently_in_one_lane(monkeypatch, tmp_path) -> None:
    """Both stores land in the small lane and are processed in one concurrent batch;
    each persists its own product and reconciles independently."""
    settings = Settings(
        LOG_LEVEL="INFO", SUPABASE_URL="https://x.supabase.co", SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images", SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv", PERSISTENCE_TARGET="supabase",
        STORE_PAGE_STREAMING_ENABLED=True, SHOPIFY_PRODUCTS_PAGE_LIMIT=50, SHOPIFY_PRODUCTS_MAX_PAGES=10,
        CRAWL_SMALL_STORE_CONCURRENCY=5, CRAWL_RUN_STATE_PATH=str(tmp_path / "run_id.txt"),
    )
    seeds = [StoreSeed(store_url="https://storea.com"), StoreSeed(store_url="https://storeb.com")]

    def _fake_classify_store(_homepage, base, _settings):
        _ = _settings
        return StoreProfile(store_name="X", website=base, store_type="online")

    async def _fake_verify(*, products, fetcher, settings, max_images_per_product=None):
        _ = (fetcher, settings, max_images_per_product)

    _TwoStoreRepo.inserted_product_ids = []
    _TwoStoreRepo.completed = []
    _TwoStoreRepo.reconciled = []

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "_run_orphan_preflight", lambda _s, batch_size=200: None)
    monkeypatch.setattr(cli, "_build_db_first_seeds", lambda _s, _r: seeds)
    monkeypatch.setattr(cli, "_partition_seeds_by_size", lambda s, r, t: (list(s), []))
    monkeypatch.setattr(cli, "Repository", _TwoStoreRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _TwoStoreFetcher)
    monkeypatch.setattr(cli, "classify_store", _fake_classify_store)
    monkeypatch.setattr(cli, "verify_product_images", _fake_verify)
    monkeypatch.setattr(orchestrator, "Fetcher", _FailIfUsedFetcher)

    exit_code = cli.run_crawl(limit=2, fresh=True, mark_removed=True)

    assert exit_code == 0
    assert sorted(_TwoStoreRepo.inserted_product_ids) == ["100", "200"]
    assert sorted(_TwoStoreRepo.completed) == ["https://storea.com", "https://storeb.com"]
    # each store reconciled its own catalog
    recon = {w: ids for w, ids in _TwoStoreRepo.reconciled}
    assert recon["https://storea.com"] == {"100"}
    assert recon["https://storeb.com"] == {"200"}
