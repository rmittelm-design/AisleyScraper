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
                        "images": [{"src": "https://cdn.example.com/1001.jpg"}],
                    },
                    {
                        "id": 1002,
                        "handle": "item-1002",
                        "title": "Item 1002",
                        "images": [{"src": "https://cdn.example.com/1002.jpg"}],
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
                        "images": [{"src": "https://cdn.example.com/1003.jpg"}],
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

    async def _fake_enrich_gender_probabilities_for_products(*, products, fetcher, concurrency):
        _ = (products, fetcher, concurrency)
        for product in products:
            if product.gender_probs_csv is None:
                product.gender_probs_csv = "0.1,0.8,0.1"

    _FakeRestRepo.inserted_product_ids = []

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "_run_orphan_preflight", lambda _settings, batch_size=200: None)
    monkeypatch.setattr(cli, "_build_db_first_seeds", lambda _settings, _repo: [seed])
    monkeypatch.setattr(cli, "SupabaseRestRepository", _FakeRestRepo)
    monkeypatch.setattr(cli, "StorageUploader", _FakeUploader)
    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "classify_store", _fake_classify_store)
    monkeypatch.setattr(cli, "verify_product_images", _fake_verify_product_images)
    monkeypatch.setattr(
        cli,
        "enrich_gender_probabilities_for_products",
        _fake_enrich_gender_probabilities_for_products,
    )
    monkeypatch.setattr(orchestrator, "Fetcher", _FailIfUsedFetcher)

    exit_code = cli.run_crawl(limit=1, fresh=True)

    assert exit_code == 0
    assert _FakeRestRepo.inserted_product_ids == ["1001", "1002", "1003"]
