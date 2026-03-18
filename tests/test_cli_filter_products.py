from __future__ import annotations

from aisley_scraper import cli
from aisley_scraper.config import Settings


def _build_settings() -> Settings:
    return Settings(
        LOG_LEVEL="INFO",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
        PERSISTENCE_TARGET="supabase",
        PHASE2_FIRST_IMAGE_PRODUCT_PROB_THRESHOLD=0.65,
        IMAGE_VALIDATION_ATTEMPT_TIMEOUT_SEC=2,
        IMAGE_VALIDATION_CONCURRENCY=2,
    )


class _FakeRepo:
    deleted: list[tuple[int, str]] = []
    deleted_embeddings: list[str] = []

    def __init__(self, _settings: Settings) -> None:
        _ = _settings

    def list_products_for_first_image_validation_scan(
        self,
        *,
        limit: int,
        after_id: int | None = None,
    ):
        rows = [
            {
                "id": 1,
                "store_id": 101,
                "product_id": "keep-1",
                "item_uuid": "11111111-1111-4111-8111-111111111111",
                "images": ["https://cdn.example.com/keep-1.jpg"],
            },
            {
                "id": 2,
                "store_id": 101,
                "product_id": "drop-1",
                "item_uuid": "22222222-2222-4222-8222-222222222222",
                "images": ["https://cdn.example.com/drop-1.jpg"],
            },
            {
                "id": 3,
                "store_id": 102,
                "product_id": "keep-transient",
                "item_uuid": "33333333-3333-4333-8333-333333333333",
                "images": ["https://cdn.example.com/transient.jpg"],
            },
        ]
        start_after = after_id or 0
        eligible = [row for row in rows if int(row["id"]) > start_after]
        return eligible[: max(1, limit)]

    def delete_product(self, store_id: int, product_id: str) -> None:
        self.__class__.deleted.append((store_id, product_id))

    def delete_item_embeddings_for_item_uuid(self, item_uuid: str) -> None:
        self.__class__.deleted_embeddings.append(item_uuid)


class _FakeFetcher:
    def __init__(self, _settings: Settings) -> None:
        _ = _settings

    async def get_bytes(self, url: str) -> bytes:
        if "transient" in url:
            raise RuntimeError("temporary fetch issue")
        return b"image-bytes"

    async def close(self) -> None:
        return None


def test_filter_shopify_products_deletes_only_low_score_rows(monkeypatch) -> None:
    settings = _build_settings()
    _FakeRepo.deleted = []
    _FakeRepo.deleted_embeddings = []

    async def _fake_evaluate_first_image_product_validation(*, image_urls, fetcher, settings, semaphore=None):
        _ = (fetcher, settings, semaphore)
        first_image = image_urls[0]
        if "drop-1" in first_image:
            return False, "not_a_product_photo", 0.12
        if "transient" in first_image:
            return True, "timeout", None
        return True, None, 0.88

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "SupabaseRestRepository", _FakeRepo)
    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(
        cli,
        "evaluate_first_image_product_validation",
        _fake_evaluate_first_image_product_validation,
    )

    exit_code = cli.run_filter_shopify_products_first_image_validation(batch_size=2)

    assert exit_code == 0
    assert _FakeRepo.deleted == [(101, "drop-1")]
    assert _FakeRepo.deleted_embeddings == ["22222222-2222-4222-8222-222222222222"]


def test_filter_shopify_products_matches_phase2_hard_failure_drop(monkeypatch) -> None:
    settings = _build_settings()
    _FakeRepo.deleted = []
    _FakeRepo.deleted_embeddings = []

    async def _fake_evaluate_first_image_product_validation(*, image_urls, fetcher, settings, semaphore=None):
        _ = (fetcher, settings, semaphore)
        first_image = image_urls[0]
        if "keep-1" in first_image:
            return True, None, 0.88
        if "drop-1" in first_image:
            return False, "invalid_image", None
        return True, "fetch_error", None

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "SupabaseRestRepository", _FakeRepo)
    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(
        cli,
        "evaluate_first_image_product_validation",
        _fake_evaluate_first_image_product_validation,
    )

    exit_code = cli.run_filter_shopify_products_first_image_validation(batch_size=10)

    assert exit_code == 0
    assert _FakeRepo.deleted == [(101, "drop-1")]
    assert _FakeRepo.deleted_embeddings == ["22222222-2222-4222-8222-222222222222"]


def test_parser_supports_filter_shopify_products_command() -> None:
    parser = cli._build_parser()

    args = parser.parse_args(
        ["filter-shopify-products", "--limit", "10", "--batch-size", "50", "--dry-run"]
    )

    assert args.command == "filter-shopify-products"
    assert args.limit == 10
    assert args.batch_size == 50
    assert args.dry_run is True
