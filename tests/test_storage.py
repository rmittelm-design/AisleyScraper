from aisley_scraper.config import Settings
from aisley_scraper.storage import StorageUploader


def _settings() -> Settings:
    return Settings(
        USER_AGENT="bot",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
    )


def test_object_path_uses_env_prefix() -> None:
    uploader = StorageUploader(_settings())
    path = uploader._object_path(store_id=12, product_id="999", index=2, ext="jpg")
    assert path == "aisley/12/999/2.jpg"


def test_public_url_uses_bucket_and_path() -> None:
    uploader = StorageUploader(_settings())
    url = uploader._public_url("aisley/12/999/2.jpg")
    assert url == "https://x.supabase.co/storage/v1/object/public/product-images/aisley/12/999/2.jpg"


def test_sync_does_not_delete_before_upload_success(monkeypatch) -> None:
    uploader = StorageUploader(_settings())
    delete_calls: list[list[str]] = []

    def _fake_delete_images(public_urls: list[str]) -> None:
        delete_calls.append(list(public_urls))

    def _fake_upload_image_from_url(image_url: str, store_id: int, product_id: str, index: int) -> str:
        _ = (image_url, store_id, product_id, index)
        raise RuntimeError("synthetic upload failure")

    monkeypatch.setattr(uploader, "delete_images", _fake_delete_images)
    monkeypatch.setattr(uploader, "upload_image_from_url", _fake_upload_image_from_url)

    try:
        uploader.sync_product_images(
            current_source_urls=["https://cdn.example.com/new.jpg"],
            existing_source_urls=["https://cdn.example.com/old.jpg"],
            existing_supabase_urls=["https://x.supabase.co/storage/v1/object/public/product-images/aisley/1/p/1.jpg"],
            store_id=1,
            product_id="p",
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected sync_product_images to propagate upload failure")

    assert delete_calls == []
