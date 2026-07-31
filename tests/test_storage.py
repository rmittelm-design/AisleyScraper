import aisley_scraper.storage as storage_mod
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


def test_object_path_from_public_url_roundtrips() -> None:
    uploader = StorageUploader(_settings())
    public = "https://x.supabase.co/storage/v1/object/public/product-images/aisley/12/999/2.jpg"
    assert uploader._object_path_from_public_url(public) == "aisley/12/999/2.jpg"


def test_object_path_from_public_url_ignores_non_supabase_urls() -> None:
    uploader = StorageUploader(_settings())
    # A Shopify CDN URL is not a Supabase storage object -> nothing to delete.
    assert uploader._object_path_from_public_url("https://cdn.shopify.com/x.jpg") is None


def test_delete_images_noop_when_no_supabase_objects(monkeypatch) -> None:
    uploader = StorageUploader(_settings())

    class _BoomClient:
        def __init__(self, *a, **k) -> None:
            raise AssertionError("httpx.Client must not be created when there is nothing to delete")

    monkeypatch.setattr(storage_mod.httpx, "Client", _BoomClient)
    # All-foreign URLs resolve to zero object paths -> early return, no HTTP.
    uploader.delete_images(["https://cdn.shopify.com/only-source.jpg"])
