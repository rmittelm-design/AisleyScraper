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
