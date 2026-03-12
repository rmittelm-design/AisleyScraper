from pathlib import Path

from aisley_scraper.config import Settings
from aisley_scraper.ingest.csv_loader import load_store_seeds


def test_csv_loader_normalizes_urls(tmp_path: Path) -> None:
    csv_file = tmp_path / "stores.csv"
    csv_file.write_text("store_url\nexample.com\nhttps://foo.myshopify.com\n", encoding="utf-8")

    settings = Settings(
        USER_AGENT="bot",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH=str(csv_file),
        INPUT_CSV_HAS_HEADER=True,
        INPUT_CSV_URL_COLUMN="store_url",
    )

    seeds = load_store_seeds(str(csv_file), settings)
    assert seeds[0].store_url == "https://example.com"
    assert seeds[1].store_url == "https://foo.myshopify.com"
