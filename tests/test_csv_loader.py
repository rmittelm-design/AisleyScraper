from pathlib import Path

from aisley_scraper.config import Settings
from aisley_scraper.ingest.csv_loader import load_store_seeds


def test_tsv_loader_normalizes_urls_and_reads_store_metadata(tmp_path: Path) -> None:
    tsv_file = tmp_path / "stores.tsv"
    tsv_file.write_text(
        "url\tStore Name\tStore Address\n"
        "example.com\tExample Store\t123 Example St\n"
        "https://foo.myshopify.com\t\t\n",
        encoding="utf-8",
    )

    settings = Settings(
        USER_AGENT="bot",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH=str(tsv_file),
        INPUT_CSV_HAS_HEADER=True,
    )

    seeds = load_store_seeds(str(tsv_file), settings)
    assert seeds[0].store_url == "https://example.com"
    assert seeds[0].store_name == "Example Store"
    assert seeds[0].address == "123 Example St"
    assert seeds[1].store_url == "https://foo.myshopify.com"
    assert seeds[1].store_name is None
    assert seeds[1].address is None


def test_tsv_loader_auto_detects_headerless_rows_and_swapped_url_name_columns(tmp_path: Path) -> None:
    tsv_file = tmp_path / "stores.tsv"
    tsv_file.write_text(
        "https://example.com\tExample Store\t123 Example St\n"
        "Nati Boutique\thttps://natiboutique.com/\t44 Prince St, New York, NY 10012\n",
        encoding="utf-8",
    )

    settings = Settings(
        USER_AGENT="bot",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH=str(tsv_file),
        INPUT_CSV_HAS_HEADER=True,
    )

    seeds = load_store_seeds(str(tsv_file), settings)

    assert len(seeds) == 2
    assert seeds[0].store_url == "https://example.com"
    assert seeds[0].store_name == "Example Store"
    assert seeds[1].store_url == "https://natiboutique.com"
    assert seeds[1].store_name == "Nati Boutique"
    assert seeds[1].address == "44 Prince St, New York, NY 10012"


def test_tsv_loader_recovers_shifted_address_column(tmp_path: Path) -> None:
    tsv_file = tmp_path / "stores.tsv"
    tsv_file.write_text(
        "url\tStore Name\tStore Address\n"
        "https://thefrankieshop.com/collections/\tFrankie Shop\t\t100 Stanton St, New York, NY 10002\n",
        encoding="utf-8",
    )

    settings = Settings(
        USER_AGENT="bot",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH=str(tsv_file),
        INPUT_CSV_HAS_HEADER=True,
    )

    seeds = load_store_seeds(str(tsv_file), settings)
    assert len(seeds) == 1
    assert seeds[0].store_url == "https://thefrankieshop.com"
    assert seeds[0].store_name == "Frankie Shop"
    assert seeds[0].address == "100 Stanton St, New York, NY 10002"


def test_tsv_loader_recovers_shifted_address_column_headerless(tmp_path: Path) -> None:
    tsv_file = tmp_path / "stores.tsv"
    tsv_file.write_text(
        "https://thefrankieshop.com/collections/\tFrankie Shop\t\t100 Stanton St, New York, NY 10002\n",
        encoding="utf-8",
    )

    settings = Settings(
        USER_AGENT="bot",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH=str(tsv_file),
        INPUT_CSV_HAS_HEADER=False,
    )

    seeds = load_store_seeds(str(tsv_file), settings)
    assert len(seeds) == 1
    assert seeds[0].store_url == "https://thefrankieshop.com"
    assert seeds[0].store_name == "Frankie Shop"
    assert seeds[0].address == "100 Stanton St, New York, NY 10002"
