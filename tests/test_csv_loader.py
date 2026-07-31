from pathlib import Path

from aisley_scraper.config import Settings
from aisley_scraper.ingest.csv_loader import (
    dedupe_seeds_by_domain,
    load_store_seeds,
    load_store_seeds_from_dir,
)
from aisley_scraper.models import StoreSeed


def _dir_settings(tmp_path: Path) -> Settings:
    return Settings(
        USER_AGENT="bot",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_TSV_DIR=str(tmp_path),
        INPUT_CSV_HAS_HEADER=False,
    )


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


def test_dedupe_by_domain_keeps_row_with_most_branches() -> None:
    seeds = [
        StoreSeed(store_url="https://shop.com", addresses=["A"]),
        StoreSeed(store_url="https://shop.com", addresses=["A", "B", "C"]),  # most
        StoreSeed(store_url="https://shop.com", addresses=["A", "B"]),
        StoreSeed(store_url="https://other.com", addresses=[]),
    ]
    out = dedupe_seeds_by_domain(seeds)
    by_dom = {s.store_url: s for s in out}
    assert len(out) == 2
    assert [a for a in by_dom["https://shop.com"].addresses] == ["A", "B", "C"]


def test_dedupe_by_domain_is_scheme_and_www_insensitive() -> None:
    seeds = [
        StoreSeed(store_url="http://www.brand.com", addresses=["One"]),
        StoreSeed(store_url="https://brand.com", addresses=["One", "Two"]),  # most
    ]
    out = dedupe_seeds_by_domain(seeds)
    assert len(out) == 1
    assert out[0].store_url == "https://brand.com"
    assert len(out[0].addresses) == 2


def test_dedupe_by_domain_tie_keeps_first_seen_order() -> None:
    seeds = [
        StoreSeed(store_url="https://b.com", addresses=["x"]),
        StoreSeed(store_url="https://a.com", addresses=["x", "y"]),
        StoreSeed(store_url="https://b.com", addresses=["z"]),  # tie with first b.com -> first kept
    ]
    out = dedupe_seeds_by_domain(seeds)
    assert [s.store_url for s in out] == ["https://b.com", "https://a.com"]
    assert out[0].addresses == ["x"]  # first b.com row kept on tie


def test_load_store_seeds_from_dir_dedupes_domains_across_files(tmp_path: Path) -> None:
    # Same domain split across two files with differing branch counts + www/scheme variants.
    (tmp_path / "a.tsv").write_text(
        "https://www.acme.com\tAcme\t1 Main St\n"
        "https://soloshop.com\tSolo\n",
        encoding="utf-8",
    )
    (tmp_path / "b.tsv").write_text(
        "http://acme.com\tAcme\t1 Main St\t2 Oak Ave\t3 Elm Rd\n"  # most branches
        "https://another.com\tAnother\t9 Pine St\n",
        encoding="utf-8",
    )

    seeds = load_store_seeds_from_dir(str(tmp_path), _dir_settings(tmp_path))

    # acme deduped to a single seed with the 3-branch row; others untouched.
    assert len(seeds) == 3
    acme = next(s for s in seeds if "acme.com" in s.store_url)
    assert len(acme.addresses) == 3
