from aisley_scraper.config import Settings
from aisley_scraper.extract.shopify_products import extract_products_from_products_json


def _settings() -> Settings:
    return Settings(
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        INPUT_CSV_PATH="./data/stores.csv",
    )


def test_extract_sku_from_variants() -> None:
    payload = {
        "products": [
            {
                "id": 21,
                "title": "Boot",
                "handle": "boot",
                "body_html": "",
                "images": [{"src": "https://cdn.example.com/boot.jpg"}],
                "options": [],
                "variants": [
                    {"sku": ""},
                    {"sku": "BOOT-001"},
                ],
                "vendor": "Brand",
            }
        ]
    }

    out = extract_products_from_products_json(payload, _settings())
    assert len(out) == 1
    assert out[0].sku == "BOOT-001"
