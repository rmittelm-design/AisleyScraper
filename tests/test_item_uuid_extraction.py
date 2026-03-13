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


def test_extract_item_uuid_from_product_payload() -> None:
    payload = {
        "products": [
            {
                "id": 31,
                "item_uuid": "fd68d4dc-6c2a-4d92-9d1e-456d6dddbbb7",
                "title": "Tee",
                "handle": "tee",
                "body_html": "",
                "images": [{"src": "https://cdn.example.com/tee.jpg"}],
                "options": [],
                "variants": [],
                "vendor": "Brand",
            }
        ]
    }

    out = extract_products_from_products_json(payload, _settings())
    assert len(out) == 1
    assert out[0].item_uuid == "fd68d4dc-6c2a-4d92-9d1e-456d6dddbbb7"