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


def test_extract_price_from_price_min() -> None:
    payload = {
        "products": [
            {
                "id": 10,
                "title": "Jacket",
                "handle": "jacket",
                "body_html": "",
                "price_min": "89.50",
                "images": [{"src": "https://cdn.example.com/jacket.jpg"}],
                "options": [],
                "variants": [],
                "vendor": "Brand",
            }
        ]
    }

    out = extract_products_from_products_json(payload, _settings())
    assert len(out) == 1
    assert out[0].price_cents == 8950


def test_extract_price_from_variant_minimum() -> None:
    payload = {
        "products": [
            {
                "id": 11,
                "title": "Sneaker",
                "handle": "sneaker",
                "body_html": "",
                "images": [{"src": "https://cdn.example.com/sneaker.jpg"}],
                "options": [],
                "variants": [{"price": "130"}, {"price": "120.99"}],
                "vendor": "Brand",
            }
        ]
    }

    out = extract_products_from_products_json(payload, _settings())
    assert len(out) == 1
    assert out[0].price_cents == 12099
