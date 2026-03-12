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


def test_extract_gender_from_explicit_gender_field() -> None:
    payload = {
        "products": [
            {
                "id": 1,
                "title": "Tee",
                "handle": "tee",
                "body_html": "",
                "images": [{"src": "https://cdn.example.com/tee.jpg"}],
                "options": [],
                "variants": [],
                "vendor": "Brand",
                "gender": "Female",
            }
        ]
    }

    out = extract_products_from_products_json(payload, _settings())
    assert len(out) == 1
    assert out[0].gender_label == "female"


def test_extract_gender_none_when_not_explicit() -> None:
    payload = {
        "products": [
            {
                "id": 2,
                "title": "Classic Tee",
                "handle": "classic-tee",
                "body_html": "",
                "images": [{"src": "https://cdn.example.com/classic.jpg"}],
                "options": [{"name": "Size", "values": ["M"]}],
                "variants": [{"option1": "M"}],
                "vendor": "Brand",
                "tags": "summer, cotton",
            }
        ]
    }

    out = extract_products_from_products_json(payload, _settings())
    assert len(out) == 1
    assert out[0].gender_label is None
