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


def test_extract_product_url_from_online_store_url() -> None:
    payload = {
        "products": [
            {
                "id": 201,
                "title": "Bag",
                "handle": "bag",
                "online_store_url": "https://shop.example.com/products/bag",
                "body_html": "",
                "images": [{"src": "https://cdn.example.com/bag.jpg"}],
                "options": [],
                "variants": [],
                "vendor": "Brand",
            }
        ]
    }

    out = extract_products_from_products_json(payload, _settings(), base_url="https://shop.example.com")
    assert len(out) == 1
    assert out[0].product_url == "https://shop.example.com/products/bag"


def test_extract_product_url_from_handle_when_base_url_available() -> None:
    payload = {
        "products": [
            {
                "id": 202,
                "title": "Cap",
                "handle": "cap",
                "body_html": "",
                "images": [{"src": "https://cdn.example.com/cap.jpg"}],
                "options": [],
                "variants": [],
                "vendor": "Brand",
            }
        ]
    }

    out = extract_products_from_products_json(payload, _settings(), base_url="https://shop.example.com")
    assert len(out) == 1
    assert out[0].product_url == "https://shop.example.com/products/cap"
