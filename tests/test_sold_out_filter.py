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


def test_mark_unavailable_when_product_available_false() -> None:
    payload = {
        "products": [
            {
                "id": 101,
                "title": "Sold Out Tee",
                "handle": "sold-out-tee",
                "body_html": "",
                "available": False,
                "images": [{"src": "https://cdn.example.com/tee.jpg"}],
                "options": [],
                "variants": [{"available": False}],
                "vendor": "Brand",
            }
        ]
    }

    out = extract_products_from_products_json(payload, _settings())
    assert len(out) == 1
    assert out[0].product_id == "101"
    assert out[0].unavailable is True


def test_mark_unavailable_when_all_variants_unavailable() -> None:
    payload = {
        "products": [
            {
                "id": 102,
                "title": "Sold Out Shoe",
                "handle": "sold-out-shoe",
                "body_html": "",
                "images": [{"src": "https://cdn.example.com/shoe.jpg"}],
                "options": [],
                "variants": [
                    {"available": False, "inventory_quantity": 0, "inventory_policy": "deny"},
                    {"available": False, "inventory_quantity": 0, "inventory_policy": "deny"},
                ],
                "vendor": "Brand",
            }
        ]
    }

    out = extract_products_from_products_json(payload, _settings())
    assert len(out) == 1
    assert out[0].product_id == "102"
    assert out[0].unavailable is True


def test_keep_product_when_any_variant_available() -> None:
    payload = {
        "products": [
            {
                "id": 103,
                "title": "In Stock Jacket",
                "handle": "in-stock-jacket",
                "body_html": "",
                "images": [{"src": "https://cdn.example.com/jacket.jpg"}],
                "options": [],
                "variants": [
                    {"available": False, "inventory_quantity": 0, "inventory_policy": "deny"},
                    {"available": True, "inventory_quantity": 4, "inventory_policy": "deny"},
                ],
                "vendor": "Brand",
            }
        ]
    }

    out = extract_products_from_products_json(payload, _settings())
    assert len(out) == 1
    assert out[0].product_id == "103"
    assert out[0].unavailable is False
