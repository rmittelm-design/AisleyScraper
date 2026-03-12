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


def test_extract_gender_from_women_tag_variants() -> None:
    payload = {
        "products": [
            {
                "id": 3,
                "title": "Dress",
                "handle": "dress",
                "body_html": "",
                "images": [{"src": "https://cdn.example.com/dress.jpg"}],
                "options": [],
                "variants": [],
                "vendor": "Brand",
                "tags": "new, women's-clothing, sale",
            },
            {
                "id": 4,
                "title": "Skirt",
                "handle": "skirt",
                "body_html": "",
                "images": [{"src": "https://cdn.example.com/skirt.jpg"}],
                "options": [],
                "variants": [],
                "vendor": "Brand",
                "tags": "womens, summer",
            },
        ]
    }

    out = extract_products_from_products_json(payload, _settings())
    assert len(out) == 2
    assert out[0].gender_label == "female"
    assert out[1].gender_label == "female"


def test_extract_gender_from_mens_tag_variant() -> None:
    payload = {
        "products": [
            {
                "id": 5,
                "title": "Jacket",
                "handle": "jacket",
                "body_html": "",
                "images": [{"src": "https://cdn.example.com/jacket.jpg"}],
                "options": [],
                "variants": [],
                "vendor": "Brand",
                "tags": "mens-wear, winter",
            }
        ]
    }

    out = extract_products_from_products_json(payload, _settings())
    assert len(out) == 1
    assert out[0].gender_label == "male"


def test_extract_gender_and_strip_gender_only_product_type() -> None:
    payload = {
        "products": [
            {
                "id": 6,
                "title": "Polo",
                "handle": "polo",
                "body_html": "",
                "images": [{"src": "https://cdn.example.com/polo.jpg"}],
                "options": [],
                "variants": [],
                "vendor": "Brand",
                "product_type": "Mens",
            },
            {
                "id": 7,
                "title": "Blouse",
                "handle": "blouse",
                "body_html": "",
                "images": [{"src": "https://cdn.example.com/blouse.jpg"}],
                "options": [],
                "variants": [],
                "vendor": "Brand",
                "product_type": "Womens",
            },
            {
                "id": 8,
                "title": "Cap",
                "handle": "cap",
                "body_html": "",
                "images": [{"src": "https://cdn.example.com/cap.jpg"}],
                "options": [],
                "variants": [],
                "vendor": "Brand",
                "product_type": "Unisex",
            },
        ]
    }

    out = extract_products_from_products_json(payload, _settings())
    assert len(out) == 3

    assert out[0].gender_label == "male"
    assert out[0].product_type is None

    assert out[1].gender_label == "female"
    assert out[1].product_type is None

    assert out[2].gender_label == "unisex"
    assert out[2].product_type is None


def test_extract_gender_from_product_type_and_keep_category() -> None:
    payload = {
        "products": [
            {
                "id": 9,
                "title": "Crew Tee",
                "handle": "crew-tee",
                "body_html": "",
                "images": [{"src": "https://cdn.example.com/tee.jpg"}],
                "options": [],
                "variants": [],
                "vendor": "Brand",
                "product_type": "Men's T-Shirts",
            },
            {
                "id": 10,
                "title": "Drape Top",
                "handle": "drape-top",
                "body_html": "",
                "images": [{"src": "https://cdn.example.com/top.jpg"}],
                "options": [],
                "variants": [],
                "vendor": "Brand",
                "product_type": "Womens / Tops",
                "gender": "female",
            },
        ]
    }

    out = extract_products_from_products_json(payload, _settings())
    assert len(out) == 2

    assert out[0].gender_label == "male"
    assert out[0].product_type == "T Shirts"

    # Explicit gender still wins while category is preserved.
    assert out[1].gender_label == "female"
    assert out[1].product_type == "Tops"
