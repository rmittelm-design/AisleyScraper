from aisley_scraper.models import ProductRecord
from aisley_scraper.normalize.products import enforce_attribute_policy


def test_policy_clears_attributes_without_images() -> None:
    p = ProductRecord(
        product_id="1",
        product_handle="h",
        item_name="Item",
        description=None,
        images=[],
        sizes=["M"],
        colors=["Red"],
        brand="BrandX",
        raw={"vendor": "BrandX", "options": [{"name": "Size", "values": ["M"]}]},
    )

    out = enforce_attribute_policy(p)
    assert out.sizes == []
    assert out.colors == []
    assert out.brand is None


def test_policy_keeps_attributes_with_images_and_explicit_source() -> None:
    p = ProductRecord(
        product_id="2",
        product_handle="h2",
        item_name="Item 2",
        description=None,
        images=["https://example.com/x.jpg"],
        sizes=["L"],
        colors=["Blue"],
        brand="BrandY",
        raw={"vendor": "BrandY", "options": [{"name": "Color", "values": ["Blue"]}]},
    )

    out = enforce_attribute_policy(p)
    assert out.sizes == ["L"]
    assert out.colors == ["Blue"]
    assert out.brand == "BrandY"
