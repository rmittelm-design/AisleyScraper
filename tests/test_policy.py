from aisley_scraper.models import ProductRecord
from aisley_scraper.normalize.products import enforce_attribute_policy, normalize_product, should_exclude_product


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


def test_should_exclude_cosmetics_product_type() -> None:
    p = ProductRecord(
        product_id="3",
        product_handle="h3",
        item_name="Lip Gloss",
        description=None,
        images=["https://example.com/gloss.jpg"],
        product_type="cosmetics",
    )

    assert should_exclude_product(p) is True
    assert normalize_product(p) is None


def test_normalize_product_keeps_non_cosmetics() -> None:
    p = ProductRecord(
        product_id="4",
        product_handle="h4",
        item_name="Dress",
        description=None,
        images=["https://example.com/dress.jpg"],
        product_type="dresses",
        brand="BrandZ",
        raw={"vendor": "BrandZ"},
    )

    out = normalize_product(p)
    assert out is not None
    assert out.product_id == "4"
