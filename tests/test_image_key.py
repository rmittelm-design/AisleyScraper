"""_image_key: compare product images by base path so a bumped Shopify ?v=
cache-buster does not defeat the skip-revalidation optimisation."""
from aisley_scraper.cli import _image_key


def test_strips_shopify_version_param():
    u = "https://cdn.shopify.com/s/files/1/0282/3313/8275/files/a-Max-Origin.webp?v=1756126500"
    assert _image_key(u) == "https://cdn.shopify.com/s/files/1/0282/3313/8275/files/a-Max-Origin.webp"


def test_same_image_different_version_is_equal():
    base = "https://cdn.shopify.com/s/files/1/0016/files/CA419BA0.jpg"
    assert _image_key(base + "?v=1779994514") == _image_key(base + "?v=1779994343")


def test_different_paths_stay_distinct():
    a = "https://cdn.shopify.com/s/files/1/0016/files/A.jpg?v=1"
    b = "https://cdn.shopify.com/s/files/1/0016/files/B.jpg?v=1"
    assert _image_key(a) != _image_key(b)


def test_strips_whitespace_and_any_query():
    assert _image_key("  https://cdn/x.jpg?width=800&v=9  ") == "https://cdn/x.jpg"


def test_unchanged_catalog_normalizes_equal():
    # a store re-scraped: same images, only ?v= timestamps moved -> lists compare equal
    stored = ["https://cdn/x.jpg?v=100", "https://cdn/y.jpg?v=100"]
    scraped = ["https://cdn/x.jpg?v=200", "https://cdn/y.jpg?v=205"]
    assert [_image_key(u) for u in scraped] == [_image_key(u) for u in stored]
