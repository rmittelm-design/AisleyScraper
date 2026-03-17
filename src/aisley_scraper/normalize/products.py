from __future__ import annotations

from aisley_scraper.models import ProductRecord


def should_exclude_product(product: ProductRecord) -> bool:
    product_type = (product.product_type or "").strip().lower()
    return product_type == "cosmetics"


def normalize_product(product: ProductRecord) -> ProductRecord | None:
    if should_exclude_product(product):
        return None
    return enforce_attribute_policy(product)


def enforce_attribute_policy(product: ProductRecord) -> ProductRecord:
    # Policy: only keep size/color/brand when explicitly scraped and product has image context.
    has_images = len(product.images) > 0
    if not has_images:
        product.sizes = []
        product.colors = []
        product.brand = None
        return product

    # Only retain explicit values if present in the raw payload keys.
    raw_text = str(product.raw).lower()

    if not any(key in raw_text for key in ("size", "option", "variant")):
        product.sizes = []

    if not any(key in raw_text for key in ("color", "colour", "option", "variant")):
        product.colors = []

    if "vendor" not in raw_text and "brand" not in raw_text:
        product.brand = None

    return product
