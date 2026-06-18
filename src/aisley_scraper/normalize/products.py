from __future__ import annotations

import re

from aisley_scraper.models import ProductRecord


# Minimum number of scraped images an item must have to be kept.
MIN_PRODUCT_IMAGES = 4


# Substrings that mark an item as kids/children/baby apparel. Matched
# aggressively (anywhere in the item name, url, or handle) per product
# requirements — this intentionally also drops items like "boyfriend jeans"
# or "babydoll dress" whose names contain these tokens.
_KIDS_SUBSTRINGS: tuple[str, ...] = (
    "kid",
    "child",
    "boy",
    "girl",
    "toddler",
    "baby",
    "babies",
    "infant",
    "newborn",
)


# Non-apparel categories to drop (furniture, home goods, pet, drinkware, gifts,
# beauty, etc.), matched in BOTH singular and plural form. NOTE: jewelry and
# watches are intentionally NOT filtered (kept as valid products). These are
# matched as WHOLE WORDS / PHRASES (word boundaries), NOT substrings: substring
# matching would wreck the apparel catalog — e.g. "card" ⊂ "cardigan",
# "pet" ⊂ "petite", "book" ⊂ "lookbook", "box" ⊂ "boxy"/"boxer",
# "towel" ⊂ "toweling", "glass" ⊂ "sunglasses"/"hourglass", "cup" ⊂ "cupro",
# "home" ⊂ "homecoming", "can" ⊂ "canvas", "oil" ⊂ "oilskin",
# "mirror" ⊂ "mirrored". "scrunchie" is matched but bare "scrunch" (an apparel
# ruching detail) is not; "hair pin" is matched as a phrase so bare "pin"
# (pinstripe/pintuck) is preserved. Checked against item name, url, handle, and
# product_type. \b is a Unicode word boundary, so "décor" works. ("baby" is
# already handled by the kids list.)
_NON_APPAREL_PATTERN = re.compile(
    r"\b(?:"
    r"furniture|box(?:es)?|"
    r"petwear|pets?|gift\s+cards?|gifts?|cards?|puzzles?|sundries|sundry|"
    r"bar\s+goodies?|candles?|lighters?|catchalls?|coffee\s+table\s+books?|books?|"
    r"d[eé]cor|picture\s+frames?|serveware|soaps?|diffusers?|towels?|"
    r"station[ae]ry|homes?|cans?|glass(?:es)?|tumblers?|mugs?|cups?|pouch(?:es)?|"
    r"tanners?|mirrors?|perfumes?|hair\s*pins?|rollerballs?|scrunch(?:ies|ie|y)|"
    r"oils?|sponges?|cleansers?|deodorants?|balms?|conditioners?|shampoos?|serums?"
    r")\b",
    re.IGNORECASE,
)


def should_exclude_product(product: ProductRecord) -> bool:
    # Require at least MIN_PRODUCT_IMAGES scraped images — drop image-poor items.
    # Checked on the raw scraped image list (before CLIP validation trims it).
    if len(product.images or []) < MIN_PRODUCT_IMAGES:
        return True

    product_type = (product.product_type or "").strip().lower()
    if product_type == "cosmetics":
        return True

    # Kids/children: aggressive substring match (name/url/handle).
    kids_haystack = " ".join(
        part
        for part in (product.item_name, product.product_url, product.product_handle)
        if part
    ).lower()
    if any(token in kids_haystack for token in _KIDS_SUBSTRINGS):
        return True

    # Non-apparel categories: whole-word/phrase match (name/url/handle/type).
    category_haystack = " ".join(
        part
        for part in (
            product.item_name,
            product.product_url,
            product.product_handle,
            product.product_type,
        )
        if part
    )
    return bool(_NON_APPAREL_PATTERN.search(category_haystack))


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
