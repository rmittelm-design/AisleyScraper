from __future__ import annotations

import re

from aisley_scraper.models import ProductRecord


# Minimum number of scraped images an item must have to be kept.
MIN_PRODUCT_IMAGES = 3


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
    r"vintage|beaut(?:y|ies)|"
    r"furniture|box(?:es)?|"
    r"petwear|pets?|gift\s+cards?|gifts?|cards?|puzzles?|sundries|sundry|"
    r"bar\s+goodies?|candles?|lighters?|catchalls?|coffee\s+table\s+books?|books?|"
    r"d[eé]cor|picture\s+frames?|serveware|soaps?|diffusers?|towels?|"
    r"station[ae]ry|homes?|cans?|glass(?:es)?|tumblers?|mugs?|cups?|pouch(?:es)?|"
    r"tanners?|mirrors?|perfumes?|hair\s*pins?|rollerballs?|scrunch(?:ies|ie|y)|"
    r"oils?|sponges?|cleansers?|deodorants?|balms?|conditioners?|shampoos?|serums?|"
    # Nail care + makeup + fragrance/skincare (cosmetics that previously slipped
    # through, e.g. nail polish). High-precision terms only — color/material names
    # like blush, foundation, bronze are deliberately excluded to protect apparel.
    r"nail[\s-]*polish(?:es)?|nail[\s-]*lacquers?|lacquers?|manicures?|pedicures?|"
    r"cuticles?|make[\s-]*up|cosmetics?|mascaras?|lipsticks?|lip[\s-]*gloss(?:es)?|"
    r"concealers?|eye[\s-]*liners?|eye[\s-]*shadows?|bronzers?|fragrances?|skin[\s-]*care|"
    # Body accessories + shoe care/accessories (not garments or shoes themselves).
    # Matched as phrases — bare "lace"/"patch"/"petal"/"tape" are NOT matched
    # because they collide with apparel (lace dress, patch pocket, petal sleeve).
    r"nipple[\s-]*covers?|pasties|breast[\s-]*petals?|(?:boob|body|breast|fashion)[\s-]*tape|"
    r"insoles?|shoe[\s-]*laces?|shoelaces?|shoe[\s-]*patch(?:es)?|"
    # Toys / dolls. Bare "plush" (a fabric) and bare "teddy" (lingerie) are NOT
    # matched — we match "plushie" and "teddy bear". "doll" won't match "babydoll"
    # (no word boundary), so "babydoll" is listed explicitly per request.
    r"babydolls?|dolls?|teddy[\s-]*bears?|plushies?|stuffed[\s-]*animals?|action[\s-]*figures?|"
    r"toys?"
    r")\b",
    re.IGNORECASE,
)


# product_type is a controlled category label (not a free-text style name), so we
# match beauty/cosmetics categories more aggressively here without the apparel
# collision risk that bare substrings carry in item names.
_BEAUTY_PRODUCT_TYPE_PATTERN = re.compile(
    r"\b(?:cosmetics?|beauty|make[\s-]*up|nails?|nail[\s-]*polish|fragrances?|"
    r"perfumes?|skin[\s-]*care|grooming)\b",
    re.IGNORECASE,
)


def matches_excluded_category(
    *,
    item_name: str | None,
    product_url: str | None,
    product_handle: str | None,
    product_type: str | None,
) -> bool:
    """True when an item is a non-apparel / excluded category (cosmetics, nail care,
    kids, home goods, etc.) by keyword — independent of image count.

    Shared by the scrape-time filter and the ``prune-nonfashion`` cleanup so saved
    items are judged by exactly the same rules as freshly scraped ones.
    """
    # Beauty/cosmetics by product_type (controlled category field).
    if _BEAUTY_PRODUCT_TYPE_PATTERN.search((product_type or "").strip()):
        return True

    # Kids/children: aggressive substring match (name/url/handle).
    kids_haystack = " ".join(
        part for part in (item_name, product_url, product_handle) if part
    ).lower()
    if any(token in kids_haystack for token in _KIDS_SUBSTRINGS):
        return True

    # Non-apparel categories: whole-word/phrase match (name/url/handle/type).
    category_haystack = " ".join(
        part for part in (item_name, product_url, product_handle, product_type) if part
    )
    return bool(_NON_APPAREL_PATTERN.search(category_haystack))


# --- Conservative cleanup of already-saved items (prune-nonfashion --safe) -----
# The scrape-time filter above is intentionally aggressive (it drops vintage /
# beauty / boyfriend / girl-named apparel). Re-applying that to the saved catalog
# would delete thousands of real garments AND jewelry caught by word collisions.
# The "clear" detector below only removes UNAMBIGUOUS non-fashion and never
# touches anything typed as jewelry or apparel.

# Collision-prone words stripped before testing — they ride on real apparel /
# brand names (Vintage Havana, Gilded Beauty, glass-bead jewelry).
_CLEAR_COLLISION_STRIP = re.compile(r"\bvintage\b|\bbeaut(?:y|ies)\b|\bglass(?:es)?\b", re.IGNORECASE)

# product_type values that mark a real fashion category — never delete these,
# even if a keyword (e.g. "card" in "Tarot Card Necklace") matches the name.
_FASHION_TYPE_GUARD = re.compile(
    r"\b(?:"
    r"apparel|clothing|jewel(?:le)?ry|jwlnck|necklaces?|bracelets?|earrings?|rings?|anklets?|"
    r"pendants?|charms?|brooch(?:es)?|cuffs?|chains?|"
    r"tops?|tees?|t-shirts?|shirts?|blouses?|sweaters?|cardigans?|hoodies?|sweatshirts?|knitwear|denim|"
    r"dress(?:es)?|skirts?|pants?|trousers?|jeans|shorts?|leggings?|jumpsuits?|rompers?|gowns?|bottoms?|"
    r"jackets?|coats?|trench(?:es)?|blazers?|suits?|outerwear|puffers?|vests?|bombers?|"
    r"robes?|kimonos?|cami(?:sole)?s?|bodysuits?|tunics?|polos?|jumpers?|overalls?|tanks?|sets?|"
    r"jerseys?|thermals?|crewnecks?|sweatpants?|sweats|"
    r"lingerie|bras?|bralettes?|underwear|sleepwear|pajamas?|swim(?:wear|suits?)?|activewear|loungewear|"
    r"socks?|tights|hosiery|stockings?|"
    r"footwear|shoes?|boots?|sneakers?|heels?|sandals?|flats?|loafers?|mules?|slippers?|espadrilles?|"
    r"pumps?|flatforms?|platforms?|clogs?|"
    r"bags?|totes?|handbags?|clutch(?:es)?|crossbody|backpacks?|purses?|satchels?|pouch(?:es)?|"
    r"pochettes?|wristlets?|wallets?|card[\s-]*(?:holder|case)s?|belts?|scarves|scarf|hats?|"
    r"beanies?|caps?|gloves?|mittens?|earmuffs?|sunglasses|headbands?"
    r")\b",
    re.IGNORECASE,
)

# Explicitly requested removals that OVERRIDE the fashion guard — these are
# apparel-shaped (babydoll) or toy-shaped but the user wants them gone regardless.
# Hard removals OVERRIDE the apparel guard (these are apparel-shaped but unwanted).
# Checked on NAME + TYPE only — NOT url/handle, whose hyphen tokenization invents
# word boundaries (e.g. a "larroude-l131-doll-plat-dolly-sandal" shoe handle would
# otherwise trip "doll"). The soft toy/doll terms (doll, teddy bear, plushie, toy)
# live in _NON_APPAREL_PATTERN and DO yield to the guard, so a "Doll Plat" sandal
# or "Teddy" coat survives while real dolls/toys (non-fashion type) are removed.
_HARD_DELETE_PATTERN = re.compile(
    r"\b(?:"
    r"babydolls?|nipple[\s-]*covers?|pasties|breast[\s-]*petals?|"
    r"(?:boob|body|breast|fashion)[\s-]*tape"
    r")\b",
    re.IGNORECASE,
)


def matches_clear_nonfashion(
    *,
    item_name: str | None,
    product_url: str | None,
    product_handle: str | None,
    product_type: str | None,
) -> bool:
    """Conservative cleanup test: True only for UNAMBIGUOUS non-fashion items.

    Differs from ``matches_excluded_category``: it never deletes anything typed
    as jewelry/apparel, ignores the kids substrings, and strips the collision-prone
    words (vintage/beauty/glasses) so brand+style names don't trigger a delete.
    """
    pt = (product_type or "").strip()
    # Hard removals + the guard look at NAME + TYPE only (url/handle tokenization
    # is too noisy — see _HARD_DELETE_PATTERN).
    name_type = " ".join(part for part in (item_name, product_type) if part)
    # Hard removals win even over the fashion guard (babydoll dresses, nipple covers).
    if _HARD_DELETE_PATTERN.search(name_type):
        return True
    # Guard: anything NAMED or TYPED as jewelry/apparel/bags stays, period —
    # protects coats/cardigans/robes/sets/clutches/shoes caught by word collisions
    # or mislabeled product_types.
    if _FASHION_TYPE_GUARD.search(name_type):
        return False
    # Beauty/cosmetics by product_type (Beauty / Nail Polish / Fragrance / ...).
    if _BEAUTY_PRODUCT_TYPE_PATTERN.search(pt):
        return True
    # Clear non-apparel categories by keyword (full text, collision words removed,
    # kids substrings NOT applied). The guard above already vetoed fashion items.
    full_haystack = " ".join(
        part for part in (item_name, product_url, product_handle, product_type) if part
    )
    return bool(_NON_APPAREL_PATTERN.search(_CLEAR_COLLISION_STRIP.sub(" ", full_haystack)))


def should_exclude_product(product: ProductRecord) -> bool:
    # Require at least MIN_PRODUCT_IMAGES scraped images — drop image-poor items.
    # Checked on the raw scraped image list (before CLIP validation trims it).
    if len(product.images or []) < MIN_PRODUCT_IMAGES:
        return True

    return matches_excluded_category(
        item_name=product.item_name,
        product_url=product.product_url,
        product_handle=product.product_handle,
        product_type=product.product_type,
    )


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
