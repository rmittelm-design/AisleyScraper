from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re
from typing import Any
from urllib.parse import urljoin

from aisley_scraper.config import Settings
from aisley_scraper.models import ProductRecord


def _to_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(stripped)
        except ValueError:
            return None
    return None


def _to_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value in (0, 1):
            return bool(value)
        return None
    if isinstance(value, str):
        token = value.strip().lower()
        if token in {"true", "1", "yes", "y", "on"}:
            return True
        if token in {"false", "0", "no", "n", "off"}:
            return False
    return None

def _normalize_gender_token(value: str) -> str | None:
    token = value.strip().lower()
    compact = token.replace("'", "").replace("\u2019", "")

    if compact in {"male", "man", "men", "mens", "boy", "boys"}:
        return "male"
    if compact in {"female", "woman", "women", "womens", "girl", "girls"}:
        return "female"
    if compact in {"unisex", "all-gender", "all genders", "gender neutral", "gender-neutral"}:
        return "unisex"

    # Support common tag shapes like women's-clothing, mens_wear, for-women.
    words = [w for w in re.split(r"[^a-z0-9]+", compact) if w]
    if not words:
        return None
    word_set = set(words)

    if "unisex" in word_set:
        return "unisex"
    if {"all", "gender"}.issubset(word_set) or {"gender", "neutral"}.issubset(word_set):
        return "unisex"

    if word_set & {"women", "womens", "woman", "female", "girl", "girls"}:
        return "female"
    if word_set & {"men", "mens", "man", "male", "boy", "boys"}:
        return "male"

    return None

def _extract_explicit_gender_label(prod: dict[str, Any]) -> str | None:
    for key in ("gender", "target_gender"):
        value = prod.get(key)
        if isinstance(value, str):
            parsed = _normalize_gender_token(value)
            if parsed:
                return parsed

    for option in prod.get("options", []):
        if not isinstance(option, dict):
            continue
        name = str(option.get("name", "")).strip().lower()
        if name not in {"gender", "sex"}:
            continue
        values = option.get("values", [])
        if isinstance(values, list):
            for value in values:
                parsed = _normalize_gender_token(str(value))
                if parsed:
                    return parsed

    tags_raw = prod.get("tags")
    if isinstance(tags_raw, str):
        for tag in [t.strip() for t in tags_raw.split(",") if t.strip()]:
            parsed = _normalize_gender_token(tag)
            if parsed:
                return parsed

    return None


def _extract_gender_from_item_name(item_name: Any) -> str | None:
    if not isinstance(item_name, str):
        return None
    return _normalize_gender_token(item_name)


def _normalize_product_type_and_gender(value: Any) -> tuple[str | None, str | None]:
    if value is None:
        return None, None

    product_type = str(value).strip()
    if not product_type:
        return None, None

    detected_gender = _normalize_gender_token(product_type)

    category = product_type
    gender_patterns = [
        r"\bmen'?s\b",
        r"\bmens\b",
        r"\bmen\b",
        r"\bman\b",
        r"\bmale\b",
        r"\bboys?\b",
        r"\bwomen'?s\b",
        r"\bwomens\b",
        r"\bwomen\b",
        r"\bwoman\b",
        r"\bfemale\b",
        r"\bgirls?\b",
        r"\bunisex\b",
        r"\ball[-\s]?genders?\b",
        r"\bgender[-\s]?neutral\b",
    ]
    for pattern in gender_patterns:
        category = re.sub(pattern, " ", category, flags=re.IGNORECASE)

    category = re.sub(r"[\\/_|]+", " ", category)
    category = re.sub(r"\s*[-:]+\s*", " ", category)
    category = re.sub(r"\s+", " ", category).strip(" -_/|:")
    normalized_category = category or None

    return normalized_category, detected_gender


def _to_cents(value: Any) -> int | None:
    if isinstance(value, (int, float)):
        amount = Decimal(str(value))
        return int((amount * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            amount = Decimal(stripped)
            return int((amount * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        except InvalidOperation:
            return None
    return None


def _extract_explicit_price_cents(prod: dict[str, Any]) -> int | None:
    for key in ("price", "price_min"):
        candidate = _to_cents(prod.get(key))
        if candidate is not None:
            return candidate

    variant_prices: list[int] = []
    for variant in prod.get("variants", []):
        if not isinstance(variant, dict):
            continue
        candidate = _to_cents(variant.get("price"))
        if candidate is not None:
            variant_prices.append(candidate)

    if variant_prices:
        return min(variant_prices)

    return None


def _extract_explicit_sku(prod: dict[str, Any]) -> str | None:
    for variant in prod.get("variants", []):
        if not isinstance(variant, dict):
            continue
        sku_value = variant.get("sku")
        if isinstance(sku_value, str):
            stripped = sku_value.strip()
            if stripped:
                return stripped
    return None


def _variant_availability_signal(variant: dict[str, Any]) -> bool | None:
    explicit = _to_bool(variant.get("available"))
    if explicit is not None:
        return explicit

    inventory_policy = str(variant.get("inventory_policy", "")).strip().lower()
    if inventory_policy == "continue":
        return True

    quantity = _to_int(variant.get("inventory_quantity"))
    if quantity is not None:
        return quantity > 0

    return None


def _is_unavailable_product(prod: dict[str, Any]) -> bool:
    product_available = _to_bool(prod.get("available"))
    if product_available is False:
        return True

    variants = [v for v in prod.get("variants", []) if isinstance(v, dict)]
    if not variants:
        return False

    has_signal = False
    for variant in variants:
        variant_available = _variant_availability_signal(variant)
        if variant_available is None:
            continue
        has_signal = True
        if variant_available:
            return False

    return has_signal


def _extract_product_url(prod: dict[str, Any], base_url: str | None) -> str | None:
    for key in ("online_store_url", "url"):
        value = prod.get(key)
        if isinstance(value, str):
            candidate = value.strip()
            if candidate:
                if base_url:
                    return urljoin(f"{base_url.rstrip('/')}/", candidate)
                return candidate

    handle = prod.get("handle")
    if isinstance(handle, str):
        handle_clean = handle.strip().strip("/")
        if handle_clean and base_url:
            return f"{base_url.rstrip('/')}/products/{handle_clean}"

    return None


def _build_policy_raw_hint(prod: dict[str, Any]) -> dict[str, Any]:
    # Keep only lightweight fields needed by attribute policy checks.
    option_names: list[str] = []
    for option in prod.get("options", []):
        if not isinstance(option, dict):
            continue
        name = option.get("name")
        if isinstance(name, str) and name.strip():
            option_names.append(name.strip().lower())

    return {
        "keys": [str(key).lower() for key in prod.keys()],
        "option_names": option_names,
    }


def extract_products_from_products_json(
    payload: dict[str, Any],
    settings: Settings,
    *,
    base_url: str | None = None,
) -> list[ProductRecord]:
    _ = settings
    products_raw = payload.get("products", [])
    out: list[ProductRecord] = []

    for prod in products_raw:
        product_id = str(prod.get("id") or "")
        if not product_id:
            continue
        unavailable = _is_unavailable_product(prod)

        images = [img.get("src") for img in prod.get("images", []) if img.get("src")]
        options = prod.get("options", [])
        variants = prod.get("variants", [])

        sizes: set[str] = set()
        colors: set[str] = set()
        for option in options:
            name = str(option.get("name", "")).lower()
            values = [str(v).strip() for v in option.get("values", []) if str(v).strip()]
            if "size" in name:
                sizes.update(values)
            if "color" in name or "colour" in name:
                colors.update(values)

        # Fallback: detect size/color in variant option labels.
        for variant in variants:
            for key in ("option1", "option2", "option3"):
                val = str(variant.get(key, "")).strip()
                if not val:
                    continue
                if any(ch.isdigit() for ch in val) or val.upper() in {"XS", "S", "M", "L", "XL", "XXL"}:
                    sizes.add(val)

        product_type, gender_from_product_type = _normalize_product_type_and_gender(prod.get("product_type"))
        explicit_gender = _extract_explicit_gender_label(prod)
        gender_from_item_name = _extract_gender_from_item_name(prod.get("title"))
        gender_label = explicit_gender or gender_from_product_type or gender_from_item_name

        out.append(
            ProductRecord(
                product_id=product_id,
                product_handle=prod.get("handle"),
                item_name=prod.get("title") or "",
                description=prod.get("body_html"),
                sku=_extract_explicit_sku(prod),
                updated_at=str(prod.get("updated_at")) if prod.get("updated_at") is not None else None,
                price_cents=_extract_explicit_price_cents(prod),
                images=images,
                gender_label=gender_label,
                sizes=sorted(sizes),
                colors=sorted(colors),
                brand=(prod.get("vendor") or None),
                product_type=product_type,
                product_url=_extract_product_url(prod, base_url),
                unavailable=unavailable,
                raw=_build_policy_raw_hint(prod),
            )
        )

    return out
