from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

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

def _normalize_gender_token(value: str) -> str | None:
    token = value.strip().lower()
    if token in {"male", "man", "men", "mens", "boy", "boys"}:
        return "male"
    if token in {"female", "woman", "women", "womens", "girl", "girls"}:
        return "female"
    if token in {"unisex", "all-gender", "all genders", "gender neutral", "gender-neutral"}:
        return "unisex"
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


def extract_products_from_products_json(payload: dict[str, Any], settings: Settings) -> list[ProductRecord]:
    _ = settings
    products_raw = payload.get("products", [])
    out: list[ProductRecord] = []

    for prod in products_raw:
        product_id = str(prod.get("id") or "")
        if not product_id:
            continue

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

        out.append(
            ProductRecord(
                product_id=product_id,
                product_handle=prod.get("handle"),
                item_name=prod.get("title") or "",
                description=prod.get("body_html"),
                sku=_extract_explicit_sku(prod),
                updated_at=str(prod.get("updated_at")) if prod.get("updated_at") is not None else None,
                position=_to_int(prod.get("position")),
                price_cents=_extract_explicit_price_cents(prod),
                images=images,
                gender_label=_extract_explicit_gender_label(prod),
                sizes=sorted(sizes),
                colors=sorted(colors),
                brand=(prod.get("vendor") or None),
                raw=prod,
            )
        )

    return out
