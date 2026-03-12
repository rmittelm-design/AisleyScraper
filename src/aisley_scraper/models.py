from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class StoreSeed:
    store_url: str
    source_id: str | None = None
    notes: str | None = None


@dataclass(slots=True)
class StoreProfile:
    store_name: str
    website: str
    store_type: str
    instagram_handle: str | None = None
    address: str | None = None


@dataclass(slots=True)
class ProductRecord:
    product_id: str
    product_handle: str | None
    item_name: str
    description: str | None
    images: list[str]
    sku: str | None = None
    updated_at: str | None = None
    position: int | None = None
    price_cents: int | None = None
    supabase_images: list[str] = field(default_factory=list)
    gender_label: str | None = None
    sizes: list[str] = field(default_factory=list)
    colors: list[str] = field(default_factory=list)
    brand: str | None = None
    product_type: str | None = None
    product_url: str | None = None
    unavailable: bool = False
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ScrapeResult:
    store: StoreProfile
    products: list[ProductRecord]
