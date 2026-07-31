from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class StoreSeed:
    store_url: str
    source_id: str | None = None
    notes: str | None = None
    store_name: str | None = None
    # One entry per branch (TSV columns 3+). The first entry is the primary
    # branch that scraped products are associated with.
    addresses: list[str] = field(default_factory=list)

    @property
    def address(self) -> str | None:
        """Primary (first) branch address, or None for online/address-less stores."""
        return self.addresses[0] if self.addresses else None


@dataclass(slots=True)
class StoreProfile:
    store_name: str
    website: str
    store_type: str
    instagram_handle: str | None = None
    address: str | None = None
    lat: float | None = None
    long: float | None = None
    shipping_returns: str | None = None
    shipping_returns_url: str | None = None


@dataclass(slots=True)
class ProductRecord:
    product_id: str
    product_handle: str | None
    item_name: str
    description: str | None
    images: list[str]
    item_uuid: str | None = None
    sku: str | None = None
    updated_at: str | None = None
    price_cents: int | None = None
    supabase_images: list[str] = field(default_factory=list)
    gender_label: str | None = None
    gender_probs_csv: str | None = None
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
    # True only when the scrape reached a genuine end of the store's catalog
    # (not truncated by a fetch error, the item cap, a block, or max_pages).
    # Removal reconciliation (marking absent products unavailable) must only run
    # on a complete scrape. Default True for non-scrape construction sites.
    scrape_complete: bool = True
