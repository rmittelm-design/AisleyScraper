from __future__ import annotations

from datetime import UTC, datetime

import httpx

from aisley_scraper.config import Settings
from aisley_scraper.models import ProductRecord, StoreProfile


class SupabaseRestRepository:
    def __init__(self, settings: Settings) -> None:
        self._base_url = f"{settings.supabase_url.rstrip('/')}/rest/v1"
        self._headers = {
            "Authorization": f"Bearer {settings.supabase_service_role_key}",
            "apikey": settings.supabase_service_role_key,
            "Content-Type": "application/json",
        }
        self._timeout = httpx.Timeout(30.0)

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(UTC).isoformat()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json_body: dict[str, object] | list[dict[str, object]] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        req_headers = dict(self._headers)
        if headers:
            req_headers.update(headers)

        with httpx.Client(timeout=self._timeout) as client:
            response = client.request(
                method,
                f"{self._base_url}{path}",
                params=params,
                json=json_body,
                headers=req_headers,
            )
            response.raise_for_status()
            return response

    def ensure_schema(self) -> None:
        # REST mode assumes migrations are already applied.
        return None

    def upsert_store(self, store: StoreProfile) -> int:
        payload: dict[str, object] = {
            "website": store.website,
            "store_name": store.store_name,
            "store_type": store.store_type,
            "instagram_handle": store.instagram_handle,
            "address": store.address,
            "raw": {
                "website": store.website,
                "store_name": store.store_name,
                "store_type": store.store_type,
                "instagram_handle": store.instagram_handle,
                "address": store.address,
            },
            "scraped": True,
            "last_seen_at": self._utc_now_iso(),
        }
        response = self._request(
            "POST",
            "/shopify_stores",
            params={"on_conflict": "website", "select": "id"},
            json_body=payload,
            headers={"Prefer": "resolution=merge-duplicates,return=representation"},
        )

        rows = response.json()
        if not isinstance(rows, list) or not rows:
            raise RuntimeError("failed to upsert store")

        row = rows[0]
        if not isinstance(row, dict) or "id" not in row:
            raise RuntimeError("failed to upsert store")
        return int(row["id"])

    def get_product_image_state(self, store_id: int, product_id: str) -> tuple[list[str], list[str]] | None:
        response = self._request(
            "GET",
            "/shopify_products",
            params={
                "select": "images,supabase_images",
                "store_id": f"eq.{store_id}",
                "product_id": f"eq.{product_id}",
                "limit": "1",
            },
        )
        rows = response.json()
        if not isinstance(rows, list) or not rows:
            return None

        row = rows[0]
        if not isinstance(row, dict):
            return None

        images = row.get("images")
        supabase_images = row.get("supabase_images")
        return list(images or []), list(supabase_images or [])

    def upsert_product(self, store_id: int, product: ProductRecord) -> None:
        payload: dict[str, object] = {
            "store_id": store_id,
            "product_id": product.product_id,
            "product_handle": product.product_handle,
            "product_url": product.product_url,
            "item_name": product.item_name,
            "description": product.description,
            "sku": product.sku,
            "updated_at": product.updated_at,
            "price_cents": product.price_cents,
            "images": product.images,
            "supabase_images": product.supabase_images,
            "gender_label": product.gender_label,
            "gender_probs_csv": product.gender_probs_csv,
            "sizes": product.sizes,
            "colors": product.colors,
            "brand": product.brand,
            "product_type": product.product_type,
            "unavailable": product.unavailable,
            "scraped": True,
            "last_seen_at": self._utc_now_iso(),
        }

        self._request(
            "POST",
            "/shopify_products",
            params={"on_conflict": "store_id,product_id"},
            json_body=payload,
            headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )
