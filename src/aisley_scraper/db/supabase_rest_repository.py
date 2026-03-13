from __future__ import annotations

from datetime import UTC, datetime
import time

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

        attempts = 4
        retry_statuses = {408, 409, 425, 429, 500, 502, 503, 504}

        for attempt in range(1, attempts + 1):
            try:
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
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status not in retry_statuses or attempt == attempts:
                    raise
            except httpx.RequestError:
                if attempt == attempts:
                    raise

            time.sleep(0.25 * attempt)

        raise RuntimeError("supabase request failed without explicit exception")

    def ensure_schema(self) -> None:
        # REST mode assumes migrations are already applied.
        return None

    def list_store_websites(self, *, limit: int = 1000, offset: int = 0) -> list[str]:
        response = self._request(
            "GET",
            "/shopify_stores",
            params={
                "select": "website",
                "order": "id.asc",
                "limit": str(max(1, limit)),
                "offset": str(max(0, offset)),
            },
        )
        rows = response.json()
        if not isinstance(rows, list):
            return []

        websites: list[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            website = row.get("website")
            if isinstance(website, str) and website:
                websites.append(website)
        return websites

    def list_all_store_websites(self) -> list[str]:
        websites: list[str] = []
        offset = 0
        page_size = 1000

        while True:
            batch = self.list_store_websites(limit=page_size, offset=offset)
            if not batch:
                break
            websites.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size

        return websites

    def initialize_crawl_run(self, *, run_id: str, websites: list[str]) -> None:
        if not websites:
            return

        now = self._utc_now_iso()
        payload = [
            {
                "run_id": run_id,
                "website": website,
                "status": "pending",
                "attempt_count": 0,
                "created_at": now,
                "updated_at": now,
            }
            for website in websites
        ]
        self._request(
            "POST",
            "/crawl_store_runs",
            params={"on_conflict": "run_id,website"},
            json_body=payload,
            headers={"Prefer": "resolution=ignore-duplicates,return=minimal"},
        )

    def list_run_store_websites(
        self,
        *,
        run_id: str,
        statuses: list[str],
        limit: int = 1000,
        offset: int = 0,
    ) -> list[str]:
        if not statuses:
            return []

        encoded_statuses = ",".join(statuses)
        response = self._request(
            "GET",
            "/crawl_store_runs",
            params={
                "select": "website",
                "run_id": f"eq.{run_id}",
                "status": f"in.({encoded_statuses})",
                "order": "id.asc",
                "limit": str(max(1, limit)),
                "offset": str(max(0, offset)),
            },
        )
        rows = response.json()
        if not isinstance(rows, list):
            return []

        out: list[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            website = row.get("website")
            if isinstance(website, str) and website:
                out.append(website)
        return out

    def list_all_run_store_websites(self, *, run_id: str, statuses: list[str]) -> list[str]:
        websites: list[str] = []
        offset = 0
        page_size = 1000

        while True:
            batch = self.list_run_store_websites(
                run_id=run_id,
                statuses=statuses,
                limit=page_size,
                offset=offset,
            )
            if not batch:
                break
            websites.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size

        return websites

    def mark_run_store_status(
        self,
        *,
        run_id: str,
        website: str,
        status: str,
        error_message: str | None = None,
    ) -> None:
        attempt_count = 1
        current = self._request(
            "GET",
            "/crawl_store_runs",
            params={
                "select": "status,attempt_count",
                "run_id": f"eq.{run_id}",
                "website": f"eq.{website}",
                "limit": "1",
            },
        ).json()
        if isinstance(current, list) and current and isinstance(current[0], dict):
            existing_status = current[0].get("status")
            existing_attempts = current[0].get("attempt_count")
            if isinstance(existing_attempts, int):
                # Count only the first processing attempt for a run/store row.
                # Repeated terminal-state writes should be idempotent.
                if existing_status == "pending":
                    attempt_count = max(1, existing_attempts + 1)
                else:
                    attempt_count = max(1, existing_attempts)

        payload: dict[str, object] = {
            "status": status,
            "attempt_count": attempt_count,
            "last_attempt_at": self._utc_now_iso(),
            "updated_at": self._utc_now_iso(),
        }
        if error_message:
            payload["error_message"] = error_message[:2000]

        self._request(
            "PATCH",
            "/crawl_store_runs",
            params={
                "run_id": f"eq.{run_id}",
                "website": f"eq.{website}",
            },
            json_body=payload,
            headers={"Prefer": "return=minimal"},
        )

    def count_run_store_status(self, *, run_id: str, status: str) -> int:
        response = self._request(
            "GET",
            "/crawl_store_runs",
            params={
                "select": "id",
                "run_id": f"eq.{run_id}",
                "status": f"eq.{status}",
            },
            headers={"Prefer": "count=exact"},
        )
        count_header = response.headers.get("content-range", "")
        if "/" not in count_header:
            return 0
        try:
            return int(count_header.split("/")[-1])
        except ValueError:
            return 0

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

    def get_product_image_state(
        self,
        store_id: int,
        product_id: str,
    ) -> tuple[list[str], list[str], str | None] | None:
        response = self._request(
            "GET",
            "/shopify_products",
            params={
                "select": "images,supabase_images,gender_probs_csv",
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
        gender_probs_csv = row.get("gender_probs_csv")
        if not isinstance(gender_probs_csv, str):
            gender_probs_csv = None
        return list(images or []), list(supabase_images or []), gender_probs_csv

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

        if product.item_uuid is not None:
            payload["item_uuid"] = product.item_uuid

        self._request(
            "POST",
            "/shopify_products",
            params={"on_conflict": "store_id,product_id"},
            json_body=payload,
            headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    def delete_product(self, store_id: int, product_id: str) -> None:
        self._request(
            "DELETE",
            "/shopify_products",
            params={
                "store_id": f"eq.{store_id}",
                "product_id": f"eq.{product_id}",
            },
            headers={"Prefer": "return=minimal"},
        )

    def list_products_for_integrity_scan(self, *, limit: int, offset: int) -> list[dict[str, object]]:
        response = self._request(
            "GET",
            "/shopify_products",
            params={
                "select": "store_id,product_id,images,supabase_images,gender_label,gender_probs_csv",
                "images": "neq.[]",
                "order": "id.asc",
                "limit": str(max(1, limit)),
                "offset": str(max(0, offset)),
            },
        )
        rows = response.json()
        if not isinstance(rows, list):
            return []
        return [row for row in rows if isinstance(row, dict)]

    def patch_product_integrity_fields(
        self,
        *,
        store_id: int,
        product_id: str,
        supabase_images: list[str],
        gender_probs_csv: str,
    ) -> None:
        self._request(
            "PATCH",
            "/shopify_products",
            params={
                "store_id": f"eq.{store_id}",
                "product_id": f"eq.{product_id}",
            },
            json_body={
                "supabase_images": supabase_images,
                "gender_probs_csv": gender_probs_csv,
                "last_seen_at": self._utc_now_iso(),
            },
            headers={"Prefer": "return=minimal"},
        )
