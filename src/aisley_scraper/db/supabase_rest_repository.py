from __future__ import annotations

from datetime import UTC, datetime
import logging
import time

import httpx

from aisley_scraper.config import Settings
from aisley_scraper.models import ProductRecord, StoreProfile


logger = logging.getLogger(__name__)


class SupabaseRestRepository:
    def __init__(self, settings: Settings) -> None:
        self._base_url = f"{settings.supabase_url.rstrip('/')}/rest/v1"
        self._phase2_db_upsert_batch_size = max(1, int(settings.phase2_db_upsert_batch_size))
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

    def list_store_profiles(self, *, limit: int = 1000, offset: int = 0) -> list[StoreProfile]:
        response = self._request(
            "GET",
            "/shopify_stores",
            params={
                "select": "website,store_name,store_type,instagram_handle,address,lat,long",
                "order": "id.asc",
                "limit": str(max(1, limit)),
                "offset": str(max(0, offset)),
            },
        )
        rows = response.json()
        if not isinstance(rows, list):
            return []

        profiles: list[StoreProfile] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            website = row.get("website")
            store_name = row.get("store_name")
            store_type = row.get("store_type")
            if not isinstance(website, str) or not website:
                continue
            if not isinstance(store_name, str) or not store_name:
                continue
            if not isinstance(store_type, str) or not store_type:
                continue

            lat = row.get("lat")
            long = row.get("long")
            profiles.append(
                StoreProfile(
                    website=website,
                    store_name=store_name,
                    store_type=store_type,
                    instagram_handle=row.get("instagram_handle") if isinstance(row.get("instagram_handle"), str) else None,
                    address=row.get("address") if isinstance(row.get("address"), str) else None,
                    lat=float(lat) if isinstance(lat, (float, int)) else None,
                    long=float(long) if isinstance(long, (float, int)) else None,
                )
            )
        return profiles

    def list_all_store_profiles(self) -> list[StoreProfile]:
        profiles: list[StoreProfile] = []
        offset = 0
        page_size = 1000

        while True:
            batch = self.list_store_profiles(limit=page_size, offset=offset)
            if not batch:
                break
            profiles.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size

        return profiles

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
            "raw": {
                "website": store.website,
                "store_name": store.store_name,
                "store_type": store.store_type,
                "instagram_handle": store.instagram_handle,
                "address": store.address,
                "lat": store.lat,
                "long": store.long,
            },
            "scraped": True,
            "last_seen_at": self._utc_now_iso(),
        }
        # Only include address/lat/long when non-null so that merge-duplicates
        # never overwrites an existing value with NULL.
        if store.address is not None:
            payload["address"] = store.address
        if store.lat is not None:
            payload["lat"] = store.lat
        if store.long is not None:
            payload["long"] = store.long
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

    def get_product_image_states(
        self,
        store_id: int,
        product_ids: list[str],
    ) -> dict[str, tuple[list[str], list[str], str | None]]:
        if not product_ids:
            return {}

        # Keep URL query size bounded while reducing per-product roundtrips.
        page_size = 200
        out: dict[str, tuple[list[str], list[str], str | None]] = {}

        for start in range(0, len(product_ids), page_size):
            chunk = [pid for pid in product_ids[start : start + page_size] if pid]
            if not chunk:
                continue

            encoded_ids = ",".join(f'"{pid.replace('"', '\\"')}"' for pid in chunk)
            response = self._request(
                "GET",
                "/shopify_products",
                params={
                    "select": "product_id,images,supabase_images,gender_probs_csv",
                    "store_id": f"eq.{store_id}",
                    "product_id": f"in.({encoded_ids})",
                    "limit": str(len(chunk)),
                },
            )

            rows = response.json()
            if not isinstance(rows, list):
                continue

            for row in rows:
                if not isinstance(row, dict):
                    continue
                row_product_id = row.get("product_id")
                if not isinstance(row_product_id, str) or not row_product_id:
                    continue
                images = row.get("images")
                supabase_images = row.get("supabase_images")
                gender_probs_csv = row.get("gender_probs_csv")
                if not isinstance(gender_probs_csv, str):
                    gender_probs_csv = None
                out[row_product_id] = (
                    list(images or []),
                    list(supabase_images or []),
                    gender_probs_csv,
                )

        return out

    def upsert_product(self, store_id: int, product: ProductRecord) -> None:
        self.upsert_products_batch(store_id, [product])

    def upsert_products_batch(self, store_id: int, products: list[ProductRecord]) -> None:
        if not products:
            return

        now = self._utc_now_iso()
        page_size = self._phase2_db_upsert_batch_size
        for start in range(0, len(products), page_size):
            chunk = products[start : start + page_size]
            payload: list[dict[str, object]] = [
                {
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
                    "last_seen_at": now,
                }
                for product in chunk
            ]

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

    def list_products_for_first_image_validation_scan(
        self,
        *,
        limit: int,
        after_id: int | None = None,
    ) -> list[dict[str, object]]:
        target_limit = max(1, limit)
        cursor_id = max(0, after_id) if after_id is not None else None
        out: list[dict[str, object]] = []

        while len(out) < target_limit:
            page_limit = min(200, max(50, target_limit - len(out)))
            params = {
                "select": "id,store_id,product_id,item_uuid,images",
                "images": "neq.[]",
                "order": "id.asc",
                "limit": str(page_limit),
            }
            if cursor_id is not None:
                params["id"] = f"gt.{cursor_id}"

            response = self._request(
                "GET",
                "/shopify_products",
                params=params,
            )
            rows = response.json()
            if not isinstance(rows, list) or not rows:
                break

            typed_rows = [row for row in rows if isinstance(row, dict)]
            max_row_id = max(
                (int(row_id) for row in typed_rows if isinstance((row_id := row.get("id")), int)),
                default=cursor_id or 0,
            )
            cursor_id = max_row_id

            uuids = [
                item_uuid
                for row in typed_rows
                if isinstance((item_uuid := row.get("item_uuid")), str) and item_uuid
            ]
            if not uuids:
                continue

            # item_embeddings has a composite PK; we only need existence by item_uuid.
            unique_uuids = sorted(set(uuids))
            uuid_filter = ",".join(unique_uuids)
            emb_response = self._request(
                "GET",
                "/item_embeddings",
                params={
                    "select": "item_uuid",
                    "item_uuid": f"in.({uuid_filter})",
                },
            )
            emb_rows = emb_response.json()
            if not isinstance(emb_rows, list) or not emb_rows:
                continue

            embedded_item_uuids = {
                item_uuid
                for row in emb_rows
                if isinstance(row, dict)
                if isinstance((item_uuid := row.get("item_uuid")), str)
            }
            if not embedded_item_uuids:
                continue

            for row in typed_rows:
                row_item_uuid = row.get("item_uuid")
                if not isinstance(row_item_uuid, str) or row_item_uuid not in embedded_item_uuids:
                    continue
                out.append(row)
                if len(out) >= target_limit:
                    break

        return out

    def delete_item_embeddings_for_item_uuid(self, item_uuid: str) -> None:
        self._request(
            "DELETE",
            "/item_embeddings",
            params={
                "item_uuid": f"eq.{item_uuid}",
            },
            headers={"Prefer": "return=minimal"},
        )

    # ── Staging helpers (two-phase pipeline) ─────────────────────────────────

    def upsert_staged_store(self, run_id: str, store: StoreProfile) -> None:
        payload: dict[str, object] = {
            "run_id": run_id,
            "website": store.website,
            "store_name": store.store_name,
            "store_type": store.store_type,
            "instagram_handle": store.instagram_handle,
            "address": store.address,
            "lat": store.lat,
            "long": store.long,
            "raw": {
                "website": store.website,
                "store_name": store.store_name,
                "store_type": store.store_type,
                "instagram_handle": store.instagram_handle,
                "address": store.address,
                "lat": store.lat,
                "long": store.long,
            },
            "scraped_at": self._utc_now_iso(),
        }
        self._request(
            "POST",
            "/shopify_stores_staging",
            params={"on_conflict": "run_id,website"},
            json_body=payload,
            headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
        )

    def upsert_staged_products(
        self, run_id: str, website: str, products: list[ProductRecord]
    ) -> None:
        if not products:
            return
        now = self._utc_now_iso()
        page_size = 500
        for start in range(0, len(products), page_size):
            chunk = products[start : start + page_size]
            payload = [
                {
                    "run_id": run_id,
                    "website": website,
                    "product_id": p.product_id,
                    "product_handle": p.product_handle,
                    "product_url": p.product_url,
                    "item_name": p.item_name,
                    "description": p.description,
                    "sku": p.sku,
                    "updated_at": p.updated_at,
                    "price_cents": p.price_cents,
                    "images": p.images,
                    "gender_label": p.gender_label,
                    "sizes": p.sizes,
                    "colors": p.colors,
                    "brand": p.brand,
                    "product_type": p.product_type,
                    "unavailable": p.unavailable,
                    "scraped_at": now,
                }
                for p in chunk
            ]
            self._request(
                "POST",
                "/shopify_products_staging",
                params={"on_conflict": "run_id,website,product_id"},
                json_body=payload,
                headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
            )

    def get_staged_store(self, run_id: str, website: str) -> StoreProfile | None:
        response = self._request(
            "GET",
            "/shopify_stores_staging",
            params={
                "select": "store_name,store_type,instagram_handle,address,lat,long",
                "run_id": f"eq.{run_id}",
                "website": f"eq.{website}",
                "limit": "1",
            },
        )
        rows = response.json()
        if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
            return None
        row = rows[0]
        lat = row.get("lat")
        long_ = row.get("long")
        return StoreProfile(
            website=website,
            store_name=row.get("store_name", ""),
            store_type=row.get("store_type", "online"),
            instagram_handle=row.get("instagram_handle"),
            address=row.get("address"),
            lat=float(lat) if lat is not None else None,
            long=float(long_) if long_ is not None else None,
        )

    def list_all_staged_run_websites(self, *, run_id: str) -> list[str]:
        websites: list[str] = []
        offset = 0
        page_size = 1000

        while True:
            response = self._request(
                "GET",
                "/shopify_stores_staging",
                params={
                    "select": "website",
                    "run_id": f"eq.{run_id}",
                    "order": "id.asc",
                    "limit": str(page_size),
                    "offset": str(offset),
                },
            )
            rows = response.json()
            if not isinstance(rows, list) or not rows:
                break

            batch: list[str] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                website = row.get("website")
                if isinstance(website, str) and website:
                    batch.append(website)

            if not batch:
                break

            websites.extend(batch)
            if len(rows) < page_size:
                break
            offset += page_size

        return websites

    def get_staged_products(self, run_id: str, website: str) -> list[ProductRecord]:
        out: list[ProductRecord] = []
        offset = 0
        page_size = 500
        while True:
            response = self._request(
                "GET",
                "/shopify_products_staging",
                params={
                    "select": (
                        "product_id,product_handle,product_url,item_name,description,"
                        "sku,updated_at,price_cents,images,gender_label,"
                        "sizes,colors,brand,product_type,unavailable"
                    ),
                    "run_id": f"eq.{run_id}",
                    "website": f"eq.{website}",
                    "order": "id.asc",
                    "limit": str(page_size),
                    "offset": str(offset),
                },
            )
            rows = response.json()
            if not isinstance(rows, list) or not rows:
                break
            for row in rows:
                if not isinstance(row, dict):
                    continue
                out.append(
                    ProductRecord(
                        product_id=row["product_id"],
                        product_handle=row.get("product_handle"),
                        product_url=row.get("product_url"),
                        item_name=row.get("item_name", ""),
                        description=row.get("description"),
                        sku=row.get("sku"),
                        updated_at=row.get("updated_at"),
                        price_cents=row.get("price_cents"),
                        images=list(row.get("images") or []),
                        gender_label=row.get("gender_label"),
                        sizes=list(row.get("sizes") or []),
                        colors=list(row.get("colors") or []),
                        brand=row.get("brand"),
                        product_type=row.get("product_type"),
                        unavailable=bool(row.get("unavailable", False)),
                    )
                )
            if len(rows) < page_size:
                break
            offset += page_size
        return out

    def delete_staged_run_website(self, run_id: str, website: str) -> None:
        self._request(
            "DELETE",
            "/shopify_products_staging",
            params={"run_id": f"eq.{run_id}", "website": f"eq.{website}"},
            headers={"Prefer": "return=minimal"},
        )
        self._request(
            "DELETE",
            "/shopify_stores_staging",
            params={"run_id": f"eq.{run_id}", "website": f"eq.{website}"},
            headers={"Prefer": "return=minimal"},
        )

    def purge_run(self, run_id: str) -> None:
        """Delete all staging rows and crawl_store_runs rows for a given run ID."""
        for table in ("shopify_products_staging", "shopify_stores_staging", "crawl_store_runs"):
            try:
                self._delete_rows_for_run_in_chunks(table, run_id)
            except Exception as exc:
                logger.warning(
                    "Fresh cleanup: failed to delete table=%s run_id=%s: %s",
                    table,
                    run_id,
                    exc,
                )

    def purge_other_runs(self, keep_run_id: str) -> None:
        """Delete temporary rows for all run IDs except keep_run_id."""
        # Only scan the small tables to discover run_ids — shopify_products_staging can
        # have hundreds of thousands of rows and scanning it just to find run_ids is
        # unnecessary because every run that wrote products also wrote to the other two.
        run_ids: set[str] = set()
        for table in ("shopify_stores_staging", "crawl_store_runs"):
            try:
                run_ids.update(self._list_run_ids(table))
            except Exception as exc:
                logger.warning(
                    "Fresh cleanup: failed to list run_ids from %s: %s",
                    table,
                    exc,
                )

        run_ids_to_purge = sorted(run_id for run_id in run_ids if run_id != keep_run_id)
        if run_ids_to_purge:
            logger.warning(
                "Fresh cleanup: purging historical run_ids count=%s (keeping run_id=%s)",
                len(run_ids_to_purge),
                keep_run_id,
            )
        else:
            logger.warning("Fresh cleanup: no historical run_ids to purge")

        for index, run_id in enumerate(run_ids_to_purge, start=1):
            logger.warning(
                "Fresh cleanup progress: purging run_id %s/%s (%s)",
                index,
                len(run_ids_to_purge),
                run_id,
            )
            try:
                self.purge_run(run_id)
            except Exception as exc:
                logger.warning(
                    "Fresh cleanup: failed to purge run_id=%s: %s",
                    run_id,
                    exc,
                )

    def _list_run_ids(self, table: str) -> set[str]:
        out: set[str] = set()
        offset = 0
        page_size = 1000

        while True:
            response = self._request(
                "GET",
                f"/{table}",
                params={
                    "select": "run_id",
                    "order": "run_id.asc",
                    "limit": str(page_size),
                    "offset": str(offset),
                },
            )
            rows = response.json()
            if not isinstance(rows, list) or not rows:
                break

            for row in rows:
                if not isinstance(row, dict):
                    continue
                run_id = row.get("run_id")
                if isinstance(run_id, str) and run_id:
                    out.add(run_id)

            if len(rows) < page_size:
                break
            offset += page_size

        return out

    def _delete_rows_for_run_in_chunks(self, table: str, run_id: str, *, batch_size: int = 200) -> None:
        """Delete all rows for a run_id using a single indexed DELETE."""
        self._request(
            "DELETE",
            f"/{table}",
            params={"run_id": f"eq.{run_id}"},
            headers={"Prefer": "return=minimal"},
        )
