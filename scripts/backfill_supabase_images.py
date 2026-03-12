from __future__ import annotations

import argparse

import httpx

from aisley_scraper.config import get_settings
from aisley_scraper.storage import StorageUploader


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Backfill supabase_images for existing rows")
    p.add_argument("--limit", type=int, default=100, help="Max rows to process")
    return p


def _rest_headers(service_role_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {service_role_key}",
        "apikey": service_role_key,
        "Content-Type": "application/json",
    }


def main() -> int:
    args = _parser().parse_args()
    settings = get_settings()

    base_url = f"{settings.supabase_url.rstrip('/')}/rest/v1"
    headers = _rest_headers(settings.supabase_service_role_key)

    with httpx.Client(timeout=30.0) as client:
        resp = client.get(
            f"{base_url}/shopify_products",
            params={
                "select": "id,store_id,product_id,images,supabase_images",
                "supabase_images": "eq.[]",
                "order": "id.desc",
                "limit": str(max(1, args.limit)),
            },
            headers=headers,
        )
        resp.raise_for_status()
        rows = resp.json()

    uploader = StorageUploader(settings)
    updated = 0
    skipped = 0

    with httpx.Client(timeout=30.0) as client:
        for row in rows:
            if not isinstance(row, dict):
                skipped += 1
                continue

            row_id = row.get("id")
            store_id = row.get("store_id")
            product_id = row.get("product_id")
            images = list(row.get("images") or [])
            existing_supabase_images = list(row.get("supabase_images") or [])

            if row_id is None or store_id is None or product_id is None or not images:
                skipped += 1
                continue

            try:
                uploaded = uploader.sync_product_images(
                    current_source_urls=images,
                    existing_source_urls=images,
                    existing_supabase_urls=existing_supabase_images,
                    store_id=int(store_id),
                    product_id=str(product_id),
                )
            except Exception:
                skipped += 1
                continue

            patch_resp = client.patch(
                f"{base_url}/shopify_products",
                params={"id": f"eq.{row_id}"},
                json={"supabase_images": uploaded},
                headers={**headers, "Prefer": "return=minimal"},
            )
            patch_resp.raise_for_status()
            updated += 1

    print(f"Processed: {len(rows)}")
    print(f"Updated supabase_images: {updated}")
    print(f"Skipped: {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
