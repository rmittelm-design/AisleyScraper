"""Fast delete of all storage objects under SUPABASE_STORAGE_PATH.

Queries shopify_products.images from the DB to collect all stored image
URLs (which are public storage URLs), converts them to object paths, and
deletes them in bulk batches using the storage bulk-delete API.

This avoids the slow recursive folder-by-folder traversal.
"""
from __future__ import annotations

import json
import sys

import httpx

from aisley_scraper.config import get_settings


def _collect_stored_image_paths(
    client: httpx.Client,
    *,
    base_url: str,
    bucket: str,
    storage_path_prefix: str,
    auth_headers: dict[str, str],
) -> list[str]:
    """Query shopify_products for all stored image URLs and return their object paths."""
    rest_url = f"{base_url}/rest/v1/shopify_products"
    headers = {
        **auth_headers,
        "Accept": "application/json",
    }
    # Build the public URL prefix to filter and strip
    public_prefix = f"{base_url}/storage/v1/object/public/{bucket}/"
    page_size = 1000
    offset = 0
    paths: set[str] = set()

    while True:
        resp = client.get(
            rest_url,
            params={
                "select": "supabase_images",
                "supabase_images": "not.eq.[]",
                "limit": str(page_size),
                "offset": str(offset),
            },
            headers=headers,
        )
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            break
        for row in rows:
            images = row.get("supabase_images") or []
            for url in images:
                if not isinstance(url, str):
                    continue
                if url.startswith(public_prefix):
                    obj_path = url[len(public_prefix):]
                    if obj_path.startswith(storage_path_prefix + "/") or obj_path.startswith(storage_path_prefix):
                        paths.add(obj_path)
        if len(rows) < page_size:
            break
        offset += page_size

    return sorted(paths)


def _bulk_delete(
    client: httpx.Client,
    *,
    base_url: str,
    bucket: str,
    object_names: list[str],
    auth_headers: dict[str, str],
) -> None:
    """Delete objects using the Supabase storage bulk delete endpoint."""
    batch_size = 200
    delete_url = f"{base_url}/storage/v1/object/{bucket}"
    for idx in range(0, len(object_names), batch_size):
        batch = object_names[idx : idx + batch_size]
        resp = client.request(
            "DELETE",
            delete_url,
            content=json.dumps({"prefixes": batch}).encode(),
            headers={**auth_headers, "Content-Type": "application/json"},
        )
        resp.raise_for_status()
        print(f"  deleted batch {idx // batch_size + 1}: {len(batch)} objects", flush=True)


def main() -> int:
    s = get_settings()
    base_url = s.supabase_url.rstrip("/")
    bucket = s.supabase_storage_bucket
    prefix = s.supabase_storage_path.strip("/")
    auth_headers = {
        "Authorization": f"Bearer {s.supabase_service_role_key}",
        "apikey": s.supabase_service_role_key,
    }

    with httpx.Client(timeout=60.0) as client:
        print(f"Collecting stored image paths from DB (bucket={bucket!r} prefix={prefix!r}) ...", flush=True)
        names = _collect_stored_image_paths(
            client,
            base_url=base_url,
            bucket=bucket,
            storage_path_prefix=prefix,
            auth_headers=auth_headers,
        )
        print(f"Found {len(names)} stored image objects in DB.", flush=True)

        if not names:
            print("Nothing to delete.")
            return 0

        print("Deleting...", flush=True)
        _bulk_delete(
            client,
            base_url=base_url,
            bucket=bucket,
            object_names=names,
            auth_headers=auth_headers,
        )

    print({"storage_objects_deleted": len(names)}, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
