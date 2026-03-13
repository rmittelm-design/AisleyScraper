from __future__ import annotations

from collections import deque

import httpx

from aisley_scraper.config import Settings
from aisley_scraper.storage import StorageUploader


def iter_linked_object_paths(base: str, headers: dict[str, str], public_prefix: str) -> set[str]:
    linked: set[str] = set()
    offset = 0
    page_size = 1000

    with httpx.Client(timeout=60.0) as client:
        while True:
            resp = client.get(
                f"{base}/shopify_products",
                params={
                    "select": "supabase_images",
                    "limit": str(page_size),
                    "offset": str(offset),
                },
                headers=headers,
            )
            resp.raise_for_status()
            rows = resp.json()
            if not isinstance(rows, list) or not rows:
                break

            for row in rows:
                if not isinstance(row, dict):
                    continue
                urls = row.get("supabase_images") or []
                if not isinstance(urls, list):
                    continue
                for url in urls:
                    if not isinstance(url, str):
                        continue
                    if url.startswith(public_prefix):
                        linked.add(url[len(public_prefix) :])

            if len(rows) < page_size:
                break
            offset += page_size

    return linked


def list_all_storage_objects(base_url: str, bucket: str, headers: dict[str, str], root_prefix: str) -> set[str]:
    objects: set[str] = set()
    queue: deque[str] = deque([root_prefix.strip("/")])

    with httpx.Client(timeout=60.0) as client:
        while queue:
            prefix = queue.popleft()
            offset = 0
            page_size = 1000

            while True:
                resp = client.post(
                    f"{base_url}/storage/v1/object/list/{bucket}",
                    json={
                        "prefix": prefix,
                        "limit": page_size,
                        "offset": offset,
                    },
                    headers=headers,
                )
                resp.raise_for_status()
                items = resp.json()
                if not isinstance(items, list) or not items:
                    break

                for item in items:
                    if not isinstance(item, dict):
                        continue
                    name = item.get("name")
                    if not isinstance(name, str) or not name:
                        continue

                    if item.get("id") is None:
                        queue.append(f"{prefix}/{name}".strip("/"))
                        continue

                    path = f"{prefix}/{name}".strip("/")
                    objects.add(path)

                if len(items) < page_size:
                    break
                offset += page_size

    return objects


def detect_orphan_storage_objects(settings: Settings) -> dict[str, object]:
    base = f"{settings.supabase_url.rstrip('/')}/rest/v1"
    headers = {
        "Authorization": f"Bearer {settings.supabase_service_role_key}",
        "apikey": settings.supabase_service_role_key,
        "Content-Type": "application/json",
    }
    public_prefix = (
        f"{settings.supabase_url.rstrip('/')}/storage/v1/object/public/{settings.supabase_storage_bucket}/"
    )
    root_prefix = settings.supabase_storage_path.strip("/")

    linked_paths = iter_linked_object_paths(base, headers, public_prefix)
    stored_paths = list_all_storage_objects(
        base_url=settings.supabase_url.rstrip("/"),
        bucket=settings.supabase_storage_bucket,
        headers=headers,
        root_prefix=root_prefix,
    )

    orphan_paths = sorted(stored_paths - linked_paths)
    return {
        "linked_paths": len(linked_paths),
        "stored_paths": len(stored_paths),
        "orphan_paths": orphan_paths,
        "public_prefix": public_prefix,
    }


def delete_orphan_storage_objects(
    settings: Settings,
    orphan_paths: list[str],
    *,
    batch_size: int = 200,
) -> int:
    if not orphan_paths:
        return 0

    public_prefix = (
        f"{settings.supabase_url.rstrip('/')}/storage/v1/object/public/{settings.supabase_storage_bucket}/"
    )
    uploader = StorageUploader(settings)
    deleted = 0
    safe_batch_size = max(1, batch_size)

    for idx in range(0, len(orphan_paths), safe_batch_size):
        batch_paths = orphan_paths[idx : idx + safe_batch_size]
        batch_urls = [f"{public_prefix}{path}" for path in batch_paths]
        uploader.delete_images(batch_urls)
        deleted += len(batch_urls)

    return deleted
