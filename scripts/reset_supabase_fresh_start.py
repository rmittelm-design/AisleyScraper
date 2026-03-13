from __future__ import annotations

from collections import deque

import httpx

from aisley_scraper.config import get_settings
from aisley_scraper.storage import StorageUploader


def _count_rows(client: httpx.Client, base_rest: str, headers: dict[str, str], table: str) -> int:
    resp = client.get(
        f"{base_rest}/{table}",
        params={"select": "id", "limit": "1"},
        headers={**headers, "Prefer": "count=exact"},
    )
    resp.raise_for_status()
    content_range = resp.headers.get("content-range", "0-0/0")
    try:
        return int(content_range.rsplit("/", 1)[-1])
    except Exception:
        return 0


def _delete_all_rows(client: httpx.Client, base_rest: str, headers: dict[str, str], table: str) -> None:
    # PostgREST requires a filter for DELETE; id is non-null for all persisted rows.
    resp = client.delete(
        f"{base_rest}/{table}",
        params={"id": "gt.0"},
        headers={**headers, "Prefer": "return=minimal"},
    )
    resp.raise_for_status()


def _list_all_object_paths(
    client: httpx.Client,
    *,
    base_url: str,
    bucket: str,
    headers: dict[str, str],
    root_prefix: str,
) -> list[str]:
    objects: list[str] = []
    queue: deque[str] = deque([root_prefix.strip("/")])

    while queue:
        prefix = queue.popleft()
        offset = 0
        page_size = 1000

        while True:
            resp = client.post(
                f"{base_url}/storage/v1/object/list/{bucket}",
                json={"prefix": prefix, "limit": page_size, "offset": offset},
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
                objects.append(f"{prefix}/{name}".strip("/"))

            if len(items) < page_size:
                break
            offset += page_size

    return objects


def main() -> int:
    s = get_settings()
    base_url = s.supabase_url.rstrip("/")
    base_rest = f"{base_url}/rest/v1"

    headers = {
        "Authorization": f"Bearer {s.supabase_service_role_key}",
        "apikey": s.supabase_service_role_key,
        "Content-Type": "application/json",
    }

    root_prefix = s.supabase_storage_path.strip("/")
    uploader = StorageUploader(s)

    with httpx.Client(timeout=60.0) as client:
        stores_before = _count_rows(client, base_rest, headers, "shopify_stores")
        products_before = _count_rows(client, base_rest, headers, "shopify_products")

        object_paths = _list_all_object_paths(
            client,
            base_url=base_url,
            bucket=s.supabase_storage_bucket,
            headers=headers,
            root_prefix=root_prefix,
        )

        public_prefix = f"{base_url}/storage/v1/object/public/{s.supabase_storage_bucket}/"
        if object_paths:
            batch_size = 200
            for idx in range(0, len(object_paths), batch_size):
                batch = object_paths[idx : idx + batch_size]
                uploader.delete_images([f"{public_prefix}{path}" for path in batch])

        _delete_all_rows(client, base_rest, headers, "shopify_products")
        _delete_all_rows(client, base_rest, headers, "shopify_stores")

        stores_after = _count_rows(client, base_rest, headers, "shopify_stores")
        products_after = _count_rows(client, base_rest, headers, "shopify_products")
        objects_after = len(
            _list_all_object_paths(
                client,
                base_url=base_url,
                bucket=s.supabase_storage_bucket,
                headers=headers,
                root_prefix=root_prefix,
            )
        )

    print(
        {
            "stores_before": stores_before,
            "products_before": products_before,
            "storage_objects_before": len(object_paths),
            "stores_after": stores_after,
            "products_after": products_after,
            "storage_objects_after": objects_after,
        }
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
