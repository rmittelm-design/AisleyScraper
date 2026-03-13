from __future__ import annotations

from collections import deque

import httpx

from aisley_scraper.config import get_settings
from aisley_scraper.storage import StorageUploader


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
    headers = {
        "Authorization": f"Bearer {s.supabase_service_role_key}",
        "apikey": s.supabase_service_role_key,
        "Content-Type": "application/json",
    }
    root_prefix = s.supabase_storage_path.strip("/")

    uploader = StorageUploader(s)

    with httpx.Client(timeout=60.0) as client:
        before_paths = _list_all_object_paths(
            client,
            base_url=base_url,
            bucket=s.supabase_storage_bucket,
            headers=headers,
            root_prefix=root_prefix,
        )

        if before_paths:
            public_prefix = f"{base_url}/storage/v1/object/public/{s.supabase_storage_bucket}/"
            batch_size = 200
            for idx in range(0, len(before_paths), batch_size):
                batch = before_paths[idx : idx + batch_size]
                uploader.delete_images([f"{public_prefix}{path}" for path in batch])

        after_paths = _list_all_object_paths(
            client,
            base_url=base_url,
            bucket=s.supabase_storage_bucket,
            headers=headers,
            root_prefix=root_prefix,
        )

    print({"storage_objects_before": len(before_paths), "storage_objects_after": len(after_paths)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
