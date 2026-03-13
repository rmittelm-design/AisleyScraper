from __future__ import annotations

import argparse
from collections import deque

import httpx

from aisley_scraper.config import get_settings
from aisley_scraper.storage import StorageUploader


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Find and optionally delete unlinked Supabase storage objects")
    p.add_argument("--apply", action="store_true", help="Delete orphaned objects (default: dry run)")
    p.add_argument("--batch-size", type=int, default=200, help="Delete batch size when --apply is used")
    return p


def _iter_linked_object_paths(base: str, headers: dict[str, str], public_prefix: str) -> set[str]:
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


def _list_all_storage_objects(base_url: str, bucket: str, headers: dict[str, str], root_prefix: str) -> set[str]:
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

                    # Supabase returns folders with id=None.
                    if item.get("id") is None:
                        queue.append(f"{prefix}/{name}".strip("/"))
                        continue

                    path = f"{prefix}/{name}".strip("/")
                    objects.add(path)

                if len(items) < page_size:
                    break
                offset += page_size

    return objects


def main() -> int:
    args = _parser().parse_args()
    s = get_settings()

    base = f"{s.supabase_url.rstrip('/')}/rest/v1"
    headers = {
        "Authorization": f"Bearer {s.supabase_service_role_key}",
        "apikey": s.supabase_service_role_key,
        "Content-Type": "application/json",
    }

    public_prefix = (
        f"{s.supabase_url.rstrip('/')}/storage/v1/object/public/{s.supabase_storage_bucket}/"
    )
    root_prefix = s.supabase_storage_path.strip("/")

    linked_paths = _iter_linked_object_paths(base, headers, public_prefix)
    stored_paths = _list_all_storage_objects(
        base_url=s.supabase_url.rstrip("/"),
        bucket=s.supabase_storage_bucket,
        headers=headers,
        root_prefix=root_prefix,
    )

    orphan_paths = sorted(stored_paths - linked_paths)

    print(
        {
            "linked_paths": len(linked_paths),
            "stored_paths": len(stored_paths),
            "orphan_paths": len(orphan_paths),
            "mode": "apply" if args.apply else "dry-run",
        }
    )

    if not orphan_paths or not args.apply:
        if orphan_paths:
            print({"sample_orphans": orphan_paths[:10]})
        return 0

    uploader = StorageUploader(s)
    batch_size = max(1, args.batch_size)
    deleted = 0

    for idx in range(0, len(orphan_paths), batch_size):
        batch_paths = orphan_paths[idx : idx + batch_size]
        batch_urls = [f"{public_prefix}{path}" for path in batch_paths]
        uploader.delete_images(batch_urls)
        deleted += len(batch_urls)

    print({"deleted_orphans": deleted})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
