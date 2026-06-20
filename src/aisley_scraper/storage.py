from __future__ import annotations

import httpx

from aisley_scraper.config import Settings


class StorageUploader:
    """Supabase Storage helper — deletion only.

    Image re-hosting (upload) was removed: the app reads the original Shopify CDN
    URLs from the product ``images`` column, and nothing consumes
    ``supabase_images``, so the scraper no longer copies images into Supabase
    Storage. This class is retained solely for *deleting* objects — used by the
    orphan-storage cleanup tooling (``storage_integrity``) and to remove stale
    images when a product's image set changes.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def _object_path_from_public_url(self, public_url: str) -> str | None:
        root = self._settings.supabase_url.rstrip("/")
        bucket = self._settings.supabase_storage_bucket
        prefix = f"{root}/storage/v1/object/public/{bucket}/"
        if not public_url.startswith(prefix):
            return None
        return public_url[len(prefix) :]

    def delete_images(self, public_urls: list[str]) -> None:
        object_paths = [
            path
            for path in (self._object_path_from_public_url(url) for url in public_urls)
            if path
        ]
        if not object_paths:
            return

        timeout = httpx.Timeout(30.0)
        headers = {
            "Authorization": f"Bearer {self._settings.supabase_service_role_key}",
            "apikey": self._settings.supabase_service_role_key,
        }
        base_url = (
            f"{self._settings.supabase_url.rstrip('/')}/storage/v1/object/"
            f"{self._settings.supabase_storage_bucket}/"
        )

        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            for object_path in object_paths:
                resp = client.delete(f"{base_url}{object_path}", headers=headers)
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
