from __future__ import annotations

import mimetypes
from urllib.parse import urlparse

import httpx

from aisley_scraper.config import Settings


class StorageUploader:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    @staticmethod
    def _extension_from_url_or_content_type(image_url: str, content_type: str | None) -> str:
        parsed = urlparse(image_url)
        path_ext = ""
        if "." in parsed.path:
            path_ext = parsed.path.rsplit(".", 1)[-1].lower()
            if path_ext in {"jpg", "jpeg", "png", "webp", "gif", "bmp", "svg", "avif"}:
                return path_ext

        if content_type:
            guessed = mimetypes.guess_extension(content_type.split(";", 1)[0].strip())
            if guessed:
                return guessed.lstrip(".")

        return "jpg"

    def _object_path(self, store_id: int, product_id: str, index: int, ext: str) -> str:
        base = self._settings.supabase_storage_path.strip("/")
        return f"{base}/{store_id}/{product_id}/{index}.{ext}"

    def _public_url(self, object_path: str) -> str:
        root = self._settings.supabase_url.rstrip("/")
        bucket = self._settings.supabase_storage_bucket
        return f"{root}/storage/v1/object/public/{bucket}/{object_path}"

    def upload_image_from_url(self, image_url: str, store_id: int, product_id: str, index: int) -> str:
        timeout = httpx.Timeout(30.0)
        headers = {
            "Authorization": f"Bearer {self._settings.supabase_service_role_key}",
            "apikey": self._settings.supabase_service_role_key,
        }

        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            source_resp = client.get(image_url)
            source_resp.raise_for_status()

            content_type = source_resp.headers.get("content-type", "application/octet-stream")
            ext = self._extension_from_url_or_content_type(image_url, content_type)
            object_path = self._object_path(store_id, product_id, index, ext)

            upload_url = (
                f"{self._settings.supabase_url.rstrip('/')}/storage/v1/object/"
                f"{self._settings.supabase_storage_bucket}/{object_path}"
            )
            upload_headers = {
                **headers,
                "Content-Type": content_type,
                "x-upsert": "true",
            }
            upload_resp = client.post(upload_url, headers=upload_headers, content=source_resp.content)
            upload_resp.raise_for_status()

        return self._public_url(object_path)

    def upload_product_images(self, image_urls: list[str], store_id: int, product_id: str) -> list[str]:
        uploaded: list[str] = []
        for idx, image_url in enumerate(image_urls, start=1):
            uploaded.append(self.upload_image_from_url(image_url, store_id, product_id, idx))
        return uploaded
