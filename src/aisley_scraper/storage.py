from __future__ import annotations

import mimetypes
import time
from collections import defaultdict
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

    def _object_path_from_public_url(self, public_url: str) -> str | None:
        root = self._settings.supabase_url.rstrip("/")
        bucket = self._settings.supabase_storage_bucket
        prefix = f"{root}/storage/v1/object/public/{bucket}/"
        if not public_url.startswith(prefix):
            return None
        return public_url[len(prefix) :]

    def upload_image_from_url(self, image_url: str, store_id: int, product_id: str, index: int) -> str:
        timeout = httpx.Timeout(30.0)
        headers = {
            "Authorization": f"Bearer {self._settings.supabase_service_role_key}",
            "apikey": self._settings.supabase_service_role_key,
        }

        attempts = 3
        last_exc: Exception | None = None

        for attempt in range(1, attempts + 1):
            try:
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
            except Exception as exc:
                last_exc = exc
                if attempt < attempts:
                    time.sleep(0.3 * attempt)

        if last_exc is not None:
            raise last_exc
        raise RuntimeError("image upload failed without explicit exception")

    def upload_image_from_content(
        self,
        *,
        image_url: str,
        image_content: bytes,
        store_id: int,
        product_id: str,
        index: int,
    ) -> str:
        timeout = httpx.Timeout(30.0)
        headers = {
            "Authorization": f"Bearer {self._settings.supabase_service_role_key}",
            "apikey": self._settings.supabase_service_role_key,
        }

        content_type, _ = mimetypes.guess_type(image_url)
        upload_content_type = content_type or "application/octet-stream"
        ext = self._extension_from_url_or_content_type(image_url, upload_content_type)
        object_path = self._object_path(store_id, product_id, index, ext)

        upload_url = (
            f"{self._settings.supabase_url.rstrip('/')}/storage/v1/object/"
            f"{self._settings.supabase_storage_bucket}/{object_path}"
        )
        upload_headers = {
            **headers,
            "Content-Type": upload_content_type,
            "x-upsert": "true",
        }

        attempts = 3
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                with httpx.Client(timeout=timeout, follow_redirects=True) as client:
                    upload_resp = client.post(upload_url, headers=upload_headers, content=image_content)
                    upload_resp.raise_for_status()
                return self._public_url(object_path)
            except Exception as exc:
                last_exc = exc
                if attempt < attempts:
                    time.sleep(0.3 * attempt)

        if last_exc is not None:
            raise last_exc
        raise RuntimeError("image upload failed without explicit exception")

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
        base_url = f"{self._settings.supabase_url.rstrip('/')}/storage/v1/object/{self._settings.supabase_storage_bucket}/"

        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            for object_path in object_paths:
                resp = client.delete(f"{base_url}{object_path}", headers=headers)
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()

    def sync_product_images(
        self,
        current_source_urls: list[str],
        existing_source_urls: list[str],
        existing_supabase_urls: list[str],
        store_id: int,
        product_id: str,
        *,
        delete_stale: bool = True,
    ) -> list[str]:
        # Keep a queue so duplicate source URLs can safely reuse matching existing files.
        existing_by_source: dict[str, list[str]] = defaultdict(list)
        for source_url, supabase_url in zip(existing_source_urls, existing_supabase_urls):
            existing_by_source[source_url].append(supabase_url)

        result: list[str | None] = [None] * len(current_source_urls)
        reused_supabase_urls: list[str] = []
        pending_uploads: list[tuple[int, str, int]] = []

        for output_idx, source_url in enumerate(current_source_urls):
            reusable = existing_by_source.get(source_url)
            if reusable:
                reused_url = reusable.pop(0)
                result[output_idx] = reused_url
                reused_supabase_urls.append(reused_url)
                continue
            pending_uploads.append((output_idx, source_url, output_idx + 1))

        uploaded_by_source: dict[str, str] = {}
        for output_idx, source_url, image_index in pending_uploads:
            if source_url in uploaded_by_source:
                result[output_idx] = uploaded_by_source[source_url]
                continue
            uploaded_url = self.upload_image_from_url(source_url, store_id, product_id, image_index)
            uploaded_by_source[source_url] = uploaded_url
            result[output_idx] = uploaded_url

        reused_set = set(reused_supabase_urls)
        final_set = reused_set | set(uploaded_by_source.values())
        stale_urls = [url for url in existing_supabase_urls if url not in final_set]
        if delete_stale and stale_urls:
            self.delete_images(stale_urls)

        return [url for url in result if url is not None]

    def upload_product_images(self, image_urls: list[str], store_id: int, product_id: str) -> list[str]:
        uploaded: list[str] = []
        for idx, image_url in enumerate(image_urls, start=1):
            uploaded.append(self.upload_image_from_url(image_url, store_id, product_id, idx))
        return uploaded

    def upload_product_images_from_cache(
        self,
        image_urls: list[str],
        store_id: int,
        product_id: str,
        image_bytes_by_url: dict[str, bytes],
    ) -> list[str]:
        uploaded: list[str] = []
        for idx, image_url in enumerate(image_urls, start=1):
            cached = image_bytes_by_url.get(image_url) or image_bytes_by_url.get(image_url.strip())
            if cached is None:
                uploaded.append(self.upload_image_from_url(image_url, store_id, product_id, idx))
                continue
            uploaded.append(
                self.upload_image_from_content(
                    image_url=image_url,
                    image_content=cached,
                    store_id=store_id,
                    product_id=product_id,
                    index=idx,
                )
            )
        return uploaded

    def sync_product_images_from_cache(
        self,
        *,
        current_source_urls: list[str],
        existing_source_urls: list[str],
        existing_supabase_urls: list[str],
        store_id: int,
        product_id: str,
        image_bytes_by_url: dict[str, bytes],
        delete_stale: bool = True,
    ) -> list[str]:
        existing_by_source: dict[str, list[str]] = defaultdict(list)
        for source_url, supabase_url in zip(existing_source_urls, existing_supabase_urls):
            existing_by_source[source_url].append(supabase_url)

        result: list[str | None] = [None] * len(current_source_urls)
        reused_supabase_urls: list[str] = []
        pending_uploads: list[tuple[int, str, int]] = []

        for output_idx, source_url in enumerate(current_source_urls):
            reusable = existing_by_source.get(source_url)
            if reusable:
                reused_url = reusable.pop(0)
                result[output_idx] = reused_url
                reused_supabase_urls.append(reused_url)
                continue
            pending_uploads.append((output_idx, source_url, output_idx + 1))

        uploaded_by_source: dict[str, str] = {}
        for output_idx, source_url, image_index in pending_uploads:
            if source_url in uploaded_by_source:
                result[output_idx] = uploaded_by_source[source_url]
                continue
            cached = image_bytes_by_url.get(source_url) or image_bytes_by_url.get(source_url.strip())
            if cached is None:
                uploaded_url = self.upload_image_from_url(source_url, store_id, product_id, image_index)
            else:
                uploaded_url = self.upload_image_from_content(
                    image_url=source_url,
                    image_content=cached,
                    store_id=store_id,
                    product_id=product_id,
                    index=image_index,
                )
            uploaded_by_source[source_url] = uploaded_url
            result[output_idx] = uploaded_url

        reused_set = set(reused_supabase_urls)
        final_set = reused_set | set(uploaded_by_source.values())
        stale_urls = [url for url in existing_supabase_urls if url not in final_set]
        if delete_stale and stale_urls:
            self.delete_images(stale_urls)

        return [url for url in result if url is not None]
