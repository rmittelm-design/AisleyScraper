from __future__ import annotations

import argparse
import asyncio

import httpx

from aisley_scraper.config import get_settings
from aisley_scraper.crawl.fetcher import Fetcher
from aisley_scraper.gender_probs import enrich_gender_probabilities_for_products, one_hot_gender_probs_csv
from aisley_scraper.models import ProductRecord
from aisley_scraper.storage import StorageUploader

_SYNTHETIC = "0.333333,0.333333,0.333334"


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Cleanup rows touched by previous fallback hacks")
    p.add_argument("--limit", type=int, default=1000, help="Max newest rows to inspect")
    return p


def _is_supabase_public_url(url: str, supabase_url: str, bucket: str) -> bool:
    prefix = f"{supabase_url.rstrip('/')}/storage/v1/object/public/{bucket}/"
    return url.startswith(prefix)


async def _recompute_gender_probs(row: dict[str, object], settings) -> str | None:
    label = str(row.get("gender_label") or "").strip().lower()
    one_hot = one_hot_gender_probs_csv(label if label else None)
    if one_hot is not None:
        return one_hot

    images = list(row.get("images") or [])
    if not images:
        return None

    product = ProductRecord(
        product_id=str(row.get("product_id") or ""),
        product_handle=str(row.get("product_id") or ""),
        item_name=str(row.get("item_name") or ""),
        description=None,
        images=images,
        gender_label=None,
        gender_probs_csv=None,
    )

    fetcher = Fetcher(settings)
    try:
        await enrich_gender_probabilities_for_products(
            products=[product],
            fetcher=fetcher,
            concurrency=settings.image_validation_concurrency,
        )
    finally:
        await fetcher.close()

    return product.gender_probs_csv


def main() -> int:
    args = _parser().parse_args()
    s = get_settings()
    base = f"{s.supabase_url.rstrip('/')}/rest/v1"
    headers = {
        "Authorization": f"Bearer {s.supabase_service_role_key}",
        "apikey": s.supabase_service_role_key,
        "Content-Type": "application/json",
    }

    with httpx.Client(timeout=30.0) as client:
        resp = client.get(
            f"{base}/shopify_products",
            params={
                "select": "id,store_id,product_id,item_name,images,supabase_images,gender_label,gender_probs_csv",
                "order": "id.desc",
                "limit": str(max(1, args.limit)),
            },
            headers=headers,
        )
        resp.raise_for_status()
        rows = resp.json()

    uploader = StorageUploader(s)
    inspected = 0
    updated = 0
    supa_fixed = 0
    gp_fixed = 0

    with httpx.Client(timeout=30.0) as client:
        for row in rows:
            if not isinstance(row, dict):
                continue
            inspected += 1

            row_id = row.get("id")
            if row_id is None:
                continue

            images = list(row.get("images") or [])
            current_supa = list(row.get("supabase_images") or [])
            needs_supa_fix = bool(current_supa) and any(
                not _is_supabase_public_url(str(url), s.supabase_url, s.supabase_storage_bucket)
                for url in current_supa
            )
            needs_gp_fix = row.get("gender_probs_csv") == _SYNTHETIC

            if not needs_supa_fix and not needs_gp_fix:
                continue

            patch: dict[str, object] = {}

            if needs_supa_fix and images and row.get("store_id") is not None and row.get("product_id") is not None:
                try:
                    uploaded = uploader.upload_product_images(
                        images,
                        store_id=int(row["store_id"]),
                        product_id=str(row["product_id"]),
                    )
                    patch["supabase_images"] = uploaded
                    supa_fixed += 1
                except Exception:
                    pass

            if needs_gp_fix:
                try:
                    recomputed = asyncio.run(_recompute_gender_probs(row, s))
                    patch["gender_probs_csv"] = recomputed
                    gp_fixed += 1
                except Exception:
                    pass

            if not patch:
                continue

            upd = client.patch(
                f"{base}/shopify_products",
                params={"id": f"eq.{row_id}"},
                json=patch,
                headers={**headers, "Prefer": "return=minimal"},
            )
            upd.raise_for_status()
            updated += 1

    print({
        "inspected": inspected,
        "updated_rows": updated,
        "supabase_images_fixed": supa_fixed,
        "gender_probs_fixed": gp_fixed,
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
