from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime

import httpx

from aisley_scraper.config import get_settings
from aisley_scraper.crawl.fetcher import Fetcher
from aisley_scraper.gender_probs import enrich_gender_probabilities_for_products
from aisley_scraper.models import ProductRecord


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Backfill gender_probs_csv for existing shopify_products rows")
    p.add_argument("--limit", type=int, default=100, help="Max rows to process")
    p.add_argument("--chunk-size", type=int, default=10, help="Rows per scoring chunk")
    return p


def _rest_headers(service_role_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {service_role_key}",
        "apikey": service_role_key,
        "Content-Type": "application/json",
    }


def _to_product_record(row: dict[str, object]) -> ProductRecord:
    return ProductRecord(
        product_id=str(row.get("product_id") or ""),
        product_handle=(str(row["product_handle"]) if row.get("product_handle") is not None else None),
        item_name=str(row.get("item_name") or ""),
        description=(str(row["description"]) if row.get("description") is not None else None),
        images=list(row.get("images") or []),
        gender_label=(str(row["gender_label"]) if row.get("gender_label") is not None else None),
    )


async def _score_chunk(
    records: list[tuple[dict[str, object], ProductRecord]],
    fetcher: Fetcher,
    concurrency: int,
) -> None:
    products = [rec for _, rec in records]
    await enrich_gender_probabilities_for_products(
        products=products,
        fetcher=fetcher,
        concurrency=concurrency,
    )


async def _amain(args: argparse.Namespace) -> int:
    settings = get_settings()

    base_url = f"{settings.supabase_url.rstrip('/')}/rest/v1"
    headers = _rest_headers(settings.supabase_service_role_key)

    with httpx.Client(timeout=30.0) as client:
        resp = client.get(
            f"{base_url}/shopify_products",
            params={
                "select": "id,product_id,product_handle,item_name,description,images,gender_label,gender_probs_csv",
                "gender_probs_csv": "is.null",
                "order": "id.desc",
                "limit": str(max(1, args.limit)),
            },
            headers=headers,
        )
        resp.raise_for_status()
        rows = resp.json()

    if not isinstance(rows, list) or not rows:
        print("No rows need backfill.")
        return 0

    records: list[tuple[dict[str, object], ProductRecord]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        records.append((row, _to_product_record(row)))

    fetcher = Fetcher(settings)
    try:
        for i in range(0, len(records), max(1, args.chunk_size)):
            chunk = records[i : i + max(1, args.chunk_size)]
            await _score_chunk(chunk, fetcher, settings.image_validation_concurrency)
    finally:
        await fetcher.close()

    updated = 0
    skipped = 0
    now_iso = datetime.now(UTC).isoformat()

    with httpx.Client(timeout=30.0) as client:
        for row, product in records:
            row_id = row.get("id")
            if row_id is None:
                skipped += 1
                continue
            if product.gender_probs_csv is None:
                skipped += 1
                continue

            patch_resp = client.patch(
                f"{base_url}/shopify_products",
                params={"id": f"eq.{row_id}"},
                json={"gender_probs_csv": product.gender_probs_csv, "last_seen_at": now_iso},
                headers={**headers, "Prefer": "return=minimal"},
            )
            patch_resp.raise_for_status()
            updated += 1

    print(f"Processed: {len(records)}")
    print(f"Updated gender_probs_csv: {updated}")
    print(f"Skipped (still unresolved): {skipped}")
    return 0


def main() -> int:
    args = _parser().parse_args()
    return asyncio.run(_amain(args))


if __name__ == "__main__":
    raise SystemExit(main())
