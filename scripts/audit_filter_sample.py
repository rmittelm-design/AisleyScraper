from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from urllib.parse import urlparse

from aisley_scraper.config import get_settings
from aisley_scraper.crawl.fetcher import Fetcher
from aisley_scraper.db.supabase_rest_repository import SupabaseRestRepository
from aisley_scraper.image_validation import ImageValidationFailure, validate_product_photo_only


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()

    settings = get_settings()
    repo = SupabaseRestRepository(settings)
    fetcher = Fetcher(settings)
    threshold = float(settings.phase2_first_image_product_prob_threshold)
    timeout_sec = float(settings.image_validation_attempt_timeout_sec)

    try:
        rows = repo.list_products_for_first_image_validation_scan(limit=max(1, args.limit), after_id=None)
        print(f"sampled_rows={len(rows)} threshold={threshold:.2f}")
        counts: Counter[str] = Counter()

        for index, row in enumerate(rows, start=1):
            images = row.get("images") if isinstance(row, dict) else None
            product_id = row.get("product_id") if isinstance(row, dict) else None
            item_uuid = row.get("item_uuid") if isinstance(row, dict) else None
            first_image = ""
            if isinstance(images, list):
                for value in images:
                    if isinstance(value, str) and value.strip():
                        first_image = value.strip()
                        break

            if not first_image:
                counts["missing_image"] += 1
                print(f"{index}. product_id={product_id} item_uuid={item_uuid} result=missing_image")
                continue

            host = urlparse(first_image).netloc
            try:
                content = await asyncio.wait_for(fetcher.get_bytes(first_image), timeout=timeout_sec)
                result = await asyncio.wait_for(
                    asyncio.to_thread(
                        validate_product_photo_only,
                        content=content,
                        filename=str(product_id or ""),
                        min_product_prob=threshold,
                    ),
                    timeout=timeout_sec,
                )
                product = result.get("product") if isinstance(result, dict) else None
                score = None
                if isinstance(product, dict):
                    value = product.get("product_prob")
                    if isinstance(value, (int, float)):
                        score = float(value)
                counts["keep"] += 1
                print(
                    f"{index}. product_id={product_id} item_uuid={item_uuid} result=keep score={score} host={host} url={first_image}"
                )
            except asyncio.TimeoutError:
                counts["timeout"] += 1
                print(
                    f"{index}. product_id={product_id} item_uuid={item_uuid} result=timeout host={host} url={first_image}"
                )
            except ImageValidationFailure as exc:
                details = exc.details or {}
                score = details.get("product_prob") if isinstance(details, dict) else None
                counts[exc.code] += 1
                print(
                    f"{index}. product_id={product_id} item_uuid={item_uuid} result={exc.code} score={score} host={host} url={first_image}"
                )
            except Exception as exc:
                counts[type(exc).__name__] += 1
                print(
                    f"{index}. product_id={product_id} item_uuid={item_uuid} result=error error={type(exc).__name__}:{exc} host={host} url={first_image}"
                )

        print(f"summary={dict(counts)}")
        return 0
    finally:
        await fetcher.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
