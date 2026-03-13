from __future__ import annotations

import argparse
import asyncio
import json
import time
import traceback
import warnings

from aisley_scraper.config import get_settings
from aisley_scraper.crawl.fetcher import Fetcher
from aisley_scraper.crawl.image_verifier import verify_product_images
from aisley_scraper.extract.shopify_products import extract_products_from_products_json
from aisley_scraper.extract.store_profile import classify_store
from aisley_scraper.gender_probs import enrich_gender_probabilities_for_products
from aisley_scraper.ingest.csv_loader import load_store_seeds


warnings.filterwarnings(
    "ignore",
    message="Palette images with Transparency expressed in bytes should be converted to RGBA images",
    category=UserWarning,
)


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fast stage-timed diagnosis for one store")
    p.add_argument("--seed-index", type=int, default=0, help="0-based index into loaded seeds")
    p.add_argument("--max-pages", type=int, default=25, help="Max Shopify products.json pages to inspect")
    p.add_argument("--skip-image-validation", action="store_true")
    p.add_argument("--skip-gender-enrichment", action="store_true")
    p.add_argument(
        "--max-products",
        type=int,
        default=100,
        help="Cap number of products processed in validation/enrichment for fast diagnosis",
    )
    return p


async def _run() -> int:
    args = _parser().parse_args()
    settings = get_settings()

    seeds = load_store_seeds(settings.input_csv_path, settings)
    if not seeds:
        print(json.dumps({"error": "no seeds loaded"}))
        return 1
    if args.seed_index < 0 or args.seed_index >= len(seeds):
        print(json.dumps({"error": "seed index out of range", "seed_count": len(seeds)}))
        return 1

    seed = seeds[args.seed_index]
    base = seed.store_url.rstrip("/")

    fetcher = Fetcher(settings)
    timings: dict[str, float] = {}
    page_counts: list[int] = []

    try:
        t0 = time.perf_counter()
        homepage = await fetcher.get_text(base)
        store = classify_store(homepage, base, settings)
        timings["homepage_and_classify_sec"] = round(time.perf_counter() - t0, 3)

        extracted = []
        seen_ids: set[str] = set()

        t1 = time.perf_counter()
        page_limit = max(1, settings.shopify_products_page_limit)
        max_pages = max(1, args.max_pages)
        for page in range(1, max_pages + 1):
            products_url = f"{base}/products.json?limit={page_limit}&page={page}"
            payload = await fetcher.get_json(products_url)
            products_raw = payload.get("products", []) if isinstance(payload, dict) else []
            if not isinstance(products_raw, list):
                break
            page_counts.append(len(products_raw))
            if not products_raw:
                break

            page_products = extract_products_from_products_json(payload, settings, base_url=base)
            for p in page_products:
                if p.product_id in seen_ids:
                    continue
                seen_ids.add(p.product_id)
                extracted.append(p)

        timings["fetch_products_pages_sec"] = round(time.perf_counter() - t1, 3)

        extracted_count_before_validation = len(extracted)
        if args.max_products > 0 and len(extracted) > args.max_products:
            extracted = extracted[: args.max_products]

        before_validation_images_by_product = {
            p.product_id: list(p.images)
            for p in extracted
        }
        before_validation_with_images = sum(1 for p in extracted if p.images)

        validation_error = None
        if not args.skip_image_validation:
            t2 = time.perf_counter()
            try:
                await verify_product_images(products=extracted, fetcher=fetcher, settings=settings)
            except Exception as exc:
                validation_error = {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "traceback": traceback.format_exc(limit=20),
                }
            timings["image_validation_sec"] = round(time.perf_counter() - t2, 3)

        after_validation_with_images = sum(1 for p in extracted if p.images)
        rejected_image_urls: list[str] = []
        before_image_urls: list[str] = []
        for urls in before_validation_images_by_product.values():
            for url in urls:
                if url:
                    before_image_urls.append(url)
                if len(before_image_urls) >= 20:
                    break
            if len(before_image_urls) >= 20:
                break

        for product in extracted:
            before = before_validation_images_by_product.get(product.product_id, [])
            after = set(product.images)
            rejected_image_urls.extend([url for url in before if url not in after])

        gender_error = None
        if not args.skip_gender_enrichment and validation_error is None:
            t3 = time.perf_counter()
            try:
                await enrich_gender_probabilities_for_products(
                    products=extracted,
                    fetcher=fetcher,
                    concurrency=settings.image_validation_concurrency,
                )
            except Exception as exc:
                gender_error = {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "traceback": traceback.format_exc(limit=20),
                }
            timings["gender_enrichment_sec"] = round(time.perf_counter() - t3, 3)

        unresolved_gender = sum(
            1
            for p in extracted
            if p.images and not p.gender_probs_csv
        )

        print(
            json.dumps(
                {
                    "seed_index": args.seed_index,
                    "store_url": base,
                    "store_name": store.store_name,
                    "pages_scanned": len(page_counts),
                    "page_counts_first10": page_counts[:10],
                    "products_extracted_unique_before_validation": extracted_count_before_validation,
                    "products_with_images_before_validation": before_validation_with_images,
                    "products_with_images_after_validation": after_validation_with_images,
                    "before_image_url_samples": before_image_urls[:10],
                    "rejected_image_url_samples": rejected_image_urls[:10],
                    "products_with_missing_gender_probs": unresolved_gender,
                    "timings": timings,
                    "validation_error": validation_error,
                    "gender_error": gender_error,
                },
                ensure_ascii=True,
            )
        )
        return 0
    finally:
        await fetcher.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_run()))
