from __future__ import annotations

import argparse
import asyncio
import logging
from urllib.parse import urlparse

from aisley_scraper.config import get_settings
from aisley_scraper.crawl.fetcher import Fetcher
from aisley_scraper.crawl.orchestrator import scrape_many, scrape_many_stream
from aisley_scraper.crawl.image_verifier import verify_product_images
from aisley_scraper.db.supabase_rest_repository import SupabaseRestRepository
from aisley_scraper.gender_probs import enrich_gender_probabilities_for_products
from aisley_scraper.ingest.csv_loader import load_store_seeds
from aisley_scraper.local_output import write_local_results
from aisley_scraper.models import ScrapeResult, StoreSeed
from aisley_scraper.normalize.products import enforce_attribute_policy
from aisley_scraper.storage import StorageUploader


def _dedupe_seeds_by_domain(seeds: list[StoreSeed]) -> list[StoreSeed]:
    seen_domains: set[str] = set()
    deduped: list[StoreSeed] = []
    for seed in seeds:
        domain = urlparse(seed.store_url).netloc.strip().lower()
        if not domain or domain in seen_domains:
            continue
        seen_domains.add(domain)
        deduped.append(seed)
    return deduped


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="aisley-scraper")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest-stores")
    ingest.add_argument("--csv", required=False)

    crawl = sub.add_parser("crawl-stores")
    crawl.add_argument("--limit", type=int, default=None)

    return parser


def _setup_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO))


def run_ingest(csv_path: str | None) -> int:
    settings = get_settings()
    _setup_logging(settings.log_level)

    path = csv_path or settings.input_csv_path
    seeds = load_store_seeds(path, settings)
    print(f"Loaded {len(seeds)} stores from {path}")
    return 0


def run_crawl(limit: int | None) -> int:
    settings = get_settings()
    _setup_logging(settings.log_level)

    seeds = load_store_seeds(settings.input_csv_path, settings)
    seeds = _dedupe_seeds_by_domain(seeds)
    if limit is not None:
        seeds = seeds[:limit]
    else:
        seeds = seeds[: settings.crawl_max_stores_per_run]

    if settings.persistence_target == "local":
        results = asyncio.run(scrape_many(seeds, settings))
        success_count, fail_count = write_local_results(settings.local_output_path, results)
        print(
            f"Crawled {success_count}/{len(results)} stores successfully; "
            f"saved local output to {settings.local_output_path} ({fail_count} failed)"
        )
        return 0

    # Persist in batches so rows start appearing in Supabase before the full crawl ends.
    chunk_size = max(1, settings.crawl_global_concurrency)

    repo = SupabaseRestRepository(settings)

    try:
        repo.ensure_schema()
        uploader = StorageUploader(settings)

        success_count = 0
        processed_count = 0

        def _persist_store_result(seed: StoreSeed, outcome: ScrapeResult | Exception) -> bool:
            if isinstance(outcome, Exception):
                print(f"FAIL {seed.store_url}: {outcome}")
                return False

            store_id = repo.upsert_store(outcome.store)
            existing_state_by_product_id: dict[str, tuple[list[str], list[str]] | None] = {}
            preliminary_products: list = []
            original_images_by_product_id: dict[str, list[str]] = {}

            for product in outcome.products:
                existing_image_state = repo.get_product_image_state(store_id, product.product_id)
                existing_state_by_product_id[product.product_id] = existing_image_state
                original_images_by_product_id[product.product_id] = list(product.images)
                if existing_image_state is None and product.unavailable:
                    continue

                # Insert early so rows become visible while validation/upload continues.
                repo.upsert_product(store_id, product)
                preliminary_products.append(product)

            if not preliminary_products:
                return True

            async def _postprocess_products(products: list) -> None:
                postprocess_fetcher = Fetcher(settings)
                try:
                    await verify_product_images(
                        products=products,
                        fetcher=postprocess_fetcher,
                        settings=settings,
                    )
                    await enrich_gender_probabilities_for_products(
                        products=products,
                        fetcher=postprocess_fetcher,
                        concurrency=settings.image_validation_concurrency,
                    )
                finally:
                    await postprocess_fetcher.close()

            async def _enrich_products_only(products: list) -> None:
                enrich_fetcher = Fetcher(settings)
                try:
                    await enrich_gender_probabilities_for_products(
                        products=products,
                        fetcher=enrich_fetcher,
                        concurrency=settings.image_validation_concurrency,
                    )
                finally:
                    await enrich_fetcher.close()

            processing_products = list(preliminary_products)
            asyncio.run(_postprocess_products(processing_products))
            processing_products = [enforce_attribute_policy(p) for p in processing_products if p.images]

            finalized_ids = {p.product_id for p in processing_products}
            for product in preliminary_products:
                if product.product_id in finalized_ids:
                    continue
                # Fallback: preserve original source images and upload/sync even if validation rejected all.
                product.images = original_images_by_product_id.get(product.product_id, [])
                existing_image_state = existing_state_by_product_id.get(product.product_id)
                if existing_image_state is None:
                    if product.images:
                        if not product.gender_probs_csv:
                            asyncio.run(_enrich_products_only([product]))
                        product.supabase_images = uploader.upload_product_images(
                            product.images,
                            store_id=store_id,
                            product_id=product.product_id,
                        )
                    else:
                        product.supabase_images = []
                else:
                    if product.images and not product.gender_probs_csv:
                        asyncio.run(_enrich_products_only([product]))
                    existing_images, existing_supabase_images = existing_image_state
                    product.supabase_images = uploader.sync_product_images(
                        current_source_urls=product.images,
                        existing_source_urls=existing_images,
                        existing_supabase_urls=existing_supabase_images,
                        store_id=store_id,
                        product_id=product.product_id,
                    )
                repo.upsert_product(store_id, product)

            for product in processing_products:
                existing_image_state = existing_state_by_product_id.get(product.product_id)
                if existing_image_state is None:
                    if product.images:
                        product.supabase_images = uploader.upload_product_images(
                            product.images,
                            store_id=store_id,
                            product_id=product.product_id,
                        )
                else:
                    existing_images, existing_supabase_images = existing_image_state
                    product.supabase_images = uploader.sync_product_images(
                        current_source_urls=product.images,
                        existing_source_urls=existing_images,
                        existing_supabase_urls=existing_supabase_images,
                        store_id=store_id,
                        product_id=product.product_id,
                    )

                repo.upsert_product(store_id, product)

            return True

        async def _persist_batch_stream(batch: list[StoreSeed]) -> tuple[int, int]:
            processed_in_batch = 0
            success_in_batch = 0

            async for seed, outcome in scrape_many_stream(batch, settings, include_postprocess=False):
                processed_in_batch += 1
                persisted_ok = await asyncio.to_thread(_persist_store_result, seed, outcome)
                if persisted_ok:
                    success_in_batch += 1

            return processed_in_batch, success_in_batch

        for start in range(0, len(seeds), chunk_size):
            batch = seeds[start : start + chunk_size]
            processed_in_batch, success_in_batch = asyncio.run(_persist_batch_stream(batch))
            processed_count += processed_in_batch
            success_count += success_in_batch

            print(f"Progress: persisted {processed_count}/{len(seeds)} stores")

        print(f"Crawled {success_count}/{len(seeds)} stores successfully")
        return 0
    finally:
        pass


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "ingest-stores":
        return run_ingest(args.csv)
    if args.command == "crawl-stores":
        return run_crawl(args.limit)

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
