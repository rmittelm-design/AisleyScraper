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


logger = logging.getLogger(__name__)


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
            placeholder_inserted_product_ids: set[str] = set()

            def _cleanup_placeholder_rows(product_ids: list[str]) -> None:
                delete_product = getattr(repo, "delete_product", None)
                if not callable(delete_product):
                    return

                for product_id in product_ids:
                    try:
                        delete_product(store_id, product_id)
                    except Exception as exc:
                        logger.warning(
                            "Placeholder cleanup failed for store=%s product=%s: %s",
                            store_id,
                            product_id,
                            exc,
                        )

            def _skip_no_image_product(product) -> bool:
                if product.images:
                    return False
                if product.product_id in placeholder_inserted_product_ids:
                    _cleanup_placeholder_rows([product.product_id])
                return True

            for product in outcome.products:
                existing_image_state = repo.get_product_image_state(store_id, product.product_id)
                existing_state_by_product_id[product.product_id] = existing_image_state
                original_images_by_product_id[product.product_id] = list(product.images)
                if existing_image_state is None and product.unavailable:
                    continue

                # Do not persist products until final required fields are ready.
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

            def _safe_upload_new_product_images(product) -> list[str]:
                if not product.images:
                    return []
                try:
                    return uploader.upload_product_images(
                        product.images,
                        store_id=store_id,
                        product_id=product.product_id,
                    )
                except Exception as exc:
                    logger.warning(
                        "Image upload failed for store=%s product=%s: %s",
                        store_id,
                        product.product_id,
                        exc,
                    )
                    return []

            def _safe_sync_existing_product_images(
                product,
                existing_images: list[str],
                existing_supabase_images: list[str],
            ) -> list[str]:
                try:
                    return uploader.sync_product_images(
                        current_source_urls=product.images,
                        existing_source_urls=existing_images,
                        existing_supabase_urls=existing_supabase_images,
                        store_id=store_id,
                        product_id=product.product_id,
                        delete_stale=False,
                    )
                except Exception as exc:
                    logger.warning(
                        "Image sync failed for store=%s product=%s: %s",
                        store_id,
                        product.product_id,
                        exc,
                    )
                    return existing_supabase_images

            def _safe_upsert_product(product) -> None:
                if product.images and (not product.supabase_images or not product.gender_probs_csv):
                    logger.warning(
                        "Skipping final upsert with incomplete required fields for store=%s product=%s",
                        store_id,
                        product.product_id,
                    )
                    return False

                attempts = 3
                for attempt in range(1, attempts + 1):
                    try:
                        repo.upsert_product(store_id, product)
                        return True
                    except Exception as exc:
                        logger.warning(
                            "Final upsert failed for store=%s product=%s attempt=%s/%s: %s",
                            store_id,
                            product.product_id,
                            attempt,
                            attempts,
                            exc,
                        )
                        if attempt < attempts:
                            continue
                return False

            def _cleanup_new_uploads_after_upsert_failure(
                product,
                existing_image_state: tuple[list[str], list[str]] | None,
            ) -> None:
                current_urls = list(product.supabase_images or [])
                if not current_urls:
                    return

                if existing_image_state is None:
                    to_delete = current_urls
                else:
                    _, existing_supabase_images = existing_image_state
                    existing_set = set(existing_supabase_images)
                    to_delete = [url for url in current_urls if url not in existing_set]

                if not to_delete:
                    return

                try:
                    uploader.delete_images(to_delete)
                except Exception as exc:
                    logger.warning(
                        "Cleanup of orphan uploads failed for store=%s product=%s: %s",
                        store_id,
                        product.product_id,
                        exc,
                    )

            def _delete_stale_after_success(
                product,
                existing_image_state: tuple[list[str], list[str]] | None,
            ) -> None:
                if existing_image_state is None:
                    return
                _, existing_supabase_images = existing_image_state
                if not existing_supabase_images:
                    return
                current_set = set(product.supabase_images or [])
                stale_urls = [url for url in existing_supabase_images if url not in current_set]
                if not stale_urls:
                    return
                try:
                    uploader.delete_images(stale_urls)
                except Exception as exc:
                    logger.warning(
                        "Cleanup of stale uploads failed for store=%s product=%s: %s",
                        store_id,
                        product.product_id,
                        exc,
                    )

            processing_products = list(preliminary_products)
            postprocess_failed = False
            try:
                asyncio.run(_postprocess_products(processing_products))
                processing_products = [enforce_attribute_policy(p) for p in processing_products if p.images]
            except Exception as exc:
                # Do not leave early-upserted rows incomplete when postprocess fails.
                logger.warning("Postprocess failed for %s: %s", seed.store_url, exc)
                postprocess_failed = True
                processing_products = []

            finalized_ids = {p.product_id for p in processing_products}
            final_upsert_failures: list[str] = []

            fallback_products = [
                product
                for product in preliminary_products
                if product.product_id not in finalized_ids
            ]

            # Restore source images first, then run a single enrichment batch.
            for product in fallback_products:
                product.images = original_images_by_product_id.get(product.product_id, [])

            fallback_products_needing_enrich = [
                product
                for product in fallback_products
                if product.images and not product.gender_probs_csv
            ]
            if fallback_products_needing_enrich:
                try:
                    asyncio.run(_enrich_products_only(fallback_products_needing_enrich))
                except Exception as exc:
                    logger.warning(
                        "Fallback gender enrichment batch failed for %s (store_id=%s): %s",
                        seed.store_url,
                        store_id,
                        exc,
                    )

            for product in preliminary_products:
                if product.product_id in finalized_ids:
                    continue
                if _skip_no_image_product(product):
                    continue
                existing_image_state = existing_state_by_product_id.get(product.product_id)
                if existing_image_state is None:
                    if product.images:
                        product.supabase_images = _safe_upload_new_product_images(product)
                    else:
                        product.supabase_images = []
                else:
                    existing_images, existing_supabase_images = existing_image_state
                    product.supabase_images = _safe_sync_existing_product_images(
                        product,
                        existing_images,
                        existing_supabase_images,
                    )
                if not _safe_upsert_product(product):
                    _cleanup_new_uploads_after_upsert_failure(product, existing_image_state)
                    final_upsert_failures.append(product.product_id)
                else:
                    _delete_stale_after_success(product, existing_image_state)

            for product in processing_products:
                if _skip_no_image_product(product):
                    continue
                existing_image_state = existing_state_by_product_id.get(product.product_id)
                if existing_image_state is None:
                    if product.images:
                        product.supabase_images = _safe_upload_new_product_images(product)
                else:
                    existing_images, existing_supabase_images = existing_image_state
                    product.supabase_images = _safe_sync_existing_product_images(
                        product,
                        existing_images,
                        existing_supabase_images,
                    )

                if not _safe_upsert_product(product):
                    _cleanup_new_uploads_after_upsert_failure(product, existing_image_state)
                    final_upsert_failures.append(product.product_id)
                else:
                    _delete_stale_after_success(product, existing_image_state)

            # One more repair pass for products that still have source images but are missing
            # uploaded image URLs and/or gender probabilities.
            missing_required_fields = [
                product
                for product in preliminary_products
                if product.images and (not product.supabase_images or not product.gender_probs_csv)
            ]
            if missing_required_fields:
                missing_gender = [product for product in missing_required_fields if not product.gender_probs_csv]
                if missing_gender:
                    try:
                        asyncio.run(_enrich_products_only(missing_gender))
                    except Exception as exc:
                        logger.warning(
                            "Repair enrichment batch failed for %s (store_id=%s): %s",
                            seed.store_url,
                            store_id,
                            exc,
                        )

                for product in missing_required_fields:
                    if _skip_no_image_product(product):
                        continue
                    if not product.supabase_images:
                        existing_image_state = existing_state_by_product_id.get(product.product_id)
                        if existing_image_state is None:
                            product.supabase_images = _safe_upload_new_product_images(product)
                        else:
                            existing_images, existing_supabase_images = existing_image_state
                            product.supabase_images = _safe_sync_existing_product_images(
                                product,
                                existing_images,
                                existing_supabase_images,
                            )

                    existing_image_state = existing_state_by_product_id.get(product.product_id)
                    if not _safe_upsert_product(product):
                        _cleanup_new_uploads_after_upsert_failure(product, existing_image_state)
                        final_upsert_failures.append(product.product_id)
                    else:
                        _delete_stale_after_success(product, existing_image_state)

            unresolved_required_fields = [
                product.product_id
                for product in preliminary_products
                if product.images and (not product.supabase_images or not product.gender_probs_csv)
            ]
            if unresolved_required_fields:
                _cleanup_placeholder_rows(
                    [
                        product_id
                        for product_id in unresolved_required_fields
                        if product_id in placeholder_inserted_product_ids
                    ]
                )
                logger.error(
                    "Store finalize unresolved required fields for %s (store_id=%s), products=%s",
                    seed.store_url,
                    store_id,
                    len(unresolved_required_fields),
                )
                return False

            if final_upsert_failures:
                _cleanup_placeholder_rows(
                    [
                        product_id
                        for product_id in final_upsert_failures
                        if product_id in placeholder_inserted_product_ids
                    ]
                )
                logger.error(
                    "Store finalize incomplete for %s (store_id=%s), failed final upserts=%s",
                    seed.store_url,
                    store_id,
                    len(final_upsert_failures),
                )
                return False

            # If postprocess failed globally, all products were handled through fallback branch.
            if postprocess_failed:
                return True

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
