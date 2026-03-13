from __future__ import annotations

import argparse
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from uuid import uuid4
from urllib.parse import urlparse

from aisley_scraper.config import get_settings
from aisley_scraper.crawl.fetcher import Fetcher
from aisley_scraper.crawl.orchestrator import scrape_many, scrape_many_stream
from aisley_scraper.crawl.image_verifier import verify_product_images
from aisley_scraper.db.supabase_rest_repository import SupabaseRestRepository
from aisley_scraper.gender_probs import (
    enrich_gender_probabilities_for_products,
    one_hot_gender_probs_csv,
)
from aisley_scraper.ingest.csv_loader import load_store_seeds
from aisley_scraper.local_output import write_local_results
from aisley_scraper.models import ScrapeResult, StoreSeed
from aisley_scraper.normalize.products import enforce_attribute_policy
from aisley_scraper.storage import StorageUploader
from aisley_scraper.storage_integrity import (
    delete_orphan_storage_objects,
    detect_orphan_storage_objects,
)


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
    crawl.add_argument("--run-id", required=False)
    crawl.add_argument("--fresh", action="store_true")

    return parser


def _setup_logging(level: str) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    file_handler = RotatingFileHandler(
        ".aisley_scraper.log",
        maxBytes=10_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setLevel(root.level)
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(fmt)
    root.addHandler(console_handler)


def _get_store_urls_from_repo(repo: SupabaseRestRepository) -> list[str]:
    list_all = getattr(repo, "list_all_store_websites", None)
    if callable(list_all):
        return list_all()
    return []


def _resolve_run_id(state_path: str, run_id: str | None, fresh: bool) -> str:
    state_file = Path(state_path)

    if fresh:
        resolved = run_id or str(uuid4())
        state_file.write_text(resolved, encoding="utf-8")
        return resolved

    if run_id:
        state_file.write_text(run_id, encoding="utf-8")
        return run_id

    if state_file.exists():
        persisted = state_file.read_text(encoding="utf-8").strip()
        if persisted:
            return persisted

    resolved = str(uuid4())
    state_file.write_text(resolved, encoding="utf-8")
    return resolved


def _run_orphan_preflight(settings, *, batch_size: int = 200) -> None:
    audit = detect_orphan_storage_objects(settings)
    orphan_paths = list(audit["orphan_paths"])
    if not orphan_paths:
        logger.info(
            "Orphan preflight passed linked=%s stored=%s orphans=0",
            audit["linked_paths"],
            audit["stored_paths"],
        )
        return

    deleted = delete_orphan_storage_objects(settings, orphan_paths, batch_size=batch_size)
    logger.warning(
        "Orphan preflight auto-clean deleted=%s linked=%s stored=%s",
        deleted,
        audit["linked_paths"],
        audit["stored_paths"],
    )

    verify = detect_orphan_storage_objects(settings)
    remaining_orphans = list(verify["orphan_paths"])
    if remaining_orphans:
        raise RuntimeError(f"orphan preflight failed: remaining_orphans={len(remaining_orphans)}")


def _build_db_first_seeds(settings, repo: SupabaseRestRepository) -> list[StoreSeed]:
    csv_seeds = _dedupe_seeds_by_domain(load_store_seeds(settings.input_csv_path, settings))
    db_websites = _get_store_urls_from_repo(repo)

    db_seeds = [StoreSeed(store_url=website) for website in db_websites]
    db_seeds = _dedupe_seeds_by_domain(db_seeds)

    seen_domains = {urlparse(seed.store_url).netloc.strip().lower() for seed in db_seeds}
    csv_new = [
        seed
        for seed in csv_seeds
        if urlparse(seed.store_url).netloc.strip().lower() not in seen_domains
    ]
    return db_seeds + csv_new


def run_ingest(csv_path: str | None) -> int:
    settings = get_settings()
    _setup_logging(settings.log_level)

    path = csv_path or settings.input_csv_path
    seeds = load_store_seeds(path, settings)
    print(f"Loaded {len(seeds)} stores from {path}")
    return 0


def run_crawl(
    limit: int | None,
    run_id: str | None = None,
    fresh: bool = False,
    *,
    enforce_preflight: bool = False,
) -> int:
    settings = get_settings()
    _setup_logging(settings.log_level)

    if settings.persistence_target == "local":
        seeds = load_store_seeds(settings.input_csv_path, settings)
        seeds = _dedupe_seeds_by_domain(seeds)
        if limit is not None:
            seeds = seeds[:limit]
        else:
            seeds = seeds[: settings.crawl_max_stores_per_run]
        results = asyncio.run(scrape_many(seeds, settings))
        success_count, fail_count = write_local_results(settings.local_output_path, results)
        print(
            f"Crawled {success_count}/{len(results)} stores successfully; "
            f"saved local output to {settings.local_output_path} ({fail_count} failed)"
        )
        return 0

    # Persist in small batches to keep runtime memory bounded.
    chunk_size = max(
        1,
        min(settings.crawl_store_batch_size, settings.crawl_global_concurrency),
    )

    repo = SupabaseRestRepository(settings)

    try:
        repo.ensure_schema()
        if enforce_preflight:
            _run_orphan_preflight(settings)

        all_seeds = _build_db_first_seeds(settings, repo)
        all_seeds = _dedupe_seeds_by_domain(all_seeds)
        if limit is not None:
            all_seeds = all_seeds[:limit]
        else:
            all_seeds = all_seeds[: settings.crawl_max_stores_per_run]

        resolved_run_id = _resolve_run_id(settings.crawl_run_state_path, run_id, fresh)
        init_run = getattr(repo, "initialize_crawl_run", None)
        if callable(init_run):
            init_run(run_id=resolved_run_id, websites=[seed.store_url for seed in all_seeds])

        list_pending = getattr(repo, "list_all_run_store_websites", None)
        if callable(list_pending):
            eligible_urls = set(
                list_pending(run_id=resolved_run_id, statuses=["pending", "failed"])
            )
            seeds = [seed for seed in all_seeds if seed.store_url in eligible_urls]
        else:
            seeds = all_seeds

        uploader = StorageUploader(settings)

        success_count = 0
        processed_count = 0

        def _persist_store_result(seed: StoreSeed, outcome: ScrapeResult | Exception) -> bool:
            if isinstance(outcome, Exception):
                print(f"FAIL {seed.store_url}: {outcome}")
                return False

            store_id = repo.upsert_store(outcome.store)
            existing_state_by_product_id: dict[
                str,
                tuple[list[str], list[str]] | tuple[list[str], list[str], str | None] | None,
            ] = {}
            preliminary_products: list = []
            original_images_by_product_id: dict[str, list[str]] = {}
            placeholder_inserted_product_ids: set[str] = set()

            def _split_existing_state(
                state: tuple[list[str], list[str]] | tuple[list[str], list[str], str | None] | None,
            ) -> tuple[list[str], list[str], str | None]:
                if state is None:
                    return [], [], None
                if len(state) >= 3:
                    return list(state[0] or []), list(state[1] or []), state[2]
                return list(state[0] or []), list(state[1] or []), None

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

                existing_image_state = existing_state_by_product_id.get(product.product_id)
                if existing_image_state is not None:
                    _, existing_supabase_images = existing_image_state
                    if existing_supabase_images:
                        try:
                            uploader.delete_images(existing_supabase_images)
                        except Exception as exc:
                            logger.warning(
                                "Failed deleting existing images for no-image product store=%s product=%s: %s",
                                store_id,
                                product.product_id,
                                exc,
                            )

                if product.supabase_images:
                    try:
                        uploader.delete_images(product.supabase_images)
                    except Exception as exc:
                        logger.warning(
                            "Failed deleting newly-uploaded images for no-image product store=%s product=%s: %s",
                            store_id,
                            product.product_id,
                            exc,
                        )

                delete_product = getattr(repo, "delete_product", None)
                if callable(delete_product):
                    try:
                        delete_product(store_id, product.product_id)
                    except Exception as exc:
                        logger.warning(
                            "Failed deleting no-image product row store=%s product=%s: %s",
                            store_id,
                            product.product_id,
                            exc,
                        )

                if product.product_id in placeholder_inserted_product_ids:
                    _cleanup_placeholder_rows([product.product_id])
                return True

            for product in outcome.products:
                existing_image_state = repo.get_product_image_state(store_id, product.product_id)
                existing_state_by_product_id[product.product_id] = existing_image_state
                original_images_by_product_id[product.product_id] = list(product.images)

                explicit_gender_probs = one_hot_gender_probs_csv(product.gender_label)
                if explicit_gender_probs is not None:
                    product.gender_probs_csv = explicit_gender_probs
                elif existing_image_state is not None:
                    _, _, existing_gender_probs = _split_existing_state(existing_image_state)
                    if existing_gender_probs:
                        product.gender_probs_csv = existing_gender_probs

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

            def _try_enrich_from_supabase_images(product) -> None:
                if product.gender_probs_csv:
                    return
                if not product.supabase_images:
                    return

                original_images = list(product.images)
                try:
                    # Source CDN URLs can intermittently fail for CLIP fetch; use
                    # already-uploaded Supabase URLs as a second-pass scoring source.
                    product.images = list(product.supabase_images)
                    asyncio.run(_enrich_products_only([product]))
                except Exception as exc:
                    logger.warning(
                        "Supabase-image gender enrichment failed for store=%s product=%s: %s",
                        store_id,
                        product.product_id,
                        exc,
                    )
                finally:
                    product.images = original_images

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
                if product.images and (
                    len(product.supabase_images or []) != len(product.images)
                    or not product.gender_probs_csv
                ):
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

            def _normalize_source_urls(urls: list[str]) -> list[str]:
                return [url.strip() for url in urls if url and url.strip()]

            def _needs_postprocess(product) -> bool:
                existing_image_state = existing_state_by_product_id.get(product.product_id)
                if existing_image_state is None:
                    return True

                existing_images, existing_supabase_images, existing_gender_probs = _split_existing_state(
                    existing_image_state
                )
                if len(existing_images) != len(existing_supabase_images):
                    return True

                current_images = _normalize_source_urls(product.images)
                stored_images = _normalize_source_urls(existing_images)
                if current_images != stored_images:
                    return True

                # Recompute only when existing score is missing.
                return not bool(existing_gender_probs or product.gender_probs_csv)

            processing_products = [
                product for product in preliminary_products if _needs_postprocess(product)
            ]
            postprocess_failed = False
            try:
                if processing_products:
                    asyncio.run(_postprocess_products(processing_products))
                processing_products = [enforce_attribute_policy(p) for p in processing_products if p.images]
            except Exception as exc:
                # Do not leave early-upserted rows incomplete when postprocess fails.
                logger.warning("Postprocess failed for %s: %s", seed.store_url, exc)
                postprocess_failed = True
                processing_products = []

            finalized_ids = {p.product_id for p in processing_products}
            final_upsert_failures: list[str] = []

            fallback_products = []
            if postprocess_failed:
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
                    existing_images, existing_supabase_images, _ = _split_existing_state(
                        existing_image_state
                    )
                    product.supabase_images = _safe_sync_existing_product_images(
                        product,
                        existing_images,
                        existing_supabase_images,
                    )

                _try_enrich_from_supabase_images(product)
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
                    existing_images, existing_supabase_images, _ = _split_existing_state(
                        existing_image_state
                    )
                    product.supabase_images = _safe_sync_existing_product_images(
                        product,
                        existing_images,
                        existing_supabase_images,
                    )

                _try_enrich_from_supabase_images(product)

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
                if product.images
                and (
                    len(product.supabase_images or []) != len(product.images)
                    or not product.gender_probs_csv
                )
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
                            existing_images, existing_supabase_images, _ = _split_existing_state(
                                existing_image_state
                            )
                            product.supabase_images = _safe_sync_existing_product_images(
                                product,
                                existing_images,
                                existing_supabase_images,
                            )

                    _try_enrich_from_supabase_images(product)

                    existing_image_state = existing_state_by_product_id.get(product.product_id)
                    if not _safe_upsert_product(product):
                        _cleanup_new_uploads_after_upsert_failure(product, existing_image_state)
                        final_upsert_failures.append(product.product_id)
                    else:
                        _delete_stale_after_success(product, existing_image_state)

            unresolved_required_fields = [
                product.product_id
                for product in preliminary_products
                if product.images
                and (
                    len(product.supabase_images or []) != len(product.images)
                    or not product.gender_probs_csv
                )
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
            stall_interval = int(getattr(settings, "crawl_stall_log_interval_sec", 60) or 0)

            async for seed, outcome in scrape_many_stream(batch, settings, include_postprocess=False):
                processed_in_batch += 1
                persist_task = asyncio.create_task(asyncio.to_thread(_persist_store_result, seed, outcome))
                while True:
                    try:
                        if stall_interval > 0:
                            persisted_ok = await asyncio.wait_for(
                                asyncio.shield(persist_task),
                                timeout=stall_interval,
                            )
                        else:
                            persisted_ok = await persist_task
                        break
                    except asyncio.TimeoutError:
                        logger.warning(
                            "Store persist still running: store=%s processed_in_batch=%s/%s overall=%s/%s",
                            seed.store_url,
                            processed_in_batch,
                            len(batch),
                            processed_count + processed_in_batch,
                            len(seeds),
                        )

                mark_status = getattr(repo, "mark_run_store_status", None)
                if callable(mark_status):
                    if persisted_ok:
                        mark_status(
                            run_id=resolved_run_id,
                            website=seed.store_url,
                            status="completed",
                        )
                    else:
                        error_message = str(outcome) if isinstance(outcome, Exception) else "store_persist_failed"
                        mark_status(
                            run_id=resolved_run_id,
                            website=seed.store_url,
                            status="failed",
                            error_message=error_message,
                        )

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

        count_status = getattr(repo, "count_run_store_status", None)
        if callable(count_status):
            pending = count_status(run_id=resolved_run_id, status="pending")
            failed = count_status(run_id=resolved_run_id, status="failed")
            if pending == 0 and failed == 0:
                state_file = Path(settings.crawl_run_state_path)
                if state_file.exists():
                    state_file.unlink()

        return 0
    finally:
        pass


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "ingest-stores":
        return run_ingest(args.csv)
    if args.command == "crawl-stores":
        return run_crawl(
            args.limit,
            run_id=args.run_id,
            fresh=args.fresh,
            enforce_preflight=True,
        )

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
