from __future__ import annotations

import argparse
import asyncio
from collections import Counter
import errno
import gc
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import sys
from uuid import uuid4
from urllib.parse import urlparse

from aisley_scraper.config import get_settings
from aisley_scraper.crawl.fetcher import Fetcher
from aisley_scraper.crawl.orchestrator import scrape_many, scrape_many_stream
from aisley_scraper.crawl.image_verifier import (
    evaluate_first_image_product_validation,
    verify_first_image_product_validation,
    verify_product_images,
)
from aisley_scraper.db.supabase_rest_repository import SupabaseRestRepository
from aisley_scraper.diagnostics import diagnose_staged_runs
from aisley_scraper.extract.shopify_products import extract_products_from_products_json
from aisley_scraper.extract.store_profile import classify_store
from aisley_scraper.geocoding import geocode_address
from aisley_scraper.gender_probs import (
    enrich_gender_probabilities_for_products,
    one_hot_gender_probs_csv,
)
from aisley_scraper.ingest.csv_loader import load_store_seeds
from aisley_scraper.local_output import write_local_results
from aisley_scraper.models import ProductRecord, ScrapeResult, StoreProfile, StoreSeed
from aisley_scraper.normalize.products import enforce_attribute_policy, normalize_product
from aisley_scraper.storage import StorageUploader
from aisley_scraper.storage_integrity import (
    delete_orphan_storage_objects,
    detect_orphan_storage_objects,
)


logger = logging.getLogger(__name__)


class _DiskSafeRotatingFileHandler(RotatingFileHandler):
    """File handler that auto-disables itself when the disk is full."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._disabled_for_disk_full = False

    def emit(self, record: logging.LogRecord) -> None:
        if self._disabled_for_disk_full:
            return
        try:
            super().emit(record)
        except OSError as exc:
            if getattr(exc, "errno", None) != errno.ENOSPC:
                raise
            self._disabled_for_disk_full = True
            try:
                self.acquire()
                if self.stream is not None:
                    self.stream.close()
                    self.stream = None
            finally:
                self.release()
            print(
                "WARNING: Disk full; disabling .aisley_scraper.log file logging for this run.",
                file=sys.stderr,
            )


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


def _chunk_products_for_phase2(
    products: list[ProductRecord],
    *,
    max_products: int,
    max_unique_image_urls: int,
    max_images_per_product_for_budget: int | None = None,
) -> list[list[ProductRecord]]:
    if not products:
        return []

    capped_max_products = max(1, int(max_products))
    capped_max_unique_image_urls = max(1, int(max_unique_image_urls))
    capped_max_images_per_product = (
        max(1, int(max_images_per_product_for_budget))
        if max_images_per_product_for_budget is not None
        else None
    )

    chunks: list[list[ProductRecord]] = []
    current_chunk: list[ProductRecord] = []
    current_urls: set[str] = set()

    for product in products:
        source_images = (
            product.images[:capped_max_images_per_product]
            if capped_max_images_per_product is not None
            else product.images
        )
        product_urls = {
            image_url.strip()
            for image_url in source_images
            if image_url and image_url.strip()
        }
        next_urls = current_urls | product_urls
        would_exceed_product_cap = len(current_chunk) >= capped_max_products
        would_exceed_url_cap = bool(current_chunk) and len(next_urls) > capped_max_unique_image_urls

        if would_exceed_product_cap or would_exceed_url_cap:
            chunks.append(current_chunk)
            current_chunk = [product]
            current_urls = set(product_urls)
            continue

        current_chunk.append(product)
        current_urls = next_urls

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="aisley-scraper")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest-stores")
    ingest.add_argument("--csv", required=False)

    sub.add_parser("diagnose-staged-runs")

    cleanup = sub.add_parser("cleanup-runs", help="Delete all temporary staging rows except the active run")
    cleanup.add_argument("--run-id", required=False, help="Active run ID to keep (default: read from state file)")

    filter_products = sub.add_parser(
        "filter-shopify-products",
        help=(
            "Delete existing shopify_products rows whose first image fails product-photo validation "
            "below PHASE2_FIRST_IMAGE_PRODUCT_PROB_THRESHOLD"
        ),
    )
    filter_products.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of rows to scan",
    )
    filter_products.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Scan batch size",
    )
    filter_products.add_argument(
        "--dry-run",
        action="store_true",
        help="Report rows that would be deleted without deleting",
    )

    crawl = sub.add_parser("crawl-stores")
    crawl.add_argument("--limit", type=int, default=None)
    crawl.add_argument("--run-id", required=False)
    crawl.add_argument("--fresh", action="store_true")
    crawl.add_argument(
        "--skip-image-upload",
        action="store_true",
        help="Skip uploading product images to Supabase Storage",
    )
    crawl.add_argument(
        "--phase",
        choices=["1", "2", "both"],
        default="both",
        help=(
            "1=scrape to staging only; "
            "2=enrich staged data and write to production; "
            "both=standard single-phase run (default)"
        ),
    )

    return parser


def _setup_logging(level: str) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    file_handler = _DiskSafeRotatingFileHandler(
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

    # Keep third-party HTTP client chatter out of the rotating file logs.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def _clear_fetcher_disk_cache(settings) -> tuple[int, Path | None]:
    if not getattr(settings, "fetcher_disk_cache_enabled", False):
        return 0, None

    cache_dir = Path(settings.fetcher_disk_cache_dir)
    if not cache_dir.exists():
        return 0, cache_dir

    removed = 0
    for pattern in ("*.tmp",):
        for file_path in cache_dir.glob(pattern):
            try:
                file_path.unlink()
                removed += 1
            except FileNotFoundError:
                continue
            except Exception as exc:
                logger.warning("Failed to remove stale fetcher disk cache file %s: %s", file_path, exc)

    if removed:
        logger.info("Cleared stale fetcher disk cache temp files=%s dir=%s", removed, cache_dir)
    return removed, cache_dir


def _get_store_urls_from_repo(repo: SupabaseRestRepository) -> list[str]:
    list_all = getattr(repo, "list_all_store_websites", None)
    if callable(list_all):
        return list_all()
    return []


def _resolve_run_id(state_path: str, run_id: str | None, fresh: bool) -> tuple[str, str | None]:
    """Return (new_run_id, old_run_id_to_purge). old_run_id_to_purge is set only on --fresh."""
    state_file = Path(state_path)

    if fresh:
        old_run_id: str | None = None
        if state_file.exists():
            persisted = state_file.read_text(encoding="utf-8").strip()
            if persisted:
                old_run_id = persisted
        resolved = run_id or str(uuid4())
        state_file.write_text(resolved, encoding="utf-8")
        return resolved, old_run_id

    if run_id:
        state_file.write_text(run_id, encoding="utf-8")
        return run_id, None

    if state_file.exists():
        persisted = state_file.read_text(encoding="utf-8").strip()
        if persisted:
            return persisted, None

    resolved = str(uuid4())
    state_file.write_text(resolved, encoding="utf-8")
    return resolved, None


def _resolve_existing_run_id(state_path: str, run_id: str | None) -> str:
    state_file = Path(state_path)

    if run_id:
        state_file.write_text(run_id, encoding="utf-8")
        return run_id

    if state_file.exists():
        persisted = state_file.read_text(encoding="utf-8").strip()
        if persisted:
            return persisted

    raise RuntimeError(
        "Phase 2 requires an existing run ID. Pass --run-id or ensure .aisley_active_run_id exists."
    )


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

    list_profiles = getattr(repo, "list_all_store_profiles", None)
    if callable(list_profiles):
        existing_profiles = list_profiles()
        existing_by_domain: dict[str, StoreProfile] = {}
        for profile in existing_profiles:
            domain = urlparse(profile.website).netloc.strip().lower()
            if domain and domain not in existing_by_domain:
                existing_by_domain[domain] = profile

        geocode_cache: dict[str, tuple[float, float] | None] = {}
        for seed in csv_seeds:
            if not seed.address:
                continue

            domain = urlparse(seed.store_url).netloc.strip().lower()
            if not domain:
                continue

            existing_profile = existing_by_domain.get(domain)
            if existing_profile is None:
                continue

            existing_address = (existing_profile.address or "").strip()
            if existing_address:
                continue

            existing_profile.address = seed.address

            cache_key = seed.address.strip().lower()
            coords = geocode_cache.get(cache_key)
            if cache_key not in geocode_cache:
                user_agent = (settings.user_agent or "").strip() or "aisley-scraper/1.0"
                coords = geocode_address(
                    seed.address,
                    user_agent=user_agent,
                    timeout_sec=float(getattr(settings, "crawl_request_timeout_sec", 25)),
                )
                geocode_cache[cache_key] = coords

            if coords is not None:
                existing_profile.lat, existing_profile.long = coords

            try:
                repo.upsert_store(existing_profile)
            except Exception as exc:
                logger.warning(
                    "Failed store backfill from TSV address for website=%s: %s",
                    existing_profile.website,
                    exc,
                )

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


def run_diagnose_staged_runs() -> int:
    diagnose_staged_runs()
    return 0


def run_crawl(
    limit: int | None,
    run_id: str | None = None,
    fresh: bool = False,
    phase: str = "both",
    skip_image_upload: bool = False,
) -> int:
    settings = get_settings()
    _setup_logging(settings.log_level)
    allow_null_gender_probs = settings.phase2_first_image_product_validation_only

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

    cleared_disk_cache_files, disk_cache_dir = _clear_fetcher_disk_cache(settings)

    repo = SupabaseRestRepository(settings)
    upload_images = False  # Disabled: Phase 2 no longer uploads images to Supabase

    try:
        repo.ensure_schema()

        if phase == "2":
            resolved_run_id = _resolve_existing_run_id(settings.crawl_run_state_path, run_id)
            all_seeds: list[StoreSeed] = []
            seeds: list[StoreSeed] = []
        else:
            all_seeds = _build_db_first_seeds(settings, repo)
            all_seeds = _dedupe_seeds_by_domain(all_seeds)
            if limit is not None:
                all_seeds = all_seeds[:limit]
            else:
                all_seeds = all_seeds[: settings.crawl_max_stores_per_run]

            resolved_run_id, old_run_id = _resolve_run_id(settings.crawl_run_state_path, run_id, fresh)
            if old_run_id and old_run_id != resolved_run_id:
                purge_run = getattr(repo, "purge_run", None)
                if callable(purge_run):
                    try:
                        purge_run(old_run_id)
                        logger.info("Purged previous run staging data run_id=%s", old_run_id)
                    except Exception as exc:
                        logger.warning("Failed to purge previous run run_id=%s: %s", old_run_id, exc)
            if fresh:
                purge_other_runs = getattr(repo, "purge_other_runs", None)
                if callable(purge_other_runs):
                    try:
                        purge_other_runs(resolved_run_id)
                        logger.info(
                            "Purged historical temporary rows excluding run_id=%s",
                            resolved_run_id,
                        )
                    except Exception as exc:
                        logger.warning(
                            "Failed to purge historical temporary rows excluding run_id=%s: %s",
                            resolved_run_id,
                            exc,
                        )
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
        geocode_cache: dict[str, tuple[float, float] | None] = {}

        def _apply_store_geocode_if_available(store: StoreProfile) -> None:
            if not store.address:
                return
            if store.lat is not None and store.long is not None:
                return

            cache_key = store.address.strip().lower()
            if not cache_key:
                return

            if cache_key in geocode_cache:
                coords = geocode_cache[cache_key]
            else:
                user_agent = (settings.user_agent or "").strip() or "aisley-scraper/1.0"
                coords = geocode_address(
                    store.address,
                    user_agent=user_agent,
                    timeout_sec=float(getattr(settings, "crawl_request_timeout_sec", 25)),
                )
                geocode_cache[cache_key] = coords

            if coords is not None:
                store.lat, store.long = coords

        success_count = 0
        processed_count = 0

        def _persist_store_result(seed: StoreSeed, outcome: ScrapeResult | Exception) -> bool:
            if isinstance(outcome, Exception):
                print(f"FAIL {seed.store_url}: {outcome}")
                return False

            _apply_store_geocode_if_available(outcome.store)
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

            bulk_get_states = getattr(repo, "get_product_image_states", None)
            if callable(bulk_get_states):
                try:
                    existing_state_by_product_id = {
                        pid: state
                        for pid, state in bulk_get_states(
                            store_id,
                            [p.product_id for p in outcome.products if p.product_id],
                        ).items()
                    }
                except Exception as exc:
                    logger.warning(
                        "Bulk existing-state fetch failed for store=%s: %s",
                        store_id,
                        exc,
                    )
                    existing_state_by_product_id = {}

            def _skip_no_image_product(product) -> bool:
                if product.images:
                    return False

                existing_image_state = existing_state_by_product_id.get(product.product_id)
                if existing_image_state is not None:
                    _, existing_supabase_images, _ = _split_existing_state(existing_image_state)
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
                existing_image_state = existing_state_by_product_id.get(product.product_id)
                if existing_image_state is None:
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

            chunk_size = max(1, int(settings.postprocess_product_chunk_size))
            image_bytes_by_url: dict[str, bytes] = {}

            def _chunk_products(products: list) -> list[list]:
                return [products[i : i + chunk_size] for i in range(0, len(products), chunk_size)]

            async def _postprocess_products(products: list) -> None:
                postprocess_fetcher = Fetcher(settings)
                try:
                    await verify_product_images(
                        products=products,
                        fetcher=postprocess_fetcher,
                        settings=settings,
                    )
                    # Image cap for validation/scoring: keep all but validate only first N.
                    max_images_for_validation = max(
                        1, settings.phase2_max_images_per_product
                    )
                    original_images_map: dict[str, list[str]] = {}
                    for product in products:
                        if product.images:
                            original_images_map[product.product_id] = list(product.images)
                            product.images = product.images[:max_images_for_validation]
                    try:
                        await enrich_gender_probabilities_for_products(
                            products=products,
                            fetcher=postprocess_fetcher,
                            concurrency=settings.image_validation_concurrency,
                        )
                    finally:
                        # Restore all original images after scoring.
                        for product in products:
                            if product.product_id in original_images_map:
                                product.images = original_images_map[product.product_id]
                    for product in products:
                        for image_url in product.images:
                            normalized_url = image_url.strip()
                            if not normalized_url:
                                continue
                            cached = postprocess_fetcher.get_cached_bytes(normalized_url)
                            if cached is not None:
                                image_bytes_by_url[normalized_url] = cached
                finally:
                    clear_cached_bytes = getattr(postprocess_fetcher, "clear_cached_bytes", None)
                    if callable(clear_cached_bytes):
                        clear_cached_bytes()
                    await postprocess_fetcher.close()

            async def _enrich_products_only(products: list) -> None:
                enrich_fetcher = Fetcher(settings)
                try:
                    # Image cap for validation/scoring: keep all but validate only first N.
                    max_images_for_validation = max(
                        1, settings.phase2_max_images_per_product
                    )
                    original_images_map: dict[str, list[str]] = {}
                    for product in products:
                        if product.images:
                            original_images_map[product.product_id] = list(product.images)
                            product.images = product.images[:max_images_for_validation]
                    try:
                        await enrich_gender_probabilities_for_products(
                            products=products,
                            fetcher=enrich_fetcher,
                            concurrency=settings.image_validation_concurrency,
                        )
                    finally:
                        # Restore all original images after scoring.
                        for product in products:
                            if product.product_id in original_images_map:
                                product.images = original_images_map[product.product_id]
                finally:
                    clear_cached_bytes = getattr(enrich_fetcher, "clear_cached_bytes", None)
                    if callable(clear_cached_bytes):
                        clear_cached_bytes()
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
                if not upload_images:
                    return list(product.supabase_images or [])
                if not product.images:
                    return []
                try:
                    upload_from_cache = getattr(uploader, "upload_product_images_from_cache", None)
                    cached_for_product = {
                        image_url.strip(): image_bytes_by_url[image_url.strip()]
                        for image_url in product.images
                        if image_url and image_url.strip() and image_url.strip() in image_bytes_by_url
                    }
                    if callable(upload_from_cache) and cached_for_product:
                        return upload_from_cache(
                            product.images,
                            store_id,
                            product.product_id,
                            cached_for_product,
                        )
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
                if not upload_images:
                    return list(existing_supabase_images)
                try:
                    sync_from_cache = getattr(uploader, "sync_product_images_from_cache", None)
                    cached_for_product = {
                        image_url.strip(): image_bytes_by_url[image_url.strip()]
                        for image_url in product.images
                        if image_url and image_url.strip() and image_url.strip() in image_bytes_by_url
                    }
                    if callable(sync_from_cache) and cached_for_product:
                        return sync_from_cache(
                            current_source_urls=product.images,
                            existing_source_urls=existing_images,
                            existing_supabase_urls=existing_supabase_images,
                            store_id=store_id,
                            product_id=product.product_id,
                            image_bytes_by_url=cached_for_product,
                            delete_stale=False,
                        )
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
                images_incomplete = upload_images and (
                    len(product.supabase_images or []) != len(product.images)
                )
                if product.images and (images_incomplete or not product.gender_probs_csv):
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
                existing_image_state: tuple[list[str], list[str]] | tuple[list[str], list[str], str | None] | None,
            ) -> None:
                current_urls = list(product.supabase_images or [])
                if not current_urls:
                    return

                if existing_image_state is None:
                    to_delete = current_urls
                else:
                    _, existing_supabase_images, _ = _split_existing_state(existing_image_state)
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
                existing_image_state: tuple[list[str], list[str]] | tuple[list[str], list[str], str | None] | None,
            ) -> None:
                if existing_image_state is None:
                    return
                _, existing_supabase_images, _ = _split_existing_state(existing_image_state)
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
                    processed: list = []
                    for chunk in _chunk_products(processing_products):
                        asyncio.run(_postprocess_products(chunk))
                        processed.extend(
                            normalized
                            for p in chunk
                            if p.images
                            if (normalized := normalize_product(p)) is not None
                        )
                    processing_products = processed
                else:
                    processing_products = []
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
                    for chunk in _chunk_products(fallback_products_needing_enrich):
                        try:
                            asyncio.run(_enrich_products_only(chunk))
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
                # Skip products that are fully populated with no image changes.
                if not _needs_postprocess(product) and existing_image_state is not None:
                    _, existing_supabase_images, existing_gender_probs = _split_existing_state(
                        existing_image_state
                    )
                    if existing_supabase_images and existing_gender_probs:
                        continue

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
                    (upload_images and len(product.supabase_images or []) != len(product.images))
                    or not product.gender_probs_csv
                )
            ]
            if missing_required_fields:
                missing_gender = [product for product in missing_required_fields if not product.gender_probs_csv]
                if missing_gender:
                    for chunk in _chunk_products(missing_gender):
                        try:
                            asyncio.run(_enrich_products_only(chunk))
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
                    (upload_images and len(product.supabase_images or []) != len(product.images))
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

        def _persist_to_staging(seed: StoreSeed, outcome: ScrapeResult | Exception) -> bool:
            """Phase 1: write raw scrape output to staging tables (sync, for thread use)."""
            if isinstance(outcome, Exception):
                print(f"FAIL {seed.store_url}: {outcome}")
                return False
            try:
                _apply_store_geocode_if_available(outcome.store)
                repo.upsert_staged_store(resolved_run_id, outcome.store)
                repo.upsert_staged_products(resolved_run_id, seed.store_url, outcome.products)
                return True
            except Exception as exc:
                logger.warning("Staging persist failed for %s: %s", seed.store_url, exc)
                return False

        def _run_phase1() -> int:
            """
            Phase 1 pipeline — one event loop, one Fetcher, full concurrency.

            scrape_many_stream already handles crawl_global_concurrency via its
            semaphore and yields results as each store completes. Staging writes
            (2 REST calls, ~100ms) run in asyncio.to_thread so they don't block
            the event loop while other stores continue fetching in the background.
            """

            async def _run_async() -> int:
                stall_interval = int(getattr(settings, "crawl_stall_log_interval_sec", 60) or 0)
                success = 0
                done = 0

                # scrape_many_stream with include_postprocess=False: pure JSON fetch,
                # no image validation, no CLIP.  Semaphore concurrency is
                # crawl_global_concurrency; results stream out as stores complete.
                async for seed, outcome in scrape_many_stream(
                    seeds, settings, include_postprocess=False
                ):
                    done += 1

                    # Run the 2-REST-call staging write in a thread so the event
                    # loop stays free for the ongoing concurrent fetches.
                    write_task = asyncio.create_task(
                        asyncio.to_thread(_persist_to_staging, seed, outcome)
                    )
                    while True:
                        try:
                            if stall_interval > 0:
                                ok = await asyncio.wait_for(
                                    asyncio.shield(write_task), timeout=float(stall_interval)
                                )
                            else:
                                ok = await write_task
                            break
                        except asyncio.TimeoutError:
                            logger.warning(
                                "Phase 1 staging write still running: store=%s",
                                seed.store_url,
                            )

                    mark_status = getattr(repo, "mark_run_store_status", None)
                    if callable(mark_status):
                        if ok:
                            mark_status(
                                run_id=resolved_run_id,
                                website=seed.store_url,
                                status="scraped",
                            )
                        else:
                            error_msg = (
                                str(outcome) if isinstance(outcome, Exception)
                                else "staging_write_failed"
                            )
                            mark_status(
                                run_id=resolved_run_id,
                                website=seed.store_url,
                                status="failed",
                                error_message=error_msg,
                            )

                    if ok:
                        success += 1
                    print(f"Phase 1 progress: {done}/{len(seeds)}")

                return success

            return asyncio.run(_run_async())

        def _run_phase2() -> int:
            """
            Bounded three-stage pipeline — processes staged stores in chunks.

            Stage 1: Load staged data, upsert stores, and fetch existing product states
                     in parallel (bounded by crawl_global_concurrency).
            Stage 2: Image validation + CLIP scoring for products in the current
                     chunk of stores.
            Stage 3: Storage uploads + DB upserts for the current chunk concurrently
                     (bounded by image_validation_concurrency for upload operations).
            """

            async def _run_async() -> int:
                fetcher = Fetcher(settings)
                try:
                    io_sem = asyncio.Semaphore(settings.crawl_global_concurrency)
                    upload_sem = asyncio.Semaphore(settings.phase2_upload_concurrency)
                    stall_interval = int(getattr(settings, "crawl_stall_log_interval_sec", 60) or 0)
                    progress_lock = asyncio.Lock()
                    completed_count = 0
                    phase2_store_batch_size = max(
                        1,
                        min(settings.crawl_store_batch_size, settings.crawl_global_concurrency),
                    )
                    phase2_product_chunk_size = max(1, settings.postprocess_product_chunk_size)
                    phase2_unique_url_budget = max(
                        1,
                        min(
                            settings.phase2_max_unique_image_urls_per_chunk,
                            max(1, settings.fetcher_byte_cache_max_mb),
                        ),
                    )

                    def _norm(urls: list[str]) -> list[str]:
                        return [u.strip() for u in (urls or []) if u.strip()]

                    # ── Stage 1: load staged data + upsert stores in parallel ─────
                    async def _load_one(website: str):
                        async with io_sem:
                            staged_store = await asyncio.to_thread(
                                repo.get_staged_store, resolved_run_id, website
                            )
                            if staged_store is None:
                                raise RuntimeError("staging store row missing")
                            await asyncio.to_thread(_apply_store_geocode_if_available, staged_store)
                            staged_products = await asyncio.to_thread(
                                repo.get_staged_products, resolved_run_id, website
                            )
                            store_id = await asyncio.to_thread(repo.upsert_store, staged_store)
                            product_ids = [p.product_id for p in staged_products if p.product_id]
                            existing_states: dict = {}
                            if product_ids:
                                existing_states = await asyncio.to_thread(
                                    repo.get_product_image_states, store_id, product_ids
                                )
                            return store_id, staged_products, existing_states

                    # ── Stage 2: per-chunk image validation + CLIP scoring ─────────
                    def _needs_enrichment(product: ProductRecord, existing_states: dict) -> bool:
                        """True when image validation and/or CLIP scoring must run."""
                        if not product.images:
                            return False
                        existing = existing_states.get(product.product_id)
                        if existing is None:
                            return True
                        existing_imgs, existing_supa, existing_probs = existing
                        # Previous upload was incomplete: re-process.
                        if len(existing_imgs) != len(existing_supa):
                            return True
                        if _norm(product.images) != _norm(existing_imgs):
                            return True
                        if allow_null_gender_probs:
                            return False
                        return not bool(existing_probs or product.gender_probs_csv)

                    # ── Stage 3: storage uploads + DB upserts ─────────────────────
                    async def _prepare_product_for_upsert(
                        product: ProductRecord, store_id: int, existing_states: dict
                    ) -> tuple[bool, ProductRecord | None]:
                        existing = existing_states.get(product.product_id)

                        # New products with no valid images or marked unavailable: skip.
                        if existing is None and (not product.images or product.unavailable):
                            return True, None
                        # Products whose images were all rejected by validation: skip upsert.
                        if not product.images:
                            return True, None

                        existing_imgs = list(existing[0]) if existing else []
                        existing_supa = list(existing[1]) if existing else []
                        images_unchanged = bool(
                            existing
                            and _norm(product.images) == _norm(existing_imgs)
                            and len(existing_supa) == len(product.images)
                        )

                        if not upload_images:
                            # Preserve existing storage URLs when uploads are disabled.
                            product.supabase_images = existing_supa if existing else []
                        elif existing is None:
                            # Ensure every image URL that needs uploading has its bytes
                            # loaded through the shared Fetcher (hits cache for URLs already
                            # processed in stage 2; fetches unconditionally otherwise).
                            image_bytes_by_url = {
                                image_url: cached
                                for image_url in _norm(product.images)
                                if (cached := fetcher.get_cached_bytes(image_url)) is not None
                            }
                            async with upload_sem:
                                try:
                                    product.supabase_images = await asyncio.to_thread(
                                        uploader.upload_product_images_from_cache,
                                        product.images,
                                        store_id,
                                        product.product_id,
                                        image_bytes_by_url,
                                    )
                                except Exception as exc:
                                    logger.warning(
                                        "Upload failed store_id=%s product=%s: %s",
                                        store_id, product.product_id, exc,
                                    )
                                    return False, None
                        elif images_unchanged:
                            # Images and upload count match: reuse existing Supabase URLs.
                            product.supabase_images = existing_supa
                        else:
                            for image_url in _norm(product.images):
                                if fetcher.get_cached_bytes(image_url) is None:
                                    try:
                                        await fetcher.get_bytes(image_url)
                                    except Exception as exc:
                                        logger.warning(
                                            "Pre-upload fetch failed store_id=%s url=%s: %s",
                                            store_id, image_url, exc,
                                        )

                            image_bytes_by_url = {
                                image_url: cached
                                for image_url in _norm(product.images)
                                if (cached := fetcher.get_cached_bytes(image_url)) is not None
                            }
                            async with upload_sem:
                                try:
                                    product.supabase_images = await asyncio.to_thread(
                                        uploader.sync_product_images_from_cache,
                                        current_source_urls=product.images,
                                        existing_source_urls=existing_imgs,
                                        existing_supabase_urls=existing_supa,
                                        store_id=store_id,
                                        product_id=product.product_id,
                                        image_bytes_by_url=image_bytes_by_url,
                                        delete_stale=False,
                                    )
                                except Exception as exc:
                                    logger.warning(
                                        "Image sync failed store_id=%s product=%s: %s",
                                        store_id, product.product_id, exc,
                                    )
                                    product.supabase_images = existing_supa or []

                        # Supabase-image fallback gender scoring.
                        if (
                            not allow_null_gender_probs
                            and not product.gender_probs_csv
                            and product.supabase_images
                        ):
                            orig = list(product.images)
                            product.images = list(product.supabase_images)
                            # Image cap for validation/scoring: keep all but validate only first N.
                            max_images_for_validation = max(
                                1, settings.phase2_max_images_per_product
                            )
                            product.images = product.images[:max_images_for_validation]
                            try:
                                await enrich_gender_probabilities_for_products(
                                    [product], fetcher=fetcher, concurrency=1
                                )
                            except Exception as exc:
                                logger.warning(
                                    "Supabase-fallback scoring failed store_id=%s product=%s: %s",
                                    store_id, product.product_id, exc,
                                )
                            finally:
                                product.images = orig

                        images_incomplete = upload_images and (
                            len(product.supabase_images) != len(product.images)
                        )
                        missing_gender_probs = (not allow_null_gender_probs) and (not product.gender_probs_csv)
                        if images_incomplete or missing_gender_probs:
                            logger.warning(
                                "Skipping upsert: incomplete required fields store_id=%s product=%s",
                                store_id, product.product_id,
                            )
                            return False, None

                        if allow_null_gender_probs:
                            product.gender_probs_csv = None

                        return True, product

                    async def _finalize_store(website: str, store_map: dict[str, tuple]) -> bool:
                        store_id, staged_products, existing_states = store_map[website]

                        results = await asyncio.gather(
                            *[
                                _prepare_product_for_upsert(p, store_id, existing_states)
                                for p in staged_products
                            ],
                            return_exceptions=True,
                        )

                        failure_count = 0
                        to_upsert: list[ProductRecord] = []
                        for result in results:
                            if isinstance(result, Exception):
                                failure_count += 1
                                continue
                            ok, prepared = result
                            if not ok:
                                failure_count += 1
                                continue
                            if prepared is not None:
                                to_upsert.append(prepared)

                        if failure_count:
                            logger.error(
                                "Phase 2: %s/%s products failed for %s — staging preserved for retry",
                                failure_count, len(staged_products), website,
                            )
                            return False

                        if to_upsert:
                            try:
                                await asyncio.to_thread(
                                    repo.upsert_products_batch,
                                    store_id,
                                    to_upsert,
                                )
                            except Exception as exc:
                                logger.error(
                                    "Phase 2: batch upsert failed for %s (%s products): %s",
                                    website,
                                    len(to_upsert),
                                    exc,
                                )
                                return False

                        await asyncio.to_thread(
                            repo.delete_staged_run_website, resolved_run_id, website
                        )
                        await asyncio.to_thread(
                            repo.mark_run_store_status,
                            run_id=resolved_run_id, website=website, status="completed",
                        )

                        nonlocal completed_count
                        async with progress_lock:
                            completed_count += 1
                            pct = (completed_count / len(scraped_websites)) * 100.0
                            print(
                                f"Phase 2 progress: {completed_count}/{len(scraped_websites)} "
                                f"({pct:.1f}%)"
                            )

                        return True

                    p2_success = 0
                    total_websites = len(scraped_websites)
                    for batch_index, batch_start in enumerate(
                        range(0, total_websites, phase2_store_batch_size),
                        start=1,
                    ):
                        website_batch = scraped_websites[batch_start: batch_start + phase2_store_batch_size]
                        logger.info(
                            "Phase 2: processing batch %s (%s stores)",
                            batch_index,
                            len(website_batch),
                        )

                        load_task = asyncio.gather(
                            *[_load_one(w) for w in website_batch],
                            return_exceptions=True,
                        )
                        while True:
                            try:
                                if stall_interval > 0:
                                    load_outcomes = await asyncio.wait_for(
                                        asyncio.shield(load_task),
                                        timeout=float(stall_interval),
                                    )
                                else:
                                    load_outcomes = await load_task
                                break
                            except asyncio.TimeoutError:
                                logger.warning(
                                    "Phase 2 stage 1 still running (batch=%s stores=%s)",
                                    batch_index,
                                    len(website_batch),
                                )

                        store_map: dict[str, tuple] = {}  # website -> (store_id, products, existing_states)
                        failed_load: list[str] = []
                        for website, outcome in zip(website_batch, load_outcomes):
                            if isinstance(outcome, Exception):
                                logger.error("Phase 2 stage 1 failed for %s: %s", website, outcome)
                                failed_load.append(website)
                                continue
                            store_id, staged_products, existing_states = outcome
                            # Apply one-hot gender overrides and inherit existing scores before Stage 2.
                            for product in staged_products:
                                if allow_null_gender_probs:
                                    product.gender_probs_csv = None
                                else:
                                    explicit = one_hot_gender_probs_csv(product.gender_label)
                                    if explicit is not None:
                                        product.gender_probs_csv = explicit
                                    elif not product.gender_probs_csv:
                                        state = existing_states.get(product.product_id)
                                        if state is not None and state[2]:
                                            product.gender_probs_csv = state[2]
                            store_map[website] = (store_id, staged_products, existing_states)

                        to_enrich: list[ProductRecord] = [
                            p
                            for _w, (_, prods, states) in store_map.items()
                            for p in prods
                            if _needs_enrichment(p, states)
                        ]

                        # Capture all candidate image URLs before stage 2 filters
                        # out rejected images so we can clear every fetched URL from
                        # the byte cache at the end of this batch, not just the
                        # survivors.  Avoids accumulation of rejected bytes.
                        batch_prefetch_urls: set[str] = {
                            image_url.strip()
                            for p in to_enrich
                            for image_url in p.images
                            if image_url and image_url.strip()
                        }

                        if to_enrich:
                            product_chunks = _chunk_products_for_phase2(
                                to_enrich,
                                max_products=phase2_product_chunk_size,
                                max_unique_image_urls=phase2_unique_url_budget,
                                max_images_per_product_for_budget=settings.phase2_max_images_per_product,
                            )
                            total_product_chunks = len(product_chunks)
                            logger.info(
                                "Phase 2 stage 2: validating images for %s products across %s stores in %s chunks (max_products=%s max_unique_urls=%s max_images_per_product=%s)",
                                len(to_enrich),
                                len(store_map),
                                total_product_chunks,
                                phase2_product_chunk_size,
                                phase2_unique_url_budget,
                                settings.phase2_max_images_per_product,
                            )
                            for chunk_index, product_chunk in enumerate(product_chunks, start=1):
                                chunk_pct = (chunk_index / total_product_chunks) * 100.0
                                logger.info(
                                    "Phase 2 stage 2: validating chunk %s/%s (%s products)",
                                    chunk_index,
                                    total_product_chunks,
                                    len(product_chunk),
                                )
                                print(
                                    f"Phase 2 validation chunk: {chunk_index}/{total_product_chunks} "
                                    f"({chunk_pct:.1f}%) products={len(product_chunk)}"
                                )

                                # ── Image cap for validation/scoring: keep all but validate only first N ──
                                max_images_for_validation = max(
                                    1, settings.phase2_max_images_per_product
                                )
                                original_images_map: dict[str, list[str]] = {}
                                for product in product_chunk:
                                    if product.images:
                                        original_images_map[product.product_id] = list(product.images)
                                        product.images = product.images[:max_images_for_validation]

                                if stall_interval > 0:
                                    if settings.phase2_first_image_product_validation_only:
                                        vtask = asyncio.create_task(
                                            verify_first_image_product_validation(
                                                products=product_chunk,
                                                fetcher=fetcher,
                                                settings=settings,
                                            )
                                        )
                                    else:
                                        vtask = asyncio.create_task(
                                            verify_product_images(
                                                products=product_chunk,
                                                fetcher=fetcher,
                                                settings=settings,
                                            )
                                        )
                                    while True:
                                        try:
                                            await asyncio.wait_for(
                                                asyncio.shield(vtask),
                                                timeout=float(stall_interval),
                                            )
                                            break
                                        except asyncio.TimeoutError:
                                            logger.warning(
                                                "Phase 2 image validation still running "
                                                "(chunk=%s/%s products=%s)",
                                                chunk_index,
                                                total_product_chunks,
                                                len(product_chunk),
                                            )
                                else:
                                    if settings.phase2_first_image_product_validation_only:
                                        await verify_first_image_product_validation(
                                            products=product_chunk,
                                            fetcher=fetcher,
                                            settings=settings,
                                        )
                                    else:
                                        await verify_product_images(
                                            products=product_chunk,
                                            fetcher=fetcher,
                                            settings=settings,
                                        )

                                to_score = [p for p in product_chunk if p.images and not p.gender_probs_csv]
                                if to_score and not allow_null_gender_probs:
                                    logger.info(
                                        "Phase 2 stage 2: CLIP scoring chunk %s/%s (%s products)",
                                        chunk_index,
                                        total_product_chunks,
                                        len(to_score),
                                    )
                                    print(
                                        f"Phase 2 scoring chunk: {chunk_index}/{total_product_chunks} "
                                        f"products={len(to_score)}"
                                    )
                                    if stall_interval > 0:
                                        stask = asyncio.create_task(
                                            enrich_gender_probabilities_for_products(
                                                products=to_score,
                                                fetcher=fetcher,
                                                concurrency=settings.image_validation_concurrency,
                                            )
                                        )
                                        while True:
                                            try:
                                                await asyncio.wait_for(
                                                    asyncio.shield(stask),
                                                    timeout=float(stall_interval),
                                                )
                                                break
                                            except asyncio.TimeoutError:
                                                still_pending = sum(
                                                    1 for p in to_score if not p.gender_probs_csv
                                                )
                                                logger.warning(
                                                    "Phase 2 CLIP scoring still running "
                                                    "(chunk=%s/%s): %s/%s products pending",
                                                    chunk_index,
                                                    total_product_chunks,
                                                    still_pending,
                                                    len(to_score),
                                                )
                                    else:
                                        await enrich_gender_probabilities_for_products(
                                            products=to_score,
                                            fetcher=fetcher,
                                            concurrency=settings.image_validation_concurrency,
                                        )
                                elif to_score:
                                    for product in to_score:
                                        product.gender_probs_csv = None

                                # ── Restore all original images after validation/scoring ──
                                for product in product_chunk:
                                    if product.product_id in original_images_map:
                                        product.images = original_images_map[product.product_id]

                        finalize_task = asyncio.gather(
                            *[_finalize_store(w, store_map) for w in store_map],
                            return_exceptions=True,
                        )
                        while True:
                            try:
                                if stall_interval > 0:
                                    finalize_outcomes = await asyncio.wait_for(
                                        asyncio.shield(finalize_task),
                                        timeout=float(stall_interval),
                                    )
                                else:
                                    finalize_outcomes = await finalize_task
                                break
                            except asyncio.TimeoutError:
                                logger.warning(
                                    "Phase 2 stage 3 still running (batch=%s stores=%s)",
                                    batch_index,
                                    len(store_map),
                                )

                        for website, outcome in zip(store_map, finalize_outcomes):
                            if isinstance(outcome, Exception) or outcome is False:
                                try:
                                    repo.mark_run_store_status(
                                        run_id=resolved_run_id,
                                        website=website,
                                        status="failed",
                                        error_message=(
                                            str(outcome)[:2000]
                                            if isinstance(outcome, Exception)
                                            else "phase2_finalize_failed"
                                        ),
                                    )
                                except Exception:
                                    pass
                            else:
                                p2_success += 1

                        for website in failed_load:
                            try:
                                repo.mark_run_store_status(
                                    run_id=resolved_run_id,
                                    website=website,
                                    status="failed",
                                    error_message="staging_load_failed",
                                )
                            except Exception:
                                pass

                        # Also include any URLs fetched during stage 3 pre-upload
                        # (for products whose images didn't go through stage 2).
                        batch_prefetch_urls.update(
                            image_url.strip()
                            for _, products, _ in store_map.values()
                            for product in products
                            for image_url in product.images
                            if image_url and image_url.strip()
                        )
                        if batch_prefetch_urls:
                            fetcher.clear_cached_bytes(list(batch_prefetch_urls), clear_disk_cache=False)

                        # Release product-heavy structures before loading the next chunk.
                        store_map.clear()
                        load_outcomes.clear()
                        to_enrich.clear()
                        failed_load.clear()
                        gc.collect()

                    return p2_success
                finally:
                    await fetcher.close()

            list_staged = getattr(repo, "list_all_staged_run_websites", None)
            if callable(list_staged):
                scraped_websites = list_staged(run_id=resolved_run_id)
            else:
                # Backward-compatible fallback for older repository implementations.
                scraped_websites = repo.list_all_run_store_websites(
                    run_id=resolved_run_id, statuses=["scraped"]
                )

            if not scraped_websites:
                logger.info("Phase 2: no staged websites found for run_id=%s", resolved_run_id)
                pending_count = 0
                failed_count = 0
                scraped_count = 0
                completed_count = 0
                count_status = getattr(repo, "count_run_store_status", None)
                if callable(count_status):
                    pending_count = count_status(run_id=resolved_run_id, status="pending")
                    failed_count = count_status(run_id=resolved_run_id, status="failed")
                    scraped_count = count_status(run_id=resolved_run_id, status="scraped")
                    completed_count = count_status(run_id=resolved_run_id, status="completed")
                print(
                    "Phase 2: no staged websites to process "
                    f"(run_id={resolved_run_id}, crawl_store_runs pending={pending_count}, "
                    f"scraped={scraped_count}, failed={failed_count}, completed={completed_count})."
                )
                if pending_count > 0 and scraped_count == 0 and failed_count == 0 and completed_count == 0:
                    print(
                        "Phase 2 warning: this run looks like a fresh or phase-1-not-started run, not a resumable staged run. "
                        "Use --run-id with the actual staged run ID or run scripts/diagnose_staged_runs.py."
                    )
                return 0

            logger.info("Phase 2: enriching %s staged websites", len(scraped_websites))
            print(
                f"Phase 2: enriching {len(scraped_websites)} staged websites "
                f"(run_id={resolved_run_id})..."
            )
            p2_success = asyncio.run(_run_async())
            print(
                f"Phase 2 complete: {p2_success}/{len(scraped_websites)} stores enriched successfully"
            )
            return p2_success

        async def _persist_batch_stream(batch: list[StoreSeed]) -> tuple[int, int]:
            processed_in_batch = 0
            success_in_batch = 0
            stall_interval = int(getattr(settings, "crawl_stall_log_interval_sec", 60) or 0)

            async def _iter_page_outcomes(seed: StoreSeed, fetcher: Fetcher):
                base = seed.store_url.rstrip("/")
                homepage = await fetcher.get_text(base)
                store = classify_store(homepage, base, settings)

                page_limit = max(1, settings.shopify_products_page_limit)
                max_pages = max(1, settings.shopify_products_max_pages)
                max_items_per_store = max(0, settings.shopify_products_max_items_per_store)

                seen_product_ids: set[str] = set()
                yielded_any = False

                for page in range(1, max_pages + 1):
                    products_url = f"{base}/products.json?limit={page_limit}&page={page}"
                    payload = await fetcher.get_json(products_url)
                    extracted = extract_products_from_products_json(payload, settings, base_url=base)

                    page_products = []
                    for product in extracted:
                        if product.product_id in seen_product_ids:
                            continue
                        seen_product_ids.add(product.product_id)
                        if product.images:
                            normalized = normalize_product(product)
                            if normalized is not None:
                                page_products.append(normalized)

                        if max_items_per_store > 0 and len(seen_product_ids) >= max_items_per_store:
                            logger.warning(
                                "Reached per-store product cap for %s: collected=%s cap=%s",
                                base,
                                len(seen_product_ids),
                                max_items_per_store,
                            )
                            break

                    if page_products:
                        yielded_any = True
                        yield ScrapeResult(store=store, products=page_products)

                    if max_items_per_store > 0 and len(seen_product_ids) >= max_items_per_store:
                        break

                    products_raw = payload.get("products", []) if isinstance(payload, dict) else []
                    if not isinstance(products_raw, list) or not products_raw:
                        break

                if not yielded_any:
                    # Persist store row even when no products are present.
                    yield ScrapeResult(store=store, products=[])

            use_streaming_mode = (
                settings.store_page_streaming_enabled
                and getattr(scrape_many_stream, "__module__", "")
                == "aisley_scraper.crawl.orchestrator"
            )

            if use_streaming_mode:
                fetcher = Fetcher(settings)
                try:
                    for seed in batch:
                        processed_in_batch += 1
                        persisted_ok = True
                        error_message = "store_persist_failed"

                        try:
                            async for outcome in _iter_page_outcomes(seed, fetcher):
                                persist_task = asyncio.create_task(
                                    asyncio.to_thread(_persist_store_result, seed, outcome)
                                )
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

                                if not persisted_ok:
                                    error_message = "store_persist_failed"
                                    break
                                # Reduce retained objects between streamed pages on large stores.
                                gc.collect()
                        except Exception as exc:
                            persisted_ok = False
                            error_message = str(exc)

                        mark_status = getattr(repo, "mark_run_store_status", None)
                        if callable(mark_status):
                            if persisted_ok:
                                mark_status(
                                    run_id=resolved_run_id,
                                    website=seed.store_url,
                                    status="completed",
                                )
                            else:
                                mark_status(
                                    run_id=resolved_run_id,
                                    website=seed.store_url,
                                    status="failed",
                                    error_message=error_message,
                                )

                        if persisted_ok:
                            success_in_batch += 1

                    return processed_in_batch, success_in_batch
                finally:
                    await fetcher.close()

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

        if phase == "1":
            if disk_cache_dir is not None:
                print(
                    "Phase 1 startup: fetcher disk cache preserved "
                    f"files={cleared_disk_cache_files} dir={disk_cache_dir}"
                )
            success_count = _run_phase1()
            print(f"Phase 1 complete: {success_count}/{len(seeds)} stores staged successfully")
        elif phase == "2":
            if disk_cache_dir is not None:
                print(
                    "Phase 2 startup: fetcher disk cache preserved "
                    f"files={cleared_disk_cache_files} dir={disk_cache_dir}"
                )
            _run_phase2()
        else:
            # --phase both: existing single-phase pipeline unchanged.
            for start in range(0, len(seeds), chunk_size):
                batch = seeds[start : start + chunk_size]
                processed_in_batch, success_in_batch = asyncio.run(
                    _persist_batch_stream(batch)
                )
                processed_count += processed_in_batch
                success_count += success_in_batch

                print(f"Progress: persisted {processed_count}/{len(seeds)} stores")

            print(f"Crawled {success_count}/{len(seeds)} stores successfully")

        count_status = getattr(repo, "count_run_store_status", None)
        if callable(count_status):
            if phase == "1":
                # Keep the run state file; Phase 2 needs it to find staged data.
                scraped = count_status(run_id=resolved_run_id, status="scraped")
                logger.info(
                    "Phase 1 complete: scraped=%s stores staged for Phase 2 (run_id=%s)",
                    scraped,
                    resolved_run_id,
                )
            else:
                pending = count_status(run_id=resolved_run_id, status="pending")
                scraped = count_status(run_id=resolved_run_id, status="scraped")
                failed = count_status(run_id=resolved_run_id, status="failed")
                if pending == 0 and scraped == 0 and failed == 0:
                    state_file = Path(settings.crawl_run_state_path)
                    if state_file.exists():
                        state_file.unlink()

        return 0
    finally:
        pass


def run_cleanup_runs(run_id: str | None = None) -> int:
    settings = get_settings()
    _setup_logging(settings.log_level)
    repo = SupabaseRestRepository(settings)

    keep_run_id = run_id
    if not keep_run_id:
        state_file = Path(settings.crawl_run_state_path)
        if state_file.exists():
            keep_run_id = state_file.read_text(encoding="utf-8").strip() or None

    if not keep_run_id:
        print("No active run ID found. Pass --run-id or ensure .aisley_active_run_id exists.")
        return 1

    print(f"Cleaning all temporary tables excluding run_id={keep_run_id} ...")
    purge_other_runs = getattr(repo, "purge_other_runs", None)
    if callable(purge_other_runs):
        purge_other_runs(keep_run_id)
    print("Cleanup complete.")
    return 0


def run_filter_shopify_products_first_image_validation(
    *,
    limit: int | None = None,
    batch_size: int = 200,
    dry_run: bool = False,
) -> int:
    settings = get_settings()
    _setup_logging(settings.log_level)
    repo = SupabaseRestRepository(settings)

    effective_batch_size = max(1, int(batch_size))
    requested_limit = max(0, int(limit)) if limit is not None else None
    concurrency = max(1, int(settings.image_validation_concurrency))
    threshold = float(settings.phase2_first_image_product_prob_threshold)

    print(
        "Filtering shopify_products by first-image product validation "
        f"(threshold={threshold:.2f}, dry_run={dry_run})..."
    )

    async def _run_async() -> int:
        fetcher = Fetcher(settings)
        try:
            processed = 0
            deleted = 0
            last_id: int | None = None
            failure_reasons: Counter[str] = Counter()

            while True:
                if requested_limit is not None and processed >= requested_limit:
                    break

                fetch_limit = effective_batch_size
                if requested_limit is not None:
                    fetch_limit = min(fetch_limit, requested_limit - processed)
                if fetch_limit <= 0:
                    break

                list_for_filter_scan = getattr(repo, "list_products_for_first_image_validation_scan", None)
                if callable(list_for_filter_scan):
                    rows = list_for_filter_scan(limit=fetch_limit, after_id=last_id)
                else:
                    rows = repo.list_products_for_integrity_scan(limit=fetch_limit, offset=processed)

                if not rows:
                    break

                async def _evaluate_row(
                    row: dict[str, object],
                    sem: asyncio.Semaphore,
                ) -> tuple[str, int | None, int, str, str | None, float | None, str | None]:
                    row_id_raw = row.get("id")
                    row_id = int(row_id_raw) if isinstance(row_id_raw, int) else None
                    store_id_raw = row.get("store_id")
                    product_id_raw = row.get("product_id")
                    item_uuid_raw = row.get("item_uuid")
                    item_uuid = item_uuid_raw if isinstance(item_uuid_raw, str) and item_uuid_raw else None
                    image_urls_raw = row.get("images")

                    if not isinstance(store_id_raw, int) or not isinstance(product_id_raw, str):
                        return ("keep", row_id, 0, "", item_uuid, None, "invalid_row")

                    first_image = ""
                    if isinstance(image_urls_raw, list):
                        for value in image_urls_raw:
                            if isinstance(value, str) and value.strip():
                                first_image = value.strip()
                                break
                    if not first_image:
                        return (
                            "keep",
                            row_id,
                            store_id_raw,
                            product_id_raw,
                            item_uuid,
                            None,
                            "missing_image",
                        )

                    keep, reason, product_prob = await evaluate_first_image_product_validation(
                        image_urls=[first_image],
                        fetcher=fetcher,
                        settings=settings,
                        semaphore=sem,
                    )
                    return (
                        "keep" if keep else "delete",
                        row_id,
                        store_id_raw,
                        product_id_raw,
                        item_uuid,
                        product_prob,
                        reason,
                    )

                sem = asyncio.Semaphore(concurrency)
                evaluations = await asyncio.gather(*(_evaluate_row(row, sem) for row in rows))

                deleted_item_embedding_uuids: set[str] = set()
                for action, row_id, store_id, product_id, item_uuid, product_prob, reason in evaluations:
                    processed += 1
                    if row_id is not None and (last_id is None or row_id > last_id):
                        last_id = row_id
                    if reason:
                        failure_reasons[reason] += 1
                    if action != "delete":
                        continue
                    if dry_run:
                        deleted += 1
                        continue
                    repo.delete_product(store_id, product_id)
                    if item_uuid and item_uuid not in deleted_item_embedding_uuids:
                        delete_embedding = getattr(repo, "delete_item_embeddings_for_item_uuid", None)
                        if callable(delete_embedding):
                            delete_embedding(item_uuid)
                        deleted_item_embedding_uuids.add(item_uuid)
                    deleted += 1
                    logger.info(
                        "Deleted low-score product row store_id=%s product_id=%s score=%s threshold=%.2f",
                        store_id,
                        product_id,
                        product_prob,
                        threshold,
                    )

                print(
                    "Filter progress: "
                    f"processed={processed} deleted={deleted} dry_run={dry_run}"
                )

            summary = (
                "Filter complete: "
                f"processed={processed} "
                f"{'would_delete' if dry_run else 'deleted'}={deleted} "
                f"threshold={threshold:.2f} "
                f"failure_reasons={dict(failure_reasons)}"
            )
            print(summary)
            return 0
        finally:
            await fetcher.close()

    return asyncio.run(_run_async())


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "ingest-stores":
        return run_ingest(args.csv)
    if args.command == "diagnose-staged-runs":
        return run_diagnose_staged_runs()
    if args.command == "cleanup-runs":
        return run_cleanup_runs(getattr(args, "run_id", None))
    if args.command == "filter-shopify-products":
        return run_filter_shopify_products_first_image_validation(
            limit=getattr(args, "limit", None),
            batch_size=getattr(args, "batch_size", 200),
            dry_run=getattr(args, "dry_run", False),
        )
    if args.command == "crawl-stores":
        return run_crawl(
            args.limit,
            run_id=args.run_id,
            fresh=args.fresh,
            phase=args.phase,
            skip_image_upload=args.skip_image_upload,
        )

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
