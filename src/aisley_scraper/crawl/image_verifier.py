from __future__ import annotations

import asyncio
from collections import Counter, deque
import logging
import time
from pathlib import PurePosixPath
from urllib.parse import urlparse

import httpx

from aisley_scraper.config import Settings
from aisley_scraper.crawl.fetcher import Fetcher
from aisley_scraper.image_validation import (
    ImageValidationFailure,
    validate_and_normalize_upload,
    validate_product_photo_only,
)
from aisley_scraper.models import ProductRecord

logger = logging.getLogger(__name__)

_NETWORK_TRANSIENT_REASONS = {"timeout", "chunk_timeout", "fetch_error", "task_error", "unexpected_error"}


class ImageVerificationFailure(Exception):
    def __init__(self, reason: str, detail: str | None = None) -> None:
        super().__init__(detail or reason)
        self.reason = reason
        self.detail = detail or reason


def _filename_from_url(image_url: str) -> str:
    path = urlparse(image_url).path
    name = PurePosixPath(path).name
    if name:
        return name
    return "image.jpg"


async def evaluate_first_image_product_validation(
    *,
    image_urls: list[str],
    fetcher: Fetcher,
    settings: Settings,
    semaphore: asyncio.Semaphore | None = None,
) -> tuple[bool, str | None, float | None]:
    """Return (keep_product, reason, product_prob) using the Phase-2 first-image rules."""
    first_image_url = next(
        (
            image_url.strip()
            for image_url in image_urls
            if image_url and image_url.strip()
        ),
        "",
    )
    if not first_image_url:
        return False, "missing_image", None

    threshold = float(settings.phase2_first_image_product_prob_threshold)
    per_image_timeout_sec = float(settings.image_validation_attempt_timeout_sec)

    try:
        if semaphore is None:
            content = await asyncio.wait_for(
                fetcher.get_bytes(first_image_url),
                timeout=per_image_timeout_sec,
            )
        else:
            async with semaphore:
                content = await asyncio.wait_for(
                    fetcher.get_bytes(first_image_url),
                    timeout=per_image_timeout_sec,
                )
        result = await asyncio.wait_for(
            asyncio.to_thread(
                validate_product_photo_only,
                content=content,
                filename=_filename_from_url(first_image_url),
                min_product_prob=threshold,
            ),
            timeout=per_image_timeout_sec,
        )
        product_payload = result.get("product") if isinstance(result, dict) else None
        product_prob = None
        if isinstance(product_payload, dict):
            prob = product_payload.get("product_prob")
            if isinstance(prob, (int, float)):
                product_prob = float(prob)
        return True, None, product_prob
    except asyncio.TimeoutError:
        return True, "timeout", None
    except ImageValidationFailure as exc:
        if exc.code in {"not_a_product_photo", "unsupported_file_type", "invalid_image"}:
            product_prob = None
            details = exc.details or {}
            if isinstance(details, dict):
                prob = details.get("product_prob")
                if isinstance(prob, (int, float)):
                    product_prob = float(prob)
            return False, exc.code, product_prob
        return True, exc.code, None
    except (httpx.HTTPError, TimeoutError):
        return True, "fetch_error", None
    except Exception:
        return True, "task_error", None


async def _verify_single_image_url(
    image_url: str,
    fetcher: Fetcher,
    max_retries: int,
    min_width: int,
    min_height: int,
) -> bool:
    attempts = max(1, max_retries + 1)
    for attempt in range(1, attempts + 1):
        try:
            content = await fetcher.get_bytes(image_url)
            validate_and_normalize_upload(
                content=content,
                filename=_filename_from_url(image_url),
                min_width=min_width,
                min_height=min_height,
            )
            return True
        except ImageValidationFailure as exc:
            if attempt >= attempts:
                raise ImageVerificationFailure(
                    reason=exc.code,
                    detail=exc.message,
                ) from exc
            await asyncio.sleep(0.25 * attempt)
        except (httpx.HTTPError, TimeoutError) as exc:
            if attempt >= attempts:
                raise ImageVerificationFailure(
                    reason="fetch_error",
                    detail=str(exc),
                ) from exc
            await asyncio.sleep(0.25 * attempt)
        except Exception as exc:
            raise ImageVerificationFailure(
                reason="unexpected_error",
                detail=str(exc),
            ) from exc
    return False


async def verify_product_images(
    *,
    products: list[ProductRecord],
    fetcher: Fetcher,
    settings: Settings,
) -> None:
    if not settings.image_validation_enabled:
        return

    seen_urls: set[str] = set()
    unique_urls: list[str] = []
    for product in products:
        for image_url in product.images:
            normalized_url = image_url.strip() if image_url else ""
            if not normalized_url or normalized_url in seen_urls:
                continue
            seen_urls.add(normalized_url)
            unique_urls.append(normalized_url)
    if not unique_urls:
        return

    total_products = len(products)
    domain_counts: dict[str, int] = {}
    for image_url in unique_urls:
        domain = urlparse(image_url).netloc or "<unknown>"
        domain_counts[domain] = domain_counts.get(domain, 0) + 1

    verdicts: dict[str, bool] = {}
    terminal_reasons: dict[str, str] = {}
    failure_reasons: Counter[str] = Counter()
    max_queue_retries = max(0, int(settings.image_validation_queue_max_retries))
    logger.info(
        "Image validation start: products=%s unique_urls=%s domains=%s concurrency=%s retries=%s",
        total_products,
        len(unique_urls),
        len(domain_counts),
        settings.image_validation_concurrency,
        max_queue_retries,
    )
    # Keep each URL attempt short; failed URLs are retried in subsequent passes.
    per_image_timeout_sec = float(settings.image_validation_attempt_timeout_sec)
    # Bound total verification runtime for this call so phase-2 chunk processing keeps moving.
    total_validation_timeout_sec = float(settings.image_validation_chunk_timeout_sec)
    slow_url_threshold_sec = max(2.0, per_image_timeout_sec * 0.75)
    started_at = time.monotonic()
    deadline = started_at + total_validation_timeout_sec

    # Retry loop: process all URLs, then retry failures on next iteration.
    urls_to_process = list(unique_urls)
    for retry_pass in range(max_queue_retries + 1):
        if not urls_to_process:
            break

        remaining_budget_sec = deadline - time.monotonic()
        if remaining_budget_sec <= 0:
            pending_urls = [url for url in urls_to_process if url not in verdicts]
            for image_url in pending_urls:
                failure_reasons["chunk_timeout"] += 1
                verdicts[image_url] = False
                terminal_reasons[image_url] = "chunk_timeout"
            logger.warning(
                "Image validation global timeout exhausted before pass %d: completed=%s/%s pending=%s",
                retry_pass,
                len(verdicts),
                len(unique_urls),
                len(pending_urls),
            )
            break
        
        queue: deque[str] = deque(urls_to_process)
        in_flight: set[str] = set()
        queue_lock = asyncio.Lock()
        failed_urls_this_pass: list[str] = []

        async def _worker() -> None:
            while True:
                async with queue_lock:
                    if not queue:
                        return
                    image_url = queue.popleft()
                    in_flight.add(image_url)

                item_started_at = time.monotonic()
                try:
                    verified = await asyncio.wait_for(
                        _verify_single_image_url(
                            image_url=image_url,
                            fetcher=fetcher,
                            max_retries=0,
                            min_width=settings.image_min_width,
                            min_height=settings.image_min_height,
                        ),
                        timeout=per_image_timeout_sec,
                    )
                    verdicts[image_url] = bool(verified)
                    if verified:
                        terminal_reasons.pop(image_url, None)
                    if not verified:
                        failure_reasons["verify_false"] += 1
                except ImageVerificationFailure as exc:
                    failure_reasons[exc.reason] += 1
                    if retry_pass < max_queue_retries:
                        # Queue for retry in next pass.
                        async with queue_lock:
                            failed_urls_this_pass.append(image_url)
                    else:
                        # Out of retries.
                        logger.info(
                            "Image validation rejected url=%s code=%s detail=%s",
                            image_url,
                            exc.reason,
                            exc.detail,
                        )
                        verdicts[image_url] = False
                        terminal_reasons[image_url] = exc.reason
                except asyncio.TimeoutError:
                    failure_reasons["timeout"] += 1
                    if retry_pass < max_queue_retries:
                        # Queue for retry in next pass.
                        async with queue_lock:
                            failed_urls_this_pass.append(image_url)
                    else:
                        logger.warning(
                            "Image validation timed out for %s after %.1fs",
                            image_url,
                            per_image_timeout_sec,
                        )
                        verdicts[image_url] = False
                        terminal_reasons[image_url] = "timeout"
                except Exception as exc:
                    failure_reasons["task_error"] += 1
                    if retry_pass < max_queue_retries:
                        # Queue for retry in next pass.
                        async with queue_lock:
                            failed_urls_this_pass.append(image_url)
                    else:
                        logger.warning("Image validation task failed for %s: %s", image_url, exc)
                        verdicts[image_url] = False
                        terminal_reasons[image_url] = "task_error"
                finally:
                    elapsed = time.monotonic() - item_started_at
                    if elapsed >= slow_url_threshold_sec:
                        logger.warning(
                            "Slow image validation URL: elapsed=%.2fs url=%s result=%s retry_pass=%s",
                            elapsed,
                            image_url,
                            verdicts.get(image_url),
                            retry_pass,
                        )
                    async with queue_lock:
                        in_flight.discard(image_url)

        workers = [
            asyncio.create_task(_worker())
            for _ in range(max(1, settings.image_validation_concurrency))
        ]

        try:
            await asyncio.wait_for(
                asyncio.gather(*workers),
                timeout=remaining_budget_sec,
            )
        except asyncio.TimeoutError:
            async with queue_lock:
                pending_urls = list(in_flight) + list(queue)
            if not pending_urls:
                # Safety net: if timeout fires during a race while workers are winding down,
                # classify any URLs lacking verdicts as pending/timeout instead of silently dropping them.
                pending_urls = [url for url in urls_to_process if url not in verdicts]
            # Keep timeout accounting deterministic.
            pending_urls = list(dict.fromkeys(pending_urls))
            pending_domain_counts: dict[str, int] = {}
            for image_url in pending_urls:
                domain = urlparse(image_url).netloc or "<unknown>"
                pending_domain_counts[domain] = pending_domain_counts.get(domain, 0) + 1
            pending_domain_summary = sorted(
                pending_domain_counts.items(),
                key=lambda kv: kv[1],
                reverse=True,
            )[:5]
            logger.warning(
                "Image validation pass %d timeout after %.1fs: completed=%s/%s pending=%s top_pending_domains=%s sample_pending_urls=%s",
                retry_pass,
                remaining_budget_sec,
                len(verdicts),
                len(urls_to_process),
                len(pending_urls),
                pending_domain_summary,
                pending_urls[:5],
            )
            for task in workers:
                task.cancel()
            try:
                await asyncio.wait_for(
                    asyncio.gather(*workers, return_exceptions=True),
                    timeout=2.0,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Image validation worker cancellation exceeded 2s; continuing with timeout accounting"
                )
            for image_url in pending_urls:
                failure_reasons["chunk_timeout"] += 1
                verdicts[image_url] = False
                terminal_reasons[image_url] = "chunk_timeout"
            # Don't retry after timeout.
            urls_to_process = []
        else:
            # Prepare failed URLs for next pass if retries remain.
            urls_to_process = failed_urls_this_pass[:] if retry_pass < max_queue_retries else []

    rejected_count = sum(1 for ok in verdicts.values() if not ok)
    elapsed_total = time.monotonic() - started_at
    if rejected_count:
        logger.info(
            "Image validation complete: rejected=%s/%s elapsed=%.2fs failure_reasons=%s",
            rejected_count,
            len(verdicts),
            elapsed_total,
            dict(failure_reasons),
        )
    else:
        logger.info(
            "Image validation complete: rejected=0/%s elapsed=%.2fs",
            len(verdicts),
            elapsed_total,
        )

    network_preserved_products = 0
    for product in products:
        normalized_urls = [url.strip() for url in product.images if url and url.strip()]
        validated_urls = [url for url in normalized_urls if verdicts.get(url, False)]
        if validated_urls:
            product.images = validated_urls
            continue

        if normalized_urls:
            reasons = [terminal_reasons.get(url) for url in normalized_urls if url in verdicts]
            known_reasons = {reason for reason in reasons if reason}
            if known_reasons and known_reasons.issubset(_NETWORK_TRANSIENT_REASONS):
                # Keep network-failed images so transient CDN behavior doesn't zero out otherwise valid products.
                product.images = normalized_urls
                network_preserved_products += 1
                continue

        product.images = validated_urls

    if network_preserved_products:
        logger.warning(
            "Image validation preserved %s products due to network-only image failures",
            network_preserved_products,
        )

    # Keep only products that still have at least one validated image.
    products[:] = [product for product in products if product.images]


async def verify_first_image_product_validation(
    *,
    products: list[ProductRecord],
    fetcher: Fetcher,
    settings: Settings,
) -> None:
    """Phase-2 lightweight mode: validate only first image as a product photo."""
    if not settings.image_validation_enabled:
        return

    threshold = float(settings.phase2_first_image_product_prob_threshold)
    per_image_timeout_sec = float(settings.image_validation_attempt_timeout_sec)
    concurrency = max(1, int(settings.image_validation_concurrency))
    semaphore = asyncio.Semaphore(concurrency)
    failure_reasons: Counter[str] = Counter()
    network_preserved_products = 0

    async def _validate_product(product: ProductRecord) -> tuple[ProductRecord, bool, str | None]:
        keep, reason, _product_prob = await evaluate_first_image_product_validation(
            image_urls=product.images,
            fetcher=fetcher,
            settings=settings,
            semaphore=semaphore,
        )
        return product, keep, reason

    results = await asyncio.gather(*(_validate_product(product) for product in products))

    kept_products: list[ProductRecord] = []
    for product, keep, reason in results:
        if keep:
            kept_products.append(product)
            if reason is not None:
                failure_reasons[reason] += 1
                network_preserved_products += 1
            continue
        if reason is not None:
            failure_reasons[reason] += 1

    dropped = len(products) - len(kept_products)
    if dropped:
        logger.info(
            "First-image product validation complete: dropped=%s/%s threshold=%.2f failure_reasons=%s",
            dropped,
            len(products),
            threshold,
            dict(failure_reasons),
        )
    else:
        logger.info(
            "First-image product validation complete: dropped=0/%s threshold=%.2f",
            len(products),
            threshold,
        )

    if network_preserved_products:
        logger.warning(
            "First-image product validation preserved %s products due to network/transient failures",
            network_preserved_products,
        )

    products[:] = kept_products
