"""Validate first-image classifier on a larger sample of scraped Shopify product images.

This script pulls product images from store /products.json feeds, then scores the
first image of each sampled product with product_probability_clip.
"""

from __future__ import annotations

import argparse
import csv
import random
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from statistics import mean, median
from typing import Any
from urllib.parse import urlparse

import httpx
from PIL import Image

from aisley_scraper.image_validation import product_probability_clip


@dataclass
class ProductSample:
    store_url: str
    product_id: str
    title: str
    image_url: str


@dataclass
class ScoreResult:
    sample: ProductSample
    product_prob: float
    best_prompt: str
    best_positive_prompt: str
    best_negative_prompt: str


def read_store_urls(csv_path: Path, max_stores: int) -> list[str]:
    urls: list[str] = []
    for line in csv_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        # This CSV currently stores one URL per line with no header.
        url = line.split(",", 1)[0].strip()
        if not url:
            continue
        urls.append(url)
        if len(urls) >= max_stores:
            break
    return urls


def normalize_base_url(url: str) -> str:
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc or parsed.path
    return f"{scheme}://{netloc}".rstrip("/")


def fetch_product_samples(
    *,
    client: httpx.Client,
    store_urls: list[str],
    max_samples: int,
    per_store_max_products: int,
) -> tuple[list[ProductSample], list[str]]:
    samples: list[ProductSample] = []
    errors: list[str] = []
    seen_images: set[str] = set()

    for store_url in store_urls:
        if len(samples) >= max_samples:
            break

        base_url = normalize_base_url(store_url)
        feed_url = f"{base_url}/products.json?limit=250"

        try:
            resp = client.get(feed_url, timeout=20.0, follow_redirects=True)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            errors.append(f"{base_url}: {exc}")
            continue

        products = payload.get("products", [])
        if not isinstance(products, list):
            errors.append(f"{base_url}: invalid products payload")
            continue

        picked = 0
        for prod in products:
            if len(samples) >= max_samples:
                break
            if picked >= per_store_max_products:
                break
            if not isinstance(prod, dict):
                continue

            images = prod.get("images", [])
            if not isinstance(images, list) or not images:
                continue

            first_image = images[0] if isinstance(images[0], dict) else None
            image_url = str((first_image or {}).get("src") or "").strip()
            if not image_url or image_url in seen_images:
                continue

            product_id = str(prod.get("id") or "")
            title = str(prod.get("title") or "").strip() or "(untitled)"
            if not product_id:
                continue

            samples.append(
                ProductSample(
                    store_url=base_url,
                    product_id=product_id,
                    title=title,
                    image_url=image_url,
                )
            )
            seen_images.add(image_url)
            picked += 1

    return samples, errors


def score_samples(client: httpx.Client, samples: list[ProductSample]) -> tuple[list[ScoreResult], list[str]]:
    results: list[ScoreResult] = []
    errors: list[str] = []

    for idx, sample in enumerate(samples, start=1):
        try:
            resp = client.get(sample.image_url, timeout=20.0, follow_redirects=True)
            resp.raise_for_status()
            img = Image.open(BytesIO(resp.content))
            payload = product_probability_clip(img)
            results.append(
                ScoreResult(
                    sample=sample,
                    product_prob=float(payload["product_prob"]),
                    best_prompt=str(payload.get("best_prompt") or ""),
                    best_positive_prompt=str(payload.get("best_positive_prompt") or ""),
                    best_negative_prompt=str(payload.get("best_negative_prompt") or ""),
                )
            )
        except Exception as exc:
            errors.append(f"{sample.store_url} [{sample.title[:40]}]: {exc}")

        if idx % 20 == 0:
            print(f"Scored {idx}/{len(samples)} images...")

    return results, errors


def percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = p * (len(sorted_values) - 1)
    low = int(rank)
    high = min(low + 1, len(sorted_values) - 1)
    weight = rank - low
    return sorted_values[low] * (1.0 - weight) + sorted_values[high] * weight


def summarize(results: list[ScoreResult], threshold: float) -> None:
    if not results:
        print("No results to summarize.")
        return

    probs = sorted(r.product_prob for r in results)
    passed = sum(1 for r in results if r.product_prob >= threshold)
    below = len(results) - passed

    print("\n=== SUMMARY ===")
    print(f"Scored images: {len(results)}")
    print(f"Threshold: {threshold:.2f}")
    print(f">= threshold: {passed} ({passed / len(results):.1%})")
    print(f"<  threshold: {below} ({below / len(results):.1%})")
    print(
        "Score stats: "
        f"min={probs[0]:.4f}  p10={percentile(probs, 0.10):.4f}  p25={percentile(probs, 0.25):.4f}  "
        f"median={median(probs):.4f}  mean={mean(probs):.4f}  p75={percentile(probs, 0.75):.4f}  "
        f"p90={percentile(probs, 0.90):.4f}  max={probs[-1]:.4f}"
    )

    print("\nLowest-scoring 15 samples (manual spot-check candidates):")
    for r in sorted(results, key=lambda x: x.product_prob)[:15]:
        print(
            f"{r.product_prob:0.4f} | {r.sample.store_url} | {r.sample.title[:60]} | "
            f"best={r.best_prompt} | pos={r.best_positive_prompt} | neg={r.best_negative_prompt}"
        )


def write_audit_csv(path: Path, results: list[ScoreResult], threshold: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "product_prob",
                "threshold",
                "predicted_keep",
                "store_url",
                "product_id",
                "title",
                "image_url",
                "best_prompt",
                "best_positive_prompt",
                "best_negative_prompt",
                "actual_label",  # fill manually: keep/drop
                "is_correct",  # fill manually: 1/0
                "notes",
            ]
        )
        for r in sorted(results, key=lambda x: x.product_prob):
            writer.writerow(
                [
                    f"{r.product_prob:.6f}",
                    f"{threshold:.2f}",
                    int(r.product_prob >= threshold),
                    r.sample.store_url,
                    r.sample.product_id,
                    r.sample.title,
                    r.sample.image_url,
                    r.best_prompt,
                    r.best_positive_prompt,
                    r.best_negative_prompt,
                    "",
                    "",
                    "",
                ]
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stores-csv", type=Path, default=Path("data/stores.csv"))
    parser.add_argument("--max-stores", type=int, default=80)
    parser.add_argument("--max-samples", type=int, default=160)
    parser.add_argument("--per-store-max-products", type=int, default=4)
    parser.add_argument("--threshold", type=float, default=0.65)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--output-csv", type=Path, default=Path("out/classifier_validation_audit.csv"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    random.seed(args.seed)

    if not args.stores_csv.exists():
        print(f"Missing stores CSV: {args.stores_csv}")
        return 1

    store_urls = read_store_urls(args.stores_csv, args.max_stores)
    if not store_urls:
        print("No stores found in CSV.")
        return 1

    random.shuffle(store_urls)

    print(f"Loading samples from up to {len(store_urls)} stores...")

    with httpx.Client(headers={"User-Agent": "AisleyScraperValidation/1.0"}) as client:
        samples, sample_errors = fetch_product_samples(
            client=client,
            store_urls=store_urls,
            max_samples=args.max_samples,
            per_store_max_products=args.per_store_max_products,
        )
        print(f"Collected {len(samples)} unique first-image samples.")
        if sample_errors:
            print(f"Store fetch errors: {len(sample_errors)}")

        results, score_errors = score_samples(client, samples)

    summarize(results, args.threshold)
    write_audit_csv(args.output_csv, results, args.threshold)
    print(f"Audit CSV written: {args.output_csv}")

    total_errors = len(sample_errors) + len(score_errors)
    print(f"\nImage score errors: {len(score_errors)}")
    print(f"Total non-fatal errors: {total_errors}")

    if score_errors:
        print("\nFirst 10 score errors:")
        for msg in score_errors[:10]:
            print(f"- {msg}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
