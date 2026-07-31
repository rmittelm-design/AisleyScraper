#!/usr/bin/env python
"""Sample products from the store TSVs, score them with the configured encoder
(Marqo-FashionSigLIP), and report the product_prob distribution + a threshold
sweep so PHASE2_FIRST_IMAGE_PRODUCT_PROB_THRESHOLD can be picked from real data.

Items are UNLABELED but predominantly apparel, and the kids/non-apparel keyword
filter (should_exclude_product) is applied first — mirroring production — so the
score distribution approximates the "should-keep" apparel distribution: pick a
threshold that retains the bulk of it. Each item is scored exactly like
production: max product_prob over its first K images. The lowest-scoring items
are listed so you can eyeball whether they are genuine non-apparel or
false-negatives.

Usage:
  python scripts/sample_store_product_probs.py --stores 5 --per-store 20 --images-per-item 3
"""

from __future__ import annotations

import argparse
import statistics
import sys
from pathlib import Path

import httpx
from PIL import Image
import io

_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"


def _default_thresholds() -> list[float]:
    return [round(0.50 + 0.05 * i, 2) for i in range(10)]  # 0.50 .. 0.95


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--stores", type=int, default=5, help="number of stores to sample")
    ap.add_argument("--per-store", type=int, default=20, help="products per store")
    ap.add_argument("--images-per-item", type=int, default=3, help="lead images scored per item (max taken)")
    ap.add_argument("--thresholds", default=None)
    ap.add_argument("--threshold", type=float, default=0.9, help="decision threshold for kept/dropped buckets")
    args = ap.parse_args()

    thresholds = (
        [round(float(x), 4) for x in args.thresholds.split(",") if x.strip()]
        if args.thresholds
        else _default_thresholds()
    )

    from aisley_scraper.config import get_settings
    from aisley_scraper.ingest.csv_loader import load_store_seeds_from_dir
    from aisley_scraper.image_validation import product_probability_clip, warmup_clip
    from aisley_scraper.models import ProductRecord
    from aisley_scraper.normalize.products import should_exclude_product

    settings = get_settings()
    print(f"Encoder: {settings.clip_model_name!r}", file=sys.stderr)
    print("Loading model (first run downloads weights)...", file=sys.stderr)
    warmup_clip(strict=False)

    seeds = load_store_seeds_from_dir(settings.input_tsv_dir, settings)[: args.stores]
    client = httpx.Client(timeout=20.0, follow_redirects=True, headers={"User-Agent": _UA})

    scored: list[dict] = []
    keyword_dropped = 0
    for seed in seeds:
        base = seed.store_url.rstrip("/")
        try:
            resp = client.get(f"{base}/products.json", params={"limit": args.per_store, "page": 1})
            resp.raise_for_status()
            products = resp.json().get("products", [])
        except Exception as exc:
            print(f"  store fetch failed {base}: {exc}", file=sys.stderr)
            continue
        print(f"  {base}: {len(products)} products", file=sys.stderr)

        for prod in products:
            title = prod.get("title") or ""
            handle = prod.get("handle") or ""
            ptype = prod.get("product_type") or ""
            img_srcs = [im.get("src") for im in (prod.get("images") or []) if im.get("src")]
            url = f"{base}/products/{handle}" if handle else base

            rec = ProductRecord(
                product_id=str(prod.get("id", "")), product_handle=handle, item_name=title,
                description=None, images=img_srcs, product_url=url, product_type=ptype,
            )
            if should_exclude_product(rec):
                keyword_dropped += 1
                continue
            if not img_srcs:
                continue

            best = None
            for src in img_srcs[: max(1, args.images_per_item)]:
                try:
                    b = client.get(src).content
                    img = Image.open(io.BytesIO(b)).convert("RGB")
                    prob = float(product_probability_clip(img).get("product_prob", 0.0))
                except Exception:
                    continue
                best = prob if best is None else max(best, prob)
            if best is not None:
                scored.append({"store": base, "title": title, "ptype": ptype, "prob": best, "img": img_srcs[0]})
        print(f"    scored so far: {len(scored)} (keyword-dropped {keyword_dropped})", file=sys.stderr)

    if not scored:
        print("No items scored.", file=sys.stderr)
        return 1

    probs = [s["prob"] for s in scored]
    ordered = sorted(probs)
    n = len(ordered)
    print()
    print(f"Scored {n} items (keyword-dropped before scoring: {keyword_dropped}) across {len(seeds)} stores.")
    print(
        f"product_prob: min={ordered[0]:.3f} p10={ordered[n//10]:.3f} median={ordered[n//2]:.3f} "
        f"mean={statistics.mean(ordered):.3f} p90={ordered[min(n-1,(9*n)//10)]:.3f} max={ordered[-1]:.3f}"
    )
    print()
    print(f"{'threshold':>9}  {'kept':>5}  {'dropped':>7}  {'keep_rate':>9}")
    print("-" * 38)
    for t in thresholds:
        kept = sum(1 for p in probs if p >= t)
        print(f"{t:>9.2f}  {kept:>5}  {n - kept:>7}  {kept / n:>8.1%}")
    # ── Decision buckets at --threshold (kept = predicted fashion) ──
    t = args.threshold
    kept = sorted([s for s in scored if s["prob"] >= t], key=lambda x: x["prob"])
    dropped = sorted([s for s in scored if s["prob"] < t], key=lambda x: x["prob"])
    # Heuristic: product_type that smells non-apparel (for false-positive hunting).
    nonapparel_hint = (
        "beauty", "cosmetic", "candle", "home", "fragrance", "skincare", "hair",
        "accessor", "gift", "drink", "mug", "tumbler", "decor", "soap", "bath",
        "pet", "book", "stationery", "jewel",
    )

    def _row(s: dict) -> str:
        return f"  {s['prob']:.3f}  [{(s['ptype'] or '-')[:18]:18}] {s['title'][:40]:40}  {s['img']}"

    print()
    print(f"=== Decision at threshold {t:.2f}: kept={len(kept)}  dropped={len(dropped)} ===")
    print()
    print("TRUE-POSITIVE candidates — highest-scoring KEPT (clearly apparel):")
    for s in sorted(kept, key=lambda x: -x["prob"])[:8]:
        print(_row(s))
    print()
    print("FALSE-POSITIVE candidates — KEPT but lowest score / non-apparel product_type:")
    fp = [s for s in kept if any(h in (s["ptype"] or "").lower() for h in nonapparel_hint)]
    fp_shown = (fp + [s for s in kept if s not in fp])[:12]
    for s in fp_shown:
        tag = "  <-- non-apparel type?" if s in fp else ""
        print(_row(s) + tag)
    print()
    print(f"DROPPED — predicted NOT fashion (true-negatives if non-apparel, false-negatives if apparel):")
    if dropped:
        for s in dropped:
            print(_row(s))
    else:
        print("  (none)")
    print()
    print("Eyeball the image URLs: KEPT non-apparel = false positives; DROPPED apparel = false negatives.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
