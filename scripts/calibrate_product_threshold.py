#!/usr/bin/env python
"""Calibrate the product-photo CLIP threshold against a labeled image sample.

Scores every image in an "apparel" folder (should be KEPT) and a "non-apparel"
folder (should be DROPPED) with the configured encoder
(``CLIP_MODEL_NAME`` — defaults to Marqo-FashionSigLIP), then sweeps candidate
thresholds and prints precision / recall / F1 so you can pick a value.

The kept decision mirrors production: an image is "kept" when
``product_prob >= threshold`` (see image_validation.product_probability_clip).
Apparel = positive class; precision = how few non-apparel slip through.

Usage:
  python scripts/calibrate_product_threshold.py \
      --apparel-dir ./calib/apparel --non-apparel-dir ./calib/non_apparel
  python scripts/calibrate_product_threshold.py \
      --apparel-dir A --non-apparel-dir B --thresholds 0.5,0.6,0.7,0.8,0.9
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".heic", ".heif"}


def _iter_images(directory: Path) -> list[Path]:
    return sorted(
        p for p in directory.rglob("*") if p.is_file() and p.suffix.lower() in _IMAGE_EXTS
    )


def _default_thresholds() -> list[float]:
    return [round(0.50 + 0.05 * i, 2) for i in range(10)]  # 0.50 .. 0.95


def _summary(probs: list[float]) -> str:
    if not probs:
        return "n=0"
    ordered = sorted(probs)
    n = len(ordered)
    mean = sum(ordered) / n
    return (
        f"n={n} min={ordered[0]:.3f} median={ordered[n // 2]:.3f} "
        f"mean={mean:.3f} max={ordered[-1]:.3f}"
    )


def sweep_thresholds(
    apparel_probs: list[float],
    nonapparel_probs: list[float],
    thresholds: list[float],
) -> list[dict[str, float]]:
    """Pure threshold sweep — apparel is the positive (should-keep) class.

    Returns one row per threshold with precision/recall/F1 and raw counts.
    """
    rows: list[dict[str, float]] = []
    total_apparel = len(apparel_probs)
    for t in thresholds:
        tp = sum(1 for p in apparel_probs if p >= t)       # apparel kept (correct)
        fn = total_apparel - tp                            # apparel dropped (recall loss)
        fp = sum(1 for p in nonapparel_probs if p >= t)    # non-apparel kept (precision loss)
        kept = tp + fp
        precision = (tp / kept) if kept else 1.0
        recall = (tp / total_apparel) if total_apparel else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
        rows.append(
            {
                "threshold": t,
                "precision": precision,
                "recall": recall,
                "f1": f1,
                "apparel_kept": tp,
                "apparel_dropped": fn,
                "nonapparel_kept": fp,
            }
        )
    return rows


def _score_dir(directory: Path, product_probability_clip, label: str) -> list[float]:
    from PIL import Image

    images = _iter_images(directory)
    if not images:
        print(f"WARNING: no images found in {label} dir: {directory}", file=sys.stderr)
        return []

    probs: list[float] = []
    for idx, path in enumerate(images, start=1):
        try:
            with Image.open(path) as img:
                result = product_probability_clip(img.convert("RGB"))
            probs.append(float(result.get("product_prob", 0.0)))
        except Exception as exc:  # keep going; report at end
            print(f"  skip {path.name}: {exc}", file=sys.stderr)
        if idx % 25 == 0:
            print(f"  {label}: scored {idx}/{len(images)}", file=sys.stderr)
    return probs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apparel-dir", required=True, help="folder of images that SHOULD be kept")
    parser.add_argument("--non-apparel-dir", required=True, help="folder of images that SHOULD be dropped")
    parser.add_argument(
        "--thresholds",
        default=None,
        help="comma-separated thresholds (default 0.50..0.95 step 0.05)",
    )
    args = parser.parse_args()

    thresholds = (
        [round(float(x), 4) for x in args.thresholds.split(",") if x.strip()]
        if args.thresholds
        else _default_thresholds()
    )

    from aisley_scraper.config import get_settings
    from aisley_scraper.image_validation import product_probability_clip, warmup_clip

    settings = get_settings()
    print(f"Encoder: CLIP_MODEL_NAME={settings.clip_model_name!r} CLIP_PRETRAINED={settings.clip_pretrained!r}")
    print("Loading model + warming prompts (first run downloads weights)...", file=sys.stderr)
    warmup_clip(strict=False)

    apparel_probs = _score_dir(Path(args.apparel_dir), product_probability_clip, "apparel")
    nonapparel_probs = _score_dir(Path(args.non_apparel_dir), product_probability_clip, "non-apparel")

    if not apparel_probs and not nonapparel_probs:
        print("No images scored — nothing to calibrate.", file=sys.stderr)
        return 1

    print()
    print(f"apparel product_prob:      {_summary(apparel_probs)}")
    print(f"non-apparel product_prob:  {_summary(nonapparel_probs)}")
    print()
    print(f"{'thresh':>7}  {'precision':>9}  {'recall':>7}  {'f1':>6}  "
          f"{'kept(app)':>9}  {'drop(app)':>9}  {'kept(non)':>9}")
    print("-" * 72)
    rows = sweep_thresholds(apparel_probs, nonapparel_probs, thresholds)
    best_f1 = max(rows, key=lambda r: r["f1"]) if rows else None
    for r in rows:
        marker = "  <- best F1" if r is best_f1 else ""
        print(
            f"{r['threshold']:>7.2f}  {r['precision']:>9.3f}  {r['recall']:>7.3f}  "
            f"{r['f1']:>6.3f}  {int(r['apparel_kept']):>9}  {int(r['apparel_dropped']):>9}  "
            f"{int(r['nonapparel_kept']):>9}{marker}"
        )
    print()
    print("Precision = apparel_kept / all_kept (higher = fewer non-apparel slip through).")
    print("Recall    = apparel_kept / all_apparel (lower = more good items dropped).")
    print("For precision-first: pick the highest threshold whose recall is still acceptable.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
