"""Export dropped-image and <0.9 score review artifacts from threshold-0.9 audit CSV."""

from __future__ import annotations

import csv
from pathlib import Path

IN_CSV = Path("out/classifier_validation_audit_t090_300.csv")
OUT_DROPPED_CSV = Path("out/dropped_images_t090_300.csv")
OUT_GALLERY_MD = Path("out/dropped_and_lt09_gallery.md")
SAMPLE_LT09_COUNT = 20


def main() -> int:
    if not IN_CSV.exists():
        print(f"Missing input CSV: {IN_CSV}")
        return 1

    rows = list(csv.DictReader(IN_CSV.open("r", encoding="utf-8", newline="")))
    if not rows:
        print("Input CSV has no rows.")
        return 1

    dropped = [r for r in rows if r.get("predicted_keep") == "0"]
    lt09_some = [r for r in rows if float(r.get("product_prob", 0.0)) < 0.9][:SAMPLE_LT09_COUNT]

    OUT_DROPPED_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_DROPPED_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(dropped)

    with OUT_GALLERY_MD.open("w", encoding="utf-8") as f:
        f.write("# Dropped Images at Threshold 0.90\n\n")
        f.write(f"Total dropped: {len(dropped)}\\n\\n")
        for i, r in enumerate(dropped, 1):
            score = float(r.get("product_prob", 0.0))
            f.write(f"## {i}. score={score:.6f} | {r.get('title','')}\\n")
            f.write(f"- store: {r.get('store_url','')}\\n")
            f.write(f"- product_id: {r.get('product_id','')}\\n")
            f.write(f"- image_url: {r.get('image_url','')}\\n")
            f.write(f"![img_{i}]({r.get('image_url','')})\\n\\n")

        f.write("# Some Images with Score < 0.90\n\n")
        f.write(f"Sample size: {len(lt09_some)}\\n\\n")
        for i, r in enumerate(lt09_some, 1):
            score = float(r.get("product_prob", 0.0))
            f.write(f"## S{i}. score={score:.6f} | {r.get('title','')}\\n")
            f.write(f"- store: {r.get('store_url','')}\\n")
            f.write(f"- product_id: {r.get('product_id','')}\\n")
            f.write(f"- image_url: {r.get('image_url','')}\\n")
            f.write(f"![sample_{i}]({r.get('image_url','')})\\n\\n")

    print(f"Wrote: {OUT_DROPPED_CSV}")
    print(f"Wrote: {OUT_GALLERY_MD}")
    print(f"dropped_count={len(dropped)}")
    print(f"sample_lt09_count={len(lt09_some)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
