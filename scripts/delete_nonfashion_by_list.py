#!/usr/bin/env python3
"""Delete EXACTLY the products listed in a reviewed candidate TSV.

Input: the TSV produced by list_nonfashion_candidates.py (after you've reviewed
it and removed any rows you want to keep). Columns used:
    id, item_uuid, supabase_images_json   (other columns ignored)

For each listed id it deletes the shopify_products row, then its item_embeddings
(by item_uuid), then its /scraped Supabase storage images. Driven purely by the
reviewed id list — there is NO live category re-scan, so it can only ever delete
what you approved.

    python scripts/delete_nonfashion_by_list.py <reviewed.tsv>            # DRY RUN
    python scripts/delete_nonfashion_by_list.py <reviewed.tsv> --execute  # APPLY

Writes (deletes) are reliable on the cross-region pooler; only big READS freeze.
"""
from __future__ import annotations

import json
import sys

from aisley_scraper.config import get_settings
from aisley_scraper.db.repository import Repository
from aisley_scraper.storage import StorageUploader

ROW_BATCH = 500
IMG_BATCH = 200


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    execute = "--execute" in sys.argv[1:]
    if not args:
        print("Usage: delete_nonfashion_by_list.py <reviewed.tsv> [--execute]")
        return 2
    path = args[0]

    ids: list[int] = []
    uuids: list[str] = []
    image_urls: list[str] = []
    with open(path, encoding="utf-8") as fh:
        header = fh.readline().rstrip("\n").split("\t")
        idx = {name: i for i, name in enumerate(header)}
        if "id" not in idx:
            print(f"ABORT: no 'id' column in {path} header={header}")
            return 2
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) <= idx["id"]:
                continue
            try:
                ids.append(int(parts[idx["id"]]))
            except ValueError:
                continue
            if "item_uuid" in idx and len(parts) > idx["item_uuid"]:
                u = parts[idx["item_uuid"]].strip()
                if u:
                    uuids.append(u)
            if "supabase_images_json" in idx and len(parts) > idx["supabase_images_json"]:
                try:
                    image_urls.extend(json.loads(parts[idx["supabase_images_json"]] or "[]"))
                except json.JSONDecodeError:
                    pass

    print(
        f"Loaded {len(ids)} product ids, {len(uuids)} embeddings, "
        f"{len(image_urls)} storage images from {path}",
        flush=True,
    )
    if not execute:
        print("DRY RUN — nothing deleted. Re-run with --execute to apply.")
        return 0

    settings = get_settings()
    repo = Repository(settings)
    uploader = StorageUploader(settings)

    deleted_rows = 0
    for chunk in _chunks(ids, ROW_BATCH):
        deleted_rows += repo.delete_products_by_ids(chunk)
        print(f"rows: {deleted_rows}/{len(ids)}", flush=True)

    deleted_emb = 0
    for chunk in _chunks(uuids, ROW_BATCH):
        try:
            deleted_emb += repo.delete_item_embeddings_batch(chunk)
        except Exception as exc:  # noqa: BLE001
            print(f"WARN embedding batch failed ({len(chunk)}): {exc}", flush=True)
    print(f"embeddings deleted: {deleted_emb}", flush=True)

    deleted_imgs = 0
    for chunk in _chunks(image_urls, IMG_BATCH):
        try:
            uploader.delete_images(chunk)
            deleted_imgs += len(chunk)
        except Exception as exc:  # noqa: BLE001
            print(f"WARN storage batch failed ({len(chunk)}): {exc}", flush=True)
    print(f"storage images deleted: {deleted_imgs}", flush=True)

    print(f"DONE: rows={deleted_rows} embeddings={deleted_emb} images={deleted_imgs}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
