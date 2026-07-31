#!/usr/bin/env python3
"""Read-only: list every saved product the SAFE prune would delete.

Scans shopify_products and writes one TSV row per product matching the safe
non-fashion rule (matches_clear_nonfashion — never jewelry/apparel). Deletes
NOTHING. Review the output before running the delete step.

    python scripts/list_nonfashion_candidates.py [out.tsv]

Output columns:
    id, store_id, product_id, product_type, item_name, product_url,
    item_uuid, supabase_images_json

Resilience: the cross-region Supabase pooler can FREEZE a read mid-scan (socket
stays open, no data flows, 0% CPU) in a way TCP keepalives can't catch. Each page
is fetched in a worker thread with a wall-clock timeout; on timeout we abandon the
frozen connection (daemon thread) and retry the same page with a fresh connection.
"""
from __future__ import annotations

import json
import queue
import sys
import threading

from aisley_scraper.config import get_settings
from aisley_scraper.db.repository import Repository
from aisley_scraper.normalize.products import matches_clear_nonfashion

PAGE = 500
PAGE_TIMEOUT_SEC = 75
MAX_TIMEOUTS = 20


def _fetch_page(repo: Repository, after_id, q: "queue.Queue") -> None:
    try:
        q.put(("ok", repo.list_products_for_category_scan(limit=PAGE, after_id=after_id)))
    except Exception as exc:  # noqa: BLE001 - reported to the main thread
        q.put(("err", exc))


def _page_with_timeout(repo: Repository, after_id):
    """Return the page rows, or raise TimeoutError if the read froze."""
    q: "queue.Queue" = queue.Queue(maxsize=1)
    t = threading.Thread(target=_fetch_page, args=(repo, after_id, q), daemon=True)
    t.start()
    try:
        kind, val = q.get(timeout=PAGE_TIMEOUT_SEC)
    except queue.Empty as exc:
        raise TimeoutError(f"page read froze after {PAGE_TIMEOUT_SEC}s (after_id={after_id})") from exc
    if kind == "err":
        raise val
    return val


def main() -> int:
    out_path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/prune_candidates.tsv"
    settings = get_settings()
    repo = Repository(settings)

    after_id = None
    scanned = 0
    matched = 0
    timeouts = 0

    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(
            "id\tstore_id\tproduct_id\tproduct_type\titem_name\tproduct_url\t"
            "item_uuid\tsupabase_images_json\n"
        )
        while True:
            try:
                rows = _page_with_timeout(repo, after_id)
            except Exception as exc:  # noqa: BLE001 - frozen/failed page -> reconnect+resume
                timeouts += 1
                print(f"RETRY page after_id={after_id} ({timeouts}/{MAX_TIMEOUTS}): {exc}", flush=True)
                if timeouts >= MAX_TIMEOUTS:
                    print("ABORT: too many frozen pages", flush=True)
                    return 1
                continue
            if not rows:
                break
            for row in rows:
                after_id = int(row["id"])
                scanned += 1
                if matches_clear_nonfashion(
                    item_name=row["item_name"],
                    product_url=row["product_url"],
                    product_handle=row["product_handle"],
                    product_type=row["product_type"],
                ):
                    matched += 1
                    name = (row["item_name"] or "").replace("\t", " ").replace("\n", " ")
                    url = (row["product_url"] or "").replace("\t", " ").replace("\n", " ")
                    ptype = (row["product_type"] or "").replace("\t", " ").replace("\n", " ")
                    imgs = json.dumps(row.get("supabase_images") or [])
                    fh.write(
                        f"{row['id']}\t{row['store_id']}\t{row['product_id']}\t"
                        f"{ptype}\t{name}\t{url}\t{row.get('item_uuid') or ''}\t{imgs}\n"
                    )
                    fh.flush()
            print(f"progress: scanned={scanned} matched={matched} timeouts={timeouts}", flush=True)
            if len(rows) < PAGE:
                break

    print(f"DONE: scanned={scanned} matched={matched} timeouts={timeouts} -> {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
