#!/usr/bin/env python3
"""Cloud Run Jobs entrypoint for the scraper pipeline.

Runs ONE phase per job, selected by env, against a shared run-id:

  AISLEY_PHASE=1   -> scrape every store into staging (single task)
  AISLEY_PHASE=2   -> enrich staged data + write production (shard per task)

Required env:
  AISLEY_RUN_ID            the run to scrape/enrich (one id ties Phase 1 -> Phase 2)
  DATABASE_URL, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY,
  SUPABASE_STORAGE_BUCKET, SUPABASE_STORAGE_PATH

Safety env (set by the image; override consciously):
  PRUNE_NONTSV_STORES=false   never delete stores on the Phase 1 crawl
  GEOCODING_ENABLED=false     skip branch geocoding (existing coords are kept)
  FETCHER_DISK_CACHE_ENABLED=false

Phase 2 sharding is automatic from Cloud Run's CLOUD_RUN_TASK_INDEX/COUNT:
task k of N enriches the staged stores where hash(website) % N == k.

Progress + ETA are printed by each phase (visible in Cloud Run logs):
  "Phase 2 (enrich): 45/180 (25.0%) | elapsed 10m00s | ~30m00s left | 4.5/min"
"""
from __future__ import annotations

import os
import sys


def main() -> int:
    run_id = (os.environ.get("AISLEY_RUN_ID") or "").strip()
    if not run_id:
        print("FATAL: AISLEY_RUN_ID is required.", file=sys.stderr)
        return 2

    phase = (os.environ.get("AISLEY_PHASE") or "2").strip()
    if phase not in ("1", "2"):
        print(f"FATAL: AISLEY_PHASE must be '1' or '2' (got {phase!r}).", file=sys.stderr)
        return 2

    shard_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", "0") or 0)
    shard_count = int(os.environ.get("CLOUD_RUN_TASK_COUNT", "1") or 1)
    if shard_count < 1:
        shard_count = 1

    from aisley_scraper.cli import run_crawl

    if phase == "1":
        print(f"Pipeline: Phase 1 (scrape) for run_id={run_id}", flush=True)
        return run_crawl(limit=None, run_id=run_id, phase="1")

    if not (0 <= shard_index < shard_count):
        print(f"FATAL: invalid shard {shard_index}/{shard_count}.", file=sys.stderr)
        return 2
    print(
        f"Pipeline: Phase 2 (enrich) shard {shard_index}/{shard_count} for run_id={run_id}",
        flush=True,
    )
    return run_crawl(
        limit=None, run_id=run_id, phase="2",
        shard_index=shard_index, shard_count=shard_count,
    )


if __name__ == "__main__":
    raise SystemExit(main())
