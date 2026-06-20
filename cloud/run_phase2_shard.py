#!/usr/bin/env python3
"""Cloud Run Jobs entrypoint: run ONE shard of Phase 2 enrichment.

Each Cloud Run task runs this once. Cloud Run sets ``CLOUD_RUN_TASK_INDEX``
(0-based) and ``CLOUD_RUN_TASK_COUNT`` for every task, which we turn into a
disjoint shard of the staged stores — task k enriches the websites where
``hash(website) % CLOUD_RUN_TASK_COUNT == k``. With ``--tasks N`` the run fans
out into N independent workers writing to the same Supabase DB + Storage; the
work is per-store and idempotent, so no cross-task coordination is needed.

Required env:
  AISLEY_RUN_ID           the staged run to enrich (from `diagnose-staged-runs`)
  DATABASE_URL            Supabase Postgres (pooler) DSN
  SUPABASE_URL            Supabase project URL (object storage)
  SUPABASE_SERVICE_ROLE_KEY
  SUPABASE_STORAGE_BUCKET / SUPABASE_STORAGE_PATH

Provided automatically by Cloud Run Jobs:
  CLOUD_RUN_TASK_INDEX, CLOUD_RUN_TASK_COUNT

Run it locally with the same semantics:
  AISLEY_RUN_ID=<id> CLOUD_RUN_TASK_INDEX=0 CLOUD_RUN_TASK_COUNT=3 \
      python cloud/run_phase2_shard.py
"""
from __future__ import annotations

import os
import sys


def main() -> int:
    run_id = (os.environ.get("AISLEY_RUN_ID") or "").strip()
    if not run_id:
        print("FATAL: AISLEY_RUN_ID is required (the staged run to enrich).", file=sys.stderr)
        return 2

    shard_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", "0") or 0)
    shard_count = int(os.environ.get("CLOUD_RUN_TASK_COUNT", "1") or 1)
    if shard_count < 1:
        shard_count = 1
    if not (0 <= shard_index < shard_count):
        print(
            f"FATAL: invalid shard index={shard_index} count={shard_count}.",
            file=sys.stderr,
        )
        return 2

    # Import after the env checks so a misconfigured task fails fast and cheap.
    from aisley_scraper.cli import run_crawl

    print(
        f"Phase 2 shard {shard_index}/{shard_count} starting for run_id={run_id}",
        flush=True,
    )
    return run_crawl(
        limit=None,
        run_id=run_id,
        phase="2",
        shard_index=shard_index,
        shard_count=shard_count,
    )


if __name__ == "__main__":
    raise SystemExit(main())
