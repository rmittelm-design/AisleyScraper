# Parallel Phase 2 on Cloud Run Jobs

Phase 2 (CLIP validation + image upload + production write) is the slow part of
the scraper, and it's **per-store and idempotent**, so it parallelizes cleanly.
This runs it as a **Cloud Run Job** with `--tasks N`: each task enriches a
disjoint shard of the staged stores, all writing to the same Supabase DB +
Storage. No coordination needed.

```
Phase 1 (scrape -> staging)         Phase 2 (this) — N parallel Cloud Run tasks
  local / single run                  task 0 ─┐
  populates shopify_*_staging         task 1 ─┼─> CLIP + upload + upsert -> Supabase
  for a run_id                        task 2 ─┘   (each task: hash(website) % N == k)
```

## How sharding works
Cloud Run sets `CLOUD_RUN_TASK_INDEX` (0..N-1) and `CLOUD_RUN_TASK_COUNT` (N) on
every task. [`run_phase2_shard.py`](run_phase2_shard.py) turns those into a
shard: task `k` processes the staged websites where
`sha256(website) % N == k` (stable hash, so shards are disjoint and complete).
The same flags exist on the CLI: `crawl-stores --phase 2 --shard-index k --shard-count N`.

## Prerequisites
- A staged run to enrich. Get its id with `aisley-scraper diagnose-staged-runs`.
- `gcloud` authed to the right project (currently `aisley`, region `us-east1`).
- Your local `.env` with `DATABASE_URL`, `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`,
  `SUPABASE_STORAGE_BUCKET`, `SUPABASE_STORAGE_PATH`.

## One-time setup
```bash
./cloud/deploy_phase2_job.sh secrets   # pushes DB + service-role key to Secret Manager, grants access
./cloud/deploy_phase2_job.sh build     # builds the CPU image (torch-cpu + baked CLIP model + TSVs)
./cloud/deploy_phase2_job.sh deploy    # creates the Cloud Run Job (TASKS shards)
```

## Run it
```bash
./cloud/deploy_phase2_job.sh run 182c2695-7e6f-46fd-9a45-f05afb80817e
```
This executes all shards and waits. Re-running is safe — finished products are
skipped and completed stores are removed from staging, so it resumes.

## Scaling & tuning
- **More workers:** `TASKS=10 ./cloud/deploy_phase2_job.sh deploy` (then `run`).
- **The real ceiling is Supabase, not your workers.** Total concurrency =
  `TASKS × PHASE2_UPLOAD_CONCURRENCY`. If you see 429s or pooler "too many
  connections", lower `PARALLELISM` (cap concurrent tasks) or
  `PHASE2_UPLOAD_CONCURRENCY` / `CRAWL_GLOBAL_CONCURRENCY` (set via
  `--set-env-vars` on deploy, or edit the Dockerfile defaults).
- **Image config defaults** (override at deploy): `CPU=2`, `MEMORY=4Gi`,
  `PHASE2_UPLOAD_CONCURRENCY=6`, `IMAGE_VALIDATION_CONCURRENCY=4`,
  `CRAWL_GLOBAL_CONCURRENCY=4`, `PHASE2_STORE_BATCH_SIZE=6`,
  `FETCHER_DISK_CACHE_ENABLED=false` (Cloud Run's FS is in-memory, so the disk
  cache would just eat RAM — the in-memory byte cache covers it).
- **Load balance:** hash sharding splits by store *count*, not size, so a few
  giant stores can make one shard finish last. If that bites, raise `TASKS`.

## Caveats
- Branch fan-out reads the TSVs (baked into the image at `/app/data/stores`). If
  you change the TSVs, rebuild the image.
- Geocoding only runs for branches still missing lat/long; if many are missing,
  many parallel tasks could hit the geocoder's rate limit. Fill coordinates
  first (you already have) to avoid this.
- Run Phase 1 and Phase 2 for a given run **one at a time**; only Phase 2 is
  sharded here.

## Logs
```bash
gcloud run jobs executions list --job aisley-phase2 --region us-east1
gcloud beta run jobs executions logs <EXECUTION> --region us-east1
```
