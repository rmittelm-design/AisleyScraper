# Running the scraper on Google Cloud (Cloud Run Jobs)

This runs the full pipeline on GCP, where the network path to Supabase is far
more stable than a local machine (local Phase 2 reliably stalls on cross-region
DB calls). It's two Cloud Run **Jobs** sharing one **run-id**:

```
Phase 1  job "aisley-scrape"   1 task     scrape every store -> shopify_*_staging
Phase 2  job "aisley-enrich"   N tasks    enrich staging -> shopify_products (sharded)
```

Both run the same image (`cloud/run_pipeline.py`), selected by `AISLEY_PHASE`.
Project/region default to the Aisley backend's: **`aisley` / `us-east1`** (override
with `PROJECT=` / `REGION=`). Images go to `gcr.io/aisley/aisley-scraper`.

## Safety (baked into the image — this is why it won't repeat the deletion)
- **`PRUNE_NONTSV_STORES=false`** — the crawl will **never delete stores**.
- **`PRUNE_MAX_STORES=25`** — even if enabled, a crawl refuses to prune more than
  this many (guard against an incomplete TSV wiping the catalog).
- **`GEOCODING_ENABLED=false`** — skips branch geocoding; existing lat/long is
  preserved via `coalesce` (avoids the Nominatim-429 stall).
- **Image uploads are removed in code** — no Supabase Storage writes/egress.
- **`prune-nonfashion` is dry-run by default** and `--safe` (never deletes
  jewelry/apparel) — destructive deletes require an explicit `--execute`.

## Prerequisites
- A run-id. For a normal run, generate one (`uuidgen`). For the **recovery**
  re-scrape, you can reuse any id; the pipeline is idempotent.
- `gcloud` authed with access to project `aisley`.
- Local `.env` with `DATABASE_URL`, `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`,
  `SUPABASE_STORAGE_BUCKET`, `SUPABASE_STORAGE_PATH` (already point at the
  prod Supabase).

## One-time setup
```bash
./cloud/deploy_pipeline.sh secrets   # push DB + service-role key to Secret Manager (reads .env)
./cloud/deploy_pipeline.sh build     # build & push the image (torch-cpu + baked CLIP model + TSVs)
./cloud/deploy_pipeline.sh deploy    # create both jobs (TASKS=5 enrich shards by default)
```

## Run it
```bash
RID=$(uuidgen)                       # or reuse an existing run-id
./cloud/deploy_pipeline.sh run "$RID"   # Phase 1 (scrape) then Phase 2 (enrich), waiting on each
```
Or run the phases separately:
```bash
./cloud/deploy_pipeline.sh scrape "$RID"   # Phase 1 only
./cloud/deploy_pipeline.sh enrich "$RID"   # Phase 2 only (sharded)
```

## Track progress + time remaining
Each phase prints a live line (visible in Cloud Run logs):
```
Phase 1 (scrape): 132/210 (62.9%) | elapsed 4m10s | ~2m27s left | 31.7/min
Phase 2 (enrich): 45/180  (25.0%) | elapsed 10m00s | ~30m00s left | 4.5/min
```
Watch them stream:
```bash
./cloud/deploy_pipeline.sh logs scrape    # Phase 1
./cloud/deploy_pipeline.sh logs enrich    # Phase 2
```
Or in the console: **Cloud Run → Jobs → aisley-scrape / aisley-enrich → Executions → Logs**.
(Phase 2 ETA is per shard; with N shards the wall-clock is ~the slowest shard.)

## Scaling & the real ceiling
- More enrich shards: `TASKS=10 ./cloud/deploy_pipeline.sh deploy` then `run`.
- The bottleneck is **Supabase**, not your shards: total load = `TASKS ×
  PHASE2_UPLOAD_CONCURRENCY` (uploads are off now, so this is mostly DB writes +
  image *downloads* for CLIP). If you see `429`s or pooler "too many connections",
  lower `PARALLELISM` or the per-task concurrency env vars.

## Cost (rough)
- **Compute**: 2 vCPU / 4 GiB ≈ **$0.21/task-hour**; a full run is a handful of
  task-hours → **~$2–6**, much of it inside the monthly free tier.
- **Egress**: ~$0 — uploads removed; image downloads are ingress (free).
- **Build + registry**: free build tier + pennies of image storage.

## The recovery (restore deleted items + finish the cleanup)
A full re-scrape is **self-correcting**:
- products that pass the filter (apparel/jewelry) are re-added — restoring the
  items the earlier aggressive delete removed by mistake;
- products that fail the filter (cosmetics, nipple covers, candles, …) are left
  out — completing the non-fashion cleanup.

```bash
./cloud/deploy_pipeline.sh build && ./cloud/deploy_pipeline.sh deploy
./cloud/deploy_pipeline.sh run "$(uuidgen)"
```
Then verify with `aisley-scraper diagnose-staged-runs` and spot-checks.

## Caveats
- Rebuild the image if you change `data/stores/` (TSVs are baked in for branch fan-out).
- Run Phase 1 → Phase 2 in order for a given run-id; don't run two enrich jobs on
  the same run concurrently.
