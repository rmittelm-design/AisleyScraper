# Aisley Scraper

Env-driven Shopify store scraper that ingests URLs from CSV and persists store + product data to Supabase Postgres.

## Quick start

1. Copy `.env.example` to `.env` and fill required values.
2. Install dependencies:

	```bash
	pip install -r requirements.txt
	pip install -e .
	pip check
	source .venv/bin/activate
	```

	This installs image-validation dependencies used at runtime, including:
	`opencv-python-headless` (`cv2`) and `google-cloud-vision`.
3. Run `aisley-scraper ingest-stores --csv ./data/stores.csv`.
4. Run `aisley-scraper crawl-stores`.

If you are upgrading an existing deployment, apply migrations in order before crawling:

- `supabase/migrations/20260313120000_add_crawl_store_runs.sql`
- `supabase/migrations/20260314000000_add_staging_tables.sql`

Restart behavior for `crawl-stores`:

- Crawl source is DB-first: existing `shopify_stores` are processed first, then unseen CSV stores are appended.
- A run id is persisted in `.aisley_active_run_id` by default, so restarts resume from pending/failed stores in the same run.
- Use `--fresh` to start a new run id.
- Use `--run-id <id>` to explicitly resume a specific run.
- For two-phase resume, `--phase 2` now requires an existing staged run id and will not create a new run implicitly.

Before every crawl run (Supabase mode), orphaned storage objects are checked and auto-cleaned; crawl starts only when no orphans remain.

## Crawl Run Modes

Start a new crawl run id:

```bash
aisley-scraper crawl-stores --fresh
```

Resume from the active run id stored at `CRAWL_RUN_STATE_PATH` (default `.aisley_active_run_id`):

```bash
aisley-scraper crawl-stores
```

Resume a specific run id explicitly:

```bash
aisley-scraper crawl-stores --run-id <run-id>
```

List resumable staged runs:

```bash
aisley-scraper diagnose-staged-runs
```

Limit stores in a run (useful for canary runs):

```bash
aisley-scraper crawl-stores --fresh --limit 50
```

Run crawl and persist products without uploading images to Supabase Storage:

```bash
aisley-scraper crawl-stores --skip-image-upload
```

## Two-Phase Pipeline (`--phase`)

By default (`--phase both`) the scraper fetches, enriches (image validation + CLIP gender scoring), uploads to storage, and writes to `shopify_stores` / `shopify_products` all in one pass per store.

Use `--skip-image-upload` with `--phase both` or `--phase 2` to bypass image uploads and image sync entirely while still writing product rows.

The `--phase` flag splits this into two independent stages, which lets Phase 1 run at much higher concurrency (no image downloads or CLIP scoring) and keeps `shopify_stores` / `shopify_products` consistent — partial results are never visible to readers until a store is fully enriched.

### Phase 1 — scrape to staging

Fetches product JSON from all stores and writes raw data to intermediate tables (`shopify_stores_staging`, `shopify_products_staging`). No images are uploaded, no CLIP scoring is performed, and `shopify_stores` / `shopify_products` are not touched. Each store is marked `scraped` in `crawl_store_runs` when done.

```bash
aisley-scraper crawl-stores --phase 1 --fresh
```

Safe to run at high concurrency since it only makes lightweight JSON requests:

```
CRAWL_GLOBAL_CONCURRENCY=20
CRAWL_STORE_BATCH_SIZE=10
```

### Phase 2 — enrich and persist

Reads each staged store from the staging tables, runs image validation, CLIP gender scoring, uploads images to Supabase Storage, then writes the fully enriched rows to `shopify_stores` and `shopify_products`. Staging rows are deleted after each successful store. The run id is read automatically from `.aisley_active_run_id` (written by Phase 1), or you can pass `--run-id` explicitly.

```bash
aisley-scraper crawl-stores --phase 2
```

Resume a specific staged run explicitly:

```bash
aisley-scraper crawl-stores --phase 2 --run-id <run-id>
```

Find resumable staged run ids:

```bash
aisley-scraper diagnose-staged-runs
```

Before Phase 2 starts, the scraper verifies that no orphaned scraped images remain in storage. Phase 2 progress output includes both fraction and percent complete.

If Phase 2 prints a message like `no staged websites to process` together with `pending > 0` and `scraped=0`, that run id is not a resumable staged run. In that case, use `aisley-scraper diagnose-staged-runs` and restart Phase 2 with the correct `--run-id`.

Tune enrichment concurrency independently of the crawl:

```
IMAGE_VALIDATION_CONCURRENCY=6
PHASE2_UPLOAD_CONCURRENCY=8
POSTPROCESS_PRODUCT_CHUNK_SIZE=150
```

#### Fast validation mode (`PHASE2_FIRST_IMAGE_PRODUCT_VALIDATION_ONLY`)

By default Phase 2 runs full image validation (size, quality, sharpness, CLIP product check) on all images per product, then computes CLIP gender probabilities (`gender_probs_csv`). This can be slow under unstable CDN conditions.

Set `PHASE2_FIRST_IMAGE_PRODUCT_VALIDATION_ONLY=true` to enable a lightweight alternative:

- Only the **first image** per product is fetched and checked.
- The check is a single CLIP product-photo classifier against a configurable threshold (`PHASE2_FIRST_IMAGE_PRODUCT_PROB_THRESHOLD`, default `0.5`).
- Products whose first image is clearly not a product photo (below threshold) are dropped; products with transient fetch/timeout failures are preserved.
- Full image quality checks (blur, brightness, sharpness) and size checks are **skipped**.
- **CLIP gender scoring is skipped entirely.** Products are persisted to `shopify_products` with `gender_probs_csv = NULL`.

This mode is useful for a fast initial ingestion pass when gender scoring is not yet required.

Relevant env settings:

```
PHASE2_FIRST_IMAGE_PRODUCT_VALIDATION_ONLY=true
PHASE2_FIRST_IMAGE_PRODUCT_PROB_THRESHOLD=0.65   # drop products below this product-photo probability
```

`PHASE2_FIRST_IMAGE_PRODUCT_PROB_THRESHOLD` must be a float in `[0, 1]`. Higher values are stricter (more products dropped). Start with `0.5` and increase if non-product images are passing through.

Gender scoring can be backfilled later by re-running Phase 2 with `PHASE2_FIRST_IMAGE_PRODUCT_VALIDATION_ONLY=false`.

#### Standalone filter for existing shopify_products rows

To apply the same first-image product-photo gate to rows already in `shopify_products` (outside scraping), run:

```bash
aisley-scraper filter-shopify-products
```

This command:

- Scans existing `shopify_products` rows with at least one image.
- Validates only the first image using the same product-photo classifier.
- Uses `PHASE2_FIRST_IMAGE_PRODUCT_PROB_THRESHOLD` as the drop threshold.
- Deletes rows whose first-image product probability is below threshold.
- Preserves rows on transient fetch/timeout failures.

Useful flags:

```bash
aisley-scraper filter-shopify-products --dry-run --limit 1000 --batch-size 200
```

Disk-backed image cache for Phase 2:

```
FETCHER_DISK_CACHE_ENABLED=true
FETCHER_DISK_CACHE_DIR=.aisley_image_cache
FETCHER_DISK_CACHE_MAX_MB=2048
FETCHER_BYTE_CACHE_MAX_MB=256
```

- `FETCHER_DISK_CACHE_DIR` is defined in [src/aisley_scraper/config.py](src/aisley_scraper/config.py) with default value `.aisley_image_cache`.
- Phase 2 uses this directory as a temporary on-disk cache for fetched image bytes so reuse does not require keeping all bytes in RAM.
- `FETCHER_DISK_CACHE_MAX_MB` sets a hard cap for the on-disk cache; oldest cached files are evicted once the directory exceeds this size.
- Old cache files from prior runs are cleared automatically at crawl startup, and current-run cache files are deleted during normal batch cleanup.

### Skip-upsert optimisation

Both `--phase both` and `--phase 2` skip the DB upsert for products whose images and gender scores have not changed since the last run — only metadata fields (price, availability, etc.) require a write when images are unchanged. This significantly reduces Supabase write traffic on re-crawls of large catalogs.

### Staging tables

| Table | Purpose |
|---|---|
| `shopify_stores_staging` | Raw store profile per `(run_id, website)` |
| `shopify_products_staging` | Raw product rows per `(run_id, website, product_id)` — no `supabase_images` or `gender_probs_csv` |

Staging rows are automatically removed after a successful Phase 2 persist, so they never accumulate across runs.

## Before Running

Update these required values in `.env`:

Note: `.env.example` is only a template. Runtime values are loaded from `.env`.

- `SUPABASE_URL`: your Supabase project URL (for example `https://xxxx.supabase.co`).
- `SUPABASE_SERVICE_ROLE_KEY`: backend service role key (used for Storage uploads).
- `SUPABASE_STORAGE_BUCKET`: bucket name for uploaded product images.
- `SUPABASE_STORAGE_PATH`: folder prefix inside the bucket (for example `aisley`).
- `PERSISTENCE_TARGET`: `supabase` (default) or `local`.
- `LOCAL_OUTPUT_PATH`: local JSON output path used when `PERSISTENCE_TARGET=local`.
- `INPUT_CSV_PATH`: path to your input CSV file.
- `USER_AGENT` (optional): crawler user agent with contact info. Defaults to blank if unset.

Recommended preflight checks:

- Ensure the storage bucket exists in Supabase and is readable if you plan to use public URLs.
- Ensure your CSV has the expected URL column (default `store_url`) or update `INPUT_CSV_URL_COLUMN`.
- Optionally tune crawl parameters (`CRAWL_GLOBAL_CONCURRENCY`, `CRAWL_STORE_BATCH_SIZE`, `CRAWL_GLOBAL_QPS`) before large runs.
- Default concurrency is conservative for long-run stability: `CRAWL_GLOBAL_CONCURRENCY=15`, `CRAWL_STORE_BATCH_SIZE=3`, `IMAGE_VALIDATION_CONCURRENCY=4`.
- If the OS kills the process during heavy runs, try `CRAWL_STORE_BATCH_SIZE=1`, `CRAWL_GLOBAL_CONCURRENCY=2`, and `IMAGE_VALIDATION_CONCURRENCY=1`.
- For very large catalogs, lower `POSTPROCESS_PRODUCT_CHUNK_SIZE` (default `200`) to bound peak memory during image verification and gender enrichment batches.
- Optional: set `CRAWL_RUN_STATE_PATH` to change where the active run id is stored (default `.aisley_active_run_id`).
- Optional: set `CRAWL_STALL_LOG_INTERVAL_SEC` (default `60`) to control how often long-running crawl/persist heartbeat warnings are printed; set `0` to disable.
- Optional: set `FETCHER_DISK_CACHE_ENABLED` to enable or disable the temporary on-disk image cache used during validation/upload reuse. Default is `true`.
- Optional: set `FETCHER_DISK_CACHE_DIR` to change where temporary cached image files are written. Default is `.aisley_image_cache`.
- Optional: set `FETCHER_DISK_CACHE_MAX_MB` to cap total on-disk cached image bytes before oldest files are evicted. Default is `2048`.
- Optional: set `FETCHER_BYTE_CACHE_MAX_MB` to cap the in-memory portion of the fetch cache. Default is `256`.
- Optional: set `PHASE2_UPLOAD_CONCURRENCY` to control Stage 3 upload/sync parallelism in `--phase 2`. Default is `8`.
- Optional: set `PHASE2_FIRST_IMAGE_PRODUCT_VALIDATION_ONLY=true` to skip full image quality checks and gender scoring in Phase 2, checking only whether the first product image looks like a product photo. Products are persisted with `gender_probs_csv = NULL`. Default is `false`.
- Optional: set `PHASE2_FIRST_IMAGE_PRODUCT_PROB_THRESHOLD` to control the minimum CLIP product-photo probability required when `PHASE2_FIRST_IMAGE_PRODUCT_VALIDATION_ONLY=true`. Products below this threshold are dropped. Must be in `[0, 1]`. Default is `0.5`.
- Optional: set `HF_TOKEN` to authenticate Hugging Face model downloads (higher limits, fewer unauthenticated warnings).
- Writes use Supabase REST (`/rest/v1`) with `SUPABASE_SERVICE_ROLE_KEY`.

Local mode notes:

- Set `PERSISTENCE_TARGET=local` to skip Supabase writes and save results to `LOCAL_OUTPUT_PATH`.
- In local mode, scraped image URLs are preserved; Supabase image upload is not performed.

## Troubleshooting

## GCP Redis Pause/Resume Runbook

Use this runbook when you need to temporarily pause Redis and later restore it.

### 1) Confirm project/account context

```bash
gcloud config list --format='text(core.account,core.project,compute.region,compute.zone)'
```

Set project explicitly if needed:

```bash
gcloud config set project <PROJECT_ID>
```

### 2) Enable required APIs (one-time)

```bash
gcloud services enable container.googleapis.com --project <PROJECT_ID>
gcloud services enable redis.googleapis.com --project <PROJECT_ID>
```

### 3) If Redis runs as a Kubernetes Deployment (GKE)

Get cluster credentials:

```bash
gcloud container clusters get-credentials <CLUSTER_NAME> --zone <ZONE> --project <PROJECT_ID>
# or regional cluster:
gcloud container clusters get-credentials <CLUSTER_NAME> --region <REGION> --project <PROJECT_ID>
```

Find Redis deployment + namespace:

```bash
kubectl get deploy -A | grep -i redis
```

Pause Redis deployment (scale to zero):

```bash
kubectl scale deployment <REDIS_DEPLOYMENT_NAME> --replicas=0 -n <NAMESPACE>
kubectl get deploy <REDIS_DEPLOYMENT_NAME> -n <NAMESPACE>
```

Restart Redis deployment (restore replicas):

```bash
kubectl scale deployment <REDIS_DEPLOYMENT_NAME> --replicas=1 -n <NAMESPACE>
kubectl rollout status deployment/<REDIS_DEPLOYMENT_NAME> -n <NAMESPACE>
```

If your deployment normally uses more than one replica, restore that original count.

### 4) If Redis runs as Memorystore (managed Redis)

List instances in region:

```bash
gcloud redis instances list --region <REGION> --project <PROJECT_ID>
```

Pause/Resume behavior for Memorystore:

- Memorystore does not support a direct "pause" state like a GKE deployment scale-to-zero.
- To stop billing/traffic you typically use maintenance or deprovision/recreate workflows (or switch clients away temporarily).
- If needed, use application-side pause (disable workers) while keeping Memorystore up.

### 5) Quick verification checklist

- GKE pause expected: deployment shows `AVAILABLE=0` and no Redis pods in namespace.
- GKE resume expected: rollout completes and Redis health checks pass.
- App checks: queue workers reconnect cleanly and timeout/error rates normalize.

### `zsh: killed aisley-scraper crawl-stores`

This usually means the OS terminated the process due to memory pressure (SIGKILL), not a Python exception.

1. Re-run with a low-memory profile:

```bash
CRAWL_STORE_BATCH_SIZE=1 CRAWL_GLOBAL_CONCURRENCY=2 IMAGE_VALIDATION_CONCURRENCY=1 aisley-scraper crawl-stores
```

2. If you want these defaults for all future runs, add them to `.env`:

```bash
CRAWL_STORE_BATCH_SIZE=1
CRAWL_GLOBAL_CONCURRENCY=2
IMAGE_VALIDATION_CONCURRENCY=1
```

3. Resume behavior:

- Running `aisley-scraper crawl-stores` (without `--fresh`) resumes from pending/failed stores in the active run id.
- Use `--fresh` only when you intentionally want to start a new run id.

### `Phase 2: no staged websites to process`

This means the selected run id has no rows left in `shopify_stores_staging`.

Common cases:

- Phase 2 already finished and cleaned up staging for that run.
- `.aisley_active_run_id` points at the wrong run id.
- The run id has only `pending` rows in `crawl_store_runs`, which means it is not a valid Phase 2 resume target.

To find the correct staged run id:

```bash
aisley-scraper diagnose-staged-runs
```

Then resume explicitly:

```bash
aisley-scraper crawl-stores --phase 2 --run-id <run-id>
```

## Requirements handled

- Store profile extraction: store name, website, instagram (online), or address (offline).
- Product extraction: item name, description, `sku`, `price_cents` (integer), `updated_at`, images, sizes/colors/brand only when explicitly present and associated with product image context.
- Product extraction also includes `gender_label` (`male` / `female` / `unisex`) only when explicitly present in scraped product data.
- Image persistence: scraped source image URLs are kept in `products.images`, and uploaded Supabase public URLs are stored in `products.supabase_images`.
- Supabase persistence with idempotent upserts.
