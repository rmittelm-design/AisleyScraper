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

If you are upgrading an existing deployment, apply the new migration before crawling:

- `supabase/migrations/20260313120000_add_crawl_store_runs.sql`

Restart behavior for `crawl-stores`:

- Crawl source is DB-first: existing `shopify_stores` are processed first, then unseen CSV stores are appended.
- A run id is persisted in `.aisley_active_run_id` by default, so restarts resume from pending/failed stores in the same run.
- Use `--fresh` to start a new run id.
- Use `--run-id <id>` to explicitly resume a specific run.

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

Limit stores in a run (useful for canary runs):

```bash
aisley-scraper crawl-stores --fresh --limit 50
```

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
- Optional: set `CRAWL_RUN_STATE_PATH` to change where the active run id is stored (default `.aisley_active_run_id`).
- Optional: set `CRAWL_STALL_LOG_INTERVAL_SEC` (default `60`) to control how often long-running crawl/persist heartbeat warnings are printed; set `0` to disable.
- Optional: set `HF_TOKEN` to authenticate Hugging Face model downloads (higher limits, fewer unauthenticated warnings).
- Writes use Supabase REST (`/rest/v1`) with `SUPABASE_SERVICE_ROLE_KEY`.

Local mode notes:

- Set `PERSISTENCE_TARGET=local` to skip Supabase writes and save results to `LOCAL_OUTPUT_PATH`.
- In local mode, scraped image URLs are preserved; Supabase image upload is not performed.

## Troubleshooting

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

## Requirements handled

- Store profile extraction: store name, website, instagram (online), or address (offline).
- Product extraction: item name, description, `sku`, `price_cents` (integer), `updated_at`, images, sizes/colors/brand only when explicitly present and associated with product image context.
- Product extraction also includes `gender_label` (`male` / `female` / `unisex`) only when explicitly present in scraped product data.
- Image persistence: scraped source image URLs are kept in `products.images`, and uploaded Supabase public URLs are stored in `products.supabase_images`.
- Supabase persistence with idempotent upserts.
