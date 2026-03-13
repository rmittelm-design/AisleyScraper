# Aisley Scraper

Env-driven Shopify store scraper that ingests URLs from CSV and persists store + product data to Supabase Postgres.

## Quick start

1. Copy `.env.example` to `.env` and fill required values.
2. Install dependencies:

	```bash
	pip install -r requirements.txt
	```

	This installs image-validation dependencies used at runtime, including:
	`opencv-python-headless` (`cv2`) and `google-cloud-vision`.
3. Run `aisley-scraper ingest-stores --csv ./data/stores.csv`.
4. Run `aisley-scraper crawl-stores`.

## Before Running

Update these required values in `.env`:

- `SUPABASE_URL`: your Supabase project URL (for example `https://xxxx.supabase.co`).
- `SUPABASE_SERVICE_ROLE_KEY`: backend service role key (used for Storage uploads).
- `SUPABASE_STORAGE_BUCKET`: bucket name for uploaded product images.
- `SUPABASE_STORAGE_PATH`: folder prefix inside the bucket (for example `aisley`).
- `PERSISTENCE_TARGET`: `supabase` (default) or `local`.
- `LOCAL_OUTPUT_PATH`: local JSON output path used when `PERSISTENCE_TARGET=local`.
- `INPUT_CSV_PATH`: path to your input CSV file.
- `USER_AGENT` (optional): crawler user agent with contact info. Defaults to blank if unset.
- `IMAGE_VALIDATION_USE_GCLOUD_VISION` (optional): `true` (default) runs Google Vision NSFW checks for images; set to `false` to skip that step.

Recommended preflight checks:

- Ensure the storage bucket exists in Supabase and is readable if you plan to use public URLs.
- Ensure your CSV has the expected URL column (default `store_url`) or update `INPUT_CSV_URL_COLUMN`.
- Optionally tune crawl parameters (`CRAWL_GLOBAL_CONCURRENCY`, `CRAWL_GLOBAL_QPS`) before large runs.
- Writes use Supabase REST (`/rest/v1`) with `SUPABASE_SERVICE_ROLE_KEY`.

Local mode notes:

- Set `PERSISTENCE_TARGET=local` to skip Supabase writes and save results to `LOCAL_OUTPUT_PATH`.
- In local mode, scraped image URLs are preserved; Supabase image upload is not performed.

## Requirements handled

- Store profile extraction: store name, website, instagram (online), or address (offline).
- Product extraction: item name, description, `sku`, `price_cents` (integer), `updated_at`, images, sizes/colors/brand only when explicitly present and associated with product image context.
- Product extraction also includes `gender_label` (`male` / `female` / `unisex`) only when explicitly present in scraped product data.
- Image persistence: scraped source image URLs are kept in `products.images`, and uploaded Supabase public URLs are stored in `products.supabase_images`.
- Supabase persistence with idempotent upserts.
