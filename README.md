# Aisley Scraper

Env-driven Shopify store scraper that ingests store URLs from a folder of TSV files and persists store + product data **directly to Postgres** via `psycopg` (`DATABASE_URL`). Supabase is used only for image storage (Storage bucket), not for table reads/writes.

Key behaviors:

- **Direct-DB**: all `shopify_stores` / `shopify_products` (and staging / run-tracking) reads and writes use a raw Postgres connection — no Supabase REST/PostgREST.
- **One folder of TSV seeds**: every `*.tsv` in `INPUT_TSV_DIR` (default `./data/stores`) is parsed and merged by url.
- **Store branches**: each TSV row is `url <tab> store_name <tab> addr1 <tab> addr2 …`. Products are scraped once; one `shopify_stores` row is written per branch address (same url + name, each branch's own address + geocoded lat/long), and scraped products link to the **first** branch (third column). Online/address-less stores collapse to a single row.
- **Non-apparel filtering**: scraped items are dropped when they match (a) kids/children terms — kid/child/boy/girl/toddler/baby/infant/newborn (aggressive substring on name/url/handle), or (b) non-apparel categories — furniture, boxes, pet/petwear, gift cards/cards, puzzles, sundries, bar goodies, candles, lighters, catchalls, books/coffee-table books, home décor, picture frames, serveware, soaps, diffusers, towels/tea towels, drinkware, beauty (perfume/serum/balm/etc.) (whole-word/phrase match on name/url/handle/product_type, so apparel like "spring", "cardigan", "petite" is preserved). **Jewelry and watches are intentionally kept.**
- **Shipping/returns**: both a **return/refund policy** and a **shipping/delivery policy** are sought independently for each store (canonical Shopify `/policies/*`, common `/pages/*`, combined shipping+returns pages, then matching homepage links). Each is captured with its own length budget so one can't crowd out the other, and stored in `shopify_stores.shipping_returns` (labelled `RETURNS:` / `SHIPPING:` sections) and `shopify_stores.shipping_returns_url` (the page URL(s), `|`-joined). Keeping these complete and accurate is a defined standard with a repeatable maintenance loop — see [Keeping shipping/returns policies at standard](docs/shipping-returns-policy-standard.md).

## Quick start

Run all commands from the project root directory:

```bash
cd /Users/ronimittelman/Desktop/Projects/Projects/AisleyScraper
```

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
3. Put your store TSV files in `INPUT_TSV_DIR` (default `./data/stores/`). Each row is
   `url <tab> store_name <tab> addr1 <tab> addr2 …` (a bare-url row with no name/address is fine — the name is taken from the scraped homepage). The crawl reads **all** `*.tsv` in that folder and merges them by url.

	The standalone `ingest-stores --csv <path>` command still loads a single TSV file (the `--csv` flag name is kept for backward compatibility).
4. Run the full scrape/update: **`aisley-scraper crawl-stores --mark-removed`** — this is the one command to scrape and update everything (see [Running a full scrape / update](#running-a-full-scrape--update-the-command-to-run) just below for exactly what it does).

If you are upgrading an existing deployment, apply migrations in order before crawling:

- `supabase/migrations/20260313120000_add_crawl_store_runs.sql`
- `supabase/migrations/20260314000000_add_staging_tables.sql`
- `supabase/migrations/20260323160000_add_store_lat_long.sql`
- `supabase/migrations/20260323170000_add_lat_long_to_stores_staging.sql`
- `supabase/migrations/20260615120000_branch_stores_and_shipping_returns.sql` — drops the `website` unique constraint in favor of `unique(website, address)` and adds `shipping_returns` / `shipping_returns_url`.

`crawl-stores` also calls `ensure_schema()` at startup, which applies these additive/idempotent changes automatically — but applying the migration explicitly is recommended for production.

Restart behavior for `crawl-stores`:

- Crawl source is DB-first: existing `shopify_stores` are processed first, then unseen stores from the TSV folder are appended.
- Use `--csv <path>` with `crawl-stores` to run Phase 1 from that single TSV only (no DB-first merge, no folder scan).
- A run id is persisted in `.aisley_active_run_id` by default, so restarts resume from pending/failed stores in the same run.
- Use `--fresh` to start a new run id.
- Use `--run-id <id>` to explicitly resume a specific run.
- For two-phase resume, `--phase 2` now requires an existing staged run id and will not create a new run implicitly.

Orphaned-storage preflight runs only for `--phase both`.

## Running a full scrape / update — the command to run

To scrape and update **everything** in one pass, run:

```bash
aisley-scraper crawl-stores --mark-removed
```

This is the canonical command for a full catalog scrape + update. In a single default `--phase both` pass it:

- **scrapes new stores** — any store in `shopify_stores` or in the `./data/stores/*.tsv` seeds that isn't fully scraped yet is fetched and inserted (DB-first, then unseen TSV stores are appended);
- **scrapes new items for existing stores** — every product in each store's `products.json` is upserted (new products inserted, existing ones updated);
- **validates + stores product images** — `--phase both` runs the fashion/CLIP image validation and persists images (this is the default; it is *not* image-free);
- **marks removed items unavailable** — `--mark-removed` flags products that have disappeared from a store's catalog as `unavailable=true`, guarded by `--min-coverage` (default `0.5`) so a partial/bot-blocked scrape can't wrongly flag a whole catalog;
- runs **two-lane concurrency** — small stores concurrently first (data usable sooner), then the largest store(s) last at lower concurrency (gentler on Shopify's CDN). See `_split_lanes` / `run_crawl` in `src/aisley_scraper/cli.py`.

Notes:

- **Resume vs. new cycle (important):** without `--fresh`, the command **resumes** the active run — it processes only `pending`/`failed` stores and **never silently restarts from scratch**. The startup log always tells you which it is: `RESUMING run_id=… : N done, M pending …` or `STARTING NEW run_id=… : all N pending …`. When a run finishes, its run-id pointer (`.aisley_active_run_id`) is **retained**, so re-running without `--fresh` on a completed run is a safe no-op ("0 to process"). To begin a **brand-new full cycle**, add `--fresh` — it mints a new run id and purges the previous run's tracking (products are safe; run bookkeeping is not). **Never `--fresh` while a crawl is live**; a *completed* run is not live, so `--fresh` is safe once it has finished.
- Canary first if you like: `aisley-scraper crawl-stores --mark-removed --limit 20`.
- **Do not** use `refresh-products` for a full update — it is an *update-only* path: it refreshes metadata on existing products and marks removed ones unavailable, but it **skips new products and never scrapes new/empty stores** (and it runs sequentially, not two-lane). Use it only for a fast price/availability sweep of the existing catalog. See [Refresh metadata only (`refresh-products`)](#refresh-metadata-only-refresh-products) below.

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

By default (`--phase both`) the scraper fetches, validates product images (fashion/non-fashion classifier), and writes to `shopify_stores` / `shopify_products` all in one pass per store. (CLIP gender scoring has been removed; `gender_probs_csv` is always `NULL`.)

Image uploads are enabled by default in `--phase both` and `--phase 2`; use `--skip-image-upload` to disable uploads for that run.

The `--phase` flag splits this into two independent stages, which lets Phase 1 run at much higher concurrency (no image downloads or CLIP scoring) and keeps `shopify_stores` / `shopify_products` consistent — partial results are never visible to readers until a store is fully enriched.

### Phase 1 — scrape to staging

Fetches product JSON from all stores and writes raw data to intermediate tables (`shopify_stores_staging`, `shopify_products_staging`). No images are uploaded, no CLIP scoring is performed, and `shopify_stores` / `shopify_products` are not touched. Each store is marked `scraped` in `crawl_store_runs` when done.

Important: Phase 1 relies on each store exposing a public Shopify JSON endpoint at `/products.json`.
If a site returns `404` or `410` for `/products.json` on the **first** page, that store is marked `failed` for the run and is not staged.
This can happen even when the homepage URL opens normally in a browser.

Pagination is resilient past page 1. If a store returns a `4xx`/non-JSON response at a **deep** page (some shops answer `400` past their last page instead of an empty list), or repeats the same products on every page, Phase 1 treats that as end-of-catalog: it stops paginating and keeps everything scraped so far instead of failing the whole store. Only a failure on page 1 fails the store.

Broken/expired TLS certificates: set `CRAWL_SSL_VERIFY=false` to scrape stores whose certs fail verification (applies to the httpx clients, the curl fallback `-k`, and curl_cffi). Default is `true` (secure) — only disable it for a run that specifically targets known broken-cert stores, since it skips MITM protection for every fetch in that run.

```bash
aisley-scraper crawl-stores --phase 1 --fresh
```

Safe to run at high concurrency since it only makes lightweight JSON requests:

```
CRAWL_GLOBAL_CONCURRENCY=20
CRAWL_STORE_BATCH_SIZE=10
```

### Phase 2 — enrich and persist

Reads each staged store from the staging tables, runs image validation (fashion/non-fashion classifier), then writes the enriched rows to `shopify_stores` and `shopify_products`. Staging rows are deleted after each successful store. The run id is read automatically from `.aisley_active_run_id` (written by Phase 1), or you can pass `--run-id` explicitly.

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

Phase 2 progress output includes both fraction and percent complete.

If Phase 2 prints a message like `no staged websites to process` together with `pending > 0` and `scraped=0`, that run id is not a resumable staged run. In that case, use `aisley-scraper diagnose-staged-runs` and restart Phase 2 with the correct `--run-id`.

Tune enrichment concurrency independently of the crawl:

```
IMAGE_VALIDATION_CONCURRENCY=6
PHASE2_UPLOAD_CONCURRENCY=8
PHASE2_DB_UPSERT_BATCH_SIZE=500
POSTPROCESS_PRODUCT_CHUNK_SIZE=150
```

#### Images validated per product (`PHASE2_MAX_IMAGES_PER_PRODUCT`)

In full-enrich mode (`PHASE2_FIRST_IMAGE_PRODUCT_VALIDATION_ONLY=false`), Phase 2 validates up to `PHASE2_MAX_IMAGES_PER_PRODUCT` lead images per product (default `5`) and keeps the product if **any** of them passes the classifier. Set `PHASE2_MAX_IMAGES_PER_PRODUCT=1` to validate only the first image (fastest; the trade-off is that a valid item with a non-product lead image — e.g. a size chart first — is dropped).

Either way, the **full image gallery is restored** before the product is written to production. Validation only decides whether to keep the product; it never trims a kept product's `images`.

A typical full-enrich run (non-default `.env` values made explicit):

```bash
PHASE2_FIRST_IMAGE_PRODUCT_VALIDATION_ONLY=false \
PHASE2_MAX_IMAGES_PER_PRODUCT=1 \
FETCHER_DISK_CACHE_ENABLED=false \
CRAWL_SSL_VERIFY=false \
aisley-scraper crawl-stores --phase 2 --run-id <run-id>
```

#### Fast validation mode (`PHASE2_FIRST_IMAGE_PRODUCT_VALIDATION_ONLY`)

By default Phase 2 runs full image validation (size, quality, sharpness, CLIP product check) on all images per product. This can be slow under unstable CDN conditions.

Set `PHASE2_FIRST_IMAGE_PRODUCT_VALIDATION_ONLY=true` to enable a lightweight alternative:

- The first **K** images per product are checked (`PRODUCT_VALIDATION_MAX_IMAGES`, default `3`); the item is kept if **any** scores as a product photo (max over images).
- The check is the CLIP/SigLIP product-photo classifier against a configurable threshold (`PHASE2_FIRST_IMAGE_PRODUCT_PROB_THRESHOLD`).
- Products whose sampled images are all clearly not product photos (below threshold) are dropped; products with transient fetch/timeout failures are preserved.
- A lightweight size + blur gate is applied; full brightness/contrast checks are skipped.

Relevant env settings:

```
PHASE2_FIRST_IMAGE_PRODUCT_VALIDATION_ONLY=true
PHASE2_FIRST_IMAGE_PRODUCT_PROB_THRESHOLD=0.90   # stricter: drop products below this product-photo probability
```

`PHASE2_FIRST_IMAGE_PRODUCT_PROB_THRESHOLD` must be a float in `[0, 1]`. Higher values are stricter (more products dropped). For this project's current tuning, `0.90` is recommended.

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

### Skip-validation optimisation

On a re-crawl, products whose image URLs are unchanged since the last run skip the expensive step — image download and CLIP validation (`_needs_enrichment`) — **not** the DB write. Their scraped metadata (price, availability, description, sizes, etc.) is still upserted, so metadata stays current while unchanged-image products cost no image or CLIP work. On the product upsert only `gender_label` / `gender_probs_csv` are preserved (via `coalesce`); every other column is overwritten from the fresh scrape.

### Refresh metadata only (`refresh-products`)

To refresh metadata on **existing** `shopify_products` rows without any staging, image download, or CLIP, use:

```bash
aisley-scraper refresh-products --domain <domain>   # one store; omit --domain for all
aisley-scraper refresh-products --domain <domain> --dry-run   # scrape + report, no writes
```

It re-scrapes each store's `products.json` (the same lightweight fetch as Phase 1) and updates only scraped metadata — `item_name`, `description`, `price_cents`, `unavailable`, `sizes`, `colors`, `brand`, `product_type`, `sku`, `product_handle`, `product_url`, `last_seen_at` — **in place**. It deliberately does **not** touch `images` / `supabase_images` (preserving the CLIP-validated set) or gender labels, and **never inserts**: a product not already in production is skipped, because a genuinely new item needs the full crawl's image validation. Use it to keep prices/availability/descriptions current far more cheaply than a full re-crawl.

**Delisted products.** A production product that is **absent from the scrape** (removed from the store's catalog) is flagged `unavailable = true`. (A sold-out-but-still-listed item is handled separately — Shopify keeps it in `products.json` with `available:false`.) Two guards prevent a bad scrape from wrongly flagging a live catalog:

1. **Completeness** — removal marking runs only when the scrape reached the *true end* of the catalog. If pagination stopped because of the per-store item cap, a fetch error, or an anomalous `200` that lacks a products array (a WAF/block), the scrape is treated as incomplete and marking is **skipped** — the un-scraped tail is not actually gone.
2. **Coverage** — even on a complete scrape, if it re-found less than `--min-coverage` (default `0.5`) of the store's existing products, marking is skipped.

A wrongly-flagged item also self-corrects on the next successful scrape (its refresh restores availability).

```bash
aisley-scraper refresh-products                      # all stores
aisley-scraper refresh-products --dry-run            # all stores, report only
aisley-scraper refresh-products --no-mark-removed    # metadata only; don't flag delisted
aisley-scraper refresh-products --min-coverage 0.8   # stricter guard before flagging
```

**Full re-scrape + mark removed in one pass.** To *also add newly-listed products* (which need image validation) while flagging delisted ones, run the full crawl with `--mark-removed` — same two guards apply:

```bash
aisley-scraper crawl-stores --mark-removed                  # add new + update + flag delisted
aisley-scraper crawl-stores --mark-removed --min-coverage 0.8
```

`--mark-removed` applies to `--phase both` (the live scrape); it is ignored for `--phase 2` because staged data does not record whether the Phase 1 scrape reached the end of the catalog.

### Staging tables

| Table | Purpose |
|---|---|
| `shopify_stores_staging` | Raw store profile per `(run_id, website)`, including `shipping_returns` / `shipping_returns_url` |
| `shopify_products_staging` | Raw product rows per `(run_id, website, product_id)` — no `supabase_images` or `gender_probs_csv` |

Branch fan-out happens at Phase 2 (the production write), not in staging: staging holds one row per `(run_id, website)`, and Phase 2 reads the branch addresses from the TSV folder to create one `shopify_stores` row per branch.

Staging rows are automatically removed after a successful Phase 2 persist, so they never accumulate across runs.

## Before Running

Update these required values in `.env`:

Note: `.env.example` is only a template. Runtime values are loaded from `.env`.

- `DATABASE_URL`: **required.** Direct Postgres connection string used for all table reads/writes (e.g. `postgresql://postgres:PASSWORD@db.<ref>.supabase.co:5432/postgres?sslmode=require`). The password is embedded in this string — there is no separate password variable. For Supabase, copy it from Dashboard → Settings → Database (use the connection pooler host if the direct host is unreachable). The host is auto-pinned to IPv4 at connect time.
- `DB_CONNECT_TIMEOUT_SEC` (optional): psycopg connect timeout in seconds. Default `10`.
- `INPUT_TSV_DIR`: folder of per-store TSV seed files (all `*.tsv` merged by url). Default `./data/stores`.
- `SUPABASE_URL`: your Supabase project URL — used **only** for Storage (image uploads), not table I/O.
- `SUPABASE_SERVICE_ROLE_KEY`: service role key — used **only** for Storage uploads (this is *not* the database password).
- `SUPABASE_STORAGE_BUCKET`: bucket name for uploaded product images.
- `SUPABASE_STORAGE_PATH`: folder prefix inside the bucket (for example `aisley`).
- `PERSISTENCE_TARGET`: `supabase` (default; direct-DB + Storage) or `local`.
- `LOCAL_OUTPUT_PATH`: local JSON output path used when `PERSISTENCE_TARGET=local`.
- `INPUT_CSV_PATH` (optional): single TSV file used only by `ingest-stores` and the `--csv` override; the crawl uses `INPUT_TSV_DIR`.
- `USER_AGENT` (optional): crawler user agent with contact info. If unset, the crawler uses a browser-like default user agent.

Recommended preflight checks:

- Ensure `DATABASE_URL` points at a reachable Postgres and that the schema migrations are applied (or rely on the automatic `ensure_schema()` at crawl startup).
- Ensure the storage bucket exists in Supabase and is readable if you plan to use public URLs.
- Place your store TSV files in `INPUT_TSV_DIR`. Each row's first column is the store url; an optional second column is the store name; columns 3+ are branch addresses (one `shopify_stores` row is created per branch).
- Optionally tune crawl parameters (`CRAWL_GLOBAL_CONCURRENCY`, `CRAWL_STORE_BATCH_SIZE`, `CRAWL_GLOBAL_QPS`) before large runs.
- Default concurrency is conservative for long-run stability: `CRAWL_GLOBAL_CONCURRENCY=15`, `CRAWL_STORE_BATCH_SIZE=3`, `IMAGE_VALIDATION_CONCURRENCY=4`.
- If the OS kills the process during heavy runs, try `CRAWL_STORE_BATCH_SIZE=1`, `CRAWL_GLOBAL_CONCURRENCY=2`, and `IMAGE_VALIDATION_CONCURRENCY=1`.
- For very large catalogs, lower `POSTPROCESS_PRODUCT_CHUNK_SIZE` (default `200`) to bound peak memory during image verification batches.
- Optional: set `CRAWL_RUN_STATE_PATH` to change where the active run id is stored (default `.aisley_active_run_id`).
- Optional: set `CRAWL_STALL_LOG_INTERVAL_SEC` (default `60`) to control how often long-running crawl/persist heartbeat warnings are printed; set `0` to disable.
- Optional: set `FETCHER_DISK_CACHE_ENABLED` to enable or disable the temporary on-disk image cache used during validation/upload reuse. Default is `true`.
- Optional: set `FETCHER_DISK_CACHE_DIR` to change where temporary cached image files are written. Default is `.aisley_image_cache`.
- Optional: set `FETCHER_DISK_CACHE_MAX_MB` to cap total on-disk cached image bytes before oldest files are evicted. Default is `2048`.
- Optional: set `FETCHER_BYTE_CACHE_MAX_MB` to cap the in-memory portion of the fetch cache. Default is `256`.
- Optional: set `PHASE2_UPLOAD_CONCURRENCY` to control Stage 3 upload/sync parallelism in `--phase 2`. Default is `8`.
- Optional: set `PHASE2_DB_UPSERT_BATCH_SIZE` to control how many products are upserted per batched Postgres `executemany` in Phase 2 Stage 3. Default is `500`.
- Optional: set `PHASE2_FIRST_IMAGE_PRODUCT_VALIDATION_ONLY=true` to skip full image quality checks in Phase 2, checking only whether the first few product images look like a product photo (kept if any does). Default is `false`. Tune `PRODUCT_VALIDATION_MAX_IMAGES` (default `3`) for how many lead images are sampled.
- Optional: set `PHASE2_FIRST_IMAGE_PRODUCT_PROB_THRESHOLD` to control the minimum CLIP product-photo probability required when `PHASE2_FIRST_IMAGE_PRODUCT_VALIDATION_ONLY=true`. Products below this threshold are dropped. Must be in `[0, 1]`. Default is `0.5` (current recommended runtime value: `0.90`).
- Optional: set `HF_TOKEN` to authenticate Hugging Face model downloads (higher limits, fewer unauthenticated warnings).
- Table reads/writes go directly to Postgres (`DATABASE_URL`) via `psycopg`; `SUPABASE_*` is used only for image Storage.

Local mode notes:

- Set `PERSISTENCE_TARGET=local` to skip Supabase writes and save results to `LOCAL_OUTPUT_PATH`.
- In local mode, scraped image URLs are preserved; Supabase image upload is not performed.

## Keeping shipping/returns policies at standard

A store's `shipping_returns` is "at standard" when it covers both returns and shipping (a `RETURNS:` and a `SHIPPING:` section, or a single `SHIPPING & RETURNS:` section from a combined page), is a real policy (not navigation/legal boilerplate — see `stored_policy_is_weak()` in `src/aisley_scraper/extract/policies.py`), and states the concrete facets the store publishes (rate table, free-shipping threshold, return window, restocking fee, final-sale exclusions, international/customs terms, carriers, contact). Established stores average ~2,400 characters; a policy far below ~1,200 is almost always missing facets and should be re-audited.

Maintain it with a three-tier loop, cheapest first:

1. **Automated recapture** — `aisley-scraper recapture-policies --only-broken` re-fetches and rewrites NULL/boilerplate rows (`--dry-run`, `--domain <d>`, `--limit N`, `--clear-unfixable`). The extractor merges rate tables across candidate pages so costs aren't dropped.
2. **LLM audit + verify sweep** — a two-stage `pipeline(stores, audit, verify)` workflow that compares each stored policy against the store's **live** pages and adds only facets an independent verifier can re-confirm (the verify stage is mandatory — it strips hallucinated facts).
3. **Manual capture** — for bot-blocked (HTTP 429) or JS-rendered help centers, transcribe from the live URLs/screenshots into the same format.

**Never overwrite a good DB value with a rate-limited (thin) re-fetch** — a re-verify "FAIL" means "couldn't re-fetch," not "the stored value is wrong": check the current DB value first, dry-run, and only write when the new text is richer.

Full runbook, facet checklist, and acceptance checks: [docs/shipping-returns-policy-standard.md](docs/shipping-returns-policy-standard.md).

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

### Phase 2 stalls / `ECHECKOUTTIMEOUT` / `get_staged_store … froze (>25s)`

On a long Phase 2 run against a **cross-region** Supabase pooler, reads can intermittently freeze: the pooler stops responding and connections can't be checked out (`psycopg.errors.InternalError_: (ECHECKOUTTIMEOUT) unable to check out connection from the pool`), and stores fail with `Phase 2 stage 1 failed … get_staged_store[…] froze (>25s)`. The process can sit at 0% CPU, stalled. This is a pooler/network issue, not a data problem — finished stores are already committed.

Mitigations:

- **Re-run.** Phase 2 is idempotent and per-store: staging rows are deleted as each store completes. Stop the stalled run and re-run the same `--phase 2 --run-id <id>`; it processes only the stores still in staging, chipping away across runs. `stores still staged = 0` means done.
- **Don't poll the DB concurrently.** A separate `while true` count loop competes for the same connection pool and can starve the run (and itself hit `ECHECKOUTTIMEOUT`). Watch the run's log instead.
- **Reduce pool pressure** with a smaller `PHASE2_STORE_BATCH_SIZE` (fewer concurrent DB connections).
- **Avoid the cross-region pooler for big runs:** run from a machine in the DB's region, or use the direct (non-pooler) connection. Writes don't freeze — only reads do.

Tip: when piping output through `tee`, Python block-buffers stdout, so the progress `print()`s (store count, `validation chunk X/Y`) lag behind the `stderr` log lines. Prepend `PYTHONUNBUFFERED=1` and/or send logs to a file (`… 2> run.log`) to see clean, live progress.

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

### `Phase 1 complete: X/46 stores staged successfully`

When this number is below the TSV row count, check run status counts:

```bash
python - <<'PY'
from aisley_scraper.config import get_settings
from aisley_scraper.db.repository import Repository

repo = Repository(get_settings())
run_id = "<run-id>"
for status in ["pending", "scraped", "failed", "completed"]:
	print(status, repo.count_run_store_status(run_id=run_id, status=status))
PY
```

Typical interpretation:

- `scraped = N` means `N` stores were staged successfully.
- `failed = M` means `M` stores failed before staging (often `/products.json` returns `404` or `410`).
- `pending = 0` means the run is complete for all input stores.

If you need all rows to stage, remove known non-`/products.json` domains from the TSV or add a separate HTML/JSON-LD fallback extractor.

## Requirements handled

- Store profile extraction: store name (from the TSV, or the scraped homepage `<title>` when the TSV omits it), website, instagram (online), or address (offline).
- Store branches: one `shopify_stores` row per branch address from the TSV (same url + name, each with its own address and geocoded `lat`/`long`); products are scraped once and linked to the first branch.
- Shipping/returns capture: both the return/refund policy and the shipping/delivery policy are located independently (Shopify `/policies/*`, common `/pages/*`, combined pages, homepage links) and stored in `shopify_stores.shipping_returns` (labelled `RETURNS:`/`SHIPPING:` sections, each length-capped) and `shopify_stores.shipping_returns_url` (`|`-joined source urls). The completeness/accuracy standard and its maintenance loop are documented in [docs/shipping-returns-policy-standard.md](docs/shipping-returns-policy-standard.md).
- Product extraction: item name, description, `sku`, `price_cents` (integer), `updated_at`, images, sizes/colors/brand only when explicitly present and associated with product image context.
- Product extraction also includes `gender_label` (`male` / `female` / `unisex`) only when explicitly present in scraped product data.
- Kids/children exclusion: items whose name/url/handle contain kid/child/boy/girl/toddler/baby/infant/newborn are dropped (aggressive substring match).
- Non-apparel category exclusion: items matching furniture, boxes, pet/petwear, gift cards/cards, puzzles, sundries, bar goodies, candles, lighters, catchalls, books, home décor, picture frames, serveware, soaps, diffusers, towels, drinkware, and beauty/personal-care (perfume, serum, balm, shampoo, etc.) are dropped via whole-word/phrase matching on name/url/handle/product_type — so apparel terms that merely contain those substrings (cardigan, spring, petite, herringbone, box pleat) are preserved. Jewelry and watches are intentionally NOT filtered (kept).
- Image persistence: scraped source image URLs are kept in `products.images`. `products.supabase_images` is populated during Phase 2 unless `--skip-image-upload` is used.
- Direct Postgres persistence (`psycopg`, `DATABASE_URL`) with idempotent upserts; Supabase used only for image Storage.
