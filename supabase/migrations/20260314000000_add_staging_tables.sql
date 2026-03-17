-- Extend crawl_store_runs to support the intermediate 'scraped' status
-- (Phase 1 complete, waiting for Phase 2 enrichment).
alter table public.crawl_store_runs
  drop constraint if exists crawl_store_runs_status_check;
alter table public.crawl_store_runs
  add constraint crawl_store_runs_status_check
  check (status in ('pending', 'scraped', 'completed', 'failed'));

-- Staging: raw store profiles scraped in Phase 1 (no enrichment yet).
create table if not exists public.shopify_stores_staging (
  id               bigserial primary key,
  run_id           text not null,
  website          text not null,
  store_name       text not null,
  store_type       text not null check (store_type in ('online', 'offline')),
  instagram_handle text,
  address          text,
  raw              jsonb,
  scraped_at       timestamptz not null default now(),
  unique (run_id, website)
);

-- Staging: raw product rows scraped in Phase 1.
-- supabase_images and gender_probs_csv are intentionally absent;
-- they are computed and written directly to shopify_products in Phase 2.
create table if not exists public.shopify_products_staging (
  id             bigserial primary key,
  run_id         text not null,
  website        text not null,
  product_id     text not null,
  product_handle text,
  product_url    text,
  item_name      text not null,
  description    text,
  sku            text,
  updated_at     text,
  price_cents    bigint,
  images         jsonb not null,
  gender_label   text,
  sizes          jsonb not null default '[]'::jsonb,
  colors         jsonb not null default '[]'::jsonb,
  brand          text,
  product_type   text,
  unavailable    boolean not null default false,
  scraped_at     timestamptz not null default now(),
  unique (run_id, website, product_id)
);

create index if not exists idx_products_staging_run_website
  on public.shopify_products_staging(run_id, website);
