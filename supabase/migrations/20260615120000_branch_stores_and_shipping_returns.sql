-- Store branches: allow one shopify_stores row per (website, address) instead
-- of a single row per website, and capture each store's returns/shipping policy.

-- 1. Returns/shipping policy text + source url on the production + staging tables.
alter table public.shopify_stores
  add column if not exists shipping_returns text,
  add column if not exists shipping_returns_url text;

alter table public.shopify_stores_staging
  add column if not exists shipping_returns text,
  add column if not exists shipping_returns_url text;

-- 2. One row per branch: drop the website-unique constraint and key on
--    (website, address). NULLS NOT DISTINCT keeps online (address-less) stores
--    singular so re-runs upsert rather than duplicate them.
alter table public.shopify_stores drop constraint if exists shopify_stores_website_key;
create unique index if not exists shopify_stores_website_address_key
  on public.shopify_stores (website, address) nulls not distinct;
