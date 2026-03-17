create extension if not exists pgcrypto;

create table if not exists public.shopify_stores (
  id bigserial primary key,
  website text unique not null,
  store_name text not null,
  store_type text not null check (store_type in ('online','offline')),
  instagram_handle text,
  address text,
  scraped boolean not null default true,
  raw jsonb,
  first_seen_at timestamptz default now(),
  last_seen_at timestamptz default now()
);

create table if not exists public.shopify_products (
  id bigserial primary key,
  store_id bigint not null references public.shopify_stores(id) on delete cascade,
  product_id text not null,
  item_uuid uuid not null default gen_random_uuid(),
  product_handle text,
  product_url text,
  item_name text not null,
  description text,
  sku text,
  updated_at text,
  price_cents bigint,
  images jsonb not null,
  supabase_images jsonb not null default '[]'::jsonb,
  gender_label text,
  gender_probs_csv text,
  sizes jsonb not null default '[]'::jsonb,
  colors jsonb not null default '[]'::jsonb,
  brand text,
  product_type text,
  unavailable boolean not null default false,
  scraped boolean not null default true,
  first_seen_at timestamptz default now(),
  last_seen_at timestamptz default now(),
  unique (store_id, product_id)
);

create index if not exists idx_shopify_products_store_id on public.shopify_products(store_id);
create index if not exists idx_shopify_products_item_uuid on public.shopify_products(item_uuid);
create index if not exists idx_shopify_products_item_uuid_unavailable on public.shopify_products(item_uuid, unavailable);


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

create table if not exists public.crawl_store_runs (
  id bigserial primary key,
  run_id text not null,
  website text not null,
  status text not null check (status in ('pending', 'scraped', 'completed', 'failed')),
  attempt_count integer not null default 0,
  last_attempt_at timestamptz,
  error_message text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (run_id, website)
);

create index if not exists idx_crawl_store_runs_run_status on public.crawl_store_runs(run_id, status);
create index if not exists idx_crawl_store_runs_website on public.crawl_store_runs(website);
