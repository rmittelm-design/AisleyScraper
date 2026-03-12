create table if not exists shopify_stores (
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

create table if not exists shopify_products (
  id bigserial primary key,
  store_id bigint not null references shopify_stores(id) on delete cascade,
  product_id text not null,
  product_handle text,
  product_url text,
  item_name text not null,
  description text,
  sku text,
  updated_at text,
  position integer,
  price_cents bigint,
  images jsonb not null,
  supabase_images jsonb not null default '[]'::jsonb,
  gender_label text,
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

create index if not exists idx_shopify_products_store_id on shopify_products(store_id);
