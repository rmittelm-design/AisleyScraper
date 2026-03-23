alter table public.shopify_stores
  add column if not exists lat double precision,
  add column if not exists long double precision;
