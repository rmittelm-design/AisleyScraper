from __future__ import annotations

import json

import psycopg

from aisley_scraper.models import ProductRecord, StoreProfile


class Repository:
    def __init__(self, dsn: str | None = None, **connect_kwargs: str | int) -> None:
        self._dsn = dsn
        self._connect_kwargs = connect_kwargs

    def _connect(self) -> psycopg.Connection:
        if self._dsn:
            return psycopg.connect(self._dsn)
        return psycopg.connect(**self._connect_kwargs)

    def ensure_schema(self) -> None:
        ddl = """
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

                alter table shopify_stores add column if not exists scraped boolean not null default true;
        alter table shopify_products add column if not exists gender_label text;
        alter table shopify_products add column if not exists gender_probs_csv text;
        alter table shopify_products add column if not exists price_cents bigint;
                alter table shopify_products add column if not exists updated_at text;
                alter table shopify_products add column if not exists sku text;
            alter table shopify_products add column if not exists product_type text;
                alter table shopify_products add column if not exists product_url text;
                alter table shopify_products add column if not exists unavailable boolean not null default false;
                alter table shopify_products add column if not exists scraped boolean not null default true;
                alter table shopify_products drop column if exists position;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()

    def upsert_store(self, store: StoreProfile) -> int:
        sql = """
        insert into shopify_stores (website, store_name, store_type, instagram_handle, address, raw)
        values (%s, %s, %s, %s, %s, %s)
        on conflict (website) do update
          set store_name = case when shopify_stores.store_name is distinct from excluded.store_name then excluded.store_name else shopify_stores.store_name end,
              store_type = case when shopify_stores.store_type is distinct from excluded.store_type then excluded.store_type else shopify_stores.store_type end,
              instagram_handle = case when shopify_stores.instagram_handle is distinct from excluded.instagram_handle then excluded.instagram_handle else shopify_stores.instagram_handle end,
              address = case when shopify_stores.address is distinct from excluded.address then excluded.address else shopify_stores.address end,
              raw = case when shopify_stores.raw is distinct from excluded.raw then excluded.raw else shopify_stores.raw end,
              last_seen_at = now()
        returning id;
        """
        payload = {
            "website": store.website,
            "store_name": store.store_name,
            "store_type": store.store_type,
            "instagram_handle": store.instagram_handle,
            "address": store.address,
        }
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        store.website,
                        store.store_name,
                        store.store_type,
                        store.instagram_handle,
                        store.address,
                        json.dumps(payload),
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        if row is None:
            raise RuntimeError("failed to upsert store")
        return int(row[0])

    def get_product_image_state(self, store_id: int, product_id: str) -> tuple[list[str], list[str]] | None:
        sql = """
        select images, supabase_images
        from shopify_products
        where store_id = %s and product_id = %s;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (store_id, product_id))
                row = cur.fetchone()
        if row is None:
            return None
        images = list(row[0] or [])
        supabase_images = list(row[1] or [])
        return images, supabase_images

    def upsert_product(self, store_id: int, product: ProductRecord) -> None:
        sql = """
                        insert into shopify_products (store_id, product_id, product_handle, product_url, item_name, description, sku, updated_at, price_cents, images, supabase_images, gender_label, gender_probs_csv, sizes, colors, brand, product_type, unavailable)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (store_id, product_id) do update
                    set product_handle = case when shopify_products.product_handle is distinct from excluded.product_handle then excluded.product_handle else shopify_products.product_handle end,
                            product_url = case when shopify_products.product_url is distinct from excluded.product_url then excluded.product_url else shopify_products.product_url end,
                            item_name = case when shopify_products.item_name is distinct from excluded.item_name then excluded.item_name else shopify_products.item_name end,
                            description = case when shopify_products.description is distinct from excluded.description then excluded.description else shopify_products.description end,
                            sku = case when shopify_products.sku is distinct from excluded.sku then excluded.sku else shopify_products.sku end,
                            updated_at = case when shopify_products.updated_at is distinct from excluded.updated_at then excluded.updated_at else shopify_products.updated_at end,
                            price_cents = case when shopify_products.price_cents is distinct from excluded.price_cents then excluded.price_cents else shopify_products.price_cents end,
                            images = case when shopify_products.images is distinct from excluded.images then excluded.images else shopify_products.images end,
                            supabase_images = case when shopify_products.supabase_images is distinct from excluded.supabase_images then excluded.supabase_images else shopify_products.supabase_images end,
                            gender_label = case when shopify_products.gender_label is distinct from excluded.gender_label then excluded.gender_label else shopify_products.gender_label end,
                            gender_probs_csv = case when shopify_products.gender_probs_csv is distinct from excluded.gender_probs_csv then excluded.gender_probs_csv else shopify_products.gender_probs_csv end,
                            sizes = case when shopify_products.sizes is distinct from excluded.sizes then excluded.sizes else shopify_products.sizes end,
                            colors = case when shopify_products.colors is distinct from excluded.colors then excluded.colors else shopify_products.colors end,
                            brand = case when shopify_products.brand is distinct from excluded.brand then excluded.brand else shopify_products.brand end,
                            product_type = case when shopify_products.product_type is distinct from excluded.product_type then excluded.product_type else shopify_products.product_type end,
                            unavailable = case when shopify_products.unavailable is distinct from excluded.unavailable then excluded.unavailable else shopify_products.unavailable end,
              last_seen_at = now();
        """

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        store_id,
                        product.product_id,
                        product.product_handle,
                        product.product_url,
                        product.item_name,
                        product.description,
                        product.sku,
                        product.updated_at,
                        product.price_cents,
                        json.dumps(product.images),
                        json.dumps(product.supabase_images),
                        product.gender_label,
                        product.gender_probs_csv,
                        json.dumps(product.sizes),
                        json.dumps(product.colors),
                        product.brand,
                        product.product_type,
                        product.unavailable,
                    ),
                )
            conn.commit()

    def delete_product(self, store_id: int, product_id: str) -> None:
        sql = """
        delete from shopify_products
        where store_id = %s and product_id = %s;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (store_id, product_id))
            conn.commit()
