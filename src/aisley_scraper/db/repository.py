from __future__ import annotations

import json

import psycopg

from aisley_scraper.models import ProductRecord, StoreProfile


class Repository:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self._dsn)

    def ensure_schema(self) -> None:
        ddl = """
        create table if not exists stores (
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

        create table if not exists products (
          id bigserial primary key,
          store_id bigint not null references stores(id) on delete cascade,
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
                    scraped boolean not null default true,
          first_seen_at timestamptz default now(),
          last_seen_at timestamptz default now(),
          unique (store_id, product_id)
        );

                alter table stores add column if not exists scraped boolean not null default true;
        alter table products add column if not exists gender_label text;
        alter table products add column if not exists price_cents bigint;
                alter table products add column if not exists updated_at text;
                alter table products add column if not exists position integer;
                alter table products add column if not exists sku text;
            alter table products add column if not exists product_type text;
                alter table products add column if not exists product_url text;
                alter table products add column if not exists scraped boolean not null default true;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()

    def upsert_store(self, store: StoreProfile) -> int:
        sql = """
        insert into stores (website, store_name, store_type, instagram_handle, address, raw)
        values (%s, %s, %s, %s, %s, %s)
        on conflict (website) do update
          set store_name = excluded.store_name,
              store_type = excluded.store_type,
              instagram_handle = excluded.instagram_handle,
              address = excluded.address,
              raw = excluded.raw,
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

    def upsert_product(self, store_id: int, product: ProductRecord) -> None:
        sql = """
                insert into products (store_id, product_id, product_handle, product_url, item_name, description, sku, updated_at, position, price_cents, images, supabase_images, gender_label, sizes, colors, brand, product_type)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (store_id, product_id) do update
          set product_handle = excluded.product_handle,
              product_url = excluded.product_url,
              item_name = excluded.item_name,
              description = excluded.description,
              sku = excluded.sku,
              updated_at = excluded.updated_at,
              position = excluded.position,
              price_cents = excluded.price_cents,
              images = excluded.images,
              supabase_images = excluded.supabase_images,
              gender_label = excluded.gender_label,
              sizes = excluded.sizes,
              colors = excluded.colors,
              brand = excluded.brand,
              product_type = excluded.product_type,
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
                        product.position,
                        product.price_cents,
                        json.dumps(product.images),
                        json.dumps(product.supabase_images),
                        product.gender_label,
                        json.dumps(product.sizes),
                        json.dumps(product.colors),
                        product.brand,
                        product.product_type,
                    ),
                )
            conn.commit()
