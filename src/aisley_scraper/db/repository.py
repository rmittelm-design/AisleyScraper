from __future__ import annotations

from urllib.parse import urlparse

import psycopg
from psycopg.types.json import Jsonb

from aisley_scraper.config import Settings
from aisley_scraper.db.pg_ipv4 import force_ipv4_conninfo
from aisley_scraper.models import ProductRecord, StoreProfile


def _domain_key(url: str) -> str:
    """Scheme- and www-insensitive domain key (http://www.x.com -> x.com)."""
    netloc = urlparse((url or "").strip()).netloc.strip().lower()
    if not netloc:
        netloc = (url or "").strip().lower().split("//")[-1].split("/")[0]
    return netloc[4:] if netloc.startswith("www.") else netloc


def canonical_website(url: str) -> str:
    """One canonical website string per domain: https + bare domain (no www,
    scheme, path or trailing slash). All rows for a domain converge to this so
    http/https/www variants are treated as the same store."""
    domain = _domain_key(url)
    return f"https://{domain}" if domain else (url or "").strip()


class Repository:
    """Direct Postgres (psycopg) repository.

    All store/product table reads and writes go through a raw Postgres
    connection (no Supabase REST/PostgREST). A fresh connection is opened per
    call so the repository is safe to use from the worker threads the CLI
    spawns via ``asyncio.to_thread``.
    """

    def __init__(self, dsn: "str | Settings | None" = None, *, connect_timeout: int = 10) -> None:
        if isinstance(dsn, Settings):
            resolved = (dsn.database_url or "").strip()
            if not resolved:
                raise RuntimeError(
                    "DATABASE_URL is required for direct-DB persistence. Set it in .env."
                )
            self._dsn: str | None = resolved
            self._connect_timeout = max(1, int(dsn.db_connect_timeout_sec))
        else:
            self._dsn = dsn
            self._connect_timeout = max(1, int(connect_timeout))

    def _connect(self) -> psycopg.Connection:
        if not self._dsn:
            raise RuntimeError("Repository requires a DATABASE_URL dsn")
        return psycopg.connect(
            force_ipv4_conninfo(self._dsn),
            prepare_threshold=None,
            connect_timeout=self._connect_timeout,
            # Cross-region (residential -> Supabase pooler) connections can freeze
            # mid-scan: the socket stays "open" but no data flows and the read
            # blocks forever (0% CPU). TCP keepalives + tcp_user_timeout make the
            # OS surface a frozen/dead path as an OperationalError within ~30s, so
            # callers can reconnect and resume instead of wedging. These fire only
            # on an unresponsive path, never on a slow-but-live query.
            keepalives=1,
            keepalives_idle=15,
            keepalives_interval=5,
            keepalives_count=3,
            tcp_user_timeout=30000,
        )

    # ── Schema ────────────────────────────────────────────────────────────────
    def ensure_schema(self) -> None:
        ddl = """
        create extension if not exists pgcrypto;

        create table if not exists shopify_stores (
          id bigserial primary key,
          website text not null,
          store_name text not null,
          store_type text not null check (store_type in ('online','offline')),
          instagram_handle text,
          address text,
          lat double precision,
          long double precision,
          shipping_returns text,
          shipping_returns_url text,
          scraped boolean not null default true,
          raw jsonb,
          first_seen_at timestamptz default now(),
          last_seen_at timestamptz default now()
        );

        create table if not exists shopify_products (
          id bigserial primary key,
          store_id bigint not null references shopify_stores(id) on delete cascade,
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

        -- New / additive columns (idempotent).
        alter table shopify_stores add column if not exists scraped boolean not null default true;
        alter table shopify_stores add column if not exists lat double precision;
        alter table shopify_stores add column if not exists long double precision;
        alter table shopify_stores add column if not exists shipping_returns text;
        alter table shopify_stores add column if not exists shipping_returns_url text;

        -- Allow one row per (website, address) so each store branch is its own
        -- row. NULLS NOT DISTINCT keeps online (address-less) stores singular.
        alter table shopify_stores drop constraint if exists shopify_stores_website_key;
        create unique index if not exists shopify_stores_website_address_key
          on shopify_stores (website, address) nulls not distinct;

        alter table shopify_products add column if not exists gender_label text;
        alter table shopify_products add column if not exists gender_probs_csv text;
        alter table shopify_products add column if not exists price_cents bigint;
        alter table shopify_products add column if not exists updated_at text;
        alter table shopify_products add column if not exists sku text;
        alter table shopify_products add column if not exists product_type text;
        alter table shopify_products add column if not exists product_url text;
        alter table shopify_products add column if not exists unavailable boolean not null default false;
        alter table shopify_products add column if not exists scraped boolean not null default true;

        do $$
        declare
            item_uuid_udt text;
        begin
            select c.udt_name
            into item_uuid_udt
            from information_schema.columns as c
            where c.table_schema = current_schema()
                and c.table_name = 'shopify_products'
                and c.column_name = 'item_uuid';

            if item_uuid_udt is null then
                alter table shopify_products add column item_uuid uuid;
            end if;

            alter table shopify_products alter column item_uuid set default gen_random_uuid();
            update shopify_products set item_uuid = gen_random_uuid() where item_uuid is null;
            alter table shopify_products alter column item_uuid set not null;
        end $$;

        -- Two-phase pipeline + run-tracking tables.
        create table if not exists shopify_stores_staging (
          id               bigserial primary key,
          run_id           text not null,
          website          text not null,
          store_name       text not null,
          store_type       text not null check (store_type in ('online','offline')),
          instagram_handle text,
          address          text,
          lat              double precision,
          long             double precision,
          shipping_returns text,
          shipping_returns_url text,
          raw              jsonb,
          scraped_at       timestamptz not null default now(),
          unique (run_id, website)
        );
        alter table shopify_stores_staging add column if not exists shipping_returns text;
        alter table shopify_stores_staging add column if not exists shipping_returns_url text;

        create table if not exists shopify_products_staging (
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
          on shopify_products_staging(run_id, website);

        create table if not exists crawl_store_runs (
          id bigserial primary key,
          run_id text not null,
          website text not null,
          status text not null check (status in ('pending','scraped','completed','failed')),
          attempt_count integer not null default 0,
          last_attempt_at timestamptz,
          error_message text,
          created_at timestamptz not null default now(),
          updated_at timestamptz not null default now(),
          unique (run_id, website)
        );
        create index if not exists idx_crawl_store_runs_run_status on crawl_store_runs(run_id, status);
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()

    # ── Stores ──────────────────────────────────────────────────────────────
    def upsert_store(self, store: StoreProfile) -> int:
        # Persist one canonical website per domain and reuse an existing row for
        # the same (domain, address) — even under a different scheme/www string —
        # so http/https/www variants collapse onto one row and its id (and its
        # products) is preserved instead of spawning a duplicate.
        canon = canonical_website(store.website)
        domain = _domain_key(store.website)
        insert_sql = """
        insert into shopify_stores
            (website, store_name, store_type, instagram_handle, address, lat, long,
             shipping_returns, shipping_returns_url, raw)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (website, address) do update
          set store_name = excluded.store_name,
              store_type = excluded.store_type,
              instagram_handle = coalesce(excluded.instagram_handle, shopify_stores.instagram_handle),
              lat = coalesce(excluded.lat, shopify_stores.lat),
              long = coalesce(excluded.long, shopify_stores.long),
              shipping_returns = coalesce(excluded.shipping_returns, shopify_stores.shipping_returns),
              shipping_returns_url = coalesce(excluded.shipping_returns_url, shopify_stores.shipping_returns_url),
              raw = excluded.raw,
              scraped = true,
              last_seen_at = now()
        returning id;
        """
        update_sql = """
        update shopify_stores set
            website = %s,
            store_name = %s,
            store_type = %s,
            instagram_handle = coalesce(%s, instagram_handle),
            address = %s,
            lat = coalesce(%s, lat),
            long = coalesce(%s, long),
            shipping_returns = coalesce(%s, shipping_returns),
            shipping_returns_url = coalesce(%s, shipping_returns_url),
            raw = %s,
            scraped = true,
            last_seen_at = now()
        where id = %s
        returning id;
        """
        raw = {
            "website": canon,
            "store_name": store.store_name,
            "store_type": store.store_type,
            "instagram_handle": store.instagram_handle,
            "address": store.address,
            "lat": store.lat,
            "long": store.long,
            "shipping_returns_url": store.shipping_returns_url,
        }
        values = (
            store.store_name,
            store.store_type,
            store.instagram_handle,
            store.address,
            store.lat,
            store.long,
            store.shipping_returns,
            store.shipping_returns_url,
            Jsonb(raw),
        )
        want_address = store.address or None
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "select id, website, address from shopify_stores where website ilike %s;",
                    (f"%{domain}%",),
                )
                target_id = None
                for cid, cweb, caddr in cur.fetchall():
                    if _domain_key(cweb) == domain and (caddr or None) == want_address:
                        target_id = int(cid)
                        break
                if target_id is not None:
                    cur.execute(update_sql, (canon, *values, target_id))
                else:
                    cur.execute(insert_sql, (canon, *values))
                row = cur.fetchone()
            conn.commit()
        if row is None:
            raise RuntimeError("failed to upsert store")
        return int(row[0])

    def sync_store_branches(self, website: str, branches: list[StoreProfile]) -> list[int]:
        """Reconcile a website's ``shopify_stores`` rows to exactly ``branches``,
        reusing existing row ids so product links survive.

        Without this, re-running accumulates duplicates: a row written before a
        store had a branch address (``address`` NULL) lingers next to the new
        addressed row. We instead map branches onto existing rows and update in
        place — each branch first claims an existing row with the SAME address;
        any remaining branch reuses a leftover existing row (oldest id first) by
        updating its address; a branch with no row left to reuse is inserted.
        Existing rows not claimed by any branch are deleted (their products
        cascade via the ON DELETE CASCADE FK).

        Returns the resulting row ids in ``branches`` order (index 0 = primary /
        product-target branch). Reusing the primary's existing id keeps its
        ``shopify_products`` attached. Runs in one transaction.
        """
        if not branches:
            return []

        # Existing rows are matched by the www/scheme-insensitive DOMAIN (not the
        # exact website string), and every reused/inserted row is written with the
        # one canonical website. So prior rows under http/https/www variants of the
        # same domain are found, reused (id preserved), and converged — not
        # duplicated.
        canon = canonical_website(website)
        domain = _domain_key(website)

        insert_sql = """
        insert into shopify_stores
            (website, store_name, store_type, instagram_handle, address, lat, long,
             shipping_returns, shipping_returns_url, raw)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        returning id;
        """
        update_sql = """
        update shopify_stores set
            website = %s,
            store_name = %s,
            store_type = %s,
            instagram_handle = coalesce(%s, instagram_handle),
            address = %s,
            lat = coalesce(%s, lat),
            long = coalesce(%s, long),
            shipping_returns = coalesce(%s, shipping_returns),
            shipping_returns_url = coalesce(%s, shipping_returns_url),
            raw = %s,
            scraped = true,
            last_seen_at = now()
        where id = %s;
        """

        def _raw(store: StoreProfile) -> Jsonb:
            return Jsonb(
                {
                    "website": canon,
                    "store_name": store.store_name,
                    "store_type": store.store_type,
                    "instagram_handle": store.instagram_handle,
                    "address": store.address,
                    "lat": store.lat,
                    "long": store.long,
                    "shipping_returns_url": store.shipping_returns_url,
                }
            )

        result_ids: list[int] = [0] * len(branches)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "select id, website, address from shopify_stores where website ilike %s order by id;",
                    (f"%{domain}%",),
                )
                existing = [
                    (int(r[0]), r[2]) for r in cur.fetchall() if _domain_key(r[1]) == domain
                ]

                by_address: dict[object, list[int]] = {}
                for sid, addr in existing:
                    by_address.setdefault(addr, []).append(sid)

                used: set[int] = set()
                assigned: list[int | None] = [None] * len(branches)

                # Pass 1: each branch claims an existing row with the same address.
                for i, branch in enumerate(branches):
                    pool = by_address.get(branch.address)
                    if pool:
                        sid = pool.pop(0)
                        assigned[i] = sid
                        used.add(sid)

                # Pass 2: remaining branches reuse leftover rows (oldest id first).
                leftover = [sid for sid, _addr in existing if sid not in used]
                for i in range(len(branches)):
                    if assigned[i] is None and leftover:
                        sid = leftover.pop(0)
                        assigned[i] = sid
                        used.add(sid)

                # Apply: update reused rows in place, insert genuinely-new branches.
                for i, branch in enumerate(branches):
                    sid = assigned[i]
                    if sid is not None:
                        cur.execute(
                            update_sql,
                            (
                                canon,
                                branch.store_name,
                                branch.store_type,
                                branch.instagram_handle,
                                branch.address,
                                branch.lat,
                                branch.long,
                                branch.shipping_returns,
                                branch.shipping_returns_url,
                                _raw(branch),
                                sid,
                            ),
                        )
                        result_ids[i] = int(sid)
                    else:
                        cur.execute(
                            insert_sql,
                            (
                                canon,
                                branch.store_name,
                                branch.store_type,
                                branch.instagram_handle,
                                branch.address,
                                branch.lat,
                                branch.long,
                                branch.shipping_returns,
                                branch.shipping_returns_url,
                                _raw(branch),
                            ),
                        )
                        row = cur.fetchone()
                        if row is None:
                            raise RuntimeError("failed to insert store branch")
                        result_ids[i] = int(row[0])

                # Before deleting unclaimed rows, move any products they hold onto
                # the primary branch (index 0) so the ON DELETE CASCADE can never
                # drop a product — products that are already on the primary are
                # left to cascade away as duplicates. This keeps every store's
                # products on its single primary id.
                stale = [sid for sid, _addr in existing if sid not in used]
                if stale:
                    primary_id = result_ids[0]
                    cur.execute(
                        "update shopify_products set store_id = %s "
                        "where store_id = any(%s) and product_id not in "
                        "(select product_id from shopify_products where store_id = %s);",
                        (primary_id, stale, primary_id),
                    )
                    cur.execute("delete from shopify_stores where id = any(%s);", (stale,))
            conn.commit()
        return result_ids

    def list_all_store_websites(self) -> list[str]:
        # website is no longer unique (one row per branch); de-dupe in SQL.
        sql = "select distinct website from shopify_stores order by website asc;"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        return [row[0] for row in rows if isinstance(row[0], str) and row[0]]

    def list_all_store_profiles(self) -> list[StoreProfile]:
        # One representative profile per website (lowest id), used only to learn
        # which websites already exist and their primary address.
        sql = """
        select distinct on (website)
               website, store_name, store_type, instagram_handle, address, lat, long,
               shipping_returns, shipping_returns_url
        from shopify_stores
        order by website asc, id asc;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        profiles: list[StoreProfile] = []
        for row in rows:
            website, store_name, store_type = row[0], row[1], row[2]
            if not (isinstance(website, str) and website):
                continue
            if not (isinstance(store_name, str) and store_name):
                continue
            if not (isinstance(store_type, str) and store_type):
                continue
            profiles.append(
                StoreProfile(
                    website=website,
                    store_name=store_name,
                    store_type=store_type,
                    instagram_handle=row[3] if isinstance(row[3], str) else None,
                    address=row[4] if isinstance(row[4], str) else None,
                    lat=float(row[5]) if row[5] is not None else None,
                    long=float(row[6]) if row[6] is not None else None,
                    shipping_returns=row[7] if isinstance(row[7], str) else None,
                    shipping_returns_url=row[8] if isinstance(row[8], str) else None,
                )
            )
        return profiles

    def update_store_policies(
        self, website: str, shipping_returns: str | None, shipping_returns_url: str | None
    ) -> int:
        """Set shipping_returns/url on EVERY row of this website's domain.

        Branch rows share one store's policy, so all of a domain's rows are
        written together. Matched by www/scheme-insensitive domain (like
        ``sync_store_branches``) so http/https/www variants are all updated.
        Returns the number of rows written.
        """
        domain = _domain_key(website)
        if not domain:
            return 0
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "select id, website from shopify_stores where website ilike %s;",
                    (f"%{domain}%",),
                )
                ids = [int(sid) for sid, site in cur.fetchall() if _domain_key(site) == domain]
                if not ids:
                    return 0
                cur.execute(
                    "update shopify_stores set shipping_returns = %s, "
                    "shipping_returns_url = %s where id = any(%s);",
                    (shipping_returns, shipping_returns_url, ids),
                )
                written = cur.rowcount
            conn.commit()
        return int(written)

    def delete_stores_not_in_domains(self, keep_domains: set[str]) -> tuple[int, list[str]]:
        """Delete every ``shopify_stores`` row whose www/scheme-insensitive domain
        is NOT in ``keep_domains`` (the TSV is the source of truth). Products
        cascade via the FK. Returns ``(rows_deleted, removed_domains)``.

        No-op if ``keep_domains`` is empty — refuses to wipe the table against an
        empty/missing TSV set.
        """
        if not keep_domains:
            return 0, []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("select id, website from shopify_stores;")
                rows = cur.fetchall()
                to_delete: list[int] = []
                removed_domains: set[str] = set()
                for sid, website in rows:
                    domain = _domain_key(website)
                    if domain not in keep_domains:
                        to_delete.append(int(sid))
                        removed_domains.add(domain)
                if to_delete:
                    cur.execute("delete from shopify_stores where id = any(%s);", (to_delete,))
            conn.commit()
        return len(to_delete), sorted(removed_domains)

    # ── Crawl run tracking ────────────────────────────────────────────────────
    def initialize_crawl_run(self, *, run_id: str, websites: list[str]) -> None:
        if not websites:
            return
        sql = """
        insert into crawl_store_runs (run_id, website, status, attempt_count, created_at, updated_at)
        values (%s, %s, 'pending', 0, now(), now())
        on conflict (run_id, website) do nothing;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, [(run_id, website) for website in websites])
            conn.commit()

    def list_all_run_store_websites(self, *, run_id: str, statuses: list[str]) -> list[str]:
        if not statuses:
            return []
        sql = """
        select website from crawl_store_runs
        where run_id = %s and status = any(%s)
        order by id asc;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (run_id, list(statuses)))
                rows = cur.fetchall()
        return [row[0] for row in rows if isinstance(row[0], str) and row[0]]

    def mark_run_store_status(
        self, *, run_id: str, website: str, status: str, error_message: str | None = None
    ) -> None:
        sql = """
        insert into crawl_store_runs
            (run_id, website, status, attempt_count, last_attempt_at, error_message, created_at, updated_at)
        values (%s, %s, %s, 1, now(), %s, now(), now())
        on conflict (run_id, website) do update set
          status = excluded.status,
          attempt_count = case
              when crawl_store_runs.status = 'pending' then crawl_store_runs.attempt_count + 1
              else greatest(1, crawl_store_runs.attempt_count)
          end,
          last_attempt_at = now(),
          error_message = coalesce(excluded.error_message, crawl_store_runs.error_message),
          updated_at = now();
        """
        trimmed = error_message[:2000] if error_message else None
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (run_id, website, status, trimmed))
            conn.commit()

    def count_run_store_status(self, *, run_id: str, status: str) -> int:
        sql = "select count(*) from crawl_store_runs where run_id = %s and status = %s;"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (run_id, status))
                row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    # ── Products ──────────────────────────────────────────────────────────────
    def get_product_image_state(
        self, store_id: int, product_id: str
    ) -> tuple[list[str], list[str], str | None] | None:
        sql = """
        select images, supabase_images, gender_probs_csv
        from shopify_products
        where store_id = %s and product_id = %s;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (store_id, product_id))
                row = cur.fetchone()
        if row is None:
            return None
        gender_probs_csv = row[2] if isinstance(row[2], str) else None
        return list(row[0] or []), list(row[1] or []), gender_probs_csv

    def get_product_image_states(
        self, store_id: int, product_ids: list[str]
    ) -> dict[str, tuple[list[str], list[str], str | None]]:
        if not product_ids:
            return {}
        sql = """
        select product_id, images, supabase_images, gender_probs_csv
        from shopify_products
        where store_id = %s and product_id = any(%s);
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (store_id, list(product_ids)))
                rows = cur.fetchall()
        out: dict[str, tuple[list[str], list[str], str | None]] = {}
        for row in rows:
            product_id = row[0]
            if not isinstance(product_id, str):
                continue
            out[product_id] = (
                list(row[1] or []),
                list(row[2] or []),
                row[3] if isinstance(row[3], str) else None,
            )
        return out

    def upsert_product(self, store_id: int, product: ProductRecord) -> None:
        self.upsert_products_batch(store_id, [product])

    def upsert_products_batch(self, store_id: int, products: list[ProductRecord]) -> None:
        if not products:
            return
        sql = """
        insert into shopify_products
            (store_id, product_id, product_handle, product_url, item_name, description, sku,
             updated_at, price_cents, images, supabase_images, gender_label, gender_probs_csv,
             sizes, colors, brand, product_type, unavailable)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (store_id, product_id) do update set
            product_handle = excluded.product_handle,
            product_url = excluded.product_url,
            item_name = excluded.item_name,
            description = excluded.description,
            sku = excluded.sku,
            updated_at = excluded.updated_at,
            price_cents = excluded.price_cents,
            images = excluded.images,
            supabase_images = excluded.supabase_images,
            gender_label = coalesce(excluded.gender_label, shopify_products.gender_label),
            gender_probs_csv = coalesce(excluded.gender_probs_csv, shopify_products.gender_probs_csv),
            sizes = excluded.sizes,
            colors = excluded.colors,
            brand = excluded.brand,
            product_type = excluded.product_type,
            unavailable = excluded.unavailable,
            scraped = true,
            last_seen_at = now();
        """
        params = [
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
                Jsonb(list(product.images)),
                Jsonb(list(product.supabase_images)),
                product.gender_label,
                product.gender_probs_csv,
                Jsonb(list(product.sizes)),
                Jsonb(list(product.colors)),
                product.brand,
                product.product_type,
                product.unavailable,
            )
            for product in products
        ]
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, params)
            conn.commit()

    def update_products_metadata(
        self,
        website: str,
        products: list[ProductRecord],
        *,
        dry_run: bool = False,
        mark_removed: bool = True,
        min_coverage: float = 0.5,
    ) -> dict[str, int | str | None]:
        """Refresh scraped metadata on EXISTING shopify_products rows for a domain.

        For each scraped product already in production, overwrites only metadata
        sourced from the fresh scrape (name, description, price, availability,
        sizes, colors, brand, product_type, sku, handle, url) plus last_seen_at.
        Deliberately does NOT touch images / supabase_images (preserving the
        CLIP-validated set) or gender_* columns, and never INSERTs — a genuinely
        new product needs image validation via the full crawl.

        When ``mark_removed`` is set, production products NOT present in the scrape
        (delisted from the store's catalog) are flagged ``unavailable = true`` —
        BUT only if the scrape looks complete. The scraped/filtered set is a
        superset of production, so a healthy scrape re-finds nearly all of
        production; if it re-finds fewer than ``min_coverage`` of the store's
        products (or returns nothing at all), the scrape is treated as
        partial/bot-blocked and the removal marking is skipped, so a bad fetch
        can't wrongly flag a whole catalog. A wrongly-flagged item self-corrects
        on the next successful run (its metadata refresh restores availability).

        Matched by domain (all branch rows) + product_id, like sync_store_branches.
        Returns a summary dict.
        """
        result: dict[str, int | str | None] = {
            "production": 0,
            "scraped": len(products),
            "updated": 0,
            "missing": 0,
            "marked_unavailable": 0,
            "mark_skipped_reason": None,
        }
        domain = _domain_key(website)
        if not domain:
            return result
        by_id: dict[str, ProductRecord] = {}
        for product in products:
            by_id[product.product_id] = product  # last wins; scrape already deduped
        scraped_ids = set(by_id)

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "select id, website from shopify_stores where website ilike %s;",
                    (f"%{domain}%",),
                )
                store_ids = [
                    int(sid) for sid, site in cur.fetchall() if _domain_key(site) == domain
                ]
                if not store_ids:
                    return result

                cur.execute(
                    "select product_id, unavailable from shopify_products "
                    "where store_id = any(%s);",
                    (store_ids,),
                )
                prod_rows = cur.fetchall()
                production_ids = {row[0] for row in prod_rows}
                available_ids = {row[0] for row in prod_rows if not row[1]}
                result["production"] = len(production_ids)

                matched = [pid for pid in production_ids if pid in scraped_ids]
                missing = [pid for pid in production_ids if pid not in scraped_ids]
                result["updated"] = len(matched)
                result["missing"] = len(missing)

                # Decide whether the "removed -> unavailable" marking is trustworthy.
                to_flag: list[str] = []
                if mark_removed and missing:
                    if not products:
                        result["mark_skipped_reason"] = "scrape returned no products"
                    elif production_ids and (len(matched) / len(production_ids)) < min_coverage:
                        pct = len(matched) / len(production_ids)
                        result["mark_skipped_reason"] = (
                            f"scrape covered {pct:.0%} of catalog "
                            f"(< {min_coverage:.0%}); likely partial/blocked"
                        )
                    else:
                        to_flag = [pid for pid in missing if pid in available_ids]
                        result["marked_unavailable"] = len(to_flag)

                if dry_run:
                    return result

                if matched:
                    meta_sql = """
                    update shopify_products set
                        product_handle = %s,
                        product_url = %s,
                        item_name = %s,
                        description = %s,
                        sku = %s,
                        updated_at = %s,
                        price_cents = %s,
                        sizes = %s,
                        colors = %s,
                        brand = %s,
                        product_type = %s,
                        unavailable = %s,
                        last_seen_at = now()
                    where store_id = any(%s) and product_id = %s;
                    """
                    params = [
                        (
                            p.product_handle,
                            p.product_url,
                            p.item_name,
                            p.description,
                            p.sku,
                            p.updated_at,
                            p.price_cents,
                            Jsonb(list(p.sizes)),
                            Jsonb(list(p.colors)),
                            p.brand,
                            p.product_type,
                            p.unavailable,
                            store_ids,
                            pid,
                        )
                        for pid, p in ((pid, by_id[pid]) for pid in matched)
                    ]
                    cur.executemany(meta_sql, params)

                if to_flag:
                    # Delisted products: only flip availability. last_seen_at is
                    # left as-is (they were NOT seen this run — a useful signal).
                    cur.execute(
                        "update shopify_products set unavailable = true "
                        "where store_id = any(%s) and product_id = any(%s);",
                        (store_ids, to_flag),
                    )
            conn.commit()
        return result

    def delete_product(self, store_id: int, product_id: str) -> None:
        sql = "delete from shopify_products where store_id = %s and product_id = %s;"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (store_id, product_id))
            conn.commit()

    def delete_products_by_ids(self, ids: list[int]) -> int:
        """Delete many products in one statement by their primary-key ids."""
        if not ids:
            return 0
        sql = "delete from shopify_products where id = any(%s);"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, ([int(i) for i in ids],))
                deleted = cur.rowcount
            conn.commit()
        return deleted

    def delete_item_embeddings_batch(self, item_uuids: list[str]) -> int:
        if not item_uuids:
            return 0
        sql = "delete from item_embeddings where item_uuid = any(%s::uuid[]);"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, ([str(u) for u in item_uuids],))
                deleted = cur.rowcount
            conn.commit()
        return deleted

    def iter_linked_supabase_image_paths(self, public_prefix: str) -> set[str]:
        """Return every storage object path referenced by shopify_products.supabase_images.

        Used by the orphan-storage preflight to know which uploaded objects are
        still linked. Paths are returned relative to ``public_prefix`` (the
        Supabase public Storage url prefix).
        """
        linked: set[str] = set()
        sql = "select supabase_images from shopify_products where supabase_images <> '[]'::jsonb;"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                while True:
                    rows = cur.fetchmany(1000)
                    if not rows:
                        break
                    for (urls,) in rows:
                        if not isinstance(urls, list):
                            continue
                        for url in urls:
                            if isinstance(url, str) and url.startswith(public_prefix):
                                linked.add(url[len(public_prefix):])
        return linked

    def list_products_for_integrity_scan(self, *, limit: int, offset: int) -> list[dict[str, object]]:
        sql = """
        select store_id, product_id, images, supabase_images, gender_label, gender_probs_csv
        from shopify_products
        where images <> '[]'::jsonb
        order by id asc
        limit %s offset %s;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (max(1, limit), max(0, offset)))
                rows = cur.fetchall()
        out: list[dict[str, object]] = []
        for row in rows:
            out.append(
                {
                    "store_id": row[0],
                    "product_id": row[1],
                    "images": list(row[2] or []),
                    "supabase_images": list(row[3] or []),
                    "gender_label": row[4],
                    "gender_probs_csv": row[5],
                }
            )
        return out

    def list_products_for_first_image_validation_scan(
        self, *, limit: int, after_id: int | None = None
    ) -> list[dict[str, object]]:
        if after_id is None:
            sql = """
            select id, store_id, product_id, item_uuid, images
            from shopify_products
            where images <> '[]'::jsonb
              and exists (
                  select 1 from item_embeddings
                  where item_embeddings.item_uuid = shopify_products.item_uuid
              )
            order by id asc
            limit %s;
            """
            params: tuple[object, ...] = (max(1, limit),)
        else:
            sql = """
            select id, store_id, product_id, item_uuid, images
            from shopify_products
            where images <> '[]'::jsonb
              and id > %s
              and exists (
                  select 1 from item_embeddings
                  where item_embeddings.item_uuid = shopify_products.item_uuid
              )
            order by id asc
            limit %s;
            """
            params = (max(0, after_id), max(1, limit))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        out: list[dict[str, object]] = []
        for row in rows:
            out.append(
                {
                    "id": row[0],
                    "store_id": row[1],
                    "product_id": row[2],
                    "item_uuid": str(row[3]) if row[3] is not None else None,
                    "images": list(row[4] or []),
                }
            )
        return out

    def list_products_for_category_scan(
        self, *, limit: int, after_id: int | None = None
    ) -> list[dict[str, object]]:
        """Page through saved products with the fields needed to re-apply the
        non-apparel / cosmetics keyword filter (``prune-nonfashion``)."""
        cols = (
            "id, store_id, product_id, item_name, product_url, product_handle, "
            "product_type, item_uuid, supabase_images"
        )
        if after_id is None:
            sql = f"select {cols} from shopify_products order by id asc limit %s;"
            params: tuple[object, ...] = (max(1, limit),)
        else:
            sql = f"select {cols} from shopify_products where id > %s order by id asc limit %s;"
            params = (max(0, after_id), max(1, limit))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        out: list[dict[str, object]] = []
        for row in rows:
            out.append(
                {
                    "id": row[0],
                    "store_id": row[1],
                    "product_id": row[2],
                    "item_name": row[3],
                    "product_url": row[4],
                    "product_handle": row[5],
                    "product_type": row[6],
                    "item_uuid": str(row[7]) if row[7] is not None else None,
                    "supabase_images": list(row[8] or []),
                }
            )
        return out

    def delete_item_embeddings_for_item_uuid(self, item_uuid: str) -> None:
        sql = "delete from item_embeddings where item_uuid = %s;"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (item_uuid,))
            conn.commit()

    # ── Staging (two-phase pipeline) ───────────────────────────────────────────
    def upsert_staged_store(self, run_id: str, store: StoreProfile) -> None:
        sql = """
        insert into shopify_stores_staging
            (run_id, website, store_name, store_type, instagram_handle, address, lat, long,
             shipping_returns, shipping_returns_url, raw, scraped_at)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        on conflict (run_id, website) do update set
            store_name = excluded.store_name,
            store_type = excluded.store_type,
            instagram_handle = excluded.instagram_handle,
            address = excluded.address,
            lat = excluded.lat,
            long = excluded.long,
            shipping_returns = excluded.shipping_returns,
            shipping_returns_url = excluded.shipping_returns_url,
            raw = excluded.raw,
            scraped_at = now();
        """
        raw = {
            "website": store.website,
            "store_name": store.store_name,
            "store_type": store.store_type,
            "instagram_handle": store.instagram_handle,
            "address": store.address,
            "lat": store.lat,
            "long": store.long,
            "shipping_returns_url": store.shipping_returns_url,
        }
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        run_id,
                        store.website,
                        store.store_name,
                        store.store_type,
                        store.instagram_handle,
                        store.address,
                        store.lat,
                        store.long,
                        store.shipping_returns,
                        store.shipping_returns_url,
                        Jsonb(raw),
                    ),
                )
            conn.commit()

    def upsert_staged_products(
        self, run_id: str, website: str, products: list[ProductRecord]
    ) -> None:
        if not products:
            return
        sql = """
        insert into shopify_products_staging
            (run_id, website, product_id, product_handle, product_url, item_name, description,
             sku, updated_at, price_cents, images, gender_label, sizes, colors, brand,
             product_type, unavailable, scraped_at)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        on conflict (run_id, website, product_id) do update set
            product_handle = excluded.product_handle,
            product_url = excluded.product_url,
            item_name = excluded.item_name,
            description = excluded.description,
            sku = excluded.sku,
            updated_at = excluded.updated_at,
            price_cents = excluded.price_cents,
            images = excluded.images,
            gender_label = excluded.gender_label,
            sizes = excluded.sizes,
            colors = excluded.colors,
            brand = excluded.brand,
            product_type = excluded.product_type,
            unavailable = excluded.unavailable,
            scraped_at = now();
        """
        params = [
            (
                run_id,
                website,
                p.product_id,
                p.product_handle,
                p.product_url,
                p.item_name,
                p.description,
                p.sku,
                p.updated_at,
                p.price_cents,
                Jsonb(list(p.images)),
                p.gender_label,
                Jsonb(list(p.sizes)),
                Jsonb(list(p.colors)),
                p.brand,
                p.product_type,
                p.unavailable,
            )
            for p in products
        ]
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, params)
            conn.commit()

    def get_staged_store(self, run_id: str, website: str) -> StoreProfile | None:
        sql = """
        select store_name, store_type, instagram_handle, address, lat, long,
               shipping_returns, shipping_returns_url
        from shopify_stores_staging
        where run_id = %s and website = %s
        limit 1;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (run_id, website))
                row = cur.fetchone()
        if row is None:
            return None
        return StoreProfile(
            website=website,
            store_name=row[0] or "",
            store_type=row[1] or "online",
            instagram_handle=row[2] if isinstance(row[2], str) else None,
            address=row[3] if isinstance(row[3], str) else None,
            lat=float(row[4]) if row[4] is not None else None,
            long=float(row[5]) if row[5] is not None else None,
            shipping_returns=row[6] if isinstance(row[6], str) else None,
            shipping_returns_url=row[7] if isinstance(row[7], str) else None,
        )

    def get_staged_products(self, run_id: str, website: str) -> list[ProductRecord]:
        sql = """
        select product_id, product_handle, product_url, item_name, description, sku,
               updated_at, price_cents, images, gender_label, sizes, colors, brand,
               product_type, unavailable
        from shopify_products_staging
        where run_id = %s and website = %s
        order by id asc;
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (run_id, website))
                rows = cur.fetchall()
        out: list[ProductRecord] = []
        for row in rows:
            out.append(
                ProductRecord(
                    product_id=row[0],
                    product_handle=row[1],
                    product_url=row[2],
                    item_name=row[3] or "",
                    description=row[4],
                    sku=row[5],
                    updated_at=row[6],
                    price_cents=row[7],
                    images=list(row[8] or []),
                    gender_label=row[9],
                    sizes=list(row[10] or []),
                    colors=list(row[11] or []),
                    brand=row[12],
                    product_type=row[13],
                    unavailable=bool(row[14]),
                )
            )
        return out

    def list_all_staged_run_websites(self, *, run_id: str) -> list[str]:
        sql = "select website from shopify_stores_staging where run_id = %s order by id asc;"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (run_id,))
                rows = cur.fetchall()
        return [row[0] for row in rows if isinstance(row[0], str) and row[0]]

    def delete_staged_run_website(self, run_id: str, website: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "delete from shopify_products_staging where run_id = %s and website = %s;",
                    (run_id, website),
                )
                cur.execute(
                    "delete from shopify_stores_staging where run_id = %s and website = %s;",
                    (run_id, website),
                )
            conn.commit()

    def purge_run(self, run_id: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("delete from shopify_products_staging where run_id = %s;", (run_id,))
                cur.execute("delete from shopify_stores_staging where run_id = %s;", (run_id,))
                cur.execute("delete from crawl_store_runs where run_id = %s;", (run_id,))
            conn.commit()

    def purge_other_runs(self, keep_run_id: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "delete from shopify_products_staging where run_id <> %s;", (keep_run_id,)
                )
                cur.execute(
                    "delete from shopify_stores_staging where run_id <> %s;", (keep_run_id,)
                )
                cur.execute("delete from crawl_store_runs where run_id <> %s;", (keep_run_id,))
            conn.commit()
