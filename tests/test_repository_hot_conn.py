"""Tests for the per-thread reused connection on the hot upsert path.

The crawl persists tens of thousands of products per large store; opening a fresh
psycopg connection (TCP+TLS to the pooler) per product was the dominant cost.
upsert_products_batch now reuses a per-thread connection and drops it on error so
the next call reconnects (callers retry, so a stale connection recovers).
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from aisley_scraper.db.repository import Repository


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def executemany(self, sql, params):
        self._conn.executes += 1
        if self._conn.fail:
            raise RuntimeError("simulated broken connection")


class _FakeConn:
    def __init__(self):
        self.closed = False
        self.commits = 0
        self.executes = 0
        self.fail = False

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def close(self):
        self.closed = True


def _fake_product():
    return SimpleNamespace(
        product_id="p1",
        product_handle="h",
        product_url="u",
        item_name="n",
        description="d",
        sku="s",
        updated_at=None,
        price_cents=100,
        images=[],
        supabase_images=[],
        gender_label=None,
        gender_probs_csv=None,
        sizes=[],
        colors=[],
        brand=None,
        product_type=None,
        unavailable=False,
    )


def _repo_with_fake_connect(monkeypatch):
    repo = Repository("postgresql://fake/db")
    conns: list[_FakeConn] = []

    def fake_connect():
        c = _FakeConn()
        conns.append(c)
        return c

    monkeypatch.setattr(repo, "_connect", fake_connect)
    return repo, conns


def test_hot_connection_is_reused_across_calls(monkeypatch):
    repo, conns = _repo_with_fake_connect(monkeypatch)
    p = _fake_product()
    repo.upsert_products_batch(1, [p])
    repo.upsert_products_batch(1, [p])
    repo.upsert_products_batch(1, [p])
    assert len(conns) == 1  # a single connection served all three upserts
    assert conns[0].commits == 3
    assert conns[0].executes == 3


def test_stale_connection_is_dropped_then_reconnects(monkeypatch):
    repo, conns = _repo_with_fake_connect(monkeypatch)
    p = _fake_product()
    repo.upsert_products_batch(1, [p])  # opens conn #1
    conns[0].fail = True  # simulate the pooler dropping the idle connection
    with pytest.raises(RuntimeError):
        repo.upsert_products_batch(1, [p])
    assert conns[0].closed  # broken connection was dropped
    # a caller retry reconnects transparently on a fresh connection
    repo.upsert_products_batch(1, [p])
    assert len(conns) == 2
    assert conns[1].commits == 1


def test_empty_products_is_a_noop(monkeypatch):
    repo, conns = _repo_with_fake_connect(monkeypatch)
    repo.upsert_products_batch(1, [])
    assert conns == []  # no connection opened for an empty batch
