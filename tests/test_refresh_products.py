from __future__ import annotations

from aisley_scraper.db.repository import Repository
from aisley_scraper.models import ProductRecord


class _FakeCursor:
    """Records SQL and serves canned fetchall results for the two SELECTs."""

    def __init__(self, stores, existing) -> None:
        self._stores = stores
        self._existing = existing
        self.executed: list[tuple[str, object]] = []
        self.executemany_calls: list[tuple[str, list]] = []
        self._fetch: list = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        low = sql.lower()
        if "from shopify_stores" in low:
            self._fetch = self._stores
        elif "from shopify_products" in low:
            self._fetch = self._existing
        else:
            self._fetch = []

    def executemany(self, sql, seq):
        self.executemany_calls.append((sql, list(seq)))

    def fetchall(self):
        return self._fetch

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeConn:
    def __init__(self, cursor) -> None:
        self._cursor = cursor
        self.committed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _repo(monkeypatch, cursor) -> Repository:
    repo = Repository.__new__(Repository)  # skip __init__ (no DB/settings needed)
    monkeypatch.setattr(repo, "_connect", lambda: _FakeConn(cursor))
    return repo


def _product(pid: str, **kw) -> ProductRecord:
    return ProductRecord(
        product_id=pid,
        product_handle=f"h-{pid}",
        item_name=f"name-{pid}",
        description="desc",
        images=["https://cdn/img1.jpg", "https://cdn/img2.jpg"],
        **kw,
    )


def test_update_products_metadata_only_touches_metadata_columns(monkeypatch):
    cur = _FakeCursor(
        stores=[(10, "https://shop.example.com")],
        existing=[("P1",), ("P2",)],  # P3 is new -> not in production
    )
    repo = _repo(monkeypatch, cur)
    products = [
        _product("P1", price_cents=100, unavailable=True),
        _product("P2", price_cents=200),
        _product("P3", price_cents=300),
    ]

    updated, scraped = repo.update_products_metadata(
        "https://www.shop.example.com/", products
    )

    assert scraped == 3
    assert updated == 2  # only the existing P1, P2

    assert len(cur.executemany_calls) == 1
    sql, rows = cur.executemany_calls[0]
    assert len(rows) == 2  # P3 skipped
    low = sql.lower()
    assert low.strip().startswith("update shopify_products")  # never INSERT
    for col in (
        "item_name", "description", "price_cents", "unavailable",
        "sizes", "colors", "brand", "product_type", "last_seen_at",
    ):
        assert col in low, f"metadata column missing from UPDATE: {col}"
    # CLIP-validated / gender columns must never be overwritten by a refresh.
    for col in ("images", "supabase_images", "gender_label", "gender_probs_csv"):
        assert col not in low, f"refresh must not touch {col}"


def test_update_products_metadata_dry_run_writes_nothing(monkeypatch):
    cur = _FakeCursor(stores=[(10, "https://shop.example.com")], existing=[("P1",)])
    repo = _repo(monkeypatch, cur)

    updated, scraped = repo.update_products_metadata(
        "shop.example.com", [_product("P1"), _product("P9")], dry_run=True
    )

    assert (updated, scraped) == (1, 2)  # reports the real match count
    assert cur.executemany_calls == []  # but writes nothing


def test_update_products_metadata_unknown_domain_is_noop(monkeypatch):
    cur = _FakeCursor(stores=[], existing=[])  # no store rows for this domain
    repo = _repo(monkeypatch, cur)

    updated, scraped = repo.update_products_metadata("nope.example", [_product("P1")])

    assert (updated, scraped) == (0, 1)
    assert cur.executemany_calls == []
