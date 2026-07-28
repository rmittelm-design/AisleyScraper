from __future__ import annotations

from aisley_scraper.db.repository import Repository
from aisley_scraper.models import ProductRecord


class _FakeCursor:
    """Records SQL; serves canned rows for the store + production SELECTs.

    Routing: the store lookup contains 'from shopify_stores', the production
    lookup contains 'from shopify_products'; the removal UPDATE contains
    'update shopify_products' (no 'from'), so the three are distinguishable.
    """

    def __init__(self, stores, production) -> None:
        self._stores = stores            # [(id, website)]
        self._production = production     # [(product_id, unavailable_bool)]
        self.executed: list[tuple[str, object]] = []
        self.executemany_calls: list[tuple[str, list]] = []
        self._fetch: list = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        low = sql.lower()
        if "from shopify_stores" in low:
            self._fetch = self._stores
        elif "from shopify_products" in low:
            self._fetch = self._production
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


def _mark_update(cur: _FakeCursor):
    """The 'removed -> unavailable' UPDATE, if it ran."""
    return [(s, p) for (s, p) in cur.executed if "set unavailable = true" in s.lower()]


def test_metadata_update_only_touches_metadata_columns(monkeypatch):
    cur = _FakeCursor(
        stores=[(10, "https://shop.example.com")],
        production=[("P1", False), ("P2", False)],
    )
    repo = _repo(monkeypatch, cur)
    products = [
        _product("P1", price_cents=100, unavailable=True),
        _product("P2", price_cents=200),
        _product("P3", price_cents=300),  # new -> skipped
    ]

    r = repo.update_products_metadata("https://www.shop.example.com/", products)

    assert r["scraped"] == 3
    assert r["updated"] == 2
    assert r["missing"] == 0 and r["marked_unavailable"] == 0

    assert len(cur.executemany_calls) == 1
    sql, rows = cur.executemany_calls[0]
    assert len(rows) == 2  # P3 skipped
    low = sql.lower()
    assert low.strip().startswith("update shopify_products")
    for col in ("item_name", "description", "price_cents", "unavailable", "last_seen_at"):
        assert col in low
    for col in ("images", "supabase_images", "gender_label", "gender_probs_csv"):
        assert col not in low
    assert _mark_update(cur) == []  # nothing missing


def test_removed_products_are_marked_unavailable(monkeypatch):
    cur = _FakeCursor(
        stores=[(10, "https://shop.example.com")],
        production=[("P1", False), ("P2", False), ("P4", False)],  # P4 delisted
    )
    repo = _repo(monkeypatch, cur)
    products = [_product("P1"), _product("P2"), _product("P3")]  # coverage 2/3

    r = repo.update_products_metadata("shop.example.com", products)

    assert r["updated"] == 2 and r["missing"] == 1
    assert r["marked_unavailable"] == 1
    marks = _mark_update(cur)
    assert len(marks) == 1
    sql, params = marks[0]
    assert params == ([10], ["P4"])  # only the delisted, available product
    # the removal UPDATE only flips availability, nothing else
    assert "images" not in sql.lower() and "price_cents" not in sql.lower()


def test_partial_scrape_does_not_flag_catalog(monkeypatch):
    """Low coverage (bot-block/partial) must skip the removal marking entirely."""
    cur = _FakeCursor(
        stores=[(10, "https://shop.example.com")],
        production=[(f"P{i}", False) for i in range(10)],
    )
    repo = _repo(monkeypatch, cur)

    r = repo.update_products_metadata("shop.example.com", [_product("P0")])  # 10% coverage

    assert r["updated"] == 1 and r["missing"] == 9
    assert r["marked_unavailable"] == 0
    assert "partial" in str(r["mark_skipped_reason"])
    assert _mark_update(cur) == []  # nothing flagged


def test_empty_scrape_never_flags(monkeypatch):
    cur = _FakeCursor(
        stores=[(10, "https://shop.example.com")],
        production=[("P1", False), ("P2", False)],
    )
    repo = _repo(monkeypatch, cur)

    r = repo.update_products_metadata("shop.example.com", [])

    assert r["updated"] == 0 and r["missing"] == 2
    assert r["marked_unavailable"] == 0
    assert r["mark_skipped_reason"] == "scrape returned no products"
    assert _mark_update(cur) == [] and cur.executemany_calls == []


def test_no_mark_removed_flag_disables_flagging(monkeypatch):
    cur = _FakeCursor(
        stores=[(10, "https://shop.example.com")],
        production=[("P1", False), ("P4", False)],
    )
    repo = _repo(monkeypatch, cur)

    r = repo.update_products_metadata(
        "shop.example.com", [_product("P1")], mark_removed=False
    )

    assert r["missing"] == 1 and r["marked_unavailable"] == 0
    assert r["mark_skipped_reason"] is None
    assert _mark_update(cur) == []


def test_already_unavailable_missing_is_not_reflagged(monkeypatch):
    cur = _FakeCursor(
        stores=[(10, "https://shop.example.com")],
        production=[("P1", False), ("P5", True)],  # P5 already unavailable + missing
    )
    repo = _repo(monkeypatch, cur)

    r = repo.update_products_metadata("shop.example.com", [_product("P1")])

    assert r["missing"] == 1  # P5 is missing...
    assert r["marked_unavailable"] == 0  # ...but already unavailable, so no write
    assert _mark_update(cur) == []


def test_dry_run_writes_nothing(monkeypatch):
    cur = _FakeCursor(
        stores=[(10, "https://shop.example.com")],
        production=[("P1", False), ("P4", False)],
    )
    repo = _repo(monkeypatch, cur)

    r = repo.update_products_metadata("shop.example.com", [_product("P1")], dry_run=True)

    assert r["updated"] == 1 and r["marked_unavailable"] == 1  # reports intent
    assert cur.executemany_calls == [] and _mark_update(cur) == []  # writes nothing


def test_unknown_domain_is_noop(monkeypatch):
    cur = _FakeCursor(stores=[], production=[])
    repo = _repo(monkeypatch, cur)

    r = repo.update_products_metadata("nope.example", [_product("P1")])

    assert r["updated"] == 0 and r["marked_unavailable"] == 0
    assert cur.executemany_calls == [] and _mark_update(cur) == []


# ── Repository.mark_removed_products_unavailable (used by the full crawl) ──

def test_mark_removed_flags_delisted_products(monkeypatch):
    cur = _FakeCursor(
        stores=[(10, "https://shop.example.com")],
        production=[("P1", False), ("P2", False), ("P4", False)],  # P4 delisted
    )
    repo = _repo(monkeypatch, cur)
    r = repo.mark_removed_products_unavailable("shop.example.com", ["P1", "P2", "P3"])
    assert r["matched"] == 2 and r["missing"] == 1 and r["marked_unavailable"] == 1
    marks = _mark_update(cur)
    assert len(marks) == 1 and marks[0][1] == ([10], ["P4"])


def test_mark_removed_guard_skips_partial_scrape(monkeypatch):
    cur = _FakeCursor(
        stores=[(10, "https://shop.example.com")],
        production=[(f"P{i}", False) for i in range(10)],
    )
    repo = _repo(monkeypatch, cur)
    r = repo.mark_removed_products_unavailable("shop.example.com", ["P0"])  # 10%
    assert r["marked_unavailable"] == 0
    assert "partial" in str(r["mark_skipped_reason"])
    assert _mark_update(cur) == []


def test_mark_removed_empty_scrape_skips(monkeypatch):
    cur = _FakeCursor(
        stores=[(10, "https://shop.example.com")],
        production=[("P1", False), ("P2", False)],
    )
    repo = _repo(monkeypatch, cur)
    r = repo.mark_removed_products_unavailable("shop.example.com", [])
    assert r["mark_skipped_reason"] == "scrape returned no products"
    assert _mark_update(cur) == []


def test_mark_removed_dry_run_reports_but_writes_nothing(monkeypatch):
    cur = _FakeCursor(
        stores=[(10, "https://shop.example.com")],
        production=[("P1", False), ("P4", False)],
    )
    repo = _repo(monkeypatch, cur)
    r = repo.mark_removed_products_unavailable("shop.example.com", ["P1"], dry_run=True)
    assert r["marked_unavailable"] == 1 and _mark_update(cur) == []


def test_mark_removed_skips_already_unavailable(monkeypatch):
    cur = _FakeCursor(
        stores=[(10, "https://shop.example.com")],
        production=[("P1", False), ("P5", True)],  # P5 delisted but already unavailable
    )
    repo = _repo(monkeypatch, cur)
    r = repo.mark_removed_products_unavailable("shop.example.com", ["P1"])
    assert r["missing"] == 1 and r["marked_unavailable"] == 0
    assert _mark_update(cur) == []
