"""Branch reconciliation for `shopify_stores`.

`Repository.sync_store_branches` makes a website's rows exactly the current
branch set while REUSING existing row ids, matched by the www/scheme-insensitive
DOMAIN and converged onto one canonical website string. The primary branch
keeps its id (and its `shopify_products`); stale rows are removed.
"""

from aisley_scraper.db.repository import Repository, _domain_key, canonical_website
from aisley_scraper.models import StoreProfile


class _FakeCursor:
    def __init__(self, existing: list[tuple], new_ids: list[int]) -> None:
        self._existing = existing  # rows of (id, website, address)
        self._new_ids = list(new_ids)
        self.calls: list[tuple[str, str, object]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_args) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        self.calls.append((sql.strip().split()[0].lower(), sql, params))

    def fetchall(self) -> list[tuple]:
        return list(self._existing)

    def fetchone(self) -> tuple[int]:
        return (self._new_ids.pop(0),)


class _FakeConn:
    def __init__(self, cur: _FakeCursor) -> None:
        self._cur = cur
        self.committed = False

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *_args) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return self._cur

    def commit(self) -> None:
        self.committed = True


def _repo() -> Repository:
    return Repository("postgresql://u:p@localhost:5432/db")


def _branch(address: str, website: str = "https://zieboutique.com") -> StoreProfile:
    return StoreProfile(
        store_name="ZIE BOUTIQUE",
        website=website,
        store_type="online",
        address=address,
        lat=34.1895616,
        long=-118.388119,
    )


def _by_verb(cur: _FakeCursor, verb: str):
    return [c for c in cur.calls if c[0] == verb]


# ---- helpers -------------------------------------------------------------

def test_domain_key_strips_scheme_and_www() -> None:
    assert _domain_key("https://www.x.com/path") == "x.com"
    assert _domain_key("http://x.com/") == "x.com"
    assert _domain_key("https://x.com") == "x.com"


def test_canonical_website_normalizes_to_https_bare_domain() -> None:
    for url in ("http://www.x.com/", "https://x.com", "http://x.com/collections"):
        assert canonical_website(url) == "https://x.com"


# ---- sync_store_branches -------------------------------------------------

def test_sync_reuses_placeholder_row_id_for_first_branch(monkeypatch) -> None:
    cur = _FakeCursor(existing=[(587, "https://zieboutique.com", None)], new_ids=[])
    repo = _repo()
    monkeypatch.setattr(repo, "_connect", lambda: _FakeConn(cur))

    ids = repo.sync_store_branches("https://zieboutique.com", [_branch("13013 Victory Blvd")])

    assert ids == [587]
    assert not _by_verb(cur, "insert")
    assert not _by_verb(cur, "delete")
    update = _by_verb(cur, "update")
    assert len(update) == 1
    params = update[0][2]
    assert params[0] == "https://zieboutique.com"  # canonical website written
    assert params[4] == "13013 Victory Blvd"       # address column
    assert params[-1] == 587                        # WHERE id = %s


def test_sync_prefers_exact_address_match_and_deletes_stale(monkeypatch) -> None:
    cur = _FakeCursor(
        existing=[(587, "https://zieboutique.com", None),
                  (811, "https://zieboutique.com", "13013 Victory Blvd")],
        new_ids=[],
    )
    repo = _repo()
    monkeypatch.setattr(repo, "_connect", lambda: _FakeConn(cur))

    ids = repo.sync_store_branches("https://zieboutique.com", [_branch("13013 Victory Blvd")])

    assert ids == [811]
    assert _by_verb(cur, "update")[0][2][-1] == 811
    delete = _by_verb(cur, "delete")
    assert len(delete) == 1 and delete[0][2] == ([587],)


def test_sync_matches_existing_rows_across_scheme_and_www(monkeypatch) -> None:
    # Existing rows live under http+www; the new scrape is https without www.
    cur = _FakeCursor(
        existing=[(700, "http://www.zieboutique.com", "13013 Victory Blvd")],
        new_ids=[],
    )
    repo = _repo()
    monkeypatch.setattr(repo, "_connect", lambda: _FakeConn(cur))

    ids = repo.sync_store_branches("https://zieboutique.com", [_branch("13013 Victory Blvd")])

    # Reuses id 700 (same domain) and rewrites it to the canonical website.
    assert ids == [700]
    assert not _by_verb(cur, "insert")
    assert not _by_verb(cur, "delete")
    assert _by_verb(cur, "update")[0][2][0] == "https://zieboutique.com"


def test_sync_inserts_canonical_when_no_existing_row(monkeypatch) -> None:
    cur = _FakeCursor(existing=[], new_ids=[900])
    repo = _repo()
    conn = _FakeConn(cur)
    monkeypatch.setattr(repo, "_connect", lambda: conn)

    ids = repo.sync_store_branches("https://zieboutique.com", [_branch("13013 Victory Blvd")])

    assert ids == [900]
    insert = _by_verb(cur, "insert")
    assert len(insert) == 1 and insert[0][2][0] == "https://zieboutique.com"
    assert not _by_verb(cur, "delete")
    assert conn.committed is True


def test_sync_multi_branch_creates_a_row_per_address(monkeypatch) -> None:
    cur = _FakeCursor(existing=[], new_ids=[901, 902, 903])
    repo = _repo()
    monkeypatch.setattr(repo, "_connect", lambda: _FakeConn(cur))

    ids = repo.sync_store_branches(
        "https://zieboutique.com",
        [_branch("A St"), _branch("B St"), _branch("C St")],
    )

    assert ids == [901, 902, 903]
    assert len(_by_verb(cur, "insert")) == 3


def test_sync_no_branches_is_noop(monkeypatch) -> None:
    repo = _repo()

    def _no_connect():
        raise AssertionError("must not connect when there are no branches")

    monkeypatch.setattr(repo, "_connect", _no_connect)
    assert repo.sync_store_branches("https://zieboutique.com", []) == []
