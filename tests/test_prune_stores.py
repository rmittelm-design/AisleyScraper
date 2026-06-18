from aisley_scraper import cli
from aisley_scraper.config import Settings
from aisley_scraper.db.repository import Repository
from aisley_scraper.models import StoreProfile, StoreSeed


def _settings() -> Settings:
    return Settings(
        LOG_LEVEL="INFO",
        USER_AGENT="aisley-test-agent",
        SUPABASE_URL="https://x.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="key",
        SUPABASE_STORAGE_BUCKET="product-images",
        SUPABASE_STORAGE_PATH="aisley",
        PERSISTENCE_TARGET="supabase",
    )


# ---- repository method (faked connection) --------------------------------

class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql.strip().split()[0].lower(), params))

    def fetchall(self):
        return list(self._rows)


class _FakeConn:
    def __init__(self, cur):
        self._cur = cur
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False

    def cursor(self):
        return self._cur

    def commit(self):
        self.committed = True


def _repo() -> Repository:
    return Repository("postgresql://u:p@localhost:5432/db")


def test_delete_stores_not_in_domains_removes_only_non_tsv(monkeypatch):
    cur = _FakeCursor(
        [(1, "https://www.keep.com"), (2, "http://drop.com"), (3, "https://keep.com/x")]
    )
    repo = _repo()
    monkeypatch.setattr(repo, "_connect", lambda: _FakeConn(cur))

    deleted, removed = repo.delete_stores_not_in_domains({"keep.com"})

    assert deleted == 1
    assert removed == ["drop.com"]
    delete_calls = [c for c in cur.executed if c[0] == "delete"]
    assert delete_calls and delete_calls[0][1] == ([2],)  # only id 2 (drop.com)


def test_delete_stores_not_in_domains_noop_on_empty_keep_set(monkeypatch):
    repo = _repo()

    def _no_connect():
        raise AssertionError("must not connect when keep_domains is empty")

    monkeypatch.setattr(repo, "_connect", _no_connect)
    assert repo.delete_stores_not_in_domains(set()) == (0, [])


# ---- run_prune_stores command --------------------------------------------

def _install(monkeypatch, deleted_flag):
    settings = _settings()
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "_setup_logging", lambda *_a, **_k: None)
    monkeypatch.setattr(
        cli, "load_store_seeds_from_dir",
        lambda *_a, **_k: [StoreSeed(store_url="https://keep.com")],
    )

    class _FakeRepo:
        def __init__(self, *_a, **_k):
            pass

        def ensure_schema(self):
            pass

        def list_all_store_profiles(self):
            return [
                StoreProfile(store_name="K", website="https://keep.com", store_type="online"),
                StoreProfile(store_name="D", website="https://drop.com", store_type="online"),
            ]

        def delete_stores_not_in_domains(self, keep):
            deleted_flag["called"] = True
            return (1, ["drop.com"])

    monkeypatch.setattr(cli, "Repository", _FakeRepo)


def test_run_prune_stores_dry_run_does_not_delete(monkeypatch):
    flag = {"called": False}
    _install(monkeypatch, flag)
    assert cli.run_prune_stores(execute=False) == 0
    assert flag["called"] is False


def test_run_prune_stores_execute_deletes(monkeypatch):
    flag = {"called": False}
    _install(monkeypatch, flag)
    assert cli.run_prune_stores(execute=True) == 0
    assert flag["called"] is True
