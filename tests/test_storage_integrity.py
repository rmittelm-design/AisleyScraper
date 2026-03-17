import httpx

from aisley_scraper import storage_integrity


def _response(status_code: int, request: httpx.Request, payload) -> httpx.Response:
    return httpx.Response(status_code=status_code, request=request, json=payload)


def test_request_with_retries_retries_http_502_then_succeeds() -> None:
    request = httpx.Request("POST", "https://example.com/storage")

    class _FakeClient:
        def __init__(self) -> None:
            self.calls = 0

        def request(self, method: str, url: str, **kwargs) -> httpx.Response:
            _ = (method, url, kwargs)
            self.calls += 1
            if self.calls < 3:
                return _response(502, request, {"error": "bad gateway"})
            return _response(200, request, [])

    client = _FakeClient()

    response = storage_integrity._request_with_retries(client, "POST", str(request.url), attempts=4)

    assert response.status_code == 200
    assert client.calls == 3


def test_request_with_retries_retries_request_error_then_succeeds() -> None:
    request = httpx.Request("GET", "https://example.com/rest")

    class _FakeClient:
        def __init__(self) -> None:
            self.calls = 0

        def request(self, method: str, url: str, **kwargs) -> httpx.Response:
            _ = (method, url, kwargs)
            self.calls += 1
            if self.calls == 1:
                raise httpx.ConnectError("boom", request=request)
            return _response(200, request, [])

    client = _FakeClient()

    response = storage_integrity._request_with_retries(client, "GET", str(request.url), attempts=4)

    assert response.status_code == 200
    assert client.calls == 2


def test_list_all_storage_objects_skips_prefixes_that_return_400(monkeypatch) -> None:
    request = httpx.Request("POST", "https://x.supabase.co/storage/v1/object/list/uploads")

    class _FakeClient:
        def __init__(self, timeout: float) -> None:
            _ = timeout
            self.calls: list[dict[str, object]] = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            _ = (exc_type, exc, tb)
            return None

        def request(self, method: str, url: str, **kwargs) -> httpx.Response:
            _ = (method, url)
            self.calls.append(kwargs)
            payload = kwargs["json"]
            prefix = payload["prefix"]
            if prefix == "uploads":
                return httpx.Response(
                    200,
                    request=request,
                    json=[
                        {"name": "good", "id": None},
                        {"name": "bad", "id": None},
                    ],
                )
            if prefix == "uploads/good":
                return httpx.Response(
                    200,
                    request=request,
                    json=[
                        {"name": "file.jpg", "id": "obj-1"},
                    ],
                )
            if prefix == "uploads/bad":
                return httpx.Response(
                    400,
                    request=request,
                    json={"message": "invalid prefix"},
                )
            return httpx.Response(200, request=request, json=[])

    monkeypatch.setattr(storage_integrity.httpx, "Client", _FakeClient)

    paths = storage_integrity.list_all_storage_objects(
        base_url="https://x.supabase.co",
        bucket="uploads",
        headers={"Authorization": "Bearer key"},
        root_prefix="uploads",
    )

    assert paths == {"uploads/good/file.jpg"}