import asyncio
import builtins
import sys
from types import ModuleType, SimpleNamespace

import httpx
import pytest

from smallsat_dl.scraper import HttpClient, SmallSatDownloader, is_retryable_download_error
from smallsat_dl.transports import CurlCffiClient


def install_session(monkeypatch, response=None, error=None):
    sessions = []

    class Session:
        def __init__(self, **kwargs):
            self.options = kwargs
            self.closed = False
            sessions.append(self)

        async def get(self, url, **kwargs):
            self.last_request = (url, kwargs)
            if error is not None:
                raise error
            return response

        async def close(self):
            self.closed = True

    requests = ModuleType("curl_cffi.requests")
    requests.AsyncSession = Session
    package = ModuleType("curl_cffi")
    package.requests = requests
    monkeypatch.setitem(sys.modules, "curl_cffi", package)
    monkeypatch.setitem(sys.modules, "curl_cffi.requests", requests)
    return sessions


def test_curl_adapter_normalizes_already_decompressed_body(monkeypatch):
    response = SimpleNamespace(
        status_code=200, url="https://example.test/final",
        headers={"Content-Encoding": "gzip", "Content-Length": "999", "Content-Type": "text/xml"},
        content=b"<metadata>already decoded</metadata>",
    )
    sessions = install_session(monkeypatch, response=response)

    async def run():
        client = CurlCffiClient(timeout=12)
        try:
            return await client.get("https://example.test/start")
        finally:
            await client.aclose()

    result = asyncio.run(run())
    assert result.content == response.content
    assert result.headers["content-length"] == str(len(response.content))
    assert "content-encoding" not in result.headers
    assert str(result.request.url) == response.url
    assert sessions[0].options == {"impersonate": "chrome", "timeout": 12}
    assert sessions[0].closed


@pytest.mark.parametrize("status", [403, 429, 503])
def test_curl_status_errors_keep_download_retry_classification(monkeypatch, status):
    install_session(monkeypatch, response=SimpleNamespace(
        status_code=status, url="https://example.test/file.pdf",
        headers={"retry-after": "60"}, content=b"blocked",
    ))

    async def run():
        client = HttpClient(transport="curl-cffi")
        try:
            await client.get_bytes("https://example.test/file.pdf")
        finally:
            await client.close()

    with pytest.raises(httpx.HTTPStatusError) as raised:
        asyncio.run(run())
    assert raised.value.response.status_code == status
    assert raised.value.response.headers["retry-after"] == "60"
    assert is_retryable_download_error(raised.value)


def test_curl_network_errors_remain_retryable(monkeypatch):
    install_session(monkeypatch, error=OSError("connection failed"))

    async def run():
        client = CurlCffiClient()
        try:
            await client.get("https://example.test/file.pdf")
        finally:
            await client.aclose()

    with pytest.raises(httpx.TransportError) as raised:
        asyncio.run(run())
    assert is_retryable_download_error(raised.value)


@pytest.mark.parametrize("factory", [HttpClient, SmallSatDownloader])
def test_default_client_and_downloader_construct_curl_transport(monkeypatch, factory):
    sessions = install_session(monkeypatch)

    async def run():
        instance = factory()
        try:
            client = instance.client if isinstance(instance, SmallSatDownloader) else instance
            assert isinstance(client.client, CurlCffiClient)
            if isinstance(instance, SmallSatDownloader):
                assert instance.downloads_per_minute == 30
        finally:
            await instance.close()

    asyncio.run(run())

    assert len(sessions) == 1
    assert sessions[0].options == {"impersonate": "chrome", "timeout": 30.0}
    assert sessions[0].closed


@pytest.mark.parametrize("factory", [HttpClient, SmallSatDownloader])
def test_explicit_httpx_works_without_importing_curl(monkeypatch, factory):
    original_import = builtins.__import__

    def no_curl(name, *args, **kwargs):
        if name.startswith("curl_cffi"):
            raise ImportError("curl dependency missing")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_curl)

    async def run():
        instance = factory(transport="httpx")
        try:
            client = instance.client if isinstance(instance, SmallSatDownloader) else instance
            assert isinstance(client.client, httpx.AsyncClient)
        finally:
            await instance.close()

    asyncio.run(run())
    with pytest.raises(RuntimeError, match="uv sync.*--transport httpx"):
        factory()


def test_injected_clients_bypass_default_transport_construction(monkeypatch):
    original_import = builtins.__import__

    def no_curl(name, *args, **kwargs):
        if name.startswith("curl_cffi"):
            raise AssertionError("injected clients must not import a transport dependency")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_curl)
    injected = object()

    assert HttpClient(client=injected).client is injected
    assert SmallSatDownloader(client=injected).client is injected


@pytest.mark.parametrize("option", ["delay", "downloads_per_minute", "retry_backoff"])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_throttling_settings_rejected_before_client_creation(monkeypatch, option, value):
    def forbidden(*args, **kwargs):
        pytest.fail("invalid throttling configuration created an HTTP client")

    monkeypatch.setattr("smallsat_dl.scraper.HttpClient", forbidden)
    with pytest.raises(ValueError, match="finite"):
        SmallSatDownloader(**{option: value})
