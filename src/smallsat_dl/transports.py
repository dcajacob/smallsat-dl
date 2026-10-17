"""Browser-compatible transport behind the normal HTTPX response API."""

from __future__ import annotations

import tempfile
import time
from collections.abc import AsyncIterator, Callable
from pathlib import Path

import httpx


async def write_stream_atomic(
    destination: Path, chunks: AsyncIterator[bytes], *,
    progress: Callable[[int], None] | None = None,
) -> int:
    """Keep large media off the heap and publish only a complete response."""
    temporary_path: Path | None = None
    count = 0
    prefix = bytearray()
    reported = time.monotonic()
    try:
        with tempfile.NamedTemporaryFile(
            dir=destination.parent, prefix=".smallsat-", suffix=".part", delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            async for chunk in chunks:
                if len(prefix) < 512:
                    prefix.extend(chunk[:512 - len(prefix)])
                    if prefix.lstrip().lower().startswith((b"<html", b"<!doctype html")):
                        raise RuntimeError("expected a downloadable file but received HTML")
                temporary.write(chunk)
                count += len(chunk)
                now = time.monotonic()
                if progress is not None and now - reported >= 5:
                    progress(count)
                    reported = now
        if not count:
            raise RuntimeError("received an empty downloadable file")
        temporary_path.replace(destination)
        if progress is not None:
            progress(count)
        return count
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


class CurlCffiClient:
    def __init__(self, timeout: float = 30.0) -> None:
        try:
            from curl_cffi.requests import AsyncSession
        except ImportError as exc:
            raise RuntimeError(
                "curl_cffi is not installed; run uv sync or pip install .; "
                "alternatively select --transport httpx"
            ) from exc
        self._session = AsyncSession(impersonate="chrome", timeout=timeout)

    async def get(self, url: str) -> httpx.Response:
        try:
            response = await self._session.get(url, allow_redirects=True)
        except Exception as exc:
            raise httpx.TransportError(str(exc), request=httpx.Request("GET", url)) from exc
        # curl has already decoded compressed responses. Keeping these wire
        # headers would make HTTPX attempt to decode the same content again.
        headers = {
            key: value for key, value in response.headers.items()
            if key.lower() not in {"content-encoding", "content-length", "transfer-encoding"}
        }
        return httpx.Response(
            response.status_code,
            headers=headers,
            content=response.content,
            request=httpx.Request("GET", response.url or url),
        )

    async def aclose(self) -> None:
        await self._session.close()

    async def download_to(
        self, url: str, destination: Path, *, progress: Callable[[int], None] | None = None,
    ) -> int:
        try:
            # In streaming mode curl_cffi applies the configured timeout to
            # stalls, rather than the total duration of a large video transfer.
            response = await self._session.get(url, allow_redirects=True, stream=True)
        except Exception as exc:
            raise httpx.TransportError(str(exc), request=httpx.Request("GET", url)) from exc
        try:
            checked = httpx.Response(
                response.status_code, headers=dict(response.headers),
                request=httpx.Request("GET", response.url or url),
            )
            checked.raise_for_status()
            if "html" in checked.headers.get("content-type", "").lower():
                raise RuntimeError(f"expected a downloadable file but received HTML from {url}")

            async def chunks():
                try:
                    async for chunk in response.aiter_content():
                        yield chunk
                except Exception as exc:
                    raise httpx.TransportError(str(exc), request=httpx.Request("GET", url)) from exc

            return await write_stream_atomic(destination, chunks(), progress=progress)
        finally:
            await response.aclose()
