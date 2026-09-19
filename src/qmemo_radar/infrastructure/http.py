import asyncio
import logging
import time
import zlib
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import httpx

logger = logging.getLogger(__name__)

Sleep = Callable[[float], Awaitable[None]]


class HttpFailure(Exception):
    """Final HTTP failure after retries. The message never contains credentials."""

    def __init__(self, code: str, status_code: int | None = None) -> None:
        super().__init__(code if status_code is None else f"{code} ({status_code})")
        self.code = code
        self.status_code = status_code


async def send_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    params: Mapping[str, str | int] | None = None,
    json: Any = None,
    attempts: int = 3,
    max_wait_seconds: float = 60.0,
    sleep: Sleep = asyncio.sleep,
    before_attempt: Callable[[], Awaitable[None]] | None = None,
    after_attempt: Callable[[httpx.Response | None], Awaitable[None]] | None = None,
) -> httpx.Response:
    """Retries network errors, 5xx and 429 up to `attempts` in total; other 4xx fail at once.

    `before_attempt` runs before every request, so a paid call reserves budget per attempt and
    an exception there stops the call. `after_attempt` gets the response, or None when none
    arrived (the server may still have processed the request).
    """
    for attempt in range(1, attempts + 1):
        last = attempt == attempts
        if before_attempt is not None:
            await before_attempt()
        try:
            response = await client.request(method, url, params=params, json=json)
        except httpx.TransportError as exc:
            if after_attempt is not None:
                await after_attempt(None)
            logger.warning(
                "http request failed",
                extra={"operation": "http", "result": "retry", "error_code": type(exc).__name__},
            )
            if last:
                raise HttpFailure("network_error") from exc
            await sleep(float(2 ** (attempt - 1)))
            continue

        if after_attempt is not None:
            await after_attempt(response)
        status = response.status_code
        if status == 429:
            wait = _rate_limit_wait(response)
            if last or wait > max_wait_seconds:
                raise HttpFailure("rate_limited", status)
            await sleep(wait)
            continue
        if status >= 500:
            if last:
                raise HttpFailure("server_error", status)
            await sleep(float(2 ** (attempt - 1)))
            continue
        if status >= 400:
            raise HttpFailure("client_error", status)
        return response
    raise AssertionError("unreachable")


def _rate_limit_wait(response: httpx.Response) -> float:
    retry_after = response.headers.get("retry-after")
    if retry_after and retry_after.strip().isdigit():
        return float(retry_after)
    reset = response.headers.get("x-rate-limit-reset")
    if reset and reset.strip().isdigit():
        return max(0.0, float(reset) - time.time())
    return 1.0


async def get_limited(
    client: httpx.AsyncClient,
    url: str,
    *,
    max_bytes: int,
    headers: Mapping[str, str] | None = None,
    attempts: int = 3,
    sleep: Sleep = asyncio.sleep,
) -> tuple[httpx.Response, bytes]:
    """GET that stops reading once the body exceeds `max_bytes` (HttpFailure response_too_large).

    Network errors, 5xx and 429 are retried like `send_with_retry` and end in HttpFailure; every
    other status (200, 3xx, 304, 404, other 4xx) is returned for the caller to interpret. Redirects
    are never followed here. The body is the decoded content and the limit applies to it: a
    compressed Content-Encoding is decoded here with a bounded output size, so a decompression
    bomb stops at `max_bytes` + 1 decoded bytes instead of expanding a whole chunk in memory.
    """
    # Only encodings decoded below are offered; httpx would add br/zstd when those are installed.
    headers = {**(headers or {}), "Accept-Encoding": "gzip, deflate"}
    for attempt in range(1, attempts + 1):
        last = attempt == attempts
        failure: HttpFailure
        try:
            async with client.stream("GET", url, headers=headers) as response:
                status = response.status_code
                if status == 429 or status >= 500:
                    code = "rate_limited" if status == 429 else "server_error"
                    failure = HttpFailure(code, status)
                else:
                    declared = response.headers.get("content-length", "")
                    if declared.isdigit() and int(declared) > max_bytes:
                        raise HttpFailure("response_too_large", status)
                    return response, await _read_limited(response, max_bytes)
        except httpx.TransportError as exc:
            failure = HttpFailure("network_error")
            failure.__cause__ = exc
        logger.warning(
            "http request failed",
            extra={"operation": "http", "result": "retry", "error_code": str(failure)},
        )
        if last:
            raise failure
        await sleep(float(2 ** (attempt - 1)))
    raise AssertionError("unreachable")


async def _read_limited(response: httpx.Response, max_bytes: int) -> bytes:
    if response.is_stream_consumed:  # built in memory (httpx.MockTransport): already decoded
        if len(response.content) > max_bytes:
            raise HttpFailure("response_too_large", response.status_code)
        return response.content
    encoding = response.headers.get("content-encoding", "").strip().lower()
    if encoding not in ("", "identity", "gzip", "x-gzip", "deflate"):
        raise HttpFailure("unsupported_encoding", response.status_code)
    decoder = (
        _Decoder(raw_fallback=encoding == "deflate") if encoding not in ("", "identity") else None
    )
    body = bytearray()
    try:
        async for raw in response.aiter_raw():
            if decoder is None:
                body += raw
            else:
                # Never decode more than one byte past the limit, however small `raw` is.
                body += decoder.feed(raw, max_bytes + 1 - len(body))
            if len(body) > max_bytes:
                raise HttpFailure("response_too_large", response.status_code)
        if decoder is not None and not decoder.finished:
            raise HttpFailure("decode_error", response.status_code)  # cut off mid-stream
    except zlib.error as exc:
        raise HttpFailure("decode_error", response.status_code) from exc
    return bytes(body)


class _Decoder:
    """gzip or zlib (several gzip members in a row allowed), or raw deflate, which some servers
    send as `deflate`. The caller bounds the output of every call."""

    def __init__(self, *, raw_fallback: bool) -> None:
        self._raw_fallback = raw_fallback
        self._started = False
        self._stream = zlib.decompressobj(47)  # zlib or gzip header, detected automatically

    @property
    def finished(self) -> bool:
        return self._stream.eof

    def feed(self, data: bytes, limit: int) -> bytes:
        out = bytearray()
        while data and len(out) < limit:
            try:
                out += self._stream.decompress(data, limit - len(out))
            except zlib.error:
                if not (self._raw_fallback and not self._started):
                    raise
                self._raw_fallback = False
                self._stream = zlib.decompressobj(-15)
                continue
            self._started = True
            if self._stream.eof and self._stream.unused_data:
                data = self._stream.unused_data  # the next gzip member
                self._stream = zlib.decompressobj(47)
            else:
                data = self._stream.unconsumed_tail
        return bytes(out)
