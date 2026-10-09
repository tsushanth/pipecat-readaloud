"""Framework-neutral async HTTP client for the public ReadAloud TTS API.

This file is intentionally identical in ``pipecat-readaloud`` and ``livekit-plugins-readaloud``
(a test in each repo directory-pair keeps them in sync) so each package is self-contained and
depends on nothing but ``aiohttp``.

Two public HTTP surfaces are spoken, both streaming raw audio in the response body:

* native  ``POST {base}/v1/text-to-speech``   Bearer key, JSON ``{text, voice, speed, format, engine}``
          formats: ``pcm_24000`` ``pcm_8000`` ``mulaw_8000`` ``alaw_8000``
* compat  ``POST {base}/v1/text-to-speech/{voice}/stream?output_format=...`` (ElevenLabs-shaped),
          ``xi-api-key`` header, JSON ``{text, model_id, voice_settings:{speed}}``;
          used only for formats the native endpoint does not produce (``pcm_16000``, ``pcm_22050`` ...)
"""
from __future__ import annotations

import asyncio
import json
import random
from dataclasses import dataclass
from typing import AsyncIterator, Optional

import aiohttp

DEFAULT_BASE_URL = "https://api.readaloudai.org"
NATIVE_FORMATS = {"pcm_24000": 24000, "pcm_8000": 8000, "mulaw_8000": 8000, "alaw_8000": 8000}
# formats served by the ElevenLabs-compatible endpoint (name used there -> sample rate)
COMPAT_FORMATS = {"pcm_16000": 16000, "pcm_22050": 22050, "pcm_32000": 32000, "pcm_44100": 44100, "pcm_48000": 48000}
SUPPORTED_FORMATS = {**NATIVE_FORMATS, **COMPAT_FORMATS}
RETRYABLE_STATUSES = {429, 502, 503, 504}


class ReadAloudAPIError(Exception):
    """An HTTP/protocol error from the ReadAloud API."""

    def __init__(self, message: str, *, status: Optional[int] = None, code: Optional[str] = None,
                 retry_after: Optional[float] = None, retryable: bool = False):
        super().__init__(message)
        self.message = message
        self.status = status
        self.code = code            # e.g. invalid_api_key, quota_exceeded, voice_not_found, capacity
        self.retry_after = retry_after
        self.retryable = retryable

    @property
    def is_auth(self) -> bool:
        return self.status == 401

    @property
    def is_capacity(self) -> bool:
        return self.code == "capacity"


def parse_error(status: int, body: bytes, retry_after_header: Optional[str]) -> ReadAloudAPIError:
    """Map a native ``{"error": ...}`` or ElevenLabs-style ``{"detail": ...}`` error body."""
    msg, code = "", None
    try:
        obj = json.loads(body.decode("utf-8", "replace"))
        if isinstance(obj, dict):
            d = obj.get("detail")
            if isinstance(d, dict):
                msg, code = str(d.get("message") or ""), d.get("status")
            elif isinstance(d, list) and d:
                msg = str(d[0].get("msg", d[0])) if isinstance(d[0], dict) else str(d[0])
            else:
                msg = str(obj.get("error") or obj.get("message") or "")
    except Exception:
        msg = body[:200].decode("utf-8", "replace")
    retry_after = None
    if retry_after_header:
        try:
            retry_after = float(retry_after_header)
        except ValueError:
            pass
    if status == 401:
        code = code or "invalid_api_key"
    elif status == 402:
        code = code or "quota_exceeded"
    elif status in (429, 503):
        code = "capacity"            # native 503 "worker busy/at capacity", compat 429 too_many_concurrent_requests
    elif status == 404:
        code = code or "voice_not_found"
    return ReadAloudAPIError(msg or f"HTTP {status}", status=status, code=code, retry_after=retry_after,
                             retryable=status in RETRYABLE_STATUSES)


@dataclass
class RequestConfig:
    api_key: str
    base_url: str = DEFAULT_BASE_URL
    engine: str = "piper"
    output_format: str = "pcm_24000"
    connect_timeout: float = 10.0
    read_timeout: float = 30.0       # max silence between chunks
    max_retries: int = 3             # retries of capacity/5xx/connect errors BEFORE the first audio byte
    backoff_base: float = 0.25
    backoff_cap: float = 4.0
    user_agent: str = "readaloud-voice-plugins/0.1"

    @property
    def sample_rate(self) -> int:
        return SUPPORTED_FORMATS[self.output_format]

    @property
    def wire_sample_rate(self) -> int:
        return self.sample_rate

    def validate(self) -> None:
        if not self.api_key:
            raise ValueError("ReadAloud API key missing: pass api_key= or set READALOUD_API_KEY")
        if self.output_format not in SUPPORTED_FORMATS:
            raise ValueError(f"output_format must be one of {sorted(SUPPORTED_FORMATS)}")
        if self.engine not in ("piper", "kokoro"):
            raise ValueError("engine must be 'piper' or 'kokoro'")


def build_request(cfg: RequestConfig, text: str, voice: str, speed: float):
    """Returns (url, headers, json_body) for the endpoint matching cfg.output_format."""
    base = cfg.base_url.rstrip("/")
    ua = {"User-Agent": cfg.user_agent}
    if cfg.output_format in NATIVE_FORMATS:
        return (f"{base}/v1/text-to-speech",
                {"Authorization": f"Bearer {cfg.api_key}", "Content-Type": "application/json", **ua},
                {"text": text, "voice": voice, "speed": speed, "format": cfg.output_format, "engine": cfg.engine})
    from urllib.parse import quote
    v = "piper-default" if (voice == "default" and cfg.engine == "piper") else voice
    model = "eleven_flash_v2_5" if cfg.engine == "piper" else "eleven_multilingual_v2"
    return (f"{base}/v1/text-to-speech/{quote(v, safe='')}/stream?output_format={cfg.output_format}",
            {"xi-api-key": cfg.api_key, "Content-Type": "application/json", **ua},
            {"text": text, "model_id": model, "voice_settings": {"speed": speed}})


def _g711_table(decode) -> list:
    return [decode(i).to_bytes(2, "little", signed=True) for i in range(256)]


def _ulaw(b: int) -> int:
    b = ~b & 0xFF
    t = (((b & 0x0F) << 3) + 0x84) << ((b & 0x70) >> 4)
    return (0x84 - t) if (b & 0x80) else (t - 0x84)


def _alaw(b: int) -> int:
    b ^= 0x55
    t = (b & 0x0F) << 4
    seg = (b & 0x70) >> 4
    if seg == 0:
        t += 8
    elif seg == 1:
        t += 0x108
    else:
        t = (t + 0x108) << (seg - 1)
    return t if (b & 0x80) else -t


_ULAW_TABLE = _g711_table(_ulaw)
_ALAW_TABLE = _g711_table(_alaw)


def g711_to_pcm16(data: bytes, law: str) -> bytes:
    """Decode G.711 mu-law/A-law bytes to 16-bit little-endian PCM (audioop is gone in Python 3.13)."""
    table = _ULAW_TABLE if law == "mulaw" else _ALAW_TABLE
    return b"".join([table[b] for b in data])


class AudioStream:
    """Async iterator of PCM16-LE mono chunks (sample aligned) at ``sample_rate``.

    Retries capacity/5xx/connect failures with exponential backoff (honouring Retry-After) until
    the first audio byte has been received; after that a failure is raised, never retried (retrying
    would replay audio). ``aclose()`` / cancellation drops the HTTP connection, which is the stop
    signal: the gateway aborts the worker request when the client goes away.
    """

    def __init__(self, session: aiohttp.ClientSession, cfg: RequestConfig, text: str, voice: str,
                 speed: float = 1.0):
        self._session, self._cfg, self._text, self._voice, self._speed = session, cfg, text, voice, speed
        self._resp: Optional[aiohttp.ClientResponse] = None
        self.request_id: Optional[str] = None
        self.attempts = 0

    async def _open(self) -> aiohttp.ClientResponse:
        cfg = self._cfg
        url, headers, body = build_request(cfg, self._text, self._voice, self._speed)
        timeout = aiohttp.ClientTimeout(total=None, connect=cfg.connect_timeout, sock_connect=cfg.connect_timeout,
                                        sock_read=cfg.read_timeout)
        last: Optional[ReadAloudAPIError] = None
        for attempt in range(cfg.max_retries + 1):
            self.attempts = attempt + 1
            try:
                resp = await self._session.post(url, headers=headers, json=body, timeout=timeout)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last = ReadAloudAPIError(f"connection error: {e!r}", code="connection", retryable=True)
            else:
                if resp.status == 200:
                    self.request_id = resp.headers.get("request-id") or resp.headers.get("x-request-id")
                    return resp
                err = parse_error(resp.status, await resp.read(), resp.headers.get("Retry-After"))
                resp.release()
                if not err.retryable:
                    raise err
                last = err
            if attempt >= cfg.max_retries:
                break
            delay = last.retry_after if last.retry_after is not None else min(cfg.backoff_cap, cfg.backoff_base * 2 ** attempt)
            delay = min(delay, cfg.backoff_cap) * random.uniform(0.9, 1.1)
            await asyncio.sleep(delay)
        assert last is not None
        raise last

    async def __aiter__(self) -> AsyncIterator[bytes]:
        cfg = self._cfg
        resp = self._resp = await self._open()
        law = "mulaw" if cfg.output_format == "mulaw_8000" else "alaw" if cfg.output_format == "alaw_8000" else None
        carry = b""
        try:
            async for chunk in resp.content.iter_any():
                if not chunk:
                    continue
                if law:
                    yield g711_to_pcm16(chunk, law)
                    continue
                chunk = carry + chunk
                if len(chunk) % 2:
                    chunk, carry = chunk[:-1], chunk[-1:]
                else:
                    carry = b""
                if chunk:
                    yield chunk
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            raise ReadAloudAPIError(f"stream interrupted: {e!r}", code="stream", retryable=False) from e
        finally:
            await self.aclose()

    async def aclose(self) -> None:
        resp, self._resp = self._resp, None
        if resp is not None:
            resp.close()   # closes the connection (does not return it to the pool): this is the "stop"
