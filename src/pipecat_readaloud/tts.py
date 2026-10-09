"""ReadAloud TTS service for Pipecat.

Streams raw PCM from the public ReadAloud HTTP API (``POST /v1/text-to-speech``) and yields
``TTSAudioRawFrame`` chunks as bytes arrive.

* Interruption: Pipecat cancels the in-flight ``run_tts`` on an ``InterruptionFrame``; the
  generator's cleanup closes the HTTP connection, which is ReadAloud's stop signal (the gateway
  aborts the worker request). Nothing else is needed and nothing is left running.
* Word timestamps: NOT supported (the API returns no alignment), so this is a plain ``TTSService``.
* Errors: 401/402/400/404 -> ``ErrorFrame`` immediately; "at capacity" (HTTP 503/429) and 502/504
  are retried with exponential backoff (honouring Retry-After) until the first audio byte.
"""
from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
from loguru import logger

from pipecat.frames.frames import ErrorFrame, Frame, StartFrame, TTSAudioRawFrame
from pipecat.services.settings import TTSSettings
from pipecat.services.tts_service import TTSService
from pipecat.utils.tracing.service_decorators import traced_tts
from pipecat.utils.types import NOT_GIVEN, NotGiven, is_given

from ._client import (DEFAULT_BASE_URL, NATIVE_FORMATS, SUPPORTED_FORMATS, AudioStream, ReadAloudAPIError,
                      RequestConfig)


@dataclass
class ReadAloudTTSSettings(TTSSettings):
    """Runtime-updatable settings (``TTSUpdateSettingsFrame`` works for ``voice`` and ``speed``).

    Parameters:
        speed: Speaking rate multiplier, 0.5-4.0 (default 1.0).
    """

    speed: float | None | NotGiven = field(default_factory=lambda: NOT_GIVEN)


class ReadAloudTTSService(TTSService):
    """Pipecat TTS service backed by ReadAloud (https://readaloudai.org).

    Args:
        api_key: ReadAloud API key (``rtts_...``). Falls back to env ``READALOUD_API_KEY``.
        base_url: API origin, default ``https://api.readaloudai.org``.
        engine: ``"piper"`` (default, ~200 ms to first audio) or ``"kokoro"``.
        output_format: ``"auto"`` (default) or one of ``pcm_24000, pcm_8000, mulaw_8000, alaw_8000,
            pcm_16000``. ``auto`` asks for the rate closest to the pipeline's output rate that the
            native endpoint produces (8000 -> ``pcm_8000``, everything else ``pcm_24000``) and lets
            Pipecat resample. ``pcm_16000`` is served by the ElevenLabs-compatible endpoint
            (``/v1/text-to-speech/{voice}/stream``); ``mulaw_8000``/``alaw_8000`` are decoded to PCM16.
        sample_rate: Pipeline output rate (Pipecat convention). 8000 for Twilio/Telnyx.
        aiohttp_session: Optional shared session. If omitted one is created and closed with the service.
        max_retries: Retries before the first audio byte on capacity / 5xx / connect errors.
        settings: ``ReadAloudTTSService.Settings(voice="default", speed=1.0)``.
    """

    Settings = ReadAloudTTSSettings
    _settings: Settings

    def __init__(self, *, api_key: Optional[str] = None, base_url: str = DEFAULT_BASE_URL, engine: str = "piper",
                 output_format: str = "auto", sample_rate: Optional[int] = None,
                 aiohttp_session: Optional[aiohttp.ClientSession] = None, max_retries: int = 3,
                 backoff_base: float = 0.25, settings: Optional[ReadAloudTTSSettings] = None, **kwargs):
        default_settings = self.Settings(model=None, voice="default", language=None, speed=1.0)
        if settings is not None:
            default_settings.apply_update(settings)
        super().__init__(sample_rate=sample_rate, push_start_frame=True, push_stop_frames=True,
                         settings=default_settings, **kwargs)
        if output_format != "auto" and output_format not in SUPPORTED_FORMATS:
            raise ValueError(f"output_format must be 'auto' or one of {sorted(SUPPORTED_FORMATS)}")
        self._api_key = api_key or os.environ.get("READALOUD_API_KEY", "")
        if not self._api_key:
            raise ValueError("ReadAloud API key missing: pass api_key= or set READALOUD_API_KEY")
        self._base_url, self._engine, self._output_format = base_url, engine, output_format
        self._max_retries, self._backoff_base = max_retries, backoff_base
        self._session = aiohttp_session
        self._own_session = aiohttp_session is None

    def can_generate_metrics(self) -> bool:
        return True

    # ------------------------------------------------------------------ helpers
    def _wire_format(self) -> str:
        if self._output_format != "auto":
            return self._output_format
        return "pcm_8000" if self.sample_rate == 8000 else "pcm_24000"

    def _config(self) -> RequestConfig:
        return RequestConfig(api_key=self._api_key, base_url=self._base_url, engine=self._engine,
                             output_format=self._wire_format(), max_retries=self._max_retries,
                             backoff_base=self._backoff_base)

    def _http(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
            self._own_session = True
        return self._session

    async def start(self, frame: StartFrame):
        await super().start(frame)
        fmt = self._wire_format()
        if fmt in NATIVE_FORMATS or fmt.startswith("pcm_"):
            wire = SUPPORTED_FORMATS[fmt]
            if wire != self.sample_rate:
                logger.debug(f"{self}: ReadAloud {fmt} will be resampled to {self.sample_rate} Hz")

    async def cleanup(self):
        await super().cleanup()
        if self._own_session and self._session and not self._session.closed:
            await self._session.close()

    # ------------------------------------------------------------------ synthesis
    @traced_tts
    async def run_tts(self, text: str, context_id: str) -> AsyncGenerator[Frame, None]:
        voice = self._settings.voice if is_given(self._settings.voice) and self._settings.voice else "default"
        speed = self._settings.speed if is_given(self._settings.speed) and self._settings.speed else 1.0
        cfg = self._config()
        stream = AudioStream(self._http(), cfg, text, voice, float(speed))
        got_audio = False
        try:
            await self.start_tts_usage_metrics(text)
            src_rate = cfg.sample_rate
            async for frame in self._stream_audio_frames_from_iterator(
                stream, in_sample_rate=src_rate, context_id=context_id
            ):
                if not got_audio:
                    got_audio = True
                    await self.stop_ttfb_metrics()
                yield frame
        except ReadAloudAPIError as e:
            logger.error(f"{self}: {e.status} {e.code}: {e.message} (attempts={stream.attempts}, audio_started={got_audio})")
            yield ErrorFrame(error=f"ReadAloud TTS error ({e.status or 'n/a'} {e.code}): {e.message}")
        except Exception as e:  # noqa: BLE001
            logger.error(f"{self}: unexpected error: {e!r}")
            yield ErrorFrame(error=f"ReadAloud TTS unexpected error: {e}")
        finally:
            # Cancellation (barge-in) lands here as GeneratorExit/CancelledError: drop the connection.
            await stream.aclose()
            await self.stop_ttfb_metrics()
