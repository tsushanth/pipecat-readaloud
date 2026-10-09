import asyncio
import os
import sys

import aiohttp
import pytest

sys.path.insert(0, os.path.dirname(__file__))
from fake_readaloud import KEY, FakeReadAloud  # noqa: E402

from pipecat.frames.frames import ErrorFrame, TTSAudioRawFrame  # noqa: E402
from pipecat_readaloud import ReadAloudTTSService  # noqa: E402
from pipecat_readaloud._client import g711_to_pcm16  # noqa: E402


@pytest.fixture
async def server():
    s = await FakeReadAloud().start()
    yield s
    await s.stop()


@pytest.fixture
async def session():
    async with aiohttp.ClientSession() as s:
        yield s


def make(server, session, rate=24000, **kw):
    kw.setdefault("api_key", KEY)
    svc = ReadAloudTTSService(base_url=server.url, aiohttp_session=session, sample_rate=rate, backoff_base=0.01, **kw)
    svc._sample_rate = rate  # normally set from StartFrame
    return svc


async def collect(svc, text="hello"):
    return [f async for f in svc.run_tts(text, "ctx1")]


async def test_native_pcm24k_streams_aligned_frames(server, session):
    frames = await collect(make(server, session))
    audio = [f for f in frames if isinstance(f, TTSAudioRawFrame)]
    assert audio and all(len(f.audio) % 2 == 0 for f in audio)
    assert all(f.sample_rate == 24000 and f.num_channels == 1 for f in audio)
    assert sum(len(f.audio) for f in audio) >= 641 + 2 * 3200 - 2
    r = server.requests[0]
    assert r["native"] and r["format"] == "pcm_24000"
    assert r["headers"]["Authorization"] == f"Bearer {KEY}"
    assert r["body"] == {"text": "hello", "voice": "default", "speed": 1.0, "format": "pcm_24000", "engine": "piper"}


async def test_auto_format_8k_for_telephony(server, session):
    frames = await collect(make(server, session, rate=8000))
    assert server.requests[0]["format"] == "pcm_8000"
    assert all(f.sample_rate == 8000 for f in frames if isinstance(f, TTSAudioRawFrame))


async def test_auto_16k_asks_24k_and_resamples(server, session):
    frames = await collect(make(server, session, rate=16000))
    assert server.requests[0]["format"] == "pcm_24000"
    assert all(f.sample_rate == 16000 for f in frames if isinstance(f, TTSAudioRawFrame))


async def test_explicit_pcm16k_uses_compat_endpoint(server, session):
    frames = await collect(make(server, session, rate=16000, output_format="pcm_16000"))
    r = server.requests[0]
    assert not r["native"] and r["query"]["output_format"] == "pcm_16000"
    assert r["headers"]["xi-api-key"] == KEY
    assert r["body"]["voice_settings"] == {"speed": 1.0}
    assert any(isinstance(f, TTSAudioRawFrame) and f.sample_rate == 16000 for f in frames)


async def test_mulaw_decoded_to_pcm(server, session):
    frames = await collect(make(server, session, rate=8000, output_format="mulaw_8000"))
    audio = b"".join(f.audio for f in frames if isinstance(f, TTSAudioRawFrame))
    assert set(audio) == {0}          # 0xFF mu-law == digital silence
    assert g711_to_pcm16(b"\x00", "mulaw") == (-32124).to_bytes(2, "little", signed=True)
    assert g711_to_pcm16(b"\x80", "mulaw") == (32124).to_bytes(2, "little", signed=True)


async def test_capacity_retry_with_backoff_then_success(server, session):
    server.capacity_left = 2
    frames = await collect(make(server, session), "CAPACITY2 hi")
    assert len(server.requests) == 3
    assert any(isinstance(f, TTSAudioRawFrame) for f in frames) and not any(isinstance(f, ErrorFrame) for f in frames)


async def test_capacity_exhausted_yields_error_frame(server, session):
    frames = await collect(make(server, session, max_retries=2), "CAPACITY_ALWAYS")
    assert len(server.requests) == 3
    errs = [f for f in frames if isinstance(f, ErrorFrame)]
    assert errs and "capacity" in errs[0].error


async def test_compat_capacity_429_is_retried(server, session):
    server.capacity_left = 1
    frames = await collect(make(server, session, output_format="pcm_16000", rate=16000), "CAPACITY2 x")
    assert len(server.requests) == 2 and any(isinstance(f, TTSAudioRawFrame) for f in frames)


@pytest.mark.parametrize("text,status", [("QUOTA", "402")])
async def test_quota_not_retried(server, session, text, status):
    frames = await collect(make(server, session), text)
    assert len(server.requests) == 1
    assert status in [f for f in frames if isinstance(f, ErrorFrame)][0].error


async def test_bad_key_is_error_not_retried(server, session):
    frames = await collect(make(server, session, api_key="nope"))
    assert len(server.requests) == 0
    err = [f for f in frames if isinstance(f, ErrorFrame)]
    assert err and "401" in err[0].error


async def test_barge_in_cancel_closes_connection(server, session):
    svc = make(server, session)

    async def consume():
        async for _ in svc.run_tts("SLOW", "c"):
            pass

    t = asyncio.create_task(consume())
    await asyncio.sleep(0.15)       # some audio is flowing
    t.cancel()                       # what an InterruptionFrame does to the in-flight synthesis
    with pytest.raises(asyncio.CancelledError):
        await t
    await asyncio.wait_for(server.disconnected.wait(), 2)
    assert server.completed == 0


async def test_runtime_voice_and_speed_settings(server, session):
    svc = make(server, session, settings=ReadAloudTTSService.Settings(voice="af_heart", speed=1.25))
    await collect(svc)
    b = server.requests[0]["body"]
    assert b["voice"] == "af_heart" and b["speed"] == 1.25


def test_requires_api_key(monkeypatch):
    monkeypatch.delenv("READALOUD_API_KEY", raising=False)
    with pytest.raises(ValueError):
        ReadAloudTTSService()


async def test_mid_stream_failure_no_retry_after_audio(server, session):
    frames = await collect(make(server, session), "MIDFAIL")
    assert len(server.requests) == 1              # never replays audio that already started
    assert any(isinstance(f, TTSAudioRawFrame) for f in frames)
    assert any(isinstance(f, ErrorFrame) for f in frames)
