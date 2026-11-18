"""Runs the service inside a real Pipecat pipeline (TTSSpeakFrame -> audio frames), then interrupts it."""
import asyncio
import os
import sys

import aiohttp
import pytest

sys.path.insert(0, os.path.dirname(__file__))
from fake_readaloud import KEY, FakeReadAloud  # noqa: E402

from pipecat.frames.frames import (InterruptionFrame, TTSAudioRawFrame, TTSSpeakFrame, TTSStartedFrame,  # noqa: E402
                                   TTSStoppedFrame)
from pipecat.pipeline.pipeline import Pipeline  # noqa: E402
from pipecat.pipeline.runner import PipelineRunner  # noqa: E402
from pipecat.pipeline.task import PipelineParams, PipelineTask  # noqa: E402
from pipecat.processors.frame_processor import FrameProcessor  # noqa: E402
from pipecat_readaloud import ReadAloudHttpTTSService  # noqa: E402


class Sink(FrameProcessor):
    def __init__(self):
        super().__init__()
        self.frames = []

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        self.frames.append(frame)
        await self.push_frame(frame, direction)


async def _run(text, interrupt_after=None, rate=24000):
    server = await FakeReadAloud().start()
    sink = Sink()
    async with aiohttp.ClientSession() as session:
        tts = ReadAloudHttpTTSService(api_key=KEY, base_url=server.url, aiohttp_session=session)
        task = PipelineTask(Pipeline([tts, sink]), params=PipelineParams(audio_out_sample_rate=rate))

        async def driver():
            await task.queue_frame(TTSSpeakFrame(text))
            if interrupt_after is None:
                await asyncio.sleep(1.5)
            else:
                await asyncio.sleep(interrupt_after)
                await task.queue_frame(InterruptionFrame())
                await asyncio.sleep(0.5)
            await task.stop_when_done()

        d = asyncio.create_task(driver())
        await asyncio.wait_for(PipelineRunner(handle_sigint=False).run(task), 20)
        await d
    await server.stop()
    return server, sink


async def test_pipeline_speak_frame_produces_audio():
    server, sink = await _run("hello world")
    kinds = [type(f) for f in sink.frames]
    assert TTSStartedFrame in kinds and TTSAudioRawFrame in kinds and TTSStoppedFrame in kinds
    assert server.completed == 1


async def test_pipeline_interruption_drops_connection():
    server, sink = await _run("SLOW", interrupt_after=0.4)
    assert server.disconnected.is_set() and server.completed == 0


async def test_pipeline_16k_output_is_resampled_with_same_duration():
    # The pipeline rate is 16 kHz while the service asks the 24 kHz stream (auto). Frames reaching the next processor
    # must be 16 kHz and cover the same time as the 24 kHz audio the server sent (641 + 2 * 3200 bytes).
    server, sink = await _run("hello world", rate=16000)
    audio = [f for f in sink.frames if isinstance(f, TTSAudioRawFrame)]
    assert server.requests[0]["format"] == "pcm_24000"
    assert audio and all(f.sample_rate == 16000 and f.num_channels == 1 for f in audio)
    seconds = sum(len(f.audio) for f in audio) / 2 / 16000
    assert seconds == pytest.approx((641 + 2 * 3200) / 2 / 24000, rel=0.1)
