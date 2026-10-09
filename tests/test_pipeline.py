"""Runs the service inside a real Pipecat pipeline (TTSSpeakFrame -> audio frames), then interrupts it."""
import asyncio
import os
import sys

import aiohttp

sys.path.insert(0, os.path.dirname(__file__))
from fake_readaloud import KEY, FakeReadAloud  # noqa: E402

from pipecat.frames.frames import (InterruptionFrame, TTSAudioRawFrame, TTSSpeakFrame, TTSStartedFrame,  # noqa: E402
                                   TTSStoppedFrame)
from pipecat.pipeline.pipeline import Pipeline  # noqa: E402
from pipecat.pipeline.runner import PipelineRunner  # noqa: E402
from pipecat.pipeline.task import PipelineParams, PipelineTask  # noqa: E402
from pipecat.processors.frame_processor import FrameProcessor  # noqa: E402
from pipecat_readaloud import ReadAloudTTSService  # noqa: E402


class Sink(FrameProcessor):
    def __init__(self):
        super().__init__()
        self.frames = []

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        self.frames.append(frame)
        await self.push_frame(frame, direction)


async def _run(text, interrupt_after=None):
    server = await FakeReadAloud().start()
    sink = Sink()
    async with aiohttp.ClientSession() as session:
        tts = ReadAloudTTSService(api_key=KEY, base_url=server.url, aiohttp_session=session)
        task = PipelineTask(Pipeline([tts, sink]), params=PipelineParams(audio_out_sample_rate=24000))

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
