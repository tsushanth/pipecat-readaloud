"""Interactive demo: ReadAloud speaking through your speakers in a Pipecat pipeline, with interruption.

Needs only a ReadAloud API key and PortAudio (macOS: `brew install portaudio`):

    pip install "pipecat-ai[local]" pipecat-readaloud aiohttp
    export READALOUD_API_KEY=rtts_...
    python examples/interruption_demo.py

It starts speaking a long paragraph straight away. While it speaks:
    press Enter          -> interrupt: Pipecat cancels the synthesis and the audio stops
    type a sentence+Enter -> speak that sentence instead (this also interrupts whatever is playing)
    type q+Enter          -> quit
"""
import asyncio
import os
import sys
import time

import aiohttp
from loguru import logger
from pipecat.frames.frames import (BotStartedSpeakingFrame, BotStoppedSpeakingFrame, EndFrame, Frame, InterruptionFrame,
                                   TTSAudioRawFrame, TTSSpeakFrame, TTSStartedFrame, TTSStoppedFrame)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.transports.local.audio import LocalAudioTransport, LocalAudioTransportParams
from pipecat.workers.runner import WorkerRunner

from pipecat_readaloud import ReadAloudHttpTTSService

SAMPLE_RATE = 24000
PARAGRAPH = (
    "Thanks for calling Riverside Dental. I can help you book a cleaning, move an appointment, or answer questions "
    "about insurance. Our office is open Monday through Friday from eight in the morning until five in the evening, "
    "and we also see patients on the first Saturday of every month. If this is a dental emergency, press one now and "
    "I will connect you straight to the on call dentist. Otherwise, tell me what you need and I will take it from there."
)


class Tracer(FrameProcessor):
    """Prints what the pipeline is doing, with timings, so the video shows it."""

    def __init__(self):
        super().__init__()
        self.t_request = None
        self.t_first_audio = None
        self.t_playback = None
        self.t_interrupt = None
        self.generating = False
        self.audio_s = 0.0

    def request(self):
        self.t_request, self.t_first_audio, self.audio_s = time.monotonic(), None, 0.0

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        now = time.monotonic()
        if isinstance(frame, TTSStartedFrame):
            self.generating = True
            print("  [tts]   request sent")
        elif isinstance(frame, TTSAudioRawFrame):
            if self.t_first_audio is None and self.t_request is not None:
                self.t_first_audio = now
                print(f"  [tts]   first audio after {(now - self.t_request) * 1000:.0f} ms")
            self.audio_s += len(frame.audio) / 2 / frame.sample_rate
        elif isinstance(frame, TTSStoppedFrame):
            self.generating = False
            print(f"  [tts]   generated {self.audio_s:.1f} s of audio")
        elif isinstance(frame, BotStartedSpeakingFrame):
            self.t_playback, self.t_interrupt = now, None
            print("  [audio] playback started")
        elif isinstance(frame, BotStoppedSpeakingFrame) and self.t_playback is not None:
            if self.t_interrupt is not None:
                print(f"  [audio] audio output STOPPED {(now - self.t_interrupt) * 1000:.0f} ms after the interruption")
            else:
                print(f"  [audio] playback finished ({now - self.t_playback:.1f} s)")
            self.t_playback = None
        elif isinstance(frame, InterruptionFrame) and (self.generating or self.t_playback is not None):
            self.t_interrupt = now
            what = "the in-flight request is cancelled" if self.generating else "generation had already finished"
            print(f"  [pipecat] INTERRUPTION: {what}, queued audio is dropped")
            self.generating = False
        await self.push_frame(frame, direction)


async def read_lines(queue: asyncio.Queue):
    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if line == "":
            await queue.put("q")
            return
        await queue.put(line.rstrip("\n"))
        if line.strip().lower() == "q":
            return


async def main():
    logger.remove()
    logger.add(sys.stderr, level="WARNING")  # quiet terminal for the recording, but never hide problems
    async with aiohttp.ClientSession() as session:
        tts = ReadAloudHttpTTSService(
            api_key=os.environ["READALOUD_API_KEY"],
            aiohttp_session=session,
            settings=ReadAloudHttpTTSService.Settings(voice="default", speed=1.0),
        )
        transport = LocalAudioTransport(
            LocalAudioTransportParams(audio_out_enabled=True, audio_out_sample_rate=SAMPLE_RATE)
        )
        tracer = Tracer()
        worker = PipelineWorker(
            Pipeline([tts, tracer, transport.output()]),
            params=PipelineParams(audio_out_sample_rate=SAMPLE_RATE),
        )
        runner = WorkerRunner(handle_sigint=True)
        await runner.add_workers(worker)
        run = asyncio.create_task(runner.run())
        await asyncio.sleep(1.0)

        print("ReadAloud + Pipecat demo.  Enter = interrupt, text+Enter = speak it, q+Enter = quit.\n")
        lines: asyncio.Queue = asyncio.Queue()
        asyncio.create_task(read_lines(lines))

        async def say(text: str):
            print(f'> speaking: "{text[:70]}{"..." if len(text) > 70 else ""}"')
            tracer.request()
            await worker.queue_frame(TTSSpeakFrame(text))

        await say(PARAGRAPH)
        while True:
            line = await lines.get()
            if line.strip().lower() == "q":
                break
            if line.strip() == "":
                await worker.queue_frame(InterruptionFrame())
            else:
                await worker.queue_frame(InterruptionFrame())
                await say(line.strip())
        await worker.queue_frame(InterruptionFrame())
        await worker.queue_frame(EndFrame())
        await run


if __name__ == "__main__":
    asyncio.run(main())
