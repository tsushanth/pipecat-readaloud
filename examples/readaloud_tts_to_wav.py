"""Foundational example: speak a sentence with ReadAloud TTS inside a Pipecat pipeline and save it as a WAV file.

Needs only a ReadAloud API key (no STT, LLM or transport):

    export READALOUD_API_KEY=rtts_...
    python examples/readaloud_tts_to_wav.py "Thanks for calling. How can I help you today?"

The output is written to readaloud_example.wav (mono, 16-bit, 24 kHz).
"""
import asyncio
import os
import sys
import wave

import aiohttp
from pipecat.frames.frames import EndFrame, Frame, TTSAudioRawFrame, TTSSpeakFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.workers.runner import WorkerRunner

from pipecat_readaloud import ReadAloudHttpTTSService

OUTPUT = "readaloud_example.wav"
SAMPLE_RATE = 24000


class WavWriter(FrameProcessor):
    """Collects the TTS audio frames and writes them to a WAV file when the pipeline ends."""

    def __init__(self, path: str, sample_rate: int):
        super().__init__()
        self._path, self._rate, self._chunks = path, sample_rate, []

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, TTSAudioRawFrame):
            self._chunks.append(frame.audio)
        elif isinstance(frame, EndFrame):
            with wave.open(self._path, "wb") as f:
                f.setnchannels(1)
                f.setsampwidth(2)
                f.setframerate(self._rate)
                f.writeframes(b"".join(self._chunks))
        await self.push_frame(frame, direction)


async def main(text: str):
    async with aiohttp.ClientSession() as session:
        tts = ReadAloudHttpTTSService(
            api_key=os.environ["READALOUD_API_KEY"],
            aiohttp_session=session,
            settings=ReadAloudHttpTTSService.Settings(voice="default", speed=1.0),
        )
        worker = PipelineWorker(
            Pipeline([tts, WavWriter(OUTPUT, SAMPLE_RATE)]),
            params=PipelineParams(audio_out_sample_rate=SAMPLE_RATE),
        )
        await worker.queue_frames([TTSSpeakFrame(text), EndFrame()])
        runner = WorkerRunner(handle_sigint=False)
        await runner.add_workers(worker)
        await runner.run()
    print(f"wrote {OUTPUT}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "Thanks for calling. How can I help you today?"))
