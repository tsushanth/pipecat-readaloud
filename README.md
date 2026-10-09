# pipecat-readaloud

[ReadAloud](https://readaloudai.org) text-to-speech for [Pipecat](https://github.com/pipecat-ai/pipecat).
`ReadAloudHttpTTSService` streams audio from the ReadAloud API as it is generated, so a voice agent can start speaking
within a few hundred milliseconds. It supports 8 kHz output for Twilio and Telnyx calls, runtime voice and speed
updates, and stops generation when Pipecat interrupts it.

**Maintainer:** this integration is written and maintained by the author of ReadAloud, the speech API provider.

## Installation

```bash
uv add pipecat-readaloud        # or: pip install pipecat-readaloud
export READALOUD_API_KEY=rtts_...
```

Get an API key at [readaloudai.org/developers](https://readaloudai.org/developers). New keys include a small free
allowance.

## Usage

```python
import os
import aiohttp
from pipecat.pipeline.pipeline import Pipeline
from pipecat_readaloud import ReadAloudHttpTTSService

async with aiohttp.ClientSession() as session:
    tts = ReadAloudHttpTTSService(
        api_key=os.environ["READALOUD_API_KEY"],  # or omit and set READALOUD_API_KEY
        aiohttp_session=session,                   # optional; one is created if omitted
        sample_rate=8000,                          # 8000 for Twilio/Telnyx; 24000 native; 16000 also works
        settings=ReadAloudHttpTTSService.Settings(voice="default", speed=1.0),
    )
    pipeline = Pipeline([transport.input(), stt, user_aggregator, llm, tts, transport.output(), assistant_aggregator])
```

Constructor options: `base_url` (default `https://api.readaloudai.org`), `engine` (`piper` default, or `kokoro`),
`output_format` (`auto`, `pcm_24000`, `pcm_16000`, `pcm_8000`, `mulaw_8000`, `alaw_8000`), `max_retries`,
`aiohttp_session`. `auto` asks for 8 kHz when the pipeline runs at 8 kHz and 24 kHz otherwise, and lets Pipecat
resample. `voice` and `speed` can be changed at runtime with a `TTSUpdateSettingsFrame`. The earlier class name
`ReadAloudTTSService` still works as an alias.

## Running the example

[`examples/readaloud_tts_to_wav.py`](examples/readaloud_tts_to_wav.py) is a single-file pipeline that speaks a sentence
and saves it as a WAV file. It needs only a ReadAloud API key, with no STT, LLM or transport:

```bash
uv add pipecat-readaloud
export READALOUD_API_KEY=rtts_...
python examples/readaloud_tts_to_wav.py "Thanks for calling. How can I help you today?"
```

## Behaviour

- Audio frames are yielded as bytes arrive, sample-aligned.
- On interruption Pipecat cancels the in-flight synthesis and the connection is closed, which stops generation on the
  server.
- "At capacity" (HTTP 503 or 429), 502 and 504 responses are retried with exponential backoff (`Retry-After` honoured)
  until the first audio byte arrives. Authentication (401), quota (402), bad request and unknown voice (400, 404)
  errors surface immediately as an `ErrorFrame`.
- The API returns no word timestamps, so `TTSTextFrame`s are not time-aligned.

## Compatibility

Tested with Pipecat 1.12.0 and Python 3.14; requires `pipecat-ai>=1.12,<2`. The tests run against a local fake of the
ReadAloud HTTP protocol (`pip install -e '.[dev]' && pytest`), and the example and service were also run against the
live API.

## Changelog

See [CHANGELOG.md](CHANGELOG.md). Licensed under MIT.
