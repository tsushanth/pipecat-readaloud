# pipecat-readaloud

[ReadAloud](https://readaloudai.org) text-to-speech for [Pipecat](https://github.com/pipecat-ai/pipecat): Piper voices,
low first-audio latency, 8 kHz telephony output, barge-in by connection close. Status: **alpha, unpublished** (see
`../docs/STATUS_MATRIX.md` for what is verified).

```bash
pip install pipecat-readaloud          # not yet on PyPI: install from this directory with `pip install .`
export READALOUD_API_KEY=rtts_...
```

```python
from pipecat_readaloud import ReadAloudTTSService

tts = ReadAloudTTSService(
    api_key=os.environ["READALOUD_API_KEY"],   # or omit and set READALOUD_API_KEY
    sample_rate=8000,                           # 8000 for Twilio/Telnyx; 24000 native; 16000 works too
    settings=ReadAloudTTSService.Settings(voice="default", speed=1.0),
)
# ... Pipeline([transport.input(), stt, user_agg, llm, tts, transport.output(), assistant_agg])
```

Options: `base_url` (default `https://api.readaloudai.org`), `engine` (`piper` default | `kokoro`),
`output_format` (`auto` | `pcm_24000` | `pcm_8000` | `mulaw_8000` | `alaw_8000` | `pcm_16000`), `max_retries`,
`aiohttp_session`. `auto` requests 8 kHz when the pipeline runs at 8 kHz, otherwise 24 kHz, and lets Pipecat resample.

Behaviour: audio frames are yielded as bytes arrive (sample-aligned). On interruption Pipecat cancels the
in-flight synthesis and the connection is closed, which stops generation server-side. "At capacity" (HTTP 503/429),
502 and 504 are retried with exponential backoff (Retry-After honoured) until the first audio byte; auth (401), quota
(402), bad request and unknown voice (400/404) surface immediately as `ErrorFrame`. No word timestamps (the API
returns none), so `TTSTextFrame`s are not time-aligned. Tested with `pipecat-ai==1.12.0`; requires `pipecat-ai>=1.12,<2`.

Tests: `pip install -e '.[dev]' && pytest` (a local fake ReadAloud server speaks the public HTTP protocol).
