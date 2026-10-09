# Changelog

## 0.1.1 - 2026-10-09
- Renamed the service class to `ReadAloudHttpTTSService` (Pipecat's naming convention for HTTP TTS services). `ReadAloudTTSService` remains as an alias.
- Added a single-file example (`examples/readaloud_tts_to_wav.py`) that needs only a ReadAloud API key.
- README rewritten: install, usage, running the example, compatibility.
- Verified against the live ReadAloud API with Pipecat 1.12.0.

## 0.1.0 - 2026-10-09
- First release: HTTP streaming TTS service for Pipecat, Piper and Kokoro voices, 24 kHz / 16 kHz / 8 kHz PCM and mu-law / A-law output, retries before the first audio byte, cancellation on interruption.
