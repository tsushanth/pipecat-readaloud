"""A local fake ReadAloud server speaking the documented public HTTP protocol (native + ElevenLabs-compat).

Magic text triggers: "CAPACITY2" -> first 2 requests 503 (Retry-After: 0), "CAPACITY_ALWAYS" -> always 503,
"QUOTA" -> 402, "SLOW" -> a long stream (for barge-in tests), "MIDFAIL" -> connection dropped after first chunk.
"""
from __future__ import annotations

import asyncio
from aiohttp import web

KEY = "rtts_test_key"
RATES = {"pcm_24000": 24000, "pcm_8000": 8000, "mulaw_8000": 8000, "alaw_8000": 8000, "pcm_16000": 16000}


class FakeReadAloud:
    def __init__(self):
        self.requests: list[dict] = []
        self.capacity_left = 0
        self.disconnected = asyncio.Event()
        self.completed = 0
        self.runner = None
        self.url = ""

    async def start(self):
        app = web.Application()
        app.router.add_post("/v1/text-to-speech", self.native)
        app.router.add_post("/v1/text-to-speech/{voice}/stream", self.compat)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
        return self

    async def stop(self):
        await self.runner.cleanup()

    # -- shared
    async def _audio(self, request, body, fmt, native: bool):
        text = body.get("text", "")
        rec = {"native": native, "format": fmt, "body": body, "headers": dict(request.headers), "query": dict(request.query)}
        self.requests.append(rec)
        if text == "CAPACITY_ALWAYS" or (text.startswith("CAPACITY2") and self.capacity_left > 0) :
            self.capacity_left -= 1
            return self._err(503 if native else 429, "TTS worker busy, please retry", native, capacity=True)
        if text == "QUOTA":
            return self._err(402, "Free tier exhausted for this key.", native, code="quota_exceeded")
        rate = RATES[fmt]
        n_chunks = 400 if text == "SLOW" else 3
        resp = web.StreamResponse(status=200, headers={"Content-Type": "audio/pcm", "X-Sample-Rate": str(rate),
                                                       "request-id": "req_fake_1"})
        await resp.prepare(request)
        try:
            for i in range(n_chunks):
                size = 641 if i == 0 else 3200 // (1 if fmt.startswith("pcm") else 2)   # odd first chunk: tests alignment
                if fmt == "mulaw_8000":
                    data = b"\xff" * size
                elif fmt == "alaw_8000":
                    data = b"\xd5" * size
                else:
                    data = (b"\x01\x00" * (size // 2 + 1))[:size]
                await resp.write(data)
                if text == "MIDFAIL" and i == 0:
                    request.transport.close()
                    return resp
                await asyncio.sleep(0.02)
            await resp.write_eof()
            self.completed += 1
        except (ConnectionResetError, asyncio.CancelledError):
            self.disconnected.set()
            raise
        return resp

    def _err(self, status, msg, native, code=None, capacity=False):
        headers = {"Retry-After": "0"} if capacity else {}
        if native:
            return web.json_response({"error": msg}, status=status, headers=headers)
        c = "too_many_concurrent_requests" if capacity else (code or "error")
        return web.json_response({"detail": {"status": c, "message": msg}}, status=status, headers=headers)

    async def native(self, request: web.Request):
        if request.headers.get("Authorization") != f"Bearer {KEY}":
            return web.json_response({"error": "invalid or missing API key"}, status=401)
        body = await request.json()
        if body.get("format") not in RATES or body.get("format") == "pcm_16000":
            return web.json_response({"error": "bad format"}, status=400)
        return await self._audio(request, body, body["format"], True)

    async def compat(self, request: web.Request):
        if request.headers.get("xi-api-key") != KEY:
            return web.json_response({"detail": {"status": "invalid_api_key", "message": "Invalid API key"}}, status=401)
        body = await request.json()
        return await self._audio(request, body, request.query.get("output_format", "mp3_44100_128"), False)
