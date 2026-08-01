"""BrainSink's outbound buffer — same resilience the transcriber backend got.

A stream break on the brain path must not lose frames: unsent chunks wait in the
buffer and replay on reconnect (which re-opens ProcessCall with the same context,
and the gateway resumes the engine session). Dependency-light: fake frames and a
fake call, no grpc, no meeting_pb2 (built lazily and not touched here).

    python -m pytest libs/capture/tests/test_brain_sink_buffer.py -v
"""
from __future__ import annotations

import asyncio
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "src")))
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "core", "src")))

from talk_capture.brain_client import BrainSink  # noqa: E402


def _sink():
    cfg = SimpleNamespace(brain_grpc_target="x", brain_grpc_tls=False,
                          brain_api_token="")
    return BrainSink(cfg, {"session_id": "sess-xyz"})


def _frame(nbytes=1000):
    # Duck-types pb.CallFrame(audio=AudioChunk(pcm=...)): .audio.pcm is bytes.
    return SimpleNamespace(audio=SimpleNamespace(pcm=b"\x00" * nbytes))


class _FakeCall:
    def __init__(self, fail_times=0):
        self.written = []
        self._fail_times = fail_times

    async def write(self, frame):
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("stream down")
        self.written.append(frame)


def _run(fn):
    return asyncio.run(fn())


def test_enqueue_and_overflow():
    async def go():
        s = _sink()
        s._MAX_OUTBOX_BYTES = 250
        s._enqueue(_frame(100))
        s._enqueue(_frame(100))
        s._enqueue(_frame(100))  # 300 > 250 → oldest dropped
        return s
    s = _run(go)
    assert s._lost_frames == 1 and len(s._outbox) == 2 and s._outbox_bytes == 200


def test_pump_delivers_buffered_frames():
    async def go():
        s = _sink()
        s._call = _FakeCall()
        s._enqueue(_frame())
        s._enqueue(_frame())
        await s._pump()
        return s
    s = _run(go)
    assert len(s._call.written) == 2 and len(s._outbox) == 0


def test_write_failure_buffers_and_reconnects():
    async def go():
        s = _sink()
        call = _FakeCall(fail_times=1)
        s._call = call
        reconnected = []

        async def rec(dead):
            reconnected.append(dead)

        s._reconnect = rec
        s._enqueue(_frame())
        await s._pump()
        return s, call, reconnected
    s, call, reconnected = _run(go)
    assert len(s._outbox) == 1 and s._lost_frames == 0  # not lost — buffered
    assert reconnected == [call]


def test_frame_replays_after_recovery():
    async def go():
        s = _sink()
        s._call = _FakeCall(fail_times=99)

        async def norec(dead):
            pass

        s._reconnect = norec
        s._enqueue(_frame())
        await s._pump()          # write fails, buffered
        assert len(s._outbox) == 1 and s._lost_frames == 0
        good = _FakeCall()       # stream recovers
        s._call = good
        await s._pump()
        return good
    good = _run(go)
    assert len(good.written) == 1  # replayed


def test_finalize_without_call_counts_buffered_as_lost():
    async def go():
        s = _sink()
        s._call = None

        async def nopump():
            pass

        s._pump = nopump  # can't deliver — gateway unreachable
        s._enqueue(_frame())
        s._enqueue(_frame())
        res = await s.finalize(call_end_ms=1)
        return s, res
    s, res = _run(go)
    assert res is None and s._lost_frames == 2
