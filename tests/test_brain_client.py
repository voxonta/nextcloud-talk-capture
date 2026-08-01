"""Capture-side brain client against a real in-process meeting.v1 gateway.

Exercises the transport end to end: BrainSink streams a call (context → audio →
end) and BrainArtifacts fetches/acks — over a genuine grpc.aio server, no mocks
of the wire. Needs grpc + the generated stubs, so it is skipped where they are
absent and runs in CI (which generates them).

    python -m pytest libs/capture/tests/test_brain_client.py -v
"""
from __future__ import annotations

import asyncio
import os
import sys
from types import SimpleNamespace

import numpy as np
import pytest

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "src")))

grpc = pytest.importorskip("grpc")

# The stubs ship inside the package, so there is nothing to skip on: if they are
# missing or stale the transmit half is broken and these tests must say so.
from talk_capture._pb import meeting_pb2 as pb  # noqa: E402
from talk_capture._pb import meeting_pb2_grpc as pb_grpc  # noqa: E402

from talk_capture import brain_client  # noqa: E402
from talk_capture.brain_client import BrainArtifacts, BrainSink  # noqa: E402


class _FakeGateway(pb_grpc.MeetingGatewayServicer):
    def __init__(self):
        self.context = None
        self.audio = []
        self.end = None
        self.acked = []

    async def ProcessCall(self, request_iterator, context):
        async for frame in request_iterator:
            which = frame.WhichOneof("frame")
            if which == "context":
                self.context = frame.context
            elif which == "audio":
                self.audio.append(frame.audio)
            elif which == "end":
                self.end = frame.end
        sid = self.context.session_id if self.context else ""
        return pb.CallAccepted(session_id=sid, status=pb.CALL_STATUS_ANALYZING)

    async def GetArtifacts(self, request, context):
        return pb.ArtifactsResponse(
            session_id=request.session_id, status=pb.CALL_STATUS_TRANSCRIBED,
            artifacts=[pb.Artifact(
                name="2025-05-19 - Планёрка.md", kind=pb.ARTIFACT_KIND_TRANSCRIPT,
                content=b"# transcript", sha256="deadbeef",
                media_type="text/markdown; charset=utf-8")])

    async def AckArtifacts(self, request, context):
        self.acked.append((request.session_id, list(request.sha256)))
        return pb.Ack()

    async def Health(self, request, context):
        return pb.HealthStatus(ready=True, version="fake")


async def _serve(gateway):
    server = grpc.aio.server()
    pb_grpc.add_MeetingGatewayServicer_to_server(gateway, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, f"127.0.0.1:{port}"


def _cfg(addr):
    return SimpleNamespace(brain_grpc_target=addr, brain_grpc_tls=False,
                           brain_api_token="")


def _context():
    return {"session_id": "sess-xyz", "source": "nc-talk",
            "room_token": "abc", "room_name": "Планёрка",
            "room_kind": pb.ROOM_KIND_GROUP, "participants": ["Анна", "Борис"],
            "language": "ru", "timezone": "Europe/Moscow",
            "call_start_ms": 1_747_638_000_000}


def test_sink_streams_context_audio_and_end(monkeypatch):
    monkeypatch.setattr(brain_client, "frame_to_pcm",
                        lambda frame, rate: np.zeros(320, dtype=np.float32))

    async def go():
        gw = _FakeGateway()
        server, addr = await _serve(gw)
        try:
            sink = BrainSink(_cfg(addr), _context())
            frame = SimpleNamespace(pts=None, time_base=None)
            await sink.process_audio_frame("spk-a", frame, 0.0)
            await sink.process_audio_frame("spk-a", frame, 0.0)
            result = await sink.finalize(
                call_end_ms=1_747_638_120_000, uncaptured=["Виктор"],
                present_count=3, participants=["Анна", "Борис", "Виктор"])
            return gw, result
        finally:
            await server.stop(grace=0)

    gw, result = asyncio.run(go())
    assert gw.context.room_name == "Планёрка"
    assert list(gw.context.participants) == ["Анна", "Борис"]
    assert len(gw.audio) == 2
    assert gw.audio[0].speaker_id == "spk-a"
    assert gw.end.present_count == 3 and list(gw.end.uncaptured) == ["Виктор"]
    # the final roster (incl. the late/silent Виктор) rides the end frame
    assert list(gw.end.participants) == ["Анна", "Борис", "Виктор"]
    assert result == ("sess-xyz", pb.CALL_STATUS_ANALYZING)


def test_finalize_without_frames_is_a_noop():
    async def go():
        gw = _FakeGateway()
        server, addr = await _serve(gw)
        try:
            sink = BrainSink(_cfg(addr), _context())
            return await sink.finalize(call_end_ms=1)
        finally:
            await server.stop(grace=0)

    assert asyncio.run(go()) is None  # stream never opened → nothing to finalize


def test_artifacts_fetch_and_ack():
    async def go():
        gw = _FakeGateway()
        server, addr = await _serve(gw)
        try:
            arts = BrainArtifacts(_cfg(addr))
            status, files, _ = await arts.fetch("sess-xyz", have_sha256=set())
            await arts.ack("sess-xyz", ["deadbeef"])
            return status, files, gw.acked
        finally:
            await server.stop(grace=0)

    status, files, acked = asyncio.run(go())
    assert status == pb.CALL_STATUS_TRANSCRIBED
    assert files[0].name == "2025-05-19 - Планёрка.md"
    assert files[0].sha256 == "deadbeef"
    assert acked == [("sess-xyz", ["deadbeef"])]
