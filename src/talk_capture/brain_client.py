"""Capture-side client for the SaaS meeting gateway (meeting.v1).

The thin client's half of the split: instead of transcribing and finalizing a
call itself, the capture service streams the call's audio to the brain and later
pulls back the meeting's files to write into the customer's Nextcloud.

Two parts:

  * ``BrainSink`` — a per-call audio sink shaped like the transcriber backend
    (``process_audio_frame`` / ``finalize``), so the capture loop feeds it the
    same way. It opens one ProcessCall stream, sends the meeting context first,
    forwards each speaker's audio as it arrives, and closes with an end frame.

  * ``BrainArtifacts`` — polling: ``fetch`` returns the call's status and any new
    files (the ones the client does not already hold), ``ack`` confirms a durable
    write so the gateway can retire them.

The engine is never addressed directly here — the brain is the only endpoint.
"""
from __future__ import annotations

import asyncio
import collections
import logging
import time

from talk_capture.audio import TARGET_RATE, _frame_pts_ms, frame_to_pcm

logger = logging.getLogger(__name__)


def _stubs():
    """The meeting.v1 stubs, as (pb, pb_grpc).

    Flat top-level stubs win when they exist: a service image generates them at
    build time next to its code, and that copy is what our deploys hot-swap, so
    it must not be shadowed by a package copy from an older image layer. A plain
    `pip install` has no top-level stubs and falls through to the ones shipped
    inside the package. Both come from the same contract; CI keeps the packaged
    pair current.
    """
    try:
        import meeting_pb2 as pb
        import meeting_pb2_grpc as pb_grpc
    except ImportError:
        from talk_capture._pb import meeting_pb2 as pb
        from talk_capture._pb import meeting_pb2_grpc as pb_grpc
    return pb, pb_grpc


def _channel(target: str, tls: bool, opts):
    import grpc

    if tls:
        return grpc.aio.secure_channel(target, grpc.ssl_channel_credentials(),
                                       options=opts)
    return grpc.aio.insecure_channel(target, options=opts)


_GRPC_OPTS = [
    ("grpc.keepalive_time_ms", 30000),
    ("grpc.keepalive_timeout_ms", 20000),
    ("grpc.keepalive_permit_without_calls", 1),
    ("grpc.http2.max_pings_without_data", 0),
]


class BrainSink:
    """Streams one call's audio to the gateway's ProcessCall.

    Mirrors GrpcTranscriberBackend's frame interface so the capture loop drives
    it unchanged. ``context`` is the meeting's metadata (room, participants,
    language, timezone) known at join; the end frame carries what is only known
    when the call finishes.
    """

    _RECONNECT_COOLDOWN_S = 2.0
    _MAX_OUTBOX_BYTES = 128 * 1024 * 1024

    def __init__(self, config, context: dict):
        self._target = config.brain_grpc_target
        self._tls = getattr(config, "brain_grpc_tls", False)
        self._token = config.brain_api_token
        self._ctx = context
        self._session_id = context["session_id"]
        self._channel_obj = None
        self._call = None
        self._pb = None
        self._write_lock = asyncio.Lock()
        self._init_lock = asyncio.Lock()
        # Outbound buffer — same resilience as the transcriber backend: a frame
        # that can't be sent waits here and replays when the stream recovers, so
        # a break over the network path does not lose audio. On reconnect the
        # ProcessCall is re-opened with the SAME context, and the gateway resumes
        # the engine session (it kept it RUNNING on the broken stream).
        self._outbox: collections.deque = collections.deque()
        self._outbox_bytes = 0
        self._lost_frames = 0
        self._send_failures = 0
        self._reconnects = 0
        self._last_reconnect = 0.0

    def _pbmod(self):
        if self._pb is None:
            self._pb, _ = _stubs()
        return self._pb

    async def _open_call(self) -> None:
        pb = self._pbmod()
        _, pb_grpc = _stubs()
        self._channel_obj = _channel(self._target, self._tls, _GRPC_OPTS)
        stub = pb_grpc.MeetingGatewayStub(self._channel_obj)
        md = [("authorization", f"Bearer {self._token}")] if self._token else None
        call = stub.ProcessCall(metadata=md)
        c = self._ctx
        async with self._write_lock:
            await call.write(pb.CallFrame(context=pb.MeetingContext(
                session_id=c["session_id"], source=c.get("source", "nc-talk"),
                room_token=c.get("room_token", ""), room_name=c.get("room_name", ""),
                room_kind=c.get("room_kind", pb.ROOM_KIND_GROUP),
                participants=list(c.get("participants", [])),
                language=c.get("language", "ru"),
                timezone=c.get("timezone", "Europe/Moscow"),
                call_start_ms=c.get("call_start_ms", 0))))
        self._call = call

    async def _ensure_call(self) -> None:
        if self._call is not None:
            return
        async with self._init_lock:
            if self._call is not None:
                return
            await self._open_call()

    async def _reconnect(self, dead_call) -> None:
        async with self._init_lock:
            if self._call is not dead_call:
                return
            now = time.time()
            if now - self._last_reconnect < self._RECONNECT_COOLDOWN_S:
                return
            self._last_reconnect = now
            old = self._channel_obj
            self._channel_obj = None
            self._call = None
            if old is not None:
                try:
                    await old.close()
                except Exception:
                    pass
            try:
                await self._open_call()
            except Exception:
                logger.exception("brain sink: reconnect failed [%s]",
                                 self._session_id[:12])
                return
            self._reconnects += 1
            logger.warning("brain sink: reconnected [%s] (#%d)",
                           self._session_id[:12], self._reconnects)

    def _enqueue(self, frame) -> None:
        self._outbox.append(frame)
        self._outbox_bytes += len(frame.audio.pcm)
        while self._outbox_bytes > self._MAX_OUTBOX_BYTES and len(self._outbox) > 1:
            old = self._outbox.popleft()
            self._outbox_bytes -= len(old.audio.pcm)
            self._lost_frames += 1

    async def process_audio_frame(self, speaker_sid, frame, call_start_time) -> None:
        try:
            pb = self._pbmod()
            recv_ts_ms = int(time.time() * 1000)
            pts_ms = _frame_pts_ms(frame)
            pcm = frame_to_pcm(frame, TARGET_RATE)
            f = pb.CallFrame(audio=pb.AudioChunk(
                speaker_id=speaker_sid, speaker_name="",
                pcm=pcm.astype("float32").tobytes(),
                recv_ts_ms=recv_ts_ms, pts_ms=pts_ms))
        except Exception:
            logger.exception("brain sink: frame prep failed [%s]",
                             self._session_id[:12])
            return
        self._enqueue(f)
        await self._pump()

    async def _pump(self) -> None:
        try:
            await self._ensure_call()
        except Exception:
            return  # gateway unreachable — frames wait in the outbox
        dead = None
        async with self._write_lock:
            while self._outbox and self._call is not None:
                f = self._outbox[0]
                try:
                    await self._call.write(f)
                except Exception:
                    self._send_failures += 1
                    dead = self._call
                    break
                self._outbox.popleft()
                self._outbox_bytes -= len(f.audio.pcm)
        if dead is not None:
            await self._reconnect(dead)

    async def finalize(self, *, call_end_ms: int, uncaptured=None,
                       present_count: int | None = None, participants=None,
                       speakers=None, spans=None):
        """Flush buffered audio, send the end frame, half-close, and await the
        gateway's acceptance. Returns (session_id, status_int) or None if the
        call never opened.

        ``participants`` is the final roster. Who spoke is told in one of two
        ways, depending on how this source captures: ``speakers`` maps a track's
        speaker_id to a name (one track per person), while ``spans`` are
        ``(start_s, end_s, name)`` turns for a source that gets one mixed track
        carrying everybody — a bot joining as a guest. Both are known only now."""
        if self._outbox:
            await self._pump()
        if self._call is None:
            self._lost_frames += len(self._outbox)
            return None
        self._lost_frames += len(self._outbox)  # anything still unflushed is lost
        pb = self._pb
        try:
            async with self._write_lock:
                await self._call.write(pb.CallFrame(end=pb.CallEnd(
                    call_end_ms=call_end_ms,
                    uncaptured=list(uncaptured or []),
                    present_count=present_count or 0,
                    participants=list(participants or []),
                    speakers=dict(speakers or {}),
                    spans=[pb.SpeakerSpan(start_ms=int(s * 1000),
                                          end_ms=int(e * 1000), name=n)
                           for (s, e, n) in (spans or [])])))
                await self._call.done_writing()
            accepted = await self._call
            logger.info("brain accepted call [%s] status=%d "
                        "(lost_frames=%d, reconnects=%d)",
                        self._session_id[:12], accepted.status,
                        self._lost_frames, self._reconnects)
            return accepted.session_id, accepted.status
        finally:
            if self._channel_obj is not None:
                try:
                    await self._channel_obj.close()
                except Exception:
                    pass


class BrainArtifacts:
    """Polling the gateway for a call's files and acknowledging writes."""

    def __init__(self, config):
        self._target = config.brain_grpc_target
        self._tls = getattr(config, "brain_grpc_tls", False)
        self._token = config.brain_api_token

    def _md(self):
        return [("authorization", f"Bearer {self._token}")] if self._token else None

    async def fetch(self, session_id: str, have_sha256=None):
        """Return (status_int, [artifact]) — artifacts the client does not
        already hold come with content; known ones come back metadata-only."""
        pb, pb_grpc = _stubs()
        ch = _channel(self._target, self._tls, _GRPC_OPTS)
        try:
            stub = pb_grpc.MeetingGatewayStub(ch)
            resp = await stub.GetArtifacts(pb.ArtifactsRequest(
                session_id=session_id, have_sha256=list(have_sha256 or [])),
                metadata=self._md())
            return resp.status, list(resp.artifacts), resp.detail
        finally:
            await ch.close()

    async def ack(self, session_id: str, sha256) -> None:
        pb, pb_grpc = _stubs()
        ch = _channel(self._target, self._tls, _GRPC_OPTS)
        try:
            stub = pb_grpc.MeetingGatewayStub(ch)
            await stub.AckArtifacts(pb.AckRequest(
                session_id=session_id, sha256=list(sha256)), metadata=self._md())
        finally:
            await ch.close()
