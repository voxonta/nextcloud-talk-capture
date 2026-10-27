"""Joining a LiveKit call — Element Call — as a hidden listener.

The Matrix integration does everything that needs Matrix: it decides the call
is on, signs a listen-only token for its LiveKit room, and maps every LiveKit
identity to the person behind it. All of that arrives in the call's entry in
/calls (CallInfo.details), which the monitor replaces on every poll. This
driver only connects, listens, and reports who was there.

One stream per LiveKit identity, which is one device: a person on a laptop and
a phone is two tracks with the same name, never one mixed buffer. The gateway
keys its buffers by speaker id, so the two cannot interleave.

Audio comes out of the SDK decoded, mono, at the contract rate; it is handed on
as Pcm16Frame and frame_to_pcm (aiortc's path) is never involved.

The SDK pulls every subscribed track every 10 ms whether packets came or not,
and fills the gaps with exact zeros: a muted microphone is a steady stream of
digital silence. Those frames prove the track is alive and are not sent — a
live microphone, even in a quiet room, is never exactly zero.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging

from talk_capture.audio import TARGET_RATE, Pcm16Frame

logger = logging.getLogger(__name__)

# Waits before rejoining after the SDK gave up on the connection. Its own
# reconnects cover a network blip; these cover a LiveKit restart or a long
# outage, each attempt with whatever token the integration issued last.
REJOIN_DELAYS_S = (2, 5, 10, 30)
# Other capture services join hidden too; they are not people.
CAPTURE_IDENTITY_PREFIX = "voxonta:"


class LiveKitClient:
    """A driver for talk_capture.driver.CallDriver, on LiveKit.

    `rtc` is the SDK's module, passed in only by tests; everything the driver
    touches in it goes through that one name.
    """

    def __init__(self, config, call_info, on_audio_frame, *, rtc=None):
        self.config = config
        self.call_info = call_info
        self.room_token = call_info.room_token
        self._on_audio_frame = on_audio_frame
        if rtc is None:
            # Imported on first use: the SDK carries a native library that a
            # Talk-only installation never needs.
            from livekit import rtc
        self._rtc = rtc

        # Identities whose audio actually arrived, silence aside: the keys of
        # the end frame's track-to-name map.
        self.speakers: dict[str, int] = {}
        # Everyone who was in the room, by identity, in order of arrival.
        self._present: dict[str, None] = {}
        # Microphones asked for, and those whose stream delivered anything at
        # all, silence included. The difference is what failed to capture.
        self._attempted: set[str] = set()
        self._captured: set[str] = set()
        # Names as last seen, so a person who left is still named if the map
        # stops listing them.
        self._names: dict[str, tuple[str, str]] = {}
        self._streams: dict[str, asyncio.Task] = {}
        self._room = None
        self._gone = asyncio.Event()
        self._stopping = False

    # ── joining ───────────────────────────────────────────────────────

    async def connect_and_join(self) -> None:
        """Join, and rejoin after the SDK gives up, until cancelled."""
        attempt = 0
        while not self._stopping:
            try:
                await self._join()
                attempt = 0
                await self._gone.wait()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("LiveKit join failed for %s: %s", self.room_token, e)
            if self._stopping:
                return
            delay = REJOIN_DELAYS_S[min(attempt, len(REJOIN_DELAYS_S) - 1)]
            attempt += 1
            logger.info("rejoining %s in %ds", self.room_token, delay)
            await asyncio.sleep(delay)

    async def _join(self) -> None:
        lk = (getattr(self.call_info, "details", None) or {}).get("livekit") or {}
        url, token = lk.get("url"), lk.get("token")
        if not url or not token:
            raise RuntimeError("the call's entry has no LiveKit address or token")
        await self._close_room()
        self._gone = asyncio.Event()
        room = self._rtc.Room()
        room.on("participant_connected", self._on_participant)
        room.on("track_published", self._on_published)
        room.on("track_subscribed", self._on_subscribed)
        room.on("track_unsubscribed", self._on_unsubscribed)
        room.on("disconnected", self._on_disconnected)
        # Subscribing is ours to decide: video and screen-share audio are of
        # no use and cost bandwidth and decoding.
        await room.connect(url, token, options=self._rtc.RoomOptions(auto_subscribe=False))
        self._room = room
        logger.info("joined LiveKit room for %s (%d already there)",
                    self.room_token, len(room.remote_participants))
        for participant in list(room.remote_participants.values()):
            self._on_participant(participant)
            for publication in list(participant.track_publications.values()):
                self._on_published(publication, participant)

    def _on_disconnected(self, reason=None) -> None:
        if not self._stopping:
            logger.warning("LiveKit dropped %s: %s", self.room_token, reason)
        self._gone.set()

    # ── tracks ────────────────────────────────────────────────────────

    def _is_person(self, participant) -> bool:
        return not str(participant.identity).startswith(CAPTURE_IDENTITY_PREFIX)

    def _on_participant(self, participant) -> None:
        if self._is_person(participant):
            self._present.setdefault(participant.identity, None)
            self._remember_name(participant.identity)

    def _on_published(self, publication, participant) -> None:
        if not self._is_person(participant):
            return
        self._on_participant(participant)
        if self._is_microphone(publication):
            self._attempted.add(participant.identity)
            publication.set_subscribed(True)

    def _is_microphone(self, publication) -> bool:
        return (publication.kind == self._rtc.TrackKind.KIND_AUDIO
                and publication.source == self._rtc.TrackSource.SOURCE_MICROPHONE)

    def _on_subscribed(self, track, publication, participant) -> None:
        if not self._is_person(participant) or not self._is_microphone(publication):
            return
        identity = participant.identity
        previous = self._streams.pop(publication.sid, None)
        if previous is not None:
            previous.cancel()
        self._streams[publication.sid] = asyncio.create_task(
            self._read(identity, track), name=f"lk-audio-{identity}")

    def _on_unsubscribed(self, track, publication, participant) -> None:
        task = self._streams.pop(publication.sid, None)
        if task is not None:
            task.cancel()

    async def _read(self, identity: str, track) -> None:
        stream = self._rtc.AudioStream(track, sample_rate=TARGET_RATE, num_channels=1)
        try:
            async for event in stream:
                self._captured.add(identity)
                pcm16 = bytes(event.frame.data)
                if pcm16.count(0) == len(pcm16):
                    continue
                if identity not in self.speakers:
                    self.speakers[identity] = 0
                    self._remember_name(identity)
                    logger.info("hearing %s in %s", self.get_speaker_name(identity),
                                self.room_token)
                self.speakers[identity] += 1
                await self._on_audio_frame(identity, Pcm16Frame(pcm16))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("audio stream of %s in %s broke", identity, self.room_token)
        finally:
            with contextlib.suppress(Exception):
                await stream.aclose()

    # ── leaving ───────────────────────────────────────────────────────

    async def disconnect(self) -> None:
        self._stopping = True
        self._gone.set()
        await self._close_room()
        logger.info("left LiveKit room for %s", self.room_token)

    async def _close_room(self) -> None:
        tasks, self._streams = list(self._streams.values()), {}
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        room, self._room = self._room, None
        if room is not None:
            with contextlib.suppress(Exception):
                await room.disconnect()

    # ── who was there ─────────────────────────────────────────────────

    def _member(self, identity: str) -> dict:
        members = (getattr(self.call_info, "details", None) or {}).get("members") or {}
        entry = members.get(identity)
        return entry if isinstance(entry, dict) else {}

    def _remember_name(self, identity: str) -> None:
        entry = self._member(identity)
        if entry.get("name") or entry.get("user"):
            self._names[identity] = (str(entry.get("name") or ""), str(entry.get("user") or ""))

    def _person(self, identity: str) -> tuple[str, str]:
        """(name, user) for an identity: the integration's map, else what was
        last known, else a label that admits not knowing."""
        self._remember_name(identity)
        name, user = self._names.get(identity, ("", ""))
        if not name:
            # `@login:server:DEVICE` names itself; a hash does not.
            if identity.startswith("@") and ":" in identity:
                name = identity[1:].split(":", 1)[0]
            else:
                name = f"Участник ({identity[:8]})"
        return name, user or name

    def get_speaker_name(self, speaker_id: str) -> str:
        return self._person(speaker_id)[0]

    def get_call_participants(self) -> list[str]:
        """Everyone who was in the call, once each — two devices are one person."""
        seen, names = set(), []
        for identity in self._present:
            name, user = self._person(identity)
            if user not in seen:
                seen.add(user)
                names.append(name)
        return names

    def get_call_participant_count(self) -> int:
        return len(self.get_call_participants())

    def get_uncaptured_participants(self) -> list[str]:
        """People whose microphone was asked for and whose stream never
        delivered a frame — not even silence, so not someone who was muted.
        Unless the same person was captured on another device; placeholders
        are left out, as in Talk: there is no one to name in the gap."""
        captured = {self._person(i)[1] for i in self._captured}
        missing = set()
        for identity in self._attempted - self._captured:
            name, user = self._person(identity)
            if user not in captured and not name.startswith("Участник ("):
                missing.add(name)
        return sorted(missing)
