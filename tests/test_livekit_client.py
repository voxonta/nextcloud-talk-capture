"""The LiveKit driver against a stand-in for the SDK: recorded frames, no server.

What stage 2 of Element Call is accepted on, short of a live call: only
microphones are listened to, every device is its own track under its own
identity, a person on two devices is one person in the roster, nobody is named
"unknown" who could be named, and losing the connection does not end the
capture — it rejoins with whatever token the integration issued last.
"""
from __future__ import annotations

import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from talk_capture import livekit_client  # noqa: E402
from talk_capture.call_info import CallInfo  # noqa: E402
from talk_capture.livekit_client import LiveKitClient  # noqa: E402

MIC, CAMERA, SCREEN_AUDIO = 2, 1, 4
AUDIO, VIDEO = 1, 2
# One sample each. The SDK fills a track that sends nothing with exact zeros.
VOICE, SILENCE = b"\x10\x00", b"\x00\x00"


class Publication:
    def __init__(self, sid, kind=AUDIO, source=MIC, frames=()):
        self.sid, self.kind, self.source = sid, kind, source
        self.frames = list(frames)
        self.subscribed = False

    def set_subscribed(self, value):
        self.subscribed = value


class Participant:
    def __init__(self, identity, *publications):
        self.identity = identity
        self.track_publications = {p.sid: p for p in publications}


class Room:
    made: list[Room] = []
    present: list[Participant] = []

    def __init__(self):
        self.handlers = {}
        self.remote_participants = {}
        self.connected = None
        self.disconnected = False
        Room.made.append(self)

    def on(self, event, handler):
        self.handlers[event] = handler

    def emit(self, event, *args):
        self.handlers[event](*args)

    async def connect(self, url, token, options=None):
        self.connected = (url, token, options)
        self.remote_participants = {p.identity: p for p in Room.present}

    async def disconnect(self):
        self.disconnected = True


class AudioStream:
    """Plays the publication's recorded frames, then waits like a quiet track."""

    def __init__(self, track, sample_rate, num_channels):
        assert (sample_rate, num_channels) == (16000, 1)
        self.frames = list(track.frames)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.frames:
            return SimpleNamespace(frame=SimpleNamespace(data=memoryview(self.frames.pop(0))))
        await asyncio.sleep(3600)

    async def aclose(self):
        pass


FAKE_RTC = SimpleNamespace(
    Room=Room, AudioStream=AudioStream,
    RoomOptions=lambda **kw: kw,
    TrackKind=SimpleNamespace(KIND_AUDIO=AUDIO, KIND_VIDEO=VIDEO),
    TrackSource=SimpleNamespace(SOURCE_MICROPHONE=MIC, SOURCE_CAMERA=CAMERA,
                                SOURCE_SCREENSHARE_AUDIO=SCREEN_AUDIO),
)

ALICE_LAPTOP, ALICE_PHONE = "@alice:example.org:LAPTOP", "@alice:example.org:PHONE"
BOB = "@bob:example.org:DESK"


def call(token="jwt-1", **members):
    details = {"livekit": {"url": "wss://lk.example.org", "room": "r", "token": token},
               "members": {
                   ALICE_LAPTOP: {"name": "Алиса", "user": "@alice:example.org"},
                   ALICE_PHONE: {"name": "Алиса", "user": "@alice:example.org"},
                   BOB: {"name": "Боб", "user": "@bob:example.org"},
                   **members}}
    return CallInfo("!daily:example.org", "Дейли", 2, 1, session_id="s1",
                    platform="livekit", source="element-call", details=details)


@pytest.fixture(autouse=True)
def fresh_rooms():
    Room.made, Room.present = [], []


async def joined(info, *participants):
    """A driver that has joined a room holding these participants."""
    Room.present = list(participants)
    heard = []

    async def on_frame(identity, frame):
        heard.append((identity, frame.pcm16))

    driver = LiveKitClient(SimpleNamespace(), info, on_frame, rtc=FAKE_RTC)
    task = asyncio.create_task(driver.connect_and_join())
    for _ in range(5):
        await asyncio.sleep(0)
    room = Room.made[-1]
    # The SDK answers a subscription with track_subscribed; so does this.
    for p in participants:
        for pub in p.track_publications.values():
            if pub.subscribed:
                room.emit("track_subscribed", pub, pub, p)
    for _ in range(5):
        await asyncio.sleep(0)
    return driver, task, heard


async def leave(driver, task):
    await driver.disconnect()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


def run(coro):
    return asyncio.run(coro)


# ── joining and listening ─────────────────────────────────────────────

def test_it_joins_with_the_token_from_the_calls_entry_and_subscribes_by_hand():
    async def go():
        driver, task, _ = await joined(call())
        await leave(driver, task)

    run(go())
    assert Room.made[0].connected == ("wss://lk.example.org", "jwt-1", {"auto_subscribe": False})


def test_only_microphones_are_listened_to():
    mic = Publication("TR_mic")
    camera = Publication("TR_cam", kind=VIDEO, source=CAMERA)
    screen = Publication("TR_screen", source=SCREEN_AUDIO)

    async def go():
        driver, task, _ = await joined(call(), Participant(BOB, mic, camera, screen))
        await leave(driver, task)

    run(go())
    assert (mic.subscribed, camera.subscribed, screen.subscribed) == (True, False, False)


def test_each_device_is_its_own_track_under_its_own_identity():
    async def go():
        driver, task, heard = await joined(
            call(),
            Participant(ALICE_LAPTOP, Publication("TR_1", frames=[b"\x01\x00"])),
            Participant(ALICE_PHONE, Publication("TR_2", frames=[b"\x02\x00"])))
        await leave(driver, task)
        return driver, heard

    driver, heard = run(go())
    assert sorted(heard) == [(ALICE_LAPTOP, b"\x01\x00"), (ALICE_PHONE, b"\x02\x00")]
    assert set(driver.speakers) == {ALICE_LAPTOP, ALICE_PHONE}


def test_someone_who_publishes_later_is_heard_too():
    async def go():
        driver, task, heard = await joined(call())
        room = Room.made[-1]
        late = Participant(BOB)
        pub = Publication("TR_late", frames=[b"\x05\x00"])
        room.emit("participant_connected", late)
        room.emit("track_published", pub, late)
        room.emit("track_subscribed", pub, pub, late)
        for _ in range(5):
            await asyncio.sleep(0)
        await leave(driver, task)
        return heard, pub

    heard, pub = run(go())
    assert pub.subscribed
    assert heard == [(BOB, b"\x05\x00")]


def test_another_capture_service_in_the_room_is_not_a_person():
    async def go():
        driver, task, heard = await joined(
            call(), Participant("voxonta:other-session", Publication("TR_x", frames=[VOICE])))
        await leave(driver, task)
        return driver, heard

    driver, heard = run(go())
    assert heard == [] and driver.get_call_participants() == []


# ── who was there ─────────────────────────────────────────────────────

def test_two_devices_are_one_person_in_the_roster():
    async def go():
        driver, task, _ = await joined(
            call(),
            Participant(ALICE_LAPTOP, Publication("TR_1", frames=[VOICE])),
            Participant(ALICE_PHONE, Publication("TR_2", frames=[VOICE])),
            Participant(BOB, Publication("TR_3", frames=[VOICE])))
        await leave(driver, task)
        return driver

    driver = run(go())
    assert driver.get_call_participants() == ["Алиса", "Боб"]
    assert driver.get_call_participant_count() == 2
    assert {i: driver.get_speaker_name(i) for i in driver.speakers} == {
        ALICE_LAPTOP: "Алиса", ALICE_PHONE: "Алиса", BOB: "Боб"}


def test_digital_silence_is_not_sent_and_does_not_make_a_speaker():
    async def go():
        driver, task, heard = await joined(call(), Participant(
            BOB, Publication("TR_1", frames=[SILENCE, SILENCE, VOICE, SILENCE])))
        await leave(driver, task)
        return driver, heard

    driver, heard = run(go())
    assert heard == [(BOB, VOICE)]
    assert driver.speakers == {BOB: 1}


def test_a_muted_microphone_is_present_and_captured_but_not_a_speaker():
    async def go():
        driver, task, heard = await joined(call(), Participant(
            BOB, Publication("TR_1", frames=[SILENCE] * 3)))
        await leave(driver, task)
        return driver, heard

    driver, heard = run(go())
    assert heard == [] and driver.speakers == {}
    assert driver.get_call_participants() == ["Боб"]
    assert driver.get_uncaptured_participants() == []


def test_a_stream_that_never_delivered_is_reported_unless_captured_elsewhere():
    async def go():
        driver, task, _ = await joined(
            call(),
            Participant(ALICE_LAPTOP, Publication("TR_1", frames=[SILENCE])),
            Participant(ALICE_PHONE, Publication("TR_2")),   # her other device failed
            Participant(BOB, Publication("TR_3")),           # never got through
            Participant("kQ0hashedIdentityFromMsc4195xx", Publication("TR_4")))
        await leave(driver, task)
        return driver

    # The unnamed one is left out: a gap with no one to name is not marked.
    assert run(go()).get_uncaptured_participants() == ["Боб"]


def test_a_name_that_arrives_later_is_used():
    # The integration may add a device to its map a poll after the device
    # joined LiveKit; the monitor refreshes details, and the name follows.
    newcomer = "kQ0hashedIdentityFromMsc4195xx"

    async def go():
        info = call()
        driver, task, _ = await joined(info, Participant(newcomer, Publication(
            "TR_1", frames=[VOICE])))
        before = driver.get_speaker_name(newcomer)
        info.details = {**info.details, "members": {
            **info.details["members"], newcomer: {"name": "Вера", "user": "@vera:example.org"}}}
        after = driver.get_speaker_name(newcomer)
        await leave(driver, task)
        return before, after

    before, after = run(go())
    assert before == "Участник (kQ0hashe)"
    assert after == "Вера"


def test_an_unmapped_device_id_still_names_its_owner():
    async def go():
        info = call()
        info.details["members"] = {}
        driver, task, _ = await joined(info, Participant(
            "@carol:example.org:TAB", Publication("TR_1", frames=[VOICE])))
        await leave(driver, task)
        return driver

    assert run(go()).get_call_participants() == ["carol"]


# ── staying in ────────────────────────────────────────────────────────

def test_a_dropped_connection_is_rejoined_with_the_latest_token(monkeypatch):
    monkeypatch.setattr(livekit_client, "REJOIN_DELAYS_S", (0,))

    async def go():
        info = call(token="jwt-1")
        driver, task, _ = await joined(info, Participant(BOB, Publication("TR_1")))
        info.details = {**info.details, "livekit": {**info.details["livekit"], "token": "jwt-2"}}
        Room.made[-1].emit("disconnected", "SIGNAL_CLOSE")
        for _ in range(10):
            await asyncio.sleep(0)
        rooms = list(Room.made)
        await leave(driver, task)
        return driver, rooms

    driver, rooms = run(go())
    assert [r.connected[1] for r in rooms] == ["jwt-1", "jwt-2"]
    assert rooms[0].disconnected
    # The roster survives the rejoin: it is the meeting's, not the connection's.
    assert driver.get_call_participants() == ["Боб"]


def test_leaving_stops_the_streams_and_the_room():
    async def go():
        driver, task, _ = await joined(call(), Participant(BOB, Publication("TR_1")))
        streams = list(driver._streams.values())
        await leave(driver, task)
        return streams, Room.made[-1], task

    streams, room, task = run(go())
    assert streams and all(t.done() for t in streams)
    assert room.disconnected and task.done()
