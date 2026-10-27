""""/no-record" has to stop what goes out, and only for as long as it is on.

Until 2026-10-27 the app stored the opt-out and the room was told recording had
stopped, while this service never heard of it and every frame still went to the
gateway. The app now lists each call with a `recording` flag; these tests pin
what this side does with it: the flag follows the app on every poll, a paused
call stays joined, and nothing reaches the sink while it is off.
"""
from __future__ import annotations

import asyncio
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

import main
from talk_capture.app_monitor import AppCallMonitor


def monitor():
    started, ended = [], []

    async def on_started(info):
        started.append(info)

    async def on_ended(token):
        ended.append(token)

    config = SimpleNamespace(included_rooms=[], excluded_rooms=[], poll_interval=5)
    return AppCallMonitor(config, client=None, on_call_started=on_started,
                          on_call_ended=on_ended), started, ended


def call(token="room1", **extra):
    return {"token": token, "name": "Daily", "type": 2, "session_id": "s1", **extra}


async def poll(mon, *calls):
    await mon._process(list(calls))
    await asyncio.sleep(0)  # let the start/end callbacks run


def test_the_flag_follows_the_app_without_restarting_the_call():
    async def run():
        mon, started, ended = monitor()
        await poll(mon, call(recording=True))
        info = started[0]
        assert info.recording is True

        await poll(mon, call(recording=False))
        assert info.recording is False, "the session's own CallInfo must see the opt-out"

        await poll(mon, call(recording=True))
        assert info.recording is True
        assert len(started) == 1 and ended == [], \
            "pausing must not end the meeting: /record resumes the same one"

    asyncio.run(run())


def test_a_call_can_start_already_opted_out():
    async def run():
        mon, started, _ = monitor()
        await poll(mon, call(recording=False))
        assert started[0].recording is False, \
            "/no-record posted before the call holds from its first frame"

    asyncio.run(run())


def test_an_app_without_the_flag_records_as_before():
    async def run():
        mon, started, _ = monitor()
        await poll(mon, call())
        assert started[0].recording is True

    asyncio.run(run())


class RecordingSink:
    def __init__(self):
        self.frames = []

    async def process_audio_frame(self, sid, frame, call_start):
        self.frames.append((sid, frame))


def session(recording=True):
    s = main.CallSession.__new__(main.CallSession)
    s.call_info = SimpleNamespace(room_token="room1", recording=recording)
    s.call_start = 0.0
    s._dropped_frames = 0
    s._sink = RecordingSink()
    return s


def test_nothing_reaches_the_gateway_while_the_room_opted_out():
    async def run():
        s = session(recording=True)
        await s._on_audio_frame("sid-1", "before")

        s.call_info.recording = False
        await s._on_audio_frame("sid-1", "during")
        await s._on_audio_frame("sid-2", "during")

        s.call_info.recording = True
        await s._on_audio_frame("sid-1", "after")

        assert [f for _, f in s._sink.frames] == ["before", "after"]
        assert s._dropped_frames == 2

    asyncio.run(run())


def test_a_session_without_the_flag_records():
    async def run():
        s = session()
        del s.call_info.recording  # a CallInfo from before the flag existed
        await s._on_audio_frame("sid-1", "frame")
        assert len(s._sink.frames) == 1

    asyncio.run(run())
