"""Ending a call: let go of it first, then hand it over — bounded and retried.

Two morning meetings were lost to the old order and its unbounded wait. On
2026-09-11 the close failed on a DNS error that was gone minutes later and was
never tried again; on 2026-09-18 it hung, the disconnect behind it never ran,
and the client sat in the finished call for six hours while the gateway counted
it as live. These tests are those two mornings, and what must happen instead.
"""
from __future__ import annotations

import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

import main


class FakeSpreed:
    def __init__(self, order, hang=False):
        self.order = order
        self.hang = hang
        self.speakers = {"sid-1": object()}

    def get_call_participants(self):
        return ["Алексей Морозов"]

    def get_uncaptured_participants(self):
        return []

    def get_call_participant_count(self):
        return 1

    def get_speaker_name(self, sid):
        return "Алексей Морозов"

    async def disconnect(self):
        self.order.append("disconnect")
        if self.hang:
            await asyncio.sleep(3600)


class FakeSink:
    """finalize() fails, hangs or succeeds per a script, one entry per attempt."""

    def __init__(self, order, script):
        self.order = order
        self.script = list(script)
        self.reopened = 0

    async def reopen(self):
        self.order.append("reopen")
        self.reopened += 1

    async def finalize(self, **end):
        self.order.append("finalize")
        step = self.script.pop(0)
        if step == "hang":
            await asyncio.sleep(3600)
        if step == "fail":
            raise RuntimeError("Temporary failure in name resolution")
        return ("sess-1", 2)


def session(order, script, *, hang_disconnect=False, taken=()):
    s = main.CallSession.__new__(main.CallSession)
    s.config = SimpleNamespace()
    s.call_info = SimpleNamespace(room_token="2sgiqi5p")
    s.session_id = "sess-1"
    s._task = None
    s._spreed = FakeSpreed(order, hang=hang_disconnect)
    s._sink = FakeSink(order, script)
    answers = list(taken)

    async def already_taken():
        order.append("status?")
        return answers.pop(0) if answers else False

    s._already_taken = already_taken
    return s


@pytest.fixture(autouse=True)
def fast(monkeypatch):
    monkeypatch.setattr(main, "DISCONNECT_TIMEOUT_S", 0.05)
    monkeypatch.setattr(main, "HANDOVER_TIMEOUT_S", 0.05)
    monkeypatch.setattr(main, "HANDOVER_RETRY_DELAYS_S", (0.01, 0.01, 0.01))


def run(s):
    asyncio.run(asyncio.wait_for(s.stop(), timeout=5))


def test_the_call_is_left_before_it_is_handed_over():
    order: list[str] = []
    run(session(order, ["ok"]))
    assert order == ["disconnect", "finalize"]


def test_a_failed_close_is_tried_again_on_a_fresh_stream():
    # 2026-09-11: DNS was down for minutes, the one attempt failed, the meeting
    # stayed open in the gateway for good.
    order: list[str] = []
    s = session(order, ["fail", "ok"])
    run(s)
    assert order == ["disconnect", "finalize", "status?", "reopen", "finalize"]
    assert s._sink.reopened == 1


def test_a_hung_close_is_given_up_on_and_retried():
    # 2026-09-18: the hand-over never returned. Now it is an attempt with a
    # deadline, and the next one starts on a new stream.
    order: list[str] = []
    run(session(order, ["hang", "ok"]))
    assert order.count("finalize") == 2


def test_a_call_the_gateway_already_took_is_not_ended_twice():
    # The attempt timed out on our side but got through: a second end frame
    # would be refused by the engine and could turn a finished call into a
    # failed one.
    order: list[str] = []
    run(session(order, ["hang"], taken=[True]))
    assert order == ["disconnect", "finalize", "status?"]


def test_a_hung_disconnect_does_not_stop_the_hand_over():
    order: list[str] = []
    run(session(order, ["ok"], hang_disconnect=True))
    assert order == ["disconnect", "finalize"]


def test_retries_end_and_say_so():
    order: list[str] = []
    s = session(order, ["fail", "fail", "fail", "fail"])
    run(s)
    assert order.count("finalize") == 4
