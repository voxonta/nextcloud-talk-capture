"""Which driver joins a call, and what the call tells it.

Until 2026-10-27 the session built a SpreedClient unconditionally, wrote
"nc-talk" into every meeting it streamed, and the service refused to start
without a Nextcloud address. All three assumed one platform. The app now says
which platform a call is on; these tests pin what this side does with that —
and that an app which says nothing still gets exactly what it always did.
"""
from __future__ import annotations

import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

import main
from talk_capture.app_monitor import AppCallMonitor
from talk_capture.call_info import CallInfo
from talk_capture.driver import CannotJoin, make_driver


def monitor():
    started = []

    async def on_started(info):
        started.append(info)

    async def on_ended(token):
        pass

    config = SimpleNamespace(included_rooms=[], excluded_rooms=[], poll_interval=5)
    return AppCallMonitor(config, client=None, on_call_started=on_started,
                          on_call_ended=on_ended), started


async def poll(mon, *calls):
    await mon._process(list(calls))
    await asyncio.sleep(0)


# ── what the monitor reads from the app ───────────────────────────────

def test_an_app_that_says_nothing_is_talk():
    async def run():
        mon, started = monitor()
        await poll(mon, {"token": "room1", "name": "Daily", "type": 2})
        assert (started[0].platform, started[0].source) == ("talk", "nc-talk")

    asyncio.run(run())


def test_the_source_follows_the_platform():
    async def run():
        mon, started = monitor()
        await poll(mon, {"token": "!r:x", "name": "Планёрка", "type": 2,
                         "platform": "livekit"})
        assert started[0].source == "element-call"

    asyncio.run(run())


def test_the_app_can_name_the_source_itself():
    # The integration that collects the files asks the gateway for its own by
    # this name; it is the one that knows what that name is.
    async def run():
        mon, started = monitor()
        await poll(mon, {"token": "r", "type": 2, "platform": "livekit",
                         "source": "livekit-meet"})
        assert started[0].source == "livekit-meet"

    asyncio.run(run())


def test_the_entry_is_kept_and_refreshed_on_every_poll():
    # A LiveKit token expires and the integration issues another; the driver
    # holds the same CallInfo, so this is how the new one reaches it.
    async def run():
        mon, started = monitor()
        await poll(mon, {"token": "r", "type": 2, "platform": "livekit",
                         "livekit": {"token": "jwt-1"}})
        info = started[0]
        await poll(mon, {"token": "r", "type": 2, "platform": "livekit",
                         "livekit": {"token": "jwt-2"}})
        assert info.details["livekit"]["token"] == "jwt-2"
        assert len(started) == 1

    asyncio.run(run())


# ── which driver ──────────────────────────────────────────────────────

async def _noop(sid, frame):
    pass


def test_talk_gets_the_spreed_client():
    from talk_capture.spreed_client import SpreedClient

    config = SimpleNamespace(nc_url="https://cloud.example")
    driver = make_driver(config, CallInfo("tok", "n", 2, 1), _noop)
    assert isinstance(driver, SpreedClient)
    assert driver.room_token == "tok"


def test_talk_without_a_nextcloud_address_is_refused_not_attempted():
    with pytest.raises(CannotJoin, match="NEXTCLOUD_URL"):
        make_driver(SimpleNamespace(nc_url=""), CallInfo("tok", "n", 2, 1), _noop)


def test_a_platform_this_version_does_not_know_is_refused():
    info = CallInfo("r", "n", 2, 1, platform="jitsi", source="jitsi")
    with pytest.raises(CannotJoin, match="jitsi"):
        make_driver(SimpleNamespace(nc_url="https://cloud.example"), info, _noop)


def _livekit_call(**livekit):
    return CallInfo("!r:example.org", "n", 2, 1, platform="livekit", source="element-call",
                    details={"livekit": livekit})


def test_livekit_gets_the_livekit_client_and_needs_no_nextcloud():
    pytest.importorskip("livekit.rtc")
    from talk_capture.livekit_client import LiveKitClient

    info = _livekit_call(url="wss://lk.example.org", token="jwt")
    driver = make_driver(SimpleNamespace(nc_url=""), info, _noop)
    assert isinstance(driver, LiveKitClient)
    assert driver.room_token == "!r:example.org"


def test_livekit_without_a_token_is_refused_not_attempted():
    # The integration had no LiveKit key, or the entry came from an app that
    # says "livekit" and gives nothing to join with.
    with pytest.raises(CannotJoin, match="token"):
        make_driver(SimpleNamespace(nc_url=""), _livekit_call(url="wss://lk.example.org"), _noop)


# ── the service around it ─────────────────────────────────────────────

def _service():
    s = main.CaptureService.__new__(main.CaptureService)
    s.config = SimpleNamespace(nc_url="", app_base_url="https://matrix.example",
                               app_service_token="k", brain_grpc_target="gw:443",
                               brain_grpc_tls=True)
    s._sessions = {}
    return s


def test_a_call_nobody_can_join_is_skipped_without_opening_a_stream(monkeypatch):
    opened = []
    monkeypatch.setattr(main, "BrainSink", lambda *a, **k: opened.append(a))

    async def run():
        service = _service()
        await service._on_call_started(
            CallInfo("r", "n", 2, 1, platform="jitsi", source="jitsi"))
        return service

    service = asyncio.run(run())
    assert service._sessions == {} and opened == []


def test_the_meeting_is_streamed_under_the_calls_source(monkeypatch):
    contexts = []

    class Driver:
        speakers = {}

        async def connect_and_join(self):
            pass

    monkeypatch.setattr(main, "make_driver", lambda *a: Driver())
    monkeypatch.setattr(main, "BrainSink", lambda config, ctx: contexts.append(ctx))

    async def run():
        main.CallSession(SimpleNamespace(transcript_language="ru", timezone="UTC"),
                         CallInfo("r", "n", 2, 1, session_id="s1",
                                  platform="livekit", source="element-call"))
        await asyncio.sleep(0)

    asyncio.run(run())
    assert contexts[0]["source"] == "element-call"


def test_starting_needs_the_app_not_nextcloud():
    # An integration other than the Nextcloud app has no Nextcloud to name.
    class Reached(Exception):
        pass

    async def fetch_settings():
        raise Reached

    service = _service()
    service._app = SimpleNamespace(fetch_settings=fetch_settings)
    with pytest.raises(Reached):
        asyncio.run(service.run())

    service.config.app_base_url = ""
    with pytest.raises(SystemExit, match="APP_BASE_URL"):
        asyncio.run(service.run())
