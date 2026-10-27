"""The LiveKit driver against a real server and the real SDK.

test_livekit_client pins what the driver does with events; this pins that the
SDK still sends the events it expects, with the arguments it expects — and the
two behaviours no fake can tell: a hidden participant is invisible to people,
and a track that sends nothing still yields frames, of exact zeros.

Skipped unless LIVEKIT_E2E_URL points at a server in dev mode (key `devkey`,
secret `secret`):

    docker run -d --rm -p 7880:7880 -p 7881:7881 -p 7882:7882/udp \\
        livekit/livekit-server:v1.8.4 --dev --bind 0.0.0.0 --node-ip 127.0.0.1
    LIVEKIT_E2E_URL=ws://127.0.0.1:7880 python -m pytest tests/test_livekit_e2e.py
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.request
from types import SimpleNamespace

import pytest

URL = os.environ.get("LIVEKIT_E2E_URL", "")
pytestmark = pytest.mark.skipif(not URL, reason="LIVEKIT_E2E_URL is not set")
rtc = pytest.importorskip("livekit.rtc")
np = pytest.importorskip("numpy")

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from talk_capture import livekit_client  # noqa: E402
from talk_capture.call_info import CallInfo  # noqa: E402
from talk_capture.driver import make_driver  # noqa: E402

KEY, SECRET = "devkey", "secret"
# Shaped like an MSC4195 room name: base64 with + and /.
ROOM = "TfPYplPHbz2DrYLObIjB6+FFBUk7RcrjigEFrKLUrME"
ALICE, BOB, CAROL, DAN = ("@alice:example.org:LAPTOP", "@bob:example.org:DESK",
                          "@carol:example.org:TAB", "@dan:example.org:PC")
US = "voxonta:e2e"


def token(identity: str, **video) -> str:
    def b64(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    now = int(time.time())
    body = {"iss": KEY, "sub": identity, "nbf": now, "exp": now + 600,
            "video": {"room": ROOM, **video}}
    header = b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    signing = f"{header}.{b64(json.dumps(body).encode())}"
    sig = hmac.new(SECRET.encode(), signing.encode(), hashlib.sha256).digest()
    return f"{signing}.{b64(sig)}"


def person(identity):
    return token(identity, roomJoin=True, canPublish=True, canSubscribe=True)


def ours():
    # What voxonta-matrix-app issues.
    return token(US, roomJoin=True, canSubscribe=True, canPublish=False,
                 canPublishData=False, hidden=True, roomCreate=False)


def kick(identity):
    api = URL.replace("ws", "http", 1)
    req = urllib.request.Request(
        f"{api}/twirp/livekit.RoomService/RemoveParticipant",
        data=json.dumps({"room": ROOM, "identity": identity}).encode(),
        headers={"Authorization": f"Bearer {token('admin', roomAdmin=True)}",
                 "Content-Type": "application/json"})
    urllib.request.urlopen(req, timeout=5).read()


async def join(identity, *tracks):
    """Join as a person publishing (source, hz): hz None sends nothing, 0 is
    a live microphone in a quiet room."""
    room = rtc.Room()
    await room.connect(URL, person(identity))
    room.tracks, room.pushers = [], []
    for source, hz in tracks:
        src = rtc.AudioSource(48000, 1)
        track = rtc.LocalAudioTrack.create_audio_track(f"t{source}", src)
        await room.local_participant.publish_track(track, rtc.TrackPublishOptions(source=source))
        room.tracks.append(track)
        if hz is not None:
            room.pushers.append(asyncio.create_task(_play(src, hz)))
    return room


async def _play(src, hz):
    n, t, rng = 480, 0, np.random.default_rng(1)
    while True:
        if hz:
            x = np.sin(2 * np.pi * hz * (np.arange(n) + t) / 48000) * 12000
        else:
            x = rng.integers(-3, 4, n)
        frame = rtc.AudioFrame.create(48000, 1, n)
        np.frombuffer(frame.data, dtype=np.int16)[:] = x.astype(np.int16)
        await src.capture_frame(frame)
        t += n


async def until(condition, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("timed out")
        await asyncio.sleep(0.1)


def dominant_hz(chunks):
    x = np.frombuffer(b"".join(chunks), dtype="<i2").astype(np.float32)
    spectrum = np.abs(np.fft.rfft(x * np.hanning(len(x))))
    return np.fft.rfftfreq(len(x), 1 / 16000)[spectrum.argmax()]


def test_a_call_on_a_real_server(monkeypatch):
    monkeypatch.setattr(livekit_client, "REJOIN_DELAYS_S", (1,))
    mic, screen = rtc.TrackSource.SOURCE_MICROPHONE, rtc.TrackSource.SOURCE_SCREENSHARE_AUDIO

    async def go():
        people = [await join(ALICE, (mic, 440), (screen, 1000)),
                  await join(CAROL, (mic, None)),
                  await join(DAN, (mic, 0))]
        heard, sizes = {}, set()

        async def on_frame(identity, frame):
            heard.setdefault(identity, []).append(frame.pcm16)
            sizes.add(len(frame.pcm16))

        info = CallInfo("!daily:example.org", "Дейли", 2, 1, session_id="e2e",
                        platform="livekit", source="element-call", details={
                            "livekit": {"url": URL, "room": ROOM, "token": ours()},
                            "members": {ALICE: {"name": "Алиса", "user": "@alice:example.org"}}})
        driver = make_driver(SimpleNamespace(nc_url=""), info, on_frame)
        task = asyncio.create_task(driver.connect_and_join())
        try:
            await until(lambda: len(heard.get(ALICE, [])) > 100 and len(heard.get(DAN, [])) > 100)
            people.append(await join(BOB, (mic, 300)))
            await until(lambda: len(heard.get(BOB, [])) > 100)

            # Hidden: people do not see us.
            assert US not in {p.identity for p in people[0].remote_participants.values()}

            # Thrown out; meanwhile the integration has issued a fresh token.
            first = driver._room
            info.details = {**info.details, "livekit": {**info.details["livekit"], "token": ours()}}
            kick(US)
            mark = len(heard[ALICE])
            await until(lambda: driver._room not in (None, first) and len(heard[ALICE]) > mark + 100)

            # Muting the way Element does stops what is sent within moments.
            people[3].tracks[0].mute()
            await asyncio.sleep(2)
            muted_at = len(heard[BOB])
            await asyncio.sleep(2)
            assert len(heard[BOB]) == muted_at
        finally:
            await driver.disconnect()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            for room in people:
                for pusher in room.pushers:
                    pusher.cancel()
                await room.disconnect()
        return driver, heard, sizes

    driver, heard, sizes = asyncio.run(go())

    assert sizes == {320}                                   # 10 ms, 16 kHz, mono
    assert abs(dominant_hz(heard[ALICE]) - 440) < 10        # her mic, not her screen
    assert abs(dominant_hz(heard[BOB]) - 300) < 10          # joined after us
    assert DAN in heard                                     # quiet is not silent
    assert CAROL not in heard and CAROL not in driver.speakers
    assert sorted(driver.get_call_participants()) == ["bob", "carol", "dan", "Алиса"]
    assert driver.get_uncaptured_participants() == []
