"""What joins a call — the one part of capture that differs by platform.

Everything around it is the same whatever the call is: the gateway stream, the
opt-out, the hand-over at the end and its retries. A driver is what knows how to
get into a call, which audio belongs to whom, and who was there. Talk's driver is
`SpreedClient`; the app's entry for each call says which one to use.

Kept to what `CallSession` actually calls, so a second driver has a short list
to meet rather than SpreedClient's whole surface.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol

# (speaker id, frame) — the id is what the driver keys `speakers` by.
AudioFrameHandler = Callable[[str, Any], Awaitable[None]]


class CallDriver(Protocol):
    # One entry per audio track heard, keyed by the id its frames arrive under.
    speakers: Mapping[str, Any]

    async def connect_and_join(self) -> None:
        """Join and stay until cancelled; frames go to the handler meanwhile."""

    async def disconnect(self) -> None:
        """Leave the call. Bounded by the caller, so it may simply try."""

    def get_call_participants(self) -> list[str]:
        """Names of everyone who was present, silent attendees included."""

    def get_uncaptured_participants(self) -> list[str]:
        """Names of those who were there but whose audio never arrived."""

    def get_call_participant_count(self) -> int:
        """How many distinct people were present."""

    def get_speaker_name(self, speaker_id: str) -> str:
        """The display name for a key of `speakers`."""


class CannotJoin(Exception):
    """This service cannot take the call; the message says why."""


def make_driver(config, call_info, on_audio_frame: AudioFrameHandler) -> CallDriver:
    """The driver for this call's platform, or CannotJoin.

    Raised rather than logged here so the caller can skip the call before
    anything else is set up — no gateway stream is opened for a call nobody
    will join.
    """
    platform = getattr(call_info, "platform", "talk")
    if platform == "talk":
        if not config.nc_url:
            raise CannotJoin("a Talk call, and NEXTCLOUD_URL is not set")
        # Imported here: the WebRTC stack is ~120 MB and only Talk needs it.
        from talk_capture.spreed_client import SpreedClient

        return SpreedClient(config=config, room_token=call_info.room_token,
                            on_audio_frame=on_audio_frame)
    if platform == "livekit":
        lk = (getattr(call_info, "details", None) or {}).get("livekit") or {}
        if not lk.get("url") or not lk.get("token"):
            raise CannotJoin("a LiveKit call without an address or token in its entry")
        try:
            from talk_capture.livekit_client import LiveKitClient
            from livekit import rtc  # noqa: F401  (fail here, not mid-call)
        except ImportError:
            raise CannotJoin("a LiveKit call, and the livekit package is not installed "
                             "(pip install 'nextcloud-talk-capture[livekit]')") from None
        return LiveKitClient(config, call_info, on_audio_frame)
    raise CannotJoin(f"platform {platform!r} is not supported by this version")
