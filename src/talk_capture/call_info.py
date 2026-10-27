"""The shape of an active call, shared by the two monitors.

Its own module so the app-based monitor need not import monitor.py, which pulls
in pymysql for the database path — a dependency the app path does not have and
the test environment does not install.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class CallInfo:
    """Active call information."""
    room_token: str
    room_name: str
    room_type: int  # 1=one-to-one, 2=group, 3=public, 4=changelog
    call_flag: int  # bitmask: 1=audio, 2=video, 4=force_muted
    # Meeting id for this call, issued by the Nextcloud app and carried through
    # to the SaaS as MeetingContext.session_id. It belongs to whoever owns the
    # call's lifecycle: the app knows a call is still THE SAME call across a
    # capture restart, so it hands back the same id and the stream resumes
    # instead of arriving as a second meeting. It is also what lets the app
    # collect the finished files without asking capture what to poll for.
    # Empty when the source cannot issue one (MySQL monitor, older app) — the
    # caller generates one instead.
    session_id: str = ""
    # False while the room has opted out of recording ("/no-record"). Owned by
    # the app and refreshed on every poll, on this same object, so the session
    # that holds it sees the change within one poll interval. The call stays
    # joined while it is off: resuming continues the same meeting. True when the
    # source does not say (MySQL monitor, older app) — what those always did.
    recording: bool = True
    # Which kind of call this is, as the app reports it; picks the driver.
    # "talk" when the source does not say — the only kind there used to be.
    platform: str = "talk"
    # MeetingContext.source. The integration that collects the files asks the
    # gateway for its own meetings by this name, so it must match what that
    # integration expects: the app may name it, otherwise SOURCE_BY_PLATFORM.
    source: str = "nc-talk"
    # The app's latest entry for this call, as sent, replaced on every poll.
    # Where a driver finds what is particular to its platform (an address, a
    # token that gets refreshed, who is who) without this class growing a
    # field for each.
    details: dict = field(default_factory=dict)


# MeetingContext.source for a call whose app does not name one.
SOURCE_BY_PLATFORM = {"talk": "nc-talk", "livekit": "element-call"}
