"""The shape of an active call, shared by the two monitors.

Its own module so the app-based monitor need not import monitor.py, which pulls
in pymysql for the database path — a dependency the app path does not have and
the test environment does not install.
"""
from __future__ import annotations

from dataclasses import dataclass


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
