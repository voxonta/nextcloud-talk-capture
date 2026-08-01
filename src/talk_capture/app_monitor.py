"""How capture learns a call started: by asking the Nextcloud app.

The app polls nothing — it is subscribed to Talk's own call events — and serves
the live ones from /service/calls. Capture asks it, and is therefore blind to
Nextcloud's database and to the host it runs on.

That blindness is the point. Detection used to read oc_talk_rooms directly over
an SSH tunnel, which meant an installation had to be handed database credentials
and a shell account on someone else's Nextcloud. No product may ask for that, so
that path is gone and this is the only one.

Polled, not pushed: the capture service runs inside the customer's network and
need not be reachable from outside, while Nextcloud is reachable by definition.
The cadence matches the old database poll, so detection is no slower.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine

from talk_capture.app_client import AppClient, AppClientError
from talk_capture.call_info import CallInfo

logger = logging.getLogger(__name__)


class AppCallMonitor:
    """Watches the archive app for active calls and triggers callbacks."""

    def __init__(
        self,
        config,
        client: AppClient,
        on_call_started: Callable[[CallInfo], Coroutine],
        on_call_ended: Callable[[str], Coroutine],
    ):
        self.config = config
        self._client = client
        self._on_call_started = on_call_started
        self._on_call_ended = on_call_ended
        self._active_calls: dict[str, CallInfo] = {}
        self._stop = asyncio.Event()
        # DailySummary checks monitor._tunnel to decide whether it can run its
        # own MySQL query; None means "no database here", and it falls back to
        # the REST path. The app monitor has no tunnel, so it says so.
        self._tunnel = None

    async def start(self) -> None:
        """Poll the app until stopped."""
        logger.info(
            "Call monitor started (app polling every %ds)", self.config.poll_interval
        )
        while not self._stop.is_set():
            try:
                calls = await self._client.fetch_calls()
                await self._process(calls)
            except AppClientError as e:
                # A poll can fail without the world ending: the app may be
                # restarting. Keep the current set and try again — an end event
                # missed this way is swept by the app's own abandoned-call
                # timeout, not left to strand a session here.
                logger.warning("could not read live calls: %s", e)
            except Exception:
                logger.exception("Unexpected error in app monitor loop")
            try:
                await asyncio.wait_for(self._stop.wait(), self.config.poll_interval)
            except asyncio.TimeoutError:
                pass

    def stop(self) -> None:
        self._stop.set()

    async def count_incall_participants(self, token: str) -> int:
        """How many people are in this call right now.

        The one-to-one gate polls this before joining, to wait out the ringing.
        The app reports it per call; a fresh fetch is used rather than the last
        poll's value because the gate polls faster than the monitor loop and
        needs the current count, not a five-second-old one.

        Returns 0 when the app cannot tell (no Talk, room gone) — which the gate
        treats the same as "not enough yet" and keeps waiting, up to its own
        timeout, then joins anyway. Never blocks a call forever on a null.
        """
        try:
            for call in await self._client.fetch_calls():
                if call.get("token") == token:
                    count = call.get("active_participants")
                    return int(count) if count is not None else 0
        except AppClientError as e:
            logger.debug("could not count participants for %s: %s", token, e)
        return 0

    async def _process(self, calls: list[dict]) -> None:
        """Turn a poll result into start/end events, as CallMonitor does."""
        current = set()

        for call in calls:
            token = call.get("token")
            if not token:
                continue

            # The room allowlist is applied here as well as in the app, because
            # the app's list is advisory config while this is the last gate
            # before a call is joined — the two should never disagree, but if
            # they do, not capturing is the safe side.
            if self.config.included_rooms and token not in self.config.included_rooms:
                continue
            if token in self.config.excluded_rooms:
                continue

            current.add(token)

            if token not in self._active_calls:
                info = CallInfo(
                    room_token=token,
                    room_name=call.get("name") or token,
                    room_type=int(call.get("type", 0)),
                    # The app does not report the call flag; assume audio, which
                    # is what capture needs and what every call has.
                    call_flag=1,
                    # Issued by the app when it is new enough to do so; empty
                    # otherwise, and the caller falls back to generating one.
                    session_id=str(call.get("session_id") or ""),
                )
                self._active_calls[token] = info
                logger.info(
                    "Call started: %s (%s), type=%d",
                    info.room_name, token, info.room_type,
                )
                asyncio.create_task(self._on_call_started(info))

        ended = set(self._active_calls) - current
        for token in ended:
            info = self._active_calls.pop(token)
            logger.info("Call ended: %s (%s)", info.room_name, token)
            asyncio.create_task(self._on_call_ended(token))
