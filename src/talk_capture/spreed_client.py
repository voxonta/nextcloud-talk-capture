"""HPB Signaling client with WebRTC audio reception.

Based on the Nextcloud live_transcription exApp protocol:
1. Connect to HPB signaling via WebSocket
2. Authenticate with HMAC-SHA256 (internal auth)
3. Join a room
4. Request offers from publishers
5. Establish recvonly WebRTC connections via aiortc
6. Receive per-speaker audio frames
"""

import asyncio
import hashlib
import hmac
import json
import logging
import random
import re
import time
from enum import IntEnum
from secrets import token_urlsafe
from dataclasses import dataclass, field

import aiohttp
import websockets
from aiortc import (
    RTCConfiguration,
    RTCIceServer,
    RTCPeerConnection,
    RTCSessionDescription,
)
from aiortc.mediastreams import MediaStreamTrack


logger = logging.getLogger(__name__)

# Protocol constants
MSG_RECEIVE_TIMEOUT = 10
MAX_CONNECT_TRIES = 5
HPB_PING_TIMEOUT = 120
RECONNECT_DELAY = 5

# Offer request retry limits (edge-triggered, join-time burst)
MAX_OFFER_RETRIES = 5
OFFER_RETRY_BASE_DELAY = 10  # seconds, doubles each retry

# Level-triggered resubscribe (background reconciliation sweep). The
# join-time backoff above eventually gives up permanently; this sweep is
# the safety net that keeps re-requesting an offer for a participant who is
# still in the call with audio but has no live peer (e.g. their subscriber
# peer never finished ICE and got closed). Without it, a silent participant
# whose peer failed early is never recorded even if they speak much later.
RECONCILE_INTERVAL = 30  # seconds between reconciliation sweeps
RESUBSCRIBE_MIN_INTERVAL = 30  # min seconds between resubscribe attempts per peer

# Fast connect watchdog: a healthy subscriber peer reaches "connected" in a
# few seconds. aiortc's own ICE failure only fires at ~64s, so a stuck
# negotiation wastes a full minute of audio before we re-request. The
# watchdog closes a peer that hasn't connected within this window and
# re-requests immediately, shrinking the gap ~3x.
ICE_CONNECT_TIMEOUT = 20  # seconds to reach "connected" before forcing a retry

# Small random delay before sending each requestoffer. When several
# participants are subscribed in one burst (e.g. joining a room with
# attendees already in the call), spacing the requests de-synchronises
# Janus-side subscriber-handle setup, which reduces the ICE negotiation
# races that cause first-attempt failures.
OFFER_REQUEST_JITTER = 0.3  # seconds, upper bound of uniform jitter


class CallFlag(IntEnum):
    DISCONNECTED = 0
    IN_CALL = 1
    WITH_AUDIO = 2
    WITH_VIDEO = 4
    WITH_PHONE = 8


@dataclass
class SpeakerInfo:
    """Info about a speaker in the call."""
    session_id: str  # HPB session ID
    user_id: str = ""  # Nextcloud user ID
    display_name: str = ""
    nc_session_id: str = ""  # Nextcloud session ID
    # Presence intervals in seconds (unix ts) from start to end of each
    # continuous stay in the room. One participant may have several intervals
    # if signaling disconnected and rejoined within the same call. Final
    # time_in_call = sum((end - start) for (start, end) in intervals).
    intervals: list[tuple[float, float | None]] = field(default_factory=list)


@dataclass
class TranscriptSegment:
    """A single transcribed segment."""
    timestamp: float  # seconds from call start
    speaker_session_id: str
    text: str
    is_final: bool = True


class SpreedClient:
    """Connects to HPB signaling, joins a room, receives per-speaker audio."""

    def __init__(
        self,
        config,
        room_token: str,
        on_audio_frame: "asyncio.coroutines | None" = None,
    ):
        self.config = config
        self.room_token = room_token
        self._on_audio_frame = on_audio_frame

        self._ws = None
        self._session_id = None
        self._resume_id = None
        self._running = False
        self._call_start_time = 0.0

        # Speaker tracking
        self.speakers: dict[str, SpeakerInfo] = {}  # session_id -> info
        self._nc_to_hpb_session: dict[str, str] = {}  # nc_session -> hpb_session
        # user_id -> last known display name (survives session reconnect so
        # we can re-attach a name when a participant rejoins with a new SID).
        self._user_display_names: dict[str, str] = {}

        # WebRTC peer connections per speaker
        self._peer_connections: dict[str, RTCPeerConnection] = {}
        self._audio_tracks: dict[str, MediaStreamTrack] = {}

        # Capture-status tracking for operational visibility. A subscriber
        # peer can fail ICE/DTLS and never produce audio — those gaps must
        # be greppable and marked in the published transcript.
        # session_id -> best-known display name, for every peer we created
        # a subscriber PeerConnection for (MCU offered us their stream).
        self._attempted_peers: dict[str, str] = {}
        # session_ids that produced >=1 decoded audio frame.
        self._captured_peers: set[str] = set()

        # ICE servers from HPB settings
        self._ice_servers: list[RTCIceServer] = []

        # Events received during room join (before signaling loop starts)
        self._pending_events: list[dict] = []

        # Offer request retry tracking: session_id -> (attempt_count, last_attempt_time)
        self._offer_retries: dict[str, tuple[int, float]] = {}
        # Per-speaker lock to prevent concurrent offer handling
        self._offer_locks: dict[str, asyncio.Lock] = {}
        # Diagnostic: ICE negotiation timing
        self._offer_request_time: dict[str, float] = {}

        # Level-triggered resubscribe state. `_want_audio` is the roster of
        # hpb session IDs that are currently in the call with audio and so
        # should have a live peer. The reconcile sweep compares this roster
        # against `_peer_connections` and re-requests offers for the gap.
        self._want_audio: set[str] = set()
        # session_id -> last resubscribe attempt time (rate limiter for the
        # sweep so a peer that keeps failing isn't busy-looped).
        self._last_resubscribe: dict[str, float] = {}
        self._reconcile_task: "asyncio.Task | None" = None

        # Transcript accumulator
        self.transcript: list[TranscriptSegment] = []
        self._transcript_lock = asyncio.Lock()

        # Message ID counter for tracking
        self._msg_counter = 0

    async def connect_and_join(self):
        """Connect to HPB, authenticate, and join the room."""
        self._running = True
        self._call_start_time = time.time()

        # When diagnostics are on, surface aioice's per-candidate-pair ICE
        # connectivity checks so a failed negotiation can be traced to the
        # exact pair that never validated.
        if self.config.diagnostic_logging:
            logging.getLogger("aioice").setLevel(logging.DEBUG)

        # Fetch ICE servers from Nextcloud
        await self._fetch_ice_servers()

        for attempt in range(MAX_CONNECT_TRIES):
            try:
                await self._connect()
                await self._authenticate()
                await self._join_room()
                logger.info("Joined room %s (session: %s)", self.room_token, self._session_id)
                # Start signaling monitor
                await self._signaling_loop()
                return
            except (websockets.exceptions.ConnectionClosed, ConnectionError) as e:
                logger.warning(
                    "Connection attempt %d/%d failed: %s",
                    attempt + 1, MAX_CONNECT_TRIES, e,
                )
                if attempt < MAX_CONNECT_TRIES - 1:
                    await asyncio.sleep(RECONNECT_DELAY * (attempt + 1))
            except Exception:
                logger.exception("Fatal error in spreed client")
                break

        logger.error("Failed to connect to HPB after %d attempts", MAX_CONNECT_TRIES)

    async def disconnect(self):
        """Gracefully disconnect from HPB and close WebRTC connections."""
        self._running = False

        # Stop the reconciliation sweep so it doesn't re-request offers
        # while we're tearing the call down.
        if self._reconcile_task:
            self._reconcile_task.cancel()
            self._reconcile_task = None

        # Close any still-open presence intervals at the end of the call.
        # Prefer the real call-end wall-clock moment over "now" only if a
        # call-end timestamp were available here — we just use time.time()
        # which differs from call_end by milliseconds in normal shutdown.
        now = time.time()
        for sp in self.speakers.values():
            if sp.intervals and sp.intervals[-1][1] is None:
                start, _ = sp.intervals[-1]
                sp.intervals[-1] = (start, now)

        # Close all peer connections
        for sid, pc in list(self._peer_connections.items()):
            try:
                await pc.close()
            except Exception:
                pass
        self._peer_connections.clear()
        self._audio_tracks.clear()

        # Close WebSocket
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        logger.info("Disconnected from room %s", self.room_token)

    def get_speaker_name(self, session_id: str) -> str:
        """Get display name for a speaker by session ID."""
        info = self.speakers.get(session_id)
        if info and info.display_name:
            return info.display_name
        if info and info.user_id:
            return info.user_id
        return f"Участник ({session_id[:8]})"

    def get_speaker_userid(self, session_id: str) -> str:
        """Get Nextcloud user ID for a speaker by session ID."""
        info = self.speakers.get(session_id)
        return info.user_id if info else ""

    def get_call_participants(self) -> list[str]:
        """Display names of everyone who was ever in the call (had a presence
        interval) — silent attendees included, by product decision the
        transcript `participants` list is "who was present", not "who spoke".
        First-seen order. Dedupe by user_id (fallback to session id) so a
        reconnect under a new SID doesn't double-count. Placeholder labels
        for peers we couldn't name are dropped downstream by the formatter,
        not here.
        """
        seen: set[str] = set()
        names: list[str] = []
        for sid, sp in self.speakers.items():
            if not sp.intervals:
                continue
            key = sp.user_id or f"sid:{sid}"
            if key in seen:
                continue
            seen.add(key)
            names.append(self.get_speaker_name(sid))
        return names

    def get_call_participant_user_ids(self) -> set[str]:
        """NC user IDs of everyone who was ever in the call (had a presence
        interval) — silent attendees included, anonymous guests excluded.

        Symmetric to `get_call_participants()` (display names), but returns
        user_ids for share routing: 1:1 calls share the transcript + analyzer
        summary with each participant individually (shareType=0), and the old
        path that iterated only the GigaAM-recognised speakers dropped anyone
        who joined and stayed muted — they got neither file. Guests without
        a user_id can't be share targets at all, so they're omitted here (the
        roster still surfaces them via display name).
        """
        return {sp.user_id for sp in self.speakers.values() if sp.intervals and sp.user_id}

    def get_call_participant_count(self) -> int:
        """Distinct people who were ever in the call (had a presence
        interval). Used to decide the `type/meeting/1-1` tag — accuracy
        matters, so a reconnect must not inflate the count. Mirrors
        get_call_participants() exactly so the count and the roster never
        diverge.
        """
        return len(self.get_call_participants())

    def get_uncaptured_participants(self) -> list[str]:
        """Display names of participants whose subscriber peer was attempted
        but never produced any audio (peer failed / 0 frames). Deduplicated,
        excluding any that DID get captured under any session_id for the same
        person. Used to mark gaps in the published transcript."""
        # Names of people we DID capture under at least one session id, so a
        # participant who reconnected under a fresh SID and was recorded there
        # is not flagged. Resolve names live (signaling may have filled them in
        # after capture started).
        captured_names: set[str] = set()
        for sid in self._captured_peers:
            name = self._best_name_for_sid(sid)
            if name:
                captured_names.add(name)

        uncaptured: set[str] = set()
        for sid, attempted_name in self._attempted_peers.items():
            if sid in self._captured_peers:
                continue
            if sid == self._session_id:
                continue  # never flag our own session
            name = self._best_name_for_sid(sid) or attempted_name
            if not name or name == "?":
                continue
            if name.startswith("Участник ("):
                continue  # placeholder, no real identity to report
            if name in captured_names:
                continue  # same person captured under a different SID
            uncaptured.add(name)

        return sorted(uncaptured)

    def _best_name_for_sid(self, session_id: str) -> str:
        """Resolve the best display name for a SID, falling back to the name
        recorded when the peer was first attempted."""
        info = self.speakers.get(session_id)
        if info and info.display_name:
            return info.display_name
        if info and info.user_id:
            return info.user_id
        return self._attempted_peers.get(session_id, "")

    # --- time_in_call interval helpers ---

    def _open_interval(self, session_id: str):
        """Start a presence interval for the given SID if none is open.

        Idempotent: a second call with a still-open last interval is a no-op,
        so duplicate join/inCall events don't create phantom intervals.
        """
        sp = self.speakers.get(session_id)
        if not sp:
            return
        if sp.intervals and sp.intervals[-1][1] is None:
            return  # already open
        sp.intervals.append((time.time(), None))

    def _close_interval(self, session_id: str):
        """Close the last open presence interval for the given SID.

        No-op if the SID is unknown or its last interval is already closed —
        lets us safely react to leave/inCall=0/call-end without needing to
        track state separately.
        """
        sp = self.speakers.get(session_id)
        if not sp or not sp.intervals:
            return
        start, end = sp.intervals[-1]
        if end is None:
            sp.intervals[-1] = (start, time.time())

    def _failsafe_close_previous_sids(self, user_id: str, exclude_sid: str):
        """When a brand-new SID appears for a user who already had intervals
        under a different SID, defensively close any still-open interval on
        those older SIDs. Covers the case where signaling dropped the
        inCall=0 event for the lost connection and we only know the user
        is back because a fresh SID arrived.
        """
        if not user_id:
            return
        for other_sid, sp in self.speakers.items():
            if other_sid == exclude_sid:
                continue
            if sp.user_id != user_id:
                continue
            if sp.intervals and sp.intervals[-1][1] is None:
                start, _ = sp.intervals[-1]
                sp.intervals[-1] = (start, time.time())
                logger.info(
                    "Failsafe closed interval on %s (user=%s) due to new SID %s",
                    other_sid[:16], user_id, exclude_sid[:16],
                )

    # --- Internal methods ---

    async def _fetch_ice_servers(self):
        """Fetch STUN/TURN servers from Nextcloud signaling settings."""
        url = f"{self.config.nc_url}/ocs/v2.php/apps/spreed/api/v3/signaling/settings"
        auth = aiohttp.BasicAuth(self.config.nc_user, self.config.nc_password)
        async with aiohttp.ClientSession(
            auth=auth,
            headers={"OCS-APIRequest": "true", "Accept": "application/json"},
        ) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.warning("Failed to fetch signaling settings: %d", resp.status)
                    return
                data = await resp.json()

        settings = data.get("ocs", {}).get("data", {})
        for stun in settings.get("stunservers", []):
            urls = stun.get("urls", stun.get("url", []))
            if isinstance(urls, str):
                urls = [urls]
            if urls:
                self._ice_servers.append(RTCIceServer(urls=urls))

        for turn in settings.get("turnservers", []):
            urls = turn.get("urls", turn.get("url", []))
            if isinstance(urls, str):
                urls = [urls]
            if urls:
                self._ice_servers.append(RTCIceServer(
                    urls=urls,
                    username=turn.get("username", ""),
                    credential=turn.get("credential", ""),
                ))

        logger.info("ICE servers: %d STUN + TURN entries", len(self._ice_servers))

    async def _connect(self):
        """Open WebSocket to HPB signaling."""
        self._ws = await websockets.connect(
            self.config.hpb_url,
            ping_timeout=HPB_PING_TIMEOUT,
            max_size=2**22,
        )
        logger.debug("WebSocket connected to %s", self.config.hpb_url)

    async def _authenticate(self):
        """Send hello with HMAC-SHA256 internal auth."""
        if self._resume_id:
            await self._send({
                "type": "hello",
                "hello": {"version": "2.0", "resumeid": self._resume_id},
            })
        else:
            nonce = token_urlsafe(64)
            token = hmac.new(
                self.config.hpb_secret.encode(),
                nonce.encode(),
                hashlib.sha256,
            ).hexdigest()
            # Send only the base NC URL -- the signaling server appends
            # the API path itself.  Sending the full path here caused a
            # doubled-URL bug in room-ping requests.
            backend_url = self.config.nc_url
            await self._send({
                "type": "hello",
                "hello": {
                    "version": "2.0",
                    # Declare "internal-incall" so signaling does NOT
                    # auto-set inCall=3.  Without this feature flag the
                    # server treats the bot as a regular participant,
                    # causing join/leave sounds and "deadline exceeded"
                    # errors when others try to subscribe to our
                    # (non-existent) publisher.
                    "features": ["internal-incall"],
                    "auth": {
                        "type": "internal",
                        "params": {
                            "random": nonce,
                            "token": token,
                            "backend": backend_url,
                        },
                    },
                },
            })

        # Wait for hello response
        while True:
            msg = await self._recv()
            if msg["type"] == "welcome":
                continue
            if msg["type"] == "hello":
                hello_data = msg["hello"]
                self._session_id = hello_data["sessionid"]
                self._resume_id = hello_data.get("resumeid", self._resume_id)
                logger.info("Authenticated, session: %s", self._session_id)
                return
            if msg["type"] == "error":
                code = msg.get("error", {}).get("code", "")
                if code == "no_such_session":
                    self._resume_id = None
                    return await self._authenticate()
                raise ConnectionError(f"Auth error: {msg}")
            if msg["type"] == "bye":
                raise ConnectionError("Server sent bye during auth")

    async def _join_room(self):
        """Join the Talk room via signaling."""
        # Step 1: Join room FIRST (must be in room before incall)
        await self._send({
            "type": "room",
            "room": {"roomid": self.room_token, "sessionid": self._session_id},
        })

        # Step 2: Wait for the room response before proceeding
        while True:
            msg = await self._recv()
            msg_type = msg.get("type", "")
            if msg_type == "room":
                room_data = msg.get("room", {})
                logger.info(
                    "Room joined: keys=%s",
                    list(room_data.keys()) if isinstance(room_data, dict) else type(room_data),
                )
                break
            elif msg_type == "error":
                raise ConnectionError(f"Room join error: {msg}")
            elif msg_type == "event":
                # Queue events received during room join for later processing
                self._pending_events.append(msg)
            elif msg_type == "message":
                self._pending_events.append(msg)
            else:
                logger.debug("During room join, received: %s", msg_type)

        # NOTE: We intentionally do NOT send the "incall" message here.
        # Sending incall triggers a join notification sound for other participants.
        # The bot can still receive room events and request WebRTC offers
        # without announcing itself as "in call".

    async def _signaling_loop(self):
        """Main loop processing signaling messages."""
        # Start the level-triggered reconciliation sweep (idempotent — a
        # reconnect re-enters this loop but reuses the running task).
        if self._reconcile_task is None or self._reconcile_task.done():
            self._reconcile_task = asyncio.create_task(self._reconcile_loop())

        # First, process any events received during room join
        for pending_msg in self._pending_events:
            msg_type = pending_msg.get("type", "")
            logger.debug("Processing pending %s event", msg_type)
            if msg_type == "event":
                await self._handle_event(pending_msg)
            elif msg_type == "message":
                await self._handle_message(pending_msg)
        self._pending_events.clear()

        while self._running:
            try:
                msg = await self._recv()
            except asyncio.TimeoutError:
                continue
            except websockets.exceptions.ConnectionClosed:
                if self._running:
                    logger.warning("Connection closed, attempting reconnect")
                    await self._reconnect()
                    continue
                break

            msg_type = msg.get("type", "")
            if msg_type not in ("event",):
                logger.info("MSG: type=%s keys=%s", msg_type, list(msg.keys()))

            if msg_type == "event":
                await self._handle_event(msg)
            elif msg_type == "message":
                await self._handle_message(msg)
            elif msg_type == "room":
                room_data = msg.get("room", {})
                logger.info("Room response: keys=%s", list(room_data.keys()) if isinstance(room_data, dict) else type(room_data))
            elif msg_type == "bye":
                logger.info("Received bye from signaling")
                break
            elif msg_type == "error":
                error = msg.get("error", {})
                code = error.get("code", "")
                error_id = msg.get("id", "")
                if code == "processing_failed":
                    continue
                # Don't break on client_not_found -- log and retry
                logger.error(
                    "Signaling error (id=%s code=%s): %s",
                    error_id, code, error.get("message", ""),
                )
                if code == "client_not_found":
                    # MCU subscriber not found -- will retry on next participant event
                    continue
                break

    async def _handle_event(self, msg: dict):
        """Handle participant events (join/leave/update)."""
        event = msg.get("event", {})
        target = event.get("target", "")
        event_type = event.get("type", "")
        logger.info(
            "Event: target=%s type=%s keys=%s",
            target, event_type, list(event.keys()),
        )

        if target == "room":
            if event_type == "join":
                # Room join events use LOWERCASE keys: sessionid, userid, user
                join_list = event.get("join", [])
                for entry in join_list:
                    # NB: lowercase keys in room events!
                    hpb_session = entry.get("sessionid", "") or entry.get("sessionId", "")
                    nc_user_id = entry.get("userid", "") or entry.get("userId", "")
                    user_data = entry.get("user", {})

                    logger.info(
                        "Room join entry: session=%s userid=%s keys=%s user=%s",
                        hpb_session[:16] if hpb_session else "?",
                        nc_user_id,
                        list(entry.keys()),
                        json.dumps(user_data, ensure_ascii=False)[:200] if user_data else "{}",
                    )

                    if not hpb_session or hpb_session == self._session_id:
                        continue

                    # Register speaker from room event. NB: room.join
                    # означает «открыл вкладку чата», а не «подключился к
                    # звонку». Интервал time_in_call открываем строго по
                    # участию в call (participants.update с inCall!=0 /
                    # participants.join с inCall).
                    display_name = ""
                    if isinstance(user_data, dict):
                        display_name = user_data.get("displayname", "")
                    speaker = self.speakers.get(hpb_session)
                    if speaker is None:
                        speaker = SpeakerInfo(
                            session_id=hpb_session,
                            user_id=nc_user_id,
                            display_name=display_name,
                        )
                        self.speakers[hpb_session] = speaker
                    else:
                        # Reconnect под тем же SID — подтянем имя если не было.
                        if nc_user_id and not speaker.user_id:
                            speaker.user_id = nc_user_id
                        if display_name and not speaker.display_name:
                            speaker.display_name = display_name
                    # Remember display name per user_id so a later reconnect
                    # (which may skip room.join and only emit participants.update)
                    # can still resolve the participant name.
                    if nc_user_id and display_name:
                        self._user_display_names[nc_user_id] = display_name
                    logger.info(
                        "Speaker registered: %s (%s)",
                        speaker.display_name or speaker.user_id or "?",
                        hpb_session[:16],
                    )

                    # Request offer if not already connected
                    if hpb_session not in self._peer_connections:
                        await self._request_offer(hpb_session)

            elif event_type == "leave":
                leave_list = event.get("leave", [])
                for entry in leave_list:
                    hpb_session = (entry if isinstance(entry, str)
                                   else entry.get("sessionid", "") or entry.get("sessionId", ""))
                    if hpb_session:
                        logger.info("Room leave: %s", hpb_session[:16])
                        # Failsafe: обычно интервал уже закрыт событием
                        # participants.update(inCall=0). Но если клиент
                        # рухнул — закроем здесь.
                        self._close_interval(hpb_session)
                        self._unmark_want_audio(hpb_session)
                        await self._close_peer(hpb_session)

        elif target == "participants":
            if event_type == "update":
                update_data = event.get("update", {})
                users = update_data.get("users", []) if isinstance(update_data, dict) else []
                for user in users:
                    session_id = user.get("sessionId", "")
                    in_call = user.get("inCall", 0)
                    internal = user.get("internal", False)

                    logger.info(
                        "Participant update: session=%s inCall=%d internal=%s keys=%s",
                        session_id[:16] if session_id else "?",
                        in_call, internal, list(user.keys()),
                    )

                    if self.config.diagnostic_logging:
                        logger.info(
                            "DIAG participant: session=%s userId=%s "
                            "displayName=%s actorType=%s actorId=%s",
                            session_id[:16] if session_id else "?",
                            user.get("userId", ""),
                            user.get("displayName", ""),
                            user.get("actorType", ""),
                            user.get("actorId", ""),
                        )

                    # Skip our own internal session
                    if internal:
                        continue

                    # inCall != 0 means the participant is in the call
                    # (any combination of WITH_AUDIO/WITH_VIDEO/WITH_PHONE).
                    # inCall == 0 means they left the call (but may still
                    # sit in the chat room).
                    if in_call != CallFlag.DISCONNECTED:
                        # If we see a new SID but never got a room.join for
                        # it (happens on participant reconnect when signaling
                        # resumes the session), register the speaker on the
                        # fly using data from this event.
                        if (session_id
                                and session_id != self._session_id
                                and session_id not in self.speakers):
                            reconnect_user_id = user.get("userId", "") or user.get("actorId", "")
                            display_name = self._user_display_names.get(
                                reconnect_user_id, ""
                            )
                            self.speakers[session_id] = SpeakerInfo(
                                session_id=session_id,
                                user_id=reconnect_user_id,
                                display_name=display_name,
                            )
                            logger.info(
                                "Speaker registered via participants.update "
                                "(likely reconnect): %s userId=%s (%s)",
                                display_name or reconnect_user_id or "?",
                                reconnect_user_id,
                                session_id[:16],
                            )
                            # Defensively close still-open intervals of
                                # prior SIDs for the same user — covers the
                                # case when signaling dropped the inCall=0
                                # event on the previous connection.
                            self._failsafe_close_previous_sids(
                                reconnect_user_id, session_id,
                            )

                        # Open presence interval (idempotent — does nothing
                        # if already open, so repeated inCall updates don't
                        # stack).
                        if session_id in self.speakers and session_id != self._session_id:
                            self._open_interval(session_id)

                        # Resolve the hpb session this update refers to —
                        # either the SID itself (known speaker) or via the
                        # nc->hpb mapping.
                        target_sid = (
                            session_id if session_id in self.speakers
                            else self._nc_to_hpb_session.get(session_id, "")
                        )

                        # Subscribe to audio only for participants actually
                        # carrying audio — transcriber has nothing to do with
                        # phone/video-only participants.
                        if in_call & CallFlag.WITH_AUDIO:
                            # Add to the desired-audio roster so the
                            # reconcile sweep keeps a peer alive for them
                            # even if this initial request fails.
                            self._mark_want_audio(target_sid)
                            if (target_sid
                                    and target_sid != self._session_id
                                    and target_sid not in self._peer_connections):
                                await self._request_offer(target_sid)
                        else:
                            # In call but no audio anymore (video/phone only)
                            # — stop wanting a peer for them.
                            self._unmark_want_audio(target_sid)
                    else:
                        # Left the call — close interval and drop peer.
                        if session_id in self.speakers:
                            self._close_interval(session_id)
                            self._unmark_want_audio(session_id)
                            await self._close_peer(session_id)
                        else:
                            hpb_session = self._nc_to_hpb_session.get(session_id, "")
                            if hpb_session:
                                self._close_interval(hpb_session)
                                self._unmark_want_audio(hpb_session)
                                await self._close_peer(hpb_session)

            elif event_type == "join":
                join_list = event.get("join", [])
                for entry in join_list:
                    hpb_session = entry.get("sessionId", "")
                    user_data = entry.get("user", {})
                    in_call = entry.get("inCall", 0)

                    logger.info(
                        "Participant join: session=%s inCall=%d keys=%s user=%s",
                        hpb_session[:16] if hpb_session else "?",
                        in_call, list(entry.keys()),
                        json.dumps(user_data, ensure_ascii=False)[:200] if isinstance(user_data, dict) else str(user_data),
                    )

                    if not hpb_session or hpb_session == self._session_id:
                        continue

                    # Register or merge speaker — don't overwrite an existing
                    # one, that would wipe accumulated intervals.
                    user_id = ""
                    display_name = ""
                    if isinstance(user_data, dict):
                        user_id = user_data.get("uid", "")
                        display_name = user_data.get("displayname", "")
                        if user_id and display_name:
                            self._user_display_names[user_id] = display_name
                    speaker = self.speakers.get(hpb_session)
                    if speaker is None:
                        speaker = SpeakerInfo(
                            session_id=hpb_session,
                            user_id=user_id,
                            display_name=display_name,
                        )
                        self.speakers[hpb_session] = speaker
                    else:
                        if user_id and not speaker.user_id:
                            speaker.user_id = user_id
                        if display_name and not speaker.display_name:
                            speaker.display_name = display_name

                    # Open presence interval only if this join carries
                    # inCall — i.e. they're actually in the call, not just
                    # entering the chat room.
                    if in_call != CallFlag.DISCONNECTED:
                        self._open_interval(hpb_session)

                    if in_call & CallFlag.WITH_AUDIO:
                        self._mark_want_audio(hpb_session)
                        if hpb_session not in self._peer_connections:
                            await self._request_offer(hpb_session)

            elif event_type in ("disinvite", "leave"):
                leave_list = event.get(event_type, [])
                for entry in leave_list:
                    hpb_session = entry if isinstance(entry, str) else entry.get("sessionId", "")
                    if hpb_session:
                        self._close_interval(hpb_session)
                        self._unmark_want_audio(hpb_session)
                        await self._close_peer(hpb_session)

    async def _handle_message(self, msg: dict):
        """Handle signaling messages (offers, candidates)."""
        message = msg.get("message", {})
        data = message.get("data", {})
        msg_data_type = data.get("type", "")
        sender = message.get("sender", {}).get("sessionid", "")

        if msg_data_type == "offer":
            logger.info(
                "Offer from sender=%s, data keys=%s, data.from=%s, data.sid=%s",
                sender, list(data.keys()), data.get("from", ""), data.get("sid", ""),
            )
            await self._handle_offer(sender, data)
        elif msg_data_type == "candidate":
            await self._handle_candidate(sender, data)
        elif msg_data_type == "endOfCandidates":
            pass  # Normal -- no action needed
        elif msg_data_type == "unshareScreen":
            pass
        else:
            logger.debug("Unhandled message type: %s", msg_data_type)

    async def _handle_offer(self, sender_sid: str, data: dict):
        """Handle WebRTC offer from a publisher -- create recvonly answer."""
        # Per-speaker lock prevents concurrent offer processing
        if sender_sid not in self._offer_locks:
            self._offer_locks[sender_sid] = asyncio.Lock()
        lock = self._offer_locks[sender_sid]

        if lock.locked():
            logger.info(
                "Skipping offer from %s — already processing one",
                sender_sid[:16],
            )
            return

        async with lock:
            await self._handle_offer_locked(sender_sid, data)

    async def _handle_offer_locked(self, sender_sid: str, data: dict):
        """Handle offer with lock held."""
        payload = data.get("payload", {})
        sdp = payload.get("sdp", "")
        offer_sid = data.get("sid", "")
        offer_room_type = data.get("roomType", "video")

        if not sdp:
            logger.warning("Offer without SDP from %s", sender_sid)
            return

        logger.info(
            "Processing offer: sender=%s sid=%s roomType=%s sdp_lines=%d",
            sender_sid[:16], offer_sid, offer_room_type, len(sdp.splitlines()),
        )

        # Close existing peer connection if any
        await self._close_peer(sender_sid)

        # Create new peer connection -- do NOT add transceivers manually,
        # let aiortc create them from the remote offer SDP
        rtc_config = RTCConfiguration(iceServers=self._ice_servers)
        pc = RTCPeerConnection(configuration=rtc_config)
        self._peer_connections[sender_sid] = pc

        # Record that we attempted a subscriber peer for this participant.
        # Keep the best-known display name (overwrite if a real name resolved
        # later — placeholder names are replaced once signaling fills them in).
        attempted_name = self.get_speaker_name(sender_sid)
        prev_name = self._attempted_peers.get(sender_sid)
        if prev_name is None or prev_name.startswith("Участник ("):
            self._attempted_peers[sender_sid] = attempted_name

        # Fast connect watchdog: don't wait the full ~64s ICE timeout if the
        # negotiation is stuck — re-request after ICE_CONNECT_TIMEOUT.
        asyncio.create_task(self._connect_watchdog(sender_sid, pc))

        @pc.on("track")
        async def on_track(track):
            if track.kind == "video":
                # Stop video tracks immediately to prevent memory-heavy decoding
                track.stop()
                return
            if track.kind == "audio":
                self._audio_tracks[sender_sid] = track
                logger.info(
                    "Audio track received from %s",
                    self.get_speaker_name(sender_sid),
                )
                if self._on_audio_frame:
                    asyncio.create_task(
                        self._read_audio_loop(sender_sid, track)
                    )

        @pc.on("connectionstatechange")
        async def on_state_change():
            state = pc.connectionState
            speaker_name = self.get_speaker_name(sender_sid)
            logger.info("Peer %s (%s) state: %s", sender_sid[:16], speaker_name, state)
            if state == "connected":
                self._reset_offer_retries(sender_sid)
                # Log selected ICE candidate pair
                self._log_ice_selected_pair(pc, sender_sid, speaker_name)
                if self.config.diagnostic_logging:
                    req_time = self._offer_request_time.pop(sender_sid, 0)
                    if req_time:
                        elapsed = time.time() - req_time
                        logger.info(
                            "DIAG ICE negotiation %s (%s): %.1fs",
                            sender_sid[:16], speaker_name, elapsed,
                        )
            elif state == "failed":
                logger.warning(
                    "ICE FAILED for %s (%s) — peer may have connectivity issues",
                    sender_sid[:16], speaker_name,
                )
                # Attempt-level signal (NOT an authoritative gap): this
                # subscription attempt failed before producing audio. With the
                # resubscribe/reconcile path it is usually recovered, so it is
                # deliberately NOT logged as CAPTURE_GAP. The authoritative
                # "never recorded" verdict is the finalize summary
                # (get_uncaptured_participants), which dedups by sid + name.
                if sender_sid not in self._captured_peers:
                    logger.warning(
                        "SUBSCRIBE_RETRY: %s (session=%s) failed ICE/DTLS with "
                        "0 frames — re-requesting offer",
                        speaker_name, sender_sid[:16],
                    )
                await self._close_peer(sender_sid)
                # Immediate recovery attempt instead of waiting up to a full
                # sweep interval — but only if they're still expected to have
                # audio (rate-limited inside _maybe_resubscribe).
                await self._maybe_resubscribe(sender_sid)
            elif state == "closed":
                await self._close_peer(sender_sid)

        # Set remote description and create answer
        offer = RTCSessionDescription(type="offer", sdp=sdp)
        await pc.setRemoteDescription(offer)

        # Set video transceivers to inactive to prevent video decoding
        for transceiver in pc.getTransceivers():
            if transceiver.receiver and transceiver.receiver.track:
                if transceiver.receiver.track.kind == "video":
                    transceiver.direction = "inactive"

        answer = await pc.createAnswer()
        await pc.setLocalDescription(answer)

        # Wait for ICE gathering to complete
        while pc.iceGatheringState != "complete":
            await asyncio.sleep(0.1)

        # Log local ICE candidates from gathered SDP
        local_candidates = self._parse_ice_candidates(pc.localDescription.sdp)
        candidate_types = {}
        for c in local_candidates:
            ctype = c.get("type", "unknown")
            candidate_types[ctype] = candidate_types.get(ctype, 0) + 1
        speaker_name = self.get_speaker_name(sender_sid)
        logger.info(
            "ICE LOCAL candidates for %s (%s): %s (total: %d)",
            sender_sid[:16], speaker_name,
            ", ".join(f"{t}={n}" for t, n in sorted(candidate_types.items())),
            len(local_candidates),
        )
        for c in local_candidates:
            logger.info(
                "  ICE LOCAL %s: %s:%s proto=%s type=%s raddr=%s rport=%s",
                sender_sid[:12],
                c.get("ip"), c.get("port"), c.get("proto"),
                c.get("type"), c.get("raddr", "-"), c.get("rport", "-"),
            )

        logger.info(
            "Answer ready: ICE=%s, SDP lines=%d",
            pc.iceGatheringState,
            len(pc.localDescription.sdp.splitlines()),
        )

        # Send answer back -- use the SAME roomType from the offer
        await self._send({
            "type": "message",
            "message": {
                "recipient": {"type": "session", "sessionid": sender_sid},
                "data": {
                    "type": "answer",
                    "roomType": offer_room_type,
                    "sid": offer_sid,
                    "payload": {
                        "type": "answer",
                        "sdp": pc.localDescription.sdp,
                    },
                },
            },
        })

        # Send end-of-candidates
        await self._send({
            "type": "message",
            "message": {
                "recipient": {"type": "session", "sessionid": sender_sid},
                "data": {
                    "type": "endOfCandidates",
                    "sid": offer_sid,
                    "roomType": offer_room_type,
                },
            },
        })

    async def _handle_candidate(self, sender_sid: str, data: dict):
        """Handle incoming ICE candidate."""
        pc = self._peer_connections.get(sender_sid)
        if not pc:
            return

        payload = data.get("payload", {})
        candidate_data = payload.get("candidate", {})
        candidate_str = candidate_data.get("candidate", "")
        if not candidate_str:
            return

        # Log remote ICE candidate details
        parsed = self._parse_candidate_str(candidate_str)
        if parsed:
            speaker_name = self.get_speaker_name(sender_sid)
            logger.info(
                "ICE REMOTE %s (%s): %s:%s proto=%s type=%s raddr=%s rport=%s",
                sender_sid[:12], speaker_name,
                parsed.get("ip"), parsed.get("port"), parsed.get("proto"),
                parsed.get("type"), parsed.get("raddr", "-"), parsed.get("rport", "-"),
            )

        # Actually add the trickled candidate to the peer connection.
        # Previously this was a no-op ("candidates are already in the SDP")
        # which only holds when the publisher/MCU embeds all candidates in
        # the offer. Janus runs full-trickle, so the working candidate often
        # arrives here *after* the offer — dropping it left the subscriber
        # with no usable remote candidate and ICE failed at the ~64s timeout
        # ("trickle pending" on the Janus side). Adding it lets the pair form.
        from aiortc.sdp import candidate_from_sdp
        try:
            sdp = candidate_str
            if sdp.startswith("candidate:"):
                sdp = sdp[len("candidate:"):]
            candidate = candidate_from_sdp(sdp)
            candidate.sdpMid = candidate_data.get("sdpMid", "0")
            candidate.sdpMLineIndex = candidate_data.get("sdpMLineIndex", 0)
            await pc.addIceCandidate(candidate)
        except Exception as e:
            logger.debug("Failed to add ICE candidate for %s: %s", sender_sid[:12], e)

    def _can_request_offer(self, publisher_sid: str) -> bool:
        """Check if we can request an offer (respects backoff and retry limit)."""
        if publisher_sid not in self._offer_retries:
            return True
        attempts, last_time = self._offer_retries[publisher_sid]
        if attempts >= MAX_OFFER_RETRIES:
            return False
        # Exponential backoff: 10s, 20s, 40s, 80s, 160s
        delay = OFFER_RETRY_BASE_DELAY * (2 ** attempts)
        return (time.time() - last_time) >= delay

    def _record_offer_attempt(self, publisher_sid: str):
        """Record an offer request attempt for backoff tracking."""
        attempts, _ = self._offer_retries.get(publisher_sid, (0, 0.0))
        self._offer_retries[publisher_sid] = (attempts + 1, time.time())

    def _reset_offer_retries(self, publisher_sid: str):
        """Reset retry counter on successful connection."""
        self._offer_retries.pop(publisher_sid, None)

    async def _request_offer(self, publisher_sid: str):
        """Request WebRTC offer from a publisher (with backoff)."""
        if not self._can_request_offer(publisher_sid):
            attempts, _ = self._offer_retries.get(publisher_sid, (0, 0.0))
            if attempts >= MAX_OFFER_RETRIES:
                logger.warning(
                    "Giving up requestoffer for %s after %d attempts",
                    publisher_sid[:12], attempts,
                )
            return

        self._record_offer_attempt(publisher_sid)
        self._offer_request_time[publisher_sid] = time.time()
        attempts, _ = self._offer_retries[publisher_sid]
        logger.info(
            "Requesting offer from %s (attempt %d/%d)",
            publisher_sid[:12], attempts, MAX_OFFER_RETRIES,
        )
        # De-synchronise burst subscriptions to reduce Janus-side
        # subscriber-handle setup races (see OFFER_REQUEST_JITTER).
        if OFFER_REQUEST_JITTER > 0:
            await asyncio.sleep(random.uniform(0, OFFER_REQUEST_JITTER))
        await self._send({
            "type": "message",
            "message": {
                "recipient": {"type": "session", "sessionid": publisher_sid},
                "data": {"type": "requestoffer", "roomType": "video"},
            },
        })

    async def _connect_watchdog(self, session_id: str, pc: RTCPeerConnection):
        """Force a re-subscribe if a subscriber peer doesn't connect within
        ICE_CONNECT_TIMEOUT, instead of waiting for aiortc's ~64s ICE timeout.

        Each peer instance gets its own watchdog; a stale one (peer already
        replaced or closed) no-ops, so retries are naturally paced at one per
        peer-instance with no busy-loop.
        """
        try:
            await asyncio.sleep(ICE_CONNECT_TIMEOUT)
        except asyncio.CancelledError:
            return
        if not self._running:
            return
        # Act only if this exact pc is still the live one and not yet up.
        if self._peer_connections.get(session_id) is not pc:
            return
        if pc.connectionState in ("connected", "closed", "failed"):
            return
        logger.warning(
            "CONNECT TIMEOUT %s (%s): no connection in %ds — closing and "
            "re-requesting offer",
            session_id[:16], self.get_speaker_name(session_id), ICE_CONNECT_TIMEOUT,
        )
        await self._close_peer(session_id)
        # Re-request immediately if the participant should still carry audio.
        # Clear the join-time backoff so this fast retry isn't throttled.
        if session_id in self._want_audio:
            self._reset_offer_retries(session_id)
            await self._request_offer(session_id)

    # --- Level-triggered resubscribe (reconciliation sweep) ---

    def _mark_want_audio(self, session_id: str):
        """Add a session to the desired-audio roster (idempotent)."""
        if session_id and session_id != self._session_id:
            self._want_audio.add(session_id)

    def _unmark_want_audio(self, session_id: str):
        """Remove a session from the desired-audio roster."""
        self._want_audio.discard(session_id)
        self._last_resubscribe.pop(session_id, None)

    async def _maybe_resubscribe(self, session_id: str):
        """Re-request an offer for a desired-audio peer that has no live peer.

        Shared by the periodic reconcile sweep and the immediate `failed`
        handler. Per-peer rate-limited by RESUBSCRIBE_MIN_INTERVAL so a peer
        that keeps failing is retried for the whole call without busy-looping.
        """
        if not self._running:
            return
        if session_id == self._session_id or session_id not in self._want_audio:
            return
        # A live or still-negotiating peer already exists — nothing to do.
        if session_id in self._peer_connections:
            return
        lock = self._offer_locks.get(session_id)
        if lock and lock.locked():
            return  # an offer is already in flight

        now = time.time()
        last = self._last_resubscribe.get(session_id, 0.0)
        if now - last < RESUBSCRIBE_MIN_INTERVAL:
            return
        self._last_resubscribe[session_id] = now

        speaker_name = self.get_speaker_name(session_id)
        logger.warning(
            "RESUBSCRIBE %s (%s): in-call with audio but no live peer — "
            "re-requesting offer",
            session_id[:16], speaker_name,
        )
        # Clear the join-time backoff so its permanent give-up never strands
        # a participant who is still present. The sweep's min-interval is the
        # rate limiter from here on.
        self._reset_offer_retries(session_id)
        await self._request_offer(session_id)

    async def _reconcile_subscriptions(self):
        """One reconciliation pass: re-request offers for desired-audio
        participants that currently have no peer connection."""
        for session_id in list(self._want_audio):
            await self._maybe_resubscribe(session_id)

    async def _reconcile_loop(self):
        """Background sweep that reconciles the desired-audio roster against
        live peer connections, recovering peers whose subscriber connection
        failed (so a participant who is silent early and speaks later is
        still recorded)."""
        logger.info("Reconcile sweep started (interval=%ds)", RECONCILE_INTERVAL)
        try:
            while self._running:
                await asyncio.sleep(RECONCILE_INTERVAL)
                try:
                    await self._reconcile_subscriptions()
                except Exception:
                    logger.exception("Reconcile sweep iteration failed")
        except asyncio.CancelledError:
            pass
        finally:
            logger.info("Reconcile sweep stopped")

    async def _read_audio_loop(self, speaker_sid: str, track: MediaStreamTrack):
        """Read audio frames from a track and forward to callback."""
        frame_count = 0
        error_count = 0
        try:
            while self._running and speaker_sid in self._audio_tracks:
                try:
                    frame = await asyncio.wait_for(track.recv(), timeout=5.0)
                    frame_count += 1
                    if frame_count == 1:
                        # First decoded frame — this peer is now captured.
                        self._captured_peers.add(speaker_sid)
                    if self._on_audio_frame:
                        await self._on_audio_frame(speaker_sid, frame)
                    # Log progress periodically
                    if frame_count % 500 == 0:
                        logger.info(
                            "Audio loop [%s]: %d frames received, %d errors",
                            speaker_sid[:12], frame_count, error_count,
                        )
                except asyncio.TimeoutError:
                    logger.debug("Audio recv timeout for %s", speaker_sid[:12])
                    continue
                except Exception as e:
                    error_str = str(e).lower()
                    if "ended" in error_str or "stop" in error_str:
                        logger.info(
                            "Track ended for %s: %s (after %d frames)",
                            speaker_sid[:12], e, frame_count,
                        )
                        break
                    error_count += 1
                    logger.warning(
                        "Audio read error for %s: %s (frame %d, error #%d)",
                        speaker_sid[:12], e, frame_count, error_count,
                    )
                    if error_count > 10:
                        logger.error(
                            "Too many audio errors for %s, stopping", speaker_sid[:12],
                        )
                        break
        finally:
            display_name = self.get_speaker_name(speaker_sid)
            logger.info(
                "Audio stream ended for %s: %d frames total, %d errors",
                display_name, frame_count, error_count,
            )
            if frame_count == 0:
                # Attempt-level signal: this subscriber peer instance ended
                # without audio (failed / closed / replaced, or call end). It
                # does NOT mean the participant is unrecorded — they may be (or
                # have been) captured by another peer instance. Reserve the
                # CAPTURE_GAP token for the authoritative finalize summary
                # (get_uncaptured_participants).
                logger.warning(
                    "SUBSCRIBE_RETRY: subscriber peer for %s (session=%s) "
                    "ended with 0 frames",
                    display_name, speaker_sid[:16],
                )

    @staticmethod
    def _parse_candidate_str(candidate_str: str) -> dict | None:
        """Parse a single ICE candidate string into a dict."""
        # Format: candidate:<foundation> <component> <proto> <priority> <ip> <port> typ <type> [raddr <raddr> rport <rport>]
        m = re.match(
            r"candidate:\S+\s+\d+\s+(\S+)\s+\d+\s+(\S+)\s+(\d+)\s+typ\s+(\S+)"
            r"(?:\s+raddr\s+(\S+)\s+rport\s+(\d+))?",
            candidate_str,
        )
        if not m:
            return None
        result = {
            "proto": m.group(1),
            "ip": m.group(2),
            "port": m.group(3),
            "type": m.group(4),
        }
        if m.group(5):
            result["raddr"] = m.group(5)
            result["rport"] = m.group(6)
        return result

    @staticmethod
    def _parse_ice_candidates(sdp: str) -> list[dict]:
        """Extract ICE candidates from SDP."""
        candidates = []
        for line in sdp.splitlines():
            if line.startswith("a=candidate:"):
                candidate_str = line[2:]  # strip "a="
                parsed = SpreedClient._parse_candidate_str(candidate_str)
                if parsed:
                    candidates.append(parsed)
        return candidates

    def _log_ice_selected_pair(self, pc: RTCPeerConnection, sender_sid: str, speaker_name: str):
        """Log the selected ICE candidate pair after connection."""
        try:
            # Access ICE transport via DTLS transport chain
            for transceiver in pc.getTransceivers():
                if transceiver.receiver and transceiver.receiver.track:
                    if transceiver.receiver.track.kind == "audio":
                        transport = getattr(transceiver.receiver, "_transport", None)
                        if transport:
                            # DTLS transport -> ICE transport
                            ice = getattr(transport, "_transport", None)
                            if ice:
                                pair = getattr(ice, "_nominated", None) or getattr(ice, "_pair", None)
                                if pair:
                                    local = getattr(pair, "local_candidate", None)
                                    remote = getattr(pair, "remote_candidate", None)
                                    logger.info(
                                        "ICE SELECTED %s (%s): local=%s:%s(%s) remote=%s:%s(%s)",
                                        sender_sid[:12], speaker_name,
                                        getattr(local, "host", "?"),
                                        getattr(local, "port", "?"),
                                        getattr(local, "type", "?"),
                                        getattr(remote, "host", "?"),
                                        getattr(remote, "port", "?"),
                                        getattr(remote, "type", "?"),
                                    )
                                    return
            # Fallback: parse remote SDP for candidate info
            if pc.remoteDescription:
                remote_candidates = self._parse_ice_candidates(pc.remoteDescription.sdp)
                remote_types = {}
                for c in remote_candidates:
                    ctype = c.get("type", "unknown")
                    remote_types[ctype] = remote_types.get(ctype, 0) + 1
                logger.info(
                    "ICE REMOTE candidates in SDP for %s (%s): %s",
                    sender_sid[:12], speaker_name,
                    ", ".join(f"{t}={n}" for t, n in sorted(remote_types.items())),
                )
        except Exception as e:
            logger.debug("Could not log ICE pair for %s: %s", sender_sid[:12], e)

    async def _close_peer(self, session_id: str):
        """Close a peer connection and clean up."""
        self._audio_tracks.pop(session_id, None)
        self._offer_locks.pop(session_id, None)
        pc = self._peer_connections.pop(session_id, None)
        if pc:
            try:
                await pc.close()
            except Exception:
                pass

    async def _reconnect(self):
        """Attempt to reconnect to signaling."""
        try:
            if self._ws:
                await self._ws.close()
        except Exception:
            pass

        await asyncio.sleep(RECONNECT_DELAY)
        try:
            await self._connect()
            await self._authenticate()
            await self._join_room()
            logger.info("Reconnected to room %s", self.room_token)
        except Exception:
            logger.exception("Reconnect failed")
            self._running = False

    async def _send(self, msg: dict):
        """Send JSON message to signaling."""
        if self._ws:
            # Add message ID for tracking
            self._msg_counter += 1
            msg_id = f"t{self._msg_counter}"
            msg["id"] = msg_id

            # Log outgoing messages (non-trivial ones)
            msg_type = msg.get("type", "")
            if msg_type == "message":
                data = msg.get("message", {}).get("data", {})
                recipient = msg.get("message", {}).get("recipient", {})
                logger.info(
                    "SEND [%s] %s -> %s (roomType=%s, sid=%s)",
                    msg_id, data.get("type", "?"),
                    recipient.get("sessionid", "?")[:16],
                    data.get("roomType", ""),
                    data.get("sid", ""),
                )
            elif msg_type not in ("hello",):
                logger.info("SEND [%s] type=%s", msg_id, msg_type)

            await self._ws.send(json.dumps(msg))

    async def _recv(self) -> dict:
        """Receive JSON message from signaling."""
        raw = await asyncio.wait_for(self._ws.recv(), timeout=MSG_RECEIVE_TIMEOUT)
        return json.loads(raw)
