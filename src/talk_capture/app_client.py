"""Client for the Nextcloud archive app's service endpoints.

The app (done-transcription-app) is the single place a customer configures this
system: it hands out the settings, the Nextcloud credentials to sign in with,
and the list of live calls. This is the capture side of that — one HTTP client
that fetches those three things and reports back a heartbeat.

Why it exists: the alternative is what we do today — read Nextcloud's MySQL over
an SSH tunnel and take every setting from a .env on the host. Both need access a
customer cannot reasonably grant to a vendor, which is what has kept this from
being something they install themselves. The app needs only a shared secret,
entered once.

Authentication is a bearer token: the local secret the administrator set in the
app's settings and in this service's own config. It never leaves the customer's
network — the app is inside it — so it gates access to the credentials the app
returns without itself being a credential to anything of ours.
"""
from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class ServiceSettings:
    """What the app says this instance is configured to do.

    Folder names and the room allowlist are the customer's, not ours; taking
    them from the app rather than a .env is what lets an instance rename them
    without a redeploy.
    """

    enabled: bool
    publish_to_chat: bool
    rooms: tuple[str, ...]
    analysis_folder: str
    transcripts_folder: str
    minutes_folder: str
    # The Nextcloud account to sign in as, or None when the administrator has
    # not set one up yet — in which case there is nothing to capture with and
    # the caller must wait rather than guess.
    nc_user: str | None
    nc_password: str | None
    # The signaling server and its secret, from Talk's own config, or None when
    # Talk has no external signaling server set — then HPB_URL/HPB_SECRET stay
    # the administrator's to provide.
    hpb_url: str | None
    hpb_secret: str | None


class AppClientError(Exception):
    """The app could not be reached or refused the request."""


class AppClient:
    """Talks to the archive app's /service endpoints over HTTP."""

    def __init__(self, base_url: str, token: str, *, app_id: str = "voxonta", timeout: float = 15.0):
        # base_url is the Nextcloud root; the app lives under a fixed path below
        # it, so the caller configures one URL, not three. The app id is a
        # parameter rather than a constant because it has already changed once:
        # the app was renamed to `voxonta` while this still asked for
        # `done_transcription`, every poll answered 404, and the interceptor
        # went blind to calls for a week without saying anything louder than a
        # warning. Configurable, it can follow a rename without a rebuild.
        self._base = f"{base_url.rstrip('/')}/apps/{app_id}/api/v1/service"
        self._token = token
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def fetch_settings(self) -> ServiceSettings:
        """The current configuration, including the sign-in credentials."""
        data = await self._get("/config")
        nc = data.get("nextcloud") or {}
        folders = data.get("folders") or {}
        sig = data.get("signaling") or {}
        return ServiceSettings(
            enabled=bool(data.get("enabled", True)),
            publish_to_chat=bool(data.get("publish_to_chat", True)),
            rooms=tuple(data.get("rooms") or ()),
            analysis_folder=str(folders.get("analysis", "")),
            transcripts_folder=str(folders.get("transcripts", "")),
            minutes_folder=str(folders.get("minutes", "")),
            # Absent, not empty: a missing account and an account with a blank
            # name are the same non-answer, and both mean "not ready".
            nc_user=nc.get("user") or None,
            nc_password=nc.get("password") or None,
            hpb_url=sig.get("url") or None,
            hpb_secret=sig.get("secret") or None,
        )

    async def fetch_calls(self) -> list[dict]:
        """The calls live right now.

        Each is ``{token, name, type, started_at}``; call_flag is not among them
        because the app learns of a call from an event, not the room's live
        flags, and the capture side only needs to know a call is happening.

        @return list, oldest first — the order to pick them up in.
        """
        data = await self._get("/calls")
        calls = data.get("calls")
        return calls if isinstance(calls, list) else []

    async def heartbeat(self, version: str, note: str = "") -> None:
        """Report that this service is alive, so the administrator can see it.

        Best-effort: a failed heartbeat is not worth interrupting capture for,
        and the app already treats silence as "not connected".
        """
        try:
            await self._post("/heartbeat", {"version": version, "note": note})
        except AppClientError as e:
            logger.warning("heartbeat failed: %s", e)

    async def _get(self, path: str) -> dict:
        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            try:
                async with session.get(
                    self._base + path, headers=self._headers()
                ) as resp:
                    return await self._body(resp)
            except aiohttp.ClientError as e:
                raise AppClientError(f"GET {path}: {e}") from e

    async def _post(self, path: str, payload: dict) -> dict:
        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            try:
                async with session.post(
                    self._base + path, headers=self._headers(), json=payload
                ) as resp:
                    return await self._body(resp)
            except aiohttp.ClientError as e:
                raise AppClientError(f"POST {path}: {e}") from e

    async def _body(self, resp: aiohttp.ClientResponse) -> dict:
        if resp.status == 401:
            # The one error worth naming: a wrong or missing key is a
            # misconfiguration a human has to fix, not a transient fault.
            raise AppClientError(
                "unauthorised — the access key here does not match the app's"
            )
        if resp.status >= 400:
            raise AppClientError(f"HTTP {resp.status}")
        try:
            return await resp.json()
        except (aiohttp.ContentTypeError, ValueError) as e:
            raise AppClientError(f"bad response: {e}") from e

    def _headers(self) -> dict:
        # Both header forms, matching the app: a bearer token is the
        # convention, while some proxies strip Authorization headers.
        return {
            "Authorization": f"Bearer {self._token}",
            "X-Transcription-Token": self._token,
        }


def _sanitize_ws_url(url: str) -> str:
    """Normalise a signaling URL to the ws(s):// form ending in /spreed.

    Duplicated from the orchestrator's config on purpose: this library must not
    import the orchestrator, and the transform is three lines. Talk stores the
    server as https://host; the capture side dials wss://host/spreed.
    """
    url = re.sub(r"^http://", "ws://", url)
    url = re.sub(r"^https://", "wss://", url)
    if not url.rstrip("/").endswith("/spreed"):
        url = url.rstrip("/") + "/spreed"
    return url


def settings_fingerprint(settings: ServiceSettings) -> str:
    """A stable digest of the settings a running capture cares about.

    Lets a periodic refresh tell a real change (a credential created or rotated
    after startup, a folder renamed, a room allowlist edited) from a no-op,
    without ever putting the secret it covers into a log line. Covers exactly
    the fields ``overlay_app_settings`` writes onto Config — change one and this
    digest moves, change nothing and it holds.
    """
    raw = repr((
        settings.nc_user, settings.nc_password,
        settings.hpb_url, settings.hpb_secret,
        settings.enabled, settings.rooms,
        settings.transcripts_folder, settings.analysis_folder,
    ))
    return hashlib.sha256(raw.encode()).hexdigest()


def overlay_app_settings(config, settings: ServiceSettings) -> None:
    """Write the app's settings onto a live capture Config.

    A free function so it can be checked without standing up the orchestrator:
    it is plain field assignment, and the value is that the right fields move.
    Config's consumers read these at use-time, so mutating the shared object
    reaches all of them.

    A missing account leaves the credentials untouched rather than blanking
    them — the error then reads as "no account set up", not a sign-in as an
    empty user.
    """
    if settings.nc_user and settings.nc_password:
        config.nc_user = settings.nc_user
        config.nc_password = settings.nc_password
        # The account that writes transcripts is the same one that signs in.
        config.transcript_user = settings.nc_user

    # Folder names are the app's to own; it stores the bare name, the
    # orchestrator wants it under Talk/.
    if settings.transcripts_folder:
        config.transcript_folder = "Talk/" + settings.transcripts_folder
    if settings.analysis_folder:
        config.analysis_folder = "Talk/" + settings.analysis_folder

    # The signaling server, from Talk's config, so a client install need not be
    # told the HPB address or its secret. The capture side normalises the URL to
    # the wss form it dials, so the raw https server is stored as-is here and
    # sanitised where it is consumed — same as an env-provided HPB_URL.
    if settings.hpb_url and settings.hpb_secret:
        config.hpb_url = _sanitize_ws_url(settings.hpb_url)
        config.hpb_secret = settings.hpb_secret

    # The room allowlist, so the app is the one place a pilot is steered from.
    if settings.rooms:
        config.included_rooms = list(settings.rooms)
