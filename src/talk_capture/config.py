"""What the capture service needs to know, and where it gets it.

Almost nothing comes from the environment. An installation supplies two things —
the address of its Nextcloud and a key — and the Nextcloud app hands over the
rest at startup: the signalling server and its secret, the bot account, which
rooms are in scope, where files belong. That is deliberate: an admin should be
able to run this without being handed a page of settings, and without needing
shell access to change one later.

The gateway endpoint is the exception, since it is not the customer's to know.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _rooms(raw: str) -> list:
    return [t.strip() for t in raw.split(",") if t.strip()]


@dataclass
class CaptureConfig:
    # ── Nextcloud ──
    # The only address an installation must supply. Fields marked "from the app"
    # start empty and are filled by overlay_app_settings() once it answers.
    nc_url: str = ""
    nc_user: str = ""          # from the app (bot account)
    nc_password: str = ""      # from the app (app password)

    # ── Talk signalling (from the app, which reads Talk's own config) ──
    hpb_url: str = ""
    hpb_secret: str = ""

    # ── The app ──
    # Shared secret gating /config, /calls and /heartbeat. Set once at install
    # on both sides; app_base_url defaults to nc_url, the app living there.
    app_service_token: str = ""
    app_base_url: str = ""
    poll_interval: float = 5.0

    # ── The meeting gateway ──
    # Where captured audio goes, and the key identifying this installation.
    # Outbound only: an installation behind NAT needs no inbound access.
    brain_grpc_target: str = ""
    brain_grpc_tls: bool = True
    brain_api_token: str = ""

    # ── Scope (from the app) ──
    # Empty included_rooms means every room; excluded_rooms wins over it.
    included_rooms: list = field(default_factory=list)
    excluded_rooms: list = field(default_factory=list)

    # ── Where finished files belong (from the app) ──
    # Travels with the call so returned files are named the way the customer
    # expects. This service does not write them — the app does.
    transcript_folder: str = ""
    analysis_folder: str = ""
    transcript_user: str = ""

    # ── Presentation, travels with the call ──
    transcript_language: str = "ru"
    timezone: str = "Europe/Moscow"

    diagnostic_logging: bool = False

    @classmethod
    def from_env(cls) -> "CaptureConfig":
        nc_url = os.environ.get("NEXTCLOUD_URL", "").rstrip("/")
        return cls(
            nc_url=nc_url,
            app_service_token=os.environ.get("APP_SERVICE_TOKEN", ""),
            app_base_url=os.environ.get("APP_BASE_URL", "").rstrip("/") or nc_url,
            poll_interval=float(os.environ.get("POLL_INTERVAL", "5")),
            brain_grpc_target=os.environ.get("GATEWAY_TARGET", ""),
            brain_grpc_tls=os.environ.get("GATEWAY_TLS", "true").lower() != "false",
            brain_api_token=os.environ.get("GATEWAY_TOKEN", ""),
            transcript_language=os.environ.get("TRANSCRIPT_LANGUAGE", "ru"),
            timezone=os.environ.get("TIMEZONE", "Europe/Moscow"),
            diagnostic_logging=(
                os.environ.get("DIAGNOSTIC_LOGGING", "false").lower() == "true"),
            # Room scope is the app's to decide; these exist so a canary can be
            # pinned locally without changing settings for everyone.
            included_rooms=_rooms(os.environ.get("INCLUDED_ROOMS", "")),
            excluded_rooms=_rooms(os.environ.get("EXCLUDED_ROOMS", "")),
        )
