"""Capturing a Nextcloud Talk call and streaming it to the meeting gateway.

Two halves, and nothing between them: `AppCallMonitor` learns from the Nextcloud
app that a call is live and what the settings are, and `BrainSink` streams the
per-speaker audio out. What happens to that audio afterwards is not this
package's concern — the gateway returns finished files, and the Nextcloud app
puts them where they belong.

Names resolve on first use rather than at import. Joining a call needs the WebRTC
stack, ~120 MB of it; talking to the gateway needs none of that, and a service
that only forwards audio somebody else captured should not pay for the half it
never touches.
"""
from importlib import import_module

_WHERE = {
    "AppClient": "app_client",
    "ServiceSettings": "app_client",
    "overlay_app_settings": "app_client",
    "AppCallMonitor": "app_monitor",
    "BrainArtifacts": "brain_client",
    "BrainSink": "brain_client",
    "CallInfo": "call_info",
    "CaptureConfig": "config",
    "SpreedClient": "spreed_client",
}

__all__ = list(_WHERE)


def __getattr__(name: str):
    module = _WHERE.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return getattr(import_module(f".{module}", __name__), name)


def __dir__():
    return sorted(__all__)
