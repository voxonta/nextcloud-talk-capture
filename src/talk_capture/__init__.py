"""Capturing a Nextcloud Talk call and streaming it to the meeting gateway.

Two halves, and nothing between them: `AppCallMonitor` learns from the Nextcloud
app that a call is live and what the settings are, and `BrainSink` streams the
per-speaker audio out. What happens to that audio afterwards is not this
package's concern — the gateway returns finished files, and the Nextcloud app
puts them where they belong.
"""

from .app_client import AppClient, ServiceSettings
from .app_monitor import AppCallMonitor
from .brain_client import BrainArtifacts, BrainSink
from .call_info import CallInfo
from .config import CaptureConfig
from .spreed_client import SpreedClient

__all__ = [
    "AppClient", "AppCallMonitor", "BrainArtifacts", "BrainSink",
    "CallInfo", "CaptureConfig", "ServiceSettings", "SpreedClient",
]
