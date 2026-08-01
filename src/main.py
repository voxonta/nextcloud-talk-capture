"""The capture service: join calls, stream them out, and nothing else.

Three moving parts and a short life:

  * the Nextcloud app says a call is live and hands over the settings,
  * `SpreedClient` joins it and yields one audio stream per speaker,
  * `BrainSink` streams those to the meeting gateway and closes with who was
    present and who each track belonged to.

What comes back — the transcript, the analysis — is not this service's business.
The gateway returns finished files and the Nextcloud app puts them where they
belong, using credentials that never leave the customer's side.
"""
from __future__ import annotations

# Must precede any aiortc import: it patches a race in aioice's candidate
# handling that otherwise drops a subscription now and then.
import talk_capture.aioice_patch  # noqa: F401  (import for side effect)

import asyncio
import logging
import os
import signal
import time
import uuid

from talk_capture.app_client import AppClient, overlay_app_settings
from talk_capture.app_monitor import AppCallMonitor
from talk_capture.brain_client import BrainSink
from talk_capture.config import CaptureConfig
from talk_capture.spreed_client import SpreedClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
for _noisy in ("aiortc.rtcrtpreceiver", "aioice", "websockets"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

logger = logging.getLogger("capture")

VERSION = "0.3.0"


class CallSession:
    """One call, from joining to the gateway accepting it."""

    def __init__(self, config: CaptureConfig, call_info):
        self.config = config
        self.call_info = call_info
        self.call_start = time.time()
        # The meeting id. The app issues it when it can, so the same call keeps
        # its id across a restart of this service and the gateway resumes rather
        # than recording a second meeting.
        self.session_id = getattr(call_info, "session_id", "") or str(uuid.uuid4())
        self._sink = BrainSink(config, {
            "session_id": self.session_id,
            "source": "nc-talk",
            "room_token": call_info.room_token,
            "room_name": call_info.room_name,
            "room_kind": call_info.room_type,
            "language": config.transcript_language,
            "timezone": config.timezone,
            "call_start_ms": int(self.call_start * 1000),
        })
        self._spreed = SpreedClient(
            config=config,
            room_token=call_info.room_token,
            on_audio_frame=self._on_audio_frame,
        )
        self._task = asyncio.create_task(self._run())

    async def _on_audio_frame(self, speaker_sid, frame):
        await self._sink.process_audio_frame(speaker_sid, frame, self.call_start)

    async def _run(self):
        try:
            await self._spreed.connect_and_join()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("capture failed for %s", self.call_info.room_token)

    async def stop(self):
        """Close the call: tell the gateway who was there, then let go.

        The roster and the track-to-person map are known only now — names settle
        as people join — so they ride out on the end frame rather than the
        opening context.
        """
        if self._task and not self._task.done():
            self._task.cancel()

        participants, uncaptured, present, speakers = [], [], None, {}
        try:
            participants = self._spreed.get_call_participants()
            uncaptured = self._spreed.get_uncaptured_participants()
            present = self._spreed.get_call_participant_count()
            speakers = {sid: self._spreed.get_speaker_name(sid)
                        for sid in self._spreed.speakers}
        except Exception:
            logger.exception("reading the roster failed for %s",
                             self.call_info.room_token)

        try:
            accepted = await self._sink.finalize(
                call_end_ms=int(time.time() * 1000),
                uncaptured=uncaptured,
                present_count=present,
                participants=participants,
                speakers=speakers,
            )
            if accepted:
                logger.info("gateway accepted %s [%s]",
                            self.call_info.room_token, accepted[0][:12])
        except Exception:
            logger.exception("closing the stream failed for %s",
                             self.call_info.room_token)

        try:
            await self._spreed.disconnect()
        except Exception:
            pass


class CaptureService:
    def __init__(self):
        self.config = CaptureConfig.from_env()
        self._app = AppClient(self.config.app_base_url, self.config.app_service_token)
        self._sessions: dict = {}
        self._monitor = None
        self._stop = asyncio.Event()

    async def _on_call_started(self, call_info):
        if call_info.room_token in self._sessions:
            return
        logger.info("call started: %s (%s)", call_info.room_name, call_info.room_token)
        self._sessions[call_info.room_token] = CallSession(self.config, call_info)

    async def _on_call_ended(self, room_token):
        session = self._sessions.pop(room_token, None)
        if session is None:
            return
        logger.info("call ended: %s", room_token)
        await session.stop()

    async def _heartbeat(self):
        """Tell the app this service is alive, and re-read its settings.

        Both are best-effort and share one tick, so a settings change reaches a
        running service without anyone touching the host."""
        while not self._stop.is_set():
            try:
                await self._app.heartbeat(VERSION)
                overlay_app_settings(self.config, await self._app.fetch_settings())
            except Exception as e:
                logger.warning("app unreachable: %s", e)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass

    async def run(self):
        if not self.config.nc_url or not self.config.app_service_token:
            raise SystemExit("NEXTCLOUD_URL and APP_SERVICE_TOKEN are required")
        if not self.config.brain_grpc_target:
            raise SystemExit("GATEWAY_TARGET is required — there is nowhere to send audio")

        # Everything else comes from the app: signalling, the bot account, room
        # scope, folder names. Fail loudly here rather than half-working later.
        overlay_app_settings(self.config, await self._app.fetch_settings())
        logger.info("settings loaded from %s", self.config.app_base_url)
        logger.info("streaming to gateway %s (tls=%s)",
                    self.config.brain_grpc_target, self.config.brain_grpc_tls)

        self._monitor = AppCallMonitor(
            config=self.config,
            client=self._app,
            on_call_started=self._on_call_started,
            on_call_ended=self._on_call_ended,
        )
        asyncio.create_task(self._heartbeat())
        logger.info("watching for calls")
        await self._monitor.start()

    async def shutdown(self):
        self._stop.set()
        if self._monitor is not None:
            self._monitor.stop()
        # Close live calls properly: a dropped stream would leave the gateway
        # holding a meeting it thinks is still running.
        await asyncio.gather(*(s.stop() for s in self._sessions.values()),
                             return_exceptions=True)


async def _main():
    service = CaptureService()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(service.shutdown()))
    await service.run()


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
