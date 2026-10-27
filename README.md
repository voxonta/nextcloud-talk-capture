# nextcloud-talk-capture

Captures Nextcloud Talk and Element Call meetings: the service joins a call,
takes one audio stream per speaker, and streams it to a processing gateway.

That is all it does. No recognition, no analysis, no writing files — the finished
files come back from the gateway, and the [Nextcloud app](https://github.com/voxonta/voxonta-nextcloud-app)
puts them away under its own credentials, which never leave the customer's
premises.

Part of [Voxonta](https://voxonta.com). Documentation:
[running the connector yourself](https://voxonta.com/docs/self-hosting/).

## Three parts, one of them here

```
Nextcloud + the app             the connector (this repo)            the cloud
───────────────────             ─────────────────────────            ─────────
keeps the meeting archive       reads its settings from the app
serves settings and calls  ───▶ joins the call
writes the finished files       streams per-speaker audio       ───▶ processing
        ▲                                                            │
        └────────────────── finished files ◀─────────────────────────┘
```

Nextcloud is a black box to the cloud and the other way round: the connector does
not know what happens to the audio once it is sent, and the cloud has no access to
Nextcloud.

When a room opts out of recording (`/no-record` in its chat), the app lists the
call with `recording: false` and the connector stops sending its audio within
one poll — five seconds by default — while staying in the call, so `/record`
continues the same meeting. Needs the app 3.0.9 or later; with an older app
every call is recorded, as before.

Joining a call is the one part that depends on the platform. The app lists each
call with a `platform` — `talk` when it says nothing — and the service picks a
driver for it (`talk_capture.driver`); a call on a platform it does not know is
logged and left alone.

| `platform` | Calls | Listed by | Driver |
|---|---|---|---|
| `talk` | Nextcloud Talk | the [Nextcloud app](https://github.com/voxonta/voxonta-nextcloud-app) | WebRTC through the signalling server, as a participant |
| `livekit` | Element Call | [voxonta-matrix-app](https://github.com/voxonta/voxonta-matrix-app) | LiveKit, hidden and listen-only, with a token the integration signs |

On LiveKit only microphones are listened to — not cameras, not screen-share
audio — one stream per device, so a person on a laptop and a phone is two
tracks under one name. A muted microphone is not sent: the SDK fills a silent
track with exact zeros, and those are dropped. If the connection is lost the
service rejoins with the newest token the integration has listed.

## Install

Two values are needed. Everything else the service asks the app for at startup —
the signalling server and its secret, the bot account, which conversations to
capture, the folder names.

```bash
docker run -d --name nextcloud-talk-capture \
  -e NEXTCLOUD_URL=https://cloud.example.com \
  -e APP_SERVICE_TOKEN=<key from the app's admin settings> \
  -e GATEWAY_TARGET=<gateway address> \
  -e GATEWAY_TOKEN=<gateway key> \
  nextcloud-talk-capture
```

For Element Call there is no Nextcloud: point `APP_BASE_URL` at
voxonta-matrix-app's service port instead, with its `SERVICE_TOKEN` as
`APP_SERVICE_TOKEN`. One service serves one integration; for both platforms,
run two. The image includes the LiveKit SDK; installing from PyPI, ask for it:
`pip install 'nextcloud-talk-capture[livekit]'`.

The service makes **outbound connections only**: behind NAT it needs no port
forwarding and no inbound webhooks.

### Where to put it

Capture is a WebRTC client — it connects to the signalling server the way an
ordinary participant does. Running it next to the signalling server is not
required, but the closer the two are on the network, the fewer reasons ICE has to
fail to agree. For Element Call it dials the LiveKit address the integration
gives, and needs the LiveKit server's media ports the way a browser does.

## Environment

| Variable | Required | What it is |
|---|---|---|
| `NEXTCLOUD_URL` | for Talk | the Nextcloud address; also where the app is, unless `APP_BASE_URL` says otherwise |
| `APP_BASE_URL` | without `NEXTCLOUD_URL` | where the integration's service API is: the Nextcloud app if not on `NEXTCLOUD_URL`, or voxonta-matrix-app |
| `APP_SERVICE_TOKEN` | yes | shared secret with the integration |
| `GATEWAY_TARGET` | yes | `host:port` of the processing gateway |
| `GATEWAY_TOKEN` | yes | access key for the gateway |
| `GATEWAY_TLS` | no | `false` for local debugging only |
| `POLL_INTERVAL` | no | how often to ask the app about calls, seconds (5) |
| `NC_APP_ID` | no | the app's id, `voxonta` by default — only if you renamed it |
| `INCLUDED_ROOMS` / `EXCLUDED_ROOMS` | no | narrow the scope locally without changing it for everyone |
| `DIAGNOSTIC_LOGGING` | no | verbose capture logs |

## As a library

```bash
pip install nextcloud-talk-capture
```

```python
from talk_capture import AppClient, AppCallMonitor, BrainSink, make_driver
```

The generated contract stubs ship inside the package (`talk_capture/_pb`), so
installing needs no `protoc`. After editing `proto/meeting/v1/meeting.proto`
regenerate them with `scripts/regen_stubs.sh`.

## Development

```bash
pip install -e '.[dev,livekit]' pytest
python -m pytest tests -v
```

`tests/test_livekit_e2e.py` runs a call against a real LiveKit server when
`LIVEKIT_E2E_URL` is set; its docstring has the one `docker run` it needs.

## Licence

MIT.
