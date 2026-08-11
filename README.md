# nextcloud-talk-capture

Captures Nextcloud Talk calls: the service joins a call, takes one audio stream
per speaker, and streams it to a processing gateway.

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

The service makes **outbound connections only**: behind NAT it needs no port
forwarding and no inbound webhooks.

### Where to put it

Capture is a WebRTC client — it connects to the signalling server the way an
ordinary participant does. Running it next to the signalling server is not
required, but the closer the two are on the network, the fewer reasons ICE has to
fail to agree.

## Environment

| Variable | Required | What it is |
|---|---|---|
| `NEXTCLOUD_URL` | yes | the Nextcloud address |
| `APP_SERVICE_TOKEN` | yes | shared secret with the app |
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
from talk_capture import AppClient, AppCallMonitor, BrainSink, SpreedClient
```

The generated contract stubs ship inside the package (`talk_capture/_pb`), so
installing needs no `protoc`. After editing `proto/meeting/v1/meeting.proto`
regenerate them with `scripts/regen_stubs.sh`.

## Development

```bash
pip install -e '.[dev]' pytest
python -m pytest tests -v
```

## Licence

MIT.
