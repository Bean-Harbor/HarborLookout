# External recording control v1 (private, disabled by default)

HarborLookout worker accepts TF recording control only through a local Unix
socket. The existing loopback HTTP recording start, restart, and stop handlers
reject external leases or active external sessions. Internal recording keeps its
existing HTTP behavior.

The socket is created only when `HARBORLOOKOUT_EXTERNAL_CONTROL_UID` is set to
the provisioned HarborLink service UID. Its default path is
`/run/harborlookout/external-control.sock`, overridable with
`HARBORLOOKOUT_EXTERNAL_CONTROL_SOCKET`. The runtime directory must be owned by
the service; the socket is mode `0660`, and every connection's Unix peer UID is
checked. Provisioning must ensure HarborLink can connect without allowing a
browser or another service to impersonate it. No WebUI session or CSRF token
is accepted on this boundary.

Each connection carries one request and one response, both UTF-8 JSON with a
four-byte, network-order length prefix. Empty frames and frames over 64 KiB
are rejected. The envelope is:

```json
{"schema":"harborlookout.external-recording-control.v1","request_id":"opaque-id","operation":"start","payload":{}}
```

`start` payload: `camera` (the existing camera contract),
`recording_lease` (the existing opaque TF lease contract), and `session_ref`
(an opaque HarborLink session ID). `stop` and `status` payloads contain
`camera_id` and the same `session_ref`. Responses echo `schema` and
`request_id`, with `ok`, plus either `data` or `{code,message}` under `error`.
The start response omits FFmpeg arguments and source credentials. A repeated
start for the same live `session_ref` returns its existing result; another
session or camera conflicts. Only one TF recording may be active at a time.

HarborLink must first obtain the camera-bound claim from HarborOS. This socket
alone does not prove Owner approval or authorize a TF target. The current
HarborOS writer lease still needs its planned camera/session binding extension
before this path can be enabled in production. The release switch remains off
until that contract, deployment identity, seal/index recovery, and hardware
tests pass.
