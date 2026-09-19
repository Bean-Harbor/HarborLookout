# HarborLookout Merge Contract

HarborLookout is intentionally structured so that HarborOS can absorb it in
pieces instead of inheriting a monolith.

## Intended Merge Targets

- Control plane -> HarborOS middleware
- UI -> HarborOS webui
- Worker -> managed standalone binary or service

## Stability Rules

- Keep domain models framework-agnostic.
- Keep contracts explicit and versionable.
- Keep media backend dependencies out of the control plane.
- Avoid HarborOS-specific assumptions inside the worker.

## Removable recording lease boundary

External recording targets are represented by `harborlookout_domain::ExternalRecordingLease`.
The lease is limited to the hardware-confirmed `TF-1` slot and contains an
opaque source token plus an expiry. It never contains a mount point, `/dev`
node, filesystem path or URL. HarborOS remains responsible for media
enumeration, role assignment, scoped mounting, flushing and byte I/O. The
current recording worker still accepts the established managed output
 directory contract; the external recording worker uses the HarborOS-owned
 Unix writer adapter before it starts ffmpeg. The JSON boundary also accepts
 the HarborOS resolver's `source_ref` name for the opaque token.

The versioned writer seam is `harboros.recording-writer.v1`: segment start,
bounded base64 chunk write, and terminal complete requests carry only
`lease_ref`, `segment_id`, sequence/byte metadata, and digest state. Requests
deny unknown fields such as `output_directory`, `path`, mount points, and
 device nodes. The worker sends these requests over the service-authenticated
 Unix socket `/run/harboros/recording-writer.sock`; these request types do not
 authorize direct ffmpeg writes.
