# HarborLookout

HarborLookout is a standalone Rust surveillance project designed to grow into a
camera ingest, recording, eventing, and playback system that can later merge
cleanly into HarborOS.

## Local Development Flow

HarborLookout currently runs as three local binaries:

- detector adapter as an optional local analysis helper process
- worker on `http://127.0.0.1:5050`
- server on `http://127.0.0.1:4040`
- CLI as a thin operator client against the server

The server persists local state in `data/harborlookout.db`. Recording output
directories are chosen per camera request and should usually live under
`data/segments/<camera-id>` during development.

### Start The Worker

```powershell
cargo run -p harborlookout-worker
```

### Build The Detector Adapter

The detector adapter is a separate local process that the worker launches for
each sampled frame during analysis. The first implementation is heuristic: it
reports a detection when the captured frame file exists and meets the configured
minimum byte threshold. It can also proxy to a real local model runner and
normalize that runner's JSON back into the worker-facing detection contract.

```powershell
cargo build -p harborlookout-detector-adapter
```

To inspect the adapter directly:

```powershell
target/debug/harborlookout-detector-adapter.exe --frame sample.jpg --backend heuristic --event-kind PackageDetected --event-severity Warning --min-bytes 1
```

To proxy a real local model runner through the adapter:

```powershell
cargo build -p harborlookout-model-runner-yolo
$env:HARBORLOOKOUT_YOLO_MODEL = "models/yolov8n.pt"
target/debug/harborlookout-detector-adapter.exe --frame sample.jpg --backend runner --runner-program target/debug/harborlookout-model-runner-yolo.exe --runner-arg --image --runner-arg {frame_path} --runner-arg --model --runner-arg models/yolov8n.pt --runner-arg --fallback-kind --runner-arg {event_kind}
```

### Start The Server

The server talks to the worker through `HARBORLOOKOUT_WORKER_URL` and defaults
to the local worker port.

```powershell
$env:HARBORLOOKOUT_WORKER_URL = "http://127.0.0.1:5050"
cargo run -p harborlookout-server
```

### Example Camera Payload

```json
{
	"id": "cam-front-door",
	"name": "Front Door",
	"streams": [
		{
			"role": "Record",
			"source": {
				"url": "rtsp://camera.local/front-door/main",
				"transport": "Tcp"
			},
			"stream_profile": {
				"width": 1920,
				"height": 1080,
				"fps": 15,
				"segment_seconds": 10
			}
		}
	],
	"source": {
		"url": "rtsp://camera.local/front-door/main",
		"transport": "Tcp"
	},
	"stream_profile": {
		"width": 1920,
		"height": 1080,
		"fps": 15,
		"segment_seconds": 10
	},
	"enabled": true
}
```

Save that JSON as `camera-front-door.json`, then drive the server through the
CLI.

By default the CLI prints concise text for operators. Add `--json` when you
want script-friendly output.

### CLI Smoke Path

```powershell
cargo run -p harborlookout-cli -- health
cargo run -p harborlookout-cli -- --json health
cargo run -p harborlookout-cli -- cameras register --file camera-front-door.json
cargo run -p harborlookout-cli -- cameras register --file camera-front-door.json --start-recording --output-dir data/segments/cam-front-door
cargo run -p harborlookout-cli -- recordings status --camera-id cam-front-door
cargo run -p harborlookout-cli -- recordings sync --camera-id cam-front-door
cargo run -p harborlookout-cli -- recordings list-segments --camera-id cam-front-door
cargo run -p harborlookout-cli -- analysis start --file camera-front-door.json --interval-ms 1000 --kind PackageDetected --severity Warning --detector-program target/debug/harborlookout-detector-adapter.exe --detector-arg --frame --detector-arg {frame_path} --detector-arg --event-kind --detector-arg {event_kind} --detector-arg --event-severity --detector-arg {event_severity} --detector-arg --backend --detector-arg runner --detector-arg --runner-program --detector-arg target/debug/harborlookout-model-runner-yolo.exe --detector-arg --runner-arg --detector-arg --image --detector-arg --runner-arg --detector-arg {frame_path} --detector-arg --runner-arg --detector-arg --model --detector-arg --runner-arg --detector-arg models/yolov8n.pt --detector-arg --runner-arg --detector-arg --fallback-kind --detector-arg --runner-arg --detector-arg {event_kind} --message "package detected by local detector" --artifacts-dir data/artifacts
cargo run -p harborlookout-cli -- analysis status --camera-id cam-front-door
cargo run -p harborlookout-cli -- analysis sync --camera-id cam-front-door --max-events 4
cargo run -p harborlookout-cli -- events create --file event-front-door.json
cargo run -p harborlookout-cli -- events list --camera-id cam-front-door
cargo run -p harborlookout-cli -- events artifacts --event-id event-front-door-001
cargo run -p harborlookout-cli -- storage summary
cargo run -p harborlookout-cli -- storage retention-preview --retain-after-ms 1743897600000
cargo run -p harborlookout-cli -- storage retention-cleanup --retain-after-ms 1743897600000
cargo run -p harborlookout-cli -- storage artifacts-retention-preview --retain-after-ms 1743897600000 --kind-retain Keyframe=1743897600000 --kind-retain Clip=1743811200000 --protect-events-after-ms 1743984000000
cargo run -p harborlookout-cli -- storage artifacts-retention-cleanup --retain-after-ms 1743897600000 --kind-retain Keyframe=1743897600000 --kind-retain Clip=1743811200000 --protect-events-after-ms 1743984000000
cargo run -p harborlookout-cli -- analysis stop --camera-id cam-front-door
cargo run -p harborlookout-cli -- recordings stop --camera-id cam-front-door
```

For `events create`, the JSON file should match the server contract shape:

```json
{
	"event": {
		"id": "event-front-door-001",
		"camera_id": "cam-front-door",
		"kind": "PackageDetected",
		"severity": "Warning",
		"occurred_at_unix_ms": 1743897600000,
		"message": "package detected near front door"
	},
	"artifacts": [
		{
			"id": "artifact-front-door-001",
			"event_id": "event-front-door-001",
			"camera_id": "cam-front-door",
			"kind": "Keyframe",
			"path": "artifacts/event-front-door-001/frame.jpg",
			"mime_type": "image/jpeg",
			"created_at_unix_ms": 1743897600500,
			"started_at_unix_ms": 1743897600000,
			"ended_at_unix_ms": 1743897600500,
			"size_bytes": 4096
		}
	]
}
```

### Runtime Notes

- `cameras register` only stores camera configuration unless `--start-recording`
	is provided.
- `recordings status` returns worker exit detail when FFmpeg exits with a
	failure code.
- `analysis start` launches a sampled-frame analysis session in the worker that
	uses FFmpeg to capture frames from the detect stream and passes each frame to a
	local detector adapter process.

- The bundled `harborlookout-detector-adapter` binary is a minimal runnable
- detector adapter. It can run in a heuristic mode for local bring-up, or in a
	runner mode that shells out to a real local model process and normalizes that
	process's JSON response.

- `harborlookout-model-runner-yolo` is the first concrete model wrapper. It
	executes Ultralytics YOLO through the local Python interpreter and maps common
	classes like `person`, `truck`, and `box` into HarborLookout event kinds.

- A detector process must print a JSON payload like
	`{"detected":true,"message":"package detected","event_kind":"PackageDetected","event_severity":"Warning"}`
	on stdout.
- `analysis sync` pulls the pending worker-side detection results into the
	server and persists events plus artifacts through the existing SQLite event
	tables.
- `events create`, `events list`, and `events artifacts` expose the current
	Phase 2 persistence surface for structured events and their saved artifacts.
- `storage retention-preview --retain-after-ms <unix-ms>` shows what cleanup
	would delete before you run it.
- `storage retention-cleanup --retain-after-ms <unix-ms>` removes non-running
	segments older than the cutoff and deletes their SQLite index rows.
- `storage artifacts-retention-preview --retain-after-ms <unix-ms>` shows which
	event artifacts older than the cutoff are reclaimable.
- `storage artifacts-retention-cleanup --retain-after-ms <unix-ms>` deletes
	artifact files and removes their SQLite rows when older than the cutoff.
- `storage artifacts-retention-*` supports `--kind-retain <Kind>=<unix-ms>` for
	per-artifact-type retention tiers and `--protect-events-after-ms <unix-ms>`
	to keep artifacts linked to recent events.
- The CLI defaults to concise text output; use `--json` for automation and
	for tests that need machine-readable responses.

## Current Direction

- RTSP-first MVP
- Independent Rust worker and control plane
- Clean contracts for future HarborOS integration
- UI kept separate from the Rust workspace
- First media backend: FFmpeg
- SQLite camera registry for local development

## Repository Shape

- `crates/domain`: core surveillance entities and invariants
- `crates/contracts`: API and inter-process payloads
- `crates/media-core`: media pipeline abstractions
- `crates/control-plane`: orchestration-facing services
- `apps/worker`: media worker binary
- `apps/server`: control-plane server binary
- `apps/cli`: diagnostic and admin CLI
- `apps/detector-adapter`: minimal local detector adapter binary for sampled frames
- `apps/model-runner-example`: concrete local model runner example for detector adapter `runner` mode
- `apps/model-runner-yolo`: Ultralytics YOLO wrapper binary for real local model execution

## Planning Docs

- `docs/roadmap.md`: product roadmap and execution phases
- `docs/plans/phase-1-recording-mvp.md`: crate-by-crate implementation checklist for the recording MVP
- `docs/work_log.md`: dated decisions and implementation progress

## Immediate Next Steps

1. Replace the heuristic detector adapter with a model-backed local detector.
2. Add zone- and schedule-aware triggering on top of the analysis session model.
3. Design enrichment job contracts for later semantic indexing phases.
4. Define the first event and artifact retention policy model.

## Future HarborOS Merge Intent

- Merge control-plane behavior into HarborOS middleware.
- Port UI behavior into HarborOS webui.
- Keep the Rust worker as an isolated, managed media component.