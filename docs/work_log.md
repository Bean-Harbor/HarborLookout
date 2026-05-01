# HarborLookout Work Log

## 2026-04-05

### Direction Locked

- Confirmed HarborLookout is not only an NVR project; it is a local-first video
  intelligence system.
- Locked three product goals:
  - turn ordinary cameras into AI cameras through local realtime analysis
  - save continuous recordings and event clips reliably
  - index saved footage so users can ask natural-language investigative
    questions

### Architecture Decisions

- Keep RTSP as the first supported ingest path.
- Keep ONVIF as a follow-up integration path.
- Defer Matter until after recording and search fundamentals are working.
- Separate realtime analysis from heavyweight multimodal enrichment.
- Keep continuous recording independent from AI enrichment so recording remains
  reliable when local inference is slow.
- Treat recordings, events, artifacts, and semantic index entries as separate
  persisted objects.

### Planning Outcome

- Added `docs/roadmap.md` as the primary phase-based execution plan.
- Added `docs/plans/phase-1-recording-mvp.md` as the crate-by-crate execution checklist for the next delivery phase.
- Defined five roadmap phases:
  - current foundation
  - recording MVP
  - realtime event pipeline
  - semantic index MVP
  - investigative queries
  - HarborOS merge path

### Near-Term Execution Focus

1. Persist recording segments, events, and artifacts.
2. Wire server-to-worker execution and lifecycle management.
3. Expand camera models to support detect and record stream roles.
4. Add retention, cleanup, and status reporting.
5. Design enrichment job contracts for future local multimodal indexing.

### Phase 1 Breakdown

- Broke the recording MVP into workspace-specific tasks so implementation can be tracked by crate and app instead of only by phase name.
- Locked the first execution order inside Phase 1:
  1. domain and contracts for multi-stream cameras and worker lifecycle
  2. storage schema for segments and worker state
  3. worker process registry and status APIs
  4. server orchestration and playback-oriented APIs
  5. CLI smoke flows and phase acceptance checks

### Implementation Progress

- Started the first code slice for Phase 1.
- Expanded the domain model to support explicit stream roles alongside the legacy single-stream fields.
- Added contract types for stop, restart, and status-oriented recording lifecycle APIs.
- Updated FFmpeg plan selection to prefer the record-role stream when multiple streams are configured.
- Extended SQLite camera persistence to store configured streams as JSON for forward-compatible camera definitions.
- Revalidated the workspace with `cargo test`; all tests passed after the first slice.
- Added recording session and recording state domain models for Phase 1 persistence.
- Added SQLite tables and repository traits for recording segments and active recording sessions.
- Added storage tests covering camera persistence, segment range queries, and session lifecycle operations.
- Added an HTTP worker client and extended the control plane to orchestrate start, stop, restart, status, and segment query flows.
- Reworked the worker into a managed process registry instead of fire-and-forget FFmpeg spawns.
- Added server endpoints for recording lifecycle and segment queries, backed by worker calls plus SQLite session tracking.
- Kept the new worker client dependency-free by implementing it over `tokio::net::TcpStream` instead of adding a new HTTP client crate.
- Revalidated the workspace with `cargo test --offline`; all tests passed after wiring worker lifecycle and server orchestration.
- Connected FFmpeg output naming to SQLite segment persistence via scan-based segment discovery.
- Added automatic segment sync during recording status refresh and recording stop, plus a manual segment sync endpoint for debugging and smoke tests.
- Added a reusable server app factory and an end-to-end smoke integration test that boots a fake worker plus the real server and validates the recording flow.
- Validated the smoke flow with `cargo test --offline`, covering register, start, status, segment sync, segment query, and stop.
- Added storage summary and retention preview APIs backed by SQLite aggregate queries over indexed recording segments.
- Fixed a retention preview edge case where very large timestamp thresholds overflowed SQLite `i64` parameters and caused reclaimable segment counts to read as zero.
- Added a storage-layer regression test for the `u64::MAX` retention boundary and revalidated the full workspace with `cargo test --offline`.

## 2026-04-06

### CLI Smoke Commands

- Replaced the placeholder CLI bootstrap with a lightweight HTTP-based operator CLI for the local server surface.
- Added commands for health, camera list/register/probe, recording start/stop/restart/status, segment query/sync, and storage summary/retention preview.
- Kept the CLI dependency-light by reusing the workspace's existing serde and tokio stack instead of pulling in a full HTTP client.
- Added parser tests covering explicit server selection, camera registration input, segment query bounds, and unexpected extra arguments.

### Recording Lifecycle Follow-Up

- Extended camera registration so the server only starts recording when explicitly requested through the registration contract.
- Enriched worker recording status responses with exit codes and failure messages so FFmpeg exits are debuggable from the control-plane surface.
- Updated the README with a concrete local development path for worker, server, CLI, and example camera payloads.

### Retention Execution

- Added a retention cleanup API that deletes non-running segment files older than a cutoff and removes their SQLite rows.
- Kept cleanup conservative by skipping active `running` segments even when their timestamps fall before the retention cutoff.
- Added storage and end-to-end server tests covering retention preview plus destructive cleanup execution.

### Event Persistence Foundation

- Added explicit domain entities for event artifacts so Phase 2 can persist keyframes, clips, thumbnails, and metadata blobs separately from event rows.
- Added SQLite tables plus repository traits for surveillance events and event artifacts, including camera/time filtered event listing and event-scoped artifact lookup.
- Added storage tests covering event persistence, artifact persistence, and typed decode paths for event kind, severity, and artifact kind values.

### Event API Surface

- Wired the new event and artifact repositories into the control plane.
- Added minimal server APIs for event create, event list, and event-artifact list.
- Extended the CLI with `events create`, `events list`, and `events artifacts` so the Phase 2 persistence layer is reachable without direct HTTP crafting.

### Event Taxonomy Expansion

- Expanded `EventKind` beyond system lifecycle events to include the first Phase 2 analysis taxonomy: motion in zone, person, vehicle, pet, package, drink container, object removed, and spill candidate.
- Added a small classification helper so analysis events and system events can be separated without string matching at call sites.
- Revalidated the persistence layer against one of the new analysis event kinds instead of only the legacy motion event.

### Sampled-Frame Detector Entry

- Replaced the previous mock worker-side event generation path with a sampled-frame analysis session lifecycle that captures a JPEG from the detect stream through the media backend.
- Added a detector adapter contract so the worker can invoke a local process for each sampled frame and parse JSON detection results without hard-coding a model dependency into the Rust workspace.
- Kept the first implementation queue-free by following the existing recording sync pattern: the server pulls pending analysis events from the worker and persists them into the SQLite event and artifact tables.
- Extended the CLI analysis flow so operators can pass a detector program plus detector args when starting analysis sessions.
- Revalidated the pipeline with unit coverage for snapshot planning and detector result parsing plus the existing end-to-end server smoke surface.

### Runnable Detector Adapter

- Added `apps/detector-adapter` as a minimal runnable Rust binary that implements the detector stdout JSON protocol expected by the worker.
- The first adapter is intentionally heuristic instead of model-backed: it reports a detection when the sampled frame file exists and meets the configured byte threshold, which is enough to exercise the live worker path without external ML dependencies.
- Updated the README analysis command to point at the built adapter binary and pass frame path, event kind, and severity through detector args.
- Extended the adapter with a `runner` backend so it can now proxy a real local model process and normalize that process's JSON output back into HarborLookout's detector contract.

### Example Model Runner

- Added `apps/model-runner-example` as a concrete local runner example that the detector adapter can call in `runner` mode.
- The example runner accepts `--image` and `--label`, performs a minimal JPEG-like byte check, and returns the normalized JSON shape the adapter expects from real model runners.
- Updated README examples so the end-to-end local analysis path can be exercised with two repo-local binaries instead of abstract placeholders.

### YOLO Runner Wrapper

- Added `apps/model-runner-yolo` as the first concrete real-model wrapper, using the local Python interpreter plus `ultralytics` at runtime instead of requiring a Rust-side ML dependency in the workspace.
- Mapped common YOLO classes such as `person`, `truck`, `dog`, and `box` into the HarborLookout event taxonomy so detections can flow into existing event storage without string matching in the worker.
- Updated README examples so the preferred real-model path now goes through `harborlookout-model-runner-yolo` while keeping `model-runner-example` available as a fully repo-local offline fallback.

### Real Worker Analysis Smoke

- Added a Windows integration smoke test in `apps/worker/tests/live_analysis_chain.rs` that launches the real worker binary and drives the full process chain through the real detector adapter and the real YOLO wrapper binary.
- Kept the smoke deterministic and offline by injecting a fake FFmpeg shim plus a mock YOLO output file, while still exercising real subprocess boundaries, worker HTTP endpoints, sampled-frame capture flow, detector adapter runner mode, and YOLO class-to-event mapping.
- Added environment hooks for worker bind address and FFmpeg program override so real-process integration tests can run without conflicting with the default local ports or requiring a system FFmpeg install.

### Real Server Analysis Smoke

- Added a Windows integration smoke test in `apps/server/tests/live_server_analysis_chain.rs` that launches the real server binary against the real worker binary and drives the analysis flow through the server API surface instead of calling the worker directly.
- Added environment hooks for server bind address and data directory override so the real-process server smoke can run on ephemeral ports with isolated SQLite state.
- Validated that server-side analysis sync persists the worker-emitted event and artifact, and that the persisted event can be queried back through the server event APIs.

### CLI End-To-End Smoke

- Added a Windows integration smoke test in `apps/cli/tests/live_cli_analysis_chain.rs` that launches the real CLI binary against the real server and real worker processes.
- The smoke writes a temporary camera JSON file, starts analysis through the CLI, syncs analysis events through the CLI, and verifies the persisted event plus artifact through the CLI event query commands.
- This now gives HarborLookout one full executable path that starts at the operator-facing CLI and runs through every local boundary down to the YOLO wrapper process.

### CLI Output Modes

- Added a concise text output mode as the default CLI presentation for operators so common commands no longer require visually scanning pretty-printed JSON.
- Added an explicit `--json` switch for script-friendly output and kept the live CLI smoke on that machine-readable path.
- Added renderer coverage for key operational surfaces such as recording status and event listing, plus parser coverage for the new output flags.

### Artifact Retention Policy Surface

- Added first-pass artifact retention preview and cleanup contracts so event artifacts can be managed with the same operator flow as recording segment retention.
- Implemented SQLite-backed artifact retention preview and cleanup using artifact creation timestamps, including per-camera aggregates and conservative skipped-file accounting.
- Exposed artifact retention APIs through the server and control plane, and added CLI commands for preview and cleanup with both text and `--json` output paths.

### Artifact Retention Policy V2

- Added event-link safety support so retention preview and cleanup can protect artifacts attached to events newer than a configured cutoff.
- Added tiered artifact retention rules by kind (`Keyframe`, `Clip`, `Thumbnail`, `Metadata`) so operators can keep heavier artifacts for shorter windows without affecting keyframes.
- Extended the CLI surface with `--kind-retain <Kind>=<unix-ms>` and `--protect-events-after-ms <unix-ms>`, plus text-mode counters for protected artifacts.

### V2 Stabilization And Day-End Validation

- Fixed a compile break in the shared retention contract by deriving `PartialEq` and `Eq` for `ArtifactKindRetentionRule`, which is required by CLI command enum equality derives in tests.
- Stabilized `harborlookout-media-ffmpeg` unit tests by serializing env-var-mutating tests behind a shared test mutex, preventing parallel races on `HARBORLOOKOUT_FFMPEG_PROGRAM`.
- Revalidated the entire workspace with `cargo test --offline`; all unit tests, integration smokes, and doc-tests passed.