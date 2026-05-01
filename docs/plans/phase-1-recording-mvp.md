# Phase 1 Implementation Plan: Recording MVP

## Objective

Phase 1 turns HarborLookout from a probe-and-start prototype into a reliable
local recording system with explicit worker lifecycle management and time-based
recording retrieval.

This plan maps work directly onto the current workspace so implementation can
be tracked without reinterpreting the roadmap.

## Phase 1 Acceptance Criteria

- A camera can be registered with separate detect and record stream roles.
- The server can instruct the worker to start, stop, and query recording state.
- Continuous recording segments are persisted in SQLite.
- Segments are queryable by camera and time range.
- Worker failures and stale recordings are visible through status APIs.

## Recommended Execution Order

1. Domain and contracts
2. Storage schema and repositories
3. Worker lifecycle implementation
4. Server orchestration
5. Playback and retention APIs
6. CLI smoke flows and acceptance validation

## Checklist By Workspace Component

### crates/domain

Status: in progress

- [x] Replace the single-source camera model with explicit stream inputs.
- [x] Introduce stream roles for detect, record, and live view.
- [x] Keep validation rules for RTSP-first inputs and segment duration.
- [x] Add a recording segment entity with camera id, path, start, end, duration,
      and status.
- [x] Add worker lifecycle entities for recording session state and worker
      health/status snapshots.
- [x] Define storage-facing enums for recording state such as pending, running,
      completed, failed, and interrupted.

Definition of done:

- Domain types describe Phase 1 data without HTTP or SQLite concerns.

### crates/contracts

Status: in progress

- [x] Update camera registration and probe requests to support multi-stream
      inputs.
- [x] Add contracts for worker lifecycle operations:
      start, stop, restart, status.
- [x] Add contracts for recording segment queries by camera and time range.
- [x] Add contracts for retention and storage summary responses.
- [x] Keep request and response shapes stable enough for future HarborOS merge.

Definition of done:

- The server and worker can communicate only through explicit contract types.

### crates/storage

Status: in progress

- [x] Keep the existing camera registry and extend it instead of replacing it.
- [x] Add normalized tables for recording segments.
- [x] Add tables for active recording sessions or worker runtime state.
- [x] Add repository traits for:
      camera persistence,
      segment persistence,
      recording session persistence,
      storage queries.
- [x] Add range queries for segments by camera and time window.
- [x] Add migration-safe initialization logic for all new tables.
- [x] Add tests covering insert, update, completion, failure, and range lookup.

Definition of done:

- SQLite can persist enough state for recording recovery and playback listing.

### crates/media-core

Status: pending

- [ ] Keep recording-plan generation separate from process lifecycle.
- [ ] Introduce worker-facing abstractions for startable and stoppable recording
      jobs if needed.
- [ ] Define the minimal metadata the backend must return so the worker can
      persist and report runtime state.

Definition of done:

- Media-core expresses backend capabilities without embedding FFmpeg-specific
  server logic.

### crates/media-ffmpeg

Status: in progress

- [x] Extend the FFmpeg backend to build plans from explicit record-role inputs.
- [x] Standardize output naming so persisted segments can be matched to camera
      and time range.
- [x] Define how the backend reports output directory, file naming pattern, and
      expected segment cadence.
- [x] Add tests for multi-stream input selection and segment-output plan shape.

Definition of done:

- FFmpeg plan generation is deterministic enough for storage indexing.

### crates/control-plane

Status: in progress

- [x] Extend the control plane from camera CRUD into orchestration logic.
- [x] Add worker client integration points instead of local-only probe actions.
- [x] Register cameras without auto-start side effects unless explicitly
      requested.
- [x] Add operations for start recording, stop recording, restart recording, and
      fetch recording status.
- [x] Add segment query services that compose storage lookups into API-ready
      responses.
- [x] Add health reporting that includes worker reachability and active session
      counts.

Additional progress:

- Segment indexing is currently scan-based from the FFmpeg output pattern and
  runs on status, stop, and manual sync.

Definition of done:

- The control plane owns orchestration, not process execution details.

### apps/worker

Status: in progress

- [x] Replace fire-and-forget start behavior with a managed in-process session
      registry.
- [x] Track spawned FFmpeg processes by camera or session id.
- [x] Add stop and status endpoints.
- [x] Report failed spawns and exited processes clearly.
- [x] Ensure worker startup creates required recording directories.
- [x] Decide whether the worker or server is authoritative for session state,
      then implement consistently.

Current decision:

- Worker is authoritative for live process state.
- Server persists the control-plane view of active sessions in SQLite.

Definition of done:

- The worker can be asked what is recording now and can stop or recover cleanly.

### apps/server

Status: in progress

- [x] Add a worker client for HTTP calls to the worker service.
- [x] Wire camera registration to optional recording start flows.
- [x] Add endpoints for start, stop, restart, and status.
- [x] Add endpoints to list recording segments by camera and time range.
- [x] Add endpoints for storage summary and retention preview if feasible in
      Phase 1.
- [x] Add retention cleanup execution for non-running segments.
- [x] Keep API errors explicit so setup and worker failures are debuggable.

Additional progress:

- Server routes are now exposed through a reusable app factory so integration
      tests can boot the real control-plane surface.

Definition of done:

- The server is the only control-plane surface a local UI or HarborOS would need
  for recording flows.

### apps/cli

Status: in progress

- [x] Add smoke commands for camera probe, camera register, recording start,
      recording stop, and segment list.
- [ ] Keep CLI focused on operator diagnostics, not full product workflows.

Definition of done:

- CLI can exercise the main Phase 1 server flows end-to-end in development.

## Cross-Cutting Tasks

### API And Runtime Behavior

- [ ] Define the server and worker port and base URL configuration model.
- [ ] Define local data directory layout for database, segments, and logs.
- [ ] Decide how camera-specific recording output paths are derived.
- [x] Decide whether segment indexing is optimistic, callback-driven, or scan-
      based.

### Testing

- [x] Add unit tests for new domain and storage logic.
- [x] Add integration tests for server-to-worker lifecycle flows.
- [x] Add at least one end-to-end local smoke scenario with a fixture stream.

Current smoke coverage:

- register camera
- list cameras
- probe camera recording plan
- start recording
- restart recording
- fetch running status
- manual segment sync
- query indexed segments
- stop recording
- inspect storage summary and retention preview
- execute retention cleanup

### Documentation

- [x] Update README once Phase 1 endpoints and flows are stable.
- [x] Record implementation milestones in `docs/work_log.md`.
- [x] Update this checklist as items move from pending to done.

## Suggested First Slice

To keep momentum high, implement Phase 1 in this order:

1. Expand domain and contracts for multi-stream camera inputs and recording
   lifecycle requests.
2. Add SQLite tables and repositories for recording segments and active
   sessions.
3. Teach the worker to manage running FFmpeg processes with status and stop
   support.
4. Teach the server to call the worker and persist session and segment state.
5. Add segment listing APIs and a CLI smoke path.

## Out Of Scope For Phase 1

- Realtime object detection or event generation
- Multimodal captioning or semantic embeddings
- Natural-language query answering
- HarborOS-specific runtime integration