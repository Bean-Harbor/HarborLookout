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