# HarborLookout Roadmap

## Product Goal

HarborLookout is a local-first video intelligence system that turns ordinary
cameras into software-defined AI cameras, records their streams reliably, and
indexes saved footage so a user can ask natural-language questions such as:

- Who knocked over the cola today?
- When did someone leave a package at the door?
- Show me the clips where a person entered the garage this morning.

The system should remain merge-friendly for HarborOS by keeping control-plane,
worker, and UI boundaries explicit.

## Outcome Pillars

1. AI camera ingestion: run local vision analysis continuously on incoming
   camera streams without requiring cloud inference.
2. Reliable recording: preserve continuous recordings plus event clips with
   clear retention policies.
3. Searchable video memory: index saved footage with structured events and
   semantic descriptions so natural-language search is practical.

## Architecture Guardrails

- RTSP-first, ONVIF next, Matter later.
- Separate realtime analysis from heavyweight multimodal understanding.
- Keep recording usable even when AI enrichment is disabled or behind.
- Treat events, recordings, and semantic index entries as separate objects.
- Keep HarborOS integration as a later merge step, not a current runtime
  dependency.

## Execution Streams

### 1. Camera Ingest And Recording

- Probe cameras and validate transport, codec, and stream topology.
- Support distinct stream roles for detect, record, and live view.
- Persist continuous recording segments.
- Add retention, cleanup, and storage accounting.

### 2. Realtime AI Analysis

- Run lightweight local vision workers on sampled frames.
- Produce structured event candidates such as person, package, spill candidate,
  object removed, and motion in zone.
- Capture keyframes and short clips around events.
- Keep this pipeline independent from large-model summarization.

### 3. Semantic Enrichment And Search

- Add asynchronous enrichment jobs for captions, summaries, and embeddings.
- Store searchable metadata for events, keyframes, and clips.
- Expose natural-language search and evidence-backed answers.
- Return answer, timestamp, camera, and clip references for every query.

## Delivery Phases

## Phase 0: Current Foundation

Status: in progress

- Rust workspace skeleton exists.
- FFmpeg backend exists.
- Basic server and worker binaries exist.
- Camera registration and probe path exist.
- SQLite is present for camera registry.

Exit criteria:

- Server and worker can be started locally with documented flow.
- Camera registration, listing, and probe are stable.
- Local development storage path is consistent.

## Phase 1: Recording MVP

Goal: make HarborLookout a reliable local recorder before adding heavy AI.

Execution plan:

- See `docs/plans/phase-1-recording-mvp.md` for the crate-by-crate checklist.

Deliverables:

- Multi-role camera inputs in domain and contracts.
- Recording segment persistence.
- Playback-oriented recording APIs.
- Worker lifecycle APIs: start, stop, status, restart.
- Retention and cleanup jobs.

Exit criteria:

- A camera can be added and recorded continuously.
- Segments are queryable by camera and time range.
- Worker failures are visible through health and status endpoints.

## Phase 2: Realtime Event Pipeline

Goal: turn ordinary cameras into local AI cameras.

Deliverables:

- Lightweight realtime analysis worker.
- Event and artifact tables.
- Event clip and keyframe extraction.
- Zone- and schedule-aware triggering.
- Initial event taxonomy for person, vehicle, pet, package, drink container,
  object removed, and spill candidate.

Exit criteria:

- Realtime analysis can create searchable event records from live streams.
- Every event links back to clips or keyframes.
- Recording continues even if analysis falls behind.

## Phase 3: Semantic Index MVP

Goal: let users search saved footage with natural language over indexed events.

Deliverables:

- Enrichment job queue.
- Caption and summary generation for keyframes and event clips.
- Embedding storage and retrieval.
- Search endpoints for text query, time window, camera filter, and event type.

Exit criteria:

- A query like "show me the person at the front door this morning" returns
  ranked event candidates.
- Search results always include timestamps and supporting artifacts.

## Phase 4: Investigative Queries

Goal: answer user questions such as "who knocked over the cola today?" with
evidence, not only keyword matches.

Deliverables:

- Query planner that converts questions into time, object, and action filters.
- Candidate retrieval over structured events and semantic embeddings.
- Multimodal reranking over shortlisted clips.
- Answer synthesis with confidence and evidence links.

Exit criteria:

- The system can produce an answer plus top supporting clips for targeted
  household and office investigation questions.
- Query latency is acceptable on local hardware for bounded searches.

## Phase 5: HarborOS Merge Path

Goal: merge HarborLookout into HarborOS without collapsing boundaries.

Deliverables:

- Control-plane mapping into HarborOS middleware.
- UI migration plan into HarborOS webui.
- Worker packaging and lifecycle management for HarborOS deployment.
- Clear API and storage compatibility notes.

Exit criteria:

- HarborOS can adopt control-plane and UI layers without rewriting the Rust
  worker internals.

## Near-Term Backlog

1. Replace the heuristic detector adapter with a model-backed local detector.
2. Design enrichment job contracts before choosing the first local VLM.
3. Define the first searchable event schema and answer format.
4. Harden artifact retention with event-link safety rules and tiered policies.
5. Add event-to-recording and event-to-clip linking conventions.
6. Decide whether event ingestion stays server-local or gets a dedicated analysis service boundary.

## Tracking Notes

- Update this file when a phase boundary, scope, or acceptance criterion changes.
- Update `docs/plans/phase-1-recording-mvp.md` as implementation status changes inside Phase 1.
- Use `docs/work_log.md` for dated progress entries and implementation notes.