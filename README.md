# HarborLookout

HarborLookout is a standalone Rust surveillance project designed to grow into a
camera ingest, recording, eventing, and playback system that can later merge
cleanly into HarborOS.

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

## Planning Docs

- `docs/roadmap.md`: product roadmap and execution phases
- `docs/plans/phase-1-recording-mvp.md`: crate-by-crate implementation checklist for the recording MVP
- `docs/work_log.md`: dated decisions and implementation progress

## Immediate Next Steps

1. Persist segment indexes and surveillance events, not just camera registration.
2. Expand worker lifecycle beyond start-only into stop/restart/status APIs.
3. Add worker-to-server callbacks or queue-based coordination.
4. Expand the server from camera/probe endpoints into retention and playback APIs.

## Future HarborOS Merge Intent

- Merge control-plane behavior into HarborOS middleware.
- Port UI behavior into HarborOS webui.
- Keep the Rust worker as an isolated, managed media component.