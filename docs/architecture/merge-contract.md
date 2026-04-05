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