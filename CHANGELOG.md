# Changelog

## 0.2.0 - 2025-11-24

### Added
- Optional tray icon (`--tray`) showing live server status, client and pending counts, and restart reasons. ([5eefde4](https://github.com/LibraxisAI/rmcp_mux/commit/5eefde4))
- Config file support (JSON/YAML/TOML) with auto-detection and CLI overrides. ([5eefde4](https://github.com/LibraxisAI/rmcp_mux/commit/5eefde4))
- `rmcp_mux_proxy` helper binary plus launchd template and installer tweaks for easier setup. ([04e5402](https://github.com/LibraxisAI/rmcp_mux/commit/04e5402))
- GitHub Actions CI workflow for formatting, linting, testing, and coverage, including an async proxy forwarding test. ([ad2b9aa](https://github.com/LibraxisAI/rmcp_mux/commit/ad2b9aa))
- Mux hooks, Semgrep rules, and expanded README documentation. ([e80083c](https://github.com/LibraxisAI/rmcp_mux/commit/e80083c))
- `health` subcommand to resolve config and assert socket reachability, plus unit tests for healthy/missing sockets.

### Changed
- Refactored mux state management and tray functionality into dedicated `state` and `tray` modules, with tray dependencies gated behind an optional `tray` feature; CI updated to run with `--no-default-features`. ([0d60764](https://github.com/LibraxisAI/rmcp_mux/commit/0d60764), [ad2b9aa](https://github.com/LibraxisAI/rmcp_mux/commit/ad2b9aa))

## 0.1.5
- Added JSON status snapshots (`--status-file` / `status_file`) including PID, queue depth, request limits, restart/backoff settings.
- Hardened runtime: lazy child start, request size guard, request timeouts, capped restart backoff, max restarts.
- Config/Wizard/Scan updated to surface new fields; defaults documented in README.
- Status writer task for tray/automation; MuxState now tracks queue depth and child PID.
- Tests cover initialize cache, resets, status snapshots, and proxy; CI runs fmt/clippy/tests/tarpaulin with `--no-default-features` (tray off in CI).
