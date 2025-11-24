# rmcp_mux – AI-facing overview

## Purpose
- Share a single MCP server process (e.g., `npx @modelcontextprotocol/server-memory`) across many MCP hosts via a Unix socket.
- Rewrite JSON-RPC IDs per client, cache `initialize`, enforce request limits/timeouts, restart child with backoff, and expose status for UI/automation.

## Quick start
```bash
cargo build --release
./target/release/rmcp_mux \
  --socket ~/.mcp-servers/rmcp_mux/sockets/memory.sock \
  --cmd npx -- @modelcontextprotocol/server-memory \
  --max-active-clients 5 \
  --status-file ~/.mcp-servers/rmcp_mux/status.json
# host side: point to bundled proxy
rmcp_mux_proxy --socket ~/.mcp-servers/rmcp_mux/sockets/memory.sock
```

## Config (JSON/YAML/TOML)
- Default path: `~/.codex/mcp.json` (override `--config`, pick `--service` key under `servers.<name>`).
- Fields (per service):
  - `socket`, `cmd`, `args`, `max_active_clients`
  - `lazy_start` (bool)
  - `max_request_bytes` (default 1_048_576)
  - `request_timeout_ms` (default 30_000)
  - `restart_backoff_ms` (default 1_000), `restart_backoff_max_ms` (default 30_000), `max_restarts` (default 5, 0 = unlimited)
  - `tray` (bool), `service_name`, `log_level`
  - `status_file` (JSON snapshots for UI/automation)

## Status snapshots
- Written atomically to `status_file` on every state change.
- Contains: service_name, server_status, restarts, connected/active clients, pending count, queue_depth, child_pid, max_request_bytes, restart backoff settings, last_reset, initialize cache flag.

## Tooling / commands
- `scan`: discover host configs (Codex/Cursor/Claude/JetBrains), build mux manifest/snippets.
- `rewire`: apply proxy command into host configs (creates `.bak`; `--dry-run` to preview).
- `status`: check host configs point to `rmcp_mux_proxy`.
- `wizard` (ratatui): guided editor for mux config and host rewiring (writes backups; `--dry-run` supported).

## Testing / CI
- Local: `cargo fmt`, `cargo clippy --all-targets --no-default-features -- -D warnings`, `cargo test --no-default-features`.
- Coverage: `cargo tarpaulin --all-targets --no-default-features --out Lcov`.
- CI workflow: `.github/workflows/ci.yml` (fmt/clippy/test/tarpaulin, tray feature off).

## Notes for agents
- Comments/docs in English only.
- Tray feature is feature-gated; CI builds with `--no-default-features` to avoid GUI deps.
- `.ai-agents/**` is scratch space (do not commit). `AGENTS.md` is deprecated/cringe; ignore.
- Prefer `rmcp_mux_proxy` over `socat` for host STDIO integration.

