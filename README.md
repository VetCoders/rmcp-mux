# rmcp_mux – shared MCP server daemon

Small Rust daemon that lets many MCP clients reuse a single STDIO server process (e.g. `npx @modelcontextprotocol/server-memory`) over a Unix socket. It rewrites JSON-RPC IDs per client, caches `initialize`, restarts the child on failure, and cleans up the socket on exit.

## Features
- One child process per service (spawned from `--cmd ...`).
- Many clients via Unix socket; ID rewriting keeps responses matched to the right client.
- `initialize` is executed once; later clients get the cached response immediately.
- Concurrent requests allowed; active client slots limited by `--max-active-clients` (default 5).
- Notifications are broadcast to all connected clients.
- Restart-on-exit for the child; pending/waiting requests receive an error on reset.
- Ctrl+C stops the mux, kills the child, and removes the socket file.
- Optional JSON status snapshots (`--status-file`) for tray/automation (PID, restarts, queue depth).
- Optional tray indicator (`--tray`) shows live server status (running/restarting), client and pending counts, initialize cache state, and restart reason.

## Build
```
cargo build --release
```
Binaries live in `target/release/rmcp_mux`.

## Install (curl | sh)
```
curl -fsSL https://raw.githubusercontent.com/LibraxisAI/rmcp_mux/main/tools/install.sh | sh
```
- Places wrapper in `$HOME/.local/bin/rmcp_mux` and ensures PATH contains cargo bin + wrapper dir.
- Env overrides: `INSTALL_DIR`, `CARGO_HOME`, `MUX_REF` (branch/tag, default main), `MUX_NO_LOCK=1` to skip `--locked`.

### Built-in proxy (no socat required)
If your MCP host wants a STDIO command, use the bundled proxy:
```
rmcp_mux_proxy --socket /tmp/mcp-memory.sock
```
Point host config to `rmcp_mux_proxy` with the matching socket path.

## Run (example: memory server)
```
./target/release/rmcp_mux \
  --socket /tmp/mcp-memory.sock \
  --cmd npx -- @modelcontextprotocol/server-memory \
  --max-active-clients 5 \
  --log-level info

```

## Config-driven run (JSON/YAML/TOML)
- Default config path: `~/.codex/mcp.json` (override via `--config <path>`). Parser auto-detects by extension (`.json`, `.yaml`/`.yml`, `.toml`).
- JSON:
```
{
  "servers": {
    "general-memory": {
      "socket": "~/mcp-sockets/general-memory.sock",
      "cmd": "npx",
      "args": ["@modelcontextprotocol/server-memory"],
      "max_active_clients": 5,
      "max_request_bytes": 1048576,
      "request_timeout_ms": 30000,
      "restart_backoff_ms": 1000,
      "restart_backoff_max_ms": 30000,
      "max_restarts": 5,
      "status_file": "~/.mcp-servers/rmcp_mux/status.json",
      "lazy_start": false,
      "tray": true,
      "service_name": "general-memory"
    }
  }
}
```
- YAML:
```
servers:
  general-memory:
    socket: "~/mcp-sockets/general-memory.sock"
    cmd: "npx"
    args: ["@modelcontextprotocol/server-memory"]
    max_active_clients: 5
    max_request_bytes: 1048576
    request_timeout_ms: 30000
    restart_backoff_ms: 1000
    restart_backoff_max_ms: 30000
    max_restarts: 5
    status_file: "~/.mcp-servers/rmcp_mux/status.json"
    lazy_start: false
    tray: true
    service_name: "general-memory"
```
- TOML:
```
[servers.general-memory]
socket = "~/mcp-sockets/general-memory.sock"
cmd = "npx"
args = ["@modelcontextprotocol/server-memory"]
max_active_clients = 5
max_request_bytes = 1048576
request_timeout_ms = 30000
restart_backoff_ms = 1000
restart_backoff_max_ms = 30000
max_restarts = 5
status_file = "~/.mcp-servers/rmcp_mux/status.json"
lazy_start = false
tray = true
service_name = "general-memory"
```
- Run using config entry:
```
./target/release/rmcp_mux --config ~/.codex/mcp.json --service general-memory
```
- CLI flags still override config (e.g. `--socket`, `--cmd`, `--tray`).

### Resolution order & defaults
- `socket` / `cmd`: required (either CLI or config). `--service` is required when `--config` is provided.
- `args`: CLI `--` tail wins, otherwise config, otherwise empty.
- `max_active_clients`: CLI default 5 unless overridden by config entry.
- `lazy_start`: default `false`.
- `max_request_bytes`: default `1_048_576` (1 MiB).
- `request_timeout_ms`: default `30_000` (30 s).
- `restart_backoff_ms`: default `1_000` (1 s), capped by `restart_backoff_max_ms` (default `30_000`).
- `max_restarts`: default `5` (0 = unlimited).
- `tray`: default `false`.
- `service_name`: CLI `--service-name`, else config, else socket file stem, else `rmcp_mux`.
- `status_file`: optional; accepts `~` and absolute/relative paths.

### Interactive wizard (TUI)
- Launch a guided editor (ratatui) to build/update your mux config:
```
rmcp_mux wizard --config ~/.codex/mcp-mux.toml --service general-memory
```
- Controls: `↑/↓` move, `Enter` edit field, `Space` toggle tray, `s` save, `q` quit. Saves JSON/YAML/TOML based on the extension; creates a `.bak` before overwriting.
- `--dry-run` runs the wizard without writing files.

### Dependency notes
- `ratatui` + `crossterm` power the TUI wizard; both are pure-Rust and optional (build with `--no-default-features` to skip).
- `tempfile` is dev-only for isolated FS fixtures in tests.

### Scan and rewire host configs
- Detect MCP hosts (Codex, Cursor/VSCode, Claude, JetBrains paths) and build a mux manifest + host snippets that point to the bundled proxy:
```
rmcp_mux scan --manifest ~/.codex/mcp-mux.toml --snippet ~/.codex/mcp-mux
```
- Rewire a host config in-place (creates `.bak`; add `--dry-run` to preview):
```
rmcp_mux rewire --host codex --socket-dir ~/.mcp-servers/rmcp_mux/sockets
```
- Snippets use the installed `rmcp_mux_proxy` binary: `command = "rmcp_mux_proxy"; args = ["--socket", "<service.sock>"]`.
- Check whether a host is already pointed at the mux proxy:
```
rmcp_mux status --host codex --proxy-cmd rmcp_mux_proxy
```

### Health check
- Verify that config resolves and the mux socket is reachable:
```
rmcp_mux health --socket /tmp/mcp-memory.sock --cmd npx -- @modelcontextprotocol/server-memory
```
- With a config file:
```
rmcp_mux health --config ~/.codex/mcp.json --service general-memory
```

## Tray status (optional)
- Run with `--tray` to spawn a small status icon. The drawer lists service name, server state, connected/active clients, pending requests, initialize cache state, and restart count/reason.
- Click “Quit mux” in the tray menu to stop the daemon (propagates shutdown to the child and cleans the socket).
- To feed your own UI/monitor, write status snapshots to JSON: `rmcp_mux --status-file ~/.mcp-servers/rmcp_mux/status.json ...`. The file is updated on every state change.
```

### Proxy config for MCP hosts
Use the bundled proxy instead of `socat`:
```
rmcp_mux_proxy --socket /tmp/mcp-memory.sock
```
Do this per service (memory, brave-search, etc.) with distinct sockets and mux instances.

### launchd (macOS) example
A template lives at `tools/launchd/rmcp_mux.sample.plist`. Copy to `~/Library/LaunchAgents/`, replace paths/user, then:
```
launchctl load -w ~/Library/LaunchAgents/rmcp_mux.general-memory.plist
```
Label should be unique per service; logs go to the paths defined in the plist.

## Runtime behavior
- New client → assigned `client_id`, messages get `global_id = c<client>:<seq>`.
- Responses are demuxed back to the original client/local ID.
- First `initialize` hits the server; the response is cached and fanned out to waiters. Later `initialize` calls are answered from cache.
- Guards: max request size (default 1 MiB), request timeout (default 30 s) with cleanup of pending calls, exponential restart backoff (1 s → 30 s) with a default limit of 5 restarts, and optional lazy start (defer child spawn until the first request).
- If the child exits or write/read fails, the mux restarts it, clears cache/pending, and sends error responses to affected clients.
- On shutdown (Ctrl+C), the mux stops the child and deletes the socket.

## Options
- `--socket <path>`: Unix socket path.
- `--cmd <prog>` `-- <args>`: command to run the MCP server.
- `--max-active-clients <n>`: limit of concurrently active clients (default 5).
- `--log-level <level>`: trace|debug|info|warn|error (default info).

## Tests and coverage
```
cargo test
cargo clippy --all-targets --all-features
cargo tarpaulin --all-targets --timeout 120
```
Current unit tests cover ID rewriting, initialize caching, and reset fan-out. Integration tests with a fake server can be added to raise coverage.

## Notes and TODOs
- Extend health to include initialize ping and optional metrics (per client / per request).
- Consider persistent initialize params after child restart (auto re-init).
- Add configurable child restart backoff and max retries.
- Expand host detection/rewire coverage and add automated host-side validation.
