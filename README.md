# mcp_mux – shared MCP server daemon

Small Rust daemon that lets many MCP clients reuse a single STDIO server process (e.g. `npx @modelcontextprotocol/server-memory`) over a Unix socket. It rewrites JSON-RPC IDs per client, caches `initialize`, restarts the child on failure, and cleans up the socket on exit.

## Features
- One child process per service (spawned from `--cmd ...`).
- Many clients via Unix socket; ID rewriting keeps responses matched to the right client.
- `initialize` is executed once; later clients get the cached response immediately.
- Concurrent requests allowed; active client slots limited by `--max-active-clients` (default 5).
- Notifications are broadcast to all connected clients.
- Restart-on-exit for the child; pending/waiting requests receive an error on reset.
- Ctrl+C stops the mux, kills the child, and removes the socket file.
- Optional tray indicator (`--tray`) shows live server status (running/restarting), client and pending counts, initialize cache state, and restart reason.

## Build
```
cargo build --release
```
Binaries live in `target/release/mcp_mux`.

## Install (curl | sh)
```
curl -fsSL https://raw.githubusercontent.com/LibraxisAI/mcp_mux/main/tools/install.sh | sh
```
- Places wrapper in `$HOME/.local/bin/mcp_mux` and ensures PATH contains cargo bin + wrapper dir.
- Env overrides: `INSTALL_DIR`, `CARGO_HOME`, `MUX_REF` (branch/tag, default main), `MUX_NO_LOCK=1` to skip `--locked`.

### Built-in proxy (no socat required)
If your MCP host wants a STDIO command, use the bundled proxy:
```
mcp_mux_proxy --socket /tmp/mcp-memory.sock
```
Point host config to `mcp_mux_proxy` with the matching socket path.

## Run (example: memory server)
```
./target/release/mcp_mux \
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
tray = true
service_name = "general-memory"
```
- Run using config entry:
```
./target/release/mcp_mux --config ~/.codex/mcp.json --service general-memory
```
- CLI flags still override config (e.g. `--socket`, `--cmd`, `--tray`).

## Tray status (optional)
- Run with `--tray` to spawn a small status icon. The drawer lists service name, server state, connected/active clients, pending requests, initialize cache state, and restart count/reason.
- Click “Quit mux” in the tray menu to stop the daemon (propagates shutdown to the child and cleans the socket).
```

### Proxy config for MCP hosts
Use the bundled proxy instead of `socat`:
```
command = "mcp_mux_proxy"
args = ["--socket", "/tmp/mcp-memory.sock"]
```
Do this per service (memory, brave-search, etc.) with distinct sockets and mux instances.

### launchd (macOS) example
A template lives at `tools/launchd/mcp_mux.sample.plist`. Copy to `~/Library/LaunchAgents/`, replace paths/user, then:
```
launchctl load -w ~/Library/LaunchAgents/mcp_mux.general-memory.plist
```
Label should be unique per service; logs go to the paths defined in the plist.

## Runtime behavior
- New client → assigned `client_id`, messages get `global_id = c<client>:<seq>`.
- Responses are demuxed back to the original client/local ID.
- First `initialize` hits the server; the response is cached and fanned out to waiters. Later `initialize` calls are answered from cache.
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
- Add a tiny TCP/Unix proxy binary (instead of `socat`) for environments without `socat`.
- Add health pings and optional metrics (per client / per request).
- Consider persistent initialize params after child restart (auto re-init).
- Add configurable child restart backoff and max retries.
