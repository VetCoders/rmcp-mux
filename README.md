# rmcp-mux – MCP Server Multiplexer

[![CI](https://github.com/Loctree/rmcp-mux/actions/workflows/ci.yml/badge.svg)](https://github.com/Loctree/rmcp-mux/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/rmcp-mux.svg)](https://crates.io/crates/rmcp-mux)
[![Version](https://img.shields.io/badge/version-0.3.4-blue.svg)](Cargo.toml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A Rust daemon that manages **all your MCP servers from a single process**. Define servers in `mux.toml`, run one `rmcp-mux` command, and get Unix sockets for each service. Features ID rewriting, initialize caching, auto-restart, and an interactive TUI dashboard.

**NEW in 0.3.x**: Unified process model with daemon status socket! One `rmcp-mux` process manages all servers from config file. Interactive TUI dashboard for monitoring and control. Query live status via `rmcp-mux daemon-status`.

## Table of Contents
- [Features](#features)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Configuration](#configuration)
- [CLI Reference](#cli-reference)
- [Interactive TUI Dashboard](#interactive-tui-dashboard)
- [Library Usage](#library-usage)
- [Runtime Behavior](#runtime-behavior)
- [Project Structure](#project-structure)
- [Testing](#testing)
- [Contributing](#contributing)

## Features

### Core
- **Single process, multiple servers** – one `rmcp-mux` manages all MCP servers defined in config
- **Unix socket per service** – each server gets its own socket for client connections
- **ID rewriting** – responses matched to correct client across all services
- **Initialize caching** – executed once per server; cached response served to subsequent clients
- **Auto-restart** – servers restart on failure with exponential backoff
- **Graceful shutdown** – Ctrl+C stops all servers, removes sockets

### Monitoring & Control
- **Interactive TUI** (`--tui`) – real-time dashboard with server status, restart controls
- **Status command** (`--show-status`) – JSON snapshot of all server states
- **Per-server control** – restart, stop, start individual servers at runtime
- **Selective startup** – `--only` and `--except` flags for partial launches

### Configuration
- **TOML config** – simple, readable server definitions
- **Environment variables** – per-server env injection
- **Flexible parameters** – timeouts, client limits, restart policies

## Quick Start

```bash
# 1. Create config file
cat > mux.toml << 'EOF'
[servers.memory]
socket = "/tmp/mcp-memory.sock"
cmd = "npx"
args = ["-y", "@modelcontextprotocol/server-memory"]

[servers.filesystem]
socket = "/tmp/mcp-fs.sock"
cmd = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"]
EOF

# 2. Start all servers
rmcp-mux --config mux.toml

# 3. Or start with TUI dashboard
rmcp-mux --config mux.toml --tui

# 4. Connect via proxy (for MCP hosts expecting STDIO)
rmcp-mux-proxy --socket /tmp/mcp-memory.sock
```

## Installation

### From source
```bash
cargo build --release
# Binaries: target/release/rmcp-mux, target/release/rmcp-mux-proxy
```

### One-liner (curl | sh)
```bash
curl -fsSL https://raw.githubusercontent.com/Loctree/rmcp-mux/main/tools/install.sh | sh
```

**Environment overrides:**
- `INSTALL_DIR` – wrapper location (default: `$HOME/.local/bin`)
- `CARGO_HOME` – cargo home (default: `~/.cargo`)
- `MUX_REF` – branch/tag/commit (default: `main`)
- `MUX_NO_LOCK=1` – skip `--locked` flag

### Built-in proxy
If your MCP host needs a STDIO command, use the bundled proxy:
```bash
rmcp-mux-proxy --socket /tmp/mcp-memory.sock
```

## Configuration

### Config file format (TOML)

```toml
[servers.memory]
socket = "~/mcp-sockets/memory.sock"
cmd = "npx"
args = ["-y", "@modelcontextprotocol/server-memory"]
max_active_clients = 5
tray = true

[servers.brave-search]
socket = "~/mcp-sockets/brave.sock"
cmd = "npx"
args = ["-y", "@anthropic/mcp-server-brave-search"]
env = { BRAVE_API_KEY = "your-api-key" }
request_timeout_ms = 60000

[servers.filesystem]
socket = "~/mcp-sockets/fs.sock"
cmd = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "/home/user/docs"]
lazy_start = true

[servers.rmcp-memex]
socket = "~/.rmcp-servers/sockets/rmcp-memex.sock"
cmd = "/path/to/rmcp-memex"
args = ["serve", "--config", "config.toml", "--db-path", "~/.ai-memories/lancedb"]
env = { SLED_PATH = "~/.rmcp-servers/sled/memex" }
lazy_start = false
```

### Parameter reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `socket` | required | Unix socket path (supports `~` expansion) |
| `cmd` | required | MCP server command |
| `args` | `[]` | Arguments for command |
| `env` | `{}` | Environment variables for child process |
| `max_active_clients` | `5` | Concurrent client limit |
| `lazy_start` | `false` | Defer child spawn until first request |
| `max_request_bytes` | `1048576` | Max request size (1 MiB) |
| `request_timeout_ms` | `30000` | Request timeout (30s) |
| `restart_backoff_ms` | `1000` | Initial restart delay (1s) |
| `restart_backoff_max_ms` | `30000` | Max restart delay (30s) |
| `max_restarts` | `5` | Restart limit (0 = unlimited) |
| `tray` | `false` | Enable tray icon for this server |
| `status_file` | none | Path for JSON status snapshots |

### Client Configuration (Claude Desktop, etc.)

MCP hosts expecting STDIO communication connect through `rmcp-mux-proxy`:

```json
{
  "mcpServers": {
    "rmcp-memex": {
      "command": "rmcp-mux-proxy",
      "args": ["--socket", "~/.rmcp-servers/sockets/rmcp-memex.sock"]
    },
    "loctree": {
      "command": "rmcp-mux-proxy",
      "args": ["--socket", "~/.rmcp-servers/sockets/loctree.sock"]
    },
    "brave-search": {
      "command": "rmcp-mux-proxy",
      "args": ["--socket", "~/.rmcp-servers/sockets/brave-search.sock"]
    }
  }
}
```

Each proxy instance translates STDIO <-> Unix socket, allowing standard MCP hosts to communicate with rmcp-mux managed servers.

## CLI Reference

### Main command

```bash
rmcp-mux --config mux.toml [OPTIONS]
```

**Required:**
- `--config <PATH>` – Path to configuration file (TOML)

**Server selection:**
- `--only <NAMES>` – Start only specified servers (comma-separated)
- `--except <NAMES>` – Start all servers except specified (comma-separated)

**Runtime control:**
- `--show-status` – Show status of all servers and exit
- `--restart-service <NAME>` – Restart a specific server
- `--tui` – Launch interactive TUI dashboard

**Examples:**
```bash
# Start all servers from config
rmcp-mux --config mux.toml

# Start only memory and filesystem servers
rmcp-mux --config mux.toml --only memory,filesystem

# Start all except brave-search
rmcp-mux --config mux.toml --except brave-search

# Check status of running servers
rmcp-mux --config mux.toml --status

# Restart a specific server
rmcp-mux --config mux.toml --restart-service memory

# Interactive dashboard
rmcp-mux --config mux.toml --tui
```

### Subcommands

#### `daemon-status` – Query running daemon
```bash
# Get status of running daemon (requires daemon to be running)
rmcp-mux daemon-status

# Output as JSON
rmcp-mux daemon-status --json

# Use custom status socket
rmcp-mux daemon-status --socket /custom/path.sock
```

Returns: version, uptime, server count, per-server status (active clients, pending requests, restarts, heartbeat latency).

#### `health` – Verify connectivity
```bash
# Check specific socket
rmcp-mux health --socket /tmp/mcp-memory.sock

# Check service from config
rmcp-mux health --config mux.toml --service memory
```

#### `proxy` – STDIO proxy
```bash
rmcp-mux proxy --socket /tmp/mcp-memory.sock
```

#### `scan` – Discover and generate configs
```bash
rmcp-mux scan \
  --manifest ~/.codex/mcp-mux.toml \
  --snippet ~/.codex/mcp-mux \
  --socket-dir ~/.rmcp-servers/rmcp-mux/sockets
```

#### `rewire` – Update host configs
```bash
# Rewire a host config to use rmcp-mux proxy (creates .bak backup)
rmcp-mux rewire --host codex --socket-dir ~/.rmcp-servers/rmcp-mux/sockets

# Preview changes without writing
rmcp-mux rewire --host codex --dry-run
```

#### `wizard` – Interactive configuration
```bash
rmcp-mux wizard --config ~/.codex/mcp-mux.toml
```

## Interactive TUI Dashboard

Launch with `--tui` for a real-time dashboard:

```bash
rmcp-mux --config mux.toml --tui
```

### Display
- Server list with status indicators (running/stopped/error)
- Connected clients count per server
- Pending requests
- Restart count and last restart reason
- Real-time updates

### Keyboard controls

| Key | Action |
|-----|--------|
| `j` / `Down` | Move selection down |
| `k` / `Up` | Move selection up |
| `r` | Restart selected server |
| `s` | Stop selected server |
| `S` | Start selected server |
| `q` / `Esc` | Quit |

## Library Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
rmcp-mux = { version = "0.3", default-features = false }
```

### Basic Example - Single Mux Server

```rust
use rmcp_mux::{MuxConfig, run_mux_server};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = MuxConfig::new("/tmp/my-mcp.sock", "npx")
        .with_args(vec!["-y".into(), "@anthropic/mcp-server".into()])
        .with_max_clients(10)
        .with_service_name("my-mcp-server");

    run_mux_server(config).await
}
```

### Multiple Mux Instances (Single Process)

```rust
use rmcp_mux::{MuxConfig, spawn_mux_server, MuxHandle};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let services = vec![
        ("memory", "/tmp/mcp-memory.sock", "npx", vec!["@mcp/server-memory"]),
        ("filesystem", "/tmp/mcp-fs.sock", "npx", vec!["@mcp/server-filesystem"]),
    ];

    let mut handles: Vec<MuxHandle> = Vec::new();
    for (name, socket, cmd, args) in services {
        let config = MuxConfig::new(socket, cmd)
            .with_args(args.into_iter().map(String::from).collect())
            .with_service_name(name)
            .with_request_timeout(Duration::from_secs(60));

        handles.push(spawn_mux_server(config).await?);
    }

    for handle in handles {
        handle.wait().await?;
    }
    Ok(())
}
```

### Health Check

```rust
use rmcp_mux::check_health;

async fn verify_service() -> bool {
    check_health("/tmp/mcp-memory.sock").await.is_ok()
}
```

### Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `cli` | yes | CLI binary, wizard, scan commands |
| `tray` | yes | System tray icon support |

For library-only usage (minimal dependencies):

```toml
[dependencies]
rmcp-mux = { version = "0.3", default-features = false }
```

## Runtime Behavior

### Client handling
1. New client connects to server's socket -> assigned `client_id`
2. Messages get `global_id = c<client>:<seq>`
3. Responses demuxed back to original client with local ID
4. First `initialize` hits server; response cached
5. Later `initialize` calls answered from cache

### Safety guards
- **Max request size** – default 1 MiB
- **Request timeout** – default 30s with cleanup of pending calls
- **Exponential restart backoff** – 1s -> 30s with configurable limit
- **Lazy start** – defer child spawn until first request

### Error handling
- Child exit or I/O failure -> restart child, clear cache/pending, send errors to affected clients
- Graceful shutdown (Ctrl+C) -> stop all children, delete all sockets

## Project Structure

```
rmcp-mux/
├── src/
│   ├── lib.rs            # Library entry point, MuxConfig, public API
│   ├── config.rs         # Config types, loading, validation
│   ├── state.rs          # MuxState, StatusSnapshot, helpers
│   ├── scan.rs           # Host discovery and rewiring (cli feature)
│   ├── tray.rs           # Tray icon (tray feature)
│   ├── bin/
│   │   ├── rmcp_mux.rs       # CLI binary (cli feature)
│   │   └── rmcp_mux_proxy.rs # STDIO proxy (cli feature)
│   ├── runtime/          # Core mux daemon
│   │   ├── mod.rs        # run_mux, health_check
│   │   ├── types.rs      # ServerEvent, constants
│   │   ├── client.rs     # Client connection handling
│   │   ├── server.rs     # Child process management
│   │   ├── proxy.rs      # STDIO proxy logic
│   │   ├── status.rs     # Status file writing & daemon status socket
│   │   └── heartbeat.rs  # Backend health monitoring
│   └── wizard/           # Interactive TUI wizard (cli feature)
├── tools/
│   ├── install.sh        # One-liner installer
│   └── launchd/          # macOS launchd templates
└── public/
    └── rmcp_mux_icon.png # Tray icon
```

## Testing

```bash
# Run all tests
cargo test

# Run tests without tray feature (for CI/headless)
cargo test --no-default-features

# Linting
cargo clippy --all-targets --all-features
```

## launchd (macOS)

Template at `tools/launchd/rmcp-mux.sample.plist`:
```bash
cp tools/launchd/rmcp-mux.sample.plist ~/Library/LaunchAgents/rmcp-mux.plist
# Edit paths: set --config to your mux.toml
launchctl load -w ~/Library/LaunchAgents/rmcp-mux.plist
```

## Contributing

See [.ai-agents/AI_GUIDELINES.md](.ai-agents/AI_GUIDELINES.md) for development guidelines.

## License

MIT
