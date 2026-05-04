//! rmcp_mux CLI binary
//!
//! This is the command-line interface for rmcp_mux. For library usage,
//! see the `rmcp_mux` crate documentation.

use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::{Args, Parser, Subcommand};
use tracing_subscriber::filter::LevelFilter;

use rmcp_mux::config::{
    CliOptions, expand_path, load_config, resolve_params, resolve_params_multi,
};
use rmcp_mux::runtime::{health_check, run_mux, run_proxy};
use rmcp_mux::scan::{
    RewireArgs, ScanArgs, StatusArgs, run_rewire_cmd, run_scan_cmd, run_status_cmd,
};
use rmcp_mux::wizard::WizardArgs;
use rmcp_mux::{
    DEFAULT_STATUS_SOCKET, print_status_table, query_status, restart_single, run_mux_multi,
    status_all,
};

/// Robust MCP mux: single MCP server child, many clients via UNIX socket,
/// initialize cache, ID rewriting, child restarts, and active client limit.
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct RootCli {
    #[command(subcommand)]
    command: Option<CliCommand>,
    #[command(flatten)]
    run: Cli,
}

#[derive(Subcommand, Debug)]
enum CliCommand {
    /// Interactive wizard (ratatui) to build mux and host config snippets.
    Wizard(WizardArgs),
    /// Scan host configs and generate mux manifests/snippets.
    Scan(ScanArgs),
    /// Rewire a host config to point to rmcp_mux proxy.
    Rewire(RewireArgs),
    /// Proxy STDIO to a mux socket (for MCP hosts).
    Proxy(ProxyArgs),
    /// Check whether host configs are already pointed at the mux proxy.
    Status(StatusArgs),
    /// Simple health check: resolve config and try connecting to the mux socket.
    Health(Box<HealthArgs>),
    /// Query the running mux daemon for status of all managed servers.
    DaemonStatus(DaemonStatusArgs),
    /// Launch system tray dashboard showing all managed servers.
    #[cfg(feature = "tray")]
    Dashboard(DashboardArgs),
}

#[derive(Args, Debug, Clone)]
struct Cli {
    /// Unix socket path for the mux listener. Can be overridden by config.
    #[arg(long)]
    socket: Option<PathBuf>,
    /// MCP server command (e.g. `npx`). Can be overridden by config.
    #[arg(long)]
    cmd: Option<String>,
    /// Arguments passed to the MCP server command.
    #[arg(last = true)]
    args: Vec<String>,
    /// Max active clients (permits for concurrent server use).
    #[arg(long, default_value = "5")]
    max_active_clients: usize,
    /// Lazy start MCP child only when first request arrives.
    #[arg(long)]
    lazy_start: Option<bool>,
    /// Maximum request size in bytes before rejecting.
    #[arg(long)]
    max_request_bytes: Option<usize>,
    /// Request timeout in milliseconds before the mux aborts pending calls.
    #[arg(long)]
    request_timeout_ms: Option<u64>,
    /// Initial restart backoff in milliseconds.
    #[arg(long)]
    restart_backoff_ms: Option<u64>,
    /// Maximum restart backoff in milliseconds.
    #[arg(long)]
    restart_backoff_max_ms: Option<u64>,
    /// Maximum restarts before marking server failed (0 = unlimited).
    #[arg(long)]
    max_restarts: Option<u64>,
    /// Log level (trace|debug|info|warn|error).
    #[arg(long, default_value = "info")]
    log_level: String,
    /// Enable tray icon with live server status.
    #[arg(long, default_value_t = false)]
    tray: bool,
    /// Service name shown in tray (defaults to socket file stem).
    #[arg(long)]
    service_name: Option<String>,
    /// Optional config file (default ~/.codex/mcp.json)
    #[arg(long)]
    config: Option<PathBuf>,
    /// Service key inside config (`servers.<name>`). If not provided with --config,
    /// all servers from config will be started.
    #[arg(long)]
    service: Option<String>,
    /// Only start these services (comma-separated). Requires --config.
    #[arg(long, value_delimiter = ',')]
    only: Option<Vec<String>>,
    /// Exclude these services (comma-separated). Requires --config.
    #[arg(long, value_delimiter = ',')]
    except: Option<Vec<String>>,
    /// Show status of all configured servers and exit.
    #[arg(long)]
    show_status: bool,
    /// Restart a specific service by name.
    #[arg(long)]
    restart_service: Option<String>,
    /// Optional path to write JSON status snapshots.
    #[arg(long)]
    status_file: Option<PathBuf>,
    /// Heartbeat probe interval in milliseconds (default: 30000).
    #[arg(long)]
    heartbeat_interval_ms: Option<u64>,
    /// Heartbeat response timeout in milliseconds (default: 30000).
    #[arg(long)]
    heartbeat_timeout_ms: Option<u64>,
    /// Max consecutive heartbeat failures before restart (default: 3).
    #[arg(long)]
    heartbeat_max_failures: Option<u32>,
    /// Enable/disable heartbeat monitoring (default: true).
    #[arg(long)]
    heartbeat_enabled: Option<bool>,
}

#[derive(Args, Debug, Clone)]
struct ProxyArgs {
    /// Socket path to connect to.
    #[arg(long)]
    socket: PathBuf,
}

#[derive(Args, Debug, Clone)]
struct HealthArgs {
    #[command(flatten)]
    cli: Cli,
}

#[derive(Args, Debug, Clone)]
struct DaemonStatusArgs {
    /// Status socket path (default: /tmp/rmcp-mux.status.sock)
    #[arg(long)]
    socket: Option<std::path::PathBuf>,
    /// Output as JSON instead of table
    #[arg(long)]
    json: bool,
}

#[cfg(feature = "tray")]
#[derive(Args, Debug, Clone)]
struct DashboardArgs {
    /// Status socket path (default: /tmp/rmcp-mux.status.sock)
    #[arg(long)]
    socket: Option<std::path::PathBuf>,
}

fn main() -> Result<()> {
    let cli = RootCli::parse();

    // Handle Dashboard command BEFORE starting tokio runtime (macOS requires main thread)
    #[cfg(feature = "tray")]
    if let Some(CliCommand::Dashboard(args)) = &cli.command {
        return run_dashboard(args.clone());
    }

    // Run async main for all other commands
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main(cli))
}

async fn async_main(cli: RootCli) -> Result<()> {
    match &cli.command {
        Some(CliCommand::Wizard(wargs)) => {
            rmcp_mux::wizard::run_wizard(wargs.clone()).await?;
            return Ok(());
        }
        Some(CliCommand::Scan(args)) => {
            run_scan_cmd(args.clone())?;
            return Ok(());
        }
        Some(CliCommand::Rewire(args)) => {
            run_rewire_cmd(args.clone())?;
            return Ok(());
        }
        Some(CliCommand::Proxy(args)) => {
            return run_proxy(args.socket.clone()).await;
        }
        Some(CliCommand::Status(args)) => {
            run_status_cmd(args.clone())?;
            return Ok(());
        }
        Some(CliCommand::Health(args)) => {
            run_health(args.cli.clone()).await?;
            return Ok(());
        }
        Some(CliCommand::DaemonStatus(args)) => {
            run_daemon_status(args.clone()).await?;
            return Ok(());
        }
        #[cfg(feature = "tray")]
        Some(CliCommand::Dashboard(_)) => {
            // Already handled in main() before tokio starts
            unreachable!("Dashboard handled before async runtime");
        }
        None => {}
    }

    let cli = cli.run;

    let config_path = cli
        .config
        .clone()
        .unwrap_or_else(|| expand_path("~/.codex/mcp.json"));
    let config = load_config(&config_path)?;

    // Handle --show-status: show status of all servers and exit
    if cli.show_status {
        let cfg = config
            .as_ref()
            .ok_or_else(|| anyhow!("--show-status requires a config file (use --config)"))?;
        return status_all(cfg).await;
    }

    // Handle --restart-service: restart a specific service
    if let Some(ref service_name) = cli.restart_service {
        let cfg = config
            .as_ref()
            .ok_or_else(|| anyhow!("--restart-service requires a config file (use --config)"))?;
        return restart_single(cfg, service_name).await;
    }

    // Determine if we're running single service or multi-service mode
    let is_multi_mode = config.is_some()
        && cli.service.is_none()
        && (cli.only.is_some() || cli.except.is_some() || cli.socket.is_none());

    if is_multi_mode {
        // Multi-service mode: run all (or filtered) servers from config
        let cfg = config
            .as_ref()
            .ok_or_else(|| anyhow!("multi-service mode requires a config file"))?;

        let params_list = resolve_params_multi(&cli, cfg)?;

        if params_list.is_empty() {
            return Err(anyhow!(
                "no services to start (check --only/--except filters)"
            ));
        }

        let level = cli
            .log_level
            .parse::<LevelFilter>()
            .map_err(|_| anyhow!("invalid log level: {}", cli.log_level))?;

        tracing_subscriber::fmt()
            .with_max_level(level)
            .with_target(false)
            .init();

        tracing::info!(
            services = params_list.len(),
            "starting mux in multi-service mode"
        );

        run_mux_multi(params_list).await
    } else {
        // Single service mode (legacy behavior)
        let params = resolve_params(&cli, config.as_ref())?;

        let level = params
            .log_level
            .parse::<LevelFilter>()
            .map_err(|_| anyhow!("invalid log level: {}", params.log_level))?;

        tracing_subscriber::fmt()
            .with_max_level(level)
            .with_target(false)
            .init();

        tracing::info!(
            service = params.service_name.as_str(),
            socket = %params.socket.display(),
            cmd = %params.cmd,
            max_clients = params.max_clients,
            tray = params.tray_enabled,
            "mux starting"
        );

        run_mux(params).await
    }
}

async fn run_health(cli: Cli) -> Result<()> {
    let config_path = cli
        .config
        .clone()
        .unwrap_or_else(|| expand_path("~/.codex/mcp.json"));
    let config = load_config(&config_path)?;
    let params = resolve_params(&cli, config.as_ref())?;
    health_check(&params).await?;
    println!("OK: connected to {}", params.socket.display());
    Ok(())
}

async fn run_daemon_status(args: DaemonStatusArgs) -> Result<()> {
    let socket = args
        .socket
        .unwrap_or_else(|| std::path::PathBuf::from(DEFAULT_STATUS_SOCKET));

    let status = query_status(&socket).await.map_err(|e| {
        anyhow!(
            "failed to connect to mux daemon at {}: {} (is rmcp-mux running?)",
            socket.display(),
            e
        )
    })?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&status)?);
    } else {
        print_status_table(&status);
    }

    Ok(())
}

#[cfg(feature = "tray")]
fn run_dashboard(args: DashboardArgs) -> Result<()> {
    use tokio_util::sync::CancellationToken;

    let shutdown = CancellationToken::new();

    println!("Starting rmcp-mux dashboard...");
    println!("Click 'Quit Dashboard' in tray menu to exit");

    let icon = rmcp_mux::tray::find_tray_icon();
    // Run on main thread - required for macOS tray menu creation
    rmcp_mux::tray_dashboard::run_tray_dashboard(shutdown, icon, args.socket);

    println!("Dashboard closed");
    Ok(())
}

// Implement CliOptions trait for the Cli struct
impl CliOptions for Cli {
    fn socket(&self) -> Option<PathBuf> {
        self.socket.clone()
    }
    fn cmd(&self) -> Option<String> {
        self.cmd.clone()
    }
    fn args(&self) -> Vec<String> {
        self.args.clone()
    }
    fn max_active_clients(&self) -> usize {
        self.max_active_clients
    }
    fn lazy_start(&self) -> Option<bool> {
        self.lazy_start
    }
    fn max_request_bytes(&self) -> Option<usize> {
        self.max_request_bytes
    }
    fn request_timeout_ms(&self) -> Option<u64> {
        self.request_timeout_ms
    }
    fn restart_backoff_ms(&self) -> Option<u64> {
        self.restart_backoff_ms
    }
    fn restart_backoff_max_ms(&self) -> Option<u64> {
        self.restart_backoff_max_ms
    }
    fn max_restarts(&self) -> Option<u64> {
        self.max_restarts
    }
    fn log_level(&self) -> String {
        self.log_level.clone()
    }
    fn tray(&self) -> bool {
        self.tray
    }
    fn service_name(&self) -> Option<String> {
        self.service_name.clone()
    }
    fn service(&self) -> Option<String> {
        self.service.clone()
    }
    fn status_file(&self) -> Option<PathBuf> {
        self.status_file.clone()
    }
    fn heartbeat_interval_ms(&self) -> Option<u64> {
        self.heartbeat_interval_ms
    }
    fn heartbeat_timeout_ms(&self) -> Option<u64> {
        self.heartbeat_timeout_ms
    }
    fn heartbeat_max_failures(&self) -> Option<u32> {
        self.heartbeat_max_failures
    }
    fn heartbeat_enabled(&self) -> Option<bool> {
        self.heartbeat_enabled
    }
    fn only(&self) -> Option<Vec<String>> {
        self.only.clone()
    }
    fn except(&self) -> Option<Vec<String>> {
        self.except.clone()
    }
}
