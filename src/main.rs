use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use crossbeam_channel::{bounded, unbounded, Receiver};
use futures::{SinkExt, StreamExt};
use image::ImageFormat;
use rmcp::transport::async_rw::JsonRpcMessageCodec;
use serde_json::Value;
use tokio::net::{UnixListener, UnixStream};
use tokio::process::Command;
use tokio::signal;
use tokio::sync::{mpsc, watch, Mutex, Semaphore};
use tokio::time::sleep;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::filter::LevelFilter;
use tray_icon::{
    menu::{Menu, MenuEvent, MenuId, MenuItem, PredefinedMenuItem},
    Icon, TrayIconBuilder,
};

const MAX_QUEUE: usize = 1024;
const MAX_PENDING: usize = 2048;

/// Robust MCP mux: single MCP server child, many clients via UNIX socket,
/// initialize cache, ID rewriting, child restarts, and active client limit.
#[derive(Parser, Debug)]
#[command(author, version, about)]
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
    /// Service key inside config (`servers.<name>`)
    #[arg(long)]
    service: Option<String>,
}

#[derive(Clone, Debug)]
struct Pending {
    client_id: u64,
    local_id: Value,
    is_initialize: bool,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct Config {
    servers: HashMap<String, ServerConfig>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ServerConfig {
    socket: Option<String>,
    cmd: Option<String>,
    args: Option<Vec<String>>,
    max_active_clients: Option<usize>,
    tray: Option<bool>,
    service_name: Option<String>,
    log_level: Option<String>,
}

#[derive(Clone, Debug)]
enum ServerStatus {
    Starting,
    Running,
    Restarting,
    Failed(String),
    Stopped,
}

#[derive(Clone, Debug)]
struct StatusSnapshot {
    service_name: String,
    server_status: ServerStatus,
    restarts: u64,
    connected_clients: usize,
    active_clients: usize,
    max_active_clients: usize,
    pending_requests: usize,
    cached_initialize: bool,
    initializing: bool,
    last_reset: Option<String>,
}

#[derive(Clone, Debug)]
struct LoadedIcon {
    data: Vec<u8>,
    width: u32,
    height: u32,
}

#[derive(Clone)]
struct MuxState {
    next_client_id: u64,
    next_global_id: u64,
    clients: HashMap<u64, mpsc::UnboundedSender<Value>>,
    pending: HashMap<String, Pending>,
    cached_initialize: Option<Value>,
    init_waiting: Vec<(u64, Value)>,
    initializing: bool,
    server_status: ServerStatus,
    restarts: u64,
    last_reset: Option<String>,
    max_active_clients: usize,
    service_name: String,
}

impl MuxState {
    fn new(max_active_clients: usize, service_name: String) -> Self {
        Self {
            next_client_id: 1,
            next_global_id: 1,
            clients: HashMap::new(),
            pending: HashMap::new(),
            cached_initialize: None,
            init_waiting: Vec::new(),
            initializing: false,
            server_status: ServerStatus::Starting,
            restarts: 0,
            last_reset: None,
            max_active_clients,
            service_name,
        }
    }

    fn register_client(&mut self, tx: mpsc::UnboundedSender<Value>) -> u64 {
        let id = self.next_client_id;
        self.next_client_id += 1;
        self.clients.insert(id, tx);
        id
    }

    fn unregister_client(&mut self, client_id: u64) {
        self.clients.remove(&client_id);
        self.pending.retain(|_, p| p.client_id != client_id);
        self.init_waiting.retain(|(cid, _)| *cid != client_id);
    }

    fn next_request_id(&mut self) -> u64 {
        let id = self.next_global_id;
        self.next_global_id += 1;
        id
    }
}

enum ServerEvent {
    Message(Value),
    Reset(String),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Wczytanie configa (opcjonalnie)
    let config_path = cli
        .config
        .clone()
        .unwrap_or_else(|| expand_path("~/.codex/mcp.json"));
    let config = load_config(&config_path)?;

    // Zbierz parametry z configa + CLI overrides
    let params = resolve_params(&cli, config.as_ref())?;

    let level = params
        .log_level
        .parse::<LevelFilter>()
        .map_err(|_| anyhow!("invalid log level: {}", params.log_level))?;

    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_target(false)
        .init();

    let service_name = Arc::new(params.service_name.clone());
    let socket_path = params.socket.clone();
    let cmd = params.cmd.clone();
    let args = params.args.clone();
    let max_clients = params.max_clients;
    let tray_enabled = params.tray_enabled;

    tracing::info!(
        service = service_name.as_str(),
        socket = %socket_path.display(),
        cmd = %cmd,
        max_clients = max_clients,
        tray = tray_enabled,
        "mux starting"
    );

    if let Some(parent) = socket_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .context("failed to create socket parent dir")?;
    }
    let _ = tokio::fs::remove_file(&socket_path).await;

    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("failed to bind socket {}", socket_path.display()))?;
    info!("mcp_mux listening on {}", socket_path.display());

    let shutdown = CancellationToken::new();
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        shutdown_signal.cancel();
    });

    let state = Arc::new(Mutex::new(MuxState::new(
        max_clients,
        service_name.as_ref().clone(),
    )));
    let active_clients = Arc::new(Semaphore::new(max_clients));

    let (status_tx, status_rx) = {
        let st = state.lock().await;
        let initial = snapshot_for_state(&st, 0);
        drop(st);
        watch::channel(initial)
    };

    let tray_icon = find_tray_icon();
    let tray_handle = if tray_enabled {
        Some(spawn_tray(status_rx.clone(), shutdown.clone(), tray_icon))
    } else {
        None
    };

    let (to_server_tx, to_server_rx) = mpsc::channel::<Value>(MAX_QUEUE);
    let (server_events_tx, server_events_rx) = mpsc::unbounded_channel::<ServerEvent>();

    // Server -> clients router
    let router_state = state.clone();
    let router_active = active_clients.clone();
    let status_for_router = status_tx.clone();
    tokio::spawn(async move {
        handle_server_events(
            router_state,
            router_active,
            status_for_router,
            server_events_rx,
        )
        .await;
    });

    // Child process manager
    let server_state = state.clone();
    let server_shutdown = shutdown.clone();
    let server_active = active_clients.clone();
    let status_for_server = status_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = server_manager(
            cmd.clone(),
            args.clone(),
            to_server_rx,
            server_events_tx,
            server_state,
            server_active,
            status_for_server,
            server_shutdown,
        )
        .await
        {
            error!("server manager exited with error: {e}");
        }
    });

    // Accept clients
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("shutdown requested; closing listener");
                break;
            }
            accept_res = listener.accept() => {
                let (stream, _) = match accept_res {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("accept failed: {e}");
                        continue;
                    }
                };
                let state = state.clone();
                let to_server_tx = to_server_tx.clone();
                let active_clients = active_clients.clone();
                let shutdown = shutdown.clone();
                let status_tx = status_tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream, state, to_server_tx, active_clients, status_tx, shutdown).await {
                        warn!("client handler error: {e}");
                    }
                });
            }
        }
    }

    // Cleanup socket
    let _ = tokio::fs::remove_file(&socket_path).await;
    if let Some(handle) = tray_handle {
        let _ = handle.join();
    }
    Ok(())
}

async fn handle_client(
    stream: UnixStream,
    state: Arc<Mutex<MuxState>>,
    to_server_tx: mpsc::Sender<Value>,
    active_clients: Arc<Semaphore>,
    status_tx: watch::Sender<StatusSnapshot>,
    shutdown: CancellationToken,
) -> Result<()> {
    // limit active clients
    let _permit = active_clients
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| anyhow!("semaphore closed"))?;

    let (read_half, write_half) = stream.into_split();
    let mut client_reader = FramedRead::new(read_half, JsonRpcMessageCodec::<Value>::new());
    let mut client_writer = FramedWrite::new(write_half, JsonRpcMessageCodec::<Value>::new());

    let (client_tx, mut client_rx) = mpsc::unbounded_channel::<Value>();
    let client_id = {
        let mut st = state.lock().await;
        st.register_client(client_tx)
    };
    info!("client {client_id} connected");
    publish_status(&state, &active_clients, &status_tx).await;

    // Writer task
    let writer_state = state.clone();
    let writer_status = status_tx.clone();
    let writer_active = active_clients.clone();
    let writer_handle = tokio::spawn(async move {
        while let Some(msg) = client_rx.recv().await {
            if let Err(e) = client_writer.send(msg).await {
                warn!("write to client {client_id} failed: {e}");
                break;
            }
        }
        let mut st = writer_state.lock().await;
        st.unregister_client(client_id);
        drop(st);
        publish_status(&writer_state, &writer_active, &writer_status).await;
        info!("client {client_id} writer closed");
    });

    // Reader loop
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            frame = client_reader.next() => {
                let Some(frame) = frame else { break; };
                let msg = frame?;
                if let Err(e) = handle_client_message(
                    client_id,
                    msg,
                    &state,
                    &to_server_tx,
                    &active_clients,
                    &status_tx,
                )
                .await
                {
                    warn!("client {client_id} message error: {e}");
                }
            }
        }
    }

    {
        let mut st = state.lock().await;
        st.unregister_client(client_id);
    }
    publish_status(&state, &active_clients, &status_tx).await;
    writer_handle.abort();
    info!("client {client_id} disconnected");
    Ok(())
}

async fn handle_client_message(
    client_id: u64,
    mut msg: Value,
    state: &Arc<Mutex<MuxState>>,
    to_server_tx: &mpsc::Sender<Value>,
    active_clients: &Arc<Semaphore>,
    status_tx: &watch::Sender<StatusSnapshot>,
) -> Result<()> {
    // Notifications (no id) are forwarded best-effort; if the queue is full we drop with a warning.
    if msg.get("id").is_none() {
        if let Err(e) = to_server_tx.try_send(msg) {
            warn!("dropping notification from client {client_id}: {e}");
        }
        publish_status(state, active_clients, status_tx).await;
        return Ok(());
    }

    let method = msg
        .get("method")
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_string();
    let local_id = msg
        .get("id")
        .cloned()
        .ok_or_else(|| anyhow!("missing id"))?;

    if method == "initialize" {
        let mut st = state.lock().await;
        if let Some(cached) = st.cached_initialize.clone() {
            // Serve initialize from cache
            if let Some(tx) = st.clients.get(&client_id) {
                let mut resp = cached.clone();
                set_id(&mut resp, local_id);
                tx.send(resp).ok();
            }
            return Ok(());
        }

        if st.initializing {
            // Czekamy na wynik initialize
            st.init_waiting.push((client_id, local_id));
            return Ok(());
        }

        // Pierwszy initialize: przepuszczamy do serwera
        st.initializing = true;
        if st.pending.len() >= MAX_PENDING {
            if let Some(tx) = st.clients.get(&client_id) {
                tx.send(error_response(
                    local_id.clone(),
                    "mux overloaded (too many pending)",
                ))
                .ok();
            }
            return Ok(());
        }
        let global_id = format!("c{client_id}:{}", st.next_request_id());
        st.pending.insert(
            global_id.clone(),
            Pending {
                client_id,
                local_id: local_id.clone(),
                is_initialize: true,
            },
        );
        drop(st);
        set_id(&mut msg, Value::String(global_id));
        to_server_tx
            .send(msg)
            .await
            .map_err(|_| anyhow!("server channel closed"))?;
        return Ok(());
    }

    // Normal request
    let global_id = {
        let mut st = state.lock().await;
        if st.pending.len() >= MAX_PENDING {
            if let Some(tx) = st.clients.get(&client_id) {
                tx.send(error_response(
                    local_id.clone(),
                    "mux overloaded (too many pending)",
                ))
                .ok();
            }
            return Ok(());
        }
        let gid = format!("c{client_id}:{}", st.next_request_id());
        st.pending.insert(
            gid.clone(),
            Pending {
                client_id,
                local_id: local_id.clone(),
                is_initialize: false,
            },
        );
        gid
    };

    set_id(&mut msg, Value::String(global_id));
    to_server_tx
        .send(msg)
        .await
        .map_err(|_| anyhow!("server channel closed"))?;
    publish_status(state, active_clients, status_tx).await;
    Ok(())
}

async fn handle_server_events(
    state: Arc<Mutex<MuxState>>,
    active_clients: Arc<Semaphore>,
    status_tx: watch::Sender<StatusSnapshot>,
    mut rx: mpsc::UnboundedReceiver<ServerEvent>,
) {
    while let Some(evt) = rx.recv().await {
        match evt {
            ServerEvent::Message(msg) => {
                if let Err(e) =
                    handle_server_message(msg, &state, &active_clients, &status_tx).await
                {
                    warn!("server message routing failed: {e}");
                }
            }
            ServerEvent::Reset(reason) => {
                reset_state(&state, &reason, &active_clients, &status_tx).await;
            }
        }
    }
}

fn expand_path<P: AsRef<str>>(raw: P) -> PathBuf {
    let s = raw.as_ref();
    if let Some(stripped) = s.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(stripped);
        }
    }
    PathBuf::from(s)
}

fn load_config(path: &Path) -> Result<Option<Config>> {
    if !path.exists() {
        return Ok(None);
    }
    let data = fs::read_to_string(path)
        .with_context(|| format!("failed to read config at {}", path.display()))?;

    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    let cfg: Config = match ext.as_str() {
        "yaml" | "yml" => serde_yaml::from_str(&data)
            .with_context(|| format!("failed to parse yaml config {}", path.display()))?,
        "toml" => toml::from_str(&data)
            .with_context(|| format!("failed to parse toml config {}", path.display()))?,
        _ => serde_json::from_str(&data)
            .with_context(|| format!("failed to parse json config {}", path.display()))?,
    };
    Ok(Some(cfg))
}

fn resolve_params(cli: &Cli, config: Option<&Config>) -> Result<ResolvedParams> {
    let service_cfg = if let Some(cfg) = config {
        if let Some(name) = &cli.service {
            let found = cfg
                .servers
                .get(name)
                .cloned()
                .ok_or_else(|| anyhow!("service '{name}' not found in config"))?;
            Some((name.clone(), found))
        } else {
            None
        }
    } else {
        None
    };

    if config.is_some() && cli.service.is_none() {
        return Err(anyhow!("--service is required when using --config"));
    }

    let socket = cli
        .socket
        .clone()
        .or_else(|| {
            service_cfg
                .as_ref()
                .and_then(|(_, c)| c.socket.clone().map(expand_path))
        })
        .ok_or_else(|| anyhow!("socket path not provided (use --socket or config)"))?;

    let cmd = cli
        .cmd
        .clone()
        .or_else(|| service_cfg.as_ref().and_then(|(_, c)| c.cmd.clone()))
        .ok_or_else(|| anyhow!("cmd not provided (use --cmd or config)"))?;

    let args = if !cli.args.is_empty() {
        cli.args.clone()
    } else {
        service_cfg
            .as_ref()
            .and_then(|(_, c)| c.args.clone())
            .unwrap_or_default()
    };

    let max_clients = service_cfg
        .as_ref()
        .and_then(|(_, c)| c.max_active_clients)
        .unwrap_or(cli.max_active_clients);

    let tray_enabled = if cli.tray {
        true
    } else {
        service_cfg
            .as_ref()
            .and_then(|(_, c)| c.tray)
            .unwrap_or(false)
    };

    let log_level = service_cfg
        .as_ref()
        .and_then(|(_, c)| c.log_level.clone())
        .unwrap_or_else(|| cli.log_level.clone());

    let service_name_raw = cli
        .service_name
        .clone()
        .or_else(|| {
            service_cfg
                .as_ref()
                .and_then(|(_, c)| c.service_name.clone())
        })
        .or_else(|| {
            socket
                .file_name()
                .and_then(|n| n.to_string_lossy().split('.').next().map(|s| s.to_string()))
        })
        .unwrap_or_else(|| "mcp_mux".to_string());

    Ok(ResolvedParams {
        socket,
        cmd,
        args,
        max_clients,
        tray_enabled,
        log_level,
        service_name: service_name_raw,
    })
}

#[derive(Clone, Debug)]
struct ResolvedParams {
    socket: PathBuf,
    cmd: String,
    args: Vec<String>,
    max_clients: usize,
    tray_enabled: bool,
    log_level: String,
    service_name: String,
}

async fn handle_server_message(
    msg: Value,
    state: &Arc<Mutex<MuxState>>,
    active_clients: &Arc<Semaphore>,
    status_tx: &watch::Sender<StatusSnapshot>,
) -> Result<()> {
    if msg.get("id").is_none() {
        // notification -> broadcast
        let st = state.lock().await;
        for tx in st.clients.values() {
            tx.send(msg.clone()).ok();
        }
        return Ok(());
    }

    let id_val = msg
        .get("id")
        .cloned()
        .ok_or_else(|| anyhow!("missing id in server response"))?;
    let id_str = id_val
        .as_str()
        .map(|s| s.to_string())
        .or_else(|| id_val.as_i64().map(|n| n.to_string()))
        .ok_or_else(|| anyhow!("unsupported id type"))?;

    let pending = {
        let mut st = state.lock().await;
        st.pending.remove(&id_str)
    };

    let Some(pending) = pending else {
        warn!("no pending request for id {id_str}");
        return Ok(());
    };

    let target_tx = {
        let st = state.lock().await;
        st.clients.get(&pending.client_id).cloned()
    };

    if let Some(tx) = target_tx {
        let mut resp = msg.clone();
        set_id(&mut resp, pending.local_id.clone());
        let is_init = pending.is_initialize;
        tx.send(resp.clone()).ok();

        if is_init {
            let mut st = state.lock().await;
            st.cached_initialize = Some(resp.clone());
            st.initializing = false;
            // Respond to waiting initialize callers
            let waiters = std::mem::take(&mut st.init_waiting);
            for (cid, lid) in waiters {
                if let Some(wait_tx) = st.clients.get(&cid) {
                    let mut clone_resp = resp.clone();
                    set_id(&mut clone_resp, lid);
                    wait_tx.send(clone_resp).ok();
                }
            }
        }
    }
    publish_status(state, active_clients, status_tx).await;
    Ok(())
}

async fn reset_state(
    state: &Arc<Mutex<MuxState>>,
    reason: &str,
    active_clients: &Arc<Semaphore>,
    status_tx: &watch::Sender<StatusSnapshot>,
) {
    let mut st = state.lock().await;
    let pending = std::mem::take(&mut st.pending);
    let waiters = std::mem::take(&mut st.init_waiting);
    st.cached_initialize = None;
    st.initializing = false;
    st.last_reset = Some(reason.to_string());

    for (_, p) in pending {
        if let Some(tx) = st.clients.get(&p.client_id) {
            tx.send(error_response(p.local_id, reason)).ok();
        }
    }
    for (cid, lid) in waiters {
        if let Some(tx) = st.clients.get(&cid) {
            tx.send(error_response(lid, reason)).ok();
        }
    }
    drop(st);
    publish_status(state, active_clients, status_tx).await;
}

fn error_response(id: Value, message: &str) -> Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": -32000,
            "message": message,
        }
    })
}

fn snapshot_for_state(st: &MuxState, active_clients: usize) -> StatusSnapshot {
    StatusSnapshot {
        service_name: st.service_name.clone(),
        server_status: st.server_status.clone(),
        restarts: st.restarts,
        connected_clients: st.clients.len(),
        active_clients,
        max_active_clients: st.max_active_clients,
        pending_requests: st.pending.len(),
        cached_initialize: st.cached_initialize.is_some(),
        initializing: st.initializing,
        last_reset: st.last_reset.clone(),
    }
}

async fn publish_status(
    state: &Arc<Mutex<MuxState>>,
    active_clients: &Arc<Semaphore>,
    status_tx: &watch::Sender<StatusSnapshot>,
) {
    let st = state.lock().await;
    let active = st
        .max_active_clients
        .saturating_sub(active_clients.available_permits());
    let snapshot = snapshot_for_state(&st, active);
    drop(st);
    let _ = status_tx.send(snapshot);
}

fn set_id(msg: &mut Value, id: Value) {
    if let Some(obj) = msg.as_object_mut() {
        obj.insert("id".to_string(), id);
    }
}

#[allow(clippy::too_many_arguments)]
async fn server_manager(
    cmd: String,
    args: Vec<String>,
    mut to_server_rx: mpsc::Receiver<Value>,
    server_events_tx: mpsc::UnboundedSender<ServerEvent>,
    state: Arc<Mutex<MuxState>>,
    active_clients: Arc<Semaphore>,
    status_tx: watch::Sender<StatusSnapshot>,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut backoff = Duration::from_secs(1);

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        info!("starting MCP server: {} {:?}", cmd, args);
        let mut child = Command::new(&cmd)
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .context("failed to spawn MCP server")?;

        {
            let mut st = state.lock().await;
            st.server_status = ServerStatus::Running;
        }
        publish_status(&state, &active_clients, &status_tx).await;

        let child_stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("failed to capture stdin"))?;
        let child_stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("failed to capture stdout"))?;

        let mut writer = FramedWrite::new(child_stdin, JsonRpcMessageCodec::<Value>::new());
        let mut reader = FramedRead::new(child_stdout, JsonRpcMessageCodec::<Value>::new());

        // czytanie z serwera
        let reader_task = {
            let server_events_tx = server_events_tx.clone();
            tokio::spawn(async move {
                loop {
                    let next = reader.next().await;
                    match next {
                        Some(Ok(msg)) => {
                            if server_events_tx.send(ServerEvent::Message(msg)).is_err() {
                                break;
                            }
                        }
                        Some(Err(e)) => {
                            error!("server reader error: {e}");
                            break;
                        }
                        None => {
                            warn!("server stdout closed");
                            break;
                        }
                    }
                }
            })
        };

        // write loop and monitor
        let server_events_tx_clone = server_events_tx.clone();
        while !shutdown.is_cancelled() {
            tokio::select! {
                maybe_msg = to_server_rx.recv() => {
                    let Some(msg) = maybe_msg else { break; };
                    if let Err(e) = writer.send(msg).await {
                        warn!("write to server failed: {e}");
                        {
                            let mut st = state.lock().await;
                            st.server_status = ServerStatus::Failed(e.to_string());
                            st.last_reset = Some("write failure".into());
                        }
                        publish_status(&state, &active_clients, &status_tx).await;
                        break;
                    }
                }
                status = child.wait() => {
                    match status {
                        Ok(status) => warn!("server exited with status {status}"),
                        Err(e) => {
                            warn!("server wait error: {e}");
                            let mut st = state.lock().await;
                            st.server_status = ServerStatus::Failed(e.to_string());
                            st.last_reset = Some("wait error".into());
                        }
                    }
                    publish_status(&state, &active_clients, &status_tx).await;
                    break;
                }
                _ = shutdown.cancelled() => { break; }
            }
        }

        // child cleanup
        let _ = child.kill().await;
        reader_task.abort();

        // reset stanu
        server_events_tx_clone
            .send(ServerEvent::Reset("MCP server restarted".into()))
            .ok();
        {
            let mut st = state.lock().await;
            st.cached_initialize = None;
            st.initializing = false;
            if shutdown.is_cancelled() {
                st.server_status = ServerStatus::Stopped;
            } else {
                st.server_status = ServerStatus::Restarting;
                st.restarts = st.restarts.saturating_add(1);
            }
        }
        publish_status(&state, &active_clients, &status_tx).await;

        if shutdown.is_cancelled() {
            break;
        }
        info!("restarting MCP server after failure, backoff {:?}", backoff);
        sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
    Ok(())
}

fn spawn_tray(
    status_rx: watch::Receiver<StatusSnapshot>,
    shutdown: CancellationToken,
    icon: Option<LoadedIcon>,
) -> thread::JoinHandle<()> {
    let (snap_tx, snap_rx) = unbounded();
    let (stop_tx, stop_rx) = bounded(1);

    // Most recent snapshot forwarded to blocking tray thread.
    tokio::spawn(async move {
        let mut rx = status_rx;
        let _ = snap_tx.send(rx.borrow().clone());
        while rx.changed().await.is_ok() {
            let snap = rx.borrow().clone();
            if snap_tx.send(snap).is_err() {
                break;
            }
        }
    });

    // Stop signal when shutdown is requested.
    tokio::spawn({
        let stop_tx = stop_tx.clone();
        let shutdown = shutdown.clone();
        async move {
            shutdown.cancelled().await;
            let _ = stop_tx.send(());
        }
    });

    thread::spawn(move || tray_loop(snap_rx, stop_rx, shutdown, icon))
}

struct TrayUi {
    _tray: tray_icon::TrayIcon,
    header: MenuItem,
    status: MenuItem,
    clients: MenuItem,
    pending: MenuItem,
    init_state: MenuItem,
    restarts: MenuItem,
    quit_id: MenuId,
}

impl TrayUi {
    fn update(&self, snapshot: &StatusSnapshot) {
        self.header
            .set_text(format!("Service: {}", snapshot.service_name));
        self.status.set_text(status_line(snapshot));
        self.clients.set_text(client_line(snapshot));
        self.pending.set_text(pending_line(snapshot));
        self.init_state.set_text(init_line(snapshot));
        self.restarts.set_text(restart_line(snapshot));
    }
}

fn tray_loop(
    snap_rx: Receiver<StatusSnapshot>,
    stop_rx: Receiver<()>,
    shutdown: CancellationToken,
    icon: Option<LoadedIcon>,
) {
    let mut current = match snap_rx.recv() {
        Ok(s) => s,
        Err(_) => return,
    };

    let ui = match build_tray(&current, icon.as_ref()) {
        Ok(u) => u,
        Err(e) => {
            eprintln!("tray init failed: {e}");
            return;
        }
    };

    loop {
        while let Ok(event) = MenuEvent::receiver().try_recv() {
            if event.id == ui.quit_id {
                shutdown.cancel();
                return;
            }
        }

        crossbeam_channel::select! {
            recv(stop_rx) -> _ => { return; }
            recv(snap_rx) -> msg => {
                match msg {
                    Ok(snap) => {
                        current = snap;
                        ui.update(&current);
                    }
                    Err(_) => return,
                }
            }
            default(Duration::from_millis(150)) => {}
        }
    }
}

fn build_tray(snapshot: &StatusSnapshot, icon_data: Option<&LoadedIcon>) -> Result<TrayUi> {
    let menu = Menu::new();
    let header = MenuItem::new(format!("Service: {}", snapshot.service_name), false, None);
    let status_item = MenuItem::new(status_line(snapshot), false, None);
    let clients_item = MenuItem::new(client_line(snapshot), false, None);
    let pending_item = MenuItem::new(pending_line(snapshot), false, None);
    let init_item = MenuItem::new(init_line(snapshot), false, None);
    let restart_item = MenuItem::new(restart_line(snapshot), false, None);
    let quit_item = MenuItem::new("Quit mux", true, None);
    let sep = PredefinedMenuItem::separator();

    for item in [
        &header,
        &status_item,
        &clients_item,
        &pending_item,
        &init_item,
        &restart_item,
    ] {
        menu.append(item)?;
    }
    menu.append(&sep)?;
    menu.append(&quit_item)?;

    let icon = if let Some(data) = icon_data {
        Icon::from_rgba(data.data.clone(), data.width, data.height)?
    } else {
        default_icon()
    };
    let tray = TrayIconBuilder::new()
        .with_tooltip(format!("mcp_mux – {}", snapshot.service_name))
        .with_icon(icon)
        .with_menu(Box::new(menu.clone()))
        .build()?;

    Ok(TrayUi {
        _tray: tray,
        header,
        status: status_item,
        clients: clients_item,
        pending: pending_item,
        init_state: init_item,
        restarts: restart_item,
        quit_id: quit_item.id().clone(),
    })
}

fn status_line(snapshot: &StatusSnapshot) -> String {
    let status_text = match &snapshot.server_status {
        ServerStatus::Starting => "Starting".to_string(),
        ServerStatus::Running => "Running".to_string(),
        ServerStatus::Restarting => "Restarting".to_string(),
        ServerStatus::Stopped => "Stopped".to_string(),
        ServerStatus::Failed(reason) => format!("Failed: {reason}"),
    };
    format!("Status: {status_text}")
}

fn client_line(snapshot: &StatusSnapshot) -> String {
    format!(
        "Clients: {} (active {}/{})",
        snapshot.connected_clients, snapshot.active_clients, snapshot.max_active_clients
    )
}

fn pending_line(snapshot: &StatusSnapshot) -> String {
    format!("Pending requests: {}", snapshot.pending_requests)
}

fn init_line(snapshot: &StatusSnapshot) -> String {
    let cache = if snapshot.cached_initialize {
        "cached"
    } else {
        "uncached"
    };
    let init = if snapshot.initializing {
        "initializing"
    } else {
        "idle"
    };
    format!("Initialize: {cache}, {init}")
}

fn restart_line(snapshot: &StatusSnapshot) -> String {
    match &snapshot.last_reset {
        Some(reason) => format!("Restarts: {} (last: {})", snapshot.restarts, reason),
        None => format!("Restarts: {}", snapshot.restarts),
    }
}

fn default_icon() -> Icon {
    let (w, h) = (16, 16);
    let mut data = Vec::with_capacity(w * h * 4);
    for y in 0..h {
        for x in 0..w {
            let gradient = 0x60 + ((x + y) % 32) as u8;
            data.extend_from_slice(&[0x4b, gradient, 0xff, 0xff]);
        }
    }
    Icon::from_rgba(data, w as u32, h as u32).expect("valid icon")
}

fn find_tray_icon() -> Option<LoadedIcon> {
    let candidates = [
        PathBuf::from("public/mcp_mux_icon.png"),
        std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|d| d.join("public/mcp_mux_icon.png")))
            .unwrap_or_else(|| PathBuf::from("public/mcp_mux_icon.png")),
    ];

    for path in candidates {
        if let Some(icon) = load_icon_from_file(&path) {
            return Some(icon);
        }
    }
    None
}

fn load_icon_from_file(path: &Path) -> Option<LoadedIcon> {
    let data = fs::read(path).ok()?;
    let img = image::load_from_memory_with_format(&data, ImageFormat::Png).ok()?;
    let rgba = img.to_rgba8();
    let (width, height) = rgba.dimensions();
    Some(LoadedIcon {
        data: rgba.into_raw(),
        width,
        height,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs::{self, File};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::mpsc::UnboundedReceiver;

    fn capture_client(state: &mut MuxState) -> (u64, UnboundedReceiver<Value>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let id = state.register_client(tx);
        (id, rx)
    }

    fn tmp_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_nanos();
        env::temp_dir().join(format!("{}-{}", name, nanos))
    }

    #[tokio::test]
    async fn set_id_updates_object() {
        let mut obj = serde_json::json!({"id": "old"});
        set_id(&mut obj, Value::String("new".into()));
        assert_eq!(obj.get("id"), Some(&Value::String("new".into())));
    }

    #[tokio::test]
    async fn error_response_has_code_and_message() {
        let resp = error_response(Value::Number(1.into()), "boom");
        assert_eq!(resp.get("id"), Some(&Value::Number(1.into())));
        assert_eq!(
            resp.get("error").and_then(|e| e.get("message")),
            Some(&Value::String("boom".into()))
        );
    }

    #[tokio::test]
    async fn initialize_response_is_cached_and_fanned_out() {
        let state = Arc::new(Mutex::new(MuxState::new(5, "test".into())));
        let active_clients = Arc::new(Semaphore::new(5));
        let (status_tx, _status_rx) = {
            let st = state.lock().await;
            watch::channel(snapshot_for_state(&st, 0))
        };
        let mut st = state.lock().await;
        let (cid1, mut rx1) = capture_client(&mut st);
        let (cid2, mut rx2) = capture_client(&mut st);

        // pending initialize for client1
        st.pending.insert(
            "g1".into(),
            Pending {
                client_id: cid1,
                local_id: Value::String("loc1".into()),
                is_initialize: true,
            },
        );
        // waiter client2
        st.init_waiting.push((cid2, Value::String("loc2".into())));
        st.initializing = true;
        drop(st);

        let server_msg = serde_json::json!({
            "id": "g1",
            "result": { "ok": true }
        });
        assert!(
            handle_server_message(server_msg, &state, &active_clients, &status_tx)
                .await
                .is_ok()
        );

        // client1 got response with local id
        let m1 = rx1.recv().await.expect("client1 message");
        assert_eq!(m1.get("id"), Some(&Value::String("loc1".into())));

        // client2 got fanned out cached response with its id
        let m2 = rx2.recv().await.expect("client2 message");
        assert_eq!(m2.get("id"), Some(&Value::String("loc2".into())));

        // cache is populated, initializing flag cleared
        let st = state.lock().await;
        assert!(st.cached_initialize.is_some());
        assert!(!st.initializing);
        assert!(st.init_waiting.is_empty());
    }

    #[tokio::test]
    async fn non_initialize_response_routed_without_caching() {
        let state = Arc::new(Mutex::new(MuxState::new(5, "test".into())));
        let active_clients = Arc::new(Semaphore::new(5));
        let (status_tx, _status_rx) = {
            let st = state.lock().await;
            watch::channel(snapshot_for_state(&st, 0))
        };
        let mut st = state.lock().await;
        let (cid1, mut rx1) = capture_client(&mut st);
        st.pending.insert(
            "g2".into(),
            Pending {
                client_id: cid1,
                local_id: Value::Number(7.into()),
                is_initialize: false,
            },
        );
        drop(st);

        let server_msg = serde_json::json!({"id": "g2", "result": 123});
        assert!(
            handle_server_message(server_msg, &state, &active_clients, &status_tx)
                .await
                .is_ok()
        );

        let msg = rx1.recv().await.expect("client message");
        assert_eq!(msg.get("id"), Some(&Value::Number(7.into())));
        let st = state.lock().await;
        assert!(st.cached_initialize.is_none());
    }

    #[tokio::test]
    async fn reset_state_broadcasts_errors() {
        let state = Arc::new(Mutex::new(MuxState::new(5, "test".into())));
        let active_clients = Arc::new(Semaphore::new(5));
        let (status_tx, _status_rx) = {
            let st = state.lock().await;
            watch::channel(snapshot_for_state(&st, 0))
        };
        let mut st = state.lock().await;
        let (cid1, mut rx1) = capture_client(&mut st);
        let (cid2, mut rx2) = capture_client(&mut st);
        st.pending.insert(
            "g3".into(),
            Pending {
                client_id: cid1,
                local_id: Value::Number(1.into()),
                is_initialize: false,
            },
        );
        st.init_waiting.push((cid2, Value::Number(2.into())));
        drop(st);

        reset_state(&state, "reset", &active_clients, &status_tx).await;

        let m1 = rx1.recv().await.expect("pending error");
        let m2 = rx2.recv().await.expect("waiter error");
        assert_eq!(
            m1.get("error").and_then(|e| e.get("message")),
            Some(&Value::String("reset".into()))
        );
        assert_eq!(
            m2.get("error").and_then(|e| e.get("message")),
            Some(&Value::String("reset".into()))
        );
    }

    #[test]
    fn expand_path_expands_home() {
        let home = tmp_path("home-test");
        fs::create_dir_all(&home).expect("create home temp dir");
        env::set_var("HOME", &home);
        let expanded = expand_path("~/socket.sock");
        assert!(expanded.starts_with(&home));
    }

    #[test]
    fn load_config_parses_json_yaml_toml() {
        let base = tmp_path("cfg");
        fs::create_dir_all(&base).expect("create base temp dir");

        let json_path = base.join("c.json");
        let yaml_path = base.join("c.yaml");
        let toml_path = base.join("c.toml");

        let json = r#"{
  "servers": {
    "s": {"socket": "/tmp/a", "cmd": "npx", "args": ["@mcp"], "max_active_clients": 2, "tray": true, "service_name": "s"}
  }
}"#;
        let yaml = r#"servers:
  s:
    socket: "/tmp/a"
    cmd: "npx"
    args: ["@mcp"]
    max_active_clients: 2
    tray: true
    service_name: "s"
"#;
        let toml = r#"[servers.s]
socket = "/tmp/a"
cmd = "npx"
args = ["@mcp"]
max_active_clients = 2
tray = true
service_name = "s"
"#;

        fs::write(&json_path, json).expect("write json config");
        fs::write(&yaml_path, yaml).expect("write yaml config");
        fs::write(&toml_path, toml).expect("write toml config");

        assert!(load_config(&json_path).unwrap().is_some());
        assert!(load_config(&yaml_path).unwrap().is_some());
        assert!(load_config(&toml_path).unwrap().is_some());
    }

    #[test]
    fn load_config_missing_returns_none() {
        let missing = tmp_path("nope.json");
        assert!(load_config(&missing).unwrap().is_none());
    }

    #[test]
    fn resolve_params_overrides_from_config() {
        let cfg = Config {
            servers: HashMap::from([(
                "svc".into(),
                ServerConfig {
                    socket: Some("/tmp/override.sock".into()),
                    cmd: Some("npx".into()),
                    args: Some(vec!["@mcp".into()]),
                    max_active_clients: Some(7),
                    tray: Some(true),
                    service_name: Some("svc-name".into()),
                    log_level: Some("debug".into()),
                },
            )]),
        };

        let cli = Cli {
            socket: None,
            cmd: None,
            args: vec![],
            max_active_clients: 5,
            log_level: "info".into(),
            tray: false,
            service_name: None,
            config: None,
            service: Some("svc".into()),
        };

        let params = resolve_params(&cli, Some(&cfg)).expect("resolve params from config");
        assert_eq!(params.socket, PathBuf::from("/tmp/override.sock"));
        assert_eq!(params.cmd, "npx");
        assert_eq!(params.args, vec!["@mcp".to_string()]);
        assert_eq!(params.max_clients, 7);
        assert!(params.tray_enabled);
        assert_eq!(params.service_name, "svc-name");
        assert_eq!(params.log_level, "debug");
    }

    #[test]
    fn resolve_params_requires_service_with_config() {
        let cfg = Config {
            servers: HashMap::new(),
        };
        let cli = Cli {
            socket: None,
            cmd: None,
            args: vec![],
            max_active_clients: 5,
            log_level: "info".into(),
            tray: false,
            service_name: None,
            config: None,
            service: None,
        };
        let err = resolve_params(&cli, Some(&cfg)).unwrap_err();
        assert!(err.to_string().contains("--service"));
    }

    #[test]
    fn resolve_params_cli_overrides_socket() {
        let cfg = Config {
            servers: HashMap::from([(
                "svc".into(),
                ServerConfig {
                    socket: Some("/tmp/override.sock".into()),
                    cmd: Some("npx".into()),
                    args: Some(vec!["@mcp".into()]),
                    max_active_clients: Some(2),
                    tray: Some(false),
                    service_name: Some("svc".into()),
                    log_level: Some("info".into()),
                },
            )]),
        };
        let cli = Cli {
            socket: Some(PathBuf::from("/tmp/cli.sock")),
            cmd: Some("node".into()),
            args: vec!["srv".into()],
            max_active_clients: 1,
            log_level: "warn".into(),
            tray: true,
            service_name: Some("cli".into()),
            config: None,
            service: Some("svc".into()),
        };
        let params = resolve_params(&cli, Some(&cfg)).expect("resolve params cli overrides");
        assert_eq!(params.socket, PathBuf::from("/tmp/cli.sock"));
        assert_eq!(params.cmd, "node");
        assert_eq!(params.args, vec!["srv".to_string()]);
        // max_clients remains from config unless absent; other fields override from CLI
        assert_eq!(params.max_clients, 2);
        assert!(params.tray_enabled);
        assert_eq!(params.service_name, "cli");
        // log_level comes from config when present
        assert_eq!(params.log_level, "info");
    }

    #[tokio::test]
    async fn publish_status_counts_active() {
        let state = Arc::new(Mutex::new(MuxState::new(3, "svc".into())));
        let active = Arc::new(Semaphore::new(3));
        let (tx, rx) = {
            let st = state.lock().await;
            watch::channel(snapshot_for_state(&st, 0))
        };

        // take two permits to simulate 2 active clients
        let p1 = active.clone().acquire_owned().await.expect("first permit");
        let p2 = active.clone().acquire_owned().await.expect("second permit");
        publish_status(&state, &active, &tx).await;
        let snap = rx.borrow().clone();
        assert_eq!(snap.active_clients, 2);
        drop(p1);
        drop(p2);
    }

    #[test]
    fn load_icon_from_file_works_for_png() {
        let path = tmp_path("icon.png");
        let mut file = File::create(&path).expect("create icon file");
        // Tiny 2x2 RGBA white image encoded as PNG via image crate
        let buf = vec![255u8; 2 * 2 * 4];
        let img = image::RgbaImage::from_raw(2, 2, buf).expect("build raw image");
        img.write_to(&mut file, image::ImageFormat::Png)
            .expect("write png");

        let icon = load_icon_from_file(&path);
        assert!(icon.is_some());
    }

    #[test]
    fn status_and_lines_render() {
        let base = StatusSnapshot {
            service_name: "s".into(),
            server_status: ServerStatus::Starting,
            restarts: 0,
            connected_clients: 1,
            active_clients: 1,
            max_active_clients: 3,
            pending_requests: 2,
            cached_initialize: false,
            initializing: true,
            last_reset: None,
        };
        assert!(status_line(&base).contains("Starting"));
        assert!(client_line(&base).contains("active 1/3"));
        assert!(pending_line(&base).contains("2"));
        assert!(init_line(&base).contains("uncached"));
        assert!(restart_line(&base).contains("0"));

        let mut running = base.clone();
        running.server_status = ServerStatus::Failed("x".into());
        running.cached_initialize = true;
        running.initializing = false;
        running.last_reset = Some("fail".into());
        assert!(status_line(&running).contains("Failed"));
        assert!(init_line(&running).contains("cached"));
        assert!(restart_line(&running).contains("fail"));
    }

    #[test]
    fn default_icon_does_not_panic() {
        let icon = default_icon();
        // Construction succeeded
        let _ = icon;
    }
}
