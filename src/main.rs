use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use futures::{SinkExt, StreamExt};
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

mod config;
mod state;
#[cfg(feature = "tray")]
mod tray;

use crate::config::{expand_path, load_config, resolve_params};
use crate::state::{
    error_response, publish_status, reset_state, set_id, snapshot_for_state, MuxState, Pending,
    ServerStatus, StatusSnapshot,
};
#[cfg(feature = "tray")]
use crate::tray::{find_tray_icon, spawn_tray};

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
    #[cfg(not(feature = "tray"))]
    let _ = &status_rx;

    #[cfg(feature = "tray")]
    let tray_icon = find_tray_icon();
    #[cfg(feature = "tray")]
    let tray_handle: Option<std::thread::JoinHandle<()>> = if tray_enabled {
        Some(spawn_tray(status_rx.clone(), shutdown.clone(), tray_icon))
    } else {
        None
    };
    #[cfg(not(feature = "tray"))]
    let _tray_handle: Option<()> = if tray_enabled {
        warn!("tray support compiled out; ignoring --tray");
        None
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
    #[cfg(feature = "tray")]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{expand_path, load_config, Config, ServerConfig};
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::mpsc::UnboundedReceiver;
    use tokio::sync::mpsc::{self};

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

    #[tokio::test]
    async fn reset_state_updates_last_reset_and_status() {
        let state = Arc::new(Mutex::new(MuxState::new(2, "svc".into())));
        let active = Arc::new(Semaphore::new(2));
        let (status_tx, mut status_rx) = {
            let st = state.lock().await;
            watch::channel(snapshot_for_state(&st, 0))
        };

        // consume initial snapshot
        let _ = status_rx.borrow().clone();

        reset_state(&state, "restart-test", &active, &status_tx).await;

        // wait for updated snapshot to propagate
        status_rx.changed().await.expect("status update");
        let snap = status_rx.borrow().clone();
        assert_eq!(snap.last_reset.as_deref(), Some("restart-test"));
        assert!(!snap.initializing);
        assert_eq!(snap.pending_requests, 0);
    }

    #[tokio::test]
    async fn initialize_served_from_cache_does_not_queue() {
        let state = Arc::new(Mutex::new(MuxState::new(5, "svc".into())));
        let active = Arc::new(Semaphore::new(5));
        let (status_tx, _status_rx) = {
            let st = state.lock().await;
            watch::channel(snapshot_for_state(&st, 0))
        };
        let (to_server_tx, mut to_server_rx) = mpsc::channel::<Value>(1);

        let mut st = state.lock().await;
        let (cid, mut rx) = capture_client(&mut st);
        st.cached_initialize = Some(serde_json::json!({"id": "server-init", "result": "ok"}));
        drop(st);

        let msg = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "client-init",
            "method": "initialize",
            "params": {}
        });

        handle_client_message(cid, msg, &state, &to_server_tx, &active, &status_tx)
            .await
            .expect("handle cached init");

        // nothing forwarded to server
        assert!(to_server_rx.try_recv().is_err());

        // client got cached response with rewritten id
        let resp = rx.recv().await.expect("cached init response");
        assert_eq!(resp.get("id"), Some(&Value::String("client-init".into())));

        let st = state.lock().await;
        assert!(st.cached_initialize.is_some());
        assert!(!st.initializing);
        assert!(st.pending.is_empty());
    }

    #[tokio::test]
    async fn reset_state_clears_initialize_and_pending() {
        let state = Arc::new(Mutex::new(MuxState::new(5, "svc".into())));
        let active = Arc::new(Semaphore::new(5));
        let (status_tx, _status_rx) = {
            let st = state.lock().await;
            watch::channel(snapshot_for_state(&st, 0))
        };

        let mut st = state.lock().await;
        let (cid, mut rx) = capture_client(&mut st);
        st.cached_initialize = Some(serde_json::json!({"id": "init", "result": true}));
        st.initializing = true;
        st.pending.insert(
            "g-pending".into(),
            Pending {
                client_id: cid,
                local_id: Value::String("local-id".into()),
                is_initialize: true,
            },
        );
        st.init_waiting
            .push((cid, Value::String("waiter-id".into())));
        drop(st);

        reset_state(&state, "reset-reason", &active, &status_tx).await;

        // pending + waiters get errors
        let errs: Vec<_> = rx.recv().await.into_iter().collect();
        assert!(!errs.is_empty());

        let st = state.lock().await;
        assert!(st.pending.is_empty());
        assert!(st.init_waiting.is_empty());
        assert!(st.cached_initialize.is_none());
        assert!(!st.initializing);
        assert_eq!(st.last_reset.as_deref(), Some("reset-reason"));
    }
}
