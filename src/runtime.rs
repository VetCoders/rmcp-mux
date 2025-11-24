use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use futures::{SinkExt, StreamExt};
use rmcp::transport::async_rw::JsonRpcMessageCodec;
use serde_json::Value;
use tokio::fs;
use tokio::io::{stdin, stdout, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::process::Command as TokioCommand;
use tokio::sync::{mpsc, watch, Mutex, Semaphore};
use tokio::time::sleep;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::ResolvedParams;
use crate::state::{
    error_response, publish_status, reset_state, set_id, snapshot_for_state, MuxState, Pending,
    ServerStatus, StatusSnapshot,
};
#[cfg(feature = "tray")]
use crate::tray::{find_tray_icon, spawn_tray};

pub const MAX_QUEUE: usize = 1024;
pub const MAX_PENDING: usize = 2048;

/// Lightweight health check: verifies the mux socket is reachable.
pub async fn health_check(params: &ResolvedParams) -> Result<()> {
    let mut stream = UnixStream::connect(&params.socket)
        .await
        .with_context(|| format!("failed to connect to {}", params.socket.display()))?;
    stream
        .shutdown()
        .await
        .context("failed to shutdown health check stream")?;
    Ok(())
}

/// Start the mux daemon with resolved parameters.
pub async fn run_mux(params: ResolvedParams) -> Result<()> {
    let service_name = Arc::new(params.service_name.clone());
    let socket_path = params.socket.clone();
    let cmd = params.cmd.clone();
    let args = params.args.clone();
    let max_clients = params.max_clients;
    let tray_enabled = params.tray_enabled;
    let lazy_start = params.lazy_start;
    let max_request_bytes = params.max_request_bytes;
    let request_timeout = params.request_timeout;
    let restart_backoff = params.restart_backoff;
    let restart_backoff_max = params.restart_backoff_max;
    let max_restarts = params.max_restarts;

    if let Some(parent) = socket_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .context("failed to create socket parent dir")?;
    }
    let _ = tokio::fs::remove_file(&socket_path).await;

    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("failed to bind socket {}", socket_path.display()))?;
    info!("rmcp_mux listening on {}", socket_path.display());

    let shutdown = CancellationToken::new();
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown_signal.cancel();
    });

    let state = Arc::new(Mutex::new(MuxState::new(
        max_clients,
        service_name.as_ref().clone(),
        max_request_bytes,
        request_timeout,
        restart_backoff,
        restart_backoff_max,
        max_restarts,
        0,
        None,
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

    let _status_file_handle: Option<tokio::task::JoinHandle<()>> = params
        .status_file
        .clone()
        .map(|path| spawn_status_writer(status_rx.clone(), path));

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
    let to_server_tx_for_server = to_server_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = server_manager(
            cmd.clone(),
            args.clone(),
            to_server_rx,
            to_server_tx_for_server,
            server_events_tx,
            server_state,
            server_active,
            status_for_server,
            server_shutdown,
            lazy_start,
            restart_backoff,
            restart_backoff_max,
            max_restarts,
        )
        .await
        {
            error!("server manager exited with error: {e}");
        }
    });

    // Timeout reaper
    let reaper_state = state.clone();
    let reaper_active = active_clients.clone();
    let reaper_status = status_tx.clone();
    let reaper_shutdown = shutdown.clone();
    tokio::spawn(async move {
        reap_timeouts(reaper_state, reaper_active, reaper_status, reaper_shutdown).await;
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
                let max_request_bytes = {
                    let st = state.lock().await;
                    st.max_request_bytes
                };
                if let Err(e) = handle_client_message(
                    client_id,
                    msg,
                    &state,
                    &to_server_tx,
                    &active_clients,
                    &status_tx,
                    max_request_bytes,
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
    max_request_bytes: usize,
) -> Result<()> {
    let encoded_len = serde_json::to_vec(&msg)?.len();
    if encoded_len > max_request_bytes {
        let st = state.lock().await;
        if let Some(tx) = st.clients.get(&client_id) {
            tx.send(error_response(
                msg.get("id").cloned().unwrap_or(Value::Null),
                "request too large",
            ))
            .ok();
        }
        return Ok(());
    }
    // Notifications (no id) are forwarded best-effort; if the queue is full we drop with a warning.
    if msg.get("id").is_none() {
        if let Err(e) = to_server_tx.try_send(msg) {
            warn!("dropping notification from client {client_id}: {e}");
        }
        update_queue_depth(state, to_server_tx).await;
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
            st.init_waiting.push((client_id, local_id));
            return Ok(());
        }

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
                started_at: std::time::Instant::now(),
            },
        );
        drop(st);
        set_id(&mut msg, Value::String(global_id));
        to_server_tx
            .send(msg)
            .await
            .map_err(|_| anyhow!("server channel closed"))?;
        update_queue_depth(state, to_server_tx).await;
        publish_status(state, active_clients, status_tx).await;
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
                started_at: std::time::Instant::now(),
            },
        );
        gid
    };

    set_id(&mut msg, Value::String(global_id));
    to_server_tx
        .send(msg)
        .await
        .map_err(|_| anyhow!("server channel closed"))?;
    update_queue_depth(state, to_server_tx).await;
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

async fn reap_timeouts(
    state: Arc<Mutex<MuxState>>,
    active_clients: Arc<Semaphore>,
    status_tx: watch::Sender<StatusSnapshot>,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = ticker.tick() => {
                let mut expired = Vec::new();
                let timeout = {
                    let st = state.lock().await;
                    st.request_timeout
                };
                {
                    let mut st = state.lock().await;
                    let now = Instant::now();
                    st.pending.retain(|_, p| {
                        if now.duration_since(p.started_at) > timeout {
                            expired.push((p.client_id, p.local_id.clone()));
                            false
                        } else {
                            true
                        }
                    });
                }
                if !expired.is_empty() {
                    let st = state.lock().await;
                    for (cid, lid) in expired {
                        if let Some(tx) = st.clients.get(&cid) {
                            tx.send(error_response(lid, "request timeout")).ok();
                        }
                    }
                }
                publish_status(&state, &active_clients, &status_tx).await;
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
    to_server_meter: mpsc::Sender<Value>,
    server_events_tx: mpsc::UnboundedSender<ServerEvent>,
    state: Arc<Mutex<MuxState>>,
    active_clients: Arc<Semaphore>,
    status_tx: watch::Sender<StatusSnapshot>,
    shutdown: CancellationToken,
    lazy_start: bool,
    restart_backoff: Duration,
    restart_backoff_max: Duration,
    max_restarts: u64,
) -> Result<()> {
    let mut backoff = restart_backoff;
    let mut restarts = 0u64;

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        let mut first_msg: Option<Value> = None;
        if lazy_start && restarts == 0 {
            info!("lazy start enabled; waiting for first client message");
            let first = tokio::select! {
                _ = shutdown.cancelled() => None,
                msg = to_server_rx.recv() => msg,
            };
            if shutdown.is_cancelled() {
                break;
            }
            if let Some(msg) = first {
                first_msg = Some(msg);
                update_queue_depth(&state, &to_server_meter).await;
            } else {
                break;
            }
        }

        if max_restarts > 0 && restarts >= max_restarts {
            let mut st = state.lock().await;
            st.server_status = ServerStatus::Failed("max restarts reached".into());
            st.last_reset = Some("max restarts reached".into());
            st.child_pid = None;
            publish_status(&state, &active_clients, &status_tx).await;
            break;
        }

        info!("starting MCP server: {} {:?}", cmd, args);
        let mut child = TokioCommand::new(&cmd)
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .context("failed to spawn MCP server")?;

        {
            let mut st = state.lock().await;
            st.server_status = ServerStatus::Running;
            st.child_pid = child.id();
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

        let server_events_tx_clone = server_events_tx.clone();
        let mut child_wait = tokio::spawn(async move { child.wait().await });

        // write loop and monitor
        let mut should_restart = true;
        if let Some(msg) = first_msg.take() {
            if let Err(e) = writer.send(msg).await {
                warn!("write to server failed on first message: {e}");
            }
        }
        while !shutdown.is_cancelled() {
            tokio::select! {
                maybe_msg = to_server_rx.recv() => {
                    let Some(msg) = maybe_msg else {
                        update_queue_depth(&state, &to_server_meter).await;
                        should_restart = false;
                        break;
                    };
                    update_queue_depth(&state, &to_server_meter).await;
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
                status = &mut child_wait => {
                    match status {
                        Ok(Ok(status)) => warn!("server exited with status {status}"),
                        Ok(Err(e)) => {
                            warn!("server wait error: {e}");
                            let mut st = state.lock().await;
                            st.server_status = ServerStatus::Failed(e.to_string());
                            st.last_reset = Some("wait error".into());
                        }
                        Err(join_err) => {
                            warn!("server wait join error: {join_err}");
                        }
                    }
                    publish_status(&state, &active_clients, &status_tx).await;
                    break;
                }
                _ = shutdown.cancelled() => { break; }
            }
        }

        // child cleanup
        child_wait.abort();
        reader_task.abort();

        // reset stanu
        server_events_tx_clone
            .send(ServerEvent::Reset("MCP server restarted".into()))
            .ok();
        {
            let mut st = state.lock().await;
            st.cached_initialize = None;
            st.initializing = false;
            st.child_pid = None;
            if shutdown.is_cancelled() || !should_restart {
                st.server_status = ServerStatus::Stopped;
            } else {
                st.server_status = ServerStatus::Restarting;
                st.restarts = st.restarts.saturating_add(1);
            }
        }
        update_queue_depth(&state, &to_server_meter).await;
        publish_status(&state, &active_clients, &status_tx).await;

        if shutdown.is_cancelled() || !should_restart {
            break;
        }
        restarts = restarts.saturating_add(1);
        info!("restarting MCP server after failure, backoff {:?}", backoff);
        sleep(backoff).await;
        backoff = (backoff * 2).min(restart_backoff_max);
    }
    Ok(())
}

pub async fn run_proxy(socket: PathBuf) -> Result<()> {
    let stream = UnixStream::connect(&socket)
        .await
        .with_context(|| format!("failed to connect to {}", socket.display()))?;
    let (sr, sw) = stream.into_split();
    let mut sock_reader = FramedRead::new(sr, JsonRpcMessageCodec::<Value>::new());
    let mut sock_writer = FramedWrite::new(sw, JsonRpcMessageCodec::<Value>::new());
    let mut stdin_reader = FramedRead::new(stdin(), JsonRpcMessageCodec::<Value>::new());
    let mut stdout_writer = FramedWrite::new(stdout(), JsonRpcMessageCodec::<Value>::new());

    let to_socket = async {
        while let Some(msg) = stdin_reader.next().await {
            let msg = match msg {
                Ok(v) => v,
                Err(e) => {
                    warn!("stdin decode error: {e}");
                    break;
                }
            };
            if let Err(e) = sock_writer.send(msg).await {
                warn!("socket write error: {e}");
                break;
            }
        }
    };

    let to_stdout = async {
        while let Some(msg) = sock_reader.next().await {
            let msg = match msg {
                Ok(v) => v,
                Err(e) => {
                    warn!("socket decode error: {e}");
                    break;
                }
            };
            if let Err(e) = stdout_writer.send(msg).await {
                warn!("stdout write error: {e}");
                break;
            }
        }
    };

    tokio::select! {
        _ = to_socket => {},
        _ = to_stdout => {},
    }

    Ok(())
}

async fn update_queue_depth(state: &Arc<Mutex<MuxState>>, to_server_tx: &mpsc::Sender<Value>) {
    let depth = MAX_QUEUE.saturating_sub(to_server_tx.capacity());
    let mut st = state.lock().await;
    st.queue_depth = depth;
}

async fn write_status_file(path: &Path, snapshot: &StatusSnapshot) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create status dir {}", parent.display()))?;
    }
    let tmp = path.with_extension("tmp");
    let data = serde_json::to_vec_pretty(snapshot)?;
    fs::write(&tmp, data)
        .await
        .with_context(|| format!("failed to write status tmp {}", tmp.display()))?;
    fs::rename(&tmp, path)
        .await
        .with_context(|| format!("failed to atomically replace status {}", path.display()))?;
    Ok(())
}

fn spawn_status_writer(
    mut rx: watch::Receiver<StatusSnapshot>,
    path: PathBuf,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // write initial snapshot
        let mut current = rx.borrow().clone();
        if let Err(e) = write_status_file(&path, &current).await {
            warn!("failed to write initial status file: {e}");
        }
        while rx.changed().await.is_ok() {
            current = rx.borrow().clone();
            if let Err(e) = write_status_file(&path, &current).await {
                warn!("failed to write status file: {e}");
            }
        }
    })
}

enum ServerEvent {
    Message(Value),
    Reset(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{expand_path, load_config, Config, ServerConfig};
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::tempdir;
    use tokio::net::UnixListener;
    use tokio::sync::mpsc::UnboundedReceiver;
    use tokio::sync::mpsc::{self};

    fn test_state_with_max(max: usize) -> Arc<Mutex<MuxState>> {
        Arc::new(Mutex::new(MuxState::new(
            max,
            "test".into(),
            1_048_576,
            Duration::from_secs(30),
            Duration::from_secs(1),
            Duration::from_secs(30),
            5,
            0,
            None,
        )))
    }

    fn test_state() -> Arc<Mutex<MuxState>> {
        test_state_with_max(5)
    }

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

    fn params_with_socket(socket: PathBuf) -> ResolvedParams {
        ResolvedParams {
            socket,
            cmd: "echo".into(),
            args: vec![],
            max_clients: 5,
            tray_enabled: false,
            log_level: "info".into(),
            service_name: "test".into(),
            lazy_start: false,
            max_request_bytes: 1_048_576,
            request_timeout: Duration::from_secs(30),
            restart_backoff: Duration::from_secs(1),
            restart_backoff_max: Duration::from_secs(30),
            max_restarts: 3,
            status_file: None,
        }
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
        let state = test_state();
        let active_clients = Arc::new(Semaphore::new(5));
        let (status_tx, _status_rx) = {
            let st = state.lock().await;
            watch::channel(snapshot_for_state(&st, 0))
        };
        let mut st = state.lock().await;
        let (cid1, mut rx1) = capture_client(&mut st);
        let (cid2, mut rx2) = capture_client(&mut st);

        st.pending.insert(
            "g1".into(),
            Pending {
                client_id: cid1,
                local_id: Value::String("loc1".into()),
                is_initialize: true,
                started_at: Instant::now(),
            },
        );
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

        let m1 = rx1.recv().await.expect("client1 message");
        assert_eq!(m1.get("id"), Some(&Value::String("loc1".into())));

        let m2 = rx2.recv().await.expect("client2 message");
        assert_eq!(m2.get("id"), Some(&Value::String("loc2".into())));

        let st = state.lock().await;
        assert!(st.cached_initialize.is_some());
        assert!(!st.initializing);
        assert!(st.init_waiting.is_empty());
    }

    #[tokio::test]
    async fn non_initialize_response_routed_without_caching() {
        let state = test_state();
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
                started_at: Instant::now(),
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
        let state = test_state();
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
                started_at: Instant::now(),
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
        fs::create_dir_all(&base).expect("create base dir");

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
                    lazy_start: Some(false),
                    max_request_bytes: Some(1_048_576),
                    request_timeout_ms: Some(30_000),
                    restart_backoff_ms: Some(1_000),
                    restart_backoff_max_ms: Some(30_000),
                    max_restarts: Some(5),
                    status_file: None,
                },
            )]),
        };

        let cli = crate::Cli {
            socket: None,
            cmd: None,
            args: vec![],
            max_active_clients: 5,
            lazy_start: None,
            max_request_bytes: None,
            request_timeout_ms: None,
            restart_backoff_ms: None,
            restart_backoff_max_ms: None,
            max_restarts: None,
            log_level: "info".into(),
            tray: false,
            service_name: None,
            config: None,
            service: Some("svc".into()),
            status_file: None,
        };

        let params =
            crate::config::resolve_params(&cli, Some(&cfg)).expect("resolve params from config");
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
        let cli = crate::Cli {
            socket: None,
            cmd: None,
            args: vec![],
            max_active_clients: 5,
            lazy_start: None,
            max_request_bytes: None,
            request_timeout_ms: None,
            restart_backoff_ms: None,
            restart_backoff_max_ms: None,
            max_restarts: None,
            log_level: "info".into(),
            tray: false,
            service_name: None,
            config: None,
            service: None,
            status_file: None,
        };
        let err = crate::config::resolve_params(&cli, Some(&cfg)).unwrap_err();
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
                    lazy_start: Some(false),
                    max_request_bytes: Some(1_048_576),
                    request_timeout_ms: Some(30_000),
                    restart_backoff_ms: Some(1_000),
                    restart_backoff_max_ms: Some(30_000),
                    max_restarts: Some(5),
                    status_file: None,
                },
            )]),
        };
        let cli = crate::Cli {
            socket: Some(PathBuf::from("/tmp/cli.sock")),
            cmd: Some("node".into()),
            args: vec!["srv".into()],
            max_active_clients: 1,
            lazy_start: None,
            max_request_bytes: None,
            request_timeout_ms: None,
            restart_backoff_ms: None,
            restart_backoff_max_ms: None,
            max_restarts: None,
            log_level: "warn".into(),
            tray: true,
            service_name: Some("cli".into()),
            config: None,
            service: Some("svc".into()),
            status_file: None,
        };
        let params =
            crate::config::resolve_params(&cli, Some(&cfg)).expect("resolve params cli overrides");
        assert_eq!(params.socket, PathBuf::from("/tmp/cli.sock"));
        assert_eq!(params.cmd, "node");
        assert_eq!(params.args, vec!["srv".to_string()]);
        assert_eq!(params.max_clients, 2);
        assert!(params.tray_enabled);
        assert_eq!(params.service_name, "cli");
        assert_eq!(params.log_level, "info");
    }

    #[test]
    fn resolve_params_applies_defaults_without_config() {
        let cli = crate::Cli {
            socket: Some(PathBuf::from("/tmp/simple.sock")),
            cmd: Some("npx".into()),
            args: vec!["@mcp/server".into()],
            max_active_clients: 5,
            lazy_start: None,
            max_request_bytes: None,
            request_timeout_ms: None,
            restart_backoff_ms: None,
            restart_backoff_max_ms: None,
            max_restarts: None,
            log_level: "info".into(),
            tray: false,
            service_name: None,
            config: None,
            service: None,
            status_file: None,
        };

        let params = crate::config::resolve_params(&cli, None).expect("resolve params defaults");
        assert_eq!(params.socket, PathBuf::from("/tmp/simple.sock"));
        assert_eq!(params.cmd, "npx");
        assert_eq!(params.args, vec!["@mcp/server".to_string()]);
        assert_eq!(params.max_clients, 5);
        assert!(!params.lazy_start);
        assert_eq!(params.max_request_bytes, 1_048_576);
        assert_eq!(params.request_timeout.as_millis(), 30_000);
        assert_eq!(params.restart_backoff.as_millis(), 1_000);
        assert_eq!(params.restart_backoff_max.as_millis(), 30_000);
        assert_eq!(params.max_restarts, 5);
        assert_eq!(params.service_name, "simple");
    }

    #[test]
    fn resolve_params_prefers_cli_over_config_for_timeouts() {
        let cfg = Config {
            servers: HashMap::from([(
                "svc".into(),
                ServerConfig {
                    socket: Some("/tmp/cfg.sock".into()),
                    cmd: Some("node".into()),
                    args: Some(vec!["svc.js".into()]),
                    max_active_clients: Some(3),
                    tray: Some(false),
                    service_name: Some("cfg-name".into()),
                    log_level: Some("debug".into()),
                    lazy_start: Some(true),
                    max_request_bytes: Some(2_000_000),
                    request_timeout_ms: Some(10_000),
                    restart_backoff_ms: Some(5_000),
                    restart_backoff_max_ms: Some(60_000),
                    max_restarts: Some(9),
                    status_file: Some("/tmp/status.json".into()),
                },
            )]),
        };

        let cli = crate::Cli {
            socket: None,
            cmd: None,
            args: vec![],
            max_active_clients: 4,
            lazy_start: None,
            max_request_bytes: None,
            request_timeout_ms: Some(2_000),
            restart_backoff_ms: Some(1_500),
            restart_backoff_max_ms: None,
            max_restarts: Some(3),
            log_level: "info".into(),
            tray: false,
            service_name: None,
            config: None,
            service: Some("svc".into()),
            status_file: None,
        };

        let params =
            crate::config::resolve_params(&cli, Some(&cfg)).expect("resolve params precedence");
        // CLI overrides
        assert_eq!(params.request_timeout.as_millis(), 2_000);
        assert_eq!(params.restart_backoff.as_millis(), 1_500);
        assert_eq!(params.max_restarts, 3);
        // Config values used when CLI omitted
        assert_eq!(params.restart_backoff_max.as_millis(), 60_000);
        assert_eq!(params.max_request_bytes, 2_000_000);
        assert!(params.lazy_start);
        assert_eq!(params.status_file, Some(PathBuf::from("/tmp/status.json")));
    }

    #[test]
    fn resolve_params_errors_when_socket_missing() {
        let cli = crate::Cli {
            socket: None,
            cmd: Some("npx".into()),
            args: vec![],
            max_active_clients: 5,
            lazy_start: None,
            max_request_bytes: None,
            request_timeout_ms: None,
            restart_backoff_ms: None,
            restart_backoff_max_ms: None,
            max_restarts: None,
            log_level: "info".into(),
            tray: false,
            service_name: None,
            config: None,
            service: None,
            status_file: None,
        };
        let err = crate::config::resolve_params(&cli, None).unwrap_err();
        assert!(err.to_string().contains("socket"));
    }

    #[test]
    fn resolve_params_errors_when_cmd_missing() {
        let cli = crate::Cli {
            socket: Some(PathBuf::from("/tmp/sock.sock")),
            cmd: None,
            args: vec![],
            max_active_clients: 5,
            lazy_start: None,
            max_request_bytes: None,
            request_timeout_ms: None,
            restart_backoff_ms: None,
            restart_backoff_max_ms: None,
            max_restarts: None,
            log_level: "info".into(),
            tray: false,
            service_name: None,
            config: None,
            service: None,
            status_file: None,
        };
        let err = crate::config::resolve_params(&cli, None).unwrap_err();
        assert!(err.to_string().contains("cmd"));
    }

    #[test]
    fn resolve_params_errors_when_service_missing_in_config() {
        let cfg = Config {
            servers: HashMap::new(),
        };
        let cli = crate::Cli {
            socket: None,
            cmd: None,
            args: vec![],
            max_active_clients: 5,
            lazy_start: None,
            max_request_bytes: None,
            request_timeout_ms: None,
            restart_backoff_ms: None,
            restart_backoff_max_ms: None,
            max_restarts: None,
            log_level: "info".into(),
            tray: false,
            service_name: None,
            config: None,
            service: Some("missing".into()),
            status_file: None,
        };
        let err = crate::config::resolve_params(&cli, Some(&cfg)).unwrap_err();
        assert!(err
            .to_string()
            .contains("service 'missing' not found in config"));
    }

    #[tokio::test]
    async fn publish_status_counts_active() {
        let state = test_state_with_max(3);
        let active = Arc::new(Semaphore::new(3));
        let (tx, rx) = {
            let st = state.lock().await;
            watch::channel(snapshot_for_state(&st, 0))
        };

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
        let state = test_state_with_max(2);
        let active = Arc::new(Semaphore::new(2));
        let (status_tx, mut status_rx) = {
            let st = state.lock().await;
            watch::channel(snapshot_for_state(&st, 0))
        };

        let _ = status_rx.borrow().clone();

        reset_state(&state, "restart-test", &active, &status_tx).await;

        status_rx.changed().await.expect("status update");
        let snap = status_rx.borrow().clone();
        assert_eq!(snap.last_reset.as_deref(), Some("restart-test"));
        assert!(!snap.initializing);
        assert_eq!(snap.pending_requests, 0);
    }

    #[tokio::test]
    async fn initialize_served_from_cache_does_not_queue() {
        let state = test_state();
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

        let max_req = { state.lock().await.max_request_bytes };
        handle_client_message(
            cid,
            msg,
            &state,
            &to_server_tx,
            &active,
            &status_tx,
            max_req,
        )
        .await
        .expect("handle cached init");

        assert!(to_server_rx.try_recv().is_err());

        let resp = rx.recv().await.expect("cached init response");
        assert_eq!(resp.get("id"), Some(&Value::String("client-init".into())));

        let st = state.lock().await;
        assert!(st.cached_initialize.is_some());
        assert!(!st.initializing);
        assert!(st.pending.is_empty());
    }

    #[tokio::test]
    async fn reset_state_clears_initialize_and_pending() {
        let state = test_state();
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
                started_at: Instant::now(),
            },
        );
        st.init_waiting
            .push((cid, Value::String("waiter-id".into())));
        drop(st);

        reset_state(&state, "reset-reason", &active, &status_tx).await;

        let errs: Vec<_> = rx.recv().await.into_iter().collect();
        assert!(!errs.is_empty());

        let st = state.lock().await;
        assert!(st.pending.is_empty());
        assert!(st.init_waiting.is_empty());
        assert!(st.cached_initialize.is_none());
        assert!(!st.initializing);
        assert_eq!(st.last_reset.as_deref(), Some("reset-reason"));
    }

    #[tokio::test]
    async fn publish_status_includes_queue_and_pid() {
        let state = test_state();
        {
            let mut st = state.lock().await;
            st.queue_depth = 7;
            st.child_pid = Some(4242);
        }
        let active = Arc::new(Semaphore::new(5));
        let (tx, rx) = {
            let st = state.lock().await;
            watch::channel(snapshot_for_state(&st, 0))
        };
        publish_status(&state, &active, &tx).await;
        let snap = rx.borrow().clone();
        assert_eq!(snap.queue_depth, 7);
        assert_eq!(snap.child_pid, Some(4242));
        assert_eq!(snap.max_request_bytes, 1_048_576);
        assert_eq!(snap.restart_backoff_ms, 1_000);
        assert_eq!(snap.restart_backoff_max_ms, 30_000);
        assert_eq!(snap.max_restarts, 5);
    }

    #[tokio::test]
    async fn status_file_writer_persists_snapshot() {
        let dir = tempfile::tempdir().expect("tmp dir");
        let path = dir.path().join("status.json");
        let base = StatusSnapshot {
            service_name: "svc".into(),
            server_status: ServerStatus::Starting,
            restarts: 0,
            connected_clients: 0,
            active_clients: 0,
            max_active_clients: 5,
            pending_requests: 0,
            cached_initialize: false,
            initializing: false,
            last_reset: None,
            queue_depth: 0,
            child_pid: Some(99),
            max_request_bytes: 1_048_576,
            restart_backoff_ms: 1_000,
            restart_backoff_max_ms: 30_000,
            max_restarts: 5,
        };
        let (tx, rx) = watch::channel(base.clone());
        let handle = spawn_status_writer(rx, path.clone());

        let mut updated = base.clone();
        updated.queue_depth = 3;
        tx.send(updated.clone()).ok();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let text = fs::read_to_string(&path).expect("status file");
        assert!(text.contains("\"queue_depth\": 3"));
        assert!(text.contains("\"child_pid\": 99"));

        handle.abort();
    }

    #[tokio::test]
    async fn health_check_succeeds_when_socket_listens() {
        let dir = tempdir().expect("tempdir");
        let socket = dir.path().join("health.sock");
        let listener = UnixListener::bind(&socket).expect("bind listener");
        let accept = tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let params = params_with_socket(socket.clone());
        health_check(&params).await.expect("health ok");
        accept.abort();
    }

    #[tokio::test]
    async fn health_check_fails_for_missing_socket() {
        let dir = tempdir().expect("tempdir");
        let socket = dir.path().join("missing.sock");
        let params = params_with_socket(socket);
        let err = health_check(&params).await.expect_err("should fail");
        assert!(
            err.to_string().contains("failed to connect"),
            "unexpected error: {err}"
        );
    }
}
