use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, Semaphore, mpsc, watch};
use tokio_util::sync::CancellationToken;

/// Server startup mode.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum ServerMode {
    /// Start server immediately when mux starts.
    #[default]
    Eager,
    /// Start server only on first client connection.
    Lazy,
}

/// State for managing multiple mux servers in a single process.
pub struct MultiMuxState {
    /// Per-server state keyed by service name.
    pub servers: HashMap<String, Arc<Mutex<MuxState>>>,
    /// Global shutdown token.
    pub shutdown: CancellationToken,
    /// When the multi-mux runtime started.
    pub start_time: Instant,
}

impl MultiMuxState {
    pub fn new(shutdown: CancellationToken) -> Self {
        Self {
            servers: HashMap::new(),
            shutdown,
            start_time: Instant::now(),
        }
    }

    pub fn add_server(&mut self, name: String, state: Arc<Mutex<MuxState>>) {
        self.servers.insert(name, state);
    }

    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }
}

#[cfg_attr(not(feature = "tray"), allow(dead_code))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ServerStatus {
    Starting,
    Running,
    Restarting,
    Failed(String),
    Stopped,
    /// Server not yet started due to lazy_start=true
    Lazy,
    /// In exponential backoff after crash
    Backoff,
}

/// Health status for dashboard display.
#[cfg_attr(not(feature = "tray"), allow(dead_code))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum HealthStatus {
    Ok,
    Warn,
    Error,
    Lazy,
    Starting,
    Backoff,
}

/// Metrics collected by the heartbeat inspector.
///
/// These metrics are exposed in StatusSnapshot for monitoring and alerting.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HeartbeatMetrics {
    /// Timestamp of the last successful heartbeat (Unix millis).
    pub last_heartbeat_ms: Option<u64>,
    /// Average response time in milliseconds (rolling window).
    pub avg_response_ms: Option<u64>,
    /// Current consecutive failure count.
    pub consecutive_failures: u32,
    /// Total successful heartbeats since start.
    pub total_success: u64,
    /// Total failed heartbeats since start.
    pub total_failures: u64,
    /// Whether heartbeat monitoring is enabled.
    pub enabled: bool,
}

#[cfg_attr(not(feature = "tray"), allow(dead_code))]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatusSnapshot {
    pub service_name: String,
    pub server_status: ServerStatus,
    /// Computed health status for dashboard display
    pub health_status: HealthStatus,
    pub restarts: u64,
    pub connected_clients: usize,
    pub active_clients: usize,
    pub max_active_clients: usize,
    pub pending_requests: usize,
    pub cached_initialize: bool,
    pub initializing: bool,
    pub last_reset: Option<String>,
    pub queue_depth: usize,
    pub child_pid: Option<u32>,
    pub max_request_bytes: usize,
    pub restart_backoff_ms: u64,
    pub restart_backoff_max_ms: u64,
    pub max_restarts: u64,
    /// Heartbeat health metrics.
    pub heartbeat: HeartbeatMetrics,
    /// Server uptime since last start (milliseconds)
    pub uptime_ms: u64,
    /// Whether server is currently in exponential backoff
    pub in_backoff: bool,
    /// Current heartbeat latency in milliseconds
    pub heartbeat_latency_ms: Option<u64>,
}

/// Central runtime state shared between the async mux loops.
///
/// - `queue_depth` caps queued client messages to avoid unbounded memory growth
///   under bursty hosts.
/// - `max_request_bytes` and `request_timeout` are enforced per forwarded
///   request to prevent slowloris/DoS patterns.
/// - Restart backoff (`restart_backoff`..`restart_backoff_max`) and
///   `max_restarts` gate child respawns so a flapping server cannot burn CPU.
#[derive(Clone)]
pub struct MuxState {
    pub next_client_id: u64,
    pub next_global_id: u64,
    pub clients: HashMap<u64, mpsc::UnboundedSender<Value>>,
    pub pending: HashMap<String, Pending>,
    pub cached_initialize: Option<Value>,
    pub init_waiting: Vec<(u64, Value)>,
    pub initializing: bool,
    /// True after first notifications/initialized was forwarded to backend server.
    /// Subsequent clients' initialized notifications should NOT be forwarded.
    pub server_initialized: bool,
    pub server_status: ServerStatus,
    pub restarts: u64,
    pub last_reset: Option<String>,
    pub max_active_clients: usize,
    pub service_name: String,
    pub max_request_bytes: usize,
    pub request_timeout: Duration,
    pub restart_backoff: Duration,
    pub restart_backoff_max: Duration,
    pub max_restarts: u64,
    pub queue_depth: usize,
    pub child_pid: Option<u32>,
    /// Per-client handshake state for MCP protocol tolerance.
    /// Tracks handshake progress to buffer out-of-order messages.
    pub client_handshakes: HashMap<u64, ClientHandshakeState>,
    /// Heartbeat health metrics for backend monitoring.
    pub heartbeat_metrics: HeartbeatMetrics,
    /// When the server was last started (for uptime calculation)
    pub started_at: Option<Instant>,
    /// Whether we're currently in backoff waiting before restart
    pub in_backoff: bool,
}

#[derive(Clone, Debug)]
pub struct Pending {
    pub client_id: u64,
    pub local_id: Value,
    pub is_initialize: bool,
    pub started_at: std::time::Instant,
}

/// Handshake state for a single client connection.
///
/// Tracks MCP protocol handshake progress to ensure proper message ordering.
/// Claude Code and other clients sometimes send `tools/list` before completing
/// the initialize handshake, which crashes rmcp-based backends.
#[derive(Clone, Debug)]
pub struct ClientHandshakeState {
    /// Whether the client has completed the full handshake sequence.
    pub handshake_complete: bool,
    /// Whether we're waiting for initialize response from server.
    pub initialize_pending: bool,
    /// Messages buffered while waiting for handshake to complete.
    pub buffered_messages: Vec<Value>,
    /// When the handshake started (for timeout tracking).
    pub started_at: Instant,
}

impl ClientHandshakeState {
    pub fn new() -> Self {
        Self {
            handshake_complete: false,
            initialize_pending: false,
            buffered_messages: Vec::new(),
            started_at: Instant::now(),
        }
    }
}

impl Default for ClientHandshakeState {
    fn default() -> Self {
        Self::new()
    }
}

/// Handshake timeout duration (10 seconds).
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

impl MuxState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        max_active_clients: usize,
        service_name: String,
        max_request_bytes: usize,
        request_timeout: Duration,
        restart_backoff: Duration,
        restart_backoff_max: Duration,
        max_restarts: u64,
        queue_depth: usize,
        child_pid: Option<u32>,
    ) -> Self {
        Self {
            next_client_id: 1,
            next_global_id: 1,
            clients: HashMap::new(),
            pending: HashMap::new(),
            cached_initialize: None,
            init_waiting: Vec::new(),
            initializing: false,
            server_initialized: false,
            server_status: ServerStatus::Starting,
            restarts: 0,
            last_reset: None,
            max_active_clients,
            service_name,
            max_request_bytes,
            request_timeout,
            restart_backoff,
            restart_backoff_max,
            max_restarts,
            queue_depth,
            child_pid,
            client_handshakes: HashMap::new(),
            heartbeat_metrics: HeartbeatMetrics::default(),
            started_at: None,
            in_backoff: false,
        }
    }

    pub fn register_client(&mut self, tx: mpsc::UnboundedSender<Value>) -> u64 {
        let id = self.next_client_id;
        self.next_client_id += 1;
        self.clients.insert(id, tx);
        self.client_handshakes
            .insert(id, ClientHandshakeState::new());
        id
    }

    pub fn unregister_client(&mut self, client_id: u64) {
        self.clients.remove(&client_id);
        self.client_handshakes.remove(&client_id);
        self.pending.retain(|_, p| p.client_id != client_id);
        self.init_waiting.retain(|(cid, _)| *cid != client_id);
    }

    /// Get mutable reference to client handshake state.
    pub fn get_handshake_mut(&mut self, client_id: u64) -> Option<&mut ClientHandshakeState> {
        self.client_handshakes.get_mut(&client_id)
    }

    /// Check if client handshake is complete.
    pub fn is_handshake_complete(&self, client_id: u64) -> bool {
        self.client_handshakes
            .get(&client_id)
            .map(|h| h.handshake_complete)
            .unwrap_or(false)
    }

    /// Mark client handshake as complete and return buffered messages.
    pub fn complete_handshake(&mut self, client_id: u64) -> Vec<Value> {
        if let Some(h) = self.client_handshakes.get_mut(&client_id) {
            h.handshake_complete = true;
            h.initialize_pending = false;
            std::mem::take(&mut h.buffered_messages)
        } else {
            Vec::new()
        }
    }

    /// Buffer a message for a client that hasn't completed handshake.
    pub fn buffer_message(&mut self, client_id: u64, msg: Value) -> bool {
        if let Some(h) = self.client_handshakes.get_mut(&client_id) {
            h.buffered_messages.push(msg);
            true
        } else {
            false
        }
    }

    /// Check if client handshake has timed out.
    pub fn is_handshake_timed_out(&self, client_id: u64) -> bool {
        self.client_handshakes
            .get(&client_id)
            .map(|h| !h.handshake_complete && h.started_at.elapsed() > HANDSHAKE_TIMEOUT)
            .unwrap_or(false)
    }

    pub fn next_request_id(&mut self) -> u64 {
        let id = self.next_global_id;
        self.next_global_id += 1;
        id
    }
}

pub fn set_id(msg: &mut Value, id: Value) {
    if let Some(obj) = msg.as_object_mut() {
        obj.insert("id".to_string(), id);
    }
}

pub fn error_response(id: Value, message: &str) -> Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": -32000,
            "message": message,
        }
    })
}

pub fn snapshot_for_state(st: &MuxState, active_clients: usize) -> StatusSnapshot {
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
        queue_depth: st.queue_depth,
        child_pid: st.child_pid,
        max_request_bytes: st.max_request_bytes,
        restart_backoff_ms: st.restart_backoff.as_millis() as u64,
        restart_backoff_max_ms: st.restart_backoff_max.as_millis() as u64,
        max_restarts: st.max_restarts,
        heartbeat: st.heartbeat_metrics.clone(),
        health_status: compute_health_status(st, active_clients),
        uptime_ms: st
            .started_at
            .map(|t| t.elapsed().as_millis() as u64)
            .unwrap_or(0),
        in_backoff: st.in_backoff,
        heartbeat_latency_ms: st.heartbeat_metrics.avg_response_ms,
    }
}

pub async fn publish_status(
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

pub async fn reset_state(
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
    st.server_initialized = false;
    st.last_reset = Some(reason.to_string());
    st.queue_depth = 0;
    st.child_pid = None;

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

/// Compute health status from MuxState.
fn compute_health_status(st: &MuxState, active_clients: usize) -> HealthStatus {
    match &st.server_status {
        ServerStatus::Running => {
            // Explicit guard-then-divide reads clearer than checked_div here:
            // we want a plain 0 default, not an Option. Behaviour unchanged.
            #[allow(clippy::manual_checked_ops)]
            let load_pct = if st.max_active_clients > 0 {
                (active_clients * 100) / st.max_active_clients
            } else {
                0
            };
            if st.heartbeat_metrics.consecutive_failures > 0 {
                HealthStatus::Error
            } else if load_pct > 80 || st.restarts > 0 {
                HealthStatus::Warn
            } else {
                HealthStatus::Ok
            }
        }
        ServerStatus::Starting => HealthStatus::Starting,
        ServerStatus::Restarting => HealthStatus::Warn,
        ServerStatus::Failed(_) => HealthStatus::Error,
        ServerStatus::Stopped | ServerStatus::Lazy => HealthStatus::Lazy,
        ServerStatus::Backoff => HealthStatus::Backoff,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_request_id_increments_sequentially() {
        let mut state = MuxState::new(
            5,
            "test-service".into(),
            1_048_576,
            Duration::from_secs(30),
            Duration::from_millis(1_000),
            Duration::from_millis(30_000),
            5,
            0,
            None,
        );

        let first = state.next_request_id();
        let second = state.next_request_id();

        assert_eq!(first + 1, second);
    }

    #[test]
    fn error_response_uses_jsonrpc_2_0() {
        let resp = error_response(Value::Number(1.into()), "oops");
        assert_eq!(resp.get("jsonrpc"), Some(&Value::String("2.0".into())));
    }
}
