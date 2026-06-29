//! Heartbeat inspector for MCP backend health monitoring.
//!
//! This module provides proactive health monitoring of the backend MCP server.
//! It periodically sends lightweight ping probes and tracks response times,
//! triggering server restarts when the backend becomes unresponsive.
//!
//! Created by Vetcoders (c)2025

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Mutex, Semaphore, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::state::{HeartbeatMetrics, MuxState, StatusSnapshot, publish_status};

/// Maximum number of response time samples to keep for averaging.
const RESPONSE_TIME_SAMPLES: usize = 10;

/// Heartbeat configuration parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatConfig {
    /// Interval between heartbeat probes (default: 30s).
    pub interval: Duration,
    /// Timeout waiting for heartbeat response (default: 30s).
    pub timeout: Duration,
    /// Number of consecutive failures before triggering restart.
    pub max_failures: u32,
    /// Whether heartbeat is enabled.
    pub enabled: bool,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(30),
            max_failures: 3,
            enabled: true,
        }
    }
}

impl HeartbeatConfig {
    /// Create a new heartbeat config with specified interval and timeout.
    pub fn new(interval: Duration, timeout: Duration) -> Self {
        Self {
            interval,
            timeout,
            max_failures: 3,
            enabled: true,
        }
    }

    /// Disable heartbeat monitoring.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }
}

/// Internal state for the heartbeat inspector.
struct HeartbeatState {
    /// Rolling window of response times for averaging.
    response_times: Vec<u64>,
    /// Current metrics.
    metrics: HeartbeatMetrics,
    /// Configuration.
    config: HeartbeatConfig,
    /// Counter for generating unique heartbeat IDs.
    next_heartbeat_id: u64,
    /// Pending heartbeat responses (id -> (started_at, oneshot sender)).
    pending: std::collections::HashMap<String, (Instant, oneshot::Sender<()>)>,
}

impl HeartbeatState {
    fn new(config: HeartbeatConfig) -> Self {
        Self {
            response_times: Vec::with_capacity(RESPONSE_TIME_SAMPLES),
            metrics: HeartbeatMetrics {
                enabled: config.enabled,
                ..Default::default()
            },
            config,
            next_heartbeat_id: 1,
            pending: std::collections::HashMap::new(),
        }
    }

    fn next_id(&mut self) -> String {
        let id = self.next_heartbeat_id;
        self.next_heartbeat_id += 1;
        format!("__heartbeat_{}", id)
    }

    fn record_success(&mut self, response_time_ms: u64) {
        // Update rolling average
        if self.response_times.len() >= RESPONSE_TIME_SAMPLES {
            self.response_times.remove(0);
        }
        self.response_times.push(response_time_ms);

        // Calculate average
        let avg = self.response_times.iter().sum::<u64>() / self.response_times.len() as u64;

        self.metrics.last_heartbeat_ms = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        );
        self.metrics.avg_response_ms = Some(avg);
        self.metrics.consecutive_failures = 0;
        self.metrics.total_success += 1;
    }

    fn record_failure(&mut self) {
        self.metrics.consecutive_failures += 1;
        self.metrics.total_failures += 1;
    }

    fn should_restart(&self) -> bool {
        self.metrics.consecutive_failures >= self.config.max_failures
    }

    fn get_metrics(&self) -> HeartbeatMetrics {
        self.metrics.clone()
    }
}

/// Event for signaling heartbeat responses from the server router.
pub enum HeartbeatEvent {
    /// A heartbeat response was received.
    Response { id: String },
}

/// Spawn the heartbeat inspector task.
///
/// This task periodically sends ping probes to the backend server and monitors
/// response times. If the server becomes unresponsive (no response within timeout
/// for `max_failures` consecutive probes), it signals for a server restart.
///
/// # Arguments
/// * `config` - Heartbeat configuration parameters
/// * `to_server_tx` - Channel to send requests to the backend server
/// * `heartbeat_rx` - Channel to receive heartbeat response notifications
/// * `state` - Shared mux state
/// * `active_clients` - Active client semaphore for status updates
/// * `status_tx` - Status update channel
/// * `restart_tx` - Channel to signal server restart
/// * `shutdown` - Cancellation token for graceful shutdown
#[allow(clippy::too_many_arguments)]
pub fn spawn_heartbeat_inspector(
    config: HeartbeatConfig,
    to_server_tx: mpsc::Sender<Value>,
    mut heartbeat_rx: mpsc::UnboundedReceiver<HeartbeatEvent>,
    state: Arc<Mutex<MuxState>>,
    active_clients: Arc<Semaphore>,
    status_tx: watch::Sender<StatusSnapshot>,
    restart_tx: mpsc::UnboundedSender<String>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if !config.enabled {
            debug!("heartbeat inspector disabled");
            return;
        }

        info!(
            "heartbeat inspector started: interval={:?}, timeout={:?}, max_failures={}",
            config.interval, config.timeout, config.max_failures
        );

        let mut hb_state = HeartbeatState::new(config.clone());
        let mut interval = tokio::time::interval(config.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    debug!("heartbeat inspector shutting down");
                    break;
                }

                _ = interval.tick() => {
                    // Check if server is running AND initialized before sending heartbeat
                    // The backend MCP server requires proper handshake (initialize + initialized)
                    // before accepting any requests including ping
                    {
                        let st = state.lock().await;
                        match &st.server_status {
                            crate::state::ServerStatus::Running => {}
                            _ => {
                                debug!("skipping heartbeat - server not running");
                                continue;
                            }
                        }
                        // Wait for at least one client to complete FULL handshake with backend
                        // MCP servers require initialize + notifications/initialized before accepting ping
                        if !st.server_initialized {
                            debug!("skipping heartbeat - server not fully initialized (no client completed handshake yet)");
                            continue;
                        }
                    }

                    let hb_id = hb_state.next_id();
                    let probe = json!({
                        "jsonrpc": "2.0",
                        "id": hb_id.clone(),
                        "method": "ping",
                        "params": {}
                    });

                    let started_at = Instant::now();
                    let (response_tx, response_rx) = oneshot::channel();
                    hb_state.pending.insert(hb_id.clone(), (started_at, response_tx));

                    // Send probe to server
                    if let Err(e) = to_server_tx.send(probe).await {
                        warn!("failed to send heartbeat probe: {}", e);
                        hb_state.pending.remove(&hb_id);
                        hb_state.record_failure();
                        update_state_metrics(&state, &hb_state, &active_clients, &status_tx).await;
                        check_restart(&hb_state, &restart_tx).await;
                        continue;
                    }

                    debug!("heartbeat probe sent: {}", hb_id);

                    // Wait for response with timeout
                    let timeout_result = tokio::time::timeout(config.timeout, response_rx).await;

                    // Clean up pending entry
                    hb_state.pending.remove(&hb_id);

                    match timeout_result {
                        Ok(Ok(())) => {
                            let elapsed = started_at.elapsed().as_millis() as u64;
                            debug!("heartbeat response received in {}ms", elapsed);
                            hb_state.record_success(elapsed);

                            // Log slow responses
                            if elapsed > config.timeout.as_millis() as u64 / 2 {
                                warn!("slow heartbeat response: {}ms", elapsed);
                            }
                        }
                        Ok(Err(_)) => {
                            // Channel closed unexpectedly
                            warn!("heartbeat response channel closed");
                            hb_state.record_failure();
                        }
                        Err(_) => {
                            // Timeout
                            error!(
                                "heartbeat timeout after {:?} (failure {}/{})",
                                config.timeout,
                                hb_state.metrics.consecutive_failures + 1,
                                config.max_failures
                            );
                            hb_state.record_failure();
                        }
                    }

                    update_state_metrics(&state, &hb_state, &active_clients, &status_tx).await;
                    check_restart(&hb_state, &restart_tx).await;
                }

                Some(event) = heartbeat_rx.recv() => {
                    match event {
                        HeartbeatEvent::Response { id } => {
                            if let Some((started_at, sender)) = hb_state.pending.remove(&id) {
                                let elapsed = started_at.elapsed().as_millis() as u64;
                                debug!("heartbeat {} completed in {}ms", id, elapsed);
                                // Notify the waiting task
                                let _ = sender.send(());
                            }
                        }
                    }
                }
            }
        }
    })
}

/// Update MuxState with current heartbeat metrics.
async fn update_state_metrics(
    state: &Arc<Mutex<MuxState>>,
    hb_state: &HeartbeatState,
    active_clients: &Arc<Semaphore>,
    status_tx: &watch::Sender<StatusSnapshot>,
) {
    let metrics = hb_state.get_metrics();
    {
        let mut st = state.lock().await;
        st.heartbeat_metrics = metrics;
    }
    publish_status(state, active_clients, status_tx).await;
}

/// Check if restart is needed and signal if so.
async fn check_restart(hb_state: &HeartbeatState, restart_tx: &mpsc::UnboundedSender<String>) {
    if hb_state.should_restart() {
        error!(
            "triggering server restart due to {} consecutive heartbeat failures",
            hb_state.metrics.consecutive_failures
        );
        let _ = restart_tx.send(format!(
            "heartbeat timeout ({} consecutive failures)",
            hb_state.metrics.consecutive_failures
        ));
    }
}

/// Check if a response ID is a heartbeat probe response.
pub fn is_heartbeat_response(id: &str) -> bool {
    id.starts_with("__heartbeat_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_config_defaults() {
        let config = HeartbeatConfig::default();
        assert_eq!(config.interval, Duration::from_secs(30));
        assert_eq!(config.timeout, Duration::from_secs(30));
        assert_eq!(config.max_failures, 3);
        assert!(config.enabled);
    }

    #[test]
    fn heartbeat_config_disabled() {
        let config = HeartbeatConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn heartbeat_state_records_success() {
        let config = HeartbeatConfig::default();
        let mut state = HeartbeatState::new(config);

        state.record_success(100);
        assert_eq!(state.metrics.total_success, 1);
        assert_eq!(state.metrics.consecutive_failures, 0);
        assert!(state.metrics.last_heartbeat_ms.is_some());
        assert_eq!(state.metrics.avg_response_ms, Some(100));
    }

    #[test]
    fn heartbeat_state_records_failure() {
        let config = HeartbeatConfig::default();
        let mut state = HeartbeatState::new(config);

        state.record_failure();
        assert_eq!(state.metrics.total_failures, 1);
        assert_eq!(state.metrics.consecutive_failures, 1);
    }

    #[test]
    fn heartbeat_state_triggers_restart() {
        let config = HeartbeatConfig {
            max_failures: 3,
            ..Default::default()
        };
        let mut state = HeartbeatState::new(config);

        state.record_failure();
        assert!(!state.should_restart());

        state.record_failure();
        assert!(!state.should_restart());

        state.record_failure();
        assert!(state.should_restart());
    }

    #[test]
    fn heartbeat_state_resets_on_success() {
        let config = HeartbeatConfig {
            max_failures: 3,
            ..Default::default()
        };
        let mut state = HeartbeatState::new(config);

        state.record_failure();
        state.record_failure();
        assert_eq!(state.metrics.consecutive_failures, 2);

        state.record_success(50);
        assert_eq!(state.metrics.consecutive_failures, 0);
        assert_eq!(state.metrics.total_failures, 2);
        assert_eq!(state.metrics.total_success, 1);
    }

    #[test]
    fn is_heartbeat_response_detects_ids() {
        assert!(is_heartbeat_response("__heartbeat_1"));
        assert!(is_heartbeat_response("__heartbeat_123"));
        assert!(!is_heartbeat_response("heartbeat_1"));
        assert!(!is_heartbeat_response("request_1"));
        assert!(!is_heartbeat_response("1"));
    }

    #[test]
    fn heartbeat_rolling_average() {
        let config = HeartbeatConfig::default();
        let mut state = HeartbeatState::new(config);

        state.record_success(100);
        assert_eq!(state.metrics.avg_response_ms, Some(100));

        state.record_success(200);
        assert_eq!(state.metrics.avg_response_ms, Some(150));

        state.record_success(300);
        assert_eq!(state.metrics.avg_response_ms, Some(200));
    }
}
