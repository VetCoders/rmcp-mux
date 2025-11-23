use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{mpsc, watch, Mutex, Semaphore};

#[cfg_attr(not(feature = "tray"), allow(dead_code))]
#[derive(Clone, Debug)]
pub enum ServerStatus {
    Starting,
    Running,
    Restarting,
    Failed(String),
    Stopped,
}

#[cfg_attr(not(feature = "tray"), allow(dead_code))]
#[derive(Clone, Debug)]
pub struct StatusSnapshot {
    pub service_name: String,
    pub server_status: ServerStatus,
    pub restarts: u64,
    pub connected_clients: usize,
    pub active_clients: usize,
    pub max_active_clients: usize,
    pub pending_requests: usize,
    pub cached_initialize: bool,
    pub initializing: bool,
    pub last_reset: Option<String>,
}

#[derive(Clone)]
pub struct MuxState {
    pub next_client_id: u64,
    pub next_global_id: u64,
    pub clients: HashMap<u64, mpsc::UnboundedSender<Value>>,
    pub pending: HashMap<String, Pending>,
    pub cached_initialize: Option<Value>,
    pub init_waiting: Vec<(u64, Value)>,
    pub initializing: bool,
    pub server_status: ServerStatus,
    pub restarts: u64,
    pub last_reset: Option<String>,
    pub max_active_clients: usize,
    pub service_name: String,
}

#[derive(Clone, Debug)]
pub struct Pending {
    pub client_id: u64,
    pub local_id: Value,
    pub is_initialize: bool,
}

impl MuxState {
    pub fn new(max_active_clients: usize, service_name: String) -> Self {
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

    pub fn register_client(&mut self, tx: mpsc::UnboundedSender<Value>) -> u64 {
        let id = self.next_client_id;
        self.next_client_id += 1;
        self.clients.insert(id, tx);
        id
    }

    pub fn unregister_client(&mut self, client_id: u64) {
        self.clients.remove(&client_id);
        self.pending.retain(|_, p| p.client_id != client_id);
        self.init_waiting.retain(|(cid, _)| *cid != client_id);
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
