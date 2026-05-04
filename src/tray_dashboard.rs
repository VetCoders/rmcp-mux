//! Multi-server tray dashboard for rmcp_mux.
//!
//! Displays status of all managed MCP servers in a system tray menu.
//! Queries the daemon status socket periodically to update the display.

use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tray_icon::{
    Icon, TrayIconBuilder,
    menu::{Menu, MenuEvent, MenuId, MenuItem, PredefinedMenuItem, Submenu},
};

use crate::multi::StatusLevel;
use crate::runtime::{DEFAULT_STATUS_SOCKET, DaemonStatus, query_status};
use crate::tray::LoadedIcon;

/// Run the tray dashboard on the current thread (required for macOS main thread).
///
/// This creates a system tray icon that shows status of all managed servers.
/// It queries the daemon status socket periodically and updates the menu.
/// Must be called from the main thread on macOS.
pub fn run_tray_dashboard(
    shutdown: CancellationToken,
    icon: Option<LoadedIcon>,
    status_socket: Option<PathBuf>,
) {
    let socket = status_socket.unwrap_or_else(|| PathBuf::from(DEFAULT_STATUS_SOCKET));

    // Create a tokio runtime for async status queries
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime");

    rt.block_on(async move {
        tray_dashboard_loop(socket, shutdown, icon).await;
    });
}

/// Spawn the multi-server tray dashboard in a background thread.
///
/// Note: On macOS, tray menus must be created on the main thread.
/// Use `run_tray_dashboard` instead for standalone dashboard commands.
pub fn spawn_tray_dashboard(
    shutdown: CancellationToken,
    icon: Option<LoadedIcon>,
    status_socket: Option<PathBuf>,
) -> thread::JoinHandle<()> {
    let socket = status_socket.unwrap_or_else(|| PathBuf::from(DEFAULT_STATUS_SOCKET));

    thread::spawn(move || {
        // Create a tokio runtime for async status queries
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create tokio runtime");

        rt.block_on(async move {
            tray_dashboard_loop(socket, shutdown, icon).await;
        });
    })
}

/// Menu items for a single server.
struct ServerMenuItems {
    submenu: Submenu,
    status: MenuItem,
    clients: MenuItem,
    pending: MenuItem,
    restarts: MenuItem,
    heartbeat: MenuItem,
}

/// The tray UI state.
struct DashboardUi {
    _tray: tray_icon::TrayIcon,
    header: MenuItem,
    summary: MenuItem,
    servers: Vec<ServerMenuItems>,
    quit_id: MenuId,
}

impl DashboardUi {
    fn update(&mut self, status: &DaemonStatus) {
        // Update header
        self.header
            .set_text(format!("rmcp-mux v{} | {}", status.version, status.uptime));

        // Update summary
        self.summary.set_text(format!(
            "{} servers: {} running, {} errors",
            status.server_count, status.running_count, status.error_count
        ));

        // Update per-server items
        for (i, server) in status.servers.iter().enumerate() {
            if let Some(items) = self.servers.get(i) {
                let icon = status_icon(server.level);
                items.submenu.set_text(format!("{} {}", icon, server.name));
                items
                    .status
                    .set_text(format!("  Status: {}", server.status_text));
                items.clients.set_text(format!(
                    "  Clients: {}/{}",
                    server.active_clients, server.max_active_clients
                ));
                items
                    .pending
                    .set_text(format!("  Pending: {}", server.pending_requests));
                items
                    .restarts
                    .set_text(format!("  Restarts: {}", server.restarts));
                items.heartbeat.set_text(format!(
                    "  Heartbeat: {}",
                    server
                        .heartbeat_latency_ms
                        .map(|ms| format!("{}ms", ms))
                        .unwrap_or_else(|| "-".to_string())
                ));
            }
        }
    }
}

fn status_icon(level: StatusLevel) -> &'static str {
    match level {
        StatusLevel::Ok => "✓",
        StatusLevel::Warn => "⚠",
        StatusLevel::Error => "✗",
        StatusLevel::Lazy => "○",
    }
}

async fn tray_dashboard_loop(
    socket: PathBuf,
    shutdown: CancellationToken,
    icon: Option<LoadedIcon>,
) {
    // Wait for daemon to start and get initial status
    let mut status = loop {
        if shutdown.is_cancelled() {
            return;
        }
        match query_status(&socket).await {
            Ok(s) => break s,
            Err(_) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };

    // Build the tray UI
    let mut ui = match build_dashboard(&status, icon.as_ref()) {
        Ok(u) => u,
        Err(e) => {
            eprintln!("tray dashboard init failed: {e}");
            return;
        }
    };

    let mut interval = tokio::time::interval(Duration::from_secs(2));

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                return;
            }
            _ = interval.tick() => {
                // Check for quit event
                while let Ok(event) = MenuEvent::receiver().try_recv() {
                    if event.id == ui.quit_id {
                        shutdown.cancel();
                        return;
                    }
                }

                // Query updated status
                if let Ok(new_status) = query_status(&socket).await {
                    status = new_status;
                    ui.update(&status);
                }
            }
        }
    }
}

fn build_dashboard(status: &DaemonStatus, icon_data: Option<&LoadedIcon>) -> Result<DashboardUi> {
    let menu = Menu::new();

    // Header
    let header = MenuItem::new(
        format!("rmcp-mux v{} | {}", status.version, status.uptime),
        false,
        None,
    );
    menu.append(&header)?;

    // Separator
    menu.append(&PredefinedMenuItem::separator())?;

    // Summary
    let summary = MenuItem::new(
        format!(
            "{} servers: {} running, {} errors",
            status.server_count, status.running_count, status.error_count
        ),
        false,
        None,
    );
    menu.append(&summary)?;

    // Separator
    menu.append(&PredefinedMenuItem::separator())?;

    // Per-server submenus
    let mut servers = Vec::with_capacity(status.servers.len());
    for server in &status.servers {
        let icon = status_icon(server.level);
        let submenu = Submenu::new(format!("{} {}", icon, server.name), true);

        let status_item = MenuItem::new(format!("  Status: {}", server.status_text), false, None);
        let clients_item = MenuItem::new(
            format!(
                "  Clients: {}/{}",
                server.active_clients, server.max_active_clients
            ),
            false,
            None,
        );
        let pending_item = MenuItem::new(
            format!("  Pending: {}", server.pending_requests),
            false,
            None,
        );
        let restarts_item = MenuItem::new(format!("  Restarts: {}", server.restarts), false, None);
        let heartbeat_item = MenuItem::new(
            format!(
                "  Heartbeat: {}",
                server
                    .heartbeat_latency_ms
                    .map(|ms| format!("{}ms", ms))
                    .unwrap_or_else(|| "-".to_string())
            ),
            false,
            None,
        );

        submenu.append(&status_item)?;
        submenu.append(&clients_item)?;
        submenu.append(&pending_item)?;
        submenu.append(&restarts_item)?;
        submenu.append(&heartbeat_item)?;

        menu.append(&submenu)?;

        servers.push(ServerMenuItems {
            submenu,
            status: status_item,
            clients: clients_item,
            pending: pending_item,
            restarts: restarts_item,
            heartbeat: heartbeat_item,
        });
    }

    // Separator before quit
    menu.append(&PredefinedMenuItem::separator())?;

    // Quit item
    let quit_item = MenuItem::new("Quit Dashboard", true, None);
    let quit_id = quit_item.id().clone();
    menu.append(&quit_item)?;

    // Build tray icon
    let icon = if let Some(data) = icon_data {
        Icon::from_rgba(data.data.clone(), data.width, data.height)?
    } else {
        default_dashboard_icon()
    };

    let tray = TrayIconBuilder::new()
        .with_tooltip("rmcp-mux Dashboard")
        .with_icon(icon)
        .with_menu(Box::new(menu))
        .build()?;

    Ok(DashboardUi {
        _tray: tray,
        header,
        summary,
        servers,
        quit_id,
    })
}

fn default_dashboard_icon() -> Icon {
    // 16x16 icon with gradient - slightly different from single-server icon
    let (w, h) = (16, 16);
    let mut data = Vec::with_capacity(w * h * 4);
    for y in 0..h {
        for x in 0..w {
            let gradient = 0x60 + ((x + y) % 32) as u8;
            // Purple-ish color to distinguish from single-server tray
            data.extend_from_slice(&[0x8b, gradient, 0xff, 0xff]);
        }
    }
    Icon::from_rgba(data, w as u32, h as u32).expect("valid icon")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_icon_mapping() {
        assert_eq!(status_icon(StatusLevel::Ok), "✓");
        assert_eq!(status_icon(StatusLevel::Warn), "⚠");
        assert_eq!(status_icon(StatusLevel::Error), "✗");
        assert_eq!(status_icon(StatusLevel::Lazy), "○");
    }

    #[test]
    fn default_dashboard_icon_creates() {
        let icon = default_dashboard_icon();
        let _ = icon;
    }
}
