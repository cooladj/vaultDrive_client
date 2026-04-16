use std::net::SocketAddr;
use serde::{Deserialize, Serialize};
use slint::{ComponentHandle, ModelRc, SharedString, VecModel};
use strum_macros::Display;
use tracing::debug;

// Re-export from your existing crate
use crate::commands::{send_command_to_daemon, CommandResponse, ResponseData, SocketCommand};
use crate::shared::{elevated, get_user_id};

// ── Generated Slint bindings ───────────────────────────────────────────────

slint::include_modules!();

// ── Domain Types (unchanged from original) ─────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Connection {
    pub ip_addr: SocketAddr,
    pub drive: Vec<Drive>,
    pub username: String,
    pub host_name: String,
}

impl Connection {
    pub fn new(ip_addr: SocketAddr, drive: Vec<Drive>, username: String, host_name: String) -> Self {
        Self { ip_addr, drive, username, host_name }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Drive {
    pub name: String,
    pub server_mount_point: String,
    pub mount_path: String,
    pub sync_path: String,
    pub mounted: bool,
    pub used_space_gb: u64,
    pub total_space_gb: u64,
    pub scope: String,
    pub compress: bool,
}

impl Drive {
    pub fn new(
        name: &str, server_mount_point: &str, mount_path: &str, sync_path: &str,
        mounted: bool, used_gb: u64, total_gb: u64, scope: String, compress: bool,
    ) -> Self {
        Self {
            name: name.to_string(),
            server_mount_point: server_mount_point.to_string(),
            mount_path: mount_path.to_string(),
            sync_path: sync_path.to_string(),
            mounted,
            used_space_gb: used_gb,
            total_space_gb: total_gb,
            scope,
            compress,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize, Display)]
pub enum ConnectionType {
    #[default]
    Direct,
    Hub,
}

impl ConnectionType {
    pub fn to_i32(&self) -> i32 {
        match self {
            ConnectionType::Direct => 0,
            ConnectionType::Hub => 1,
        }
    }

    pub fn from_i32(value: i32) -> anyhow::Result<Self> {
        match value {
            0 => Ok(ConnectionType::Direct),
            1 => Ok(ConnectionType::Hub),
            _ => Err(anyhow::anyhow!("Invalid connection type value: {}", value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Display)]
pub enum ScopeType {
    #[strum(to_string = "{0}")]
    CurrentUser(String),
    #[strum(to_string = "")]
    All,
}

impl Default for ScopeType {
    fn default() -> Self {
        ScopeType::CurrentUser(get_user_id().unwrap_or_default())
    }
}

// ── Conversions: Domain ↔ Slint ────────────────────────────────────────────

fn drive_to_slint(d: &Drive) -> DriveInfo {
    DriveInfo {
        name: d.name.clone().into(),
        server_mount_point: d.server_mount_point.clone().into(),
        mount_path: d.mount_path.clone().into(),
        sync_path: d.sync_path.clone().into(),
        mounted: d.mounted,
        used_space_gb: d.used_space_gb as i32,
        total_space_gb: d.total_space_gb as i32,
        scope: d.scope.clone().into(),
        compress: d.compress,
    }
}

fn connection_to_slint(c: &Connection) -> ConnectionInfo {
    let drives: Vec<DriveInfo> = c.drive.iter().map(drive_to_slint).collect();
    ConnectionInfo {
        ip_addr: c.ip_addr.to_string().into(),
        username: c.username.clone().into(),
        host_name: c.host_name.clone().into(),
        drives: ModelRc::new(VecModel::from(drives)),
    }
}

fn connections_to_model(connections: &[Connection]) -> ModelRc<ConnectionInfo> {
    let items: Vec<ConnectionInfo> = connections.iter().map(connection_to_slint).collect();
    ModelRc::new(VecModel::from(items))
}

// ── Shared async helper ────────────────────────────────────────────────────
// Every daemon operation follows the same pattern:
//   1. Send a command
//   2. Refresh volumes
//   3. Update the UI model
// This helper eliminates the repetition that plagued the egui version.

async fn execute_and_refresh(command: SocketCommand) -> Result<Vec<Connection>, String> {
    send_command_to_daemon(command)
        .await
        .map_err(|e| e.to_string())?;

    match send_command_to_daemon(SocketCommand::Volumes {
        user_id: get_user_id().unwrap_or_default(),
        connection: None,
        elevated: elevated(),
    })
    .await
    {
        Ok(CommandResponse::Success(ResponseData::VolumesInfoData(connections))) => {
            Ok(connections)
        }
        Ok(_) => Ok(vec![]),
        Err(e) => Err(e.to_string()),
    }
}

/// Push new connections into the UI on the main thread.
fn update_ui(ui: &slint::Weak<AppWindow>, connections: Vec<Connection>) {
    let ui = ui.clone();
    slint::invoke_from_event_loop(move || {
        if let Some(window) = ui.upgrade() {
            window.set_connections(connections_to_model(&connections));
            window.set_loading_key(SharedString::default());
            window.set_is_connecting(false);
        }
    })
    .ok();
}

fn show_error(ui: &slint::Weak<AppWindow>, err: &str) {
    debug!("Command error: {err}");
    let ui = ui.clone();
    slint::invoke_from_event_loop(move || {
        if let Some(window) = ui.upgrade() {
            window.set_loading_key(SharedString::default());
            window.set_is_connecting(false);
        }
    })
    .ok();
}

// ── Async dispatcher ───────────────────────────────────────────────────────
// A single function that spawns the command, refreshes, and updates UI.
// Replaces every Bind + HashMap + read_or_request + just_completed flow.

fn spawn_command(
    ui: &slint::Weak<AppWindow>,
    state: &std::sync::Arc<std::sync::Mutex<Vec<Connection>>>,
    command: SocketCommand,
) {
    let ui = ui.clone();
    let state = state.clone();
    tokio::spawn(async move {
        match execute_and_refresh(command).await {
            Ok(connections) => {
                *state.lock().unwrap() = connections.clone();
                update_ui(&ui, connections);
            }
            Err(e) => show_error(&ui, &e),
        }
    });
}

// ── Entry point ────────────────────────────────────────────────────────────

pub async fn run_ui() -> Result<(), slint::PlatformError> {
    // Fetch initial data
    let connections = match send_command_to_daemon(SocketCommand::Volumes {
        user_id: get_user_id().unwrap_or_default(),
        connection: None,
        elevated: elevated(),
    })
    .await
    {
        Ok(CommandResponse::Success(ResponseData::VolumesInfoData(c))) => c,
        _ => vec![],
    };

    debug!("connections: {:?}", connections);

    // Store a clone for callback closures to reference
    // (Slint callbacks capture by move, so we use Arc<Mutex<>> for shared state)
    let state = std::sync::Arc::new(std::sync::Mutex::new(connections.clone()));

    let app = AppWindow::new()?;
    app.set_connections(connections_to_model(&connections));
    app.set_is_elevated(elevated());

    // ── Connect Drive ──────────────────────────────────────────────────
    {
        let ui = app.as_weak();
        let state = state.clone();
        app.on_connect_drive(move |conn_type, system, username, password, scope_type, tofu| {
            let ui = ui.clone();
            if let Some(window) = ui.upgrade() {
                window.set_is_connecting(true);
            }

            let connection_type = ConnectionType::from_i32(conn_type).unwrap_or_default();
            let scope = if scope_type == 1 {
                ScopeType::All
            } else {
                ScopeType::default()
            };

            let command = SocketCommand::Connect {
                connection_type,
                connection_point: system.to_string(),
                username: username.to_string(),
                password: password.to_string(),
                scope_type: scope,
                tofu_enable: tofu,
            };

            let state = state.clone();
            let ui2 = ui.clone();
            tokio::spawn(async move {
                match execute_and_refresh(command).await {
                    Ok(connections) => {
                        // Update shared state
                        *state.lock().unwrap() = connections.clone();
                        // Update UI + close dialog + clear form
                        let ui = ui2.clone();
                        slint::invoke_from_event_loop(move || {
                            if let Some(window) = ui.upgrade() {
                                window.set_connections(connections_to_model(&connections));
                                window.set_is_connecting(false);
                                window.set_dialog_open(false);
                                window.invoke_clear_dialog();
                            }
                        })
                        .ok();
                    }
                    Err(e) => show_error(&ui2, &e),
                }
            });
        });
    }

    // ── Disconnect ─────────────────────────────────────────────────────
    {
        let ui = app.as_weak();
        let state = state.clone();
        app.on_disconnect(move |ci| {
            let command = {
                let conns = state.lock().unwrap();
                conns.get(ci as usize).map(|conn| SocketCommand::Disconnect {
                    username: conn.username.clone(),
                    socketaddr: conn.ip_addr,
                })
            };
            if let Some(command) = command {
                spawn_command(&ui, &state, command);
            }
        });
    }

    // ── Mount / Unmount ────────────────────────────────────────────────
    {
        let ui = app.as_weak();
        let state = state.clone();
        app.on_mount_clicked(move |ci, di| {
            let result = {
                let conns = state.lock().unwrap();
                conns.get(ci as usize).and_then(|conn| {
                    conn.drive.get(di as usize).map(|drive| {
                        let command = if drive.mounted {
                            SocketCommand::UnMount {
                                username: conn.username.clone(),
                                socketaddr: conn.ip_addr,
                                mount_point: drive.mount_path.clone(),
                            }
                        } else {
                            SocketCommand::Mount {
                                mount_point: drive.mount_path.clone(),
                                drive: drive.server_mount_point.clone(),
                                socketaddr: conn.ip_addr,
                                username: conn.username.clone(),
                                scope: drive.scope.clone(),
                                compress: drive.compress,
                            }
                        };
                        let loading = format!("{}-mount", drive.server_mount_point);
                        (command, loading)
                    })
                })
            };
            if let Some((command, loading)) = result {
                if let Some(window) = ui.upgrade() {
                    window.set_loading_key(loading.into());
                }
                spawn_command(&ui, &state, command);
            }
        });
    }

    // ── Scope Toggle ───────────────────────────────────────────────────
    {
        let ui = app.as_weak();
        let state = state.clone();
        app.on_scope_clicked(move |ci, di| {
            let command = {
                let mut conns = state.lock().unwrap();
                conns.get_mut(ci as usize).and_then(|conn| {
                    conn.drive.get_mut(di as usize).map(|drive| {
                        drive.scope = if drive.scope.is_empty() {
                            get_user_id().unwrap_or_default()
                        } else {
                            String::new()
                        };
                        SocketCommand::Mount {
                            mount_point: drive.mount_path.clone(),
                            drive: drive.server_mount_point.clone(),
                            socketaddr: conn.ip_addr,
                            username: conn.username.clone(),
                            scope: drive.scope.clone(),
                            compress: drive.compress,
                        }
                    })
                })
            };
            if let Some(command) = command {
                spawn_command(&ui, &state, command);
            }
        });
    }

    // ── Compress Toggle ────────────────────────────────────────────────
    {
        let ui = app.as_weak();
        let state = state.clone();
        app.on_compress_toggled(move |ci, di, val| {
            let command = {
                let mut conns = state.lock().unwrap();
                conns.get_mut(ci as usize).and_then(|conn| {
                    conn.drive.get_mut(di as usize).map(|drive| {
                        drive.compress = val;
                        SocketCommand::Mount {
                            mount_point: drive.mount_path.clone(),
                            drive: drive.server_mount_point.clone(),
                            socketaddr: conn.ip_addr,
                            username: conn.username.clone(),
                            scope: drive.scope.clone(),
                            compress: val,
                        }
                    })
                })
            };
            if let Some(command) = command {
                spawn_command(&ui, &state, command);
            }
        });
    }

    // ── Mount Path Changed ─────────────────────────────────────────────
    {
        let state = state.clone();
        app.on_mount_path_changed(move |ci, di, new_path| {
            let mut conns = state.lock().unwrap();
            if let Some(conn) = conns.get_mut(ci as usize) {
                if let Some(drive) = conn.drive.get_mut(di as usize) {
                    drive.mount_path = new_path.to_string();
                }
            }
        });
    }

    // ── Run ────────────────────────────────────────────────────────────
    app.run()
}
