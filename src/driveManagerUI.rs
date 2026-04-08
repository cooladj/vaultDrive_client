use std::collections::HashMap;
use crate::commands::{send_command_to_daemon, CommandResponse, ResponseData, SocketCommand};
use eframe::egui;
use egui_async::{EguiAsyncPlugin, Bind};
use egui::{Color32, ComboBox, CornerRadius, Margin, RichText, Sense, Stroke, TextEdit, Vec2, Window};
use std::net::SocketAddr;
use serde::{Deserialize, Serialize};
use tracing::debug;
use strum_macros::{Display, EnumIter};
use strum::IntoEnumIterator;
use crate::shared::{elevated, get_user_id};

// ── Theme ────────────────────────────────────────────────────────────────────

struct Theme;

impl Theme {
    /// Backgrounds
    const BG_PRIMARY: Color32 = Color32::from_rgb(248, 249, 251);
    const BG_CARD: Color32 = Color32::from_rgb(255, 255, 255);
    const BG_CARD_MOUNTED: Color32 = Color32::from_rgb(247, 252, 249);
    const BG_INPUT: Color32 = Color32::from_rgb(243, 244, 246);
    const BG_INPUT_ACTIVE: Color32 = Color32::WHITE;

    /// Text
    const TEXT_PRIMARY: Color32 = Color32::from_rgb(17, 24, 39);
    const TEXT_SECONDARY: Color32 = Color32::from_rgb(107, 114, 128);
    const TEXT_MUTED: Color32 = Color32::from_rgb(156, 163, 175);
    const TEXT_WHITE: Color32 = Color32::WHITE;

    /// Accent / Buttons
    const ACCENT: Color32 = Color32::from_rgb(24, 24, 27);
    const ACCENT_LIGHT: Color32 = Color32::from_rgb(63, 63, 70);
    const ACCENT_SUBTLE: Color32 = Color32::from_rgb(244, 244, 245);

    /// Status
    const SUCCESS: Color32 = Color32::from_rgb(22, 163, 74);
    const SUCCESS_LIGHT: Color32 = Color32::from_rgb(220, 252, 231);
    const DANGER: Color32 = Color32::from_rgb(220, 38, 38);
    const DANGER_LIGHT: Color32 = Color32::from_rgb(254, 226, 226);
    const WARNING: Color32 = Color32::from_rgb(234, 179, 8);

    /// Borders
    const BORDER: Color32 = Color32::from_rgb(229, 231, 235);
    const BORDER_LIGHT: Color32 = Color32::from_rgb(243, 244, 246);

    /// Spacing
    const SP_XS: f32 = 4.0;
    const SP_SM: f32 = 8.0;
    const SP_MD: f32 = 16.0;
    const SP_LG: f32 = 24.0;
    const SP_XL: f32 = 32.0;

    /// Sizing
    const CARD_MIN_H: f32 = 260.0;
    const BTN_H: f32 = 36.0;
    const BTN_H_SM: f32 = 32.0;
    const INPUT_H: f32 = 34.0;
    const RADIUS: f32 = 8.0;
    const RADIUS_SM: f32 = 6.0;
    const RADIUS_LG: f32 = 12.0;
    const PAGE_PAD: f32 = 28.0;
    const BAR_H: f32 = 6.0;
}


pub async fn run_ui() -> eframe::Result<()> {
    let connections = match send_command_to_daemon(
        SocketCommand::Volumes {
            user_id: get_user_id().unwrap_or_default(),
            connection: None,
            elevated: elevated(),
        }
    ).await {
        Ok(CommandResponse::Success(ResponseData::VolumesInfoData(connections))) => connections,
        _ => vec![],
    };

    debug!("connections: {:?}", connections);

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1400.0, 600.0])
            .with_resizable(true),
        ..Default::default()
    };

    eframe::run_native(
        "Drive Manager",
        options,
        Box::new(|_cc| Ok(Box::new(DriveManagerApp::new(connections)))),
    )
}

#[derive(Eq, Hash, PartialEq, EnumIter, Clone, Debug)]
pub enum Operation {
    MOUNT,
    SYNC,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Connection {
    ip_addr: SocketAddr,
    drive: Vec<Drive>,
    username: String,
    host_name: String,
}

impl Connection {
    pub(crate) fn new(ip_addr: SocketAddr, drive: Vec<Drive>, username: String, host_name: String) -> Self {
        Self { ip_addr, drive, username, host_name }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Drive {
    name: String,
    server_mount_point: String,
    mount_path: String,
    sync_path: String,
    mounted: bool,
    used_space_gb: u64,
    total_space_gb: u64,
    scope: String,
    compress: bool,
}

impl Drive {
    pub fn new(
        name: &str, server_mount_point: &str, mount_path: &str, sync_path: &str,
        mounted: bool, used_gb: u64, total_gb: u64, scope: String, compress: bool
    ) -> Self {
        Self {
            name: name.to_string(),
            server_mount_point: server_mount_point.to_string(),
            mount_path: mount_path.to_string(),
            sync_path: sync_path.to_string(),
            mounted,
            used_space_gb: used_gb,
            total_space_gb: total_gb,
            compress,
            scope,
        }
    }

    fn usage_percent(&self) -> f32 {
        if self.total_space_gb == 0 { return 0.0; }
        (self.used_space_gb as f32 / self.total_space_gb as f32) * 100.0
    }

    fn usage_fraction(&self) -> f32 {
        if self.total_space_gb == 0 { return 0.0; }
        self.used_space_gb as f32 / self.total_space_gb as f32
    }

    fn is_critical(&self) -> bool {
        self.usage_percent() > 80.0
    }

    fn is_warning(&self) -> bool {
        self.usage_percent() > 60.0
    }
}

// ── App ──────────────────────────────────────────────────────────────────────

pub struct DriveManagerApp {
    connections: Vec<Connection>,
    connect_bind: Bind<Vec<Connection>, String>,
    drive_bind: HashMap<(String, Operation), Bind<Vec<Connection>, String>>,
    dialog: ConnectDriveDialog,
}

impl DriveManagerApp {
    pub fn new(connection: Vec<Connection>) -> Self {
        Self {
            connections: connection,
            connect_bind: Bind::new(false),
            drive_bind: HashMap::default(),
            dialog: ConnectDriveDialog::default(),
        }
    }
}


// ── Main update loop ─────────────────────────────────────────────────────────

impl eframe::App for DriveManagerApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        ctx.plugin_or_default::<EguiAsyncPlugin>();

        let panel_frame = egui::Frame::NONE
            .fill(Theme::BG_PRIMARY)
            .inner_margin(0.0);

        egui::CentralPanel::default().frame(panel_frame).show(ctx, |ui| {
            egui::ScrollArea::vertical().show(ui, |ui| {
                ui.add_space(Theme::SP_LG);

                // ── Header ───────────────────────────────────────────────
                self.render_header(ui);

                ui.add_space(Theme::SP_SM);

                // ── Separator ────────────────────────────────────────────
                let rect = ui.available_rect_before_wrap();
                ui.painter().line_segment(
                    [
                        egui::pos2(rect.left() + Theme::PAGE_PAD, rect.top()),
                        egui::pos2(rect.right() - Theme::PAGE_PAD, rect.top()),
                    ],
                    Stroke::new(1.0, Theme::BORDER_LIGHT),
                );
                ui.add_space(Theme::SP_LG);

                // ── Connections ───────────────────────────────────────────
                for (connection_index, connection) in self.connections.clone().iter().enumerate() {
                    self.render_connection_header(ui, connection, connection_index);

                    ui.add_space(Theme::SP_MD);

                    ui.horizontal(|ui| {
                        egui::ScrollArea::horizontal()
                            .id_salt(format!("connection_scroll_{}", connection_index))
                            .show(ui, |ui| {
                                ui.add_space(Theme::PAGE_PAD);

                                let available_width = ui.available_width() - Theme::PAGE_PAD * 2.0;
                                let card_width = ((available_width - Theme::SP_MD * 2.0) / 3.0).max(280.0);

                                for drive_index in 0..self.connections[connection_index].drive.len() {
                                    self.render_drive_card(ui, drive_index, connection_index, card_width);
                                    ui.add_space(Theme::SP_MD);
                                }
                            });
                    });

                    ui.add_space(Theme::SP_LG);
                }
            });

            // ── Dialog ───────────────────────────────────────────────────
            if let Some(form_data) = self.dialog.show(ctx, &mut self.connect_bind) {
                let _ = self.connect_bind.read_or_request(|| {
                    async move {
                        match send_command_to_daemon(SocketCommand::Connect {
                            connection_type: form_data.connection_type,
                            connection_point: form_data.system,
                            username: form_data.username,
                            password: form_data.password,
                            scope_type: form_data.scope_type,
                            tofu_enable: form_data.tofu_enable,
                        }).await {
                            Ok(_) => {
                                match send_command_to_daemon(SocketCommand::Volumes {
                                    user_id: get_user_id().unwrap_or_default(),
                                    connection: None,
                                    elevated: elevated(),
                                }).await {
                                    Ok(CommandResponse::Success(ResponseData::VolumesInfoData(connections))) => Ok(connections),
                                    _ => Ok(vec![]),
                                }
                            }
                            Err(e) => Err(e.to_string()),
                        }
                    }
                });
            }

            // Handle async result
            if self.connect_bind.just_completed() {
                if let Some(new_connections) = self.connect_bind.take_ok() {
                    debug!("Successfully loaded {} connections", new_connections.len());
                    self.connections = new_connections;
                    self.dialog.open = false;
                    self.dialog.system.clear();
                    self.dialog.username.clear();
                    self.dialog.password.clear();
                    self.dialog.connection_type = ConnectionType::default();
                } else if self.connect_bind.is_err() {
                    debug!("Connect error occurred");
                }
            }
        });
    }
}

// ── Rendering helpers ────────────────────────────────────────────────────────

impl DriveManagerApp {
    fn render_header(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.add_space(Theme::PAGE_PAD);

            ui.vertical(|ui| {
                ui.label(
                    RichText::new("Drive Manager")
                        .size(26.0)
                        .strong()
                        .color(Theme::TEXT_PRIMARY),
                );
                ui.add_space(Theme::SP_XS);
                ui.label(
                    RichText::new("Manage local and remote storage connections")
                        .size(13.0)
                        .color(Theme::TEXT_SECONDARY),
                );
            });

            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.add_space(Theme::PAGE_PAD);

                if ui.add_sized(
                    [150.0, Theme::BTN_H],
                    egui::Button::new(
                        RichText::new("+ Connect Drive")
                            .size(13.0)
                            .color(Theme::TEXT_WHITE),
                    )
                        .fill(Theme::ACCENT)
                        .stroke(Stroke::NONE)
                        .corner_radius(Theme::RADIUS),
                ).clicked() {
                    self.dialog.open = true;
                }
            });
        });
    }

    fn render_connection_header(&mut self, ui: &mut egui::Ui, connection: &Connection, _index: usize) {
        ui.horizontal(|ui| {
            ui.add_space(Theme::PAGE_PAD);

            // Status dot
            let dot_rect = ui.allocate_exact_size(Vec2::splat(8.0), Sense::hover()).0;
            ui.painter().circle_filled(dot_rect.center(), 4.0, Theme::SUCCESS);

            ui.add_space(Theme::SP_SM);

            ui.label(
                RichText::new(format!("{} → {} → {}", connection.ip_addr, connection.host_name, connection.username))
                    .size(14.0)
                    .strong()
                    .color(Theme::TEXT_PRIMARY),
            );

            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.add_space(Theme::PAGE_PAD);

                if ui.add_sized(
                    [130.0, Theme::BTN_H_SM],
                    egui::Button::new(
                        RichText::new("Disconnect")
                            .size(12.0)
                            .color(Theme::DANGER),
                    )
                        .fill(Theme::DANGER_LIGHT)
                        .stroke(Stroke::NONE)
                        .corner_radius(Theme::RADIUS_SM),
                ).clicked() {
                    let conn_clone = connection.clone();
                    let _ = self.connect_bind.read_or_request(|| {
                        async move {
                            match send_command_to_daemon(SocketCommand::Disconnect {
                                username: conn_clone.username,
                                socketaddr: conn_clone.ip_addr,
                            }).await {
                                Ok(_) => {
                                    match send_command_to_daemon(SocketCommand::Volumes {
                                        user_id: get_user_id().unwrap_or_default(),
                                        connection: None,
                                        elevated: elevated(),
                                    }).await {
                                        Ok(CommandResponse::Success(ResponseData::VolumesInfoData(connections))) => Ok(connections),
                                        _ => Ok(vec![]),
                                    }
                                }
                                Err(e) => Err(e.to_string()),
                            }
                        }
                    });
                }
            });
        });
    }

    fn render_drive_card(
        &mut self, ui: &mut egui::Ui, drive_idx: usize,
        connection_idx: usize, width: f32,
    ) {
        let connection = &mut self.connections[connection_idx];
        let conn_clone = connection.clone();
        let ip_addr = connection.ip_addr;
        let username = connection.username.clone();
        let drive = &mut connection.drive[drive_idx];

        let card_color = if drive.mounted { Theme::BG_CARD_MOUNTED } else { Theme::BG_CARD };

        let mount_loading = self.drive_bind
            .get_mut(&(drive.server_mount_point.to_string(), Operation::MOUNT))
            .map(|b| b.is_pending())
            .unwrap_or(false);

        let sync_loading = self.drive_bind
            .get_mut(&(drive.server_mount_point.to_string(), Operation::SYNC))
            .map(|b| b.is_pending())
            .unwrap_or(false);

        let drive_clone = drive.clone();

        egui::Frame::NONE
            .fill(card_color)
            .stroke(Stroke::new(1.0, Theme::BORDER))
            .corner_radius(Theme::RADIUS_LG)
            .inner_margin(Theme::SP_LG)
            .shadow(egui::epaint::Shadow {
                offset: [0, 1],
                blur: 6,
                spread: 0,
                color: Color32::from_black_alpha(8),
            })
            .show(ui, |ui| {
                ui.set_width(width);
                ui.set_min_height(Theme::CARD_MIN_H);

                ui.vertical(|ui| {
                    // ── Card header ──────────────────────────────────
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("💾").size(18.0));
                        ui.add_space(Theme::SP_XS);
                        ui.label(
                            RichText::new(&drive.name)
                                .size(16.0)
                                .strong()
                                .color(Theme::TEXT_PRIMARY),
                        );

                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            let (badge_text, badge_bg, badge_fg) = if drive.mounted {
                                ("Mounted", Theme::SUCCESS_LIGHT, Theme::SUCCESS)
                            } else {
                                ("Unmounted", Theme::ACCENT_SUBTLE, Theme::TEXT_SECONDARY)
                            };

                            egui::Frame::NONE
                                .fill(badge_bg)
                                .corner_radius(Theme::RADIUS_SM)
                                .inner_margin(Margin::symmetric(10, 3))
                                .show(ui, |ui| {
                                    ui.label(
                                        RichText::new(badge_text)
                                            .size(11.0)
                                            .color(badge_fg)
                                            .strong(),
                                    );
                                });
                        });
                    });

                    ui.add_space(Theme::SP_LG);

                    // ── Path ─────────────────────────────────────────
                    ui.label(
                        RichText::new("Mount Path")
                            .size(11.0)
                            .color(Theme::TEXT_MUTED)
                            .strong(),
                    );
                    ui.add_space(Theme::SP_XS);

                    if drive.mounted {
                        egui::Frame::default()
                            .fill(Theme::BG_INPUT)
                            .corner_radius(Theme::RADIUS_SM)
                            .inner_margin(Theme::SP_SM)
                            .show(ui, |ui| {
                                ui.label(
                                    RichText::new(&drive.mount_path)
                                        .size(13.0)
                                        .color(Theme::TEXT_PRIMARY),
                                );
                            });
                    } else {
                        egui::Frame::default()
                            .fill(Theme::BG_INPUT_ACTIVE)
                            .stroke(Stroke::new(1.0, Theme::BORDER))
                            .corner_radius(Theme::RADIUS_SM)
                            .inner_margin(Theme::SP_SM)
                            .show(ui, |ui| {
                                ui.add(
                                    TextEdit::singleline(&mut drive.mount_path)
                                        .font(egui::FontId::proportional(13.0))
                                        .frame(false)
                                        .text_color(Theme::TEXT_PRIMARY)
                                        .hint_text("Enter mount path..."),
                                );
                            });
                    }

                    ui.add_space(Theme::SP_MD);

                    // ── Storage ──────────────────────────────────────
                    ui.label(
                        RichText::new("Storage")
                            .size(11.0)
                            .color(Theme::TEXT_MUTED)
                            .strong(),
                    );
                    ui.add_space(Theme::SP_XS);

                    ui.horizontal(|ui| {
                        ui.label(
                            RichText::new(format!("{} GB", drive.used_space_gb))
                                .size(14.0)
                                .strong()
                                .color(Theme::TEXT_PRIMARY),
                        );
                        ui.label(
                            RichText::new(format!("/ {} GB", drive.total_space_gb))
                                .size(14.0)
                                .color(Theme::TEXT_SECONDARY),
                        );

                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            ui.label(
                                RichText::new(format!("{:.0}%", drive.usage_percent()))
                                    .size(12.0)
                                    .color(if drive.is_critical() {
                                        Theme::DANGER
                                    } else if drive.is_warning() {
                                        Theme::WARNING
                                    } else {
                                        Theme::TEXT_SECONDARY
                                    }),
                            );
                        });
                    });

                    ui.add_space(Theme::SP_SM);

                    // Progress bar
                    let progress = drive.usage_fraction();
                    let bar_color = if drive.is_critical() {
                        Theme::DANGER
                    } else if drive.is_warning() {
                        Theme::WARNING
                    } else {
                        Theme::SUCCESS
                    };

                    let (rect, _) = ui.allocate_exact_size(
                        Vec2::new(ui.available_width(), Theme::BAR_H),
                        Sense::hover(),
                    );

                    ui.painter().rect_filled(rect, 3.0, Theme::BG_INPUT);

                    let filled_rect = egui::Rect::from_min_size(
                        rect.min,
                        Vec2::new(rect.width() * progress, Theme::BAR_H),
                    );
                    ui.painter().rect_filled(filled_rect, 3.0, bar_color);

                    ui.add_space(Theme::SP_LG);

                    // ── Action buttons ───────────────────────────────
                    ui.with_layout(egui::Layout::top_down(egui::Align::Center), |ui| {
                        ui.horizontal(|ui| {
                            // Mount / Unmount button
                            let (button_text, button_fill, text_color, btn_stroke) = if drive.mounted {
                                ("Unmount", Theme::ACCENT_SUBTLE, Theme::TEXT_PRIMARY, Stroke::new(1.0, Theme::BORDER))
                            } else {
                                ("Mount", Theme::ACCENT, Theme::TEXT_WHITE, Stroke::NONE)
                            };

                            let button_response = ui.add_enabled(
                                !mount_loading,
                                egui::Button::new(
                                    RichText::new(button_text)
                                        .size(12.0)
                                        .color(text_color),
                                )
                                    .fill(button_fill)
                                    .stroke(btn_stroke)
                                    .corner_radius(Theme::RADIUS_SM)
                                    .min_size(Vec2::new(80.0, Theme::BTN_H_SM)),
                            );

                            // Scope and Compress only visible when mounted
                            let mut scope_response: Option<egui::Response> = None;
                            let mut compress_changed = false;

                            if drive.mounted {
                                ui.add_space(Theme::SP_SM);

                                // Scope button
                                let (scope_fill, scope_text) = if drive.scope.is_empty() {
                                    (Theme::SUCCESS_LIGHT, "All Users")
                                } else {
                                    (Theme::ACCENT_SUBTLE, "Current User")
                                };

                                scope_response = Some(ui.add_enabled(
                                    !mount_loading && elevated(),
                                    egui::Button::new(
                                        RichText::new(scope_text)
                                            .size(12.0)
                                            .color(Theme::TEXT_PRIMARY),
                                    )
                                        .fill(scope_fill)
                                        .stroke(Stroke::new(1.0, Theme::BORDER))
                                        .corner_radius(Theme::RADIUS_SM)
                                        .min_size(Vec2::new(90.0, Theme::BTN_H_SM)),
                                ));

                                ui.add_space(Theme::SP_SM);

                                // Compress toggle
                                ui.add_enabled_ui(!mount_loading, |ui| {
                                    if toggle(ui, &mut drive.compress).clicked() {
                                        compress_changed = true;
                                    }
                                    ui.label(
                                        RichText::new("Compress")
                                            .size(12.0)
                                            .color(Theme::TEXT_PRIMARY),
                                    );
                                });
                            }

                            // ── Scope click handler ─────────────────────
                            if let Some(resp) = scope_response {
                                if resp.clicked() {
                                    drive.scope = if drive.scope.is_empty() {
                                        get_user_id().unwrap_or_default()
                                    } else {
                                        String::new()
                                    };

                                    let mount_point = drive.mount_path.clone();
                                    let username = username.clone();
                                    let drive_mount = drive.server_mount_point.clone();
                                    let scope = drive.scope.clone();
                                    let compress = drive.compress;

                                    let _ = self.connect_bind.read_or_request(|| {
                                        async move {
                                            match send_command_to_daemon(SocketCommand::Mount {
                                                mount_point,
                                                drive: drive_mount,
                                                socketaddr: ip_addr,
                                                username,
                                                scope,
                                                compress,
                                            }).await {
                                                Ok(_) => {
                                                    match send_command_to_daemon(SocketCommand::Volumes {
                                                        user_id: get_user_id().unwrap_or_default(),
                                                        connection: None,
                                                        elevated: elevated(),
                                                    }).await {
                                                        Ok(CommandResponse::Success(ResponseData::VolumesInfoData(connections))) => Ok(connections),
                                                        _ => Ok(vec![]),
                                                    }
                                                }
                                                Err(e) => Err(e.to_string()),
                                            }
                                        }
                                    });
                                }
                            }

                            // ── Compress toggle handler ──────────────────
                            if compress_changed {
                                let mount_point = drive.mount_path.clone();
                                let username = username.clone();
                                let drive_mount = drive.server_mount_point.clone();
                                let scope = drive.scope.clone();
                                let compress = drive.compress;

                                let _ = self.connect_bind.read_or_request(|| {
                                    async move {
                                        match send_command_to_daemon(SocketCommand::Mount {
                                            mount_point,
                                            drive: drive_mount,
                                            socketaddr: ip_addr,
                                            username,
                                            scope,
                                            compress,
                                        }).await {
                                            Ok(_) => {
                                                match send_command_to_daemon(SocketCommand::Volumes {
                                                    user_id: get_user_id().unwrap_or_default(),
                                                    connection: None,
                                                    elevated: elevated(),
                                                }).await {
                                                    Ok(CommandResponse::Success(ResponseData::VolumesInfoData(connections))) => Ok(connections),
                                                    _ => Ok(vec![]),
                                                }
                                            }
                                            Err(e) => Err(e.to_string()),
                                        }
                                    }
                                });
                            }

                            // ── Mount click handler ─────────────────────
                            if button_response.clicked() {
                                let (socket_command, operation) = if !drive.mounted {
                                    (SocketCommand::Mount {
                                        mount_point: drive.mount_path.clone(),
                                        drive: drive.server_mount_point.clone(),
                                        socketaddr: ip_addr,
                                        username: username.clone(),
                                        scope: drive.scope.clone(),
                                        compress: drive.compress,
                                    }, Operation::MOUNT)
                                } else {
                                    (SocketCommand::UnMount {
                                        username,
                                        socketaddr: ip_addr,
                                        mount_point: drive.mount_path.clone(),
                                    }, Operation::MOUNT)
                                };

                                debug!("Mount button was clicked");

                                let bind = Bind::new(false);
                                self.drive_bind.insert((drive.server_mount_point.to_string(), operation.clone()), bind);
                                let got_bind = self.drive_bind
                                    .get_mut(&(drive.server_mount_point.to_string(), operation))
                                    .expect("Insert just succeeded");

                                got_bind.read_mut_or_request(|| {
                                    async move {
                                        match send_command_to_daemon(socket_command).await {
                                            Ok(_) => {
                                                match send_command_to_daemon(SocketCommand::Volumes {
                                                    user_id: get_user_id().unwrap_or_default(),
                                                    connection: Some(conn_clone),
                                                    elevated: elevated(),
                                                }).await {
                                                    Ok(CommandResponse::Success(ResponseData::VolumesInfoData(connections))) => Ok(connections),
                                                    _ => Ok(vec![]),
                                                }
                                            }
                                            Err(e) => Err(e.to_string()),
                                        }
                                    }
                                });
                            }
                        });
                    });
                });
            });

        // ── Handle async completions for this card ──────────────────
        for op in Operation::iter() {
            if let Some(bind) = self.drive_bind.get_mut(&(drive_clone.server_mount_point.to_string(), op.clone())) {
                if bind.just_completed() {
                    if let Some(new_connections) = bind.take_ok() {
                        debug!("The {:?} was completed", op);
                        for new_conn in new_connections {
                            if let Some(existing_conn) = self.connections.iter_mut()
                                .find(|c| c.ip_addr == new_conn.ip_addr && c.username == new_conn.username)
                            {
                                if new_conn.drive.is_empty() {
                                    // Server returned no drives — update in place
                                    // to preserve current UI state rather than
                                    // wiping everything
                                    continue;
                                }
                                // Replace the entire drive list so newly mounted/
                                // unmounted drives are reflected
                                existing_conn.drive = new_conn.drive;
                            }
                        }
                    } else if bind.is_err() {
                        debug!("Connect error occurred");
                    }
                }
            }
        }
    }
}

// ── Dialog types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct FormData {
    connection_type: ConnectionType,
    system: String,
    username: String,
    password: String,
    scope_type: ScopeType,
    tofu_enable: bool,
}

#[derive(Default)]
struct ConnectDriveDialog {
    open: bool,
    connection_type: ConnectionType,
    scope: ScopeType,
    system: String,
    username: String,
    password: String,
    tofu_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize, Display)]
pub enum ConnectionType {
    #[default]
    Direct,
    Hub,
}

impl ConnectionType {
    pub(crate) fn to_i32(&self) -> i32 {
        match self {
            ConnectionType::Direct => 0,
            ConnectionType::Hub => 1,
        }
    }

    pub(crate) fn from_i32(value: i32) -> anyhow::Result<Self> {
        match value {
            0 => Ok(ConnectionType::Direct),
            1 => Ok(ConnectionType::Hub),
            _ => Err(anyhow::anyhow!("Invalid connection type value: {}", value)),
        }
    }
}

impl Default for ScopeType {
    fn default() -> Self {
        ScopeType::CurrentUser(get_user_id().unwrap_or_default())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Display)]
pub enum ScopeType {
    #[strum(to_string = "{0}")]
    CurrentUser(String),
    #[strum(to_string = "")]
    All,
}

// ── Connect dialog ───────────────────────────────────────────────────────────

impl ConnectDriveDialog {
    pub fn show(&mut self, ctx: &egui::Context, connect_bind: &mut Bind<Vec<Connection>, String>) -> Option<FormData> {
        let mut submit_data = None;
        let is_loading = connect_bind.is_pending();

        if self.open {
            let response = egui::Modal::new(egui::Id::new(20))
                .show(ctx, |ui| {
                    ui.add_enabled_ui(!is_loading, |ui| {
                        ui.label(
                            RichText::new("Add a network or remote drive to access files from other systems")
                                .color(Theme::TEXT_SECONDARY),
                        );
                        ui.add_space(Theme::SP_MD);

                        // Scope
                        ui.add_enabled_ui(elevated(), |ui| {
                            ui.label(
                                RichText::new("Scope")
                                    .size(12.0)
                                    .strong()
                                    .color(Theme::TEXT_PRIMARY),
                            );
                            ui.add_space(Theme::SP_XS);
                            ComboBox::from_id_salt("scope")
                                .selected_text(match self.scope {
                                    ScopeType::CurrentUser(_) => "Current User",
                                    ScopeType::All => "All Users",
                                })
                                .width(ui.available_width())
                                .show_ui(ui, |ui| {
                                    ui.selectable_value(&mut self.scope, ScopeType::default(), "Current User");
                                    ui.selectable_value(&mut self.scope, ScopeType::All, "All Users");
                                });
                        });

                        ui.add_space(Theme::SP_MD);

                        // Connection Type
                        ui.label(
                            RichText::new("Connection Type")
                                .size(12.0)
                                .strong()
                                .color(Theme::TEXT_PRIMARY),
                        );
                        ui.add_space(Theme::SP_XS);
                        ComboBox::from_id_salt("connection_type")
                            .selected_text(format!("{:?}", self.connection_type))
                            .width(ui.available_width())
                            .show_ui(ui, |ui| {
                                ui.selectable_value(&mut self.connection_type, ConnectionType::Direct, "Direct");
                                ui.selectable_value(&mut self.connection_type, ConnectionType::Hub, "Hub");
                            });

                        ui.add_space(Theme::SP_MD);

                        // System / Hostname
                        let (label, placeholder) = if self.connection_type == ConnectionType::Direct {
                            ("System Address", "192.168.1.100:1234")
                        } else {
                            ("Hostname", "my-server.local")
                        };

                        ui.label(
                            RichText::new(label)
                                .size(12.0)
                                .strong()
                                .color(Theme::TEXT_PRIMARY),
                        );
                        ui.add_space(Theme::SP_XS);
                        ui.add_sized(
                            [ui.available_width(), Theme::INPUT_H],
                            TextEdit::singleline(&mut self.system).hint_text(placeholder),
                        );

                        ui.add_space(Theme::SP_MD);

                        // Username
                        ui.label(
                            RichText::new("Username")
                                .size(12.0)
                                .strong()
                                .color(Theme::TEXT_PRIMARY),
                        );
                        ui.add_space(Theme::SP_XS);
                        ui.add_sized(
                            [ui.available_width(), Theme::INPUT_H],
                            TextEdit::singleline(&mut self.username).hint_text("Username"),
                        );

                        ui.add_space(Theme::SP_MD);

                        // Password
                        ui.label(
                            RichText::new("Password")
                                .size(12.0)
                                .strong()
                                .color(Theme::TEXT_PRIMARY),
                        );
                        ui.add_space(Theme::SP_XS);
                        ui.add_sized(
                            [ui.available_width(), Theme::INPUT_H],
                            TextEdit::singleline(&mut self.password)
                                .hint_text("Password")
                                .password(true),
                        );

                        ui.add_space(Theme::SP_LG);

                        // TOFU toggle
                        ui.horizontal(|ui| {
                            toggle(ui, &mut self.tofu_enabled);
                            ui.add_space(Theme::SP_SM);
                            ui.label(
                                RichText::new("Trust on First Use (TOFU)")
                                    .size(13.0)
                                    .color(Theme::TEXT_PRIMARY),
                            );
                        });
                        ui.add_space(Theme::SP_XS);
                        ui.label(
                            RichText::new("Accept and remember the server's certificate on first connection")
                                .color(Theme::TEXT_MUTED)
                                .small(),
                        );

                        ui.add_space(Theme::SP_MD);
                    });

                    // Loading indicator
                    if is_loading {
                        ui.add_space(Theme::SP_SM);
                        ui.horizontal(|ui| {
                            ui.spinner();
                            ui.label(
                                RichText::new("Connecting to drive...")
                                    .color(Theme::TEXT_SECONDARY),
                            );
                        });
                        ui.add_space(Theme::SP_SM);
                    }

                    // Buttons
                    ui.add_space(Theme::SP_SM);
                    ui.horizontal(|ui| {
                        if ui.add_enabled(
                            !is_loading,
                            egui::Button::new(
                                RichText::new("Cancel").color(Theme::TEXT_PRIMARY),
                            )
                                .fill(Theme::ACCENT_SUBTLE)
                                .stroke(Stroke::new(1.0, Theme::BORDER))
                                .corner_radius(Theme::RADIUS_SM)
                                .min_size([90.0, Theme::BTN_H].into()),
                        ).clicked() {
                            self.open = false;
                        }

                        ui.add_space(ui.available_width() - 90.0);

                        let can_submit = !is_loading
                            && !self.system.is_empty()
                            && !self.username.is_empty()
                            && !self.password.is_empty();

                        if ui.add_enabled(
                            can_submit,
                            egui::Button::new(
                                RichText::new("Connect")
                                    .size(13.0)
                                    .color(Theme::TEXT_WHITE),
                            )
                                .fill(Theme::ACCENT)
                                .stroke(Stroke::NONE)
                                .corner_radius(Theme::RADIUS_SM)
                                .min_size([90.0, Theme::BTN_H].into()),
                        ).clicked() {
                            submit_data = Some(FormData {
                                connection_type: self.connection_type,
                                system: self.system.clone(),
                                username: self.username.clone(),
                                password: self.password.clone(),
                                scope_type: self.scope.clone(),
                                tofu_enable: self.tofu_enabled,
                            });
                        }
                    });
                });

            if response.should_close() {
                self.open = !self.open;
            }
        }

        submit_data
    }
}

// ── Toggle widget ────────────────────────────────────────────────────────────

fn toggle(ui: &mut egui::Ui, on: &mut bool) -> egui::Response {
    let size = egui::vec2(44.0, 22.0);
    let (rect, response) = ui.allocate_exact_size(size, egui::Sense::click());

    if response.clicked() {
        *on = !*on;
    }

    let how_on = ui.ctx().animate_bool_with_time(response.id, *on, 0.2);

    let radius = rect.height() / 2.0;
    let bg = Color32::from_rgb(
        egui::lerp((200.0)..=(24.0), how_on) as u8,
        egui::lerp((200.0)..=(24.0), how_on) as u8,
        egui::lerp((205.0)..=(27.0), how_on) as u8,
    );
    let circle_x = egui::lerp(rect.left() + radius..=rect.right() - radius, how_on);

    ui.painter().rect_filled(rect, radius, bg);
    ui.painter().circle_filled(
        egui::pos2(circle_x, rect.center().y),
        radius - 2.5,
        Color32::WHITE,
    );

    response
}