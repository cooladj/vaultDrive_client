use std::collections::HashMap;
use crate::commands::{execute_get_ui_connections, send_command_to_daemon, CommandResponse, ResponseData, SocketCommand};
use eframe::egui;
use egui_async::{EguiAsyncPlugin, Bind};
use egui::{Color32, ComboBox, CornerRadius, Margin, RichText, Sense, Stroke, TextEdit, Vec2, Window};
use std::net::SocketAddr;
use keyring::Entry;
use serde::{Deserialize, Serialize};
use tracing::debug;
use strum_macros::{Display, EnumIter};
use strum::IntoEnumIterator;


pub async fn runUI() -> eframe::Result<()> {
    let connections =match send_command_to_daemon(
        SocketCommand::Volumes {
            connection: None,
        }
    ).await{
        Ok(CommandResponse::Success(ResponseData::VolumesInfoData(connections))) => {
            connections
        }
        _ => vec![]
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
pub enum Operation{
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
    pub(crate) fn new(ip_addr: SocketAddr, drive: Vec<Drive>, username:String, host_name: String) -> Self {
        Self { ip_addr, drive, username, host_name  }
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
    auto_run: bool,

    editable_mount: String,
    editable_sync: String,
}

impl Drive {
    pub fn new(name: &str, server_mount_point: &str, mount_path: &str, sync_path: &str,mounted: bool, used_gb: u64, total_gb: u64, auto_run: bool) -> Self {
        Self {
            name: name.to_string(),
            server_mount_point: server_mount_point.to_string(),
            mount_path: mount_path.to_string(),
            sync_path: sync_path.to_string(),
            mounted,
            used_space_gb: used_gb,
            total_space_gb: total_gb,
            editable_mount: String::default(),
            editable_sync:String::default(),
            auto_run
        }
    }

    fn usage_percent(&self) -> f32 {
        (self.used_space_gb as f32 / self.total_space_gb as f32) * 100.0
    }

    fn is_critical(&self) -> bool {
        self.usage_percent() > 80.0
    }
}

pub struct DriveManagerApp {
    connections: Vec<Connection>,
    connect_bind: Bind<Vec<Connection>, String>,
    
    drive_bind: HashMap<(String, Operation), Bind<Vec<Connection>, String>>,
    dialog: ConnectDriveDialog,

}
impl DriveManagerApp {
    pub fn new(connection: Vec<Connection>) -> Self {
        Self { connections: connection,
            connect_bind: Bind::new(false),
            drive_bind: HashMap::default(),
            dialog: ConnectDriveDialog::default(),


        }
    }

}
//this is for testing
impl Default for DriveManagerApp {
    fn default() -> Self {
        Self {
            connections: vec![
                Connection {
                    ip_addr: "192.168.1.100".parse().unwrap(),
                    username: "Jimmy".to_string(),
                    host_name: "JimmyDesktop".to_string(),
                    drive: vec![
                        Drive::new("System Drive", "","C:\\", "", true, 320, 500, true),
                        Drive::new("Data Drive", "","D:\\", "",true, 450, 1000, false),
                        Drive::new("External Backup", "","E:\\", "",false, 1200, 2000, true),
                    ]
                }
            ],
            drive_bind: HashMap::default(),
            connect_bind: Bind::new(false),

            dialog: Default::default(),
        }
    }
}

impl eframe::App for DriveManagerApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        ctx.plugin_or_default::<EguiAsyncPlugin>();

        egui::CentralPanel::default().show(ctx, |ui| {
            egui::ScrollArea::vertical().show(ui, |ui| {
                ui.add_space(20.0);

                // Header
                ui.horizontal(|ui| {
                    ui.add_space(20.0);

                    // Icon and title
                    ui.label(RichText::new("🖴").size(32.0));
                    ui.add_space(10.0);

                    ui.vertical(|ui| {
                        ui.label(RichText::new("Drive Manager").size(28.0).strong());
                        ui.label(RichText::new("Manage local and remote storage connections")
                            .size(14.0)
                            .color(Color32::GRAY));
                    });

                    ui.add_space(ui.available_width() - 180.0);

                    // Connect Drive button
                    let button_response = ui.add_sized(
                        [160.0, 40.0],
                        egui::Button::new(RichText::new("+ Connect Drive").size(14.0).color(Color32::WHITE))
                            .fill( Color32::BLACK)
                            .stroke(Stroke::NONE)
                            .corner_radius(6.0)
                        //todo figure out how to disable
                        // .enabled(self.connect_bind.is_idle())
                    );


                    if button_response.clicked() {
                        self.dialog.open = true;
                    }

                    ui.add_space(20.0);
                });

                ui.add_space(30.0);

                for (connection_index, connection) in self.connections.clone().iter_mut().enumerate() {
                    ui.horizontal(|ui| {
                        ui.add_space(20.0);
                        ui.label(
                            RichText::new(format!("{} ➔ {} ➔ {}", connection.ip_addr, connection.host_name,connection.username))
                                .size(16.0)
                                .strong()
                        );

                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {

                            let disconnect_response = ui.add_sized(
                                [160.0, 40.0],
                                egui::Button::new(RichText::new("+ Disconnect ").size(14.0).color(Color32::WHITE))
                                    .fill( Color32::BLACK)
                                    .stroke(Stroke::NONE)
                                    .corner_radius(6.0)

                            );


                            if disconnect_response.clicked() {
                                let conn_clone = connection.clone();
                                let _ = self.connect_bind.read_or_request(|| {
                                    async move {

                                        match send_command_to_daemon(SocketCommand::Disconnect {
                                            username: conn_clone.username,
                                            socketaddr: conn_clone.ip_addr,
                                        }
                                        ).await {
                                            Ok(_) => {
                                                match send_command_to_daemon(SocketCommand::Volumes{
                                                    connection:None
                                                }).await {
                                                    Ok(CommandResponse::Success(ResponseData::VolumesInfoData(connections))) => {
                                                        Ok(connections)
                                                    }
                                                    _ => { Ok(vec![]) }
                                                }
                                            }
                                            Err(e) => {
                                                Err(e.to_string())
                                            }
                                        }
                                    }
                                });
                            }

                        });

                    });


                    ui.add_space(15.0);

                    ui.horizontal(|ui| {
                        egui::ScrollArea::horizontal()
                            .id_salt(format!("connection_scroll_{}", connection_index))  // Add this!
                            .show(ui, |ui| {
                            ui.add_space(20.0);

                            let available_width = ui.available_width() - 40.0;
                            let card_width = (available_width - 40.0) / 3.0;


                            for drive_index in 0..self.connections[connection_index].drive.len() {
                                self.render_drive_card(ui, drive_index, connection_index, card_width);
                                ui.add_space(20.0);
                            }
                        });

                        ui.add_space(20.0);
                    });
                }
            });


            if let Some(form_data) = self.dialog.show(ctx, &mut self.connect_bind) {
                let _ = self.connect_bind.read_or_request(|| {
                    async move {

                        let service = format!("vaultDrive|{}|{}",
                                              form_data.connection_type,
                                              form_data.system
                        );
                        let entry = Entry::new(&service, form_data.username.as_str()).map_err(
                            |e| e.to_string())?;

                        entry.set_password(form_data.password.as_str())
                            .map_err(|e| e.to_string())?;
                        match send_command_to_daemon(SocketCommand::Connect {
                            connection_type: form_data.connection_type,
                            connection_point: form_data.system,
                            username: form_data.username,
                        }).await {
                            Ok(_) => {
                                match send_command_to_daemon(SocketCommand::Volumes{
                                    connection:None
                                }).await {
                                    Ok(CommandResponse::Success(ResponseData::VolumesInfoData(connections))) => {
                                        Ok(connections)
                                    }
                                    _ => { Ok(vec![]) }
                                }
                            }
                            Err(e) => {
                                Err(e.to_string())
                            }
                        }
                    }
                });
            }


            // Handle the result when async request completes
            if self.connect_bind.just_completed() {
                if let Some(new_connections) = self.connect_bind.take_ok() {
                    debug!("Successfully loaded {} connections", new_connections.len());
                    self.connections = new_connections;
                    self.dialog.open = false;

                    // Clear form
                    self.dialog.system.clear();
                    self.dialog.username.clear();
                    self.dialog.password.clear();
                    self.dialog.connection_type = ConnectionType::default();
                } else if self.connect_bind.is_err() {
                    debug!("Connect error occurred");
                    //todo
                    // Keep dialog open to show error
                    // renable
                }
            }
        });
    }
}

impl DriveManagerApp {
    fn render_drive_card(&mut self, ui: &mut egui::Ui, drive_idx: usize,
                         connection_idx: usize
                         , width: f32)  {

        let connection = &mut self.connections[connection_idx];
        let conn_clone = connection.clone();
        let ip_addr = connection.ip_addr;
        let username = connection.username.clone();

        // THEN borrow mutably
        let drive = &mut connection.drive[drive_idx];


        let card_color = if drive.mounted {
            Color32::from_rgb(240, 248, 245)
        } else {
            Color32::from_rgb(245, 245, 245)
        };
        let mount_loading = match self.drive_bind.get_mut(&(drive.server_mount_point.to_string(), Operation::MOUNT)){
            None => { false}
            Some( mut bind) => {bind.is_pending()}
        };
        let sync_load = match self.drive_bind.get_mut(&(drive.server_mount_point.to_string(), Operation::SYNC)){
            None => { false}
            Some( mut bind) => {bind.is_pending()}
        };
        let drive_clone = drive.clone();


        egui::Frame::none()
            .fill(card_color)
            .stroke(Stroke::new(1.0, Color32::from_rgb(220, 220, 220)))
            .corner_radius(12.0)
            .inner_margin(20.0)
            .show(ui, |ui| {
                    ui.set_width(width);
                    ui.set_min_height(250.0);

                    ui.vertical(|ui| {
                        // Header with drive name and status badge
                        ui.horizontal(|ui| {
                            ui.label(RichText::new("🖴").size(20.0));
                            ui.add_space(5.0);
                            ui.label(RichText::new(&drive.name).size(18.0).strong());

                            ui.add_space(ui.available_width() - 90.0);

                            // Status badge
                            let (badge_text, badge_color) = if drive.mounted {
                                ("Mounted", Color32::BLACK)
                            } else {
                                ("Unmounted", Color32::from_rgb(100, 100, 100))
                            };

                            egui::Frame::none()
                                .fill(badge_color)
                                .corner_radius(12.0)
                                .inner_margin(Margin::symmetric(12.0 as i8, 4.0 as i8))
                                .show(ui, |ui| {
                                    ui.label(RichText::new(badge_text)
                                        .size(12.0)
                                        .color(Color32::WHITE)
                                        .strong());
                                });
                        });

                        ui.add_space(25.0);

                        // Path
                        ui.label(RichText::new("Path").size(12.0).color(Color32::GRAY));
                        ui.add_space(5.0);

                        if drive.mounted {
                            egui::Frame::default()
                                .fill(Color32::from_rgb(230, 230, 230))
                                .corner_radius(6.0)
                                .inner_margin(8.0)
                                .show(ui, |ui| {
                                    ui.label(
                                        RichText::new(&drive.mount_path)
                                            .size(14.0)
                                    );
                                });
                        } else {
                            egui::Frame::default()
                                .fill(Color32::WHITE)
                                .corner_radius(6.0)
                                .inner_margin(8.0)
                                .show(ui, |ui| {
                                    ui.add(
                                        TextEdit::singleline(&mut drive.editable_mount)
                                            .font(egui::FontId::proportional(14.0))
                                            .frame(false)
                                            .text_color(Color32::BLACK)
                                    );
                                });
                        }

                        ui.add_space(20.0);

                        // Storage info
                        ui.label(RichText::new("Storage").size(12.0).color(Color32::GRAY));
                        ui.add_space(5.0);

                        let storage_text = format!("{} GB / {} GB", drive.used_space_gb, drive.total_space_gb);
                        ui.label(RichText::new(storage_text).size(14.0).strong());

                        ui.add_space(8.0);

                        // Progress bar
                        let progress = drive.used_space_gb as f32 / drive.total_space_gb as f32;
                        let bar_color = if drive.is_critical() {
                            Color32::from_rgb(220, 38, 38)
                        } else {
                            Color32::from_rgb(34, 197, 94)
                        };

                        let bar_height = 8.0;
                        let (rect, _) = ui.allocate_exact_size(
                            Vec2::new(ui.available_width(), bar_height),
                            Sense::hover()
                        );

                        // Background
                        ui.painter().rect_filled(
                            rect,
                            CornerRadius::same(4.0 as u8),
                            Color32::from_rgb(220, 220, 220)
                        );

                        // Progress fill
                        let filled_width = rect.width() * progress;
                        let filled_rect = egui::Rect::from_min_size(
                            rect.min,
                            Vec2::new(filled_width, bar_height)
                        );
                        ui.painter().rect_filled(
                            filled_rect,
                            CornerRadius::same(4.0 as u8),
                            bar_color
                        );

                        ui.add_space(25.0);

                        let (button_text, button_color, text_color, stroke) = if drive.mounted {
                            (
                                "🔌 Unmount",
                                Color32::from_rgb(250, 250, 250),
                                Color32::BLACK,
                                Stroke::new(1.0, Color32::from_rgb(200, 200, 200))
                            )
                        } else {
                            (
                                "🔌 Mount",
                                Color32::from_rgb(20, 20, 20),
                                Color32::WHITE,
                                Stroke::NONE
                            )
                        };

                        ui.horizontal(|ui| {

                        let button_response = ui.add_enabled(
                            !mount_loading,
                            egui::Button::new(RichText::new(button_text)
                                .size(14.0)
                                .color(text_color))
                                .fill(button_color)
                                .stroke(stroke)
                                .corner_radius(6.0)
                        );
                        if drive.mounted {

                            let (button_color, button_text ) = if drive.auto_run{
                                (Color32::RED, "Remove Auto Mount")
                            }else {
                                (Color32::GREEN, "Add Auto Mount")
                            };

                            let button_response = ui.add_enabled(
                                !mount_loading,
                                egui::Button::new(RichText::new(button_text)
                                    .size(14.0)
                                    .color(Color32::BLACK))
                                    .fill(button_color)
                                    .stroke(if drive.mounted {
                                        Stroke::new(1.0, Color32::from_rgb(200, 200, 200))
                                    } else {
                                        Stroke::NONE
                                    })
                                    .corner_radius(6.0)
                            );
                            if button_response.clicked() {

                               let command = match button_text {
                                    "Remove Auto Mount" => {
                                        SocketCommand:: UnAutoMount {
                                            drive: drive.server_mount_point.clone(),
                                            socketaddr: connection.ip_addr,
                                            username: username.clone(),
                                        }

                                    }
                                    "Add Auto Mount" => {
                                        SocketCommand::AutoMount {
                                            mount_point: drive.mount_path.clone(),
                                            drive: drive.server_mount_point.clone(),
                                            socketaddr: connection.ip_addr,
                                            username: username.clone(),
                                        }
                                    }
                                    _ => {
                                        unreachable!()
                                    }

                                };
                                let _ = self.connect_bind.read_or_request(|| {
                                    async move {

                                        match send_command_to_daemon(command).await {
                                            Ok(_) => {
                                                match send_command_to_daemon(SocketCommand::Volumes{
                                                    connection:None
                                                }).await {
                                                    Ok(CommandResponse::Success(ResponseData::VolumesInfoData(connections))) => {
                                                        Ok(connections)
                                                    }
                                                    _ => { Ok(vec![]) }
                                                }
                                            }
                                            Err(e) => {
                                                Err(e.to_string())
                                            }
                                        }
                                    }
                                });
                            }
                        }


                        if button_response.clicked() {
                            let (socket_command, operation) =
                                if !drive.mounted {
                                    (SocketCommand::Mount {
                                        mount_point: drive.editable_mount.clone(),
                                        drive: drive.server_mount_point.clone(),
                                        socketaddr: ip_addr,
                                        username: username.clone(),
                                    }, Operation::MOUNT)
                                }
                                else {
                                    (SocketCommand::UnMount {
                                        username,
                                        socketaddr: ip_addr,
                                        mount_point: drive.mount_path.clone(),
                                    }, Operation::MOUNT)
                                };

                            debug!("Mount button was clicked");


                            let  bind = Bind::new(false);
                            self.drive_bind.insert((drive.server_mount_point.to_string(), operation.clone()), bind);
                            let got_bind=self.drive_bind.get_mut(&(drive.server_mount_point.to_string(), operation)).expect("This should never fail unless insert fails");
                                got_bind.read_mut_or_request(|| {
                                async move {
                                    match send_command_to_daemon(socket_command).await {
                                        Ok(_) => {
                                            match send_command_to_daemon(SocketCommand::Volumes{
                                                connection: Some(conn_clone),
                                            }).await {
                                                Ok(CommandResponse::Success(ResponseData::VolumesInfoData(connections))) => {
                                                    Ok(connections)
                                                }
                                                _ => Ok(vec![])
                                            }
                                        }
                                        Err(e) => Err(e.to_string())
                                    }
                                }
                            });

                        }
                    });

                    });
            });
        for op in Operation::iter() {
            match self.drive_bind.get_mut(&(drive_clone.server_mount_point.to_string(), op.clone())) {
                None => {}
                Some(bind) => {
                    if bind.just_completed() {
                        if let Some(new_connections) = bind.take_ok() {
                            debug!("The {:?} was completed", op);
                            // Iterate through new connections
                            for new_conn in new_connections {
                                // Find matching connection by ip_addr and username
                                if let Some(existing_conn) = self.connections.iter_mut()
                                    .find(|c| c.ip_addr == new_conn.ip_addr && c.username == new_conn.username) {

                                    // Iterate through new drives
                                    for new_drive in &new_conn.drive {
                                        // Find and replace matching drive by server_mount_point
                                        if let Some(existing_drive) = existing_conn.drive.iter_mut()
                                            .find(|d| d.server_mount_point == new_drive.server_mount_point) {

                                            *existing_drive = new_drive.clone();
                                        }
                                    }
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
}


#[derive(Debug, Clone)]
struct FormData {
    connection_type: ConnectionType,
    system: String,
    username: String,
    password: String,
}
#[derive(Default)]
struct ConnectDriveDialog {
    open: bool,
    connection_type: ConnectionType,
    system: String,
    username: String,
    password: String,
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

impl ConnectDriveDialog {
    pub fn show(&mut self, ctx: &egui::Context, connect_bind: &mut Bind<Vec<Connection>, String>) -> Option<FormData> {
        let mut submit_data = None;
        let is_loading = connect_bind.is_pending();

        // Dialog window
        if self.open {
            // Window::new("Connect to Remote System")
            //     .open(&mut open)
            //     .collapsible(false)
            //     .resizable(false)
            let response  = egui::Modal::new(egui::Id::new(20))

                .show(ctx, |ui| {
                    ui.add_enabled_ui(!is_loading, |ui| {
                        ui.label(RichText::new("Add a network or remote drive to access files from other systems")
                            .color(Color32::GRAY));
                        ui.add_space(15.0);

                        // Connection Type
                        ui.label("Connection Type");
                        ui.add_space(5.0);
                        ComboBox::from_id_salt("connection_type")
                            .selected_text(format!("{:?}", self.connection_type))
                            .width(ui.available_width())
                            .show_ui(ui, |ui| {
                                ui.selectable_value(&mut self.connection_type, ConnectionType::Direct, "Direct");
                                ui.selectable_value(&mut self.connection_type, ConnectionType::Hub, "Hub");
                            });
                        ui.add_space(12.0);

                        // System/Hostname field
                        let label = if self.connection_type == ConnectionType::Direct {
                            "System Address"
                        } else {
                            "Hostname"
                        };
                        let placeholder = if self.connection_type == ConnectionType::Direct {
                            "192.168.1.100:1234"
                        } else {
                            "Hostname"
                        };

                        ui.label(label);
                        ui.add_space(5.0);
                        ui.add_sized(
                            [ui.available_width(), 30.0],
                            TextEdit::singleline(&mut self.system)
                                .hint_text(placeholder)
                        );
                        ui.add_space(12.0);

                        // Username field
                        ui.label("Username");
                        ui.add_space(5.0);
                        ui.add_sized(
                            [ui.available_width(), 30.0],
                            TextEdit::singleline(&mut self.username)
                                .hint_text("Username")
                        );
                        ui.add_space(15.0);

                        //password field
                        ui.label("Password");
                        ui.add_space(5.0);
                        ui.add_sized(
                            [ui.available_width(), 30.0],
                            TextEdit::singleline(&mut self.password)
                                .hint_text("password")
                                .password(true)
                        );
                        ui.add_space(15.0);
                    });



                    // Loading indicator
                    if is_loading {
                        ui.add_space(8.0);
                        ui.horizontal(|ui| {
                            ui.spinner();
                            ui.label("Connecting to drive...");
                        });
                        ui.add_space(8.0);
                    }

                    // Buttons
                    ui.horizontal(|ui| {
                        if ui.add_enabled(!is_loading,
                                          egui::Button::new("Cancel")
                                              .min_size([80.0, 35.0].into())
                        ).clicked() {
                            ui.close();
                        }

                        ui.add_space(ui.available_width() - 80.0);

                        if ui.add_enabled(!is_loading && !self.system.is_empty() && !self.username.is_empty() && !self.password.is_empty(),
                                          egui::Button::new(egui::RichText::new("Connect").color(Color32::WHITE))
                                              .fill( Color32::BLACK)
                                              .min_size([80.0, 35.0].into())
                        ).clicked() {

                            submit_data = Some(FormData {
                                connection_type: self.connection_type,
                                system: self.system.clone(),
                                username: self.username.clone(),
                                password: self.password.clone()


                            });
                        }
                    });
                });
            if response.should_close(){
                self.open = !self.open;
            }
        }

        submit_data
    }

}