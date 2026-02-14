use anyhow::{anyhow, Context, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use std::fs;
use tracing;
use std::time::Duration;
use egui::TextBuffer;
use futures::{SinkExt, StreamExt};
use futures::future::join_all;
use interprocess::local_socket;
use interprocess::local_socket::{GenericFilePath, ToFsName};
use interprocess::local_socket::traits::tokio::Stream;
use log::debug;
use tokio::join;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use keyring::Entry;
use rusqlite::fallible_iterator::FallibleIterator;
use serde::{Deserialize, Serialize};
use crate::autoRun::{auto_run, exists, insert, remove};
use crate::client::{VAULT_DRIVE_MAP, VaultDriveClient};
use crate::driveManagerUI::{Connection, ConnectionType, Drive};
use crate::filesystem::{mount_to_UI_tuple};
use crate::network::ZERO_ADDR;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SocketCommand {

    Connect{
        connection_type: ConnectionType,
        connection_point:String,
        username: String
    },
    UnMount {
        mount_point: String,
        username: String,
        socketaddr: SocketAddr,
    },

    Mount {
        mount_point: String,
        drive: String,
        socketaddr: SocketAddr,
        username: String,

    },
    Volumes {
        connection: Option<Connection>
    },
    Health,

    Disconnect{
        username:String,
        socketaddr: SocketAddr,
    },
    AutoMount{
        mount_point: String,
        drive: String,
        socketaddr: SocketAddr,
        username: String,
    },
    UnAutoMount{
        drive: String,
        socketaddr: SocketAddr,
        username: String,
}
}
#[derive(Debug, Serialize, Deserialize)]
pub enum ResponseData {
    Text(String),
    Empty,
    VolumesInfoData(Vec<Connection>),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CommandResponse {
    Success(ResponseData),
    Error(String),
}

pub async fn execute_socket_command(command: SocketCommand) -> CommandResponse {
    match command {
        SocketCommand::Connect { connection_type: connection_method, connection_point,username} => {
            let service = format!("vaultDrive|{}|{}",
                                  connection_method,
                                    connection_point
            );
            let entry = match Entry::new(&service, username.as_str()) {
                Ok(entry) => entry,
                Err(e) => return CommandResponse::Error(e.to_string())
            };
          let operation =  if connection_method == ConnectionType::Direct{
              let sockerAdder:SocketAddr = match   connection_point.parse(){
                  Ok(socket) => socket,
                  Err(e) => return CommandResponse::Error(e.to_string()),
              };

                  connectionDirect(sockerAdder, username.as_str(),&entry).await
            }else {
              connectionHub( &*connection_point, username.as_str(),&entry).await
          };
          match operation {
              Ok(socket_addr) => CommandResponse::Success(ResponseData::Text(socket_addr.to_string())),
              Err(e) => CommandResponse::Error(e.to_string()),
          }
        }

        SocketCommand::Mount {mount_point, socketaddr, username, drive} => {
            match execute_mount(&mount_point, socketaddr,  &username,  &drive).await {
                Ok(_) => CommandResponse::Success(ResponseData::Empty),
                Err(e) => CommandResponse::Error(e.to_string()),
            }
        }
        SocketCommand::UnMount { mount_point, username, socketaddr} => {
            match execute_unmount(mount_point, socketaddr, username).await {
                Ok(_) => CommandResponse::Success(ResponseData::Empty),
                Err(e) => CommandResponse::Error(e.to_string()),
            }
        }
        SocketCommand::Volumes { connection }  => {
            match connection {
                None => {
                    match execute_get_ui_connections().await {
                    Ok(volumes) => CommandResponse::Success(ResponseData::VolumesInfoData(volumes)),
                    Err(e) => CommandResponse::Error(e.to_string()),
                }}
                Some(connection) => {
                    // todo instead of refreshing all connection refresh one
                    match execute_get_ui_connections().await {
                        Ok(volumes) => CommandResponse::Success(ResponseData::VolumesInfoData(volumes)),
                        Err(e) => CommandResponse::Error(e.to_string()),
                    }}
            }

        }
        SocketCommand::AutoMount {mount_point, drive,  username, socketaddr } => {
            match execute_set_auto_mount(mount_point, socketaddr, username, drive).await {
                Ok(_) => CommandResponse::Success(ResponseData::Empty),
                Err(e) => CommandResponse::Error(e.to_string()),
            }
        },
        SocketCommand::UnAutoMount {  drive,  username, socketaddr} => {
            match execute_un_auto_mount(drive, socketaddr, username).await {
                Ok(_) => CommandResponse::Success(ResponseData::Empty),
                Err(e) => CommandResponse::Error(e.to_string()),
            }
        }
        SocketCommand::Disconnect {username, socketaddr} => {

            match execute_disconnect(username, socketaddr).await {
                Ok(_) => CommandResponse::Success(ResponseData::Empty),
                Err(e) => CommandResponse::Error(e.to_string()),
            }
        }
        ,

        SocketCommand::Health  => {
            CommandResponse::Success(ResponseData::Text(String::from("Pong")))
        }
        _ => CommandResponse::Error("Invalid command".to_string()),
    }
}

pub async fn send_command_to_daemon(command: SocketCommand) -> Result<CommandResponse> {
    #[cfg(unix)]
    let path_str = "/tmp/vaultDriveClient.sock";

    #[cfg(windows)]
    let path_str = r"\\.\pipe\vaultDriveClient";

    let name = path_str.to_fs_name::<GenericFilePath>()?;

    let stream = match local_socket::tokio::Stream::connect(name).await {
        Ok(stream) => stream,
        Err(e) => {
            tracing::debug!("Failed to connect to daemon: {:?}", e.kind());
            return Ok(CommandResponse::Error(e.to_string()));
        }
    };

    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());

    let command_bytes = postcard::to_allocvec(&command)?;
    timeout(
        Duration::from_secs(5),
        framed.send(command_bytes.into())
    )
        .await
        .map_err(|_| anyhow!("Write timed out"))??;

    // Read response with timeout
    let response_bytes = timeout(
        Duration::from_secs(5),
        framed.next()
    )
        .await
        .map_err(|_| anyhow!("Read timed out"))?
        .ok_or_else(|| anyhow!("Connection closed"))??;

    // Deserialize response
    let (response) = postcard::from_bytes(
        &response_bytes,
    )?;

    Ok(response)
}


pub async fn execute_unmount(mount_point: String, server: SocketAddr, username:String) -> Result<()>{
    let client = VAULT_DRIVE_MAP
        .get(&(server, username.to_string()))
        .map(|entry| entry.clone())
        .ok_or_else(|| anyhow!("Client not found"))?;

    let host = client.mounts
        .remove(&mount_point);

    if let Some((_, (_, host_drive))) = host {
        let (connection_type, connection_point) = client.session.read().await
            .as_ref()
            .and_then(|session| session.hostname.as_ref())
            .map(|hostname| (ConnectionType::Hub, hostname.clone()))
            .unwrap_or_else(|| (ConnectionType::Direct, server.to_string()));

        let auto_run = auto_run::new(
            connection_type,
            connection_point,
            username,
            host_drive,
            "".to_string()
        );

        tokio::task::spawn_blocking(move || {
            remove(auto_run).context(format!("Failed to remove auto-mount {:?}", mount_point));

        }).await?;
    }

    Ok(())
}
pub async fn execute_set_auto_mount(mount_point: String, server: SocketAddr, username:String, drive: String) -> Result<()>{
    let client = VAULT_DRIVE_MAP
        .get(&(server, username.to_string()))
        .map(|entry| entry.clone())
        .ok_or_else(|| anyhow!("Client not found"))?;

    let (connection_type, connection_point) = client.session.read().await
        .as_ref()
        .and_then(|session| session.hostname.as_ref())
        .map(|hostname| (ConnectionType::Hub, hostname.clone()))
        .unwrap_or_else(|| (ConnectionType::Direct, server.to_string()));
    
    let auto_run = auto_run::new(connection_type, connection_point, username, drive, mount_point);
    tokio::task::spawn_blocking(move || {
        insert(auto_run).context(format!("Failed to remove auto-mount {:?}", connection_type));
    }).await?;
    Ok(())
}

pub async fn execute_un_auto_mount(drive:  String, server: SocketAddr, username:String) -> Result<()>{
    let client = VAULT_DRIVE_MAP
        .get(&(server, username.to_string()))
        .map(|entry| entry.clone())
        .ok_or_else(|| anyhow!("Client not found"))?;

    let (connection_type, connection_point) = client.session.read().await
        .as_ref()
        .and_then(|session| session.hostname.as_ref())
        .map(|hostname| (ConnectionType::Hub, hostname.clone()))
        .unwrap_or_else(|| (ConnectionType::Direct, server.to_string()));
    let auto_run = auto_run::new(connection_type, connection_point, username, drive, "".to_string());
    tokio::task::spawn_blocking(move || {
        remove(auto_run).context(format!("Failed to remove auto-mount {:?}", connection_type));
    }).await?;
    Ok(())
}

pub async fn execute_disconnect(username: String, server: SocketAddr) -> Result<()>{
    debug!("Disconnecting from vaultDrive: {:?} and username: {:?}", server, username);
    let client = VAULT_DRIVE_MAP
        .remove(&(server, username.to_string()))
        .map(|entry| entry.clone())
        .ok_or_else(|| anyhow!("Client not found"))?;

    // Have to call clear first because the the virtual file system own the ARC
    // if you dont clear before calling drop you will just decrement the arc and not drop the vale
    client.1.mounts.clear();

    drop(client);
    Ok(())
}






pub async fn execute_get_ui_connections() -> Result<Vec<Connection>> {
    let clients: Vec<Arc<VaultDriveClient>> =
        VAULT_DRIVE_MAP
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

    debug!("{:?} this is the client length", clients.len());

    if clients.is_empty() {
        return Ok(Vec::new());
    }

    let mut set = JoinSet::new();

    for client in clients {
        set.spawn(async move {
            let (volumes_result, mount_result, session_guard, server_addr_guard) = join!(
                client.list_volumes(),
                mount_to_UI_tuple(client.clone()),
                client.session.read(),
                client.server_addr.read()
            );

            let volumes = volumes_result.unwrap_or_default();
            let mountUITuple = mount_result.unwrap_or_default();
            let session = session_guard.clone().unwrap();
            let server_addr = server_addr_guard.clone();

            debug!("This is the mountUiTuple {:?}", mountUITuple.clone());
            drop(session_guard);
            tracing::debug!("{:?} this is the volumes length", volumes.len());
            tracing::debug!("{:?} this is the mountUITuple length", mountUITuple.len());

            let username = session.authenticate_request.user_id.clone();

            // Prepare all exists checks and volume data separately
            let mut exists_futures = Vec::new();
            let mut volume_data = Vec::new();

            for volume in volumes {
                let mount_info = mountUITuple.iter()
                    .find(|(_, label)| label.as_ref() == volume.root_path);

                let (path, label) = match mount_info {
                    Some((p, l)) => (Some(p.as_ref().to_string()), Some(l.as_ref().to_string())),
                    None => (None, None),
                };

                let (connection_type, connection_point) = if let Some(hostname) = session.hostname.clone() {
                    (ConnectionType::Hub, hostname.clone())
                } else {
                    (ConnectionType::Direct, server_addr.to_string())
                };

                let auto_run = auto_run::new(
                    connection_type,
                    connection_point,
                    username.clone(),
                    volume.root_path.clone(),
                    "".to_string(),
                );

                // Spawn the blocking task
                exists_futures.push(tokio::task::spawn_blocking(move || exists(auto_run)));

                // Store the volume data
                volume_data.push((volume, path, label));
            }

            // Await all exists checks concurrently
            let exists_results = join_all(exists_futures).await;

            // Build drives from results
            let drives: Vec<Drive> = volume_data.into_iter()
                .zip(exists_results.into_iter())
                .map(|((volume, path, label), exists_result)| {
                    let exist = match exists_result {
                        Ok(Ok(result)) => result,
                        Ok(Err(e)) => {
                            tracing::error!("exists check failed for {}: {:?}", volume.name, e);
                            false
                        }
                        Err(e) => {
                            tracing::error!("spawn_blocking failed for {}: {:?}", volume.name, e);
                            false
                        }
                    };

                    Drive::new(
                        &volume.name,
                        &volume.root_path,
                        path.as_deref().unwrap_or_default(),
                        "",
                        label.is_some(),
                        volume.available_space / 1073741824,
                        volume.total_size / 1073741824,
                        exist,
                    )
                })
                .collect();

            Connection::new(
                client.server_addr.read().await.clone(),
                drives,
                username,
                session.authenticate_request.host_name,
            )
        });
    }

    let mut connections = Vec::new();

    while let Some(result) = set.join_next().await {
        match result {
            Ok(conn) => connections.push(conn),
            Err(err) => {
                tracing::error!("task failed: {:?}", err);
                continue;
            }
        }
    }

    tracing::debug!("This is the connections {:?} ", connections);

    Ok(connections)
}


pub async fn connectionDirect( server: SocketAddr, username: &str, entry: &Entry) -> Result<SocketAddr> {
    tracing::debug!("Connecting to Direct Vault Drive for user {:?} socketaddr {:?}", username, server);
    let client = VaultDriveClient::new(server, username.parse()?).await?;

    let server_addr = client.connect(&username, entry, None).await?;
    
    
    Ok(server_addr)
}
pub async fn connectionHub( hostname: &str, username: &str,  entry: &Entry) -> Result<SocketAddr> {
    tracing::debug!("Connecting to hub Vault Drive for user {:?} ", username );

    //this is just used to make it default could make it an optional
    //but this should be the only time it is it really need an empty
    let defaultSockerAddr: SocketAddr = ZERO_ADDR;
    let client = VaultDriveClient::new(defaultSockerAddr, username.parse()?).await?;
    client.connect_to_hub( hostname).await?;
    let server_addr = client.connect(username, entry, Some(hostname.to_string())).await?;

    

    Ok(server_addr)

}

pub async fn execute_mount(
    mount_point: &str,
    server: SocketAddr,
    username: &str,
    drive: &str
) -> Result<()> {
    let client = VAULT_DRIVE_MAP
        .get(&(server, username.to_string()))
        .map(|entry| entry.clone())
        .ok_or_else(|| anyhow!("Client not found"))?;

    debug!("Executing mount {:?}", mount_point);

    let _mount = crate::filesystem::mount(client, mount_point, drive).await?;
    Ok(())
}





