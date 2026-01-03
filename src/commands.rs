use anyhow::{anyhow, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use std::fs;
use tracing;
use std::time::Duration;
use egui::TextBuffer;
use futures::{SinkExt, StreamExt};
use interprocess::local_socket;
use interprocess::local_socket::{GenericFilePath, ToFsName};
use interprocess::local_socket::traits::tokio::Stream;
use log::debug;
use tokio::join;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use keyring::Entry;
use serde::{Deserialize, Serialize};
use crate::auth::session;
use crate::client::{VAULT_DRIVE_MAP, VaultDriveClient};
use crate::driveManagerUI::{Connection, ConnectionType, Drive};
use crate::filesystem::{mount_to_UI_tuple, unmount};
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

}
#[derive(Debug, Serialize, Deserialize)]
pub enum ResponseData {
    Text(String),
    Empty,
    KeyData(String),
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
              Ok(_) => CommandResponse::Success(ResponseData::Empty),
              Err(e) => CommandResponse::Error(e.to_string()),
          }
        }

        SocketCommand::Mount {mount_point, socketaddr, username, drive} => {
            match execute_mount(&mount_point, socketaddr,  &username,  &drive).await {
                Ok(_) => CommandResponse::Success(ResponseData::Empty),
                Err(e) => CommandResponse::Error(e.to_string()),
            }
        }
        SocketCommand::UnMount { mount_point} => {
            match execute_unmount(mount_point).await {
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


pub async fn execute_unmount(mount_Point: String) -> Result<()>{
    unmount(&*mount_Point).await?;
    Ok(())
}





pub async fn execute_get_ui_connections() -> Result<Vec<Connection>> {
    let clients: Vec<Arc<VaultDriveClient>> =
        VAULT_DRIVE_MAP
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

    debug!("{:?} this is the client length", clients.len());

    let mut set = JoinSet::new();

    for client in clients {
        set.spawn(async move {
            let (volumes_result, mount_result, session_guard) = join!(
                client.list_volumes(),
                mount_to_UI_tuple(),
                client.session.read()
            );

            let volumes = volumes_result.unwrap_or_default();
            let mountUITuple = mount_result.unwrap_or_default();
            let session = session_guard.clone().unwrap();

            debug!("This is the mountUiTuple {:?}", mountUITuple.clone());
            drop(session_guard);
            tracing::debug!("{:?} this is the volumes length", volumes.len());
            tracing::debug!("{:?} this is the mountUITuple length", mountUITuple.len());

            let username =  session.authenticate_request.user_id;
            let drives: Vec<Drive> = volumes.into_iter()
                .map(|volume| {
                    let mount_info = mountUITuple.iter()
                        .find(|(_, label)| label.as_ref() == volume.root_path);


                    let (path, label) = match mount_info {

                        Some((p, l)) => (Some(p.as_ref()), Some(l.as_ref())),
                        None => (None, None),
                    };

                    Drive::new(
                        &volume.name,
                        &volume.root_path,
                        path.unwrap_or_default(),
                        "",
                        label.is_some(),
                        volume.available_space / 1073741824,
                        volume.total_size / 1073741824,
                    )
                })
                .collect();

            Connection::new(client.server_addr.read().await.clone() , drives, username, session.authenticate_request.host_name)
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


pub async fn connectionDirect( server: SocketAddr, username: &str, entry: &Entry) -> Result<()> {
    tracing::debug!("Connecting to Direct Vault Drive for user {:?} socketaddr {:?}", username, server);
    let client = VaultDriveClient::new(server, username.parse()?).await?;

    client.clone().connect(&username, entry, None).await?;
    
    
    Ok(())
}
pub async fn connectionHub( hostname: &str, username: &str,  entry: &Entry) -> Result<()> {
    tracing::debug!("Connecting to hub Vault Drive for user {:?} ", username );

    //this is just used to make it default could make it an optional
    //but this should be the only time it is it really need an empty
    let defaultSockerAddr: SocketAddr = ZERO_ADDR;
    let client = VaultDriveClient::new(defaultSockerAddr, username.parse()?).await?;
    client.connect_to_hub( hostname).await?;
    client.connect(username, entry, Some(hostname.to_string())).await?;
    

    Ok(())

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





