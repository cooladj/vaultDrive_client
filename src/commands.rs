use anyhow::{anyhow, Context, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread::scope;
use tracing;
use std::time::Duration;
use bytes::Bytes;
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
use serde::{Deserialize, Serialize};

use crate::auth::session;
use crate::autoRun::{ get_scope_and_compress, insert_mount, mounts, remove_connection, remove_mount, update_scope_and_compress};
use crate::client::{VAULT_DRIVE_MAP, VaultDriveClient};
use crate::deamonize::{DAEMON_PUB_KEY_PATH, NOISE_PATTERN};
use crate::driveManagerUI::{Connection, ConnectionType, Drive, ScopeType};
use crate::filesystem::{mount_to_UI_tuple};
use crate::network::{ ZERO_ADDR};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SocketCommand {

    Connect{
        connection_type: ConnectionType,
        connection_point:String,
        username: String,
        password: String,
        scope_type: ScopeType,
        tofu_enable: bool,
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
        scope: String,
        compress: bool,

    },
    Volumes {
        user_id: String,
        connection: Option<Connection>,
        elevated : bool,
    },
    Health,

    Disconnect{
        username:String,
        socketaddr: SocketAddr,
    },
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
        SocketCommand::Connect { connection_type: connection_method, connection_point,username, password, scope_type, tofu_enable} => {


          let operation =  if connection_method == ConnectionType::Direct{
              let sockerAdder:SocketAddr = match   connection_point.parse(){
                  Ok(socket) => socket,
                  Err(e) => return CommandResponse::Error(e.to_string()),
              };

                  connection_direct(sockerAdder, username.as_str(), password, scope_type, tofu_enable).await
            }else {
              connection_hub( &connection_point, username.as_str(),password, scope_type, tofu_enable).await
          };
          match operation {
              Ok(socket_addr) => CommandResponse::Success(ResponseData::Text(socket_addr.to_string())),
              Err(e) => CommandResponse::Error(e.to_string()),
          }
        }

        SocketCommand::Mount {mount_point, socketaddr, username, drive,  scope, compress} => {
            match execute_mount(&mount_point, socketaddr,  &username,  &drive, scope, compress).await {
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
        SocketCommand::Volumes { connection, user_id,elevated }  => {
            match connection {
                None => {
                    match execute_get_ui_connections(user_id, elevated).await {
                    Ok(volumes) => CommandResponse::Success(ResponseData::VolumesInfoData(volumes)),
                    Err(e) => CommandResponse::Error(e.to_string()),
                }}
                Some(connection) => {
                    // todo instead of refreshing all connection refresh one
                    match execute_get_ui_connections(user_id, elevated).await {
                        Ok(volumes) => CommandResponse::Success(ResponseData::VolumesInfoData(volumes)),
                        Err(e) => CommandResponse::Error(e.to_string()),
                    }}
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


#[cfg(windows)]
const TIMEOUT_SECS: u64 = 5;

pub async fn send_command_to_daemon(command: SocketCommand) -> Result<CommandResponse> {
    #[cfg(unix)]
    let path_str = "/tmp/vaultDriveClient.sock";
    #[cfg(windows)]
    let path_str = r"\\.\pipe\vaultDriveClient";

    let name = path_str.to_fs_name::<GenericFilePath>()?;

    let stream = match local_socket::tokio::Stream::connect(name).await {
        Ok(s) => s,
        Err(e) => {
            tracing::debug!("Failed to connect to daemon: {:?}", e.kind());
            return Ok(CommandResponse::Error(e.to_string()));
        }
    };

    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());

    let daemon_pub = std::fs::read(DAEMON_PUB_KEY_PATH)?;
    let params: snow::params::NoiseParams = NOISE_PATTERN.parse()?;
    let mut initiator = snow::Builder::new(params)
        .remote_public_key(&daemon_pub)?
        .build_initiator()?;

    let mut buf = vec![0u8; 65535];

    // -> e, es
    let len = initiator.write_message(&[], &mut buf)?;
    timeout(
        Duration::from_secs(TIMEOUT_SECS),
        framed.send(Bytes::copy_from_slice(&buf[..len]))
    ).await.map_err(|_| anyhow!("Handshake write timed out"))??;

    // <- e, ee
    let msg = timeout(
        Duration::from_secs(TIMEOUT_SECS),
        framed.next()
    ).await
        .map_err(|_| anyhow!("Handshake read timed out"))?
        .ok_or_else(|| anyhow!("Connection closed during handshake"))??;
    initiator.read_message(&msg, &mut buf)?;

    let mut noise = initiator.into_transport_mode()?;

    let command_bytes = postcard::to_allocvec(&command)?;
    let len = noise.write_message(&command_bytes, &mut buf)?;
    timeout(
        Duration::from_secs(TIMEOUT_SECS),
        framed.send(Bytes::copy_from_slice(&buf[..len]))
    ).await.map_err(|_| anyhow!("Write timed out"))??;

    let encrypted = timeout(
        Duration::from_secs(TIMEOUT_SECS),
        framed.next()
    ).await
        .map_err(|_| anyhow!("Read timed out"))?
        .ok_or_else(|| anyhow!("Connection closed"))??;

    let len = noise.read_message(&encrypted, &mut buf)?;
    let response: CommandResponse = postcard::from_bytes(&buf[..len])?;

    Ok(response)
}


pub async fn execute_unmount(mount_point: String, server: SocketAddr, username:String) -> Result<()>{
    let client = VAULT_DRIVE_MAP
        .get(&(server, username.to_string()))
        .map(|entry| entry.clone())
        .ok_or_else(|| anyhow!("Client not found"))?;

    let host = client.mounts
        .remove(&mount_point);

    if let Some((_, (_, host_drive, _ ,_))) = host {
        let (connection_type, connection_point) = client.session.read().await
            .as_ref()
            .and_then(|session| session.hostname.as_ref())
            .map(|hostname| (ConnectionType::Hub, hostname.clone()))
            .unwrap_or_else(|| (ConnectionType::Direct, server.to_string()));



            remove_mount(connection_type, &connection_point, &username, &mount_point).await?;
    }

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
    let (connection_type,connection_point )=client.1.session.read().await
        .as_ref()
        .and_then(|session| session.hostname.as_ref())
        .map(|hostname| (ConnectionType::Hub, hostname.clone()))
        .unwrap_or_else(|| (ConnectionType::Direct, server.to_string()));

   if let Some(connection_id) = remove_connection(connection_type, connection_point, &username).await?{
        client.1.disconnect(connection_id).await?;

    };

    drop(client);
    Ok(())
}






pub async fn execute_get_ui_connections(user_id: String, elevated: bool) -> Result<Vec<Connection>> {

    let clients: Vec<Arc<VaultDriveClient>> = VAULT_DRIVE_MAP
        .iter()
        .filter(|entry| {
            entry.value().scope == user_id || (elevated && entry.value().scope == "")
        })
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

            debug!("This is the mountUiTuple {:?}", mountUITuple.clone());
            drop(session_guard);
            tracing::debug!("{:?} this is the volumes length", volumes.len());
            tracing::debug!("{:?} this is the mountUITuple length", mountUITuple.len());

            let username = session.authenticate_request.user_id.clone();

            let mut volume_data = Vec::new();

            for volume in volumes {
                let mount_info = mountUITuple.iter()
                    .find(|(_, label)| label.as_ref() == volume.root_path);

                let (path, label) = match mount_info {
                    Some((p, l)) => (Some(p.as_ref().to_string()), Some(l.as_ref().to_string())),
                    None => (None, None),
                };
                let (scope, compress) = if let Some(path) = path.clone() {
                    if let Some(entry) = client.mounts.get(&path) {
                        let (_, _, scope_rwlock, atomic_compress) = entry.value();
                        let scope_val = scope_rwlock.read().clone();
                        let compress_val = atomic_compress.load(Ordering::Relaxed);
                        (scope_val, compress_val)
                    } else {
                        (Default::default(), false)
                    }
                } else {
                    (Default::default(), false)
                };

                volume_data.push((volume, path, label, scope, compress ));
            }


            let drives: Vec<Drive> = volume_data.into_iter()
                .map(|((volume, path, label, scope, compress))| {


                    Drive::new(
                        &volume.name,
                        &volume.root_path,
                        path.as_deref().unwrap_or_default(),
                        "",
                        label.is_some(),
                        volume.available_space / 1073741824,
                        volume.total_size / 1073741824,
                        scope,
                        compress,
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


pub async fn connection_direct(server: SocketAddr, username: &str, password: String, scope_type: ScopeType, tofu_enable: bool) -> Result<SocketAddr> {
    tracing::debug!("Connecting to Direct Vault Drive for user {:?} socketaddr {:?}", username, server);
    let client = VaultDriveClient::new(server, username.parse()?, scope_type.to_string())
        .await.inspect_err(|e| tracing::error!("VaultDriveClient::new failed: {:?}", e))?;
    tracing::debug!("Client was made");
    let server_addr = client.connect(&username, tofu_enable).await?;

    let authenticate_request = client.authenticate_internal(username, password, None).await
        .context("Authentication failed")?;


    let session : session = session{
        hostname: None,
        authenticate_request,
    };


    *client.session.write().await = Some(session.clone());

    VAULT_DRIVE_MAP.insert((server_addr, session.authenticate_request.user_id), client);
    tracing::debug!("{:?} this is the VAULT DRIVE CLIENT MAP", VAULT_DRIVE_MAP.len());
    
    
    Ok(server_addr)
}
pub async fn connection_hub(
    hostname: &str,
    username: &str,
    password: String,
    scope_type: ScopeType,
    tofu_enable: bool,
) -> Result<SocketAddr> {
    tracing::debug!("Connecting to hub Vault Drive for user {:?}", username);

    let client = Arc::new(
        VaultDriveClient::new(ZERO_ADDR, username.parse()?, scope_type.to_string()).await?
    );

    // 1. Connect to hub and resolve the target server's address
    let hub_addr = client.connect_to_hub().await?;
    client.get_server_from_hub(hostname, hub_addr).await?;

    // 2. Attempt direct connection, falling back to relay if needed
    let server_addr = client
        .try_connect_or_relay( tofu_enable, hostname)
        .await?;

    // 3. Authenticate and store session
    let authenticate_request = client
        .authenticate_internal(username, password, Some(hostname))
        .await
        .context("Authentication failed")?;

    let session = session {
        hostname: Some(hostname.to_string()),
        authenticate_request: authenticate_request.clone(),
    };

    *client.session.write().await = Some(session);

    VAULT_DRIVE_MAP.insert(
        (server_addr, authenticate_request.user_id),
        Arc::clone(&client),
    );

    Ok(server_addr)
}

pub async fn execute_mount(
    mount_point: &str,
    server: SocketAddr,
    username: &str,
    drive: &str,
    scope: String,
    compress: bool
) -> Result<()> {
    let client = VAULT_DRIVE_MAP
        .get(&(server, username.to_string()))
        .map(|entry| entry.clone())
        .ok_or_else(|| anyhow!("Client not found"))?;
    debug!("Executing mount {:?}", mount_point);
    debug!("mounts: {:?}", client.mounts.iter().map(|entry| entry.key().clone()).collect::<Vec<_>>());
    let (connection_type, connection_point) = client.session.read().await
        .as_ref()
        .and_then(|session| session.hostname.as_ref())
        .map(|hostname| (ConnectionType::Hub, hostname.clone()))
        .unwrap_or_else(|| (ConnectionType::Direct, server.to_string()));
    if let Some(entry) = client.mounts.get(mount_point) {
        let (_, _, rwlock_scope, atomic_compress) = entry.value();

        *rwlock_scope.write() = scope.clone();
        atomic_compress.store(compress, Ordering::Relaxed);
        update_scope_and_compress(
            connection_type,
            &connection_point,
            username,
            &mount_point,
            &drive,
            &scope,
            compress
        )
            .await?;

        return Ok(());
    }



    debug!("Executing mount {:?}", mount_point);

    let _mount = crate::filesystem::mount(client, mount_point, drive, scope.clone(), compress).await?;
    insert_mount(connection_type, &connection_point, &username, &mount_point, &drive, &compress, &scope).await?;

    Ok(())
}





