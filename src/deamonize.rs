use std::{env, io, process};
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::net::SocketAddr;
use std::str::FromStr;
use std::thread::scope;
use futures::{SinkExt, StreamExt, TryFutureExt};
use interprocess::local_socket::{GenericFilePath, ListenerOptions, ToFsName, };
use interprocess::local_socket::traits::tokio::{Listener, };
use log::{debug, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use service_manager::*;
use crate::autoRun::{init_db, get_connections_with_mounts};
use crate::client::VaultDriveClient;
use crate::commands::{connectionDirect, connectionHub, execute_socket_command, CommandResponse, ResponseData, SocketCommand};
use crate::driveManagerUI::ConnectionType;
use crate::network::ZERO_ADDR;
pub const SERVICE_NAME: &str = "com.vaultDrive_client.service";
pub fn is_daemon_process() -> bool {
   if  std::env::var("IS_DAEMON").is_ok() || std::env::args().any(|arg| arg == "--service"){
       return true;
   }
    false

}

fn start_service() -> io::Result<()> {
    let label: ServiceLabel = ServiceLabel::from_str(SERVICE_NAME)?;

    // Get generic service by detecting what is available on the platform
    let manager = <dyn ServiceManager>::native()?;

    let exe_path = std::env::current_exe()?;
    debug!("{:?}", exe_path);





    if  service_status()? == ServiceStatus::NotInstalled {

        let username = if cfg!(windows) {
            Some("NT AUTHORITY\\NetworkService".to_string())
        } else {
            None
        };

        info!("service is not installed, Installing...");
        manager.install(ServiceInstallCtx {
            label: label.clone(),
            program: exe_path,
            args: vec![OsString::from("--service")],
            contents: None,
             username,
            working_directory: Some(std::env::current_dir()?),
            environment: Some(vec![
                ("IS_DAEMON".to_string(), "1".to_string()),
                ("IS_SERVICE".to_string(), "1".to_string()),
            ]), // Optional list of environment variables to supply the service process.
            autostart: true,
            restart_policy: RestartPolicy::default(),
        })?;
    }

    if service_status()? != ServiceStatus::Running {
        info!("service is not running, Running...");
        // Start our service using the underlying service management platform
        match manager.start(ServiceStartCtx {
            label: label.clone()
        }) {
            Ok(_) => {
                info!("service is now running");
                Ok(())},
            Err(err) => {debug!("There was an error thrown on start {:?}", err);
                Err(err)

            },
        }
    }else {
        info!("service is already running");
        Ok(())
    }
}

fn stop_service() -> io::Result<()> {
    info!("stopping service");
    let label: ServiceLabel = ServiceLabel::from_str(SERVICE_NAME)?;
    let manager = <dyn ServiceManager>::native()
        .expect("Failed to detect management platform");
    manager.stop(ServiceStopCtx {
        label: label.clone()
    })

}
fn uninstall_service() -> io::Result<()> {
    info!("uninstalling service");
    let label: ServiceLabel = ServiceLabel::from_str(SERVICE_NAME)?;
    let manager = <dyn ServiceManager>::native()
        .expect("Failed to detect management platform");
    manager.uninstall(ServiceUninstallCtx {
        label: label.clone()
    })
}
fn service_manager_available() -> bool {
    let service_manager = match <dyn ServiceManager>::native() {
        Ok(sm) => sm,
        Err(_) => return false,
    };

    service_manager.available().unwrap_or(false)
}
fn service_status() -> std::io::Result<ServiceStatus> {
    let label: ServiceLabel = ServiceLabel::from_str(SERVICE_NAME)?;
    let manager = <dyn ServiceManager>::native()?;

    manager.status(ServiceStatusCtx {
        label: label.clone()
    })
}

pub fn is_daemon_running() -> io::Result<bool> {
    match service_status(){
        Ok(status) => {
            match status {
                ServiceStatus::Running => { Ok(true) },
                _ => Ok(false)
            }

        }
        _ => {Ok(false)}
    }
}

pub async fn restart_process_daemon() -> io::Result<()> {
    match service_status() {
        Ok(status) => {
            match status {
                ServiceStatus::NotInstalled => {
                    if !service_manager_available() {
                        info!("Unable to start daemon as a service ");

                    }

                    start_service()

                }
                ServiceStatus::Running => {
                    stop_service()?;
                    start_service()
                }
                ServiceStatus::Stopped(_) => {
                    start_service()
                }
            }
        }
        Err(e) => Err(e)
    }
}


pub(crate) async fn run_daemon_server() -> anyhow::Result<()> {

    tokio::spawn(async {
        init_db().await.ok();

        let connections = match get_connections_with_mounts().await {
            Ok(entries) => entries,
            Err(_) => return,
        };

        for connection in connections {
            tokio::spawn(async move {
                let client = if connection.connection_type == ConnectionType::Direct {
                    let socket_addr: SocketAddr = match connection.connection_point.parse() {
                        Ok(socket) => socket,
                        Err(e) => {
                            tracing::error!("Failed to parse socket address: {}", e);
                            return;
                        }
                    };
                    match VaultDriveClient::new(socket_addr, connection.username, connection.scope).await {
                        Ok(client) => client,
                        Err(e) => {
                            tracing::error!("Failed to create client: {}", e);
                            return;
                        }
                    }
                } else {
                    let default_socket_addr: SocketAddr = ZERO_ADDR;
                    let client = match VaultDriveClient::new(default_socket_addr, connection.username, connection.scope).await {
                        Ok(client) => client,
                        Err(e) => {
                            tracing::error!("Failed to create hub client: {}", e);
                            return;
                        }
                    };
                    if let Err(e) = client.connect_to_hub(connection.connection_point.as_str()).await {
                        tracing::error!("Failed to connect to hub: {}", e);
                        return;
                    }
                    client
                };

                let session = client.session.read().await
                    .as_ref()
                    .expect("Session is None")
                    .clone();

                if let Err(e) = client.reauthenticate(session).await {
                    tracing::error!("Reauthentication failed: {}", e);
                    return;
                }

                if let Some(mounts) = connection.mounts {
                    for mount in mounts {
                        if let Err(e) = crate::filesystem::mount(
                            client.clone(),
                            mount.host_drive.as_str(),
                            mount.mount_point.as_str(),
                            mount.scope,
                        )
                            .await
                        {
                            tracing::error!("Failed to mount {}: {}", mount.host_drive, e);
                        }
                    }
                }
            });
        }
    });


    #[cfg(unix)]
    let path_str = "/tmp/vaultDriveClient.sock";

    #[cfg(windows)]
    let path_str = r"\\.\pipe\vaultDriveClient";

    let name = path_str.to_fs_name::<GenericFilePath>()?;



    let listener = ListenerOptions::new()
        .name(name)
        .reclaim_name(true)
        .try_overwrite(true)
        .create_tokio()?;


    info!("Daemon listening on {}", path_str);

    loop {
        let stream = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to accept connection: {}", e);
                continue;
            }
        };

        tokio::spawn(async move {
            handle_client(stream).await;
        });
    }
}

async fn handle_client(stream: impl AsyncReadExt + AsyncWriteExt + Unpin ){
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());

    let command_bytes = match framed.next().await {
        Some(Ok(bytes)) => bytes,
        Some(Err(e)) => {
            warn!("Failed to read command: {}", e);
            return;
        }
        None => {
            debug!("Connection closed");
            return;
        }
    };

    let (command) = match postcard::from_bytes(&command_bytes) {
        Ok(cmd) => cmd,
        Err(e) => {
            warn!("Failed to deserialize command: {}", e);
            let error_response = CommandResponse::Error(format!("Deserialization error: {}", e));
            if let Ok(response_bytes) = postcard::to_allocvec(&error_response) {
                let _ = framed.send(response_bytes.into()).await;
            }
            return;
        }
    };

    let response = execute_socket_command(command).await;

    let response_bytes = match postcard::to_allocvec(&response) {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!("Failed to serialize response: {}", e);
            return;
        }
    };

    if let Err(e) = framed.send(response_bytes.into()).await {
        warn!("Failed to write response: {}", e);
    }
}