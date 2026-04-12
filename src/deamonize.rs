use std::{env, io, process};
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::net::SocketAddr;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use std::thread::scope;
use anyhow::{anyhow, Context};
use bytes::Bytes;
use futures::{SinkExt, StreamExt, TryFutureExt};
use interprocess::local_socket::{GenericFilePath, ListenerOptions, ToFsName, };
use interprocess::local_socket::traits::tokio::{Listener, };
use log::{debug, info, warn};
use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::ServerConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use service_manager::*;
use tokio_rustls::TlsAcceptor;
#[cfg(windows)]
use windows::Win32::Security::PSID;
#[cfg(windows)]
use windows::Win32::Storage::FileSystem::FILE_GENERIC_READ;
use crate::autoRun::{init_db, get_connections_with_mounts};
use crate::client::VaultDriveClient;
use crate::commands::{connection_direct,  execute_socket_command, CommandResponse, ResponseData, SocketCommand};
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
                let result = async {
                    let (client, hostname_option) = match connection.connection_type {
                        ConnectionType::Direct => {
                            let socket_addr: SocketAddr = connection.connection_point.parse()?;
                            let client =
                                VaultDriveClient::new(socket_addr, connection.username.clone(), connection.scope).await?;
                            (client, None)
                        }
                        ConnectionType::Hub => {
                            let client =
                                VaultDriveClient::new(ZERO_ADDR, connection.username.clone(), connection.scope).await?;

                            let hub_addr = client.connect_to_hub().await?;
                            client.get_server_from_hub(&connection.connection_point, hub_addr).await?;
                            *client.server_addr.write().await = client.try_connect_or_relay(false, &connection.connection_point).await?;

                            (client, Some(connection.connection_point))
                        }
                    };

                    client
                        .reauthenticate(hostname_option.as_deref(), connection.username.as_str())
                        .await
                        .context("Reauthentication failed")?;

                    if let Some(mounts) = connection.mounts {
                        for mount in mounts {
                            if let Err(e) = crate::filesystem::mount(
                                Arc::clone(&client),
                                mount.host_drive.as_str(),
                                mount.mount_point.as_str(),
                                mount.scope,
                                mount.compress
                                
                            )
                                .await
                            {
                                tracing::error!("Failed to mount {}: {}", mount.host_drive, e);
                            }
                        }
                    }

                    Ok::<_, anyhow::Error>(())
                }
                    .await;

                if let Err(e) = result {
                    tracing::error!("Connection failed for {}: {}", connection.username, e);
                }
            });
        }
    });

    generate_and_save_keypair()?;

    let static_key = std::fs::read(DAEMON_PRIV_KEY_PATH)?;

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

        let static_key = static_key.clone();
        tokio::spawn(async move {
            let mut framed = Framed::new(stream, LengthDelimitedCodec::new());

            let noise = match noise_handshake(&mut framed, &static_key).await {
                Ok(n) => n,
                Err(e) => {
                    warn!("Handshake failed: {}", e);
                    return;
                }
            };

            handle_client(framed, noise).await;
        });
    }
}
pub const NOISE_PATTERN: &str = "Noise_NK_25519_ChaChaPoly_BLAKE2s";
#[cfg(unix)]
const DAEMON_PRIV_KEY_PATH: &str = "/etc/vaultdrive/daemon.key";
#[cfg(unix)]
pub const DAEMON_PUB_KEY_PATH: &str = "/etc/vaultdrive/daemon.pub";

#[cfg(windows)]
const DAEMON_PRIV_KEY_PATH: &str = r"C:\ProgramData\VaultDrive\daemon.key";
#[cfg(windows)]
pub const DAEMON_PUB_KEY_PATH: &str = r"C:\ProgramData\VaultDrive\daemon.pub";

pub fn generate_and_save_keypair() -> anyhow::Result<()> {
    if std::path::Path::new(DAEMON_PUB_KEY_PATH).exists()
        && std::path::Path::new(DAEMON_PRIV_KEY_PATH).exists()
    {
        return Ok(());
    }

    // Create the directory if it doesn't exist
    if let Some(parent) = std::path::Path::new(DAEMON_PRIV_KEY_PATH).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let builder = snow::Builder::new(NOISE_PATTERN.parse()?);
    let keypair = builder.generate_keypair()?;

    std::fs::write(DAEMON_PUB_KEY_PATH, &keypair.public)?;
    save_private_key(DAEMON_PRIV_KEY_PATH, &keypair.private)?;

    Ok(())
}
#[cfg(unix)]
fn save_private_key(path: &str, data: &[u8]) -> anyhow::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    use std::io::Write;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(data)?;
    Ok(())
}

#[cfg(windows)]
fn save_private_key(path: &str, data: &[u8]) -> anyhow::Result<()> {
    std::fs::write(path, data)?;

    fn icacls(path: &str, args: &[&str]) -> anyhow::Result<()> {
        let output = std::process::Command::new("icacls")
            .arg(path)
            .args(args)
            .output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("icacls failed: {}", stderr);
        }
        Ok(())
    }

    icacls(path, &["/inheritance:r"])?;
    icacls(path, &["/grant:r", "SYSTEM:(R)"])?;
    icacls(path, &["/grant:r", "Administrators:(R)"])?;

    Ok(())
}

async fn handle_client(
    mut framed: Framed<impl AsyncReadExt + AsyncWriteExt + Unpin, LengthDelimitedCodec>,
    mut noise: snow::TransportState,
) {
    let mut buf = vec![0u8; 65535];

    let encrypted = match framed.next().await {
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

    let len = match noise.read_message(&encrypted, &mut buf) {
        Ok(len) => len,
        Err(e) => {
            warn!("Failed to decrypt command: {}", e);
            return;
        }
    };

    let command: SocketCommand = match postcard::from_bytes(&buf[..len]) {
        Ok(cmd) => cmd,
        Err(e) => {
            warn!("Failed to deserialize command: {}", e);
            let error_response = CommandResponse::Error(format!("Deserialization error: {}", e));
            if let Ok(response_bytes) = postcard::to_allocvec(&error_response) {
                if let Ok(enc_len) = noise.write_message(&response_bytes, &mut buf) {
                    let _ = framed.send(Bytes::copy_from_slice(&buf[..enc_len])).await;
                }
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

    match noise.write_message(&response_bytes, &mut buf) {
        Ok(len) => {
            if let Err(e) = framed.send(Bytes::copy_from_slice(&buf[..len])).await {
                warn!("Failed to write response: {}", e);
            }
        }
        Err(e) => {
            warn!("Failed to encrypt response: {}", e);
        }
    }
}

async fn noise_handshake(
    framed: &mut Framed<impl AsyncReadExt + AsyncWriteExt + Unpin, LengthDelimitedCodec>,
    static_key: &[u8],
) -> anyhow::Result<snow::TransportState> {
    let mut buf = vec![0u8; 65535];

    let mut responder = snow::Builder::new(NOISE_PATTERN.parse()?)
        .local_private_key(static_key)?
        .build_responder()?;

    let msg = framed.next().await
        .ok_or_else(|| anyhow!("No handshake message"))??;
    responder.read_message(&msg, &mut buf)?;

    let len = responder.write_message(&[], &mut buf)?;
    framed.send(Bytes::copy_from_slice(&buf[..len])).await?;

    Ok(responder.into_transport_mode()?)
}

