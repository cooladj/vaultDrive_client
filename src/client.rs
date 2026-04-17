use anyhow::{Context, Result, bail, anyhow};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use bytes::Bytes;
use dashmap::{DashMap};

use log::info;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::join;
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::debug;
#[cfg(windows)]
use winfsp::host::FileSystemHost;
#[cfg(windows)]
use crate::filesystem::winfsp::{VirtualFileSystem};

use quinn::{Connection, RecvStream, SendStream};
use crate::auth::{authenticate, reauthenticate, session};
use crate::autoRun::{connection, remove_connection};
use crate::commands::connection_hub;
use crate::driveManagerUI::{ConnectionType};
use crate::network::{QuicClient, read_message, write_message, ZERO_ADDR};
use crate::proto::hub;
use crate::proto::hub::hub_request::RequestType;
use crate::proto::hub::{ClientHelloRequest, ClientHelloResponse, ClientRelayRequest, ClientRelayResponse};
use crate::proto::vaultdrive::*;

#[cfg(windows)]
type MOUNT_TYPES = Mutex<FileSystemHost<VirtualFileSystem>>;
#[cfg(unix)]
type MOUNT_TYPES = Arc<Mutex<fuser::BackgroundSession>>;


/// VaultDrive client
pub struct VaultDriveClient {
    pub quic: &'static QuicClient,
    pub session: Arc<RwLock<Option<session>>>,
    pub server_addr: RwLock<SocketAddr>,
    pub secondary_server_addr: RwLock<Option<SocketAddr>>,

    ///local mount point , (mount thread handle, host mount point, Scope, Compression)
    pub mounts: DashMap<String, (MOUNT_TYPES, String, Arc<parking_lot::RwLock<String>>, Arc<AtomicBool>)>,
    pub scope: String,

}

pub static VAULT_DRIVE_MAP: Lazy<DashMap<(SocketAddr, String), Arc<VaultDriveClient>>> =
    Lazy::new(|| DashMap::new());



impl VaultDriveClient {
    pub async fn new(server_addr: SocketAddr, username: String, scope: String) -> Result<Arc<Self>> {
        if let Some(existing) = VAULT_DRIVE_MAP.get(&(server_addr, username)) {
            return Ok(existing.clone());
        }

        let quic = QuicClient::new().await?;
        let client = Arc::new(Self {
            quic,
            session: Arc::new(RwLock::new(None)),
            server_addr: RwLock::new(server_addr),
            secondary_server_addr: RwLock::new(None),
            mounts: DashMap::new(),
            scope,
        });

        Ok(client)
    }

    pub async fn renew_connection(self: Arc<Self>) -> Result<SocketAddr> {
        debug!("Starting renew connection");

        let session = self.session.read().await
            .as_ref()
            .expect("Session is None")
            .clone();

        let user_id = session.authenticate_request.user_id.clone();
        let old_addr = *self.server_addr.read().await;
        let old_connection = self.quic.connection.remove(&old_addr);

        let result = {
            let inner = async {
                if let Some(hostname) = &session.hostname {
                    debug!("Renew using hostname: {}", hostname);
                    let hub_addr = self.connect_to_hub().await?;
                    self.get_server_from_hub(hostname, hub_addr).await?;

                    let new_addr = self
                        .try_connect_or_relay(true, hostname)
                        .await?;

                    if old_addr != new_addr {
                        VAULT_DRIVE_MAP.insert((new_addr, user_id.clone()), self.clone());
                        VAULT_DRIVE_MAP.remove(&(old_addr, user_id.clone()));
                        debug!("Performing renew connection clean up");
                    }
                    Ok(new_addr)
                } else {
                    debug!("Connecting directly to addr: {}", old_addr);
                    self.connect(&session.authenticate_request.user_id, true).await?;
                    Ok(old_addr)
                }
            }.await;

            match inner {
                Ok(addr) => addr,
                Err(e) => {
                    Self::restore_old_conn(self.quic, old_connection);
                    return Err(e);
                }
            }
        };



        let reauth = self.reauthenticate(session.hostname.as_deref(), &session.authenticate_request.user_id).await
            .context("Authentication failed")?;

        debug!("Authentication succeeded for {}", reauth.host_name);

        Ok(result)
    }

    fn restore_old_conn(
        quic: &QuicClient,
        old_connection: Option<(SocketAddr, Connection)>,
    ) {
        if let Some(old_conn) = old_connection {
            quic.connection.insert(old_conn.0, old_conn.1);
        }
    }



    pub async fn try_connect_or_relay(
        self: &Arc<Self>,
        tofu_enable: bool,
        hostname: &str,
    ) -> Result<SocketAddr> {
        let primary = *self.server_addr.read().await;

        // 1. Try primary
        if self.quic.connect(&primary, Some(tofu_enable), None, None).await.is_ok() {
            return Ok(primary);
        }
        tracing::debug!("Primary connection failed, trying secondary");

        // 2. Try secondary
        let secondary = *self.secondary_server_addr.read().await;
        if let Some(sec_addr) = secondary {
            if self.quic.connect(&sec_addr, Some(tofu_enable), None, None).await.is_ok() {
                *self.server_addr.write().await = sec_addr;
                return Ok(sec_addr);
            }
            tracing::debug!("Secondary connection failed, falling back to hub relay");
        }

        // 3. Both failed (or no secondary) — use hub relay
        self.connect_via_relay( tofu_enable, hostname.to_string(), primary).await
    }

    async fn connect_via_relay(
        self: &Arc<Self>,
        tofu_enable: bool,
        hostname: String,
        server_ip: SocketAddr,
    ) -> Result<SocketAddr> {
        // Re-establish hub connection for the relay request
        let hub_addr = self.connect_to_hub().await?;

        let (mut send, mut recv) = self.quic.open_bi_safe(hub_addr).await?;

        let relay_req = hub::HubRequest {
            request_type: Some(RequestType::ClientRelayReq(ClientRelayRequest { hostname })),
        };
        write_message(&mut send, &relay_req).await?;

        let relay_response: ClientRelayResponse = timeout(
            Duration::from_secs(10),
            read_message(&mut recv),
        )
            .await
            .context("Timeout waiting for relay response from hub")?
            .context("Failed to read relay response")?;

        let token = relay_response
            .token
            .context("Hub relay succeeded but returned no token")?;
        let socket_addr = if let Some(socketaddr) = relay_response.socket_addr{
            socketaddr.parse().unwrap_or(ZERO_ADDR)
        }else {
            ZERO_ADDR
        };


        self.quic
            .connect(&socket_addr, Some(tofu_enable), Some(token), Some(server_ip))
            .await
            .context("Failed to connect to remote server via hub relay")?;

        *self.server_addr.write().await = server_ip;

        Ok(server_ip)
    }


    pub async fn connect(self: &Arc<Self>, username: &str,  tofu_enable:  bool) -> Result<SocketAddr> {
        tracing::debug!("Connecting to Vault Drive for user {:?}", username);
        let server_addr = *self.server_addr.read().await;

         self.quic.connect(&server_addr, Some(tofu_enable), None, None).await?;

        Ok(server_addr)
    }




    pub async fn connect_to_hub(&self) -> Result<SocketAddr> {
        let socket_addr: SocketAddr = ZERO_ADDR;

        let (addr, conn) = self.quic.connect(&socket_addr, None, None, None).await
            .context("Failed to connect to hub")?;

        let connection_map = self.quic.connection.clone();
        tokio::spawn(async move {
            conn.closed().await;
            connection_map.remove(&socket_addr);
        });

        Ok(addr)
    }


    
   pub async fn get_server_from_hub(&self, hostname: &str, addr: SocketAddr) -> Result<()> {

        let (mut send, mut recv) = self.quic.open_bi_safe(addr).await?;
       let port = match self.quic.endpoint.local_addr().ok() {
           None => { None }
           Some(sock) => { Some(sock.port()) }
       };


       let private_socket_addr = port
           .and_then(|p| local_ip_address::local_ip().ok().map(|ip| format!("{}:{}", ip, p)));


        let init_req = hub::HubRequest {
            request_type: Some(RequestType::ClientHelloReq(
                ClientHelloRequest {
                    unique_id: hostname.parse()
                        .context("Failed to parse hostname")?,
                    private_socket_addr
                }
            ))
        };


        write_message(&mut send, &init_req).await?;
        send.finish()?;

        let response: ClientHelloResponse = timeout(
            Duration::from_secs(10),
            read_message(&mut recv)
        )
            .await
            .context("Timeout waiting for response from hub")?
            .context("Failed to read response")?;

       if !response.found {
           bail!("Failed to connect to server: {}", hostname);
       }


       let primary = response.socket_addr
           .ok_or_else(|| anyhow!("Server found but no address provided"))?;

       let server_addr: SocketAddr = match primary.parse() {
           Ok(addr) => {
               // Primary worked — store secondary if available
               if let Some(ref sec) = response.secondary_socket_addr {
                   if let Ok(sec_addr) = sec.parse::<SocketAddr>() {
                       *self.secondary_server_addr.write().await = Some(sec_addr);
                   }
               }
               addr
           }
           Err(_) => {
               // Primary failed to parse — try secondary
               let sec = response.secondary_socket_addr
                   .ok_or_else(|| anyhow!("Primary address invalid and no secondary available"))?;
               sec.parse().context("Both primary and secondary addresses failed to parse")?
           }
       };

       *self.server_addr.write().await = server_addr;


        Ok(())
    }
    pub(crate) async fn reauthenticate(&self, hostname: Option<&str>, username: &str) -> Result<AuthenticationSuccessResponse>{
        let server_addr = *self.server_addr.read().await;

        tracing::debug!("reauthenticate: Opening bidirectional stream to {:?}", server_addr);
        let (mut send, mut recv) = self.quic.open_bi_safe(server_addr).await
            .context("Failed to open bidirectional stream")?;
        let (connection_type ,connection_point) = match hostname {
            None => { (ConnectionType::Direct,server_addr.to_string() )}
            Some(hostname) => {(ConnectionType::Hub,hostname.to_string())}
        };


        let result = reauthenticate(&mut send, &mut recv, username, connection_type, connection_point.as_str(),self.scope.as_str()).await;
        if result.is_err(){
            remove_connection(connection_type,connection_point, username);

        }else {
            self.quic.reAuth.notify_waiters();
        }
        result

    }

    pub(crate) async fn authenticate_internal(&self, username: &str, password: String, hostname: Option<&str>) -> Result<AuthenticationSuccessResponse> {
        tracing::debug!("authenticate_internal: Getting server address");
        let server_addr = *self.server_addr.read().await;


        tracing::debug!("authenticate_internal: Opening bidirectional stream to {:?}", server_addr);
        let (mut send, mut recv) = self.quic.open_bi(server_addr.clone(), username.to_string()).await
            .context("Failed to open bidirectional stream")?;

        tracing::debug!("this is a new log");

        tracing::debug!("authenticate_internal: Stream opened successfully, calling authenticate");
        tracing::debug!("this is a new log");

        let (connection_type, connection_point) = if let Some(hostname) = hostname {
            tracing::debug!("authenticate_internal: using hub connection");
            (ConnectionType::Hub, hostname.to_string())
        } else {
            tracing::debug!("authenticate_internal: using direct connection");
            (ConnectionType::Direct, server_addr.to_string())
        };
        tracing::debug!("authenticate_internal: building connection row");

        tracing::debug!("authenticate_internal: key generated");

        let connection_row = connection{
            connection_id: uuid::Uuid::new_v4().to_string(),
            connection_type,
            connection_point,
            username: username.to_string(),
            scope: self.scope.clone(),
            key: Vec::new(),
            mounts: None,
        };

        debug!("sending authenticate");

        let result = authenticate(&mut send, &mut recv, username, password, connection_row).await;

        debug!("failed to authenticate");


        result
    }





    async fn execute_request(&self, request: Request, stream: Option<(SendStream, RecvStream)>) -> Result<Response> {


        let (mut send, mut recv) = if let Some((send, recv)) = stream {
            (send, recv)
        } else {
            let (server_addr_guard, session_guard) = join!(
    self.server_addr.read(),
    self.session.read()
);

            let server_addr = *server_addr_guard;
            let username = session_guard.as_ref().unwrap().authenticate_request.user_id.clone();
            drop(session_guard);
            drop(server_addr_guard);
            self.quic.open_bi(server_addr, username).await?
        };


        write_message(&mut send, &request).await?;
        let response = read_message::<Response>(&mut recv).await?;

        // Check for errors
        if let Some(response::ResponseType::Error(err)) = &response.response_type {
            bail!("Server error: {}", err.message);
        }

        Ok(response)
    }

    
    pub async fn open_file(&self, path: &str, file_id: u64, flags: u32) -> Result<OperationSuccessResponse> {
        let request = Request {
            request_type: Some(request::RequestType::OpenFile(OpenFileRequest {
                path: path.to_string(),
                file_id,
                flags,
            })),
        };
        let response = self.execute_request(request, None).await?;
        match response.response_type {
            Some(response::ResponseType::OperationSuccess(operationSuccess)) => Ok(operationSuccess),
            _ => bail!("Unexpected response type for delete file"),
        }
    }


    pub async fn list_directory(&self, path: &str) -> Result<Vec<DirectoryEntry>> {
        let request = Request {
            request_type: Some(request::RequestType::ListDirectory(ListDirectoryRequest {
                path: path.to_string(),
            })),
        };

        let response = self.execute_request(request, None).await?;

        match response.response_type {
            Some(response::ResponseType::DirectoryListing(listing)) => {
                Ok(listing.entries)
            }
            _ => bail!("Unexpected response type for list directory"),
        }
    }
    pub async fn flush_file(&self,  file_id: u64,include_file_info: Option<bool>
    ) -> Result<OperationSuccessResponse> {
        let request = Request {
            request_type: Some(request::RequestType::FlushFile(FlushFileRequest {
                file_id,
                include_file_info,
            })),
        };
        let response = self.execute_request(request, None).await?;
        match response.response_type {
            Some(response::ResponseType::OperationSuccess(success)) => Ok(success),
            _ => bail!("Unexpected response type for delete file"),
        }

    }

    pub async fn get_file_info(&self, path: &str) -> Result<FileInfoResponse>
    {
        debug!("get_file_info");
        let request = Request {
            request_type: Some(request::RequestType::GetFileInfo(GetFileInfoRequest {
                path: path.to_string(),
            })),
        };

        let response = self.execute_request(request, None).await?;

        match response.response_type {
            Some(response::ResponseType::FileInfo(info)) =>{
                debug!("get_file_info successfully for: {:?}", path);
                Ok(info)},
            _ => {
                debug!("failed to get file info for: {:?}", path);
                bail!("Unexpected response type for get file info")},
        }
    }

    pub async fn read_file(&self, path: &str, offset: u64, length: u32, file_id: u64, compress: Arc<AtomicBool>) -> Result<Vec<u8>> {


        let request = Request {
            request_type: Some(request::RequestType::ReadFile(ReadFileRequest {
                path: path.to_string(),
                offset,
                length,
                file_id,
                compress: compress.load(Ordering::Relaxed),
            })),
        };

        let response = self.execute_request(request, None).await?;

        match response.response_type {
            Some(response::ResponseType::FileData(data)) => {
                let bytes = if data.compressed {
                    let decompressed = zstd::decode_all(data.data.as_slice())?;
                    decompressed
                } else {
                    data.data
                };
                Ok(bytes)
            }
            _ => bail!("Unexpected response type for read file"),
        }
    }
    pub async fn create_file(&self, path: &str, flags: u32, file_id: u64, include_file_info: Option<bool>) -> Result<OperationSuccessResponse> {
        let request = Request {
            request_type: Some(request::RequestType::CreateFile(CreateFileRequest {
                path: path.to_string(),
                file_id ,
                flags,
                include_file_info,
            })),
        };
        let response = self.execute_request(request, None).await?;

        match response.response_type {
            Some(response::ResponseType::OperationSuccess(success)) => Ok(success),
            _ => bail!("Unexpected response type for delete file"),
        }

    }
    pub async fn override_file(
        &self,
        file_id: u64,
        include_file_info: Option<bool>
    ) -> Result<OperationSuccessResponse> {
        let request = Request {
            request_type: Some(request::RequestType::OverrideFile(OverrideFileRequest {
                file_id,
                include_file_info,
            })),
        };
        let response = self.execute_request(request, None).await?;

        match response.response_type {
            Some(response::ResponseType::OperationSuccess(success)) => {
                Ok(success)
            }
            _ => bail!("Unexpected response type for override file"),
        }
    }






    pub async fn write_file(
        &self,
        path: &str,
        offset: u64,
        data: &[u8],
        file_id: u64,
        flags: u32,
        compress: Arc<AtomicBool>,
        is_compressed: bool,
        include_file_info: Option<bool>,
        stream: Option<(SendStream, RecvStream)>,
    ) -> Result<WriteSuccessResponse> {
        let (data, compressed) = if compress.load(Ordering::Relaxed)
            && !is_compressed
        {
            let c = zstd::encode_all(data, 3)?;
            if c.len() < data.len() {
                (c, true)
            } else {
                (data.to_vec(), false)
            }
        } else {
            (data.to_vec(), false)
        };

        let request = Request {
            request_type: Some(request::RequestType::WriteFile(WriteFileRequest {
                path: path.to_string(),
                offset,
                data,
                file_id,
                flags,
                compressed,
                include_file_info
            })),
        };

        let response = self.execute_request(request, stream).await?;

        match response.response_type {
            Some(response::ResponseType::WriteSuccess(success)) => {
                Ok(success)
            }
            _ => bail!("Unexpected response type for write file"),
        }
    }
    pub async fn close_file(&self, file_handler: u64) -> Result<()> {
        let request = Request{
            request_type:Some(request::RequestType::CloseFile(CloseFileRequest{
                file_handler,
            }))
        };
        let response = self.execute_request(request, None).await?;
        match response.response_type {
            Some(response::ResponseType::OperationSuccess(_)) => Ok(()),
            _ => bail!("Unexpected response type for delete file"),
        }
    }

    pub async fn delete_file(&self, path: &str) -> Result<()> {

        let request = Request {
            request_type: Some(request::RequestType::DeleteFile(DeleteFileRequest {
                path: path.to_string(),
            })),
        };

        let response = self.execute_request(request, None).await?;

        match response.response_type {
            Some(response::ResponseType::OperationSuccess(_)) => Ok(()),
            _ => bail!("Unexpected response type for delete file"),
        }
    }

    pub async fn rename_file(&self, old_path: &str, new_path: &str) -> Result<()> {


        let request = Request {
            request_type: Some(request::RequestType::RenameFile(RenameFileRequest {
                old_path: old_path.to_string(),
                new_path: new_path.to_string(),
            })),
        };

        let response = self.execute_request(request, None).await?;

        match response.response_type {
            Some(response::ResponseType::OperationSuccess(_)) => Ok(()),
            _ => bail!("Unexpected response type for rename file"),
        }
    }

    pub async fn create_directory(&self, path: &str, include_file_info: Option<bool>
    ) -> Result<OperationSuccessResponse> {
        let request = Request {
            request_type: Some(request::RequestType::CreateDirectory(CreateDirectoryRequest {
                path: path.to_string(),

                include_file_info,
            })),
        };

        let response = self.execute_request(request, None).await?;

        match response.response_type {
            Some(response::ResponseType::OperationSuccess(success)) => Ok(success),
            _ => bail!("Unexpected response type for create directory"),
        }
    }

    pub async fn delete_directory(&self, path: &str) -> Result<()> {
        let request = Request {
            request_type: Some(request::RequestType::DeleteDirectory(DeleteDirectoryRequest {
                path: path.to_string(),
            })),
        };

        let response = self.execute_request(request, None).await?;

        match response.response_type {
            Some(response::ResponseType::OperationSuccess(_)) => Ok(()),
            _ => bail!("Unexpected response type for delete directory"),
        }
    }

    pub async fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        let request = Request {
            request_type: Some(request::RequestType::ListVolumes(ListVolumesRequest {})),
        };

        let response = self.execute_request(request, None).await?;

        match response.response_type {
            Some(response::ResponseType::VolumeList(list)) => Ok(list.volumes),
            _ => bail!("Unexpected response type for list volumes"),
        }
    }

    pub async fn get_volume_info(&self, volume_id: &str) -> Result<VolumeInfo> {
        debug!("Getting volume info for volume {}", volume_id);
        let request = Request {
            request_type: Some(request::RequestType::GetVolumeInfo(GetVolumeInfoRequest {
                volume_id: volume_id.to_string(),
            })),
        };

        let response = self.execute_request(request, None).await?;

        match response.response_type {
            Some(response::ResponseType::VolumeInfo(info)) => Ok(info.volume.unwrap()),
            _ => bail!("Unexpected response type for get volume info"),
        }
    }
    fn do_cleanup(&self, session: &session, server_addr: SocketAddr) {
        let (connection_type, connection_point) = match &session.hostname {
            None => (ConnectionType::Direct, server_addr.to_string()),
            Some(hostname) => (ConnectionType::Hub, hostname.clone()),
        };
        let username = session.authenticate_request.user_id.clone();

        VAULT_DRIVE_MAP.remove(&(server_addr, username.clone()));
        self.mounts.clear();

        remove_connection(connection_type, connection_point, username.as_str());

    }
    
    pub async fn disconnect(&self, connection_id : String) -> Result<()> {
        let (server_addr_guard, session_guard) = join!(
    self.server_addr.read(),
    self.session.read()
);
        let request = Request {
            request_type: Some(request::RequestType::Remove(RemoveRequest {
                 connection_id,
            })),
        };

        self.mounts.clear();


        self.execute_request(request, None).await?;

        let server_addr = *server_addr_guard;
        let username = session_guard.as_ref().unwrap().authenticate_request.user_id.clone();

        self.quic.close(server_addr, username);
        *self.session.write().await= None;
        Ok(())
    }
}

impl Drop for VaultDriveClient {
    fn drop(&mut self) {

        if tokio::runtime::Handle::try_current().is_ok() {
            debug!("Tokio runtime is ok");
            tokio::task::block_in_place(|| {
                debug!("Blocking tokio thread to perform cleanup");
                let session_guard = self.session.blocking_read();
                if let Some(session) = &*session_guard {
                    let server_addr = *self.server_addr.blocking_read();
                    self.do_cleanup(session, server_addr);
                }
            });
        } else {

            let session_guard = self.session.blocking_read();
            if let Some(session) = &*session_guard {
                let server_addr = *self.server_addr.blocking_read();
                self.do_cleanup(session, server_addr);
            }
        }
    }
}
