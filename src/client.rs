use anyhow::{Context, Result, bail, anyhow};
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use dashmap::{DashMap, DashSet};
use keyring::Entry;
use log::info;
use once_cell::sync::Lazy;
use tokio::join;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::debug;
use crate::auth::{authenticate, session};
use crate::driveManagerUI::ConnectionType;
use crate::network::{QuicClient, read_message, write_message, ZERO_ADDR};
use crate::proto::hub;
use crate::proto::hub::hub_request::RequestType;
use crate::proto::hub::{ClientHelloRequest, ClientHelloResponse};
use crate::proto::vaultdrive::*;


/// VaultDrive client
pub struct VaultDriveClient {
    pub quic: &'static QuicClient,
    pub session: Arc<RwLock<Option<session>>>,
    pub server_addr: RwLock<SocketAddr>,
    pub mount_points: DashSet<String>,

}

pub static VAULT_DRIVE_MAP: Lazy<DashMap<(SocketAddr, String), Arc<VaultDriveClient>>> =
    Lazy::new(|| DashMap::new());



impl VaultDriveClient {
    pub async fn new(server_addr: SocketAddr, username: String) -> Result<Arc<Self>> {
        if let Some(existing) = VAULT_DRIVE_MAP.get(&(server_addr, username)) {
            return Ok(existing.clone());
        }

        let quic = QuicClient::new().await?;
        let client = Arc::new(Self {
            quic,
            session: Arc::new(RwLock::new(None)),
            server_addr: RwLock::new(server_addr),
            mount_points: DashSet::new(),
        });

        Ok(client)
    }

    pub async fn renew_connection(self: Arc<Self>, session: session) -> Result<SocketAddr> {
        debug!("Starting renew connection");

        if let Some(hostname) = &session.hostname {
            let current_server_addr = *self.server_addr.read().await;

            self.connect_to_hub(hostname.as_str()).await?;
            debug!("Got server addr from hub");

            let new_server_addr = *self.server_addr.read().await;

            if current_server_addr != new_server_addr {
                let sessionClone = session.clone();
                VAULT_DRIVE_MAP.insert((new_server_addr, sessionClone.authenticate_request.user_id.clone()), self.clone());
                VAULT_DRIVE_MAP.remove(&(current_server_addr, sessionClone.authenticate_request.user_id));
                self.quic.connection.remove(&current_server_addr);
                debug!("Performing renew connection clean up")
            }
        }

        let server_addr = *self.server_addr.read().await;
        debug!("Removing old connection");
        let old_connection = self.quic.connection.remove(&server_addr);
        debug!("About to call connect for {}", server_addr);

        match self.quic.connect(&server_addr).await{
            Ok(_) => {}
            Err(_) => {
                // this is here because if connection fails because no internet,
                // instead of removing the connection and unmounting, keep the dead connection
                // and mount which allow for auto reauth later 
                if let Some(old_conn) = old_connection {
                    self.quic.connection.insert(server_addr, old_conn.1);
                }
                return Err(anyhow!("Failed to connect to {}", server_addr));
            }
        }
        drop(old_connection);
        debug!("Connecting to {}", server_addr);

        let _reauthenticate_request = self.reauthenticate(session).await
            .context("Authentication failed")?;

        debug!("Authentication succeeded for {}", _reauthenticate_request.host_name);

        Ok(server_addr)
    }


    pub async fn connect(self: Arc<Self>, username: &str, entry: &Entry, hostname: Option<String>) -> Result<()> {
        tracing::debug!("Connecting to Vault Drive for user {:?}", username);
        let server_addr = *self.server_addr.read().await;

        self.quic.connect(&server_addr).await
            .context("Failed to connect to server")?;


        let authenticate_request = self.authenticate_internal(username, &entry).await
            .context("Authentication failed")?;


        let session : session = session{
            hostname,
            authenticate_request,
        };

        
        *self.session.write().await = Some(session.clone());

        VAULT_DRIVE_MAP.insert((server_addr, session.authenticate_request.user_id), self);
        tracing::debug!("{:?} this is the VAULT DRIVE CLIENT MAP", VAULT_DRIVE_MAP.len());
        Ok(())
    }


    pub async fn connect_to_hub(&self, hostname: &str) -> Result<()> {
        let socket_addr: SocketAddr = ZERO_ADDR;


        let (addr, conn) = self.quic.connect(&socket_addr).await
            .context("Failed to connect to server")?;

        let connection_map = self.quic.connection.clone();
        tokio::spawn(async move {
            conn.closed().await;
            connection_map.remove(&socket_addr);
        });


        let clientHelloRes = self.get_server_from_hub(hostname, addr).await?;
        if !clientHelloRes.found {
            bail!(format!("Failed to connect to server: {}", hostname));
        }
        if let Some(addr) = clientHelloRes.socket_addr {
            *self.server_addr.write().await = addr.parse()?;
        } else {
            bail!("Failed to be server_addr");
        };

    Ok(())
    }
    
   pub async fn get_server_from_hub(&self, hostname: &str, addr: SocketAddr) -> Result<ClientHelloResponse> {

        let (mut send, mut recv) = self.quic.open_bi_safe(addr).await?;

        let init_req = hub::HubRequest {
            request_type: Some(RequestType::ClientHelloReq(
                ClientHelloRequest {
                    unique_id: hostname.parse()
                        .context("Failed to parse hostname")?,
                }
            ))
        };


        write_message(&mut send, &init_req).await?;
        send.finish()?;

        let response = timeout(
            Duration::from_secs(10),
            read_message(&mut recv)
        )
            .await
            .context("Timeout waiting for response from hub")?
            .context("Failed to read response")?;


        Ok(response)
    }
    async fn reauthenticate(&self, session: session) -> Result<AuthenticationSuccessResponse>{
        let server_addr = *self.server_addr.read().await;

        tracing::debug!("authenticate_internal: Opening bidirectional stream to {:?}", server_addr);
        let (mut send, mut recv) = self.quic.open_bi_safe(server_addr).await
            .context("Failed to open bidirectional stream")?;
        let (connection_type ,connection_point) = match session.hostname {
            None => { (ConnectionType::Direct,server_addr.to_string() )}
            Some(hostname) => {(ConnectionType::Hub,hostname)}
        };
        let service = format!("vaultDrive|{}|{}",
                              connection_type,
                              connection_point,
        );
        let entry = Entry::new(&service, session.authenticate_request.user_id.as_str())?;
        let result=authenticate(&mut send, &mut recv, session.authenticate_request.user_id.as_str(), entry.get_password()?.as_str() ).await;
        if result.is_err(){
            entry.delete_credential()?;
        }else {
            self.quic.reAuth.notify_waiters();
        }
        result

    }

    async fn authenticate_internal(&self, username: &str, entry: &Entry) -> Result<AuthenticationSuccessResponse> {
        tracing::debug!("authenticate_internal: Getting server address");
        let server_addr = *self.server_addr.read().await;

        tracing::debug!("authenticate_internal: Opening bidirectional stream to {:?}", server_addr);
        let (mut send, mut recv) = self.quic.open_bi(server_addr, username.to_string()).await
            .context("Failed to open bidirectional stream")?;

        tracing::debug!("authenticate_internal: Stream opened successfully, calling authenticate");

        let result = authenticate(&mut send, &mut recv, username, entry.get_password()?.as_str()).await;

        if result.is_err(){
            entry.delete_credential()?;
        }
        result
    }



    async fn execute_request(&self, request: Request) -> Result<Response> {
        let (server_addr_guard, session_guard) = join!(
    self.server_addr.read(),
    self.session.read()
);

        let server_addr = *server_addr_guard;
        let username = session_guard.as_ref().unwrap().authenticate_request.user_id.clone();



        let (mut send, mut recv) = self.quic.open_bi(server_addr, username).await?;



        write_message(&mut send, &request).await?;
        let response = read_message::<Response>(&mut recv).await?;

        // Check for errors
        if let Some(response::ResponseType::Error(err)) = &response.response_type {
            bail!("Server error: {}", err.message);
        }

        Ok(response)
    }

    
    pub async fn open_file(&self, path: &str, file_id: u64, flags: u32) -> Result<()> {
        let request = Request {
            request_type: Some(request::RequestType::OpenFile(OpenFileRequest {
                path: path.to_string(),
                file_id,
                flags,
            })),
        };
        let response = self.execute_request(request).await?;
        match response.response_type {
            Some(response::ResponseType::OperationSuccess(_)) => Ok(()),
            _ => bail!("Unexpected response type for delete file"),
        }
    }


    pub async fn list_directory(&self, path: &str) -> Result<Vec<DirectoryEntry>> {
        let request = Request {
            request_type: Some(request::RequestType::ListDirectory(ListDirectoryRequest {
                path: path.to_string(),
            })),
        };

        let response = self.execute_request(request).await?;

        match response.response_type {
            Some(response::ResponseType::DirectoryListing(listing)) => {
                Ok(listing.entries)
            }
            _ => bail!("Unexpected response type for list directory"),
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

        let response = self.execute_request(request).await?;

        match response.response_type {
            Some(response::ResponseType::FileInfo(info)) => Ok(info),
            _ => bail!("Unexpected response type for get file info"),
        }
    }

    pub async fn read_file(&self, path: &str, offset: u64, length: u32, file_id: u64) -> Result<Bytes> {


        let request = Request {
            request_type: Some(request::RequestType::ReadFile(ReadFileRequest {
                path: path.to_string(),
                offset,
                length,
                file_id,
            })),
        };

        let response = self.execute_request(request).await?;

        match response.response_type {
            Some(response::ResponseType::FileData(data)) => {
                let bytes = Bytes::from(data.data);
                

                Ok(bytes)
            }
            _ => bail!("Unexpected response type for read file"),
        }
    }
    pub async fn create_file(&self, path: &str, flags: u32, file_id: u64) -> Result<()> {
        let request = Request {
            request_type: Some(request::RequestType::CreateFile(CreateFileRequest {
                path: path.to_string(),
                file_id ,
                flags,
            })),
        };
        let response = self.execute_request(request).await?;

        match response.response_type {
            Some(response::ResponseType::OperationSuccess(_)) => Ok(()),
            _ => bail!("Unexpected response type for delete file"),
        }

    }
    pub async fn override_file(
        &self,
        file_id: u64,
    ) -> Result<()>{
        let request = Request {
            request_type: Some(request::RequestType::OverrideFile(OverrideFileRequest {
                file_id
            })),
        };
        let response = self.execute_request(request).await?;

        match response.response_type {
            Some(response::ResponseType::OperationSuccess(success)) => {
                Ok(())
            }
            _ => bail!("Unexpected response type for override file"),
        }
    }


    pub async fn write_file(
        &self,
        path: &str,
        offset: u64,
        data: Vec<u8>,
        file_id: u64,
        flags: u32
    ) -> Result<u64> {

        let request = Request {
            request_type: Some(request::RequestType::WriteFile(WriteFileRequest {
                path: path.to_string(),
                offset,
                data,
                file_id ,
                flags,
            })),
        };

        let response = self.execute_request(request).await?;

        match response.response_type {
            Some(response::ResponseType::WriteSuccess(success)) => {
                Ok(success.bytes_written)
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
        let response = self.execute_request(request).await?;
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

        let response = self.execute_request(request).await?;

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

        let response = self.execute_request(request).await?;

        match response.response_type {
            Some(response::ResponseType::OperationSuccess(_)) => Ok(()),
            _ => bail!("Unexpected response type for rename file"),
        }
    }

    pub async fn create_directory(&self, path: &str) -> Result<()> {
        let request = Request {
            request_type: Some(request::RequestType::CreateDirectory(CreateDirectoryRequest {
                path: path.to_string(),
            })),
        };

        let response = self.execute_request(request).await?;

        match response.response_type {
            Some(response::ResponseType::OperationSuccess(_)) => Ok(()),
            _ => bail!("Unexpected response type for create directory"),
        }
    }

    pub async fn delete_directory(&self, path: &str) -> Result<()> {
        let request = Request {
            request_type: Some(request::RequestType::DeleteDirectory(DeleteDirectoryRequest {
                path: path.to_string(),
            })),
        };

        let response = self.execute_request(request).await?;

        match response.response_type {
            Some(response::ResponseType::OperationSuccess(_)) => Ok(()),
            _ => bail!("Unexpected response type for delete directory"),
        }
    }

    pub async fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        let request = Request {
            request_type: Some(request::RequestType::ListVolumes(ListVolumesRequest {})),
        };

        let response = self.execute_request(request).await?;

        match response.response_type {
            Some(response::ResponseType::VolumeList(list)) => Ok(list.volumes),
            _ => bail!("Unexpected response type for list volumes"),
        }
    }

    pub async fn get_volume_info(&self, volume_id: &str) -> Result<VolumeInfo> {
        info!("Getting volume info for volume {}", volume_id);
        let request = Request {
            request_type: Some(request::RequestType::GetVolumeInfo(GetVolumeInfoRequest {
                volume_id: volume_id.to_string(),
            })),
        };

        let response = self.execute_request(request).await?;

        match response.response_type {
            Some(response::ResponseType::VolumeInfo(info)) => Ok(info.volume.unwrap()),
            _ => bail!("Unexpected response type for get volume info"),
        }
    }
    
    pub async fn disconnect(&self) {
        let (server_addr_guard, session_guard) = join!(
    self.server_addr.read(),
    self.session.read()
);

        let server_addr = *server_addr_guard;
        let username = session_guard.as_ref().unwrap().authenticate_request.user_id.clone();

        self.quic.close(server_addr, username);
        *self.session.write().await= None;
    }
}

impl Drop for VaultDriveClient {
    fn drop(&mut self) {

        self.disconnect();
        let session_guard = self.session.blocking_read();

        let session = match &*session_guard {
            None => return,
            Some(s) => s,
        };

        let server_addr = *self.server_addr.blocking_read();
        let (connection_type ,connection_point) = match session.hostname.clone() {
            None => { (ConnectionType::Direct,server_addr.to_string() )}
            Some(hostname) => {(ConnectionType::Hub,hostname)}
        };
        let service = format!("vaultDrive|{}|{}",
                              connection_type,
                              connection_point,
        );
        let entry = match Entry::new(&service, session.authenticate_request.user_id.as_str()){
            Ok(entry) => entry,
            Err(_) => return,
        };
        entry.delete_credential().expect("panic");
    }
}
