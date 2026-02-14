#![cfg(target_os = "windows")]

use anyhow::{Result, bail, anyhow, Context, Error};
use std::sync::{Arc, OnceLock};
use parking_lot::{Condvar, Once};
use std::collections::HashMap;
use std::ffi::c_void;
use std::future::Future;
use std::mem;
use std::ops::Deref;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use parking_lot::{RwLock, Mutex};
use tokio::sync::{oneshot, Mutex as TokioMutex};

use bytes::Bytes;
use tokio::sync::OnceCell;
use once_cell::sync::{Lazy};
use dashmap::DashMap;
use log::debug;
use path_slash::PathExt;
use strum_macros::Display;
use tokio::runtime::{Handle};
use tokio::sync::oneshot::error::RecvError;
use tracing::{info, warn, Instrument};
use windows::core::HSTRING;
use windows::Win32::Foundation::{LocalFree, HLOCAL, STATUS_OBJECT_NAME_NOT_FOUND,STATUS_NOT_A_DIRECTORY, STATUS_INTERNAL_ERROR};
use windows::Win32::Security::Authorization::ConvertStringSecurityDescriptorToSecurityDescriptorW;

use windows::Win32::Security::{InitializeSecurityDescriptor, SetSecurityDescriptorDacl, PSECURITY_DESCRIPTOR, SECURITY_DESCRIPTOR,};
use windows::Win32::Storage::FileSystem::{FILE_APPEND_DATA, FILE_FLAG_DELETE_ON_CLOSE, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_READ_DATA, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_WRITE_DATA};
use windows::Win32::System::SystemServices::SECURITY_DESCRIPTOR_REVISION;
use crate::client::VaultDriveClient;
use crate::proto::vaultdrive::{DirectoryEntry, FileInfoResponse};

use winfsp::host::{FileSystemHost, MountPoint, VolumeParams};
use winfsp::filesystem::{
    DirInfo, WideNameInfo,
    FileSystemContext, AsyncFileSystemContext, FileInfo, VolumeInfo,
    OpenFileInfo, FileSecurity, DirMarker, DirBuffer, ModificationDescriptor
};
use winfsp::*;

use winfsp::{FspError, U16CStr, U16CString};
use winfsp::constants::FspCleanupFlags::FspCleanupSetAllocationSize;
use winfsp::FspError::NTSTATUS;
use winfsp_sys::FILE_FLAGS_AND_ATTRIBUTES;
use crate::filesystem::{InodeAllocator, APPEND, DELETE, DELETE_CHILDREN, EXCL, EXCLUSIVE_LOCK, READ, SHARE_LOCK, TRANSVERSE, WRITE};
use crate::proto;

const FILE_ATTRIBUTE_NORMAL: u32 = 0x00000080;
const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x00000010;
const FILE_ATTRIBUTE_READONLY: u32 = 0x00000001;

const STATUS_SUCCESS: i32 = 0;
const STATUS_NOT_FOUND: i32 = 0xC0000225_u32 as i32;
const STATUS_INVALID_HANDLE: i32 = 0xC0000008_u32 as i32;
const STATUS_FILE_IS_A_DIRECTORY: i32 = 0xC00000BA_u32 as i32;

const FSP_CLEANUP_DELETE: u32 = 0x01;



///todo this seem to be an issue where with jetbrain safe write
/// the safe write fail but the actually write is succedd
/// proabably something to do with noramilzation
pub static MOUNTS: Lazy<DashMap<String, (Mutex<FileSystemHost<VirtualFileSystem>>, String)>> =
    Lazy::new(|| DashMap::new());


#[derive(Clone, Debug)]
struct FileNode {
    path: String,
    name: String,
    size: i64,
    is_directory: bool,
    file_attributes: u32,
    creation_time: SystemTime,
    last_access_time: SystemTime,
    last_write_time: SystemTime,
    change_time: SystemTime,
    flags: u32,
}

impl FileNode {
    fn create_custom_security_descriptor(&self) -> Result<Vec<u8>, FspError> {
        debug!("Creating a custom security descriptor for flags: {}", self.flags);
        let mut access_mask = 0u32;

            if self.flags & TRANSVERSE != 0 {
                access_mask |= 0x00000020;

        }
            if self.flags & READ != 0 {
                access_mask |= 0x00120089; // FILE_GENERIC_READ without EXECUTE
                // = STANDARD_RIGHTS_READ | FILE_READ_DATA |
                //   FILE_READ_ATTRIBUTES | FILE_READ_EA | SYNCHRONIZE
            }


        if self.flags & WRITE != 0 {
            access_mask |= 0x00120116; // FILE_GENERIC_WRITE without EXECUTE
            // = STANDARD_RIGHTS_WRITE | FILE_WRITE_DATA |
            //   FILE_WRITE_ATTRIBUTES | FILE_WRITE_EA |
            //   FILE_APPEND_DATA | SYNCHRONIZE
        }

        if self.flags & DELETE != 0 {
            access_mask |= 0x00010000; // DELETE
        }

        if self.flags & DELETE_CHILDREN != 0 {
            access_mask |= 0x00000040; // FILE_DELETE_CHILD
        }

        let sddl = if access_mask == 0 {
            // No permissions - deny all
            HSTRING::from("O:BAG:BAD:(D;;FA;;;WD)")
        } else {
            // Grant specified permissions to Everyone
            HSTRING::from(&format!("O:BAG:BAD:(A;;0x{:08x};;;WD)", access_mask))
        };

        let mut psd = PSECURITY_DESCRIPTOR::default();
        let mut sd_size = 0u32;

        unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                &sddl,
                SECURITY_DESCRIPTOR_REVISION,
                &mut psd,
                Some(&mut sd_size),
            ).map_err(|e| FspError::from(e))?;

            let buffer = std::slice::from_raw_parts(
                psd.0 as *const u8,
                sd_size as usize
            ).to_vec();

            LocalFree(Some(HLOCAL(psd.0)));
            Ok(buffer)
        }
    }
    fn from_file_info(info: FileInfoResponse) -> Self {
        let creation_time = UNIX_EPOCH + Duration::from_secs(info.created_at as u64);
        let modified_time = UNIX_EPOCH + Duration::from_secs(info.modified_at as u64);
        let accessed_time = UNIX_EPOCH + Duration::from_secs(info.accessed_at as u64);

        let file_attributes = if info.is_directory {
            FILE_ATTRIBUTE_DIRECTORY
        } else {
            FILE_ATTRIBUTE_NORMAL
        };

        Self {
            path: info.path.clone(),
            name: info.name.clone(),
            size: info.size as i64,
            is_directory: info.is_directory,
            file_attributes,
            creation_time,
            last_access_time: accessed_time,
            last_write_time: modified_time,
            change_time: modified_time,
            flags: info.flags
        }
    }

    fn from_directory_entry(entry: &DirectoryEntry, parent_path: &str) -> Self {
        let path = if parent_path == "\\" {
            format!("\\{}", entry.name)
        } else {
            format!("{}\\{}", parent_path, entry.name)
        };

        let modified_time = UNIX_EPOCH + Duration::from_secs(entry.modified as u64);

        let file_attributes = if entry.is_directory {
            FILE_ATTRIBUTE_DIRECTORY
        } else {
            FILE_ATTRIBUTE_NORMAL
        };

        Self {
            path: path.clone(),
            name: entry.name.clone(),
            size: entry.size as i64,
            is_directory: entry.is_directory,
            file_attributes,
            creation_time: modified_time,
            last_access_time: modified_time,
            last_write_time: modified_time,
            change_time: modified_time,
            flags: 0
        }
    }
}

#[derive(Clone, Debug)]
struct OpenFileContext {
    path: String,
    is_directory: bool,
    is_marked_deleted: bool,
    flags: u32
}


#[derive(Clone)]
pub struct VirtualFileSystem {
    client: Arc<VaultDriveClient>,
    handle: Handle,

    open_files: Arc<DashMap<u64, OpenFileContext>>,
    next_context: Arc<InodeAllocator>,



    pending_file_requests: DashMap<String, Arc<PendingRequest>>,
    pending_dir_requests: Arc<DashMap<String, Arc<OnceCell<Vec<DirectoryEntry>>>>>,

    drive: Arc<RwLock<String>>,
}
struct PendingRequest {
    result: OnceCell<Result<FileNode, i32>>,
}

impl VirtualFileSystem {
    pub fn new(
        client: Arc<VaultDriveClient>,
        handle: Handle,
        drive: &str,
    ) -> Self {
        Self {

            client,
            handle,
            open_files: Arc::new(DashMap::new()),
            next_context: Arc::new(InodeAllocator::new(1)),
            pending_file_requests: (DashMap::new()),
            pending_dir_requests: Arc::new(DashMap::new()),
            drive: Arc::new(RwLock::new(drive.to_string())),
        }
    }

    fn to_unix_path(&self, path: &str) -> String {
        Path::new(path)
            .to_slash_lossy()
            .to_string()
    }

    fn normalize_path(&self, path: &str) -> String {
        let mut normalized = path.replace('/', "\\");
        if !normalized.starts_with('\\') {
            normalized = format!("\\{}", normalized);
        }
        if normalized.len() > 1 && normalized.ends_with('\\') {
            normalized = normalized.trim_end_matches('\\').to_string();
        }
        normalized
    }


    async fn get_or_fetch_file_info(&self, path: &str) -> Result<FileNode, i32> {

        let path_str = path.to_string();

        let pending = self
            .pending_file_requests
            .entry(path_str.clone())
            .or_insert_with(|| Arc::new(PendingRequest {
                result: OnceCell::new(),
            }))
            .clone();

        let result = pending.result.get_or_init(|| async {
            let unix_path = path_str.replace("\\", "/");
            const TIMEOUT_DURATION: Duration = Duration::from_secs(5);

            match tokio::time::timeout(
                TIMEOUT_DURATION,
                self.client.get_file_info(&unix_path)
            ).await {
                Ok(Ok(info)) => {
                    let node = FileNode::from_file_info(info);
                    Ok(node)
                }
                Ok(Err(_)) => Err(STATUS_NOT_FOUND),
                Err(_) => {
                    warn!("File info fetch timed out for {}", unix_path);
                    Err(STATUS_INTERNAL_ERROR.0)
                }
            }
        }).await;

        self.pending_file_requests.remove(&path_str);

        result.clone()
    }

    async fn get_or_fetch_directory_list(&self, path: &str) -> Result<Vec<DirectoryEntry>> {
        let cell = self.pending_dir_requests
            .entry(path.to_string())
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone();

        let entries = cell.get_or_try_init(|| async {


            tracing::debug!("Fetching directory from network: {}", path);
            let unix_path = self.to_unix_path(path);
            let response = self.client.list_directory(&unix_path).await?;
            let entries = response;

            Ok::<Vec<DirectoryEntry>, anyhow::Error>(entries)
        }).await?;

        self.pending_dir_requests.remove(path);

        Ok(entries.clone())
    }

}



fn system_time_to_filetime(time: SystemTime) -> u64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => {
            // Windows FILETIME is 100-nanosecond intervals since January 1, 1601
            // Unix epoch is January 1, 1970, which is 11644473600 seconds after Windows epoch
            const UNIX_EPOCH_FILETIME: u64 = 116444736000000000;
            let intervals = duration.as_secs() * 10_000_000 + u64::from(duration.subsec_nanos()) / 100;
            UNIX_EPOCH_FILETIME + intervals
        }
        Err(_) => 0,
    }
}

impl FileSystemContext for VirtualFileSystem {
    type FileContext = u64;

    fn get_security_by_name(
        &self,
        file_name: &U16CStr,
        security_descriptor: Option<&mut [c_void]>,
        _reparse_point_resolver: impl FnOnce(&U16CStr) -> Option<FileSecurity>,
    ) -> Result<FileSecurity, FspError> {
        debug!("get_security_by_name was called");

        let path = self.normalize_path(&file_name.to_string_lossy());
        let self_clone = self.clone();
        let (tx, rx) = oneshot::channel();
        self.handle.spawn(async move {
            let node = self_clone.get_or_fetch_file_info(&path).await
                .map_err(|_| FspError::from(NTSTATUS(STATUS_NOT_FOUND)));
            tx.send(node);
        });
        let node=rx.blocking_recv().map_err(|_| FspError::from(NTSTATUS(STATUS_OBJECT_NAME_NOT_FOUND.0)))??;




        let sd = node.create_custom_security_descriptor()?;
        let sd_size = sd.len() as u64;

        if let Some(buffer) = security_descriptor {
            if buffer.len() >= sd.len() {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        sd.as_ptr(),
                        buffer.as_mut_ptr() as *mut u8,
                        sd.len()
                    );
                }
            }
        }

        Ok(FileSecurity {
            attributes: node.file_attributes,
            reparse: false,
            sz_security_descriptor: sd_size,
        })
    }
    fn open(
        &self,
        file_name: &U16CStr,
        _create_options: u32,
        granted_access: u32,
        out_file_info: &mut OpenFileInfo,
    ) -> Result<Self::FileContext, FspError> {
        debug!("Open was called");
        let share_access  = unsafe {
            match winfsp_sys::FspFileSystemGetOperationContext().as_ref() {
                Some(context) => match context.Request.as_ref() {
                    Some(request) =>
                        request.Req.Create.ShareAccess,
                    None => {
                        debug!("Request is null");
                        0x00000007u32
                    }
                },
                None => {
                    debug!("FspFileSystemGetOperationContext returned null");
                    0x00000007u32
                }
            }
        };

        let mut flags: u32 = 0;



        if (granted_access & FILE_READ_DATA.0) != 0 || (granted_access & FILE_GENERIC_READ.0) != 0 {
            flags |= READ;
        }

        if (granted_access & FILE_WRITE_DATA.0) != 0 || (granted_access & FILE_GENERIC_WRITE.0) != 0 {
            flags |= WRITE;
        }

        if (granted_access & FILE_APPEND_DATA.0) != 0 {
            flags |= APPEND;
        }



        if share_access == 0 {
            flags |= EXCLUSIVE_LOCK;
        } else if (share_access & (FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)) != 0 {
            flags |= SHARE_LOCK;
        }

        let path = self.normalize_path(&file_name.to_string_lossy());
        let unix_path = self.to_unix_path(&path);

        debug!("open was called for {:}  with flags: {:b}", path, flags);
        let client = self.client.clone();
        let (tx, rx) = oneshot::channel();


        let context_value = self.next_context.allocate();

        self.handle.spawn(async move {
            let result =
                client.open_file(&unix_path, context_value, flags).await.map(|_| ());
            let _ = tx.send(result);
        });
         match rx.blocking_recv() {
             Ok(result ) => {}
             Err(_) => {}
         }

        let self_clone = self.clone();
        let (tx, rx) = oneshot::channel();
        let path_clone = path.clone();
        self.handle.spawn(async move {
            let node = self_clone.get_or_fetch_file_info(&path_clone).await
                .map_err(|_| FspError::from(NTSTATUS(STATUS_NOT_FOUND)));
            tx.send(node);
        });
        let node=rx.blocking_recv().map_err(|_| FspError::from(NTSTATUS(STATUS_NOT_FOUND)))??;

        let context = OpenFileContext {
             path,
            is_directory: node.is_directory,
            is_marked_deleted : _create_options & FILE_FLAG_DELETE_ON_CLOSE.0 != 0,
            flags
        };

        self.open_files.insert(context_value, context.clone());

        let file_info = FileInfo {
            file_attributes: node.file_attributes,
            reparse_tag: 0,
            allocation_size: ((node.size as u64 + 4095) / 4096) * 4096,
            file_size: node.size as u64,
            creation_time: system_time_to_filetime(node.creation_time),
            last_access_time: system_time_to_filetime(node.last_access_time),
            last_write_time: system_time_to_filetime(node.last_write_time),
            change_time: system_time_to_filetime(node.last_write_time),
            index_number: 0,
            hard_links: 0,
            ea_size: 0,
        };

        *out_file_info.as_mut() = file_info;

        Ok(context_value)
    }



    fn close(&self, context: Self::FileContext) {
        debug!("Close was called");
        let (tx, rx) = oneshot::channel();
        let client = self.client.clone();

        self.handle.spawn(async move {
            let result = client.close_file(context).await;
            let _ = tx.send(result);
        });

        match rx.blocking_recv() {
            Ok(Ok(_)) => {
            }
            Ok(Err(e)) => {
                tracing::error!("close: failed to close file: {:?}", e);
            }
            Err(_) => {
                tracing::error!("close: channel recv failed");
            }
        }

        self.open_files.remove(&context);
        self.next_context.free(context);
    }



    fn create(
        &self,
        file_name: &U16CStr,
        create_options: u32,
        _granted_access: u32,
        _file_attributes: u32,
        _security_descriptor: Option<&[c_void]>,
        _allocation_size: u64,
        _extra_buffer: Option<&[u8]>,
        _extra_buffer_is_reparse_point: bool,
        out_file_info: &mut OpenFileInfo,
    ) -> Result<Self::FileContext, FspError> {
        let share_access = unsafe {
            match winfsp_sys::FspFileSystemGetOperationContext().as_ref() {
                Some(context) => match context.Request.as_ref() {
                    Some(request) => request.Req.Create.ShareAccess,
                    None => {
                        warn!("Request is null");
                        0x00000007u32
                    }
                },
                None => {
                    warn!("FspFileSystemGetOperationContext returned null");
                    0x00000007u32
                }
            }
        };
        
        
        debug!("create called: {}", file_name.to_string_lossy());

        let path = self.normalize_path(&file_name.to_string_lossy());
        let is_directory = (create_options & 0x00000001) != 0;

        let unix_path = self.to_unix_path(&path);
        let client = self.client.clone();
        let file_handler = self.next_context.allocate();
        let (tx, rx) = oneshot::channel();

        let mut flags: u32 = 0;

        flags |= EXCL;
        if (_granted_access & FILE_READ_DATA.0) != 0 || (_granted_access & FILE_GENERIC_READ.0) != 0 {
            flags |= READ;
        }

        if (_granted_access & FILE_WRITE_DATA.0) != 0 || (_granted_access & FILE_GENERIC_WRITE.0) != 0 {
            flags |= WRITE;
        }

        if (_granted_access & FILE_APPEND_DATA.0) != 0 {
            flags |= APPEND;
        }

        if share_access == 0 {
            flags |= EXCLUSIVE_LOCK;
        } else if (share_access & (FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)) != 0 {
            flags |= SHARE_LOCK;
        }

        self.handle.spawn(async move {
            let result = if is_directory {
                client.create_directory(&unix_path).await.map(|_| ())
            } else {
                client.create_file(&unix_path, 0, file_handler.clone()).await.map(|_| ())
            };


            let _ = tx.send(result);
        });

        rx.blocking_recv()
            .map_err(|_| {
                tracing::error!("create: channel recv failed for {}", path);
                FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0))
            })?
            .map_err(|e| {
                tracing::error!("create: failed to create {}: {:?}", path, e);
                FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0))
            })?;

        let self_clone = self.clone();
        let (tx, rx) = oneshot::channel();
        let path_clone = path.clone();
        self.handle.spawn(async move {
            let node = self_clone.get_or_fetch_file_info(&path_clone).await
                .map_err(|_| FspError::from(NTSTATUS(STATUS_NOT_FOUND)));
            tx.send(node);
        });
        let node=rx.blocking_recv().map_err(|_| FspError::from(NTSTATUS(STATUS_NOT_FOUND)))??;

        let context = OpenFileContext {
            path,
            is_directory,
            is_marked_deleted : create_options & FILE_FLAG_DELETE_ON_CLOSE.0 != 0,
            flags

        };

        self.open_files.insert(file_handler, context);

        *out_file_info.as_mut() = FileInfo {
            file_attributes: node.file_attributes,
            reparse_tag: 0,
            allocation_size: node.size as u64,
            file_size: node.size as u64,
            creation_time: system_time_to_filetime(node.creation_time),
            last_access_time: system_time_to_filetime(node.last_access_time),
            last_write_time: system_time_to_filetime(node.last_write_time),
            change_time: system_time_to_filetime(node.last_write_time),
            index_number: 0,
            hard_links: 0,
            ea_size: 0,
        };

        Ok(file_handler)
    }




    fn cleanup(
        &self,
        context: &Self::FileContext,
        _file_name: Option<&U16CStr>,
        flags: u32,
    )  {
        debug!("cleanup was called");
        if (flags & FSP_CLEANUP_DELETE) == 0 {
            return;
        }

        let open_ctx = match self.open_files.get(context) {
            Some(ctx) => ctx,
            None => {
                tracing::error!("cleanup: invalid handle");
                return;
            }
        };

        if open_ctx.is_directory {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let path = open_ctx.path.clone();

            let self_clone = self.clone();

            self.handle.spawn(async move {
                let result = self_clone.get_or_fetch_directory_list(&path).await;
                let _ = tx.send(result);
            });

            let entries = match rx.blocking_recv() {
                Ok(Ok(e)) => e,
                Ok(Err(e)) => {
                    tracing::error!("cleanup: failed to fetch dir list: {:?}", e);
                    return;
                }
                Err(_) => {
                    tracing::error!("cleanup: oneshot channel closed");
                    return;
                }
            };

            if !entries.is_empty() {
                tracing::warn!("cleanup: directory not empty, skipping delete: {:?}", open_ctx.path);
                return;
            }
        }

        // Delete on server
        if open_ctx.is_marked_deleted {
            let unix_path = self.to_unix_path(&open_ctx.path);
            let client = self.client.clone(); // Clone before moving into async block
            let (tx, rx) = oneshot::channel();

            self.handle.spawn(async move {
                let result = client.delete_file(&unix_path).await;
                let _ = tx.send(result); // Ignore send errors (receiver might be dropped)
            });

            match rx.blocking_recv() {
                Ok(Ok(())) => {

                }
                Ok(Err(e)) => {
                    tracing::error!("cleanup: failed to delete file: {:?}", e);
                }
                Err(e) => {
                    tracing::error!("cleanup: channel recv failed: {}", e);
                }
            }
        }
    }
    fn overwrite(&self, context: &Self::FileContext, file_attributes: FILE_FLAGS_AND_ATTRIBUTES, replace_file_attributes: bool, allocation_size: u64, extra_buffer: Option<&[u8]>, file_info: &mut FileInfo) -> Result<(), FspError> {
        debug!("override was called");
        let open_ctx = self.open_files
            .get(context)
            .ok_or(FspError::from(NTSTATUS(STATUS_INVALID_HANDLE)))?
            .clone();

        if open_ctx.is_directory {
            return Err(FspError::from(NTSTATUS(STATUS_FILE_IS_A_DIRECTORY)));
        }
        let (tx, rx) = oneshot::channel();
        let self_clone = self.clone();
        let context_clone = context.clone();
        self.handle.spawn(async move {
           let result= self_clone.client.override_file(context_clone.clone())
                .await
                .map_err(|_| FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)));

            match result {
                Ok(_) => {
                   let node_result =self_clone.get_or_fetch_file_info(open_ctx.path.as_str())
                        .await
                        .map_err(|_| FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)));
                    tx.send(node_result);
                }
                Err(_) => {
                    tx.send(Err(FspError::from(NTSTATUS(STATUS_INVALID_HANDLE))));
                }
            }

        });

        let node=rx.blocking_recv().map_err(|_| FspError::from(NTSTATUS(STATUS_NOT_FOUND)))??;
        *file_info = FileInfo {
            file_attributes: node.file_attributes,
            reparse_tag: 0,
            allocation_size: ((node.size as u64 + 4095) / 4096) * 4096,
            file_size: node.size as u64,
            creation_time: system_time_to_filetime(node.creation_time),
            last_access_time: system_time_to_filetime(node.last_access_time),
            last_write_time: system_time_to_filetime(node.last_write_time),
            change_time: system_time_to_filetime(node.last_write_time),
            index_number: 0,
            hard_links: 0,
            ea_size: 0,
        };

        Ok(())

    }

    fn get_file_info(
        &self,
        context: &Self::FileContext,
        out_file_info: &mut FileInfo,
    ) -> Result<(), FspError> {
        debug!("get_file_info was call");
        let open_ctx = self.open_files
            .get(context)
            .ok_or(FspError::from(NTSTATUS(STATUS_INVALID_HANDLE)))?
            .clone();


        let self_clone = self.clone();
        let path = open_ctx.path.clone();
        let (tx, rx) = oneshot::channel();
        self.handle.spawn(async move {
            let node = self_clone.get_or_fetch_file_info(&path).await
                .map_err(|_| FspError::from(NTSTATUS(STATUS_NOT_FOUND)));
            tx.send(node);
        });
        let node=rx.blocking_recv().map_err(|_| FspError::from(NTSTATUS(STATUS_NOT_FOUND)))??;
        *out_file_info = FileInfo {
            file_attributes: node.file_attributes,
            reparse_tag: 0,
            allocation_size: ((node.size as u64 + 4095) / 4096) * 4096,
            file_size: node.size as u64,
            creation_time: system_time_to_filetime(node.creation_time),
            last_access_time: system_time_to_filetime(node.last_access_time),
            last_write_time: system_time_to_filetime(node.last_write_time),
            change_time: system_time_to_filetime(node.last_write_time),
            index_number: 0,
            hard_links: 0,
            ea_size: 0,
        };

        Ok(())
    }


    fn get_security(
        &self,
        _context: &Self::FileContext,
        security_descriptor: Option<&mut [c_void]>,
    ) -> Result<u64, FspError> {
        debug!("get_security was called");
        let open_ctx = self.open_files
            .get(_context)
            .ok_or(FspError::from(NTSTATUS(STATUS_INVALID_HANDLE)))?
            .clone();
        let self_clone = self.clone();
        let path = open_ctx.path.clone();
        let (tx, rx) = oneshot::channel();
        self.handle.spawn(async move {
            let node = self_clone.get_or_fetch_file_info(&path).await
                .map_err(|_| FspError::from(NTSTATUS(STATUS_NOT_FOUND)));
            tx.send(node);
        });
        let node=rx.blocking_recv().map_err(|_| FspError::from(NTSTATUS(STATUS_NOT_FOUND)))??;


        let sd = node.create_custom_security_descriptor()?;
        let sd_size = sd.len() as u64;

        if let Some(buffer) = security_descriptor {
            if buffer.len() >= sd.len() {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        sd.as_ptr(),
                        buffer.as_mut_ptr() as *mut u8,
                        sd.len()
                    );
                }
            }
        }

        Ok(sd_size)
    }

    fn rename(
        &self,
        context: &Self::FileContext,
        file_name: &U16CStr,
        new_file_name: &U16CStr,
        _replace_if_exists: bool,
    ) -> Result<(), FspError> {
        debug!("rename was call");
        let open_ctx = self.open_files
            .get(context)
            .ok_or(FspError::from(NTSTATUS(STATUS_INVALID_HANDLE)))?
            .clone();

        let old_path = self.normalize_path(&file_name.to_string_lossy());
        let new_path = self.normalize_path(&new_file_name.to_string_lossy());

        let old_unix_path = self.to_unix_path(&old_path);
        let new_unix_path = self.to_unix_path(&new_path);
        let client = self.client.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();

        self.handle.spawn(async move {
            let result = client.rename_file(&old_unix_path, &new_unix_path).await;
            let _ = tx.send(result);
        });

        rx.blocking_recv();

        Ok(())
    }

    fn set_basic_info(
        &self,
        context: &Self::FileContext,
        _file_attributes: u32,
        _creation_time: u64,
        _last_access_time: u64,
        _last_write_time: u64,
        _change_time: u64,
        out_file_info: &mut FileInfo,
    ) -> Result<(), FspError> {
        self.get_file_info(context, out_file_info)
    }

    fn set_file_size(
        &self,
        context: &Self::FileContext,
        _new_size: u64,
        _set_allocation_size: bool,
        out_file_info: &mut FileInfo,
    ) -> Result<(), FspError> {
        let open_ctx = self.open_files
            .get(context)
            .ok_or(FspError::from(NTSTATUS(STATUS_INVALID_HANDLE)))?
            .clone();

        if open_ctx.is_directory {
            return Err(FspError::from(NTSTATUS(STATUS_FILE_IS_A_DIRECTORY)));
        }

        self.get_file_info(context, out_file_info)
    }

    fn get_volume_info(&self, out_volume_info: &mut VolumeInfo) -> Result<(), FspError> {
        debug!("get_volume_info called");

        let label = self.drive.read().clone();
        let client = self.client.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();

        self.handle.spawn(async move {
            let result = client.get_volume_info(&*label).await;
            let _ = tx.send(result);
        });

        let info = rx.blocking_recv()
            .map_err(|_| {
                tracing::error!("get_volume_info: channel recv failed");
                FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0))
            })?
            .map_err(|e| {
                tracing::error!("get_volume_info: API call failed: {:?}", e);
                FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0))
            })?;

        let mut volume_info = unsafe { mem::zeroed::<VolumeInfo>() };

        volume_info.total_size = info.total_size;
        volume_info.free_size = info.available_space;
        volume_info.set_volume_label(&info.name);

        *out_volume_info = volume_info;

        Ok(())
    }
}



impl AsyncFileSystemContext for VirtualFileSystem {
    fn read_async(&self, context: &Self::FileContext, buffer: &mut [u8], offset: u64)
                  -> impl Future<Output = Result<u32, FspError>> + Send
    {
        let open_ctx_result = self.open_files
            .get(context)
            .ok_or(FspError::from(NTSTATUS(STATUS_INVALID_HANDLE)));

        async move {
            let open_ctx = open_ctx_result?;

            if open_ctx.is_directory {
                return Err(FspError::from(NTSTATUS(STATUS_FILE_IS_A_DIRECTORY)));
            }

            let data = self.client
                .read_file(&open_ctx.path, offset, buffer.len() as u32, *context)
                .await
                .map_err(|_| FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)))?;

            let bytes_read = data.len().min(buffer.len());
            buffer[..bytes_read].copy_from_slice(&data[..bytes_read]);

            Ok(bytes_read as u32)
        }
    }

    fn write_async(&self, context: &Self::FileContext, buffer: &[u8], offset: u64, _write_to_eof: bool, _constrained_io: bool, _file_info: &mut FileInfo) -> impl Future<Output=winfsp::Result<u32>> + Send {
        let open_ctx_result = self.open_files
            .get(context)
            .ok_or(FspError::from(NTSTATUS(STATUS_INVALID_HANDLE)));
        let buffer_vec = buffer.to_vec();

        async move {
            let open_ctx = open_ctx_result?;

            if open_ctx.is_directory {
                return Err(FspError::from(NTSTATUS(STATUS_FILE_IS_A_DIRECTORY)));
            }
            self.client.write_file(&open_ctx.path, offset, buffer_vec.clone(), context.clone(), open_ctx.flags)
                .await
                .map_err(|_| FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)))?;


            let node = self.get_or_fetch_file_info(open_ctx.path.as_str()).await
                .map_err(|_| FspError::from(NTSTATUS(STATUS_INVALID_HANDLE)))?;

            *_file_info = FileInfo {
                file_attributes: node.file_attributes,
                reparse_tag: 0,
                allocation_size: ((node.size as u64 + 4095) / 4096) * 4096,
                file_size: node.size as u64,
                creation_time: system_time_to_filetime(node.creation_time),
                last_access_time: system_time_to_filetime(node.last_access_time),
                last_write_time: system_time_to_filetime(node.last_write_time),
                change_time: system_time_to_filetime(node.last_write_time),
                index_number: 0,
                hard_links: 0,
                ea_size: 0,
            };



            Ok(buffer_vec.len() as u32)
        }
    }

    fn read_directory_async(
        &self,
        context: &Self::FileContext,
        pattern: Option<&U16CStr>,
        marker: DirMarker<'_>,
        buffer: &mut [u8],
    ) -> impl Future<Output = winfsp::Result<u32>> + Send {
        debug!("read_directory_async start");
        let open_ctx_result = self.open_files
            .get(context)
            .ok_or(FspError::from(NTSTATUS(STATUS_INVALID_HANDLE)));

        async move {
            let open_ctx = open_ctx_result?.clone();

            if !open_ctx.is_directory {
                return Err(FspError::from(NTSTATUS(STATUS_NOT_A_DIRECTORY.0)));
            }

            let entries = self.get_or_fetch_directory_list(&open_ctx.path)
                .await
                .map_err(|_| FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)))?;

            let node = self.get_or_fetch_file_info(open_ctx.path.as_str()).await
                .map_err(|_| FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)))?;



            let dir_buffer = DirBuffer::new();
            let reset = marker.is_none();

            let lock = dir_buffer.acquire(reset, None)
                .map_err(|_| FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)))?;

            if marker.is_none() {
                let mut dot_info = DirInfo::<1024>::new();
                dot_info.set_name(".").map_err(|_| FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)))?;

                let file_info = dot_info.file_info_mut();
                file_info.file_attributes = FILE_ATTRIBUTE_DIRECTORY;
                file_info.reparse_tag = 0;
                file_info.allocation_size = 0;
                file_info.file_size = 0;
                file_info.creation_time = system_time_to_filetime(node.creation_time);
                file_info.last_access_time = system_time_to_filetime(node.last_access_time);
                file_info.last_write_time = system_time_to_filetime(node.last_write_time);
                file_info.change_time = system_time_to_filetime(node.last_write_time);
                file_info.index_number = 0;
                file_info.hard_links = 0;
                file_info.ea_size = 0;

                lock.write(&mut dot_info).ok();
            }

            if marker.is_none() || marker.is_current() {
                let mut dotdot_info = DirInfo::<1024>::new();
                dotdot_info.set_name("..").map_err(|_| FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)))?;

                let file_info = dotdot_info.file_info_mut();
                file_info.file_attributes = FILE_ATTRIBUTE_DIRECTORY;
                file_info.reparse_tag = 0;
                file_info.allocation_size = 0;
                file_info.file_size = 0;
                file_info.creation_time = system_time_to_filetime(node.creation_time);
                file_info.last_access_time = system_time_to_filetime(node.last_access_time);
                file_info.last_write_time = system_time_to_filetime(node.last_write_time);
                file_info.change_time = system_time_to_filetime(node.last_write_time);
                file_info.index_number = 0;
                file_info.hard_links = 0;
                file_info.ea_size = 0;

                lock.write(&mut dotdot_info).ok();
            }

            let mut should_add = marker.is_none() || marker.is_parent();

            for entry in entries.iter() {
                if !should_add {
                    if let Some(marker_name) = marker.inner_as_cstr() {
                        if marker_name.to_string_lossy() == entry.name {
                            should_add = true;
                        }
                        continue;
                    }
                }

                let node = FileNode::from_directory_entry(entry, &open_ctx.path);

                let mut dir_info = DirInfo::<1024>::new();
                dir_info.set_name(&entry.name)
                    .map_err(|_| FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)))?;

                let file_info = dir_info.file_info_mut();
                file_info.file_attributes = node.file_attributes;
                file_info.reparse_tag = 0;
                file_info.allocation_size = ((node.size as u64 + 4095) / 4096) * 4096;
                file_info.file_size = node.size as u64;
                file_info.creation_time = system_time_to_filetime(node.creation_time);
                file_info.last_access_time = system_time_to_filetime(node.last_access_time);
                file_info.last_write_time = system_time_to_filetime(node.last_write_time);
                file_info.change_time = system_time_to_filetime(node.last_write_time);
                file_info.index_number = 0;
                file_info.hard_links = 0;
                file_info.ea_size = 0;


                if lock.write(&mut dir_info).is_err() {
                    break; // Buffer full
                }
            }

            drop(lock);

            let bytes_written = dir_buffer.read(marker, buffer);
            Ok(bytes_written)
        }
    }

    fn spawn_task(&self, future: impl Future<Output=()> + Send + 'static) {
        self.handle.spawn(future);
    }
}




///todo add support for chosoing a random drive, Problem now if a random letter is choose, I have no idea have to accuractly get said info
/// probably should read though winfsp-sys call to see otherwise query mount before, than after and see difference
pub async fn mount(client: Arc<VaultDriveClient>, mount_point: &str, drive: &str) -> anyhow::Result<()> {
    tracing::info!("Mounting VaultDrive at {}", mount_point);

    let mount_point = normalize_mount_point(mount_point)?;
    let handle = Handle::current();
    let fs = VirtualFileSystem::new(client.clone(),  handle, drive);

    let mut volume_params = VolumeParams::new();
    volume_params.sector_size(512);
    volume_params.sectors_per_allocation_unit(1);
    volume_params.volume_creation_time(system_time_to_filetime(std::time::SystemTime::now()));
    volume_params.volume_serial_number(0);
    volume_params.volume_info_timeout(1000);


    tracing::info!("Creating FileSystemHost...");

    let mut host = FileSystemHost::<VirtualFileSystem>::new_async(volume_params, fs)
        .context("Failed to create filesystem host")?;


    tracing::info!("Mounting filesystem at {}", mount_point);



    if mount_point.is_empty(){
        host.mount(MountPoint::NextFreeDrive).context("Failed to mount filesystem")?;

    }else {
        host.mount(&mount_point).context("Failed to mount filesystem")?;

    }

    tracing::info!("Starting dispatcher threads...");
    host.start().context("Failed to start FSP dispatcher")?;
    client.mounts.insert(mount_point.clone(), (Mutex::new(host), drive.to_string()));
    client.mount_points.insert(mount_point.clone());



    Ok(())
}


fn normalize_mount_point(raw: &str) -> anyhow::Result<String> {
    let trimmed = raw.trim();

    if trimmed.len() == 1 && trimmed.chars().next().unwrap().is_ascii_alphabetic() {
        return Ok(format!("{}:", trimmed.to_ascii_uppercase()));
    }

    if trimmed.len() == 2 && trimmed.ends_with(':') {
        return Ok(trimmed.to_ascii_uppercase());
    }

    let path = Path::new(trimmed);
    if path.exists() && path.is_dir() {
        return Ok(trimmed.to_string());
    }

    Err(anyhow::anyhow!(
        "Invalid mount point '{}': must be a drive letter or existing directory",
        raw
    ))
}


