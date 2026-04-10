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
use std::sync::atomic::AtomicBool;
use std::thread::Scope;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use parking_lot::{RwLock, Mutex};
use tokio::sync::{oneshot, Mutex as TokioMutex};

use bytes::Bytes;
use tokio::sync::OnceCell;
use once_cell::sync::{Lazy};
use dashmap::DashMap;
use futures::{FutureExt, SinkExt};
use log::debug;
use path_slash::PathExt;
use strum_macros::Display;
use tokio::join;
use tokio::runtime::{Handle};
use tokio::sync::oneshot::error::RecvError;
use tokio::task::JoinSet;
use tracing::{info, warn, Instrument};
use windows::core::{HSTRING, PCWSTR};
use windows::Win32::Foundation::{LocalFree, HLOCAL, STATUS_OBJECT_NAME_NOT_FOUND, STATUS_NOT_A_DIRECTORY, STATUS_INTERNAL_ERROR, STATUS_UNSUCCESSFUL, STATUS_IO_DEVICE_ERROR, STATUS_INVALID_DEVICE_REQUEST, STATUS_PENDING};
use windows::Win32::Security::Authorization::{ConvertStringSecurityDescriptorToSecurityDescriptorW, SetNamedSecurityInfoW, SE_FILE_OBJECT};

use windows::Win32::Security::{InitializeSecurityDescriptor, SetSecurityDescriptorDacl, OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, SECURITY_DESCRIPTOR};
use windows::Win32::Storage::FileSystem::{GetLogicalDrives, FILE_APPEND_DATA, FILE_FLAG_DELETE_ON_CLOSE, FILE_FLAG_NO_BUFFERING, FILE_FLAG_WRITE_THROUGH, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_READ_DATA, FILE_SHARE_DELETE, FILE_SHARE_NONE, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_WRITE_DATA};
use windows::Win32::System::SystemServices::SECURITY_DESCRIPTOR_REVISION;
use crate::client::VaultDriveClient;
use crate::proto::vaultdrive::{DirectoryEntry, FileInfoResponse, WriteSuccessResponse};

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
use x509_parser::nom::{AsBytes, Parser};
use crate::filesystem::{InodeAllocator, APPEND, DELETE, DELETE_CHILDREN, DSYNC, EXCL, EXCLUSIVE_LOCK, READ, SHARE_LOCK, SYNC, TRANSVERSE, WINDOW_SHARE_DELETE, WINDOW_SHARE_READ, WINDOW_SHARE_WRITE, WRITE};
use crate::proto;

const FILE_ATTRIBUTE_NORMAL: u32 = 0x00000080;
const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x00000010;
const FILE_ATTRIBUTE_READONLY: u32 = 0x00000001;

const STATUS_SUCCESS: i32 = 0;
const STATUS_NOT_FOUND: i32 = 0xC0000225_u32 as i32;
const STATUS_INVALID_HANDLE: i32 = 0xC0000008_u32 as i32;
const STATUS_FILE_IS_A_DIRECTORY: i32 = 0xC00000BA_u32 as i32;

const FSP_CLEANUP_DELETE: u32 = 0x01;






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
    is_compressed: bool,
}

impl FileNode {
    fn create_custom_security_descriptor(&self, scope: String) -> Result<Vec<u8>, FspError> {

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
            if !scope.is_empty() {
                HSTRING::from(format!("O:{}G:BAD:(D;;FA;;;WD)", scope))
            } else {
                HSTRING::from("O:BAG:BAD:(D;;FA;;;WD)")
            }
        } else if !scope.is_empty() {
            HSTRING::from(format!("O:{}G:BAD:(A;;0x{:08x};;;{})", scope, access_mask, scope))
        } else {
            HSTRING::from(format!("O:BAG:BAD:(A;;0x{:08x};;;WD)", access_mask))
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
            flags: info.flags,
            is_compressed: info.is_compressed
        }
    }

    fn from_directory_entry(entry: &DirectoryEntry, parent_path: &str) -> Self {
        let path = if parent_path == "\\" {
            format!("\\{}", entry.name)
        } else {
            format!("{}\\{}", parent_path, entry.name)
        };

        let modified_time = UNIX_EPOCH + Duration::from_secs(entry.modified as u64);
        let accessed_time =  UNIX_EPOCH + Duration::from_secs(entry.accessed as u64);
        let created_time =  UNIX_EPOCH + Duration::from_secs(entry.created as u64);

        let file_attributes = if entry.is_directory {
            FILE_ATTRIBUTE_DIRECTORY
        } else {
            FILE_ATTRIBUTE_NORMAL
        };

        Self {
            path,
            name: entry.name.clone(),
            size: entry.size as i64,
            is_directory: entry.is_directory,
            file_attributes,
            creation_time: created_time,
            last_access_time: accessed_time,
            last_write_time: modified_time,
            change_time: modified_time,
            flags: 0,
            is_compressed: false,

        }
    }
}

#[derive(Clone, Debug)]
struct OpenFileContext {
    path: String,
    is_directory: bool,
    is_marked_deleted: bool,
    flags: u32,
    is_compressed: bool,
}


#[derive(Clone)]
pub struct VirtualFileSystem {
    client: Arc<VaultDriveClient>,
    handle: Handle,

    open_files: Arc<DashMap<u64, OpenFileContext>>,
    next_context: Arc<InodeAllocator>,



    pending_file_requests: Arc<DashMap<String, Arc<PendingRequest>>>,
    pending_dir_requests: Arc<DashMap<String, Arc<OnceCell<Vec<DirectoryEntry>>>>>,
    flushable_writes: Arc<DashMap<u64, JoinSet<Result<WriteSuccessResponse>>>>,

    scope: Arc<RwLock<String>>,

    drive: Arc<String>,
    compress: Arc<AtomicBool>,
}
struct PendingRequest {
    result: OnceCell<Result<FileNode, i32>>,
}

impl VirtualFileSystem {
    pub fn new(
        client: Arc<VaultDriveClient>,
        handle: Handle,
        drive: String,
        scope: Arc<RwLock<String>>,
        compression: Arc<AtomicBool>,
    ) -> Self {
        Self {
            client,
            handle,
            open_files: Arc::new(DashMap::new()),
            next_context: Arc::new(InodeAllocator::new(1)),
            pending_file_requests: Arc::new(DashMap::new()),
            pending_dir_requests: Arc::new(DashMap::new()),
            flushable_writes: Arc::new(DashMap::new()),
            scope,
            drive: Arc::new(drive),
            compress: compression,
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

        debug!("get_or_fetch_file_info for: {}", path_str);

        let pending = self
            .pending_file_requests
            .entry(path_str.clone())
            .or_insert_with(|| Arc::new(PendingRequest {
                result: OnceCell::new(),
            }))
            .clone();

        let result = pending.result.get_or_init(|| async {
            let unix_path = path_str.replace("\\", "/");
            debug!("get_or_fetch_file_info unix_path: {}", unix_path);
            const TIMEOUT_DURATION: Duration = Duration::from_secs(5);

            match tokio::time::timeout(
                TIMEOUT_DURATION,
                self.client.get_file_info(&unix_path)
            ).await {
                Ok(Ok(info)) => {
                    let node = FileNode::from_file_info(info);
                    Ok(node)
                }
                ///this is the issue
                Ok(Err(_)) => Err(STATUS_OBJECT_NAME_NOT_FOUND.0),
                Err(_) => {
                    warn!("File info fetch timed out for {}", unix_path);
                    Err(STATUS_INTERNAL_ERROR.0)
                }
            }
        }).await;

        self.pending_file_requests.remove(&path_str);

        result.clone()
    }

    async fn get_or_fetch_directory_list(&self, path: &str) -> Result<Vec<DirectoryEntry>, i32> {
        let cell = self.pending_dir_requests
            .entry(path.to_string())
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone();

        let entries = cell.get_or_try_init(|| async {
            tracing::debug!("Fetching directory from network: {}", path);
            let unix_path = self.to_unix_path(path);
            let entries = self.client.list_directory(&unix_path).await
                .map_err(|_| STATUS_INTERNAL_ERROR.0)?;
            Ok::<Vec<DirectoryEntry>, i32>(entries)
        }).await
            .map_err(|_| STATUS_INTERNAL_ERROR.0)?;

        self.pending_dir_requests.remove(path);

        Ok(entries.clone())
    }
    async fn flush_pending_writes(&self, context: &u64) -> winfsp::Result<()> {
        if let Some((_, mut set)) = self.flushable_writes.remove(context) {
            while let Some(result) = set.join_next().await {
                result.map_err(|_| FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)))?;
            }
        }
        Ok(())
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
        let path = self.normalize_path(&file_name.to_string_lossy());
        debug!("get_security_by_name was called for: {}", path);

        let self_clone = self.clone();
        let (tx, rx) = oneshot::channel();
        self.handle.spawn(async move {
            let node = self_clone.get_or_fetch_file_info(&path).await
                .map_err(|_| FspError::from(NTSTATUS(STATUS_OBJECT_NAME_NOT_FOUND.0)));
            tx.send(node);
        });
        let node = rx.blocking_recv().map_err(|_| FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)))??;


        let sd = node.create_custom_security_descriptor(self.scope.read().clone())?;
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

        let share_access = unsafe {
            match winfsp_sys::FspFileSystemGetOperationContext().as_ref() {
                Some(context) => match context.Request.as_ref() {
                    Some(request) => request.Req.Create.ShareAccess,
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

        debug!("share_access raw value: {:b} granted_access: {:b}", share_access, granted_access);


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
        if (_create_options & FILE_FLAG_NO_BUFFERING.0) != 0 && (_create_options & FILE_FLAG_WRITE_THROUGH.0) != 0 {
            flags |= SYNC;
        } else if (_create_options & FILE_FLAG_WRITE_THROUGH.0) != 0 {
            flags |= DSYNC;
        }


        if share_access == FILE_SHARE_NONE.0 {
            flags |= EXCLUSIVE_LOCK;
        } else {
            flags |= SHARE_LOCK;
            if (share_access & FILE_SHARE_READ.0) != 0 {
                flags |= WINDOW_SHARE_READ;
            }
            if (share_access & FILE_SHARE_WRITE.0) != 0 {
                flags |= WINDOW_SHARE_WRITE;
            }
            if (share_access & FILE_SHARE_DELETE.0) != 0 {
                flags |= WINDOW_SHARE_DELETE;
            }
        }

        let path = self.normalize_path(&file_name.to_string_lossy());
        let unix_path = self.to_unix_path(&path);

        debug!("open was called for {:}  with flags: {:b}", path, flags);
        let client = self.client.clone();
        let self_clone = self.clone();
        let (tx, rx) = oneshot::channel();

        let context_value = self.next_context.allocate();

        self.handle.spawn(async move {
            let (open_result, info_result) = tokio::join!(
            client.open_file(&unix_path, context_value, flags),
            self_clone.get_or_fetch_file_info(&unix_path)
        );

            match (open_result, info_result) {
                (Ok(_file), Ok(info)) => {
                    if tx.send(Ok(info)).is_err() {
                        debug!("Receiver dropped before send");
                    }
                }
                (Err(e), _) => {
                    let _ = tx.send(Err(FspError::from(NTSTATUS(STATUS_IO_DEVICE_ERROR.0))));
                }
                (_, Err(number)) => {
                    let _ = tx.send(Err(FspError::from(NTSTATUS(number))));
                }
            }
        });

        let node = rx.blocking_recv()
            .map_err(|_| FspError::from(NTSTATUS(STATUS_NOT_FOUND)))??;

        let context = OpenFileContext {
            path,
            is_directory: node.is_directory,
            is_marked_deleted: _create_options & FILE_FLAG_DELETE_ON_CLOSE.0 != 0,
            flags,
            is_compressed: node.is_compressed,
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
        debug!("share_access raw value: {:b} granted_access: {:b}", share_access, _granted_access);



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
        if (create_options & FILE_FLAG_NO_BUFFERING.0) != 0 && (create_options & FILE_FLAG_WRITE_THROUGH.0) != 0 {
            flags |= SYNC;
        } else if (create_options & FILE_FLAG_WRITE_THROUGH.0) != 0 {
            flags |= DSYNC;
        }

        if share_access == FILE_SHARE_NONE.0 {
            flags |= EXCLUSIVE_LOCK;
        } else {
            flags |= SHARE_LOCK;
            if (share_access & FILE_SHARE_READ.0) != 0 {
                flags |= WINDOW_SHARE_READ;
            }
            if (share_access & FILE_SHARE_WRITE.0) != 0 {
                flags |= WINDOW_SHARE_WRITE;
            }
            if (share_access & FILE_SHARE_DELETE.0) != 0 {
                flags |= WINDOW_SHARE_DELETE;
            }
        }

        self.handle.spawn(async move {
            let result = if is_directory {
                client.create_directory(&unix_path, Some(true)).await
            } else {
                client.create_file(&unix_path, flags, file_handler.clone(), Some(true)).await
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

        let node = rx.blocking_recv().map_err(|_| FspError::from(NTSTATUS(STATUS_NOT_FOUND)))??;

        let context = OpenFileContext {
            path,
            is_directory,
            is_marked_deleted: create_options & FILE_FLAG_DELETE_ON_CLOSE.0 != 0,
            flags,
            is_compressed: node.is_compressed,
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
            let context = context.clone();
            let self_clone= self.clone();

            let (tx, rx) = tokio::sync::oneshot::channel();

            self.handle.spawn(async move {
                tx.send( self_clone.flush_pending_writes(&context).await);

            });
            rx.blocking_recv();
        }else {
            let open_ctx = match self.open_files.get(context) {
                Some(ctx) => ctx,
                None => {
                    tracing::error!("cleanup: invalid handle");
                    return;
                }
            };

            if open_ctx.is_directory  {
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
                let unix_path = self.to_unix_path(&open_ctx.path);
                let client = self.client.clone(); // Clone before moving into async block
                let (tx, rx) = oneshot::channel();

                self.handle.spawn(async move {
                    let result = client.delete_file(&unix_path).await;
                    let _ = tx.send(result); // Ignore send errors (receiver might be dropped)
                });

                match rx.blocking_recv() {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        tracing::error!("cleanup: failed to delete file: {:?}", e);
                    }
                    Err(e) => {
                        tracing::error!("cleanup: channel recv failed: {}", e);
                    }
                }
                self.flushable_writes.remove(context);

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
        debug!("overwrite was called with {:?}", open_ctx.path);
        self.handle.spawn(async move {
           let result= self_clone.client.override_file(context_clone.clone(), Some(true))
                .await
                .map_err(|_| FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)));



            match result {
                Ok(success) => {
                    let node_result=   if let Some(file_info) = success.file_info{
                        Ok(FileNode::from_file_info(file_info))
                    }else {
                        self_clone.get_or_fetch_file_info(open_ctx.path.as_str())
                            .await
                            .map_err(|_| FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)))
                    };
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


        let sd = node.create_custom_security_descriptor(self.scope.read().clone())?;
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
        debug!("renaming file: {:?} to {:?}", old_path, new_unix_path);

        self.handle.spawn(async move {
            let result = client.rename_file(&old_unix_path, &new_unix_path).await;
            let _ = tx.send(result);
        });

        rx.blocking_recv();

        Ok(())
    }


    fn set_delete(
        &self,
        context: &Self::FileContext,
        file_name: &U16CStr,
        delete_file: bool,
    ) -> winfsp::Result<()> {
        let mut open_ctx_result = self.open_files
            .get_mut(context)
            .ok_or(FspError::from(NTSTATUS(STATUS_INVALID_HANDLE)))?;

        open_ctx_result.is_marked_deleted = delete_file;

        Ok(())
    }

    fn flush(
        &self,
        context: Option<&Self::FileContext>,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        debug!("flush was called");
        if let Some(context) = context {
            let open_ctx = self.open_files
                .get(context)
                .ok_or(FspError::from(NTSTATUS(STATUS_INVALID_HANDLE)))?
                .clone();

            if open_ctx.is_marked_deleted {
                self.flushable_writes.remove(context);
            } else {
                let self_clone = self.clone();
                let context = context.clone();
                let (tx, rx) = tokio::sync::oneshot::channel();

                self.handle.spawn(async move {
                    self_clone.flush_pending_writes(&context).await;

                    let result: Result<FileNode, i32> = match self_clone.client.flush_file( context, Some(true)).await {
                        Ok(node) => {
                            if let Some(info) = node.file_info {
                                Ok(FileNode::from_file_info(info))
                            } else {
                                self_clone.get_or_fetch_file_info(open_ctx.path.as_str()).await
                                    .map_err(|_| STATUS_IO_DEVICE_ERROR.0)
                            }
                        }
                        Err(_err) => Err(STATUS_IO_DEVICE_ERROR.0),
                    };

                    let _ = tx.send(result);
                });

                match rx.blocking_recv() {
                    Ok(Ok(node)) => {
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
                    }
                    Ok(Err(status)) => {
                        return Err(FspError::from(NTSTATUS(status)))
                    }
                    Err(_) => {
                        return Err(FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)))
                    }
                }
            }
        }
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

        let label = self.drive.clone();
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
            .ok_or(FspError::from(NTSTATUS(STATUS_INVALID_HANDLE)))
            .map(|r| r.clone());

        const CHUNK_SIZE: usize = 256 * 1024; // 256KB

        async move {
            debug!("starting reading of offset {}", offset);
            let open_ctx = open_ctx_result?;
            self.flush_pending_writes(context).await;

            if open_ctx.is_directory {
                return Err(FspError::from(NTSTATUS(STATUS_FILE_IS_A_DIRECTORY)));
            }

            let total_len = buffer.len();

            if total_len <= CHUNK_SIZE {
                // Small read — no need to split
                let data = self.client
                    .read_file(&open_ctx.path, offset, total_len as u32, *context, self.compress.clone())
                    .await
                    .map_err(|_| FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)))?;

                let bytes_read = data.len().min(total_len);
                buffer[..bytes_read].copy_from_slice(&data[..bytes_read]);

                debug!("ending reading of offset {}", offset);
                return Ok(bytes_read as u32);
            }

            let mut futures = Vec::new();
            let mut chunk_offset = 0usize;

            while chunk_offset < total_len {
                let chunk_len = CHUNK_SIZE.min(total_len - chunk_offset);
                let file_offset = offset + chunk_offset as u64;
                let path = open_ctx.path.clone();
                let client = self.client.clone();
                let compress = self.compress.clone();
                let ctx = *context;

                futures.push(async move {
                    let data = client
                        .read_file(&path, file_offset, chunk_len as u32, ctx, compress)
                        .await
                        .map_err(|_| FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)))?;
                    Ok::<(usize, Vec<u8>), FspError>((chunk_offset, data))
                });

                chunk_offset += chunk_len;
            }

            let results = futures::future::join_all(futures).await;

            let mut total_bytes_read = 0u32;
            for result in results {
                let (chunk_off, data) = result?;
                let bytes_to_copy = data.len().min(total_len - chunk_off);
                buffer[chunk_off..chunk_off + bytes_to_copy].copy_from_slice(&data[..bytes_to_copy]);
                total_bytes_read += bytes_to_copy as u32;
            }

            debug!("ending reading of offset {}", offset);
            Ok(total_bytes_read)
        }
    }

    fn write_async(&self, context: &Self::FileContext, buffer: &[u8], offset: u64, _write_to_eof: bool, _constrained_io: bool, _file_info: &mut FileInfo) -> impl Future<Output=winfsp::Result<u32>> + Send {
        let open_ctx_result = self.open_files
            .get(context)
            .ok_or( FspError::from(NTSTATUS(STATUS_INVALID_HANDLE)))
            .map(|r| r.clone());

            async move {
            debug!("starting writing of offset {}", offset);
            let open_ctx = open_ctx_result?.clone();

            if open_ctx.is_directory {
                return Err(FspError::from(NTSTATUS(STATUS_FILE_IS_A_DIRECTORY)));
            }

            if open_ctx.flags != SYNC && open_ctx.flags != DSYNC {
                let (server_addr_guard, session_guard) = join!(
    self.client.server_addr.read(),
    self.client.session.read()
);

                let server_addr = *server_addr_guard;
                let username = session_guard.as_ref().unwrap().authenticate_request.user_id.clone();
                drop(session_guard);
                drop(server_addr_guard);
                let stream = self.client.quic.open_bi(server_addr, username).await
                    .map_err(|_| FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)))?;
                let client = self.client.clone();
                let path = open_ctx.path.clone();
                let compress = self.compress.clone();
                let context = context.clone();
                let flags = open_ctx.flags;
                let is_compressed = open_ctx.is_compressed;


                self.flushable_writes
                    .entry(context.clone())
                    .or_insert_with(|| JoinSet::new())
                    .spawn({
                        let buffer = buffer.to_vec();
                        async move{
                            client.write_file(&path, offset, &buffer, context.clone(), flags, compress, is_compressed, Some(false), Some(stream))
                                .await
                        }
                    });

                return Ok(buffer.len() as u32);
            }

            let written = self.client.write_file(&open_ctx.path, offset, buffer, context.clone(),open_ctx.flags, self.compress.clone(), open_ctx.is_compressed, Some(true), None)
                .await
                .map_err(|_| FspError::from(NTSTATUS(STATUS_INTERNAL_ERROR.0)))?;

            let node = if let Some(file_info) = written.file_info {
                FileNode::from_file_info(file_info)
            } else {
                self.get_or_fetch_file_info(open_ctx.path.as_str()).await
                    .map_err(|_| FspError::from(NTSTATUS(STATUS_INVALID_HANDLE)))?
            };

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

            debug!("ending writing of offset {}", offset);
            Ok(written.bytes_written as u32)
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
            .ok_or(FspError::from(NTSTATUS(STATUS_INVALID_HANDLE)))
            .map(|r| r.clone());

        async move {
            let open_ctx = open_ctx_result?;

            if !open_ctx.is_directory {
                return Err(FspError::from(NTSTATUS(STATUS_NOT_A_DIRECTORY.0)));
            }

            let (entries, node) = tokio::try_join!(
    self.get_or_fetch_directory_list(&open_ctx.path),
    self.get_or_fetch_file_info(open_ctx.path.as_str()),
)
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


pub async fn mount(client: Arc<VaultDriveClient>, mount_point: &mut String, drive: &str, scope: Arc<RwLock<String>>, compression: Arc<AtomicBool>) -> anyhow::Result<()> {
    if mount_point.is_empty() {
        *mount_point = next_free_drive().context("No free drive letters available")?;
    }
    tracing::info!("Mounting VaultDrive at {}", mount_point);

    let mut mount_point = normalize_mount_point(mount_point)?;
    let handle = Handle::current();

    let fs = VirtualFileSystem::new(client.clone(),  handle, drive.to_string(), scope.clone(), compression.clone());
    

    let mut volume_params = VolumeParams::new();
    volume_params.filesystem_name("VaultDrive");

    volume_params.sector_size(4096 );
    volume_params.sectors_per_allocation_unit(1);
    volume_params.volume_creation_time(system_time_to_filetime(std::time::SystemTime::now()));
    volume_params.volume_serial_number(0);
    volume_params.volume_info_timeout(1000);
    volume_params.file_info_timeout(10);
    volume_params.dir_info_timeout(10);
    volume_params.security_timeout(1000);
    volume_params.allow_open_in_kernel_mode(false);
    volume_params.flush_and_purge_on_cleanup(false);
    volume_params.post_cleanup_when_modified_only(false);





    tracing::info!("Creating FileSystemHost...");

    let mut host = FileSystemHost::<VirtualFileSystem>::new_async(volume_params, fs)
        .inspect_err(|e| tracing::error!("Mount failed: {:?}", e))
        .context("Failed to create filesystem host")?;


    tracing::info!("Mounting filesystem at {}", mount_point);

    host.mount(&mount_point).context("Failed to mount filesystem")?;

    tracing::info!("Starting dispatcher threads...");
    host.start().context("Failed to start FSP dispatcher")?;
    client.mounts.insert(mount_point.clone(), (Mutex::new(host), drive.to_string(), scope, compression));


    Ok(())
}

fn next_free_drive() -> Option<String> {
    let used = unsafe { GetLogicalDrives() };
    (0..26u32)
        .rev()
        .find(|&i| used & (1 << i) == 0)
        .map(|i| format!("{}:", (b'A' + i as u8) as char))
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


