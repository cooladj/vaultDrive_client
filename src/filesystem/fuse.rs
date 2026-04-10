

#![cfg(unix)]

use anyhow::{Result, Context};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use fuser::{FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow, INodeNo, KernelConfig, Generation, FileHandle, OpenFlags, FopenFlags, LockOwner, WriteFlags, RenameFlags, AccessFlags};
use libc::{chown, EACCES, EEXIST, EINVAL, EIO, EISDIR, ENOENT, ENOSYS, ENOTDIR, ENOTEMPTY, O_APPEND, O_CREAT, O_EXCL, O_RDONLY, O_RDWR, O_TRUNC, O_WRONLY};
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use tokio::runtime::Handle;
use tokio::sync::{oneshot, OnceCell};
use tracing::{debug, error, info, warn};

use crate::client::VaultDriveClient;
use crate::filesystem::{
    InodeAllocator, APPEND, DELETE, DELETE_CHILDREN, EXCL, EXCLUSIVE_LOCK,
    READ, SHARE_LOCK, TRANSVERSE, WRITE,
};
use crate::proto::vaultdrive::{DirectoryEntry, FileInfoResponse};

// ============================================================================
// Constants
// ============================================================================

const TTL: Duration = Duration::from_secs(1);
const BLOCK_SIZE: u64 = 512;

// ============================================================================
// Global State
// ============================================================================

/// Global mount tracking
pub static MOUNTS: Lazy<DashMap<String, (Arc<Mutex<fuser::BackgroundSession>>, String)>> =
    Lazy::new(|| DashMap::new());

// ============================================================================
// Data Structures
// ============================================================================

/// Internal file node representing cached metadata
#[derive(Clone, Debug)]
struct FileNode {
    /// Inode number
    ino: u64,
    /// Full path (Unix-style)
    path: String,
    /// File/directory name
    name: String,
    /// Size in bytes
    size: i64,
    /// Is this a directory?
    is_directory: bool,
    /// Creation time
    creation_time: SystemTime,
    /// Last access time
    last_access_time: SystemTime,
    /// Last modification time
    last_write_time: SystemTime,
    /// Permission flags from server
    flags: u32,
}

impl FileNode {
    fn from_file_info(info: FileInfoResponse, ino: u64) -> Self {
        let creation_time = UNIX_EPOCH + Duration::from_secs(info.created_at as u64);
        let modified_time = UNIX_EPOCH + Duration::from_secs(info.modified_at as u64);
        let accessed_time = UNIX_EPOCH + Duration::from_secs(info.accessed_at as u64);

        Self {
            ino,
            path: info.path.clone(),
            name: info.name.clone(),
            size: info.size as i64,
            is_directory: info.is_directory,
            creation_time,
            last_access_time: accessed_time,
            last_write_time: modified_time,
            flags: info.flags,
        }
    }

    fn from_directory_entry(entry: &DirectoryEntry, parent_path: &str, ino: u64) -> Self {
        let path = if parent_path == "/" {
            format!("/{}", entry.name)
        } else {
            format!("{}/{}", parent_path, entry.name)
        };

        let modified_time = UNIX_EPOCH + Duration::from_secs(entry.modified as u64);

        Self {
            ino,
            path,
            name: entry.name.clone(),
            size: entry.size as i64,
            is_directory: entry.is_directory,
            creation_time: modified_time,
            last_access_time: modified_time,
            last_write_time: modified_time,
            flags: 0,
        }
    }

    /// Convert to fuser FileAttr
    fn to_file_attr(&self, uid: u32, gid: u32) -> FileAttr {
        let kind = if self.is_directory {
            FileType::Directory
        } else {
            FileType::RegularFile
        };

        // Build permission bits based on flags
        let perm = self.calculate_permissions();

        let blocks = if self.is_directory {
            0
        } else {
            (self.size as u64 + BLOCK_SIZE - 1) / BLOCK_SIZE
        };

        FileAttr {
            ino: INodeNo(self.ino),
            size: self.size as u64,
            blocks,
            atime: self.last_access_time,
            mtime: self.last_write_time,
            ctime: self.last_write_time,
            crtime: self.creation_time,
            kind,
            perm,
            nlink: if self.is_directory { 2 } else { 1 },
            uid,
            gid,
            rdev: 0,
            blksize: BLOCK_SIZE as u32,
            flags: 0,
        }
    }

    /// Calculate Unix permission bits from flags
    fn calculate_permissions(&self) -> u16 {
        let mut perm: u16 = 0;

        if self.is_directory {
            // Directories need execute bit to be traversable
            if self.flags & TRANSVERSE != 0 || self.flags & READ != 0 {
                perm |= 0o111; // Execute for owner, group, others
            }
        }

        if self.flags & READ != 0 {
            perm |= 0o444; // Read for owner, group, others
        }

        if self.flags & WRITE != 0 {
            perm |= 0o222; // Write for owner, group, others
        }
        perm
    }
}

/// Context for an open file/directory handle
#[derive(Clone, Debug)]
struct OpenFileContext {
    /// Inode number
    ino: u64,
    /// Full path
    path: String,
    /// Is this a directory?
    is_directory: bool,
    /// Open flags (READ, WRITE, APPEND, etc.)
    flags: u32,
    /// File handle ID (for server communication)
    fh: u64,
}

/// Pending request for deduplication
struct PendingRequest {
    result: OnceCell<Result<FileNode, i32>>,
}


#[derive(Clone)]
pub struct VirtualFileSystem {
    client: Arc<VaultDriveClient>,
    handle: Handle,
    drive: Arc<RwLock<String>>,

    inode_allocator: Arc<InodeAllocator>,
    path_to_inode: Arc<DashMap<String, u64>>,
    inode_to_node: Arc<DashMap<u64, FileNode>>,

    /// File handle allocator
    fh_allocator: Arc<InodeAllocator>,
    open_files: Arc<DashMap<u64, OpenFileContext>>,

    pending_file_requests: Arc<DashMap<String, Arc<PendingRequest>>>,
    pending_dir_requests: Arc<DashMap<String, Arc<OnceCell<Vec<DirectoryEntry>>>>>,
    
    lookup_counts: Arc<DashMap<u64, u64>>,


    scope: Arc<RwLock<String>>,

    compress: Arc<AtomicBool>,

    uid: u32,
    gid: u32,
}

impl VirtualFileSystem {
    pub fn new(client: Arc<VaultDriveClient>, handle: Handle, drive: &str,scope: Arc<RwLock<String>>, compress: Arc<AtomicBool>,) -> Self {
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };

        let fs = Self {
            client,
            handle,
            drive: Arc::new(RwLock::new(drive.to_string())),
            inode_allocator: Arc::new(InodeAllocator::new(INodeNo::ROOT.0 + 1)),
            path_to_inode: Arc::new(DashMap::new()),
            inode_to_node: Arc::new(DashMap::new()),
            fh_allocator: Arc::new(InodeAllocator::new(1)),
            open_files: Arc::new(DashMap::new()),
            pending_file_requests: Arc::new(DashMap::new()),
            pending_dir_requests: Arc::new(DashMap::new()),
            lookup_counts: Arc::new(DashMap::new()),
            scope,
            compress,
            uid,
            gid,
        };

        fs.path_to_inode.insert("/".to_string(), INodeNo::ROOT.0);

        fs
    }

    fn get_or_create_inode(&self, path: &str) -> u64 {
        if let Some(ino) = self.path_to_inode.get(path) {
            return *ino;
        }

        let ino = self.inode_allocator.allocate();
        self.path_to_inode.insert(path.to_string(), ino);
        ino
    }

    fn get_inode(&self, path: &str) -> Option<u64> {
        self.path_to_inode.get(path).map(|v| *v)
    }

    fn get_node(&self, ino: u64) -> Option<FileNode> {
        self.inode_to_node.get(&ino).map(|v| v.clone())
    }

    fn store_node(&self, node: FileNode) {
        self.path_to_inode.insert(node.path.clone(), node.ino);
        self.inode_to_node.insert(node.ino, node);
    }

    fn get_path(&self, ino: u64) -> Option<String> {
        self.inode_to_node.get(&ino).map(|n| n.path.clone())
    }

    fn build_path(&self, parent: u64, name: &OsStr) -> Option<String> {
        let parent_path = if parent == INodeNo::ROOT.0 {
            "/".to_string()
        } else {
            self.get_path(parent)?
        };

        let name_str = name.to_str()?;
        if parent_path == "/" {
            Some(format!("/{}", name_str))
        } else {
            Some(format!("{}/{}", parent_path, name_str))
        }
    }

    fn convert_open_flags(&self, flags: i32) -> u32 {

        let mut internal_flags: u32 = 0;

        let access_mode = flags & libc::O_ACCMODE;
        match access_mode {
            O_RDONLY => {
                internal_flags |= READ;
            }
            O_WRONLY => {
                internal_flags |= WRITE;
            }
            O_RDWR => {
                internal_flags |= READ | WRITE;
            }
            _ => {}
        }

        if flags & O_APPEND != 0 {
            internal_flags |= APPEND;
        }

        if flags & O_EXCL != 0 {
            internal_flags |= EXCL;
        }

        // FUSE doesn't have direct share mode support like Windows,
        // so we default to shared access
        internal_flags |= SHARE_LOCK;

        internal_flags
    }

    fn get_or_fetch_file_info_blocking(&self, path: &str) -> Result<FileNode, i32> {
        let path_str = path.to_string();

        let pending = self
            .pending_file_requests
            .entry(path_str.clone())
            .or_insert_with(|| {
                Arc::new(PendingRequest {
                    result: OnceCell::new(),
                })
            })
            .clone();

        let self_clone = self.clone();
        let path_clone = path_str.clone();

        let (tx, rx) = oneshot::channel();

        self.handle.spawn(async move {
            let result = pending
                .result
                .get_or_init(|| async {
                    const TIMEOUT_DURATION: Duration = Duration::from_secs(5);

                    match tokio::time::timeout(
                        TIMEOUT_DURATION,
                        self_clone.client.get_file_info(&path_clone),
                    )
                        .await
                    {
                        Ok(Ok(info)) => {
                            let ino = self_clone.get_or_create_inode(&path_clone);
                            let node = FileNode::from_file_info(info, ino);
                            self_clone.store_node(node.clone());
                            Ok(node)
                        }
                        Ok(Err(_)) => Err(ENOENT),
                        Err(_) => {
                            warn!("File info fetch timed out for {}", path_clone);
                            Err(EIO)
                        }
                    }
                })
                .await;

            let _ = tx.send(result.clone());
        });

        let result = rx.blocking_recv().map_err(|_| EIO)?;

        // Cleanup
        self.pending_file_requests.remove(&path_str);

        result
    }

    fn get_or_fetch_directory_list_blocking(&self, path: &str) -> Result<Vec<DirectoryEntry>, i32> {
        let path_str = path.to_string();

        let cell = self
            .pending_dir_requests
            .entry(path_str.clone())
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone();

        let self_clone = self.clone();
        let path_clone = path_str.clone();

        let (tx, rx) = oneshot::channel();

        self.handle.spawn(async move {
            let result = cell
                .get_or_try_init(|| async {
                    debug!("Fetching directory from network: {}", path_clone);
                    self_clone
                        .client
                        .list_directory(&path_clone)
                        .await
                        .map_err(|_| EIO)
                })
                .await;

            let _ = tx.send(result.cloned());
        });

        let result = rx.blocking_recv().map_err(|_| EIO)??;

        // Cleanup
        self.pending_dir_requests.remove(&path_str);

        Ok(result)
    }

}



impl Filesystem for VirtualFileSystem {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig)  -> Result<(), libc::c_int> {
        info!("FUSE filesystem initialized");
        Ok(())
    }

    fn destroy(&mut self) {
        info!("FUSE filesystem destroyed");
    }
    fn forget(&self, _req: &Request, _ino: INodeNo, _nlookup: u64) {
        debug!("forget: ino={}, nlookup={}", _ino, _nlookup);

        if _ino == INodeNo::ROOT {
            return;
        }

        let should_remove = self
            .lookup_counts
            .get_mut(&_ino.0)
            .map(|mut count| {
                *count = count.saturating_sub(_nlookup);
                *count == 0
            })
            .unwrap_or(true);

        if should_remove {
            self.lookup_counts.remove(&_ino.0);
            if let Some((_, node)) = self.inode_to_node.remove(&_ino.0) {
                self.path_to_inode.remove(&node.path);
                debug!("forget: removed inode {} (path: {})", _ino.0, node.path);
            }
        }
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry)  {
        debug!("lookup: parent={}, name={:?}", parent.0, name);

        let path = match self.build_path(parent.0, name) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        match self.get_or_fetch_file_info_blocking(&path) {
            Ok(node) => {
                self.lookup_counts
                    .entry(node.ino)
                    .and_modify(|c| *c += 1)
                    .or_insert(1);

                let attr = node.to_file_attr(self.uid, self.gid);
                reply.entry(&TTL, &attr, Generation);
            }
            Err(e) => {
                reply.error(e);
            }
        }
    }

        fn getattr(&self, _req: &Request, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        debug!("getattr: ino={}", ino);

        // Handle root inode specially
        if ino == INodeNo::ROOT {
            match self.get_or_fetch_file_info_blocking("/") {
                Ok(node) => {
                    let mut attr = node.to_file_attr(self.uid, self.gid);
                    attr.ino = INodeNo::ROOT;
                    reply.attr(&TTL, &attr);
                }
                Err(_) => {
                    // Return default root attributes
                    let attr = FileAttr {
                        ino: INodeNo::ROOT,
                        size: 0,
                        blocks: 0,
                        atime: UNIX_EPOCH,
                        mtime: UNIX_EPOCH,
                        ctime: UNIX_EPOCH,
                        crtime: UNIX_EPOCH,
                        kind: FileType::Directory,
                        perm: 0o755,
                        nlink: 2,
                        uid: self.uid,
                        gid: self.gid,
                        rdev: 0,
                        blksize: BLOCK_SIZE as u32,
                        flags: 0,
                    };
                    reply.attr(&TTL, &attr);
                }
            }
            return;
        }

        // Get path for inode
        let path = match self.get_path(ino) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        match self.get_or_fetch_file_info_blocking(&path) {
            Ok(node) => {
                let attr = node.to_file_attr(self.uid, self.gid);
                reply.attr(&TTL, &attr);
            }
            Err(e) => {
                reply.error(e);
            }
        }
    }


    /// Open a file
    fn open(&self, _req: &Request, _ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        debug!("open: ino={}, flags={:#x}", _ino, _flags);

        let path = match self.get_path(_ino.0) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let internal_flags = self.convert_open_flags(_flags.0);
        let fh = self.fh_allocator.allocate();

        let client = self.client.clone();
        let path_clone = path.clone();

        let (tx, rx) = oneshot::channel();

        self.handle.spawn(async move {
            let result = client.open_file(&path_clone, fh, internal_flags).await;
            let _ = tx.send(result);
        });

        match rx.blocking_recv() {
            Ok(Ok(_)) => {
                let ctx = OpenFileContext {
                    ino,
                    path,
                    is_directory: false,
                    flags: internal_flags,
                    fh,
                };
                self.open_files.insert(fh, ctx);

                reply.opened(FileHandle(fh), FopenFlags());
            }
            Ok(Err(e)) => {
                error!("open: failed to open file: {:?}", e);
                self.fh_allocator.free(fh);
                reply.error(EIO);
            }
            Err(_) => {
                self.fh_allocator.free(fh);
                reply.error(EIO);
            }
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        debug!("read: ino={}, fh={}, offset={}, size={}", ino, fh, offset, size);

        let ctx = match self.open_files.get(&fh.0) {
            Some(c) => c.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let client = self.client.clone();

        let (tx, rx) = oneshot::channel();

        self.handle.spawn(async move {
            let result = client
                .read_file(&ctx.path, offset as u64, size, fh.0, self.compress)
                .await;
            let _ = tx.send(result);
        });

        match rx.blocking_recv() {
            Ok(Ok(data)) => {
                reply.data(&data);
            }
            Ok(Err(e)) => {
                error!("read: failed: {:?}", e);
                reply.error(EIO);
            }
            Err(_) => {
                reply.error(EIO);
            }
        }
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        write_flags: WriteFlags,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        debug!(
            "write: ino={}, fh={}, offset={}, size={}",
            ino,
            fh,
            offset,
            data.len()
        );

        let ctx = match self.open_files.get(&fh.0) {
            Some(c) => c.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let client = self.client.clone();
        let flags = ctx.flags;

        let (tx, rx) = oneshot::channel();

        self.handle.spawn(async move {
            let result = client
                .write_file(&ctx.path, offset as u64, data, fh.0, flags, None)
                .await;
            let _ = tx.send(result);
        });

        match rx.blocking_recv() {
            Ok(Ok(written)) => {
                reply.written(written.bytes_written as u32);
            }
            Ok(Err(e)) => {
                error!("write: failed: {:?}", e);
                reply.error(EIO);
            }
            Err(_) => {
                reply.error(EIO);
            }
        }
    }

    fn flush(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        debug!("flush: ino={}, fh={}", ino, fh);
        reply.ok();
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        debug!("release: ino={}, fh={}", _ino, _fh);

        if let Some((_, ctx)) = self.open_files.remove(&_fh.0) {
            let client = self.client.clone();

            let (tx, rx) = oneshot::channel();

            self.handle.spawn(async move {
                let result = client.close_file(_fh.0).await;
                let _ = tx.send(result);
            });

            match rx.blocking_recv() {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    error!("release: failed to close file: {:?}", e);
                }
                Err(_) => {
                    error!("release: channel recv failed");
                }
            }

            self.fh_allocator.free(_fh.0);
        }

        reply.ok();
    }

    fn opendir(&self, _req: &Request, _ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        debug!("opendir: ino={}", _ino.0);

        let path = if ino == INodeNo::ROOT.0 {
            "/".to_string()
        } else {
            match self.get_path(_ino.0) {
                Some(p) => p,
                None => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        let fh = self.fh_allocator.allocate();
        let ctx = OpenFileContext {
            ino,
            path,
            is_directory: true,
            flags: READ,
            fh,
        };
        self.open_files.insert(fh, ctx);

        reply.opened(FileHandle(fh), 0);
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        reply: ReplyDirectory,
    ){
        debug!("readdir: ino={}, fh={}, offset={}", ino, fh, offset);

        let ctx = match self.open_files.get(&fh.0) {
            Some(c) => c.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        if !ctx.is_directory {
            reply.error(ENOTDIR);
            return;
        }

        let entries = match self.get_or_fetch_directory_list_blocking(&ctx.path) {
            Ok(e) => e,
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        let mut current_offset = 0u64;

        if offset <= current_offset {
            if reply.add(ino, (current_offset + 1) as u64, FileType::Directory, ".") {
                reply.ok();
                return;
            }
        }
        current_offset += 1;

        if offset <= current_offset {
            let parent_ino = if ino == INodeNo::ROOT {
                INodeNo::ROOT.0
            } else {
                let parent_path = Path::new(&ctx.path)
                    .parent()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_else(|| "/".to_string());
                self.get_inode(&parent_path).unwrap_or(INodeNo::ROOT.0)
            };

            if reply.add(INodeNo(parent_ino), (current_offset + 1) as u64, FileType::Directory, "..") {
                reply.ok();
                return;
            }
        }
        current_offset += 1;

        for entry in entries.iter() {
            if offset <= current_offset {
                let entry_ino = self.get_or_create_inode(&format!(
                    "{}/{}",
                    if ctx.path == "/" { "" } else { &ctx.path },
                    entry.name
                ));

                let file_type = if entry.is_directory {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                };

                let node = FileNode::from_directory_entry(entry, &ctx.path, entry_ino);
                self.store_node(node);

                if reply.add(INodeNo(entry_ino), (current_offset + 1) as u64, file_type, &entry.name) {
                    reply.ok();
                    return;
                }
            }
            current_offset += 1;
        }

        reply.ok();
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    )
    {
        debug!("releasedir: ino={}, fh={}", _ino, _fh);

        if let Some((_, _ctx)) = self.open_files.remove(&_fh.0) {
            let (tx, rx) = oneshot::channel();
            let client = self.client.clone();
            self.handle.spawn(async move {
                let result = client.close_file(_fh.0).await;
                let _ = tx.send(result);
            });

            match rx.blocking_recv() {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    error!("release: failed to close file: {:?}", e);
                }
                Err(_) => {
                    error!("release: channel recv failed");
                }
            }
            self.fh_allocator.free(_fh.0);
        }

        reply.ok();
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        debug!("create: parent={}, name={:?}, flags={:#x}", parent, name, flags);

        let path = match self.build_path(parent.0, name) {
            Some(p) => p,
            None => {
                reply.error(EINVAL);
                return;
            }
        };

        let internal_flags = self.convert_open_flags(flags) | EXCL;
        let fh = self.fh_allocator.allocate();

        let client = self.client.clone();
        let path_clone = path.clone();

        let (tx, rx) = oneshot::channel();

        self.handle.spawn(async move {
            let result = client.create_file(&path_clone, 0, fh).await;
            let _ = tx.send(result);
        });

        match rx.blocking_recv() {
            Ok(Ok(_)) => {
                // Fetch the created file's info
                match self.get_or_fetch_file_info_blocking(&path) {
                    Ok(node) => {
                        let ctx = OpenFileContext {
                            ino: node.ino,
                            path: path.clone(),
                            is_directory: false,
                            flags: internal_flags,
                            fh,
                        };
                        self.open_files.insert(fh, ctx);

                        let attr = node.to_file_attr(self.uid, self.gid);
                        reply.created(&TTL, &attr, 0, FileHandle(fh), 0);
                    }
                    Err(e) => {
                        self.fh_allocator.free(fh);
                        reply.error(e);
                    }
                }
            }
            Ok(Err(e)) => {
                error!("create: failed to create file: {:?}", e);
                self.fh_allocator.free(fh);
                reply.error(EIO);
            }
            Err(_) => {
                self.fh_allocator.free(fh);
                reply.error(EIO);
            }
        }
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        debug!("mkdir: parent={}, name={:?}", parent, name);

        let path = match self.build_path(parent.0, name) {
            Some(p) => p,
            None => {
                reply.error(EINVAL);
                return;
            }
        };

        let client = self.client.clone();
        let path_clone = path.clone();

        let (tx, rx) = oneshot::channel();

        self.handle.spawn(async move {
            let result = client.create_directory(&path_clone, Some(true)).await;
            let _ = tx.send(result);
        });

        match rx.blocking_recv() {
            Ok(Ok(_)) => {
                match self.get_or_fetch_file_info_blocking(&path) {
                    Ok(node) => {
                        let attr = node.to_file_attr(self.uid, self.gid);
                        reply.entry(&TTL, &attr, 0);
                    }
                    Err(e) => {
                        reply.error(e);
                    }
                }
            }
            Ok(Err(e)) => {
                error!("mkdir: failed: {:?}", e);
                reply.error(EIO);
            }
            Err(_) => {
                reply.error(EIO);
            }
        }
    }

    fn unlink(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        reply: ReplyEmpty,
    ) {
        debug!("unlink: parent={}, name={:?}", parent, name);

        let path = match self.build_path(parent.0, name) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let client = self.client.clone();
        let path_clone = path.clone();

        let (tx, rx) = oneshot::channel();

        self.handle.spawn(async move {
            let result = client.delete_file(&path_clone).await;
            let _ = tx.send(result);
        });

        match rx.blocking_recv() {
            Ok(Ok(_)) => {
                // Remove from cache
                if let Some(ino) = self.path_to_inode.remove(&path) {
                    self.inode_to_node.remove(&ino.1);
                }
                reply.ok();
            }
            Ok(Err(e)) => {
                error!("unlink: failed: {:?}", e);
                reply.error(EIO);
            }
            Err(_) => {
                reply.error(EIO);
            }
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("rmdir: parent={}, name={:?}", parent, name);

        let path = match self.build_path(parent, name) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        match self.get_or_fetch_directory_list_blocking(&path) {
            Ok(entries) => {
                if !entries.is_empty() {
                    reply.error(ENOTEMPTY);
                    return;
                }
            }
            Err(e) => {
                reply.error(e);
                return;
            }
        }

        let client = self.client.clone();
        let path_clone = path.clone();

        let (tx, rx) = oneshot::channel();

        self.handle.spawn(async move {
            let result = client.delete_file(&path_clone).await;
            let _ = tx.send(result);
        });

        match rx.blocking_recv() {
            Ok(Ok(_)) => {
                if let Some(ino) = self.path_to_inode.remove(&path) {
                    self.inode_to_node.remove(&ino.1);
                }
                reply.ok();
            }
            Ok(Err(e)) => {
                error!("rmdir: failed: {:?}", e);
                reply.error(EIO);
            }
            Err(_) => {
                reply.error(EIO);
            }
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        debug!(
            "rename: parent={}, name={:?}, newparent={}, newname={:?}",
            parent, name, newparent, newname
        );

        let old_path = match self.build_path(parent.0, name) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let new_path = match self.build_path(newparent.0, newname) {
            Some(p) => p,
            None => {
                reply.error(EINVAL);
                return;
            }
        };

        let client = self.client.clone();
        let old_path_clone = old_path.clone();
        let new_path_clone = new_path.clone();

        let (tx, rx) = oneshot::channel();

        self.handle.spawn(async move {
            let result = client.rename_file(&old_path_clone, &new_path_clone).await;
            let _ = tx.send(result);
        });

        match rx.blocking_recv() {
            Ok(Ok(_)) => {
                // Update cache
                if let Some((_, ino)) = self.path_to_inode.remove(&old_path) {
                    if let Some(mut node) = self.inode_to_node.get_mut(&ino) {
                        node.path = new_path.clone();
                        node.name = newname.to_string_lossy().to_string();
                    }
                    self.path_to_inode.insert(new_path, ino);
                }
                reply.ok();
            }
            Ok(Err(e)) => {
                error!("rename: failed: {:?}", e);
                reply.error(EIO);
            }
            Err(_) => {
                reply.error(EIO);
            }
        }
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        debug!("statfs");

        let label = self.drive.read().clone();
        let client = self.client.clone();

        let (tx, rx) = oneshot::channel();

        self.handle.spawn(async move {
            let result = client.get_volume_info(&label).await;
            let _ = tx.send(result);
        });

        match rx.blocking_recv() {
            Ok(Ok(info)) => {
                let blocks = info.total_size / BLOCK_SIZE;
                let bfree = info.available_space / BLOCK_SIZE;

                reply.statfs(
                    blocks,        
                    bfree,         
                    bfree,         
                    0,          
                    0,             
                    BLOCK_SIZE as u32, 
                    255,           
                    BLOCK_SIZE as u32, 
                );
            }
            Ok(Err(e)) => {
                error!("statfs: failed: {:?}", e);
                // Return default values
                reply.statfs(0, 0, 0, 0, 0, BLOCK_SIZE as u32, 255, BLOCK_SIZE as u32);
            }
            Err(_) => {
                reply.statfs(0, 0, 0, 0, 0, BLOCK_SIZE as u32, 255, BLOCK_SIZE as u32);
            }
        }
    }

    fn access(
        &self,
        _req: &Request,
        ino: INodeNo,
        mask: AccessFlags,
        reply: ReplyEmpty,
    ) {
        debug!("access: ino={}, mask={:#x}", ino, mask);


        // probably need to get file_info than compare the mask with the flags 
        // if ok return ok if false return .err
        reply.ok();
    }
}


pub async fn mount(
    client: Arc<VaultDriveClient>,
    mount_point: &str,
    drive: &str,
    scope: Arc<RwLock<String>>,
    compression: Arc<AtomicBool>
) -> Result<()> {
    info!("Mounting VaultDrive at {}", mount_point);

    let mount_point = normalize_mount_point(mount_point)?;
    let handle = Handle::current();
    let fs = VirtualFileSystem::new(client.clone(), handle, drive,scope.clone(), compression.clone());

    let options = vec![
        MountOption::FSName("vaultdrive".to_string()),
        MountOption::AutoUnmount,
        MountOption::Async,
        MountOption::DefaultPermissions,
    ];



    info!("Starting FUSE session...");
    let mut config = fuser::Config::default();
    config.mount_options = options;


    let session = fuser::spawn_mount2(fs, &mount_point, &config)
        .context("Failed to mount filesystem")?;

    client.mounts.insert(
        mount_point.clone(),
        (Arc::new(Mutex::new(session)), drive.to_string(), scope, compression),
    );

    info!("Filesystem mounted successfully at {}", mount_point);

    Ok(())
}




fn normalize_mount_point(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    let path = Path::new(trimmed);

    // Ensure path is absolute
    if !path.is_absolute() {
        anyhow::bail!(
            "Mount point '{}' must be an absolute path",
            raw
        );
    }

    // Create directory if it doesn't exist
    if !path.exists() {
        std::fs::create_dir_all(path).context("Failed to create mount point directory")?;
    }

    // Verify it's a directory
    if !path.is_dir() {
        anyhow::bail!(
            "Mount point '{}' is not a directory",
            raw
        );
    }

    Ok(trimmed.to_string())
}
