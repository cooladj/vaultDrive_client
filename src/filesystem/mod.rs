use anyhow::Result;
use std::sync::Arc;
use tracing::debug;
use crate::client::VaultDriveClient;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::collections::VecDeque;

#[cfg(target_os = "windows")]
pub mod winfsp;

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub mod fuse;

pub async fn mount(
    client: Arc<VaultDriveClient>,
    mount_point: &str,
    drive: &str,
) -> Result<()> {
    #[cfg(target_os = "windows")]
    {
        let mount = winfsp::mount(client.clone(), mount_point, drive).await?;
    }

    #[cfg(unix)]
    {
        let mount = fuse::mount(client.clone(), mount_point, drive).await?;
    }

    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    {
        anyhow::bail!("Filesystem mounting not supported on this platform")
    }





    Ok(())
}



/// this return a tuple of (path, drivename)
pub async fn mount_to_UI_tuple(client: Arc<VaultDriveClient> ) -> Result<Vec<(Arc<str>, Arc<str>)>> {
        debug!("mount_map_to_tuple: {} mounts", client.mounts.len());

        let results: Vec<(Arc<str>, Arc<str>)> = client.mounts
            .iter()
            .map(|entry| {
                let (k, (_, label)) = entry.pair();
                (Arc::<str>::from(k.as_str()), Arc::<str>::from(label.as_str()))
            })
            .collect();

        Ok(results)
    

}

pub const READ: u32 = 1 << 0;
pub const WRITE: u32 = 1 << 1;
pub const APPEND: u32 = 1 << 2;
pub const TRUNCATE: u32 = 1 << 3;
pub const EXCL: u32 = 1 << 4;
pub const SYNC: u32 = 1 << 5;
pub const DSYNC: u32 = 1 << 6;
pub const EXCLUSIVE_LOCK: u32 = 1 << 7;
pub const SHARE_LOCK: u32 = 1 << 8;
pub const UNLOCK_LOCK: u32 = 1 << 9;
pub const DELETE: u32 = 1 << 10;
pub const DELETE_CHILDREN: u32 = 1 << 11;
pub const TRANSVERSE: u32 = 1 << 12;





struct InodeAllocator {
    next_inode: AtomicU64,
    free_list: Mutex<VecDeque<u64>>,
}

impl InodeAllocator {
    fn new(starting: u64) -> Self {
        Self {
            next_inode: AtomicU64::new(starting),
            free_list: Mutex::new(VecDeque::new()),
        }
    }

    fn allocate(&self) -> u64 {
        // Try to reuse a freed inode first
        if let Ok(mut free_list) = self.free_list.lock() {
            if let Some(inode) = free_list.pop_front() {
                return inode;
            }
        }

        // No free inodes, allocate a new one
        self.next_inode.fetch_add(1, Ordering::Relaxed)
    }

    fn free(&self, inode: u64) {
        if let Ok(mut free_list) = self.free_list.lock() {
            free_list.push_back(inode);
        }
    }
}


