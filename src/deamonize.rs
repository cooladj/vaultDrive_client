use std::{env, io, process};
use std::fs::{File, OpenOptions};
use std::net::SocketAddr;
use futures::{SinkExt, StreamExt, TryFutureExt};
use interprocess::local_socket::{GenericFilePath, ListenerOptions, ToFsName, };
use interprocess::local_socket::traits::tokio::{Listener, };
use log::{debug, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use crate::autoRun::{auto_run, create_table, get_auto_run};
use crate::commands::{execute_socket_command, CommandResponse, ResponseData, SocketCommand};

pub fn is_daemon_process() -> bool {
    std::env::var("IS_DAEMON").is_ok()
}

pub fn is_daemon_running() -> io::Result<bool> {
    let pid_file = get_pid_file_path();

    let pid_str = match std::fs::read_to_string(&pid_file) {
        Ok(content) => content,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok(false); 
        }
        Err(e) => return Err(e),
    };

    let pid: u32 = match pid_str.trim().parse() {
        Ok(p) => p,
        Err(_) => return Ok(false), 
    };

    Ok(check_process_running(pid))
}


pub async fn restart_process_daemon() -> io::Result<()> {
    let pid_file = get_pid_file_path();

    let pid_str = match tokio::fs::read_to_string(&pid_file).await {
        Ok(content) => content,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return spawn_daemon();
        }
        Err(e) => return Err(e),
    };

    let pid: u32 = match pid_str.trim().parse() {
        Ok(p) => p,
        Err(_) => return spawn_daemon(),
    };

    let pid_file_clone = pid_file.clone();

    tokio::task::spawn_blocking(move || -> io::Result<()> {
        kill_process_by_pid(pid).ok();
        std::fs::remove_file(&pid_file_clone).ok();
        Ok(())
    })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;

    spawn_daemon()
}

fn kill_process_by_pid(pid: u32) -> io::Result<()> {
    #[cfg(unix)]
    {
        use nix::sys::signal::{kill, Signal};
        use nix::unistd::Pid;
        use nix::sys::wait::waitpid;

        kill(Pid::from_raw(pid as i32), Signal::SIGTERM)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let _ = waitpid(Pid::from_raw(pid as i32), None);
    }

    #[cfg(windows)]
    {
        use windows::Win32::System::Threading::{
            OpenProcess, TerminateProcess, WaitForSingleObject,
            PROCESS_TERMINATE, PROCESS_SYNCHRONIZE
        };
        use windows::Win32::Foundation::{CloseHandle, HANDLE};

        unsafe {
            let handle: HANDLE = OpenProcess(PROCESS_TERMINATE | PROCESS_SYNCHRONIZE, false, pid)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            let result = (|| {
                TerminateProcess(handle, 0)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                WaitForSingleObject(handle, u32::MAX);
                Ok::<(), io::Error>(())
            })();

            CloseHandle(handle).ok();
            result?;
        }
    }

    Ok(())
}


pub fn spawn_daemon() -> io::Result<()> {
    #[cfg(unix)]
    {
        spawn_daemon_unix()
    }

    #[cfg(windows)]
    {
        spawn_daemon_windows()
    }

}




#[cfg(unix)]
fn spawn_daemon_unix() -> io::Result<()> {
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};

    let temp_file = get_daemon_log_file();

    let child = Command::new(std::env::current_exe()?)
        .env("IS_DAEMON", "1")
        .stdin(Stdio::null())
        .stdout(temp_file.as_ref().map_or(Stdio::inherit(), |f| Stdio::from(f.try_clone().unwrap())))
        .stderr(temp_file.map_or(Stdio::inherit(), Stdio::from))
        .process_group(0)
        .spawn()?;

    // Write PID file
    let pid = child.id();
    std::fs::write(get_pid_file_path(), pid.to_string())?;

    Ok(())
}

fn get_daemon_log_file() -> io::Result<File> {
    let temp_dir = env::temp_dir();
    debug!("the daemon log are going here {:?}",temp_dir );
    OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(temp_dir.join("vaultDrive_daemon.log"))
}

#[cfg(windows)]
fn spawn_daemon_windows() -> io::Result<()> {
    use std::os::windows::process::CommandExt;
    use std::process::{Command, Stdio};

    const CREATE_NO_WINDOW: u32 = 0x08000000;
    const DETACHED_PROCESS: u32 = 0x00000008;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x00000200;

    let temp_file = get_daemon_log_file();

    let child = Command::new(std::env::current_exe()?)
        .env("IS_DAEMON", "1")
        .stdin(Stdio::null())
        .stdout(temp_file.as_ref().map_or(Stdio::inherit(), |f| Stdio::from(f.try_clone().unwrap())))
        .stderr(temp_file.map_or(Stdio::inherit(), Stdio::from))
        .creation_flags(CREATE_NO_WINDOW | DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP)
        .spawn()?;

    // Write PID file
    let pid = child.id();
    std::fs::write(get_pid_file_path(), pid.to_string())?;

    Ok(())
}

fn check_process_running(pid: u32) -> bool {
    #[cfg(unix)]
    {
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }

    #[cfg(windows)]
    {
        use windows::Win32::System::Threading::{OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION};

        unsafe {
             match OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, false, pid){
                Ok(_) => {  true}
                Err(_) => {  false }
            }
        }
    }
}

fn get_pid_file_path() -> String {
    #[cfg(unix)]
    {
        "/tmp/vaultdrive.pid".to_string()
    }

    #[cfg(windows)]
    {
        format!("{}\\vaultdrive.pid",
                std::env::var("TEMP").unwrap_or_else(|_| "C:\\Temp".to_string()))
    }
}

pub(crate) async fn run_daemon_server() -> anyhow::Result<()> {
    #[cfg(any(windows, target_os = "macos", target_os = "linux"))]
    {
        tokio::spawn(async {
            let auto = match tokio::task::spawn_blocking(|| {
                create_table().ok()?;
                get_auto_run().ok()
            })
                .await
            {
                Ok(Some(entries)) => entries,
                _ => return,
            };

            for x in auto {
                tokio::spawn(async move {
                    let conn_command = SocketCommand::Connect {
                        connection_type: x.connection_type,
                        connection_point: x.connection_point.clone(),
                        username: x.username.clone(),
                    };

                    match execute_socket_command(conn_command).await {
                        CommandResponse::Success(ResponseData::Text(s)) => {
                            let Ok(socketaddr) = s.parse::<std::net::SocketAddr>() else {
                                return;
                            };

                            let mount_command = SocketCommand::Mount {
                                mount_point: x.mount_point,  
                                drive: x.host_drive,
                                socketaddr,
                                username: x.username,
                            };

                            execute_socket_command(mount_command).await;
                        }
                        _ => return,
                    };
                });
            }
        });
    }

    #[cfg(unix)]
    let path_str = "/tmp/vaultDriveClient.sock";

    #[cfg(windows)]
    let path_str = r"\\.\pipe\vaultDriveClient";

    let name = path_str.to_fs_name::<GenericFilePath>()?;



    let listener = ListenerOptions::new()
        .name(name)
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






