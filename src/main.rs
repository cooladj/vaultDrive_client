use std::env;
use std::ffi::OsString;
use std::fs::File;
use std::future::Future;
use std::time::Duration;
use anyhow::Result;
use log::{debug, info, warn};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use service_manager::ServiceLabel;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};


#[cfg(windows)]
use windows_service::service::{ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus, ServiceType};
#[cfg(windows)]
use windows_service::{define_windows_service, service_control_handler, service_dispatcher};
#[cfg(windows)]
use windows_service::service_control_handler::{ServiceControlHandlerResult};

mod auth;
mod client;
mod commands;
mod filesystem;
mod network;
mod deamonize;
mod driveManagerUI;
mod autoRun;
mod shared;

mod proto {
    pub mod vaultdrive {
        include!("proto/vaultdrive.rs");
    }
    #[allow(dead_code)]
    pub mod hub {
        include!("proto/hub.rs");
    }
}



use crate::commands::{send_command_to_daemon, CommandResponse, SocketCommand};
use crate::deamonize::{is_daemon_process, restart_process_daemon, SERVICE_NAME};
use crate::shared::elevated;

#[cfg(windows)]
define_windows_service!(ffi_service_main, windows_service_handler);

#[cfg(windows)]
static RUNTIME_HANDLE: OnceCell<Handle> = OnceCell::new();

#[tokio::main]
async fn main() -> Result<()> {

    let guard = init_logging(is_daemon_process())?;

    if deamonize::is_daemon_process() {
        info!("Starting daemon process");

        #[cfg(windows)]
        {
            RUNTIME_HANDLE.set(Handle::current());


            let (tx, rx) = oneshot::channel();

            std::thread::spawn(move || {
                let result = service_dispatcher::start(
                    SERVICE_NAME,
                    ffi_service_main
                );

                let _ = tx.send(result);
            });

            rx.await?;

        }

        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};

            // Set up signal handlers
            let mut sigterm = signal(SignalKind::terminate())?;
            let mut sigint = signal(SignalKind::interrupt())?;

            // Start your server
            tokio::spawn(async move {
                if let Err(e) = deamonize::run_daemon_server().await {
                    tracing::error!(" failed to start server {}", e);
                }
            });

            // Wait for shutdown signal
            tokio::select! {
        _ = sigterm.recv() => {
            println!("Received SIGTERM, shutting down...");
        }
        _ = sigint.recv() => {
            println!("Received SIGINT, shutting down...");
        }
    }
        }
    } else {
        // This is the main/GUI process
        info!("Starting main process");

        match send_command_to_daemon(SocketCommand::Health).await {

            Ok(command) => {
                match command {
                    CommandResponse::Success(_) => {
                        info!("Daemon already running");

                    }
                    CommandResponse::Error(_) => {
                        if elevated()
                        {
                            restart_process_daemon().await?;
                            match retry(|| async {
                                send_command_to_daemon(SocketCommand::Health).await
                            }, 3, 25).await {
                                Ok(_) => debug!("Daemon listening to commands"),
                                Err(e) => debug!("Health check failed after retries: {}", e),
                            }
                        }
                    }
                }
                }
            Err(_) =>{



            }

        }

        if !deamonize::is_daemon_running()? && elevated() {
            match deamonize::restart_process_daemon().await {
                Ok(_) => {
                    info!("Daemon spawned successfully");
                    // Give daemon time to start
                    match retry(|| async {
                        send_command_to_daemon(SocketCommand::Health).await
                    }, 3, 25).await {
                        Ok(_) => debug!("Daemon listening to commands"),
                        Err(e) => debug!("Health check failed after retries: {}", e),
                    }
                }
                Err(e) => {
                    warn!("Failed to spawn daemon: {}", e);
                    return Err(e.into());
                }
            }
        }

        driveManagerUI::run_ui().await.expect("Failed to run drive manager");


        info!("Tauri application closed");
    }

    Ok(())
}

async fn retry<F, Fut, T>(mut make_future: F, max_attempts: u32, delay: u64) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut attempts = 0;
    loop {
        match make_future().await {
            Ok(val) => return Ok(val),
            Err(e) if attempts < max_attempts => {
                attempts += 1;
                let delay = delay * 2_u64.pow(attempts);
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            }
            Err(e) => return Err(e),
        }
    }
}


#[cfg(windows)]
fn windows_service_handler(_arguments: Vec<OsString>) -> Result<()> {
    use parking_lot::{ Condvar};
    use std::sync::Arc;



    let pair = Arc::new((Mutex::new(false), Condvar::new()));
    let pair2 = pair.clone();


    let event_handler = move |control_event| -> ServiceControlHandlerResult {
        match control_event {
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            ServiceControl::Stop => {
                let &(ref lock, ref cvar) = &*pair2;
                let mut started = lock.lock();
                *started = true;
                cvar.notify_one();
                ServiceControlHandlerResult::NoError
            }
            _ => ServiceControlHandlerResult::NotImplemented,
        }
    };


    let status_handle = match service_control_handler::register(SERVICE_NAME, event_handler) {
        Ok(handle) => {
            handle
        }
        Err(err) => {
            return Err(err.into());
        }
    };

    match status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::Running,
        controls_accepted: ServiceControlAccept::STOP,
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::default(),
        process_id: None,
    }) {
        Ok(status) => {

        }
        Err(err) => {
            debug!("Failed to set service status: {}", err);

        }
    }

    let handle = RUNTIME_HANDLE.get()
        .ok_or_else(|| anyhow::anyhow!("Runtime handle not initialized"))?;

    debug!("number of tokio worker: {:?}", handle.metrics().num_workers());

    let abort_handle = handle.spawn(async move {
        if let Err(e) = deamonize::run_daemon_server().await {
            tracing::error!(" failed to start server {}", e);
        }
    }).abort_handle();

    let &(ref lock, ref cvar) = &*pair;
    let mut started = lock.lock();
    if !*started {
        cvar.wait(&mut started);
    }

    abort_handle.abort();



    match status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::Stopped,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::default(),
        process_id: None,
    }) {
        Ok(_) => {

        }
        Err(e) => {
            tracing::error!(" failed to set status {}", e);

        }
    }


    Ok(())
}

fn init_logging(is_service: bool) -> Result<WorkerGuard> {
    let level = if cfg!(debug_assertions) {
        Level::DEBUG
    } else {
        Level::INFO
    };

    let subscriber = tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(false)
        .with_level(true)
        .with_max_level(level);

    if is_service {
        let log_path = env::temp_dir().join("vaultDrive_client_service.log");
        let file = File::create(&log_path)?;
        let (non_blocking, guard) = tracing_appender::non_blocking(file);
        subscriber.with_writer(non_blocking).init();
        Ok(guard)
    } else {
        subscriber.init();
        Ok(tracing_appender::non_blocking(std::io::stdout()).1)
    }
}
