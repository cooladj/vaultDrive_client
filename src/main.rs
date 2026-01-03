use std::future::Future;
use anyhow::Result;
use log::{debug, info, warn};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod auth;
mod client;
mod commands;
mod filesystem;
mod network;
mod deamonize;
mod driveManagerUI;

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
use crate::deamonize::restart_process_daemon;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env().add_directive(tracing::Level::DEBUG.into()))
        .init();


    if deamonize::is_daemon_process() {
        info!("Starting daemon process");
        deamonize::run_daemon_server().await?;
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
            Err(_) =>{



            }

        }

        if !deamonize::is_daemon_running()? {
            match deamonize::spawn_daemon() {
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
        } else {
        }




        driveManagerUI::runUI().await.expect("Failed to run drive manager");


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




