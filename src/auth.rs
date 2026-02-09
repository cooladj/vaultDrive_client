use std::net::SocketAddr;
use anyhow::{Context, Result, bail};
use crate::network::{read_message, write_message, QuicClient};
use crate::proto::vaultdrive::*;
use quinn::{ RecvStream, SendStream};
use rustls::pki_types::ServerName;
use crate::commands::connectionDirect;
use crate::proto::vaultdrive::response::ResponseType;

#[derive(Clone, Default)]
pub struct session{
    pub hostname: Option<String>,
    pub authenticate_request: AuthenticationSuccessResponse
}

pub async fn authenticate(
    send: &mut SendStream,
    recv: &mut RecvStream,
    username: &str,
    password: &str,
    socket_addr: SocketAddr,
) -> Result<AuthenticationSuccessResponse> {
    tracing::info!("Starting authentication for user: {}", username);

    let init_req = Request {
        request_type: Some(request::RequestType::Authenticate(
            AuthenticateRequest {
                username: username.to_string(),
                password: password.to_string(),
            },
        )),
    };
    drop(password);
    write_message(send, &init_req).await?;
    
    let auth_response: Response = read_message(recv).await
        .context("Failed to read authentication response")?;

    match auth_response.response_type {
        Some(response::ResponseType::AuthenticationSuccess(success)) => {
            tracing::info!("Authentication successful for user: {}", username);

            match success.port {
                None => {}
                Some(port) => {
                    let child_connect = SocketAddr::new(socket_addr.ip(), port as u16);
                    let quic = QuicClient::new().await?;
                    let server_name= ServerName::try_from("vaultDriveServer")
                        .context("Invalid server name")?;

                    let conn = quic.endpoint
                        .connect(child_connect, &server_name.to_str())?
                        .await
                        .context("Failed to establish QUIC connection")?;

                    quic.connection.insert(socket_addr, conn);

                }
            }

            Ok(success)
        }
        Some(response::ResponseType::Error(err)) => {

            bail!("Authentication failed: {}", err.message);
        }
        _ => {
            bail!("Unexpected response type during authentication");
        }
    }
}


