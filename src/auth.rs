use std::net::SocketAddr;
use std::pin::Pin;
use anyhow::{Context, Result, bail};
use crate::network::{pin_cert_client_config, read_message, write_message, QuicClient};
use crate::proto::vaultdrive::*;
use quinn::{ RecvStream, SendStream};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use crate::commands::connectionDirect;
use crate::proto::vaultdrive::response::ResponseType;
use ed25519_dalek::{Signer, SigningKey};
use rand_core::OsRng;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::{DigitallySignedStruct, Error, SignatureScheme};
use x509_parser::nom::AsBytes;
use crate::autoRun::{connection, get_connection_from_db, insert_connection};
use crate::driveManagerUI::ConnectionType;

#[derive(Clone, Default)]
pub struct session{
    pub hostname: Option<String>,
    pub authenticate_request: AuthenticationSuccessResponse
}


pub async fn authenticate(
    send: &mut SendStream,
    recv: &mut RecvStream,
    username: &str,
    password: String,
    socket_addr: SocketAddr,
    connection: connection
) -> Result<AuthenticationSuccessResponse> {
    tracing::info!("Starting authentication for user: {}", username);



    let init_req = Request {
        request_type: Some(request::RequestType::Authenticate(
            AuthenticateRequest {
                username: username.to_string(),
                password: password.to_string(),
                connection_id: connection.connection_id.to_string(),
                key: connection.key.clone()
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
                insert_connection(connection).await;

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
pub async fn reauthenticate(
    send: &mut SendStream,
    recv: &mut RecvStream,
    username: String,
    connection_type: ConnectionType,
    connection_point: String,
    socket_addr:SocketAddr,
    scope: String
) -> Result<AuthenticationSuccessResponse> {
    let username_clone = username.clone();
    let connection_point_clone = connection_point.clone();
    let connection = get_connection_from_db(connection_type, connection_point_clone, username_clone, scope)
    .await?;

    let init_req = Request {
        request_type: Some(request::RequestType::Reauthenticate(
            ReAuthenticateRequest {
                connection_id: connection.connection_id.clone(),
                key: connection.key.clone(),
            },
        )),
    };


    let auth_response: Response = read_message(recv).await
        .context("Failed to read authentication response")?;

    match auth_response.response_type {

        Some(response::ResponseType::AuthenticationSuccess(success)) => {
            tracing::info!("Authentication successful for user: {}", username);

            if let (Some(port), Some(cert)) = (success.port.clone(), success.cert.clone()) {

                let child_connect = SocketAddr::new(socket_addr.ip(), port as u16);
                let quic = QuicClient::new().await?;
                let server_name= ServerName::try_from("vaultDriveServer")
                    .context("Invalid server name")?;



                let conn = quic.endpoint
                    .connect_with(pin_cert_client_config(cert)?, child_connect, &server_name.to_str())?
                    .await
                    .context("Failed to establish QUIC connection")?;

                quic.connection.insert(socket_addr, conn);
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



