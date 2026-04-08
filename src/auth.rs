use std::net::SocketAddr;
use std::pin::Pin;
use anyhow::{Context, Result, bail};
use crate::network::{pin_cert_client_config, read_message, write_message, QuicClient};
use crate::proto::vaultdrive::*;
use quinn::{ RecvStream, SendStream};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use crate::commands::connection_direct;
use crate::proto::vaultdrive::response::ResponseType;
use ed25519_dalek::{Signer, SigningKey};
use rand_core::OsRng;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::{DigitallySignedStruct, Error, SignatureScheme};
use x509_parser::nom::AsBytes;
use crate::autoRun::{connection, get_connection_from_db, insert_connection, update_connect_key};
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
    mut connection: connection
) -> Result<AuthenticationSuccessResponse> {
    tracing::info!("Starting authentication for user: {}", username);



    let init_req = Request {
        request_type: Some(request::RequestType::Authenticate(
            AuthenticateRequest {
                username: username.to_string(),
                password: password.to_string(),
                connection_id: connection.connection_id.to_string(),
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
            connection.key = success.key.clone();


                insert_connection(connection).await;

            Ok(success.authentication_success.context("Missing authentication_success field")?)
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
    username: &str,
    connection_type: ConnectionType,
    connection_point: &str,
    scope: &str
) -> Result<AuthenticationSuccessResponse> {
    let connection = get_connection_from_db(connection_type, connection_point, username, scope)
    .await?;

    let init_req = Request {
        request_type: Some(request::RequestType::Reauthenticate(
            ReAuthenticateRequest {
                connection_id: connection.connection_id.clone(),
                key: connection.key.clone(),
            },
        )),
    };
    write_message(send, &init_req).await?;


    let auth_response: Response = read_message(recv).await
        .context("Failed to read authentication response")?;

    match auth_response.response_type {


        Some(response::ResponseType::AuthenticationSuccess(success)) => {
            tracing::info!("Authentication successful for user: {}", username);

            update_connect_key(&connection.connection_id, &success.key).await?;

            Ok(success.authentication_success.context("Missing authentication_success field")?)
        }
        Some(response::ResponseType::Error(err)) => {

            bail!("Authentication failed: {}", err.message);
        }
        _ => {
            bail!("Unexpected response type during authentication");
        }
    }



}



