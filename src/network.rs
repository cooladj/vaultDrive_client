use anyhow::{Context, Result, bail, anyhow};
use quinn::{Connection, Endpoint, RecvStream, SendStream, ClientConfig, TransportConfig, congestion};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;
use bytes::{BufMut, BytesMut};
use dashmap::{DashMap, Equivalent};
use futures::future::join_all;
use log::debug;
use prost::Message;

use rcgen::{ CertificateParams, DistinguishedName, DnType, KeyPair};
use rustls::RootCertStore;
use tokio::io::{join, AsyncWriteExt};
use tokio::sync::OnceCell;
use once_cell::sync::Lazy;
use tracing::error;
use crate::client::{ VAULT_DRIVE_MAP};
use crate::commands::{connectionDirect, connectionHub};

pub struct QuicClient {
    pub(crate) endpoint: Endpoint,
    pub connection: DashMap<SocketAddr,Connection>,
    pub reAuth: tokio::sync::Notify

}

 static RECONNECT_CELLS: Lazy<DashMap<SocketAddr, Arc<OnceCell<Result<SocketAddr>>>>> =
    Lazy::new(|| DashMap::new());

pub(crate) const ZERO_ADDR: SocketAddr = SocketAddr::new(
    IpAddr::V4(Ipv4Addr::UNSPECIFIED),
    0
);

///This is currently turned off
const DEFAULT_HUB_DNS: &str = "vaultdrive.org";


static QUIC_CLIENT: OnceCell<QuicClient> = OnceCell::const_new();



impl QuicClient {

        pub async fn new() -> Result<&'static Self> {
            QUIC_CLIENT
                .get_or_try_init(|| async {
                    Self::initialize().await
                })
                .await
        }
    async fn initialize() -> Result<Self> {

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse()?)
            .context("Failed to create QUIC endpoint")?;

        let (client_cert, client_key) = Self::generate_client_cert()?;

        let mut transport = TransportConfig::default();
        transport.max_concurrent_bidi_streams(100_u32.into());
        transport.max_concurrent_uni_streams(100_u32.into());
        transport.keep_alive_interval(Some(Duration::from_secs(30)));
        transport.max_idle_timeout(Some(Duration::from_secs(90).try_into()?));
        transport.congestion_controller_factory(Arc::new(congestion::CubicConfig::default()));


        let mut crypto = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(VaultDriveServerVerifier::new()))
            .with_client_auth_cert(vec![client_cert], client_key)?;
        crypto.alpn_protocols = vec![b"valuedrive".to_vec()];


        let mut client_config = ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?
        ));
        client_config.transport_config(Arc::new(transport));

        endpoint.set_default_client_config(client_config);

        Ok(Self {
            endpoint,
            connection: DashMap::new(),
            reAuth: tokio::sync::Notify::new(),
        })
    }


    fn generate_client_cert() -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>)> {
        let mut params = CertificateParams::new(vec!["vaultDriveClient".to_string()])
            .context("Failed to create certificate params")?;

        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, "vaultDriveClient");
        params.distinguished_name = dn;

        let key_pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
            .context("Failed to generate key pair")?;

        let cert = params.self_signed(&key_pair)
            .context("Failed to generate client certificate")?;

        let cert_der = CertificateDer::from(cert.der().to_vec());
        let key_der = PrivateKeyDer::try_from(key_pair.serialize_der())
            .map_err(|e| anyhow::anyhow!("Failed to serialize private key: {}", e))?;

        tracing::info!("Generated client certificate with CN=vaultDriveClient");

        Ok((cert_der, key_der))
    }




    pub async fn connect(&self, addr: &SocketAddr) -> Result<(SocketAddr,Connection)> {
        debug!("Connecting to {}", addr);
        match self.connection.get(addr){
            None => {}
            Some(conn) => {
                    return Ok((conn.remote_address(), conn.value().clone()));
            }
        }


        let ( addr, server_name) = if addr.equivalent(&ZERO_ADDR){
            debug!("DNS resolving hub");
            (tokio::net::lookup_host(DEFAULT_HUB_DNS)
                .await?
                .next()
                .ok_or_else(|| anyhow::anyhow!("DNS resolution failed for hub"))?,

            ServerName::try_from("hub.vaultdrive.org")
                .context("Invalid server name")?)
        } else {(
            addr.to_owned(),
            ServerName::try_from("vaultDriveServer")
                .context("Invalid server name")?
        )
        };

        let connection = self.endpoint
            .connect(addr, &server_name.to_str())?
            .await
            .context("Failed to establish QUIC connection")?;


        tracing::info!("Connected to {} via QUIC", addr);

        self.connection.insert(addr, connection.clone());


        Ok((addr, connection))
    }


    pub async fn open_bi_safe(&self, socket_addr: SocketAddr) -> Result<(SendStream, RecvStream)> {
        let connection = self.connection
            .get(&socket_addr)
            .context("Not connected")?
            .clone();

        if connection.close_reason().is_some() {
            self.connection.remove(&socket_addr);
            return Err(anyhow::anyhow!("Connection closed"));
        }

        connection.open_bi().await
            .context("Failed to open bidirectional stream")
    }

    pub async fn open_bi(&self, mut socket_addr: SocketAddr, username: String) -> Result<(SendStream, RecvStream)> {
        let mut connection = self.connection
            .get(&socket_addr)
            .context("Not connected")?;



        if connection.close_reason().is_some() {
            drop(connection);
            let cell = RECONNECT_CELLS
                .entry(socket_addr)
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone();

            let result =cell.get_or_init(|| async {
                let client = VAULT_DRIVE_MAP
                    .get(&(socket_addr, username))
                    .expect("Client not found")
                    .clone();

                let session = client.session.read().await
                    .as_ref()
                    .expect("Session is None")
                    .clone(); // Clone the Session itself

                client.renew_connection(session).await

            })
                .await;
            RECONNECT_CELLS.remove(&socket_addr);

            match result{
                Ok(socketAddr) => {
                    if !socketAddr.equivalent(&socket_addr){
                        socket_addr = *socketAddr;
                    }
                }
                Err(err) => {
                    error!("Failed to renew_connetion {}", err);
                    return Err(anyhow::anyhow!("Failed to renew_connection {}", err));
                }
            }
             connection = self.connection.get(&(socket_addr)).context("Not connected")?;
        }

        let connection = connection.value();

        connection.open_bi().await
            .inspect_err(|_| {
                self.connection.remove(&socket_addr);
            })
            .context("Failed to open bidirectional stream")
    }



    pub fn close(&self, socket_addr: SocketAddr, username: String) -> Result<()> {
        let entry = self.connection
            .remove(&socket_addr)
            .context("Not connected")?;

        Ok(entry.1.close(0u32.into(), b"client closing"))
    }

}



#[derive(Debug)]
pub struct VaultDriveServerVerifier {
    root_store: Arc<RootCertStore>,
}

impl VaultDriveServerVerifier {
    pub fn new() -> Self {
        let mut root_store = RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        Self {
            root_store: Arc::new(root_store),
        }
    }
}

impl rustls::client::danger::ServerCertVerifier for VaultDriveServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        // First, try standard CA verification (for Cloudflare certs)
        let ca_verifier = rustls::client::WebPkiServerVerifier::builder(self.root_store.clone())
            .build()
            .map_err(|e| {
                tracing::debug!("Failed to build CA verifier: {}", e);
                rustls::Error::General(format!("Verifier build failed: {}", e))
            })?;

        match ca_verifier.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Ok(verified) => {
                tracing::info!("Server certificate verified via CA chain");
                return Ok(verified);
            }
            Err(ca_err) => {
                tracing::debug!("CA verification failed: {}, trying self-signed verification", ca_err);

                // CA verification failed, try self-signed verification
                // Parse the certificate to verify CN
                let cert = x509_parser::parse_x509_certificate(end_entity)
                    .map_err(|_| rustls::Error::InvalidCertificate(
                        rustls::CertificateError::BadEncoding
                    ))?
                    .1;

                // Extract Common Name from subject
                let subject_cn = cert.subject()
                    .iter_common_name()
                    .next()
                    .and_then(|cn| cn.as_str().ok())
                    .ok_or_else(|| rustls::Error::InvalidCertificate(
                        rustls::CertificateError::BadEncoding
                    ))?;

                // Verify CN matches expected value for self-signed
                if subject_cn != "vaultDriveServer" && subject_cn != "vaultDriveHub" {
                    tracing::error!(
                        "Certificate verification failed: CA validation failed and CN mismatch (expected 'vaultDriveServer', got '{}')",
                        subject_cn
                    );
                    return Err(rustls::Error::InvalidCertificate(
                        rustls::CertificateError::NotValidForName
                    ));
                }
                

                tracing::info!("Server certificate verified as self-signed: CN={}", subject_cn);
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
        ]
    }
}


pub async fn write_message<T: Message>(send: &mut SendStream, msg: &T) -> Result<()> {
    let mut buf = BytesMut::new();
    msg.encode(&mut buf)
        .context("Failed to encode message")?;

    let len = buf.len() as u32;
    let mut frame = BytesMut::with_capacity(4 + buf.len());
    frame.put_u32(len);
    frame.put(buf);

    send.write_all(&frame).await
        .context("Failed to write message")?;
    send.flush().await.context("Failed to flush message")?;

    Ok(())
}

pub async fn read_message<T: Message + Default>(recv: &mut RecvStream) -> Result<T> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf).await
        .context("Failed to read message length")?;
    let len = u32::from_be_bytes(len_buf) as usize;


    let mut msg_buf = vec![0u8; len];
    recv.read_exact(&mut msg_buf).await
        .context("Failed to read message body")?;

    T::decode(&msg_buf[..])
        .context("Failed to decode message")
}


