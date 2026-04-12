use std::io;
use std::io::IoSliceMut;
use anyhow::{Context, Result, bail, anyhow};
use quinn::{Connection, Endpoint, RecvStream, SendStream, ClientConfig, TransportConfig, congestion, AsyncUdpSocket, UdpPoller};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use bytes::{BufMut, BytesMut};
use dashmap::{DashMap, Equivalent};
use futures::future::join_all;
use log::debug;
use prost::Message;

use rcgen::{ CertificateParams, DistinguishedName, DnType, KeyPair};
use rustls::{DigitallySignedStruct, RootCertStore};
use tokio::io::{join, AsyncWriteExt};
use tokio::sync::OnceCell;
use once_cell::sync::Lazy;
use quinn::udp::{RecvMeta, Transmit};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use tracing::error;
use crate::autoRun::{contain_cert, insert_cert};
use crate::client::{ VAULT_DRIVE_MAP};
use crate::commands::{connection_direct};

pub struct QuicClient {
    pub(crate) endpoint: Endpoint,
    pub connection: DashMap<SocketAddr,Connection>,
    pub reAuth: tokio::sync::Notify,

}

 static RECONNECT_CELLS: Lazy<DashMap<SocketAddr, Arc<OnceCell<Result<SocketAddr>>>>> =
    Lazy::new(|| DashMap::new());

pub(crate) const ZERO_ADDR: SocketAddr = SocketAddr::new(
    IpAddr::V4(Ipv4Addr::UNSPECIFIED),
    0
);

///This is currently turned off
const DEFAULT_HUB_DNS: &str = "hub.vaultdrive.org:443";


static QUIC_CLIENT: OnceCell<QuicClient> = OnceCell::const_new();



impl QuicClient {

    pub async fn new() -> Result<&'static Self> {
        let result = QUIC_CLIENT
            .get_or_try_init(|| async {
                let r = Self::initialize().await;
                r
            })
            .await;
        result
    }
    async fn initialize() -> Result<Self> {
        let mut endpoint = Endpoint::client("0.0.0.0:0".parse()?)
            .context("Failed to create QUIC endpoint")?;

        let mut root_store = RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());


        let mut crypto = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        crypto.alpn_protocols = vec![b"vaultdrive".to_vec()];

        let mut client_config = ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?
        ));

        let mut transport = TransportConfig::default();
        transport.send_fairness(false);

        transport.congestion_controller_factory(Arc::new(congestion::CubicConfig::default()));
        client_config.transport_config(Arc::new(transport));

        endpoint.set_default_client_config(client_config);

        Ok(Self {
            endpoint,
            connection: DashMap::new(),
            reAuth: tokio::sync::Notify::new(),
        })
    }



    pub async fn connect(&self, addr: &SocketAddr, tofu_enable: Option<bool>, token: Option<Vec<u8>>, relay_socket_addr: Option<SocketAddr>) -> Result<(SocketAddr,Connection)> {
        debug!("Connecting to {}", addr);
        if !relay_socket_addr.is_some() {
            match self.connection.get(addr) {
                None => {}
                Some(conn) => {
                    return Ok((conn.remote_address(), conn.value().clone()));
                }
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
        let connecting = if let Some(tofu_enable) = tofu_enable {
            self.endpoint.connect_with(tofu_client_config(tofu_enable, token)?, addr, &server_name.to_str())?
        } else {
            self.endpoint.connect(addr, &server_name.to_str())?
        };
        let connection = connecting
            .await
            .context("Failed to establish QUIC connection")?;


        tracing::info!("Connected to {} via QUIC", addr);
        if let Some(relay_socket_addr) = relay_socket_addr {
            self.connection.insert(relay_socket_addr, connection.clone());


            Ok((relay_socket_addr, connection))
        }else {
            self.connection.insert(addr, connection.clone());


            Ok((addr, connection))
        }
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



                client.renew_connection().await

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

pub fn pin_cert_client_config(cert:  Vec<u8>) -> Result<ClientConfig> {
    let mut transport = TransportConfig::default();
    transport.congestion_controller_factory(Arc::new(congestion::CubicConfig::default()));

    let tofu_verifier: PinnedCertVerifier = PinnedCertVerifier{
        expected_cert: cert,
    };



    let mut crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(tofu_verifier))
        .with_no_client_auth();

    crypto.alpn_protocols = vec![b"vaultdrive".to_vec()];


    let mut client_config = ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?
    ));
    let mut transport = TransportConfig::default();
    transport.congestion_controller_factory(Arc::new(congestion::CubicConfig::default()));
    client_config.transport_config(Arc::new(transport));

    Ok(client_config)
}


fn tofu_client_config(tofu_enable: bool, token: Option<Vec<u8>> ) -> Result<ClientConfig> {
    let mut transport = TransportConfig::default();
    transport.congestion_controller_factory(Arc::new(congestion::CubicConfig::default()));

    let tofu_verifier: TofuVerifier = TofuVerifier{
        tofu_enabled: tofu_enable,
    };



    let mut crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(tofu_verifier))
        .with_no_client_auth();

    crypto.alpn_protocols = vec![b"vaultdrive".to_vec()];


    let mut client_config = ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?
    ));
    let mut transport = TransportConfig::default();
    transport.congestion_controller_factory(Arc::new(congestion::CubicConfig::default()));
    client_config.transport_config(Arc::new(transport));
    if let Some(token) = token {

        client_config.initial_dst_cid_provider(Arc::new(move || {
            quinn::ConnectionId::new(&token)
        }));
    }

    Ok(client_config)



}

#[derive(Debug)]
pub struct TofuVerifier {
    pub tofu_enabled: bool,
}

impl ServerCertVerifier for TofuVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let cert_bytes = end_entity.as_ref();

        if !self.tofu_enabled {
            if contain_cert(&cert_bytes).map_err(|e| rustls::Error::General(format!("{e}")))? {
                Ok(ServerCertVerified::assertion())
            } else {
                Err(rustls::Error::General("Unknown certificate".into()))
            }
        } else {
            insert_cert(&cert_bytes).map_err(|e| rustls::Error::General(format!("{e}")))?;
            Ok(ServerCertVerified::assertion())
        }
    }

    fn verify_tls12_signature(
        &self, _message: &[u8], _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self, _message: &[u8], _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}


#[derive(Debug)]
pub struct PinnedCertVerifier {
    expected_cert: Vec<u8>,
}

impl ServerCertVerifier for PinnedCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if end_entity.as_ref() == self.expected_cert {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General("Certificate doesn't match expected".into()))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Debug)]
pub struct AcceptAllVerifier;

impl ServerCertVerifier for AcceptAllVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}


pub async fn write_message<T: Message>(send: &mut SendStream, msg: &T) -> Result<()> {
    let mut buf = BytesMut::new();
    msg.encode(&mut buf).context("Failed to encode message")?;

    send.write_u32(buf.len() as u32).await.context("Failed to write length")?;
    send.write_all(&buf).await.context("Failed to write message")?;
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


