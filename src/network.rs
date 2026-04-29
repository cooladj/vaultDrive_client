use std::borrow::Cow;
use std::cmp::min;
use std::io;
use std::io::{Cursor, IoSliceMut, Read};
use anyhow::{Context, Result, bail, anyhow};
use quinn::{Connection, Endpoint, RecvStream, SendStream, ClientConfig, TransportConfig, congestion, AsyncUdpSocket, UdpPoller};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytes_utils::SegmentedBuf;
use bytes_varint::{VarIntSupportMut, VarIntSupport};
use dashmap::{DashMap, Equivalent};
use futures::AsyncWriteExt as future_async;
use futures::future::join_all;
use log::debug;
use prost::Message;


use rcgen::{ CertificateParams, DistinguishedName, DnType, KeyPair};
use rustls::{DigitallySignedStruct, RootCertStore};
use tokio::io::{join, AsyncReadExt, AsyncWriteExt};
use tokio::sync::OnceCell;
use once_cell::sync::{Lazy};
use quinn::udp::{RecvMeta, Transmit};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use tracing::error;
use unsigned_varint::aio;
use unsigned_varint::encode::usize;
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
    debug!("connection with tofu: {}, token: {:?}", tofu_enable, token);

    client_config.transport_config(Arc::new(transport));
    if let Some(token) = token {
        let connection_id =  quinn::ConnectionId::new(&token);

        debug!("connection id: {}", connection_id);

        client_config.initial_dst_cid_provider(Arc::new(move || {
            connection_id
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

pub async fn write_hub_msg<T: Message>(
    stream: &mut SendStream,
    msg: &T,
) -> Result<()> {
    let msg_len = msg.encoded_len();
    let mut header_mut = BytesMut::with_capacity(10 + msg_len);

    header_mut.put_usize_varint(msg_len);
    msg.encode(&mut header_mut).context("encode message")?;

    stream.write_chunk(header_mut.freeze()).await?;

    Ok(())
}
pub async fn read_hub_msg<T: Message + Default>(
    stream: &mut RecvStream,
) -> anyhow::Result<T> {
    let mut bufs = SegmentedBuf::new();
    let mut msg_len: Option<usize> = None;

    loop {
        let chunk = stream.read_chunk(usize::MAX, true).await?
            .ok_or_else(|| anyhow::anyhow!("stream closed"))?.bytes;

        if bufs.remaining() == 0 && msg_len.is_none() {
            let mut cursor = Cursor::new(chunk.clone());

            let Ok(m_len) = cursor.try_get_usize_varint() else {
                bufs.push(chunk);
                continue;
            };

            let header_end = cursor.position() as usize;

            if cursor.remaining() >= m_len {
                let msg = T::decode(&chunk[header_end..header_end + m_len])
                    .context("decode message")?;
                return Ok(msg);
            }

            msg_len = Some(m_len);
            bufs.push(chunk.slice(header_end..));
            continue;
        }

        bufs.push(chunk);

        if msg_len.is_none() {
            match bufs.try_get_usize_varint() {
                Ok(n) => msg_len = Some(n),
                Err(_) => continue,
            }
        }

        let m_len = msg_len.unwrap();

        if bufs.remaining() < m_len {
            continue;
        }


        let msg = T::decode((&mut bufs).take(m_len))
            .context("decode message")?;
        return Ok(msg);
    }
}




pub async fn write_message<'a, T: Message>(
    send: &mut SendStream,
    msg: &T,
    data: Cow<'a, [u8]>,
) -> Result<()> {
    let msg_len = msg.encoded_len();
    let data_len = data.len();
    let mut header_mut = BytesMut::with_capacity(20 + msg_len);

    header_mut.put_usize_varint(msg_len );
    header_mut.put_usize_varint(data_len );
    msg.encode(&mut header_mut).context("encode message")?;
    let header = header_mut.freeze();

    match data {
        Cow::Borrowed(inner) => {
            send.write_chunk(header).await?;
            if data_len >0 {
                send.write_all(inner).await?;
            }
        }
        Cow::Owned(inner) => {
            if data_len > 0 {
                let mut chunks = [header, Bytes::from(inner)];

                send.write_all_chunks(&mut chunks).await.context("write frame")?;
            }
            else {
                send.write_chunk(header).await?;
            }
        }
    }

    Ok(())
}


pub async fn read_message<T: Message + Default>(
    recv: &mut RecvStream,
) -> anyhow::Result<(T, Bytes)> {
    let mut bufs = SegmentedBuf::new();
    let mut msg_len: Option<usize> = None;
    let mut data_len: Option<usize> = None;

    loop {
        let chunk = recv.read_chunk(usize::MAX, true).await?
            .ok_or_else(|| anyhow::anyhow!("stream closed"))?.bytes;



        if bufs.remaining() ==0 {
            let mut cursor = Cursor::new(chunk.clone());

            let Ok(m_len) = cursor.try_get_usize_varint() else {
                bufs.push(chunk);
                continue;
            };

            let Ok(d_len) = cursor.try_get_usize_varint() else {
                msg_len = Some(m_len);
                bufs.push(chunk.slice(cursor.position() as usize..));
                continue;
            };

            let header_end = cursor.position() as usize;

            if cursor.remaining() >= m_len + d_len {
                // Everything in one chunk — zero copy
                let msg = T::decode(&chunk[header_end..header_end + m_len])
                    .context("decode message")?;
                let data = chunk.slice(header_end + m_len..header_end + m_len + d_len);
                return Ok((msg, data));
            }

            msg_len = Some(m_len);
            data_len = Some(d_len);
            bufs.push(chunk.slice(header_end..)); // pre-advanced past header
            continue;
        }

        bufs.push(chunk);

        if msg_len.is_none() {
            match bufs.try_get_usize_varint() {
                Ok(n) => msg_len = Some(n),
                Err(_) => continue,
            }
        }

        if data_len.is_none() {
            match bufs.try_get_usize_varint() {
                Ok(n) => data_len = Some(n),
                Err(_) => continue,
            }
        }

        let m_len = msg_len.unwrap();
        let d_len = data_len.unwrap();

        if bufs.remaining() < m_len + d_len {
            continue;
        }

        let msg = T::decode((&mut bufs).take(m_len))
            .context("decode message")?;
        let data = if d_len >  0 {
            ///chances to be zero copy as
            bufs.copy_to_bytes(d_len)
        }else {
            Bytes::new()
        };
        return Ok((msg, data));
    }
}

pub async fn read_messages<T: Message + Default>(
    recv: &mut RecvStream,
) -> Result<(T, Bytes)> {

    let msg_len = aio::read_usize(&mut *recv).await
        .context("Failed to read message length prefix")?;
    let data_len = aio::read_usize(&mut *recv).await
        .context("Failed to read data length prefix")?;
    let total = msg_len + data_len;


    let mut combined = BytesMut::with_capacity(total );
    while combined.len()  < total {
        let n = recv.read_buf(&mut combined).await
            .context("Failed to read frame body")?;
        if n == 0 {
            return Err(anyhow!("unexpected EOF reading frame body"));
        }
    }

    let mut combined = combined.freeze();
    let msg_bytes = combined.split_to(msg_len as usize);

    let msg = T::decode(msg_bytes)
        .context("Failed to decode message")?;

    Ok((msg, combined))
}

