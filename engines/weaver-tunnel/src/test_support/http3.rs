//! Local HTTP/3 CONNECT fixture with an explicit destination map and private CA.
use crate::{Http3ProxyCredentials, Http3TunnelSpec, TunnelProvider, shared::SharedProvider};
use base64::Engine;
use bytes::{Buf, Bytes};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    task::{JoinHandle, JoinSet},
};
use tokio_rustls::rustls;

pub struct Http3ServerDouble {
    pub addr: SocketAddr,
    pub targets: Arc<Mutex<Vec<(String, u16)>>>,
    roots: rustls::RootCertStore,
    endpoint: quinn::Endpoint,
    task: JoinHandle<()>,
}

impl Http3ServerDouble {
    pub async fn start(mapping: HashMap<(String, u16), SocketAddr>) -> Self {
        let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
        )
        .unwrap();
        tls.alpn_protocols = vec![b"h3".to_vec()];
        let config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
        ));
        let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = endpoint.local_addr().unwrap();
        let server = endpoint.clone();
        let targets = Arc::new(Mutex::new(Vec::new()));
        let recorded = targets.clone();
        let mapping = Arc::new(mapping);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    incoming = server.accept() => {
                        let Some(incoming) = incoming else { break; };
                        let mapping = mapping.clone();
                        let recorded = recorded.clone();
                        connections.spawn(async move {
                            let Ok(connection) = incoming.await else { return; };
                            let Ok(mut server) = h3::server::builder().build(h3_quinn::Connection::new(connection)).await else { return; };
                            let mut streams = JoinSet::new();
                            loop {
                                tokio::select! {
                                    request = server.accept() => {
                                        let Ok(Some(resolver)) = request else { break; };
                                        let mapping = mapping.clone();
                                        let recorded = recorded.clone();
                                        streams.spawn(async move {
                                            let serve = async {
                                                let (request, mut stream) = resolver.resolve_request().await?;
                                                assert_eq!(request.method(), http::Method::CONNECT);
                                                assert!(request.uri().scheme().is_none());
                                                assert!(request.uri().path_and_query().is_none());
                                                let expected = format!("Basic {}", base64::engine::general_purpose::STANDARD.encode("fixture:fixture-only"));
                                                if request.headers().get(http::header::PROXY_AUTHORIZATION).and_then(|v| v.to_str().ok()) != Some(expected.as_str()) {
                                                    stream.send_response(http::Response::builder().status(407).body(())?).await?;
                                                    stream.finish().await?;
                                                    return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
                                                }
                                                let authority = request.uri().authority().unwrap();
                                                let target = (authority.host().trim_matches(['[', ']']).to_string(), authority.port_u16().unwrap());
                                                recorded.lock().unwrap().push(target.clone());
                                                let Some(destination) = mapping.get(&target) else {
                                                    stream.send_response(http::Response::builder().status(502).body(())?).await?;
                                                    stream.finish().await?;
                                                    return Ok(());
                                                };
                                                let upstream = TcpStream::connect(destination).await?;
                                                stream.send_response(http::Response::builder().status(200).body(())?).await?;
                                                let (mut send, mut recv) = stream.split();
                                                let (mut read, mut write) = upstream.into_split();
                                                let upload = async {
                                                    while let Some(mut data) = recv.recv_data().await? {
                                                        write.write_all_buf(&mut data).await?;
                                                        assert_eq!(data.remaining(), 0);
                                                    }
                                                    write.shutdown().await?;
                                                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                                                };
                                                let download = async {
                                                    let mut buffer = vec![0; 64 * 1024];
                                                    loop {
                                                        let n = read.read(&mut buffer).await?;
                                                        if n == 0 { break; }
                                                        send.send_data(Bytes::copy_from_slice(&buffer[..n])).await?;
                                                    }
                                                    send.finish().await?;
                                                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                                                };
                                                tokio::try_join!(upload, download)?;
                                                Ok(())
                                            };
                                            let _ = tokio::time::timeout(Duration::from_secs(30), serve).await;
                                        });
                                    }
                                    _ = streams.join_next(), if !streams.is_empty() => {}
                                }
                            }
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Self {
            addr,
            targets,
            roots,
            endpoint,
            task,
        }
    }

    pub fn provider(&self) -> Arc<dyn TunnelProvider> {
        Arc::new(SharedProvider(
            proxy_tunnels::Http3TunnelProvider::with_root_certificates(
                Http3TunnelSpec {
                    proxy_config_id: "http3-fixture".into(),
                    revision: "1".into(),
                    host: self.addr.ip().to_string(),
                    port: self.addr.port(),
                    credentials: Some(
                        Http3ProxyCredentials::new("fixture".into(), "fixture-only".into())
                            .unwrap(),
                    ),
                    request_timeout: Duration::from_secs(5),
                },
                self.roots.clone(),
            )
            .unwrap(),
        ))
    }
}

impl Drop for Http3ServerDouble {
    fn drop(&mut self) {
        self.endpoint.close(0u32.into(), b"fixture stopped");
        self.task.abort();
    }
}
