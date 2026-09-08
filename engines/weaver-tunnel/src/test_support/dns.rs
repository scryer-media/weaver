//! DNS-over-TCP fixture. Only names in the explicit table receive addresses.
use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
pub struct DnsServerDouble {
    pub addr: SocketAddr,
    pub names: Arc<Mutex<HashMap<String, Ipv4Addr>>>,
    pub queries: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for DnsServerDouble {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl DnsServerDouble {
    pub async fn start(names: HashMap<String, Ipv4Addr>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let names = Arc::new(Mutex::new(names));
        let queries = Arc::new(Mutex::new(Vec::new()));
        let table = names.clone();
        let log = queries.clone();
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((mut stream,_)) = accepted else { break; }; let table=table.clone(); let log=log.clone();
                        tasks.spawn(async move {
                            let Ok(size)=stream.read_u16().await else { return; }; if size > 4096 { return; }
                            let mut query=vec![0;size as usize]; if stream.read_exact(&mut query).await.is_err() { return; }
                            let Some((name, qtype))=question(&query) else { return; }; log.lock().unwrap().push(name.clone());
                            let address=table.lock().unwrap().get(&name).copied();
                            let mut response=query.clone(); response[2]=0x81; response[3]=if address.is_some() {0x80} else {0x83};
                            if let Some(ip)=address.filter(|_| qtype == 1) {
                                response[6..8].copy_from_slice(&1u16.to_be_bytes());
                                response.extend_from_slice(&[0xc0,12,0,1,0,1,0,0,0,1,0,4]); response.extend_from_slice(&ip.octets());
                            }
                            let _=stream.write_u16(response.len() as u16).await; let _=stream.write_all(&response).await;
                        });
                    }
                    _ = tasks.join_next(), if !tasks.is_empty() => {}
                }
            }
        });
        Self {
            addr,
            names,
            queries,
            task,
        }
    }
}
fn question(packet: &[u8]) -> Option<(String, u16)> {
    let mut at = 12;
    let mut labels = Vec::new();
    loop {
        let n = *packet.get(at)? as usize;
        at += 1;
        if n == 0 {
            break;
        }
        if n > 63 {
            return None;
        }
        labels.push(std::str::from_utf8(packet.get(at..at + n)?).ok()?);
        at += n;
    }
    Some((
        labels.join("."),
        u16::from_be_bytes(packet.get(at..at + 2)?.try_into().ok()?),
    ))
}
