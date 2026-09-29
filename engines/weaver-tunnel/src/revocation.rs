//! Explicit socket ownership for revoking a consumer's current route.
use socket2::{SockRef, Socket};
use std::{
    io,
    net::Shutdown,
    sync::{Arc, Mutex, Weak},
};

#[derive(Debug, Default)]
pub struct SocketRegistry(Mutex<State>);
#[derive(Debug, Default)]
struct State {
    revoked: bool,
    sockets: Vec<Weak<Socket>>,
}

impl SocketRegistry {
    pub fn check(&self) -> io::Result<()> {
        if self.0.lock().expect("socket registry").revoked {
            Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "consumer route revoked",
            ))
        } else {
            Ok(())
        }
    }
    pub fn track(&self, socket: SockRef<'_>) -> io::Result<Arc<Socket>> {
        let mut state = self.0.lock().expect("socket registry");
        if state.revoked {
            let _ = socket.shutdown(Shutdown::Both);
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "consumer route revoked",
            ));
        }
        state.sockets.retain(|s| s.strong_count() != 0);
        if state.sockets.len() >= 16384 {
            return Err(io::Error::other("consumer socket limit reached"));
        }
        let owned = Arc::new(socket.try_clone()?);
        state.sockets.push(Arc::downgrade(&owned));
        Ok(owned)
    }
    pub fn revoke(&self) {
        let mut state = self.0.lock().expect("socket registry");
        state.revoked = true;
        for socket in state.sockets.drain(..).filter_map(|s| s.upgrade()) {
            let _ = socket.shutdown(Shutdown::Both);
        }
    }
}
