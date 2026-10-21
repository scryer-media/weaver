// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Explicit socket ownership for revoking a consumer's current route.
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
        sync_connected_state(&owned)?;
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

// A Windows duplicate copies the socket's connection state as it stood when
// the duplicate was taken, and a connect that has just finished can still read
// as connecting there. A duplicate left believing it never connected refuses
// shutdown, so revoke would leave the connection open. Updating the connect
// context reads the state afresh.
#[cfg(target_os = "windows")]
fn sync_connected_state(socket: &Socket) -> io::Result<()> {
    use std::os::windows::io::AsRawSocket;
    use windows_sys::Win32::Networking::WinSock::{
        SO_UPDATE_CONNECT_CONTEXT, SOL_SOCKET, setsockopt,
    };
    // SAFETY: the option takes no value, and the handle is open for the call.
    let result = unsafe {
        setsockopt(
            socket.as_raw_socket() as usize,
            SOL_SOCKET,
            SO_UPDATE_CONNECT_CONTEXT,
            std::ptr::null(),
            0,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "windows"))]
fn sync_connected_state(_socket: &Socket) -> io::Result<()> {
    Ok(())
}
