// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The buffers a TCP socket inside a WireGuard tunnel asks for, by what it
// carries. Each socket's buffers are memory the tunnel holds for as long as
// the socket lives, so a socket asks for what its traffic needs, each way:
// the receive buffer is the window the far side may fill, the send buffer
// what may wait for the far side's acknowledgement. Anything not named here
// keeps the tunnel's own sizes.
//
// A usenet download sends only short commands, so its send buffer is small
// and its receive buffer large.
use std::sync::atomic::{AtomicUsize, Ordering};

use super::Purpose;
use crate::wireguard::TcpBuffers;

const KIB: usize = 1024;

// A usenet connection or a test of a usenet server. Its receive buffer
// follows the hardware profile in force; its send buffer carries commands.
pub const USENET_SEND_BYTES: usize = 64 * KIB;

// The receive buffer a usenet socket asks for, as the hardware profile in
// force sets it. A change applies to the next socket dialled; one already
// open keeps the buffers it was dialled with. Until a profile sets it, a
// usenet socket asks for what a tuned tunnel gives every socket.
static USENET_RECEIVE_BYTES: AtomicUsize = AtomicUsize::new(USENET_RECEIVE_BEFORE_PROFILE_BYTES);

const USENET_RECEIVE_BEFORE_PROFILE_BYTES: usize = 1024 * KIB;

// Put `bytes` in force as the receive buffer of every usenet socket dialled
// from now on.
pub fn set_usenet_receive_bytes(bytes: usize) {
    USENET_RECEIVE_BYTES.store(bytes, Ordering::Relaxed);
}

// The buffers a socket for `purpose` asks for, or `None` for the tunnel's
// own sizes. A proxy's endpoint does not say what it carries, so it keeps
// the tunnel's sizes.
pub fn socket_buffers(purpose: &Purpose) -> Option<TcpBuffers> {
    match purpose {
        Purpose::Nntp { .. } | Purpose::NntpProbe { .. } => Some(TcpBuffers::new(
            USENET_RECEIVE_BYTES.load(Ordering::Relaxed),
            USENET_SEND_BYTES,
        )),
        Purpose::ProxyEndpoint { .. }
        | Purpose::Feed { .. }
        | Purpose::Outbound
        | Purpose::Probe
        | Purpose::Dns => None,
    }
}
