// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

//! The read and write halves of a connection after the encrypted handshake.
//!
//! Everything past the handshake sees plain bytes: the reader first hands out
//! what the handshake already read (decrypted), then decrypts in place as it
//! reads; the writer encrypts each outgoing buffer in place, once.

use std::{
    io::IoSliceMut,
    pin::Pin,
    task::{Context, Poll, ready},
};

use tokio::io::{AsyncRead, ReadBuf};

use super::rc4::Rc4;
use crate::{
    type_aliases::{BoxAsyncReadVectored, BoxAsyncWrite},
    vectored_traits::AsyncReadVectored,
};

/// Reads `prefix` first (already plain), then the inner stream, decrypted when
/// `cipher` is set.
pub(crate) struct DecryptingReader {
    prefix: Vec<u8>,
    prefix_pos: usize,
    inner: BoxAsyncReadVectored,
    cipher: Option<Rc4>,
}

impl DecryptingReader {
    pub fn new(prefix: Vec<u8>, inner: BoxAsyncReadVectored, cipher: Option<Rc4>) -> Self {
        Self {
            prefix,
            prefix_pos: 0,
            inner,
            cipher,
        }
    }

    /// Box it, or hand back the inner reader when there is nothing to add.
    pub fn into_boxed(self) -> BoxAsyncReadVectored {
        if self.cipher.is_none() && self.prefix.is_empty() {
            return self.inner;
        }
        Box::new(self)
    }
}

impl AsyncRead for DecryptingReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.prefix_pos < this.prefix.len() {
            let n = buf.remaining().min(this.prefix.len() - this.prefix_pos);
            buf.put_slice(&this.prefix[this.prefix_pos..this.prefix_pos + n]);
            this.prefix_pos += n;
            return Poll::Ready(Ok(()));
        }
        let before = buf.filled().len();
        ready!(Pin::new(&mut this.inner).poll_read(cx, buf))?;
        if let Some(c) = this.cipher.as_mut() {
            c.apply(&mut buf.filled_mut()[before..]);
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncReadVectored for DecryptingReader {
    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        vec: &mut [IoSliceMut<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        if this.prefix_pos < this.prefix.len() {
            let mut total = 0;
            for s in vec.iter_mut() {
                let rest = &this.prefix[this.prefix_pos..];
                if rest.is_empty() {
                    break;
                }
                let n = s.len().min(rest.len());
                s[..n].copy_from_slice(&rest[..n]);
                this.prefix_pos += n;
                total += n;
            }
            return Poll::Ready(Ok(total));
        }
        let n = ready!(Pin::new(&mut this.inner).poll_read_vectored(cx, vec))?;
        if let Some(c) = this.cipher.as_mut() {
            let mut left = n;
            for s in vec.iter_mut() {
                if left == 0 {
                    break;
                }
                let k = s.len().min(left);
                c.apply(&mut s[..k]);
                left -= k;
            }
        }
        Poll::Ready(Ok(n))
    }
}

/// The writing half of a connection, and the RC4 cipher its bytes go under
/// when the encryption handshake settled on RC4.
pub(crate) struct Outbound {
    inner: BoxAsyncWrite,
    cipher: Option<Rc4>,
}

impl From<BoxAsyncWrite> for Outbound {
    fn from(inner: BoxAsyncWrite) -> Self {
        Self {
            inner,
            cipher: None,
        }
    }
}

impl Outbound {
    pub fn rc4(inner: BoxAsyncWrite, cipher: Rc4) -> Self {
        Self {
            inner,
            cipher: Some(cipher),
        }
    }

    /// Writes `buf` whole, encrypting it in place first when the connection
    /// is under RC4. `buf` holds the bytes as they went out afterwards.
    pub async fn write_all(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        // The cipher has run over exactly the bytes handed to the inner
        // writer, in order: each byte once, here, before its first write. A
        // write cut short or pending leaves bytes already encrypted, which are
        // written as they are; a call dropped midway ends the connection.
        if let Some(cipher) = &mut self.cipher {
            cipher.apply(buf);
        }
        self.inner.write_all(buf).await
    }

    #[cfg(test)]
    pub async fn shutdown(&mut self) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        self.inner.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};

    use super::*;
    use crate::vectored_traits::{AsyncReadVectoredExt, AsyncReadVectoredIntoCompat};

    /// Takes at most `max` bytes per write, and returns `Pending` on every other call.
    struct Stingy {
        out: Arc<Mutex<Vec<u8>>>,
        max: usize,
        calls: usize,
    }

    impl AsyncWrite for Stingy {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            this.calls += 1;
            if this.calls.is_multiple_of(2) {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let n = buf.len().min(this.max);
            this.out.lock().extend_from_slice(&buf[..n]);
            Poll::Ready(Ok(n))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_short_writes_and_pending_keep_the_keystream_aligned() {
        let plain: Vec<u8> = (0..10_000u32)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        let mut expected = plain.clone();
        Rc4::new_discard_1024(b"k").apply(&mut expected);

        let out = Arc::new(Mutex::new(Vec::new()));
        let mut w = Outbound::rc4(
            Box::new(Stingy {
                out: out.clone(),
                max: 7,
                calls: 0,
            }),
            Rc4::new_discard_1024(b"k"),
        );
        // Several writes of odd sizes, each split into short writes and Pendings.
        for chunk in plain.chunks(333) {
            w.write_all(&mut chunk.to_vec()).await.unwrap();
        }
        assert_eq!(*out.lock(), expected);
    }

    #[tokio::test]
    async fn test_writer_then_reader_round_trip() {
        let plain: Vec<u8> = (0..100_000u32)
            .map(|i| u8::try_from(i % 253).unwrap())
            .collect();
        let (tx, rx) = tokio::io::duplex(4096);
        let mut w = Outbound::rc4(Box::new(tx), Rc4::new_discard_1024(b"k"));
        let mut r = DecryptingReader::new(
            Vec::new(),
            Box::new(rx.into_vectored_compat()),
            Some(Rc4::new_discard_1024(b"k")),
        );
        let writer = async {
            w.write_all(&mut plain.clone()).await.unwrap();
            w.shutdown().await.unwrap();
        };
        let reader = async {
            let mut got = Vec::new();
            r.read_to_end(&mut got).await.unwrap();
            got
        };
        let ((), got) = tokio::join!(writer, reader);
        assert_eq!(got, plain);
    }

    #[tokio::test]
    async fn test_reader_serves_prefix_then_decrypts() {
        let mut c = Rc4::new_discard_1024(b"key");
        let mut wire = b"hello world".to_vec();
        c.apply(&mut wire);
        let (mut tx, rx) = tokio::io::duplex(64);
        tx.write_all(&wire).await.unwrap();
        drop(tx);
        let mut r = DecryptingReader::new(
            b"prefix:".to_vec(),
            Box::new(rx.into_vectored_compat()),
            Some(Rc4::new_discard_1024(b"key")),
        )
        .into_boxed();
        let mut got = Vec::new();
        let mut a = [0u8; 3];
        let mut b = [0u8; 5];
        loop {
            let n = r
                .read_vectored(&mut [IoSliceMut::new(&mut a), IoSliceMut::new(&mut b)])
                .await
                .unwrap();
            if n == 0 {
                break;
            }
            got.extend_from_slice(&a[..n.min(3)]);
            if n > 3 {
                got.extend_from_slice(&b[..n - 3]);
            }
        }
        assert_eq!(got, b"prefix:hello world");
    }

    #[tokio::test]
    async fn test_a_piece_frame_written_in_pieces_decrypts_and_runs_the_keystream_once() {
        let mut frame = vec![0u8; 13 + 16_384];
        frame[..4].copy_from_slice(&(16_384u32 + 9).to_be_bytes());
        frame[4] = 7;
        for (k, b) in frame[13..].iter_mut().enumerate() {
            *b = u8::try_from(k % 241).unwrap();
        }
        let plain = frame.clone();
        let cipher = Rc4::new_discard_1024(b"k");
        let probe = cipher.clone();
        let out = Arc::new(Mutex::new(Vec::new()));
        let mut w = Outbound::rc4(
            Box::new(Stingy {
                out: out.clone(),
                max: 5_000,
                calls: 0,
            }),
            cipher,
        );
        w.write_all(&mut frame).await.unwrap();
        // Short writes and Pendings in the middle; every byte went through the
        // keystream once, and the buffer holds what went out.
        assert_eq!(probe.keystream_bytes() - 1024, plain.len() as u64);
        assert_eq!(*out.lock(), frame);
        let mut got = out.lock().clone();
        Rc4::new_discard_1024(b"k").apply(&mut got);
        assert_eq!(got, plain);
    }

    /// The send-side measurement; it reads process CPU time, which needs Unix.
    #[cfg(unix)]
    mod send_side {
        use super::*;

        const ARM: &str = "B";

        fn sender(inner: BoxAsyncWrite, cipher: Rc4) -> Outbound {
            Outbound::rc4(inner, cipher)
        }

        async fn send(w: &mut Outbound, buf: &mut [u8]) {
            w.write_all(buf).await.unwrap();
        }

        /// Counts what the socket below the cipher did: polls left pending, and
        /// writes that took less than they were offered.
        struct Counting {
            inner: tokio::net::tcp::OwnedWriteHalf,
            pending: Arc<std::sync::atomic::AtomicU64>,
            short: Arc<std::sync::atomic::AtomicU64>,
        }

        impl AsyncWrite for Counting {
            fn poll_write(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<std::io::Result<usize>> {
                use std::sync::atomic::Ordering::Relaxed;
                let this = self.get_mut();
                match Pin::new(&mut this.inner).poll_write(cx, buf) {
                    Poll::Pending => {
                        this.pending.fetch_add(1, Relaxed);
                        Poll::Pending
                    }
                    Poll::Ready(Ok(n)) => {
                        if n < buf.len() {
                            this.short.fetch_add(1, Relaxed);
                        }
                        Poll::Ready(Ok(n))
                    }
                    other => other,
                }
            }
            fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
                Pin::new(&mut self.get_mut().inner).poll_flush(cx)
            }
            fn poll_shutdown(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
            }
        }

        /// A piece message for block `i`: 13-byte header, then 16 KiB of payload.
        fn piece_frame(i: u32, frame: &mut [u8]) {
            let (head, payload) = frame.split_at_mut(13);
            head[..4].copy_from_slice(&(16_384u32 + 9).to_be_bytes());
            head[4] = 7;
            head[5..9].copy_from_slice(&(i / 64).to_be_bytes());
            head[9..13].copy_from_slice(&((i % 64) * 16_384).to_be_bytes());
            for (k, b) in payload.iter_mut().enumerate() {
                *b = u8::try_from((k ^ i as usize) & 0xff).unwrap();
            }
        }

        fn cpu() -> (f64, f64) {
            use nix::sys::resource::{UsageWho, getrusage};
            let u = getrusage(UsageWho::RUSAGE_SELF).unwrap();
            let ms = |t: nix::sys::time::TimeVal| {
                t.tv_sec() as f64 * 1000.0 + t.tv_usec() as f64 / 1000.0
            };
            (ms(u.user_time()), ms(u.system_time()))
        }

        /// Sends `frames` piece messages over loopback TCP under RC4. With
        /// `verify`, the receiver keeps every byte, decrypts them and compares;
        /// otherwise it only drains. `sndbuf` sets the sender's socket buffer,
        /// small enough to cut writes short. Returns (user ms, sys ms, pending polls,
        /// short writes, keystream bytes past the discard, bytes on the wire).
        async fn send_over_loopback(
            frames: u32,
            verify: bool,
            sndbuf: Option<usize>,
        ) -> (f64, f64, u64, u64, u64, u64) {
            use crate::network::SocketFactory;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let drain = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async {
                    let (mut s, _) = crate::test_support::sockets()
                        .connect_tcp(addr)
                        .await
                        .unwrap();
                    let mut buf = vec![0u8; 65_536];
                    let mut kept = Vec::new();
                    let mut total = 0u64;
                    loop {
                        let n = s.read(&mut buf).await.unwrap();
                        if n == 0 {
                            break;
                        }
                        total += n as u64;
                        if verify {
                            kept.extend_from_slice(&buf[..n]);
                        }
                    }
                    (total, kept)
                })
            });
            let (stream, _) = listener.accept().await.unwrap();
            if let Some(size) = sndbuf {
                socket2::SockRef::from(&stream)
                    .set_send_buffer_size(size)
                    .unwrap();
            }
            let (_read, write) = stream.into_split();
            let pending = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let short = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let cipher = Rc4::new_discard_1024(b"send-side key");
            let probe = cipher.clone();
            let mut w = sender(
                Box::new(Counting {
                    inner: write,
                    pending: pending.clone(),
                    short: short.clone(),
                }),
                cipher,
            );
            let mut frame = vec![0u8; 13 + 16_384];
            let before = cpu();
            for i in 0..frames {
                piece_frame(i, &mut frame);
                send(&mut w, &mut frame).await;
            }
            let after = cpu();
            w.shutdown().await.unwrap();
            drop(w);
            let (total, mut kept) = drain.join().unwrap();
            if verify {
                Rc4::new_discard_1024(b"send-side key").apply(&mut kept);
                let mut expected = Vec::with_capacity(kept.len());
                for i in 0..frames {
                    piece_frame(i, &mut frame);
                    expected.extend_from_slice(&frame);
                }
                assert!(kept == expected, "receiver decrypted different bytes");
            }
            use std::sync::atomic::Ordering::Relaxed;
            (
                after.0 - before.0,
                after.1 - before.1,
                pending.load(Relaxed),
                short.load(Relaxed),
                probe.keystream_bytes() - 1024,
                total,
            )
        }

        /// Send-side cost of RC4 peers over loopback, for comparing writer shapes.
        #[tokio::test]
        #[ignore = "measurement, run by hand"]
        async fn rc4_send_side_ab() {
            for (label, sndbuf) in [("default", None), ("sndbuf8k", Some(8_192usize))] {
                // The receiver decrypts every byte and compares.
                send_over_loopback(512, true, sndbuf).await;
                let (user, sys, pending, short, ks, wire) =
                    send_over_loopback(16_384, false, sndbuf).await;
                let mib = wire as f64 / 1_048_576.0;
                println!(
                    "ab arm={ARM} socket={label} mib={mib:.1} user_ms_per_mib={:.4} \
                     sys_ms_per_mib={:.4} pending_per_mib={:.2} short_per_mib={:.2} \
                     keystream_per_wire={:.4}",
                    user / mib,
                    sys / mib,
                    pending as f64 / mib,
                    short as f64 / mib,
                    ks as f64 / wire as f64,
                );
            }
        }
    }
}
