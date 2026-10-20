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
//! reads; the writer encrypts on the way out.

use std::{
    io::IoSliceMut,
    pin::Pin,
    task::{Context, Poll, ready},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

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

/// The most bytes encrypted per write call. The keystream for bytes the inner
/// stream does not take is recomputed on the next call, so this bounds that work.
const MAX_WRITE_CHUNK: usize = 64 * 1024;

/// Encrypts everything written.
///
/// The cipher only advances by what the inner stream accepted: each call
/// encrypts with a copy of the state, and keeps that copy only as far as the
/// bytes the inner stream took. A caller that retries with different bytes
/// after `Pending` or a short write still gets a correct stream.
pub(crate) struct EncryptingWriter {
    inner: BoxAsyncWrite,
    cipher: Rc4,
    scratch: Vec<u8>,
}

impl EncryptingWriter {
    pub fn new(inner: BoxAsyncWrite, cipher: Rc4) -> Self {
        Self {
            inner,
            cipher,
            scratch: Vec::new(),
        }
    }
}

impl AsyncWrite for EncryptingWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let len = buf.len().min(MAX_WRITE_CHUNK);
        this.scratch.clear();
        this.scratch.extend_from_slice(&buf[..len]);
        let mut next = this.cipher.clone();
        next.apply(&mut this.scratch);
        let n = ready!(Pin::new(&mut this.inner).poll_write(cx, &this.scratch))?;
        if n == len {
            this.cipher = next;
        } else {
            this.cipher.skip(n);
        }
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
        let mut w = EncryptingWriter::new(
            Box::new(Stingy {
                out: out.clone(),
                max: 7,
                calls: 0,
            }),
            Rc4::new_discard_1024(b"k"),
        );
        // Several writes of odd sizes, each split into short writes and Pendings.
        for chunk in plain.chunks(333) {
            w.write_all(chunk).await.unwrap();
        }
        assert_eq!(*out.lock(), expected);
    }

    #[tokio::test]
    async fn test_writer_then_reader_round_trip() {
        let plain: Vec<u8> = (0..100_000u32)
            .map(|i| u8::try_from(i % 253).unwrap())
            .collect();
        let (tx, rx) = tokio::io::duplex(4096);
        let mut w = EncryptingWriter::new(Box::new(tx), Rc4::new_discard_1024(b"k"));
        let mut r = DecryptingReader::new(
            Vec::new(),
            Box::new(rx.into_vectored_compat()),
            Some(Rc4::new_discard_1024(b"k")),
        );
        let writer = async {
            w.write_all(&plain).await.unwrap();
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
}
