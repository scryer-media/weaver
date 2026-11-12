// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One run of a stream: its volume decoded from the front into its one member,
// waiting for bytes still on their way. A decode that fails ends the member:
// what was said of it is retracted and the stream refused. Its decoder's
// checks speak of what the stream reads out, never of the volume's bytes,
// so a failed decode marks nothing in the volume.

use std::io::{self, Read};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use weaver_graph::{Emit, Extraction, MemberId, Provenance, Sink, Volumes, Why};

use super::{Decoder, Front};

pub(super) const MEMBER: MemberId = MemberId(0);

// How much decoded output is handed on at once.
pub(super) const CHUNK: usize = 64 << 10;

pub(super) struct Run<D> {
    decoder: PhantomData<fn() -> D>,
    name: String,
    // The window the stream's front sized against the grant it was built
    // with.
    window: u64,
    // The stream's front, which sizes the window again against the grant
    // the run is given, where that is wider.
    first: Vec<u8>,
    // The volume's length as the stream knew it, which sizes the window
    // with the front.
    length: Option<u64>,
}

impl<D: Decoder> Run<D> {
    pub(super) fn new(name: String, window: u64, first: Vec<u8>, length: Option<u64>) -> Self {
        Self {
            decoder: PhantomData,
            name,
            window,
            first,
            length,
        }
    }
}

enum Ended {
    Whole,
    Stopped,
}

impl<D: Decoder> Extraction for Run<D> {
    fn run(
        self: Box<Self>,
        volumes: Arc<dyn Volumes>,
        sink: &mut Sink<'_>,
        budget: weaver_graph::Budget,
    ) {
        // Asked before anything is read, so a volume still growing is
        // never waited on for its tail.
        let closed = volumes.len(weaver_graph::VolumeIndex(0)).is_some();
        // The window the stream was built to, or the run's grant's where
        // that is wider: an owner that lets a window go above its ceiling
        // grants the run the wider one. An owner that reserved the whole
        // grant for the run gives the decoder all of it, since a later
        // block or frame may need more than the first declares.
        let window = if budget.window_reserved {
            budget.window_or(self.window).max(self.window)
        } else {
            self.window
                .max(D::window(&self.first, self.length, &budget))
        };
        // What the front declares the decoder needs, where the decoder's
        // own refusal does not say.
        let declared = D::declared(&self.first);
        let input = Front::new(Arc::clone(&volumes));
        let failed = input.failure();
        let announced = sink(Emit::Member {
            member: MEMBER,
            name: self.name,
            length: None,
        });
        if !announced {
            return;
        }
        let pumped = D::open_with(input, window, budget, closed)
            .and_then(|mut reader| pump(&mut *reader, sink));
        let error = match pumped {
            Ok(Ended::Whole) => {
                sink(Emit::MemberClosed(MEMBER));
                return;
            }
            Ok(Ended::Stopped) => return,
            Err(error) => error,
        };
        // The owner failed a read, so it already knows why.
        if failed.load(Ordering::Acquire) || !sink(Emit::Retract(MEMBER)) {
            return;
        }
        sink(Emit::Refuse(why(&error, window, declared)));
    }
}

// Decoded bytes to the sink a whole chunk at a time, however little each
// read of the decoder gives, until the stream ends or fails or the owner
// stops listening: every emit is a step of the owner's, so only the last is
// short. What decoded before a failure is said before it.
fn pump<R: Read + ?Sized>(reader: &mut R, sink: &mut Sink<'_>) -> io::Result<Ended> {
    let mut buffer = vec![0u8; CHUNK];
    let mut offset = 0u64;
    loop {
        let (filled, failed) = fill(&mut *reader, &mut buffer);
        if !say(sink, offset, &buffer[..filled]) {
            return Ok(Ended::Stopped);
        }
        offset += filled as u64;
        match failed {
            Some(error) => return Err(error),
            None if filled < buffer.len() => return Ok(Ended::Whole),
            None => {}
        }
    }
}

// Bytes of the member to the sink, nothing for none; `false` once the owner
// stopped listening.
fn say(sink: &mut Sink<'_>, offset: u64, bytes: &[u8]) -> bool {
    bytes.is_empty()
        || sink(Emit::Bytes {
            member: MEMBER,
            offset,
            bytes: Bytes::copy_from_slice(bytes).into(),
            provenance: Provenance::Fresh,
        })
}

// As much of `buffer` as the stream holds, short only at its end or where
// the decoder failed.
fn fill<R: Read + ?Sized>(reader: &mut R, buffer: &mut [u8]) -> (usize, Option<io::Error>) {
    let mut filled = 0;
    while filled < buffer.len() {
        match reader.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return (filled, Some(error)),
        }
    }
    (filled, None)
}

// A stream that asks for more memory than its window, or for something its
// decoder does not implement, is not damage; everything else a decoder
// fails on is, and nothing here can repair the decoder's input. A decoder
// that states the sizes it refused says them. zstd states none: it was held
// to the run's window, and needs what its front declares when that is more.
fn why(error: &io::Error, window: u64, declared: Option<u64>) -> Why {
    if let Some(why) = crate::verdict::too_large(error) {
        return why;
    }
    if zstd_0_13_frame_too_wide(&error.to_string()) {
        return Why::TooLarge {
            needed: declared.filter(|declared| *declared > window),
            allowed: Some(window),
        };
    }
    match error.kind() {
        io::ErrorKind::Unsupported => Why::Unsupported,
        _ => Why::Unrepairable,
    }
}

// zstd 0.13 (libzstd 1.5.7): a frame whose window is wider than the
// decoder's limit is an `io::Error` carrying `ZSTD_getErrorName`'s text for
// `frameParameter_windowTooLarge`. zstd exposes no typed error code through
// its `Read` adapters.
fn zstd_0_13_frame_too_wide(text: &str) -> bool {
    text.contains("Frame requires too much memory for decoding")
}
