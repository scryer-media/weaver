// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One run of a stream: its volume decoded from the front into its one member,
// waiting for bytes still on their way. A decode that fails ends the member:
// what was said of it is retracted, and the stream is called damaged where
// its format can say so and refused where it cannot.

use std::io::{self, Read};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use weaver_graph::{Emit, Extraction, MemberId, Provenance, Sink, Volumes, Why};

use super::{Decoder, Front};

const MEMBER: MemberId = MemberId(0);

// How much decoded output is handed on at once.
pub(super) const CHUNK: usize = 64 << 10;

pub(super) struct Run<D> {
    decoder: PhantomData<fn() -> D>,
    name: String,
    window: u64,
}

impl<D: Decoder> Run<D> {
    pub(super) fn new(name: String, window: u64) -> Self {
        Self {
            decoder: PhantomData,
            name,
            window,
        }
    }
}

enum Ended {
    Whole,
    Stopped,
}

impl<D: Decoder> Extraction for Run<D> {
    fn run(self: Box<Self>, volumes: Arc<dyn Volumes>, sink: &mut Sink<'_>) {
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
        let error = match D::open(input, self.window).and_then(|reader| pump(reader, sink)) {
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
        let mut again = Front::new(volumes);
        let damaged = D::damaged(&error, &mut again)
            .map(|range| again.place(range))
            .transpose();
        if again.failure().load(Ordering::Acquire) {
            return;
        }
        match damaged {
            Ok(Some(pieces)) if !pieces.is_empty() => {
                for (volume, range) in pieces {
                    if !sink(Emit::Damaged { volume, range }) {
                        return;
                    }
                }
            }
            _ => {
                sink(Emit::Refuse(why(&error)));
            }
        }
    }
}

// Decoded bytes to the sink a whole chunk at a time, however little each
// read of the decoder gives, until the stream ends or fails or the owner
// stops listening: every emit is a step of the owner's, so only the last is
// short. What decoded before a failure is said before it.
fn pump(mut reader: Box<dyn Read + Send>, sink: &mut Sink<'_>) -> io::Result<Ended> {
    let mut buffer = vec![0u8; CHUNK];
    let mut offset = 0u64;
    loop {
        let (filled, failed) = fill(&mut *reader, &mut buffer);
        if filled > 0 {
            let heard = sink(Emit::Bytes {
                member: MEMBER,
                offset,
                bytes: Bytes::copy_from_slice(&buffer[..filled]),
                provenance: Provenance::Fresh,
            });
            if !heard {
                return Ok(Ended::Stopped);
            }
            offset += filled as u64;
        }
        if let Some(error) = failed {
            return Err(error);
        }
        if filled < buffer.len() {
            return Ok(Ended::Whole);
        }
    }
}

// As much of `buffer` as the stream holds, short only at its end or where
// the decoder failed.
fn fill(reader: &mut dyn Read, buffer: &mut [u8]) -> (usize, Option<io::Error>) {
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

// A stream that asks for more than its reservation, or for something its
// decoder does not implement, is not damage; everything else a decoder
// fails on is, and nothing here can repair the decoder's input.
fn why(error: &io::Error) -> Why {
    match error.kind() {
        io::ErrorKind::Unsupported | io::ErrorKind::OutOfMemory => Why::Unsupported,
        _ => Why::Unrepairable,
    }
}
