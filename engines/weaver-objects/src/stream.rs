// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The stream objects: one compressed volume in, one member out. A decoder
// cannot jump, so a stream reads its volume from the front and nothing else;
// it is run by its owner the way an engine is, over the volume as a file that
// may still be growing, and its bytes are its envelope, kept whole by the
// owner, because nothing can rebuild compressed bytes from what they decode
// to. A format supplies only how its decoder opens and how big a window the
// stream's first bytes declare.

mod brotli;
mod bzip2;
mod deflate;
mod front;
mod gzip;
mod run;
mod xz;
mod zstd;

#[cfg(test)]
mod tests;

use std::io::{self, Read};
use std::marker::PhantomData;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use weaver_graph::{
    ByteRange, Delivery, Durable, Emit, Engine, Extent, Extraction, Facts, Kind, MemberId, Object,
    Placement, Pull, Seek, VolumeIndex,
};

pub use self::brotli::Brotli;
pub use self::bzip2::Bzip2;
pub use self::deflate::Deflate;
pub use self::gzip::Gzip;
pub use self::xz::Xz;
pub use self::zstd::Zstd;
pub use front::Front;

// What one compression format knows.
pub trait Decoder: Send + Sync + 'static {
    const KIND: Kind;

    // The bytes a run of this stream holds resident, as the stream's first
    // bytes declare them, or the format's largest while they do not say.
    fn window(first: &[u8]) -> u64;

    // The decoded bytes of the stream read from `input`, holding no more
    // than `window` resident.
    fn open(input: Front, window: u64) -> io::Result<Box<dyn Read + Send>>;

    // The range of the volume a failed decode proves bad, when the format
    // can say: read through `input`, a fresh view of the volume.
    fn damaged(_error: &io::Error, _input: &mut Front) -> Option<ByteRange> {
        None
    }
}

pub type XzStream = Stream<Xz>;
pub type GzipStream = Stream<Gzip>;
pub type Bzip2Stream = Stream<Bzip2>;
pub type ZstdStream = Stream<Zstd>;
pub type BrotliStream = Stream<Brotli>;
pub type DeflateStream = Stream<Deflate>;

// The front of the volume a window is read from: enough for every format's
// header up to the field that sizes its window.
const FIRST: usize = 64;

// Its facts are its name and its front. How far a run has read is the run's
// live progress, kept on the owner's shelf and never here: a restored stream
// is one whose run has not started, and it replays from the stream's start
// over the partial.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Stream<D> {
    #[serde(skip)]
    decoder: PhantomData<fn() -> D>,
    name: String,
    first: Vec<u8>,
    // The volume grows no further, so a front shorter than FIRST is the
    // whole of it.
    #[serde(default)]
    closed: bool,
}

impl<D: Decoder> Default for Stream<D> {
    fn default() -> Self {
        Self {
            decoder: PhantomData,
            name: String::new(),
            first: Vec::new(),
            closed: false,
        }
    }
}

impl<D: Decoder> Stream<D> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<D: Decoder> Durable for Stream<D> {
    const VERSION: u16 = 2;

    fn impossible(&self) -> Option<&'static str> {
        (self.first.len() > FIRST).then_some("a front longer than a stream keeps")
    }
}

impl<D: Decoder> Object for Stream<D> {
    fn kind(&self) -> Kind {
        D::KIND
    }

    // The run reads the volume from the owner's file. Only the front is
    // kept, for the window it declares; a repair of it replaces it.
    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _out: &mut Vec<Emit>,
    ) {
        let at = delivery.offset as usize;
        if delivery.volume != VolumeIndex(0) || at > self.first.len() || at >= FIRST {
            return;
        }
        let end = (at + delivery.bytes.len()).min(FIRST);
        if self.first.len() < end {
            self.first.resize(end, 0);
        }
        self.first[at..end].copy_from_slice(&delivery.bytes[..end - at]);
    }

    fn volume_closed(&mut self, _volume: VolumeIndex, _out: &mut Vec<Emit>) {
        self.closed = true;
    }

    fn volume_lost(&mut self, _volume: VolumeIndex, _range: ByteRange, _out: &mut Vec<Emit>) {}

    // One volume, read in order: there is nothing to bid for.
    fn wants(&mut self, _out: &mut Vec<Seek>) {}

    fn pulled(&mut self, _seek: Seek, _pull: Pull, _out: &mut Vec<Emit>) {}

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }

    // A decoded byte lies at no one place in the volume.
    fn extent(&self, _member: MemberId, _range: ByteRange, _out: &mut Vec<Extent>) {}

    fn envelope(&self, _envelope: u32, _range: ByteRange) -> Option<Bytes> {
        None
    }

    fn bound(&mut self, _volume: VolumeIndex, name: &str) {
        name.clone_into(&mut self.name);
    }

    fn reads_in_order(&self) -> bool {
        true
    }

    fn engine(&self) -> Option<&dyn Engine> {
        Some(self)
    }

    fn engine_mut(&mut self) -> Option<&mut dyn Engine> {
        Some(self)
    }

    fn facts(&self) -> Facts {
        Facts::of(D::KIND, self)
    }
}

impl<D: Decoder> Engine for Stream<D> {
    fn start(&self) -> Box<dyn Extraction> {
        Box::new(run::Run::<D>::new(
            member_name(&self.name),
            D::window(&self.first),
        ))
    }

    // The window is sized from the front, so it is known once the front is
    // in: a stream inside another object's member often arrives a sliver
    // at a time.
    fn reserve(&self) -> Option<u64> {
        (self.closed || self.first.len() >= FIRST).then(|| D::window(&self.first))
    }
}

// A compression extension and what it leaves behind: `.tgz` held a `.tar`.
const EXTENSIONS: [(&str, &str); 11] = [
    (".gz", ""),
    (".tgz", ".tar"),
    (".xz", ""),
    (".txz", ".tar"),
    (".bz2", ""),
    (".tbz2", ".tar"),
    (".tbz", ".tar"),
    (".zst", ""),
    (".tzst", ".tar"),
    (".br", ""),
    (".deflate", ""),
];

// The name the one member is announced under: the volume's name with its
// compression extension taken off, or the name as it is when it carries
// none, which the namespace numbers if it collides.
pub fn member_name(volume: &str) -> String {
    let lower = volume.to_ascii_lowercase();
    EXTENSIONS
        .iter()
        .find(|(extension, _)| lower.len() > extension.len() && lower.ends_with(extension))
        .map_or_else(
            || volume.to_owned(),
            |(extension, left)| format!("{}{left}", &volume[..volume.len() - extension.len()]),
        )
}
