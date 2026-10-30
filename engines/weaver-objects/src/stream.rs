// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The stream objects: one compressed stream in, one member out. A decoder
// cannot jump, so a stream reads its volumes from the front and nothing else;
// it is run by its owner the way an engine is, over the volumes as files that
// may still be growing, and its bytes are its envelope, kept whole by the
// owner, because nothing can rebuild compressed bytes from what they decode
// to. A stream posted in numbered pieces, `name.xz.001`, `name.xz.002`, …, is
// one stream over several volumes: only the first carries the signature, and
// the others' names place them. A format supplies only how its decoder opens
// and how big a window the stream's first bytes declare.

mod brotli;
mod bzip2;
mod deflate;
mod front;
mod gzip;
mod pieces;
mod run;
mod xz;
mod zstd;

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::io::{self, Read};
use std::marker::PhantomData;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use weaver_graph::{
    Budget, ByteRange, Delivery, Durable, Emit, Engine, Extent, Extraction, Facts, Kind, Located,
    MemberId, Object, Placement, Pull, Seek, Sniff, Standing, VolumeIndex, signed,
};

pub use self::brotli::Brotli;
pub use self::bzip2::Bzip2;
pub use self::deflate::Deflate;
pub use self::gzip::Gzip;
pub use self::pieces::Pieces;
pub use self::xz::Xz;
pub use self::zstd::Zstd;
pub use front::Front;

use crate::split;

// What one compression format knows.
pub trait Decoder: Send + Sync + 'static {
    const KIND: Kind;

    // The number a set's first piece carries when names are all that
    // place its pieces, so any piece founds the set and the first may come
    // after it, or never. `None` where the first piece is the one that
    // opens the signature, whatever its number.
    const FIRST_PIECE: Option<u32> = None;

    // Whether the decoded bytes are the input's own, at the same offsets:
    // a span of the input nothing will bring is then that span of the
    // member, and the run reads on past it, leaving it a gap marked damaged
    // that a recovery set over the member rebuilds. Such a decoder is
    // never opened: the run reads its input itself.
    const IDENTITY: bool = false;

    // The bytes a run of this stream holds resident, as the stream's first
    // bytes declare them, or the format's largest while they do not say. A
    // decoder whose window the format lets grow holds it to the memory
    // `budget` grants.
    fn window(first: &[u8], budget: &Budget) -> u64;

    // The decoded bytes of the stream read from `input`, holding no more
    // than `window` resident. Nothing to do for an identity decoder.
    fn open(input: Front, _window: u64) -> io::Result<Box<dyn Read + Send>> {
        Ok(Box::new(input))
    }

    // [`Self::open`], within what the run was granted. `closed` says the
    // stream is one volume already closed when the run opens it, so its
    // tail can be read at once; a decoder that reads its structure from
    // the tail may then choose how to decode.
    fn open_with(
        input: Front,
        window: u64,
        _budget: weaver_graph::Budget,
        _closed: bool,
    ) -> io::Result<Box<dyn Read + Send>> {
        Self::open(input, window)
    }

    // The name the one member is announced under, from the first volume's.
    fn member(volume: &str) -> String {
        member_name(volume)
    }

    // Why the stream gives its volumes back once nothing more will reach
    // its run: a member left damaged cannot be read whole from them, nor
    // one never closed.
    fn quiet(standing: &dyn Standing, _pieces: &BTreeSet<u32>, out: &mut Vec<Emit>) {
        out.extend(
            crate::quiet::damaged(standing)
                .or_else(|| crate::quiet::unplaced(standing))
                .map(Emit::Refuse),
        );
    }
}

pub type XzStream = Stream<Xz>;
pub type GzipStream = Stream<Gzip>;
pub type Bzip2Stream = Stream<Bzip2>;
pub type ZstdStream = Stream<Zstd>;
pub type BrotliStream = Stream<Brotli>;
pub type DeflateStream = Stream<Deflate>;
pub type SplitStream = Stream<Pieces>;

// The front of the volume a window is read from: enough for every format's
// header up to the field that sizes its window.
const FIRST: usize = 64;

// Its facts are a volume's name, the first volume's front and the volumes
// it holds. How far a run has read is the run's live progress, kept on the
// owner's shelf and never here: a restored stream is one whose run has not
// started, and it replays from the stream's start over the partial.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Stream<D> {
    #[serde(skip)]
    decoder: PhantomData<fn() -> D>,
    // The first volume's name, or the name of whichever piece founded the
    // stream until the first is bound: the member's name and the set's stem
    // read the same from either.
    name: String,
    first: Vec<u8>,
    // The first volume grows no further, so a front shorter than FIRST is
    // the whole of it.
    #[serde(default)]
    closed: bool,
    // The volumes bound, by index.
    #[serde(default)]
    pieces: BTreeSet<u32>,
    // What a run of it is granted, which sizes its window.
    #[serde(skip)]
    budget: Budget,
}

impl<D: Decoder> Default for Stream<D> {
    fn default() -> Self {
        Self {
            decoder: PhantomData,
            name: String::new(),
            first: Vec::new(),
            closed: false,
            pieces: BTreeSet::new(),
            budget: Budget::default(),
        }
    }
}

impl<D: Decoder> Stream<D> {
    pub fn new() -> Self {
        Self::default()
    }

    // Held to what `budget` grants a run of it.
    pub fn within(mut self, budget: Budget) -> Self {
        self.budget = budget;
        self
    }
}

impl<D: Decoder> Durable for Stream<D> {
    const VERSION: u16 = 3;

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

    fn volume_closed(&mut self, volume: VolumeIndex, _out: &mut Vec<Emit>) {
        self.closed |= volume == VolumeIndex(0);
    }

    fn volume_lost(&mut self, _volume: VolumeIndex, _range: ByteRange, _out: &mut Vec<Emit>) {}

    // Nothing more will reach the run; the decoder says whether its volumes
    // go back as they stand.
    fn quiet(&mut self, standing: &dyn Standing, out: &mut Vec<Emit>) {
        D::quiet(standing, &self.pieces, out);
    }

    // One volume, read in order: there is nothing to bid for.
    fn wants(&self, _out: &mut Vec<Seek>) {}

    fn pulled(&mut self, _seek: Seek, _pull: Pull, _out: &mut Vec<Emit>) {}

    // The front it copies out is in its facts, which a restart keeps, and its
    // run reads the volumes themselves: it says nothing of the posted bytes.
    fn unposted(&mut self, _volume: VolumeIndex, _range: ByteRange) {}

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }

    // A decoded byte lies at no one place in the volume.
    fn extent(&self, _member: MemberId, _range: ByteRange, _out: &mut Vec<Extent>) -> Located {
        Located::Never
    }

    fn envelope(&self, _envelope: u32, _range: ByteRange) -> Option<Bytes> {
        None
    }

    fn bound(&mut self, volume: VolumeIndex, name: &str, _front: &[u8], _length: Option<u64>) {
        if volume == VolumeIndex(0) || self.name.is_empty() {
            name.clone_into(&mut self.name);
        }
        self.pieces.insert(volume.0);
    }

    fn renamed(&mut self, volume: VolumeIndex, name: &str, _out: &mut Vec<Emit>) {
        if volume == VolumeIndex(0) {
            name.clone_into(&mut self.name);
        }
    }

    // A piece of the set, when the volumes are pieces: its place is its
    // number counted from the first piece's, the format's number for a set
    // known by names alone and else the first volume's own, whether that
    // place is held or not: a piece posted again is that piece. A name is all
    // that places a piece here, so it never outvotes a front a format
    // names: that piece is the format's, wherever its name numbers it. A
    // stream with a name counts from its own set's first piece, so only a
    // piece of that set is claimed.
    fn name_only(&self, volume: VolumeIndex) -> bool {
        // Identity joins learn no container membership from opaque bytes:
        // their numbered placement is solely the decoder's naming rule.
        D::IDENTITY && self.pieces.contains(&volume.0)
    }

    fn claim(
        &mut self,
        name: &str,
        first_bytes: &[u8],
        length: Option<u64>,
    ) -> Option<VolumeIndex> {
        if matches!(signed(first_bytes, length), Sniff::Object(_)) {
            return None;
        }
        let piece = split::parse(name)?;
        let first = match D::FIRST_PIECE {
            Some(number) if self.name.is_empty() => piece.numbered(number),
            Some(number) => split::parse(&self.name)?.numbered(number),
            None => split::parse(&self.name)?,
        };
        piece.index_from(&first).map(VolumeIndex)
    }

    // The object its bytes founded holds it now; the front it carried was
    // the first volume's only if it was that volume.
    fn taken(&mut self, volume: VolumeIndex) {
        self.pieces.remove(&volume.0);
        if volume == VolumeIndex(0) {
            self.first.clear();
            self.closed = false;
        }
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
    fn start(&mut self) -> Box<dyn Extraction> {
        Box::new(run::Run::<D>::new(
            D::member(&self.name),
            D::window(&self.first, &self.budget),
            // A stream not cut into pieces is its first volume alone.
            split::parse(&self.name).is_none(),
        ))
    }

    // The window is sized from the front, so it is known once the front is
    // in: a stream inside another object's member often arrives a sliver
    // at a time.
    fn reserve(&self) -> Option<u64> {
        (self.closed || self.first.len() >= FIRST).then(|| D::window(&self.first, &self.budget))
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

// The name the one member is announced under: the first volume's name with
// its piece number and compression extension taken off, or the name as it is
// when it carries no extension, which the namespace numbers if it collides.
pub fn member_name(volume: &str) -> String {
    let stream = split::whole(volume).unwrap_or(volume);
    let lower = stream.to_ascii_lowercase();
    EXTENSIONS
        .iter()
        .find(|(extension, _)| lower.len() > extension.len() && lower.ends_with(extension))
        .map_or_else(
            || volume.to_owned(),
            |(extension, left)| format!("{}{left}", &stream[..stream.len() - extension.len()]),
        )
}
