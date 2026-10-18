// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The smart object. One per format, each knowing only its own. Every object
// has the same two faces: it receives bytes from its parent and says what
// they were, and it asks its parent for bytes out of order. It also answers
// two questions about where bytes are, which is what makes repair, refusal,
// restart and seeding possible without ever writing the archive itself.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::deliver::{Delivery, Emit};
use crate::facts::Facts;
use crate::member::MemberId;
use crate::seek::Seek;
use crate::volume::{ByteRange, VolumeIndex};

// A format the sniffer can name and an object can own. The list is closed on
// purpose: adding a format means adding an object, and nothing else.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Kind {
    RarStore,
    SevenZipStore,
    Zip,
    Tar,
    Xz,
    Gzip,
    Bzip2,
    Zstd,
    Brotli,
    Deflate,
    RarEngine,
    SevenZipEngine,
    ZipEngine,
    Par2,
    Par3,
    Sfv,
}

impl Kind {
    // The engine that reads a store's volumes as files when the store cannot
    // stream them. The one place a store kind names its engine kind.
    pub fn engine_for(self) -> Option<Kind> {
        match self {
            Kind::RarStore => Some(Kind::RarEngine),
            Kind::SevenZipStore => Some(Kind::SevenZipEngine),
            Kind::Zip => Some(Kind::ZipEngine),
            _ => None,
        }
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Kind::RarStore => "rar_store",
            Kind::SevenZipStore => "sevenz_store",
            Kind::Zip => "zip",
            Kind::Tar => "tar",
            Kind::Xz => "stream_xz",
            Kind::Gzip => "stream_gz",
            Kind::Bzip2 => "stream_bzip2",
            Kind::Zstd => "stream_zstd",
            Kind::Brotli => "stream_brotli",
            Kind::Deflate => "stream_deflate",
            Kind::RarEngine => "rar_engine",
            Kind::SevenZipEngine => "sevenz_engine",
            Kind::ZipEngine => "zip_engine",
            Kind::Par2 => "par2",
            Kind::Par3 => "par3",
            Kind::Sfv => "sfv",
        };
        formatter.write_str(name)
    }
}

// An object's identity within its job, stable across restarts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ObjectId(pub u32);

// Where a range of an object's volume went: the inverse map, one entry per
// contiguous piece.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Placement {
    // Into a member of this object's namespace, at this member offset. The
    // bytes are in that member's partial, or wherever the member's own
    // owner put them.
    Member { member: MemberId, range: ByteRange },
    // Into this object's envelope: header and padding bytes that belong to
    // no member. The object keeps them and can hand them back.
    Envelope { envelope: u32, range: ByteRange },
    // Nowhere yet: the bytes are held, waiting for the headers that say
    // where they go. The range is the volume's own, the only coordinates
    // held bytes have, so a caller walking the pieces in order keeps its
    // place across them.
    Held { range: ByteRange },
}

impl Placement {
    // How many bytes of the located volume range this piece accounts for.
    pub fn len(&self) -> u64 {
        match self {
            Placement::Member { range, .. }
            | Placement::Envelope { range, .. }
            | Placement::Held { range } => range.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// Where a range of a member lies in the object's own volumes: the forward
// map, one entry per contiguous piece, in member order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Extent {
    pub volume: VolumeIndex,
    pub range: ByteRange,
}

// Send and Sync because a job's tree lives in the pipeline, which runs on a
// multi-threaded runtime and is shared by reference across its awaits. An
// object is plain state with no interior mutability, so both come free.
pub trait Object: Send + Sync {
    fn kind(&self) -> Kind;

    // Bytes of one of my volumes. Say what they were into `out`, which the
    // caller reuses between calls. `read` hands back what one of my
    // placements holds, as it does for `posted`: an object that deciphers
    // reads its placed plaintext to put back the cipher it came from.
    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    );

    // My volume has no more bytes coming: everything that could arrive has.
    fn volume_closed(&mut self, volume: VolumeIndex, out: &mut Vec<Emit>);

    // This range of my volume will never arrive and no recovery covers it.
    fn volume_lost(&mut self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Emit>);

    // What I want ahead of the normal order, right now. The namespace lifts
    // each one to the parent and answers through `pulled`.
    fn wants(&mut self, out: &mut Vec<Seek>);

    // The answer to a seek I made.
    fn pulled(&mut self, seek: Seek, pull: crate::seek::Pull, out: &mut Vec<Emit>);

    // The inverse map: where did this range of my volume go? One piece per
    // contiguous run, in volume order, together covering the whole range.
    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>);

    // The forward map: where does this range of my member lie in my volumes?
    // Empty when the member's layout is not known yet.
    fn extent(&self, member: MemberId, range: ByteRange, out: &mut Vec<Extent>);

    // The bytes posted for this range of my volume, rebuilt through the
    // inverse map: `read` hands back what one placement holds. An object
    // whose members are not its volume's bytes, one that deciphers, rebuilds
    // them itself. `false` when any of it cannot be read.
    fn posted(
        &self,
        volume: VolumeIndex,
        range: ByteRange,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        let mut placements = Vec::new();
        self.locate(volume, range, &mut placements);
        placements.iter().map(Placement::len).sum::<u64>() == range.len()
            && placements.iter().all(|placement| read(placement, into))
    }

    // My volume's length is known: its member declared one, or it closed at
    // the length it reached. Said ahead of every delivery while it is known
    // and once more at the close, so an object acts on it once and ignores
    // the repeats. A store that splits its container into volumes by size
    // learns its geometry here.
    fn sized(&mut self, _volume: VolumeIndex, _length: u64, _out: &mut Vec<Emit>) {}

    // A password the job offers. The object checks it against its own
    // header and, if it opens, carries on.
    fn offer_password(&mut self, _secret: &str, _out: &mut Vec<Emit>) {}

    // Bytes of my envelope, for the inverse map's remainder.
    fn envelope(&self, envelope: u32, range: ByteRange) -> Option<bytes::Bytes>;

    // A member of my parent's namespace, by its name there, is now this
    // volume of mine. The name is how an object that chains its volumes by
    // name knows its own set when a sibling asks to be claimed.
    fn bound(&mut self, _volume: VolumeIndex, _name: &str) {}

    // A new member appeared beside the one I am bound to. Is it one of my
    // volumes? A RAR store claims `.part02.rar` after `.part01.rar`, a 7z
    // store claims `.7z.002`, which has no signature of its own. Only an
    // object can say, because only it knows how its volumes chain. Answer
    // with the volume index the member becomes, its position in the set and
    // never the order it arrived in, or `None` to let the sniffer decide. A
    // freshly built object is asked first about the member it was built
    // for, and `None` makes that member its first volume. The answer only
    // reads: the volume exists once `bound` says so.
    fn claim(&mut self, _name: &str, _first_bytes: &[u8]) -> Option<VolumeIndex> {
        None
    }

    // Whether I can use a volume only once every volume before it has been
    // read: an engine or a stream, which have no map to seek by.
    fn reads_in_order(&self) -> bool {
        false
    }

    // My recovery face, if I am a recovery object: the namespace I live in
    // feeds, binds and repairs through it.
    fn recovery(&self) -> Option<&dyn crate::recovery::Recovery> {
        None
    }

    fn recovery_mut(&mut self) -> Option<&mut dyn crate::recovery::Recovery> {
        None
    }

    // My engine face, if I read my volumes as files: the owner starts and
    // runs it, and hands what it emits back to me through the tree.
    fn engine(&self) -> Option<&dyn crate::engine::Engine> {
        None
    }

    fn engine_mut(&mut self) -> Option<&mut dyn crate::engine::Engine> {
        None
    }

    // My durable state, for a checkpoint. An object that keeps none says
    // so, and a restart fetches again what it held.
    fn facts(&self) -> Facts {
        Facts::unsupported(self.kind())
    }
}
