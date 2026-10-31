// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The smart object. One per format, each knowing only its own. Every object
// has the same two faces: it receives bytes from its parent and says what
// they were, and it asks its parent for bytes out of order. It also answers
// two questions about where bytes are, which is what makes repair, refusal,
// restart and seeding possible without ever writing the archive itself.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::deliver::{Delivery, Emit, Handed};
use crate::facts::Facts;
use crate::member::MemberId;
use crate::seek::Seek;
use crate::volume::{ByteRange, VolumeIndex};

// An object's answer to a claim: the places the member could take, none
// when there are none, or not yet, while the front in hand is too short for
// the object to judge the member by, with the places that front gives it.
// Once no more of the front can come, those places are the answer. It
// reads as the places wherever only the places matter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Claim {
    Places(Vec<VolumeIndex>),
    NotYet(Vec<VolumeIndex>),
}

impl Default for Claim {
    fn default() -> Self {
        Self::Places(Vec::new())
    }
}

impl From<Vec<VolumeIndex>> for Claim {
    fn from(places: Vec<VolumeIndex>) -> Self {
        Self::Places(places)
    }
}

impl FromIterator<VolumeIndex> for Claim {
    fn from_iter<I: IntoIterator<Item = VolumeIndex>>(places: I) -> Self {
        Self::Places(places.into_iter().collect())
    }
}

impl std::ops::Deref for Claim {
    type Target = [VolumeIndex];

    fn deref(&self) -> &[VolumeIndex] {
        match self {
            Self::Places(places) | Self::NotYet(places) => places,
        }
    }
}

impl AsRef<[VolumeIndex]> for Claim {
    fn as_ref(&self) -> &[VolumeIndex] {
        self
    }
}

impl PartialEq<Vec<VolumeIndex>> for Claim {
    fn eq(&self, other: &Vec<VolumeIndex>) -> bool {
        matches!(self, Self::Places(places) if places == other)
    }
}

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
    Split,
    RarEngine,
    SevenZipEngine,
    ZipEngine,
    Par2,
    Par3,
    Sfv,
    RarRecovery,
    RarRecord,
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
            Kind::Split => "split",
            Kind::RarEngine => "rar_engine",
            Kind::SevenZipEngine => "sevenz_engine",
            Kind::ZipEngine => "zip_engine",
            Kind::Par2 => "par2",
            Kind::Par3 => "par3",
            Kind::Sfv => "sfv",
            Kind::RarRecovery => "rar_recovery",
            Kind::RarRecord => "rar_record",
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

    // The part of this piece that starts `skip` bytes into it and runs
    // `len` bytes: every placement maps its volume bytes one to one.
    pub fn slice(&self, skip: u64, len: u64) -> Placement {
        let cut = |range: &ByteRange| ByteRange::at(range.start + skip, len);
        match self {
            Placement::Member { member, range } => Placement::Member {
                member: *member,
                range: cut(range),
            },
            Placement::Envelope { envelope, range } => Placement::Envelope {
                envelope: *envelope,
                range: cut(range),
            },
            Placement::Held { range } => Placement::Held { range: cut(range) },
        }
    }
}

// The bytes posted for an object's volumes as the object stood when it
// handed this out, read away from the object: a repair reads them on a
// thread of its own while the object goes on taking deliveries. `read` hands
// back what one placement holds, as it does for `Object::posted`.
pub trait Posting: Send + Sync {
    fn posted(
        &self,
        volume: VolumeIndex,
        range: ByteRange,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool;
}

// The inverse map of some ranges of an object's volumes, taken once: it
// answers any range within them as `locate` did, and the bytes posted there
// as the default `Object::posted` does.
#[derive(Clone, Debug, Default)]
pub struct Placements {
    taken: Vec<(VolumeIndex, ByteRange, Vec<Placement>)>,
}

impl Placements {
    pub fn take(
        ranges: &[(VolumeIndex, ByteRange)],
        mut locate: impl FnMut(VolumeIndex, ByteRange, &mut Vec<Placement>),
    ) -> Self {
        let taken = ranges
            .iter()
            .map(|&(volume, range)| {
                let mut placements = Vec::new();
                locate(volume, range, &mut placements);
                (volume, range, placements)
            })
            .collect();
        Self { taken }
    }

    // The pieces of `range`, cut from the ranges taken. Short of the range
    // where it reaches past them.
    pub fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        let Some((_, whole, placements)) = self
            .taken
            .iter()
            .find(|(at, whole, _)| *at == volume && whole.covers(range))
        else {
            return;
        };
        let mut at = whole.start;
        for placement in placements {
            let piece = ByteRange::at(at, placement.len());
            at = piece.end;
            if let Some(part) = piece.intersect(range) {
                out.push(placement.slice(part.start - piece.start, part.len()));
            }
        }
    }
}

impl Posting for Placements {
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
}

// Where a range of a member lies in the object's own volumes: the forward
// map, one entry per contiguous piece, in member order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Extent {
    pub volume: VolumeIndex,
    pub range: ByteRange,
}

// What a store says of a member range it was asked where it lies, beside the
// extents it gave.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Located {
    // The extents given are all of the range.
    Whole,
    // The extents given are the part the store has placed; the rest lies in
    // volumes whose headers it has not read yet, and is located once it has.
    NotYet,
    // The bytes can never be addressed: the member is no run of the
    // volumes' raw bytes, so only the normal order reaches them.
    Never,
}

// What an object parsed of one of its volumes, which travels with the member
// when the object gives it back: the next owner adopts it in place of
// reading the member's headers again. Only an object of the same kind can
// read it; to everyone else, the tree among them, it is opaque.
pub type Carried = Box<dyn std::any::Any + Send + Sync>;

// What the tree records of an object's members and volumes, read when the
// job is quiet. Read only: the record stays the tree's, and the object
// rules on it.
pub trait Standing {
    // A member of mine still holds damage that nothing ruled on: no
    // recovery object covering it ruled on it, none covering what my
    // volumes came out of found them whole or made them so, and it is no
    // volume an object below gave back, whose damage that object ruled on.
    fn damaged(&self) -> bool;

    // I named no member, or a member I named is not placed: it never
    // closed, nor holds every byte it will, nor is a recovery object's
    // volume.
    fn unplaced(&self) -> bool;

    // A range of my volumes was lost and nothing vouched for it: what
    // `volume_lost` told just before.
    fn lost(&self) -> bool;

    // My run asked for a password, and no offer, nor a run started since,
    // has answered it.
    fn asking(&self) -> bool;

    // The members beside my volumes that no object owns, as a claim reads
    // them: what I can say of them is mine to say.
    fn beside(&self) -> Vec<Beside<'_>>;
}

// A member no object owns, as the namespace holds it: its name, its first
// bytes and its length once known, and what an object that gave it back
// parsed of it.
pub struct Beside<'a> {
    pub name: &'a str,
    pub front: &'a [u8],
    pub length: Option<u64>,
    pub carried: Option<&'a Carried>,
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
    // A close may be told again, as a loss may: a restore tells every close
    // again. A close told again is the one already heard.
    fn volume_closed(&mut self, volume: VolumeIndex, out: &mut Vec<Emit>);

    // This range of my volume will never arrive and no recovery covers it.
    // A range may be told again, alone or inside a wider run: a restore tells
    // every loss again, and a quiet job tells its data objects every loss
    // each time it settles. A loss told again is the one already held.
    fn volume_lost(&mut self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Emit>);

    // The job is quiet: nothing more will arrive and no repair will come,
    // and every lost range of my volumes was told through `volume_lost`
    // just before. An object that cannot finish gives its volumes back now,
    // for its own reason, read from its own state and from what the tree
    // records of its members: a volume of its set that never came, a member
    // still damaged, a password it is still asking for, a member it named
    // and never placed. Nothing else rules for it.
    fn quiet(&mut self, _standing: &dyn Standing, _out: &mut Vec<Emit>) {}

    // What I want ahead of the normal order, right now. The namespace lifts
    // each one to the parent and answers through `pulled`.
    fn wants(&self, out: &mut Vec<Seek>);

    // The answer to a seek I made.
    fn pulled(&mut self, seek: Seek, pull: crate::seek::Pull, out: &mut Vec<Emit>);

    // The inverse map: where did this range of my volume go? One piece per
    // contiguous run, in volume order, together covering the whole range.
    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>);

    // The forward map: where does this range of my member lie in my volumes?
    // The extents go into `out`; the answer says whether they are all of the
    // range, the part placed so far, or whether the range can never be
    // addressed.
    fn extent(&self, member: MemberId, range: ByteRange, out: &mut Vec<Extent>) -> Located;

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

    // The bytes posted at this range of my volume are not here: a restart
    // could not read them back, and they will be posted again. I forget what
    // I said of them, so I say it again as they come.
    fn unposted(&mut self, volume: VolumeIndex, range: ByteRange);

    // What `posted` answers for these ranges of my volumes, as I stand now,
    // owned and apart from me. An object that rebuilds its posted bytes
    // itself hands out what that rebuilding needs, shared rather than
    // copied.
    fn posting(&self, ranges: &[(VolumeIndex, ByteRange)]) -> Box<dyn Posting> {
        Box::new(Placements::take(ranges, |volume, range, out| {
            self.locate(volume, range, out)
        }))
    }

    // My volume's length is known: its member declared one, or it closed at
    // the length it reached. Said ahead of every delivery while it is known
    // and once more at the close, so an object acts on it once and ignores
    // the repeats. A length that differs is the producer's word now, as when
    // a store beneath a split hears its member said anew, and it is final
    // only at the close. A store that splits its container into volumes by
    // size learns its geometry here.
    fn sized(&mut self, _volume: VolumeIndex, _length: u64, _out: &mut Vec<Emit>) {}

    // A password the job offers. The object checks it against its own
    // header and, if it opens, carries on.
    fn offer_password(&mut self, _secret: &str, _out: &mut Vec<Emit>) {}

    // Bytes of my envelope, for the inverse map's remainder.
    fn envelope(&self, envelope: u32, range: ByteRange) -> Option<bytes::Bytes>;

    // A member of my parent's namespace, by its name there, the first bytes
    // it has so far and its length once known, is now this volume of mine.
    // They are how an object that chains its volumes, by name, by what their
    // headers state or by how long they are, knows its own set when a
    // sibling asks to be claimed: the same name, front and length a claim
    // reads.
    fn bound(&mut self, _volume: VolumeIndex, _name: &str, _front: &[u8], _length: Option<u64>) {}

    // A read-only copy of placement metadata for this kind's engine. The
    // retiring store retains every byte and inverse map until materialized.
    fn engine_volume(&self, _volume: VolumeIndex) -> Option<Carried> {
        None
    }

    // I am a window on recovery data another object found, and this is
    // what it handed me to read the range by: at my building, again each
    // time it hands it anew, and again when I take my node back after a
    // loan, whether I am the one lent or one rebuilt from its facts.
    fn handed(&mut self, _handed: &Handed) {}

    // A new member appeared beside the one I am bound to. Is it one of my
    // volumes? A RAR store claims `.part02.rar` after `.part01.rar`, a split
    // store `.002` after `.001`. Only an
    // object can say, because only it knows how its volumes chain. Answer
    // with every volume index the member could become, held or not, its
    // position in the set and never the order it arrived in, or none to let
    // the sniffer decide: whether the member is the one holding a place is
    // the tree's to judge, by fronts, as which of several free places it
    // takes is. A freshly built object is asked first about the member it
    // was built for, and none makes that member its first volume. The
    // answer only reads: the volume exists once `bound` says so. An object
    // that cannot judge the member by the front it has so far answers not
    // yet, with the places that front gives it, and is asked again as the
    // front grows; once no more of it can come, those places are its answer.
    fn claim(&mut self, _name: &str, _first_bytes: &[u8], _length: Option<u64>) -> Claim {
        Claim::default()
    }

    // Bytes outrank names, and what states a place outranks links: whether
    // I hold `volume` only provisionally, by links alone, where the member
    // whose claim named it, by its name, its first bytes or what was parsed
    // of it, states its place, so the member takes the place and the holder
    // is given back. Only reads.
    fn provisional(
        &self,
        _volume: VolumeIndex,
        _name: &str,
        _first_bytes: &[u8],
        _carried: Option<&Carried>,
    ) -> bool {
        false
    }

    // The parse a volume can carry to another owner, without changing this
    // object. Only a volume whose bytes contribute to no output may travel.
    // A sibling judges this same parse before the tree changes any binding;
    // `give_back` then transfers it. No bytes are read to answer.
    fn carried(&self, _volume: VolumeIndex) -> Option<&(dyn std::any::Any + Send + Sync)> {
        None
    }

    // Every volume `carried` answers for, as I stand now. I keep them as my
    // volumes come, go, and come to contribute to an output, so a sibling's
    // take reads only these, never a volume that routes output.
    fn carriable(&self, _out: &mut Vec<VolumeIndex>) {}

    // A volume I said I give back: what I parsed of it, to travel with the
    // member, or `None` when I keep nothing of it worth carrying or more
    // than an object may carry, and the member is refused instead.
    fn give_back(&mut self, _volume: VolumeIndex) -> Option<Carried> {
        None
    }

    // A volume of mine is now a volume of an object its bytes founded: my
    // claim on it was its name's, which bytes outrank. I hold nothing of it
    // from here.
    fn taken(&mut self, _volume: VolumeIndex) {}

    // A sibling's transferable volume, with its retained front and parse:
    // is it one of mine? The parse outranks the front and name wherever it
    // says more. It answers as `claim` does, not yet included. This only
    // reads, and never fetches the member again.
    fn reclaim(
        &self,
        _name: &str,
        _front: &[u8],
        _length: Option<u64>,
        _carried: &(dyn std::any::Any + Send + Sync),
    ) -> Claim {
        Claim::default()
    }

    // A member I reclaimed is now this volume of mine, and what was parsed
    // of it is mine to keep: `bound`, for a member that brings its parse.
    fn adopt(&mut self, volume: VolumeIndex, name: &str, _carried: Carried) {
        self.bound(volume, name, &[], None);
    }

    // My volume's member has another name now: a recovery set named it.
    // A stronger name can change its placement, through the same emits as
    // a delivery. The tree applies them before making further claims.
    fn renamed(&mut self, _volume: VolumeIndex, _name: &str, _out: &mut Vec<Emit>) {}

    // My set was refused, whoever said why: I read nothing more, and what I
    // claim after goes where the set went.
    fn refused(&mut self) {}

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
