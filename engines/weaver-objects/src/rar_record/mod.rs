// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A RAR recovery record: the parity a RAR volume carries over its own bytes
// before the record's header, RAR5 or RAR3. It is built over the record's
// range as a window on the volume, so it lives in the namespace of the
// volume and covers that one member, with one slice over the bytes it
// protects. It is never delivered.
//
// It rules once the volume is closed and bound. A volume every byte of
// which passed whole is clean without a read; otherwise unrar-rs checks the
// volume against the record, read by range through the namespace: clean
// when no unit is damaged, repaired once the damaged units it rebuilt are
// written back into the member, impossible when the record cannot rebuild
// them all or cannot vouch for every protected byte. A clean or repaired
// ruling proves the protected bytes only. A record that is not there or
// cannot be read says nothing of the volume: it covers nothing, and, as a
// recovery object with nothing to rule on does, rules clean on nothing.
//
// A RAR3 volume with enciphered headers is walked to its record under a
// password, taken as the volume's store takes one: each candidate the job
// offers is held against the volume's first enciphered header, the next is
// asked for while the ones offered are refuted or cannot be told yet, and
// none once one unseals it. The one that did is the last the record was
// offered, which is the one a restore offers it again.

#[cfg(any(test, feature = "test-support"))]
pub mod encode;
mod volume;

#[cfg(test)]
mod tests;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use unrar_rs::{ArchiveFormat, RarError, RecoveryRecordAssessment, RecoveryRecordLayout};
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{
    ByteRange, Coverage, Delivery, Durable, Emit, Extent, Facts, Kind, MemberId, Namespace, Object,
    Placement, Provenance, Pull, Recovery, Seek, Segment, SliceVerdict, VolumeIndex,
};
use zeroize::Zeroizing;

use self::volume::Volume;

#[derive(Default, Serialize, Deserialize)]
pub struct RarRecord {
    // The volume's name as the window was bound under it, which finds the
    // member the record sits in.
    name: String,
    member: Option<MemberId>,
    // Whether the volume is closed: nothing more arrives for it.
    closed: bool,
    // What the last ruling found.
    said: Option<Said>,
    // How many bytes at the front of the volume the record protects, as
    // the last ruling that read it found.
    protected: Option<u64>,
    // The volume's front as it was bound, kept when it says the headers
    // after it are enciphered: a candidate is held against it, after a
    // restore too.
    front: Vec<u8>,
    // The job's candidates the front did not refute, in the order offered.
    #[serde(skip)]
    passwords: Vec<Zeroizing<String>>,
    // The volume's bytes that passed whole, as the namespace fed them. A
    // restart feeds them again.
    #[serde(skip)]
    proven: Coverage,
}

// What a ruling found of the volume.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Said {
    // Ruled while the volume was open or before it was found.
    Waiting,
    // Nothing it protects is damaged.
    Intact,
    // This many damaged units were rebuilt and written back.
    Rebuilt { units: u64 },
    // This many units are damaged past what the intact parity rebuilds, or
    // unchecked, with this many parity units intact.
    Short { damaged: u64, reachable: u64 },
    // No record could be read from the volume, or the volume was never
    // found.
    Unread,
}

impl Durable for RarRecord {
    // 2: the front a sealed volume's candidates are held against, where
    // only whether it was sealed was kept.
    const VERSION: u16 = 2;
}

impl RarRecord {
    pub fn new() -> Self {
        Self::default()
    }

    fn sealed(&self) -> bool {
        !self.front.is_empty()
    }

    // Whether `password` unseals the volume's first enciphered header, as
    // far as the front reaches.
    fn unseals(&self, password: &str) -> Option<bool> {
        let front = &self.front;
        let mut read = |range: ByteRange| {
            front
                .get(range.start as usize..range.end as usize)
                .map(<[u8]>::to_vec)
        };
        crate::rar_store::rar4_unseals(&mut read, None, password)
    }

    fn proven_whole(&self, length: u64) -> bool {
        self.proven.verified(ByteRange::new(0, length))
    }

    fn rule(&mut self, access: &mut dyn MemberAccess) -> Said {
        let Some(member) = self.member else {
            return if self.closed {
                Said::Unread
            } else {
                Said::Waiting
            };
        };
        if !self.closed {
            return Said::Waiting;
        }
        let Some(length) = access.length(member) else {
            return Said::Unread;
        };
        if self.proven_whole(length) {
            return Said::Intact;
        }
        let mut volume = Volume::new(access, member, length);
        let passwords: Vec<Option<&str>> = match self.sealed() {
            true => self
                .passwords
                .iter()
                .map(|secret| Some(secret.as_str()))
                .collect(),
            false => vec![None],
        };
        let (said, protected) = examine(&mut volume, &passwords);
        self.protected = protected;
        said
    }
}

// The volume checked against its record, read under the first of
// `passwords` that leads to one, and how many bytes the record protects
// when one was read. Under a wrong key an enciphered header is noise, so a
// password that finds no record says only that it is not the one. The
// rebuilt units are written back only when the record vouches for every
// protected byte.
fn examine(volume: &mut Volume<'_>, passwords: &[Option<&str>]) -> (Said, Option<u64>) {
    for password in passwords {
        let assessment = match unrar_rs::repair_from_recovery_record(&mut *volume, *password) {
            Ok(Some(repair)) => {
                let said = said(&repair.assessment, repair.patches.len() as u64);
                found_damaged(volume, &repair.assessment);
                if matches!(said, Said::Rebuilt { .. }) {
                    for patch in &repair.patches {
                        volume.write_repaired(patch.offset, &patch.bytes);
                    }
                }
                return (said, Some(repair.assessment.layout.protected_size));
            }
            Err(RarError::RecoveryRecordInsufficient { .. }) => {
                unrar_rs::assess_recovery_record(&mut *volume, *password)
            }
            Ok(None) | Err(_) => continue,
        };
        if let Ok(Some(assessment)) = assessment {
            found_damaged(volume, &assessment);
            return (said(&assessment, 0), Some(assessment.layout.protected_size));
        }
    }
    (Said::Unread, None)
}

// Whether a volume's front is a RAR3 main header whose headers after it
// are enciphered: the signature, then the header's CRC and type, then its
// flags.
fn sealed(front: &[u8]) -> bool {
    const SIGNATURE: &[u8] = b"Rar!\x1a\x07\x00";
    const ENCRYPTED_HEADERS: u16 = 0x0080;
    front.starts_with(SIGNATURE)
        && front
            .get(10..12)
            .is_some_and(|flags| u16::from_le_bytes([flags[0], flags[1]]) & ENCRYPTED_HEADERS != 0)
}

// What the record's check says, with `rebuilt` units it can rebuild. A
// protected byte no checksum survives for cannot be vouched for, so the
// record rules it short with its damage.
fn said(assessment: &RecoveryRecordAssessment, rebuilt: u64) -> Said {
    let layout = &assessment.layout;
    let unchecked = assessment.unchecked_bytes.div_ceil(layout.unit_size.max(1));
    let damaged = assessment.unrepairable_units.len() as u64 + unchecked;
    if damaged > 0 {
        Said::Short {
            damaged,
            reachable: layout
                .parity_units
                .saturating_sub(assessment.damaged_parity.len() as u64),
        }
    } else if rebuilt > 0 {
        Said::Rebuilt { units: rebuilt }
    } else {
        Said::Intact
    }
}

// Every range the check proved bad and nothing rebuilds: the damaged data
// units of a volume the record cannot rebuild whole, and the record's own
// damaged parity and checksums, which no repair rewrites. A protected byte
// no checksum survives for is not located, so it is not named.
fn found_damaged(volume: &mut Volume<'_>, assessment: &RecoveryRecordAssessment) {
    let layout = &assessment.layout;
    let repairable = assessment.is_repairable() && assessment.unchecked_bytes == 0;
    let units = (!repairable)
        .then_some(&assessment.damaged_units)
        .into_iter()
        .flatten()
        .map(|unit| {
            let start = unit * layout.unit_size;
            ByteRange::new(start, (start + layout.unit_size).min(layout.protected_size))
        });
    let parity = assessment
        .damaged_parity
        .iter()
        .map(|unit| parity_range(layout, *unit));
    let checksums = assessment.damaged_checksums.iter().map(|unit| {
        // A RAR3 record keeps a two-byte checksum per sector at its front.
        ByteRange::at(layout.record_offset + unit * 2, 2)
    });
    for range in units.chain(parity).chain(checksums) {
        volume.found_damaged(range);
    }
}

// Where parity unit `unit` lies in the volume: a RAR5 record is its parity
// units' rows, each the same length; a RAR3 record ends in its parity
// sectors.
fn parity_range(layout: &RecoveryRecordLayout, unit: u64) -> ByteRange {
    let parity = layout.parity_units.max(1);
    match layout.format {
        ArchiveFormat::Rar5 => {
            let row = layout.record_size / parity;
            ByteRange::at(layout.record_offset + unit * row, row)
        }
        _ => {
            let sectors = layout.record_offset + layout.record_size - parity * layout.unit_size;
            ByteRange::at(sectors + unit * layout.unit_size, layout.unit_size)
        }
    }
}

impl Object for RarRecord {
    fn kind(&self) -> Kind {
        Kind::RarRecord
    }

    // The record is read from the volume when the ruling runs.
    fn deliver(
        &mut self,
        _delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _out: &mut Vec<Emit>,
    ) {
    }

    // An enciphered volume asks for the job's candidates once it is closed,
    // which is when a ruling reads it.
    fn volume_closed(&mut self, _volume: VolumeIndex, out: &mut Vec<Emit>) {
        self.closed = true;
        if self.sealed() && self.passwords.is_empty() {
            out.push(Emit::NeedsPassword);
        }
    }

    // A candidate the front refutes is dropped and the next asked for; one
    // it cannot tell yet is held and the next asked for too, so the ruling
    // holds every one that may open the headers; one that unseals them is
    // held and nothing more is asked. What the record said before a
    // candidate it holds came is said again.
    fn offer_password(&mut self, secret: &str, out: &mut Vec<Emit>) {
        if !self.sealed() || self.passwords.iter().any(|held| held.as_str() == secret) {
            return;
        }
        let unseals = self.unseals(secret);
        if unseals != Some(false) {
            self.passwords.push(Zeroizing::new(secret.to_owned()));
            self.said = None;
        }
        if unseals != Some(true) {
            out.push(Emit::NeedsPassword);
        }
    }

    // A lost range of the record is found when the ruling reads it.
    fn volume_lost(&mut self, _volume: VolumeIndex, _range: ByteRange, _out: &mut Vec<Emit>) {}

    fn wants(&self, _out: &mut Vec<Seek>) {}

    fn pulled(&mut self, _seek: Seek, _pull: Pull, _out: &mut Vec<Emit>) {}

    // The record's bytes are to come again, so the volume is open until
    // they do, and what the record said of it is said again then.
    fn unposted(&mut self, _volume: VolumeIndex, _range: ByteRange) {
        self.closed = false;
        self.said = None;
    }

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }

    fn extent(&self, _member: MemberId, _range: ByteRange, _out: &mut Vec<Extent>) {}

    fn envelope(&self, _envelope: u32, _range: ByteRange) -> Option<Bytes> {
        None
    }

    fn claim(
        &mut self,
        _name: &str,
        _first_bytes: &[u8],
        _length: Option<u64>,
    ) -> Option<VolumeIndex> {
        None
    }

    fn bound(&mut self, _volume: VolumeIndex, name: &str, front: &[u8], _length: Option<u64>) {
        self.name = name.to_owned();
        self.front = if sealed(front) {
            front.to_vec()
        } else {
            Vec::new()
        };
    }

    fn recovery(&self) -> Option<&dyn Recovery> {
        Some(self)
    }

    fn recovery_mut(&mut self) -> Option<&mut dyn Recovery> {
        Some(self)
    }

    fn facts(&self) -> Facts {
        Facts::of(Kind::RarRecord, self)
    }
}

impl Recovery for RarRecord {
    // The volume the record sits in, by the name it was bound under; once
    // found it stays the member while the namespace holds it, whatever it
    // is called since.
    fn bind(&mut self, namespace: &mut Namespace, _renamed: &mut Vec<MemberId>) {
        let found = self
            .member
            .filter(|member| namespace.get(*member).is_some())
            .or_else(|| namespace.find_by_name(&self.name).map(|member| member.id));
        if found != self.member {
            self.member = found;
            self.said = None;
            self.protected = None;
            self.proven = Coverage::new();
        }
    }

    // The volume it sits in, unless it could not read itself there.
    fn covers(&self, member: MemberId) -> bool {
        self.member == Some(member) && self.said != Some(Said::Unread)
    }

    fn feed(&mut self, member: MemberId, segments: &[Segment]) {
        if self.member != Some(member) {
            return;
        }
        for segment in segments {
            let provenance = if segment.verified {
                Provenance::Fresh
            } else {
                Provenance::Damaged
            };
            self.proven.write(segment.range, provenance);
        }
    }

    // One slice, over the bytes the record protects.
    fn verdicts(&self, member: MemberId, out: &mut Vec<SliceVerdict>) {
        if !self.covers(member) {
            return;
        }
        out.push(match self.outcome() {
            Some(RepairOutcome::Clean | RepairOutcome::Repaired { .. }) => SliceVerdict::Intact,
            Some(RepairOutcome::Impossible { .. }) => SliceVerdict::Damaged,
            _ => SliceVerdict::Unknown,
        });
    }

    fn wants(&self, _out: &mut Vec<Seek>) {}

    // The bytes before the record's header. Until a ruling read the
    // record, the volume passed whole and there is nothing to prove apart.
    fn proves(&self, _member: MemberId, length: u64) -> ByteRange {
        ByteRange::new(0, self.protected.unwrap_or(length).min(length))
    }

    fn repair(&mut self, access: &mut dyn MemberAccess) -> RepairOutcome {
        let said = self.rule(access);
        self.said = Some(said);
        self.outcome().unwrap_or(RepairOutcome::Waiting)
    }

    // A ruling made while the volume was open waits for it to close, and
    // is due again once it has. One that could not read the record has
    // nothing to rule on, so it is clean on the nothing it covers.
    fn outcome(&self) -> Option<RepairOutcome> {
        Some(match self.said? {
            Said::Waiting => return (!self.closed).then_some(RepairOutcome::Waiting),
            Said::Intact => RepairOutcome::Clean,
            Said::Rebuilt { units } => RepairOutcome::Repaired { slices: units },
            Said::Short { damaged, reachable } => RepairOutcome::Impossible { damaged, reachable },
            Said::Unread => RepairOutcome::Clean,
        })
    }
}
