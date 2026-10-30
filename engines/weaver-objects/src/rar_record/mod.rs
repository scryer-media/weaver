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
// cannot be read says nothing of the volume: it covers nothing and rules
// unread, which is no verdict on it.
//
// The volume's store says where the record lies and hands it the
// candidate that opened the volume's headers, or nothing when they are open
// to anyone; a RAR3 volume with enciphered headers is walked to its record
// under it. The record proves no candidate of its own and asks for none.
// The candidate is never kept: a store whose headers open again after a
// restore hands it again.

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
    ByteRange, Coverage, Delivery, Durable, Emit, Extent, Facts, Handed, Kind, Located, MemberId,
    Namespace, Object, Placement, Provenance, Pull, Recovery, Seek, Segment, SliceVerdict,
    VolumeIndex,
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
    // How the record cuts what it protects, as the last ruling that read it
    // found: the unit size and the parity units it holds.
    units: Option<(u64, u64)>,
    // Ranges of the volume the parent refused: they will never come.
    refused: Vec<ByteRange>,
    // The candidate the store opened the volume's headers on, as it
    // handed it.
    #[serde(skip)]
    password: Option<Zeroizing<String>>,
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
    // 3: no front, where a sealed volume's was kept: the store hands the
    // candidate its headers opened on.
    // 4: the record's units, and the ranges the parent refused.
    const VERSION: u16 = 4;
}

impl RarRecord {
    pub fn new() -> Self {
        Self::default()
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
        let password = self.password.as_ref().map(|secret| secret.as_str());
        let (said, layout) = examine(&mut volume, password);
        self.protected = layout.as_ref().map(|layout| layout.protected_size);
        self.units = layout.map(|layout| (layout.unit_size, layout.parity_units));
        said
    }

    // The record's ruling on what was refused, once a ruling read how it
    // cuts the volume: each protected unit a refused range touches will
    // never come, and the record is impossible once they outnumber its
    // parity units. Before the record is read it says nothing.
    fn refused_short(&self) -> Option<RepairOutcome> {
        let (unit, parity) = self.units.filter(|(unit, _)| *unit > 0)?;
        let protected = ByteRange::new(0, self.protected?);
        let damaged = self
            .refused
            .iter()
            .filter_map(|range| range.intersect(protected))
            .flat_map(|range| range.start / unit..=(range.end - 1) / unit)
            .collect::<std::collections::BTreeSet<u64>>()
            .len() as u64;
        (damaged > parity).then_some(RepairOutcome::Impossible {
            damaged,
            reachable: parity,
        })
    }
}

// The volume checked against its record, read under `password`, and how
// the record cuts the volume when one was read. The rebuilt units are
// written back only when the record vouches for every protected byte.
fn examine(
    volume: &mut Volume<'_>,
    password: Option<&str>,
) -> (Said, Option<RecoveryRecordLayout>) {
    let assessment = match unrar_rs::repair_from_recovery_record(&mut *volume, password) {
        Ok(Some(repair)) => {
            let said = said(&repair.assessment, repair.patches.len() as u64);
            found_damaged(volume, &repair.assessment);
            if matches!(said, Said::Rebuilt { .. }) {
                for patch in &repair.patches {
                    volume.write_repaired(patch.offset, &patch.bytes);
                }
            }
            return (said, Some(repair.assessment.layout.clone()));
        }
        Err(RarError::RecoveryRecordInsufficient { .. }) => {
            unrar_rs::assess_recovery_record(&mut *volume, password)
        }
        Ok(None) | Err(_) => return (Said::Unread, None),
    };
    match assessment {
        Ok(Some(assessment)) => {
            found_damaged(volume, &assessment);
            (said(&assessment, 0), Some(assessment.layout.clone()))
        }
        Ok(None) | Err(_) => (Said::Unread, None),
    }
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

    fn volume_closed(&mut self, _volume: VolumeIndex, _out: &mut Vec<Emit>) {
        self.closed = true;
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

    fn extent(&self, _member: MemberId, _range: ByteRange, _out: &mut Vec<Extent>) -> Located {
        Located::Never
    }

    fn envelope(&self, _envelope: u32, _range: ByteRange) -> Option<Bytes> {
        None
    }

    fn claim(
        &mut self,
        _name: &str,
        _first_bytes: &[u8],
        _length: Option<u64>,
    ) -> Vec<VolumeIndex> {
        Vec::new()
    }

    fn bound(&mut self, _volume: VolumeIndex, name: &str, _front: &[u8], _length: Option<u64>) {
        self.name = name.to_owned();
    }

    fn handed(&mut self, handed: &Handed) {
        self.password = handed.read::<Zeroizing<String>>().cloned();
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
            self.units = None;
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

    fn refused(&mut self, member: MemberId, range: ByteRange) {
        if self.member == Some(member) && !self.refused.contains(&range) {
            self.refused.push(range);
        }
    }

    // The bytes before the record's header. Until a ruling read the
    // record, the volume passed whole and there is nothing to prove apart.
    fn proves(&self, _member: MemberId, length: u64) -> ByteRange {
        ByteRange::new(0, self.protected.unwrap_or(length).min(length))
    }

    fn repair(&mut self, access: &mut dyn MemberAccess, _: weaver_graph::Budget) -> RepairOutcome {
        let said = self.rule(access);
        self.said = Some(said);
        self.outcome().unwrap_or(RepairOutcome::Waiting)
    }

    // A ruling made while the volume was open waits for it to close, and
    // is due again once it has. One that could not read the record checked
    // nothing, so it rules unread.
    fn outcome(&self) -> Option<RepairOutcome> {
        if let Some(short) = self.refused_short() {
            return Some(short);
        }
        Some(match self.said? {
            Said::Waiting => return (!self.closed).then_some(RepairOutcome::Waiting),
            Said::Intact => RepairOutcome::Clean,
            Said::Rebuilt { units } => RepairOutcome::Repaired { slices: units },
            Said::Short { damaged, reachable } => RepairOutcome::Impossible { damaged, reachable },
            Said::Unread => RepairOutcome::Unread,
        })
    }
}
