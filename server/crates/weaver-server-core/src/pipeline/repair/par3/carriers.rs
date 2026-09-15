//! What each carrier's scan found.
//!
//! The scanner authenticates every packet before it hands one over, so a
//! damaged packet is never delivered: it is skipped, and the bytes it occupied
//! simply produce nothing. That is the only damage signal available here, so
//! this module reconstructs it from the coordinates of the packets that *did*
//! authenticate — a run of carrier bytes that were readable and yielded no
//! authenticated packet is damage, and a run that was never published is a
//! hole. Both are plain integers on the carrier's own record, folded into the
//! process counters once, at the work unit's handback.

use par3_rs::ingest::{IngestedPacket, PayloadKind};
use par3_rs::source::SourceId;

/// The packet families a carrier summary counts separately. A set needs an
/// authenticated Start, matrix and Root before any of its files can be
/// planned, so those three are named rather than lumped together.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::pipeline) enum Par3PacketKind {
    Start,
    Matrix,
    Root,
    File,
    Directory,
    Data,
    Recovery,
    Other,
}

impl Par3PacketKind {
    pub const COUNT: usize = 8;

    /// The families a set cannot be planned without, in the order a reader
    /// should hear about them.
    pub const VITAL: [Self; 3] = [Self::Start, Self::Root, Self::Matrix];

    pub const fn index(self) -> usize {
        match self {
            Self::Start => 0,
            Self::Matrix => 1,
            Self::Root => 2,
            Self::File => 3,
            Self::Directory => 4,
            Self::Data => 5,
            Self::Recovery => 6,
            Self::Other => 7,
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Matrix => "matrix",
            Self::Root => "root",
            Self::File => "file",
            Self::Directory => "directory",
            Self::Data => "data",
            Self::Recovery => "recovery",
            Self::Other => "other",
        }
    }

    /// Which family an authenticated packet belongs to.
    ///
    /// The family comes from the declared type rather than from the parsed
    /// body: a packet whose body needs the set's Start packet before it can be
    /// parsed is retained verbatim, and it is still a packet of its own
    /// family.
    pub fn of(packet: &IngestedPacket) -> Self {
        use par3_rs::packet::PacketType;
        if let Some(metadata) = packet.metadata() {
            return match metadata.body().packet_type() {
                PacketType::Start => Self::Start,
                PacketType::CauchyMatrix
                | PacketType::SparseRandomMatrix
                | PacketType::ExplicitMatrix
                | PacketType::FftMatrix => Self::Matrix,
                PacketType::Root => Self::Root,
                PacketType::File => Self::File,
                PacketType::Directory => Self::Directory,
                PacketType::Data | PacketType::ExternalData => Self::Data,
                PacketType::RecoveryData | PacketType::RecoveryExternalData => Self::Recovery,
                _ => Self::Other,
            };
        }
        match packet.payload().map(|payload| payload.kind()) {
            Some(PayloadKind::Data { .. }) => Self::Data,
            Some(PayloadKind::Recovery { .. }) => Self::Recovery,
            None => Self::Other,
        }
    }
}

/// One carrier's scan record. Every field is a plain integer advanced by the
/// scan loop; nothing here touches an atomic or allocates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) struct CarrierScan {
    /// Carrier bytes the scanner has walked past, authenticated or not.
    pub bytes_scanned: u64,
    /// Authenticated packets, by family.
    pub authenticated: [u64; Par3PacketKind::COUNT],
    /// Authenticated packets a set refused to admit.
    pub rejected: u64,
    /// Start of the first readable run that produced no authenticated packet.
    pub first_damage_offset: Option<u64>,
    /// Total readable bytes that produced no authenticated packet.
    pub damaged_bytes: u64,
    /// Times the scanner had to step over a range that had not arrived.
    pub unavailable_ranges: u64,
    /// Where the next authenticated packet is expected to begin. Everything
    /// between here and the packet that actually arrives is damage.
    next_offset: u64,
}

impl Default for CarrierScan {
    fn default() -> Self {
        Self {
            bytes_scanned: 0,
            authenticated: [0; Par3PacketKind::COUNT],
            rejected: 0,
            first_damage_offset: None,
            damaged_bytes: 0,
            unavailable_ranges: 0,
            next_offset: 0,
        }
    }
}

impl CarrierScan {
    /// Credit one authenticated packet, together with whatever readable span
    /// preceded it and produced nothing.
    pub fn note_packet(&mut self, kind: Par3PacketKind, offset: u64, length: u64) {
        self.note_gap(offset);
        self.authenticated[kind.index()] += 1;
        self.advance(offset.saturating_add(length));
    }

    /// Credit one authenticated packet a set would not admit. The bytes are
    /// accounted for either way: the packet was readable and well formed.
    pub fn note_rejected(&mut self, offset: u64, length: u64) {
        self.note_gap(offset);
        self.rejected += 1;
        self.advance(offset.saturating_add(length));
    }

    /// Step over a range that has not arrived. Absent bytes are a hole, never
    /// damage, so the damage cursor moves with the scanner.
    pub fn note_unavailable(&mut self, resume: u64) {
        self.unavailable_ranges += 1;
        self.advance(resume);
    }

    /// Close the carrier at its published length, so a damaged tail counts.
    pub fn note_end(&mut self, len: u64) {
        self.note_gap(len);
        self.advance(len);
    }

    /// Whether this carrier reported anything a reader should hear about.
    pub fn is_damaged(&self) -> bool {
        self.damaged_bytes != 0 || self.rejected != 0
    }

    fn note_gap(&mut self, offset: u64) {
        let Some(gap) = offset.checked_sub(self.next_offset).filter(|gap| *gap != 0) else {
            return;
        };
        self.damaged_bytes = self.damaged_bytes.saturating_add(gap);
        self.first_damage_offset.get_or_insert(self.next_offset);
    }

    fn advance(&mut self, offset: u64) {
        if offset > self.next_offset {
            self.bytes_scanned = self.bytes_scanned.saturating_add(offset - self.next_offset);
            self.next_offset = offset;
        }
    }
}

/// One carrier's line in a damage summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) struct CarrierDamage {
    pub source: SourceId,
    pub first_damage_offset: u64,
    pub damaged_bytes: u64,
    pub rejected: u64,
    pub unavailable_ranges: u64,
}

impl std::fmt::Display for CarrierDamage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "carrier {} damaged {} byte(s) from offset {}",
            self.source.0, self.damaged_bytes, self.first_damage_offset
        )?;
        if self.rejected != 0 {
            write!(f, ", {} packet(s) refused", self.rejected)?;
        }
        if self.unavailable_ranges != 0 {
            write!(f, ", {} range(s) unread", self.unavailable_ranges)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scanned(spans: &[(u64, u64)], len: u64) -> CarrierScan {
        let mut scan = CarrierScan::default();
        for &(offset, length) in spans {
            scan.note_gap(offset);
            scan.authenticated[Par3PacketKind::Root.index()] += 1;
            scan.advance(offset + length);
        }
        scan.note_end(len);
        scan
    }

    /// A carrier whose packets abut leaves nothing unexplained.
    #[test]
    fn a_whole_carrier_reports_no_damage() {
        let scan = scanned(&[(0, 100), (100, 50), (150, 50)], 200);
        assert_eq!(scan.damaged_bytes, 0);
        assert_eq!(scan.first_damage_offset, None);
        assert_eq!(scan.bytes_scanned, 200);
        assert!(!scan.is_damaged());
    }

    /// A packet the scanner could not authenticate leaves its bytes behind,
    /// and the summary names where the run started, not where it ended.
    #[test]
    fn a_skipped_packet_becomes_a_damaged_run_at_its_own_offset() {
        let scan = scanned(&[(0, 100), (250, 50)], 300);
        assert_eq!(scan.first_damage_offset, Some(100));
        assert_eq!(scan.damaged_bytes, 150);
        assert!(scan.is_damaged());
    }

    /// Bytes that never arrived are a hole. Stepping over one must not be
    /// reported as damage, and must not shift the damage that follows it.
    #[test]
    fn an_unavailable_range_is_a_hole_and_not_damage() {
        let mut scan = CarrierScan::default();
        scan.advance(100);
        scan.note_unavailable(400);
        scan.note_gap(500);
        scan.advance(600);
        scan.note_end(600);
        assert_eq!(scan.unavailable_ranges, 1);
        assert_eq!(scan.first_damage_offset, Some(400));
        assert_eq!(scan.damaged_bytes, 100);
    }

    /// A carrier that stops producing packets before its end is damaged to
    /// its last byte, not silently accepted as finished.
    #[test]
    fn a_damaged_tail_is_counted_when_the_carrier_closes() {
        let scan = scanned(&[(0, 100)], 1_000);
        assert_eq!(scan.first_damage_offset, Some(100));
        assert_eq!(scan.damaged_bytes, 900);
    }

    /// Every family has its own slot and its own stable label.
    #[test]
    fn the_packet_families_are_distinct_and_named() {
        use std::collections::BTreeSet;
        let all = [
            Par3PacketKind::Start,
            Par3PacketKind::Matrix,
            Par3PacketKind::Root,
            Par3PacketKind::File,
            Par3PacketKind::Directory,
            Par3PacketKind::Data,
            Par3PacketKind::Recovery,
            Par3PacketKind::Other,
        ];
        assert_eq!(all.len(), Par3PacketKind::COUNT);
        assert_eq!(
            all.iter().map(|kind| kind.index()).collect::<BTreeSet<_>>(),
            (0..Par3PacketKind::COUNT).collect::<BTreeSet<_>>()
        );
        assert_eq!(
            all.iter()
                .map(|kind| kind.label())
                .collect::<BTreeSet<_>>()
                .len(),
            Par3PacketKind::COUNT
        );
        for kind in Par3PacketKind::VITAL {
            assert!(all.contains(&kind));
        }
    }
}
