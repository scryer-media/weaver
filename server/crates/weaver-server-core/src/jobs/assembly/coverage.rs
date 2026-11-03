// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The restart checkpoint of one assembled file: what it has durably received.
//
// Durability is coverage, not a prefix. A file holds whatever articles were
// written, wherever they sit, so its checkpoint is the set of those articles —
// coalesced into runs of consecutive ordinals whose decoded bytes abut — and a
// gap anywhere in the file costs a restart exactly the articles in the gap.
// Each run carries the decoded byte extent it occupies on disk, measured from
// the articles' own placements, so a restored run can be judged against the
// file's length, guarded against overlapping writes, and anchored against
// without ever turning an NZB's encoded segment sizes into file offsets.
//
// The checkpoint is the in-memory state the assembly already keeps (which
// ordinals are committed and where each one landed); this is only its durable
// form, one replaced row per file at the restart barrier, never per article.
//
// Framing is `magic | schema version | MessagePack body`, the same shape as
// the direct-store snapshot. Decoding refuses every version it was not written
// for: a row an older or newer binary wrote is refused whole, and the file
// downloads from nothing, rather than trusting coverage under a meaning it may
// not have.

use serde::{Deserialize, Serialize};

// `W`eaver `F`ile `C`o`V`erage.
const COVERAGE_MAGIC: [u8; 4] = *b"WFCV";

// Bump on any change to the body below, or to what a field means.
pub(crate) const COVERAGE_SCHEMA_VERSION: u16 = 1;

const FRAME_HEADER_LEN: usize = 6;

// A run of consecutive ordinals whose decoded bytes are on disk, back to back.
//
// `first_ordinal..first_ordinal + ordinals` are the articles; `start..end` is
// the decoded byte extent they fill, half-open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoverageRun {
    pub first_ordinal: u32,
    pub ordinals: u32,
    pub start: u64,
    pub end: u64,
}

impl CoverageRun {
    pub fn end_ordinal(&self) -> u32 {
        self.first_ordinal.saturating_add(self.ordinals)
    }

    pub fn contains(&self, ordinal: u32) -> bool {
        ordinal >= self.first_ordinal && ordinal < self.end_ordinal()
    }

    pub fn len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// One file's durable coverage: sorted, disjoint runs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileCoverage {
    runs: Vec<CoverageRun>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoverageError {
    Truncated { len: usize },
    BadMagic,
    UnsupportedVersion { found: u16, supported: u16 },
    Malformed(String),
}

impl std::fmt::Display for CoverageError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated { len } => write!(formatter, "file coverage truncated ({len} bytes)"),
            Self::BadMagic => write!(formatter, "file coverage has a bad magic"),
            Self::UnsupportedVersion { found, supported } => write!(
                formatter,
                "file coverage schema version {found} is not supported (this binary reads {supported})"
            ),
            Self::Malformed(detail) => write!(formatter, "file coverage is malformed: {detail}"),
        }
    }
}

impl FileCoverage {
    // Coverage from runs already in ordinal order, coalescing neighbours that
    // continue each other. Runs that overlap an earlier one are dropped: two
    // claims on one ordinal cannot both be the bytes on disk.
    pub fn from_ordered_runs(runs: impl IntoIterator<Item = CoverageRun>) -> Self {
        let mut coalesced: Vec<CoverageRun> = Vec::new();
        for run in runs {
            if run.ordinals == 0 || run.end < run.start {
                continue;
            }
            match coalesced.last_mut() {
                Some(last) if run.first_ordinal < last.end_ordinal() || run.start < last.end => {}
                Some(last) if run.first_ordinal == last.end_ordinal() && run.start == last.end => {
                    last.ordinals = last.ordinals.saturating_add(run.ordinals);
                    last.end = run.end;
                }
                _ => coalesced.push(run),
            }
        }
        Self { runs: coalesced }
    }

    pub fn runs(&self) -> &[CoverageRun] {
        &self.runs
    }

    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    // Decoded bytes the runs hold on disk.
    pub fn covered_bytes(&self) -> u64 {
        self.runs.iter().map(CoverageRun::len).sum()
    }

    pub fn ordinals(&self) -> impl Iterator<Item = u32> + '_ {
        self.runs
            .iter()
            .flat_map(|run| run.first_ordinal..run.end_ordinal())
    }

    #[cfg(test)]
    pub(crate) fn encode(&self) -> Vec<u8> {
        let body: Vec<(u32, u32, u64, u64)> = self
            .runs
            .iter()
            .map(|run| (run.first_ordinal, run.ordinals, run.start, run.end))
            .collect();
        let body = rmp_serde::to_vec(&body).expect("coverage runs always encode");
        let mut blob = Vec::with_capacity(FRAME_HEADER_LEN + body.len());
        blob.extend_from_slice(&COVERAGE_MAGIC);
        blob.extend_from_slice(&COVERAGE_SCHEMA_VERSION.to_le_bytes());
        blob.extend_from_slice(&body);
        blob
    }

    // Every failure is total: a row that is not exactly one well-formed
    // checkpoint of this schema claims nothing.
    pub(crate) fn decode(blob: &[u8]) -> Result<Self, CoverageError> {
        if blob.len() < FRAME_HEADER_LEN {
            return Err(CoverageError::Truncated { len: blob.len() });
        }
        if blob[..4] != COVERAGE_MAGIC {
            return Err(CoverageError::BadMagic);
        }
        let version = u16::from_le_bytes([blob[4], blob[5]]);
        if version != COVERAGE_SCHEMA_VERSION {
            return Err(CoverageError::UnsupportedVersion {
                found: version,
                supported: COVERAGE_SCHEMA_VERSION,
            });
        }
        let body = &blob[FRAME_HEADER_LEN..];
        let mut deserializer = rmp_serde::Deserializer::new(std::io::Cursor::new(body));
        let runs = Vec::<(u32, u32, u64, u64)>::deserialize(&mut deserializer)
            .map_err(|error| CoverageError::Malformed(error.to_string()))?;
        if deserializer.position() != body.len() as u64 {
            return Err(CoverageError::Malformed(
                "trailing bytes after the coverage body".into(),
            ));
        }
        let mut previous: Option<CoverageRun> = None;
        let mut validated = Vec::with_capacity(runs.len());
        for (first_ordinal, ordinals, start, end) in runs {
            let run = CoverageRun {
                first_ordinal,
                ordinals,
                start,
                end,
            };
            if ordinals == 0 || end < start || first_ordinal.checked_add(ordinals).is_none() {
                return Err(CoverageError::Malformed(format!("invalid run {run:?}")));
            }
            if let Some(previous) = previous
                && (run.first_ordinal < previous.end_ordinal() || run.start < previous.end)
            {
                return Err(CoverageError::Malformed(
                    "runs are not sorted and disjoint".into(),
                ));
            }
            previous = Some(run);
            validated.push(run);
        }
        Ok(Self { runs: validated })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(first_ordinal: u32, ordinals: u32, start: u64, end: u64) -> CoverageRun {
        CoverageRun {
            first_ordinal,
            ordinals,
            start,
            end,
        }
    }

    #[test]
    fn coverage_coalesces_only_runs_that_continue_each_other() {
        let coverage = FileCoverage::from_ordered_runs([
            run(1, 1, 10, 20),
            run(2, 2, 20, 40),
            // An ordinal gap and a byte gap each end a run.
            run(5, 1, 50, 60),
            run(6, 1, 61, 70),
        ]);
        assert_eq!(
            coverage.runs(),
            &[run(1, 3, 10, 40), run(5, 1, 50, 60), run(6, 1, 61, 70)]
        );
        assert_eq!(coverage.covered_bytes(), 30 + 10 + 9);
        assert_eq!(coverage.ordinals().collect::<Vec<_>>(), vec![1, 2, 3, 5, 6]);
    }

    #[test]
    fn coverage_round_trips_through_its_row() {
        let coverage = FileCoverage::from_ordered_runs([run(1, 3, 10, 40), run(7, 2, 70, 90)]);
        assert_eq!(FileCoverage::decode(&coverage.encode()).unwrap(), coverage);
    }

    #[test]
    fn coverage_refuses_a_row_of_any_other_schema() {
        let coverage = FileCoverage::from_ordered_runs([run(0, 2, 0, 20)]);
        for version in [0u16, COVERAGE_SCHEMA_VERSION + 1, u16::MAX] {
            let mut blob = coverage.encode();
            blob[4..6].copy_from_slice(&version.to_le_bytes());
            assert_eq!(
                FileCoverage::decode(&blob),
                Err(CoverageError::UnsupportedVersion {
                    found: version,
                    supported: COVERAGE_SCHEMA_VERSION,
                })
            );
        }
        // A row from the prefix-floor schema was a bare integer, not a framed
        // blob, and the bytes of another codec are refused the same way.
        assert_eq!(
            FileCoverage::decode(&10u64.to_le_bytes()),
            Err(CoverageError::BadMagic)
        );
        assert!(FileCoverage::decode(b"WF").is_err());
        let mut trailing = coverage.encode();
        trailing.push(0);
        assert!(matches!(
            FileCoverage::decode(&trailing),
            Err(CoverageError::Malformed(_))
        ));
    }

    #[test]
    fn coverage_refuses_overlapping_or_unsorted_runs() {
        let body = rmp_serde::to_vec(&vec![(4u32, 2u32, 40u64, 60u64), (5, 1, 60, 70)]).unwrap();
        let mut blob = COVERAGE_MAGIC.to_vec();
        blob.extend_from_slice(&COVERAGE_SCHEMA_VERSION.to_le_bytes());
        blob.extend_from_slice(&body);
        assert!(matches!(
            FileCoverage::decode(&blob),
            Err(CoverageError::Malformed(_))
        ));
    }
}
