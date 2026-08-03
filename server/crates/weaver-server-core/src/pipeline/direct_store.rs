//! Direct-store coverage checkpoint — the coarse durability model (plan 135, D6).
//!
//! Weaver's durable state goes through a DB engine, so article-proportional
//! bookkeeping is unacceptable regardless of whether it fsyncs. This subsystem
//! is the alternative: successfully written bytes are tracked as transient
//! coalesced source ranges **in memory only**, and a set-wide barrier —
//! 256 MiB of aggregate dirty bytes, 5 s of dirty age, or an explicit demand —
//! turns that into exactly one replaced snapshot row per archive set. Restart
//! is allowed to lose work, bounded by the barrier interval.
//!
//! Three pieces:
//!
//! - [`snapshot`] — the versioned blob. Schema version, generation counter, the
//!   exact layout-plan digest, destination identities with their claimed
//!   extents, and every per-volume contiguous floor, encoded and decoded in one
//!   operation. Forward-refusing: an unknown schema version is a validation
//!   error, never partial trust.
//! - [`barrier`] — the per-set controller. Ordered drain → sync → persist →
//!   publish, with the sync and persist steps behind small traits so the order
//!   is observable and each step is independently failable.
//! - [`restart`] — the reader. Validate framing, schema, generation, plan
//!   digest and the plan's volume-to-file mapping; confirm every claimed
//!   destination exists and is long enough; derive per-volume refetch floors. No
//!   byte verification and no destination reads beyond fs metadata — the
//!   integrity re-arm belongs to the verifier that must touch those bytes
//!   anyway, not to startup. A probe that cannot run is a refusal, not a pass.
//!
//! # Scope of this phase
//!
//! Phase 4 wires the rest in: [`router`] splits every decoded source span
//! across its destinations, [`plan`] admits sets and names those destinations,
//! and [`set`] joins a router to its [`barrier::CoverageBarrier`] so a routed
//! write becomes durable coverage. The [`DirectStoreGate`] still defaults
//! **off**.
//!
//! # Two checkpoint systems
//!
//! `pipeline::extraction::rar::checkpoint` (`extraction_chunks`) covers the
//! extraction phase. This one covers the download phase. They must never both
//! claim the same member: a direct set is marked extracted at finalization
//! without ever entering the incremental extractor, and
//! [`set::DirectSet::assert_not_extraction_owned`] is where that is asserted.

use std::sync::OnceLock;

pub(crate) mod barrier;
pub(crate) mod plan;
pub(crate) mod provider;
pub(crate) mod reconstruct;
pub(crate) mod restart;
pub(crate) mod router;
pub(crate) mod set;
pub(crate) mod snapshot;
pub(crate) mod wiring;

#[cfg(test)]
mod tests;

/// Operator kill switch for direct-store routing and its coverage checkpoint.
///
/// Env-only is the **phase 3 placeholder**, not a settled decision: plan 135's
/// open question 1 (config vs env, and whether a per-job opt-out is wanted) is
/// still open, and its risk list notes that every other non-test `WEAVER_*` var
/// in the tree is infrastructure rather than operator-facing. Phase 2's
/// `WEAVER_LIVE_PAR2` landed env-only as the same placeholder. If direct-store
/// is meant to be operator-facing it belongs in config, and the switch moves
/// there before phase 7.
pub(crate) const DIRECT_STORE_ENV: &str = "WEAVER_RAR_DIRECT_STORE";

/// Whether direct-store is enabled. Read once, in the style of `e2e_failpoint`.
///
/// **Defaults off.** A coverage checkpoint over physical volumes duplicates
/// what `active_file_progress` already does, so this earns its way on only
/// alongside phase 4's routing.
pub(crate) fn env_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| parse_enabled(std::env::var(DIRECT_STORE_ENV).ok().as_deref()))
}

/// Only the explicit on words enable. Note this is the inverse of
/// `WEAVER_LIVE_PAR2`, which is a kill switch for a shipped feature and so
/// defaults on; this gate guards a feature that is not finished.
fn parse_enabled(raw: Option<&str>) -> bool {
    let Some(value) = raw else {
        return false;
    };
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "on" | "yes"
    )
}

/// Resolved gate value, passed explicitly so callers and tests do not race the
/// process-wide `OnceLock`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectStoreGate {
    Enabled,
    Disabled,
}

impl DirectStoreGate {
    pub(crate) fn from_env() -> Self {
        if env_enabled() {
            Self::Enabled
        } else {
            Self::Disabled
        }
    }

    pub(crate) fn is_enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

/// Sorted, disjoint, coalesced half-open ranges over a `u64` byte space.
///
/// Used for both the transient per-source-volume coverage and a destination's
/// claimed extents. Every offset is `u64` end to end: extent arithmetic crossing
/// 4 GiB must not truncate anywhere.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ByteRanges {
    ranges: Vec<(u64, u64)>,
}

impl ByteRanges {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.ranges.len()
    }

    pub(crate) fn ranges(&self) -> &[(u64, u64)] {
        &self.ranges
    }

    /// Inserts `[start, start + len)` and returns the number of bytes that were
    /// **not** already covered.
    ///
    /// Re-writing a span (a repaired article overwriting wire-damaged bytes)
    /// therefore adds nothing to the aggregate dirty count, which is what makes
    /// the barrier's 256 MiB trigger a count of unique bytes rather than of
    /// write traffic.
    pub(crate) fn insert(&mut self, start: u64, len: u64) -> u64 {
        let Some(end) = start.checked_add(len) else {
            return 0;
        };
        if len == 0 {
            return 0;
        }

        let mut merged = (start, end);
        let mut overlapped = 0u64;
        let mut first = self.ranges.len();
        let mut removed = 0usize;

        for (index, &(range_start, range_end)) in self.ranges.iter().enumerate() {
            if range_end < merged.0 {
                continue;
            }
            if range_start > merged.1 {
                break;
            }
            if first == self.ranges.len() {
                first = index;
            }
            // Overlap is measured against the *incoming* span, not the
            // running merge, so bytes contributed by neighbouring ranges are
            // never counted as already-covered.
            overlapped = overlapped
                .saturating_add(range_end.min(end).saturating_sub(range_start.max(start)));
            merged.0 = merged.0.min(range_start);
            merged.1 = merged.1.max(range_end);
            removed += 1;
        }

        if first == self.ranges.len() {
            // No touching range: find the sorted insertion point.
            first = self
                .ranges
                .partition_point(|&(range_start, _)| range_start < merged.0);
        }
        self.ranges.splice(first..first + removed, [merged]);
        len - overlapped
    }

    /// Extends `floor` through every range that continues it — the volume's
    /// candidate contiguous floor.
    ///
    /// Coverage above a hole is deliberately not counted: floors are
    /// contiguous, so anything sitting above a stalled floor is refetched.
    /// `floor` is the last published floor, whose bytes may already have been
    /// trimmed out of the range list.
    pub(crate) fn contiguous_from(&self, floor: u64) -> u64 {
        let mut current = floor;
        for &(start, end) in &self.ranges {
            if start > current {
                break;
            }
            current = current.max(end);
        }
        current
    }

    /// [`Self::contiguous_from`] with no previously published floor.
    pub(crate) fn contiguous_from_zero(&self) -> u64 {
        self.contiguous_from(0)
    }

    /// Drops coverage entirely below `floor` and clips the range straddling it.
    /// Everything at or above the published floor is retained, because it can
    /// still extend the floor at a later barrier.
    pub(crate) fn trim_below(&mut self, floor: u64) {
        if floor == 0 {
            return;
        }
        self.ranges.retain(|&(_, end)| end > floor);
        if let Some(first) = self.ranges.first_mut()
            && first.0 < floor
        {
            first.0 = floor;
        }
    }

    /// Total covered bytes.
    pub(crate) fn covered(&self) -> u64 {
        self.ranges.iter().fold(0u64, |total, &(start, end)| {
            total.saturating_add(end - start)
        })
    }

    /// Highest covered offset, exclusive. Zero when empty.
    #[allow(dead_code)]
    pub(crate) fn end(&self) -> u64 {
        self.ranges.last().map(|&(_, end)| end).unwrap_or(0)
    }

    /// The sub-ranges of `[start, start + len)` this set does **not** cover, in
    /// order. The router asks this on every arriving article: a duplicate
    /// segment must contribute no bytes at all rather than be re-routed, and a
    /// partially overlapping one must contribute only its new part.
    pub(crate) fn missing(&self, start: u64, len: u64) -> Vec<(u64, u64)> {
        let Some(end) = start.checked_add(len) else {
            return Vec::new();
        };
        if len == 0 {
            return Vec::new();
        }
        let mut gaps = Vec::new();
        let mut cursor = start;
        for &(range_start, range_end) in &self.ranges {
            if range_end <= cursor {
                continue;
            }
            if range_start >= end {
                break;
            }
            if range_start > cursor {
                gaps.push((cursor, range_start.min(end)));
            }
            cursor = cursor.max(range_end);
            if cursor >= end {
                break;
            }
        }
        if cursor < end {
            gaps.push((cursor, end));
        }
        gaps
    }
}
