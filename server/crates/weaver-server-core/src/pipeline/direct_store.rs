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
//! Phase 4 wired the rest in: [`router`] splits every decoded source span
//! across its destinations, [`plan`] admits sets and names those destinations,
//! and [`set`] joins a router to its [`barrier::CoverageBarrier`] so a routed
//! write becomes durable coverage. The [`DirectStoreGate`] still defaults
//! **off**.
//!
//! Phase 5 wave 1 adds three things on top:
//!
//! - **Envelope v2.** Each source volume gets its own sparse envelope file
//!   holding every non-member byte at its true physical offset, replacing phase
//!   4's fixed 64 KiB half-slots in one per-set file. Unbounded by construction,
//!   restart-stable by construction, and the reason `-rr` and `-qo` sets route
//!   at all — the slot ceiling demoted every one of them.
//! - **Multi-member sets.** Admission, routing, the per-member gates,
//!   finalization (in archive order) and demotion all carry several members per
//!   set.
//! - [`provider`] and [`reconstruct`] — the hybrid virtual-volume provider that
//!   answers reads over partials plus envelopes as if the volume existed, and
//!   D8's demotion by byte-exact reconstruction, which materializes a demoting
//!   set's volumes from its own routed bytes instead of refetching them.
//!
//! Phase 5 wave 2 puts the provider to work, which is what lifts wave 1's two
//! remaining narrowings:
//!
//! - **PAR2-bearing jobs route.** [`par2_access`] presents each source volume
//!   to `par2_rs` as a file, so live verification's settle reads and the
//!   authoritative pass both read through [`provider`] instead of against
//!   volume files that do not exist. A direct set therefore **finalizes only
//!   once its job's PAR2 verification has concluded** — before then its
//!   envelopes and partials are the only copy of the volume image, and the
//!   verifier needs them. Wave 2 produced verification **verdicts** only, and a
//!   damaged direct set demoted whole.
//! - **D1's bounded small-member tolerance.** A set whose only ineligible
//!   members are small unencrypted non-solid regular files still routes: their
//!   packed ranges land in the envelope, and at finalization *only* those
//!   member indices are extracted through
//!   `unrar_rs::RarArchive::extract_member_streaming` over the hybrid
//!   provider. Direct `Store` outputs are never re-extracted or overwritten.
//!
//! Phase 6 replaces that last demotion with D8's other transition. [`repair`]
//! materializes **only the damaged volumes** into scratch files, repairs them
//! with every clean volume still read virtually, routes the repaired spans back
//! through the router with replacement semantics — destination bytes overwrite,
//! and so does the CRC composition — re-verifies through the same gates, and
//! deletes the scratch. Clean volumes never materialize, the set stays direct,
//! and no direct output is deleted. Two consequences worth naming:
//!
//! - the set's checkpoint row is **deleted before** anything the row claims is
//!   rewritten, and the next barrier recreates coverage from scratch. That is
//!   deliberately lossy — a crash in that window costs a full redownload of the
//!   set — and it is far simpler than selectively lowering per-volume floors to
//!   expose the repaired ranges (D8);
//! - every refusal along the way falls back to wave 2's whole-set demotion,
//!   which is always correct, under its own metric.
//!
//! Phase 7 is the hardening pass, and it lands four things:
//!
//! - **Windows sparse marking.** [`sparse`] marks every file this subsystem
//!   creates with holes in it — member partials, envelopes, repair and holds
//!   scratch — at creation and before any length is set or byte written. A
//!   marking failure demotes before a long-lived hole exists, so the worst case
//!   is a set on the conventional path rather than one silently paying 1× per
//!   volume in NTFS-allocated zeros.
//! - **A real config surface.** [`DirectStoreSettings`] resolves the gate and
//!   the per-set scratch ceiling from `Config`, with the `WEAVER_*` variables
//!   overriding it in both directions for incident response — plan 135's open
//!   question 1, answered as config *and* env rather than either alone. The
//!   gate still defaults **off**; flipping that default is a release decision.
//! - **Quick Open, dropped.** D2 permitted QO priming behind mandatory
//!   physical-header confirmation and said to delete it if the confirmation
//!   erased the benefit. It does — see the decision recorded at
//!   [`router::DirectSetRouter::try_parse_volume`] — so there is no QO code
//!   here.
//! - **The metric families** the plan enumerates, under `direct_store.*`.
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
pub(crate) mod par2_access;
pub(crate) mod plan;
pub(crate) mod provider;
pub(crate) mod reconstruct;
pub(crate) mod repair;
pub(crate) mod restart;
pub(crate) mod router;
pub(crate) mod set;
pub(crate) mod snapshot;
pub(crate) mod sparse;
pub(crate) mod wiring;

#[cfg(test)]
mod tests;

/// Operator kill switch for direct-store routing and its coverage checkpoint.
///
/// **Overrides the config option**, and that direction is the whole point: the
/// incident this variable exists for is one where the operator cannot reach the
/// settings UI, or where the config write itself is what they distrust. Setting
/// it to an off word forces the gate off no matter what the database says;
/// setting it to an on word forces it on. Leaving it unset defers to config.
///
/// Answering plan 135's open question 1 this way — config *and* env, config as
/// the durable operator surface and env as the override — is phase 7's
/// decision. Phase 2's `WEAVER_LIVE_PAR2` remains env-only; it guards a
/// different feature and moving it is not this phase's business.
pub(crate) const DIRECT_STORE_ENV: &str = "WEAVER_RAR_DIRECT_STORE";

/// Env override for the per-set holds-scratch ceiling, in **bytes**.
///
/// Same precedence rule as [`DIRECT_STORE_ENV`]. An unparseable or absent value
/// defers to config, and config defers to
/// [`router::HOLDS_SCRATCH_CEILING_BYTES`].
pub(crate) const DIRECT_STORE_SCRATCH_CEILING_ENV: &str =
    "WEAVER_RAR_DIRECT_STORE_SCRATCH_CEILING_BYTES";

/// Whether the env override forces direct-store on or off, if it says anything
/// at all. Read once, in the style of `e2e_failpoint`.
pub(crate) fn env_override() -> Option<bool> {
    static OVERRIDE: OnceLock<Option<bool>> = OnceLock::new();
    *OVERRIDE.get_or_init(|| parse_enabled(std::env::var(DIRECT_STORE_ENV).ok().as_deref()))
}

/// The env scratch ceiling, if one is set and parses. Read once.
fn env_scratch_ceiling() -> Option<u64> {
    static CEILING: OnceLock<Option<u64>> = OnceLock::new();
    *CEILING.get_or_init(|| {
        std::env::var(DIRECT_STORE_SCRATCH_CEILING_ENV)
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
    })
}

/// `Some(true)` for the on words, `Some(false)` for the off words, `None` for
/// absent or unrecognised.
///
/// Unrecognised deferring to config rather than to "off" is deliberate: a
/// typo'd override must not silently disable a feature the operator turned on
/// in config, and the direction that surprises least is the one where the
/// variable simply does not apply.
fn parse_enabled(raw: Option<&str>) -> Option<bool> {
    let value = raw?.trim().to_ascii_lowercase();
    match value.as_str() {
        "1" | "true" | "on" | "yes" => Some(true),
        "0" | "false" | "off" | "no" => Some(false),
        _ => None,
    }
}

/// Everything direct-store reads out of configuration, resolved once at
/// pipeline construction (plan 135, phase 7 — Risks, and open question 1).
///
/// Precedence, for both fields: **environment, then config, then default.**
/// Resolving it here rather than at each read point is what keeps the gate
/// consistent for the life of a pipeline — a set admitted under an enabled gate
/// must not find it disabled at finalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DirectStoreSettings {
    pub(crate) gate: DirectStoreGate,
    pub(crate) holds_scratch_ceiling_bytes: u64,
}

impl Default for DirectStoreSettings {
    fn default() -> Self {
        Self {
            gate: DirectStoreGate::Disabled,
            holds_scratch_ceiling_bytes: router::HOLDS_SCRATCH_CEILING_BYTES,
        }
    }
}

impl DirectStoreSettings {
    /// Resolves against a loaded config, with the environment winning.
    pub(crate) fn resolve(config: &crate::settings::Config) -> Self {
        Self::resolve_parts(
            config.direct_store.as_ref().and_then(|cfg| cfg.enabled),
            config
                .direct_store
                .as_ref()
                .and_then(|cfg| cfg.holds_scratch_ceiling_bytes),
            env_override(),
            env_scratch_ceiling(),
        )
    }

    /// The precedence rule itself, with the environment passed in so it is
    /// testable without mutating process state.
    pub(crate) fn resolve_parts(
        config_enabled: Option<bool>,
        config_ceiling: Option<u64>,
        env_enabled: Option<bool>,
        env_ceiling: Option<u64>,
    ) -> Self {
        let enabled = env_enabled.or(config_enabled).unwrap_or(false);
        Self {
            gate: if enabled {
                DirectStoreGate::Enabled
            } else {
                DirectStoreGate::Disabled
            },
            holds_scratch_ceiling_bytes: env_ceiling
                .or(config_ceiling)
                .unwrap_or(router::HOLDS_SCRATCH_CEILING_BYTES),
        }
    }
}

/// Resolved gate value, passed explicitly so callers and tests do not race the
/// process-wide `OnceLock`.
///
/// **Defaults off.** Flipping the default to on is a release decision — the
/// feature has to earn it on real fixtures and a Windows validation run — not a
/// config-default edit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectStoreGate {
    Enabled,
    Disabled,
}

impl DirectStoreGate {
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
    #[allow(dead_code)]
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
