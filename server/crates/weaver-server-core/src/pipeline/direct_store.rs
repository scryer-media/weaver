//! Direct-store coverage checkpoint — the coarse durability model.
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
//! # What is wired in
//!
//! [`router`] splits every decoded source span across its destinations,
//! [`plan`] admits sets and names those destinations, and [`set`] joins a router
//! to its [`barrier::CoverageBarrier`] so a routed write becomes durable
//! coverage. The [`DirectStoreGate`] still defaults **off**.
//!
//! Three things sit on top of that:
//!
//! - **Envelope v2.** Each source volume gets its own sparse envelope file
//!   holding every non-member byte at its true physical offset, replacing the
//!   fixed 64 KiB half-slots of one per-set file that came before it. Unbounded
//!   by construction, restart-stable by construction, and the reason `-rr` and
//!   `-qo` sets route at all — the slot ceiling demoted every one of them.
//! - **Multi-member sets.** Admission, routing, the per-member gates,
//!   finalization (in archive order) and demotion all carry several members per
//!   set.
//! - [`provider`] and [`reconstruct`] — the hybrid virtual-volume provider that
//!   answers reads over partials plus envelopes as if the volume existed, and
//!   demotion by byte-exact reconstruction, which materializes a demoting set's
//!   volumes from its own routed bytes instead of refetching them.
//!
//! Putting the provider to work is what lifts the two narrowings that remained:
//!
//! - **PAR2-bearing jobs route.** [`par2_access`] presents each source volume
//!   to `par2_rs` as a file, so live verification's settle reads and the
//!   authoritative pass both read through [`provider`] instead of against
//!   volume files that do not exist. A direct set therefore **finalizes only
//!   once its job's PAR2 verification has concluded** — before then its
//!   envelopes and partials are the only copy of the volume image, and the
//!   verifier needs them. At first that produced verification **verdicts** only,
//!   and a damaged direct set demoted whole.
//! - **The member tolerance.** A set whose ineligible members are unencrypted
//!   non-solid regular files still routes, *whatever those members weigh*:
//!   their packed ranges land in the envelope, and at finalization *only* those
//!   member indices are extracted through
//!   `unrar_rs::RarArchive::extract_member_streaming` over the hybrid provider.
//!   Direct `Store` outputs are never re-extracted or overwritten. Member shape
//!   is therefore a **member** verdict and never a set one: a store video
//!   beside a compressed subtitle pack, or a season pack with one compressed
//!   episode, routes everything routable and stream-extracts the rest.
//!
//! Repair-while-direct replaces that last demotion with the other transition.
//! [`repair`] materializes **only the damaged volumes** into scratch files,
//! repairs them with every clean volume still read virtually, routes the
//! repaired spans back through the router with replacement semantics —
//! destination bytes overwrite, and so does the CRC composition — re-verifies
//! through the same gates, and deletes the scratch. Clean volumes never
//! materialize, the set stays direct, and no direct output is deleted. Two
//! consequences worth naming:
//!
//! - the set's checkpoint row is **deleted before** anything the row claims is
//!   rewritten, and the next barrier recreates coverage from scratch. That is
//!   deliberately lossy — a crash in that window costs a full redownload of the
//!   set — and it is far simpler than selectively lowering per-volume floors to
//!   expose the repaired ranges;
//! - every refusal along the way falls back to the whole-set demotion, which is
//!   always correct, under its own metric.
//!
//! The hardening pass lands four more things:
//!
//! - **Windows sparse marking.** [`sparse`] marks every file this subsystem
//!   creates with holes in it — member partials, envelopes, repair and holds
//!   scratch — at creation and before any length is set or byte written. A
//!   marking failure demotes before a long-lived hole exists, so the worst case
//!   is a set on the conventional path rather than one silently paying 1× per
//!   volume in NTFS-allocated zeros.
//! - **A real config surface.** [`DirectStoreSettings`] resolves the gate and
//!   the per-set scratch ceiling from `Config`, with the `WEAVER_*` variables
//!   overriding it in both directions for incident response: config *and* env
//!   rather than either alone. The gate still defaults **off**; flipping that
//!   default is a release decision.
//! - **Quick Open, dropped.** QO priming was permitted behind mandatory
//!   physical-header confirmation, on the understanding that it would be deleted
//!   if the confirmation erased the benefit. It does — see the decision recorded
//!   at [`router::DirectSetRouter::try_parse_volume`] — so there is no QO code
//!   here.
//! - **The metric families** this subsystem publishes, under `direct_store.*`.
//!
//! # Which filesystem the payload is written to
//!
//! Member payload is written into the job's **staging root**,
//! `complete_dir/.weaver-staging/<job_id>` — the same root the incremental RAR
//! extractor writes its members into, and the root completion publishes from by
//! rename. Everything else this subsystem writes — the per-volume envelopes, the
//! holds scratch, the repair scratch, and the volume files a demotion
//! reconstructs — stays in the job's working (intermediate) directory. The split
//! is stated once, on [`plan::DirectSetPlan`], and every derived path follows it.
//!
//! The alternative — payload in the working directory — cost a full byte copy on
//! the ordinary split-volume install (intermediate on local disk, complete on a
//! NAS): the publish rename returned `EXDEV` and completion fell back to
//! `move_path_with_copy_fallback`, so the release was written to the destination
//! volume a second time after being written to the intermediate one. Writing it
//! straight to the staging root means the stream lands on the destination volume
//! **exactly once**, which is the whole point of direct-store.
//!
//! ## The write pattern operators should know about
//!
//! That "once" changes *how* the destination volume is written, not just how
//! often, and the difference is worth knowing before pointing `complete_dir` at
//! a network filesystem:
//!
//! - **Sparse and out of order.** A member partial is created at zero length and
//!   written at whatever logical offsets the wire delivers, so it is a sparse
//!   file with holes that fill in as articles arrive. Conventional extraction
//!   writes its output front to back. NFS and SMB both support this, but a
//!   filesystem that does not implement holes materializes zeros instead — which
//!   is the same exposure the Windows sparse rule already handles, now on the
//!   complete volume rather than the intermediate one.
//! - **Small positioned writes, for the whole download.** Where extraction
//!   writes a member in large sequential runs once the volumes are on disk,
//!   direct-store issues one positioned write per decoded span for the duration
//!   of the download. On a high-latency mount that is many more round trips than
//!   a single sequential pass, against the *same* total bytes.
//! - **fsync per barrier, per destination.** The coverage barrier fsyncs every
//!   destination it touched — members and envelopes — at most every 256 MiB of
//!   dirty bytes or 5 s. The member half of that now lands on the complete
//!   volume.
//!
//! None of it is new I/O; it is the same bytes, moved to the volume they were
//! always destined for and written once instead of twice. But an operator whose
//! complete volume is a slow or latency-bound mount is now seeing the download's
//! write pattern on it rather than the extractor's, and that is the trade the
//! feature makes.
//!
//! # Two checkpoint systems
//!
//! `pipeline::extraction::rar::checkpoint` (`extraction_chunks`) covers the
//! extraction phase. This one covers the download phase. They must never both
//! claim the same member: a direct set is marked extracted at finalization
//! without ever entering the incremental extractor, and
//! [`set::DirectSet::assert_not_extraction_owned`] is where that is asserted.

use std::sync::OnceLock;

pub(crate) mod accountant;
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
pub(crate) mod sniff;
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
/// Config *and* env — config as the durable operator surface and env as the
/// override — is the settled answer.
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

/// Env override for the process-wide resident-holds limit, in **bytes**.
/// Same precedence rule as [`DIRECT_STORE_SCRATCH_CEILING_ENV`].
pub(crate) const DIRECT_STORE_RESIDENT_LIMIT_ENV: &str =
    "WEAVER_RAR_DIRECT_STORE_HOLDS_RESIDENT_LIMIT_BYTES";

/// Env override for the process-wide holds-scratch total, in **bytes**.
/// Same precedence rule as [`DIRECT_STORE_SCRATCH_CEILING_ENV`].
pub(crate) const DIRECT_STORE_SCRATCH_TOTAL_ENV: &str =
    "WEAVER_RAR_DIRECT_STORE_HOLDS_SCRATCH_TOTAL_BYTES";

/// Env override for the free space the working directory's filesystem must
/// keep under holds scratch, in **bytes**. Same precedence rule as
/// [`DIRECT_STORE_SCRATCH_CEILING_ENV`]; zero disables the reserve.
pub(crate) const DIRECT_STORE_DISK_RESERVE_ENV: &str =
    "WEAVER_RAR_DIRECT_STORE_HOLDS_DISK_RESERVE_BYTES";

/// Everything the environment can say about direct-store, read once.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DirectStoreEnv {
    pub(crate) enabled: Option<bool>,
    pub(crate) scratch_ceiling: Option<u64>,
    pub(crate) resident_limit: Option<u64>,
    pub(crate) scratch_total: Option<u64>,
    pub(crate) disk_reserve: Option<u64>,
}

impl DirectStoreEnv {
    /// Whether any override is exported — the tests that read the real
    /// environment skip themselves when one is.
    #[cfg(test)]
    pub(crate) fn any_set(&self) -> bool {
        self.enabled.is_some()
            || self.scratch_ceiling.is_some()
            || self.resident_limit.is_some()
            || self.scratch_total.is_some()
            || self.disk_reserve.is_some()
    }
}

/// The process environment's direct-store overrides. Read once.
pub(crate) fn env() -> DirectStoreEnv {
    static ENV: OnceLock<DirectStoreEnv> = OnceLock::new();
    *ENV.get_or_init(|| DirectStoreEnv {
        enabled: env_override(),
        scratch_ceiling: env_bytes(DIRECT_STORE_SCRATCH_CEILING_ENV),
        resident_limit: env_bytes(DIRECT_STORE_RESIDENT_LIMIT_ENV),
        scratch_total: env_bytes(DIRECT_STORE_SCRATCH_TOTAL_ENV),
        disk_reserve: env_bytes(DIRECT_STORE_DISK_RESERVE_ENV),
    })
}

/// A byte-valued override, if it is set and parses.
fn env_bytes(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
}

/// What the host can tell the shared-limit defaults: the memory the process
/// may use, and the size of the filesystem under the working directory.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct HostFacts {
    pub(crate) total_memory_bytes: Option<u64>,
    pub(crate) working_fs_total_bytes: Option<u64>,
}

impl HostFacts {
    /// A host that says nothing: every derived default falls to its fallback.
    pub(crate) const UNKNOWN: Self = Self {
        total_memory_bytes: None,
        working_fs_total_bytes: None,
    };

    /// Probes the real host. `working_dir` may not exist yet at startup, so
    /// the nearest existing ancestor answers for its filesystem.
    pub(crate) fn probe(working_dir: &std::path::Path) -> Self {
        Self {
            total_memory_bytes: crate::runtime::system_probe::detect_total_memory_bytes(),
            working_fs_total_bytes: working_dir
                .ancestors()
                .find_map(crate::operations::disk::disk_space)
                .map(|space| space.total_bytes),
        }
    }
}

/// The shared resident-holds limit a host earns: a sixteenth of the memory
/// the process may use, between one set's budget and sixteen of them
/// (64 MiB to 1 GiB), and four sets' worth when the host cannot say.
pub(crate) fn default_resident_limit_bytes(total_memory_bytes: Option<u64>) -> u64 {
    const FLOOR: u64 = router::DEFAULT_HOLDS_BUDGET_BYTES;
    total_memory_bytes.map_or(4 * FLOOR, |total| (total / 16).clamp(FLOOR, 16 * FLOOR))
}

/// How many sets' worth of scratch the process-wide total allows by default.
const HOLDS_SCRATCH_TOTAL_SETS: u64 = 4;

/// `Some(true)` for the on words, `Some(false)` for the off words, `None` for
/// absent or unrecognised.
///
/// Unrecognised deferring to config rather than to "off" is deliberate: a
/// typo'd override must not silently disable a feature the operator turned on
/// in config, and the direction that surprises least is the one where the
/// variable simply does not apply.
pub(in crate::pipeline) fn parse_enabled(raw: Option<&str>) -> Option<bool> {
    let value = raw?.trim().to_ascii_lowercase();
    match value.as_str() {
        "1" | "true" | "on" | "yes" => Some(true),
        "0" | "false" | "off" | "no" => Some(false),
        _ => None,
    }
}

/// Everything direct-store reads out of configuration, resolved once at
/// pipeline construction.
///
/// Precedence, for both fields: **environment, then config, then default.**
/// Resolving it here rather than at each read point is what keeps the gate
/// consistent for the life of a pipeline — a set admitted under an enabled gate
/// must not find it disabled at finalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DirectStoreSettings {
    pub(crate) gate: DirectStoreGate,
    /// Per set. See [`router::HOLDS_SCRATCH_CEILING_BYTES`].
    pub(crate) holds_scratch_ceiling_bytes: u64,
    /// Process-wide. See [`accountant::HoldsLimits::resident_bytes`].
    pub(crate) holds_resident_limit_bytes: u64,
    /// Process-wide. See [`accountant::HoldsLimits::scratch_bytes`].
    pub(crate) holds_scratch_total_bytes: u64,
    /// See [`accountant::HoldsLimits::disk_reserve_bytes`].
    pub(crate) holds_disk_reserve_bytes: u64,
}

impl Default for DirectStoreSettings {
    /// Nothing configured, on a host that says nothing.
    fn default() -> Self {
        Self::resolve_parts(None, DirectStoreEnv::default(), HostFacts::UNKNOWN)
    }
}

impl DirectStoreSettings {
    /// Resolves against a loaded config, with the environment winning, on a
    /// host that says nothing: the derived defaults fall to their fallbacks.
    #[cfg(test)]
    pub(crate) fn resolve(config: &crate::settings::Config) -> Self {
        Self::resolve_on(config, HostFacts::UNKNOWN)
    }

    /// [`Self::resolve`] on a probed host, so the shared limits nothing
    /// configured follow what the box has.
    pub(crate) fn resolve_on(config: &crate::settings::Config, host: HostFacts) -> Self {
        Self::resolve_parts(config.direct_store.as_ref(), env(), host)
    }

    /// The precedence rule itself — **environment, then config, then a
    /// default the host may inform** — with the environment and the host
    /// passed in so it is testable without mutating process state.
    pub(crate) fn resolve_parts(
        config: Option<&crate::settings::DirectStoreOverrides>,
        env: DirectStoreEnv,
        host: HostFacts,
    ) -> Self {
        let pick = |from_env: Option<u64>, from_config: Option<u64>, default: u64| {
            from_env.or(from_config).unwrap_or(default)
        };
        let enabled = env
            .enabled
            .or(config.and_then(|cfg| cfg.enabled))
            .unwrap_or(true);
        let holds_scratch_ceiling_bytes = pick(
            env.scratch_ceiling,
            config.and_then(|cfg| cfg.holds_scratch_ceiling_bytes),
            router::HOLDS_SCRATCH_CEILING_BYTES,
        );
        Self {
            gate: if enabled {
                DirectStoreGate::Enabled
            } else {
                DirectStoreGate::Disabled
            },
            holds_scratch_ceiling_bytes,
            holds_resident_limit_bytes: pick(
                env.resident_limit,
                config.and_then(|cfg| cfg.holds_resident_limit_bytes),
                default_resident_limit_bytes(host.total_memory_bytes),
            ),
            holds_scratch_total_bytes: pick(
                env.scratch_total,
                config.and_then(|cfg| cfg.holds_scratch_total_bytes),
                holds_scratch_ceiling_bytes.saturating_mul(HOLDS_SCRATCH_TOTAL_SETS),
            ),
            holds_disk_reserve_bytes: pick(
                env.disk_reserve,
                config.and_then(|cfg| cfg.holds_disk_reserve_bytes),
                crate::pipeline::extraction::safety::default_disk_reserve_bytes(
                    host.working_fs_total_bytes,
                ),
            ),
        }
    }

    /// The process-wide ceilings, for the accountant.
    pub(crate) fn holds_limits(&self) -> accountant::HoldsLimits {
        accountant::HoldsLimits {
            resident_bytes: self.holds_resident_limit_bytes,
            scratch_bytes: self.holds_scratch_total_bytes,
            disk_reserve_bytes: self.holds_disk_reserve_bytes,
        }
    }
}

/// Resolved gate value, passed explicitly so callers and tests do not race the
/// process-wide `OnceLock`.
///
/// **Defaults on.** Operators can disable it through configuration or the
/// environment kill switch.
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
