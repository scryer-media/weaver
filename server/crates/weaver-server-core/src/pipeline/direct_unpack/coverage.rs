//! How much of a 7z set is on disk, and the parking spot for a reader that has
//! run ahead of it.
//!
//! # Why a watermark is enough
//!
//! Direct unpack reads part files that are still being written. That is only
//! safe because of what the download path already guarantees about them: a
//! file's buffered writes drain contiguously from zero, and a segment's bytes
//! are CRC-verified before they are ever committed. So a part file's flushed
//! prefix is exactly its verified prefix — there is no window in which a byte
//! below the watermark is present but wrong, and no hole below it waiting to be
//! backfilled. One integer per part therefore describes everything a reader is
//! allowed to touch.
//!
//! # Two sides, two costs
//!
//! The writer side is called from the download flush path and must stay cheap:
//! take the lock, move an integer, wake anyone waiting. The reader side runs on
//! a blocking extraction thread and is allowed to block, which is why this is a
//! [`Mutex`] + [`Condvar`] rather than anything async — the thread parked here
//! is a `spawn_blocking` thread, not a runtime worker.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};

/// Per-part progress, as the writer side has reported it so far.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PartProgress {
    /// Bytes committed contiguously from the start of this part.
    pub watermark: u64,
    /// The part's exact length, once the set's layout has settled. `None` while
    /// the part is still being written and its final size is not yet known.
    pub len: Option<u64>,
    /// Whether the part is finished; no further bytes will arrive.
    pub complete: bool,
    /// The furthest byte offset actually handed to the decoder.
    ///
    /// Not the same as the watermark: the watermark is what *could* be read,
    /// this is what *was*. Repair only has to care about bytes the decoder has
    /// already folded into its output — everything above this line it can
    /// rewrite freely, because the chase has not looked at it yet.
    pub consumed_high_water: u64,
    /// A ceiling on the servable watermark, set when the recovery data says a
    /// byte range of this part is damaged.
    ///
    /// `None` until damage is known, which is why this costs a clean job
    /// nothing: no damage, no cap, and the frontier stays the download's own.
    pub damage_cap: Option<u64>,
    /// How far the recovery set has positively vouched for this part, as a
    /// contiguous run of Intact blocks from its start.
    ///
    /// Only consulted once the set is gated. `None` means the grid has claimed
    /// nothing here, which under gating serves nothing new — unclaimed is not
    /// the same as intact, and a set already known to carry damage does not get
    /// the benefit of the doubt about the parts nobody has checked.
    pub vouched_prefix: Option<u64>,
    /// How many times repair has replaced this part's file on disk.
    ///
    /// Repair does not write into the damaged file: it moves that file aside
    /// and installs the repaired one under the same name, so the path now
    /// leads to a different inode. A reader holding the handle it opened
    /// before the repair would keep reading the file that was moved aside —
    /// the damaged bytes, right up to the moment they are deleted as a
    /// leftover. This counter is how it learns to open the path again.
    pub rewritten: u64,
}

/// Where an archive offset sits relative to one part.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionInPart {
    /// Inside this part, with `available` committed bytes readable from it.
    Inside {
        /// Bytes readable from the offset before the frontier.
        available: u64,
        /// The part's [`PartProgress::rewritten`] count at the time of the
        /// answer, so the reader can tell whether the file it has open is still
        /// the one at the path.
        rewritten: u64,
    },
    /// Past this part's end; the next part starts `len` bytes in.
    Beyond {
        /// This part's final length.
        len: u64,
    },
}

impl PartProgress {
    /// How far a reader may go: the committed frontier, held back to just
    /// below any known damage.
    fn servable(&self, gated: bool) -> u64 {
        let mut frontier = self.watermark;
        if let Some(cap) = self.damage_cap {
            frontier = frontier.min(cap);
        }
        if gated {
            // Once the set is known to carry damage, only what the recovery set
            // has actually vouched for may be served. A part it has not claimed
            // contributes nothing, so the chase parks there rather than racing
            // ahead into bytes a later verdict might condemn.
            frontier = frontier.min(self.vouched_prefix.unwrap_or(0));
        }
        frontier
    }

    /// Whether something other than the download is holding this part's
    /// frontier back, so a complete part is not yet at its end.
    fn held_back(&self, gated: bool) -> bool {
        if self.damage_cap.is_some() {
            return true;
        }
        if !gated {
            return false;
        }
        let settled = self.len.unwrap_or(self.watermark);
        self.servable(true) < settled
    }
}

#[derive(Debug)]
struct CoverageState {
    /// Set the moment the first Damaged verdict lands anywhere in this set.
    ///
    /// Before that the frontier is exactly what it always was — a clean job
    /// never reads this field's consequences, which is the whole point. After
    /// it, unverified bytes stop being served, so damage in a part the chase
    /// has not reached yet is parked rather than raced.
    gated: bool,
    parts: Vec<PartProgress>,
    /// Authoritative archive length, derived from the signature header.
    total_len: Option<u64>,
    aborted: Option<String>,
}

/// Shared download-progress view for one 7z set.
///
/// Cloneable only behind an [`Arc`](std::sync::Arc): the writer half lives on
/// the download path and the reader half on an extraction thread, and both must
/// see the same state.
#[derive(Debug)]
pub struct SetCoverage {
    state: Mutex<CoverageState>,
    advanced: Condvar,
    /// How many times a reader has actually parked. Reads that are served
    /// immediately do not count, so a test can tell parking apart from
    /// spinning without timing anything.
    parks: AtomicU64,
}

impl SetCoverage {
    /// Create coverage for a set of `part_count` ordered parts.
    pub fn new(part_count: usize) -> Self {
        Self {
            state: Mutex::new(CoverageState {
                gated: false,
                parts: vec![PartProgress::default(); part_count],
                total_len: None,
                aborted: None,
            }),
            advanced: Condvar::new(),
            parks: AtomicU64::new(0),
        }
    }

    /// Number of parts this set was created with.
    pub fn part_count(&self) -> usize {
        self.lock().parts.len()
    }

    /// How many times a reader has parked waiting for this coverage.
    pub fn park_count(&self) -> u64 {
        self.parks.load(Ordering::Relaxed)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CoverageState> {
        // A poisoned lock means a reader or writer panicked mid-update. The
        // state behind it is plain integers that are still individually
        // consistent, and refusing to read them would turn one panic into a
        // wedged extraction, so the guard is taken either way.
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    // ---- writer side -----------------------------------------------------

    /// Record the archive's authoritative total length.
    ///
    /// Set once, from the signature header. Re-declaring the same value is a
    /// no-op; declaring a *different* one means two sources disagree about the
    /// archive, which is not something a reader can paper over — it aborts.
    pub fn set_total_len(&self, len: u64) {
        let mut state = self.lock();
        match state.total_len {
            Some(existing) if existing == len => return,
            Some(existing) => {
                let reason =
                    format!("archive total length changed from {existing} to {len} mid-flight");
                Self::abort_locked(&mut state, reason);
            }
            None => state.total_len = Some(len),
        }
        self.advanced.notify_all();
    }

    /// Record a part's exact length.
    ///
    /// Set once per part, like the archive total. Re-declaring the same length
    /// is a no-op; declaring a different one aborts, because a reader may
    /// already have mapped offsets against the first value and served bytes on
    /// the strength of it — a shrunk length would retract those, and a grown
    /// one would have placed every later part at the wrong offset.
    pub fn note_part_len(&self, index: usize, len: u64) {
        let mut state = self.lock();
        let Some(part) = state.parts.get_mut(index) else {
            debug_assert!(false, "part index {index} out of range");
            return;
        };
        match part.len {
            Some(existing) if existing == len => return,
            Some(existing) => {
                let reason =
                    format!("part {index} length changed from {existing} to {len} mid-flight");
                Self::abort_locked(&mut state, reason);
            }
            None => part.len = Some(len),
        }
        drop(state);
        self.advanced.notify_all();
    }

    /// Advance a part's committed watermark.
    ///
    /// Monotonic: a lower value than the one already recorded is ignored, so an
    /// out-of-order flush report cannot retract bytes a reader may already have
    /// served.
    ///
    /// A watermark past a declared length is a contradiction in the coverage,
    /// not a programming error to assert on: one of the two facts was wrong, and
    /// which one is not knowable from here. It aborts the set — one demoted
    /// chase and a conventional extraction — in *every* build profile. It used
    /// to be a `debug_assert!`, which under a debug build panicked the pipeline
    /// task and took the whole job pass down with it, and under a release build
    /// silently served bytes past a boundary a reader had already mapped later
    /// parts against.
    pub fn advance_watermark(&self, index: usize, watermark: u64) {
        let mut state = self.lock();
        let Some(part) = state.parts.get_mut(index) else {
            debug_assert!(false, "part index {index} out of range");
            return;
        };
        if watermark <= part.watermark {
            return;
        }
        if let Some(len) = part.len
            && watermark > len
        {
            let reason =
                format!("part {index} committed {watermark} bytes past its declared length {len}");
            Self::abort_locked(&mut state, reason);
            drop(state);
            self.advanced.notify_all();
            return;
        }
        part.watermark = watermark;
        drop(state);
        self.advanced.notify_all();
    }

    /// Mark a part finished: no further bytes will arrive for it.
    ///
    /// A complete part's length *is* its watermark — the verified prefix is by
    /// then the whole file — so this fills in a length that was never declared
    /// explicitly.
    pub fn mark_part_complete(&self, index: usize) {
        let mut state = self.lock();
        let Some(part) = state.parts.get_mut(index) else {
            debug_assert!(false, "part index {index} out of range");
            return;
        };
        if part.complete {
            return;
        }
        part.complete = true;
        match part.len {
            Some(len) => part.watermark = part.watermark.max(len),
            None => part.len = Some(part.watermark),
        }

        Self::reconcile_total_when_settled(&mut state);

        drop(state);
        self.advanced.notify_all();
    }

    /// Record how far the decoder has actually read into a part.
    ///
    /// Monotone. Called from the chase's own thread after each read, never from
    /// the download path, so the lock it takes is uncontended in the common
    /// case and absent entirely when nothing is being chased.
    pub fn note_consumed(&self, index: usize, offset: u64) {
        let mut state = self.lock();
        let Some(part) = state.parts.get_mut(index) else {
            debug_assert!(false, "part index {index} out of range");
            return;
        };
        if offset > part.consumed_high_water {
            part.consumed_high_water = offset;
        }
    }

    /// Whether a part has already been marked finished.
    ///
    /// The settle passes ask before touching a part: one that settled through
    /// its own completion commit needs nothing added and, in the strict pass, is
    /// not something to complain about.
    pub fn part_is_complete(&self, index: usize) -> bool {
        self.lock()
            .parts
            .get(index)
            .map(|part| part.complete)
            .unwrap_or(false)
    }

    /// The furthest byte the decoder has read from a part.
    pub fn consumed_high_water(&self, index: usize) -> u64 {
        self.lock()
            .parts
            .get(index)
            .map(|part| part.consumed_high_water)
            .unwrap_or(0)
    }

    /// Whether the set is serving only vouched bytes.
    ///
    /// Flipped by the first Damaged verdict anywhere in the set, inside
    /// [`Self::cap_at_damage`]. It is one way — a set known to carry damage
    /// does not become trustworthy again because a later block happened to
    /// check out — until repair rewrites it and
    /// [`Self::release_after_repair`] lifts it.
    pub fn is_gated(&self) -> bool {
        self.lock().gated
    }

    /// Record how far the recovery set vouches for a part.
    ///
    /// Monotone upward: claims accumulate as articles land, and a prefix that
    /// has been proved does not become unproved.
    pub fn note_vouched_prefix(&self, index: usize, prefix: u64) {
        let mut state = self.lock();
        let Some(part) = state.parts.get_mut(index) else {
            debug_assert!(false, "part index {index} out of range");
            return;
        };
        if part.vouched_prefix.is_some_and(|current| prefix <= current) {
            return;
        }
        part.vouched_prefix = Some(prefix);
        drop(state);
        self.advanced.notify_all();
    }

    /// Hold a part's servable frontier below a byte the recovery data says is
    /// damaged.
    ///
    /// Monotone downward: once the frontier is known to be unsafe past a point
    /// it never moves back up, because a later, higher damage report does not
    /// make an earlier, lower one wrong. Clean sets never call this, which is
    /// why they keep exactly the overlap they had before repair-resume existed.
    pub fn cap_at_damage(&self, index: usize, offset: u64) {
        let mut state = self.lock();
        let Some(part) = state.parts.get_mut(index) else {
            debug_assert!(false, "part index {index} out of range");
            return;
        };
        let capped = match part.damage_cap {
            Some(existing) => existing.min(offset),
            None => offset,
        };
        if part.damage_cap == Some(capped) {
            return;
        }
        part.damage_cap = Some(capped);
        // The first damage anywhere in the set is what flips it gated: from
        // here nothing unvouched is served, so damage in a part the chase has
        // not reached is parked instead of raced.
        state.gated = true;
        drop(state);
        // A cap can only ever *lower* the frontier, so nobody is unblocked by
        // it — but a reader parked inside the newly-capped range has to be
        // woken to re-evaluate rather than left waiting on a watermark that
        // will never reach it.
        self.advanced.notify_all();
    }

    /// Whether any part is being held back by known damage.
    pub fn has_damage_cap(&self) -> bool {
        self.lock()
            .parts
            .iter()
            .any(|part| part.damage_cap.is_some())
    }

    /// Release a part after repair has rewritten it: drop the damage cap, fix
    /// the final length, and open the frontier to the whole file.
    ///
    /// The bytes on disk are now the repaired ones, and the chase is parked
    /// below the damage it was warned about — so from here it reads at disk
    /// speed over data the recovery set has vouched for.
    pub fn release_after_repair(&self, index: usize, len: u64) {
        let mut state = self.lock();
        let Some(part) = state.parts.get_mut(index) else {
            debug_assert!(false, "part index {index} out of range");
            return;
        };

        // Repair writes the file the recovery set describes, which is not
        // always the file that was on disk: an over-long part is truncated to
        // its described length. If that cut lands below what the decoder has
        // already read, the vouch this release rests on was about bytes that no
        // longer exist, and resuming would splice a repaired tail onto a stale
        // head. There is no recovering from that here.
        if len < part.consumed_high_water {
            let reason = format!(
                "repair left part {index} at {len} bytes, below the {} already read from it",
                part.consumed_high_water
            );
            Self::abort_locked(&mut state, reason);
            drop(state);
            self.advanced.notify_all();
            return;
        }

        part.damage_cap = None;
        part.vouched_prefix = None;
        part.len = Some(len);
        // Exactly `len`, never `max`: a shrunk part's old watermark describes
        // bytes the repaired file does not have, and leaving it would send the
        // reader off the end.
        part.watermark = len;
        part.complete = true;
        // The file at the path is a new one; a reader with the old one open
        // must not resume over it.
        part.rewritten += 1;
        // Repair verified what it wrote, so the gate has nothing left to add:
        // the bytes on disk are now the recovery set's own answer.
        state.gated = false;
        Self::reconcile_total_when_settled(&mut state);
        drop(state);
        self.advanced.notify_all();
    }

    /// Fail the set, waking every parked reader.
    ///
    /// Every subsequent reader call returns this reason as an error, so a
    /// download that gives up does not leave an extraction thread parked
    /// forever.
    pub fn abort(&self, reason: impl Into<String>) {
        let mut state = self.lock();
        Self::abort_locked(&mut state, reason.into());
        drop(state);
        self.advanced.notify_all();
    }

    /// Once every part has settled, what they sum to has to be what the
    /// signature header said the archive was.
    ///
    /// A disagreement means the parts on disk are not the archive the header
    /// describes, and the decoder would meet it as an unexplained EOF somewhere
    /// in the middle. It matters most for a one-part set — a bare `.7z` armed
    /// from its opening bytes has nothing but the header's word for its length
    /// until the file finishes — but it is worth checking for any shape, and
    /// after a repair as much as after a download.
    fn reconcile_total_when_settled(state: &mut CoverageState) {
        let Some(total) = state.total_len else {
            return;
        };
        if !state.parts.iter().all(|part| part.complete) {
            return;
        }
        let summed = state
            .parts
            .iter()
            .map(|part| part.len.unwrap_or(part.watermark))
            .try_fold(0u64, |sum, len| sum.checked_add(len));
        match summed {
            Some(summed) if summed == total => {}
            Some(summed) => Self::abort_locked(
                state,
                format!("parts settled at {summed} bytes but the archive header declared {total}"),
            ),
            None => Self::abort_locked(state, "part lengths overflow".to_string()),
        }
    }

    fn abort_locked(state: &mut CoverageState, reason: String) {
        if state.aborted.is_none() {
            state.aborted = Some(reason);
        }
    }

    // ---- reader side -----------------------------------------------------

    /// The archive's total length, parking until it is known.
    pub fn total_len(&self) -> io::Result<u64> {
        let mut state = self.lock();
        loop {
            if let Some(reason) = &state.aborted {
                return Err(io::Error::other(reason.clone()));
            }
            if let Some(total) = state.total_len {
                return Ok(total);
            }
            state = self.park(state);
        }
    }

    /// A part's exact length, parking until it is known.
    pub fn part_len(&self, index: usize) -> io::Result<u64> {
        let mut state = self.lock();
        loop {
            if let Some(reason) = &state.aborted {
                return Err(io::Error::other(reason.clone()));
            }
            let part = Self::part_at(&state, index)?;
            if let Some(len) = part.len {
                return Ok(len);
            }
            state = self.park(state);
        }
    }

    /// How many bytes are readable in `index` starting at `offset`, parking
    /// until at least one is — or until the part ends there.
    ///
    /// `Ok(0)` means the part is finished and `offset` is at or past its end:
    /// end of part, not "try again".
    pub fn readable_at(&self, index: usize, offset: u64) -> io::Result<u64> {
        let mut state = self.lock();
        loop {
            if let Some(reason) = &state.aborted {
                return Err(io::Error::other(reason.clone()));
            }
            let part = Self::part_at(&state, index)?;
            if part.len.is_some_and(|len| offset >= len) {
                return Ok(0);
            }
            let servable = part.servable(state.gated);
            if offset < servable {
                return Ok(servable - offset);
            }
            // A part held back by a damage cap is not finished as far as the
            // reader is concerned, however complete the download believes it
            // is: repair is still to come, and the bytes above the cap are
            // exactly the ones it will rewrite.
            if part.complete && !part.held_back(state.gated) {
                return Ok(0);
            }
            state = self.park(state);
        }
    }

    /// Where an offset sits relative to one part.
    ///
    /// The mapping walk asks this instead of asking for a length, because a
    /// length is more than it needs: to place an offset inside a part it is
    /// enough that the part's committed watermark has passed it. That matters
    /// for the part currently downloading, whose final length nobody knows yet
    /// — without this the reader could only ever consume *finished* parts, and
    /// the overlap direct unpack exists for would stop at every part boundary.
    ///
    /// Parks only when the offset is at or beyond the watermark and the part
    /// might still grow: either more bytes arrive (and it is inside) or the
    /// part ends (and it is beyond).
    pub fn resolve_position(&self, index: usize, offset: u64) -> io::Result<PositionInPart> {
        let mut state = self.lock();
        loop {
            if let Some(reason) = &state.aborted {
                return Err(io::Error::other(reason.clone()));
            }
            let part = Self::part_at(&state, index)?;

            let servable = part.servable(state.gated);
            if offset < servable {
                return Ok(PositionInPart::Inside {
                    available: servable - offset,
                    rewritten: part.rewritten,
                });
            }
            if let Some(len) = part.len {
                if offset >= len {
                    return Ok(PositionInPart::Beyond { len });
                }
                // Inside the declared length but past the watermark: the bytes
                // are still coming, unless the part has stopped growing.
                if part.complete && !part.held_back(state.gated) {
                    return Err(io::Error::other(format!(
                        "part {index} stopped at {} bytes, short of its declared length {len}",
                        part.watermark
                    )));
                }
            } else if part.complete && !part.held_back(state.gated) {
                // A complete part's length is its watermark, so this offset is
                // past the end of it.
                return Ok(PositionInPart::Beyond {
                    len: part.watermark,
                });
            }
            state = self.park(state);
        }
    }

    /// Current progress for one part, without blocking.
    pub fn part_progress(&self, index: usize) -> io::Result<PartProgress> {
        let state = self.lock();
        Self::part_at(&state, index).copied()
    }

    /// The abort reason, if the set has failed.
    pub fn abort_reason(&self) -> Option<String> {
        self.lock().aborted.clone()
    }

    fn part_at(state: &CoverageState, index: usize) -> io::Result<&PartProgress> {
        state.parts.get(index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "part index {index} out of range for a {}-part set",
                    state.parts.len()
                ),
            )
        })
    }

    fn park<'a>(
        &self,
        state: std::sync::MutexGuard<'a, CoverageState>,
    ) -> std::sync::MutexGuard<'a, CoverageState> {
        self.parks.fetch_add(1, Ordering::Relaxed);
        self.advanced
            .wait(state)
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_advances_monotonically() {
        let coverage = SetCoverage::new(2);
        coverage.advance_watermark(0, 100);
        coverage.advance_watermark(0, 40);

        assert_eq!(coverage.part_progress(0).expect("in range").watermark, 100);
    }

    #[test]
    fn readable_reports_the_gap_to_the_watermark() {
        let coverage = SetCoverage::new(1);
        coverage.advance_watermark(0, 500);

        assert_eq!(coverage.readable_at(0, 0).expect("readable"), 500);
        assert_eq!(coverage.readable_at(0, 200).expect("readable"), 300);
    }

    #[test]
    fn completion_fixes_an_undeclared_length_at_the_watermark() {
        let coverage = SetCoverage::new(1);
        coverage.advance_watermark(0, 300);
        coverage.mark_part_complete(0);

        let progress = coverage.part_progress(0).expect("in range");
        assert_eq!(progress.len, Some(300));
        assert_eq!(coverage.part_len(0).expect("known"), 300);
        // At the end of a finished part: end of part, not a park.
        assert_eq!(coverage.readable_at(0, 300).expect("readable"), 0);
    }

    #[test]
    fn an_out_of_range_part_is_an_error_not_a_park() {
        let coverage = SetCoverage::new(1);
        let error = coverage.readable_at(4, 0).expect_err("out of range");

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(coverage.park_count(), 0);
    }

    #[test]
    fn abort_turns_subsequent_reads_into_errors() {
        let coverage = SetCoverage::new(1);
        coverage.abort("download gave up");

        let error = coverage.readable_at(0, 0).expect_err("aborted");
        assert!(error.to_string().contains("download gave up"));
        assert!(coverage.total_len().is_err());
        assert!(coverage.part_len(0).is_err());
        assert_eq!(coverage.abort_reason().as_deref(), Some("download gave up"));
    }

    #[test]
    fn the_first_abort_reason_is_the_one_kept() {
        let coverage = SetCoverage::new(1);
        coverage.abort("first");
        coverage.abort("second");

        assert_eq!(coverage.abort_reason().as_deref(), Some("first"));
    }

    #[test]
    fn a_contradictory_part_length_aborts_the_set() {
        let coverage = SetCoverage::new(2);
        coverage.note_part_len(1, 4_096);
        coverage.note_part_len(1, 4_096);
        assert_eq!(coverage.part_len(1).expect("agreed"), 4_096);

        coverage.note_part_len(1, 2_048);

        let error = coverage.part_len(1).expect_err("contradiction");
        assert!(
            error
                .to_string()
                .contains("part 1 length changed from 4096 to 2048"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn completion_then_a_conflicting_length_aborts_the_set() {
        let coverage = SetCoverage::new(1);
        coverage.advance_watermark(0, 300);
        // Completion fixes the length at the watermark; a later declaration
        // that disagrees would retract bytes already served.
        coverage.mark_part_complete(0);
        coverage.note_part_len(0, 900);

        assert!(coverage.readable_at(0, 0).is_err());
    }

    #[test]
    fn a_contradictory_total_length_aborts_the_set() {
        let coverage = SetCoverage::new(1);
        coverage.set_total_len(1_000);
        coverage.set_total_len(1_000);
        assert_eq!(coverage.total_len().expect("agreed"), 1_000);

        coverage.set_total_len(2_000);
        let error = coverage.total_len().expect_err("contradiction");
        assert!(error.to_string().contains("changed from 1000 to 2000"));
    }
}
