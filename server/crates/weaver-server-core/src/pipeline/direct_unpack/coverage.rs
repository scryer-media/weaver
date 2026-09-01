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
}

/// Where an archive offset sits relative to one part.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionInPart {
    /// Inside this part, with `available` committed bytes readable from it.
    Inside {
        /// Bytes readable from the offset before the frontier.
        available: u64,
    },
    /// Past this part's end; the next part starts `len` bytes in.
    Beyond {
        /// This part's final length.
        len: u64,
    },
}

#[derive(Debug)]
struct CoverageState {
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
    pub fn advance_watermark(&self, index: usize, watermark: u64) {
        let mut state = self.lock();
        let Some(part) = state.parts.get_mut(index) else {
            debug_assert!(false, "part index {index} out of range");
            return;
        };
        if watermark <= part.watermark {
            return;
        }
        debug_assert!(
            part.len.is_none_or(|len| watermark <= len),
            "watermark {watermark} exceeds declared part length {:?}",
            part.len
        );
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
            if offset < part.watermark {
                return Ok(part.watermark - offset);
            }
            if part.complete {
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

            if offset < part.watermark {
                return Ok(PositionInPart::Inside {
                    available: part.watermark - offset,
                });
            }
            if let Some(len) = part.len {
                if offset >= len {
                    return Ok(PositionInPart::Beyond { len });
                }
                // Inside the declared length but past the watermark: the bytes
                // are still coming, unless the part has stopped growing.
                if part.complete {
                    return Err(io::Error::other(format!(
                        "part {index} stopped at {} bytes, short of its declared length {len}",
                        part.watermark
                    )));
                }
            } else if part.complete {
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
