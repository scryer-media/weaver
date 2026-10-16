// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

// A token never becomes live again. A repaired source gets a fresh token;
// old reads, sink writes and completion records must all reject the old one.
#[derive(Clone, Debug)]
pub(crate) struct RunEpoch {
    pub(crate) generation: u64,
    cancelled: Arc<AtomicBool>,
    commit_gate: Arc<Mutex<()>>,
    vouched: Arc<[PartProgress]>,
}

impl RunEpoch {
    pub(crate) fn check(&self) -> io::Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "stale positional run epoch",
            ))
        } else {
            Ok(())
        }
    }

    // The closure may settle completion metadata or publish owned files, but
    // must not acquire the coverage mutex. Repair holds coverage before taking
    // this gate; ordinary source/sink I/O uses before/after token checks instead.
    pub(crate) fn commit<T>(&self, action: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
        let _guard = self.commit_gate.lock().unwrap_or_else(|p| p.into_inner());
        self.check()?;
        action()
    }

    pub(crate) fn cancellation(&self) -> &AtomicBool {
        &self.cancelled
    }

    // Only positive PAR2 evidence from before the rewrite preserves output.
    // Unknown boundaries or unclaimed bytes make the run dirty. A later epoch
    // still rescans descriptors and validates its header before reusing checks.
    pub(crate) fn vouches_for(&self, mut offset: u64, len: u64) -> bool {
        if len == 0 || offset.checked_add(len).is_none() {
            return false;
        }
        let mut left = len;
        for part in self.vouched.iter() {
            let Some(part_len) = part.len else {
                return false;
            };
            if offset >= part_len {
                offset -= part_len;
                continue;
            }
            let take = left.min(part_len - offset);
            let prefix = part
                .vouched_prefix
                .unwrap_or(0)
                .min(part.damage_cap.unwrap_or(u64::MAX));
            if offset + take > prefix {
                return false;
            }
            left -= take;
            if left == 0 {
                return true;
            }
            offset = 0;
        }
        false
    }
}

#[derive(Debug)]
pub(super) struct ReplayState {
    pub(super) enabled: bool,
    pub(super) selected: bool,
    epoch: RunEpoch,
}

impl Default for ReplayState {
    fn default() -> Self {
        Self {
            enabled: false,
            selected: false,
            epoch: RunEpoch {
                generation: 0,
                cancelled: Arc::new(AtomicBool::new(false)),
                commit_gate: Arc::new(Mutex::new(())),
                vouched: Arc::from([]),
            },
        }
    }
}

impl ReplayState {
    pub(super) fn cancel(&self) {
        let _guard = self
            .epoch
            .commit_gate
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        self.epoch.cancelled.store(true, Ordering::Release);
    }

    pub(super) fn invalidate(&mut self, vouched: Arc<[PartProgress]>) {
        let gate = Arc::clone(&self.epoch.commit_gate);
        let _guard = gate.lock().unwrap_or_else(|p| p.into_inner());
        self.epoch.cancelled.store(true, Ordering::Release);
        self.epoch.generation += 1;
        self.epoch.vouched = vouched;
    }

    pub(super) fn resume(&mut self) {
        if self.enabled && self.epoch.cancelled.load(Ordering::Acquire) {
            self.epoch.cancelled = Arc::new(AtomicBool::new(false));
        }
    }
}

impl SetCoverage {
    // Header probing needs epoch invalidation, but keeps the ordinary repair
    // decision until the complete graph has passed positional admission.
    pub(crate) fn begin_run_probe(&self) -> io::Result<RunEpoch> {
        self.lock().replay.enabled = true;
        self.run_epoch()
    }

    pub(crate) fn select_run_replay(&self, epoch: &RunEpoch) -> io::Result<()> {
        let mut state = self.lock();
        epoch.check()?;
        if state.repair_paused || state.replay.epoch.generation != epoch.generation {
            return Err(io::Error::other("repair interrupted positional admission"));
        }
        state.replay.selected = true;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn enable_run_replay(&self) -> io::Result<RunEpoch> {
        let epoch = self.begin_run_probe()?;
        self.select_run_replay(&epoch)?;
        Ok(epoch)
    }

    pub(crate) fn disable_run_replay(&self) {
        let mut state = self.lock();
        state.replay.cancel();
        state.replay.enabled = false;
        state.replay.selected = false;
    }

    pub(crate) fn has_run_replay(&self) -> bool {
        self.lock().replay.selected
    }

    // Refresh the paused snapshot from the analysis pass immediately before
    // repair. Cached download evidence alone must not preserve a stale run.
    pub(crate) fn set_run_vouches(&self, prefixes: &[u64]) -> bool {
        let mut state = self.lock();
        if !state.repair_paused || !state.replay.selected || prefixes.len() != state.parts.len() {
            return false;
        }
        let mut vouched = state.parts.clone();
        for (part, &prefix) in vouched.iter_mut().zip(prefixes) {
            part.vouched_prefix = Some(prefix);
            part.damage_cap = None;
        }
        state.replay.epoch.vouched = Arc::from(vouched);
        true
    }

    pub(crate) fn run_epoch(&self) -> io::Result<RunEpoch> {
        let mut state = self.lock();
        loop {
            if let Some(reason) = &state.aborted {
                return Err(io::Error::other(reason.clone()));
            }
            if !state.repair_paused {
                return Ok(state.replay.epoch.clone());
            }
            state = self.park(state);
        }
    }

    // Admission proves the entire packed image, not merely the next byte.
    // In particular a dirty run may not restart at its old descriptor until
    // every byte it names is covered in the new epoch.
    pub(crate) fn wait_for_run_range(
        &self,
        epoch: &RunEpoch,
        offset: u64,
        len: u64,
    ) -> io::Result<()> {
        if len == 0 || offset.checked_add(len).is_none() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let mut state = self.lock();
        loop {
            epoch.check()?;
            if let Some(reason) = &state.aborted {
                return Err(io::Error::other(reason.clone()));
            }
            let known_total = state.total_len.or_else(|| {
                state
                    .parts
                    .iter()
                    .try_fold(0u64, |total, part| total.checked_add(part.len?))
            });
            if known_total.is_some_and(|total| offset + len > total) {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            if !state.repair_paused && Self::run_range_covered(&state, offset, len) {
                return Ok(());
            }
            state = self.park(state);
        }
    }

    // A positional source waits with its epoch, so repair wakes an old worker
    // with cancellation rather than leaving it parked until the new source is
    // readable. Used boundaries retain the ordinary split reader's invariant.
    pub(crate) fn run_read_window(
        &self,
        epoch: &RunEpoch,
        absolute: u64,
        wanted: usize,
    ) -> io::Result<(usize, u64, usize, u64)> {
        if wanted == 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let mut state = self.lock();
        loop {
            epoch.check()?;
            if let Some(reason) = &state.aborted {
                return Err(io::Error::other(reason.clone()));
            }
            if state.total_len.is_some_and(|len| absolute >= len) {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            if !state.repair_paused {
                let mut offset = absolute;
                let mut found = None;
                for index in 0..state.parts.len() {
                    let part = state.parts[index];
                    if let Some(len) = part.len
                        && offset >= len
                    {
                        state.mapped_lengths[index] = Some(len);
                        offset -= len;
                        continue;
                    }
                    let available = Self::range_end(&state, index, offset).saturating_sub(offset);
                    if available != 0 {
                        found = Some((
                            index,
                            offset,
                            wanted.min(usize::try_from(available).unwrap_or(usize::MAX)),
                            part.rewritten,
                        ));
                    }
                    break;
                }
                if let Some(window) = found {
                    return Ok(window);
                }
            }
            state = self.park(state);
        }
    }

    fn run_range_covered(state: &CoverageState, mut offset: u64, mut left: u64) -> bool {
        for (index, part) in state.parts.iter().enumerate() {
            if let Some(len) = part.len
                && offset >= len
            {
                offset -= len;
                continue;
            }
            loop {
                let end = Self::range_end(state, index, offset);
                let available = end.saturating_sub(offset);
                if available >= left {
                    return true;
                }
                if available == 0 {
                    return false;
                }
                left -= available;
                offset = end;
                if part.len == Some(end) {
                    offset = 0;
                    break;
                }
                // A committed range may start exactly at the prefix end.
                // Walk adjacent coverage without authorizing a gap.
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_header_probe_keeps_ordinary_repair_policy_until_graph_admission() {
        let coverage = SetCoverage::new(1);
        coverage.note_part_len(0, 100);
        coverage.advance_watermark(0, 100);
        let probe = coverage.begin_run_probe().unwrap();
        assert!(!coverage.has_run_replay());
        coverage.pause_for_repair();
        assert!(probe.check().is_err());
        assert!(!coverage.set_run_vouches(&[0]));
        assert!(coverage.select_run_replay(&probe).is_err());
        coverage.release_after_repair(0, 100);
        coverage.resume_after_repair();
        let current = coverage.run_epoch().unwrap();
        coverage.select_run_replay(&current).unwrap();
        assert!(coverage.has_run_replay());
        coverage.pause_for_repair();
        assert!(coverage.set_run_vouches(&[0]));
        coverage.disable_run_replay();
        assert!(!coverage.has_run_replay());
    }

    #[test]
    fn repair_invalidates_old_run_tokens_and_preserves_only_vouched_ranges() {
        let coverage = SetCoverage::new(2);
        coverage.note_part_len(0, 100);
        coverage.note_part_len(1, 100);
        coverage.advance_watermark(0, 100);
        coverage.advance_watermark(1, 100);
        coverage.note_vouched_prefix(0, 100);
        coverage.note_vouched_prefix(1, 20);
        let first = coverage.enable_run_replay().unwrap();
        coverage.pause_for_repair();
        assert!(first.check().is_err());
        coverage.release_after_repair(0, 100);
        coverage.release_after_repair(1, 100);
        coverage.resume_after_repair();
        let next = coverage.run_epoch().unwrap();
        assert_eq!(next.generation, first.generation + 1);
        assert!(next.vouches_for(90, 30));
        assert!(!next.vouches_for(90, 31));
        assert!(!next.vouches_for(u64::MAX, 1));
        assert!(first.cancellation().load(Ordering::Acquire));
        assert!(!next.cancellation().load(Ordering::Acquire));
        coverage.resume_after_repair();
        coverage.pause_for_repair();
        assert!(
            next.check().is_err(),
            "duplicate resume must not orphan live tokens"
        );
        assert!(
            next.commit(|| Ok(())).is_err(),
            "stale completion cannot commit"
        );
    }

    #[test]
    fn run_admission_waits_for_full_coverage_and_cancel_wakes_a_blocked_run() {
        let coverage = Arc::new(SetCoverage::new(1));
        coverage.note_part_len(0, 100);
        coverage.advance_watermark(0, 40);
        let epoch = coverage.enable_run_replay().unwrap();
        let worker_coverage = Arc::clone(&coverage);
        let worker = std::thread::spawn(move || worker_coverage.wait_for_run_range(&epoch, 0, 100));
        let mut state = coverage.lock();
        while coverage.park_count() == 0 {
            state = coverage.advanced.wait(state).unwrap();
        }
        assert!(!SetCoverage::run_range_covered(&state, 0, 100));
        drop(state);
        coverage.abort("cancel blocked run");
        assert!(worker.join().unwrap().is_err());
        assert!(!coverage.has_run_replay() || coverage.run_epoch().is_err());
        coverage.disable_run_replay();
        assert!(!coverage.has_run_replay());
    }
}

#[cfg(test)]
mod commit_tests {
    use super::*;
    use std::sync::{Barrier, mpsc};

    #[test]
    fn repair_waits_for_an_already_started_commit_and_refuses_later_commits() {
        let coverage = Arc::new(SetCoverage::new(1));
        let epoch = coverage.enable_run_replay().unwrap();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let commit_entered = Arc::clone(&entered);
        let commit_release = Arc::clone(&release);
        let old = epoch.clone();
        let publisher = std::thread::spawn(move || {
            old.commit(|| {
                commit_entered.wait();
                commit_release.wait();
                Ok(())
            })
        });
        entered.wait();
        let repair_coverage = Arc::clone(&coverage);
        let (started, ready) = mpsc::channel();
        let repair = std::thread::spawn(move || {
            started.send(()).unwrap();
            repair_coverage.pause_for_repair();
        });
        ready.recv().unwrap();
        release.wait();
        publisher.join().unwrap().unwrap();
        repair.join().unwrap();
        assert!(
            epoch
                .commit::<()>(|| panic!("stale publication ran"))
                .is_err()
        );
        assert!(epoch.check().is_err());
    }

    #[test]
    fn dirty_run_requires_its_whole_range_even_when_later_bytes_are_present() {
        let coverage = Arc::new(SetCoverage::new(1));
        coverage.note_part_len(0, 100);
        coverage.advance_watermark(0, 20);
        coverage.note_committed_range(0, 40, 100);
        let epoch = coverage.enable_run_replay().unwrap();
        let source = Arc::clone(&coverage);
        let reader = std::thread::spawn(move || source.wait_for_run_range(&epoch, 10, 90));
        let mut state = coverage.lock();
        while coverage.park_count() == 0 {
            state = coverage.advanced.wait(state).unwrap();
        }
        assert!(!SetCoverage::run_range_covered(&state, 10, 90));
        drop(state);
        coverage.note_committed_range(0, 20, 40);
        reader.join().unwrap().unwrap();
        assert!(
            coverage
                .wait_for_run_range(&coverage.run_epoch().unwrap(), 99, 2)
                .is_err()
        );
    }
}
