// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The machine as the hardware profile in force lends it to graph work: the
// pool a repair runs inside, and what one engine run and one repair may hold
// and spread across. A run or a repair takes the grant in force as it starts
// and keeps it until it ends; a profile change replaces the grant for the
// work that starts after it, and a replaced pool goes when the last work
// holding it ends.
//
// A run is not put on the pool. It waits on its volumes as they download,
// and a pool thread parked on a download would hold back every repair and
// every run behind it, an inner archive's run among them waiting on the
// outer one's bytes. Its grant lends it the pool for work over bytes that
// are all in, so the threads every job spreads across stay within it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError, RwLock};

use weaver_graph::{
    Budget, Builder, Facts, Kind, NodeId, Object, ObjectId, Placement, Pool, Restore, RestoreError,
    Tree, VolumeIndex,
};

use crate::RuntimeTuner;
use crate::runtime::{HardwareProfile, OverLimit};

#[derive(Clone, Debug, Default)]
pub(crate) struct Hardware {
    grant: Arc<RwLock<Grant>>,
    shutdown: Shutdown,
    // The job was asked to run with no memory limit: a run whose window is
    // over the profile's ceiling is given it, whatever the profile says.
    lifted: bool,
}

// The pipeline's shutdown as graph work reads it: an atomic set from the
// moment shutdown is asked for, read on repair threads without any lock. A
// repair does not begin once it is set, and one going stops reading.
// Outside a pipeline nothing sets it.
#[derive(Clone, Debug, Default)]
pub(crate) struct Shutdown(Arc<AtomicBool>);

impl Shutdown {
    pub(crate) fn of(started: Arc<AtomicBool>) -> Self {
        Self(started)
    }

    pub(crate) fn started(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

// What work starting now is given.
#[derive(Clone, Debug, Default)]
pub(crate) struct Grant {
    // The post-processing pool, sized to the profile's extract threads.
    // None outside a pipeline: the work runs on the thread it was given.
    pub(crate) pool: Option<Arc<rayon::ThreadPool>>,
    // The pool a repair runs inside, as wide as the repair's threads: a
    // PAR2 repair spreads across the pool it runs in. The post-processing
    // pool when that is as wide. None outside a pipeline.
    pub(crate) repair_pool: Option<Arc<rayon::ThreadPool>>,
    // An engine run: its decoder's memory and its threads.
    pub(crate) run: Budget,
    // A recovery set's repair: its workspace and its workers.
    pub(crate) repair: Budget,
    // What a run whose window is over the run's ceiling gets.
    pub(crate) over_limit: OverLimit,
    // The memory the machine can use, when it is known.
    pub(crate) machine_bytes: Option<u64>,
}

impl Grant {
    // The most one run may hold in RAM above the ceiling: the machine's
    // memory less a quarter of it, kept for the system, the holds and every
    // other job. With no machine known, nothing bounds it.
    pub(crate) fn above_ceiling(&self) -> u64 {
        self.machine_bytes
            .map_or(u64::MAX, |memory| memory - memory / 4)
    }

    // The widest window the grant holds one decoder to: its window, no
    // more than its memory, or no limit when it grants neither.
    pub(crate) fn held(&self) -> u64 {
        self.run.window_or(u64::MAX)
    }

    // The widest window a run decodes at the grant: the wider of its memory
    // and its window, or no ceiling when it grants neither.
    pub(crate) fn ceiling(&self) -> u64 {
        self.run
            .memory_bytes
            .max(self.run.window_bytes)
            .unwrap_or(u64::MAX)
    }
}

impl Grant {
    // `work`, inside the pool when there is one.
    // The pool's thread works for the job this one does.
    pub(crate) fn install<R: Send>(&self, work: impl FnOnce() -> R + Send) -> R {
        match &self.pool {
            Some(pool) => pool.install(crate::runtime::panic_trap::carried(work)),
            None => work(),
        }
    }

    // A repair's `work`, inside the repair pool when there is one.
    // The pool's thread works for the job this one does.
    pub(crate) fn install_repair<R: Send>(&self, work: impl FnOnce() -> R + Send) -> R {
        match &self.repair_pool {
            Some(pool) => pool.install(crate::runtime::panic_trap::carried(work)),
            None => work(),
        }
    }

    // What the profile the tuner holds grants, on `pool`, its repairs inside
    // `repairs` when that is as wide as they are granted.
    //
    //   run     the profile's 7z decoder allowance, and the window any one
    //           decoder may hold, on its extract threads, lent from `pool`
    //   repair  the profile's PAR3 share of the machine's memory, which a
    //           PAR2 repair is held to as well, on every core but one, no
    //           more than the profile's repair cap
    fn of(
        tuner: &RuntimeTuner,
        pool: Arc<rayon::ThreadPool>,
        repairs: Option<&Arc<rayon::ThreadPool>>,
    ) -> Self {
        let tuning = tuner.profile_tuning();
        let probe = tuner.system_profile();
        let threads = tuner.params().extract_thread_count.max(1);
        let host = HardwareProfile::effective_cores(probe)
            .saturating_sub(1)
            .max(1);
        let repair_threads = tuning.par3_cpu_cap.map_or(host, |cap| host.min(cap)).max(1);
        let repair_pool = [Some(&pool), repairs]
            .into_iter()
            .flatten()
            .find(|held| held.current_num_threads() == repair_threads)
            .cloned()
            .unwrap_or_else(|| {
                crate::runtime::postprocess_pool::build_postprocess_pool(repair_threads)
            });
        let lent: Arc<dyn Pool> = Arc::new(Lent(Arc::clone(&pool)));
        Self {
            pool: Some(pool),
            repair_pool: Some(repair_pool),
            run: Budget {
                memory_bytes: Some(tuning.sevenz_decode_memory_bytes),
                window_bytes: Some(tuning.decode_window_bytes),
                window_reserved: false,
                threads,
                pool: Some(lent),
                lender: None,
            },
            repair: Budget {
                memory_bytes: Some(
                    tuning
                        .par3_memory
                        .of(HardwareProfile::effective_memory_bytes(probe)),
                ),
                window_bytes: None,
                window_reserved: false,
                threads: repair_threads,
                pool: None,
                lender: None,
            },
            over_limit: tuning.over_limit,
            // Only memory read from the machine bounds a run: a guessed
            // size is no hard stop.
            machine_bytes: (!probe.memory.guessed)
                .then(|| HardwareProfile::effective_memory_bytes(probe)),
        }
    }
}

// The pool as a run borrows it: the run's thread waits while the work runs
// on the pool's threads, or goes on with its own beside it.
struct Lent(Arc<rayon::ThreadPool>);

impl Pool for Lent {
    // Each worker works for the job the run does.
    fn spread(&self, workers: usize, work: &(dyn Fn() + Sync)) {
        let job = crate::runtime::panic_trap::current_job();
        self.0.in_place_scope(|scope| {
            for _ in 0..workers {
                scope.spawn(move |_| {
                    let _job = crate::runtime::panic_trap::JobScope::carry(job);
                    work()
                });
            }
        });
    }

    fn spread_beside(&self, work: &(dyn Fn() + Sync), caller: &mut dyn FnMut(&dyn Fn())) {
        let job = crate::runtime::panic_trap::current_job();
        self.0.in_place_scope(|scope| {
            caller(&|| {
                scope.spawn(move |_| {
                    let _job = crate::runtime::panic_trap::JobScope::carry(job);
                    work()
                });
            });
        });
    }
}

impl Hardware {
    // What the profile the tuner holds grants, on a pool of its own.
    pub(crate) fn of(tuner: &RuntimeTuner) -> Self {
        let pool = crate::runtime::postprocess_pool::build_postprocess_pool(
            tuner.params().extract_thread_count,
        );
        Self {
            grant: Arc::new(RwLock::new(Grant::of(tuner, pool, None))),
            shutdown: Shutdown::default(),
            lifted: false,
        }
    }

    // Hardware that grants `grant`, outside a pipeline.
    #[cfg(test)]
    pub(crate) fn granting(grant: Grant) -> Self {
        Self {
            grant: Arc::new(RwLock::new(grant)),
            ..Self::default()
        }
    }

    // The same hardware, given to work that stops at `shutdown`.
    pub(crate) fn until(self, shutdown: Shutdown) -> Self {
        Self { shutdown, ..self }
    }

    // The same hardware, for a job asked to run with no memory limit when
    // `lifted`: a window over the ceiling is lent from scratch, then RAM,
    // whatever the profile says. Other jobs keep the profile's policy.
    pub(crate) fn lifted(self, lifted: bool) -> Self {
        Self { lifted, ..self }
    }

    // The pipeline's shutdown, as the work given this hardware reads it.
    pub(crate) fn shutdown(&self) -> &Shutdown {
        &self.shutdown
    }

    // What work starting now is given.
    pub(crate) fn grant(&self) -> Grant {
        let mut grant = self
            .grant
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if self.lifted {
            grant.over_limit = OverLimit::ScratchThenRam;
        }
        grant
    }

    // The profile the tuner holds now, for the work that starts from here.
    // Each pool is built again only when its size changed; work already
    // going keeps the grant it took.
    pub(crate) fn follow(&self, tuner: &RuntimeTuner) {
        let mut grant = self.grant.write().unwrap_or_else(PoisonError::into_inner);
        let threads = tuner.params().extract_thread_count;
        let pool = match &grant.pool {
            Some(pool) if pool.current_num_threads() == threads => Arc::clone(pool),
            _ => crate::runtime::postprocess_pool::build_postprocess_pool(threads),
        };
        *grant = Grant::of(tuner, pool, grant.repair_pool.as_ref());
    }
}

// The objects, each held to the run grant in force as it is built or
// restored: a 7z store's header parses and key derivations decode as a run
// of it does.
impl Builder for Hardware {
    fn build(&mut self, kind: Kind, id: ObjectId) -> Option<Box<dyn Object>> {
        weaver_objects::build(kind, id, self.grant().run)
    }

    // The objects keep no bytes: the owner that wraps them reads its own.
    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

impl Restore for Hardware {
    fn restore(&self, facts: &Facts) -> Result<Box<dyn Object>, RestoreError> {
        weaver_objects::restore(facts, self.grant().run)
    }
}
