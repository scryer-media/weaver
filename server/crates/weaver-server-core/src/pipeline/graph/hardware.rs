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
use crate::runtime::HardwareProfile;

#[derive(Clone, Debug, Default)]
pub(crate) struct Hardware {
    grant: Arc<RwLock<Grant>>,
    shutdown: Shutdown,
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
    // An engine run: its decoder's memory and its threads.
    pub(crate) run: Budget,
    // A recovery set's repair: its workspace and its workers.
    pub(crate) repair: Budget,
}

impl Grant {
    // `work`, inside the pool when there is one.
    pub(crate) fn install<R: Send>(&self, work: impl FnOnce() -> R + Send) -> R {
        match &self.pool {
            Some(pool) => pool.install(work),
            None => work(),
        }
    }

    // What the profile the tuner holds grants, on `pool`.
    //
    //   run     the profile's 7z decoder allowance, on its extract threads,
    //           lent from `pool`
    //   repair  the profile's PAR3 share of the machine's memory, which a
    //           PAR2 repair is held to as well, on every core but one, no
    //           more than the profile's repair cap
    fn of(tuner: &RuntimeTuner, pool: Arc<rayon::ThreadPool>) -> Self {
        let tuning = tuner.profile_tuning();
        let probe = tuner.system_profile();
        let threads = tuner.params().extract_thread_count.max(1);
        let host = HardwareProfile::effective_cores(probe)
            .saturating_sub(1)
            .max(1);
        let lent: Arc<dyn Pool> = Arc::new(Lent(Arc::clone(&pool)));
        Self {
            pool: Some(pool),
            run: Budget {
                memory_bytes: Some(tuning.sevenz_decode_memory_bytes),
                threads,
                pool: Some(lent),
            },
            repair: Budget {
                memory_bytes: Some(
                    tuning
                        .par3_memory
                        .of(HardwareProfile::effective_memory_bytes(probe)),
                ),
                threads: tuning.par3_cpu_cap.map_or(host, |cap| host.min(cap)).max(1),
                pool: None,
            },
        }
    }
}

// The pool as a run borrows it: the run's thread waits while the work runs
// on the pool's threads.
struct Lent(Arc<rayon::ThreadPool>);

impl Pool for Lent {
    fn spread(&self, workers: usize, work: &(dyn Fn() + Sync)) {
        self.0.in_place_scope(|scope| {
            for _ in 0..workers {
                scope.spawn(|_| work());
            }
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
            grant: Arc::new(RwLock::new(Grant::of(tuner, pool))),
            shutdown: Shutdown::default(),
        }
    }

    // The same hardware, given to work that stops at `shutdown`.
    pub(crate) fn until(self, shutdown: Shutdown) -> Self {
        Self { shutdown, ..self }
    }

    // The pipeline's shutdown, as the work given this hardware reads it.
    pub(crate) fn shutdown(&self) -> &Shutdown {
        &self.shutdown
    }

    // What work starting now is given.
    pub(crate) fn grant(&self) -> Grant {
        self.grant
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    // The profile the tuner holds now, for the work that starts from here.
    // The pool is built again only when its size changed; work already
    // going keeps the grant it took.
    pub(crate) fn follow(&self, tuner: &RuntimeTuner) {
        let mut grant = self.grant.write().unwrap_or_else(PoisonError::into_inner);
        let threads = tuner.params().extract_thread_count;
        let pool = match &grant.pool {
            Some(pool) if pool.current_num_threads() == threads => Arc::clone(pool),
            _ => crate::runtime::postprocess_pool::build_postprocess_pool(threads),
        };
        *grant = Grant::of(tuner, pool);
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
