// Event-driven campaigns against retained PAR3 state. No wall clock, network,
// reference executable, or optional corpus is needed. Each assertion names its
// seed and a minimized event trace. WEAVER_PAR3_SCHEDULE_SEED replays one seed;
// WEAVER_PAR3_SCHEDULE_CASES increases the default four-seed campaign.

use super::*;
use par3_rs::creation::{
    CreationCodec, CreationDurability, CreationOptions, CreationPlan, CreationSource, Deduplication,
};
use par3_rs::session::RepairStatus;
use par3_rs::source::MemorySourceAccess;
mod inside;

#[derive(Clone, Copy, Debug)]
enum Variant {
    ReferenceCauchy,
    Cauchy8,
    Cauchy16,
    Fft8,
    Fft16,
    Cohorts,
    ManyBlocks,
    Aligned,
    Sliding,
    DataOnly,
    PackedTails,
    Nested,
    Aliases,
    MultipleSets,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Damage {
    Clean,
    Corrupt,
    Hole,
    Exhausted,
}

struct Fixture {
    _root: tempfile::TempDir,
    inputs: Vec<(String, Vec<u8>)>,
    carriers: Vec<Vec<u8>>,
    block: usize,
    data_only: bool,
}

// SplitMix64 fixes the stream independently of library versions and host RNGs.
fn random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e3779b97f4a7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

fn bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..len).map(|_| random(&mut state) as u8).collect()
}

fn memory(id: SourceId, bytes: &[u8], generation: u64) -> Arc<dyn SourceAccess> {
    let mut access = MemorySourceAccess::default();
    access.insert(id, generation, Arc::from(bytes));
    Arc::new(access)
}

fn test_options() -> ExecutionOptions {
    // Geometry tests exercise an explicit admission policy, independent of
    // detected host RAM, other tests' leases, and the runner's core count.
    let mut options = ExecutionOptions::default();
    options.memory = par3_rs::runtime::MemoryBudget::new(256 << 20);
    options.retained_bytes = 128 << 20;
    options.workers = 1;
    options
}

fn new_job() -> Par3Job {
    Par3Job {
        options: test_options(),
        ..Par3Job::default()
    }
}

impl Fixture {
    fn new(variant: Variant) -> Self {
        let root = tempfile::tempdir().unwrap();
        if matches!(variant, Variant::ReferenceCauchy) {
            return Self {
                _root: root,
                inputs: super::tests::inputs().into(),
                carriers: vec![
                    super::tests::INDEX.to_vec(),
                    super::tests::RECOVERY.to_vec(),
                ],
                block: 2000,
                data_only: false,
            };
        }
        let mut options = CreationOptions {
            block_size: 256,
            recovery_count: 3,
            execution: test_options(),
            ..CreationOptions::default()
        };
        options.execution.workers = 1;
        let mut len = 256 * 10 + 73;
        match variant {
            Variant::Cauchy16 => len = 256 * 260 + 73,
            Variant::Fft8 | Variant::Fft16 | Variant::Cohorts | Variant::ManyBlocks => {
                let interleave = if matches!(variant, Variant::Cohorts | Variant::ManyBlocks) {
                    2
                } else {
                    0
                };
                options.codec = CreationCodec::Fft {
                    capacity_log2: 2,
                    interleave,
                };
                options.recovery_count = 3 * (interleave + 1);
                if matches!(variant, Variant::Fft16) {
                    len = 256 * 260 + 73;
                }
                if matches!(variant, Variant::ManyBlocks) {
                    options.block_size = 64;
                    len = 65_538 * 64 + 17;
                }
            }
            Variant::Aligned => options.deduplication = Deduplication::Aligned,
            Variant::Sliding => options.deduplication = Deduplication::Sliding,
            Variant::DataOnly => {
                options.store_data = true;
                options.recovery_count = 0;
            }
            _ => {}
        }
        let mut inputs = vec![("payload.bin".into(), bytes(len, 7))];
        match variant {
            Variant::Aligned | Variant::Sliding => {
                let chunk = bytes(256, 19);
                inputs[0].1 = if matches!(variant, Variant::Sliding) {
                    [b"prefix13bytes".as_slice(), chunk.as_slice()]
                        .concat()
                        .repeat(10)
                } else {
                    chunk.repeat(10)
                };
            }
            Variant::PackedTails => {
                inputs = vec![
                    ("a.bin".into(), bytes(336, 1)),
                    ("b.bin".into(), bytes(352, 2)),
                    ("c.bin".into(), bytes(384, 3)),
                ];
            }
            Variant::Nested => {
                inputs[0].0 = "nested/deeper/payload.bin".into();
                inputs.push(("nested/empty.bin".into(), vec![]));
                inputs.push(("nested/inline.bin".into(), bytes(17, 1)));
            }
            Variant::Aliases => inputs.push(("alias.bin".into(), inputs[0].1.clone())),
            Variant::MultipleSets => inputs.push(("second.bin".into(), bytes(len, 71))),
            _ => {}
        }
        let mut fixture = Self {
            _root: root,
            inputs,
            carriers: vec![],
            block: options.block_size as usize,
            data_only: options.store_data,
        };
        if matches!(variant, Variant::MultipleSets) {
            fixture.create(&[0], options.clone(), "cauchy");
            options.codec = CreationCodec::Fft {
                capacity_log2: 2,
                interleave: 0,
            };
            fixture.create(&[1], options, "fft");
        } else {
            fixture.create(
                &(0..fixture.inputs.len()).collect::<Vec<_>>(),
                options,
                "set",
            );
        }
        fixture
    }

    fn create(&mut self, indices: &[usize], options: CreationOptions, stem: &str) {
        let mut access = MemorySourceAccess::default();
        let sources: Vec<_> = indices
            .iter()
            .map(|&i| {
                let source = SourceId(i as u64);
                access.insert(source, 1, Arc::from(self.inputs[i].1.as_slice()));
                CreationSource {
                    source,
                    name: self.inputs[i].0.clone(),
                }
            })
            .collect();
        let codec = options.codec;
        let plan = CreationPlan::build(Arc::new(access), &sources, options).unwrap();
        let blocks = plan.requirements().blocks;
        if blocks > 65_536 {
            assert!(matches!(codec, CreationCodec::Fft { interleave: 2, .. }));
        } else if blocks > 256 {
            assert_eq!(
                plan.requirements().field.size,
                2,
                "the wide fixture must use GF16"
            );
        } else if !self.data_only {
            assert_eq!(
                plan.requirements().field.size,
                1,
                "the small fixture must use GF8"
            );
        }
        let paths = plan
            .execute_with_durability(
                &self._root.path().join(format!("{stem}.par3")),
                self._root.path(),
                CreationDurability::Buffered,
            )
            .unwrap();
        self.carriers
            .extend(paths.iter().map(|path| std::fs::read(path).unwrap()));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Event {
    Source(usize, u8),
    Carrier(usize, bool),
    Assess,
}

// Each source/packet has a partial then complete publication. Choosing an
// enabled chain head explores legal orders without racing background tasks.
fn schedule(fixture: &Fixture, seed: u64) -> Vec<Event> {
    let mut chains: Vec<Vec<Event>> = (0..fixture.inputs.len())
        .map(|i| {
            vec![
                Event::Source(i, 0),
                Event::Source(i, 1),
                Event::Source(i, 2),
            ]
        })
        .collect();
    chains.extend((0..fixture.carriers.len()).map(|i| {
        vec![
            Event::Carrier(i, false),
            Event::Carrier(i, true),
            Event::Carrier(i, true),
        ]
    }));
    let mut state = seed;
    let mut events = Vec::new();
    while !chains.is_empty() {
        let i = random(&mut state) as usize % chains.len();
        events.push(chains[i].remove(0));
        events.push(Event::Assess);
        if chains[i].is_empty() {
            chains.remove(i);
        }
    }
    events
}

#[derive(Debug)]
struct Failure {
    contract: &'static str,
    detail: String,
}
type Check<T = ()> = Result<T, Failure>;
fn engine<T>(result: EngineResult<T>) -> Check<T> {
    result.map_err(|error| Failure {
        contract: "engine operation",
        detail: format!("{error:?}"),
    })
}

struct Replay<'a> {
    fixture: &'a Fixture,
    damage: Damage,
    job: Par3Job,
    sources: BTreeMap<usize, u8>,
    carriers: BTreeMap<usize, bool>,
}

impl<'a> Replay<'a> {
    fn new(fixture: &'a Fixture, damage: Damage) -> Self {
        Self {
            fixture,
            damage,
            job: new_job(),
            sources: BTreeMap::new(),
            carriers: BTreeMap::new(),
        }
    }

    fn publish(fixture: &Fixture, damage: Damage, job: &mut Par3Job, i: usize, phase: u8) -> Check {
        let (name, expected) = &fixture.inputs[i];
        let mut data = expected.clone();
        let len = data.len() as u64;
        let block = fixture.block as u64;
        // Phase one rewrites bytes after an earlier assessment, phase two
        // replaces that generation. Only protected input is damaged.
        if !data.is_empty() && (phase == 1 || (damage == Damage::Corrupt && i == 0)) {
            let at = (fixture.block + 11).min(data.len() - 1);
            data[at] ^= 0x80;
        }
        let ranges = if phase == 0 || len == 0 {
            vec![]
        } else if phase == 1 {
            std::iter::once(0..len / 2).collect()
        } else if damage == Damage::Exhausted {
            vec![]
        } else if damage == Damage::Hole && i == 0 {
            vec![0..block.min(len), (block * 2).min(len)..len]
        } else {
            std::iter::once(0..len).collect()
        };
        let id = SourceId(i as u64);
        let ranges = ranges
            .into_iter()
            .filter(|range| range.start < range.end)
            .collect();
        engine(job.publish_access(
            id,
            memory(id, &data, phase as u64 + 1),
            name.clone(),
            ranges,
            None,
        ))?;
        Ok(())
    }

    fn carrier(fixture: &Fixture, job: &mut Par3Job, i: usize, complete: bool) -> Check {
        let data = &fixture.carriers[i];
        let len = data.len() as u64;
        let id = SourceId(100 + i as u64);
        let ranges = if complete {
            std::iter::once(0..len).collect()
        } else {
            // A packet can straddle this hole. A later publication must rewind
            // to its beginning instead of permanently losing the packet.
            vec![0..len / 3, len / 3 + 17..len]
        };
        engine(job.publish_carrier(id, memory(id, data, 1), len, ranges, true))?;
        engine(job.scan(id))?;
        Ok(())
    }

    fn fresh(&self) -> Check<Par3Job> {
        let mut job = new_job();
        for (&i, &phase) in &self.sources {
            Self::publish(self.fixture, self.damage, &mut job, i, phase)?;
        }
        for (&i, &complete) in &self.carriers {
            Self::carrier(self.fixture, &mut job, i, complete)?;
        }
        engine(job.assess())?;
        Ok(job)
    }

    fn step(&mut self, event: Event) -> Check {
        match event {
            Event::Source(i, phase) => {
                Self::publish(self.fixture, self.damage, &mut self.job, i, phase)?;
                self.sources.insert(i, phase);
            }
            Event::Carrier(i, complete) => {
                Self::carrier(self.fixture, &mut self.job, i, complete)?;
                self.carriers.insert(i, complete);
            }
            Event::Assess => {
                engine(self.job.assess())?;
                let fresh = self.fresh()?;
                if signature(&self.job) != signature(&fresh) {
                    return Err(Failure {
                        contract: "incremental assessment equals fresh assessment",
                        detail: format!(
                            "retained={:?}; fresh={:?}",
                            signature(&self.job),
                            signature(&fresh)
                        ),
                    });
                }
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Check {
        engine(self.job.assess())?;
        let output = tempfile::tempdir().unwrap();
        let sets: Vec<_> = self.job.sets.keys().copied().collect();
        if sets.is_empty() {
            return Err(Failure {
                contract: "authenticated set",
                detail: "no set reached assessment".into(),
            });
        }
        for id in sets {
            let status = self.job.sets[&id].view.as_ref().unwrap().status;
            if self.damage == Damage::Exhausted && !self.fixture.data_only {
                if status != RepairStatus::NeedRecovery {
                    return Err(Failure {
                        contract: "insufficient recovery refuses completion",
                        detail: format!("{status:?}"),
                    });
                }
                continue;
            }
            if status == RepairStatus::Ready {
                engine(self.job.repair(id, output.path()))?;
            } else if status != RepairStatus::Complete {
                return Err(Failure {
                    contract: "repairable input reaches ready or complete",
                    detail: format!("{status:?}"),
                });
            }
        }
        if self.damage != Damage::Clean
            && (self.damage != Damage::Exhausted || self.fixture.data_only)
        {
            for (i, (name, expected)) in self.fixture.inputs.iter().enumerate() {
                if i != 0 && self.damage != Damage::Exhausted {
                    continue;
                }
                let actual = std::fs::read(output.path().join(name)).map_err(|error| Failure {
                    contract: "repaired output exists",
                    detail: format!("{name}: {error}"),
                })?;
                if &actual != expected {
                    return Err(Failure {
                        contract: "byte-exact repair",
                        detail: name.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}

fn signature(job: &Par3Job) -> Vec<String> {
    // A retained assessment may already own verified donor blocks, eliminating
    // a zero-deficit requirement that a fresh session still lists. Compare
    // actionable deficits, not those cache-dependent planning records.
    job.sets
        .iter()
        .map(|(id, set)| {
            format!(
                "{id:?}: {:?}",
                set.view.as_ref().map(|view| (
                    view.status,
                    &view.files,
                    view.requirements
                        .iter()
                        .filter(|r| r.additional > 0)
                        .collect::<Vec<_>>()
                ))
            )
        })
        .collect()
}

// Deletion shrinking retains the same violated contract, so a shorter trace
// that merely creates a different error is not reported as the reproducer.
fn minimize(events: &[Event], mut fails: impl FnMut(&[Event]) -> bool) -> Vec<Event> {
    let mut result = events.to_vec();
    let mut width = result.len() / 2;
    while width > 0 {
        let mut start = 0;
        while start + width <= result.len() {
            let mut candidate = result.clone();
            candidate.drain(start..start + width);
            if fails(&candidate) {
                result = candidate;
            } else {
                start += 1;
            }
        }
        width /= 2;
    }
    result
}

fn seeds() -> Vec<u64> {
    if let Ok(seed) = std::env::var("WEAVER_PAR3_SCHEDULE_SEED") {
        return vec![seed.parse().expect("decimal schedule seed")];
    }
    let count = std::env::var("WEAVER_PAR3_SCHEDULE_CASES")
        .map(|s| s.parse::<u64>().expect("positive campaign size"))
        .unwrap_or(4);
    assert!(count > 0, "a campaign must execute at least one seed");
    (0..count).collect()
}

fn campaign(variant: Variant) {
    campaign_seeds(variant, &seeds());
}

fn campaign_seeds(variant: Variant, seeds: &[u64]) {
    if seeds.is_empty() {
        return;
    }
    let fixture = Fixture::new(variant);
    for damage in [
        Damage::Clean,
        Damage::Corrupt,
        Damage::Hole,
        Damage::Exhausted,
    ] {
        // Deduplicated/Data-only inputs can be wholly recovered from sources
        // other than parity, so their exhausted case belongs to donor tests.
        if damage == Damage::Exhausted
            && matches!(
                variant,
                Variant::Aligned | Variant::Sliding | Variant::Aliases | Variant::PackedTails
            )
        {
            continue;
        }
        for &seed in seeds {
            let mut events = schedule(&fixture, seed);
            if seed % 2 == 0 && !fixture.data_only {
                // Every recovery volume repeats vital metadata. Alternate
                // seeds omit the index entirely, not just deliver it late.
                events.retain(|event| !matches!(event, Event::Carrier(0, _)));
            }
            let run = |events: &[Event]| -> Check {
                let mut replay = Replay::new(&fixture, damage);
                for &event in events {
                    replay.step(event)?;
                }
                Ok(())
            };
            let mut replay = Replay::new(&fixture, damage);
            for (index, &event) in events.iter().enumerate() {
                if let Err(failure) = replay.step(event) {
                    drop(replay);
                    let minimized = minimize(&events[..=index], |trace| {
                        run(trace).is_err_and(|other| {
                            other.contract == failure.contract
                                && (failure.contract != "engine operation"
                                    || other.detail == failure.detail)
                        })
                    });
                    panic!(
                        "variant={variant:?} damage={damage:?} seed={seed}; {}: {}; minimized={minimized:?}",
                        failure.contract, failure.detail
                    );
                }
            }
            if let Err(failure) = replay.finish() {
                panic!(
                    "variant={variant:?} damage={damage:?} seed={seed}; {}: {}; trace={events:?}",
                    failure.contract, failure.detail
                );
            }
        }
    }
}

macro_rules! variant_test {
    ($name:ident, $variant:ident) => {
        #[test]
        fn $name() {
            campaign(Variant::$variant);
        }
    };
}
variant_test!(cauchy_gf8_schedules, Cauchy8);
variant_test!(official_reference_cauchy_schedules, ReferenceCauchy);
variant_test!(cauchy_gf16_schedules, Cauchy16);
variant_test!(fft_gf8_schedules, Fft8);
variant_test!(fft_gf16_schedules, Fft16);
variant_test!(fft_uneven_cohort_schedules, Cohorts);
// Large-block repair performs filesystem work for every reconstructed block.
// Keep the same seed/damage cross-product while bounding each runner slot to
// one default seed, rather than serializing all sixteen scenarios in one test.
mod fft_over_65536_blocks_schedules {
    use super::*;

    fn shard(index: u64) {
        let selected: Vec<_> = seeds()
            .into_iter()
            .filter(|seed| seed % 4 == index)
            .collect();
        campaign_seeds(Variant::ManyBlocks, &selected);
    }

    #[test]
    fn shard_00() {
        shard(0);
    }

    #[test]
    fn shard_01() {
        shard(1);
    }

    #[test]
    fn shard_02() {
        shard(2);
    }

    #[test]
    fn shard_03() {
        shard(3);
    }
}
variant_test!(aligned_deduplication_schedules, Aligned);
variant_test!(sliding_deduplication_schedules, Sliding);
variant_test!(data_only_schedules, DataOnly);
variant_test!(packed_tail_schedules, PackedTails);
variant_test!(nested_inline_empty_schedules, Nested);
variant_test!(alias_schedules, Aliases);
variant_test!(independent_cauchy_and_fft_set_schedules, MultipleSets);

// Every order in which the chains' heads can be taken, each chain keeping
// its own order.
fn interleavings(chains: &mut [Vec<Event>], prefix: &mut Vec<Event>, output: &mut Vec<Vec<Event>>) {
    if chains.iter().all(Vec::is_empty) {
        output.push(prefix.clone());
        return;
    }
    for i in 0..chains.len() {
        if chains[i].is_empty() {
            continue;
        }
        let event = chains[i].remove(0);
        prefix.push(event);
        interleavings(chains, prefix, output);
        prefix.pop();
        chains[i].insert(0, event);
    }
}

// The seeded campaigns sample a large event space. This one is exhaustive
// over a small one: the protected source's three publications, and the
// index and the last recovery carrier each published with a hole and then
// whole, in every interleaving, with an assessment checked against a fresh
// one after each event, under every damage. The index is also left out
// altogether, since every recovery carrier repeats the metadata it holds.
// Carriers outside the interleaving arrive whole afterwards, so each damage
// finishes against the same recovery whichever order came first.
#[test]
fn every_interleaving_of_source_index_and_recovery() {
    let fixture = Fixture::new(Variant::Cauchy8);
    let last = fixture.carriers.len() - 1;
    assert!(last > 0, "the fixture has an index and a recovery carrier");
    let source = vec![
        Event::Source(0, 0),
        Event::Source(0, 1),
        Event::Source(0, 2),
    ];
    let carrier = |i| vec![Event::Carrier(i, false), Event::Carrier(i, true)];
    let mut orders = Vec::new();
    interleavings(
        &mut [source.clone(), carrier(0), carrier(last)],
        &mut Vec::new(),
        &mut orders,
    );
    assert_eq!(orders.len(), 210);
    let mut without_index = Vec::new();
    interleavings(
        &mut [source, carrier(last)],
        &mut Vec::new(),
        &mut without_index,
    );
    assert_eq!(without_index.len(), 10);
    let rest: Vec<_> = (1..last).map(|i| Event::Carrier(i, true)).collect();
    for damage in [
        Damage::Clean,
        Damage::Corrupt,
        Damage::Hole,
        Damage::Exhausted,
    ] {
        for (case, order) in orders.iter().chain(&without_index).enumerate() {
            let mut events = Vec::new();
            for &event in order.iter().chain(&rest) {
                events.push(event);
                events.push(Event::Assess);
            }
            let run = |events: &[Event]| -> Check {
                let mut replay = Replay::new(&fixture, damage);
                for &event in events {
                    replay.step(event)?;
                }
                Ok(())
            };
            let mut replay = Replay::new(&fixture, damage);
            for (index, &event) in events.iter().enumerate() {
                if let Err(failure) = replay.step(event) {
                    drop(replay);
                    let minimized = minimize(&events[..=index], |trace| {
                        run(trace).is_err_and(|other| {
                            other.contract == failure.contract
                                && (failure.contract != "engine operation"
                                    || other.detail == failure.detail)
                        })
                    });
                    panic!(
                        "damage={damage:?} case={case}; {}: {}; minimized={minimized:?}",
                        failure.contract, failure.detail
                    );
                }
            }
            if let Err(failure) = replay.finish() {
                panic!(
                    "damage={damage:?} case={case}; {}: {}; trace={events:?}",
                    failure.contract, failure.detail
                );
            }
        }
    }
}

#[test]
fn fft_cohort_deficit_cannot_borrow_surplus_from_another_cohort() {
    let fixture = Fixture::new(Variant::Cohorts);
    let mut job = new_job();
    let (name, bytes) = &fixture.inputs[0];
    let ranges = vec![256..768, 1024..1536, 1792..2304, 2560..bytes.len() as u64];
    job.publish_access(
        SourceId(0),
        memory(SourceId(0), bytes, 1),
        name.clone(),
        ranges,
        None,
    )
    .unwrap();
    for i in 0..fixture.carriers.len() {
        Replay::carrier(&fixture, &mut job, i, true).unwrap();
    }
    job.assess().unwrap();
    let view = job.sets.values().next().unwrap().view.as_ref().unwrap();
    assert_eq!(view.status, RepairStatus::NeedRecovery);
    let need = view
        .requirements
        .iter()
        .find(|need| need.cohort == 0)
        .unwrap();
    assert_eq!(
        (need.lost, need.available.len(), need.additional),
        (4, 3, 1)
    );
}
