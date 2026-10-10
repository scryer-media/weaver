// Replayable source/recovery schedules, checked against fresh native sessions.
// Fixtures contain real GF(2^16) recovery, never placeholder recovery bytes.
use crate::pipeline::repair::backend::RepairBackend;
use crate::pipeline::tests::{
    build_repairable_par2_set_for_files, build_test_par2_index_for_files,
    build_test_par2_recovery_volume,
};
use par2_rs::{Par2RepairOutcome, Par2RepairSession, Par2RepairSessionOptions, Par2RepairStatus};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
enum Shape {
    ShortTail,
    MultipleFiles,
    Nested,
    DuplicateContents,
    BeyondQuickHash,
}

#[derive(Clone, Copy, Debug)]
enum Damage {
    Clean,
    Corrupt,
    Truncated,
    Missing,
    Shifted,
    Renamed,
    Exhausted,
}

#[derive(Clone, Copy, Debug)]
enum Access {
    Disk,
    Snapshot,
}

#[derive(Clone, Copy, Debug)]
enum Event {
    Source(usize, u8),
    Recovery(usize, u8),
    Assess,
    Invalidate,
    Reopen,
}

struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut n = self.0;
        n = (n ^ (n >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        n = (n ^ (n >> 27)).wrapping_mul(0x94d049bb133111eb);
        n ^ (n >> 31)
    }
}

struct Fixture {
    files: Vec<(String, Vec<u8>)>,
    set: par2_rs::Par2FileSet,
    index: Vec<u8>,
    carriers: Vec<Vec<u8>>,
}

impl Fixture {
    fn new(shape: Shape, seed: u64) -> Self {
        Self::with_recovery(shape, seed, 12)
    }

    fn with_recovery(shape: Shape, seed: u64, recovery: usize) -> Self {
        let mut random = Random(seed);
        let block = if matches!(shape, Shape::BeyondQuickHash) {
            4096
        } else {
            64
        };
        let count = if matches!(shape, Shape::MultipleFiles | Shape::DuplicateContents) {
            3
        } else {
            1
        };
        let mut files = Vec::new();
        for i in 0..count {
            let len = if matches!(shape, Shape::BeyondQuickHash) {
                block * 5 + 13
            } else {
                block * 2 + 13 + i
            };
            let name = if matches!(shape, Shape::Nested) {
                format!("nested/level/source-{i}.bin")
            } else {
                format!("source-{i}.bin")
            };
            let bytes: Vec<u8> = (0..len).map(|_| random.next() as u8).collect();
            files.push((name, bytes));
        }
        if matches!(shape, Shape::DuplicateContents) {
            for i in 1..files.len() {
                files[i].1 = files[0].1.clone();
            }
        }
        if matches!(shape, Shape::MultipleFiles) {
            files.push(("empty.bin".into(), Vec::new()));
        }
        let borrowed: Vec<_> = files
            .iter()
            .map(|(n, b)| (n.as_str(), b.as_slice()))
            .collect();
        let set = build_repairable_par2_set_for_files(&borrowed, block as u64, recovery);
        let index = build_test_par2_index_for_files(&borrowed, block as u64);
        let carriers = set
            .recovery_slices
            .values()
            .map(|slice| {
                build_test_par2_recovery_volume(
                    *set.recovery_set_id.as_bytes(),
                    &[(slice.exponent, slice.data.as_bytes().unwrap())],
                )
            })
            .collect();
        Self {
            files,
            set,
            index,
            carriers,
        }
    }

    fn schedule(&self, seed: u64, damage: Damage) -> Vec<Event> {
        let mut chains: Vec<Vec<Event>> = (0..self.files.len())
            .map(|i| vec![Event::Source(i, 1), Event::Source(i, 2)])
            .collect();
        let count = if matches!(damage, Damage::Exhausted) {
            1
        } else {
            self.carriers.len()
        };
        for i in 0..count {
            chains.push(vec![
                Event::Recovery(i, 0),
                Event::Recovery(i, 1),
                Event::Recovery(i, 2),
            ]);
        }
        chains.push(vec![
            Event::Assess,
            Event::Invalidate,
            Event::Reopen,
            Event::Assess,
        ]);
        let mut random = Random(seed);
        let mut result = vec![];
        while !chains.is_empty() {
            let i = random.next() as usize % chains.len();
            result.push(chains[i].remove(0));
            if chains[i].is_empty() {
                chains.remove(i);
            }
        }
        result
    }
}

struct Run<'a> {
    fixture: &'a Fixture,
    damage: Damage,
    access: Access,
    root: tempfile::TempDir,
    data: PathBuf,
    index: PathBuf,
    merged: Vec<PathBuf>,
    current: Vec<Option<Vec<u8>>>,
    session: Par2RepairSession,
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn signature(outcome: &Par2RepairOutcome) -> (Par2RepairStatus, u32, u32, u32, u32, u32, u32) {
    (
        outcome.status,
        outcome.available_blocks,
        outcome.missing_blocks,
        outcome.recovery_blocks_available,
        outcome.files_complete,
        outcome.files_damaged,
        outcome.files_missing,
    )
}

impl<'a> Run<'a> {
    fn options(&self) -> Par2RepairSessionOptions {
        let mut options =
            Par2RepairSessionOptions::new(self.data.clone(), vec![self.index.clone()]);
        options.recovery_paths = self.merged.clone();
        options.memory_limit = Some(64 << 20);
        options.retained_state_limit = 32 << 20;
        if matches!(self.access, Access::Snapshot) {
            options.source_access = Some(self.snapshot());
        }
        options
    }

    fn snapshot(&self) -> Arc<dyn par2_rs::FileAccess + Send + Sync> {
        let mut access = par2_rs::MemoryFileAccess::new();
        for (i, (_, original)) in self.fixture.files.iter().enumerate() {
            if let Some(bytes) = &self.current[i] {
                let id = self.fixture.set.recovery_file_ids[i];
                assert_eq!(self.fixture.set.files[&id].length, original.len() as u64);
                access.add_file(id, bytes.clone());
            }
        }
        Arc::new(access)
    }

    fn new(fixture: &'a Fixture, damage: Damage, access: Access) -> Result<Self, String> {
        let root = tempfile::tempdir().map_err(err)?;
        let data = root.path().join("data");
        std::fs::create_dir(&data).map_err(err)?;
        let index = root.path().join("index.par2");
        std::fs::write(&index, &fixture.index).map_err(err)?;
        let mut options = Par2RepairSessionOptions::new(data.clone(), vec![index.clone()]);
        options.memory_limit = Some(64 << 20);
        options.retained_state_limit = 32 << 20;
        if matches!(access, Access::Snapshot) {
            options.source_access = Some(Arc::new(par2_rs::MemoryFileAccess::new()));
        }
        let session = Par2RepairSession::open(options).map_err(err)?;
        Ok(Self {
            fixture,
            damage,
            access,
            root,
            data,
            index,
            merged: vec![],
            current: vec![None; fixture.files.len()],
            session,
        })
    }

    fn source(&mut self, i: usize, phase: u8) -> Result<(), String> {
        let (name, original) = &self.fixture.files[i];
        let mut bytes = original.clone();
        let missing = phase == 2 && matches!(self.damage, Damage::Missing);
        if phase == 1 {
            bytes.truncate(bytes.len() / 2);
        } else {
            match self.damage {
                Damage::Corrupt if !bytes.is_empty() => {
                    // The wide fixture damages bytes beyond the 16 KiB quick hash.
                    let at = if bytes.len() > 16 * 1024 {
                        16 * 1024 + 1
                    } else {
                        1
                    };
                    bytes[at] ^= 0xa5;
                }
                Damage::Truncated => bytes.truncate(bytes.len() / 2),
                Damage::Exhausted => bytes.fill(0x5a),
                Damage::Shifted => {
                    bytes.splice(0..0, [0x9b, 0x7d, 0x1f]);
                }
                _ => {}
            }
        }
        let path = self.data.join(name);
        if path.exists() {
            std::fs::remove_file(&path).map_err(err)?;
        }
        if !missing {
            let destination = if phase == 2 && matches!(self.damage, Damage::Renamed) {
                self.data.join(format!("renamed-{i}.bin"))
            } else {
                path
            };
            std::fs::create_dir_all(destination.parent().unwrap()).map_err(err)?;
            std::fs::write(destination, &bytes).map_err(err)?;
        }
        self.current[i] = (!missing).then_some(bytes);
        // Exercise the same conservative invalidation used by the host backend.
        self.session.invalidate(());
        if matches!(self.access, Access::Snapshot) {
            let snapshot = self.snapshot();
            self.session
                .set_source_access(snapshot)
                .ok_or("snapshot session rejected a replacement handle")?;
        }
        Ok(())
    }

    fn apply(&mut self, event: Event) -> Result<(), String> {
        match event {
            Event::Source(i, phase) => self.source(i, phase)?,
            Event::Recovery(i, phase) => {
                // Partial and complete publications have distinct immutable paths.
                // Repeated publication of the completed path must be idempotent.
                let path = self
                    .root
                    .path()
                    .join(format!("recovery-{i}-{}.par2", phase.min(1)));
                if phase != 2 {
                    let mut bytes = self.fixture.index.clone();
                    let mut packet = self.fixture.carriers[i].clone();
                    if phase == 0 {
                        if i % 2 == 0 {
                            packet.truncate(packet.len() - 7);
                        } else {
                            let last = packet.len() - 1;
                            packet[last] ^= 1;
                        }
                    }
                    bytes.extend_from_slice(&packet);
                    std::fs::write(&path, bytes).map_err(err)?;
                } else if !path.exists() {
                    // A reduced trace may remove the original publication.
                    std::fs::write(&path, &self.fixture.carriers[i]).map_err(err)?;
                }
                let budget = Arc::new(std::sync::Mutex::new(super::super::Par2ScanBudget::new(
                    par2_rs::PacketScanLimits::default(),
                    Arc::new(crate::pipeline::ProcessMemoryBudget::new(64 << 20)),
                    par2_rs::CancellationToken::new(),
                )));
                let groups =
                    super::super::scan_completed_par2_packet_groups(&path, &budget).map_err(err)?;
                let valid_recovery = groups
                    .iter()
                    .flat_map(|group| &group.packets)
                    .filter(|packet| matches!(packet, par2_rs::Packet::RecoverySlice(_)))
                    .count();
                if valid_recovery != usize::from(phase != 0) {
                    return Err(format!(
                        "packet admission mismatch: phase={phase} valid={valid_recovery}"
                    ));
                }
                // Exercise native merging too: a malformed-only carrier must
                // not occupy an exponent ahead of its valid replacement.
                self.session.merge_recovery_paths([&path]).map_err(err)?;
                if !self.merged.contains(&path) {
                    self.merged.push(path);
                }
            }
            Event::Invalidate => self.session.invalidate(()),
            Event::Reopen => self.session = Par2RepairSession::open(self.options()).map_err(err)?,
            Event::Assess => {}
        }
        self.compare()
    }

    fn compare(&mut self) -> Result<(), String> {
        let mut fresh_session = Par2RepairSession::open(self.options()).map_err(err)?;
        if matches!(self.access, Access::Snapshot) {
            let mut verifier = par2_rs::VerificationSession::new();
            let packets = par2_rs::packet::scan_packets(&self.fixture.index, 0)
                .map_err(err)?
                .into_iter()
                .map(|(packet, _)| packet)
                .collect::<Vec<_>>();
            verifier.add_par2_data(&packets);
            for (i, bytes) in self.current.iter().enumerate() {
                if let Some(bytes) = bytes {
                    verifier.feed_data(&self.fixture.set.recovery_file_ids[i], 0, bytes);
                }
            }
            for evidence in verifier.slice_evidence() {
                self.session
                    .add_slice_evidence_for_file(evidence)
                    .map_err(err)?;
                fresh_session
                    .add_slice_evidence_for_file(evidence)
                    .map_err(err)?;
            }
        }
        let retained = self.session.assess().map_err(err)?;
        let fresh = fresh_session.assess().map_err(err)?;
        if signature(&retained) != signature(&fresh) {
            return Err(format!(
                "assessment mismatch: retained={:?}, fresh={:?}",
                signature(&retained),
                signature(&fresh)
            ));
        }
        let again = self.session.assess().map_err(err)?;
        if signature(&again) != signature(&retained) {
            return Err("repeat assessment changed verdict".into());
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), String> {
        let assessed = self.session.assess().map_err(err)?;
        if matches!(self.damage, Damage::Exhausted) {
            if assessed.status != Par2RepairStatus::Insufficient {
                return Err(format!("exhausted set: {:?}", signature(&assessed)));
            }
            let before = self.current.clone();
            let repaired = self.session.execute(()).map_err(err)?;
            if repaired.status != Par2RepairStatus::Insufficient {
                return Err("insufficient repair claimed success".into());
            }
            for (i, (name, _)) in self.fixture.files.iter().enumerate() {
                if std::fs::read(self.data.join(name)).ok() != before[i] {
                    return Err("insufficient repair modified input".into());
                }
            }
            return Ok(());
        }
        if !matches!(
            assessed.status,
            Par2RepairStatus::Verified | Par2RepairStatus::RepairPossible
        ) {
            return Err(format!(
                "repairable set rejected: {:?}",
                signature(&assessed)
            ));
        }
        if assessed.status != Par2RepairStatus::Verified {
            let repaired = self.session.execute(()).map_err(err)?;
            if !matches!(
                repaired.status,
                Par2RepairStatus::Repaired | Par2RepairStatus::Verified
            ) {
                return Err(format!(
                    "repair failed: before={:?} after={:?} packets={:?}",
                    signature(&assessed),
                    signature(&repaired),
                    repaired.packets
                ));
            }
        }
        for (name, expected) in &self.fixture.files {
            // A verified handle-backed session has no installation to perform.
            let actual = std::fs::read(self.data.join(name)).map_err(err)?;
            if actual != *expected {
                return Err(format!("repaired bytes differ: {name}"));
            }
        }
        Ok(())
    }
}

fn replay(
    fixture: &Fixture,
    damage: Damage,
    access: Access,
    events: &[Event],
    finish: bool,
) -> Result<(), String> {
    let mut run = Run::new(fixture, damage, access)?;
    for event in events {
        run.apply(*event)?;
    }
    if finish {
        run.finish()?;
    }
    Ok(())
}

fn minimize(
    fixture: &Fixture,
    damage: Damage,
    access: Access,
    events: &[Event],
    error: &str,
) -> Vec<Event> {
    let mut reduced = events.to_vec();
    let mut i = 0;
    while i < reduced.len() {
        let mut candidate = reduced.clone();
        candidate.remove(i);
        if replay(fixture, damage, access, &candidate, false)
            .is_err_and(|e| e.split(':').next() == error.split(':').next())
        {
            reduced = candidate;
        } else {
            i += 1;
        }
    }
    reduced
}

fn campaign(shape: Shape) {
    let cases = std::env::var("WEAVER_PAR2_SCHEDULE_CASES")
        .map(|s| s.parse::<u64>().expect("positive schedule count"))
        .unwrap_or(4);
    assert!(cases > 0);
    let selected = std::env::var("WEAVER_PAR2_SCHEDULE_SEED")
        .ok()
        .map(|s| s.parse::<u64>().expect("decimal replay seed"));
    for seed in selected.map_or_else(|| (0..cases).collect(), |seed| vec![seed]) {
        let fixture = Fixture::new(shape, seed);
        for access in [Access::Disk, Access::Snapshot] {
            for damage in [
                Damage::Clean,
                Damage::Corrupt,
                Damage::Truncated,
                Damage::Missing,
                Damage::Shifted,
                Damage::Renamed,
                Damage::Exhausted,
            ] {
                if matches!(access, Access::Snapshot)
                    && matches!(damage, Damage::Shifted | Damage::Renamed)
                {
                    continue;
                }
                let events = fixture.schedule(seed, damage);
                if let Err(error) = replay(&fixture, damage, access, &events, false) {
                    let reduced = minimize(&fixture, damage, access, &events, &error);
                    panic!(
                        "{shape:?} {damage:?} {access:?} seed={seed}: {error}\nreduced trace={reduced:?}"
                    );
                }
                // Terminal repair runs on its own copy; mutation cannot mask a transition defect.
                if let Err(error) = replay(&fixture, damage, access, &events, true) {
                    panic!(
                        "{shape:?} {damage:?} {access:?} seed={seed}: {error}\ntrace={events:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn short_tail_schedules() {
    campaign(Shape::ShortTail);
}
#[test]
fn multiple_files_and_empty_schedules() {
    campaign(Shape::MultipleFiles);
}
#[test]
fn nested_path_schedules() {
    campaign(Shape::Nested);
}
#[test]
fn duplicate_content_schedules() {
    campaign(Shape::DuplicateContents);
}
#[test]
fn beyond_quick_hash_schedules() {
    campaign(Shape::BeyondQuickHash);
}

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
// over a small one: one source's partial and final publication, one
// carrier's malformed, valid and repeated publication, and an invalidation
// followed by a reopen, in every one of their 210 interleavings, under every
// damage and access mode. The rest of the recovery the final repair needs
// arrives after the interleaving, in a fixed order, so each damage finishes
// against the same recovery whichever order came first.
fn exhaustive_campaign(access: Access) {
    let fixture = Fixture::with_recovery(Shape::ShortTail, 0x45584841, 4);
    let mut orders = Vec::new();
    interleavings(
        &mut [
            vec![Event::Source(0, 1), Event::Source(0, 2)],
            vec![
                Event::Recovery(0, 0),
                Event::Recovery(0, 1),
                Event::Recovery(0, 2),
            ],
            vec![Event::Invalidate, Event::Reopen],
        ],
        &mut Vec::new(),
        &mut orders,
    );
    assert_eq!(orders.len(), 210);
    for damage in [
        Damage::Clean,
        Damage::Corrupt,
        Damage::Truncated,
        Damage::Missing,
        Damage::Shifted,
        Damage::Renamed,
        Damage::Exhausted,
    ] {
        if matches!(access, Access::Snapshot) && matches!(damage, Damage::Shifted | Damage::Renamed)
        {
            continue;
        }
        // An exhausted set keeps the one carrier the interleaving delivers.
        let rest = if matches!(damage, Damage::Exhausted) {
            1
        } else {
            fixture.carriers.len()
        };
        for (case, order) in orders.iter().enumerate() {
            let mut events = order.clone();
            events.extend((1..rest).map(|i| Event::Recovery(i, 1)));
            if let Err(error) = replay(&fixture, damage, access, &events, true) {
                let reduced = minimize(&fixture, damage, access, &events, &error);
                panic!(
                    "{damage:?} {access:?} case={case}: {error}\ntrace={events:?}\nreduced trace={reduced:?}"
                );
            }
        }
    }
}

#[test]
fn every_interleaving_from_disk() {
    exhaustive_campaign(Access::Disk);
}
#[test]
fn every_interleaving_from_snapshot() {
    exhaustive_campaign(Access::Snapshot);
}

#[test]
fn every_damage_and_recovery_subset_of_six_blocks() {
    let mut random = Random(0x50415232);
    let files: Vec<(String, Vec<u8>)> = (0..2)
        .map(|i| {
            (
                format!("file-{i}.bin"),
                (0..192).map(|_| random.next() as u8).collect(),
            )
        })
        .collect();
    let borrowed = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect::<Vec<_>>();
    let full = build_repairable_par2_set_for_files(&borrowed, 64, 6);
    for damaged in 0u32..64 {
        for recovery in 0u32..64 {
            let root = tempfile::tempdir().unwrap();
            let mut before = Vec::new();
            for (i, (name, original)) in files.iter().enumerate() {
                let mut bytes = original.clone();
                for slice in 0..3 {
                    if damaged & (1 << (i * 3 + slice)) != 0 {
                        bytes[slice * 64..slice * 64 + 64].fill(0);
                    }
                }
                std::fs::write(root.path().join(name), &bytes).unwrap();
                before.push(bytes);
            }
            let mut set = full.clone();
            set.recovery_slices
                .retain(|exponent, _| recovery & (1 << exponent) != 0);
            let mut options = Par2RepairSessionOptions::new(root.path().to_path_buf(), vec![]);
            options.file_set = Some(set);
            options.memory_limit = Some(64 << 20);
            options.retained_state_limit = 32 << 20;
            let mut session = Par2RepairSession::open(options).unwrap();
            let assessment = session.assess().unwrap();
            assert_eq!(
                assessment.missing_blocks,
                damaged.count_ones(),
                "damage={damaged:06b} recovery={recovery:06b}"
            );
            assert_eq!(assessment.recovery_blocks_available, recovery.count_ones());
            let sufficient = recovery.count_ones() >= damaged.count_ones();
            let expected = if damaged == 0 {
                Par2RepairStatus::Verified
            } else if sufficient {
                Par2RepairStatus::RepairPossible
            } else {
                Par2RepairStatus::Insufficient
            };
            assert_eq!(
                assessment.status, expected,
                "damage={damaged:06b} recovery={recovery:06b}"
            );
            let repaired = session.execute(()).unwrap();
            assert_eq!(
                repaired.status,
                if damaged == 0 {
                    Par2RepairStatus::Verified
                } else if sufficient {
                    Par2RepairStatus::Repaired
                } else {
                    Par2RepairStatus::Insufficient
                },
                "damage={damaged:06b} recovery={recovery:06b}"
            );
            for (i, (name, original)) in files.iter().enumerate() {
                assert_eq!(
                    std::fs::read(root.path().join(name)).unwrap(),
                    if sufficient { original } else { &before[i] }.as_slice(),
                    "damage={damaged:06b} recovery={recovery:06b} file={name}"
                );
            }
        }
    }
}

#[test]
fn cancellation_at_each_native_operation_boundary_preserves_sources() {
    for stage in 0..3 {
        let fixture = Fixture::new(Shape::ShortTail, 91);
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join(&fixture.files[0].0);
        let mut damaged = fixture.files[0].1.clone();
        damaged[1] ^= 0x5a;
        std::fs::write(&path, &damaged).unwrap();
        let cancel = par2_rs::CancellationToken::new();
        let mut options = Par2RepairSessionOptions::new(root.path().to_path_buf(), vec![]);
        options.file_set = Some(fixture.set.clone());
        options.memory_limit = Some(64 << 20);
        options.retained_state_limit = 32 << 20;
        options.cancel = Some(cancel.clone());
        let result = if stage == 0 {
            cancel.cancel();
            Par2RepairSession::open(options).and_then(|mut session| session.assess())
        } else {
            let mut session = Par2RepairSession::open(options).unwrap();
            if stage == 2 {
                assert_eq!(
                    session.assess().unwrap().status,
                    Par2RepairStatus::RepairPossible
                );
            }
            cancel.cancel();
            if stage == 1 {
                session.assess()
            } else {
                session.execute(())
            }
        };
        assert!(
            matches!(
                result,
                Err(par2_rs::Par2SessionError::Par2(
                    par2_rs::Par2Error::Cancelled
                ))
            ),
            "stage={stage}: {result:?}"
        );
        assert_eq!(std::fs::read(path).unwrap(), damaged);
    }
}

#[test]
fn every_recovery_packet_byte_and_truncation_preserves_the_valid_following_copy() {
    let fixture = Fixture::new(Shape::ShortTail, 0x5041434b4554);
    let valid = &fixture.carriers[0];
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("mixed.par2");
    // Each bit and each possible truncation of a recovery packet, before and
    // after a valid copy. Scanner resynchronization must retain the valid one.
    for mutation in 0..valid.len() * 9 {
        let mut bad = valid.clone();
        if mutation < valid.len() * 8 {
            bad[mutation / 8] ^= 1 << (mutation % 8);
        } else {
            bad.truncate(mutation - valid.len() * 8);
        }
        for bad_first in [false, true] {
            let mut bytes = fixture.index.clone();
            if bad_first {
                bytes.extend_from_slice(&bad);
                bytes.extend_from_slice(valid);
            } else {
                bytes.extend_from_slice(valid);
                bytes.extend_from_slice(&bad);
            }
            std::fs::write(&path, bytes).unwrap();
            let budget = Arc::new(std::sync::Mutex::new(super::super::Par2ScanBudget::new(
                par2_rs::PacketScanLimits::default(),
                Arc::new(crate::pipeline::ProcessMemoryBudget::new(64 << 20)),
                par2_rs::CancellationToken::new(),
            )));
            let groups = super::super::scan_completed_par2_packet_groups(&path, &budget).unwrap();
            let accepted = groups
                .iter()
                .flat_map(|group| &group.packets)
                .filter_map(|packet| {
                    if let par2_rs::packet::Packet::RecoverySlice(slice) = packet {
                        Some(slice)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(
                accepted.len(),
                1,
                "mutation={mutation} bad_first={bad_first}"
            );
            assert_eq!(
                accepted[0].data.to_vec().unwrap(),
                fixture.set.recovery_slices[&accepted[0].exponent]
                    .data
                    .as_bytes()
                    .unwrap(),
                "mutation={mutation} bad_first={bad_first}"
            );
        }
    }
}
