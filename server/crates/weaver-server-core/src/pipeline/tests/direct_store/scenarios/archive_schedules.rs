// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Bounded exhaustive schedules over four articles: one, two or four volumes,
// every arrival permutation, loss subset and single duplicate/interruption.
// This enumerates delivery boundaries, not background worker or filesystem
// interleavings; those require separate tests that force the competing events.
use super::*;

fn enable_schedule_trace() {
    if std::env::var_os("WEAVER_ARCHIVE_SCHEDULE_TRACE").is_none() {
        return;
    }
    struct Trace(std::sync::atomic::AtomicU64);
    impl tracing::Subscriber for Trace {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(
                self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1,
            )
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Fields(String);
            impl tracing::field::Visit for Fields {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    use std::fmt::Write;
                    let _ = write!(self.0, " {field}={value:?}");
                }
            }
            let mut fields = Fields(String::new());
            event.record(&mut fields);
            eprintln!("{}{}", event.metadata().target(), fields.0);
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }
    let _ = tracing::subscriber::set_global_default(Trace(std::sync::atomic::AtomicU64::new(0)));
}

mod extended;
pub(in crate::pipeline::tests) mod graph;
pub(in crate::pipeline::tests) mod grouping;

pub(super) fn arrival_orders() -> Vec<Vec<(u32, u32)>> {
    fn permute(at: usize, items: &mut [(u32, u32)], output: &mut Vec<Vec<(u32, u32)>>) {
        if at == items.len() {
            output.push(items.to_vec());
            return;
        }
        for i in at..items.len() {
            items.swap(at, i);
            permute(at + 1, items, output);
            items.swap(at, i);
        }
    }
    let mut orders = Vec::new();
    permute(0, &mut [(0, 0), (0, 1), (1, 0), (1, 1)], &mut orders);
    assert_eq!(orders.len(), 24);
    orders
}

pub(super) fn duplicate_orders() -> Vec<Vec<(u32, u32)>> {
    let mut result = vec![];
    for order in arrival_orders() {
        result.push(order.clone());
        for at in 0..3 {
            let mut duplicate = order.clone();
            duplicate.insert(at, order[at]);
            result.push(duplicate);
        }
    }
    result
}

// Articles per volume for `slots` article slots over `volumes` volumes: four
// split evenly, and a fifth carried by the first volume.
fn slot_layout(volumes: usize, slots: usize) -> Vec<usize> {
    assert!(
        4 % volumes == 0 && matches!(slots, 4 | 5),
        "{volumes} x {slots}"
    );
    let mut layout = vec![4 / volumes; volumes];
    layout[0] += slots - 4;
    layout
}

// A schedule names slot `file * 2 + article` whatever the volumes' own
// article counts; slots number the volumes' articles in file order.
fn slot_article(layout: &[usize], slot: u32) -> (u32, u32) {
    let mut first = 0;
    for (file, &articles) in layout.iter().enumerate() {
        if slot < first + articles as u32 {
            return (file as u32, slot - first);
        }
        first += articles as u32;
    }
    panic!("slot {slot} beyond {layout:?}");
}

fn article_slot(layout: &[usize], file: u32, article: u32) -> u32 {
    layout[..file as usize].iter().sum::<usize>() as u32 + article
}

// Every slot once, in order, as a schedule names it.
fn slot_arrivals(slots: usize) -> Vec<(u32, u32)> {
    (0..slots as u32).map(|slot| (slot / 2, slot % 2)).collect()
}

fn files_under(root: &Path) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                found.insert(
                    path.strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .replace('\\', "/"),
                );
            }
        }
    }
    found
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum BoundaryAction {
    None,
    Restart,
    // The process dies: no shutdown barrier, so the restart finds whatever
    // coverage the last barrier of its own happened to publish.
    Crash,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Interruption {
    None,
    Restart(usize),
    Crash(usize),
    Loss {
        mask: u8,
        index_first: bool,
    },
    Combined {
        mask: u8,
        index_first: bool,
        action: BoundaryAction,
        at: usize,
    },
    // Loss with a recovery set that describes the damage and cannot mend it.
    Starved {
        mask: u8,
    },
    // Two boundary actions in one run, the first never after the second.
    // At a shared boundary both happen before that boundary's arrival, in
    // order. An empty mask loses nothing and posts no recovery set.
    Twice {
        mask: u8,
        index_first: bool,
        first: BoundaryAction,
        first_at: usize,
        second: BoundaryAction,
        second_at: usize,
    },
    // A volume the post never carried: the NZB does not list it and no
    // article of it arrives, while the recovery set and any listing still
    // describe it. The index leads when `index_first`, else it trails.
    Withheld {
        volume: u32,
        index_first: bool,
    },
}

// Which slice of a campaign one test runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Selection {
    // The default suite's bounded sample.
    Smoke,
    // One shard of the combined matrix.
    Shard(usize),
    // A wrong password across every arrival order and interruption boundary.
    WrongPassword,
}

// Shards the combined matrix is cut into, each its own test.
pub(super) const SHARDS: usize = 64;

impl Interruption {
    fn loss(self) -> Option<(u8, bool)> {
        match self {
            Self::Loss { mask, index_first }
            | Self::Combined {
                mask, index_first, ..
            } => Some((mask, index_first)),
            Self::Starved { mask } => Some((mask, false)),
            Self::Twice {
                mask, index_first, ..
            } if mask != 0 => Some((mask, index_first)),
            _ => None,
        }
    }

    // The volume a withheld schedule leaves out of the post, and whether the
    // recovery index leads.
    pub(super) fn withheld(self) -> Option<(u32, bool)> {
        match self {
            Self::Withheld {
                volume,
                index_first,
            } => Some((volume, index_first)),
            _ => None,
        }
    }

    // Whether the schedule can be run over a set of `volumes`: a withheld
    // volume has to exist, and has to leave another posted, since a job that
    // posts no volume at all is no archive job.
    pub(super) fn fits(self, volumes: usize) -> bool {
        self.withheld()
            .is_none_or(|(volume, _)| volumes > 1 && (volume as usize) < volumes)
    }

    // The job cannot finish: what was lost is beyond the recovery it has.
    pub(super) fn fails(self) -> bool {
        matches!(self, Self::Starved { .. })
    }

    fn action_at(self, boundary: usize) -> BoundaryAction {
        match self {
            Self::Restart(at) if at == boundary => BoundaryAction::Restart,
            Self::Crash(at) if at == boundary => BoundaryAction::Crash,
            Self::Combined { action, at, .. } if at == boundary => action,
            _ => BoundaryAction::None,
        }
    }

    // Each boundary of a run over `arrivals` articles, with the action taken
    // there and whether that boundary's arrival follows it. A boundary with
    // two actions appears twice, and its arrival follows only the second.
    fn boundaries(self, arrivals: usize) -> Vec<(usize, BoundaryAction, bool)> {
        let mut boundaries = Vec::new();
        for step in 0..=arrivals {
            match self {
                Self::Twice {
                    first,
                    first_at,
                    second,
                    second_at,
                    ..
                } => {
                    if first_at == step {
                        boundaries.push((step, first, second_at != step));
                    }
                    if second_at == step {
                        boundaries.push((step, second, true));
                    }
                    if first_at != step && second_at != step {
                        boundaries.push((step, BoundaryAction::None, true));
                    }
                }
                _ => boundaries.push((step, self.action_at(step), true)),
            }
        }
        boundaries
    }
}

pub(super) type Schedule = (Vec<(u32, u32)>, Interruption);

pub(super) fn combined_schedules(shard: usize, shards: usize) -> Vec<(usize, Schedule)> {
    assert!(shard < shards);
    combined_schedule_cases()
        .into_iter()
        .filter(|(case, _)| case % shards == shard)
        .collect()
}

// A campaign's cases for one selection, by replay index.
pub(super) fn selected_schedules(selection: Selection) -> Vec<(usize, Schedule)> {
    match selection {
        // A withheld volume is the grouping cross's axis, where each cell
        // rules what its binding makes of it; the hand-named layouts are
        // aliases of cells and leave it there.
        Selection::Smoke => schedules()
            .into_iter()
            .enumerate()
            .filter(|(_, (_, interruption))| interruption.withheld().is_none())
            .collect(),
        Selection::Shard(shard) => combined_schedules(shard, SHARDS),
        Selection::WrongPassword => Vec::new(),
    }
}

// A wrong password's schedules: every arrival order, uninterrupted and
// interrupted at every boundary. The default suite keeps the uninterrupted
// orders; the matrix runs them all as a test of its own.
pub(super) fn wrong_password_schedules(selection: Selection) -> Vec<Schedule> {
    let mut result = Vec::new();
    let every = selection == Selection::WrongPassword;
    if selection == Selection::Smoke || every {
        for order in arrival_orders() {
            result.push((order.clone(), Interruption::None));
            if every {
                for at in 0..order.len() {
                    result.push((order.clone(), Interruption::Restart(at)));
                    result.push((order.clone(), Interruption::Crash(at)));
                }
            }
        }
    }
    result
}

pub(super) fn combined_schedule_cases() -> Vec<(usize, Schedule)> {
    let mut orders = std::collections::BTreeSet::new();
    for order in arrival_orders() {
        orders.insert(order.clone());
        // Repeat any earlier article at every nonterminal insertion point.
        for at in 1..order.len() {
            for &article in &order[..at] {
                let mut duplicate = order.clone();
                duplicate.insert(at, article);
                orders.insert(duplicate);
            }
        }
    }
    assert_eq!(orders.len(), 168);
    let mut cases = std::collections::BTreeSet::new();
    for mask in 1u8..16 {
        for order in &orders {
            let available =
                |&&(file, article): &&(u32, u32)| mask & (1 << (file * 2 + article)) == 0;
            let received = order.iter().filter(available).copied().collect::<Vec<_>>();
            for index_first in [false, true] {
                cases.insert((
                    received.clone(),
                    Interruption::Combined {
                        mask,
                        index_first,
                        action: BoundaryAction::None,
                        at: 0,
                    },
                ));
                for boundary in 0..=order.len() {
                    let at = order[..boundary].iter().filter(available).count();
                    cases.insert((
                        received.clone(),
                        Interruption::Combined {
                            mask,
                            index_first,
                            action: BoundaryAction::Restart,
                            at,
                        },
                    ));
                }
            }
        }
    }
    // Lost arrivals have no publication effect. Identical received-event and
    // interruption sequences are one case, regardless of which lost item was
    // attempted first. Keep all distinct boundaries, including empty prefixes.
    assert_eq!(cases.len(), 2484);
    let mut cases: Vec<_> = cases.into_iter().collect();
    // Also cross delayed duplicates with interruption when nothing was lost.
    // A complete clean arrival sequence may already finalize the job, so its
    // interruption boundaries stop before the last distinct article arrives.
    // Append these cases to keep the repair cases' replay indices stable.
    for order in orders {
        cases.push((order.clone(), Interruption::None));
        for at in 0..order.len() {
            cases.push((order.clone(), Interruption::Restart(at)));
        }
    }
    assert_eq!(cases.len(), 3468);
    // A crash at every boundary a restart was tried at, appended for the same
    // reason: the cases above keep their replay indices.
    let mut crashes = std::collections::BTreeSet::new();
    for (received, interruption) in &cases {
        match *interruption {
            Interruption::Combined {
                mask,
                index_first,
                action: BoundaryAction::Restart,
                at,
            } => {
                crashes.insert((
                    received.clone(),
                    Interruption::Combined {
                        mask,
                        index_first,
                        action: BoundaryAction::Crash,
                        at,
                    },
                ));
            }
            Interruption::Restart(at) => {
                crashes.insert((received.clone(), Interruption::Crash(at)));
            }
            _ => {}
        }
    }
    cases.extend(crashes);
    assert_eq!(cases.len(), 6318);
    // Every distinct received sequence again, with nothing to repair it from.
    let starved: std::collections::BTreeSet<_> = cases
        .iter()
        .filter_map(|(received, interruption)| match *interruption {
            Interruption::Combined {
                mask,
                action: BoundaryAction::None,
                ..
            } => Some((received.clone(), Interruption::Starved { mask })),
            _ => None,
        })
        .collect();
    cases.extend(starved);
    assert_eq!(cases.len(), 6543);
    let selected = std::env::var("WEAVER_ARCHIVE_COMBINED_CASE")
        .ok()
        .map(|selection| {
            if let Some((start, end)) = selection.split_once("..") {
                start.parse::<usize>().expect("decimal range start")
                    ..end.parse::<usize>().expect("decimal range end")
            } else {
                let case = selection.parse::<usize>().expect("decimal combined case");
                case..case.checked_add(1).expect("combined case in range")
            }
        });
    assert!(
        selected
            .as_ref()
            .is_none_or(|range| range.start < range.end && range.end <= cases.len())
    );
    cases
        .into_iter()
        .enumerate()
        .filter(|(case, _)| selected.as_ref().is_none_or(|range| range.contains(case)))
        .collect()
}

pub(super) fn schedules() -> Vec<(Vec<(u32, u32)>, Interruption)> {
    let mut result: Vec<_> = duplicate_orders()
        .into_iter()
        .map(|order| (order, Interruption::None))
        .collect();
    for order in arrival_orders() {
        for at in 1..4 {
            result.push((order.clone(), Interruption::Restart(at)));
        }
    }
    for mask in 1..16 {
        for index_first in [false, true] {
            result.push((
                in_order_arrivals(2),
                Interruption::Loss { mask, index_first },
            ));
        }
    }
    // Appended, so the cases above keep their replay indices.
    for order in arrival_orders() {
        for at in 1..4 {
            result.push((order.clone(), Interruption::Crash(at)));
        }
    }
    for mask in 1..16 {
        result.push((in_order_arrivals(2), Interruption::Starved { mask }));
    }
    assert_eq!(result.len(), 285);
    // Appended for the same reason: each volume a set can be written as,
    // never posted, with the index leading and trailing. A volume beyond the
    // set's own is skipped where the set is known.
    for volume in 0..4 {
        for index_first in [false, true] {
            result.push((
                in_order_arrivals(2),
                Interruption::Withheld {
                    volume,
                    index_first,
                },
            ));
        }
    }
    if let Ok(case) = std::env::var("WEAVER_ARCHIVE_SCHEDULE_CASE") {
        let case = case
            .parse::<usize>()
            .expect("decimal archive schedule index");
        vec![
            result
                .get(case)
                .expect("archive schedule index in range")
                .clone(),
        ]
    } else {
        result
    }
}

fn retire_schedule_article(pipeline: &mut Pipeline, job: JobId, file: u32, article: u32) {
    let id = SegmentId {
        file_id: NzbFileId {
            job_id: job,
            file_index: file,
        },
        segment_number: article,
    };
    // A job that already reached a terminal state has no queues to retire
    // from; the schedule keeps delivering so the run can settle.
    let Some(state) = pipeline.jobs.get_mut(&job) else {
        return;
    };
    // The schedule owns delivery, including deliberate duplicates. Retire a
    // queued copy when present so a later drain cannot fabricate a rewrite.
    for queue in [&mut state.download_queue, &mut state.recovery_queue] {
        let queued = queue.drain_all();
        for work in queued {
            if work.segment_id != id {
                queue.push(work);
            }
        }
    }
}

// A recovery file whose bytes the schedule holds, to hand over when due.
struct ScheduleRecovery {
    index: u32,
    name: String,
    bytes: Vec<u8>,
    format: RecoveryFormat,
}

async fn submit_schedule_recovery(
    pipeline: &mut Pipeline,
    job: JobId,
    recovery: &ScheduleRecovery,
    article: u32,
) {
    retire_schedule_article(pipeline, job, recovery.index, article);
    let file = NzbFileId {
        job_id: job,
        file_index: recovery.index,
    };
    match recovery.format {
        RecoveryFormat::Par2 | RecoveryFormat::Par2Ladder => {
            submit_decoded_segment(
                pipeline,
                file,
                article,
                0,
                &recovery.bytes,
                &recovery.name,
                None,
            )
            .await;
        }
        // A carrier states its true length, as a poster's does.
        RecoveryFormat::Par3 => {
            submit_decoded_segment_declaring(
                pipeline,
                file,
                article,
                0,
                &recovery.bytes,
                &recovery.name,
                None,
                true,
                None,
                recovery.bytes.len() as u64,
            )
            .await;
        }
        RecoveryFormat::Absent => {
            unreachable!("an absent set posts no recovery file")
        }
    }
}

// What a schedule's recovery set is authored as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::pipeline::tests) enum RecoveryFormat {
    // One PAR2 file carrying the descriptions and every recovery block.
    Par2,
    // The same blocks as a poster lays them out: an index of descriptions
    // alone, then volumes of doubling block counts. The index arrives where
    // the PAR2 file would; a volume arrives when the pipeline asks.
    Par2Ladder,
    // A PAR3 index and its recovery volumes. The index arrives where the
    // PAR2 file would; a recovery volume arrives when the pipeline asks.
    Par3,
    // Nothing is posted besides the volumes: no index names them and no
    // recovery block can mend a loss.
    Absent,
}

// How a schedule is driven besides its arrivals and its interruption.
#[derive(Clone, Copy, Debug)]
pub(super) struct ScheduleOptions {
    pub recovery: RecoveryFormat,
    // The recovery index arrives after every volume article, whatever the
    // schedule's loss would otherwise lead with: too late to name a volume
    // before its bytes land.
    pub index_last: bool,
    // The recovery index arrives ahead of every volume article, whatever the
    // schedule's loss would otherwise lead with.
    pub index_first: bool,
    // An `.sfv` listing the described names (the posted ones when nothing is
    // described) is posted too, and arrives where the index would.
    pub sfv: bool,
}

impl ScheduleOptions {
    // What every campaign of the archive matrix proper runs under.
    pub(super) const MATRIX: Self = Self {
        recovery: RecoveryFormat::Par2,
        index_last: false,
        index_first: false,
        sfv: false,
    };
}

// What a schedule posts of an archive written as `written`: the volumes the
// NZB carries, each one's article count, and the arrivals as articles of
// those volumes. A withheld volume leaves the spec, and its slots name
// nothing.
type PostedVolumes = (Vec<(String, Vec<u8>)>, Vec<usize>, Vec<(u32, u32)>);

fn posted_schedule(
    spec: &mut JobSpec,
    written: &[(String, Vec<u8>)],
    order: &[(u32, u32)],
    interruption: Interruption,
) -> PostedVolumes {
    let layout: Vec<usize> = spec.files[..written.len()]
        .iter()
        .map(|file| file.segments.len())
        .collect();
    assert_eq!(
        layout,
        slot_layout(written.len(), layout.iter().sum()),
        "four or five scheduled article slots"
    );
    let order: Vec<_> = order
        .iter()
        .map(|&(file, article)| slot_article(&layout, file * 2 + article))
        .collect();
    let withheld = interruption.withheld();
    match withheld {
        Some((volume, _)) => {
            assert!(interruption.fits(written.len()), "{interruption:?}");
            let at = volume as usize;
            let removed = spec.files.remove(at);
            spec.total_bytes -= removed
                .segments
                .iter()
                .map(|segment| u64::from(segment.bytes))
                .sum::<u64>();
            let mut posted = written.to_vec();
            posted.remove(at);
            let mut layout = layout;
            layout.remove(at);
            // The withheld volume's slots name nothing; every later volume
            // is one file earlier in the NZB.
            let order = order
                .into_iter()
                .filter(|&(file, _)| file != volume)
                .map(|(file, article)| (file - u32::from(file > volume), article))
                .collect();
            (posted, layout, order)
        }
        None => (written.to_vec(), layout, order),
    }
}

// The recovery a schedule posts over every volume `written`, appended to
// `spec` with the index first: a set with blocks enough to mend what the
// schedule loses, none when it starves, and the listing when asked. And
// whether the index arrives ahead of the volumes.
fn schedule_recovery(
    options: ScheduleOptions,
    spec: &mut JobSpec,
    written: &[(String, Vec<u8>)],
    described: Option<&[String]>,
    interruption: Interruption,
) -> (Vec<ScheduleRecovery>, bool) {
    let loss = interruption.loss();
    let withheld = interruption.withheld();
    let index_first = options.index_first
        || (!options.index_last
            && loss
                .map(|(_, first)| first)
                .or(withheld.map(|(_, first)| first))
                .unwrap_or(described.is_some()));
    let mut recovery = if options.recovery == RecoveryFormat::Absent {
        Vec::new()
    } else if loss.is_some() || withheld.is_some() || described.is_some() {
        // Keep several repair blocks per article even for larger compressed
        // fixtures, without turning extraction scheduling into a codec benchmark.
        let slice = written
            .iter()
            .map(|(_, bytes)| bytes.len())
            .max()
            .unwrap()
            .div_ceil(32)
            .div_ceil(4)
            * 4;
        let slice = slice.max(PAR2_SLICE_BYTES as usize);
        let blocks: usize = written
            .iter()
            .map(|(_, bytes)| bytes.len().div_ceil(slice))
            .sum();
        let described: Vec<_> = written
            .iter()
            .enumerate()
            .map(|(index, (name, bytes))| {
                let name = described.map_or(name.as_str(), |names| names[index].as_str());
                (name, bytes.as_slice())
            })
            .collect();
        let blocks = if interruption.fails() || (loss.is_none() && withheld.is_none()) {
            0
        } else {
            blocks
        };
        match options.recovery {
            RecoveryFormat::Par2 => {
                let bytes = build_test_par2_with_recovery(&described, slice as u64, blocks);
                let index = append_par2_index(spec, &bytes);
                vec![ScheduleRecovery {
                    index,
                    name: "silver.horizon.par2".to_string(),
                    bytes,
                    format: RecoveryFormat::Par2,
                }]
            }
            RecoveryFormat::Par2Ladder => {
                let carriers =
                    build_test_par2_ladder(&described, slice as u64, blocks, "silver.horizon");
                let indices = append_single_article_files(spec, &carriers);
                indices
                    .into_iter()
                    .zip(carriers)
                    .map(|(index, (name, bytes))| ScheduleRecovery {
                        index,
                        name,
                        bytes,
                        format: RecoveryFormat::Par2Ladder,
                    })
                    .collect()
            }
            RecoveryFormat::Par3 => {
                let described: Vec<_> = described
                    .iter()
                    .map(|(name, bytes)| (name.to_string(), bytes.to_vec()))
                    .collect();
                let mut carriers = par3_carriers_over(&described, slice as u64, blocks);
                // The index leads: it is what arrives where a PAR2 file would.
                carriers.sort_by_key(|(name, _)| name.contains(".vol"));
                let indices = append_single_article_files(spec, &carriers);
                indices
                    .into_iter()
                    .zip(carriers)
                    .map(|(index, (name, bytes))| ScheduleRecovery {
                        index,
                        name,
                        bytes,
                        format: RecoveryFormat::Par3,
                    })
                    .collect()
            }
            RecoveryFormat::Absent => unreachable!("handled above"),
        }
    } else {
        Vec::new()
    };
    if options.sfv {
        // A listing checks content; it carries no recovery block and, posted
        // alone, it arrives where an index would.
        let listing: String = written
            .iter()
            .enumerate()
            .map(|(index, (name, bytes))| {
                let name = described.map_or(name.as_str(), |names| names[index].as_str());
                format!("{name} {:08x}\n", checksum::crc32(bytes))
            })
            .collect();
        let carrier = ("silver.horizon.sfv".to_string(), listing.into_bytes());
        let index = append_single_article_files(spec, std::slice::from_ref(&carrier))[0];
        recovery.push(ScheduleRecovery {
            index,
            name: carrier.0,
            bytes: carrier.1,
            // One whole file in one article, submitted as a PAR2 file is.
            format: RecoveryFormat::Par2,
        });
    }
    (recovery, index_first)
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Format {
    Rar4,
    Rar5,
    Rar4Encrypted,
    Rar4Unsalted,
    Rar5Encrypted,
    Rar5KeyedChecksum,
    Rar5EncryptedHeaders,
    Rar5UncheckedHeaders,
    QuickOpen,
    Blake2,
    // Four single-article volumes, so two of the volumes are middle volumes
    // that both continue and are continued.
    Rar4FourVolumes,
    Rar5FourVolumes,
    Rar4EncryptedFourVolumes,
    // Volume names that say nothing. The recovery set carries the real
    // names, as an obfuscated post's does, and the set is admitted by them.
    Rar5Obfuscated,
    Rar4Obfuscated,
    // The rest take that crutch away: nothing in a volume's name, and nothing
    // a recovery set says in time, groups the volumes or orders them. Only
    // the archive headers can.
    // 32-hex extensionless volumes and nothing else posted: no index names
    // them and nothing can mend a loss.
    Rar5HexBare,
    Rar4HexBare,
    // The recovery set describes the volumes under the same hex names they
    // are posted under, so binding them to it changes nothing.
    Rar5HexSelfDescribed,
    Rar4HexSelfDescribed,
    // The recovery set carries the real names but arrives after every
    // volume article, too late to name a volume before its bytes land.
    Rar5HexLateIndex,
    Rar4HexLateIndex,
    // A hex stem with numeric extensions that misstate the order: the first
    // volume is `.002` and the second `.001`.
    Rar5HexMisnumbered,
    Rar4HexMisnumbered,
    // A hex stem whose `.rar` and `.r00` are swapped: the first volume is
    // the `.r00`.
    Rar5HexSwappedRar,
    Rar4HexSwappedRar,
    // Four volumes, each under an unrelated hex name with no shared stem,
    // in an order their names do not sort to.
    Rar5HexScattered,
    Rar4HexScattered,
    // A single-volume archive under a hex name.
    Rar5HexSingle,
    Rar4HexSingle,
}

impl Format {
    fn volume_count(self) -> usize {
        match self {
            Self::Rar4FourVolumes
            | Self::Rar5FourVolumes
            | Self::Rar4EncryptedFourVolumes
            | Self::Rar5HexScattered
            | Self::Rar4HexScattered => 4,
            Self::Rar5HexSingle | Self::Rar4HexSingle => 1,
            _ => 2,
        }
    }

    fn options(self) -> ScheduleOptions {
        match self {
            Self::Rar5HexBare | Self::Rar4HexBare => ScheduleOptions {
                recovery: RecoveryFormat::Absent,
                ..ScheduleOptions::MATRIX
            },
            Self::Rar5HexLateIndex | Self::Rar4HexLateIndex => ScheduleOptions {
                index_last: true,
                ..ScheduleOptions::MATRIX
            },
            _ => ScheduleOptions::MATRIX,
        }
    }

    // The names the volumes are posted under, given their real ones.
    fn posted_names(self, volumes: Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>)> {
        let stem = "5f0c9e2ab1d74c6e8a3f1b0d9c2e7a41";
        match self {
            Self::Rar5Obfuscated
            | Self::Rar4Obfuscated
            | Self::Rar5HexBare
            | Self::Rar4HexBare
            | Self::Rar5HexSelfDescribed
            | Self::Rar4HexSelfDescribed
            | Self::Rar5HexLateIndex
            | Self::Rar4HexLateIndex => obfuscate_volumes(&volumes),
            Self::Rar5HexMisnumbered | Self::Rar4HexMisnumbered => volumes
                .into_iter()
                .zip([".002", ".001"])
                .map(|((_, bytes), suffix)| (format!("{stem}{suffix}"), bytes))
                .collect(),
            Self::Rar5HexSwappedRar | Self::Rar4HexSwappedRar => volumes
                .into_iter()
                .zip([".r00", ".rar"])
                .map(|((_, bytes), suffix)| (format!("{stem}{suffix}"), bytes))
                .collect(),
            Self::Rar5HexScattered | Self::Rar4HexScattered => volumes
                .into_iter()
                .zip([
                    "e93a07c1d25b4f68a0c3e1f7b9d24c85",
                    "1b7f3d9e05a24c6f8e1d0b3a7c9f2e64",
                    "c40e8b2f6a1d4973b5e0f2c8d16a9b37",
                    "7a2c5e9f0b3d4e81a6f9c2b07d4e1f58",
                ])
                .map(|((_, bytes), name)| (name.to_string(), bytes))
                .collect(),
            Self::Rar5HexSingle | Self::Rar4HexSingle => volumes
                .into_iter()
                .map(|(_, bytes)| (stem.to_string(), bytes))
                .collect(),
            _ => volumes,
        }
    }
}

// The possible cells of the grouping cross, in the generator's form, handed to
// `$emit`: the macro that writes one consumer's tests for them.
macro_rules! grouping_listing {
    ($emit:ident) => {
        $emit! {
            smoke rar4 Rar4 {
                conventional Conventional [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                conventional_four ConventionalFour [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                single Single [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                mixed_case MixedCase [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                old_style OldStyle [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                hex_bare HexBare [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_single HexSingle [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_misnumbered HexMisnumbered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_swapped_rar HexSwappedRar [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_scattered HexScattered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_numbered_gap HexNumberedGap [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                first_volume_hex FirstVolumeHex [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                reposted Reposted [par2_posted Par2Posted nothing Nothing]
                hex_reposted HexReposted [par2_posted Par2Posted nothing Nothing]
            }
            smoke rar5 Rar5 {
                conventional Conventional [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                conventional_four ConventionalFour [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                single Single [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                mixed_case MixedCase [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                hex_bare HexBare [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_single HexSingle [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_misnumbered HexMisnumbered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_swapped_rar HexSwappedRar [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_scattered HexScattered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_numbered_gap HexNumberedGap [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                first_volume_hex FirstVolumeHex [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                reposted Reposted [par2_posted Par2Posted nothing Nothing]
                hex_reposted HexReposted [par2_posted Par2Posted nothing Nothing]
            }
            smoke rar5_encrypted Rar5Encrypted {
                conventional Conventional [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                conventional_four ConventionalFour [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                single Single [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                mixed_case MixedCase [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                hex_bare HexBare [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_single HexSingle [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_misnumbered HexMisnumbered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_swapped_rar HexSwappedRar [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_scattered HexScattered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_numbered_gap HexNumberedGap [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                first_volume_hex FirstVolumeHex [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                reposted Reposted [par2_posted Par2Posted nothing Nothing]
                hex_reposted HexReposted [par2_posted Par2Posted nothing Nothing]
            }
            smoke rar5_encrypted_headers Rar5EncryptedHeaders {
                conventional Conventional [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                conventional_four ConventionalFour [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                single Single [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                mixed_case MixedCase [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                hex_bare HexBare [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_single HexSingle [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_misnumbered HexMisnumbered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_swapped_rar HexSwappedRar [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_scattered HexScattered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_numbered_gap HexNumberedGap [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                first_volume_hex FirstVolumeHex [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                reposted Reposted [par2_posted Par2Posted nothing Nothing]
                hex_reposted HexReposted [par2_posted Par2Posted nothing Nothing]
            }
            smoke seven_zip SevenZip {
                conventional Conventional [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                conventional_four ConventionalFour [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                single Single [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                mixed_case MixedCase [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                hex_bare HexBare [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_single HexSingle [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_misnumbered HexMisnumbered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_scattered HexScattered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_numbered_gap HexNumberedGap [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                first_volume_hex FirstVolumeHex [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                reposted Reposted [par2_posted Par2Posted nothing Nothing]
                hex_reposted HexReposted [par2_posted Par2Posted nothing Nothing]
                bare_first_part BareFirstPart [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
            }
            smoke seven_zip_solid SevenZipSolid {
                conventional Conventional [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                conventional_four ConventionalFour [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                single Single [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                mixed_case MixedCase [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                hex_bare HexBare [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_single HexSingle [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_misnumbered HexMisnumbered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_scattered HexScattered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_numbered_gap HexNumberedGap [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                first_volume_hex FirstVolumeHex [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                reposted Reposted [par2_posted Par2Posted nothing Nothing]
                hex_reposted HexReposted [par2_posted Par2Posted nothing Nothing]
                bare_first_part BareFirstPart [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
            }
            smoke rar4_solid Rar4Solid {
                conventional Conventional [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                conventional_four ConventionalFour [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                single Single [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                mixed_case MixedCase [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                old_style OldStyle [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                hex_bare HexBare [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_single HexSingle [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_misnumbered HexMisnumbered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_swapped_rar HexSwappedRar [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_scattered HexScattered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                first_volume_hex FirstVolumeHex [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                reposted Reposted [par2_posted Par2Posted nothing Nothing]
                hex_reposted HexReposted [par2_posted Par2Posted nothing Nothing]
            }
            smoke rar4_solid_encrypted Rar4SolidEncrypted {
                conventional Conventional [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                conventional_four ConventionalFour [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                single Single [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                mixed_case MixedCase [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                old_style OldStyle [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                hex_bare HexBare [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_single HexSingle [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_misnumbered HexMisnumbered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_swapped_rar HexSwappedRar [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_scattered HexScattered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                first_volume_hex FirstVolumeHex [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                reposted Reposted [par2_posted Par2Posted nothing Nothing]
                hex_reposted HexReposted [par2_posted Par2Posted nothing Nothing]
            }
            smoke rar5_solid Rar5Solid {
                conventional Conventional [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                conventional_four ConventionalFour [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                single Single [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                mixed_case MixedCase [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                hex_bare HexBare [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_single HexSingle [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_misnumbered HexMisnumbered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_swapped_rar HexSwappedRar [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_scattered HexScattered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                first_volume_hex FirstVolumeHex [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                reposted Reposted [par2_posted Par2Posted nothing Nothing]
                hex_reposted HexReposted [par2_posted Par2Posted nothing Nothing]
            }
            smoke rar5_solid_encrypted Rar5SolidEncrypted {
                conventional Conventional [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                conventional_four ConventionalFour [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                single Single [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                mixed_case MixedCase [par2_real_first Par2RealFirst par2_real_last Par2RealLast sfv Sfv nothing Nothing]
                hex_bare HexBare [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_single HexSingle [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_misnumbered HexMisnumbered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_swapped_rar HexSwappedRar [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                hex_scattered HexScattered [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                first_volume_hex FirstVolumeHex [par2_real_first Par2RealFirst par2_real_last Par2RealLast par2_posted Par2Posted sfv Sfv nothing Nothing]
                reposted Reposted [par2_posted Par2Posted nothing Nothing]
                hex_reposted HexReposted [par2_posted Par2Posted nothing Nothing]
            }
        }
    };
}
pub(super) use grouping_listing;

// A layout over `slots` article slots (see [`slot_layout`]): its one member
// and that member's bytes, the volumes as posted, the names its recovery set
// describes them by, and the job, with the password when the archive needs
// one.
struct SlotFixture {
    name: &'static str,
    payload: Vec<u8>,
    volumes: Vec<(String, Vec<u8>)>,
    described: Option<Vec<String>>,
    encrypted: bool,
    spec: JobSpec,
}

fn slot_fixture(format: Format, slots: usize) -> SlotFixture {
    let name = "nested/feature.mkv";
    let password = "moonlit-harbour";
    let length = match format {
        Format::QuickOpen => 193,
        // A described volume is bound by the fingerprint of its first 16 KiB,
        // which its offset-zero article has to cover whole, as every real
        // article does. Two articles a volume puts that at 32 KiB a volume.
        Format::Rar5Obfuscated
        | Format::Rar4Obfuscated
        | Format::Rar5HexSelfDescribed
        | Format::Rar4HexSelfDescribed
        | Format::Rar5HexLateIndex
        | Format::Rar4HexLateIndex => 70_001,
        _ => 6001,
    };
    let payload: Vec<u8> = (0..length)
        .map(|n| ((n * 7 + n / 251) % 253) as u8)
        .collect();
    let count = format.volume_count();
    let volumes = match format {
        Format::Rar4
        | Format::Rar4FourVolumes
        | Format::Rar4Obfuscated
        | Format::Rar4HexBare
        | Format::Rar4HexSelfDescribed
        | Format::Rar4HexLateIndex
        | Format::Rar4HexMisnumbered
        | Format::Rar4HexSwappedRar
        | Format::Rar4HexSingle => {
            format.posted_names(single_member_rar4_store_set(name, &payload, count))
        }
        // The two middle volumes of an old-numbering set both open on the
        // tail of the one member and end on more of it, so nothing in their
        // headers says which comes first. A numbered set states it, as every
        // RAR 3.x and later writer does.
        Format::Rar4HexScattered => {
            format.posted_names(single_member_rar4_store_set_numbered(name, &payload, count))
        }
        Format::Rar5
        | Format::Rar5FourVolumes
        | Format::Rar5Obfuscated
        | Format::Rar5HexBare
        | Format::Rar5HexSelfDescribed
        | Format::Rar5HexLateIndex
        | Format::Rar5HexMisnumbered
        | Format::Rar5HexSwappedRar
        | Format::Rar5HexScattered
        | Format::Rar5HexSingle => {
            format.posted_names(single_member_store_set(name, &payload, count))
        }
        Format::Rar4Encrypted | Format::Rar4EncryptedFourVolumes => {
            encrypted_rar4_store_set(name, &payload, count, password, Some(TEST_RAR4_SALT))
        }
        Format::Rar4Unsalted => encrypted_rar4_store_set(name, &payload, 2, password, None),
        Format::Rar5Encrypted => {
            encrypted_store_set(name, &payload, 2, password, Some(password), false)
        }
        Format::Rar5KeyedChecksum => {
            encrypted_store_set(name, &payload, 2, password, Some(password), true)
        }
        Format::Rar5EncryptedHeaders => {
            header_encrypted_store_set(name, &payload, 2, password, HeaderCheck::For(password))
        }
        Format::Rar5UncheckedHeaders => {
            header_encrypted_store_set(name, &payload, 2, password, HeaderCheck::Absent)
        }
        Format::QuickOpen => quick_open_store_set(name, &payload, None),
        Format::Blake2 => blake2_store_set(
            name,
            &payload,
            2,
            unrar_rs::crypto::blake2sp_hash(&payload),
            true,
        ),
    };
    if matches!(format, Format::Blake2) {
        let mut archive = unrar_rs::RarArchive::open_volumes(
            volumes
                .iter()
                .map(|(_, bytes)| {
                    Box::new(std::io::Cursor::new(bytes.clone())) as Box<dyn unrar_rs::ReadSeek>
                })
                .collect(),
        )
        .unwrap();
        let mut extracted = Vec::new();
        archive
            .by_index(0)
            .unwrap()
            .copy_to(&mut extracted)
            .unwrap();
        assert_eq!(extracted, payload);
    }
    let described = match format {
        Format::Rar5Obfuscated | Format::Rar5HexLateIndex => {
            Some(single_member_store_set(name, &payload, 2))
        }
        Format::Rar4Obfuscated | Format::Rar4HexLateIndex => {
            Some(single_member_rar4_store_set(name, &payload, 2))
        }
        // Described, but under the very names it is posted under.
        Format::Rar5HexSelfDescribed | Format::Rar4HexSelfDescribed => Some(volumes.clone()),
        _ => None,
    }
    .map(|volumes| {
        volumes
            .into_iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>()
    });
    let encrypted = matches!(
        format,
        Format::Rar4Encrypted
            | Format::Rar4Unsalted
            | Format::Rar4EncryptedFourVolumes
            | Format::Rar5Encrypted
            | Format::Rar5KeyedChecksum
            | Format::Rar5EncryptedHeaders
            | Format::Rar5UncheckedHeaders
    );
    let mut spec = direct_store_job_spec_with_articles("Archive schedules", &volumes, 4 / count);
    for (file, articles) in slot_layout(count, slots).into_iter().enumerate() {
        if articles != spec.files[file].segments.len() {
            let volume = &volumes[file..=file];
            spec.files[file] = direct_store_job_spec_with_articles("", volume, articles)
                .files
                .remove(0);
        }
    }
    spec.password = encrypted.then(|| password.to_string());
    SlotFixture {
        name,
        payload,
        volumes,
        described,
        encrypted,
        spec,
    }
}

// The members of every two-set fixture, one a set, and their payloads.
pub(super) fn two_set_members() -> ([&'static str; 2], Vec<Vec<u8>>) {
    let payloads = [(6001, 7), (4093, 11)]
        .into_iter()
        .map(|(len, step)| {
            (0..len)
                .map(|n| ((n * step + n / 251) % 253) as u8)
                .collect()
        })
        .collect();
    (["alpha.mkv", "nested/beta.mkv"], payloads)
}

// Two single-volume stored sets in one job, two articles each: the first
// set in slots 0 and 1, the second in slots 2 and 3.
pub(super) fn two_set_fixture() -> TwoSetFixture {
    let (members, payloads) = two_set_members();
    let volumes: Vec<_> = ["alpha", "beta"]
        .into_iter()
        .zip(members.iter().zip(&payloads))
        .map(|(stem, (member, payload))| {
            let (_, bytes) = single_member_store_set(member, payload, 1).remove(0);
            (format!("{stem}.part01.rar"), bytes)
        })
        .collect();
    let spec = direct_store_job_spec("Two set schedules", &volumes);
    (members, payloads, volumes, spec)
}

// Two two-volume stored sets in one job, one article a volume, every volume
// under an unrelated hex name and the two sets' volumes posted interleaved.
// No name says which set a volume belongs to or where in it, so only the
// volumes' own headers can keep the sets apart; merged, neither extracts.
#[derive(Clone, Copy, Debug)]
pub(super) enum HexTwoSets {
    Rar5,
    Rar4,
}

impl HexTwoSets {
    // Posted order: the first set's volumes in slots 0 and 2, the second's
    // in slots 1 and 3.
    const NAMES: [&'static str; 4] = [
        "d3b07384d113edec49eaa6238ad5ff00",
        "0f2c6e9a4b8d1e3f5a7c9b0d2e4f6a81",
        "8e4a1c7f3b9d5e2a0c6f8b1d4e7a3c95",
        "26f9b0e3c7a14d58b2e6f0a9c3d7e1b4",
    ];
}

// The members, their payloads, the posted volumes and the job of a two-set
// fixture.
pub(super) type TwoSetFixture = (
    [&'static str; 2],
    Vec<Vec<u8>>,
    Vec<(String, Vec<u8>)>,
    JobSpec,
);

pub(super) fn hex_two_set_fixture(format: HexTwoSets) -> TwoSetFixture {
    let (members, payloads) = two_set_members();
    let sets: Vec<Vec<Vec<u8>>> = members
        .iter()
        .zip(&payloads)
        .map(|(member, payload)| {
            let volumes = match format {
                HexTwoSets::Rar5 => single_member_store_set(member, payload, 2),
                HexTwoSets::Rar4 => single_member_rar4_store_set(member, payload, 2),
            };
            volumes.into_iter().map(|(_, bytes)| bytes).collect()
        })
        .collect();
    let volumes: Vec<(String, Vec<u8>)> = [(0, 0), (1, 0), (0, 1), (1, 1)]
        .into_iter()
        .zip(HexTwoSets::NAMES)
        .map(|((set, part), name)| (name.to_string(), sets[set][part].clone()))
        .collect();
    let spec = direct_store_job_spec_with_articles("Hex two set schedules", &volumes, 1);
    (members, payloads, volumes, spec)
}

#[derive(Clone, Copy, Debug)]
pub(super) enum CompressedFormat {
    Rar4Mixed,
    Rar4Lz,
    Rar4Solid,
    Rar4SolidEncrypted,
    Rar4SolidHeaders,
    Rar4Ppmd,
    Rar4PpmdEncrypted,
    Rar4PpmdHeaders,
    Rar4Encrypted,
    Rar4Headers,
    Rar5Mixed,
    Rar5Lz,
    Rar5Encrypted,
    Rar5Headers,
    Rar5Solid,
    Rar5SolidEncrypted,
    Rar5SolidHeaders,
    // One compressed member spanning every volume of the set.
    Rar4LzTwoVolumes,
    Rar5LzTwoVolumes,
    Rar4LzFourVolumes,
    Rar5LzFourVolumes,
    // A solid stream continued across every volume, with members after the
    // spanning one; and data or headers encrypted across every volume.
    Rar4SolidTwoVolumes,
    Rar5SolidTwoVolumes,
    Rar4SolidFourVolumes,
    Rar5SolidFourVolumes,
    Rar4EncryptedTwoVolumes,
    Rar5EncryptedTwoVolumes,
    Rar4EncryptedFourVolumes,
    Rar5EncryptedFourVolumes,
    Rar4HeadersTwoVolumes,
    Rar5HeadersTwoVolumes,
    Rar4HeadersFourVolumes,
    Rar5HeadersFourVolumes,
    Rar4SolidEncryptedTwoVolumes,
    Rar5SolidEncryptedTwoVolumes,
    Rar4SolidEncryptedFourVolumes,
    Rar5SolidEncryptedFourVolumes,
}

impl CompressedFormat {
    // The posted volumes of a multi-volume set and its oracle key. Every
    // other format posts its one fixture archive whole.
    fn volumes(self) -> Option<(&'static str, Vec<&'static [u8]>)> {
        macro_rules! volumes {
            ($set:literal, $($part:literal),+) => {
                Some((
                    $set,
                    vec![$(include_bytes!(concat!(
                        env!("CARGO_MANIFEST_DIR"),
                        "/tests/fixtures/extraction_profiles/",
                        $set,
                        ".part",
                        $part,
                        ".rar"
                    ))
                    .as_slice()),+],
                ))
            };
        }
        match self {
            Self::Rar4LzTwoVolumes => volumes!("rar4_lz_two", "01", "02"),
            Self::Rar5LzTwoVolumes => volumes!("rar5_lz_two", "01", "02"),
            Self::Rar4LzFourVolumes => volumes!("rar4_lz_four", "01", "02", "03", "04"),
            Self::Rar5LzFourVolumes => volumes!("rar5_lz_four", "01", "02", "03", "04"),
            Self::Rar4SolidTwoVolumes => volumes!("rar4_solid_two", "01", "02"),
            Self::Rar5SolidTwoVolumes => volumes!("rar5_solid_two", "01", "02"),
            Self::Rar4SolidFourVolumes => volumes!("rar4_solid_four", "01", "02", "03", "04"),
            Self::Rar5SolidFourVolumes => volumes!("rar5_solid_four", "01", "02", "03", "04"),
            Self::Rar4EncryptedTwoVolumes => volumes!("rar4_enc_two", "01", "02"),
            Self::Rar5EncryptedTwoVolumes => volumes!("rar5_enc_two", "01", "02"),
            Self::Rar4EncryptedFourVolumes => volumes!("rar4_enc_four", "01", "02", "03", "04"),
            Self::Rar5EncryptedFourVolumes => volumes!("rar5_enc_four", "01", "02", "03", "04"),
            Self::Rar4HeadersTwoVolumes => volumes!("rar4_hp_two", "01", "02"),
            Self::Rar5HeadersTwoVolumes => volumes!("rar5_hp_two", "01", "02"),
            Self::Rar4HeadersFourVolumes => volumes!("rar4_hp_four", "01", "02", "03", "04"),
            Self::Rar5HeadersFourVolumes => volumes!("rar5_hp_four", "01", "02", "03", "04"),
            Self::Rar4SolidEncryptedTwoVolumes => volumes!("rar4_solid_enc_two", "01", "02"),
            Self::Rar5SolidEncryptedTwoVolumes => volumes!("rar5_solid_enc_two", "01", "02"),
            Self::Rar4SolidEncryptedFourVolumes => {
                volumes!("rar4_solid_enc_four", "01", "02", "03", "04")
            }
            Self::Rar5SolidEncryptedFourVolumes => {
                volumes!("rar5_solid_enc_four", "01", "02", "03", "04")
            }
            _ => None,
        }
    }

    // The archive the expected members are decoded from. A multi-volume set
    // packs the member its single-volume counterpart does, and the oracle
    // pins the bytes for the set under its own key.
    fn fixture(self) -> (&'static str, &'static [u8], Option<&'static str>) {
        // Real RAR encoders produced these archives. Embed them so the compiled
        // nextest archive remains self-contained on a matrix runner.
        macro_rules! fixture {
            ($name:literal, $password:expr $(,)?) => {
                (
                    $name,
                    include_bytes!(concat!(
                        env!("CARGO_MANIFEST_DIR"),
                        "/tests/fixtures/extraction_profiles/",
                        $name
                    ))
                    .as_slice(),
                    $password,
                )
            };
        }
        match self {
            Self::Rar4Mixed => fixture!("rar4_multifile_lz.rar", None),
            Self::Rar4Lz | Self::Rar4LzTwoVolumes | Self::Rar4LzFourVolumes => {
                fixture!("rar4_lz.rar", None)
            }
            Self::Rar4Solid => fixture!("rar4_lz_solid_mv.rar", None),
            Self::Rar4SolidEncrypted => {
                fixture!("rar4_solid_lz_encrypted.rar", Some("moonlit-harbour"))
            }
            Self::Rar4SolidHeaders => {
                fixture!("rar4_solid_lz_headers.rar", Some("moonlit-harbour"))
            }
            Self::Rar4Ppmd => fixture!("rar4_solid_ppmd_plain.rar", None),
            Self::Rar4PpmdEncrypted => {
                fixture!("rar4_solid_ppmd_encrypted.rar", Some("moonlit-harbour"))
            }
            Self::Rar4PpmdHeaders => {
                fixture!("rar4_solid_ppmd_headers.rar", Some("moonlit-harbour"))
            }
            // A header-encrypted set packs the member the data-encrypted one
            // does, under the same password.
            Self::Rar4Encrypted
            | Self::Rar4EncryptedTwoVolumes
            | Self::Rar4EncryptedFourVolumes
            | Self::Rar4HeadersTwoVolumes
            | Self::Rar4HeadersFourVolumes => fixture!("rar4_enc_lz.rar", Some("testpass123")),
            Self::Rar4Headers => fixture!("rar4_hp_lz.rar", Some("secretpass")),
            Self::Rar5Mixed => fixture!("rar5_multifile_lz.rar", None),
            Self::Rar5Lz | Self::Rar5LzTwoVolumes | Self::Rar5LzFourVolumes => {
                fixture!("rar5_lz.rar", None)
            }
            Self::Rar5Encrypted
            | Self::Rar5EncryptedTwoVolumes
            | Self::Rar5EncryptedFourVolumes
            | Self::Rar5HeadersTwoVolumes
            | Self::Rar5HeadersFourVolumes => fixture!("rar5_enc_lz.rar", Some("testpass123")),
            // The members decode the same from either format's archive.
            Self::Rar4SolidTwoVolumes
            | Self::Rar5SolidTwoVolumes
            | Self::Rar4SolidFourVolumes
            | Self::Rar5SolidFourVolumes => fixture!("rar5_solid_text.rar", None),
            Self::Rar4SolidEncryptedTwoVolumes
            | Self::Rar5SolidEncryptedTwoVolumes
            | Self::Rar4SolidEncryptedFourVolumes
            | Self::Rar5SolidEncryptedFourVolumes => {
                fixture!("rar5_solid_text_encrypted.rar", Some("moonlit-harbour"))
            }
            Self::Rar5Headers => fixture!("rar5_hp_lz.rar", Some("secretpass")),
            Self::Rar5Solid => fixture!("rar5_solid_small.rar", None),
            Self::Rar5SolidEncrypted => {
                fixture!("rar5_solid_encrypted_small.rar", Some("moonlit-harbour"),)
            }
            Self::Rar5SolidHeaders => {
                fixture!("rar5_solid_headers_small.rar", Some("moonlit-harbour"),)
            }
        }
    }
}

impl CompressedFormat {}

// A format's members, decoded from its fixture and pinned to the oracle.
fn compressed_members(format: CompressedFormat) -> BTreeMap<String, Vec<u8>> {
    let (fixture_name, bytes, password) = format.fixture();
    let mut archive = match password {
        Some(password) => {
            unrar_rs::RarArchive::open_with_password(std::io::Cursor::new(bytes), password)
        }
        None => unrar_rs::RarArchive::open(std::io::Cursor::new(bytes)),
    }
    .unwrap();
    let mut expected = BTreeMap::new();
    for index in 0..archive.len() {
        let info = archive.member_info(index).unwrap();
        if info.is_directory {
            continue;
        }
        let mut output = Vec::new();
        archive
            .by_index(index)
            .unwrap()
            .copy_to(&mut output)
            .unwrap();
        expected.insert(info.name, output);
    }
    assert!(!expected.is_empty());
    // Pin the oracle to the official reader, independently of the Rust reader
    // used both here and in the pipeline. A shared decoder error cannot bless
    // the bytes subsequently compared by every schedule.
    let oracle: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/extraction_profiles/expected.json"
    )))
    .unwrap();
    let posted = format.volumes();
    let oracle_key = posted.as_ref().map_or(fixture_name, |(set, _)| set);
    let oracle = oracle["archives"][oracle_key].as_object().unwrap();
    assert_eq!(expected.len(), oracle.len());
    for (name, bytes) in &expected {
        use sha2::Digest;
        assert_eq!(bytes.len() as u64, oracle[name]["size"].as_u64().unwrap());
        assert_eq!(
            hex::encode(sha2::Sha256::digest(bytes)),
            oracle[name]["sha256"].as_str().unwrap()
        );
    }
    expected
}

// A format's posted volumes and its job, with the password when the
// archive needs one.
fn compressed_job(format: CompressedFormat) -> (Vec<(String, Vec<u8>)>, JobSpec) {
    let (_, bytes, password) = format.fixture();
    let posted = format.volumes();
    let volumes: Vec<(String, Vec<u8>)> = match posted {
        Some((_, parts)) => parts
            .into_iter()
            .enumerate()
            .map(|(index, part)| {
                (
                    format!("compressed.part{:02}.rar", index + 1),
                    part.to_vec(),
                )
            })
            .collect(),
        None => vec![("compressed.rar".to_owned(), bytes.to_vec())],
    };
    let mut spec = direct_store_job_spec_with_articles(
        "Compressed archive schedules",
        &volumes,
        4 / volumes.len(),
    );
    spec.password = password.map(str::to_owned);
    (volumes, spec)
}

// These are test names, not runner jobs. Nextest partitions the named shards
// across the bounded CI runner matrix. Every shard is independently replayable.
macro_rules! combined_campaign {
    ($module:ident, $variant:expr, $run:ident) => {
        mod $module {
            use super::*;
            combined_campaign!(@shards $variant, $run);
            // A test of its own, so no shard carries the password schedules.
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn wrong_password() {
                $run($variant, Selection::WrongPassword).await;
            }
        }
    };
    (@shards $variant:expr, $run:ident) => {
        combined_campaign!(
            @each $variant, $run;
                shard_00 0,
                shard_01 1,
                shard_02 2,
                shard_03 3,
                shard_04 4,
                shard_05 5,
                shard_06 6,
                shard_07 7,
                shard_08 8,
                shard_09 9,
                shard_10 10,
                shard_11 11,
                shard_12 12,
                shard_13 13,
                shard_14 14,
                shard_15 15,
                shard_16 16,
                shard_17 17,
                shard_18 18,
                shard_19 19,
                shard_20 20,
                shard_21 21,
                shard_22 22,
                shard_23 23,
                shard_24 24,
                shard_25 25,
                shard_26 26,
                shard_27 27,
                shard_28 28,
                shard_29 29,
                shard_30 30,
                shard_31 31,
                shard_32 32,
                shard_33 33,
                shard_34 34,
                shard_35 35,
                shard_36 36,
                shard_37 37,
                shard_38 38,
                shard_39 39,
                shard_40 40,
                shard_41 41,
                shard_42 42,
                shard_43 43,
                shard_44 44,
                shard_45 45,
                shard_46 46,
                shard_47 47,
                shard_48 48,
                shard_49 49,
                shard_50 50,
                shard_51 51,
                shard_52 52,
                shard_53 53,
                shard_54 54,
                shard_55 55,
                shard_56 56,
                shard_57 57,
                shard_58 58,
                shard_59 59,
                shard_60 60,
                shard_61 61,
                shard_62 62,
                shard_63 63
        );
    };
    (@each $variant:expr, $run:ident; $($name:ident $shard:literal),+) => {
        $(
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn $name() {
                $run($variant, Selection::Shard($shard)).await;
            }
        )+
    };
}
pub(super) use combined_campaign;

// Sets that names alone place wrongly or not at all: each is placed by its
// volumes' own headers.
