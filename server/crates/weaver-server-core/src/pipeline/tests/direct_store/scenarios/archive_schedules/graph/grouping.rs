// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The grouping cross, judged by the matrix's one rule set.
//
// A cell exists when [`rule`] says a
// single job can present it. What a job delivers follows from the post
// alone. The [`Fixture`] says which volumes the archive is written as,
// which of them go up under which names, and which members the archive
// holds; the binding says what index or listing goes with them; the
// schedule says what of that arrives. A member is deliverable when the
// reference reader reads it whole from what the post leaves, and every
// member is when the post carries every volume and leaves none of its
// bytes lost.
use super::super::super::sevenz_store::schedules::graph::sevenz_reads;
use super::super::grouping::{
    Binding, Cell, Container, Fixture, Naming, Ruling, assert_generated, recovery_layouts, rule,
};
use super::super::grouping_listing;
use super::*;

// Each volume the archive is written as, as a schedule leaves the post:
// absent when no posted copy of it arrives, and otherwise its bytes with
// every article no arriving copy brings zeroed. A recovery set the schedule
// does not starve brings back every posted copy whole.
fn left(
    cell: Cell,
    fixture: &Fixture,
    recovers: bool,
    interruption: Interruption,
) -> Vec<Option<Vec<u8>>> {
    let (count, posted) = cell.naming.layout();
    let articles = 4 / posted.len();
    let withheld = interruption.withheld().map(|(volume, _)| volume as usize);
    let lost = if recovers {
        0
    } else {
        interruption.loss().map_or(0, |(mask, _)| mask)
    };
    (0..count)
        .map(|volume| {
            let copies: Vec<usize> = posted
                .iter()
                .enumerate()
                .filter(|&(copy, &at)| at == volume && (recovers || Some(copy) != withheld))
                .map(|(copy, _)| copy)
                .collect();
            let mut bytes = fixture.volumes[*copies.first()?].1.clone();
            for article in 0..articles {
                if copies
                    .iter()
                    .all(|copy| lost & (1 << (copy * articles + article)) != 0)
                {
                    let (start, end) = article_extent(bytes.len(), article as u32, articles);
                    bytes[start..end].fill(0);
                }
            }
            Some(bytes)
        })
        .collect()
}

// The members the reference reader extracts whole from a RAR set's volumes
// as the post leaves them, an absent volume left out.
fn rar_reads(
    left: &[Option<Vec<u8>>],
    password: Option<&str>,
    members: &BTreeMap<String, Vec<u8>>,
) -> BTreeSet<String> {
    let mut present = left
        .iter()
        .enumerate()
        .filter_map(|(index, bytes)| Some((index, bytes.as_ref()?)));
    let mut recovered = BTreeSet::new();
    let Some((_, first)) = present.next() else {
        return recovered;
    };
    let first = std::io::Cursor::new(first.clone());
    let opened = match password {
        Some(password) => unrar_rs::RarArchive::open_with_password(first, password),
        None => unrar_rs::RarArchive::open(first),
    };
    let Ok(mut archive) = opened else {
        return recovered;
    };
    for (index, bytes) in present {
        let reader = Box::new(std::io::Cursor::new(bytes.clone())) as Box<dyn unrar_rs::ReadSeek>;
        if archive.add_volume(index, reader).is_err() {
            return recovered;
        }
    }
    for index in 0..archive.len() {
        let Some(info) = archive.member_info(index) else {
            continue;
        };
        let mut output = Vec::new();
        let extracted = archive
            .by_index(index)
            .and_then(|entry| entry.copy_to(&mut output));
        if extracted.is_ok() && members.get(&info.name) == Some(&output) {
            recovered.insert(info.name);
        }
    }
    recovered
}

// What `cell`'s post leaves a job under `options`. The post mends the
// schedule's loss when the recovery set is posted and not starved, or when
// every lost article survives in another posted copy of its volume; a
// volume the post never carries is that volume posted and lost whole.
fn cell_post(cell: Cell, fixture: Fixture, options: ScheduleOptions) -> Deliverable {
    let members: BTreeMap<String, Vec<u8>> = fixture
        .expected
        .iter()
        .map(|(name, bytes)| (name.to_string(), bytes.clone()))
        .collect();
    Box::new(move |opens, interruption| {
        let recovers = options.recovery != RecoveryFormat::Absent && !interruption.fails();
        let mended = recovers
            || fixture
                .posted_loss(interruption)
                .loss()
                .is_none_or(|(mask, _)| !fixture.uncovered_loss(mask));
        let left = left(cell, &fixture, recovers, interruption);
        let password = fixture.spec.password.as_deref();
        // Every 7z piece but the last is as long as the first, so a set
        // with more than one middle piece has pieces only a name orders.
        let (count, _) = cell.naming.layout();
        let unordered = cell.container.is_sevenz() && count > 3 && !cell.orders_middles();
        let deliverable = if !opens || unordered {
            BTreeSet::new()
        } else if mended && left.iter().all(Option::is_some) {
            members.keys().cloned().collect()
        } else if cell.container.is_sevenz() {
            let archive = left
                .iter()
                .zip(&fixture.lengths)
                .flat_map(|(bytes, &length)| bytes.clone().unwrap_or_else(|| vec![0; length]))
                .collect();
            sevenz_reads(archive, password, &members)
        } else {
            rar_reads(&left, password, &members)
        };
        // A 7z set's only signature is in its first piece: a job holding
        // opaque pieces that no name calls a set has no reason to read them
        // as one, and holds the pieces posted as files. A recovery set under
        // the posted names says nothing of what the pieces are, and what it
        // recreates is already in what the post leaves.
        let archive = !cell.container.is_sevenz()
            || cell.naming.names_place()
            || cell.binding.names_real()
            || left.first().is_some_and(|piece| {
                piece
                    .as_ref()
                    .is_some_and(|bytes| bytes.starts_with(SEVENZ))
            });
        let files = (!archive).then(|| {
            let posted: Vec<&Vec<u8>> = left.iter().flatten().collect();
            Files {
                posted: posted.len(),
                whole: posted
                    .iter()
                    .filter(|piece| fixture.volumes.iter().any(|(_, bytes)| bytes == **piece))
                    .count(),
            }
        });
        Leaves {
            deliverable,
            mended,
            files,
        }
    })
}

// The bytes a 7z archive opens with.
const SEVENZ: &[u8] = &[b'7', b'z', 0xBC, 0xAF, 0x27, 0x1C];

// Runs `cases` over `cell` under each recovery layout a case is posted
// under, and holds every one to the rule set.
async fn run_cell(cell: Cell, cases: Vec<(usize, Schedule)>) {
    run_cell_under(cell, cases, None).await;
}

// [`run_cell`], kept to the one recovery layout `only` when it names one.
async fn run_cell_under(cell: Cell, cases: Vec<(usize, Schedule)>, only: Option<RecoveryFormat>) {
    if let Ruling::Impossible(why) = rule(cell) {
        panic!("{cell:?} is no grouping cell: {why}");
    }
    let fixture = Fixture::build(cell);
    let (options, described) = cell.binding.drive(&fixture);
    let mut layouts: Vec<(ScheduleOptions, Vec<(usize, Schedule)>)> = Vec::new();
    for (case, schedule) in cases {
        if !cell.presents(schedule.1) {
            continue;
        }
        for layout in recovery_layouts(options, schedule.1)
            .into_iter()
            .filter(|layout| only.is_none_or(|recovery| layout.recovery == recovery))
        {
            match layouts
                .iter_mut()
                .find(|(laid, _)| laid.recovery == layout.recovery)
            {
                Some((_, cases)) => cases.push((case, schedule.clone())),
                None => layouts.push((layout, vec![(case, schedule.clone())])),
            }
        }
    }
    let mut reports = Vec::new();
    for (options, cases) in layouts {
        let graph = GraphFixture {
            label: format!("{cell:?} recovery={:?}", options.recovery),
            options,
            spec: fixture.spec.clone(),
            volumes: fixture.volumes.clone(),
            described: described.clone(),
            members: fixture
                .expected
                .iter()
                .map(|(name, bytes)| (name.to_string(), bytes.clone()))
                .collect(),
            needs_password: fixture.spec.password.is_some(),
            deliverable: cell_post(cell, fixture.clone(), options),
        };
        reports.extend(graph_fixture_cases(&graph, cases).await);
    }
    assert_cells(&reports);
}

// One replay index of the matrix's schedules over `cell`, under the one
// recovery layout `recovery`, held to the rule set: a row elsewhere pins a
// case the matrix found.
pub(in crate::pipeline::tests) async fn cell_case(
    cell: Cell,
    case: usize,
    recovery: RecoveryFormat,
) {
    let schedule = schedules().swap_remove(case);
    let fixture = Fixture::build(cell);
    let (options, _) = cell.binding.drive(&fixture);
    assert!(
        recovery_layouts(options, schedule.1)
            .iter()
            .any(|layout| layout.recovery == recovery),
        "{cell:?} case={case} is never posted under {recovery:?}"
    );
    run_cell_under(cell, vec![(case, schedule)], Some(recovery)).await;
}

// A cell's campaign: the matrix's smoke schedules.
async fn cell_campaign(cell: Cell) {
    run_cell(cell, schedules().into_iter().enumerate().collect()).await;
}

// A cell's smoke: in order and reversed.
async fn cell_smoke(cell: Cell) {
    let forward = slot_arrivals(4);
    let mut backward = forward.clone();
    backward.reverse();
    run_cell(
        cell,
        vec![
            (0, (forward, Interruption::None)),
            (1, (backward, Interruption::None)),
        ],
    )
    .await;
}

// Emits one smoke under `graph::grouping::grouping_smoke` and one ignored
// campaign under `graph::grouping::combined_grouping` per listed cell, and
// the list itself as `GENERATED_CELLS`.
macro_rules! graph_grouping_cells {
    ($($tag:ident $c:ident $C:ident {
        $($n:ident $N:ident [$($b:ident $B:ident)*])*
    })*) => {
        const GENERATED_CELLS: &[Cell] = &[$($($(Cell {
            container: Container::$C,
            naming: Naming::$N,
            binding: Binding::$B,
        },)*)*)*];
        mod grouping_smoke {
            $(mod $c {
                $(mod $n {
                    use super::super::super::*;
                    $(
                        #[tokio::test]
                        async fn $b() {
                            cell_smoke(Cell {
                                container: Container::$C,
                                naming: Naming::$N,
                                binding: Binding::$B,
                            })
                            .await;
                        }
                    )*
                })*
            })*
        }
        mod combined_grouping {
            $(mod $c {
                $(mod $n {
                    use super::super::super::*;
                    $(
                        #[tokio::test]
                        #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
                        async fn $b() {
                            cell_campaign(Cell {
                                container: Container::$C,
                                naming: Naming::$N,
                                binding: Binding::$B,
                            })
                            .await;
                        }
                    )*
                })*
            })*
        }
    };
}

grouping_listing!(graph_grouping_cells);

// The generator emits a smoke and a campaign for exactly the possible cells.
#[test]
fn grouping_generator_covers_every_possible_cell() {
    assert_generated(GENERATED_CELLS);
}
