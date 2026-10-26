// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Deeper schedules over the core direct-store RAR layouts: a fifth article,
// a second duplicate, and two interruptions in one run.
//
// Each family deepens one dimension of the archive matrix and keeps every
// other dimension as the matrix has it; families are never multiplied
// together. Every case is held to the archive matrix's own rules.
//
// A fifth article lands on the first volume (see `slot_layout`): two volumes
// carry three articles and two, four volumes two, one, one and one. The
// harness cuts each volume into equal articles, so four slots split evenly
// over two or four volumes and a fifth has to sit on one volume in either
// shape; one rule for both keeps slots numbered in file order. The first
// volume, which carries the archive and member headers, gives the two-volume
// shape an article that is neither its volume's first nor its last, and the
// four-volume shape a split volume ahead of single-article ones. Schedules
// keep naming slots `(slot / 2, slot % 2)`, so the fifth slot is `(2, 0)`
// whatever volume carries it.
use super::*;

type Order = Vec<(u32, u32)>;

// One deeper dimension of the archive matrix.
#[derive(Clone, Copy, Debug)]
pub(super) enum Family {
    // Five article slots: every arrival permutation, a single duplicate,
    // every loss subset and a single interruption.
    FifthArticle,
    // Four slots with two repeated articles, crossed with loss and a single
    // interruption.
    SecondDuplicate,
    // Four slots with a single duplicate, crossed with loss and an ordered
    // pair of interruptions.
    TwoInterruptions,
}

// Cases spread across a smoke selection besides the first of each kind.
const SMOKE_SPREAD: usize = 12;

impl Family {
    pub(super) const fn slots(self) -> usize {
        match self {
            Self::FifthArticle => 5,
            Self::SecondDuplicate | Self::TwoInterruptions => 4,
        }
    }

    // Shards a campaign is cut into: about a thousand cases a shard, fewer
    // where a case restarts the pipeline twice, and twice as many for the
    // formats whose cases run long enough that a shard of the usual size
    // outruns the per-test limit.
    pub(super) const fn shards(self, format: Format) -> usize {
        let shards = match self {
            Self::FifthArticle | Self::TwoInterruptions => 80,
            Self::SecondDuplicate => 40,
        };
        let finer = match self {
            Self::FifthArticle => matches!(
                format,
                Format::Rar4Encrypted | Format::Rar5Encrypted | Format::Rar5FourVolumes
            ),
            Self::SecondDuplicate | Self::TwoInterruptions => {
                matches!(format, Format::Rar4Encrypted)
            }
        };
        if finer { 2 * shards } else { shards }
    }

    // Every case of the family, by replay index.
    fn cases(self) -> Vec<Schedule> {
        let four = permutations(4);
        match self {
            Self::FifthArticle => {
                let five = permutations(5);
                let mut orders = with_duplicate(&five);
                orders.extend(five);
                assert_eq!(orders.len(), 1320);
                // One order in two, by a fixed stride over the sorted orders.
                let orders: BTreeSet<Order> = orders.into_iter().step_by(2).collect();
                assert_eq!(orders.len(), 660);
                let cases = single_interruption_cases(5, &orders, |_| true);
                assert_eq!(cases.len(), 48_137);
                cases
            }
            Self::SecondDuplicate => {
                let orders = with_duplicate(&with_duplicate(&four));
                assert_eq!(orders.len(), 600);
                // A loss that takes a repeated article leaves a received
                // sequence the archive matrix already runs.
                let cases = single_interruption_cases(4, &orders, |received| {
                    let distinct: BTreeSet<_> = received.iter().collect();
                    received.len() - distinct.len() >= 2
                });
                assert_eq!(cases.len(), 26_008);
                cases
            }
            Self::TwoInterruptions => {
                let mut orders = with_duplicate(&four);
                orders.extend(four);
                assert_eq!(orders.len(), 168);
                let cases = two_interruption_cases(&orders);
                assert_eq!(cases.len(), 32_552);
                cases
            }
        }
    }

    // The family's cases for one selection, by replay index. A smoke run
    // takes the first case of every kind and a spread of the rest.
    //
    // `WEAVER_ARCHIVE_SCHEDULE_CASE=<n>` or `WEAVER_ARCHIVE_COMBINED_CASE=<n>`
    // (or `<start>..<end>`) replays those replay indices: a smoke run runs
    // them, a shard the ones it owns.
    pub(super) fn selected(self, format: Format, selection: Selection) -> Vec<(usize, Schedule)> {
        let cases = self.cases();
        let replay = replayed(cases.len());
        let stride = cases.len() / SMOKE_SPREAD;
        let shards = self.shards(format);
        let mut kinds = BTreeSet::new();
        cases
            .into_iter()
            .enumerate()
            .filter(|(case, (_, interruption))| match selection {
                Selection::Shard(shard) => {
                    assert!(shard < shards);
                    case % shards == shard
                        && replay.as_ref().is_none_or(|range| range.contains(case))
                }
                Selection::Smoke => match &replay {
                    Some(range) => range.contains(case),
                    None => kinds.insert(kind(*interruption)) || case % stride == stride / 2,
                },
                Selection::WrongPassword => panic!("{self:?} carries no password schedules"),
            })
            .collect()
    }
}

// The replay indices an environment override names, if any.
fn replayed(cases: usize) -> Option<std::ops::Range<usize>> {
    let selection = std::env::var("WEAVER_ARCHIVE_SCHEDULE_CASE")
        .or_else(|_| std::env::var("WEAVER_ARCHIVE_COMBINED_CASE"))
        .ok()?;
    let range = if let Some((start, end)) = selection.split_once("..") {
        start.parse::<usize>().expect("decimal range start")
            ..end.parse::<usize>().expect("decimal range end")
    } else {
        let case = selection.parse::<usize>().expect("decimal case");
        case..case.checked_add(1).expect("case in range")
    };
    assert!(
        range.start < range.end && range.end <= cases,
        "{range:?} of {cases}"
    );
    Some(range)
}

// What sets one case apart from another for a smoke selection: the shape of
// its interruption, without its boundaries or loss mask.
fn kind(interruption: Interruption) -> (u8, BoundaryAction, BoundaryAction, bool) {
    use BoundaryAction as Action;
    match interruption {
        Interruption::None => (0, Action::None, Action::None, false),
        Interruption::Restart(_) => (1, Action::Restart, Action::None, false),
        Interruption::Crash(_) => (1, Action::Crash, Action::None, false),
        Interruption::Loss { .. } => (2, Action::None, Action::None, false),
        Interruption::Combined { action, .. } => (3, action, Action::None, false),
        Interruption::Starved { .. } => (4, Action::None, Action::None, false),
        Interruption::Withheld { .. } => (7, Action::None, Action::None, false),
        Interruption::Twice {
            mask,
            first,
            first_at,
            second,
            second_at,
            ..
        } => (
            5 + u8::from(mask != 0),
            first,
            second,
            first_at == second_at,
        ),
    }
}

// Every arrival order of `slots` article slots, each once.
fn permutations(slots: usize) -> BTreeSet<Order> {
    fn permute(at: usize, items: &mut [(u32, u32)], output: &mut BTreeSet<Order>) {
        if at == items.len() {
            output.insert(items.to_vec());
            return;
        }
        for i in at..items.len() {
            items.swap(at, i);
            permute(at + 1, items, output);
            items.swap(at, i);
        }
    }
    let mut orders = BTreeSet::new();
    permute(0, &mut slot_arrivals(slots), &mut orders);
    orders
}

// Each order with one earlier article repeated at every nonterminal
// insertion point, so the last article to arrive is always a new one.
fn with_duplicate(orders: &BTreeSet<Order>) -> BTreeSet<Order> {
    let mut result = BTreeSet::new();
    for order in orders {
        for at in 1..order.len() {
            for &article in &order[..at] {
                let mut duplicate = order.clone();
                duplicate.insert(at, article);
                result.insert(duplicate);
            }
        }
    }
    result
}

fn lost(mask: u8, (file, article): (u32, u32)) -> bool {
    mask & (1 << (file * 2 + article)) != 0
}

// The archive matrix's cases for any slot count and arrival orders, in its
// order: every loss subset with the index first or last, uninterrupted and
// restarted at every received boundary; the lossless orders uninterrupted
// and restarted before their last arrival; a crash
// wherever a restart was tried; and every lossy received sequence with
// nothing to repair it from. Received sequences `keep` refuses are left out.
fn single_interruption_cases(
    slots: usize,
    orders: &BTreeSet<Order>,
    keep: impl Fn(&[(u32, u32)]) -> bool,
) -> Vec<Schedule> {
    let mut cases = BTreeSet::new();
    for mask in 1u8..(1 << slots) {
        for order in orders {
            let received: Order = order.iter().filter(|&&a| !lost(mask, a)).copied().collect();
            if !keep(&received) {
                continue;
            }
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
                    let at = order[..boundary]
                        .iter()
                        .filter(|&&a| !lost(mask, a))
                        .count();
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
    let mut cases: Vec<_> = cases.into_iter().collect();
    for order in orders.iter().filter(|order| keep(order)) {
        cases.push((order.clone(), Interruption::None));
        for at in 0..order.len() {
            cases.push((order.clone(), Interruption::Restart(at)));
        }
    }
    let mut crashes = BTreeSet::new();
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
    let starved: BTreeSet<_> = cases
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
    cases
}

// An ordered pair of boundary actions at boundaries `first_at <= second_at`.
fn action_pairs(boundaries: &[usize]) -> Vec<(BoundaryAction, usize, BoundaryAction, usize)> {
    const ACTIONS: [BoundaryAction; 2] = [BoundaryAction::Restart, BoundaryAction::Crash];
    let mut pairs = Vec::new();
    for first in ACTIONS {
        for second in ACTIONS {
            for &first_at in boundaries {
                for &second_at in boundaries.iter().filter(|&&at| at >= first_at) {
                    pairs.push((first, first_at, second, second_at));
                }
            }
        }
    }
    pairs
}

// The archive matrix's interrupted cases with a pair of interruptions in
// place of one: every loss subset with the index first or last and a pair
// at every two received boundaries; then the lossless orders with a pair
// at every two boundaries before their last arrival.
fn two_interruption_cases(orders: &BTreeSet<Order>) -> Vec<Schedule> {
    let mut cases = BTreeSet::new();
    for mask in 1u8..16 {
        for order in orders {
            let received: Order = order.iter().filter(|&&a| !lost(mask, a)).copied().collect();
            let boundaries: BTreeSet<_> = (0..=order.len())
                .map(|boundary| {
                    order[..boundary]
                        .iter()
                        .filter(|&&a| !lost(mask, a))
                        .count()
                })
                .collect();
            let boundaries: Vec<_> = boundaries.into_iter().collect();
            for index_first in [false, true] {
                for (first, first_at, second, second_at) in action_pairs(&boundaries) {
                    cases.insert((
                        received.clone(),
                        Interruption::Twice {
                            mask,
                            index_first,
                            first,
                            first_at,
                            second,
                            second_at,
                        },
                    ));
                }
            }
        }
    }
    let mut cases: Vec<_> = cases.into_iter().collect();
    for order in orders {
        let boundaries: Vec<_> = (0..order.len()).collect();
        for (first, first_at, second, second_at) in action_pairs(&boundaries) {
            cases.push((
                order.clone(),
                Interruption::Twice {
                    mask: 0,
                    index_first: false,
                    first,
                    first_at,
                    second,
                    second_at,
                },
            ));
        }
    }
    cases
}

// The family's generator, given the archive matrix's own slots and orders,
// builds the archive matrix case for case.
#[test]
fn single_interruption_cases_reproduce_the_archive_matrix() {
    let four = permutations(4);
    let mut orders = with_duplicate(&four);
    orders.extend(four);
    let expected: Vec<_> = combined_schedule_cases()
        .into_iter()
        .map(|(_, schedule)| schedule)
        .collect();
    assert_eq!(single_interruption_cases(4, &orders, |_| true), expected);
}

// Shards of a campaign named in decades, so a campaign's shard count is ten
// times the decades it lists. Each shard calls `$run` with the bracketed
// arguments and its selection, and is independently replayable.
macro_rules! depth_shards {
    ($run:path, $args:tt, $shards:expr; [$($decade:tt)+]) => {
        const _: () = assert!(10 * [$($decade),+].len() == $shards);
        $(depth_shards!(@decade $run, $args; $decade);)+
    };
    (@decade $run:path, $args:tt; 0) => {
        depth_shards!(@each $run, $args;
            shard_00 0, shard_01 1, shard_02 2, shard_03 3, shard_04 4,
            shard_05 5, shard_06 6, shard_07 7, shard_08 8, shard_09 9);
    };
    (@decade $run:path, $args:tt; 1) => {
        depth_shards!(@each $run, $args;
            shard_10 10, shard_11 11, shard_12 12, shard_13 13, shard_14 14,
            shard_15 15, shard_16 16, shard_17 17, shard_18 18, shard_19 19);
    };
    (@decade $run:path, $args:tt; 2) => {
        depth_shards!(@each $run, $args;
            shard_20 20, shard_21 21, shard_22 22, shard_23 23, shard_24 24,
            shard_25 25, shard_26 26, shard_27 27, shard_28 28, shard_29 29);
    };
    (@decade $run:path, $args:tt; 3) => {
        depth_shards!(@each $run, $args;
            shard_30 30, shard_31 31, shard_32 32, shard_33 33, shard_34 34,
            shard_35 35, shard_36 36, shard_37 37, shard_38 38, shard_39 39);
    };
    (@decade $run:path, $args:tt; 4) => {
        depth_shards!(@each $run, $args;
            shard_40 40, shard_41 41, shard_42 42, shard_43 43, shard_44 44,
            shard_45 45, shard_46 46, shard_47 47, shard_48 48, shard_49 49);
    };
    (@decade $run:path, $args:tt; 5) => {
        depth_shards!(@each $run, $args;
            shard_50 50, shard_51 51, shard_52 52, shard_53 53, shard_54 54,
            shard_55 55, shard_56 56, shard_57 57, shard_58 58, shard_59 59);
    };
    (@decade $run:path, $args:tt; 6) => {
        depth_shards!(@each $run, $args;
            shard_60 60, shard_61 61, shard_62 62, shard_63 63, shard_64 64,
            shard_65 65, shard_66 66, shard_67 67, shard_68 68, shard_69 69);
    };
    (@decade $run:path, $args:tt; 7) => {
        depth_shards!(@each $run, $args;
            shard_70 70, shard_71 71, shard_72 72, shard_73 73, shard_74 74,
            shard_75 75, shard_76 76, shard_77 77, shard_78 78, shard_79 79);
    };
    (@decade $run:path, $args:tt; 8) => {
        depth_shards!(@each $run, $args;
            shard_80 80, shard_81 81, shard_82 82, shard_83 83, shard_84 84,
            shard_85 85, shard_86 86, shard_87 87, shard_88 88, shard_89 89);
    };
    (@decade $run:path, $args:tt; 9) => {
        depth_shards!(@each $run, $args;
            shard_90 90, shard_91 91, shard_92 92, shard_93 93, shard_94 94,
            shard_95 95, shard_96 96, shard_97 97, shard_98 98, shard_99 99);
    };
    (@decade $run:path, $args:tt; 10) => {
        depth_shards!(@each $run, $args;
            shard_100 100, shard_101 101, shard_102 102, shard_103 103, shard_104 104,
            shard_105 105, shard_106 106, shard_107 107, shard_108 108, shard_109 109);
    };
    (@decade $run:path, $args:tt; 11) => {
        depth_shards!(@each $run, $args;
            shard_110 110, shard_111 111, shard_112 112, shard_113 113, shard_114 114,
            shard_115 115, shard_116 116, shard_117 117, shard_118 118, shard_119 119);
    };
    (@decade $run:path, $args:tt; 12) => {
        depth_shards!(@each $run, $args;
            shard_120 120, shard_121 121, shard_122 122, shard_123 123, shard_124 124,
            shard_125 125, shard_126 126, shard_127 127, shard_128 128, shard_129 129);
    };
    (@decade $run:path, $args:tt; 13) => {
        depth_shards!(@each $run, $args;
            shard_130 130, shard_131 131, shard_132 132, shard_133 133, shard_134 134,
            shard_135 135, shard_136 136, shard_137 137, shard_138 138, shard_139 139);
    };
    (@decade $run:path, $args:tt; 14) => {
        depth_shards!(@each $run, $args;
            shard_140 140, shard_141 141, shard_142 142, shard_143 143, shard_144 144,
            shard_145 145, shard_146 146, shard_147 147, shard_148 148, shard_149 149);
    };
    (@decade $run:path, $args:tt; 15) => {
        depth_shards!(@each $run, $args;
            shard_150 150, shard_151 151, shard_152 152, shard_153 153, shard_154 154,
            shard_155 155, shard_156 156, shard_157 157, shard_158 158, shard_159 159);
    };
    (@each $run:path, $args:tt; $($name:ident $shard:literal),+) => {
        $(
            #[tokio::test]
            #[ignore = "opt-in extended archive matrix; run with the archive-matrix-extended Nextest profile and --run-ignored all"]
            async fn $name() {
                depth_shards!(@call $run, $args, Selection::Shard($shard));
            }
        )+
    };
    (@call $run:path, [$($arg:expr),*], $selection:expr) => {
        $run($($arg,)* $selection).await
    };
}
pub(super) use depth_shards;
