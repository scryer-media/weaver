//! Bounded exhaustive schedules over four articles: one, two or four volumes,
//! every arrival permutation, loss subset and single duplicate/interruption.
//! This enumerates delivery boundaries, not background worker or filesystem
//! interleavings; those require separate tests that force the competing events.
use super::*;
use crate::pipeline::direct_store::router::MemberIneligibility;
use crate::pipeline::direct_store::router::sevenz::SevenZipRefusal;
use crate::pipeline::direct_unpack::wiring::{AbortLatch, DemotionReason as ChaseDemotion};

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

/// Articles per volume for `slots` article slots over `volumes` volumes: four
/// split evenly, and a fifth carried by the first volume.
fn slot_layout(volumes: usize, slots: usize) -> Vec<usize> {
    assert!(
        4 % volumes == 0 && matches!(slots, 4 | 5),
        "{volumes} x {slots}"
    );
    let mut layout = vec![4 / volumes; volumes];
    layout[0] += slots - 4;
    layout
}

/// A schedule names slot `file * 2 + article` whatever the volumes' own
/// article counts; slots number the volumes' articles in file order.
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

/// Every slot once, in order, as a schedule names it.
fn slot_arrivals(slots: usize) -> Vec<(u32, u32)> {
    (0..slots as u32).map(|slot| (slot / 2, slot % 2)).collect()
}

pub(super) struct Outcome {
    pub status: Option<JobStatus>,
    pub files: BTreeMap<String, Option<Vec<u8>>>,
    pub trace: Vec<String>,
    /// Sets finalized from their own partials, over every incarnation.
    pub finalized: usize,
    pub chase_armed: u64,
    pub chase_consumed: u64,
    /// Every direct-store demotion any incarnation of the pipeline recorded.
    pub demotions: Vec<DemotionReason>,
    /// The reason the schedule's own demote action claimed a live set under.
    pub schedule_demoted: Option<DemotionReason>,
    /// Every file under the job's output directory, by relative path.
    pub published: BTreeSet<String>,
    /// Everything the job left outside its output directory.
    pub leftovers: BTreeSet<String>,
    /// Volume articles the pipeline asked for again after it was handed them.
    pub rerequested: BTreeSet<(u32, u32)>,
    /// Delivered articles a direct set has no reason to ask for again.
    ///
    /// Without a restart that is every one of them. A shutdown barrier keeps
    /// what its checkpoint names: each complete volume, and each partial
    /// volume's articles under the floor short of the last, because the floor
    /// counts decoded bytes against encoded article sizes and so stops one
    /// article early. A crash promises nothing.
    pub durable: BTreeSet<(u32, u32)>,
}

/// What a schedule may do to a direct set besides finalize it.
#[derive(Clone, Copy)]
pub(super) struct Route {
    /// The set this archive forms routes direct from first article to last.
    pub direct: bool,
    /// Independent sets in the job. The schedule's own demotion claims the
    /// first one only; every other set finalizes on its own.
    pub sets: usize,
    /// The demotions the archive's own shape earns, whatever the schedule.
    pub shape_demotion: fn(DemotionReason) -> bool,
    /// Loss masks that leave the set without a layout to route by.
    ///
    /// A set whose layout lives in articles of its own has no destination
    /// for any byte while those articles are missing; only a repair of the
    /// whole payload brings them back, and by then nothing is left to route.
    pub unmapped_loss: fn(u8) -> bool,
    /// Loss masks that leave a volume with nothing to say which volume it is.
    ///
    /// A set admitted by content knows a file by its offset-zero article
    /// alone. A file that never receives one cannot be bound to its volume, and
    /// its set cannot be made whole.
    pub unnamed_loss: fn(u8) -> bool,
    /// Only the recovery set's descriptions can name the volumes, so an index
    /// that arrives after the body names nothing in time and the set goes
    /// conventional.
    pub named_by_early_index: bool,
}

impl Route {
    pub(super) const DIRECT: Self = Self {
        direct: true,
        sets: 1,
        shape_demotion: |_| false,
        unmapped_loss: |_| false,
        unnamed_loss: |_| false,
        named_by_early_index: false,
    };

    /// A set the layout refuses to route: it demotes for its shape and
    /// nothing it does afterwards is a direct-store decision.
    pub(super) const fn refused(shape_demotion: fn(DemotionReason) -> bool) -> Self {
        Self {
            direct: false,
            sets: 1,
            shape_demotion,
            unmapped_loss: |_| false,
            unnamed_loss: |_| false,
            named_by_early_index: false,
        }
    }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ExtractionProfile {
    DirectStore,
    Chase,
    Conventional,
}

impl ExtractionProfile {
    fn configure(self, pipeline: &mut Pipeline) {
        use crate::pipeline::direct_unpack::settings::{DirectUnpackGate, DirectUnpackSettings};
        use crate::pipeline::direct_unpack::wiring::DirectUnpackRuntime;
        pipeline
            .direct_store
            .set_gate(if self == Self::DirectStore {
                DirectStoreGate::Enabled
            } else {
                DirectStoreGate::Disabled
            });
        pipeline.direct_unpack = DirectUnpackRuntime::with_settings(DirectUnpackSettings {
            gate: if self == Self::Conventional {
                DirectUnpackGate::Disabled
            } else {
                DirectUnpackGate::Enabled
            },
        });
    }

    pub(super) fn includes(self, interruption: Interruption) -> bool {
        // Conventional extraction has no speculative output to demote. Keep
        // its arrival, loss and restart cases without counting no-op demotions.
        self != Self::Conventional
            || !matches!(
                interruption,
                Interruption::Demote(_)
                    | Interruption::Combined {
                        action: BoundaryAction::Demote,
                        ..
                    }
                    | Interruption::Twice {
                        first: BoundaryAction::Demote,
                        ..
                    }
                    | Interruption::Twice {
                        second: BoundaryAction::Demote,
                        ..
                    }
            )
    }

    /// Holds a schedule that cannot succeed to failing cleanly: a failed job,
    /// no set finalized, and none of the archive's members published.
    pub(super) fn assert_rejected(self, outcome: &Outcome, expected: &[&str]) {
        self.assert_route(outcome);
        let trace = &outcome.trace;
        assert!(
            matches!(outcome.status, Some(JobStatus::Failed { .. })),
            "{:?}: {trace:?}",
            outcome.status
        );
        assert_eq!(outcome.finalized, 0, "{trace:?}");
        assert!(
            outcome.files.values().all(Option::is_none)
                && expected
                    .iter()
                    .all(|name| !outcome.published.contains(*name)),
            "a failed job published output {:?}: {trace:?}",
            outcome.published
        );
    }

    /// Holds a finished schedule to the route its profile and archive shape
    /// allow: which sets may leave direct store, what reaches the output
    /// directory, and what the job may leave behind or ask for twice.
    pub(super) fn assert_delivery(
        self,
        outcome: &Outcome,
        route: Route,
        expected: &[&str],
        interruption: Interruption,
    ) {
        self.assert_route(outcome);
        let trace = &outcome.trace;
        let unmapped = interruption
            .loss()
            .is_some_and(|(mask, _)| (route.unmapped_loss)(mask));
        let unnamed = interruption.loss().is_some_and(|(mask, index_first)| {
            (route.unnamed_loss)(mask) || (route.named_by_early_index && !index_first)
        });
        let unexpected: Vec<_> = outcome
            .demotions
            .iter()
            .filter(|reason| match **reason {
                reason if outcome.schedule_demoted == Some(reason) => false,
                DemotionReason::SevenZip(SevenZipRefusal::UnreadableMap) if unmapped => false,
                DemotionReason::SevenZip(SevenZipRefusal::EndHeaderLost) if unmapped => false,
                DemotionReason::IdentityRosterUnfillable if unnamed => false,
                // The unnamed volume belongs to no set, so its damage is
                // damage no direct set can repair in place.
                DemotionReason::Par2Damaged if unnamed => false,
                reason => !(route.shape_demotion)(reason),
            })
            .collect();
        assert!(
            unexpected.is_empty(),
            "direct store demoted unexpectedly: {unexpected:?} trace={trace:?}"
        );
        let schedule_only = outcome.schedule_demoted.is_some()
            && outcome
                .demotions
                .iter()
                .all(|reason| Some(*reason) == outcome.schedule_demoted);
        if self != Self::DirectStore {
            assert!(outcome.demotions.is_empty(), "{trace:?}");
        } else if route.direct && schedule_only {
            // The schedule claimed one set; the others stay direct. A restart
            // after the claim restores the job without it, and the claimed set
            // then routes direct afresh.
            let forgotten = matches!(
                interruption,
                Interruption::Twice {
                    first: BoundaryAction::Demote,
                    second: BoundaryAction::Restart | BoundaryAction::Crash,
                    ..
                }
            ) && outcome.finalized == route.sets;
            assert_eq!(
                outcome.finalized,
                route.sets - usize::from(!forgotten),
                "a set the schedule left alone left the direct route: {trace:?}"
            );
        } else if unnamed && outcome.demotions.is_empty() {
            // A volume nothing could name may leave no set admitted at all.
            assert!(outcome.finalized <= route.sets, "{trace:?}");
        } else if route.direct && outcome.demotions.is_empty() {
            assert_eq!(
                outcome.finalized, route.sets,
                "set left the direct route: {trace:?}"
            );
            let refetched: Vec<_> = outcome.rerequested.intersection(&outcome.durable).collect();
            assert!(
                refetched.is_empty(),
                "direct set refetched durable articles {refetched:?}: {trace:?}"
            );
        } else if route.sets == 1 {
            assert_eq!(outcome.finalized, 0, "{trace:?}");
        } else {
            assert!(outcome.finalized < route.sets, "{trace:?}");
        }
        if outcome.status == Some(JobStatus::Complete) {
            let expected: BTreeSet<String> = expected.iter().map(|name| (*name).into()).collect();
            let published: BTreeSet<String> = outcome.published.iter().cloned().collect();
            assert_eq!(published, expected, "published files: {trace:?}");
        }
        assert!(
            outcome.leftovers.is_empty(),
            "job left files behind {:?}: {trace:?}",
            outcome.leftovers
        );
    }

    pub(super) fn assert_route(self, outcome: &Outcome) {
        if self != Self::DirectStore {
            assert_eq!(outcome.finalized, 0, "{:?}", outcome.trace);
        }
        if self == Self::Conventional {
            assert_eq!(outcome.chase_armed, 0, "{:?}", outcome.trace);
            assert_eq!(outcome.chase_consumed, 0, "{:?}", outcome.trace);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum BoundaryAction {
    None,
    Restart,
    Demote,
    /// The process dies: no shutdown barrier, so the restart finds whatever
    /// coverage the last barrier of its own happened to publish.
    Crash,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Interruption {
    None,
    Restart(usize),
    Demote(usize),
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
    /// Loss with a recovery set that describes the damage and cannot mend it.
    Starved {
        mask: u8,
    },
    /// Two boundary actions in one run, the first never after the second.
    /// At a shared boundary both happen before that boundary's arrival, in
    /// order. An empty mask loses nothing and posts no recovery set.
    Twice {
        mask: u8,
        index_first: bool,
        first: BoundaryAction,
        first_at: usize,
        second: BoundaryAction,
        second_at: usize,
    },
}

/// Which slice of a campaign one test runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Selection {
    /// The default suite's bounded sample.
    Smoke,
    /// One shard of the combined matrix.
    Shard(usize),
    /// One of [`FINE_SHARDS`] shards of the combined matrix, for a layout
    /// whose cases run long enough that a shard of the usual size outruns the
    /// per-test limit.
    FineShard(usize),
    /// A wrong password across every arrival order and interruption boundary.
    WrongPassword,
    /// One of [`WRONG_PASSWORD_PARTS`] parts of the wrong password's
    /// schedules, for the same layouts.
    WrongPasswordPart(usize),
}

/// Shards the combined matrix is cut into, each its own test.
pub(super) const SHARDS: usize = 64;

/// Shards a slow layout's combined matrix is cut into instead.
pub(super) const FINE_SHARDS: usize = 2 * SHARDS;

/// Parts a slow layout's wrong password schedules are cut into.
pub(super) const WRONG_PASSWORD_PARTS: usize = 2;

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

    /// The job cannot finish: what was lost is beyond the recovery it has.
    pub(super) fn fails(self) -> bool {
        matches!(self, Self::Starved { .. })
    }

    fn action_at(self, boundary: usize) -> BoundaryAction {
        match self {
            Self::Restart(at) if at == boundary => BoundaryAction::Restart,
            Self::Demote(at) if at == boundary => BoundaryAction::Demote,
            Self::Crash(at) if at == boundary => BoundaryAction::Crash,
            Self::Combined { action, at, .. } if at == boundary => action,
            _ => BoundaryAction::None,
        }
    }

    /// Each boundary of a run over `arrivals` articles, with the action taken
    /// there and whether that boundary's arrival follows it. A boundary with
    /// two actions appears twice, and its arrival follows only the second.
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

/// A campaign's cases for one selection, by replay index.
pub(super) fn selected_schedules(selection: Selection) -> Vec<(usize, Schedule)> {
    match selection {
        Selection::Smoke => schedules().into_iter().enumerate().collect(),
        Selection::Shard(shard) => combined_schedules(shard, SHARDS),
        Selection::FineShard(shard) => combined_schedules(shard, FINE_SHARDS),
        Selection::WrongPassword | Selection::WrongPasswordPart(_) => Vec::new(),
    }
}

/// A wrong password's schedules: every arrival order, uninterrupted and
/// interrupted at every boundary. The default suite keeps the uninterrupted
/// orders; the matrix runs them all as a test of its own.
pub(super) fn wrong_password_schedules(selection: Selection) -> Vec<Schedule> {
    let mut result = Vec::new();
    let every = matches!(
        selection,
        Selection::WrongPassword | Selection::WrongPasswordPart(_)
    );
    if selection == Selection::Smoke || every {
        for order in arrival_orders() {
            result.push((order.clone(), Interruption::None));
            if every {
                for at in 0..order.len() {
                    result.push((order.clone(), Interruption::Restart(at)));
                    result.push((order.clone(), Interruption::Crash(at)));
                    result.push((order.clone(), Interruption::Demote(at)));
                }
            }
        }
    }
    if let Selection::WrongPasswordPart(part) = selection {
        assert!(part < WRONG_PASSWORD_PARTS);
        result = result
            .into_iter()
            .enumerate()
            .filter(|(schedule, _)| schedule % WRONG_PASSWORD_PARTS == part)
            .map(|(_, schedule)| schedule)
            .collect();
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
                    for action in [BoundaryAction::Restart, BoundaryAction::Demote] {
                        cases.insert((
                            received.clone(),
                            Interruption::Combined {
                                mask,
                                index_first,
                                action,
                                at,
                            },
                        ));
                    }
                }
            }
        }
    }
    // Lost arrivals have no publication effect. Identical received-event and
    // interruption sequences are one case, regardless of which lost item was
    // attempted first. Keep all distinct boundaries, including empty prefixes.
    assert_eq!(cases.len(), 4518);
    let mut cases: Vec<_> = cases.into_iter().collect();
    // Also cross delayed duplicates with interruption when nothing was lost.
    // A complete clean arrival sequence may already finalize the job, so its
    // interruption boundaries stop before the last distinct article arrives.
    // Append these cases to keep the repair cases' replay indices stable.
    for order in orders {
        cases.push((order.clone(), Interruption::None));
        for at in 0..order.len() {
            cases.push((order.clone(), Interruption::Restart(at)));
            cases.push((order.clone(), Interruption::Demote(at)));
        }
    }
    assert_eq!(cases.len(), 6318);
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
    assert_eq!(cases.len(), 9168);
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
    assert_eq!(cases.len(), 9393);
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
            result.push((order.clone(), Interruption::Demote(at)));
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

pub(super) async fn run_schedule(
    gate: DirectStoreGate,
    spec: JobSpec,
    volumes: &[(String, Vec<u8>)],
    order: &[(u32, u32)],
    wanted: &[&str],
    interruption: Interruption,
) -> Outcome {
    let profile = match gate {
        DirectStoreGate::Enabled => ExtractionProfile::DirectStore,
        DirectStoreGate::Disabled => ExtractionProfile::Chase,
    };
    run_profile_schedule(profile, spec, volumes, order, wanted, interruption).await
}

async fn deliver_schedule_article(
    pipeline: &mut Pipeline,
    job: JobId,
    volumes: &[(String, Vec<u8>)],
    file: u32,
    article: u32,
    articles: usize,
) {
    retire_schedule_article(pipeline, job, file, article);
    submit_volume_article_of(pipeline, job, volumes, file, article, articles).await;
}

fn retire_schedule_article(pipeline: &mut Pipeline, job: JobId, file: u32, article: u32) {
    let id = SegmentId {
        file_id: NzbFileId {
            job_id: job,
            file_index: file,
        },
        segment_number: article,
    };
    let state = pipeline.jobs.get_mut(&job).unwrap();
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

async fn deliver_schedule_refetches(
    pipeline: &mut Pipeline,
    job: JobId,
    volumes: &[(String, Vec<u8>)],
    layout: &[usize],
    mask: u8,
    recovery: &[ScheduleRecovery],
) {
    // A job that failed while the queues settled has already been retired.
    let Some(state) = pipeline.jobs.get_mut(&job) else {
        return;
    };
    let mut available = Vec::new();
    for queue in [&mut state.download_queue, &mut state.recovery_queue] {
        for work in queue.drain_all() {
            let file = work.segment_id.file_id.file_index;
            let article = work.segment_id.segment_number;
            if (file < volumes.len() as u32
                && mask & (1 << article_slot(layout, file, article)) == 0)
                || recovery.iter().any(|carrier| carrier.index == file)
            {
                if !available.contains(&(file, article)) {
                    available.push((file, article));
                }
                // Keep every undelivered article visible to the completion
                // gate. Draining the whole batch hid future writes from PAR2.
                queue.push(work);
            }
        }
    }
    for (file, article) in available {
        if file < volumes.len() as u32 {
            let articles = layout[file as usize];
            deliver_schedule_article(pipeline, job, volumes, file, article, articles).await;
        } else {
            let carrier = recovery
                .iter()
                .find(|carrier| carrier.index == file)
                .expect("queued recovery has fixture bytes");
            submit_schedule_recovery(pipeline, job, carrier, article).await;
        }
    }
}

/// A recovery file whose bytes the schedule holds, to hand over when due.
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
        RecoveryFormat::Par2 => {
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
        RecoveryFormat::Embedded => unreachable!("an embedded set posts no recovery file"),
    }
}

/// What a schedule's demote action claims: which of the job's sets, and why.
/// Both follow from the schedule, so across the arrival orders every boundary
/// sees a budget demotion and a source-damage one against each set.
fn scheduled_demotion(order: &[(u32, u32)], step: usize, sets: usize) -> (usize, DemotionReason) {
    // A schedule that lost every article has no set to claim and nothing to
    // seed from.
    let (file, article) = order.first().copied().unwrap_or_default();
    let seed = step + file as usize * 2 + article as usize;
    let reason = if seed.is_multiple_of(2) {
        DemotionReason::HoldsBudgetExceeded
    } else {
        DemotionReason::VolumeCrcMismatch
    };
    ((seed / 2) % sets.max(1), reason)
}

/// What a schedule's recovery set is authored as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RecoveryFormat {
    /// One PAR2 file carrying the descriptions and every recovery block.
    Par2,
    /// A PAR3 index and its recovery volumes. The index arrives where the
    /// PAR2 file would; a recovery volume arrives when the pipeline asks.
    Par3,
    /// The volumes carry their own recovery set and nothing else is posted.
    Embedded,
}

/// Who a schedule's demote action claims, and why.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DemotionChoice {
    /// [`scheduled_demotion`] picks the set and the reason; speculative
    /// extraction is withdrawn for good, as yielded memory.
    Scheduled,
    /// Every demote action claims `set` under `reason` and withdraws
    /// speculative extraction under `chase`, latched by `latch`.
    Fixed {
        set: usize,
        reason: DemotionReason,
        chase: ChaseDemotion,
        latch: AbortLatch,
    },
}

impl DemotionChoice {
    fn direct(self, order: &[(u32, u32)], step: usize, sets: usize) -> (usize, DemotionReason) {
        match self {
            Self::Scheduled => scheduled_demotion(order, step, sets),
            Self::Fixed { set, reason, .. } => (set, reason),
        }
    }

    fn chase(self) -> (AbortLatch, ChaseDemotion) {
        match self {
            Self::Scheduled => (AbortLatch::Permanent, ChaseDemotion::MemoryYielded),
            Self::Fixed { chase, latch, .. } => (latch, chase),
        }
    }
}

/// How a schedule is driven besides its arrivals and its interruption.
#[derive(Clone, Copy, Debug)]
pub(super) struct ScheduleOptions {
    pub recovery: RecoveryFormat,
    pub demotion: DemotionChoice,
}

impl ScheduleOptions {
    /// What every campaign of the archive matrix proper runs under.
    pub(super) const MATRIX: Self = Self {
        recovery: RecoveryFormat::Par2,
        demotion: DemotionChoice::Scheduled,
    };
}

pub(super) async fn run_profile_schedule(
    profile: ExtractionProfile,
    spec: JobSpec,
    volumes: &[(String, Vec<u8>)],
    order: &[(u32, u32)],
    wanted: &[&str],
    interruption: Interruption,
) -> Outcome {
    run_described_schedule(profile, spec, volumes, None, order, wanted, interruption).await
}

/// A schedule whose recovery set describes the volumes under `described`
/// names rather than the posted ones, the way an obfuscated post's PAR2
/// carries the real names. Such a job always carries its index: without a
/// loss it arrives first and holds no recovery blocks.
pub(super) async fn run_described_schedule(
    profile: ExtractionProfile,
    spec: JobSpec,
    volumes: &[(String, Vec<u8>)],
    described: Option<&[String]>,
    order: &[(u32, u32)],
    wanted: &[&str],
    interruption: Interruption,
) -> Outcome {
    run_schedule_with(
        ScheduleOptions::MATRIX,
        profile,
        spec,
        volumes,
        described,
        order,
        wanted,
        interruption,
    )
    .await
}

/// [`run_described_schedule`] under `options`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_schedule_with(
    options: ScheduleOptions,
    profile: ExtractionProfile,
    mut spec: JobSpec,
    volumes: &[(String, Vec<u8>)],
    described: Option<&[String]>,
    order: &[(u32, u32)],
    wanted: &[&str],
    interruption: Interruption,
) -> Outcome {
    enable_schedule_trace();
    let layout: Vec<usize> = spec.files[..volumes.len()]
        .iter()
        .map(|file| file.segments.len())
        .collect();
    assert_eq!(
        layout,
        slot_layout(volumes.len(), layout.iter().sum()),
        "four or five scheduled article slots"
    );
    let articles = |file: u32| layout[file as usize];
    let order: Vec<_> = order
        .iter()
        .map(|&(file, article)| slot_article(&layout, file * 2 + article))
        .collect();
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete) = new_direct_pipeline(&root).await;
    profile.configure(&mut pipeline);
    let mut chase_armed = 0;
    let mut chase_consumed = 0;
    let mut finalized = 0;
    let mut demotions = Vec::new();
    let mut schedule_demoted = None;
    let mut retired = None;
    let mut delivered = BTreeSet::new();
    let mut rerequested = BTreeSet::new();
    let mut durable = None;
    let job = JobId(42200);
    let output = complete.join(crate::jobs::working_dir::sanitize_dirname(&spec.name));
    let loss = interruption.loss();
    let index_first = loss.map_or(described.is_some(), |(_, first)| first);
    let recovery = if options.recovery == RecoveryFormat::Embedded {
        Vec::new()
    } else if loss.is_some() || described.is_some() {
        // Keep several repair blocks per article even for larger compressed
        // fixtures, without turning extraction scheduling into a codec benchmark.
        let slice = volumes
            .iter()
            .map(|(_, bytes)| bytes.len())
            .max()
            .unwrap()
            .div_ceil(32)
            .div_ceil(4)
            * 4;
        let slice = slice.max(PAR2_SLICE_BYTES as usize);
        let blocks: usize = volumes
            .iter()
            .map(|(_, bytes)| bytes.len().div_ceil(slice))
            .sum();
        let described: Vec<_> = volumes
            .iter()
            .enumerate()
            .map(|(index, (name, bytes))| {
                let name = described.map_or(name.as_str(), |names| names[index].as_str());
                (name, bytes.as_slice())
            })
            .collect();
        let blocks = if interruption.fails() || loss.is_none() {
            0
        } else {
            blocks
        };
        match options.recovery {
            RecoveryFormat::Par2 => {
                let bytes = build_test_par2_with_recovery(&described, slice as u64, blocks);
                let index = append_par2_index(&mut spec, &bytes);
                vec![ScheduleRecovery {
                    index,
                    name: "silver.horizon.par2".to_string(),
                    bytes,
                    format: RecoveryFormat::Par2,
                }]
            }
            RecoveryFormat::Par3 => {
                let described: Vec<_> = described
                    .iter()
                    .map(|(name, bytes)| (name.to_string(), bytes.to_vec()))
                    .collect();
                let mut carriers = par3_carriers_over(&described, slice as u64, blocks);
                // The index leads: it is what arrives where a PAR2 file would.
                carriers.sort_by_key(|(name, _)| name.contains(".vol"));
                let indices = append_single_article_files(&mut spec, &carriers);
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
            RecoveryFormat::Embedded => unreachable!("handled above"),
        }
    } else {
        Vec::new()
    };
    insert_active_job(&mut pipeline, job, spec.clone()).await;
    let mut trace = vec![];
    if let Some(index) = recovery.first()
        && index_first
    {
        submit_schedule_recovery(&mut pipeline, job, index, 0).await;
    }
    for (step, action, arrives) in interruption.boundaries(order.len()) {
        match action {
            BoundaryAction::Demote => {
                if profile == ExtractionProfile::DirectStore {
                    let before = pipeline.direct_store.demotions.len();
                    let sets = pipeline.direct_store.sets_for(job).len();
                    let (set, reason) = options.demotion.direct(&order, step, sets);
                    pipeline.demote_direct_set(job, set, reason).await;
                    if pipeline.direct_store.demotions.len() != before {
                        schedule_demoted = Some(reason);
                    }
                }
                // An incompatible set may already have left direct store and
                // started chase. Withdraw that owner at the same boundary too.
                let (latch, chase) = options.demotion.chase();
                pipeline.direct_unpack_abort_job(
                    job,
                    "schedule withdraws speculative extraction",
                    latch,
                    chase,
                );
                settle_direct_post_repair_work(&mut pipeline).await;
                trace.push(format!("demote at {step}"));
            }
            action @ (BoundaryAction::Restart | BoundaryAction::Crash) => {
                if action == BoundaryAction::Restart {
                    pipeline
                        .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
                        .await;
                }
                // A later interruption makes a promise of its own. What the run
                // asked for again under an earlier one answers to that one:
                // keep only the requests that broke it, and carry them over.
                let broken = durable.take().map(|promise: BTreeSet<_>| {
                    rerequested.retain(|article| promise.contains(article));
                    rerequested.clone()
                });
                durable = Some(if action == BoundaryAction::Restart {
                    // What the barrier just published is what the restart may
                    // rely on. Bytes held without a destination are not in it.
                    let mut kept = BTreeSet::new();
                    for blob in pipeline.db.load_direct_coverage(job).unwrap().values() {
                        use crate::pipeline::direct_store::snapshot;
                        // A finalized set's marker vouches for every article
                        // of every file it installed from.
                        if snapshot::is_installed_marker(blob) {
                            let installed = snapshot::decode_installed(blob).unwrap();
                            for (_, file_index) in installed.volumes {
                                kept.extend(
                                    (0..articles(file_index) as u32)
                                        .map(|article| (file_index, article))
                                        .filter(|article| delivered.contains(article)),
                                );
                            }
                            continue;
                        }
                        let snapshot =
                            crate::pipeline::direct_store::snapshot::decode(blob).unwrap();
                        for floor in snapshot.floors {
                            let Some((_, bytes)) = volumes.get(floor.file_index as usize) else {
                                continue;
                            };
                            let articles = articles(floor.file_index);
                            let covered = (0..articles as u32)
                                .take_while(|article| {
                                    article_extent(bytes.len(), *article, articles).1 as u64
                                        <= floor.floor
                                })
                                .count() as u32;
                            let kept_articles = if floor.complete {
                                articles as u32
                            } else {
                                covered.saturating_sub(1)
                            };
                            kept.extend(
                                (0..kept_articles)
                                    .map(|article| (floor.file_index, article))
                                    .filter(|article| delivered.contains(article)),
                            );
                        }
                    }
                    kept
                } else {
                    BTreeSet::new()
                });
                if let (Some(promise), Some(broken)) = (durable.as_mut(), broken) {
                    promise.extend(broken);
                }
                let counters = pipeline.direct_unpack.counters();
                chase_armed += counters.armed;
                chase_consumed += counters.consumed;
                finalized += pipeline.direct_store.finalized_sets;
                demotions.append(&mut pipeline.direct_store.demotions);
                let status = job_status_for_assert(&pipeline, job);
                pipeline.direct_unpack_shutdown("schedule restart").await;
                drop(pipeline);
                // The write handles are process-wide; a dead process takes its
                // handles with it, so the next incarnation must open its own.
                settle_direct_output_removals(root.path()).await;
                (pipeline, _, _) = new_direct_pipeline(&root).await;
                profile.configure(&mut pipeline);
                // Restore from the rows the dead process left, exactly as
                // startup recovery does. A job it had already finished is not
                // restored at all.
                let recovered = pipeline.db.load_active_jobs().unwrap().remove(&job);
                let recovered = recovered.filter(|recovered| {
                    !matches!(
                        recovered.status.as_str(),
                        "complete" | "failed" | "cancelled"
                    )
                });
                trace.push(format!(
                    "{action:?} at {step}: status={status:?} recovered={:?}",
                    recovered.as_ref().map(|recovered| (
                        &recovered.status,
                        &recovered.file_progress,
                        &recovered.complete_files,
                        &recovered.extracted_members
                    ))
                ));
                let Some(recovered) = recovered else {
                    retired = Some(status);
                    break;
                };
                use crate::jobs::model::{DownloadState, PostState, RunState};
                pipeline
                    .restore_job(RestoreJobRequest {
                        job_id: job,
                        job_hash: recovered.nzb_hash,
                        spec: spec.clone(),
                        complete_files: recovered.complete_files,
                        file_progress: recovered.file_progress,
                        detected_archives: recovered.detected_archives,
                        file_identities: recovered.file_identities,
                        extracted_members: recovered.extracted_members,
                        status: crate::jobs::model::job_status_from_persisted_str(
                            &recovered.status,
                            recovered.error.as_deref(),
                        ),
                        download_state: recovered
                            .download_state
                            .as_deref()
                            .and_then(DownloadState::parse),
                        post_state: recovered.post_state.as_deref().and_then(PostState::parse),
                        run_state: recovered.run_state.as_deref().and_then(RunState::parse),
                        queued_repair_at_epoch_ms: recovered.queued_repair_at_epoch_ms,
                        queued_extract_at_epoch_ms: recovered.queued_extract_at_epoch_ms,
                        paused_resume_status: None,
                        paused_resume_download_state: None,
                        paused_resume_post_state: None,
                        working_dir: recovered.output_dir,
                    })
                    .await
                    .unwrap();
                note_rerequests(&mut pipeline, job, &delivered, &mut rerequested);
            }
            BoundaryAction::None => {}
        }
        if !arrives {
            continue;
        }
        let Some(&(file, article)) = order.get(step) else {
            break;
        };
        if loss.is_some_and(|(mask, _)| mask & (1 << article_slot(&layout, file, article)) != 0) {
            trace.push(format!("lost {file}:{article}"));
            continue;
        }
        let articles = articles(file);
        deliver_schedule_article(&mut pipeline, job, volumes, file, article, articles).await;
        delivered.insert((file, article));
        trace.push(format!(
            "arrive {file}:{article}: {:?}",
            pipeline.direct_store.sets_for(job)
        ));
    }
    if let Some(index) = recovery.first()
        && !index_first
        && retired.is_none()
    {
        submit_schedule_recovery(&mut pipeline, job, index, 0).await;
    }
    // Every wait below is for a registered operation. A demotion can request
    // more articles; service those before waiting for an extraction result.
    for _ in 0..128 {
        if retired.is_some() {
            break;
        }
        note_rerequests(&mut pipeline, job, &delivered, &mut rerequested);
        if let Some((mask, _)) = loss {
            deliver_schedule_refetches(&mut pipeline, job, volumes, &layout, mask, &recovery).await;
        }
        drain_rar_refreshes(&mut pipeline).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
        if matches!(
            job_status_for_assert(&pipeline, job),
            Some(JobStatus::Complete | JobStatus::Failed { .. })
        ) {
            break;
        }
        let mut queued = peek_queued_segments(&mut pipeline, job);
        queued.dedup();
        if !queued.is_empty() {
            if let Some((mask, _)) = loss {
                // A container probe can re-request its missing first article
                // while the queues settle. Answer that request as unavailable
                // before completion checks exhaustion, just like a server does.
                deliver_schedule_refetches(&mut pipeline, job, volumes, &layout, mask, &recovery)
                    .await;
            } else {
                trace.push(format!("refetch {queued:?}"));
                for (file, article) in queued {
                    if matches!(
                        job_status_for_assert(&pipeline, job),
                        Some(JobStatus::Complete | JobStatus::Failed { .. })
                    ) {
                        break;
                    }
                    dispatch_and_submit(&mut pipeline, job, volumes, file, article, articles(file))
                        .await;
                }
                continue;
            }
        }
        // Drive the actor's quiescent-write action explicitly. A demoted
        // partial volume can leave a short write behind after the queue drains.
        pipeline.flush_quiescent_write_backlog().await;
        pipeline.check_job_completion(job).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
        if matches!(
            job_status_for_assert(&pipeline, job),
            Some(JobStatus::Complete | JobStatus::Failed { .. })
        ) {
            break;
        }
        if !peek_queued_segments(&mut pipeline, job).is_empty() {
            continue;
        }
        if pipeline
            .inflight_extractions
            .get(&job)
            .is_some_and(|sets| !sets.is_empty())
            || pipeline.has_active_rar_workers(job)
        {
            let done = pipeline
                .extract_done_rx
                .recv()
                .await
                .expect("registered extraction receipt");
            pipeline.handle_extraction_done(done).await;
        } else if recovery.is_empty() && options.recovery != RecoveryFormat::Embedded {
            // An embedded set's verification and repair are settled by the
            // pump above and may take another completion round to land.
            panic!(
                "archive stalled without an outstanding operation: {} trace={trace:?}",
                debug_job_state(&pipeline, job)
            );
        }
    }
    let status = retired.unwrap_or_else(|| job_status_for_assert(&pipeline, job));
    let queued = if pipeline.jobs.contains_key(&job) {
        peek_queued_segments(&mut pipeline, job)
    } else {
        Vec::new()
    };
    trace.push(format!(
        "terminal: {}; analyses={} repairs={}; par2={:?}; queued={queued:?}; sets={:?}",
        debug_job_state(&pipeline, job),
        pipeline.par2_repairer_analyze_calls,
        pipeline.par2_repairer_execute_calls,
        pipeline
            .par2_set(job)
            .map(|set| (set.files.len(), set.recovery_slices.len())),
        pipeline.direct_store.sets_for(job)
    ));
    let files = wanted
        .iter()
        .map(|name| ((*name).to_string(), std::fs::read(output.join(name)).ok()))
        .collect();
    settle_direct_output_removals(root.path()).await;
    let counters = pipeline.direct_unpack.counters();
    chase_armed += counters.armed;
    chase_consumed += counters.consumed;
    finalized += pipeline.direct_store.finalized_sets;
    demotions.append(&mut pipeline.direct_store.demotions);
    let mut published = files_under(&output);
    published.remove(crate::jobs::working_dir::OUTPUT_DIR_MARKER);
    let mut leftovers: BTreeSet<String> = files_under(&pipeline.intermediate_dir)
        .into_iter()
        .map(|path| format!("intermediate/{path}"))
        .collect();
    leftovers.extend(
        files_under(&complete.join(".weaver-staging"))
            .into_iter()
            .map(|path| format!("staging/{path}")),
    );
    trace.push(format!(
        "profile={profile:?}; chase_armed={chase_armed}; chase_consumed={chase_consumed}; current={counters:?}; demotions={demotions:?}; rerequested={rerequested:?}"
    ));
    Outcome {
        status,
        files,
        trace,
        finalized,
        chase_armed,
        chase_consumed,
        demotions,
        schedule_demoted,
        published,
        leftovers,
        rerequested,
        durable: durable.unwrap_or(delivered),
    }
}

/// Records every volume article sitting in the download queue that the
/// schedule has already handed over: the pipeline is asking for it twice.
fn note_rerequests(
    pipeline: &mut Pipeline,
    job: JobId,
    delivered: &BTreeSet<(u32, u32)>,
    rerequested: &mut BTreeSet<(u32, u32)>,
) {
    if !pipeline.jobs.contains_key(&job) {
        return;
    }
    rerequested.extend(
        peek_queued_segments(pipeline, job)
            .into_iter()
            .filter(|article| delivered.contains(article)),
    );
}

#[tokio::test]
async fn conventional_par2_scan_cannot_borrow_direct_scratch_awaiting_removal() {
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job = JobId(42201);
    let volumes = quick_open_store_set("feature.mkv", &[42; 193], None);
    let index = repairable_par2_index(&volumes, 0);
    let mut spec = direct_store_job_spec("Scratch scan lifetime", &volumes);
    let index_file = append_par2_index(&mut spec, &index);
    let working = insert_active_job(&mut pipeline, job, spec).await;
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id: job,
            file_index: index_file,
        },
        0,
        0,
        &index,
        "silver.horizon.par2",
        None,
    )
    .await;
    let set = pipeline.par2_set(job).unwrap().clone();
    let plan = pipeline.direct_store.set(job, 0).unwrap().plan().clone();
    let scratch_paths = [
        plan.envelope_path(0),
        plan.repair_path(0),
        plan.holds_scratch_path(),
    ];
    for scratch in scratch_paths {
        // Deterministically hold the pre-unlink state. These bytes would
        // satisfy a source scan, but their owner is free to delete them before
        // execution: conventional repair must never retain them as evidence.
        std::fs::write(&scratch, &volumes[0].1).unwrap();
        let mut options = par2_rs::Par2RepairSessionOptions::new(working.clone(), vec![]);
        options.file_set = Some((*set).clone());
        options.exclude_paths = pipeline.par2_extra_scan_exclusions(job, set.recovery_set_id);
        let mut session = par2_rs::Par2RepairSession::open(options).unwrap();
        let outcome = session.analyze().unwrap();
        assert_eq!(outcome.available_blocks, 0, "scratch={scratch:?}");
        assert_eq!(outcome.status, par2_rs::Par2RepairStatus::Insufficient);
        std::fs::remove_file(&scratch).unwrap();
    }
}

#[tokio::test]
async fn encrypted_restart_repair_preserves_direct_finalization() {
    let name = "nested/feature.mkv";
    let password = "moonlit-harbour";
    let payload: Vec<u8> = (0..6001).map(|n| ((n * 7 + n / 251) % 253) as u8).collect();
    for salt in [Some(TEST_RAR4_SALT), None] {
        let volumes = encrypted_rar4_store_set(name, &payload, 2, password, salt);
        let mut spec = direct_store_job_spec("Encrypted restart repair", &volumes);
        spec.password = Some(password.to_string());
        let outcome = run_schedule(
            DirectStoreGate::Enabled,
            spec,
            &volumes,
            &[(0, 0), (0, 0), (1, 0), (1, 1)],
            &[name],
            Interruption::Combined {
                mask: 2,
                index_first: false,
                action: BoundaryAction::Restart,
                at: 1,
            },
        )
        .await;
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "{:?}",
            outcome.trace
        );
        assert_eq!(outcome.finalized, 1, "{:?}", outcome.trace);
        assert_eq!(outcome.files[name].as_deref(), Some(payload.as_slice()));
    }
}

#[derive(Clone, Copy, Debug)]
enum Format {
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
    /// Four single-article volumes, so two of the volumes are middle volumes
    /// that both continue and are continued.
    Rar4FourVolumes,
    Rar5FourVolumes,
    Rar4EncryptedFourVolumes,
    /// Volume names that say nothing. The recovery set carries the real
    /// names, as an obfuscated post's does, and the set is admitted by them.
    Rar5Obfuscated,
    Rar4Obfuscated,
}

impl Format {
    fn volume_count(self) -> usize {
        match self {
            Self::Rar4FourVolumes | Self::Rar5FourVolumes | Self::Rar4EncryptedFourVolumes => 4,
            _ => 2,
        }
    }

    fn route(self) -> Route {
        match self {
            Self::Rar4
            | Self::Rar5
            | Self::Rar4FourVolumes
            | Self::Rar5FourVolumes
            | Self::Rar4EncryptedFourVolumes
            | Self::Rar4Encrypted
            | Self::Rar4Unsalted
            | Self::Rar5Encrypted
            | Self::Rar5KeyedChecksum
            | Self::Rar5EncryptedHeaders
            | Self::QuickOpen => Route::DIRECT,
            // Slots 0 and 2 are the two volumes' offset-zero articles.
            Self::Rar5Obfuscated | Self::Rar4Obfuscated => Route {
                unnamed_loss: |mask| mask & 0b0101 != 0,
                ..Route::DIRECT
            },
            Self::Rar5UncheckedHeaders => {
                Route::refused(|reason| matches!(reason, DemotionReason::HeaderEncryptedRefused(_)))
            }
            Self::Blake2 => Route::refused(|reason| {
                matches!(
                    reason,
                    DemotionReason::MemberIneligible(MemberIneligibility::Blake2OnlyNoCrc32)
                )
            }),
        }
    }
}

async fn campaign(format: Format, selection: Selection) {
    profile_campaign(format, selection, ExtractionProfile::DirectStore).await;
}

async fn chase_campaign(format: Format, selection: Selection) {
    profile_campaign(format, selection, ExtractionProfile::Chase).await;
}

async fn conventional_campaign(format: Format, selection: Selection) {
    profile_campaign(format, selection, ExtractionProfile::Conventional).await;
}

async fn profile_campaign(format: Format, selection: Selection, profile: ExtractionProfile) {
    let cases = selected_schedules(selection);
    let wrong_password = wrong_password_schedules(selection);
    slot_campaign(
        format,
        selection,
        profile,
        4,
        wrong_password,
        cases,
        |_, _, _, _| false,
    )
    .await;
}

/// Adjusts a case's outcome for a product defect the campaign holds open,
/// before the case's rules see it. True holds the whole case open: none of
/// its rules apply.
type KnownDefect = fn(Format, &[(u32, u32)], Interruption, &mut Outcome) -> bool;

/// A format's campaign over `slots` article slots (see [`slot_layout`]): the
/// given wrong-password schedules, then each case held to the same rules.
async fn slot_campaign(
    format: Format,
    selection: Selection,
    profile: ExtractionProfile,
    slots: usize,
    wrong_password: Vec<Schedule>,
    cases: Vec<(usize, Schedule)>,
    known_defect: KnownDefect,
) {
    let name = "nested/feature.mkv";
    let password = "moonlit-harbour";
    let length = match format {
        Format::QuickOpen => 193,
        // A described volume is bound by the fingerprint of its first 16 KiB,
        // which its offset-zero article has to cover whole, as every real
        // article does. Two articles a volume puts that at 32 KiB a volume.
        Format::Rar5Obfuscated | Format::Rar4Obfuscated => 70_001,
        _ => 6001,
    };
    let payload: Vec<u8> = (0..length)
        .map(|n| ((n * 7 + n / 251) % 253) as u8)
        .collect();
    let count = format.volume_count();
    let volumes = match format {
        Format::Rar4 | Format::Rar4FourVolumes => {
            single_member_rar4_store_set(name, &payload, count)
        }
        Format::Rar5 | Format::Rar5FourVolumes => single_member_store_set(name, &payload, count),
        Format::Rar5Obfuscated => obfuscate_volumes(&single_member_store_set(name, &payload, 2)),
        Format::Rar4Obfuscated => {
            obfuscate_volumes(&single_member_rar4_store_set(name, &payload, 2))
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
        Format::Rar5Obfuscated => Some(single_member_store_set(name, &payload, 2)),
        Format::Rar4Obfuscated => Some(single_member_rar4_store_set(name, &payload, 2)),
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
    let baseline = run_described_schedule(
        ExtractionProfile::Conventional,
        spec.clone(),
        &volumes,
        described.as_deref(),
        &slot_arrivals(slots),
        &[name],
        Interruption::None,
    )
    .await;
    {
        assert_eq!(baseline.status, Some(JobStatus::Complete));
        ExtractionProfile::Conventional.assert_route(&baseline);
        assert!(
            baseline.files[name].as_deref() == Some(payload.as_slice()),
            "conventional oracle {format:?}"
        );
    }
    // A password the archive does not open with. An archive that needs none
    // must not notice it.
    for (order, interruption) in wrong_password {
        if !profile.includes(interruption) {
            continue;
        }
        let mut wrong = spec.clone();
        wrong.password = Some("incorrect-key".to_string());
        eprintln!(
            "wrong password {format:?} profile={profile:?} order={order:?} interruption={interruption:?}"
        );
        let outcome = run_described_schedule(
            profile,
            wrong,
            &volumes,
            described.as_deref(),
            &order,
            &[name],
            interruption,
        )
        .await;
        if encrypted {
            profile.assert_rejected(&outcome, &[name]);
        } else {
            assert_eq!(
                outcome.status,
                Some(JobStatus::Complete),
                "{:?}",
                outcome.trace
            );
            profile.assert_delivery(&outcome, format.route(), &[name], interruption);
            assert_eq!(outcome.files[name].as_deref(), Some(payload.as_slice()));
        }
    }
    for (case, (order, interruption)) in cases {
        if !profile.includes(interruption) {
            continue;
        }
        eprintln!(
            "{format:?} profile={profile:?} selection={selection:?} case={case} order={order:?} interruption={interruption:?}"
        );
        let mut actual = run_described_schedule(
            profile,
            spec.clone(),
            &volumes,
            described.as_deref(),
            &order,
            &[name],
            interruption,
        )
        .await;
        if known_defect(format, &order, interruption, &mut actual) {
            continue;
        }
        if interruption.fails() {
            profile.assert_rejected(&actual, &[name]);
            continue;
        }
        let mut route = format.route();
        if matches!(format, Format::Rar4Obfuscated) {
            // A RAR4 volume says nothing of its set in its own headers, so a
            // recovery set that arrives last finds every volume already landed.
            if interruption.loss().is_some_and(|(_, first)| !first) {
                route.unnamed_loss = |_| true;
            }
        }
        assert_eq!(
            actual.status,
            Some(JobStatus::Complete),
            "{format:?} case={case} order={order:?} interruption={interruption:?} trace={:?}",
            actual.trace
        );
        profile.assert_delivery(&actual, route, &[name], interruption);
        // A duplicate can invalidate an already-running RAR chase. Clean
        // unique arrivals must consume chase; duplicate schedules still must
        // admit it and produce the same verified output through safe fallback.
        let unique_arrivals = order.len() == slots;
        if profile == ExtractionProfile::Chase && matches!(interruption, Interruption::None) {
            assert!(actual.chase_armed > 0, "{format:?}: {:?}", actual.trace);
            if unique_arrivals {
                assert_eq!(actual.chase_consumed, 1, "{format:?}: {:?}", actual.trace);
            }
        }
        if profile == ExtractionProfile::DirectStore && matches!(interruption, Interruption::None) {
            let expected = usize::from(!matches!(
                format,
                Format::Rar5UncheckedHeaders | Format::Blake2
            ));
            assert_eq!(
                actual.finalized, expected,
                "{format:?} case={case} order={order:?}: {:?}",
                actual.trace
            );
            if expected == 0 {
                assert!(actual.chase_armed > 0, "{format:?}: {:?}", actual.trace);
                if unique_arrivals {
                    assert_eq!(actual.chase_consumed, 1, "{format:?}: {:?}", actual.trace);
                }
            }
        }
        assert_eq!(
            actual.files[name].as_deref(),
            Some(payload.as_slice()),
            "{format:?} case={case} order={order:?} {:?}",
            actual.trace
        );
    }
}

#[tokio::test]
async fn rar4_arrival_schedules() {
    campaign(Format::Rar4, Selection::Smoke).await;
}
#[tokio::test]
async fn rar5_arrival_schedules() {
    campaign(Format::Rar5, Selection::Smoke).await;
}
#[tokio::test]
async fn rar4_encrypted_arrival_schedules() {
    campaign(Format::Rar4Encrypted, Selection::Smoke).await;
}
#[tokio::test]
async fn rar4_unsalted_arrival_schedules() {
    campaign(Format::Rar4Unsalted, Selection::Smoke).await;
}
#[tokio::test]
async fn rar5_encrypted_arrival_schedules() {
    campaign(Format::Rar5Encrypted, Selection::Smoke).await;
}
#[tokio::test]
async fn rar5_keyed_checksum_arrival_schedules() {
    campaign(Format::Rar5KeyedChecksum, Selection::Smoke).await;
}
#[tokio::test]
async fn rar5_header_encrypted_arrival_schedules() {
    campaign(Format::Rar5EncryptedHeaders, Selection::Smoke).await;
}
#[tokio::test]
async fn rar5_unchecked_header_arrival_schedules() {
    campaign(Format::Rar5UncheckedHeaders, Selection::Smoke).await;
}
#[tokio::test]
async fn quick_open_arrival_schedules() {
    campaign(Format::QuickOpen, Selection::Smoke).await;
}
#[tokio::test]
async fn blake2_arrival_schedules() {
    campaign(Format::Blake2, Selection::Smoke).await;
}
#[tokio::test]
async fn rar4_four_volume_arrival_schedules() {
    campaign(Format::Rar4FourVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn rar5_four_volume_arrival_schedules() {
    campaign(Format::Rar5FourVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn rar4_encrypted_four_volume_arrival_schedules() {
    campaign(Format::Rar4EncryptedFourVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn rar5_obfuscated_arrival_schedules() {
    campaign(Format::Rar5Obfuscated, Selection::Smoke).await;
}

/// Two single-volume stored sets in one job, two articles each. A demotion,
/// a restart or a loss in one set is not a reason for the other to leave
/// direct store.
async fn two_set_campaign(profile: ExtractionProfile, selection: Selection) {
    let members = ["alpha.mkv", "nested/beta.mkv"];
    let payloads: Vec<Vec<u8>> = [(6001, 7), (4093, 11)]
        .into_iter()
        .map(|(len, step)| {
            (0..len)
                .map(|n| ((n * step + n / 251) % 253) as u8)
                .collect()
        })
        .collect();
    let volumes: Vec<_> = ["alpha", "beta"]
        .into_iter()
        .zip(members.iter().zip(&payloads))
        .map(|(stem, (member, payload))| {
            let (_, bytes) = single_member_store_set(member, payload, 1).remove(0);
            (format!("{stem}.part01.rar"), bytes)
        })
        .collect();
    let spec = direct_store_job_spec("Two set schedules", &volumes);
    let route = Route {
        sets: 2,
        ..Route::DIRECT
    };
    let check = |outcome: &Outcome, interruption: Interruption| {
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "{interruption:?}: {:?}",
            outcome.trace
        );
        profile.assert_delivery(outcome, route, &members, interruption);
        for (member, payload) in members.iter().zip(&payloads) {
            assert_eq!(
                outcome.files[*member].as_deref(),
                Some(payload.as_slice()),
                "{member} {interruption:?}: {:?}",
                outcome.trace
            );
        }
    };
    for (order, interruption) in wrong_password_schedules(selection) {
        if !profile.includes(interruption) {
            continue;
        }
        let mut wrong = spec.clone();
        wrong.password = Some("incorrect-key".to_string());
        let outcome =
            run_profile_schedule(profile, wrong, &volumes, &order, &members, interruption).await;
        check(&outcome, interruption);
    }
    for (case, (order, interruption)) in selected_schedules(selection) {
        if !profile.includes(interruption) {
            continue;
        }
        eprintln!(
            "two sets profile={profile:?} selection={selection:?} case={case} order={order:?} interruption={interruption:?}"
        );
        let outcome = run_profile_schedule(
            profile,
            spec.clone(),
            &volumes,
            &order,
            &members,
            interruption,
        )
        .await;
        if interruption.fails() {
            // The job fails, but only the set the loss reached is beyond
            // repair: the other one owes nothing to it.
            let (mask, _) = interruption.loss().unwrap();
            let damaged: Vec<_> = members
                .iter()
                .enumerate()
                .filter(|(set, _)| mask & (0b11 << (set * 2)) != 0)
                .map(|(_, member)| *member)
                .collect();
            assert!(
                matches!(outcome.status, Some(JobStatus::Failed { .. })),
                "{:?}: {:?}",
                outcome.status,
                outcome.trace
            );
            assert!(
                outcome.finalized <= members.len() - damaged.len(),
                "{:?}",
                outcome.trace
            );
            for member in &damaged {
                assert!(
                    outcome.files[*member].is_none() && !outcome.published.contains(*member),
                    "unrepairable {member} published: {:?}",
                    outcome.trace
                );
            }
            continue;
        }
        check(&outcome, interruption);
    }
}

#[tokio::test]
async fn two_set_arrival_schedules() {
    two_set_campaign(ExtractionProfile::DirectStore, Selection::Smoke).await;
}

#[derive(Clone, Copy, Debug)]
enum CompressedFormat {
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
    /// The posted volumes of a multi-volume set and its oracle key. Every
    /// other format posts its one fixture archive whole.
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

    /// The archive the expected members are decoded from. A multi-volume set
    /// packs the member its single-volume counterpart does, and the oracle
    /// pins the bytes for the set under its own key.
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

impl CompressedFormat {
    /// Each refusal names what the archive itself puts beyond a byte copy:
    /// a compressed, solid or encrypted member has no bytes of its own to
    /// place, and encrypted headers hide the layout altogether.
    fn route(self) -> Route {
        use DemotionReason::{HeaderEncryptedRefused, MemberIneligible};
        use MemberIneligibility::{Compressed, Encrypted, Solid};
        match self {
            // The stored members route; the compressed one rides the member
            // tolerance and is extracted at finalization.
            Self::Rar4Mixed | Self::Rar5Mixed => Route::DIRECT,
            Self::Rar4Lz
            | Self::Rar5Lz
            | Self::Rar4LzTwoVolumes
            | Self::Rar5LzTwoVolumes
            | Self::Rar4LzFourVolumes
            | Self::Rar5LzFourVolumes => {
                Route::refused(|reason| matches!(reason, MemberIneligible(Compressed)))
            }
            Self::Rar4Solid
            | Self::Rar4Ppmd
            | Self::Rar5Solid
            | Self::Rar4SolidTwoVolumes
            | Self::Rar5SolidTwoVolumes
            | Self::Rar4SolidFourVolumes
            | Self::Rar5SolidFourVolumes => Route::refused(|reason| {
                matches!(reason, MemberIneligible(Compressed | Solid))
            }),
            Self::Rar4Encrypted
            | Self::Rar4SolidEncrypted
            | Self::Rar4PpmdEncrypted
            | Self::Rar5Encrypted
            | Self::Rar5SolidEncrypted
            | Self::Rar4EncryptedTwoVolumes
            | Self::Rar5EncryptedTwoVolumes
            | Self::Rar4EncryptedFourVolumes
            | Self::Rar5EncryptedFourVolumes
            | Self::Rar4SolidEncryptedTwoVolumes
            | Self::Rar5SolidEncryptedTwoVolumes
            | Self::Rar4SolidEncryptedFourVolumes
            | Self::Rar5SolidEncryptedFourVolumes => Route::refused(|reason| {
                matches!(reason, MemberIneligible(Compressed | Solid | Encrypted))
            }),
            Self::Rar4Headers
            | Self::Rar4SolidHeaders
            | Self::Rar4PpmdHeaders
            | Self::Rar5Headers
            | Self::Rar4HeadersTwoVolumes
            | Self::Rar5HeadersTwoVolumes
            | Self::Rar4HeadersFourVolumes
            | Self::Rar5HeadersFourVolumes
            // Headers that open with the password refuse the member they
            // describe, which is itself encrypted.
            | Self::Rar5SolidHeaders => Route::refused(|reason| {
                matches!(
                    reason,
                    HeaderEncryptedRefused(_) | MemberIneligible(Compressed | Solid | Encrypted)
                )
            }),
        }
    }
}

async fn compressed_direct_campaign(format: CompressedFormat, selection: Selection) {
    compressed_campaign(format, selection, ExtractionProfile::DirectStore).await;
}

async fn compressed_chase_campaign(format: CompressedFormat, selection: Selection) {
    compressed_campaign(format, selection, ExtractionProfile::Chase).await;
}

async fn compressed_conventional_campaign(format: CompressedFormat, selection: Selection) {
    compressed_campaign(format, selection, ExtractionProfile::Conventional).await;
}

async fn compressed_campaign(
    format: CompressedFormat,
    selection: Selection,
    profile: ExtractionProfile,
) {
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
    let wanted = expected.keys().map(String::as_str).collect::<Vec<_>>();
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
    for (order, interruption) in wrong_password_schedules(selection) {
        if !profile.includes(interruption) {
            continue;
        }
        let mut wrong = spec.clone();
        wrong.password = Some("incorrect-key".to_owned());
        eprintln!(
            "wrong password {format:?} profile={profile:?} order={order:?} interruption={interruption:?}"
        );
        let outcome =
            run_profile_schedule(profile, wrong, &volumes, &order, &wanted, interruption).await;
        if password.is_some() {
            profile.assert_rejected(&outcome, &wanted);
        } else {
            assert_eq!(
                outcome.status,
                Some(JobStatus::Complete),
                "{:?}",
                outcome.trace
            );
            profile.assert_delivery(&outcome, format.route(), &wanted, interruption);
            for (name, bytes) in &expected {
                assert_eq!(outcome.files[name].as_deref(), Some(bytes.as_slice()));
            }
        }
    }
    for (case, (order, interruption)) in selected_schedules(selection) {
        if !profile.includes(interruption) {
            continue;
        }
        eprintln!(
            "{format:?} profile={profile:?} selection={selection:?} case={case} order={order:?} interruption={interruption:?}"
        );
        let outcome = run_profile_schedule(
            profile,
            spec.clone(),
            &volumes,
            &order,
            &wanted,
            interruption,
        )
        .await;
        if interruption.fails() {
            profile.assert_rejected(&outcome, &wanted);
            continue;
        }
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "{format:?}: {:?}",
            outcome.trace
        );
        profile.assert_delivery(&outcome, format.route(), &wanted, interruption);
        if profile != ExtractionProfile::Conventional && interruption == Interruption::None {
            assert!(outcome.chase_armed > 0, "{format:?}: {:?}", outcome.trace);
            if order.len() == 4 {
                assert_eq!(outcome.chase_consumed, 1, "{format:?}: {:?}", outcome.trace);
                if profile == ExtractionProfile::DirectStore {
                    assert_eq!(
                        outcome.finalized,
                        usize::from(matches!(
                            format,
                            CompressedFormat::Rar4Mixed | CompressedFormat::Rar5Mixed
                        )),
                        "{format:?}: {:?}",
                        outcome.trace
                    );
                }
            }
        }
        for (name, bytes) in &expected {
            assert_eq!(
                outcome.files[name].as_deref(),
                Some(bytes.as_slice()),
                "{format:?} member={name} case={case}: {:?}",
                outcome.trace
            );
        }
    }
}

#[tokio::test]
async fn compressed_rar4_mixed_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar4Mixed, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar4_lz_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar4Lz, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar4_solid_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar4Solid, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar4_solid_encrypted_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar4SolidEncrypted, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar4_solid_headers_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar4SolidHeaders, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar4_ppmd_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar4Ppmd, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar4_ppmd_encrypted_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar4PpmdEncrypted, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar4_ppmd_headers_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar4PpmdHeaders, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar4_encrypted_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar4Encrypted, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar4_headers_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar4Headers, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar5_mixed_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar5Mixed, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar5_lz_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar5Lz, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar5_encrypted_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar5Encrypted, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar5_headers_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar5Headers, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar5_solid_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar5Solid, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar5_solid_encrypted_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar5SolidEncrypted, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar5_solid_headers_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar5SolidHeaders, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar4_lz_two_volume_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar4LzTwoVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar5_lz_two_volume_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar5LzTwoVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar4_lz_four_volume_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar4LzFourVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar5_lz_four_volume_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar5LzFourVolumes, Selection::Smoke).await;
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
    // A layout slow enough to need [`FINE_SHARDS`] shards and its password
    // schedules in [`WRONG_PASSWORD_PARTS`] parts.
    ($module:ident, $variant:expr, $run:ident, fine) => {
        mod $module {
            use super::*;
            combined_campaign!(@fine_shards $variant, $run);
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn wrong_password_part_0() {
                $run($variant, Selection::WrongPasswordPart(0)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn wrong_password_part_1() {
                $run($variant, Selection::WrongPasswordPart(1)).await;
            }
            const _: () = assert!(WRONG_PASSWORD_PARTS == 2);
        }
    };
    (@fine_shards $variant:expr, $run:ident) => {
        combined_campaign!(
            @each_fine $variant, $run;
                shard_000 0, shard_001 1, shard_002 2, shard_003 3, shard_004 4,
                shard_005 5, shard_006 6, shard_007 7, shard_008 8, shard_009 9,
                shard_010 10, shard_011 11, shard_012 12, shard_013 13, shard_014 14,
                shard_015 15, shard_016 16, shard_017 17, shard_018 18, shard_019 19,
                shard_020 20, shard_021 21, shard_022 22, shard_023 23, shard_024 24,
                shard_025 25, shard_026 26, shard_027 27, shard_028 28, shard_029 29,
                shard_030 30, shard_031 31, shard_032 32, shard_033 33, shard_034 34,
                shard_035 35, shard_036 36, shard_037 37, shard_038 38, shard_039 39,
                shard_040 40, shard_041 41, shard_042 42, shard_043 43, shard_044 44,
                shard_045 45, shard_046 46, shard_047 47, shard_048 48, shard_049 49,
                shard_050 50, shard_051 51, shard_052 52, shard_053 53, shard_054 54,
                shard_055 55, shard_056 56, shard_057 57, shard_058 58, shard_059 59,
                shard_060 60, shard_061 61, shard_062 62, shard_063 63, shard_064 64,
                shard_065 65, shard_066 66, shard_067 67, shard_068 68, shard_069 69,
                shard_070 70, shard_071 71, shard_072 72, shard_073 73, shard_074 74,
                shard_075 75, shard_076 76, shard_077 77, shard_078 78, shard_079 79,
                shard_080 80, shard_081 81, shard_082 82, shard_083 83, shard_084 84,
                shard_085 85, shard_086 86, shard_087 87, shard_088 88, shard_089 89,
                shard_090 90, shard_091 91, shard_092 92, shard_093 93, shard_094 94,
                shard_095 95, shard_096 96, shard_097 97, shard_098 98, shard_099 99,
                shard_100 100, shard_101 101, shard_102 102, shard_103 103, shard_104 104,
                shard_105 105, shard_106 106, shard_107 107, shard_108 108, shard_109 109,
                shard_110 110, shard_111 111, shard_112 112, shard_113 113, shard_114 114,
                shard_115 115, shard_116 116, shard_117 117, shard_118 118, shard_119 119,
                shard_120 120, shard_121 121, shard_122 122, shard_123 123, shard_124 124,
                shard_125 125, shard_126 126, shard_127 127
        );
        const _: () = assert!(FINE_SHARDS == 128);
    };
    (@each_fine $variant:expr, $run:ident; $($name:ident $shard:literal),+) => {
        $(
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn $name() {
                $run($variant, Selection::FineShard($shard)).await;
            }
        )+
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
combined_campaign!(combined_rar4, Format::Rar4, campaign);
combined_campaign!(combined_rar5, Format::Rar5, campaign);
combined_campaign!(combined_rar4_encrypted, Format::Rar4Encrypted, campaign);
combined_campaign!(combined_rar4_unsalted, Format::Rar4Unsalted, campaign);
combined_campaign!(combined_rar5_encrypted, Format::Rar5Encrypted, campaign);
combined_campaign!(
    combined_rar5_keyed_checksum,
    Format::Rar5KeyedChecksum,
    campaign
);
combined_campaign!(
    combined_rar5_header_encrypted,
    Format::Rar5EncryptedHeaders,
    campaign
);
combined_campaign!(
    combined_rar5_unchecked_header,
    Format::Rar5UncheckedHeaders,
    campaign
);
combined_campaign!(combined_quick_open, Format::QuickOpen, campaign);
combined_campaign!(combined_blake2, Format::Blake2, campaign);

combined_campaign!(combined_chase_rar4, Format::Rar4, chase_campaign);
combined_campaign!(combined_chase_rar5, Format::Rar5, chase_campaign);
combined_campaign!(
    combined_chase_rar4_encrypted,
    Format::Rar4Encrypted,
    chase_campaign
);
combined_campaign!(
    combined_chase_rar4_unsalted,
    Format::Rar4Unsalted,
    chase_campaign
);
combined_campaign!(
    combined_chase_rar5_encrypted,
    Format::Rar5Encrypted,
    chase_campaign
);
combined_campaign!(
    combined_chase_rar5_keyed_checksum,
    Format::Rar5KeyedChecksum,
    chase_campaign
);
combined_campaign!(
    combined_chase_rar5_header_encrypted,
    Format::Rar5EncryptedHeaders,
    chase_campaign
);
combined_campaign!(
    combined_chase_rar5_unchecked_header,
    Format::Rar5UncheckedHeaders,
    chase_campaign
);
combined_campaign!(combined_chase_quick_open, Format::QuickOpen, chase_campaign);
combined_campaign!(combined_chase_blake2, Format::Blake2, chase_campaign);

combined_campaign!(
    combined_conventional_rar4,
    Format::Rar4,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_rar5,
    Format::Rar5,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_rar4_encrypted,
    Format::Rar4Encrypted,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_rar4_unsalted,
    Format::Rar4Unsalted,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_rar5_encrypted,
    Format::Rar5Encrypted,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_rar5_keyed_checksum,
    Format::Rar5KeyedChecksum,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_rar5_header_encrypted,
    Format::Rar5EncryptedHeaders,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_rar5_unchecked_header,
    Format::Rar5UncheckedHeaders,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_quick_open,
    Format::QuickOpen,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_blake2,
    Format::Blake2,
    conventional_campaign
);
combined_campaign!(
    combined_two_sets,
    ExtractionProfile::DirectStore,
    two_set_campaign
);
combined_campaign!(
    combined_chase_two_sets,
    ExtractionProfile::Chase,
    two_set_campaign
);
combined_campaign!(
    combined_conventional_two_sets,
    ExtractionProfile::Conventional,
    two_set_campaign
);
combined_campaign!(combined_rar4_four_volume, Format::Rar4FourVolumes, campaign);
combined_campaign!(
    combined_chase_rar4_four_volume,
    Format::Rar4FourVolumes,
    chase_campaign
);
combined_campaign!(
    combined_conventional_rar4_four_volume,
    Format::Rar4FourVolumes,
    conventional_campaign
);
combined_campaign!(combined_rar5_four_volume, Format::Rar5FourVolumes, campaign);
combined_campaign!(
    combined_chase_rar5_four_volume,
    Format::Rar5FourVolumes,
    chase_campaign
);
combined_campaign!(
    combined_conventional_rar5_four_volume,
    Format::Rar5FourVolumes,
    conventional_campaign
);
combined_campaign!(
    combined_rar4_encrypted_four_volume,
    Format::Rar4EncryptedFourVolumes,
    campaign
);
combined_campaign!(
    combined_chase_rar4_encrypted_four_volume,
    Format::Rar4EncryptedFourVolumes,
    chase_campaign
);
combined_campaign!(
    combined_conventional_rar4_encrypted_four_volume,
    Format::Rar4EncryptedFourVolumes,
    conventional_campaign
);
combined_campaign!(combined_rar5_obfuscated, Format::Rar5Obfuscated, campaign);
combined_campaign!(
    combined_chase_rar5_obfuscated,
    Format::Rar5Obfuscated,
    chase_campaign
);
combined_campaign!(
    combined_conventional_rar5_obfuscated,
    Format::Rar5Obfuscated,
    conventional_campaign
);

combined_campaign!(
    combined_compressed_direct_rar4_mixed,
    CompressedFormat::Rar4Mixed,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_lz,
    CompressedFormat::Rar4Lz,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_encrypted,
    CompressedFormat::Rar4Encrypted,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_headers,
    CompressedFormat::Rar4Headers,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_direct_rar5_mixed,
    CompressedFormat::Rar5Mixed,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_direct_rar5_lz,
    CompressedFormat::Rar5Lz,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_direct_rar5_encrypted,
    CompressedFormat::Rar5Encrypted,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_direct_rar5_headers,
    CompressedFormat::Rar5Headers,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_mixed,
    CompressedFormat::Rar4Mixed,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_lz,
    CompressedFormat::Rar4Lz,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_encrypted,
    CompressedFormat::Rar4Encrypted,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_headers,
    CompressedFormat::Rar4Headers,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_chase_rar5_mixed,
    CompressedFormat::Rar5Mixed,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_chase_rar5_lz,
    CompressedFormat::Rar5Lz,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_chase_rar5_encrypted,
    CompressedFormat::Rar5Encrypted,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_chase_rar5_headers,
    CompressedFormat::Rar5Headers,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_mixed,
    CompressedFormat::Rar4Mixed,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_lz,
    CompressedFormat::Rar4Lz,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_encrypted,
    CompressedFormat::Rar4Encrypted,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_headers,
    CompressedFormat::Rar4Headers,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar5_mixed,
    CompressedFormat::Rar5Mixed,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar5_lz,
    CompressedFormat::Rar5Lz,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar5_encrypted,
    CompressedFormat::Rar5Encrypted,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar5_headers,
    CompressedFormat::Rar5Headers,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar5_solid,
    CompressedFormat::Rar5Solid,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar5_solid,
    CompressedFormat::Rar5Solid,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar5_solid,
    CompressedFormat::Rar5Solid,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar5_solid_encrypted,
    CompressedFormat::Rar5SolidEncrypted,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar5_solid_encrypted,
    CompressedFormat::Rar5SolidEncrypted,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar5_solid_encrypted,
    CompressedFormat::Rar5SolidEncrypted,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar5_solid_headers,
    CompressedFormat::Rar5SolidHeaders,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar5_solid_headers,
    CompressedFormat::Rar5SolidHeaders,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar5_solid_headers,
    CompressedFormat::Rar5SolidHeaders,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_solid,
    CompressedFormat::Rar4Solid,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_solid,
    CompressedFormat::Rar4Solid,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_solid,
    CompressedFormat::Rar4Solid,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_solid_encrypted,
    CompressedFormat::Rar4SolidEncrypted,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_solid_encrypted,
    CompressedFormat::Rar4SolidEncrypted,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_solid_encrypted,
    CompressedFormat::Rar4SolidEncrypted,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_solid_headers,
    CompressedFormat::Rar4SolidHeaders,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_solid_headers,
    CompressedFormat::Rar4SolidHeaders,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_solid_headers,
    CompressedFormat::Rar4SolidHeaders,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_ppmd,
    CompressedFormat::Rar4Ppmd,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_ppmd,
    CompressedFormat::Rar4Ppmd,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_ppmd,
    CompressedFormat::Rar4Ppmd,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_ppmd_encrypted,
    CompressedFormat::Rar4PpmdEncrypted,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_ppmd_encrypted,
    CompressedFormat::Rar4PpmdEncrypted,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_ppmd_encrypted,
    CompressedFormat::Rar4PpmdEncrypted,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_ppmd_headers,
    CompressedFormat::Rar4PpmdHeaders,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_ppmd_headers,
    CompressedFormat::Rar4PpmdHeaders,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_ppmd_headers,
    CompressedFormat::Rar4PpmdHeaders,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_lz_two_volume,
    CompressedFormat::Rar4LzTwoVolumes,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_direct_rar5_lz_two_volume,
    CompressedFormat::Rar5LzTwoVolumes,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_lz_four_volume,
    CompressedFormat::Rar4LzFourVolumes,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_direct_rar5_lz_four_volume,
    CompressedFormat::Rar5LzFourVolumes,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_lz_two_volume,
    CompressedFormat::Rar4LzTwoVolumes,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_chase_rar5_lz_two_volume,
    CompressedFormat::Rar5LzTwoVolumes,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_lz_four_volume,
    CompressedFormat::Rar4LzFourVolumes,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_chase_rar5_lz_four_volume,
    CompressedFormat::Rar5LzFourVolumes,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_lz_two_volume,
    CompressedFormat::Rar4LzTwoVolumes,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar5_lz_two_volume,
    CompressedFormat::Rar5LzTwoVolumes,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_lz_four_volume,
    CompressedFormat::Rar4LzFourVolumes,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar5_lz_four_volume,
    CompressedFormat::Rar5LzFourVolumes,
    compressed_conventional_campaign
);
