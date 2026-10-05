//! A one-volume stored 7z that carries its own PAR3 recovery set after the
//! end header, under every arrival order, duplicate, loss, and boundary
//! action of the combined matrix's shape.
//!
//! The volume is four articles. The container fills the first two and part
//! of the third, which also holds the end header and the recovery set's
//! leading packets; the fourth is recovery packets only. The set carries
//! recovery for one article's worth of the container and not two, so which
//! losses the job survives follows from those four facts alone:
//!
//! - slot 1 (member bytes only) is repaired from the tail in place;
//! - slot 3 (recovery packets only) costs nothing the container needed;
//! - slot 0 (the start header) leaves no map, so the set demotes and the
//!   conventional path repairs the file from its own tail;
//! - slot 2, or any two container slots, or a container slot with the
//!   recovery-only slot, is beyond what the set can mend.
use super::super::super::super::archive_schedules::{
    BoundaryAction, DemotionChoice, Interruption, RecoveryFormat, SHARDS, ScheduleOptions,
    arrival_orders, run_schedule_with,
};
use super::super::super::embedded_par3::with_embedded_par3;
use super::*;

const SLOTS: usize = 4;

struct Fixture {
    member: Vec<u8>,
    volumes: Vec<(String, Vec<u8>)>,
    spec: JobSpec,
}

fn fixture() -> Fixture {
    let member = unrepeated_payload(71, 20_000);
    let archive = build_7z(
        &[Entry::file("feature.mkv", member.clone())],
        EncoderMethod::COPY,
        None,
    );
    let inserted = with_embedded_par3(&archive, 1024, 10);
    let chunk = inserted.len().div_ceil(SLOTS);
    // The slot facts the module's verdicts rest on.
    assert!(
        2 * chunk < archive.len() && archive.len() < 3 * chunk,
        "the container must end inside slot 2: container {} chunk {chunk}",
        archive.len()
    );
    let volumes = split_volumes(&inserted, 1);
    let spec = sevenz_job_spec(&volumes, SLOTS);
    Fixture {
        member,
        volumes,
        spec,
    }
}

/// Loss masks the set survives; see the module documentation.
fn survivable(mask: u8) -> bool {
    matches!(mask, 0 | 0b0001 | 0b0010 | 0b1000)
}

/// Every order the combined matrix uses: each arrival order, and each with
/// an earlier article repeated at every nonterminal point.
fn orders() -> BTreeSet<Vec<(u32, u32)>> {
    let mut orders = BTreeSet::new();
    for order in arrival_orders() {
        orders.insert(order.clone());
        for at in 1..order.len() {
            for &article in &order[..at] {
                let mut duplicate = order.clone();
                duplicate.insert(at, article);
                orders.insert(duplicate);
            }
        }
    }
    orders
}

type Case = (Vec<(u32, u32)>, Interruption);

/// Every case, by replay index. A loss that drops an arrival changes only
/// what is received, so identical received sequences are one case.
fn cases() -> Vec<(usize, Case)> {
    let mut cases = BTreeSet::new();
    for order in orders() {
        for mask in 0u8..16 {
            let available = |&&(file, article): &&(u32, u32)| mask & (1 << (file * 2 + article)) == 0;
            let received: Vec<_> = order.iter().filter(available).copied().collect();
            if mask == 0 {
                cases.insert((received.clone(), Interruption::None));
            } else {
                cases.insert((
                    received.clone(),
                    Interruption::Combined {
                        mask,
                        index_first: false,
                        action: BoundaryAction::None,
                        at: 0,
                    },
                ));
            }
            if !survivable(mask) {
                continue;
            }
            // A whole container may finalize on its last arrival, so, as in
            // the combined matrix's clean cases, its boundaries stop before it.
            let whole = mask & 0b0111 == 0;
            for at in 0..received.len() + usize::from(!whole) {
                for action in [
                    BoundaryAction::Restart,
                    BoundaryAction::Demote,
                    BoundaryAction::Crash,
                ] {
                    cases.insert((
                        received.clone(),
                        Interruption::Combined {
                            mask,
                            index_first: false,
                            action,
                            at,
                        },
                    ));
                }
            }
        }
    }
    let selected = std::env::var("WEAVER_EMBEDDED_PAR3_CASE").ok().map(|case| {
        let case = case.parse::<usize>().expect("decimal embedded PAR3 case");
        case..case + 1
    });
    cases
        .into_iter()
        .enumerate()
        .filter(|(case, _)| selected.as_ref().is_none_or(|range| range.contains(case)))
        .collect()
}

/// The default suite's sample: in order, every survivable loss under every
/// boundary action at the middle boundary, and one loss the set cannot mend.
fn smoke() -> Vec<(usize, Case)> {
    let order: Vec<(u32, u32)> = vec![(0, 0), (0, 1), (1, 0), (1, 1)];
    let mut cases = vec![(order.clone(), Interruption::None)];
    for mask in [0b0001u8, 0b0010, 0b1000] {
        let received: Vec<_> = order
            .iter()
            .filter(|&&(file, article)| mask & (1 << (file * 2 + article)) == 0)
            .copied()
            .collect();
        for action in [
            BoundaryAction::None,
            BoundaryAction::Restart,
            BoundaryAction::Demote,
            BoundaryAction::Crash,
        ] {
            cases.push((
                received.clone(),
                Interruption::Combined {
                    mask,
                    index_first: false,
                    action,
                    at: 2,
                },
            ));
        }
    }
    cases.push((
        vec![(0, 0), (1, 1)],
        Interruption::Combined {
            mask: 0b0110,
            index_first: false,
            action: BoundaryAction::None,
            at: 0,
        },
    ));
    cases.into_iter().enumerate().collect()
}

fn mask_of(interruption: Interruption) -> u8 {
    match interruption {
        Interruption::Combined { mask, .. } => mask,
        _ => 0,
    }
}

pub(super) async fn embedded_campaign(profile: ExtractionProfile, selection: Selection) {
    let fixture = fixture();
    let wanted = ["feature.mkv"];
    let route = Route {
        unmapped_loss: |mask| mask & 0b0001 != 0,
        ..Route::DIRECT
    };
    let options = ScheduleOptions {
        recovery: RecoveryFormat::Embedded,
        demotion: DemotionChoice::Scheduled,
    };
    let selected = match selection {
        Selection::Smoke => smoke(),
        Selection::Shard(shard) => cases()
            .into_iter()
            .filter(|(case, _)| case % SHARDS == shard)
            .collect(),
        // The archive needs no password; one it is given must change nothing.
        Selection::WrongPassword => {
            let mut spec = fixture.spec.clone();
            spec.password = Some("incorrect-key".to_string());
            for order in arrival_orders() {
                let outcome = run_schedule_with(
                    options,
                    profile,
                    spec.clone(),
                    &fixture.volumes,
                    None,
                    &order,
                    &wanted,
                    Interruption::None,
                )
                .await;
                assert_eq!(outcome.status, Some(JobStatus::Complete), "{:?}", outcome.trace);
                profile.assert_delivery(&outcome, route, &wanted, Interruption::None);
            }
            return;
        }
        Selection::FineShard(_) | Selection::WrongPasswordPart(_) => {
            unreachable!("the embedded campaign is cut into the usual shards")
        }
    };
    for (case, (order, interruption)) in selected {
        if !profile.includes(interruption) {
            continue;
        }
        eprintln!("embedded PAR3 profile={profile:?} case={case} order={order:?} interruption={interruption:?}");
        let outcome = run_schedule_with(
            options,
            profile,
            fixture.spec.clone(),
            &fixture.volumes,
            None,
            &order,
            &wanted,
            interruption,
        )
        .await;
        if !survivable(mask_of(interruption)) {
            profile.assert_rejected(&outcome, &wanted);
            continue;
        }
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "case={case} order={order:?} interruption={interruption:?}: {:?}",
            outcome.trace
        );
        profile.assert_delivery(&outcome, route, &wanted, interruption);
        assert_eq!(
            outcome.files["feature.mkv"].as_deref(),
            Some(fixture.member.as_slice()),
            "case={case} order={order:?} interruption={interruption:?}: {:?}",
            outcome.trace
        );
    }
}

#[tokio::test]
async fn embedded_par3_direct_schedules() {
    embedded_campaign(ExtractionProfile::DirectStore, Selection::Smoke).await;
}

#[tokio::test]
async fn embedded_par3_conventional_schedules() {
    embedded_campaign(ExtractionProfile::Conventional, Selection::Smoke).await;
}

#[tokio::test]
async fn embedded_par3_chase_schedules() {
    embedded_campaign(ExtractionProfile::Chase, Selection::Smoke).await;
}

combined_campaign!(
    combined_embedded_par3,
    ExtractionProfile::DirectStore,
    embedded_campaign
);
combined_campaign!(
    combined_chase_embedded_par3,
    ExtractionProfile::Chase,
    embedded_campaign
);
combined_campaign!(
    combined_conventional_embedded_par3,
    ExtractionProfile::Conventional,
    embedded_campaign
);
