//! Repair and demotion axes beyond the archive matrix proper: PAR3 as the
//! recovery format, and every demotion reason a schedule can force.
use super::*;
use crate::pipeline::direct_store::router::crypt::{CryptRefusal, HeaderCryptRefusal};

/// Combined cases that carry a loss, and so a recovery set.
const LOSS_CASES: usize = 6777;
/// Combined cases with a demote action at some boundary.
const DEMOTE_CASES: usize = 2850;

/// What a campaign schedules: one of the matrix's archive formats, or two
/// independent single-volume sets in one job.
#[derive(Clone, Copy, Debug)]
enum Target {
    Format(Format),
    TwoSets,
}

/// A job and everything a schedule over it must deliver.
struct Fixture {
    spec: JobSpec,
    volumes: Vec<(String, Vec<u8>)>,
    described: Option<Vec<String>>,
    members: Vec<(&'static str, Vec<u8>)>,
    route: Route,
}

impl Fixture {
    fn wanted(&self) -> Vec<&'static str> {
        self.members.iter().map(|(name, _)| *name).collect()
    }
}

/// The fixtures the archive matrix posts for the same targets.
fn fixture(target: Target) -> Fixture {
    let format = match target {
        Target::Format(format) => format,
        Target::TwoSets => {
            let members: Vec<_> = [("alpha.mkv", 6001, 7), ("nested/beta.mkv", 4093, 11)]
                .into_iter()
                .map(|(name, len, step)| {
                    let payload = (0..len)
                        .map(|n| ((n * step + n / 251) % 253) as u8)
                        .collect::<Vec<_>>();
                    (name, payload)
                })
                .collect();
            let volumes: Vec<_> = ["alpha", "beta"]
                .into_iter()
                .zip(&members)
                .map(|(stem, (member, payload))| {
                    let (_, bytes) = single_member_store_set(member, payload, 1).remove(0);
                    (format!("{stem}.part01.rar"), bytes)
                })
                .collect();
            return Fixture {
                spec: direct_store_job_spec("Two set schedules", &volumes),
                volumes,
                described: None,
                members,
                route: Route {
                    sets: 2,
                    ..Route::DIRECT
                },
            };
        }
    };
    let name = "nested/feature.mkv";
    let password = "moonlit-harbour";
    let length = match format {
        // A described volume is bound by the fingerprint of its first 16 KiB,
        // which its offset-zero article has to cover whole.
        Format::Rar5Obfuscated => 70_001,
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
        Format::Rar4Encrypted => {
            encrypted_rar4_store_set(name, &payload, count, password, Some(TEST_RAR4_SALT))
        }
        Format::Rar5Encrypted => {
            encrypted_store_set(name, &payload, 2, password, Some(password), false)
        }
        other => unreachable!("{other:?} has no repair campaign"),
    };
    let described = matches!(format, Format::Rar5Obfuscated).then(|| {
        single_member_store_set(name, &payload, 2)
            .into_iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>()
    });
    let mut spec = direct_store_job_spec_with_articles("Archive schedules", &volumes, 4 / count);
    spec.password = matches!(format, Format::Rar4Encrypted | Format::Rar5Encrypted)
        .then(|| password.to_string());
    Fixture {
        spec,
        volumes,
        described,
        members: vec![(name, payload)],
        route: format.route(),
    }
}

/// Which slice of a campaign one test runs.
#[derive(Clone, Copy, Debug)]
enum Slice {
    /// The default suite's sample, drawn from the smoke schedules.
    Smoke,
    /// One of `of` shards of the combined cases the campaign keeps.
    Shard { shard: usize, of: usize },
}

/// The cases a campaign keeps, by the replay index of the matrix they are
/// drawn from: `WEAVER_ARCHIVE_SCHEDULE_CASE` replays a smoke case and
/// `WEAVER_ARCHIVE_COMBINED_CASE` a combined one.
fn campaign_cases(
    slice: Slice,
    keep: fn(Interruption) -> bool,
    pinned: usize,
) -> Vec<(usize, Schedule)> {
    match slice {
        Slice::Smoke => schedules()
            .into_iter()
            .enumerate()
            .filter(|(_, (_, interruption))| keep(*interruption))
            .collect(),
        Slice::Shard { shard, of } => {
            let cases: Vec<_> = combined_schedule_cases()
                .into_iter()
                .filter(|(_, (_, interruption))| keep(*interruption))
                .collect();
            if std::env::var_os("WEAVER_ARCHIVE_COMBINED_CASE").is_none() {
                assert_eq!(cases.len(), pinned);
            }
            cases
                .into_iter()
                .filter(|(case, _)| case % of == shard)
                .collect()
        }
    }
}

fn carries_loss(interruption: Interruption) -> bool {
    interruption.loss().is_some()
}

fn demotes(interruption: Interruption) -> bool {
    matches!(
        interruption,
        Interruption::Demote(_)
            | Interruption::Combined {
                action: BoundaryAction::Demote,
                ..
            }
    )
}

/// Runs one schedule and holds it to what its profile and target allow.
async fn run_case(
    fixture: &Fixture,
    options: ScheduleOptions,
    profile: ExtractionProfile,
    case: usize,
    order: &[(u32, u32)],
    interruption: Interruption,
) {
    let wanted = fixture.wanted();
    let label = format!(
        "options={options:?} profile={profile:?} case={case} order={order:?} interruption={interruption:?}"
    );
    eprintln!("{label}");
    let outcome = run_schedule_with(
        options,
        profile,
        fixture.spec.clone(),
        &fixture.volumes,
        fixture.described.as_deref(),
        order,
        &wanted,
        interruption,
    )
    .await;
    if interruption.fails() {
        profile.assert_rejected(&outcome, &wanted);
        return;
    }
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{label} trace={:?}",
        outcome.trace
    );
    profile.assert_delivery(&outcome, fixture.route, &wanted, interruption);
    for (member, payload) in &fixture.members {
        assert_eq!(
            outcome.files[*member].as_deref(),
            Some(payload.as_slice()),
            "{label} member={member}: {:?}",
            outcome.trace
        );
    }
}

/// The matrix's loss schedules, answered by a PAR3 set instead of PAR2.
async fn par3_campaign(format: Format, profile: ExtractionProfile, slice: Slice) {
    let mut fixture = fixture(Target::Format(format));
    if matches!(format, Format::Rar5Obfuscated) {
        // A PAR3 set names a posted file by its whole image, and that image
        // has to be a file before it can take the name. So a set whose names
        // say nothing writes itself out from the bytes it routed, refetching
        // none of them, whenever a loss sends it to its recovery set.
        fixture.route.unnamed_loss = |_| true;
    }
    let options = ScheduleOptions {
        recovery: RecoveryFormat::Par3,
        ..ScheduleOptions::MATRIX
    };
    for (case, (order, interruption)) in campaign_cases(slice, carries_loss, LOSS_CASES) {
        if !profile.includes(interruption) {
            continue;
        }
        run_case(&fixture, options, profile, case, &order, interruption).await;
    }
}

/// One demotion a campaign forces at every demote boundary.
#[derive(Clone, Copy, Debug)]
enum Forced {
    /// A direct-store set demotes under this reason.
    Direct(DemotionReason),
    /// Speculative extraction is withdrawn under this reason and latch.
    Chase(ChaseDemotion, AbortLatch),
}

impl Forced {
    fn profile(self) -> ExtractionProfile {
        match self {
            Self::Direct(_) => ExtractionProfile::DirectStore,
            Self::Chase(..) => ExtractionProfile::Chase,
        }
    }

    /// Chase withdrawal is per job, so it has one target whatever the job
    /// holds; a direct demotion claims one set.
    fn targets(self, route: Route) -> usize {
        match self {
            Self::Direct(_) => route.sets,
            Self::Chase(..) => 1,
        }
    }

    fn on(self, set: usize) -> ScheduleOptions {
        let demotion = match self {
            Self::Direct(reason) => DemotionChoice::Fixed {
                set,
                reason,
                chase: ChaseDemotion::MemoryYielded,
                latch: AbortLatch::Permanent,
            },
            // Direct store is off under chase, so the direct half is never read.
            Self::Chase(chase, latch) => DemotionChoice::Fixed {
                set,
                reason: DemotionReason::HoldsBudgetExceeded,
                chase,
                latch,
            },
        };
        ScheduleOptions {
            demotion,
            ..ScheduleOptions::MATRIX
        }
    }
}

/// Every demote case of the matrix under `forced`, against every set it can
/// claim.
async fn demotion_campaign(target: Target, forced: Forced, slice: Slice) {
    let fixture = fixture(target);
    for (case, (order, interruption)) in campaign_cases(slice, demotes, DEMOTE_CASES) {
        for set in 0..forced.targets(fixture.route) {
            run_case(
                &fixture,
                forced.on(set),
                forced.profile(),
                case,
                &order,
                interruption,
            )
            .await;
        }
    }
}

/// The default suite's sample: the smoke demote cases, each under the next
/// demotion of `every` and the next set, so every one is forced somewhere.
async fn demotion_smoke(target: Target, every: &[Forced]) {
    let fixture = fixture(target);
    let cases = campaign_cases(Slice::Smoke, demotes, 0);
    for (turn, (case, (order, interruption))) in cases.into_iter().enumerate() {
        let forced = every[turn % every.len()];
        let set = (turn / every.len()) % forced.targets(fixture.route);
        run_case(
            &fixture,
            forced.on(set),
            forced.profile(),
            case,
            &order,
            interruption,
        )
        .await;
    }
}

/// Lists every direct-store demotion a schedule forces, by test name, for
/// `$callback`. A reason that carries a refusal is forced under one of them:
/// the refusal names the metric and nothing else reads it.
macro_rules! direct_demotions {
    ($callback:ident!($($args:tt)*)) => {
        $callback! {$($args)*;
            member_ineligible Forced::Direct(DemotionReason::MemberIneligible(MemberIneligibility::Solid)),
            encrypted_member_refused Forced::Direct(DemotionReason::EncryptedMemberRefused(CryptRefusal::WrongPassword)),
            header_encrypted_refused Forced::Direct(DemotionReason::HeaderEncryptedRefused(HeaderCryptRefusal::NoVerifiedCandidate)),
            encrypted_facts_disagree Forced::Direct(DemotionReason::EncryptedFactsDisagree),
            encrypted_posted_bytes_unavailable Forced::Direct(DemotionReason::EncryptedPostedBytesUnavailable),
            par2_damaged Forced::Direct(DemotionReason::Par2Damaged),
            par2_unbindable Forced::Direct(DemotionReason::Par2Unbindable),
            tolerated_extraction_failed Forced::Direct(DemotionReason::ToleratedExtractionFailed),
            holds_budget_exceeded Forced::Direct(DemotionReason::HoldsBudgetExceeded),
            par3_memory_pressure Forced::Direct(DemotionReason::Par3MemoryPressure),
            holds_scratch_failed Forced::Direct(DemotionReason::HoldsScratchFailed),
            holds_scratch_ceiling Forced::Direct(DemotionReason::HoldsScratchCeiling),
            holds_scratch_disk_reserve Forced::Direct(DemotionReason::HoldsScratchDiskReserve),
            conflicting_volume_facts Forced::Direct(DemotionReason::ConflictingVolumeFacts),
            quick_open_mismatch Forced::Direct(DemotionReason::QuickOpenMismatch),
            unconfirmed_restored_volume Forced::Direct(DemotionReason::UnconfirmedRestoredVolume),
            restart_rearm_unplaceable Forced::Direct(DemotionReason::RestartRearmUnplaceable),
            restart_reread_failed Forced::Direct(DemotionReason::RestartRereadFailed),
            repair_reroute_failed Forced::Direct(DemotionReason::RepairRerouteFailed),
            repair_gap_unreadable Forced::Direct(DemotionReason::RepairGapUnreadable),
            uuencoded_source_volume Forced::Direct(DemotionReason::UuencodedSourceVolume),
            unparsable_volume Forced::Direct(DemotionReason::UnparsableVolume),
            part_checksum_mismatch Forced::Direct(DemotionReason::PartChecksumMismatch),
            member_checksum_mismatch Forced::Direct(DemotionReason::MemberChecksumMismatch),
            format_mismatch Forced::Direct(DemotionReason::FormatMismatch),
            unsupported_format Forced::Direct(DemotionReason::UnsupportedFormat),
            unsafe_destination Forced::Direct(DemotionReason::UnsafeDestination),
            colliding_destinations Forced::Direct(DemotionReason::CollidingDestinations),
            volume_crc_mismatch Forced::Direct(DemotionReason::VolumeCrcMismatch),
            destination_write_failed Forced::Direct(DemotionReason::DestinationWriteFailed),
            sparse_mark_failed Forced::Direct(DemotionReason::SparseMarkFailed),
            finalization_failed Forced::Direct(DemotionReason::FinalizationFailed),
            identity_roster_unfillable Forced::Direct(DemotionReason::IdentityRosterUnfillable),
            identity_volume_mismatch Forced::Direct(DemotionReason::IdentityVolumeMismatch),
            seven_zip Forced::Direct(DemotionReason::SevenZip(SevenZipRefusal::Coder)),
        }
    };
}

/// Lists every chase withdrawal a schedule forces: each reason for good, and
/// the two the pipeline also withdraws under a latch that may re-arm.
macro_rules! chase_demotions {
    ($callback:ident!($($args:tt)*)) => {
        $callback! {$($args)*;
            download_ended Forced::Chase(ChaseDemotion::DownloadEnded, AbortLatch::Permanent),
            part_unreadable Forced::Chase(ChaseDemotion::PartUnreadable, AbortLatch::Permanent),
            decode_failed Forced::Chase(ChaseDemotion::DecodeFailed, AbortLatch::Permanent),
            memory_yielded Forced::Chase(ChaseDemotion::MemoryYielded, AbortLatch::Permanent),
            repair_rewrote Forced::Chase(ChaseDemotion::RepairRewrote, AbortLatch::Permanent),
            repair_failed Forced::Chase(ChaseDemotion::RepairFailed, AbortLatch::Permanent),
            gated_stall Forced::Chase(ChaseDemotion::GatedStall, AbortLatch::Permanent),
            staging_unavailable Forced::Chase(ChaseDemotion::StagingUnavailable, AbortLatch::Permanent),
            download_ended_retryable Forced::Chase(ChaseDemotion::DownloadEnded, AbortLatch::Retryable),
            part_unreadable_retryable Forced::Chase(ChaseDemotion::PartUnreadable, AbortLatch::Retryable),
        }
    };
}

macro_rules! forced_list {
    (; $($name:ident $forced:expr),+ $(,)?) => {
        [$($forced),+]
    };
}

const DIRECT_DEMOTIONS: [Forced; 35] = direct_demotions!(forced_list!());
const CHASE_DEMOTIONS: [Forced; 10] = chase_demotions!(forced_list!());

/// Each direct-store reason's variant, in declaration order. Exhaustive, so
/// a reason added to the product does not compile here until it is forced.
fn direct_variant(reason: DemotionReason) -> usize {
    use DemotionReason as R;
    match reason {
        R::MemberIneligible(_) => 0,
        R::EncryptedMemberRefused(_) => 1,
        R::HeaderEncryptedRefused(_) => 2,
        R::EncryptedFactsDisagree => 3,
        R::EncryptedPostedBytesUnavailable => 4,
        R::Par2Damaged => 5,
        R::Par2Unbindable => 6,
        R::ToleratedExtractionFailed => 7,
        R::HoldsBudgetExceeded => 8,
        R::Par3MemoryPressure => 9,
        R::HoldsScratchFailed => 10,
        R::HoldsScratchCeiling => 11,
        R::HoldsScratchDiskReserve => 12,
        R::ConflictingVolumeFacts => 13,
        R::QuickOpenMismatch => 14,
        R::UnconfirmedRestoredVolume => 15,
        R::RestartRearmUnplaceable => 16,
        R::RestartRereadFailed => 17,
        R::RepairRerouteFailed => 18,
        R::RepairGapUnreadable => 19,
        R::UuencodedSourceVolume => 20,
        R::UnparsableVolume => 21,
        R::PartChecksumMismatch => 22,
        R::MemberChecksumMismatch => 23,
        R::FormatMismatch => 24,
        R::UnsupportedFormat => 25,
        R::UnsafeDestination => 26,
        R::CollidingDestinations => 27,
        R::VolumeCrcMismatch => 28,
        R::DestinationWriteFailed => 29,
        R::SparseMarkFailed => 30,
        R::FinalizationFailed => 31,
        R::IdentityRosterUnfillable => 32,
        R::IdentityVolumeMismatch => 33,
        R::SevenZip(_) => 34,
    }
}

/// [`direct_variant`] for chase withdrawals.
fn chase_variant(reason: ChaseDemotion) -> usize {
    match reason {
        ChaseDemotion::DownloadEnded => 0,
        ChaseDemotion::PartUnreadable => 1,
        ChaseDemotion::DecodeFailed => 2,
        ChaseDemotion::MemoryYielded => 3,
        ChaseDemotion::RepairRewrote => 4,
        ChaseDemotion::RepairFailed => 5,
        ChaseDemotion::GatedStall => 6,
        ChaseDemotion::StagingUnavailable => 7,
    }
}

#[test]
fn every_demotion_reason_is_forced() {
    let direct: BTreeSet<_> = DIRECT_DEMOTIONS
        .iter()
        .map(|forced| match forced {
            Forced::Direct(reason) => direct_variant(*reason),
            Forced::Chase(..) => unreachable!(),
        })
        .collect();
    assert_eq!(direct, (0..35).collect());
    let chase: BTreeSet<_> = CHASE_DEMOTIONS
        .iter()
        .map(|forced| match forced {
            Forced::Chase(reason, _) => chase_variant(*reason),
            Forced::Direct(_) => unreachable!(),
        })
        .collect();
    assert_eq!(chase, (0..8).collect());
}

#[test]
fn repair_campaign_case_counts() {
    if std::env::var_os("WEAVER_ARCHIVE_COMBINED_CASE").is_some() {
        return;
    }
    let cases = combined_schedule_cases();
    assert_eq!(cases.len(), 9393);
    let count = |keep: fn(Interruption) -> bool| {
        cases
            .iter()
            .filter(|(_, (_, interruption))| keep(*interruption))
            .count()
    };
    assert_eq!(count(carries_loss), LOSS_CASES);
    assert_eq!(count(demotes), DEMOTE_CASES);
}

macro_rules! par3_smoke {
    ($($name:ident $format:expr, $profile:expr;)+) => {
        $(
            #[tokio::test]
            async fn $name() {
                par3_campaign($format, $profile, Slice::Smoke).await;
            }
        )+
    };
}

par3_smoke! {
    par3_rar4_loss_schedules Format::Rar4, ExtractionProfile::DirectStore;
    par3_rar5_loss_schedules Format::Rar5, ExtractionProfile::DirectStore;
    par3_rar4_encrypted_loss_schedules Format::Rar4Encrypted, ExtractionProfile::DirectStore;
    par3_rar5_encrypted_loss_schedules Format::Rar5Encrypted, ExtractionProfile::DirectStore;
    par3_rar4_four_volume_loss_schedules Format::Rar4FourVolumes, ExtractionProfile::DirectStore;
    par3_rar5_four_volume_loss_schedules Format::Rar5FourVolumes, ExtractionProfile::DirectStore;
    par3_rar5_obfuscated_loss_schedules Format::Rar5Obfuscated, ExtractionProfile::DirectStore;
    par3_chase_rar4_loss_schedules Format::Rar4, ExtractionProfile::Chase;
    par3_chase_rar5_loss_schedules Format::Rar5, ExtractionProfile::Chase;
    par3_chase_rar4_encrypted_loss_schedules Format::Rar4Encrypted, ExtractionProfile::Chase;
    par3_chase_rar5_encrypted_loss_schedules Format::Rar5Encrypted, ExtractionProfile::Chase;
    par3_chase_rar4_four_volume_loss_schedules Format::Rar4FourVolumes, ExtractionProfile::Chase;
    par3_chase_rar5_four_volume_loss_schedules Format::Rar5FourVolumes, ExtractionProfile::Chase;
    par3_chase_rar5_obfuscated_loss_schedules Format::Rar5Obfuscated, ExtractionProfile::Chase;
    par3_conventional_rar4_loss_schedules Format::Rar4, ExtractionProfile::Conventional;
    par3_conventional_rar5_loss_schedules Format::Rar5, ExtractionProfile::Conventional;
    par3_conventional_rar4_encrypted_loss_schedules Format::Rar4Encrypted, ExtractionProfile::Conventional;
    par3_conventional_rar5_encrypted_loss_schedules Format::Rar5Encrypted, ExtractionProfile::Conventional;
    par3_conventional_rar4_four_volume_loss_schedules Format::Rar4FourVolumes, ExtractionProfile::Conventional;
    par3_conventional_rar5_four_volume_loss_schedules Format::Rar5FourVolumes, ExtractionProfile::Conventional;
    par3_conventional_rar5_obfuscated_loss_schedules Format::Rar5Obfuscated, ExtractionProfile::Conventional;
}

/// A posted file the PAR3 set must name from its bytes, whose first article
/// arrives twice after that article was already held without a placement:
/// once through a completed-file restore, once through a demotion handback.
/// The duplicate's placement is the only one recorded, and publishing just
/// that range hid the rest of the file, so the set rebuilt the volume beside
/// the posted copy and left the copy's own archive set waiting on a volume.
#[tokio::test]
async fn par3_obfuscated_duplicate_after_unplaced_hold() {
    let mut fixture = fixture(Target::Format(Format::Rar5Obfuscated));
    fixture.route.unnamed_loss = |_| true;
    let options = ScheduleOptions {
        recovery: RecoveryFormat::Par3,
        ..ScheduleOptions::MATRIX
    };
    let cases = [
        (
            ExtractionProfile::Conventional,
            vec![(0, 0), (0, 1), (0, 0)],
            Interruption::Combined {
                mask: 12,
                index_first: true,
                action: BoundaryAction::Restart,
                at: 2,
            },
        ),
        (
            ExtractionProfile::DirectStore,
            vec![(0, 0), (0, 0), (1, 1), (0, 1)],
            Interruption::Combined {
                mask: 4,
                index_first: false,
                action: BoundaryAction::Demote,
                at: 2,
            },
        ),
    ];
    for (case, (profile, order, interruption)) in cases.into_iter().enumerate() {
        run_case(&fixture, options, profile, case, &order, interruption).await;
    }
}

macro_rules! demotion_smoke {
    ($($name:ident $target:expr, $every:expr;)+) => {
        $(
            #[tokio::test]
            async fn $name() {
                demotion_smoke($target, &$every).await;
            }
        )+
    };
}

demotion_smoke! {
    demotion_reasons_rar4 Target::Format(Format::Rar4), DIRECT_DEMOTIONS;
    demotion_reasons_rar5 Target::Format(Format::Rar5), DIRECT_DEMOTIONS;
    demotion_reasons_rar4_encrypted Target::Format(Format::Rar4Encrypted), DIRECT_DEMOTIONS;
    demotion_reasons_rar5_encrypted Target::Format(Format::Rar5Encrypted), DIRECT_DEMOTIONS;
    demotion_reasons_rar4_four_volume Target::Format(Format::Rar4FourVolumes), DIRECT_DEMOTIONS;
    demotion_reasons_rar5_four_volume Target::Format(Format::Rar5FourVolumes), DIRECT_DEMOTIONS;
    demotion_reasons_two_sets Target::TwoSets, DIRECT_DEMOTIONS;
    chase_demotion_reasons_rar4 Target::Format(Format::Rar4), CHASE_DEMOTIONS;
    chase_demotion_reasons_rar5 Target::Format(Format::Rar5), CHASE_DEMOTIONS;
    chase_demotion_reasons_rar4_encrypted Target::Format(Format::Rar4Encrypted), CHASE_DEMOTIONS;
    chase_demotion_reasons_rar5_encrypted Target::Format(Format::Rar5Encrypted), CHASE_DEMOTIONS;
    chase_demotion_reasons_rar4_four_volume Target::Format(Format::Rar4FourVolumes), CHASE_DEMOTIONS;
    chase_demotion_reasons_rar5_four_volume Target::Format(Format::Rar5FourVolumes), CHASE_DEMOTIONS;
}

// Shards cut each campaign to well under a thousand cases, so no test
// approaches the runner's per-test limit.
macro_rules! par3_campaign {
    ($module:ident, $format:expr, $profile:expr) => {
        mod $module {
            use super::*;
            par3_campaign!(@each $format, $profile, 8;
                shard_0 0, shard_1 1, shard_2 2, shard_3 3,
                shard_4 4, shard_5 5, shard_6 6, shard_7 7);
        }
    };
    (@each $format:expr, $profile:expr, $of:literal; $($name:ident $shard:literal),+) => {
        $(
            #[tokio::test]
            #[ignore = "opt-in extended archive matrix; run with the archive-matrix-extended Nextest profile and --run-ignored all"]
            async fn $name() {
                par3_campaign($format, $profile, Slice::Shard { shard: $shard, of: $of }).await;
            }
        )+
    };
}

par3_campaign!(
    combined_par3_rar4,
    Format::Rar4,
    ExtractionProfile::DirectStore
);
par3_campaign!(
    combined_par3_rar5,
    Format::Rar5,
    ExtractionProfile::DirectStore
);
par3_campaign!(
    combined_par3_rar4_encrypted,
    Format::Rar4Encrypted,
    ExtractionProfile::DirectStore
);
par3_campaign!(
    combined_par3_rar5_encrypted,
    Format::Rar5Encrypted,
    ExtractionProfile::DirectStore
);
par3_campaign!(
    combined_par3_rar4_four_volume,
    Format::Rar4FourVolumes,
    ExtractionProfile::DirectStore
);
par3_campaign!(
    combined_par3_rar5_four_volume,
    Format::Rar5FourVolumes,
    ExtractionProfile::DirectStore
);
par3_campaign!(
    combined_par3_rar5_obfuscated,
    Format::Rar5Obfuscated,
    ExtractionProfile::DirectStore
);
par3_campaign!(
    combined_par3_chase_rar4,
    Format::Rar4,
    ExtractionProfile::Chase
);
par3_campaign!(
    combined_par3_chase_rar5,
    Format::Rar5,
    ExtractionProfile::Chase
);
par3_campaign!(
    combined_par3_chase_rar4_encrypted,
    Format::Rar4Encrypted,
    ExtractionProfile::Chase
);
par3_campaign!(
    combined_par3_chase_rar5_encrypted,
    Format::Rar5Encrypted,
    ExtractionProfile::Chase
);
par3_campaign!(
    combined_par3_chase_rar4_four_volume,
    Format::Rar4FourVolumes,
    ExtractionProfile::Chase
);
par3_campaign!(
    combined_par3_chase_rar5_four_volume,
    Format::Rar5FourVolumes,
    ExtractionProfile::Chase
);
par3_campaign!(
    combined_par3_chase_rar5_obfuscated,
    Format::Rar5Obfuscated,
    ExtractionProfile::Chase
);
par3_campaign!(
    combined_par3_conventional_rar4,
    Format::Rar4,
    ExtractionProfile::Conventional
);
par3_campaign!(
    combined_par3_conventional_rar5,
    Format::Rar5,
    ExtractionProfile::Conventional
);
par3_campaign!(
    combined_par3_conventional_rar4_encrypted,
    Format::Rar4Encrypted,
    ExtractionProfile::Conventional
);
par3_campaign!(
    combined_par3_conventional_rar5_encrypted,
    Format::Rar5Encrypted,
    ExtractionProfile::Conventional
);
par3_campaign!(
    combined_par3_conventional_rar4_four_volume,
    Format::Rar4FourVolumes,
    ExtractionProfile::Conventional
);
par3_campaign!(
    combined_par3_conventional_rar5_four_volume,
    Format::Rar5FourVolumes,
    ExtractionProfile::Conventional
);
par3_campaign!(
    combined_par3_conventional_rar5_obfuscated,
    Format::Rar5Obfuscated,
    ExtractionProfile::Conventional
);

// One module per campaign, one per forced demotion inside it, so a failing
// shard names the reason it forced. Six shards hold a single-set campaign
// to under five hundred runs, and the two-set one, which runs every case
// once per set, to under a thousand cheaper ones.
macro_rules! forced_campaign {
    (@shards $target:expr, $forced:expr, 6) => {
        forced_campaign!(@each $target, $forced, 6;
            shard_0 0, shard_1 1, shard_2 2, shard_3 3, shard_4 4, shard_5 5);
    };
    (@each $target:expr, $forced:expr, $of:literal; $($name:ident $shard:literal),+) => {
        $(
            #[tokio::test]
            #[ignore = "opt-in extended archive matrix; run with the archive-matrix-extended Nextest profile and --run-ignored all"]
            async fn $name() {
                demotion_campaign($target, $forced, Slice::Shard { shard: $shard, of: $of }).await;
            }
        )+
    };
    ($module:ident, $target:expr, $shards:tt; $($reason:ident $forced:expr),+ $(,)?) => {
        mod $module {
            use super::*;
            $(
                mod $reason {
                    use super::*;
                    forced_campaign!(@shards $target, $forced, $shards);
                }
            )+
        }
    };
}

direct_demotions!(forced_campaign!(
    combined_demote_rar4,
    Target::Format(Format::Rar4),
    6
));
direct_demotions!(forced_campaign!(
    combined_demote_rar5,
    Target::Format(Format::Rar5),
    6
));
direct_demotions!(forced_campaign!(
    combined_demote_rar4_encrypted,
    Target::Format(Format::Rar4Encrypted),
    6
));
direct_demotions!(forced_campaign!(
    combined_demote_rar5_encrypted,
    Target::Format(Format::Rar5Encrypted),
    6
));
direct_demotions!(forced_campaign!(
    combined_demote_rar4_four_volume,
    Target::Format(Format::Rar4FourVolumes),
    6
));
direct_demotions!(forced_campaign!(
    combined_demote_rar5_four_volume,
    Target::Format(Format::Rar5FourVolumes),
    6
));
direct_demotions!(forced_campaign!(
    combined_demote_two_sets,
    Target::TwoSets,
    6
));

chase_demotions!(forced_campaign!(
    combined_chase_demote_rar4,
    Target::Format(Format::Rar4),
    6
));
chase_demotions!(forced_campaign!(
    combined_chase_demote_rar5,
    Target::Format(Format::Rar5),
    6
));
chase_demotions!(forced_campaign!(
    combined_chase_demote_rar4_encrypted,
    Target::Format(Format::Rar4Encrypted),
    6
));
chase_demotions!(forced_campaign!(
    combined_chase_demote_rar5_encrypted,
    Target::Format(Format::Rar5Encrypted),
    6
));
chase_demotions!(forced_campaign!(
    combined_chase_demote_rar4_four_volume,
    Target::Format(Format::Rar4FourVolumes),
    6
));
chase_demotions!(forced_campaign!(
    combined_chase_demote_rar5_four_volume,
    Target::Format(Format::Rar5FourVolumes),
    6
));
