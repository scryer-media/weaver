//! The 7z loss schedules answered by a PAR3 set instead of PAR2, over every
//! shape, and a one-volume set under both.
//!
//! The recovery set is posted beside the volumes. A loss inside a direct set
//! is rebuilt into it through readback; a loss that takes the start or end
//! header leaves the set nothing to route by, so it demotes and the ordinary
//! path repairs the volumes on disk. Shapes direct store refuses repair the
//! same way they always extract: from the volumes.
use super::super::super::super::archive_schedules::{
    RecoveryFormat, combined_schedule_cases, schedules,
};
use super::*;

/// Combined cases that carry a loss, and so a recovery set.
const LOSS_CASES: usize = 6777;

/// Shards each PAR3 campaign is cut into, so none runs more than a few
/// hundred cases.
const PAR3_SHARDS: usize = 16;

#[derive(Clone, Copy, Debug)]
enum Slice {
    Smoke,
    Shard(usize),
}

/// The loss cases of the matrix, by the replay index of the matrix they are
/// drawn from: `WEAVER_ARCHIVE_SCHEDULE_CASE` replays a smoke case and
/// `WEAVER_ARCHIVE_COMBINED_CASE` a combined one.
fn loss_cases(slice: Slice) -> Vec<(usize, Schedule)> {
    let carries_loss = |interruption: &Interruption| {
        matches!(
            interruption,
            Interruption::Loss { .. }
                | Interruption::Combined { .. }
                | Interruption::Starved { .. }
        )
    };
    match slice {
        Slice::Smoke => schedules()
            .into_iter()
            .enumerate()
            .filter(|(_, (_, interruption))| carries_loss(interruption))
            .collect(),
        Slice::Shard(shard) => {
            let cases: Vec<_> = combined_schedule_cases()
                .into_iter()
                .filter(|(_, (_, interruption))| carries_loss(interruption))
                .collect();
            if std::env::var_os("WEAVER_ARCHIVE_COMBINED_CASE").is_none() {
                assert_eq!(cases.len(), LOSS_CASES);
            }
            cases
                .into_iter()
                .filter(|(case, _)| case % PAR3_SHARDS == shard)
                .collect()
        }
    }
}

async fn par3_campaign(shape: Shape, profile: ExtractionProfile, slice: Slice) {
    let options = ScheduleOptions {
        recovery: RecoveryFormat::Par3,
        ..ScheduleOptions::MATRIX
    };
    run_shape(shape, profile, options, Vec::new(), loss_cases(slice)).await;
}

macro_rules! par3_smoke {
    ($($name:ident $shape:expr, $profile:expr;)+) => {
        $(
            #[tokio::test]
            async fn $name() {
                par3_campaign($shape, $profile, Slice::Smoke).await;
            }
        )+
    };
}

par3_smoke! {
    par3_copy_loss_schedules Shape::Copy, ExtractionProfile::DirectStore;
    par3_copy_four_volume_loss_schedules Shape::CopyFourVolumes, ExtractionProfile::DirectStore;
    par3_copy_single_loss_schedules Shape::CopySingle, ExtractionProfile::DirectStore;
    par3_lzma2_loss_schedules Shape::Lzma2Fallback, ExtractionProfile::DirectStore;
    par3_encrypted_copy_loss_schedules Shape::EncryptedCopy, ExtractionProfile::DirectStore;
    par3_encrypted_header_loss_schedules Shape::EncryptedHeaders, ExtractionProfile::DirectStore;
    par3_copy_obfuscated_loss_schedules Shape::CopyObfuscated, ExtractionProfile::DirectStore;
    par3_copy_single_obfuscated_loss_schedules Shape::CopySingleObfuscated, ExtractionProfile::DirectStore;
    par3_chase_copy_loss_schedules Shape::Copy, ExtractionProfile::Chase;
    par3_conventional_copy_loss_schedules Shape::Copy, ExtractionProfile::Conventional;
}

#[tokio::test]
async fn copy_single_arrival_schedules() {
    campaign(Shape::CopySingle, Selection::Smoke).await;
}

macro_rules! par3_campaign {
    ($module:ident, $shape:expr, $profile:expr) => {
        mod $module {
            use super::*;
            par3_campaign!(@each $shape, $profile;
                shard_00 0, shard_01 1, shard_02 2, shard_03 3,
                shard_04 4, shard_05 5, shard_06 6, shard_07 7,
                shard_08 8, shard_09 9, shard_10 10, shard_11 11,
                shard_12 12, shard_13 13, shard_14 14, shard_15 15);
            const _: () = assert!(PAR3_SHARDS == 16);
        }
    };
    (@each $shape:expr, $profile:expr; $($name:ident $shard:literal),+) => {
        $(
            #[tokio::test]
            #[ignore = "opt-in extended archive matrix; run with the archive-matrix-extended Nextest profile and --run-ignored all"]
            async fn $name() {
                par3_campaign($shape, $profile, Slice::Shard($shard)).await;
            }
        )+
    };
}

macro_rules! par3_campaigns {
    ($($direct:ident $chase:ident $conventional:ident $shape:expr;)+) => {
        $(
            par3_campaign!($direct, $shape, ExtractionProfile::DirectStore);
            par3_campaign!($chase, $shape, ExtractionProfile::Chase);
            par3_campaign!($conventional, $shape, ExtractionProfile::Conventional);
        )+
    };
}

par3_campaigns! {
    combined_par3_copy combined_par3_chase_copy combined_par3_conventional_copy
        Shape::Copy;
    combined_par3_copy_four_volume combined_par3_chase_copy_four_volume
        combined_par3_conventional_copy_four_volume Shape::CopyFourVolumes;
    combined_par3_copy_single combined_par3_chase_copy_single
        combined_par3_conventional_copy_single Shape::CopySingle;
    combined_par3_lzma2 combined_par3_chase_lzma2 combined_par3_conventional_lzma2
        Shape::Lzma2Fallback;
    combined_par3_encrypted_copy combined_par3_chase_encrypted_copy
        combined_par3_conventional_encrypted_copy Shape::EncryptedCopy;
    combined_par3_encrypted_header combined_par3_chase_encrypted_header
        combined_par3_conventional_encrypted_header Shape::EncryptedHeaders;
    combined_par3_copy_obfuscated combined_par3_chase_copy_obfuscated
        combined_par3_conventional_copy_obfuscated Shape::CopyObfuscated;
    combined_par3_copy_single_obfuscated combined_par3_chase_copy_single_obfuscated
        combined_par3_conventional_copy_single_obfuscated Shape::CopySingleObfuscated;
}

// The one-volume set under the matrix's own PAR2 campaign.
combined_campaign!(combined_copy_single, Shape::CopySingle, campaign);
combined_campaign!(
    combined_chase_copy_single,
    Shape::CopySingle,
    chase_campaign
);
combined_campaign!(
    combined_conventional_copy_single,
    Shape::CopySingle,
    conventional_campaign
);
