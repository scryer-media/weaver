//! Format coverage beyond the archive matrix proper: obfuscated RAR4 sets and
//! the solid and encrypted multi-volume compressed layouts.
use super::*;

#[tokio::test]
async fn rar4_obfuscated_arrival_schedules() {
    campaign(Format::Rar4Obfuscated, Selection::Smoke).await;
}
/// Both obfuscated volumes start conventionally before the recovery index
/// arrives, a restart keeps no progress for either, and the set the index then
/// admits routes the refetched articles past the dead process's bytes. Those
/// bytes sit under the volumes' obfuscated names, which no role classifies as
/// archive input, and must still be cleaned up rather than published beside
/// the member. Each case loses the first volume's second article, so a repair
/// runs before the set finalizes.
#[tokio::test]
async fn rar4_obfuscated_restart_publishes_no_pre_restart_volume() {
    let cases = combined_schedule_cases()
        .into_iter()
        .filter(|(case, _)| [136, 2559, 2942].contains(case))
        .collect::<Vec<_>>();
    assert_eq!(cases.len(), 3);
    slot_campaign(
        Format::Rar4Obfuscated,
        Selection::Smoke,
        ExtractionProfile::DirectStore,
        4,
        Vec::new(),
        cases,
        |_, _, _, _| false,
    )
    .await;
}
#[tokio::test]
async fn compressed_rar4_solid_two_volume_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar4SolidTwoVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar5_solid_two_volume_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar5SolidTwoVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar4_solid_four_volume_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar4SolidFourVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar5_solid_four_volume_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar5SolidFourVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar4_encrypted_two_volume_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar4EncryptedTwoVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar5_encrypted_two_volume_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar5EncryptedTwoVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar4_encrypted_four_volume_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar4EncryptedFourVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar5_encrypted_four_volume_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar5EncryptedFourVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar4_headers_two_volume_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar4HeadersTwoVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar5_headers_two_volume_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar5HeadersTwoVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar4_headers_four_volume_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar4HeadersFourVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar5_headers_four_volume_arrival_schedules() {
    compressed_direct_campaign(CompressedFormat::Rar5HeadersFourVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_rar4_solid_encrypted_two_volume_arrival_schedules() {
    compressed_direct_campaign(
        CompressedFormat::Rar4SolidEncryptedTwoVolumes,
        Selection::Smoke,
    )
    .await;
}
#[tokio::test]
async fn compressed_rar5_solid_encrypted_two_volume_arrival_schedules() {
    compressed_direct_campaign(
        CompressedFormat::Rar5SolidEncryptedTwoVolumes,
        Selection::Smoke,
    )
    .await;
}
#[tokio::test]
async fn compressed_rar4_solid_encrypted_four_volume_arrival_schedules() {
    compressed_direct_campaign(
        CompressedFormat::Rar4SolidEncryptedFourVolumes,
        Selection::Smoke,
    )
    .await;
}
#[tokio::test]
async fn compressed_rar5_solid_encrypted_four_volume_arrival_schedules() {
    compressed_direct_campaign(
        CompressedFormat::Rar5SolidEncryptedFourVolumes,
        Selection::Smoke,
    )
    .await;
}

combined_campaign!(combined_rar4_obfuscated, Format::Rar4Obfuscated, campaign);
combined_campaign!(
    combined_chase_rar4_obfuscated,
    Format::Rar4Obfuscated,
    chase_campaign
);
combined_campaign!(
    combined_conventional_rar4_obfuscated,
    Format::Rar4Obfuscated,
    conventional_campaign
);

// Every multi-volume compressed layout under each extraction profile. A
// layout marked `fine` runs long enough to need its shards cut finer.
macro_rules! compressed_campaigns {
    ($($direct:ident, $chase:ident, $conventional:ident => $variant:expr $(, $fine:ident)?;)+) => {
        $(
            combined_campaign!($direct, $variant, compressed_direct_campaign $(, $fine)?);
            combined_campaign!($chase, $variant, compressed_chase_campaign $(, $fine)?);
            combined_campaign!(
                $conventional,
                $variant,
                compressed_conventional_campaign
                $(, $fine)?
            );
        )+
    };
}
compressed_campaigns! {
    combined_compressed_direct_rar4_solid_two_volume,
    combined_compressed_chase_rar4_solid_two_volume,
    combined_compressed_conventional_rar4_solid_two_volume
        => CompressedFormat::Rar4SolidTwoVolumes;
    combined_compressed_direct_rar5_solid_two_volume,
    combined_compressed_chase_rar5_solid_two_volume,
    combined_compressed_conventional_rar5_solid_two_volume
        => CompressedFormat::Rar5SolidTwoVolumes;
    combined_compressed_direct_rar4_solid_four_volume,
    combined_compressed_chase_rar4_solid_four_volume,
    combined_compressed_conventional_rar4_solid_four_volume
        => CompressedFormat::Rar4SolidFourVolumes;
    combined_compressed_direct_rar5_solid_four_volume,
    combined_compressed_chase_rar5_solid_four_volume,
    combined_compressed_conventional_rar5_solid_four_volume
        => CompressedFormat::Rar5SolidFourVolumes;
    combined_compressed_direct_rar4_encrypted_two_volume,
    combined_compressed_chase_rar4_encrypted_two_volume,
    combined_compressed_conventional_rar4_encrypted_two_volume
        => CompressedFormat::Rar4EncryptedTwoVolumes;
    combined_compressed_direct_rar5_encrypted_two_volume,
    combined_compressed_chase_rar5_encrypted_two_volume,
    combined_compressed_conventional_rar5_encrypted_two_volume
        => CompressedFormat::Rar5EncryptedTwoVolumes;
    combined_compressed_direct_rar4_encrypted_four_volume,
    combined_compressed_chase_rar4_encrypted_four_volume,
    combined_compressed_conventional_rar4_encrypted_four_volume
        => CompressedFormat::Rar4EncryptedFourVolumes;
    combined_compressed_direct_rar5_encrypted_four_volume,
    combined_compressed_chase_rar5_encrypted_four_volume,
    combined_compressed_conventional_rar5_encrypted_four_volume
        => CompressedFormat::Rar5EncryptedFourVolumes;
    combined_compressed_direct_rar4_headers_two_volume,
    combined_compressed_chase_rar4_headers_two_volume,
    combined_compressed_conventional_rar4_headers_two_volume
        => CompressedFormat::Rar4HeadersTwoVolumes;
    combined_compressed_direct_rar5_headers_two_volume,
    combined_compressed_chase_rar5_headers_two_volume,
    combined_compressed_conventional_rar5_headers_two_volume
        => CompressedFormat::Rar5HeadersTwoVolumes;
    combined_compressed_direct_rar4_headers_four_volume,
    combined_compressed_chase_rar4_headers_four_volume,
    combined_compressed_conventional_rar4_headers_four_volume
        => CompressedFormat::Rar4HeadersFourVolumes, fine;
    combined_compressed_direct_rar5_headers_four_volume,
    combined_compressed_chase_rar5_headers_four_volume,
    combined_compressed_conventional_rar5_headers_four_volume
        => CompressedFormat::Rar5HeadersFourVolumes, fine;
    combined_compressed_direct_rar4_solid_encrypted_two_volume,
    combined_compressed_chase_rar4_solid_encrypted_two_volume,
    combined_compressed_conventional_rar4_solid_encrypted_two_volume
        => CompressedFormat::Rar4SolidEncryptedTwoVolumes;
    combined_compressed_direct_rar5_solid_encrypted_two_volume,
    combined_compressed_chase_rar5_solid_encrypted_two_volume,
    combined_compressed_conventional_rar5_solid_encrypted_two_volume
        => CompressedFormat::Rar5SolidEncryptedTwoVolumes;
    combined_compressed_direct_rar4_solid_encrypted_four_volume,
    combined_compressed_chase_rar4_solid_encrypted_four_volume,
    combined_compressed_conventional_rar4_solid_encrypted_four_volume
        => CompressedFormat::Rar4SolidEncryptedFourVolumes;
    combined_compressed_direct_rar5_solid_encrypted_four_volume,
    combined_compressed_chase_rar5_solid_encrypted_four_volume,
    combined_compressed_conventional_rar5_solid_encrypted_four_volume
        => CompressedFormat::Rar5SolidEncryptedFourVolumes;
}
