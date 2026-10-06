//! 7z format coverage beyond the archive matrix proper: obfuscated sets and
//! a one-volume set that carries its own recovery.
//!
//! Smoke selections here run in the default suite. The campaigns are opt-in
//! and separate from the archive matrix proper; run them with:
//! `cargo nextest run --profile archive-matrix-extended --run-ignored all --no-fail-fast`
use super::*;

mod embedded_par3;
mod recovery;

#[tokio::test]
async fn copy_obfuscated_arrival_schedules() {
    campaign(Shape::CopyObfuscated, Selection::Smoke).await;
}

combined_campaign!(combined_copy_obfuscated, Shape::CopyObfuscated, campaign);
combined_campaign!(
    combined_chase_copy_obfuscated,
    Shape::CopyObfuscated,
    chase_campaign
);
combined_campaign!(
    combined_conventional_copy_obfuscated,
    Shape::CopyObfuscated,
    conventional_campaign
);

#[tokio::test]
async fn copy_single_obfuscated_arrival_schedules() {
    campaign(Shape::CopySingleObfuscated, Selection::Smoke).await;
}

combined_campaign!(
    combined_copy_single_obfuscated,
    Shape::CopySingleObfuscated,
    campaign
);
combined_campaign!(
    combined_chase_copy_single_obfuscated,
    Shape::CopySingleObfuscated,
    chase_campaign
);
combined_campaign!(
    combined_conventional_copy_single_obfuscated,
    Shape::CopySingleObfuscated,
    conventional_campaign
);
