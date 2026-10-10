use super::{completed_folder_name, derive_release_name, strip_nzb_source_suffix};
use crate::ingest::append_original_title_metadata;

// The display name and completed folder a submission under `filename` gets.
fn display_and_folder(filename: Option<&str>, meta_title: Option<&str>) -> (String, String) {
    let title = filename.map(|value| strip_nzb_source_suffix(value).unwrap_or(value));
    let metadata = append_original_title_metadata(Vec::new(), title, meta_title);
    let display = derive_release_name(title, meta_title);
    let folder = completed_folder_name(&display, &metadata);
    (display, folder)
}

#[test]
fn completed_folder_keeps_a_season_packs_full_title() {
    assert_eq!(
        display_and_folder(Some("Copper.Meadow.S06.1080p.WEB.h264-GRP.nzb"), None),
        (
            "Copper Meadow".to_string(),
            "Copper.Meadow.S06.1080p.WEB.h264-GRP".to_string()
        )
    );
}

#[test]
fn completed_folder_keeps_every_episode_of_a_multi_episode_release() {
    assert_eq!(
        display_and_folder(
            Some("Copper.Meadow.S11E42-E43.720p.HDTV.x264-GRP.nzb.xz"),
            None
        ),
        (
            "Copper Meadow — S11E42".to_string(),
            "Copper.Meadow.S11E42-E43.720p.HDTV.x264-GRP".to_string()
        )
    );
}

#[test]
fn completed_folder_keeps_a_movies_year() {
    assert_eq!(
        display_and_folder(Some("Lantern.Field.2019.2160p.BluRay.x265-GRP.nzb"), None),
        (
            "Lantern Field".to_string(),
            "Lantern.Field.2019.2160p.BluRay.x265-GRP".to_string()
        )
    );
}

#[test]
fn completed_folder_from_the_meta_title_drops_only_what_a_filesystem_cannot_hold() {
    let (_, folder) =
        display_and_folder(None, Some(" ..Lantern/Field\\2019:Cut\u{7}.1080p-GRP. . "));
    assert_eq!(folder, "Lantern_Field_2019_Cut.1080p-GRP");
}

#[test]
fn completed_folder_without_an_original_title_keeps_the_display_name() {
    assert_eq!(completed_folder_name("Copper Meadow", &[]), "Copper Meadow");
    assert_eq!(completed_folder_name("CON", &[]), "_CON");
    // A title with nothing a folder can be named after counts as none.
    let metadata = vec![(
        crate::ingest::ORIGINAL_TITLE_METADATA_KEY.to_string(),
        " . . ".to_string(),
    )];
    assert_eq!(
        completed_folder_name("Copper Meadow", &metadata),
        "Copper Meadow"
    );
}

#[test]
fn prefers_parsed_release_title() {
    // Season-only pack: parser doesn't produce episode metadata for bare S01
    assert_eq!(
        derive_release_name(
            Some("Silver Horizon.Beyond.Journeys.End.S01.1080p.BluRay.Opus2.0.x265.DUAL-Anitsu"),
            None,
        ),
        "Silver Horizon Beyond Journeys End"
    );
}

#[test]
fn display_title_includes_season_episode() {
    assert_eq!(
        derive_release_name(
            Some("Stoneguard.S04E29.The.Final.Chapters.1080p.WEB-DL.H.265"),
            None,
        ),
        "Stoneguard — S04E29"
    );
}

#[test]
fn display_title_movie_no_episode_suffix() {
    assert_eq!(
        derive_release_name(Some("Glass Harbor.2024.2160p.BluRay.Remux.H.265"), None,),
        "Glass Harbor"
    );
}

#[test]
fn low_confidence_parse_falls_back_to_basic_cleanup() {
    let raw = "ubuntu-24.04.2-live-server-amd64";
    assert_eq!(
        derive_release_name(Some(raw), None),
        "ubuntu-24 04 2-live-server-amd64"
    );
}

#[test]
fn falls_back_to_basic_cleanup() {
    assert_eq!(
        derive_release_name(Some("some._unknown.release_name.nzb"), None),
        "some unknown release name"
    );
}

#[test]
fn strips_compressed_nzb_suffix_case_insensitively() {
    assert_eq!(
        strip_nzb_source_suffix("Some.Release.NZB.XZ"),
        Some("Some.Release")
    );
    assert_eq!(
        derive_release_name(Some("Some.Release.NZB.XZ"), None),
        "Some Release"
    );
}

#[test]
fn strips_compressed_nzb_suffix_without_splitting_unicode() {
    assert_eq!(strip_nzb_source_suffix("Молоко.nzb"), Some("Молоко"));
    assert_eq!(strip_nzb_source_suffix("日本語.NZB.XZ"), Some("日本語"));
    assert_eq!(strip_nzb_source_suffix("abc日本語"), None);
    assert_eq!(derive_release_name(Some("日本語"), None), "日本語");
}

#[test]
fn uses_secondary_when_primary_missing() {
    assert_eq!(
        derive_release_name(None, Some("Glass Harbor.2021.1080p.BluRay.x264")),
        "Glass Harbor"
    );
}
