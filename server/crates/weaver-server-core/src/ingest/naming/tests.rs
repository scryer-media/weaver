use scryer_release_parser::parse_release_metadata;

use super::{MIN_PARSE_CONFIDENCE_FOR_DISPLAY_NAME, derive_release_name};

#[test]
fn prefers_parsed_release_title() {
    // Season-only pack: parser doesn't produce episode metadata for bare S01
    assert_eq!(
        derive_release_name(
            Some("Frieren.Beyond.Journeys.End.S01.1080p.BluRay.Opus2.0.x265.DUAL-Anitsu"),
            None,
        ),
        "Frieren Beyond Journeys End"
    );
}

#[test]
fn display_title_includes_season_episode() {
    assert_eq!(
        derive_release_name(
            Some("Attack.on.Titan.S04E29.The.Final.Chapters.1080p.WEB-DL.H.265"),
            None,
        ),
        "Attack on Titan — S04E29"
    );
}

#[test]
fn display_title_movie_no_episode_suffix() {
    assert_eq!(
        derive_release_name(Some("Dune.2024.2160p.BluRay.Remux.H.265"), None,),
        "Dune"
    );
}

#[test]
fn low_confidence_parse_falls_back_to_basic_cleanup() {
    let raw = "ubuntu-24.04.2-live-server-amd64";
    let parsed = parse_release_metadata(raw);

    assert!(parsed.parse_confidence < MIN_PARSE_CONFIDENCE_FOR_DISPLAY_NAME);
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
fn uses_secondary_when_primary_missing() {
    assert_eq!(
        derive_release_name(None, Some("Dune.2021.1080p.BluRay.x264")),
        "Dune"
    );
}
