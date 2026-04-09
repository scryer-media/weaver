use super::parse_job_release;

#[test]
fn parse_job_release_prefers_original_title_metadata() {
    let parsed = parse_job_release(
        "Display Name",
        &[(
            "weaver.original_title".to_string(),
            "Attack.on.Titan.S04E29.1080p.WEB-DL.H.265".to_string(),
        )],
    );

    assert_ne!(parsed.normalized_title, "Display Name");
    assert!(parsed.normalized_title.contains("ATTACK ON TITAN"));
    assert_eq!(parsed.episode.and_then(|episode| episode.season), Some(4));
}
