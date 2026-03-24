use scryer_release_parser::{ParsedEpisodeMetadata, ParsedReleaseMetadata, parse_release_metadata};

pub const ORIGINAL_TITLE_METADATA_KEY: &str = "weaver.original_title";
const MIN_PARSE_CONFIDENCE_FOR_DISPLAY_NAME: f32 = 0.4;

pub fn derive_release_name(primary: Option<&str>, secondary: Option<&str>) -> String {
    for candidate in [primary, secondary].into_iter().flatten() {
        let trimmed = candidate.trim();
        if trimmed.is_empty() {
            continue;
        }

        let parsed = parse_release_metadata(trimmed);
        if parsed.parse_confidence >= MIN_PARSE_CONFIDENCE_FOR_DISPLAY_NAME {
            let extracted = extract_title_from_candidate(trimmed, &parsed);
            if !extracted.is_empty() {
                let name = finalize_release_name(extracted);
                return append_episode_suffix(name, parsed.episode.as_ref());
            }

            let normalized = basic_release_name(parsed.normalized_title.trim());
            if !normalized.is_empty() {
                let name = finalize_release_name(normalized);
                return append_episode_suffix(name, parsed.episode.as_ref());
            }
        }

        let fallback = basic_release_name(trimmed);
        if !fallback.is_empty() {
            return finalize_release_name(fallback);
        }
    }

    "Untitled".to_string()
}

/// Append a human-readable episode suffix (e.g. " — S04E29") when episode
/// metadata is available from the release parser.
fn append_episode_suffix(name: String, episode: Option<&ParsedEpisodeMetadata>) -> String {
    let Some(ep) = episode else {
        return name;
    };

    if let Some(season) = ep.season {
        if !ep.episode_numbers.is_empty() {
            let episodes: String = ep
                .episode_numbers
                .iter()
                .map(|e| format!("E{e:02}"))
                .collect::<Vec<_>>()
                .join("-");
            return format!("{name} — S{season:02}{episodes}");
        }
        return format!("{name} — S{season:02}");
    }

    if let Some(abs) = ep.absolute_episode {
        return format!("{name} — {abs:03}");
    }

    name
}

pub fn append_original_title_metadata(
    mut metadata: Vec<(String, String)>,
    primary: Option<&str>,
    secondary: Option<&str>,
) -> Vec<(String, String)> {
    if metadata
        .iter()
        .any(|(key, _)| key == ORIGINAL_TITLE_METADATA_KEY)
    {
        return metadata;
    }

    if let Some(original_title) = [primary, secondary]
        .into_iter()
        .flatten()
        .map(clean_original_title)
        .find(|value| !value.is_empty())
    {
        metadata.push((ORIGINAL_TITLE_METADATA_KEY.to_string(), original_title));
    }

    metadata
}

pub fn original_release_title(job_name: &str, metadata: &[(String, String)]) -> String {
    metadata
        .iter()
        .find(|(key, _)| key == ORIGINAL_TITLE_METADATA_KEY)
        .map(|(_, value)| value.clone())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| job_name.to_string())
}

pub fn parse_job_release(job_name: &str, metadata: &[(String, String)]) -> ParsedReleaseMetadata {
    parse_release_metadata(&original_release_title(job_name, metadata))
}

fn extract_title_from_candidate(raw: &str, parsed: &ParsedReleaseMetadata) -> String {
    let raw = raw.trim().trim_end_matches(".nzb").trim_end_matches(".NZB");
    let tokens = split_release_tokens(raw);
    if tokens.is_empty() {
        return String::new();
    }

    let stop_index = tokens
        .iter()
        .position(|token| is_metadata_boundary(token, parsed))
        .unwrap_or(tokens.len());

    basic_release_name(&tokens[..stop_index].join(" "))
}

fn split_release_tokens(raw: &str) -> Vec<String> {
    raw.split(|ch: char| {
        matches!(
            ch,
            '.' | '_' | '-' | '[' | ']' | '(' | ')' | '{' | '}' | ' '
        )
    })
    .filter(|token| !token.is_empty())
    .map(ToString::to_string)
    .collect()
}

fn is_metadata_boundary(token: &str, parsed: &ParsedReleaseMetadata) -> bool {
    let upper = token.to_ascii_uppercase();

    matches_release_value(&upper, parsed.quality.as_deref())
        || matches_release_value(&upper, parsed.source.as_deref())
        || matches_release_value(&upper, parsed.video_codec.as_deref())
        || matches_release_value(&upper, parsed.video_encoding.as_deref())
        || matches_release_value(&upper, parsed.audio.as_deref())
        || matches_release_value(&upper, parsed.audio_channels.as_deref())
        || matches_release_value(&upper, parsed.streaming_service.as_deref())
        || matches_release_value(&upper, parsed.edition.as_deref())
        || matches_release_value(&upper, parsed.release_group.as_deref())
        || parsed.year.is_some_and(|year| upper == year.to_string())
        || parsed
            .episode
            .as_ref()
            .is_some_and(|episode| matches_episode_token(&upper, episode))
        || matches!(
            upper.as_str(),
            "PROPER"
                | "REPACK"
                | "REMUX"
                | "HDR"
                | "HDR10"
                | "HDR10PLUS"
                | "HLG"
                | "DV"
                | "DOVI"
                | "DUAL"
                | "DUALAUDIO"
                | "MULTI"
                | "TRUEHD"
                | "ATMOS"
                | "WEB"
                | "BLURAY"
                | "BDRIP"
                | "BDMUX"
                | "BDMV"
                | "COMPLETE"
        )
        || is_generic_season_token(&upper)
}

fn matches_release_value(token: &str, value: Option<&str>) -> bool {
    let Some(value) = value else {
        return false;
    };

    let normalized = value.to_ascii_uppercase().replace(['.', '-', ' '], "");
    let token_normalized = token.replace(['.', '-', ' '], "");
    token_normalized == normalized
}

fn matches_episode_token(token: &str, episode: &ParsedEpisodeMetadata) -> bool {
    if let Some(raw) = episode.raw.as_deref() {
        let normalized = raw.to_ascii_uppercase().replace(['.', '-', ' '], "");
        let token_normalized = token.replace(['.', '-', ' '], "");
        if token_normalized == normalized {
            return true;
        }
    }

    if let Some(season) = episode.season
        && (token == format!("S{season:02}") || token == format!("SEASON{season}"))
    {
        return true;
    }

    false
}

fn basic_release_name(raw: &str) -> String {
    raw.trim()
        .trim_end_matches(".nzb")
        .trim_end_matches(".NZB")
        .trim_matches(|ch: char| matches!(ch, '[' | ']' | '(' | ')' | '{' | '}'))
        .replace(['.', '_'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn clean_original_title(raw: &str) -> String {
    raw.trim()
        .trim_end_matches(".nzb")
        .trim_end_matches(".NZB")
        .trim()
        .to_string()
}

fn is_generic_season_token(token: &str) -> bool {
    if let Some(season) = token.strip_prefix('S') {
        return !season.is_empty()
            && season.len() <= 2
            && season.chars().all(|ch| ch.is_ascii_digit());
    }

    if let Some(season) = token.strip_prefix("SEASON") {
        return !season.is_empty() && season.chars().all(|ch| ch.is_ascii_digit());
    }

    false
}

fn finalize_release_name(name: String) -> String {
    if name.chars().any(|ch| ch.is_ascii_lowercase()) {
        return name;
    }

    name.split_whitespace()
        .map(title_case_word)
        .collect::<Vec<_>>()
        .join(" ")
}

fn title_case_word(word: &str) -> String {
    let mut chars = word.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };

    let mut out = String::new();
    out.extend(first.to_uppercase());
    for ch in chars {
        out.extend(ch.to_lowercase());
    }
    out
}

#[cfg(test)]
mod tests {
    use scryer_release_parser::parse_release_metadata;

    use super::{
        MIN_PARSE_CONFIDENCE_FOR_DISPLAY_NAME, ORIGINAL_TITLE_METADATA_KEY,
        append_original_title_metadata, derive_release_name, original_release_title,
    };

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
            derive_release_name(
                Some("Dune.2024.2160p.BluRay.Remux.H.265"),
                None,
            ),
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

    #[test]
    fn appends_original_title_metadata_once() {
        let metadata = append_original_title_metadata(
            vec![("priority".to_string(), "HIGH".to_string())],
            Some("Dune.2021.1080p.BluRay.x264.nzb"),
            None,
        );

        assert_eq!(metadata.len(), 2);
        assert_eq!(
            metadata[1],
            (
                ORIGINAL_TITLE_METADATA_KEY.to_string(),
                "Dune.2021.1080p.BluRay.x264".to_string()
            )
        );
    }

    #[test]
    fn original_title_prefers_metadata() {
        let title = original_release_title(
            "Dune",
            &[(
                ORIGINAL_TITLE_METADATA_KEY.to_string(),
                "Dune.2021.1080p.BluRay.x264".to_string(),
            )],
        );

        assert_eq!(title, "Dune.2021.1080p.BluRay.x264");
    }
}
