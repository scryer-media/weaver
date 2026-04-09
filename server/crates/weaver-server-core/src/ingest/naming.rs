use scryer_release_parser::{ParsedEpisodeMetadata, ParsedReleaseMetadata, parse_release_metadata};
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
mod tests;
