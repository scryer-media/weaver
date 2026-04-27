pub fn derive_release_name(primary: Option<&str>, secondary: Option<&str>) -> String {
    for candidate in [primary, secondary].into_iter().flatten() {
        let trimmed = candidate.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(extracted) = extract_title_from_candidate(trimmed) {
            let name = finalize_release_name(extracted.name);
            return append_episode_suffix(name, extracted.episode);
        }

        let fallback = basic_release_name(trimmed);
        if !fallback.is_empty() {
            return finalize_release_name(fallback);
        }
    }

    "Untitled".to_string()
}

#[derive(Debug, Clone, Copy)]
struct EpisodeSuffix {
    season: Option<u32>,
    episode: Option<u32>,
    absolute: Option<u32>,
}

struct TitleExtraction {
    name: String,
    episode: Option<EpisodeSuffix>,
}

fn extract_title_from_candidate(raw: &str) -> Option<TitleExtraction> {
    let raw = strip_nzb_suffix(raw.trim());
    let tokens = split_release_tokens(raw);
    if tokens.is_empty() {
        return None;
    }

    let boundary = tokens
        .iter()
        .enumerate()
        .find_map(|(index, token)| metadata_boundary(token).map(|episode| (index, episode)))?;

    if boundary.0 == 0 {
        return None;
    }

    let name = basic_release_name(&tokens[..boundary.0].join(" "));
    if name.is_empty() {
        return None;
    }

    Some(TitleExtraction {
        name,
        episode: boundary.1,
    })
}

fn append_episode_suffix(name: String, episode: Option<EpisodeSuffix>) -> String {
    let Some(episode) = episode else {
        return name;
    };

    if let (Some(season), Some(episode)) = (episode.season, episode.episode) {
        return format!("{name} — S{season:02}E{episode:02}");
    }

    if let Some(abs) = episode.absolute {
        return format!("{name} — {abs:03}");
    }

    name
}

fn metadata_boundary(token: &str) -> Option<Option<EpisodeSuffix>> {
    let upper = token.to_ascii_uppercase();
    if let Some(episode) = parse_standard_episode_token(&upper) {
        return Some(Some(episode));
    }

    if is_generic_season_token(&upper)
        || is_plausible_year(&upper)
        || is_quality_token(&upper)
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
                | "WEBRIP"
                | "WEBDL"
                | "WEB-DL"
                | "BLURAY"
                | "BDRIP"
                | "BDREMUX"
                | "BDMUX"
                | "BDMV"
                | "COMPLETE"
                | "X264"
                | "X265"
                | "H264"
                | "H265"
                | "HEVC"
                | "AVC"
                | "AAC"
                | "AC3"
                | "EAC3"
                | "DTS"
                | "FLAC"
        )
    {
        return Some(None);
    }

    None
}

fn parse_standard_episode_token(token: &str) -> Option<EpisodeSuffix> {
    let rest = token.strip_prefix('S')?;
    let split = rest.find('E')?;
    let season = rest[..split].parse::<u32>().ok()?;
    let episode = rest[split + 1..].parse::<u32>().ok()?;
    Some(EpisodeSuffix {
        season: Some(season),
        episode: Some(episode),
        absolute: None,
    })
}

pub(crate) fn split_release_tokens(raw: &str) -> Vec<String> {
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

pub(crate) fn basic_release_name(raw: &str) -> String {
    strip_nzb_suffix(raw)
        .trim_matches(|ch: char| matches!(ch, '[' | ']' | '(' | ')' | '{' | '}'))
        .replace(['.', '_'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn strip_nzb_suffix(raw: &str) -> &str {
    raw.trim()
        .trim_end_matches(".nzb")
        .trim_end_matches(".NZB")
        .trim()
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

fn is_quality_token(token: &str) -> bool {
    let Some(resolution) = token.strip_suffix('P') else {
        return false;
    };
    matches!(resolution, "480" | "576" | "720" | "1080" | "2160" | "4320")
}

fn is_plausible_year(token: &str) -> bool {
    let Ok(year) = token.parse::<u32>() else {
        return false;
    };
    (1900..=2100).contains(&year)
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
