// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeSet, HashMap};
use std::sync::{Mutex, OnceLock};

use chrono::NaiveDate;
use regex::Regex;
use scryer_release_parser::{
    ContextAlias, ContextEpisode, ContextFacetHint, ContextTitle, ParseDisposition,
    ParsedEpisodeMetadata, ParsedReleaseMetadata, ReleaseParseContext, best_parse_for_target,
};
use weaver_server_core::ingest::{derive_release_name, original_release_title};

use super::types::{ParsedEpisode, ParsedRelease};

const MIN_PARSE_CONFIDENCE_FOR_DISPLAY_NAME: f32 = 0.4;
// The most jobs each listing's cache holds: above the largest queue a
// snapshot walks, so a pass over a 10,000-job queue parses each name once.
const RELEASE_DISPLAY_CACHE_MAX_ENTRIES: usize = 16_384;

/// Which listing a job is named for, and so which cache keeps its display:
/// the queue's and history's are separate, so reading history never pushes
/// a queued job's display out. A caller with no job keeps nothing.
#[derive(Clone, Copy)]
pub(crate) enum ReleaseDisplayCache {
    Queue(u64),
    History(u64),
    None,
}

pub(crate) struct ReleaseDisplayInput<'a> {
    pub job_name: &'a str,
    pub metadata: &'a [(String, String)],
    pub category: Option<&'a str>,
    pub cache: ReleaseDisplayCache,
}

#[derive(Clone)]
pub(crate) struct ReleaseDisplayInfo {
    pub original_title: String,
    pub display_title: String,
    pub parsed_release: ParsedRelease,
}

// A job's entry: its id, and a hash of the name, metadata and category the
// display was derived from, so an edit to any of them misses. Built from the
// borrowed input; a hit copies nothing of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ReleaseDisplayKey {
    job_id: u64,
    fingerprint: u64,
}

impl ReleaseDisplayKey {
    fn of(job_id: u64, input: &ReleaseDisplayInput<'_>) -> Self {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        input.job_name.hash(&mut hasher);
        input.metadata.hash(&mut hasher);
        input.category.hash(&mut hasher);
        Self {
            job_id,
            fingerprint: hasher.finish(),
        }
    }
}

const NIL: usize = usize::MAX;

struct LruNode<V> {
    key: ReleaseDisplayKey,
    value: V,
    prev: usize,
    next: usize,
}

/// A least-recently-used map with a fixed bound: a hit moves the entry to
/// the front, and an insert past the bound reuses the slot of the entry used
/// longest ago. Every step is constant time.
struct Lru<V> {
    slots: HashMap<ReleaseDisplayKey, usize>,
    nodes: Vec<LruNode<V>>,
    head: usize,
    tail: usize,
    bound: usize,
}

impl<V> Lru<V> {
    fn new(bound: usize) -> Self {
        Self {
            slots: HashMap::new(),
            nodes: Vec::new(),
            head: NIL,
            tail: NIL,
            bound: bound.max(1),
        }
    }

    fn unlink(&mut self, slot: usize) {
        let (prev, next) = (self.nodes[slot].prev, self.nodes[slot].next);
        match prev {
            NIL => self.head = next,
            prev => self.nodes[prev].next = next,
        }
        match next {
            NIL => self.tail = prev,
            next => self.nodes[next].prev = prev,
        }
    }

    fn push_front(&mut self, slot: usize) {
        self.nodes[slot].prev = NIL;
        self.nodes[slot].next = self.head;
        match self.head {
            NIL => self.tail = slot,
            head => self.nodes[head].prev = slot,
        }
        self.head = slot;
    }

    fn get(&mut self, key: &ReleaseDisplayKey) -> Option<&V> {
        let slot = *self.slots.get(key)?;
        if self.head != slot {
            self.unlink(slot);
            self.push_front(slot);
        }
        Some(&self.nodes[slot].value)
    }

    fn insert(&mut self, key: ReleaseDisplayKey, value: V) {
        if let Some(&slot) = self.slots.get(&key) {
            self.nodes[slot].value = value;
            if self.head != slot {
                self.unlink(slot);
                self.push_front(slot);
            }
            return;
        }
        let slot = if self.nodes.len() < self.bound {
            self.nodes.push(LruNode {
                key,
                value,
                prev: NIL,
                next: NIL,
            });
            self.nodes.len() - 1
        } else {
            let slot = self.tail;
            self.unlink(slot);
            self.slots.remove(&self.nodes[slot].key);
            self.nodes[slot].key = key;
            self.nodes[slot].value = value;
            slot
        };
        self.push_front(slot);
        self.slots.insert(key, slot);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.slots.len()
    }
}

struct ReleaseDisplayCaches {
    queue: Lru<ReleaseDisplayInfo>,
    history: Lru<ReleaseDisplayInfo>,
}

impl ReleaseDisplayCaches {
    fn of(&mut self, cache: ReleaseDisplayCache) -> Option<(&mut Lru<ReleaseDisplayInfo>, u64)> {
        match cache {
            ReleaseDisplayCache::Queue(id) => Some((&mut self.queue, id)),
            ReleaseDisplayCache::History(id) => Some((&mut self.history, id)),
            ReleaseDisplayCache::None => None,
        }
    }
}

pub(crate) fn release_display_info(input: ReleaseDisplayInput<'_>) -> ReleaseDisplayInfo {
    let job_id = match input.cache {
        ReleaseDisplayCache::Queue(id) | ReleaseDisplayCache::History(id) => id,
        ReleaseDisplayCache::None => return compute_release_display_info(input),
    };
    let key = ReleaseDisplayKey::of(job_id, &input);
    let cache = input.cache;
    if let Ok(mut caches) = release_display_caches().lock()
        && let Some((lru, _)) = caches.of(cache)
        && let Some(cached) = lru.get(&key)
    {
        return cached.clone();
    }

    let display = compute_release_display_info(input);
    if let Ok(mut caches) = release_display_caches().lock()
        && let Some((lru, _)) = caches.of(cache)
    {
        lru.insert(key, display.clone());
    }
    display
}

fn release_display_caches() -> &'static Mutex<ReleaseDisplayCaches> {
    static CACHES: OnceLock<Mutex<ReleaseDisplayCaches>> = OnceLock::new();
    CACHES.get_or_init(|| {
        Mutex::new(ReleaseDisplayCaches {
            queue: Lru::new(RELEASE_DISPLAY_CACHE_MAX_ENTRIES),
            history: Lru::new(RELEASE_DISPLAY_CACHE_MAX_ENTRIES),
        })
    })
}

fn compute_release_display_info(input: ReleaseDisplayInput<'_>) -> ReleaseDisplayInfo {
    let original_title = original_release_title(input.job_name, input.metadata);
    let raw = if original_title.trim().is_empty() {
        input.job_name
    } else {
        original_title.as_str()
    };
    let context = build_parse_context(raw, input.metadata, input.category);
    let parsed = best_parse_for_target(raw, &context);
    let usable = is_usable_parse(&parsed);
    let confident_display = is_confident_display_parse(&parsed);
    let display_title = if confident_display {
        display_title_from_parse(&parsed)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| fallback_display_title(&original_title, input.job_name))
    } else {
        fallback_display_title(&original_title, input.job_name)
    };
    let parsed_release = if usable {
        parsed_release_from_parser(parsed)
    } else {
        ParsedRelease::default()
    };

    ReleaseDisplayInfo {
        original_title,
        display_title,
        parsed_release,
    }
}

fn build_parse_context(
    raw: &str,
    metadata: &[(String, String)],
    category: Option<&str>,
) -> ReleaseParseContext {
    let facet_hint = infer_facet_hint(category, raw);
    let title = title_context_guess(raw, facet_hint);
    ReleaseParseContext {
        facet_hint,
        title: ContextTitle { name: title },
        aliases: context_aliases(metadata, raw),
        known_years: known_years(raw, metadata),
        imdb_ids: imdb_ids(raw, metadata),
        episodes: context_episodes(raw, facet_hint),
    }
}

fn infer_facet_hint(category: Option<&str>, raw: &str) -> ContextFacetHint {
    if let Some(category) = category {
        let tokens = category_tokens(category);
        if tokens.iter().any(|token| token == "anime") {
            return ContextFacetHint::Anime;
        }
        if tokens
            .iter()
            .any(|token| matches!(token.as_str(), "movie" | "movies" | "film" | "films"))
        {
            return ContextFacetHint::Movie;
        }
        if tokens.iter().any(|token| {
            matches!(
                token.as_str(),
                "tv" | "television" | "series" | "show" | "shows"
            )
        }) {
            return ContextFacetHint::Series;
        }
    }

    let raw_lower = raw.to_ascii_lowercase();
    if raw_lower.contains("subsplease")
        || raw_lower.contains("erai-raws")
        || raw_lower.contains("horriblesubs")
    {
        return ContextFacetHint::Anime;
    }

    ContextFacetHint::Unknown
}

fn category_tokens(category: &str) -> Vec<String> {
    category
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(|token| token.to_ascii_lowercase())
        .collect()
}

fn title_context_guess(raw: &str, facet_hint: ContextFacetHint) -> String {
    let raw = strip_bracketed_prefix(strip_nzb_suffix(raw));
    let tokens = split_release_tokens(raw);
    if tokens.is_empty() {
        return fallback_display_title(raw, raw);
    }

    let stop_index = tokens
        .iter()
        .position(|token| is_title_boundary_token(token, facet_hint))
        .unwrap_or(tokens.len());
    let candidate = if stop_index > 0 {
        basic_release_name(&tokens[..stop_index].join(" "))
    } else {
        String::new()
    };
    if !candidate.is_empty() {
        candidate
    } else {
        fallback_display_title(raw, raw)
    }
}

fn context_aliases(metadata: &[(String, String)], raw: &str) -> Vec<ContextAlias> {
    let raw_clean = basic_release_name(raw);
    metadata
        .iter()
        .filter_map(|(key, value)| {
            let key = key.to_ascii_lowercase();
            if key.contains("original_title") || !(key.contains("alias") || key.contains("title")) {
                return None;
            }
            let alias = basic_release_name(value);
            if alias.is_empty()
                || alias.eq_ignore_ascii_case(&raw_clean)
                || split_release_tokens(&alias)
                    .iter()
                    .any(|token| is_title_boundary_token(token, ContextFacetHint::Unknown))
            {
                return None;
            }
            Some(ContextAlias { name: alias })
        })
        .collect()
}

fn known_years(raw: &str, metadata: &[(String, String)]) -> Vec<i32> {
    let mut years = BTreeSet::new();
    collect_years(raw, &mut years);
    for (key, value) in metadata {
        collect_years(key, &mut years);
        collect_years(value, &mut years);
    }
    years.into_iter().collect()
}

fn collect_years(value: &str, years: &mut BTreeSet<i32>) {
    for token in split_release_tokens(value) {
        let Ok(year) = token.parse::<i32>() else {
            continue;
        };
        if (1900..=2100).contains(&year) {
            years.insert(year);
        }
    }
}

fn imdb_ids(raw: &str, metadata: &[(String, String)]) -> Vec<String> {
    let mut ids = BTreeSet::new();
    collect_imdb_ids(raw, &mut ids);
    for (key, value) in metadata {
        collect_imdb_ids(key, &mut ids);
        collect_imdb_ids(value, &mut ids);
    }
    ids.into_iter().collect()
}

fn collect_imdb_ids(value: &str, ids: &mut BTreeSet<String>) {
    for capture in imdb_regex().captures_iter(value) {
        if let Some(id) = capture.get(0) {
            ids.insert(id.as_str().to_ascii_lowercase());
        }
    }
}

fn context_episodes(raw: &str, facet_hint: ContextFacetHint) -> Vec<ContextEpisode> {
    if let Some(episode) = standard_episode_context(raw) {
        return vec![episode];
    }
    if let Some(episode) = daily_episode_context(raw) {
        return vec![episode];
    }
    if matches!(facet_hint, ContextFacetHint::Anime)
        && let Some(absolute_number) = anime_absolute_episode(raw)
    {
        return vec![ContextEpisode {
            absolute_number: Some(absolute_number),
            ..Default::default()
        }];
    }
    Vec::new()
}

fn standard_episode_context(raw: &str) -> Option<ContextEpisode> {
    let capture = standard_episode_regex().captures(raw)?;
    let season = capture.get(1)?.as_str().parse::<u32>().ok()?;
    let episode = capture.get(2)?.as_str().parse::<u32>().ok()?;
    Some(ContextEpisode {
        season: Some(season),
        episode: Some(episode),
        ..Default::default()
    })
}

fn daily_episode_context(raw: &str) -> Option<ContextEpisode> {
    for capture in ymd_date_regex().captures_iter(raw) {
        let year = capture.get(1)?.as_str().parse::<i32>().ok()?;
        let month = capture.get(2)?.as_str().parse::<u32>().ok()?;
        let day = capture.get(3)?.as_str().parse::<u32>().ok()?;
        if let Some(air_date) = NaiveDate::from_ymd_opt(year, month, day) {
            return Some(ContextEpisode {
                air_date: Some(air_date),
                ..Default::default()
            });
        }
    }

    for capture in dmy_date_regex().captures_iter(raw) {
        let day = capture.get(1)?.as_str().parse::<u32>().ok()?;
        let month = capture.get(2)?.as_str().parse::<u32>().ok()?;
        let year = capture.get(3)?.as_str().parse::<i32>().ok()?;
        if let Some(air_date) = NaiveDate::from_ymd_opt(year, month, day) {
            return Some(ContextEpisode {
                air_date: Some(air_date),
                ..Default::default()
            });
        }
    }

    None
}

fn anime_absolute_episode(raw: &str) -> Option<u32> {
    let raw = strip_bracketed_prefix(strip_nzb_suffix(raw));
    let tokens = split_release_tokens(raw);
    for (index, token) in tokens.iter().enumerate() {
        if is_quality_token(&token.to_ascii_uppercase()) {
            break;
        }
        if index == 0 || !token.chars().all(|ch| ch.is_ascii_digit()) {
            continue;
        }
        let value = token.parse::<u32>().ok()?;
        if value > 0 && value < 10_000 && !is_plausible_year(token) {
            return Some(value);
        }
    }
    None
}

fn parsed_release_from_parser(value: ParsedReleaseMetadata) -> ParsedRelease {
    ParsedRelease {
        normalized_title: value.normalized_title,
        release_group: value.release_group,
        languages_audio: value.languages_audio,
        languages_subtitles: value.languages_subtitles,
        year: value.year.and_then(|year| u32::try_from(year).ok()),
        quality: value.quality,
        source: value.source.map(|source| source.as_str().to_owned()),
        video_codec: value.video_codec.map(|codec| codec.as_str().to_owned()),
        video_encoding: value.video_encoding,
        audio: value.audio.map(|codec| codec.as_str().to_owned()),
        audio_codecs: value
            .audio_codecs
            .into_iter()
            .map(|codec| codec.as_str().to_owned())
            .collect(),
        audio_channels: value.audio_channels,
        is_dual_audio: value.is_dual_audio,
        is_atmos: value.is_atmos,
        is_dolby_vision: value.is_dolby_vision,
        detected_hdr: value.detected_hdr,
        is_hdr10plus: value.is_hdr10plus,
        is_hlg: value.is_hlg,
        fps: value.fps,
        is_proper_upload: value.is_proper_upload,
        is_repack: value.is_repack,
        is_remux: value.is_remux,
        is_bd_disk: value.is_bd_disk,
        is_ai_enhanced: value.is_ai_enhanced,
        is_hardcoded_subs: value.is_hardcoded_subs,
        streaming_service: value
            .streaming_service
            .map(|service| service.as_str().to_owned()),
        edition: value.edition,
        anime_version: value.anime_version,
        episode: value.episode.map(parsed_episode_from_parser),
        parse_confidence: value.parse_confidence.clamp(0.0, 1.0),
    }
}

fn parsed_episode_from_parser(value: ParsedEpisodeMetadata) -> ParsedEpisode {
    ParsedEpisode {
        season: value.season,
        episode_numbers: value.episode_numbers,
        absolute_episode: value.absolute_episode.or_else(|| {
            value
                .absolute_episode_numbers
                .first()
                .copied()
                .or_else(|| value.special_absolute_episode_numbers.first().copied())
        }),
        raw: value.raw,
    }
}

fn display_title_from_parse(parsed: &ParsedReleaseMetadata) -> Option<String> {
    let title = basic_release_name(&parsed.normalized_title);
    if title.is_empty() {
        return None;
    }
    Some(append_episode_suffix(
        finalize_release_name(title),
        parsed.episode.as_ref(),
    ))
}

fn append_episode_suffix(name: String, episode: Option<&ParsedEpisodeMetadata>) -> String {
    let Some(episode) = episode else {
        return name;
    };

    if let Some(season) = episode.season
        && !episode.episode_numbers.is_empty()
    {
        let episodes = episode
            .episode_numbers
            .iter()
            .map(|episode| format!("E{episode:02}"))
            .collect::<Vec<_>>()
            .join("-");
        return format!("{name} — S{season:02}{episodes}");
    }

    if let Some(abs) = episode.absolute_episode.or_else(|| {
        episode
            .absolute_episode_numbers
            .first()
            .copied()
            .or_else(|| episode.special_absolute_episode_numbers.first().copied())
    }) {
        return format!("{name} — {abs:03}");
    }

    name
}

fn is_confident_display_parse(parsed: &ParsedReleaseMetadata) -> bool {
    is_usable_parse(parsed)
        && (parsed.parse_confidence >= MIN_PARSE_CONFIDENCE_FOR_DISPLAY_NAME
            || parsed.episode.is_some())
}

fn is_usable_parse(parsed: &ParsedReleaseMetadata) -> bool {
    parsed.disposition != ParseDisposition::Unparseable
        && !parsed.normalized_title.trim().is_empty()
        && (parsed.parse_confidence >= MIN_PARSE_CONFIDENCE_FOR_DISPLAY_NAME
            || has_media_signal(parsed))
}

fn has_media_signal(parsed: &ParsedReleaseMetadata) -> bool {
    parsed.episode.is_some()
        || parsed.year.is_some()
        || parsed.quality.is_some()
        || parsed.source.is_some()
        || parsed.video_codec.is_some()
        || parsed.video_encoding.is_some()
        || parsed.audio.is_some()
        || !parsed.audio_codecs.is_empty()
        || parsed.audio_channels.is_some()
}

fn fallback_display_title(primary: &str, secondary: &str) -> String {
    derive_release_name(Some(primary), Some(secondary))
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

fn basic_release_name(raw: &str) -> String {
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

fn strip_bracketed_prefix(raw: &str) -> &str {
    let trimmed = raw.trim_start();
    if let Some(rest) = trimmed.strip_prefix('[')
        && let Some(end) = rest.find(']')
    {
        return rest[end + 1..].trim_start();
    }
    trimmed
}

fn is_title_boundary_token(token: &str, facet_hint: ContextFacetHint) -> bool {
    let upper = token.to_ascii_uppercase();
    if standard_episode_regex().is_match(&upper)
        || is_generic_season_token(&upper)
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
        return true;
    }

    if matches!(
        facet_hint,
        ContextFacetHint::Anime | ContextFacetHint::Series
    ) && upper.chars().all(|ch| ch.is_ascii_digit())
    {
        return true;
    }

    false
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
        .enumerate()
        .map(|(index, word)| title_case_word(word, index == 0))
        .collect::<Vec<_>>()
        .join(" ")
}

fn title_case_word(word: &str, is_first: bool) -> String {
    if !is_first
        && matches!(
            word,
            "A" | "AN"
                | "AND"
                | "AS"
                | "AT"
                | "BUT"
                | "BY"
                | "FOR"
                | "IN"
                | "OF"
                | "ON"
                | "OR"
                | "THE"
                | "TO"
                | "WITH"
        )
    {
        return word.to_ascii_lowercase();
    }

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

fn standard_episode_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?i)\bS(\d{1,2})E(\d{1,4})\b").expect("valid regex"))
}

fn ymd_date_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?:^|[.\s_-])(\d{4})[.\s_-](\d{1,2})[.\s_-](\d{1,2})(?:$|[.\s_-])")
            .expect("valid regex")
    })
}

fn dmy_date_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?:^|[.\s_-])(\d{1,2})[.\s_-](\d{1,2})[.\s_-](\d{4})(?:$|[.\s_-])")
            .expect("valid regex")
    })
}

fn imdb_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"tt\d{5,}").expect("valid regex"))
}

// The titles a job's release display derives from its name, metadata and
// category: the original, the display and the normalized title. The admin
// diagnostics package reads these for its stand-in table, so the display is
// derived the one way the API derives it.
pub fn release_display_names(
    job_name: &str,
    metadata: &[(String, String)],
    category: Option<&str>,
) -> Vec<String> {
    let info = release_display_info(ReleaseDisplayInput {
        job_name,
        metadata,
        category,
        cache: ReleaseDisplayCache::None,
    });
    vec![
        info.original_title,
        info.display_title,
        info.parsed_release.normalized_title,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn display(job_name: &str, category: Option<&str>) -> ReleaseDisplayInfo {
        release_display_info(ReleaseDisplayInput {
            job_name,
            metadata: &[],
            category,
            cache: ReleaseDisplayCache::None,
        })
    }

    #[test]
    fn original_title_metadata_wins_over_job_name() {
        let metadata = vec![(
            "weaver.original_title".to_string(),
            "Stoneguard.S04E29.1080p.WEB-DL.H.265".to_string(),
        )];

        let info = release_display_info(ReleaseDisplayInput {
            job_name: "Display Name",
            metadata: &metadata,
            category: Some("TV > HD"),
            cache: ReleaseDisplayCache::Queue(1),
        });

        assert_eq!(info.original_title, metadata[0].1);
        assert_eq!(info.display_title, "Stoneguard — S04E29");
    }

    #[test]
    fn series_category_parses_standard_episode_metadata() {
        let info = display("Show.Name.S01E02.1080p.WEB-DL.H264-Group", Some("TV > HD"));

        assert_eq!(info.display_title, "Show Name — S01E02");
        assert_eq!(
            info.parsed_release
                .episode
                .as_ref()
                .and_then(|episode| episode.season),
            Some(1)
        );
    }

    #[test]
    fn movie_category_keeps_numeric_movie_title_readable() {
        let info = display("Perc.30.2023.720p.WEBRip-LAMA", Some("Movies > HD"));

        assert_eq!(info.display_title, "Perc 30");
        assert_eq!(info.parsed_release.year, Some(2023));
    }

    #[test]
    fn anime_category_supports_absolute_episode_display() {
        let info = display("[SubsPlease] Emberfall - 330 [1080p]", Some("Anime"));

        assert_eq!(info.display_title, "Emberfall — 330");
        assert_eq!(
            info.parsed_release
                .episode
                .as_ref()
                .and_then(|episode| episode.absolute_episode),
            Some(330)
        );
    }

    #[test]
    fn unknown_category_still_extracts_common_release_metadata() {
        let info = display("Glass Harbor.2024.2160p.BluRay.Remux.H.265-GROUP", None);

        assert_eq!(info.display_title, "Glass Harbor");
        assert_eq!(info.parsed_release.quality.as_deref(), Some("2160p"));
        assert_eq!(info.parsed_release.source.as_deref(), Some("BluRay"));
    }

    #[test]
    fn context_collects_imdb_ids_and_years() {
        let metadata = vec![
            ("imdb".to_string(), "tt1234567".to_string()),
            ("year".to_string(), "2024".to_string()),
        ];

        let context = build_parse_context("Example.Movie.2024.1080p.WEB-DL", &metadata, None);

        assert_eq!(context.known_years, vec![2024]);
        assert_eq!(context.imdb_ids, vec!["tt1234567".to_string()]);
    }

    #[test]
    fn malformed_non_media_name_fails_open() {
        let info = display("ubuntu-24.04.2-live-server-amd64", None);

        assert_eq!(info.display_title, "ubuntu-24 04 2-live-server-amd64");
        assert_eq!(info.parsed_release, ParsedRelease::default());
    }

    fn key(job_id: u64) -> ReleaseDisplayKey {
        ReleaseDisplayKey {
            job_id,
            fingerprint: 0,
        }
    }

    #[test]
    fn the_cache_drops_the_entry_used_longest_ago_and_keeps_its_bound() {
        let mut lru = Lru::new(3);
        for id in 1..=3 {
            lru.insert(key(id), id);
        }
        // Touching 1 makes 2 the oldest; a fourth job pushes 2 out, not 1.
        assert_eq!(lru.get(&key(1)), Some(&1));
        lru.insert(key(4), 4);
        assert_eq!(lru.len(), 3);
        assert_eq!(lru.get(&key(2)), None);
        assert_eq!(lru.get(&key(1)), Some(&1));
        assert_eq!(lru.get(&key(3)), Some(&3));
        assert_eq!(lru.get(&key(4)), Some(&4));
        // A pass over more jobs than the bound keeps the latest ones.
        for id in 10..20 {
            lru.insert(key(id), id);
        }
        assert_eq!(lru.len(), 3);
        assert_eq!(
            [17, 18, 19].map(|id| lru.get(&key(id)).copied()),
            [Some(17), Some(18), Some(19)]
        );
    }

    #[test]
    fn reading_history_never_pushes_a_queued_job_out() {
        let name = "History.Reader.Never.Evicts.S01E01.1080p";
        let queued = |id| ReleaseDisplayInput {
            job_name: name,
            metadata: &[],
            category: None,
            cache: ReleaseDisplayCache::Queue(id),
        };
        let queue_id = u64::MAX - 7;
        release_display_info(queued(queue_id));
        for id in 0..(RELEASE_DISPLAY_CACHE_MAX_ENTRIES as u64 + 10) {
            release_display_info(ReleaseDisplayInput {
                job_name: name,
                metadata: &[],
                category: None,
                cache: ReleaseDisplayCache::History(id),
            });
        }
        let key = ReleaseDisplayKey::of(queue_id, &queued(queue_id));
        let mut caches = release_display_caches().lock().unwrap();
        assert!(caches.queue.get(&key).is_some());
        assert_eq!(caches.history.len(), RELEASE_DISPLAY_CACHE_MAX_ENTRIES);
    }

    #[test]
    fn an_edited_name_misses_the_cached_display() {
        let first = release_display_info(ReleaseDisplayInput {
            job_name: "Edited.Name.Before.S01E01.1080p",
            metadata: &[],
            category: None,
            cache: ReleaseDisplayCache::Queue(u64::MAX - 9),
        });
        let second = release_display_info(ReleaseDisplayInput {
            job_name: "Edited.Name.After.S01E01.1080p",
            metadata: &[],
            category: None,
            cache: ReleaseDisplayCache::Queue(u64::MAX - 9),
        });
        assert_ne!(first.display_title, second.display_title);
    }
}
