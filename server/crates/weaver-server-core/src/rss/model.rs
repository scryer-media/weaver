use std::collections::BTreeMap;
use std::io::Cursor;

use quick_xml::events::{BytesStart, Event};
use quick_xml::{Reader, XmlVersion};
use regex::Regex;
use tracing::warn;

use crate::rss::{RssFeedRow, RssRuleRow};

pub(crate) const DEFAULT_POLL_INTERVAL_SECS: u32 = 900;
pub(crate) const RSS_SYNC_TICK_SECS: u64 = 60;
pub(crate) const RSS_SEEN_RETENTION_SECS: i64 = 180 * 24 * 60 * 60;

#[derive(Debug, Clone)]
pub(crate) struct FeedItem {
    pub(crate) item_id: String,
    pub(crate) title: String,
    pub(crate) published_at: Option<i64>,
    pub(crate) size_bytes: Option<u64>,
    pub(crate) categories: Vec<String>,
    pub(crate) download_url: Option<String>,
    pub(crate) display_url: Option<String>,
}

#[derive(Debug, Default)]
struct ParsedFeedEntry {
    id: String,
    title: Option<String>,
    published_at: Option<i64>,
    updated_at: Option<i64>,
    categories: Vec<String>,
    links: Vec<ParsedFeedLink>,
    enclosures: Vec<ParsedFeedLink>,
}

#[derive(Debug, Clone)]
struct ParsedFeedLink {
    href: String,
    length: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledRule {
    pub(crate) row: RssRuleRow,
    pub(crate) title_regex: Option<Regex>,
}

pub(crate) fn unix_now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub(crate) fn is_due(feed: &RssFeedRow, now: i64) -> bool {
    let last_polled_at = feed.last_polled_at.unwrap_or(0);
    let interval = if feed.poll_interval_secs == 0 {
        DEFAULT_POLL_INTERVAL_SECS
    } else {
        feed.poll_interval_secs
    };
    now.saturating_sub(last_polled_at) >= interval as i64
}

pub(crate) fn apply_basic_auth(
    request: reqwest::RequestBuilder,
    feed: &RssFeedRow,
) -> reqwest::RequestBuilder {
    if let Some(username) = &feed.username {
        request.basic_auth(username, feed.password.clone())
    } else {
        request
    }
}

pub(crate) fn compile_rules(rules: Vec<RssRuleRow>) -> Vec<CompiledRule> {
    rules
        .into_iter()
        .filter(|rule| rule.enabled)
        .filter_map(|rule| {
            let title_regex = match &rule.title_regex {
                Some(pattern) => match Regex::new(pattern) {
                    Ok(regex) => Some(regex),
                    Err(error) => {
                        warn!(rule_id = rule.id, error = %error, "invalid RSS regex ignored");
                        return None;
                    }
                },
                None => None,
            };
            Some(CompiledRule {
                row: rule,
                title_regex,
            })
        })
        .collect()
}

pub(crate) fn evaluate_item<'a>(
    rules: &'a [CompiledRule],
    item: &FeedItem,
) -> Option<&'a CompiledRule> {
    rules.iter().find(|rule| rule_matches(rule, item))
}

pub(crate) fn rule_matches(rule: &CompiledRule, item: &FeedItem) -> bool {
    if let Some(regex) = &rule.title_regex
        && !regex.is_match(&item.title)
    {
        return false;
    }

    if !rule.row.item_categories.is_empty() {
        let wanted: Vec<String> = rule
            .row
            .item_categories
            .iter()
            .map(|value| value.to_ascii_lowercase())
            .collect();
        let item_categories: Vec<String> = item
            .categories
            .iter()
            .map(|value| value.to_ascii_lowercase())
            .collect();
        if !wanted
            .iter()
            .any(|wanted| item_categories.iter().any(|item| item == wanted))
        {
            return false;
        }
    }

    if let Some(min_size) = rule.row.min_size_bytes {
        let Some(size) = item.size_bytes else {
            return false;
        };
        if size < min_size {
            return false;
        }
    }

    if let Some(max_size) = rule.row.max_size_bytes {
        let Some(size) = item.size_bytes else {
            return false;
        };
        if size > max_size {
            return false;
        }
    }

    true
}

pub(crate) fn build_submission_metadata(
    feed: &RssFeedRow,
    rule: &RssRuleRow,
    item: &FeedItem,
) -> Vec<(String, String)> {
    let mut merged = BTreeMap::new();
    for (key, value) in &feed.default_metadata {
        merged.insert(key.clone(), value.clone());
    }
    for (key, value) in &rule.metadata {
        merged.insert(key.clone(), value.clone());
    }
    merged.insert("rss.feed_id".to_string(), feed.id.to_string());
    merged.insert("rss.feed_name".to_string(), feed.name.clone());
    merged.insert("rss.item_id".to_string(), item.item_id.clone());
    merged.insert("rss.item_title".to_string(), item.title.clone());
    if let Some(url) = item
        .download_url
        .clone()
        .or_else(|| item.display_url.clone())
    {
        merged.insert("rss.item_url".to_string(), url);
    }
    merged.into_iter().collect()
}

pub(crate) fn parse_feed_items(xml: &[u8]) -> Result<Vec<FeedItem>, String> {
    let mut reader = Reader::from_reader(Cursor::new(xml));
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut text_buf = String::new();
    let mut current_entry: Option<ParsedFeedEntry> = None;
    let mut items = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) => {
                let name = e.name();
                let raw = String::from_utf8_lossy(name.as_ref()).to_ascii_lowercase();
                let local = String::from_utf8_lossy(local_name(name.as_ref())).to_ascii_lowercase();
                if local == "item" || local == "entry" {
                    current_entry = Some(ParsedFeedEntry::default());
                } else if let Some(entry) = current_entry.as_mut() {
                    apply_empty_feed_element(&reader, &e, &raw, &local, entry)?;
                }
                text_buf.clear();
            }
            Ok(Event::Empty(e)) => {
                let name = e.name();
                let raw = String::from_utf8_lossy(name.as_ref()).to_ascii_lowercase();
                let local = String::from_utf8_lossy(local_name(name.as_ref())).to_ascii_lowercase();
                if let Some(entry) = current_entry.as_mut() {
                    apply_empty_feed_element(&reader, &e, &raw, &local, entry)?;
                }
                text_buf.clear();
            }
            Ok(Event::Text(e)) => {
                let decoded = e.decode().map_err(|e| e.to_string())?;
                text_buf = quick_xml::escape::unescape(&decoded)
                    .map_err(|e| e.to_string())?
                    .into_owned();
            }
            Ok(Event::End(e)) => {
                let name = e.name();
                let local = String::from_utf8_lossy(local_name(name.as_ref())).to_ascii_lowercase();
                if local == "item" || local == "entry" {
                    if let Some(entry) = current_entry.take() {
                        items.push(feed_item_from_entry(entry));
                    }
                } else if let Some(entry) = current_entry.as_mut() {
                    apply_text_feed_element(&local, text_buf.trim(), entry);
                }
                text_buf.clear();
            }
            Err(error) => return Err(error.to_string()),
            _ => {}
        }
        buf.clear();
    }

    Ok(items)
}

fn feed_item_from_entry(entry: ParsedFeedEntry) -> FeedItem {
    let title = entry
        .title
        .clone()
        .unwrap_or_else(|| "Untitled RSS Item".to_string());
    let media_content = entry.enclosures.iter().find(|link| !link.href.is_empty());
    let download_url = media_content
        .as_ref()
        .map(|link| link.href.clone())
        .or_else(|| {
            entry
                .links
                .iter()
                .find(|link| looks_like_direct_nzb_url(&link.href))
                .map(|link| link.href.clone())
        });
    let display_url = entry.links.first().map(|link| link.href.clone());
    let item_id = if !entry.id.is_empty() {
        entry.id
    } else if let Some(url) = download_url.clone().or_else(|| display_url.clone()) {
        url
    } else {
        fallback_item_id(&title, entry.published_at, None)
    };

    let size_bytes = media_content.and_then(|link| link.length).or_else(|| {
        entry
            .links
            .iter()
            .find(|link| download_url.as_deref() == Some(link.href.as_str()))
            .and_then(|link| link.length)
    });

    FeedItem {
        item_id,
        title,
        published_at: entry.published_at.or(entry.updated_at),
        size_bytes,
        categories: entry.categories,
        download_url,
        display_url,
    }
}

fn apply_empty_feed_element<R: std::io::BufRead>(
    reader: &Reader<R>,
    element: &BytesStart<'_>,
    raw_name: &str,
    local_name: &str,
    entry: &mut ParsedFeedEntry,
) -> Result<(), String> {
    match local_name {
        "link" => {
            if let Some(link) = feed_link_from_attrs(reader, element)? {
                let rel = attr_value_by_name(reader, element, b"rel")?;
                if rel
                    .as_deref()
                    .is_some_and(|rel| rel.eq_ignore_ascii_case("enclosure"))
                {
                    entry.enclosures.push(link.clone());
                }
                entry.links.push(link);
            }
        }
        "enclosure" => {
            if let Some(link) = feed_link_from_attrs(reader, element)? {
                entry.enclosures.push(link);
            }
        }
        "content" if raw_name == "media:content" => {
            if let Some(link) = feed_link_from_attrs(reader, element)? {
                entry.enclosures.push(link);
            }
        }
        "category" => {
            if let Some(term) = attr_value_by_name(reader, element, b"term")?
                && !term.is_empty()
            {
                entry.categories.push(term);
            }
        }
        _ => {}
    }
    Ok(())
}

fn apply_text_feed_element(local_name: &str, text: &str, entry: &mut ParsedFeedEntry) {
    if text.is_empty() {
        return;
    }
    match local_name {
        "title" if entry.title.is_none() => entry.title = Some(text.to_string()),
        "id" | "guid" if entry.id.is_empty() => entry.id = text.to_string(),
        "link" => entry.links.push(ParsedFeedLink {
            href: text.to_string(),
            length: None,
        }),
        "category" => entry.categories.push(text.to_string()),
        "pubdate" | "published" => entry.published_at = parse_feed_timestamp(text),
        "updated" => entry.updated_at = parse_feed_timestamp(text),
        _ => {}
    }
}

fn feed_link_from_attrs<R: std::io::BufRead>(
    reader: &Reader<R>,
    element: &BytesStart<'_>,
) -> Result<Option<ParsedFeedLink>, String> {
    let href = match attr_value_by_name(reader, element, b"href")? {
        Some(href) => Some(href),
        None => attr_value_by_name(reader, element, b"url")?,
    };
    let Some(href) = href.filter(|href| !href.is_empty()) else {
        return Ok(None);
    };
    let length = match attr_value_by_name(reader, element, b"length")? {
        Some(length) => Some(length),
        None => attr_value_by_name(reader, element, b"filesize")?,
    }
    .and_then(|value| value.parse::<u64>().ok());
    Ok(Some(ParsedFeedLink { href, length }))
}

fn attr_value_by_name<R: std::io::BufRead>(
    reader: &Reader<R>,
    element: &BytesStart<'_>,
    wanted: &[u8],
) -> Result<Option<String>, String> {
    for attr in element.attributes() {
        let attr = attr.map_err(|e| e.to_string())?;
        if local_name(attr.key.as_ref()).eq_ignore_ascii_case(wanted) {
            let value = attr
                .decoded_and_normalized_value(XmlVersion::Implicit1_0, reader.decoder())
                .map(std::borrow::Cow::into_owned)
                .unwrap_or_else(|_| String::from_utf8_lossy(&attr.value).into_owned());
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn parse_feed_timestamp(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .or_else(|_| chrono::DateTime::parse_from_rfc2822(value))
        .ok()
        .map(|timestamp| timestamp.timestamp())
}

fn local_name(name: &[u8]) -> &[u8] {
    name.rsplit(|byte| *byte == b':').next().unwrap_or(name)
}

pub(crate) fn looks_like_direct_nzb_url(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    if parsed.path().to_ascii_lowercase().ends_with(".nzb") {
        return true;
    }
    parsed
        .query_pairs()
        .any(|(key, value)| key.eq_ignore_ascii_case("t") && value.eq_ignore_ascii_case("get"))
}

pub(crate) fn fallback_item_id(
    title: &str,
    published_at: Option<i64>,
    size_bytes: Option<u64>,
) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(title.as_bytes());
    if let Some(published_at) = published_at {
        hasher.update(published_at.to_le_bytes());
    }
    if let Some(size_bytes) = size_bytes {
        hasher.update(size_bytes.to_le_bytes());
    }
    hex::encode(hasher.finalize())
}
