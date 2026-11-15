// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RssRuleAction {
    Accept,
    Reject,
}

impl RssRuleAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Reject => "reject",
        }
    }

    pub fn parse(value: &str) -> Result<Self, StateError> {
        match value {
            "accept" => Ok(Self::Accept),
            "reject" => Ok(Self::Reject),
            other => Err(StateError::Database(format!(
                "invalid RSS rule action: {other}"
            ))),
        }
    }
}

/// What a feed's item was judged, as its seen row keeps it and the API
/// sends it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RssSeenDecision {
    /// A rule accepted it (a preview's judgement; a poll records what the
    /// accept came to).
    Accepted,
    Rejected,
    /// No rule took it, or one already holds it.
    Ignored,
    /// It became a job.
    Submitted,
    /// Adding it failed.
    Error,
    /// A torrent held while torrents are off, in place of a decision: not
    /// seen, it is judged anew once torrents are on. Matched as a literal in
    /// SQL, where the index on held items serves it.
    Waiting,
    /// A stored word this build does not know, kept as it reads.
    Other(String),
}

impl RssSeenDecision {
    pub const KNOWN: [Self; 6] = [
        Self::Accepted,
        Self::Rejected,
        Self::Ignored,
        Self::Submitted,
        Self::Error,
        Self::Waiting,
    ];

    pub fn as_str(&self) -> &str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::Ignored => "ignored",
            Self::Submitted => "submitted",
            Self::Error => "error",
            Self::Waiting => "waiting",
            Self::Other(word) => word,
        }
    }

    /// The decision `value` names; a word no decision is stays as it is.
    pub fn parse(value: &str) -> Self {
        Self::KNOWN
            .into_iter()
            .find(|decision| decision.as_str() == value)
            .unwrap_or_else(|| Self::Other(value.to_owned()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RssFeedRow {
    // Ids of the feed instances attached to this feed, in the order they run.
    pub scripts: Vec<String>,
    pub id: u32,
    pub name: String,
    pub url: String,
    pub enabled: bool,
    pub poll_interval_secs: u32,
    pub username: Option<String>,
    pub password: Option<String>,
    pub default_category: Option<String>,
    pub default_metadata: Vec<(String, String)>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub last_polled_at: Option<i64>,
    pub last_success_at: Option<i64>,
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RssRuleRow {
    pub id: u32,
    pub feed_id: u32,
    pub sort_order: i32,
    pub enabled: bool,
    pub action: RssRuleAction,
    pub title_regex: Option<String>,
    pub item_categories: Vec<String>,
    pub min_size_bytes: Option<u64>,
    pub max_size_bytes: Option<u64>,
    pub category_override: Option<String>,
    pub metadata: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RssSeenItemRow {
    pub feed_id: u32,
    pub item_id: String,
    pub item_title: String,
    pub published_at: Option<i64>,
    pub size_bytes: Option<u64>,
    pub decision: RssSeenDecision,
    pub seen_at: i64,
    pub job_id: Option<u64>,
    pub item_url: Option<String>,
    pub error: Option<String>,
}
