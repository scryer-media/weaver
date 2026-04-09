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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RssFeedRow {
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
    pub decision: String,
    pub seen_at: i64,
    pub job_id: Option<u64>,
    pub item_url: Option<String>,
    pub error: Option<String>,
}
