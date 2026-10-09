use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptKind {
    PostProcessing,
    Queue,
    Scan,
    Scheduler,
    Feed,
}

impl ScriptKind {
    pub const ALL: [Self; 5] = [
        Self::PostProcessing,
        Self::Queue,
        Self::Scan,
        Self::Scheduler,
        Self::Feed,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::PostProcessing => "POST-PROCESSING",
            Self::Queue => "QUEUE",
            Self::Scan => "SCAN",
            Self::Scheduler => "SCHEDULER",
            Self::Feed => "FEED",
        }
    }
}

/// Declaration order is NZBGet's event priority, from lowest to highest.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QueueEvent {
    FileDownloaded,
    UrlCompleted,
    NzbMarked,
    NzbAdded,
    NzbNamed,
    NzbDownloaded,
    NzbDeleted,
}

impl QueueEvent {
    // NZB_NAMED is retained for truthful declarations, but Weaver does not raise it.
    pub const ALL: [Self; 7] = [
        Self::FileDownloaded,
        Self::UrlCompleted,
        Self::NzbMarked,
        Self::NzbAdded,
        Self::NzbNamed,
        Self::NzbDownloaded,
        Self::NzbDeleted,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::FileDownloaded => "FILE_DOWNLOADED",
            Self::UrlCompleted => "URL_COMPLETED",
            Self::NzbMarked => "NZB_MARKED",
            Self::NzbAdded => "NZB_ADDED",
            Self::NzbNamed => "NZB_NAMED",
            Self::NzbDownloaded => "NZB_DOWNLOADED",
            Self::NzbDeleted => "NZB_DELETED",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub enum ScriptTaskTime {
    Startup,
    Hourly { minute: u8 },
    Daily { hour: u8, minute: u8 },
}

impl std::str::FromStr for ScriptTaskTime {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = "invalid script task time";
        if value == "*" {
            return Ok(Self::Startup);
        }
        let (hour, minute) = value.split_once(':').ok_or(invalid)?;
        let number = |value: &str, max: u8| {
            if value.is_empty() || value.len() > 2 || !value.bytes().all(|b| b.is_ascii_digit()) {
                return Err(invalid);
            }
            value
                .parse::<u8>()
                .ok()
                .filter(|value| *value <= max)
                .ok_or(invalid)
        };
        let minute = number(minute, 59)?;
        if hour == "*" {
            Ok(Self::Hourly { minute })
        } else {
            Ok(Self::Daily {
                hour: number(hour, 23)?,
                minute,
            })
        }
    }
}

impl fmt::Display for ScriptTaskTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Startup => f.write_str("*"),
            Self::Hourly { minute } => write!(f, "*:{minute:02}"),
            Self::Daily { hour, minute } => write!(f, "{hour:02}:{minute:02}"),
        }
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub enum ScriptEventLabel {
    #[default]
    PostProcessing,
    Queue(QueueEvent),
    Scan,
    Scheduler(u64),
    Feed(u64),
}

impl ScriptEventLabel {
    pub fn kind(&self) -> ScriptKind {
        match self {
            Self::PostProcessing => ScriptKind::PostProcessing,
            Self::Queue(_) => ScriptKind::Queue,
            Self::Scan => ScriptKind::Scan,
            Self::Scheduler(_) => ScriptKind::Scheduler,
            Self::Feed(_) => ScriptKind::Feed,
        }
    }
}

impl fmt::Display for ScriptEventLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PostProcessing => f.write_str("post_processing"),
            Self::Queue(event) => write!(f, "queue:{}", event.as_str()),
            Self::Scan => f.write_str("scan"),
            Self::Scheduler(id) => write!(f, "scheduler:{id}"),
            Self::Feed(id) => write!(f, "feed:{id}"),
        }
    }
}

impl Serialize for ScriptEventLabel {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ScriptEventLabel {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        let event = match value.as_str() {
            "post_processing" => Some(Self::PostProcessing),
            "scan" => Some(Self::Scan),
            _ => value.split_once(':').and_then(|(kind, value)| match kind {
                "queue" => QueueEvent::ALL
                    .into_iter()
                    .find(|event| event.as_str() == value)
                    .map(Self::Queue),
                "scheduler" => value.parse().ok().map(Self::Scheduler),
                "feed" => value.parse().ok().map(Self::Feed),
                _ => None,
            }),
        };
        event.ok_or_else(|| serde::de::Error::custom("invalid script event"))
    }
}
