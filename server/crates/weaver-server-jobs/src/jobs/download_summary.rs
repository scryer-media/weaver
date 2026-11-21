// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How a job's download went, built once as the job finishes and kept on its
// history row as one compact JSON document: the archive tree, the recovery
// sets and the post. Reporting only: nothing reads it to schedule, repair or
// extract.
//
// Its size is bounded whatever the job: at most [`NODES_KEPT`] tree nodes,
// breadth first, with the true count and depth beside them; at most
// [`RECOVERY_KEPT`] recovery objects of at most [`SETS_KEPT`] sets each; and
// each name at most [`NAME_MAX`] characters.
//
// It never holds a password: only whether a node needed one, and where the
// one that opened it came from.

use serde::{Deserialize, Serialize};

pub const DOWNLOAD_SUMMARY_VERSION: u32 = 1;
pub const NODES_KEPT: usize = 200;
pub const RECOVERY_KEPT: usize = 64;
pub const SETS_KEPT: usize = 32;
pub const NAME_MAX: usize = 255;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadSummary {
    #[serde(rename = "v")]
    pub version: u32,
    // `None` for a job the archive graph did not run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout: Option<Layout>,
    // The first archive nodes, breadth first from the post.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<SummaryNode>,
    // Every archive node the job had, kept or not.
    #[serde(default)]
    pub node_count: u32,
    // The deepest an archive node sat: 1 for one inside the post.
    #[serde(default)]
    pub max_depth: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recovery: Vec<SummaryRecovery>,
    #[serde(default)]
    pub recovery_count: u32,
    pub post: SummaryPost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Layout {
    // Plain files only.
    Plain,
    // Every payload file is part of an archive.
    Archive,
    // Archives and plain files side by side.
    Mixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeMode {
    // Read as its volumes arrived.
    Streamed,
    // Read once its volumes were on disk.
    Extracted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SummaryNode {
    pub id: u32,
    // `None` for a node inside the post itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<u32>,
    // The engine's kind name, such as `rar_store`.
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub volumes: u32,
    pub depth: u32,
    pub mode: NodeMode,
    // It gave its volumes up, for `reason`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub given_up: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub encrypted: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub encrypted_headers: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub password_needed: bool,
    // Where the password that opened it came from, as
    // `ArchivePasswordSource::as_str` names it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SummaryRecovery {
    pub node: u32,
    // `par2`, `par3`, `sfv`, `rar_recovery` or `rar_record`.
    pub kind: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sets: Vec<SummarySet>,
    #[serde(default)]
    pub set_count: u32,
    // What its last repair came to: `clean`, `repaired`, `impossible`,
    // `waiting` or `unread`; `None` before it ran one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SummarySet {
    pub blocks_reachable: u64,
    pub damaged_slices: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    #[serde(default)]
    pub slices_repaired: u64,
    // A clean ruling that read the bytes back, rather than the evidence fed
    // as they passed.
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_back: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SummaryPost {
    pub files: u32,
    pub articles: u64,
    #[serde(default)]
    pub missing_articles: u32,
    #[serde(default)]
    pub failed_articles: u32,
    // The oldest and newest file date the NZB gave, in epoch seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_post: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newest_post: Option<u64>,
    #[serde(default)]
    pub groups: u32,
}

fn is_false(value: &bool) -> bool {
    !*value
}

// A name cut to at most `NAME_MAX` characters.
pub fn bounded_name(name: &str) -> String {
    name.chars().take(NAME_MAX).collect()
}

impl DownloadSummary {
    pub fn to_storage_json(&self) -> Option<String> {
        serde_json::to_string(self).ok()
    }

    // A stored summary, or `None` for one this build cannot read.
    pub fn from_storage_json(json: &str) -> Option<Self> {
        serde_json::from_str::<Self>(json)
            .ok()
            .filter(|summary| summary.version == DOWNLOAD_SUMMARY_VERSION)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_summary_round_trips_and_an_unknown_version_reads_as_none() {
        let summary = DownloadSummary {
            version: DOWNLOAD_SUMMARY_VERSION,
            layout: Some(Layout::Archive),
            node_count: 1,
            max_depth: 1,
            nodes: vec![SummaryNode {
                id: 1,
                parent: None,
                kind: "rar_store".into(),
                name: Some("a.rar".into()),
                volumes: 3,
                depth: 1,
                mode: NodeMode::Streamed,
                given_up: false,
                reason: None,
                encrypted: true,
                encrypted_headers: false,
                password_needed: true,
                password_source: Some("settings".into()),
            }],
            ..DownloadSummary::default()
        };
        let json = summary.to_storage_json().unwrap();
        assert_eq!(DownloadSummary::from_storage_json(&json), Some(summary));
        assert_eq!(
            DownloadSummary::from_storage_json(r#"{"v":99,"post":{"files":0,"articles":0}}"#),
            None
        );
        assert_eq!(DownloadSummary::from_storage_json("not json"), None);
    }

    #[test]
    fn a_long_name_is_cut() {
        assert_eq!(bounded_name(&"é".repeat(300)).chars().count(), NAME_MAX);
    }
}
