// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job page's download summary, as the history row keeps it.

use async_graphql::{Enum, SimpleObject};
use weaver_server_core::jobs::ArchivePasswordSource;
use weaver_server_core::jobs::download_summary as stored;

#[derive(Enum, Copy, Clone, Debug, Eq, PartialEq)]
pub enum DownloadLayout {
    /// Plain files only.
    Plain,
    /// Every payload file is part of an archive.
    Archive,
    /// Archives and plain files side by side.
    Mixed,
}

/// Where the password that opened an archive came from. Never the password.
#[derive(Enum, Copy, Clone, Debug, Eq, PartialEq)]
pub enum ArchivePasswordOrigin {
    Explicit,
    Integration,
    DnzbHeader,
    NzbMeta,
    FilenameConvention,
    Settings,
    PasswordFile,
}

impl ArchivePasswordOrigin {
    fn named(name: &str) -> Option<Self> {
        [
            (ArchivePasswordSource::Explicit, Self::Explicit),
            (ArchivePasswordSource::Integration, Self::Integration),
            (ArchivePasswordSource::DnzbHeader, Self::DnzbHeader),
            (ArchivePasswordSource::NzbMeta, Self::NzbMeta),
            (
                ArchivePasswordSource::FilenameConvention,
                Self::FilenameConvention,
            ),
            (ArchivePasswordSource::Settings, Self::Settings),
            (ArchivePasswordSource::PasswordFile, Self::PasswordFile),
        ]
        .into_iter()
        .find(|(source, _)| source.as_str() == name)
        .map(|(_, origin)| origin)
    }
}

/// How a finished job's download went. Built once as the job finished.
#[derive(SimpleObject, Clone, Debug)]
pub struct DownloadSummary {
    /// Null when the archive graph did not run for the job.
    pub layout: Option<DownloadLayout>,
    /// The first archive nodes, breadth first from the post: at most 200.
    pub nodes: Vec<DownloadNode>,
    /// Every archive node the job had, however many `nodes` holds.
    pub node_count: u32,
    /// The deepest an archive sat: 1 for one inside the post.
    pub max_depth: u32,
    pub recovery: Vec<DownloadRecovery>,
    pub recovery_count: u32,
    pub post: DownloadPost,
}

#[derive(SimpleObject, Clone, Debug)]
pub struct DownloadNode {
    pub id: u32,
    /// Null for an archive inside the post itself.
    pub parent_id: Option<u32>,
    /// The container's kind, such as `rar_store` or `sevenz_engine`.
    pub kind: String,
    pub name: Option<String>,
    pub volumes: u32,
    pub depth: u32,
    /// Read once its volumes were on disk, rather than as they arrived.
    pub extracted: bool,
    pub given_up: bool,
    /// Why it gave up, such as `encrypted` or `compressed`.
    pub reason: Option<String>,
    pub encrypted: bool,
    pub encrypted_headers: bool,
    pub password_needed: bool,
    pub password_origin: Option<ArchivePasswordOrigin>,
}

#[derive(SimpleObject, Clone, Debug)]
pub struct DownloadRecovery {
    pub node_id: u32,
    /// `par2`, `par3`, `sfv`, `rar_recovery` or `rar_record`.
    pub kind: String,
    /// `clean`, `repaired`, `impossible`, `waiting` or `unread`.
    pub outcome: Option<String>,
    pub sets: Vec<DownloadRecoverySet>,
    pub set_count: u32,
}

#[derive(SimpleObject, Clone, Debug)]
pub struct DownloadRecoverySet {
    pub blocks_reachable: u64,
    pub damaged_slices: u64,
    pub outcome: Option<String>,
    pub slices_repaired: u64,
    pub read_back: bool,
}

#[derive(SimpleObject, Clone, Debug)]
pub struct DownloadPost {
    pub files: u32,
    pub articles: u64,
    pub missing_articles: u32,
    pub failed_articles: u32,
    /// The oldest file date in the NZB, in epoch seconds.
    pub oldest_post: Option<u64>,
    pub newest_post: Option<u64>,
    pub groups: u32,
}

impl From<stored::DownloadSummary> for DownloadSummary {
    fn from(summary: stored::DownloadSummary) -> Self {
        Self {
            layout: summary.layout.map(|layout| match layout {
                stored::Layout::Plain => DownloadLayout::Plain,
                stored::Layout::Archive => DownloadLayout::Archive,
                stored::Layout::Mixed => DownloadLayout::Mixed,
            }),
            nodes: summary
                .nodes
                .into_iter()
                .map(|node| DownloadNode {
                    id: node.id,
                    parent_id: node.parent,
                    kind: node.kind,
                    name: node.name,
                    volumes: node.volumes,
                    depth: node.depth,
                    extracted: node.mode == stored::NodeMode::Extracted,
                    given_up: node.given_up,
                    reason: node.reason,
                    encrypted: node.encrypted,
                    encrypted_headers: node.encrypted_headers,
                    password_needed: node.password_needed,
                    password_origin: node
                        .password_source
                        .as_deref()
                        .and_then(ArchivePasswordOrigin::named),
                })
                .collect(),
            node_count: summary.node_count,
            max_depth: summary.max_depth,
            recovery: summary
                .recovery
                .into_iter()
                .map(|object| DownloadRecovery {
                    node_id: object.node,
                    kind: object.kind,
                    outcome: object.outcome,
                    sets: object
                        .sets
                        .into_iter()
                        .map(|set| DownloadRecoverySet {
                            blocks_reachable: set.blocks_reachable,
                            damaged_slices: set.damaged_slices,
                            outcome: set.outcome,
                            slices_repaired: set.slices_repaired,
                            read_back: set.read_back,
                        })
                        .collect(),
                    set_count: object.set_count,
                })
                .collect(),
            recovery_count: summary.recovery_count,
            post: DownloadPost {
                files: summary.post.files,
                articles: summary.post.articles,
                missing_articles: summary.post.missing_articles,
                failed_articles: summary.post.failed_articles,
                oldest_post: summary.post.oldest_post,
                newest_post: summary.post.newest_post,
                groups: summary.post.groups,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_password_source_maps_to_an_origin_and_nothing_else_does() {
        for source in [
            ArchivePasswordSource::Explicit,
            ArchivePasswordSource::Integration,
            ArchivePasswordSource::DnzbHeader,
            ArchivePasswordSource::NzbMeta,
            ArchivePasswordSource::FilenameConvention,
            ArchivePasswordSource::Settings,
            ArchivePasswordSource::PasswordFile,
        ] {
            assert!(
                ArchivePasswordOrigin::named(source.as_str()).is_some(),
                "{source:?}"
            );
        }
        assert_eq!(ArchivePasswordOrigin::named("hunter2"), None);
    }
}
