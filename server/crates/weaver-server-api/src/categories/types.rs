// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use async_graphql::{InputObject, MaybeUndefined, SimpleObject};

use crate::settings::types::{LeafGql, LeafInput, SeedingPolicyInput, SeedingPolicySettings};

#[derive(Debug, Clone, SimpleObject)]
pub struct Category {
    pub id: u32,
    pub name: String,
    pub dest_dir: Option<String>,
    pub aliases: String,
    // This category's own seeding policy; its unset goals fall through to the
    // global policy.
    pub seeding: Option<SeedingPolicySettings>,
    pub torrent_route: bool,
    // Whether the category's torrents have their archives extracted; absent
    // inherits.
    pub unpack: Option<bool>,
    /// The upload cap its torrents take in place of the global one.
    pub upload_cap: LeafGql<u64>,
}

impl From<&weaver_server_core::categories::CategoryConfig> for Category {
    fn from(c: &weaver_server_core::categories::CategoryConfig) -> Self {
        Self {
            id: c.id,
            name: c.name.clone(),
            dest_dir: c.dest_dir.clone(),
            aliases: c.aliases.clone(),
            seeding: c.seeding.map(Into::into),
            torrent_route: c.torrent_route,
            unpack: c.unpack,
            upload_cap: LeafGql::of(c.upload_cap),
        }
    }
}

#[derive(Debug, InputObject)]
pub struct CategoryInput {
    pub name: String,
    pub dest_dir: Option<String>,
    #[graphql(default)]
    pub aliases: String,
    // Left out keeps the category's policy; null removes it.
    pub seeding: MaybeUndefined<SeedingPolicyInput>,
    pub torrent_route: Option<bool>,
    // Left out keeps the category's value; null makes it inherit.
    pub unpack: MaybeUndefined<bool>,
    /// Left out keeps the category's cap.
    pub upload_cap: Option<LeafInput<u64>>,
}

/// The torrent settings a category input leaves on its category.
pub(crate) struct CategoryTorrentFields {
    pub seeding: Option<weaver_server_core::settings::SeedingPolicy>,
    pub torrent_route: bool,
    pub unpack: Option<bool>,
    pub upload_cap: weaver_server_core::settings::Leaf<u64>,
}

impl CategoryInput {
    // The torrent settings this input leaves on a category that had
    // `current`.
    pub(crate) fn torrent_fields(
        &self,
        current: Option<&weaver_server_core::categories::CategoryConfig>,
    ) -> async_graphql::Result<CategoryTorrentFields> {
        let seeding = match self.seeding {
            MaybeUndefined::Undefined => current.and_then(|category| category.seeding),
            MaybeUndefined::Null => None,
            MaybeUndefined::Value(policy) => {
                weaver_server_core::settings::SeedingPolicy::kept(Some(policy.into_policy()?))
            }
        };
        let torrent_route = self
            .torrent_route
            .unwrap_or_else(|| current.is_some_and(|category| category.torrent_route));
        let unpack = match self.unpack {
            MaybeUndefined::Undefined => current.and_then(|category| category.unpack),
            MaybeUndefined::Null => None,
            MaybeUndefined::Value(unpack) => Some(unpack),
        };
        let upload_cap = match self.upload_cap {
            None => current.map_or_else(Default::default, |category| category.upload_cap),
            Some(cap) => cap.into_leaf()?,
        };
        Ok(CategoryTorrentFields {
            seeding,
            torrent_route,
            unpack,
            upload_cap,
        })
    }
}
