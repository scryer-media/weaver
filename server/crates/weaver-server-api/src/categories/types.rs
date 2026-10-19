// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use async_graphql::{InputObject, MaybeUndefined, SimpleObject};

use crate::settings::types::{SeedingPolicyInput, SeedingPolicySettings};

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
}

impl CategoryInput {
    // The seeding policy and torrent route this input leaves on a category
    // that had `current`.
    pub(crate) fn torrent_fields(
        &self,
        current: Option<&weaver_server_core::categories::CategoryConfig>,
    ) -> async_graphql::Result<(Option<weaver_server_core::settings::SeedingPolicy>, bool)> {
        let seeding = match self.seeding {
            MaybeUndefined::Undefined => current.and_then(|category| category.seeding),
            MaybeUndefined::Null => None,
            MaybeUndefined::Value(policy) => Some(policy.into_policy()?),
        };
        let torrent_route = self
            .torrent_route
            .unwrap_or_else(|| current.is_some_and(|category| category.torrent_route));
        Ok((seeding, torrent_route))
    }
}
