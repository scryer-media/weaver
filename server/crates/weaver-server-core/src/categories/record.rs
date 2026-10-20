// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::categories::CategoryConfig;
use crate::persistence::sql_runtime::{SqlArg, SqlRow};
use crate::settings::SeedingPolicy;

pub(crate) const CATEGORY_COLUMNS: &str =
    "id, name, dest_dir, aliases, seeding, torrent_route, unpack";

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CategoryRecord {
    pub id: u32,
    pub name: String,
    pub dest_dir: Option<String>,
    pub aliases: String,
    pub seeding: Option<SeedingPolicy>,
    pub torrent_route: bool,
    pub unpack: Option<bool>,
}

impl CategoryRecord {
    pub(crate) fn into_config(self) -> CategoryConfig {
        CategoryConfig {
            id: self.id,
            name: self.name,
            dest_dir: self.dest_dir,
            aliases: self.aliases,
            seeding: self.seeding,
            torrent_route: self.torrent_route,
            unpack: self.unpack,
        }
    }

    pub(crate) fn from_config(category: &CategoryConfig) -> Self {
        Self {
            id: category.id,
            name: category.name.clone(),
            dest_dir: category.dest_dir.clone(),
            aliases: category.aliases.clone(),
            seeding: category.seeding,
            torrent_route: category.torrent_route,
            unpack: category.unpack,
        }
    }

    pub(crate) fn from_row(row: &SqlRow) -> Result<Self, StateError> {
        let seeding = row
            .opt_text("seeding")?
            .map(|json| {
                serde_json::from_str(&json).map_err(|error| {
                    StateError::Database(format!("category seeding policy is not valid: {error}"))
                })
            })
            .transpose()?;
        Ok(Self {
            id: row.i32("id")? as u32,
            name: row.text("name")?,
            dest_dir: row.opt_text("dest_dir")?,
            aliases: row.text("aliases")?,
            seeding,
            torrent_route: row.bool("torrent_route")?,
            unpack: row.opt_bool("unpack")?,
        })
    }

    // Bind order matches `CATEGORY_COLUMNS`.
    pub(crate) fn into_args(self) -> Result<Vec<SqlArg>, StateError> {
        let seeding = self
            .seeding
            .map(|policy| serde_json::to_string(&policy))
            .transpose()
            .map_err(|error| StateError::Database(error.to_string()))?;
        Ok(vec![
            SqlArg::I64(i64::from(self.id)),
            SqlArg::Text(self.name),
            SqlArg::OptText(self.dest_dir),
            SqlArg::Text(self.aliases),
            SqlArg::OptText(seeding),
            SqlArg::Bool(self.torrent_route),
            SqlArg::OptBool(self.unpack),
        ])
    }
}
