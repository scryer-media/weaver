// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::observability::with_timed_config_read;

#[derive(Default)]
pub(crate) struct CategoriesQuery;

#[Object]
impl CategoriesQuery {
    /// List all configured categories.
    #[graphql(guard = "AdminGuard")]
    async fn categories(&self, ctx: &Context<'_>) -> Result<Vec<Category>> {
        let config = ctx.data::<SharedConfig>()?;
        Ok(
            with_timed_config_read(config, "categories.query.categories", |cfg| {
                cfg.categories.iter().map(Category::from).collect()
            })
            .await,
        )
    }
}
