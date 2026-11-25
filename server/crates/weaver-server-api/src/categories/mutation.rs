// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use async_graphql::{Context, Object, Result};
use tracing::info;
use weaver_server_core::categories::persistence::CategoriesPersistenceQueries;

use crate::auth::{AdminGuard, graphql_error};
use crate::categories::types::{Category, CategoryInput};
use crate::observability::{
    spawn_blocking_db, state_error, watch_folder_error, with_timed_config_read,
    with_timed_config_write,
};
use weaver_server_core::Database;
use weaver_server_core::categories::{CategoryConfig, Made, category_writes, make_category};
use weaver_server_core::settings::SharedConfig;
use weaver_server_core::watch_folder::WatchFolderService;

#[derive(Default)]
pub(crate) struct CategoriesMutation;

fn validate_category_name(name: &str, dest_dir: Option<&str>) -> Result<()> {
    if dest_dir.is_none_or(str::is_empty) {
        weaver_server_core::categories::validate_category_path_component(name)
            .map_err(|error| graphql_error("INVALID_INPUT", error.to_string()))?;
    }
    Ok(())
}

#[Object]
impl CategoriesMutation {
    /// Add a new category.
    #[graphql(guard = "AdminGuard")]
    async fn add_category(&self, ctx: &Context<'_>, input: CategoryInput) -> Result<Category> {
        let config = ctx.data::<SharedConfig>()?;
        let db = ctx.data::<Database>()?;
        let held = category_writes().await;

        let name = input.name.trim().to_string();
        if name.is_empty() {
            return Err(graphql_error(
                "INVALID_INPUT",
                "category name must not be empty",
            ));
        }
        let dest_dir = input.dest_dir.clone().filter(|value| !value.is_empty());
        validate_category_name(&name, dest_dir.as_deref())?;
        let fields = input.torrent_fields(None)?;

        // Made by the one owner of making a category, which a torrent add
        // that names a new one makes it through too.
        let category = CategoryConfig {
            dest_dir,
            aliases: input.aliases,
            seeding: fields.seeding,
            torrent_route: fields.torrent_route,
            unpack: fields.unpack,
            upload_cap: fields.upload_cap,
            ..CategoryConfig::named(name.clone())
        };
        let added = match make_category(&held, config, db, category)
            .await
            .map_err(state_error)?
        {
            Made::New(category) => category,
            Made::There(_) => {
                return Err(graphql_error(
                    "NAME_TAKEN",
                    format!("category '{name}' already exists"),
                ));
            }
        };
        drop(held);
        let id = added.id;
        let added = Category::from(&added);

        info!(id, name = %name, "category added");
        reconcile_watch_folder(ctx).await?;
        // A category is a torrent route's consumer.
        crate::proxies::refresh(ctx).await?;
        crate::torrents::reconcile(ctx).await?;
        Ok(added)
    }
    /// Update a category by ID.
    #[graphql(guard = "AdminGuard")]
    async fn update_category(
        &self,
        ctx: &Context<'_>,
        id: u32,
        input: CategoryInput,
    ) -> Result<Category> {
        let config = ctx.data::<SharedConfig>()?;
        let db = ctx.data::<Database>()?;
        let _held = category_writes().await;

        let name = input.name.trim().to_string();
        if name.is_empty() {
            return Err(graphql_error(
                "INVALID_INPUT",
                "category name must not be empty",
            ));
        }
        let dest_dir = input.dest_dir.clone().filter(|value| !value.is_empty());
        validate_category_name(&name, dest_dir.as_deref())?;

        let updated = with_timed_config_read(
            config,
            "categories.mutation.update_category.prepare",
            |cfg| {
                let current = cfg
                    .categories
                    .iter()
                    .find(|category| category.id == id)
                    .ok_or_else(|| {
                        graphql_error("NOT_FOUND", format!("category {id} not found"))
                    })?;

                if !current.name.eq_ignore_ascii_case(&name)
                    && cfg
                        .categories
                        .iter()
                        .any(|other| other.id != id && other.name.eq_ignore_ascii_case(&name))
                {
                    return Err(graphql_error(
                        "NAME_TAKEN",
                        format!("category '{name}' already exists"),
                    ));
                }

                let fields = input.torrent_fields(Some(current))?;
                Ok(weaver_server_core::categories::CategoryConfig {
                    id,
                    name: name.clone(),
                    dest_dir: dest_dir.clone(),
                    aliases: input.aliases.clone(),
                    seeding: fields.seeding,
                    torrent_route: fields.torrent_route,
                    unpack: fields.unpack,
                    upload_cap: fields.upload_cap,
                })
            },
        )
        .await?;

        {
            let db = db.clone();
            let persisted_category = updated.clone();
            spawn_blocking_db("categories.mutation.update_category.persist", move || {
                db.update_category(&persisted_category)
            })
            .await?;
        }

        let updated_category = with_timed_config_write(
            config,
            "categories.mutation.update_category.apply",
            move |cfg| {
                if let Some(category) = cfg.categories.iter_mut().find(|category| category.id == id)
                {
                    *category = updated.clone();
                    Category::from(&*category)
                } else {
                    cfg.categories.push(updated.clone());
                    Category::from(&updated)
                }
            },
        )
        .await;

        info!(id, "category updated");
        reconcile_watch_folder(ctx).await?;
        // A category is a torrent route's consumer.
        crate::proxies::refresh(ctx).await?;
        crate::torrents::reconcile(ctx).await?;
        Ok(updated_category)
    }
    /// Remove a category by ID.
    #[graphql(guard = "AdminGuard")]
    async fn remove_category(&self, ctx: &Context<'_>, id: u32) -> Result<Vec<Category>> {
        let config = ctx.data::<SharedConfig>()?;
        let db = ctx.data::<Database>()?;
        let _held = category_writes().await;

        with_timed_config_read(
            config,
            "categories.mutation.remove_category.validate",
            |cfg| {
                if cfg.categories.iter().any(|category| category.id == id) {
                    Ok(())
                } else {
                    Err(graphql_error(
                        "NOT_FOUND",
                        format!("category {id} not found"),
                    ))
                }
            },
        )
        .await?;

        {
            let db = db.clone();
            let deleted =
                spawn_blocking_db("categories.mutation.remove_category.persist", move || {
                    db.delete_category(id)
                })
                .await?;
            if !deleted {
                return Err(graphql_error(
                    "NOT_FOUND",
                    format!("category {id} not found"),
                ));
            }
        }

        let remaining = with_timed_config_write(
            config,
            "categories.mutation.remove_category.apply",
            move |cfg| {
                cfg.categories.retain(|category| category.id != id);
                cfg.categories.iter().map(Category::from).collect()
            },
        )
        .await;

        info!(id, "category removed");
        reconcile_watch_folder(ctx).await?;
        // A category is a torrent route's consumer.
        crate::proxies::refresh(ctx).await?;
        crate::torrents::reconcile(ctx).await?;
        Ok(remaining)
    }
}

async fn reconcile_watch_folder(ctx: &Context<'_>) -> Result<()> {
    ctx.data::<WatchFolderService>()?
        .reconcile_from_config()
        .await
        .map_err(watch_folder_error)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::{RwLock, oneshot};

    use crate::observability::{persist_then_update_config, with_timed_config_read};

    use super::*;

    fn test_config() -> SharedConfig {
        Arc::new(RwLock::new(weaver_server_core::settings::Config {
            seeding: None,
            upload_cap: None,
            torrent: Default::default(),
            data_dir: "/tmp/weaver".to_string(),
            hardware_profile: None,
            intermediate_dir: None,
            complete_dir: None,
            servers: vec![],
            categories: vec![],
            retry: None,
            bandwidth: Default::default(),
            cleanup_after_extract: None,
            delete_failed_job_files: None,
            history_retention_days: None,
            propagation_delay_secs: None,
            max_concurrent_nzbs: None,
            watch_folder: weaver_server_core::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: weaver_server_core::jobs::DuplicatePolicy::default(),
            direct_store: None,
            delivery_naming: None,
            metrics: Default::default(),
            config_path: None,
        }))
    }

    #[tokio::test]
    async fn slow_persist_does_not_block_category_reads() {
        let config = test_config();
        let (release_tx, release_rx) = oneshot::channel();

        let update_task = tokio::spawn({
            let config = config.clone();
            async move {
                persist_then_update_config(
                    &config,
                    "tests.categories.persist_then_update",
                    async move {
                        release_rx.await.expect("release signal should arrive");
                        Ok(())
                    },
                    |cfg| {
                        cfg.categories
                            .push(weaver_server_core::categories::CategoryConfig {
                                seeding: None,
                                upload_cap: Default::default(),
                                torrent_route: false,
                                unpack: None,
                                id: 1,
                                name: "tv".to_string(),
                                dest_dir: None,
                                aliases: String::new(),
                            });
                    },
                )
                .await
                .expect("category update should succeed");
            }
        });

        tokio::task::yield_now().await;

        let category_count =
            with_timed_config_read(&config, "tests.categories.read", |cfg| cfg.categories.len())
                .await;
        assert_eq!(category_count, 0);

        release_tx
            .send(())
            .expect("update task should still be waiting");
        update_task
            .await
            .expect("update task should finish cleanly");
    }
}
