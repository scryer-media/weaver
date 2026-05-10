use async_graphql::{Context, Object, Result};
use std::sync::LazyLock;
use tracing::info;

use crate::auth::AdminGuard;
use crate::categories::types::{Category, CategoryInput};
use crate::observability::{
    persist_then_update_config, spawn_blocking_db, with_timed_config_read, with_timed_config_write,
};
use weaver_server_core::Database;
use weaver_server_core::settings::SharedConfig;

static CATEGORY_MUTATION_GUARD: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

#[derive(Default)]
pub(crate) struct CategoriesMutation;

#[Object]
impl CategoriesMutation {
    /// Add a new category.
    #[graphql(guard = "AdminGuard")]
    async fn add_category(&self, ctx: &Context<'_>, input: CategoryInput) -> Result<Category> {
        let config = ctx.data::<SharedConfig>()?;
        let db = ctx.data::<Database>()?;
        let _mutation_guard = CATEGORY_MUTATION_GUARD.lock().await;

        let name = input.name.trim().to_string();
        if name.is_empty() {
            return Err(async_graphql::Error::new("category name must not be empty"));
        }

        with_timed_config_read(config, "categories.mutation.add_category.validate", |cfg| {
            if cfg
                .categories
                .iter()
                .any(|category| category.name.eq_ignore_ascii_case(&name))
            {
                Err(async_graphql::Error::new(format!(
                    "category '{name}' already exists"
                )))
            } else {
                Ok(())
            }
        })
        .await?;

        let id = {
            let db = db.clone();
            spawn_blocking_db("categories.mutation.add_category.next_id", move || {
                db.next_category_id()
            })
            .await?
        };

        let cat = weaver_server_core::categories::CategoryConfig {
            id,
            name: name.clone(),
            dest_dir: input.dest_dir.filter(|s| !s.is_empty()),
            aliases: input.aliases,
        };

        let added = {
            let persisted_category = cat.clone();
            let db = db.clone();
            let persist = async move {
                spawn_blocking_db("categories.mutation.add_category.persist", move || {
                    db.insert_category(&persisted_category)
                })
                .await
            };
            persist_then_update_config(
                config,
                "categories.mutation.add_category.apply",
                persist,
                move |cfg| {
                    if let Some(existing) = cfg
                        .categories
                        .iter_mut()
                        .find(|category| category.id == cat.id)
                    {
                        *existing = cat.clone();
                        Category::from(&*existing)
                    } else {
                        cfg.categories.push(cat.clone());
                        Category::from(&cat)
                    }
                },
            )
            .await?
        };

        info!(id, name = %name, "category added");
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
        let _mutation_guard = CATEGORY_MUTATION_GUARD.lock().await;

        let name = input.name.trim().to_string();
        if name.is_empty() {
            return Err(async_graphql::Error::new("category name must not be empty"));
        }

        let updated = with_timed_config_read(
            config,
            "categories.mutation.update_category.prepare",
            |cfg| {
                let current = cfg
                    .categories
                    .iter()
                    .find(|category| category.id == id)
                    .ok_or_else(|| async_graphql::Error::new(format!("category {id} not found")))?;

                if !current.name.eq_ignore_ascii_case(&name)
                    && cfg
                        .categories
                        .iter()
                        .any(|other| other.id != id && other.name.eq_ignore_ascii_case(&name))
                {
                    return Err(async_graphql::Error::new(format!(
                        "category '{name}' already exists"
                    )));
                }

                Ok(weaver_server_core::categories::CategoryConfig {
                    id,
                    name: name.clone(),
                    dest_dir: input.dest_dir.clone().filter(|value| !value.is_empty()),
                    aliases: input.aliases.clone(),
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
        Ok(updated_category)
    }
    /// Remove a category by ID.
    #[graphql(guard = "AdminGuard")]
    async fn remove_category(&self, ctx: &Context<'_>, id: u32) -> Result<Vec<Category>> {
        let config = ctx.data::<SharedConfig>()?;
        let db = ctx.data::<Database>()?;
        let _mutation_guard = CATEGORY_MUTATION_GUARD.lock().await;

        with_timed_config_read(
            config,
            "categories.mutation.remove_category.validate",
            |cfg| {
                if cfg.categories.iter().any(|category| category.id == id) {
                    Ok(())
                } else {
                    Err(async_graphql::Error::new(format!(
                        "category {id} not found"
                    )))
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
                return Err(async_graphql::Error::new(format!(
                    "category {id} not found"
                )));
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
        Ok(remaining)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::{RwLock, oneshot};
    use tokio::time::timeout;

    use crate::observability::{persist_then_update_config, with_timed_config_read};

    use super::*;

    fn test_config() -> SharedConfig {
        Arc::new(RwLock::new(weaver_server_core::settings::Config {
            data_dir: "/tmp/weaver".to_string(),
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            tuner: None,
            servers: vec![],
            categories: vec![],
            retry: None,
            max_download_speed: None,
            cleanup_after_extract: None,
            isp_bandwidth_cap: None,
            diagnostic_upload_url: None,
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

        let category_count = timeout(
            Duration::from_millis(50),
            with_timed_config_read(&config, "tests.categories.read", |cfg| cfg.categories.len()),
        )
        .await
        .expect("category read should not block on slow persist");
        assert_eq!(category_count, 0);

        release_tx
            .send(())
            .expect("update task should still be waiting");
        update_task
            .await
            .expect("update task should finish cleanly");
    }
}
