use async_graphql::{Context, Object, Result};
use tracing::info;

use crate::auth::AdminGuard;
use crate::categories::types::{Category, CategoryInput};
use weaver_server_core::Database;
use weaver_server_core::settings::SharedConfig;

#[derive(Default)]
pub(crate) struct CategoriesMutation;

#[Object]
impl CategoriesMutation {
    /// Add a new category.
    #[graphql(guard = "AdminGuard")]
    async fn add_category(&self, ctx: &Context<'_>, input: CategoryInput) -> Result<Category> {
        let config = ctx.data::<SharedConfig>()?;
        let db = ctx.data::<Database>()?;

        let name = input.name.trim().to_string();
        if name.is_empty() {
            return Err(async_graphql::Error::new("category name must not be empty"));
        }

        let id = {
            let db = db.clone();
            tokio::task::spawn_blocking(move || db.next_category_id())
                .await
                .map_err(|e| async_graphql::Error::new(format!("{e}")))?
                .map_err(|e| async_graphql::Error::new(format!("db error: {e}")))?
        };

        let cat = weaver_server_core::categories::CategoryConfig {
            id,
            name: name.clone(),
            dest_dir: input.dest_dir.filter(|s| !s.is_empty()),
            aliases: input.aliases,
        };

        {
            let mut cfg = config.write().await;
            if cfg
                .categories
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(&name))
            {
                return Err(async_graphql::Error::new(format!(
                    "category '{name}' already exists"
                )));
            }

            let c = cat.clone();
            let db = db.clone();
            tokio::task::spawn_blocking(move || db.insert_category(&c))
                .await
                .map_err(|e| async_graphql::Error::new(format!("{e}")))?
                .map_err(|e| async_graphql::Error::new(format!("db error: {e}")))?;

            cfg.categories.push(cat);
            info!(id, name = %name, "category added");
        }

        let cfg = config.read().await;
        let cat = cfg.categories.iter().find(|c| c.id == id).unwrap();
        Ok(Category::from(cat))
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

        let name = input.name.trim().to_string();
        if name.is_empty() {
            return Err(async_graphql::Error::new("category name must not be empty"));
        }

        {
            let mut cfg = config.write().await;
            let c = cfg
                .categories
                .iter_mut()
                .find(|c| c.id == id)
                .ok_or_else(|| async_graphql::Error::new(format!("category {id} not found")))?;

            // Uniqueness check if name changed.
            if !c.name.eq_ignore_ascii_case(&name)
                && cfg
                    .categories
                    .iter()
                    .any(|other| other.id != id && other.name.eq_ignore_ascii_case(&name))
            {
                return Err(async_graphql::Error::new(format!(
                    "category '{name}' already exists"
                )));
            }

            // Re-borrow mutably after the uniqueness check.
            let c = cfg.categories.iter_mut().find(|c| c.id == id).unwrap();
            c.name = name;
            c.dest_dir = input.dest_dir.filter(|s| !s.is_empty());
            c.aliases = input.aliases;

            let cat = c.clone();
            let db = db.clone();
            tokio::task::spawn_blocking(move || db.update_category(&cat))
                .await
                .map_err(|e| async_graphql::Error::new(format!("{e}")))?
                .map_err(|e| async_graphql::Error::new(format!("db error: {e}")))?;
            info!(id, "category updated");
        }

        let cfg = config.read().await;
        let cat = cfg.categories.iter().find(|c| c.id == id).unwrap();
        Ok(Category::from(cat))
    }
    /// Remove a category by ID.
    #[graphql(guard = "AdminGuard")]
    async fn remove_category(&self, ctx: &Context<'_>, id: u32) -> Result<Vec<Category>> {
        let config = ctx.data::<SharedConfig>()?;
        let db = ctx.data::<Database>()?;

        {
            let mut cfg = config.write().await;
            let before = cfg.categories.len();
            cfg.categories.retain(|c| c.id != id);
            if cfg.categories.len() == before {
                return Err(async_graphql::Error::new(format!(
                    "category {id} not found"
                )));
            }

            let db = db.clone();
            tokio::task::spawn_blocking(move || db.delete_category(id))
                .await
                .map_err(|e| async_graphql::Error::new(format!("{e}")))?
                .map_err(|e| async_graphql::Error::new(format!("db error: {e}")))?;
            info!(id, "category removed");
        }

        let cfg = config.read().await;
        Ok(cfg.categories.iter().map(Category::from).collect())
    }
}
