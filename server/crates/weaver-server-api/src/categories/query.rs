use super::*;

#[derive(Default)]
pub(crate) struct CategoriesQuery;

#[Object]
impl CategoriesQuery {
    /// List all configured categories.
    #[graphql(guard = "AdminGuard")]
    async fn categories(&self, ctx: &Context<'_>) -> Result<Vec<Category>> {
        let config = ctx.data::<SharedConfig>()?;
        let cfg = config.read().await;
        Ok(cfg.categories.iter().map(Category::from).collect())
    }
}
