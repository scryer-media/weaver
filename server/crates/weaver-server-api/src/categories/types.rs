use async_graphql::{InputObject, SimpleObject};

#[derive(Debug, Clone, SimpleObject)]
pub struct Category {
    pub id: u32,
    pub name: String,
    pub dest_dir: Option<String>,
    pub aliases: String,
}

impl From<&weaver_server_core::categories::CategoryConfig> for Category {
    fn from(c: &weaver_server_core::categories::CategoryConfig) -> Self {
        Self {
            id: c.id,
            name: c.name.clone(),
            dest_dir: c.dest_dir.clone(),
            aliases: c.aliases.clone(),
        }
    }
}

#[derive(Debug, InputObject)]
pub struct CategoryInput {
    pub name: String,
    pub dest_dir: Option<String>,
    #[graphql(default)]
    pub aliases: String,
}
