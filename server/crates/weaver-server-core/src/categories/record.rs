use crate::categories::CategoryConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CategoryRecord {
    pub id: u32,
    pub name: String,
    pub dest_dir: Option<String>,
    pub aliases: String,
}

impl CategoryRecord {
    pub(crate) fn into_config(self) -> CategoryConfig {
        CategoryConfig {
            id: self.id,
            name: self.name,
            dest_dir: self.dest_dir,
            aliases: self.aliases,
        }
    }

    pub(crate) fn from_config(category: &CategoryConfig) -> Self {
        Self {
            id: category.id,
            name: category.name.clone(),
            dest_dir: category.dest_dir.clone(),
            aliases: category.aliases.clone(),
        }
    }
}
