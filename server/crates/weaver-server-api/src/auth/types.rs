use async_graphql::{Enum, SimpleObject};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum ApiKeyScope {
    Read,
    Control,
    Admin,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ApiKey {
    pub id: i64,
    pub name: String,
    pub scope: ApiKeyScope,
    pub created_at: f64,
    pub last_used_at: Option<f64>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct CreateApiKeyResult {
    pub key: ApiKey,
    pub raw_key: String,
}

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct LoginStatusResult {
    pub enabled: bool,
    pub username: Option<String>,
}
