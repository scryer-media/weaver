use super::*;
use weaver_server_core::operations::CreateDirectoryError;

#[derive(Default)]
pub(crate) struct SystemMutation;

#[Object]
impl SystemMutation {
    #[graphql(guard = "AdminGuard")]
    async fn create_directory(
        &self,
        _ctx: &Context<'_>,
        path: String,
        name: String,
    ) -> Result<DirectoryBrowseResult> {
        let requested_path = path.trim().to_string();
        if requested_path.is_empty() {
            return Err(graphql_error("INVALID_INPUT", "path must not be empty"));
        }

        let listing = tokio::task::spawn_blocking(move || {
            weaver_server_core::operations::create_directory(Path::new(&requested_path), &name)
        })
        .await
        .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
        .map_err(map_create_directory_error)?;

        Ok(listing.into())
    }
}

fn map_create_directory_error(error: CreateDirectoryError) -> async_graphql::Error {
    match error {
        CreateDirectoryError::InvalidInput(message) => graphql_error("INVALID_INPUT", message),
        CreateDirectoryError::Conflict(message) => graphql_error("CONFLICT", message),
        CreateDirectoryError::Internal(message) => graphql_error("INTERNAL", message),
    }
}
