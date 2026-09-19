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

    /// Check for a new release now instead of waiting for the next scheduled
    /// check, and return what it found.
    ///
    /// Nothing is fetched while the release API has asked Weaver to back off,
    /// or when checks are turned off; the returned status's `lastError` says
    /// which.
    #[graphql(guard = "AdminGuard")]
    async fn check_for_updates(&self, ctx: &Context<'_>) -> Result<UpdateStatus> {
        let service = ctx.data::<weaver_server_core::update_check::UpdateCheckService>()?;
        Ok(service.check_now().await.into())
    }

    /// Install the release the checker is currently advertising.
    ///
    /// Admin-only, and the input has to echo the notice the UI displayed: the
    /// server installs the release it already found, never one the caller names.
    #[graphql(guard = "AdminGuard")]
    async fn start_application_upgrade(
        &self,
        ctx: &Context<'_>,
        input: crate::system::types::StartApplicationUpgradeInput,
    ) -> Result<crate::system::types::ApplicationUpgradeStartPayload> {
        let service =
            ctx.data::<weaver_server_core::application_upgrade::ApplicationUpgradeService>()?;
        let run = service
            .start(
                weaver_server_core::application_upgrade::ApplicationUpgradeStartRequest {
                    expected_tag: input.expected_tag,
                    expected_version: input.expected_version,
                },
            )
            .await
            .map_err(map_application_upgrade_error)?;
        Ok(crate::system::types::ApplicationUpgradeStartPayload { run: run.into() })
    }
}

/// The upgrade's own error codes, kept out of `INTERNAL`: a refusal the operator
/// can act on (an ineligible install, a stale notice) must not read as a bug.
fn map_application_upgrade_error(
    error: weaver_server_core::application_upgrade::ApplicationUpgradeError,
) -> async_graphql::Error {
    use weaver_server_core::application_upgrade::ApplicationUpgradeError;
    let message = error.to_string();
    match error {
        ApplicationUpgradeError::Validation(_) => graphql_error("INVALID_INPUT", message),
        ApplicationUpgradeError::NotFound(_) => graphql_error("NOT_FOUND", message),
        ApplicationUpgradeError::Repository(_) => graphql_error("INTERNAL", message),
    }
}

fn map_create_directory_error(error: CreateDirectoryError) -> async_graphql::Error {
    match error {
        CreateDirectoryError::InvalidInput(message) => graphql_error("INVALID_INPUT", message),
        CreateDirectoryError::Conflict(message) => graphql_error("CONFLICT", message),
        CreateDirectoryError::Internal(message) => graphql_error("INTERNAL", message),
    }
}
