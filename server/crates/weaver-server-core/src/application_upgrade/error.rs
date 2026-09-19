//! The boundary between Weaver's upgrade errors and the shared upgrade core.
//!
//! Weaver's own [`crate::error::Error`] covers configuration and I/O, which is
//! not the shape an upgrade fails in: the three outcomes that matter to the API
//! are "the request was wrong", "the machine was wrong", and "there is nothing
//! there". Those are exactly the shared core's non-host variants, so this type
//! maps onto them one-for-one and a host error that travels through an injected
//! seam comes back as the very same variant and message it left as.

/// Why an in-application upgrade could not proceed.
#[derive(Debug, thiserror::Error)]
pub enum ApplicationUpgradeError {
    /// The request, the manifest or the installation is not acceptable. Maps to
    /// a client error on the API surface.
    #[error("{0}")]
    Validation(String),
    /// The machine, the network or the filesystem let us down.
    #[error("{0}")]
    Repository(String),
    /// A release asset or a run that should exist does not.
    #[error("{0}")]
    NotFound(String),
}

pub type ApplicationUpgradeResult<T> = Result<T, ApplicationUpgradeError>;

impl ApplicationUpgradeError {
    /// The stable snake_case code the API reports alongside the message.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Validation(_) => "validation",
            Self::Repository(_) => "repository",
            Self::NotFound(_) => "not_found",
        }
    }
}

/// Wrap an upgrade error so it can travel through the shared core unchanged.
pub fn to_updater_error(error: ApplicationUpgradeError) -> application_updater::Error {
    application_updater::Error::host(error)
}

/// Map a shared-core error onto the upgrade error it is reported as.
pub fn map_updater_error(error: application_updater::Error) -> ApplicationUpgradeError {
    match error {
        application_updater::Error::Validation(message) => {
            ApplicationUpgradeError::Validation(message)
        }
        application_updater::Error::Repository(message) => {
            ApplicationUpgradeError::Repository(message)
        }
        application_updater::Error::NotFound(message) => ApplicationUpgradeError::NotFound(message),
        application_updater::Error::Host(host) => {
            match host.downcast::<ApplicationUpgradeError>() {
                Ok(error) => *error,
                Err(error) => ApplicationUpgradeError::Repository(error.to_string()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_errors_round_trip_through_the_shared_core_unchanged() {
        for error in [
            ApplicationUpgradeError::Validation("nope".to_string()),
            ApplicationUpgradeError::Repository("disk".to_string()),
            ApplicationUpgradeError::NotFound("gone".to_string()),
        ] {
            let expected = error.to_string();
            let expected_code = error.code();
            let round_tripped = map_updater_error(to_updater_error(error));
            assert_eq!(round_tripped.to_string(), expected);
            assert_eq!(round_tripped.code(), expected_code);
        }
    }

    #[test]
    fn core_errors_map_onto_the_same_upgrade_variants() {
        assert!(matches!(
            map_updater_error(application_updater::Error::Validation("v".to_string())),
            ApplicationUpgradeError::Validation(message) if message == "v"
        ));
        assert!(matches!(
            map_updater_error(application_updater::Error::Repository("r".to_string())),
            ApplicationUpgradeError::Repository(message) if message == "r"
        ));
        assert!(matches!(
            map_updater_error(application_updater::Error::NotFound("n".to_string())),
            ApplicationUpgradeError::NotFound(message) if message == "n"
        ));
    }
}
