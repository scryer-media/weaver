#[cfg(test)]
use crate::history::{
    CLIENT_REQUEST_ID_ATTRIBUTE_KEY, DIAGNOSTIC_INCLUDE_SERVER_HOSTNAMES_ATTRIBUTE_KEY,
    DIAGNOSTIC_SOURCE_JOB_ATTRIBUTE_KEY, HistoryFilter, HistoryMetadataEquals, IntegrationEventRow,
    JobHistoryRow,
};
#[cfg(test)]
use crate::persistence::Database;

#[cfg(test)]
mod repository_tests;

#[cfg(test)]
mod integration_tests;
