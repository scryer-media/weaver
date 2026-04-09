use scryer_release_parser::{ParsedReleaseMetadata, parse_release_metadata};

use crate::ingest::metadata::original_release_title;

pub fn parse_job_release(job_name: &str, metadata: &[(String, String)]) -> ParsedReleaseMetadata {
    parse_release_metadata(&original_release_title(job_name, metadata))
}

#[cfg(test)]
mod tests;
