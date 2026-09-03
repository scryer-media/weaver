use super::*;

mod rar;
mod safety;

pub(crate) use rar::{RarArchiveOpenMode, RarExtractionContext, RarExtractionOpenRequest};
pub(crate) use safety::{
    BudgetedReader, BudgetedWriter, ExtractionLimits, ExtractionRoot, JobExtractionBudget,
    ProcessMemoryBudget,
};

/// Re-exported so the direct-store coverage snapshot gates its destination
/// paths with the same validator RAR extraction gates member paths with, rather
/// than growing a second, subtly different one.
pub(crate) use rar::validate_sanitized_rar_member_path;

/// Same reasoning for the decode ceilings: direct-store's small-member
/// tolerance opens an archive of its own to extract the tolerated members, and
/// it must open it under the operator's configured limits rather than the
/// library defaults.
pub(crate) use rar::{
    apply_server_rar_limits_with_memory_limit, ensure_rar_dictionary_within_limit,
    rar_decoder_memory_bytes,
};

#[cfg(test)]
pub(crate) use rar::RarArchiveSnapshotOpenRequest;
