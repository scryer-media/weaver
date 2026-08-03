use super::*;

mod rar;

pub(crate) use rar::{RarArchiveOpenMode, RarExtractionContext, RarExtractionOpenRequest};

/// Re-exported so the direct-store coverage snapshot gates its destination
/// paths with the same validator RAR extraction gates member paths with, rather
/// than growing a second, subtly different one (plan 135, D6).
pub(crate) use rar::validate_sanitized_rar_member_path;

#[cfg(test)]
pub(crate) use rar::RarArchiveSnapshotOpenRequest;
