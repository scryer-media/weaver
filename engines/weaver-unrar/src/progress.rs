//! Progress reporting trait for archive extraction.

use crate::error::RarError;
use crate::types::MemberInfo;

/// Trait for receiving progress updates during extraction.
pub trait ProgressHandler: Send {
    /// Called when extraction of a member begins.
    fn on_member_start(&self, member: &MemberInfo);

    /// Called periodically with the number of bytes written so far.
    fn on_member_progress(&self, member: &MemberInfo, bytes_written: u64);

    /// Called when extraction of a member finishes (success or failure).
    fn on_member_complete(&self, member: &MemberInfo, result: &Result<(), RarError>);
}

/// A no-op progress handler that discards all progress events.
pub struct NoProgress;

impl ProgressHandler for NoProgress {
    fn on_member_start(&self, _member: &MemberInfo) {}
    fn on_member_progress(&self, _member: &MemberInfo, _bytes_written: u64) {}
    fn on_member_complete(&self, _member: &MemberInfo, _result: &Result<(), RarError>) {}
}
