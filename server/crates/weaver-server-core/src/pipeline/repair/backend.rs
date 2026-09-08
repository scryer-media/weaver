//! Operation boundary for native repair engines, above their block I/O paths.
//!
//! Each backend keeps its own evidence, assessment, recovery geometry, errors,
//! and installation request. The coordinator must choose a concrete backend
//! before running blocking work; this contract adds no virtual calls to reads
//! and does not translate checksum proofs between formats.

use std::path::Path;

pub(in crate::pipeline) trait RepairBackend {
    type Assessment<'a>
    where
        Self: 'a;
    type Error: std::error::Error + Send + Sync + 'static;
    type Request<'a>;
    type Report;
    type SourceChange;

    fn assess(&mut self) -> Result<Self::Assessment<'_>, Self::Error>;
    fn execute(&mut self, request: Self::Request<'_>) -> Result<Self::Report, Self::Error>;
    fn invalidate(&mut self, change: Self::SourceChange);
    fn retained_bytes(&self) -> usize;
}

impl RepairBackend for par2_rs::Par2RepairSession {
    type Assessment<'a> = par2_rs::Par2RepairOutcome;
    type Error = par2_rs::Par2SessionError;
    // PAR2 owns its existing destination and backup policy in session options.
    type Request<'a> = ();
    type Report = par2_rs::Par2RepairOutcome;
    // Preserve Weaver's current full invalidation after writes and rebindings.
    type SourceChange = ();

    #[inline]
    fn assess(&mut self) -> Result<Self::Assessment<'_>, Self::Error> {
        self.analyze()
    }

    #[inline]
    fn execute(&mut self, (): ()) -> Result<Self::Report, Self::Error> {
        self.repair()
    }

    #[inline]
    fn invalidate(&mut self, (): ()) {
        self.invalidate_all_sources();
    }

    #[inline]
    fn retained_bytes(&self) -> usize {
        self.estimated_retained_bytes()
    }
}

pub(in crate::pipeline) struct Par3RepairRequest<'a> {
    pub output: &'a Path,
    pub backup: bool,
}

impl RepairBackend for par3_rs::Par3RepairSession {
    // Borrow the retained assessment: cloning its extent maps would allocate
    // outside the engine's accounting and repeat work on recovery-only arrivals.
    type Assessment<'a> = &'a par3_rs::session::RepairAssessment;
    type Error = par3_rs::runtime::EngineError;
    type Request<'a> = Par3RepairRequest<'a>;
    type Report = par3_rs::session_repair::SessionRepairReport;
    type SourceChange = par3_rs::source::SourceId;

    #[inline]
    fn assess(&mut self) -> Result<Self::Assessment<'_>, Self::Error> {
        par3_rs::Par3RepairSession::assess(self)
    }

    #[inline]
    fn execute(&mut self, request: Self::Request<'_>) -> Result<Self::Report, Self::Error> {
        self.repair(request.output, request.backup)
    }

    #[inline]
    fn invalidate(&mut self, source: Self::SourceChange) {
        self.invalidate_source(source);
    }

    #[inline]
    fn retained_bytes(&self) -> usize {
        par3_rs::Par3RepairSession::retained_bytes(self)
    }
}

#[cfg(test)]
mod tests;
