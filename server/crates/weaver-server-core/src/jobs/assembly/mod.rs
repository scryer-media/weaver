// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod error;
pub mod file;
pub mod job;
pub mod write_buffer;

pub use error::AssemblyError;
pub use file::{CommitResult, DetectedArchiveIdentity, DetectedArchiveKind, FileAssembly};
pub use job::{
    ArchiveMember, ArchivePendingSpan, ArchiveTopology, ArchiveType, ExtractionReadiness,
    JobAssembly,
};
