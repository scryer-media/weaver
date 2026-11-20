// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod download_summary;
pub mod download_summary_persistence;
pub mod nzb_persistence;
pub mod outline_persistence;
pub mod phase_progress;
pub mod queries;
pub mod server_attribution;
pub mod support_facts;
pub mod support_facts_persistence;
pub use weaver_server_base::jobs::ids;
pub mod duplicate;
pub mod model;
pub use ids::JobId;
pub use model::{ArchivePasswordCandidate, ArchivePasswordSource};
pub use model::{FileSpec, JobSpec, MessageIds, SegmentSpec};
