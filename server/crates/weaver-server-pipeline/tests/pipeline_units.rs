// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Unit tests of the pipeline's own modules, compiled apart from the lib so
// the lib's test build stays small. Each module below stands in for the lib
// module of the same path: it sees what its tests used of that module, and
// its tests keep their names.

use weaver_server_composition::*;
use weaver_server_pipeline::Pipeline;

mod jobs {
    pub(crate) use weaver_server_pipeline::jobs::*;
    pub(crate) mod handle {

        use weaver_server_base::jobs::ids::JobId;
        use weaver_server_pipeline::jobs::error::SchedulerError;

        use weaver_server_composition::jobs::control::*;
        pub(crate) use weaver_server_pipeline::jobs::handle::*;

        mod splice_job_order_tests;
    }
    mod service {

        use std::collections::HashMap;
        use std::path::PathBuf;

        use weaver_server_pipeline::history::timeline::JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER;

        use weaver_server_pipeline::pipeline::Pipeline;

        use weaver_server_pipeline::jobs::service::*;

        mod tests;
    }
    pub(crate) mod class {
        use crate::ingest::Kind;
        use std::collections::BTreeMap;
        use std::sync::Arc;
        use weaver_server_pipeline::jobs::error::SchedulerError;

        use weaver_server_pipeline::jobs::handle::QueueAction;
        use weaver_server_pipeline::jobs::model::Protocol;

        use weaver_server_base::jobs::ids::JobId;

        pub(crate) use weaver_server_pipeline::jobs::class::*;

        mod tests;
    }
}

mod bandwidth {
    pub(crate) use weaver_server_pipeline::bandwidth::*;
    mod service {

        use weaver_server_pipeline::bandwidth::service::*;
        use weaver_server_pipeline::jobs::HoldOrigin;

        mod tests;
    }
}

mod torrents {
    pub(crate) use weaver_server_pipeline::torrents::*;
    mod availability {
        use crate::SchedulerError;
        use weaver_server_pipeline::jobs::JobId;
        use weaver_server_pipeline::jobs::class::{RefusalReason, Submission};
        use weaver_server_pipeline::jobs::handle::{QueueAction, SchedulerHandle};
        use weaver_server_pipeline::jobs::model::Protocol;

        use std::sync::Arc;
        use weaver_server_pipeline::torrents::availability::*;

        mod tests;
    }
}

mod history {

    pub(crate) mod timeline {
        pub(crate) use weaver_server_composition::history::timeline::*;

        mod event_window_tests;
    }
}

mod settings {
    pub(crate) use weaver_server_pipeline::settings::*;
    mod archive_passwords {
        use weaver_server_pipeline::settings::archive_passwords::*;

        mod tests;
    }
}

mod pipeline {
    pub(crate) use weaver_server_pipeline::pipeline::*;
    pub(crate) mod post_health {

        use weaver_nntp::client::ProbeBatchResult;

        use weaver_server_base::jobs::ids::JobId;
        pub(crate) use weaver_server_pipeline::pipeline::units::post_health::*;

        mod given_up {

            use weaver_server_base::jobs::ids::SegmentId;

            use weaver_server_pipeline::pipeline::SegmentTerminalState;
            use weaver_server_pipeline::pipeline::units::given_up::*;

            mod tests;
        }

        mod tests;
    }
    mod extraction {
        mod error {
            use std::io;
            use weaver_server_pipeline::pipeline::units::extraction_error::*;

            mod adapters {
                use std::io;
                use weaver_server_pipeline::pipeline::units::adapters::*;
                use weaver_server_pipeline::pipeline::units::extraction_error::ExtractionErrorClass;

                mod tests;
            }

            mod tests;
        }
    }
    mod held {
        use std::path::{Path, PathBuf};
        use weaver_server_base::jobs::ids::JobId;

        use weaver_server_pipeline::jobs::graph_checkpoint::HeldPartRow;
        use weaver_server_pipeline::pipeline::units::held::*;

        mod tests;
    }
    mod diagnostics {

        use weaver_server_pipeline::pipeline::diagnostics::*;

        mod tests;
    }
    mod download {

        pub(crate) mod worker {
            pub(crate) use weaver_server_pipeline::pipeline::download::units::queued_files_may_pass;

            use std::collections::HashMap;
            use std::time::{Duration, Instant};
            use weaver_server_base::jobs::ids::JobId;
            use weaver_server_pipeline::pipeline::Pipeline;
            use weaver_server_pipeline::pipeline::download::units::worker::*;

            mod job_log_throttle_tests;
            mod warm_seat_tests;

            mod volume_pace {
                use crate::runtime::HardwareProfile;

                use weaver_server_disk::operations::disk::volume_buffers::VolumeLimits;
                use weaver_server_pipeline::pipeline::download::units::volume_pace::*;

                mod tests;
            }
        }
        mod owned_lane {
            use std::collections::VecDeque;
            use std::sync::atomic::{AtomicUsize, Ordering};
            use std::sync::{Arc, mpsc as std_mpsc};
            use std::time::{Duration, Instant};

            use tokio::sync::{mpsc, oneshot};
            use weaver_server_base::jobs::ids::{JobId, MessageId, NzbFileId, SegmentId};
            use weaver_server_pipeline::pipeline::download::units::owned_lane::*;
            use weaver_server_pipeline::pipeline::download::units::transport::{
                DownloadLaneMode, LaneParkReason,
            };
            use weaver_server_pipeline::pipeline::download::*;
            use weaver_server_pipeline::pipeline::*;

            mod support;
            use support::*;

            mod fault_tests;
            mod route_press_tests;
            mod routing_tests;
            mod tests;
        }
        mod transport {
            use std::time::{Duration, Instant};

            use weaver_server_composition::servers::latency_band::LatencyBand;
            pub(crate) use weaver_server_pipeline::pipeline::download::units::transport::*;

            mod tests;
        }
        mod queue {
            use std::sync::Arc;

            use weaver_server_pipeline::pipeline::download::queue::*;

            mod reference;
            mod tests;
        }
    }
    mod decode {
        use weaver_server_base::jobs::ids::JobId;

        use weaver_server_pipeline::pipeline::units::decode::*;

        mod active_decodes_tests;

        mod layout {
            use weaver_server_pipeline::pipeline::units::layout::*;
            use weaver_server_pipeline::pipeline::*;

            mod compatibility_tests;
            mod tests;
        }
    }
    mod orchestrator {
        mod control {

            use std::collections::HashMap;
            use weaver_server_base::jobs::ids::JobId;
            use weaver_server_pipeline::pipeline::units::control::*;

            mod position_tests;
        }
    }
    pub(crate) mod queue {
        pub(crate) use weaver_server_pipeline::pipeline::queue::*;

        use weaver_server_pipeline::jobs::JobId;
        use weaver_server_pipeline::jobs::handle::JobInfo;

        mod label_tests;

        mod admission {
            use std::path::Path;

            use crate::{JobId, JobSpec};

            use weaver_server_pipeline::jobs::working_dir::staging_dir_for;
            use weaver_server_pipeline::pipeline::queue::units::admission::*;

            mod lean_figure_tests;
            mod staging_tests;
        }
        mod order {

            use weaver_server_pipeline::jobs::JobId;
            use weaver_server_pipeline::pipeline::units::order::*;

            mod tests;
        }
        mod paging {
            use std::sync::Arc;

            use weaver_server_pipeline::jobs::JobId;
            use weaver_server_pipeline::jobs::handle::JobInfo;
            use weaver_server_pipeline::jobs::model::{DownloadState, JobStatus, RunState};
            use weaver_server_pipeline::jobs::phase_progress::JobPhaseProgress;
            use weaver_server_pipeline::pipeline::queue::paging::*;
            use weaver_server_pipeline::pipeline::queue::*;

            mod tests;
        }
    }
    mod completion {
        mod finalize {
            mod check {

                use weaver_server_pipeline::pipeline::units::check::*;

                mod tests;
            }
            mod deobfuscate {
                use std::collections::HashMap;
                use std::path::Path;

                use weaver_nzb::delivery_rename::DeliveredFile;

                use crate::ingest::FileRename;
                use crate::jobs::working_dir::OUTPUT_DIR_MARKER;
                use crate::post_processing::model::PostProcessingSettings;
                use weaver_server_base::jobs::ids::JobId;
                use weaver_server_pipeline::pipeline::units::deobfuscate::*;

                mod tests;
            }
        }
    }
}
