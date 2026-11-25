// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Unit tests of the pipeline's graph runtime, compiled apart from the lib so
// the lib's test build stays small. Each module below stands in for the lib
// module of the same path: it sees what its tests used of that module, and
// its tests keep their names.

use weaver_server_composition::*;
use weaver_server_pipeline::alloc_probe;
use weaver_server_pipeline::{SchedulerHandle, history, jobs};
use weaver_server_settings::categories;

// Counts allocations per thread for the tests that assert a hot path
// allocates nothing, as the lib's own test build does.
#[global_allocator]
static ALLOCATOR: alloc_probe::Counting = alloc_probe::Counting;

pub(crate) mod pipeline {
    pub(crate) use weaver_server_pipeline::pipeline::*;
    pub(crate) mod graph {
        pub(crate) use weaver_server_pipeline::pipeline::graph::*;

        pub(crate) mod checkpoint {
            pub(crate) use weaver_server_pipeline::pipeline::graph::engine::Heard;

            pub(crate) use weaver_server_pipeline::jobs::graph_checkpoint::{
                CoverageRow, MemberRow, NodeRow,
            };

            pub(crate) use weaver_server_pipeline::pipeline::graph::checkpoint::*;

            mod tests;
        }
        pub(crate) mod decoding {
            pub(crate) use weaver_server_pipeline::pipeline::graph::units::decoding::*;

            mod tests;
        }
        pub(crate) mod disk {
            pub(crate) use weaver_server_pipeline::pipeline::graph::units::disk::*;

            mod tests;
        }
        pub(crate) mod engine {
            pub(crate) use weaver_server_pipeline::pipeline::graph::engine::pool::DecodePool;

            pub(crate) use weaver_server_pipeline::pipeline::graph::engine::*;

            pub(crate) mod pool {
                pub(crate) use weaver_graph::Budget;
                pub(crate) use weaver_server_pipeline::pipeline::graph::engine::RUN_OVERHEAD;
                pub(crate) use weaver_server_pipeline::pipeline::graph::engine::units::pool::*;

                mod tests;
            }
            mod tests;
        }
        pub(crate) mod envelope {
            pub(crate) use weaver_server_pipeline::pipeline::graph::units::envelope::*;

            pub(crate) mod durable {
                pub(crate) use weaver_graph::{NodeId, Run};
                pub(crate) use weaver_server_pipeline::pipeline::graph::envelope::{
                    Envelopes, envelope_name,
                };

                pub(crate) use weaver_server_pipeline::pipeline::graph::units::envelope_durable::*;

                mod tests;
            }
        }
        pub(crate) mod job {
            pub(crate) use std::sync::Arc;

            pub(crate) use weaver_graph::holds::HoldsAccountant;
            pub(crate) use weaver_graph::{MemberId, NodeId};
            pub(crate) use weaver_server_pipeline::pipeline::graph::job::loans::Loans;

            pub(crate) use weaver_server_pipeline::pipeline::graph::checkpoint::Pending;
            pub(crate) use weaver_server_pipeline::pipeline::graph::engine::Said;

            pub(crate) use weaver_server_pipeline::pipeline::graph::limits::Caps;

            pub(crate) use weaver_server_pipeline::pipeline::graph::root::phase::RootPhase;

            pub(crate) use weaver_server_disk::operations::disk::CapacityReader;
            pub(crate) use weaver_server_pipeline::pipeline::graph::job::*;

            pub(crate) mod durable {
                pub(crate) use weaver_graph::NodeId;

                pub(crate) use weaver_server_pipeline::pipeline::graph::checkpoint::{
                    Generations, Touched,
                };
                pub(crate) use weaver_server_pipeline::pipeline::graph::envelope::durable::KeptRecord;

                mod tests;
            }
            pub(crate) mod repair {
                pub(crate) use weaver_server_pipeline::pipeline::graph::job::units::repair::*;

                pub(super) mod tests;
            }
            pub(crate) mod runs {
                pub(crate) use weaver_graph::{MemberId, NodeId};
                pub(crate) use weaver_server_pipeline::pipeline::graph::job::GraphJob;

                mod tests;
            }
            mod hybrid_loan_tests;
            mod loan_tests;
            mod quiet_face_tests;
            mod relaid_again_tests;
            mod verification_tests;
        }
        pub(crate) mod leaf {
            pub(crate) use std::path::Path;
            pub(crate) use std::sync::Arc;

            pub(crate) use weaver_graph::{Algorithm, ByteRange, Evidenced};

            pub(crate) use weaver_server_disk::operations::disk::writer::Submitter;

            pub(crate) use weaver_server_pipeline::pipeline::graph::leaf::*;

            mod tests;
        }
        pub(crate) mod limits {
            pub(crate) use std::sync::Arc;

            pub(crate) use weaver_graph::{MemberId, NodeId, Tree};

            pub(crate) use weaver_server_disk::operations::disk::CapacityReader;
            pub(crate) use weaver_server_disk::operations::spend::Spend;
            pub(crate) use weaver_server_pipeline::pipeline::graph::limits::*;

            mod tests;
        }
        pub(crate) mod metadata {
            pub(crate) use weaver_graph::MemberId;
            pub(crate) use weaver_model::files::FileRole;
            pub(crate) use weaver_server_pipeline::pipeline::graph::units::metadata::*;

            mod tests;
        }
        pub(crate) mod namespace {
            pub(crate) use bytes::Bytes;
            pub(crate) use std::collections::BTreeSet;
            pub(crate) use std::path::{Path, PathBuf};
            pub(crate) use std::sync::Arc;

            pub(crate) use weaver_graph::holds::HoldsAccountant;
            pub(crate) use weaver_graph::member::FIRST_BYTES;
            pub(crate) use weaver_graph::{
                Builder, ByteRange, Coverage, Effect, MemberId, NodeId, Placement, Provenance,
                Segment, Tree, VolumeIndex, Why,
            };

            pub(crate) use weaver_server_pipeline::pipeline::graph::leaf::{Leaf, LeafError};
            pub(crate) use weaver_server_pipeline::pipeline::graph::limits::Caps;

            pub(crate) use weaver_server_disk::operations::disk::CapacityReader;

            pub(crate) use crate::pipeline::graph::namespace::loan::replay::Replayed;
            pub(crate) use weaver_server_pipeline::pipeline::graph::namespace::verification::Verified;

            pub(crate) use crate::pipeline::graph::namespace::loan::Loan;

            pub(crate) use crate::pipeline::graph::namespace::moves::restored_at;
            pub(crate) use weaver_server_pipeline::pipeline::graph::namespace::recovery::{
                Coming, Lender,
            };

            pub(crate) use weaver_server_pipeline::pipeline::graph::namespace::*;

            pub(crate) mod recovery {
                pub(crate) use weaver_server_pipeline::pipeline::graph::namespace::recovery::*;

                mod name_sets;
            }
            pub(crate) mod verification {
                pub(crate) use weaver_server_pipeline::pipeline::graph::namespace::READ_BACK;
                pub(crate) use weaver_server_pipeline::pipeline::graph::namespace::verification::*;

                mod turns;
            }
            pub(crate) mod claimed {
                pub(crate) use weaver_server_pipeline::pipeline::graph::namespace::units::claimed::*;

                mod tests;
            }
            pub(crate) mod loan {
                pub(crate) use std::cell::RefCell;
                pub(crate) use std::collections::{BTreeMap, BTreeSet};

                pub(crate) use std::sync::Arc;

                pub(crate) use weaver_graph::{MemberId, NodeId};

                pub(crate) use weaver_server_disk::operations::disk::volume::Volume;

                pub(crate) use weaver_server_pipeline::pipeline::graph::hardware::Shutdown;

                pub(crate) use weaver_server_pipeline::pipeline::graph::namespace::units::loan::*;

                mod tests;
            }
            pub(crate) mod moves {
                pub(crate) use std::path::Path;

                pub(crate) use weaver_graph::{MemberId, NodeId};
                pub(crate) use weaver_server_pipeline::pipeline::graph::leaf::Leaf;
                pub(crate) use weaver_server_pipeline::pipeline::graph::namespace::NamespaceWriter;
                pub(crate) use weaver_server_pipeline::pipeline::graph::namespace::units::moves::*;

                mod tests;
            }
            mod tests;
        }
        pub(crate) mod restore {
            pub(crate) use weaver_server_pipeline::pipeline::graph::job::GraphError;
            pub(crate) use weaver_server_pipeline::pipeline::graph::job::restore::RestoreFailure;

            pub(crate) use weaver_server_pipeline::pipeline::graph::restore::*;

            mod tests;
        }
        pub(crate) mod root {
            pub(crate) use weaver_server_pipeline::pipeline::graph::root::*;

            pub(crate) mod intake {
                pub(crate) use weaver_graph::{ByteRange, NodeId, Segment, VolumeIndex};
                pub(crate) use weaver_server_pipeline::pipeline::graph::namespace::Segments;
                pub(crate) use weaver_server_pipeline::pipeline::graph::root::intake::*;

                mod tests;
            }
            pub(super) mod tests;
        }
        pub(crate) mod status {
            pub(crate) use weaver_server_pipeline::pipeline::graph::status::claims::Claim;
            pub(crate) use weaver_server_pipeline::pipeline::graph::status::claims::Delivered;
            pub(crate) use weaver_server_pipeline::pipeline::graph::status::claims::Described;
            pub(crate) use weaver_server_pipeline::pipeline::graph::status::claims::{
                Delivery, Unarrived,
            };

            pub(crate) use weaver_server_pipeline::pipeline::graph::status::slots::RepairSlots;
            pub(crate) use weaver_server_pipeline::pipeline::graph::status::summary::{
                RecoverySummary, SetSummary,
            };
            pub(crate) use weaver_server_pipeline::pipeline::graph::status::*;

            mod tests;
        }
        pub(crate) mod window {
            pub(crate) use weaver_server_pipeline::pipeline::graph::units::window::*;

            #[cfg(any(target_os = "linux", target_os = "macos"))]
            mod tests;
        }
        mod tests;
    }
}
