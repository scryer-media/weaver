// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The jobs module as the pipeline sees it: the composition crate's jobs, plus
// the parts that drive the pipeline (the classes, the scheduler handle, the
// service and a running job's state).
pub use weaver_server_composition::jobs::*;

pub mod class;
pub mod handle;
pub mod model;
pub mod service;

pub use class::{
    AddRefusal, ByProtocol, ClassIntake, ClassRate, ClassRates, JobClass, JobClasses, Owed,
    OwnedRows, PipelineClass, RefusalReason, Route, Submission, TunnelGiveBack,
};
pub use handle::{SchedulerHandle, SharedPipelineState};
pub use model::JobState;
