// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Bandwidth as the pipeline sees it: the composition crate's bandwidth, plus
// the service, whose limits the pipeline applies.
pub use weaver_server_composition::bandwidth::*;

pub mod service;
