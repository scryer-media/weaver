// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Settings as the pipeline sees them: the composition crate's settings, with the
// archive password tests that read the pipeline's own candidate assembly.
pub use weaver_server_composition::settings::*;

pub mod archive_passwords;
