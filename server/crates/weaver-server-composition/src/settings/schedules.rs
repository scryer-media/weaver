// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved to the settings crate; the tests open the composed database, so they
// stay here under the same module path.
pub use weaver_server_settings::settings::schedules::*;

#[cfg(test)]
mod tests;
