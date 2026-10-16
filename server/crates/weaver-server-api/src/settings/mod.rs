// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod first_run;
pub mod security_upgrade_notice;
pub mod types;

pub use crate::schema::settings_mutation as mutation;
pub use crate::schema::settings_query as query;
