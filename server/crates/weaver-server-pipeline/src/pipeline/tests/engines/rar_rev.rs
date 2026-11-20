// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Standalone RAR recovery volumes: a RAR5 or RAR4 set
// posted without some of its data volumes, beside `.rev` files that can
// rebuild them, or too few that cannot. The set is stored, so the member is
// whole only when every volume is.

pub(super) use super::archive_scenario_fixtures::rar_rev::rar3_rev;
pub(super) use super::rar_fixtures::{body, lz_four, rar5_revs};
