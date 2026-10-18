// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SettingRecord {
    pub key: String,
    pub value: String,
}
