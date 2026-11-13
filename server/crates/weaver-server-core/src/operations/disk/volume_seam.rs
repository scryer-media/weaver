// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Placeholder until the volume registry lands; removed at merge.
use std::path::Path;
use std::sync::Arc;

use super::StorageCapacity;
use super::volume::Volume;

impl StorageCapacity {
    pub(crate) fn volume(&self, _path: &Path) -> Arc<Volume> {
        Arc::new(Volume::new(None, String::new()))
    }

    pub(crate) fn volumes(&self) -> Vec<Arc<Volume>> {
        Vec::new()
    }
}
