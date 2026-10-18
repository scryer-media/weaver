// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::Arc;

pub(crate) type SqliteWriterGate = Arc<tokio::sync::Mutex<()>>;

pub(crate) fn new_writer_gate() -> SqliteWriterGate {
    Arc::new(tokio::sync::Mutex::new(()))
}
