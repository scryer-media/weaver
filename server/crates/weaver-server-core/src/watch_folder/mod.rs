// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_ingest::watch_folder::intake;
pub use weaver_server_ingest::watch_folder::{WatchFolderConfig, WatchFolderMode};
mod scanner;
mod service;
pub use scanner::{
    MarkerRename, ScanIssue, WatchFolderScanReport, WatchFolderScanner, WatchFolderScannerError,
};
pub use service::{WatchFolderService, WatchFolderServiceError};
