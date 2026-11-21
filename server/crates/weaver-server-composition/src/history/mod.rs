// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_history::history::attributes;
pub(crate) mod cache;
pub mod delete_files;
pub use weaver_server_history::history::failure_kind;
pub use weaver_server_history::history::model;
pub mod nzbget_state;
pub use weaver_server_history::history::output_files;
pub mod persistence;
pub mod queries;
pub use weaver_server_history::history::record;
pub mod repository;
pub mod timeline;

pub use attributes::{
    CLIENT_REQUEST_ID_ATTRIBUTE_KEY, history_release_name, history_search_columns,
    is_public_history_attribute_key, normalize_history_search_text, parse_history_metadata,
    public_history_attributes, split_history_metadata,
};
pub use failure_kind::HistoryFailureKind;
pub use model::{
    HistoryEditFields, HistoryFilter, HistoryMetadataEquals, HistoryOrder, HistoryOrderKey,
};
pub use nzbget_state::{NzbgetHistoryState, NzbgetMarkStatus};
pub use record::{IntegrationEventRow, JobHistoryRow};
pub use timeline::JobEvent;
