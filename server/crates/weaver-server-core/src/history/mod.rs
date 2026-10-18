// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod attributes;
pub mod delete_queue;
pub mod failure_kind;
pub mod model;
pub mod nzbget_state;
pub mod persistence;
pub mod queries;
pub mod record;
pub mod repository;
pub mod timeline;

pub use attributes::{
    CLIENT_REQUEST_ID_ATTRIBUTE_KEY, is_public_history_attribute_key, parse_history_metadata,
    public_history_attributes, split_history_metadata,
};
pub use failure_kind::HistoryFailureKind;
pub use model::{HistoryFilter, HistoryMetadataEquals};
pub use nzbget_state::{NzbgetHistoryState, NzbgetMarkStatus};
pub use record::{IntegrationEventRow, JobHistoryRow};
pub use timeline::JobEvent;
