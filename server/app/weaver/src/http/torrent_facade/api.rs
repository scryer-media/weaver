// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The versions of qBittorrent's WebUI API the facade answers as.
//!
//! A request is read the same under every version: the facade takes any
//! parameter a 5.x client sends. A reply carries every field any version
//! has, since a client reads past the fields it does not know. What follows
//! the version is only an answer a later version changed for the same
//! request: a status, a body, or the meaning of a value. Each such change
//! is one constant below, at the API version that brought it
//! (qBittorrent `WebAPI_Changelog.md`), and one method that answers by it.
//! A new version is a new constant, plus a constant and a method for each
//! answer it changes.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use weaver_server_core::torrents::TrackerStatus;

use super::text;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::http) struct Api(u16, u16, u16);

impl Api {
    // The API of each 5.x release line (`WebApplication::API_VERSION`).
    #[cfg_attr(not(test), expect(dead_code, reason = "the server answers as LATEST"))]
    pub(in crate::http) const V5_0: Self = Self(2, 11, 2);
    #[cfg_attr(not(test), expect(dead_code, reason = "the server answers as LATEST"))]
    pub(in crate::http) const V5_1: Self = Self(2, 11, 4);
    #[cfg_attr(not(test), expect(dead_code, reason = "the server answers as LATEST"))]
    pub(in crate::http) const V5_2: Self = Self(2, 15, 1);
    pub(in crate::http) const V5_3: Self = Self(2, 16, 2);
    pub(in crate::http) const LATEST: Self = Self::V5_3;

    // An action that gives no result answers 204, not 200 (2.11.8).
    const NO_CONTENT: Self = Self(2, 11, 8);
    // `torrents/editTracker` answers 204 (2.13.0).
    const EDIT_TRACKER_NO_CONTENT: Self = Self(2, 13, 0);
    // A tracker's state may be 5, an error, or 6, unreachable (2.13.0).
    const TRACKER_ERROR_STATES: Self = Self(2, 13, 0);
    // `auth/login` answers 204 or 401, `torrents/add` its counts, and an
    // unknown action "Endpoint does not exist" (2.14.0).
    const LOGIN_STATUS: Self = Self(2, 14, 0);
    const ADD_COUNTS: Self = Self(2, 14, 0);
    const ENDPOINT_DOES_NOT_EXIST: Self = Self(2, 14, 0);

    /// `app/webapiVersion`.
    pub(super) fn version(self) -> String {
        let Self(major, minor, patch) = self;
        format!("{major}.{minor}.{patch}")
    }

    /// The answer of an action that gives no result.
    pub(super) fn no_result(self) -> Response {
        if self >= Self::NO_CONTENT {
            StatusCode::NO_CONTENT.into_response()
        } else {
            text(StatusCode::OK, "")
        }
    }

    /// `response`, or no result where it is an empty success: for the
    /// actions qBittorrent gives none.
    pub(super) fn without_result(self, response: Response) -> Response {
        if response.status() == StatusCode::OK {
            self.no_result()
        } else {
            response
        }
    }

    /// `torrents/editTracker`'s answer to `response`.
    pub(super) fn edited_tracker(self, response: Response) -> Response {
        if self >= Self::EDIT_TRACKER_NO_CONTENT {
            self.without_result(response)
        } else {
            response
        }
    }

    /// `auth/login` when the credentials hold.
    pub(super) fn logged_in(self) -> Response {
        if self >= Self::LOGIN_STATUS {
            StatusCode::NO_CONTENT.into_response()
        } else {
            text(StatusCode::OK, "Ok.")
        }
    }

    /// `auth/login` when they do not.
    pub(super) fn login_refused(self) -> Response {
        if self >= Self::LOGIN_STATUS {
            text(StatusCode::UNAUTHORIZED, "Unauthorized")
        } else {
            text(StatusCode::OK, "Fails.")
        }
    }

    /// `torrents/add` once every torrent has been tried: the torrents
    /// added and how many failed. Where none was added, `refusal` is why,
    /// when there is one reason.
    pub(super) fn added(self, ids: &[String], failed: usize, refusal: Option<String>) -> Response {
        match (self >= Self::ADD_COUNTS, ids.is_empty()) {
            (true, false) => axum::Json(json!({
                "success_count": ids.len(),
                "failure_count": failed,
                "pending_count": 0,
                "added_torrent_ids": ids,
            }))
            .into_response(),
            (true, true) => text(
                StatusCode::CONFLICT,
                refusal.unwrap_or_else(|| "Conflict".to_owned()),
            ),
            (false, false) => text(StatusCode::OK, "Ok."),
            (false, true) => text(StatusCode::OK, "Fails."),
        }
    }

    /// An action the client does not have, in a group it does.
    pub(super) fn no_action(self) -> Response {
        if self >= Self::ENDPOINT_DOES_NOT_EXIST {
            text(StatusCode::NOT_FOUND, "Endpoint does not exist")
        } else {
            text(StatusCode::NOT_FOUND, "Not Found")
        }
    }

    /// A tracker's state (qBittorrent `TrackerEndpointState`), 0 for a
    /// tracker that is off. Before the error states, a failed tracker is
    /// not working.
    pub(super) fn tracker_status(self, status: TrackerStatus) -> Value {
        let errors = self >= Self::TRACKER_ERROR_STATES;
        json!(match status {
            TrackerStatus::Disabled => 0,
            TrackerStatus::NotContacted => 1,
            TrackerStatus::Working => 2,
            TrackerStatus::NotWorking => 4,
            TrackerStatus::TrackerError if errors => 5,
            TrackerStatus::Unreachable if errors => 6,
            TrackerStatus::TrackerError | TrackerStatus::Unreachable => 4,
        })
    }
}
