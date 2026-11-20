// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod making;
pub use weaver_server_settings::categories::model;
pub mod persistence;
pub use weaver_server_settings::categories::queries;
pub use weaver_server_settings::categories::record;
pub mod repository;
pub mod service;

pub use making::{CategoryWrites, Made, category_writes, make_category};
pub use model::{
    CategoryConfig, CategoryValidationError, completion_parent, resolve_category,
    resolve_submission_category, validate_category_path_component,
};
