// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod model;
pub mod persistence;
pub mod queries;
pub mod record;
pub mod repository;
pub mod service;

pub use model::{
    CategoryConfig, CategoryValidationError, completion_parent, resolve_category,
    resolve_submission_category, validate_category_path_component,
};
