// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod guards;
pub mod types;

pub use crate::schema::auth_mutation as mutation;
pub use crate::schema::auth_query as query;
pub use guards::{
    AdminGuard, ControlGuard, FreshAdminGuard, ReadGuard, graphql_error, internal_error,
};
pub use types::CallerIdentity;
pub use weaver_server_core::auth::{
    CachedLoginAuth, CallerScope, LoginAuthCache, check_password_length, generate_api_key,
    hash_api_key, hash_password, verify_password,
};
