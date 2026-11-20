// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub(crate) use weaver_server_migrations::schema_migrations::proxy_dns_v125::*;

#[cfg(test)]
#[path = "proxy_dns_v125_tests.rs"]
mod tests;
