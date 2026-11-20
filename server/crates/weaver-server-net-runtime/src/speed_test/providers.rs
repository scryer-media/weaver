// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::http::{HttpLeg, HttpOptions, read_capped, routed_get};
pub use weaver_server_speedtest::speed_test::providers::*;

const DISCOVERY_LIMIT: usize = 4 * 1024 * 1024;

// Find fast.com's targets through one leg: its page, the app script the
// page loads, then the targets for the token that script carries.
pub async fn discover_fast_com(
    leg: &HttpLeg,
    options: &HttpOptions,
    page: &reqwest::Url,
    api: &reqwest::Url,
) -> Result<Endpoints, String> {
    let fetch = |url: reqwest::Url, what: &'static str| async move {
        let routed = routed_get(leg, &url, options)
            .await
            .map_err(|error| format!("fetching the fast.com {what} failed: {error}"))?;
        read_capped(routed, DISCOVERY_LIMIT)
            .await
            .map_err(|error| format!("fetching the fast.com {what} failed: {error}"))
    };
    let html = fetch(page.clone(), "page").await?;
    let script = fast_com_script(page, &String::from_utf8_lossy(&html))?;
    let script = fetch(script, "script").await?;
    let token = fast_com_token(&String::from_utf8_lossy(&script))?;
    let answer = fetch(fast_com_api_url(api, &token), "targets").await?;
    fast_com_endpoints(&answer)
}
