// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The nearest server of a list provider: every listed server's ping
// endpoint is asked at once through the run's leg, and the first to answer
// with a success status is the shortest round trip through the route.

use tokio::sync::watch;
use tokio::task::JoinSet;

use super::http::{HttpLeg, HttpOptions, RoutedClient};
use super::providers::ProviderServer;
use super::runner::Stop;

pub(crate) enum Nearest {
    Found(ProviderServer),
    // No listed server answered its ping.
    NoneAnswered,
    Cancelled,
}

// Ping every server with a ping endpoint and take the first to answer.
// Every probe has let go of its bridge by the time this returns.
pub(crate) async fn nearest(
    leg: &HttpLeg,
    servers: Vec<ProviderServer>,
    options: &HttpOptions,
    stop: &mut Stop,
) -> Nearest {
    let (halt, halted) = watch::channel(false);
    let mut probes = JoinSet::new();
    for server in servers {
        let Some(url) = server
            .ping_url
            .as_deref()
            .and_then(|url| reqwest::Url::parse(url).ok())
        else {
            continue;
        };
        let (leg, options, halted) = (leg.clone(), options.clone(), halted.clone());
        probes.spawn(async move { ping(&leg, &url, &options, halted).await.then_some(server) });
    }
    let picked = loop {
        tokio::select! {
            biased;
            _ = stop.stopped() => break Nearest::Cancelled,
            joined = probes.join_next() => match joined {
                Some(Ok(Some(server))) => break Nearest::Found(server),
                Some(_) => {}
                None => break Nearest::NoneAnswered,
            },
        }
    };
    // The probes still out stop where they are and close: one that has not
    // answered does not hold the run up, and none leaves its bridge behind.
    halt.send_replace(true);
    while probes.join_next().await.is_some() {}
    picked
}

// One GET of a ping endpoint over a bridge of its own, closed on every path.
// True when it answered with a success status before `halted` was set.
async fn ping(
    leg: &HttpLeg,
    url: &reqwest::Url,
    options: &HttpOptions,
    mut halted: watch::Receiver<bool>,
) -> bool {
    let halt = async move {
        let _ = halted.wait_for(|halted| *halted).await;
    };
    tokio::pin!(halt);
    let client = tokio::select! {
        biased;
        _ = &mut halt => return false,
        connected = RoutedClient::connect(leg, url, options, None) => match connected {
            Ok(client) => client,
            Err(_) => return false,
        },
    };
    let answered = tokio::select! {
        biased;
        _ = &mut halt => false,
        response = client.get(url) => response.is_ok(),
    };
    client.close().await;
    answered
}
