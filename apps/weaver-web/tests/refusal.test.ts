// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { codedSentence, refusalKey, refusalMessage, refusalNamesSomething } from "../src/lib/refusal.ts";
import { nextEn } from "../src/next/i18n/en.ts";

const SENTENCES: Record<string, string> = { [refusalKey("ROUTE_PROXY_REPEATED")]: "Ours, for a repeated proxy." };
const t = (key: string) => SENTENCES[key] ?? key;
const failed = (...graphQLErrors: { message: string; extensions?: Record<string, unknown> }[]) => ({
  message: "[GraphQL] the whole failure",
  graphQLErrors,
});

test("a refusal is worded by its code, whatever the daemon's message says", () => {
  for (const message of ["a proxy cannot appear twice in a ladder", "reworded by a later daemon"]) {
    assert.equal(
      refusalMessage(t, failed({ message, extensions: { code: "ROUTE_PROXY_REPEATED" } })),
      "Ours, for a repeated proxy.",
    );
  }
});

test("the daemon's message stands in where there is no sentence for the code, or no code", () => {
  assert.equal(refusalMessage(t, failed({ message: "from a newer daemon", extensions: { code: "NOT_KNOWN_HERE" } })), "from a newer daemon");
  assert.equal(refusalMessage(t, failed({ message: "no code on this one" })), "no code on this one");
  assert.equal(refusalMessage(t, failed({ message: "a code that is no string", extensions: { code: 7 } })), "a code that is no string");
});

test("a failure that never reached the daemon keeps its own message", () => {
  assert.equal(refusalMessage(t, failed()), "[GraphQL] the whole failure");
});

// The dictionary as the app reads it: a missing key comes back as itself.
const english = (key: string, values: Record<string, unknown> = {}) =>
  Object.entries(values).reduce((text, [name, value]) => text.replaceAll(`{{${name}}}`, String(value)), nextEn[key] ?? key);

test("the route dialog's repeated proxy reads as our sentence, not the daemon's", () => {
  const refused = failed({
    message: "a proxy cannot appear twice in a ladder, including pools and chains",
    extensions: { code: "ROUTE_PROXY_REPEATED" },
  });
  assert.equal(
    refusalMessage(english, refused),
    "A proxy can appear only once in a proxy ladder, counting pools and chains.",
  );
});

test("a sentence repeats the numbers a refusal names", () => {
  const refused = failed({
    message: "routes require 3 WireGuard instances",
    extensions: { code: "WIREGUARD_INSTANCE_LIMIT", params: { required: 3, limit: 2, instanceMib: 516, paths: [[1]] } },
  });
  assert.equal(refusalMessage(english, refused), "These routes need 3 WireGuard instances; this host allows 2.");
});

test("too many DNS servers and too many tunnel addresses each name their own limit", () => {
  const servers = failed({
    message: "a DNS setting lists at most 10 servers",
    extensions: { code: "PROXY_DNS_SERVER_COUNT", params: { limit: 10 } },
  });
  assert.equal(refusalMessage(english, servers), "List at most 10 DNS servers.");
  const addresses = failed({
    message: "a WireGuard proxy takes at most 8 tunnel addresses",
    extensions: { code: "PROXY_ADDRESS_COUNT", params: { limit: 8 } },
  });
  assert.equal(refusalMessage(english, addresses), "A WireGuard proxy takes at most 8 tunnel addresses.");
});

test("a refusal that names routes by key reads with their names, and a key with no name stands as itself", () => {
  const refused = failed({
    message: "DNS profile is used by server 3",
    extensions: { code: "DNS_PROFILE_IN_USE", params: { routes: ["server:3", "rss:9"] } },
  });
  const names: Record<string, string> = { "server:3": "news.example.com" };
  assert.equal(refusalNamesSomething(refused), true);
  assert.equal(
    refusalMessage(english, refused, { route: (key) => names[key] ?? key }),
    "These routes still follow this profile: news.example.com, rss:9. Choose another profile for them first.",
  );
  assert.equal(refusalNamesSomething(failed({ message: "x", extensions: { code: "X", params: { limit: 2 } } })), false);
});

test("a profile in use names the proxies that find their address with it, with or without routes", () => {
  const proxy = (id: number) => (id === 7 ? "Relay" : undefined);
  const byProxies = failed({ message: "in use", extensions: { code: "DNS_PROFILE_IN_USE", params: { routes: [], proxies: [7, 9] } } });
  assert.equal(refusalNamesSomething(byProxies), true);
  assert.equal(
    refusalMessage(english, byProxies, { proxy }),
    "These proxies still use this profile to find their address: Relay, Proxy #9. Choose another profile for them first.",
  );
  const byBoth = failed({ message: "in use", extensions: { code: "DNS_PROFILE_IN_USE", params: { routes: ["server:3"], proxies: [7] } } });
  assert.equal(
    refusalMessage(english, byBoth, { route: () => "news.example.com", proxy }),
    "These routes still follow this profile: news.example.com. These proxies still use it to find their address: Relay. Choose another profile for them first.",
  );
});

test("a refusal that names proxies by id reads with their names, and an unknown one by its number", () => {
  const silent = failed({
    message: "WireGuard proxies 4 and 7 list no tunnel DNS",
    extensions: { code: "DNS_PROFILE_NEEDS_TUNNEL_DNS", params: { proxies: [4, 7], routes: ["server:3"] } },
  });
  const names = { route: () => "news.example.com", proxy: (id: number) => (id === 4 ? "Mullvad" : undefined) };
  assert.equal(refusalNamesSomething(silent), true);
  assert.equal(
    refusalMessage(english, silent, names),
    "These routes have the proxy answer DNS: news.example.com. These WireGuard proxies on them list no DNS servers for their tunnel: Mullvad, Proxy #7. Add the tunnel's DNS servers to each, or choose another profile.",
  );
});

test("a sentence names a list of names the refusal carries, joined", () => {
  const refused = failed({
    message: "server list is used by profiles",
    extensions: { code: "DNS_LIST_IN_USE", params: { profiles: ["Checked", "Tunnel"], ids: [1, 2] } },
  });
  assert.equal(refusalMessage(english, refused), "These DNS profiles still use this list: Checked, Tunnel.");
});

// Left to the daemon's own words: the tunnel engine's text, and two refusals of
// requests this interface never sends.
const UNWORDED = ["PROXY_TUNNEL_CONFIGURATION", "ROUTE_AND_ROUTING_BOTH", "ROUTE_DIRECT_PATH_FALSE"];
// Values a refusal's params carry, spelled like codes but never refused with.
const PARAM_VALUES = ["PROXY_NAME"];

test("every code the daemon refuses with has a sentence here", () => {
  const source = readFileSync(
    new URL("../../../server/crates/weaver-server-core/src/proxies/refusal.rs", import.meta.url),
    "utf8",
  );
  const codes = [...new Set([...source.matchAll(/"([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)"/g)].map((match) => match[1]!))]
    .filter((code) => !PARAM_VALUES.includes(code))
    .sort();
  assert.ok(codes.length > 60, "the daemon's codes were not found where they are defined");
  const worded = Object.keys(nextEn)
    .filter((key) => key.startsWith(refusalKey("")))
    .map((key) => key.slice(refusalKey("").length));
  assert.deepEqual([...worded, ...UNWORDED].sort(), codes);
});

const failureKey = (code: string) => `next.networking.speed.failure.${code}`;

test("a speed test failure reads as our sentence for its code, and in the daemon's words for OTHER", () => {
  assert.equal(
    codedSentence(english, failureKey("HTTP_STATUS"), { url: "https://speed.fixture.invalid/", status: 503 }, "GET https://speed.fixture.invalid/ answered HTTP 503 Service Unavailable"),
    "The provider answered HTTP 503.",
  );
  assert.equal(codedSentence(english, failureKey("OTHER"), {}, "the response has no body"), "the response has no body");
});

test("every kind of speed test failure but OTHER has a sentence here", () => {
  const source = readFileSync(
    new URL("../../../server/crates/weaver-server-api/src/speed_test.rs", import.meta.url),
    "utf8",
  );
  const body = source.match(/pub enum SpeedTestFailureCode \{([^}]*)\}/)?.[1] ?? "";
  const codes = [...body.matchAll(/^\s*([A-Z][A-Za-z]*),$/gm)]
    .map((match) => match[1]!.replace(/(?<=[a-z])(?=[A-Z])/g, "_").toUpperCase())
    .sort();
  assert.ok(codes.includes("RATE_LIMITED"), "the failure codes were not found where they are defined");
  const worded = Object.keys(nextEn)
    .filter((key) => key.startsWith(failureKey("")))
    .map((key) => key.slice(failureKey("").length));
  assert.deepEqual([...worded, "OTHER"].sort(), codes);
});
