// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { proxyFaultKey, proxyFaultSentence, proxyTestKey, proxyTestSentence } from "../src/lib/proxy-fault.ts";
import { nextEn } from "../src/next/i18n/en.ts";

// The dictionary as the app reads it: a missing key comes back as itself.
const english = (key: string, values: Record<string, unknown> = {}) =>
  Object.entries(values).reduce((text, [name, value]) => text.replaceAll(`{{${name}}}`, String(value)), nextEn[key] ?? key);

const source = (path: string) => readFileSync(new URL(`../../../${path}`, import.meta.url), "utf8");

// The body of a Rust function, from its signature to the end of its block.
function body(rust: string, signature: string): string {
  const start = rust.indexOf(signature);
  assert.ok(start >= 0, `${signature} was not found`);
  const end = rust.indexOf("\n    }\n", start);
  return rust.slice(start, end);
}

const worded = (prefix: string) =>
  Object.keys(nextEn)
    .filter((key) => key.startsWith(prefix))
    .map((key) => key.slice(prefix.length))
    .sort();

test("a proxy failure reads as our sentence for its code, with the values it names", () => {
  assert.equal(
    proxyFaultSentence(english, { code: "TIMED_OUT", params: { seconds: 30 }, message: "No answer within 30 seconds." }),
    "No answer within 30 seconds. Check the address, the port and any firewall.",
  );
  assert.equal(
    proxyFaultSentence(english, {
      code: "DESTINATION_UNREACHABLE",
      params: { target: "192.0.2.9:563", reply: "HTTP 502" },
      message: "192.0.2.9:563 could not be reached (HTTP 502).",
    }),
    "192.0.2.9:563 could not be reached (HTTP 502).",
  );
});

test("the daemon's words stand in for OTHER and for a code with no sentence here", () => {
  assert.equal(
    proxyFaultSentence(english, { code: "OTHER", params: { detail: "tunnel provider stopped" }, message: "tunnel provider stopped" }),
    "tunnel provider stopped",
  );
  assert.equal(proxyFaultSentence(english, { code: "FROM_A_NEWER_DAEMON", params: {}, message: "said by a newer daemon" }), "said by a newer daemon");
});

test("a proxy test reads by how far it got when it passed, and by its fault when it failed", () => {
  assert.equal(proxyTestSentence(english, { success: true, code: "CONNECTED", params: {}, message: "Connected." }), "Connected.");
  assert.equal(
    proxyTestSentence(english, { success: false, code: "CONNECTION_REFUSED", params: {}, message: "refused" }),
    "The connection was refused. Check the port and that the proxy is running.",
  );
});

test("every proxy failure code but OTHER has a sentence here", () => {
  const codes = [...body(source("engines/weaver-tunnel/src/fault.rs"), "pub fn code(&self)").matchAll(/=> "([A-Z][A-Z0-9_]*)"/g)]
    .map((match) => match[1]!)
    .sort();
  assert.ok(codes.includes("NAME_NOT_RESOLVED"), "the failure codes were not found where they are defined");
  assert.deepEqual([...worded(proxyFaultKey("")), "OTHER"].sort(), codes);
});

test("every way a proxy test passes has a sentence here", () => {
  const codes = [...body(source("server/crates/weaver-server-core/src/proxies/fault.rs"), "pub fn code(self)").matchAll(/=> "([A-Z][A-Z0-9_]*)"/g)]
    .map((match) => match[1]!)
    .sort();
  assert.ok(codes.includes("CONNECTED"), "the pass codes were not found where they are defined");
  assert.deepEqual(worded(proxyTestKey("")), codes);
});

test("a proxy failure sentence names only values the daemon sends", () => {
  const sent = new Set(
    [...body(source("engines/weaver-tunnel/src/fault.rs"), "pub fn params(&self)").matchAll(/\("([A-Za-z]+)", /g)].map((match) => match[1]!),
  );
  assert.ok(sent.has("publicKey"), "the parameter names were not found where they are defined");
  for (const code of worded(proxyFaultKey(""))) {
    for (const [, name] of nextEn[proxyFaultKey(code)]!.matchAll(/\{\{(\w+)\}\}/g)) {
      assert.ok(sent.has(name!), `${code} names {{${name}}}, which the daemon does not send`);
    }
  }
});
