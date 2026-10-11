import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { refusalKey, refusalMessage } from "../src/lib/refusal.ts";
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

// Left to the daemon's own words: the tunnel engine's text, and two refusals of
// requests this interface never sends.
const UNWORDED = ["PROXY_TUNNEL_CONFIGURATION", "ROUTE_AND_ROUTING_BOTH", "ROUTE_DIRECT_PATH_FALSE"];

test("every code the daemon refuses with has a sentence here", () => {
  const source = readFileSync(
    new URL("../../../server/crates/weaver-server-core/src/proxies/refusal.rs", import.meta.url),
    "utf8",
  );
  const codes = [...new Set([...source.matchAll(/"([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)"/g)].map((match) => match[1]!))].sort();
  assert.ok(codes.length > 60, "the daemon's codes were not found where they are defined");
  const worded = Object.keys(nextEn)
    .filter((key) => key.startsWith(refusalKey("")))
    .map((key) => key.slice(refusalKey("").length));
  assert.deepEqual([...worded, ...UNWORDED].sort(), codes);
});
