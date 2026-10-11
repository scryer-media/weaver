import assert from "node:assert/strict";
import { test } from "node:test";
import { refusalKey, refusalMessage } from "../src/lib/refusal.ts";

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
