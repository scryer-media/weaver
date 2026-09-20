import assert from "node:assert/strict";
import test from "node:test";
import { recheckLoginRequired, watchForSignOut } from "../src/lib/login-required.ts";

test("browser refusals recheck sign-in without replaying the refused request", async (t) => {
  const oldWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
  const oldDocument = Object.getOwnPropertyDescriptor(globalThis, "document");
  const statusUrl = "http://localhost:9090/api/auth/status";
  const calls: string[] = [];
  let status = 403;
  t.mock.method(globalThis, "fetch", async (input: RequestInfo | URL) => {
    const url = String(input);
    calls.push(url);
    return url === statusUrl
      ? Response.json({ enabled: true, authenticated: false })
      : new Response(null, { status });
  });
  const browser = { fetch: globalThis.fetch };
  Object.defineProperty(globalThis, "window", { configurable: true, value: browser });
  Object.defineProperty(globalThis, "document", {
    configurable: true, value: { baseURI: "http://localhost:9090/" },
  });
  try {
    watchForSignOut();
    for (const refusal of [401, 403]) {
      status = refusal;
      calls.length = 0;
      const response = await browser.fetch("http://localhost:9090/graphql", { method: "POST" });
      assert.equal(response.status, refusal);
      assert.deepEqual(calls, ["http://localhost:9090/graphql", statusUrl]);
      await recheckLoginRequired();
    }
    for (const [url, responseStatus] of [
      ["http://localhost:9090/graphql", 500],
      ["http://localhost:8080/graphql", 403],
      ["http://localhost:9090/api/login", 403],
    ] as const) {
      status = responseStatus;
      calls.length = 0;
      await browser.fetch(url);
      assert.deepEqual(calls, [url]);
    }
  } finally {
    if (oldWindow) Object.defineProperty(globalThis, "window", oldWindow);
    else Reflect.deleteProperty(globalThis, "window");
    if (oldDocument) Object.defineProperty(globalThis, "document", oldDocument);
    else Reflect.deleteProperty(globalThis, "document");
  }
});
