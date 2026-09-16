import assert from "node:assert/strict";
import test from "node:test";
import { signOut } from "../src/lib/logout.ts";

function recorder(status: number) {
  const requests: { url: string; method: string | undefined }[] = [];
  const visits: string[] = [];
  const request = (async (input: RequestInfo | URL, init?: RequestInit) => {
    requests.push({ url: String(input), method: init?.method });
    return new Response(null, { status });
  }) as typeof fetch;
  return { requests, visits, request, navigate: (href: string) => visits.push(href) };
}

test("signs out of the install mounted below a base path and lands on its root", async () => {
  const calls = recorder(200);
  await signOut("https://media.example.test/weaver/", calls.request, calls.navigate);

  assert.deepEqual(calls.requests, [
    { url: "https://media.example.test/weaver/api/logout", method: "POST" },
  ]);
  assert.deepEqual(calls.visits, ["https://media.example.test/weaver/"]);
});

test("signs out of an install served at the host root", async () => {
  const calls = recorder(204);
  await signOut("http://localhost:8080/", calls.request, calls.navigate);

  assert.deepEqual(calls.requests, [{ url: "http://localhost:8080/api/logout", method: "POST" }]);
  assert.deepEqual(calls.visits, ["http://localhost:8080/"]);
});

test("stays on the page when the server does not confirm the sign-out", async () => {
  for (const status of [404, 500]) {
    const calls = recorder(status);
    await assert.rejects(
      signOut("https://media.example.test/weaver/", calls.request, calls.navigate),
      new RegExp(`HTTP ${status}`),
    );
    assert.deepEqual(calls.visits, []);
  }
});

test("authenticated sign-out carries the browser CSRF proof", async () => {
  let sent: RequestInit | undefined;
  const request = (async (_input: RequestInfo | URL, init?: RequestInit) => {
    sent = init;
    return new Response(null, { status: 204 });
  }) as typeof fetch;
  await signOut("https://media.example.test/weaver/", request, () => {}, { "X-Weaver-CSRF": "test-proof" });
  assert.equal(sent?.credentials, "include");
  assert.equal(new Headers(sent?.headers).get("X-Weaver-CSRF"), "test-proof");
});
