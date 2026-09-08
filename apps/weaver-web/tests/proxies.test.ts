import test from "node:test";
import assert from "node:assert/strict";
import { appendProxy, moveProxy, directRouting, proxyLabels } from "../src/lib/proxies.ts";

test("first proxy blocks direct fallback, subsequent additions preserve the explicit choice", () => {
  const first = appendProxy(directRouting, 3);
  assert.deepEqual(first, { proxyIds: [3], allowDirect: false });
  assert.deepEqual(appendProxy({ ...first, allowDirect: true }, 7), { proxyIds: [3, 7], allowDirect: true });
  assert.deepEqual(directRouting, { proxyIds: [], allowDirect: true });
});
test("ladder enforces eight distinct alternatives without changing order", () => {
  let policy = directRouting;
  for (let id = 1; id <= 10; id++) policy = appendProxy(policy, id);
  assert.deepEqual(policy.proxyIds, [1, 2, 3, 4, 5, 6, 7, 8]);
  assert.equal(appendProxy(policy, 2), policy);
  assert.equal(policy.allowDirect, false);
});
test("moving a proxy changes priority and preserves blocked/direct policy", () => {
  const policy = { proxyIds: [1, 2, 3], allowDirect: false };
  assert.deepEqual(moveProxy(policy, 2, -1), { proxyIds: [1, 3, 2], allowDirect: false });
  assert.equal(moveProxy(policy, 0, -1), policy);
  assert.equal(moveProxy(policy, 2, 1), policy);
  assert.deepEqual(policy.proxyIds, [1, 2, 3]);
});
test("every API profile type has its settings label", () => {
  assert.deepEqual(proxyLabels, { HTTP_CONNECT: "HTTP CONNECT", SOCKS5: "SOCKS5", SSH: "SSH", WIRE_GUARD: "WireGuard" });
});
