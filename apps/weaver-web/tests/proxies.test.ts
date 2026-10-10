import test from "node:test";
import assert from "node:assert/strict";
import { appendProxy, chainHopProfiles, moveProxy, directRouting, proxyLabels } from "../src/lib/proxies.ts";

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
  assert.deepEqual(proxyLabels, { HTTP_CONNECT: "HTTP CONNECT", HTTP3_CONNECT: "HTTP/3 CONNECT", SOCKS5: "SOCKS5", SSH: "SSH", WIRE_GUARD: "WireGuard" });
});
test("a later chain hop offers WireGuard only on a WireGuard hop and never HTTP/3", () => {
  const profiles = [
    { id: 1, kind: "WIRE_GUARD" as const },
    { id: 2, kind: "WIRE_GUARD" as const },
    { id: 3, kind: "SOCKS5" as const },
    { id: 4, kind: "HTTP3_CONNECT" as const },
  ];
  const ids = (list: { id: number }[]) => list.map((profile) => profile.id);
  assert.deepEqual(ids(chainHopProfiles(profiles, [], 0)), [1, 2, 3, 4]);
  assert.deepEqual(ids(chainHopProfiles(profiles, [1], 1)), [1, 2, 3]);
  assert.deepEqual(ids(chainHopProfiles(profiles, [3], 1)), [3]);
  assert.deepEqual(ids(chainHopProfiles(profiles, [4], 1)), [3]);
  // A saved stack keeps its later WireGuard hop selectable.
  assert.deepEqual(ids(chainHopProfiles(profiles, [1, 2], 1)), [1, 2, 3]);
  assert.deepEqual(ids(chainHopProfiles(profiles, [1, 2, 3], 2)), [1, 2, 3]);
});
