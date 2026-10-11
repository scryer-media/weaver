import test from "node:test";
import assert from "node:assert/strict";
import { appendProxy, chainHopProfiles, moveProxy, directRouting, proxyLabels, withChainHop } from "../src/lib/proxies.ts";

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
  assert.deepEqual(ids(chainHopProfiles(profiles, [1], 1)), [2, 3]);
  assert.deepEqual(ids(chainHopProfiles(profiles, [3], 1)), []);
  assert.deepEqual(ids(chainHopProfiles(profiles, [4], 1)), [3]);
  // A saved stack keeps its later WireGuard hop selectable.
  assert.deepEqual(ids(chainHopProfiles(profiles, [1, 2], 1)), [2, 3]);
  assert.deepEqual(ids(chainHopProfiles(profiles, [1, 2, 3], 2)), [3]);
});
test("a chain hop never offers a proxy the chain or its ladder already uses", () => {
  const profiles = [
    { id: 1, kind: "WIRE_GUARD" as const },
    { id: 2, kind: "WIRE_GUARD" as const },
    { id: 3, kind: "SOCKS5" as const },
    { id: 5, kind: "SSH" as const },
  ];
  const ids = (list: { id: number }[]) => list.map((profile) => profile.id);
  // The first hop gives up what a later hop holds, and each keeps its own choice listed.
  assert.deepEqual(ids(chainHopProfiles(profiles, [1, 3], 0)), [1, 2, 5]);
  assert.deepEqual(ids(chainHopProfiles(profiles, [1, 3], 1)), [2, 3, 5]);
  assert.deepEqual(ids(chainHopProfiles(profiles, [1, 3], 2)), [5]);
  // Nothing rides SSH but a proxy that needs no UDP.
  assert.deepEqual(ids(chainHopProfiles(profiles, [5], 1)), [3]);
  // Another rung's proxy, or a pool's member, is the ladder's already.
  assert.deepEqual(ids(chainHopProfiles(profiles, [1], 1, { taken: new Set([2, 5]) })), [3]);
  assert.deepEqual(ids(chainHopProfiles(profiles, [], 0, { taken: new Set([1]) })), [2, 3, 5]);
});
test("a feed's chain offers only proxies that have DNS servers", () => {
  const profiles = [
    { id: 1, kind: "SOCKS5" as const, dnsServers: ["192.0.2.53"] },
    { id: 2, kind: "SOCKS5" as const, dnsServers: [] },
    { id: 3, kind: "SSH" as const, dnsServers: ["192.0.2.53"] },
  ];
  const ids = (list: { id: number }[]) => list.map((profile) => profile.id);
  assert.deepEqual(ids(chainHopProfiles(profiles, [], 0, { needsDns: true })), [1, 3]);
  assert.deepEqual(ids(chainHopProfiles(profiles, [], 0)), [1, 2, 3]);
});
test("changing a chain hop keeps the hops above it only while they can still follow", () => {
  const profiles = [
    { id: 1, kind: "WIRE_GUARD" as const },
    { id: 2, kind: "WIRE_GUARD" as const },
    { id: 3, kind: "SOCKS5" as const },
    { id: 5, kind: "SSH" as const },
  ];
  assert.deepEqual(withChainHop(profiles, [], 0, 1), [1]);
  assert.deepEqual(withChainHop(profiles, [1], 1, 2), [1, 2]);
  // WireGuard no longer rides once the hop beneath it is SSH, and what sat on it goes too.
  assert.deepEqual(withChainHop(profiles, [1, 2, 3], 0, 5), [5]);
  assert.deepEqual(withChainHop(profiles, [1, 3, 5], 0, 2), [2, 3, 5]);
  // A hop picked again further down is not kept above.
  assert.deepEqual(withChainHop(profiles, [1, 3], 0, 3), [3]);
  // Taking a hop out moves the ones above it down, where they still fit.
  assert.deepEqual(withChainHop(profiles, [1, 3, 5], 1, null), [1, 5]);
  assert.deepEqual(withChainHop(profiles, [5, 3, 1], 0, null), [3]);
  assert.deepEqual(withChainHop(profiles, [1, 3], 1, null), [1]);
});
