import { expect, graphql, test } from "./helpers";
import { waitTerminal } from "./support/downloads";
import {
  type NetworkFlow, type ProxyProfile, type ProxyProfileInput, directLeg, flowAfter, flowMark, ladderLeg, legOn,
  proxyProfiles, resetProxyHostKey, rung, saveProxyProfile, serverKey,
} from "./support/network-flow";
import { NetworkWorld, PROXIED_HOST, saveEvidence } from "./support/network-scenario";
import { fixtureEvents, fixtureMark } from "./support/proxy-fixture";
import { controlTunnel, tunnelMark, tunnelState, waitTunnelEvents } from "./support/tunnel-fixture";

/** Sessions: SSH, WireGuard, HTTP/3. */
let world: NetworkWorld;
test.beforeEach(async ({ request }) => { world = await NetworkWorld.create(request); });
test.afterEach(async ({}, info) => { await world.cleanup(info); });

/** Re-save a profile from its stored shape; secrets left out are kept. */
function resaveInput(profile: ProxyProfile, overrides: Partial<ProxyProfileInput> = {}): ProxyProfileInput {
  return {
    name: profile.name, kind: profile.kind, enabled: profile.enabled, host: profile.host, port: profile.port,
    dnsServers: profile.dnsServers, tunnelAddresses: profile.tunnelAddresses, peerPublicKey: profile.peerPublicKey,
    mtu: profile.mtu, keepaliveSeconds: profile.keepaliveSeconds, timeoutSeconds: profile.timeoutSeconds, ...overrides,
  };
}

/** Distinct auth methods the fixture accepted after the first `seen` entries; the list grows for the fixture's lifetime. */
function authSince(accepted: string[], seen: number): string[] {
  const since = accepted.slice(seen);
  expect(since.length, "at least one authentication since the snapshot").toBeGreaterThan(0);
  return [...new Set(since)].sort();
}

async function sshServer(profile: ProxyProfile) {
  const a = await world.egress("a");
  const server = await world.server({ host: "nntp", route: { legs: [ladderLeg(a.id, [rung.proxy(profile.id)], 100)] } });
  const leg = (flow: NetworkFlow) => legOn(flow, serverKey(server), a.id);
  return { a, server, leg };
}

test("S01 SSH password auth forwards to the NNTP server", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const ssh1 = await world.ssh("ssh1");
  const seen = (await tunnelState(request)).ssh.ssh1!.acceptedAuth.length;
  await sshServer(ssh1);
  const download = await world.download("s01-password");
  expect(await waitTerminal(request, download.jobId)).toBe("COMPLETED");
  const state = (await tunnelState(request)).ssh.ssh1!;
  expect(authSince(state.acceptedAuth, seen)).toEqual(["password"]);
  expect(state.forwarded).toContainEqual(["nntp", 119]);
});

test("S02 SSH key and passphrase-protected key auth; a wrong passphrase takes the leg Down", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  for (const [endpoint, auth] of [["ssh2", "key"], ["ssh3", "passphrase"]] as const) {
    const profile = await world.ssh(endpoint, { auth });
    const seen = (await tunnelState(request)).ssh[endpoint]!.acceptedAuth.length;
    const { server } = await sshServer(profile);
    const download = await world.download(`s02-${auth}`);
    expect(await waitTerminal(request, download.jobId)).toBe("COMPLETED");
    expect(authSince((await tunnelState(request)).ssh[endpoint]!.acceptedAuth, seen), endpoint).toEqual(["publickey"]);
    await world.updateServer(server, { active: false });
  }
  const wrong = await world.ssh("ssh3", { auth: "wrong-passphrase" });
  const { leg } = await sshServer(wrong);
  const mark = await flowMark(request);
  const download = await world.download("s02-wrong-passphrase");
  try {
    const flow = await flowAfter(request, mark, sample => leg(sample)?.state === "DOWN" || leg(sample)?.state === "BLOCKED", "leg Down on the key error");
    expect(leg(flow)?.reason?.trim()).toBeTruthy();
  } finally {
    // The job can never download through a leg that cannot authenticate.
    await graphql(request, "mutation($id: Int!) { cancelJob(id: $id) }", { id: download.jobId });
  }
});

test("S03 a host key that changes after first use blocks the leg until reset", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const profile = await world.ssh("ssh-switch");
  const { leg } = await sshServer(profile);
  const first = await world.download("s03-first");
  expect(await waitTerminal(request, first.jobId)).toBe("COMPLETED");
  await expect.poll(async () => (await proxyProfiles(request)).find(candidate => candidate.id === profile.id)?.hostKeyFingerprint ?? null,
    { timeout: 0, message: "host key pinned on first use" }).not.toBeNull();
  await controlTunnel(request, { endpoint: "ssh-switch", hostKey: "other", cut: true });
  const mark = await flowMark(request);
  const second = await world.download("s03-second");
  const blocked = await flowAfter(request, mark, sample => leg(sample)?.state === "BLOCKED", "leg Blocked");
  expect(leg(blocked)?.reason).toContain("host key changed");
  const reset = await resetProxyHostKey(request, profile.id);
  expect(reset.hostKeyFingerprint).toBeNull();
  const resaved = await flowMark(request);
  await saveProxyProfile(request, resaveInput(reset), profile.id);
  await flowAfter(request, resaved, sample => leg(sample)?.state === "UP" || leg(sample)?.state === "IDLE", "leg Up after the reset");
  expect(await waitTerminal(request, second.jobId)).toBe("COMPLETED");
});

test("S04 an SSH server that refuses forwarding takes only its leg Down", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const a = await world.egress("a");
  const b = await world.egress("b");
  const refuse = await world.ssh("ssh-refuse", { network: "b" });
  const server = await world.server({ host: "nntp", route: { legs: [directLeg(a.id, 50), ladderLeg(b.id, [rung.proxy(refuse.id)], 50)] } });
  const mark = await flowMark(request);
  const download = await world.download("s04-refuse");
  const flow = await flowAfter(request, mark, sample => legOn(sample, serverKey(server), b.id)?.state === "DOWN", "leg B Down");
  expect(legOn(flow, serverKey(server), b.id)?.reason?.trim()).toBeTruthy();
  expect(await waitTerminal(request, download.jobId)).toBe("COMPLETED");
});

test("S05 two servers on one SSH profile share a session that closes when both routes go", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const a = await world.egress("a");
  const ssh1 = await world.ssh("ssh1");
  const mark = await tunnelMark(request);
  const first = await world.server({ host: "nntp", route: { legs: [ladderLeg(a.id, [rung.proxy(ssh1.id)], 100)] } });
  const second = await world.server({ host: "nntp", route: { legs: [ladderLeg(a.id, [rung.proxy(ssh1.id)], 100)] } });
  // Both jobs are in flight together, so the session is busy throughout and
  // one connect is the only correct count.
  const paced = await world.pacedDownload("s05-one", { parts: 64, slowMs: 500 });
  const second = await world.download("s05-two");
  expect(await paced.release()).toBe("COMPLETED");
  expect(await waitTerminal(request, second.jobId)).toBe("COMPLETED");
  const flow = await flowAfter(request, await flowMark(request), sample => [first, second].every(id => legOn(sample, serverKey(id), a.id) !== undefined), "both legs present");
  const connects = (await waitTunnelEvents(request, mark, events => events.some(event => event.kind === "connect" && event.endpoint === "ssh1"), "an SSH session on ssh1"))
    .filter(event => event.kind === "connect" && event.endpoint === "ssh1");
  await saveEvidence(test.info(), "S05", { connects, legs: [first, second].map(id => legOn(flow, serverKey(id), a.id)) });
  expect(connects).toHaveLength(1);
  const removed = await tunnelMark(request);
  for (const id of [first, second]) {
    await graphql(request, "mutation($id: Int!) { removeServer(id: $id) { id } }", { id });
    world.servers.splice(world.servers.indexOf(id), 1);
  }
  await waitTunnelEvents(request, removed, events => events.some(event => event.kind === "disconnect" && event.endpoint === "ssh1"), "the shared session closed");
});

test("S06 a lost SSH endpoint takes the leg Down and the session is rebuilt when it returns", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  // The endpoint is taken down through the tunnel fixture's control API,
  // which refuses and closes exactly as a stopped container would.
  const ssh1 = await world.ssh("ssh1");
  const { leg } = await sshServer(ssh1);
  const warm = await world.download("s06-warm");
  expect(await waitTerminal(request, warm.jobId)).toBe("COMPLETED");
  await controlTunnel(request, { endpoint: "ssh1", up: false, cut: true });
  const mark = await flowMark(request);
  const download = await world.download("s06-after-stop");
  const down = await flowAfter(request, mark, sample => leg(sample)?.state === "DOWN", "leg Down");
  await controlTunnel(request, { endpoint: "ssh1", up: true });
  await flowAfter(request, down.sampledAt, sample => leg(sample)?.state === "UP" || leg(sample)?.state === "PROBING", "leg reconnecting");
  expect(await waitTerminal(request, download.jobId)).toBe("COMPLETED");
});

test("S07 WireGuard carries NNTP and RSS through the tunnel; a wrong preshared key takes the leg Down", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const a = await world.egress("a");
  const wg2 = await world.wireguard("wg2");
  await world.server({ host: "nntp.proxy.test", route: { legs: [ladderLeg(a.id, [rung.proxy(wg2.id)], 100)] } });
  const download = await world.download("s07-wireguard");
  expect(await waitTerminal(request, download.jobId)).toBe("COMPLETED");
  const url = await world.armFeed(`s07-${Date.now()}`, "rss.proxy.test");
  const feed = await world.feed({ url, route: { legs: [ladderLeg(a.id, [rung.proxy(wg2.id)], 100)] } });
  expect((await world.sync(feed)).errors).toEqual([]);
  const peer = (await tunnelState(request)).wireguard.wg2!;
  expect(peer.dnsQueries).toEqual(expect.arrayContaining(["nntp.proxy.test", "rss.proxy.test"]));
  expect(peer.requests.length).toBeGreaterThan(0);
  const wrong = await world.wireguard("wg2", { wrongPresharedKey: true });
  const b = await world.egress("b");
  const server = await world.server({ host: "nntp.proxy.test", route: { legs: [ladderLeg(b.id, [rung.proxy(wrong.id)], 100)] } });
  const mark = await flowMark(request);
  await world.download("s07-wrong-psk");
  const flow = await flowAfter(request, mark, sample => legOn(sample, serverKey(server), b.id)?.state === "DOWN", "leg Down with the wrong preshared key");
  expect(legOn(flow, serverKey(server), b.id)?.reason?.trim()).toBeTruthy();
});

test.fixme("S08 HTTP/3 CONNECT sessions", () => {
  // Missing observable: an HTTP/3 endpoint Weaver will complete a handshake
  // with. Weaver's HTTP/3 client trusts only the webpki roots, so the
  // fixture's self-signed certificate is refused before any CONNECT is
  // recorded. The chain-ordering half ([connect1, h3] rejected) is in F14.
});

test("S09 a profile keeps an omitted secret and clears a null one", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const a = await world.egress("a");
  const connect1 = await world.connect("connect1");
  const connect2 = await world.connect("connect2");
  const kept = await saveProxyProfile(request, resaveInput(connect1, { username: "fixture" }), connect1.id);
  expect(kept.hasPassword).toBe(true);
  const cleared = await saveProxyProfile(request, resaveInput(connect1, { username: "fixture", password: null }), connect1.id);
  expect(cleared.hasPassword).toBe(false);
  // F07's behaviour: the proxy now answers 407 and the ladder moves on.
  const server = await world.server({ host: PROXIED_HOST, route: { legs: [ladderLeg(a.id, [rung.proxy(connect1.id), rung.proxy(connect2.id)], 100)] } });
  const download = await world.download("s09-cleared");
  const flow = await flowAfter(request, await flowMark(request), sample => legOn(sample, serverKey(server), a.id)?.selectedRung === 1, "leg on rung 1");
  expect(legOn(flow, serverKey(server), a.id)?.rungStates[0]).toBe("COOLDOWN");
  expect(await waitTerminal(request, download.jobId)).toBe("COMPLETED");
});

test("S10 a disabled profile's rung is unavailable and the next rung carries the leg", async ({ request }) => {
  test.setTimeout(10 * 60_000);
  const a = await world.egress("a");
  const ssh1 = await world.ssh("ssh1");
  await saveProxyProfile(request, resaveInput(ssh1, { enabled: false }), ssh1.id);
  const connect1 = await world.connect("connect1");
  const server = await world.server({ host: PROXIED_HOST, route: { legs: [ladderLeg(a.id, [rung.proxy(ssh1.id), rung.proxy(connect1.id)], 100)] } });
  const mark = await fixtureMark(request);
  const download = await world.download("s10-disabled");
  const flow = await flowAfter(request, await flowMark(request), sample => legOn(sample, serverKey(server), a.id)?.selectedRung === 1, "leg on rung 1");
  expect(legOn(flow, serverKey(server), a.id)?.selectedProxyId).toBe(connect1.id);
  expect(legOn(flow, serverKey(server), a.id)?.rungStates[0]?.trim()).toBeTruthy();
  expect(await waitTerminal(request, download.jobId)).toBe("COMPLETED");
  expect((await fixtureEvents(request, mark)).some(event => event.kind === "connected" && event.route === "connect1")).toBe(true);
});
