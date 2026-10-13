import assert from "node:assert/strict";
import test from "node:test";

import { parseWireguardConfig, wireguardConfigProblems } from "../src/lib/wireguard-config.ts";

const CONFIG = `# provided by the VPN
[Interface]
PrivateKey = cHJpdmF0ZQ==
Address = 10.6.0.2/32, fd00::2/128
DNS = 10.6.0.1
MTU = 1420
ListenPort = 51820

[Peer]
PublicKey = cGVlcg==
PresharedKey = cHNr
AllowedIPs = 0.0.0.0/0, ::/0
Endpoint = vpn.example.com:51820   # main
PersistentKeepalive = 25
`;

test("a whole configuration fills every field the form has", () => {
  const parsed = parseWireguardConfig(CONFIG);
  assert.ok(parsed);
  assert.equal(parsed.privateKey, "cHJpdmF0ZQ==");
  assert.equal(parsed.peerPublicKey, "cGVlcg==");
  assert.equal(parsed.presharedKey, "cHNr");
  assert.equal(parsed.endpoint, "vpn.example.com:51820");
  assert.equal(parsed.tunnelAddresses, "10.6.0.2/32\nfd00::2/128");
  assert.equal(parsed.tunnelDnsServers, "10.6.0.1");
  assert.equal(parsed.tunnelMtu, "1420");
  assert.equal(parsed.tunnelKeepaliveSeconds, "25");
  assert.equal(parsed.peerCount, 1);
  // Reported rather than silently dropped: an operator who wrote them deserves
  // to know a tunnel proxy does not use them.
  assert.deepEqual(parsed.ignored, ["ListenPort", "AllowedIPs"]);
});

test("a fragment with no section headers still reads", () => {
  const parsed = parseWireguardConfig(
    "PrivateKey = cHJpdmF0ZQ==\nPublicKey = cGVlcg==\nEndpoint = vpn.test:51820",
  );
  assert.ok(parsed);
  assert.equal(parsed.privateKey, "cHJpdmF0ZQ==");
  assert.equal(parsed.peerPublicKey, "cGVlcg==");
  assert.equal(parsed.endpoint, "vpn.test:51820");
  assert.equal(parsed.peerCount, 0);
});

test("only the first peer is imported, and the file says how many there were", () => {
  const parsed = parseWireguardConfig(`[Interface]
PrivateKey = cHJpdmF0ZQ==
Address = 10.6.0.2/32

[Peer]
PublicKey = Zmlyc3Q=
Endpoint = first.test:51820

[Peer]
PublicKey = c2Vjb25k
Endpoint = second.test:51820
`);
  assert.ok(parsed);
  assert.equal(parsed.peerCount, 2);
  assert.equal(parsed.peerPublicKey, "Zmlyc3Q=");
  assert.equal(parsed.endpoint, "first.test:51820");
});

test("repeated address lines and a keepalive of zero survive", () => {
  const parsed = parseWireguardConfig(`[Interface]
Address = 10.6.0.2/32
Address = fd00::2/128
PrivateKey = cHJpdmF0ZQ==

[Peer]
PublicKey = cGVlcg==
PersistentKeepalive = 0
`);
  assert.ok(parsed);
  assert.equal(parsed.tunnelAddresses, "10.6.0.2/32\nfd00::2/128");
  // Zero is a value: it switches keepalive off, which is not the same as
  // leaving the engine's default in place.
  assert.equal(parsed.tunnelKeepaliveSeconds, "0");
});

test("text that is not a configuration is refused rather than half-read", () => {
  assert.equal(parseWireguardConfig(""), null);
  assert.equal(parseWireguardConfig("the quick brown fox"), null);
  assert.equal(parseWireguardConfig("[Interface]\n# nothing else"), null);
  // A key with no value tells us nothing, so it does not count as recognised.
  assert.equal(parseWireguardConfig("PrivateKey ="), null);
});

const KEY = (letter: string) => `${letter.repeat(43)}=`;
const WHOLE = `[Interface]
PrivateKey = ${KEY("a")}
Address = 10.6.0.2/32

[Peer]
PublicKey = ${KEY("b")}
Endpoint = vpn.example.com:51820
`;
const problems = (text: string) => {
  const parsed = parseWireguardConfig(text);
  assert.ok(parsed);
  return wireguardConfigProblems(parsed);
};

test("a whole configuration has nothing wrong with it, with or without what is optional", () => {
  assert.deepEqual(problems(WHOLE), []);
  assert.deepEqual(
    problems(`${WHOLE}PresharedKey = ${KEY("c")}\nPersistentKeepalive = off\n[Interface]\nMTU = 1380\nDNS = 10.6.0.1`),
    [],
  );
  assert.deepEqual(problems(WHOLE.replace("vpn.example.com:51820", "[2001:db8::1]:51820")), []);
});

test("every fault of a configuration is listed at once, by the key at fault", () => {
  assert.deepEqual(problems("MTU = big\nPersistentKeepalive = often\nPresharedKey = short"), [
    { kind: "missing", key: "PrivateKey", section: "Interface" },
    { kind: "missing", key: "Address", section: "Interface" },
    { kind: "number", key: "MTU" },
    { kind: "missing", key: "PublicKey", section: "Peer" },
    { kind: "key", key: "PresharedKey" },
    { kind: "missing", key: "Endpoint", section: "Peer" },
    { kind: "number", key: "PersistentKeepalive" },
  ]);
  assert.deepEqual(problems(WHOLE.replace(KEY("a"), "cHJpdmF0ZQ==").replace(KEY("b"), "cGVlcg==")), [
    { kind: "key", key: "PrivateKey" },
    { kind: "key", key: "PublicKey" },
  ]);
});

test("an endpoint is a host and a port", () => {
  for (const endpoint of ["vpn.example.com", "vpn.example.com:0", "vpn.example.com:70000", "vpn.example.com:wg"]) {
    assert.deepEqual(problems(WHOLE.replace("vpn.example.com:51820", endpoint)), [{ kind: "endpoint" }], endpoint);
  }
});
