// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { test } from "node:test";
import { announceHosts, hostPatterns, patternsOf, torrentAnnounces, trackerHost } from "../src/lib/tracker-hosts.ts";

const bytes = (text: string) => new TextEncoder().encode(text);
const str = (text: string) => `${bytes(text).length}:${text}`;

test("a tracker's host is read without its passkey, port or case", () => {
  assert.equal(trackerHost("https://Tracker.Example.org./a1b2c3/announce"), "tracker.example.org");
  assert.equal(trackerHost("udp://tracker.opentrackr.org:1337/announce"), "tracker.opentrackr.org");
  assert.equal(trackerHost("UDP://Tracker.Example.org.:80"), "tracker.example.org");
  assert.equal(trackerHost("udp://bücher.example:80"), "xn--bcher-kva.example");
  assert.equal(trackerHost("udp://0x7f.1:6969"), "127.0.0.1");
  assert.equal(trackerHost("http://[::1]:6969/announce"), null);
  assert.equal(trackerHost("not a url"), null);
});

test("a host is offered itself and the wildcard over its domain", () => {
  assert.deepEqual(hostPatterns("tracker.example.org"), ["tracker.example.org", "*.example.org"]);
  assert.deepEqual(hostPatterns("example.org"), ["example.org", "*.example.org"]);
  assert.deepEqual(hostPatterns("10.0.0.2"), ["10.0.0.2"]);
  assert.deepEqual(hostPatterns("localhost"), ["localhost"]);
});

test("known hosts give each pattern once, in the hosts' order", () => {
  assert.deepEqual(patternsOf(["a.example.org", "b.example.org", "tracker.other.net", "[::1]"]), [
    "a.example.org",
    "*.example.org",
    "b.example.org",
    "tracker.other.net",
    "*.other.net",
  ]);
});

test("a torrent file's trackers are read past its info dictionary", () => {
  const info = `d${str("length")}i1048576e${str("name")}${str("file.bin")}${str("piece length")}i262144e${str("pieces")}20:${"x".repeat(20)}e`;
  const announceList = `l l${str("https://t1.example.org/k/announce")}e l${str("udp://t2.other.net:6969")}e e`.replaceAll(" ", "");
  const file = `d${str("announce")}${str("https://t1.example.org/k/announce")}${str("announce-list")}${announceList}${str("info")}${info}e`;
  const announces = torrentAnnounces(bytes(file));
  assert.deepEqual(announces, ["https://t1.example.org/k/announce", "udp://t2.other.net:6969"]);
  assert.deepEqual(patternsOf(announceHosts(announces ?? [])), ["t1.example.org", "*.example.org", "t2.other.net", "*.other.net"]);
});

test("a torrent without trackers reads as none, and other bytes as no torrent", () => {
  assert.deepEqual(torrentAnnounces(bytes(`d${str("info")}d${str("name")}${str("a")}ee`)), []);
  assert.equal(torrentAnnounces(bytes("d8:announce")), null);
  assert.equal(torrentAnnounces(bytes("<html></html>")), null);
  assert.equal(torrentAnnounces(bytes(`d${str("announce")}${str("https://t.example.org")}e`)), null);
});
