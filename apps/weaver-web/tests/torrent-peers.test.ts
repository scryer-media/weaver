// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import {
  DEFAULT_PEER_SORT,
  PEER_PAGE_SIZE,
  nextPeerSort,
  peerPage,
  peerPageCount,
  peerSortFor,
  peersOfPage,
  sortPeers,
} from "../src/next/features/torrents/peers.ts";

const peer = (
  address: string,
  client = "",
  progress = 0,
  downloadBps = 0,
  uploadBps = 0,
) => ({
  address,
  client,
  progress,
  downloadBps,
  uploadBps,
});
const addresses = (peers: { address: string }[]) =>
  peers.map((one) => one.address);

test("peers start by address, each number of it counted as a number", () => {
  const peers = [
    peer("23.1.1.1:9"),
    peer("5.180.2.2:80"),
    peer("5.79.3.3:700"),
    peer("5.79.3.3:81"),
  ];
  assert.deepEqual(addresses(sortPeers(peers, DEFAULT_PEER_SORT)), [
    "5.79.3.3:81",
    "5.79.3.3:700",
    "5.180.2.2:80",
    "23.1.1.1:9",
  ]);
  // The peers handed in keep their order.
  assert.equal(peers[0].address, "23.1.1.1:9");
});

test("a heading sorts by its column, and a second click turns the order round", () => {
  const peers = [
    peer("1.1.1.1:1", "qBittorrent", 0.5, 10, 0),
    peer("2.2.2.2:2", "Deluge", 1, 300, 5),
  ];
  // A figure starts from the largest, words from A.
  const byDown = nextPeerSort(DEFAULT_PEER_SORT, "downloadBps");
  assert.deepEqual(byDown, { key: "downloadBps", descending: true });
  assert.deepEqual(addresses(sortPeers(peers, byDown)), [
    "2.2.2.2:2",
    "1.1.1.1:1",
  ]);
  assert.deepEqual(
    addresses(sortPeers(peers, nextPeerSort(byDown, "downloadBps"))),
    ["1.1.1.1:1", "2.2.2.2:2"],
  );
  const byClient = nextPeerSort(byDown, "client");
  assert.deepEqual(byClient, { key: "client", descending: false });
  assert.deepEqual(addresses(sortPeers(peers, byClient)), [
    "2.2.2.2:2",
    "1.1.1.1:1",
  ]);
  assert.deepEqual(
    addresses(sortPeers(peers, { key: "progress", descending: true })),
    ["2.2.2.2:2", "1.1.1.1:1"],
  );
  assert.deepEqual(
    addresses(sortPeers(peers, { key: "uploadBps", descending: false })),
    ["1.1.1.1:1", "2.2.2.2:2"],
  );
});

test("peers alike in the sorted column go by address, whichever way it runs", () => {
  const peers = [
    peer("9.9.9.9:1", "x", 1),
    peer("1.1.1.1:1", "x", 1),
    peer("4.4.4.4:1", "x", 0.2),
  ];
  assert.deepEqual(
    addresses(sortPeers(peers, { key: "progress", descending: true })),
    ["1.1.1.1:1", "9.9.9.9:1", "4.4.4.4:1"],
  );
});

test("peers show a page of 25 at a time, and a page emptied by peers leaving gives way to the last", () => {
  const peers = Array.from({ length: 98 }, (_, index) =>
    peer(`10.0.0.${index}:1`),
  );
  assert.equal(PEER_PAGE_SIZE, 25);
  assert.equal(peerPageCount(98), 4);
  assert.equal(peerPageCount(25), 1);
  assert.equal(peerPageCount(0), 1);
  assert.equal(peersOfPage(peers, 0).length, 25);
  assert.equal(peersOfPage(peers, 0)[0].address, "10.0.0.0:1");
  assert.equal(peersOfPage(peers, 3).length, 23);
  assert.equal(peersOfPage(peers, 3)[0].address, "10.0.0.75:1");
  // Page 4 of 4 was open and the peers fell to 30: page 2 shows.
  assert.equal(peerPage(3, 30), 1);
  assert.deepEqual(
    addresses(peersOfPage(peers.slice(0, 30), 3)),
    addresses(peers.slice(25, 30)),
  );
  assert.deepEqual(peersOfPage([], 2), []);
});

test("until a heading is clicked, a download shows its fastest senders and a seed its fastest receivers", () => {
  const down = { key: "downloadBps", descending: true };
  const up = { key: "uploadBps", descending: true };
  assert.deepEqual(peerSortFor({ state: "DOWNLOADING", progress: 0.4 }), down);
  assert.deepEqual(peerSortFor({ state: "STALLED", progress: 0.4 }), down);
  assert.deepEqual(peerSortFor({ state: "PAUSED", progress: 0.4 }), down);
  assert.deepEqual(peerSortFor({ state: "SEEDING", progress: 0.7 }), up);
  assert.deepEqual(peerSortFor({ state: "PAUSED", progress: 1 }), up);
  const peers = [
    peer("1.1.1.1:1", "", 1, 900, 5),
    peer("2.2.2.2:2", "", 0, 10, 400),
  ];
  assert.deepEqual(
    addresses(
      sortPeers(peers, peerSortFor({ state: "DOWNLOADING", progress: 0.4 })),
    ),
    ["1.1.1.1:1", "2.2.2.2:2"],
  );
  assert.deepEqual(
    addresses(sortPeers(peers, peerSortFor({ state: "SEEDING", progress: 1 }))),
    ["2.2.2.2:2", "1.1.1.1:1"],
  );
});
