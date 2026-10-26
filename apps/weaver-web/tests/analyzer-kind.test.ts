// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { test } from "node:test";
import { fileKind, isMagnet, maxBytes, NZB_MAX_BYTES, TORRENT_MAX_BYTES } from "../src/next/data/analyzer.ts";

const bytes = (text: string) => new TextEncoder().encode(text);

test("a bencoded dictionary is a torrent whatever its name", () => {
  assert.equal(fileKind(bytes("d8:announce35:udp://tracker.example:6969/announce4:infod")), "torrent");
  assert.equal(fileKind(bytes("d4:infod6:lengthi1ee")), "torrent");
});

test("xml, compressed and unknown bytes go to the nzb reader", () => {
  assert.equal(fileKind(bytes('<?xml version="1.0"?><nzb>')), "nzb");
  assert.equal(fileKind(new Uint8Array([0xef, 0xbb, 0xbf, 0x3c])), "nzb");
  assert.equal(fileKind(new Uint8Array([0x1f, 0x8b, 0x08])), "nzb");
  assert.equal(fileKind(new Uint8Array([0x28, 0xb5, 0x2f, 0xfd])), "nzb");
  assert.equal(fileKind(bytes("data")), "nzb");
  assert.equal(fileKind(bytes("d")), "nzb");
  assert.equal(fileKind(new Uint8Array()), "nzb");
});

test("a magnet link is told from other text", () => {
  assert.ok(isMagnet("magnet:?xt=urn:btih:0000000000000000000000000000000000000000"));
  assert.ok(isMagnet("  MAGNET:?xt=urn:btih:00\n"));
  assert.ok(!isMagnet("https://tracker.example/file.torrent"));
  assert.ok(!isMagnet("magnet"));
});

test("each kind has its own size limit", () => {
  assert.equal(maxBytes("nzb"), NZB_MAX_BYTES);
  assert.equal(maxBytes("torrent"), TORRENT_MAX_BYTES);
});
