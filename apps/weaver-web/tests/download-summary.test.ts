// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import {
  downloadPanelState,
  downloadTree,
  nodePassword,
  passwordOriginGroup,
  serverWord,
  type DownloadNodeData,
  type DownloadTreeNode,
} from "../src/next/data/download-summary.ts";
import { englishTranslate as t } from "./english-translate.ts";

const node = (id: number, parentId: number | null): DownloadNodeData => ({
  id,
  parentId,
  kind: "rar_store",
  name: `n${id}`,
  volumes: 1,
  extracted: false,
  givenUp: false,
  reason: null,
  encrypted: false,
  encryptedHeaders: false,
  passwordNeeded: false,
  passwordOrigin: null,
});

type Shape = [number, Shape[]] | number;
const shape = (branches: DownloadTreeNode[]): Shape[] =>
  branches.map((branch) =>
    branch.children.length === 0 ? branch.node.id : [branch.node.id, shape(branch.children)],
  );

test("each node sits under its parent", () => {
  // Breadth first, as stored: two at the top, then their children.
  const nodes = [node(2, null), node(5, null), node(9, 2), node(7, 5), node(11, 9)];
  assert.deepEqual(shape(downloadTree(nodes)), [
    [2, [[9, [11]]]],
    [5, [7]],
  ]);
});

test("siblings read in id order", () => {
  assert.deepEqual(shape(downloadTree([node(8, null), node(3, null)])), [3, 8]);
});

test("a node whose parent was cut off by the cap is a root", () => {
  assert.deepEqual(shape(downloadTree([node(4, 1), node(6, 4)])), [[4, [6]]]);
});

test("a loop of parents still shows every node once", () => {
  assert.deepEqual(shape(downloadTree([node(3, 4), node(4, 3), node(1, null)])), [1, [3, [4]]]);
});

test("a flat download has no tree", () => {
  assert.deepEqual(downloadTree([]), []);
});

test("passwords group as NZB, API and password list", () => {
  for (const origin of ["NZB_META", "FILENAME_CONVENTION", "DNZB_HEADER"] as const) {
    assert.equal(passwordOriginGroup(origin), "nzb", origin);
  }
  for (const origin of ["EXPLICIT", "INTEGRATION"] as const) {
    assert.equal(passwordOriginGroup(origin), "api", origin);
  }
  for (const origin of ["SETTINGS", "PASSWORD_FILE"] as const) {
    assert.equal(passwordOriginGroup(origin), "list", origin);
  }
  assert.equal(passwordOriginGroup(null), null);
});

test("a node opened from a source a newer server names is not one that needs a password", () => {
  const locked = { ...node(1, null), encrypted: true, passwordNeeded: true };
  assert.deepEqual(nodePassword({ ...locked, passwordOrigin: "SETTINGS" }), { state: "origin", group: "list" });
  assert.deepEqual(nodePassword({ ...locked, passwordOrigin: "SOMETHING_NEW" }), { state: "opened" });
  assert.deepEqual(nodePassword(locked), { state: "needed" });
  assert.equal(nodePassword(node(2, null)), null);
});

test("server words are named, and an unknown one passes through", () => {
  assert.equal(serverWord(t, "kind", "sevenz_engine"), "7z");
  assert.equal(serverWord(t, "outcome", "impossible"), "Cannot repair");
  assert.equal(serverWord(t, "reason", "too_deep"), "Nested too deep");
  assert.equal(serverWord(t, "kind", "future_kind"), "future_kind");
});

test("a queued job waits; a finished one without a summary says so", () => {
  assert.equal(downloadPanelState(true, null), "queued");
  assert.equal(downloadPanelState(false, null), "unrecorded");
  assert.equal(
    downloadPanelState(false, {
      layout: "PLAIN",
      nodes: [],
      nodeCount: 0,
      maxDepth: 0,
      recovery: [],
      recoveryCount: 0,
      post: {
        files: 1,
        articles: 1,
        missingArticles: 0,
        failedArticles: 0,
        oldestPost: null,
        newestPost: null,
        groups: 1,
      },
    }),
    "summary",
  );
});
