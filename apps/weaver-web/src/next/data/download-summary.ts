// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

/**
 * How a finished job's download went, as the job page shows it: the archives
 * as a nested list, and where an archive's password came from.
 */

export type ArchivePasswordOrigin =
  | "EXPLICIT"
  | "INTEGRATION"
  | "DNZB_HEADER"
  | "NZB_META"
  | "FILENAME_CONVENTION"
  | "SETTINGS"
  | "PASSWORD_FILE";

export interface DownloadNodeData {
  id: number;
  parentId: number | null;
  kind: string;
  name: string | null;
  volumes: number;
  extracted: boolean;
  givenUp: boolean;
  reason: string | null;
  encrypted: boolean;
  encryptedHeaders: boolean;
  passwordNeeded: boolean;
  /** A newer server may name an origin this page does not know. */
  passwordOrigin: ArchivePasswordOrigin | (string & {}) | null;
}

export interface DownloadRecoverySetData {
  blocksReachable: number;
  damagedSlices: number;
  outcome: string | null;
  slicesRepaired: number;
}

export interface DownloadRecoveryData {
  kind: string;
  outcome: string | null;
  sets: DownloadRecoverySetData[];
  setCount: number;
}

export interface DownloadPostData {
  files: number;
  articles: number;
  missingArticles: number;
  failedArticles: number;
  oldestPost: number | null;
  newestPost: number | null;
  groups: number;
}

export interface DownloadSummaryData {
  layout: "PLAIN" | "ARCHIVE" | "MIXED" | null;
  nodes: DownloadNodeData[];
  nodeCount: number;
  maxDepth: number;
  recovery: DownloadRecoveryData[];
  recoveryCount: number;
  post: DownloadPostData;
}

/** The three places a password can come from, as the page names them. */
export type PasswordOriginGroup = "nzb" | "api" | "list";

const ORIGIN_GROUP: Record<string, PasswordOriginGroup | undefined> = {
  NZB_META: "nzb",
  FILENAME_CONVENTION: "nzb",
  DNZB_HEADER: "nzb",
  EXPLICIT: "api",
  INTEGRATION: "api",
  SETTINGS: "list",
  PASSWORD_FILE: "list",
};

export function passwordOriginGroup(origin: string | null | undefined): PasswordOriginGroup | null {
  return origin ? (ORIGIN_GROUP[origin] ?? null) : null;
}

/**
 * What a node says about its password: where the one that opened it came
 * from; that one opened it from a source this page cannot name; that one
 * is needed; or nothing.
 */
export type NodePassword =
  | { state: "origin"; group: PasswordOriginGroup }
  | { state: "opened" }
  | { state: "needed" }
  | null;

export function nodePassword(node: DownloadNodeData): NodePassword {
  if (node.passwordOrigin) {
    const group = passwordOriginGroup(node.passwordOrigin);
    return group ? { state: "origin", group } : { state: "opened" };
  }
  return node.passwordNeeded ? { state: "needed" } : null;
}

/** A node of the tree and the nodes under it, siblings by id. */
export interface DownloadTreeNode {
  node: DownloadNodeData;
  children: DownloadTreeNode[];
}

/**
 * The nodes as a nested tree: each under its parent, siblings by id. A node
 * whose parent is not in the list is a root. Every node appears exactly once:
 * one whose parents only lead back round to it (a loop, which a sound summary
 * never holds) is shown as a root too, where the loop is first met.
 */
export function downloadTree(nodes: readonly DownloadNodeData[]): DownloadTreeNode[] {
  const listed = new Set(nodes.map((node) => node.id));
  const children = new Map<number | null, DownloadNodeData[]>();
  for (const node of nodes) {
    const parent = node.parentId !== null && listed.has(node.parentId) ? node.parentId : null;
    const siblings = children.get(parent);
    if (siblings) siblings.push(node);
    else children.set(parent, [node]);
  }
  for (const siblings of children.values()) siblings.sort((a, b) => a.id - b.id);

  const placed = new Set<number>();
  const grow = (node: DownloadNodeData): DownloadTreeNode => {
    placed.add(node.id);
    const branch: DownloadTreeNode = { node, children: [] };
    // Iterative, so a deep tree does not run the stack out.
    const pending: DownloadTreeNode[] = [branch];
    while (pending.length > 0) {
      const at = pending.pop()!;
      for (const child of children.get(at.node.id) ?? []) {
        if (placed.has(child.id)) continue;
        placed.add(child.id);
        const grown: DownloadTreeNode = { node: child, children: [] };
        at.children.push(grown);
        pending.push(grown);
      }
    }
    return branch;
  };

  const roots = (children.get(null) ?? []).map(grow);
  for (const node of [...nodes].sort((a, b) => a.id - b.id)) {
    if (!placed.has(node.id)) roots.push(grow(node));
  }
  return roots;
}

/**
 * A server word as the page names it: a node or recovery kind, a recovery
 * outcome, or why a node gave up. The word itself when a newer server sends
 * one this page does not know.
 */
export function serverWord(
  t: (key: string) => string,
  group: "kind" | "outcome" | "reason",
  word: string,
): string {
  const key = `next.download.${group}.${word}`;
  const label = t(key);
  return label === key ? word : label;
}

/** A flat download is one with no archive in it: it shows no tree. */
export function showsTree(summary: DownloadSummaryData): boolean {
  return summary.nodes.length > 0;
}

/** What the panel shows: the summary, or why there is none. */
export type DownloadPanelState = "queued" | "unrecorded" | "summary";

export function downloadPanelState(
  inQueue: boolean,
  summary: DownloadSummaryData | null | undefined,
): DownloadPanelState {
  if (inQueue) return "queued";
  return summary ? "summary" : "unrecorded";
}
