import assert from "node:assert/strict";
import { test } from "node:test";
import {
  filterBlocks,
  matchesTerms,
  SearchRegistry,
  searchOutcome,
  searchTerms,
  type SearchableBlock,
} from "../src/next/pages/settings/search.ts";

const section: SearchableBlock = {
  kind: "section",
  title: "Downloads",
  fields: [
    { label: "Speed limit", help: "Caps every server together", keywords: "MiB/s bandwidth" },
    { label: "Retry count", help: "How often a failed article is asked for again" },
    { label: "Proxy timeout", help: "Seconds to wait", collapsed: true },
  ],
};

const table: SearchableBlock = {
  kind: "table",
  title: "Servers",
  rows: [{ searchText: "primary news.example.test 563 TLS" }],
  groups: [
    { title: "Backup", rows: [{ searchText: "fill block.example.test 119" }] },
    { title: "Disabled", rows: [{ searchText: "old.example.test" }] },
  ],
};

const custom: SearchableBlock = { kind: "custom", title: "Flow", searchText: "egress wan0 consumer feeds" };

test("a query becomes lower-cased whitespace terms, and a blank one has none", () => {
  assert.deepEqual(searchTerms("  Speed   LIMIT "), ["speed", "limit"]);
  assert.deepEqual(searchTerms("   "), []);
  assert.ok(matchesTerms("Download speed limit", ["limit", "speed"]));
  assert.ok(!matchesTerms("Download speed", ["limit"]));
  assert.ok(matchesTerms("anything", []));
});

test("a blank query keeps every block untouched", () => {
  const blocks = [section, table, custom];
  const kept = filterBlocks(blocks, " ");
  assert.equal(kept.length, 3);
  assert.equal(kept[0], section);
});

test("fields match on label, help and keywords, in any case and order", () => {
  for (const query of ["SPEED", "caps server", "bandwidth", "limit speed"]) {
    const [kept] = filterBlocks([section], query);
    assert.ok(kept?.kind === "section", query);
    assert.deepEqual(kept.fields.map((field) => field.label), ["Speed limit"], query);
  }
  assert.deepEqual(filterBlocks([section], "nothing like this"), []);
});

test("a term may come from the block's title", () => {
  const [kept] = filterBlocks([section], "downloads retry");
  assert.ok(kept?.kind === "section");
  assert.deepEqual(kept.fields.map((field) => field.label), ["Retry count"]);
});

test("a block whose title matches survives whole", () => {
  const [kept] = filterBlocks([section], "download");
  assert.ok(kept?.kind === "section");
  assert.equal(kept.fields.length, 3);
  const [tableKept] = filterBlocks([table], "servers");
  assert.equal(tableKept, table);
});

test("a shut row still matches, and comes back open", () => {
  const [kept] = filterBlocks([section], "timeout");
  assert.ok(kept?.kind === "section");
  assert.deepEqual(kept.fields, [{ label: "Proxy timeout", help: "Seconds to wait", collapsed: false }]);
  // The panel's own copy is left alone.
  assert.equal(section.kind === "section" && section.fields[2]!.collapsed, true);
});

test("a title match opens only the shut rows that match on their own", () => {
  const block: SearchableBlock = {
    kind: "section",
    title: "Proxy",
    fields: [
      { label: "Enabled" },
      { label: "Proxy timeout", collapsed: true },
      { label: "Retries", collapsed: true },
    ],
  };
  const [kept] = filterBlocks([block], "proxy");
  assert.ok(kept?.kind === "section");
  assert.deepEqual(kept.fields.map((field) => field.collapsed), [undefined, false, true]);
});

test("table rows and grouped rows filter by their search text; a group title keeps its rows", () => {
  const [byRow] = filterBlocks([table], "block.example");
  assert.ok(byRow?.kind === "table");
  assert.deepEqual(byRow.rows, []);
  assert.deepEqual(byRow.groups?.map((group) => group.rows.length), [1, 0]);

  const [byGroup] = filterBlocks([table], "disabled");
  assert.ok(byGroup?.kind === "table");
  assert.deepEqual(byGroup.groups?.map((group) => group.rows.length), [0, 1]);

  const [topLevel] = filterBlocks([table], "563");
  assert.ok(topLevel?.kind === "table");
  assert.equal(topLevel.rows.length, 1);

  assert.deepEqual(filterBlocks([table], "nowhere"), []);
});

test("custom blocks match their title or their search text", () => {
  assert.equal(filterBlocks([custom], "wan0")[0], custom);
  assert.equal(filterBlocks([custom], "flow")[0], custom);
  assert.deepEqual(filterBlocks([custom], "lan"), []);
});

test("other properties of a kept block pass through untouched", () => {
  const rich = { ...section, id: "downloads", note: "a note" };
  const [kept] = filterBlocks([rich], "retry");
  assert.equal(kept?.id, "downloads");
  assert.equal(kept?.note, "a note");
});

test("the registry sums a panel's lists and forgets removed ones", () => {
  const registry = new SearchRegistry();
  let notified = 0;
  const unsubscribe = registry.subscribe(() => {
    notified += 1;
  });
  registry.report("a", "networking/proxies", { count: 1, loading: false });
  registry.report("b", "networking/proxies", { count: 2, loading: false });
  registry.report("c", "general", { count: 0, loading: true });
  assert.deepEqual(registry.panel("networking/proxies"), { count: 3, loading: false });
  assert.deepEqual(registry.panel("general"), { count: 0, loading: true });
  assert.deepEqual(registry.panel("backup"), { count: 0, loading: false });
  assert.equal(notified, 3);

  // An unchanged report is not news.
  registry.report("a", "networking/proxies", { count: 1, loading: false });
  assert.equal(notified, 3);

  registry.remove("b");
  assert.deepEqual(registry.panel("networking/proxies"), { count: 1, loading: false });
  registry.remove("b");
  assert.equal(notified, 4);

  unsubscribe();
  registry.remove("a");
  assert.equal(notified, 4);
});

test("the outcome lists matching panels in rail order", () => {
  const registry = new SearchRegistry();
  const rail = ["general", "servers", "networking/proxies", "backup"];
  registry.report("x", "backup", { count: 1, loading: false });
  registry.report("y", "general", { count: 2, loading: false });
  registry.report("z", "servers", { count: 0, loading: false });
  assert.deepEqual(searchOutcome(rail, (slug) => registry.panel(slug)), {
    state: "results",
    panels: ["general", "backup"],
  });
});

test("results show while another panel still loads; an empty answer waits for it", () => {
  const registry = new SearchRegistry();
  const rail = ["general", "servers"];
  const outcome = () => searchOutcome(rail, (slug) => registry.panel(slug));

  assert.deepEqual(outcome(), { state: "empty" });

  registry.report("s", "servers", { count: 0, loading: true });
  assert.deepEqual(outcome(), { state: "loading" });

  registry.report("g", "general", { count: 1, loading: false });
  assert.deepEqual(outcome(), { state: "results", panels: ["general"] });

  registry.remove("g");
  registry.report("s", "servers", { count: 0, loading: false });
  assert.deepEqual(outcome(), { state: "empty" });
});
