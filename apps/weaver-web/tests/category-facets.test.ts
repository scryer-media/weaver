import assert from "node:assert/strict";
import test from "node:test";
import {
  categoryFacets,
  facetsToCategories,
  toggleFacet,
  UNCATEGORISED,
} from "../src/next/data/categories.ts";

const CONFIGURED = [
  { id: 1, name: "movies", destDir: null },
  { id: 2, name: "tv", destDir: "/library/tv" },
  { id: 3, name: "audio", destDir: null },
];

function labels(entries: { label: string }[]): string[] {
  return entries.map((entry) => entry.label);
}

test("the rail lists what is configured, in the order it was configured", () => {
  // Nothing in the queue at all: the categories still exist, and a person can
  // still filter by them.
  const entries = categoryFacets({ configured: CONFIGURED, rows: [] });

  assert.deepEqual(labels(entries), ["All categories", "movies", "tv", "audio"]);
  assert.deepEqual(
    entries.map((entry) => entry.count),
    [0, 0, 0, 0],
  );
});

test("counts are per facet and ignore the current selection", () => {
  const entries = categoryFacets({
    configured: CONFIGURED,
    rows: [
      { category: "movies" },
      { category: "movies" },
      { category: "tv" },
      { category: null },
    ],
  });

  const byLabel = new Map(entries.map((entry) => [entry.label, entry.count]));
  assert.equal(byLabel.get("All categories"), 4);
  assert.equal(byLabel.get("movies"), 2);
  assert.equal(byLabel.get("tv"), 1);
  assert.equal(byLabel.get("audio"), 0);
  assert.equal(byLabel.get("Uncategorised"), 1);
});

test("uncategorised appears only once something is in it", () => {
  const entries = categoryFacets({ configured: CONFIGURED, rows: [{ category: "tv" }] });
  assert.equal(labels(entries).includes("Uncategorised"), false);
});

test("a category no longer configured stays reachable", () => {
  // Rename or delete a category and its finished jobs keep the old name; a
  // facet list built only from the configured names would strand them.
  const entries = categoryFacets({
    configured: CONFIGURED,
    rows: [{ category: "books" }],
    extras: ["software"],
  });

  assert.deepEqual(labels(entries), [
    "All categories",
    "movies",
    "tv",
    "audio",
    "books",
    "software",
  ]);
});

test("counts are omitted where the client cannot count", () => {
  // History pages on the server, so the only number available would describe
  // the page rather than the archive.
  const entries = categoryFacets({ configured: CONFIGURED });

  assert.deepEqual(
    entries.map((entry) => entry.count),
    [undefined, undefined, undefined, undefined, undefined],
  );
  assert.equal(labels(entries).at(-1), "Uncategorised");
});

test("a facet toggles off, and the last one off is the cleared state", () => {
  const one = toggleFacet(new Set(), "movies");
  assert.deepEqual([...one], ["movies"]);

  const two = toggleFacet(one, "tv");
  assert.deepEqual([...two].sort(), ["movies", "tv"]);

  const back = toggleFacet(two, "movies");
  assert.deepEqual([...back], ["tv"]);

  assert.equal(toggleFacet(back, "tv").size, 0);
});

test("the daemon's filter wants the empty string for uncategorised", () => {
  assert.equal(facetsToCategories(new Set()), undefined);
  assert.deepEqual(facetsToCategories(new Set(["movies"])), ["movies"]);
  assert.deepEqual(facetsToCategories(new Set([UNCATEGORISED])), [""]);
  assert.deepEqual(facetsToCategories(new Set(["tv", UNCATEGORISED])).sort(), ["", "tv"]);
});
