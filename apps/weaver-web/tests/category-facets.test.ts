import assert from "node:assert/strict";
import test from "node:test";
import {
  categoryFacets,
  countsByFacet,
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

test("counts are omitted until there is something to count from", () => {
  // Before the first page comes back there is no honest number to show, and
  // the page's own rows are not one.
  const entries = categoryFacets({ configured: CONFIGURED });

  assert.deepEqual(
    entries.map((entry) => entry.count),
    [undefined, undefined, undefined, undefined, undefined],
  );
  assert.equal(labels(entries).at(-1), "Uncategorised");
});

test("server counts describe the archive, not the page in front of you", () => {
  // The rows of a history page filtered to `anime` are all anime. Counting
  // them would put a 0 beside every other category and drop the ones with
  // nothing on this page, which is how a rail filters itself into a dead end.
  const entries = categoryFacets({
    configured: CONFIGURED,
    counts: countsByFacet([
      { category: "movies", count: 12 },
      { category: "books", count: 3 },
      { category: "", count: 5 },
    ]),
  });

  const byLabel = new Map(entries.map((entry) => [entry.label, entry.count]));
  assert.deepEqual(labels(entries), [
    "All categories",
    "movies",
    "tv",
    "audio",
    "books",
    "Uncategorised",
  ]);
  assert.equal(byLabel.get("All categories"), 20);
  assert.equal(byLabel.get("movies"), 12);
  assert.equal(byLabel.get("tv"), 0);
  assert.equal(byLabel.get("books"), 3);
  assert.equal(byLabel.get("Uncategorised"), 5);
});

test("the empty string is how the daemon spells uncategorised, both ways", () => {
  const counts = countsByFacet([{ category: "", count: 2 }]);
  assert.deepEqual([...counts], [[UNCATEGORISED, 2]]);
  assert.deepEqual(facetsToCategories(new Set([UNCATEGORISED])), [""]);
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
