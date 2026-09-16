import assert from "node:assert/strict";
import test from "node:test";
import {
  describeEntryCount,
  filterEntries,
  parentPath,
  pathCrumbs,
} from "../src/next/data/directory-browser.ts";
import { englishTranslate as t } from "./english-translate.ts";

test("crumbs walk a POSIX path from the root", () => {
  assert.deepEqual(pathCrumbs("/media/library/shows/"), [
    { label: "/", path: "/" },
    { label: "media", path: "/media" },
    { label: "library", path: "/media/library" },
    { label: "shows", path: "/media/library/shows" },
  ]);
  assert.deepEqual(pathCrumbs("/"), [{ label: "/", path: "/" }]);
});

test("crumbs keep a Windows drive and its separator", () => {
  assert.deepEqual(pathCrumbs("D:\\Downloads\\complete"), [
    { label: "D:\\", path: "D:\\" },
    { label: "Downloads", path: "D:\\Downloads" },
    { label: "complete", path: "D:\\Downloads\\complete" },
  ]);
  assert.deepEqual(pathCrumbs("C:/data"), [
    { label: "C:/", path: "C:/" },
    { label: "data", path: "C:/data" },
  ]);
});

test("crumbs treat a UNC share as the root", () => {
  assert.deepEqual(pathCrumbs("\\\\nas\\media\\incoming"), [
    { label: "\\\\nas\\media\\", path: "\\\\nas\\media\\" },
    { label: "incoming", path: "\\\\nas\\media\\incoming" },
  ]);
});

test("a relative or blank path has nothing to walk", () => {
  assert.deepEqual(pathCrumbs("downloads"), [{ label: "downloads", path: "downloads" }]);
  assert.deepEqual(pathCrumbs("  "), []);
});

test("the parent of a root is null", () => {
  assert.equal(parentPath("/media/library"), "/media");
  assert.equal(parentPath("/media"), "/");
  assert.equal(parentPath("/"), null);
  assert.equal(parentPath("E:\\"), null);
  assert.equal(parentPath("E:\\Archive"), "E:\\");
});

test("filtering matches names without regard to case", () => {
  const entries = [
    { name: "Glass Harbor", path: "/m/Glass Harbor" },
    { name: "quiet-meridian", path: "/m/quiet-meridian" },
    { name: "HARBOR LIGHTS", path: "/m/HARBOR LIGHTS" },
  ];
  assert.deepEqual(
    filterEntries(entries, " harbor ").map((entry) => entry.name),
    ["Glass Harbor", "HARBOR LIGHTS"],
  );
  assert.equal(filterEntries(entries, ""), entries);
});

test("the count says how much a filter hides", () => {
  assert.equal(describeEntryCount(t, 1204, 1204), "1,204 folders");
  assert.equal(describeEntryCount(t, 12, 1204), "12 of 1,204 folders");
  assert.equal(describeEntryCount(t, 1, 1), "1 folder");
});
